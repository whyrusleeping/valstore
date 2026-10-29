use super::*;

pub(super) fn workload_rtqueue(backend: &str, dir: &str, vsize: usize) {
    use vals_cluster::liveness::MemLivenessLog;
    use vals_cluster::observe;
    use vals_cluster::pd::{PdDaemon, RingConfig};
    use vals_cluster::reactor;
    use vals_cluster::zonestore::{FileZoneStore, MemZoneStore, ZoneGeom, ZoneStore};
    use vals_cluster::{
        Catalog, Coordinator, WriteConcern, NetTransport, Node, Ordering as ZOrdering, Ring,
        ZoneConfig, DEFAULT_ZONE,
    };

    const K: u32 = 3;
    const RF: usize = 3;
    const PEERS: u64 = K as u64 - 1;

    let settings = node_settings();
    let (placement, _) = vals_cluster::pd::PdPlacementOverrides::default()
        .resolve(|name| std::env::var_os(name).map(|value| value.to_string_lossy().into_owned()))
        .expect("invalid PD placement environment settings");
    let sweep = Duration::from_millis(env_u64("VALS_RTQ_SWEEP_MS", 200));
    let warmup = Duration::from_secs(env_u64("VALS_RTQ_WARMUP_SECS", 10));
    let secs = env_u64("VALS_RTQ_SECS", 60);
    let every = env_u64("VALS_RTQ_DELETE_EVERY", 16) as usize;
    let backlog = env_u64("VALS_RTQ_BACKLOG", 1024) as usize;
    let rate = env_u64("VALS_RTQ_RATE", 2000);
    let region_mib = env_u64("VALS_RTQ_REGION_MIB", 512);
    let geom = ZoneGeom {
        block_size: BLOCK_SIZE,
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        region_blocks: region_mib * 1024 * 1024 / BLOCK_SIZE as u64,
    };
    println!(
        "--- rtqueue ({backend}): {K} nodes, sorted zone RF={RF}, sweep={}ms, warmup={}s, measure={secs}s ---",
        sweep.as_millis(),
        warmup.as_secs()
    );
    println!(
        "    producer appends q<seq> at {rate}/s (0 = unpaced); after every {every} appends the consumer range-deletes [q, q<seq - {backlog}>)"
    );
    println!("    (knobs: VALS_RTQ_SWEEP_MS / _WARMUP_SECS / _SECS / _RATE / _DELETE_EVERY (0 = no deletes) / _BACKLOG / _REGION_MIB)");

    let mut built = Vec::new();
    for id in 1..=K {
        let (listener, addr) = Node::bind().expect("bind node listener");
        built.push((id, listener, addr));
    }
    let addrs: Vec<(u32, std::net::SocketAddr)> = built.iter().map(|(id, _, a)| (*id, *a)).collect();
    let mut boot_ring = Ring::new(RF);
    boot_ring.set_members_weighted(addrs.iter().map(|(id, _)| (*id, 1)));
    let peers_of = |id: u32| -> Vec<(u32, std::net::SocketAddr)> {
        addrs.iter().filter(|(pid, _)| *pid != id).copied().collect()
    };
    let mut handles = Vec::new();
    for (id, listener, _) in built {
        let h = match backend {
            "file" => {
                let provider = FileZoneStore::new(id, format!("{dir}/node{id}"), geom).expect("open file zone store");
                let zone0 = provider.open(DEFAULT_ZONE, ZOrdering::Unsorted).expect("open zone 0");
                Node::serve_zoned_with_liveness_and_settings(
                    id, listener, zone0, provider, Catalog::new(), boot_ring.clone(), peers_of(id), sweep,
                    60_000, MemLivenessLog::new(), None, settings.clone(),
                )
            }
            "mem" => {
                let provider = MemZoneStore::new(id, geom);
                let zone0 = provider.open(DEFAULT_ZONE, ZOrdering::Unsorted).expect("open zone 0");
                Node::serve_zoned_with_liveness_and_settings(
                    id, listener, zone0, provider, Catalog::new(), boot_ring.clone(), peers_of(id), sweep,
                    60_000, MemLivenessLog::new(), None, settings.clone(),
                )
            }
            other => {
                eprintln!("rtqueue runs on the mem or file backend, not {other}");
                std::process::exit(2);
            }
        };
        handles.push(h);
    }

    let engine = Engine::create_prewiped(MemBlockDevice::new(4096, 16 * 1024), Config::default());
    let ring_cfg = RingConfig { vnodes: vals_cluster::ring::DEFAULT_VNODES, rf: RF as u32 };
    let data_nodes: Vec<(u32, Vec<std::net::SocketAddr>)> = addrs.iter().map(|(id, a)| (*id, vec![*a])).collect();
    let pd = PdDaemon::spawn_single_with_settings(9000, engine, ring_cfg, data_nodes.clone(), placement).expect("spawn pd");
    if !poll_until(Duration::from_secs(2), Duration::from_millis(20), || !pd.members().is_empty()) {
        pd.bootstrap_members(addrs.iter().map(|(id, addr)| (*id, 1, vec![*addr])).collect());
    }
    pd.create_zone(ZoneConfig { name: "rtq".to_string(), rf: RF, ordering: ZOrdering::Sorted, scope: None })
        .expect("propose the queue zone");
    let mut zone_id = 0;
    let found = poll_until(Duration::from_secs(10), Duration::from_millis(100), || {
        pd.catalog().by_name("rtq").map(|id| zone_id = id).is_some()
    });
    assert!(found, "queue zone committed");

    let transport = NetTransport::connect_sharded_unbounded(data_nodes.clone());
    let coord = Coordinator::new(7400, boot_ring.clone(), 2, transport);
    vals_rt::block_on(coord.adopt_from_any_member(), reactor::poll_io);
    let key = |i: usize| format!("q{i:012}").into_bytes();
    let ready = poll_until(Duration::from_secs(30), Duration::from_millis(200), || {
        let _ = vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io);
        vals_rt::block_on(coord.write_in(zone_id, &key(0), b"0", WriteConcern::Quorum(2)), reactor::poll_io).is_ok()
            && vals_rt::block_on(coord.delete_range_in(zone_id, Some(b"q"), Some(&key(0)), WriteConcern::Quorum(2)), reactor::poll_io)
                .is_ok()
    });
    assert!(ready, "queue zone serves writes and range deletes");

    let mut seq = 0usize;
    let deletes = std::cell::Cell::new(0u64);
    let delete_errors = std::cell::Cell::new(0u64);
    let write_errors = std::cell::Cell::new(0u64);
    let bump = |c: &std::cell::Cell<u64>| c.set(c.get() + 1);
    let paced = Instant::now();
    let step = |seq: &mut usize| {
        let i = *seq;
        if rate > 0 {
            let due = paced + Duration::from_secs_f64(i as f64 / rate as f64);
            if let Some(wait) = due.checked_duration_since(Instant::now()) {
                std::thread::sleep(wait);
            }
        }
        if vals_rt::block_on(coord.write_in(zone_id, &key(i), &make_value(vsize, i as u64), WriteConcern::Quorum(2)), reactor::poll_io)
            .is_err()
        {
            bump(&write_errors);
        }
        *seq += 1;
        if every > 0 && (*seq).is_multiple_of(every) && *seq > backlog {
            let hi = key(*seq - backlog);
            match vals_rt::block_on(coord.delete_range_in(zone_id, Some(b"q"), Some(&hi), WriteConcern::Quorum(2)), reactor::poll_io) {
                Ok(_) => bump(&deletes),
                Err(_) => bump(&delete_errors),
            }
        }
    };
    let w0 = Instant::now();
    while w0.elapsed() < warmup {
        step(&mut seq);
    }
    let start = observe::snapshot();
    let (seq0, del0) = (seq, deletes.get());
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(secs) {
        step(&mut seq);
    }
    let wall = t0.elapsed().as_secs_f64();
    let end = observe::snapshot();
    let del1 = deletes.get();

    let d = |f: fn(&observe::CountersSnapshot) -> u64| f(&end).saturating_sub(f(&start));
    let passes = d(|s| s.sweep_passes);
    let held = d(|s| s.sweep_sorted_zones_held);
    let skips = d(|s| s.ae_seed_reconcile_skips);
    let errors = d(|s| s.ae_sync_errors);
    let slots = (passes - held.min(passes)) * PEERS;
    let lost = skips * PEERS + errors;
    println!(
        "  load        : {:.0} appends/s, {:.0} range deletes/s ({} write errors, {} delete errors)",
        (seq - seq0) as f64 / wall,
        (del1 - del0) as f64 / wall,
        write_errors.get(),
        delete_errors.get(),
    );
    println!(
        "  AE slots    : {slots} queue-zone reconcile slots (sweep passes {passes} x {PEERS} peers, {held} held); \
         {skips} local skips (x{PEERS}) + {errors} failed reconciles = {lost} lost"
    );
    println!(
        "  BUSY        : {:.2}% of queue-zone reconcile slots",
        if slots == 0 { 0.0 } else { 100.0 * lost as f64 / slots as f64 }
    );
    let fields: [(&str, u64); 19] = [
        ("ae_dirty_rebuild_requests", d(|s| s.ae_dirty_rebuild_requests)),
        ("ae_dirty_rebuild_retries", d(|s| s.ae_dirty_rebuild_retries)),
        ("ae_dirty_rebuild_errors", d(|s| s.ae_dirty_rebuild_errors)),
        ("busy_rejections", d(|s| s.busy_rejections)),
        ("ram_busy_rejections", d(|s| s.ram_busy_rejections)),
        ("ae_digest_serve_rejections", d(|s| s.ae_digest_serve_rejections)),
        ("rt_fold_not_ready_busy", d(|s| s.rt_fold_not_ready_busy)),
        ("ae_seed_not_ready_busy", d(|s| s.ae_seed_not_ready_busy)),
        ("ae_regroup_not_ready_busy", d(|s| s.ae_regroup_not_ready_busy)),
        ("ae_seed_completions", d(|s| s.ae_seed_completions)),
        ("rt_fold_completions", d(|s| s.rt_fold_completions)),
        ("rt_fold_pages", d(|s| s.rt_fold_pages)),
        ("rt_fold_winners_total", d(|s| s.rt_fold_winners_total)),
        ("rt_fold_retired", d(|s| s.rt_fold_retired)),
        ("rt_fold_memtable_removed", d(|s| s.rt_fold_memtable_removed)),
        ("range_deletes_applied_total", d(|s| s.range_deletes_applied_total)),
        ("ae_tree_nodes_examined", d(|s| s.ae_tree_nodes_examined)),
        ("ae_digests_examined", d(|s| s.ae_digests_examined)),
        ("ae_entries_applied", d(|s| s.ae_entries_applied)),
    ];
    for (name, v) in fields {
        println!("    {name:<28}: {v}");
    }

    let head = vals_rt::block_on(coord.scan_in(zone_id, None, None, 2, Some(backlog + 1)), reactor::poll_io);
    match head {
        Ok(entries) => println!(
            "  queue head  : {} live entries, first {:?} (expected about {backlog})",
            entries.len(),
            entries.first().map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        ),
        Err(e) => println!("  queue head  : scan failed: {e}"),
    }

    drop(coord);
    for h in handles {
        h.shutdown();
    }
    pd.shutdown();
}
