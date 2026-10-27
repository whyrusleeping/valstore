use super::*;

#[allow(clippy::too_many_arguments)]
fn distnode_core<D, Z, L>(
    label: &str,
    n: usize,
    vsize: usize,
    mut make: impl FnMut(u32) -> (vals_cluster::VersionedStore<D>, Z, L),
) where
    D: vals_spdk::BlockDevice + vals_spdk::AsyncRead + Send + 'static,
    Z: vals_cluster::zonestore::ZoneStore<D> + Send + 'static,
    L: vals_cluster::liveness::LivenessLog + Send + 'static,
{
    use vals_cluster::observe;
    use vals_cluster::pd::{PdDaemon, RingConfig};
    use vals_cluster::reactor;
    use vals_cluster::{
        Catalog, Coordinator, WriteConcern, NetTransport, Node, Ordering, ReadFanout, Ring,
        ZoneConfig,
    };
    use vals_store::{Config, Engine};

    const K: u32 = 3;
    const RF: usize = 3;
    const R: usize = 2;
    const JOINER: u32 = 4;

    let settings = node_settings();
    let (placement, legacy) = vals_cluster::pd::PdPlacementOverrides::default()
        .resolve(|name| std::env::var_os(name).map(|value| value.to_string_lossy().into_owned()))
        .expect("invalid PD placement environment settings");
    println!("effective PD placement settings: {placement:?}");
    if !legacy.is_empty() {
        eprintln!("deprecated PD environment settings: {}", legacy.join(", "));
    }
    let sweep = Duration::from_millis(env_u64("VALS_DISTNODE_SWEEP_MS", 2000));
    let grace_ms = env_u64("VALS_DISTNODE_GRACE_MS", 60_000);
    let churn_secs = env_u64("VALS_DISTNODE_CHURN_SECS", 20);
    let heal_sweeps = env_u64("VALS_DISTNODE_HEAL_SWEEPS", 15);
    let reb_sweeps = env_u64("VALS_DISTNODE_REB_SWEEPS", 20);

    let key = |i: usize| format!("k{i:09}").into_bytes();
    let val = |i: usize| make_value(vsize, i as u64);
    let zkey = |i: usize| format!("zk{i:09}").into_bytes();

    println!(
        "--- distnode ({label}): {K} live Nodes (id 1..{K}) + a joiner (id {JOINER}), RF={RF}, R={R} ---"
    );
    println!(
        "    sweep={}ms grace={}ms churn={}s heal_sweeps={heal_sweeps} reb_sweeps={reb_sweeps}",
        sweep.as_millis(),
        grace_ms,
        churn_secs,
    );
    println!(
        "    (knobs: VALS_DISTNODE_SWEEP_MS / _GRACE_MS / _CHURN_SECS / _HEAL_SWEEPS / _REB_SWEEPS)"
    );
    println!(
        "    AE budgets: 50k digests / 8192 value-reads per pass — size heal windows ≈ m/8192 + keys/50k sweeps"
    );

    let start = observe::snapshot();

    let mut built = Vec::new();
    for id in 1..=K {
        let (listener, addr) = Node::bind().expect("bind node listener");
        let (zone0, provider, live) = make(id);
        built.push((id, listener, addr, zone0, provider, live));
    }
    let addrs: Vec<(u32, std::net::SocketAddr)> =
        built.iter().map(|(id, _, a, _, _, _)| (*id, *a)).collect();
    let mut boot_ring = Ring::new(RF);
    boot_ring.set_members_weighted(addrs.iter().map(|(id, _)| (*id, 1)));

    let mut handles = Vec::new();
    for (id, listener, _addr, zone0, provider, live) in built {
        let peers: Vec<(u32, std::net::SocketAddr)> = addrs
            .iter()
            .filter(|(pid, _)| *pid != id)
            .map(|(pid, a)| (*pid, *a))
            .collect();
        let h = Node::serve_zoned_with_liveness_and_settings(
            id,
            listener,
            zone0,
            provider,
            Catalog::new(),
            boot_ring.clone(),
            peers,
            sweep,
            grace_ms,
            live,
            None,
            settings.clone(),
        );
        handles.push((id, h));
    }

    let engine =
        Engine::create_prewiped(vals_spdk::MemBlockDevice::new(4096, 16 * 1024), Config::default());
    let ring_cfg = RingConfig {
        vnodes: vals_cluster::ring::DEFAULT_VNODES,
        rf: RF as u32,
    };
    let data_nodes: Vec<(u32, Vec<std::net::SocketAddr>)> =
        addrs.iter().map(|(id, a)| (*id, vec![*a])).collect();
    let pd = PdDaemon::spawn_single_with_settings(9000, engine, ring_cfg, data_nodes, placement)
        .expect("spawn pd");
    let seed_deadline = Instant::now() + Duration::from_secs(2);
    while pd.members().is_empty() && Instant::now() < seed_deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    if pd.members().is_empty() {
        pd.bootstrap_members(
            addrs
                .iter()
                .map(|(id, addr)| (*id, 1, vec![*addr]))
                .collect(),
        );
    }

    let booted = poll_until(Duration::from_secs(30), Duration::from_millis(250), || {
        let s = observe::snapshot();
        s.unfences.saturating_sub(start.unfences) >= K as u64
            && s.ring_adoptions.saturating_sub(start.ring_adoptions) >= K as u64
    });
    let boot = observe::snapshot();
    println!(
        "  boot lease  : {} — unfences +{} fence_trips +{} ring_adoptions +{} catalog_adoptions +{}",
        if booted { "OK" } else { "CHECK (timed out)" },
        boot.unfences - start.unfences,
        boot.fence_trips - start.fence_trips,
        boot.ring_adoptions - start.ring_adoptions,
        boot.catalog_adoptions - start.catalog_adoptions,
    );

    pd.create_zone(ZoneConfig {
        name: "znode".to_string(),
        rf: RF,
        ordering: Ordering::Unsorted,
        scope: None,
    })
    .expect("propose benchmark zone");
    let zone_id = {
        let mut found = None;
        let _ = poll_until(Duration::from_secs(10), Duration::from_millis(100), || {
            found = pd.catalog().by_name("znode");
            found.is_some()
        });
        found.expect("named zone committed")
    };

    let mk_transport = || {
        NetTransport::connect_sharded_unbounded(
            addrs
                .iter()
                .map(|(id, a)| (*id, vec![*a]))
                .collect::<Vec<_>>(),
        )
    };
    let load = Coordinator::new(7100, boot_ring.clone(), R, mk_transport());
    vals_rt::block_on(load.adopt_from_any_member(), reactor::poll_io);

    let nz = (n / 10).max(1);
    let zone_ready = poll_until(Duration::from_secs(15), Duration::from_millis(200), || {
        let _ = vals_rt::block_on(load.refresh_catalog(1), reactor::poll_io);
        vals_rt::block_on(
            load.write_in(zone_id, b"__probe__", b"1", WriteConcern::Quorum(2)),
            reactor::poll_io,
        )
        .is_ok()
    });
    for i in 0..nz {
        let _ = vals_rt::block_on(
            load.write_in(zone_id, &zkey(i), &val(i), WriteConcern::Quorum(2)),
            reactor::poll_io,
        );
    }
    println!(
        "  named zone  : id={zone_id} rf={RF} — {} ({nz} keys loaded)",
        if zone_ready { "serving" } else { "CHECK (probe never acked)" },
    );

    let present_default = |coord: &Coordinator<NetTransport>, idxs: &[usize]| -> Option<usize> {
        let mut m = 0usize;
        for &i in idxs {
            match vals_rt::block_on(coord.read(&key(i)), reactor::poll_io) {
                Ok(Some(s)) if s.value.as_deref() == Some(val(i).as_slice()) => {}
                Ok(Some(_)) => m += 1,
                _ => return None,
            }
        }
        Some(m)
    };
    let present_zone =
        |coord: &Coordinator<NetTransport>, zid: u32, idxs: &[usize]| -> Option<usize> {
            let mut m = 0usize;
            for &i in idxs {
                match vals_rt::block_on(coord.read_in(zid, &zkey(i)), reactor::poll_io) {
                    Ok(Some(s)) if s.value.as_deref() == Some(val(i).as_slice()) => {}
                    Ok(Some(_)) => m += 1,
                    _ => return None,
                }
            }
            Some(m)
        };

    let w0 = Instant::now();
    for i in 0..n {
        vals_rt::block_on(
            load.write_with(&key(i), &val(i), WriteConcern::Quorum(2)),
            reactor::poll_io,
        )
        .expect("P1 write");
    }
    let wsecs = w0.elapsed().as_secs_f64();
    println!("  P1 load     : {:.0} ops/s ({n} keys, Quorum(2))", n as f64 / wsecs);

    let r0 = Instant::now();
    let mut p2_mism = 0usize;
    for i in 0..n {
        let got = vals_rt::block_on(load.read(&key(i)), reactor::poll_io).expect("P2 read");
        if got.and_then(|s| s.value) != Some(val(i)) {
            p2_mism += 1;
        }
    }
    let rsecs = r0.elapsed().as_secs_f64();
    println!(
        "  P2 verify   : {:.0} ops/s ({p2_mism} mismatches of {n})",
        n as f64 / rsecs
    );

    let p3 = observe::snapshot();
    load.transport().set_down(1, true);
    let m = (n / 4).max(2000);
    for i in n..n + m {
        vals_rt::block_on(
            load.write_with(&key(i), &val(i), WriteConcern::Quorum(2)),
            reactor::poll_io,
        )
        .expect("P3 write with node1 unreachable");
    }
    let hints = observe::snapshot().hints_recorded - p3.hints_recorded;
    load.transport().set_down(1, false);

    let hv = Coordinator::new(7200, boot_ring.clone(), 1, mk_transport())
        .with_read_fanout(ReadFanout::All);
    vals_rt::block_on(hv.adopt_from_any_member(), reactor::poll_io);
    hv.transport().set_down(2, true);
    hv.transport().set_down(3, true);

    let heal_idxs: Vec<usize> = (n..n + m).collect();
    let mut heal_mism = None;
    let t = Instant::now();
    let healed = poll_until(sweep * heal_sweeps as u32, Duration::from_millis(250), || {
        match present_default(&hv, &heal_idxs) {
            Some(mm) => {
                heal_mism = Some(mm);
                true
            }
            None => false,
        }
    });
    let heal_secs = t.elapsed().as_secs_f64();
    let p3_end = observe::snapshot();
    if healed {
        println!(
            "  P3 heal     : node1 re-synced {m} keys via sweep in {heal_secs:.1}s ({:.0} keys/s, {} mismatches)",
            m as f64 / heal_secs,
            heal_mism.unwrap_or(0),
        );
    } else {
        println!("  P3 heal     : CHECK — node1 did not re-sync all {m} keys within {heal_sweeps} sweeps");
    }
    println!(
        "              hints_recorded +{hints} | sweep_passes +{} ae_digests +{} ae_value_reads +{} ae_applied +{}",
        p3_end.sweep_passes - p3.sweep_passes,
        p3_end.ae_digests_examined - p3.ae_digests_examined,
        p3_end.ae_value_reads - p3.ae_value_reads,
        p3_end.ae_entries_applied - p3.ae_entries_applied,
    );

    let p4 = observe::snapshot();
    let (jl, jaddr) = Node::bind().expect("bind joiner");
    let (jzone0, jprov, jlive) = make(JOINER);
    let mut jring = Ring::new(RF);
    jring.set_members_weighted([(JOINER, 1)]);
    let jh = Node::serve_zoned_with_liveness_and_settings(
        JOINER,
        jl,
        jzone0,
        jprov,
        Catalog::new(),
        jring,
        vec![],
        sweep,
        grace_ms,
        jlive,
        None,
        settings.clone(),
    );
    pd.add_node_addressed(JOINER, 1, vec![jaddr])
        .expect("propose joining benchmark node");

    let peak_live = std::cell::Cell::new(observe::gauges().live_keys.get());
    let bump_peak = || peak_live.set(peak_live.get().max(observe::gauges().live_keys.get()));

    let rv = Coordinator::new(7300, boot_ring.clone(), 1, mk_transport())
        .with_read_fanout(ReadFanout::All);
    let joined = poll_until(Duration::from_secs(30), Duration::from_millis(250), || {
        bump_peak();
        vals_rt::block_on(rv.adopt_from_any_member(), reactor::poll_io);
        rv.ring().members().any(|mid| mid == JOINER)
    });

    const OWN_CAP: usize = 4000;
    let jr = rv.ring().clone();
    let owned_default: Vec<usize> = (0..n + m)
        .filter(|&i| jr.replicas(&key(i)).contains(&JOINER))
        .take(OWN_CAP)
        .collect();
    let owned_zone: Vec<usize> = (0..nz)
        .filter(|&i| {
            rv.replicas_for(zone_id, &zkey(i))
                .map(|r| r.contains(&JOINER))
                .unwrap_or(false)
        })
        .take(OWN_CAP)
        .collect();

    rv.transport().set_down(1, true);
    rv.transport().set_down(2, true);
    rv.transport().set_down(3, true);

    let t = Instant::now();
    let rebalanced = poll_until(sweep * reb_sweeps as u32, Duration::from_millis(250), || {
        bump_peak();
        present_default(&rv, &owned_default).is_some() && present_zone(&rv, zone_id, &owned_zone).is_some()
    });
    let reb_secs = t.elapsed().as_secs_f64();
    let reb_default_mism = present_default(&rv, &owned_default);
    let reb_zone_mism = present_zone(&rv, zone_id, &owned_zone);
    bump_peak();
    let peak_live = peak_live.get();
    std::thread::sleep(sweep * 4);
    let settled_live = observe::gauges().live_keys.get();
    let p4_end = observe::snapshot();
    if joined && rebalanced {
        println!(
            "  P4 rebalance: joiner {JOINER} owns {} default + {} zone keys, all served from it alone in {reb_secs:.1}s",
            owned_default.len(),
            owned_zone.len(),
        );
    } else {
        println!(
            "  P4 rebalance: CHECK — joined={joined} rebalanced={rebalanced} within {reb_sweeps} sweeps (owns {} default + {} zone keys)",
            owned_default.len(),
            owned_zone.len(),
        );
    }
    println!(
        "              peers_added +{} | sweep_passes +{} ae_value_reads +{} ae_applied +{} | live_keys peak {peak_live} settled {settled_live} | cleanup_errors {} cleanup_quiesce_timeouts {}",
        p4_end.peers_added - p4.peers_added,
        p4_end.sweep_passes - p4.sweep_passes,
        p4_end.ae_value_reads - p4.ae_value_reads,
        p4_end.ae_entries_applied - p4.ae_entries_applied,
        p4_end.cleanup_errors - p4.cleanup_errors,
        p4_end.cleanup_quiesce_timeouts - p4.cleanup_quiesce_timeouts,
    );

    vals_rt::block_on(load.adopt_from_any_member(), reactor::poll_io);
    let p5 = observe::snapshot();
    let churn_end = Instant::now() + Duration::from_secs(churn_secs);
    let mut churn_ops = 0usize;
    let mut churn_mism = 0usize;
    let mut i = 0usize;
    while Instant::now() < churn_end {
        let wk = i % n;
        let mut backoff_ms = 1u64;
        let wrote = loop {
            match vals_rt::block_on(
                load.write_with(&key(wk), &val(wk + 1_000_000), WriteConcern::Quorum(2)),
                reactor::poll_io,
            ) {
                Ok(_) => break true,
                Err(vals_cluster::CoordError::Busy { .. }) if Instant::now() < churn_end => {
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(64);
                    continue;
                }
                _ => break false,
            }
        };
        if wrote {
            churn_ops += 1;
        }
        let rk = (i.wrapping_mul(7)) % n;
        match vals_rt::block_on(load.read(&key(rk)), reactor::poll_io) {
            Ok(Some(_)) => churn_ops += 1,
            _ => churn_mism += 1,
        }
        i += 1;
    }
    let churn_wall = churn_secs.max(1) as f64;
    let p5_end = observe::snapshot();
    println!(
        "  P5 churn    : {:.0} ops/s over {churn_secs}s ({churn_mism} read misses)",
        churn_ops as f64 / churn_wall
    );
    println!(
        "              sweep_passes +{} compactions +{} segments_freed +{} server_ops +{}",
        p5_end.sweep_passes - p5.sweep_passes,
        p5_end.compactions - p5.compactions,
        p5_end.segments_freed - p5.segments_freed,
        p5_end.server_ops - p5.server_ops,
    );

    let end = observe::snapshot();
    let d = |a: u64, b: u64| a.saturating_sub(b);
    let fields: [(&str, u64); 20] = [
        ("sweep_passes", d(end.sweep_passes, start.sweep_passes)),
        ("ae_tree_nodes_examined", d(end.ae_tree_nodes_examined, start.ae_tree_nodes_examined)),
        ("ae_digests_examined", d(end.ae_digests_examined, start.ae_digests_examined)),
        ("ae_value_reads", d(end.ae_value_reads, start.ae_value_reads)),
        ("ae_entries_applied", d(end.ae_entries_applied, start.ae_entries_applied)),
        ("ae_sync_errors", d(end.ae_sync_errors, start.ae_sync_errors)),
        ("cleanup_errors", d(end.cleanup_errors, start.cleanup_errors)),
        ("cleanup_quiesce_timeouts", d(end.cleanup_quiesce_timeouts, start.cleanup_quiesce_timeouts)),
        ("compactions", d(end.compactions, start.compactions)),
        ("segments_freed", d(end.segments_freed, start.segments_freed)),
        ("fence_trips", d(end.fence_trips, start.fence_trips)),
        ("unfences", d(end.unfences, start.unfences)),
        ("wipes", d(end.wipes, start.wipes)),
        ("ring_adoptions", d(end.ring_adoptions, start.ring_adoptions)),
        ("catalog_adoptions", d(end.catalog_adoptions, start.catalog_adoptions)),
        ("peers_added", d(end.peers_added, start.peers_added)),
        ("pd_repushes", d(end.pd_repushes, start.pd_repushes)),
        ("pd_push_failures", d(end.pd_push_failures, start.pd_push_failures)),
        ("hints_recorded", d(end.hints_recorded, start.hints_recorded)),
        ("server_ops", d(end.server_ops, start.server_ops)),
    ];
    println!("  --- healing-counter deltas (whole run) ---");
    for (name, v) in fields {
        println!("    {name:<26}: {v}");
    }

    let unfences = d(end.unfences, start.unfences);
    let wipes = d(end.wipes, start.wipes);
    let cleanup_errors = d(end.cleanup_errors, start.cleanup_errors);
    let flows_ok = p2_mism == 0
        && healed
        && heal_mism == Some(0)
        && joined
        && rebalanced
        && reb_default_mism == Some(0)
        && reb_zone_mism == Some(0)
        && unfences >= (K + 1) as u64
        && wipes == 0
        && cleanup_errors == 0
        && churn_mism == 0;
    println!("  FLOWS       : {}", if flows_ok { "OK" } else { "CHECK" });

    drop(load);
    drop(hv);
    drop(rv);
    for (_, h) in handles {
        h.shutdown();
    }
    jh.shutdown();
    pd.shutdown();
}

pub(super) fn workload_distnode_mem(n: usize, vsize: usize) {
    use vals_cluster::liveness::MemLivenessLog;
    use vals_cluster::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};
    use vals_cluster::{Ordering, DEFAULT_ZONE};

    let region_mib = env_u64("VALS_DISTNODE_REGION_MIB_MEM", 128);
    let geom = ZoneGeom {
        block_size: BLOCK_SIZE,
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        region_blocks: region_mib * 1024 * 1024 / BLOCK_SIZE as u64,
    };
    distnode_core("mem", n, vsize, move |id| {
        let provider = MemZoneStore::new(id, geom);
        let zone0 = provider
            .open(DEFAULT_ZONE, Ordering::Unsorted)
            .expect("open zone 0");
        (zone0, provider, MemLivenessLog::new())
    });
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distnode_spdk(pci_list: &str, n: usize, vsize: usize) {
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore, ZoneStore};
    use vals_cluster::{Ordering, DEFAULT_ZONE};
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    assert!(
        pcis.len() >= 4,
        "distnode needs >= 4 comma-separated PCIe addrs (3 initial nodes + 1 joiner)"
    );
    if pcis.len() > 4 {
        println!("  (note: using first 4 of {} drives; extras ignored)", pcis.len());
    }
    let region_mib = env_u64("VALS_DISTNODE_REGION_MIB", 2048);
    let region_blocks = region_mib * 1024 * 1024 / 4096;
    let zcfg = SpdkZoneConfig {
        super_blocks: 16,
        region_blocks,
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
    };
    println!("  region      : {region_mib} MiB/zone (VALS_DISTNODE_REGION_MIB)");
    distnode_core("spdk", n, vsize, move |id| {
        let pci = pcis[(id - 1) as usize].clone();
        let ctrlr = SpdkController::attach(&SpdkConfig {
            pci_addr: pci,
            nsid: 1,
        })
        .expect("attach spdk controller");
        let provider = SpdkZoneStore::open(id, ctrlr, zcfg, true).expect("open spdk zone store");
        let zone0 = provider
            .open(DEFAULT_ZONE, Ordering::Unsorted)
            .expect("open zone 0");
        let live = provider.liveness_log();
        (zone0, provider, live)
    });
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distresil(pci_list: &str, vsize: usize) {
    use vals_cluster::reactor;
    use vals_cluster::{
        CoordError, Coordinator, WriteConcern, FdConfig, NetTransport, Ring, ShardedServer,
        VersionedStore,
    };
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const RF: usize = 3;
    const R: usize = 2;
    let nodes = pcis.len() as u32;
    assert!(
        nodes as usize >= RF + 2,
        "distresil needs >= 5 PCIe addrs (RF=3 + at least 2 fallback nodes)"
    );

    let cfg = bench_config(Ordering::Unsorted);
    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut handles = Vec::new();
    for node in 1..=nodes {
        let dev = SpdkBlockDevice::open(&SpdkConfig {
            pci_addr: pcis[node as usize - 1].clone(),
            nsid: 1,
        })
        .expect("open spdk drive");
        let store = VersionedStore::new(Engine::create(dev, cfg).unwrap(), node).unwrap();
        let (addrs, handle) = ShardedServer::spawn_with_handle(vec![store]).expect("spawn node");
        handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    let cooldown_ms = 400u64;
    let coord = Coordinator::new(
        0,
        ring,
        R,
        NetTransport::connect_sharded_unbounded(node_shards),
    )
        .with_failure_detector(FdConfig {
            failure_threshold: 3,
            open_cooldown_ms: cooldown_ms,
        });
    println!(
        "--- distresil: {nodes} nodes x 1 SPDK drive, RF={RF}, R={R}, failure detector on ---"
    );

    let key = |i: usize| format!("rk{i:09}").into_bytes();
    let val = |i: usize| make_value(vsize, i as u64);
    let w = |k: &[u8], v: &[u8], tier| {
        vals_rt::block_on(coord.write_with(k, v, tier), reactor::poll_io)
    };

    for i in 0..nodes as usize * 3 {
        let _ = w(&key(i), &val(0), WriteConcern::Quorum(2));
    }

    let victim = 1u32;
    coord.transport().set_down(victim, true);
    assert!(!coord.is_node_down(victim), "starts healthy");
    let mut trip_after = None;
    for i in 1000..1200 {
        let _ = w(&key(i), &val(i), WriteConcern::Quorum(2));
        if coord.is_node_down(victim) {
            trip_after = Some(i - 1000 + 1);
            break;
        }
    }
    match trip_after {
        Some(k) => println!(
            "  phase1 detect    : breaker tripped on node{victim} after {k} writes; down_nodes={:?}",
            coord.down_nodes()
        ),
        None => println!("  phase1 detect    : CHECK — breaker did not trip in 200 writes"),
    }

    coord.transport().set_down(victim, false);
    std::thread::sleep(std::time::Duration::from_millis(cooldown_ms + 150));
    let mut recovered_after = None;
    for i in 2000..2200 {
        let _ = w(&key(i), &val(i), WriteConcern::Quorum(2));
        if !coord.is_node_down(victim) {
            recovered_after = Some(i - 2000 + 1);
            break;
        }
    }
    match recovered_after {
        Some(k) => println!(
            "  phase1 recover   : node{victim} probed healthy + breaker closed after {k} writes"
        ),
        None => println!("  phase1 recover   : CHECK — breaker stayed open"),
    }
    let fd_ok = trip_after.is_some() && recovered_after.is_some();

    let sk = key(424242);
    let sv = val(424242);
    let home = coord.ring().replicas(&sk);
    let fallbacks: Vec<u32> = coord
        .ring()
        .replicas_n(&sk, nodes as usize)
        .into_iter()
        .filter(|n| !home.contains(n))
        .collect();
    coord.transport().set_down(home[0], true);
    coord.transport().set_down(home[1], true);

    let strict = vals_rt::block_on(
        coord.write_with(&sk, &sv, WriteConcern::All),
        reactor::poll_io,
    );
    let strict_failed = matches!(strict, Err(CoordError::QuorumFailed { .. }));

    let sloppy = vals_rt::block_on(
        coord.write_sloppy(&sk, &sv, WriteConcern::All),
        reactor::poll_io,
    );
    let sloppy_ok = sloppy.is_ok();
    let hints_held = coord.pending_hints(home[0]) > 0 && coord.pending_hints(home[1]) > 0;
    println!(
        "  phase2 home={home:?} fallbacks={fallbacks:?}: strict Replicated {} / sloppy {} / hints-held={hints_held}",
        if strict_failed { "FAILED-quorum (expected)" } else { "CHECK-unexpected-ok" },
        if sloppy_ok { "OK" } else { "CHECK-failed" },
    );

    coord.transport().set_down(home[0], false);
    coord.transport().set_down(home[1], false);
    std::thread::sleep(std::time::Duration::from_millis(cooldown_ms + 150));
    let d0 = vals_rt::block_on(coord.replay_hints(home[0]), reactor::poll_io);
    let d1 = vals_rt::block_on(coord.replay_hints(home[1]), reactor::poll_io);
    let read_back = vals_rt::block_on(coord.read(&sk), reactor::poll_io)
        .ok()
        .flatten()
        .and_then(|s| s.value);
    let durable = read_back.as_deref() == Some(sv.as_slice());
    println!(
        "  phase2 migrate   : replayed hints home (node{}:{d0}, node{}:{d1}); strict read-back after drain: {}",
        home[0],
        home[1],
        if durable { "OK" } else { "CHECK" }
    );

    let all_ok = fd_ok && strict_failed && sloppy_ok && hints_held && durable;
    println!(
        "  FLOWS            : {}",
        if all_ok { "OK" } else { "CHECK" }
    );

    drop(coord);
    for h in handles {
        h.shutdown();
    }
}

