use super::*;

#[cfg(feature = "spdk")]
pub(super) fn workload_distspdk(pci_list: &str, n: usize, vsize: usize) {
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, Ring, ShardedServer, VersionedStore,
    };
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const NODES: u32 = 3;
    const RF: usize = 3;
    const R: usize = 2;
    let dpn: usize = if pcis.len() >= NODES as usize * 2 {
        2
    } else {
        1
    };
    assert!(
        pcis.len() >= NODES as usize * dpn,
        "distspdk needs >= {} comma-separated PCIe addrs (3 nodes x {dpn} drives)",
        NODES as usize * dpn
    );

    let cfg = bench_config(Ordering::Unsorted);
    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut handles = Vec::new();
    for node in 1..=NODES {
        let base = (node as usize - 1) * dpn;
        let stores: Vec<VersionedStore<SpdkBlockDevice>> = (0..dpn)
            .map(|j| {
                let dev = SpdkBlockDevice::open(&SpdkConfig {
                    pci_addr: pcis[base + j].clone(),
                    nsid: 1,
                })
                .expect("open spdk drive");
                VersionedStore::new(Engine::create(dev, cfg).unwrap(), node).unwrap()
            })
            .collect();
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn node shards");
        handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    let coord = Coordinator::new(0, ring, R, NetTransport::connect_sharded_unbounded(node_shards));
    println!("--- distspdk: {NODES} nodes x {dpn} SPDK drives, RF={RF}, R={R}, loopback TCP ---");

    let key = |i: usize| format!("k{i:09}").into_bytes();
    let val = |i: usize| make_value(vsize, i as u64);

    for i in 0..NODES as usize * dpn * 2 {
        let _ = vals_rt::block_on(
            coord.write_with(&key(i), &val(0), WriteConcern::Quorum(2)),
            reactor::poll_io,
        );
    }

    let w0 = Instant::now();
    for i in 0..n {
        vals_rt::block_on(
            coord.write_with(&key(i), &val(i), WriteConcern::Quorum(2)),
            reactor::poll_io,
        )
        .expect("write");
    }
    let wsecs = w0.elapsed().as_secs_f64();
    let r0 = Instant::now();
    let mut mismatches = 0usize;
    for i in 0..n {
        let got = vals_rt::block_on(coord.read(&key(i)), reactor::poll_io).expect("read");
        if got.and_then(|s| s.value) != Some(val(i)) {
            mismatches += 1;
        }
    }
    let rsecs = r0.elapsed().as_secs_f64();
    println!("  phase1 write Quorum(2): {:.0} ops/s", n as f64 / wsecs);
    println!("  phase1 read R=2       : {:.0} ops/s", n as f64 / rsecs);
    println!(
        "  phase1 correctness    : {} ({mismatches} mismatches of {n})",
        if mismatches == 0 { "OK" } else { "FAIL" }
    );

    let down = 1u32;
    coord.transport().set_down(down, true);
    let m = (n / 4).max(2000);
    let mut down_mismatch = 0usize;
    for i in n..n + m {
        vals_rt::block_on(
            coord.write_with(&key(i), &val(i), WriteConcern::Quorum(2)),
            reactor::poll_io,
        )
        .expect("write with a node down");
    }
    for i in n..n + m {
        let got = vals_rt::block_on(coord.read(&key(i)), reactor::poll_io)
            .expect("read with a node down");
        if got.and_then(|s| s.value) != Some(val(i)) {
            down_mismatch += 1;
        }
    }
    let hints = coord.pending_hints(down);
    println!(
        "  phase2 node{down}-down     : wrote+read {m} keys at Quorum; {down_mismatch} read mismatches; {hints} hints captured"
    );

    coord.transport().set_down(down, false);
    let drained = vals_rt::block_on(coord.replay_hints(down), reactor::poll_io);
    println!("  phase3 node{down}-up       : replayed {drained} of {hints} retained hints (store is bounded)");
    let healed = mismatches == 0 && down_mismatch == 0 && hints > 0 && drained == hints;
    println!(
        "  FLOWS                 : {}",
        if healed { "OK" } else { "CHECK" }
    );

    drop(coord);
    for h in handles {
        h.shutdown();
    }
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distmd(pci_list: &str, n: usize, vsize: usize, partitions: usize) {
    use std::net::SocketAddr;
    use std::time::Duration;
    use vals_cluster::drive::{self, PartitionGeom, SpdkDriveSet};
    use vals_cluster::reactor;
    use vals_cluster::{Coordinator, NetTransport, Node, Ring};

    let settings = node_settings();
    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let drives = pcis.len();
    assert!(drives >= 1, "distmd needs >= 1 PCIe addr");
    let p = partitions.max(drives).max(1);
    const REGION_BLOCKS: u64 = 1 << 17;

    let ds = SpdkDriveSet::attach(&pcis).expect("attach drives");
    let caps = drive::drive_caps(&ds);
    let geom = PartitionGeom {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering: vals_cluster::Ordering::Unsorted,
    };

    let layout = drive::plan(&caps, REGION_BLOCKS, p, p).expect("plan layout");
    println!("--- distmd: 1 node, {p} partitions across {drives} SPDK drives, RF=1 ---");
    for d in 0..drives {
        println!(
            "  drive {d} ({:>12}) : {} partitions ({} blocks)",
            pcis[d],
            layout.partitions_on(d),
            caps[d]
        );
    }

    let listeners = Node::bind_n(p).expect("bind partition listeners");
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
    let shard_listeners: Vec<_> = listeners.into_iter().map(|(l, _)| l).collect();
    let mut ring = Ring::with_vnodes(1, 128);
    ring.set_members_weighted([(1, 1)]);

    let (weight, node) = Node::serve_multidrive_with_settings(
        1,
        shard_listeners,
        ds,
        REGION_BLOCKS,
        geom,
        ring.clone(),
        vec![(1u32, addrs.clone())],
        Duration::from_secs(3600),
        true,
        settings.clone(),
    )
    .expect("serve_multidrive");
    println!("  node ring weight       : {weight} (= drive count)");

    let coord = Coordinator::new(
        0,
        ring,
        1,
        NetTransport::connect_sharded_unbounded([(1u32, addrs)]),
    );
    let key = |i: usize| format!("k{i:09}").into_bytes();
    let val = |i: usize| make_value(vsize, i as u64);

    for i in 0..p * 2 {
        let _ = vals_rt::block_on(coord.write(&key(i), &val(0)), reactor::poll_io);
    }

    let w0 = Instant::now();
    for i in 0..n {
        vals_rt::block_on(coord.write(&key(i), &val(i)), reactor::poll_io).expect("write");
    }
    let wsecs = w0.elapsed().as_secs_f64();

    let r0 = Instant::now();
    let mut mismatches = 0usize;
    for i in 0..n {
        let got = vals_rt::block_on(coord.read(&key(i)), reactor::poll_io).expect("read");
        if got.and_then(|s| s.value) != Some(val(i)) {
            mismatches += 1;
        }
    }
    let rsecs = r0.elapsed().as_secs_f64();
    println!("  write                  : {:.0} ops/s", n as f64 / wsecs);
    println!("  read                   : {:.0} ops/s", n as f64 / rsecs);
    println!(
        "  correctness            : {} ({mismatches} mismatches of {n})",
        if mismatches == 0 { "OK" } else { "FAIL" }
    );
    println!(
        "  FLOWS                  : {}",
        if mismatches == 0 { "OK" } else { "CHECK" }
    );

    drop(coord);
    node.shutdown();
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distmdmove(pci_list: &str, n: usize, vsize: usize, partitions: usize) {
    use vals_cluster::drive::{self, PartitionGeom, SpdkDriveSet};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let drives = pcis.len();
    assert!(
        drives >= 2,
        "distmdmove needs >= 2 drives (the last one starts absent)"
    );
    let p = partitions.max(drives).max(1);
    const REGION_BLOCKS: u64 = 1 << 17;

    let ds = SpdkDriveSet::attach(&pcis).expect("attach drives");
    let full = drive::drive_caps(&ds);
    let geom = PartitionGeom {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering: vals_cluster::Ordering::Unsorted,
    };

    let mut before_caps = full.clone();
    *before_caps.last_mut().unwrap() = 0;
    let before = drive::plan(&before_caps, REGION_BLOCKS, p, p).expect("plan before");
    println!(
        "--- distmdmove: {p} partitions, adding drive {} to a {}-drive node ---",
        drives - 1,
        drives - 1
    );
    for d in 0..drives {
        println!(
            "  before : drive {d} -> {} partitions",
            before.partitions_on(d)
        );
    }

    let mut stores = drive::build_stores(&ds, &before, geom, 1, true).expect("build before");
    let val = make_value(vsize, 0);
    for i in 0..n {
        stores[i % p]
            .put(format!("k{i:09}").as_bytes(), &val)
            .unwrap();
    }
    for s in stores.iter_mut() {
        s.sync().unwrap();
    }
    drop(stores);

    let after = drive::replan(&before, &full, p).expect("replan after");
    for d in 0..drives {
        println!(
            "  after  : drive {d} -> {} partitions",
            after.partitions_on(d)
        );
    }
    let t = Instant::now();
    let moved = drive::relocate(&ds, &before, &after, geom, 1).expect("relocate");
    println!(
        "  relocated              : {moved} of {p} partitions in {:.3}s (new drive now holds {})",
        t.elapsed().as_secs_f64(),
        after.partitions_on(drives - 1)
    );

    let stores = drive::build_stores(&ds, &after, geom, 1, false).expect("build after");
    let mut mismatches = 0usize;
    for i in 0..n {
        let got = stores[i % p].get(format!("k{i:09}").as_bytes()).unwrap();
        if got.as_deref() != Some(val.as_slice()) {
            mismatches += 1;
        }
    }
    println!(
        "  correctness            : {} ({mismatches} mismatches of {n})",
        if mismatches == 0 { "OK" } else { "FAIL" }
    );
    let ok = mismatches == 0 && moved > 0 && after.partitions_on(drives - 1) > 0;
    println!(
        "  FLOWS                  : {}",
        if ok { "OK" } else { "CHECK" }
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distmdfail(pci_list: &str, n: usize, vsize: usize, partitions: usize) {
    use std::cell::RefCell;
    use std::sync::Arc;
    use vals_cluster::antientropy::{sweep, LocalPeer, DEFAULT_BUCKETS};
    use vals_cluster::drive::{self, build_stores, build_stores_with, PartitionGeom, SpdkDriveSet};
    use vals_cluster::reactor;
    use vals_cluster::ring::{hash64, Ring};
    use vals_cluster::{Version, VersionedStore};
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig, SpdkController};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const RF: usize = 2;
    const DPN: usize = 2;
    const NODES: usize = 3;
    const REGION: u64 = 1 << 17;
    let p = partitions.max(2);
    assert!(
        pcis.len() >= NODES * DPN + 1,
        "distmdfail needs >= 7 PCIe addrs (3 nodes x 2 drives + 1 spare)"
    );

    let ctrlrs: Vec<Arc<SpdkController>> = pcis
        .iter()
        .map(|a| {
            SpdkController::attach(&SpdkConfig {
                pci_addr: a.clone(),
                nsid: 1,
            })
            .expect("attach controller")
        })
        .collect();

    let geom = PartitionGeom {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering: vals_cluster::Ordering::Unsorted,
    };
    let node_ds = |n: usize| {
        SpdkDriveSet::from_controllers(vec![ctrlrs[n * DPN].clone(), ctrlrs[n * DPN + 1].clone()])
    };
    let caps = drive::drive_caps(&node_ds(0));
    let layout = drive::plan(&caps, REGION, p, p).expect("plan");

    type Stores = Vec<RefCell<VersionedStore<SpdkBlockDevice>>>;
    let wrap = |v: Vec<VersionedStore<SpdkBlockDevice>>| -> Stores {
        v.into_iter().map(RefCell::new).collect()
    };
    let mut node: Vec<Stores> = (0..NODES)
        .map(|nidx| {
            wrap(build_stores(&node_ds(nidx), &layout, geom, (nidx + 1) as u32, true).unwrap())
        })
        .collect();

    let mut ring = Ring::with_vnodes(RF, 128);
    ring.set_members_weighted([(1, 1), (2, 1), (3, 1)]);
    let part_of = |k: &[u8]| (hash64(k) % p as u64) as usize;
    let val = make_value(vsize, 0);
    println!("--- distmdfail: 3 nodes x 2 drives, RF=2, {p} partitions/node, +1 spare ---");
    for d in 0..DPN {
        println!("  node-drive {d} : {} partitions", layout.partitions_on(d));
    }

    let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i:09}").into_bytes()).collect();
    for (i, k) in keys.iter().enumerate() {
        let ver = Version {
            hlc: 100 + i as u64,
            node: 9,
        };
        for r in ring.replicas(k) {
            node[(r - 1) as usize][part_of(k)]
                .borrow_mut()
                .apply(k, Some(&val), ver)
                .unwrap();
        }
    }
    for nd in &node {
        for s in nd {
            s.borrow_mut().sync().unwrap();
        }
    }

    let victim = 2usize;
    let vi = victim - 1;
    let failed = 1usize;
    let lost: Vec<usize> = layout
        .partitions()
        .iter()
        .filter(|pt| pt.drive == failed)
        .map(|pt| pt.partition)
        .collect();
    println!(
        "  node 2 drive {failed} fails ({} partitions lost); swapping spare {}",
        lost.len(),
        pcis[NODES * DPN]
    );

    node[vi].clear();
    let replaced_ds =
        SpdkDriveSet::from_controllers(vec![ctrlrs[vi * DPN].clone(), ctrlrs[NODES * DPN].clone()]);
    let t = Instant::now();
    node[vi] = wrap(
        build_stores_with(&replaced_ds, &layout, geom, victim as u32, |pid| {
            lost.contains(&pid)
        })
        .unwrap(),
    );

    let owns = |k: &[u8]| ring.replicas(k).contains(&(victim as u32));
    let mut pre_heal_missing = 0usize;
    for k in &keys {
        if owns(k) {
            let here = node[vi][part_of(k)].borrow().get(k).unwrap().is_some();
            if !here {
                pre_heal_missing += 1;
            }
        }
    }
    println!(
        "  post-swap (pre-heal)   : node 2 missing {pre_heal_missing} owned keys (the failed drive's partitions)"
    );

    let mut applied = 0usize;
    for s in 0..p {
        let p1 = node[0][s].borrow();
        let p3 = node[2][s].borrow();
        let peers = [(1u32, LocalPeer(&*p1)), (3u32, LocalPeer(&*p3))];
        applied += vals_rt::block_on(
            sweep(&node[vi][s], victim as u32, &ring, DEFAULT_BUCKETS, &peers),
            reactor::poll_io,
        )
        .unwrap();
    }
    println!(
        "  heal                   : pulled {applied} entries in {:.3}s",
        t.elapsed().as_secs_f64()
    );

    let mut mismatches = 0usize;
    for k in &keys {
        let here = node[vi][part_of(k)].borrow().get(k).unwrap().as_deref() == Some(val.as_slice());
        if here != owns(k) {
            mismatches += 1;
        }
    }
    println!(
        "  correctness            : {} ({mismatches} mismatches of {n})",
        if mismatches == 0 { "OK" } else { "FAIL" }
    );
    let ok = mismatches == 0 && pre_heal_missing > 0 && applied > 0;
    println!(
        "  FLOWS                  : {}",
        if ok { "OK" } else { "CHECK" }
    );
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distspdkmc(pci_list: &str, n: usize, vsize: usize, ncoord: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, Ring, ShardedServer, VersionedStore,
    };
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const NODES: u32 = 3;
    const DPN: usize = 2;
    const RF: usize = 3;
    const R: usize = 2;
    const READ_CONC: usize = 64;
    assert!(
        pcis.len() >= NODES as usize * DPN,
        "distspdkmc needs >= 6 PCIe addrs"
    );
    let ncoord = ncoord.max(1);
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let base = (node as usize - 1) * DPN;
        let stores: Vec<VersionedStore<SpdkBlockDevice>> = (0..DPN)
            .map(|j| {
                let dev = SpdkBlockDevice::open(&SpdkConfig {
                    pci_addr: pcis[base + j].clone(),
                    nsid: 1,
                })
                .expect("open spdk drive");
                VersionedStore::new(Engine::create(dev, cfg).unwrap(), node).unwrap()
            })
            .collect();
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn node shards");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    println!(
        "--- distspdkmc: {NODES} nodes x {DPN} SPDK drives, RF={RF}, R={R}, {ncoord} coordinators ---"
    );

    let per = n / ncoord;
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Coordinator::new(
                1000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            );
            let val = make_value(vsize, c as u64);
            let key = |i: usize| format!("c{c}-k{i:09}").into_bytes();
            for i in 0..16 {
                let _ = vals_rt::block_on(
                    coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
                    reactor::poll_io,
                );
            }
            barrier.wait();

            let _ = &key;
            let coord = Rc::new(coord);

            let next = Rc::new(Cell::new(0usize));
            let werr = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let w0 = Instant::now();
            for _ in 0..READ_CONC {
                let coord = coord.clone();
                let next = next.clone();
                let werr = werr.clone();
                let val = val.clone();
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= per {
                            break;
                        }
                        next.set(i + 1);
                        if coord
                            .write_with(
                                &format!("c{c}-k{i:09}").into_bytes(),
                                &val,
                                WriteConcern::Quorum(2),
                            )
                            .await
                            .is_err()
                        {
                            werr.set(werr.get() + 1);
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            let wsecs = w0.elapsed().as_secs_f64();
            if c == 0 {
                println!("  [coord0] write errors: {}/{per}", werr.get());
            }

            let next = Rc::new(Cell::new(0usize));
            let rerr = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let r0 = Instant::now();
            for _ in 0..READ_CONC {
                let coord = coord.clone();
                let next = next.clone();
                let rerr = rerr.clone();
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= per {
                            break;
                        }
                        next.set(i + 1);
                        if coord
                            .read(&format!("c{c}-k{i:09}").into_bytes())
                            .await
                            .is_err()
                        {
                            rerr.set(rerr.get() + 1);
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            if c == 0 {
                println!("  [coord0] read errors: {}/{per}", rerr.get());
            }
            let rsecs = r0.elapsed().as_secs_f64();

            let next = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let a0 = Instant::now();
            for _ in 0..READ_CONC {
                let coord = coord.clone();
                let next = next.clone();
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= per {
                            break;
                        }
                        next.set(i + 1);
                        let _ = coord.read_any(&format!("c{c}-k{i:09}").into_bytes()).await;
                    }
                });
            }
            ex.run(reactor::poll_io);
            (wsecs, rsecs, a0.elapsed().as_secs_f64())
        }));
    }
    let (mut maxw, mut maxr, mut maxa) = (0.0f64, 0.0f64, 0.0f64);
    for h in handles {
        let (w, r, a) = h.join().unwrap();
        maxw = maxw.max(w);
        maxr = maxr.max(r);
        maxa = maxa.max(a);
    }
    let total = per * ncoord;
    println!(
        "  write Quorum(2)            : {:.0} ops/s",
        total as f64 / maxw
    );
    println!(
        "  read R=2 strong (conc {READ_CONC})  : {:.0} ops/s",
        total as f64 / maxr
    );
    println!(
        "  read get-any    (conc {READ_CONC})  : {:.0} ops/s",
        total as f64 / maxa
    );
    let connects = vals_cluster::net::CONNECTS.load(std::sync::atomic::Ordering::Relaxed);
    println!(
        "  conns opened: {connects} ({:.3}/op)",
        connects as f64 / (total * 3) as f64
    );

    for h in server_handles {
        h.shutdown();
    }
}

#[cfg(feature = "spdk")]
#[allow(clippy::type_complexity)]
fn distmdmc_thread<T: vals_cluster::coord::Transport + 'static>(
    coord: vals_cluster::Coordinator<T>,
    c: usize,
    per: usize,
    vsize: usize,
    read_conc: usize,
    barrier: &std::sync::Barrier,
) -> (f64, f64, f64, usize, usize, usize, Vec<u64>) {
    use std::cell::Cell;
    use std::rc::Rc;
    use vals_cluster::reactor;
    use vals_cluster::WriteConcern;
    use vals_rt::LocalExecutor;

    let hedge = coord.hedge_clock().zip(coord.hedge_tick_period());
    let val = make_value(vsize, c as u64);
    for i in 0..16 {
        let _ = vals_rt::block_on(
            coord.write_with(
                &format!("c{c}-k{i:09}").into_bytes(),
                &val,
                WriteConcern::Quorum(2),
            ),
            reactor::poll_io,
        );
    }
    barrier.wait();
    let coord = Rc::new(coord);

    let next = Rc::new(Cell::new(0usize));
    let werr = Rc::new(Cell::new(0usize));
    let ex = LocalExecutor::new();
    let w0 = Instant::now();
    for _ in 0..read_conc {
        let coord = coord.clone();
        let next = next.clone();
        let werr = werr.clone();
        let val = val.clone();
        ex.spawn(async move {
            loop {
                let i = next.get();
                if i >= per {
                    break;
                }
                next.set(i + 1);
                if coord
                    .write_with(
                        &format!("c{c}-k{i:09}").into_bytes(),
                        &val,
                        WriteConcern::Quorum(2),
                    )
                    .await
                    .is_err()
                {
                    werr.set(werr.get() + 1);
                }
            }
        });
    }
    ex.run(reactor::poll_io);
    let wsecs = w0.elapsed().as_secs_f64();
    if c == 0 {
        println!("  [coord0] write errors: {}/{per}", werr.get());
    }

    let next = Rc::new(Cell::new(0usize));
    let rerr = Rc::new(Cell::new(0usize));
    let rlat: Rc<std::cell::RefCell<Vec<u64>>> =
        Rc::new(std::cell::RefCell::new(Vec::with_capacity(per)));
    let ex = LocalExecutor::new();
    let rstop = Rc::new(reactor::Notify::new());
    let ractive = Rc::new(Cell::new(read_conc));
    if let Some((clock, period)) = hedge.clone() {
        let rstop = rstop.clone();
        ex.spawn(async move {
            let timer = match reactor::Timer::every(period) {
                Ok(t) => t,
                Err(_) => return,
            };
            loop {
                match vals_rt::race(timer.tick(), rstop.notified()).await {
                    vals_rt::Either::Left(()) => clock.advance(),
                    vals_rt::Either::Right(()) => return,
                }
            }
        });
    }
    let has_ticker = hedge.is_some();
    let r0 = Instant::now();
    for _ in 0..read_conc {
        let coord = coord.clone();
        let next = next.clone();
        let rerr = rerr.clone();
        let rlat = rlat.clone();
        let val = val.clone();
        let rstop = rstop.clone();
        let ractive = ractive.clone();
        ex.spawn(async move {
            loop {
                let i = next.get();
                if i >= per {
                    break;
                }
                next.set(i + 1);
                let t = Instant::now();
                let res = coord.read(&format!("c{c}-k{i:09}").into_bytes()).await;
                rlat.borrow_mut().push(t.elapsed().as_nanos() as u64);
                let ok = matches!(
                    res,
                    Ok(Some(ref s)) if s.value.as_deref() == Some(val.as_slice())
                );
                if !ok {
                    rerr.set(rerr.get() + 1);
                }
            }
            let rem = ractive.get() - 1;
            ractive.set(rem);
            if rem == 0 && has_ticker {
                rstop.signal();
            }
        });
    }
    ex.run(reactor::poll_io);
    if c == 0 {
        println!("  [coord0] read errors: {}/{per}", rerr.get());
    }
    let rsecs = r0.elapsed().as_secs_f64();
    let rlat = Rc::try_unwrap(rlat)
        .expect("rlat uniquely held")
        .into_inner();

    let next = Rc::new(Cell::new(0usize));
    let aerr = Rc::new(Cell::new(0usize));
    let ex = LocalExecutor::new();
    let a0 = Instant::now();
    for _ in 0..read_conc {
        let coord = coord.clone();
        let next = next.clone();
        let aerr = aerr.clone();
        ex.spawn(async move {
            loop {
                let i = next.get();
                if i >= per {
                    break;
                }
                next.set(i + 1);
                if coord
                    .read_any(&format!("c{c}-k{i:09}").into_bytes())
                    .await
                    .is_err()
                {
                    aerr.set(aerr.get() + 1);
                }
            }
        });
    }
    ex.run(reactor::poll_io);
    (
        wsecs,
        rsecs,
        a0.elapsed().as_secs_f64(),
        werr.get(),
        rerr.get(),
        aerr.get(),
        rlat,
    )
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distmdmc(pci_list: &str, n: usize, vsize: usize, ncoord: usize, parts_per_node: usize) {
    use std::sync::{Arc, Barrier};
    use vals_cluster::drive::{self, PartitionGeom, SpdkDriveSet};
    use vals_cluster::{Coordinator, NetTransport, Ring, ShardedServer};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const RF: usize = 3;
    const R: usize = 2;
    const READ_CONC: usize = 64;
    let dpn: usize = if pcis.len() >= RF * 2 && pcis.len().is_multiple_of(2) {
        2
    } else {
        1
    };
    assert!(
        pcis.len() >= RF * dpn && pcis.len().is_multiple_of(dpn),
        "distmdmc needs a multiple of {dpn} drives, >= {} (RF nodes)",
        RF * dpn
    );
    let nodes = (pcis.len() / dpn) as u32;
    let p = parts_per_node.max(1);
    let ncoord = ncoord.max(1);

    let keys_per_part = (n as u64 * RF as u64).div_ceil(nodes as u64 * p as u64);
    let bytes_per_part = keys_per_part.saturating_mul(vsize as u64 + 64);
    let region_blocks =
        ((bytes_per_part.saturating_mul(2) / 4096).max(1 << 17)).next_multiple_of(SEG_BLOCKS);
    let geom = PartitionGeom {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering: vals_cluster::Ordering::Unsorted,
    };

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut keep: Vec<SpdkDriveSet> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=nodes {
        let base = (node as usize - 1) * dpn;
        let drives: Vec<String> = pcis[base..base + dpn].to_vec();
        let ds = SpdkDriveSet::attach(&drives).expect("attach node drives");
        let caps = drive::drive_caps(&ds);
        let layout = drive::plan(&caps, region_blocks, p, p).expect("plan node layout");
        let stores = drive::build_stores(&ds, &layout, geom, node, true).expect("build stores");
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn node shards");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
        keep.push(ds);
    }
    let fanout_mode = std::env::var("VALS_READ_FANOUT").unwrap_or_else(|_| "digest".into());
    let hedge_delay = hedge_delay_env();
    let slow_cfg = slow_node_env();
    let slow_str = match slow_cfg {
        Some((node, dur, mode)) => format!(
            "node={node} delay={} µs mode={}",
            dur.as_micros(),
            match mode {
                SlowMode::Digest => "digest",
                SlowMode::All => "all",
            }
        ),
        None => "none".to_string(),
    };
    println!(
        "--- distmdmc: {nodes} nodes x {dpn} drives x {p} partitions \
         (= {} server cores), RF={RF}, R={R}, {ncoord} coordinators, read-fanout={fanout_mode}, \
         hedge={}, slow-replica={slow_str}, region={} MiB ---",
        nodes as usize * p,
        match hedge_delay {
            Some(d) => format!("{} µs", d.as_micros()),
            None => "off".to_string(),
        },
        region_blocks * 4096 / (1 << 20)
    );
    let hedges0 = vals_cluster::observe::counters().digest_hedges_fired.get();

    let per = n / ncoord;
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let fanout = match std::env::var("VALS_READ_FANOUT").as_deref() {
                Ok("all") => vals_cluster::ReadFanout::All,
                Ok("quorum") => vals_cluster::ReadFanout::Quorum,
                _ => vals_cluster::ReadFanout::Digest,
            };
            let inner = NetTransport::connect_sharded_unbounded(node_shards);
            match slow_cfg {
                Some((sn, sd, sm)) => {
                    let coord = Coordinator::new(
                        3000 + c as u32,
                        ring,
                        R,
                        SlowNode::new(inner, sn, sd, sm),
                    )
                    .with_read_fanout(fanout);
                    let coord = match hedge_delay {
                        Some(d) => coord.with_digest_hedge(d),
                        None => coord,
                    };
                    distmdmc_thread(coord, c, per, vsize, READ_CONC, &barrier)
                }
                None => {
                    let coord =
                        Coordinator::new(3000 + c as u32, ring, R, inner).with_read_fanout(fanout);
                    let coord = match hedge_delay {
                        Some(d) => coord.with_digest_hedge(d),
                        None => coord,
                    };
                    distmdmc_thread(coord, c, per, vsize, READ_CONC, &barrier)
                }
            }
        }));
    }
    let (mut maxw, mut maxr, mut maxa) = (0.0f64, 0.0f64, 0.0f64);
    let (mut twe, mut tre, mut tae) = (0usize, 0usize, 0usize);
    let mut all_rlat: Vec<u64> = Vec::new();
    for h in handles {
        let (w, r, a, we, re, ae, rlat) = h.join().unwrap();
        maxw = maxw.max(w);
        maxr = maxr.max(r);
        maxa = maxa.max(a);
        twe += we;
        tre += re;
        tae += ae;
        all_rlat.extend(rlat);
    }
    let total = per * ncoord;
    println!(
        "  TOTAL errors: write={twe} read={tre} get-any={tae} (of {total} each){}",
        if twe + tre + tae == 0 {
            "  [clean]"
        } else {
            "  *** NON-ZERO — throughput INVALID ***"
        }
    );
    println!(
        "  write Quorum(2)            : {:.0} ops/s",
        total as f64 / maxw
    );
    println!(
        "  read R=2 strong (conc {READ_CONC})  : {:.0} ops/s",
        total as f64 / maxr
    );
    all_rlat.sort_unstable();
    let hedges = vals_cluster::observe::counters().digest_hedges_fired.get() - hedges0;
    println!(
        "  read R=2 strong latency: p50={} p99={} p99.9={} max={} ns; digest_hedges_fired={hedges}",
        percentile(&all_rlat, 50.0),
        percentile(&all_rlat, 99.0),
        percentile(&all_rlat, 99.9),
        all_rlat.last().copied().unwrap_or(0),
    );
    println!(
        "  read get-any    (conc {READ_CONC})  : {:.0} ops/s",
        total as f64 / maxa
    );

    for h in server_handles {
        h.shutdown();
    }
    drop(keep);
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distbatch(pci_list: &str, n: usize, vsize: usize, ncoord: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, NetTransport, Persistence, Ring, ShardedServer, VersionedStore, WriteConcern,
        WriteOptions,
    };
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkBlockDevice, SpdkConfig};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const NODES: u32 = 3;
    const DPN: usize = 2;
    const RF: usize = 3;
    const R: usize = 2;
    const CONC: usize = 32;
    let batch_sizes = [8usize, 32, 128];
    assert!(
        pcis.len() >= NODES as usize * DPN,
        "distbatch needs >= 6 PCIe addrs"
    );
    let ncoord = ncoord.max(1);
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let base = (node as usize - 1) * DPN;
        let stores: Vec<VersionedStore<SpdkBlockDevice>> = (0..DPN)
            .map(|j| {
                let dev = SpdkBlockDevice::open(&SpdkConfig {
                    pci_addr: pcis[base + j].clone(),
                    nsid: 1,
                })
                .expect("open spdk drive");
                VersionedStore::new(Engine::create(dev, cfg).unwrap(), node).unwrap()
            })
            .collect();
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn node shards");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    println!(
        "--- distbatch: {NODES} nodes x {DPN} SPDK drives, RF={RF}, {ncoord} coordinators, conc={CONC} ---"
    );

    let per = n / ncoord;
    let mut phases: Vec<(usize, Persistence)> = vec![
        (0, Persistence::Buffered),
        (0, Persistence::Device),
    ];
    for &b in &batch_sizes {
        phases.push((b, Persistence::Buffered));
        phases.push((b, Persistence::Device));
    }
    let nphases = phases.len();
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        let phases = phases.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Rc::new(Coordinator::new(
                2000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            ));
            let val = make_value(vsize, c as u64);

            let vkeys: Vec<Vec<u8>> = (0..4)
                .map(|j| format!("c{c}-verify-{j}").into_bytes())
                .collect();
            let vrefs: Vec<(&[u8], &[u8])> = vkeys
                .iter()
                .map(|k| (k.as_slice(), val.as_slice()))
                .collect();
            let res = vals_rt::block_on(
                coord.write_batch(
                    &vrefs,
                    WriteOptions::device_durable(WriteConcern::All),
                ),
                reactor::poll_io,
            )
            .unwrap();
            assert!(res.all_ok(), "verify batch durable at RF");
            for k in &vkeys {
                let got = vals_rt::block_on(coord.read(k), reactor::poll_io).unwrap();
                assert_eq!(got.and_then(|s| s.value).as_deref(), Some(val.as_slice()));
            }
            barrier.wait();

            let mut secs: Vec<f64> = Vec::with_capacity(nphases);
            let mut errs: Vec<usize> = Vec::with_capacity(nphases);
            for (ph, (b, persistence)) in phases.iter().enumerate() {
                let (b, persistence) = (*b, *persistence);
                let next = Rc::new(Cell::new(0usize));
                let err = Rc::new(Cell::new(0usize));
                let ex = LocalExecutor::new();
                let t0 = Instant::now();
                for _ in 0..CONC {
                    let (coord, next, err, val) =
                        (coord.clone(), next.clone(), err.clone(), val.clone());
                    ex.spawn(async move {
                        loop {
                            let i = next.get();
                            if b == 0 {
                                if i >= per {
                                    break;
                                }
                                next.set(i + 1);
                                if coord
                                    .write_with(
                                        &format!("c{c}-s-k{i:09}").into_bytes(),
                                        &val,
                                        WriteOptions { concern: WriteConcern::All, persistence },
                                    )
                                    .await
                                    .is_err()
                                {
                                    err.set(err.get() + 1);
                                }
                            } else {
                                let nbatches = per / b;
                                if i >= nbatches {
                                    break;
                                }
                                next.set(i + 1);
                                let keys: Vec<Vec<u8>> = (0..b)
                                    .map(|j| format!("c{c}-p{ph}-{i:07}-{j:03}").into_bytes())
                                    .collect();
                                let refs: Vec<(&[u8], &[u8])> = keys
                                    .iter()
                                    .map(|k| (k.as_slice(), val.as_slice()))
                                    .collect();
                                match coord
                                    .write_batch(
                                        &refs,
                                        WriteOptions { concern: WriteConcern::All, persistence },
                                    )
                                    .await
                                {
                                    Ok(r) if r.all_ok() => {}
                                    _ => err.set(err.get() + 1),
                                }
                            }
                        }
                    });
                }
                ex.run(reactor::poll_io);
                secs.push(t0.elapsed().as_secs_f64());
                errs.push(err.get());
                barrier.wait();
            }
            (secs, errs)
        }));
    }

    let mut phase_secs = vec![0.0f64; nphases];
    let mut phase_errs = vec![0usize; nphases];
    for h in handles {
        let (secs, errs) = h.join().unwrap();
        for (p, s) in secs.iter().enumerate() {
            phase_secs[p] = phase_secs[p].max(*s);
        }
        for (p, e) in errs.iter().enumerate() {
            phase_errs[p] += e;
        }
    }

    let persistence_name = |p: Persistence| match p {
        Persistence::Buffered => "buffered",
        Persistence::Device => "device  ",
    };
    for (p, (b, persistence)) in phases.iter().enumerate() {
        let (writes, batches) = if *b == 0 {
            (per * ncoord, 0)
        } else {
            ((per / b) * b * ncoord, (per / b) * ncoord)
        };
        let label = if *b == 0 {
            "single write ".to_string()
        } else {
            format!("batch={b:<4}    ")
        };
        let bps = if batches > 0 {
            format!(", {:.0} batches/s", batches as f64 / phase_secs[p])
        } else {
            String::new()
        };
        println!(
            "  {label} RF/{:8} : {:>9.0} writes/s{}   (errors {})",
            persistence_name(*persistence),
            writes as f64 / phase_secs[p],
            bps,
            phase_errs[p]
        );
    }

    for h in server_handles {
        h.shutdown();
    }
}

#[cfg(feature = "spdk")]
pub(super) fn workload_distzone(pci_list: &str, n: usize, vsize: usize) {
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::rc::Rc;
    use vals_cluster::reactor;
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore, ZoneStore};
    use vals_cluster::{
        Catalog, Coordinator, WriteConcern, NetTransport, ReplicaServer, Ring, ZoneConfig,
    };
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    const NODES: u32 = 3;
    const CONC: usize = 64;
    const REGION_BLOCKS: u64 = (256 * 1024 * 1024) / BLOCK_SIZE as u64;
    const SUPER_BLOCKS: u64 = 16;
    let dpn: usize = if pcis.len() >= NODES as usize * 2 {
        2
    } else {
        1
    };
    assert!(
        pcis.len() >= NODES as usize * dpn,
        "distzone needs >= {} PCIe addrs",
        NODES as usize * dpn
    );

    let scope_c: BTreeSet<u32> = [1].into_iter().collect();
    let zones: [(u32, &str, usize, Option<BTreeSet<u32>>); 3] = [
        (1, "a", 3, None),
        (2, "b", 2, None),
        (3, "c", 1, Some(scope_c)),
    ];
    let mut catalog = Catalog::new();
    for (id, name, rf, scope) in &zones {
        catalog.insert(
            *id,
            ZoneConfig {
                name: (*name).to_string(),
                rf: *rf,
                ordering: vals_cluster::Ordering::Unsorted,
                scope: scope.clone(),
            },
        );
    }

    let zcfg = SpdkZoneConfig {
        super_blocks: SUPER_BLOCKS,
        region_blocks: REGION_BLOCKS,
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
    };

    let mut ring = Ring::with_vnodes(3, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let base = (node as usize - 1) * dpn;
        let mut shard_addrs = Vec::new();
        for j in 0..dpn {
            let ctrlr = SpdkController::attach(&SpdkConfig {
                pci_addr: pcis[base + j].clone(),
                nsid: 1,
            })
            .expect("attach spdk controller");
            let provider =
                SpdkZoneStore::open(node, ctrlr, zcfg, true).expect("open spdk zone store");
            let zone0 = provider
                .open(vals_cluster::DEFAULT_ZONE, vals_cluster::Ordering::Unsorted)
                .expect("open zone 0");
            let (addr, handle) =
                ReplicaServer::spawn_zoned_with_handle(node, zone0, provider, catalog.clone())
                    .expect("spawn zoned shard");
            server_handles.push(handle);
            shard_addrs.push(addr);
        }
        node_shards.push((node, shard_addrs));
        ring.add_node(node);
    }
    println!(
        "--- distzone: {NODES} nodes x {dpn} SPDK drives, zones a(rf=3) b(rf=2) c(rf=1, scope={{1}}) ---"
    );

    let coord = Coordinator::new(
        7000,
        ring,
        2,
        NetTransport::connect_sharded_unbounded(node_shards),
    );
    vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).expect("refresh catalog");

    vals_rt::block_on(
        coord.write_in(1, b"iso", b"in-a", WriteConcern::Quorum(2)),
        reactor::poll_io,
    )
    .expect("write zone a");
    vals_rt::block_on(
        coord.write_in(2, b"iso", b"in-b", WriteConcern::Quorum(2)),
        reactor::poll_io,
    )
    .expect("write zone b");
    let ra = vals_rt::block_on(coord.read_in(1, b"iso"), reactor::poll_io).expect("read zone a");
    let rb = vals_rt::block_on(coord.read_in(2, b"iso"), reactor::poll_io).expect("read zone b");
    assert_eq!(
        ra.as_ref().and_then(|s| s.value.as_deref()),
        Some(&b"in-a"[..])
    );
    assert_eq!(
        rb.as_ref().and_then(|s| s.value.as_deref()),
        Some(&b"in-b"[..])
    );
    assert_eq!(
        coord.replicas_for(1, b"iso").unwrap().len(),
        3,
        "zone a rf=3"
    );
    assert_eq!(
        coord.replicas_for(2, b"iso").unwrap().len(),
        2,
        "zone b rf=2"
    );
    let reps_c = coord.replicas_for(3, b"iso").unwrap();
    assert_eq!(reps_c, vec![1], "zone c rf=1 scoped to node 1");
    println!("  isolation + per-zone RF: OK (a/b hold independent 'iso', c -> node 1)");

    let coord = Rc::new(coord);
    for (id, name, _, _) in &zones {
        let val = make_value(vsize, *id as u64);
        let next = Rc::new(Cell::new(0usize));
        let werr = Rc::new(Cell::new(0usize));
        let ex = LocalExecutor::new();
        let w0 = Instant::now();
        for _ in 0..CONC {
            let (coord, next, werr, val, id) =
                (coord.clone(), next.clone(), werr.clone(), val.clone(), *id);
            ex.spawn(async move {
                loop {
                    let i = next.get();
                    if i >= n {
                        break;
                    }
                    next.set(i + 1);
                    if coord
                        .write_in(
                            id,
                            &format!("z-k{i:09}").into_bytes(),
                            &val,
                            WriteConcern::Majority,
                        )
                        .await
                        .is_err()
                    {
                        werr.set(werr.get() + 1);
                    }
                }
            });
        }
        ex.run(reactor::poll_io);
        let wsecs = w0.elapsed().as_secs_f64();
        let next = Rc::new(Cell::new(0usize));
        let rerr = Rc::new(Cell::new(0usize));
        let ex = LocalExecutor::new();
        let r0 = Instant::now();
        for _ in 0..CONC {
            let (coord, next, rerr, id) = (coord.clone(), next.clone(), rerr.clone(), *id);
            ex.spawn(async move {
                loop {
                    let i = next.get();
                    if i >= n {
                        break;
                    }
                    next.set(i + 1);
                    if coord
                        .read_in(id, &format!("z-k{i:09}").into_bytes())
                        .await
                        .is_err()
                    {
                        rerr.set(rerr.get() + 1);
                    }
                }
            });
        }
        ex.run(reactor::poll_io);
        let rsecs = r0.elapsed().as_secs_f64();
        println!(
            "  zone {name}: write {:>9.0} ops/s ({} errs), read {:>9.0} ops/s ({} errs)",
            n as f64 / wsecs,
            werr.get(),
            n as f64 / rsecs,
            rerr.get()
        );
    }
    println!(
        "  (superblock recovery — write/drop/reopen — is covered by the zonestore unit tests;\n   re-run after a process restart to confirm zones reopen from the on-device superblock.)"
    );

    drop(coord);
    for h in server_handles {
        h.shutdown();
    }
}

#[cfg(feature = "spdk")]
#[allow(non_snake_case)]
pub(super) fn workload_distsorted(pci_list: &str, n: usize, vsize: usize, ncoord: usize, dpn: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::zonestore::{SpdkZoneConfig, SpdkZoneStore, ZoneStore};
    use vals_cluster::{
        Catalog, Coordinator, WriteConcern, NetTransport, RangeMap, ReplicaServer, Ring, ZoneConfig,
    };
    use vals_rt::LocalExecutor;
    use vals_spdk::spdk::{SpdkConfig, SpdkController};

    let pcis: Vec<String> = pci_list
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    let RF: usize = (env_u64("VALS_DS_RF", 3) as usize).max(1);
    let R: usize = (env_u64("VALS_DS_R", 2) as usize).max(1).min(RF);
    const ZID: u32 = 1;
    const CONC: usize = 64;
    let dpn = dpn.max(1);
    let ncoord = ncoord.max(1);
    assert!(
        pcis.len() % dpn == 0 && pcis.len() / dpn >= RF,
        "distsorted needs a multiple of dpn={dpn} drives, with >= RF={RF} nodes (>= {} drives)",
        RF * dpn
    );
    let nodes = (pcis.len() / dpn) as u32;
    let nshard = nodes as usize * dpn;

    let keys_per_shard = (n as u64 * RF as u64).div_ceil(nshard as u64);
    let bytes_per_shard = keys_per_shard.saturating_mul(vsize as u64 + 64);
    let region_blocks =
        ((bytes_per_shard.saturating_mul(2) / 4096).max(1 << 16)).next_multiple_of(SEG_BLOCKS);
    let zcfg = SpdkZoneConfig {
        super_blocks: 16,
        region_blocks,
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
    };

    let mut catalog = Catalog::new();
    catalog.insert(
        ZID,
        ZoneConfig {
            name: "sorted".to_string(),
            rf: RF,
            ordering: vals_cluster::Ordering::Sorted,
            scope: None,
        },
    );

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut all_shard_addrs: Vec<(u32, usize, std::net::SocketAddr)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=nodes {
        let base = (node as usize - 1) * dpn;
        let mut shard_addrs = Vec::new();
        for j in 0..dpn {
            let ctrlr = SpdkController::attach(&SpdkConfig {
                pci_addr: pcis[base + j].clone(),
                nsid: 1,
            })
            .expect("attach spdk controller");
            let provider =
                SpdkZoneStore::open(node, ctrlr, zcfg, true).expect("open spdk zone store");
            let zone0 = provider
                .open(vals_cluster::DEFAULT_ZONE, vals_cluster::Ordering::Unsorted)
                .expect("open zone 0");
            let (addr, handle) =
                ReplicaServer::spawn_zoned_with_handle(node, zone0, provider, catalog.clone())
                    .expect("spawn zoned shard");
            server_handles.push(handle);
            all_shard_addrs.push((node, j, addr));
            shard_addrs.push(addr);
        }
        node_shards.push((node, shard_addrs));
        ring.add_node(node);
    }

    let key = |i: usize| format!("sk-{i:09}").into_bytes();
    let nranges = match env_u64("VALS_DS_NRANGES", 0) as usize {
        0 => (nshard * 8).max(16).min(n.max(1)),
        v => v.max(1).min(n.max(1)),
    };
    let splits: Vec<Vec<u8>> = (1..nranges).map(|r| key(r * n / nranges)).collect();
    let map = RangeMap::from_splits(splits);

    println!(
        "--- distsorted: {nodes} nodes x {dpn} drives (= {nshard} sorted shards), RF={RF}, R={R}, \
         {ncoord} coords, {} ranges, region={} MiB, n={n} ---",
        map.num_ranges(),
        region_blocks * 4096 / (1 << 20)
    );

    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        let map = map.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Coordinator::new(
                3000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            );
            vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).expect("refresh catalog");
            coord.set_range_map(ZID, map);
            let coord = Rc::new(coord);

            barrier.wait();
            let next = Rc::new(Cell::new(c));
            let werr = Rc::new(Cell::new(0usize));
            let cnt = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let w0 = Instant::now();
            for _ in 0..CONC {
                let (coord, next, werr, cnt) =
                    (coord.clone(), next.clone(), werr.clone(), cnt.clone());
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= n {
                            break;
                        }
                        next.set(i + ncoord);
                        let v = {
                            let mut v = make_value(vsize, i as u64);
                            let m = 8.min(v.len());
                            v[..m].copy_from_slice(&(i as u64).to_le_bytes()[..m]);
                            v
                        };
                        if coord
                            .write_in(ZID, &key(i), &v, WriteConcern::All)
                            .await
                            .is_err()
                        {
                            werr.set(werr.get() + 1);
                        } else {
                            cnt.set(cnt.get() + 1);
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            (w0.elapsed().as_secs_f64(), cnt.get(), werr.get())
        }));
    }
    let mut wsecs = 0f64;
    let mut wrote = 0usize;
    let mut werrs = 0usize;
    for h in handles {
        let (s, c, e) = h.join().unwrap();
        wsecs = wsecs.max(s);
        wrote += c;
        werrs += e;
    }
    println!(
        "  WRITE : {wrote} keys in {wsecs:.2}s = {:>9.0} ops/s ({werrs} errs){}",
        wrote as f64 / wsecs,
        if werrs > 0 { "  *** INVALID ***" } else { "" }
    );

    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        let map = map.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Coordinator::new(
                4000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            );
            vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).expect("refresh catalog");
            coord.set_range_map(ZID, map);
            let coord = Rc::new(coord);

            barrier.wait();
            let next = Rc::new(Cell::new(c));
            let rerr = Rc::new(Cell::new(0usize));
            let mism = Rc::new(Cell::new(0usize));
            let cnt = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let r0 = Instant::now();
            for _ in 0..CONC {
                let (coord, next, rerr, mism, cnt) =
                    (coord.clone(), next.clone(), rerr.clone(), mism.clone(), cnt.clone());
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= n {
                            break;
                        }
                        next.set(i + ncoord);
                        match coord.read_in(ZID, &key(i)).await {
                            Ok(Some(s)) => {
                                cnt.set(cnt.get() + 1);
                                let ok = s.value.as_deref().is_some_and(|v| {
                                    v.len() >= 8
                                        && u64::from_le_bytes(v[..8].try_into().unwrap())
                                            == i as u64
                                });
                                if !ok {
                                    mism.set(mism.get() + 1);
                                }
                            }
                            Ok(None) => {
                                cnt.set(cnt.get() + 1);
                                mism.set(mism.get() + 1);
                            }
                            Err(_) => rerr.set(rerr.get() + 1),
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            (r0.elapsed().as_secs_f64(), cnt.get(), rerr.get(), mism.get())
        }));
    }
    let (mut rsecs, mut read, mut rerrs, mut mismatches) = (0f64, 0usize, 0usize, 0usize);
    for h in handles {
        let (s, c, e, m) = h.join().unwrap();
        rsecs = rsecs.max(s);
        read += c;
        rerrs += e;
        mismatches += m;
    }
    println!(
        "  READ  : {read} keys in {rsecs:.2}s = {:>9.0} ops/s ({rerrs} errs, {mismatches} mismatches){}",
        read as f64 / rsecs,
        if rerrs + mismatches > 0 {
            "  *** INVALID ***"
        } else {
            ""
        }
    );

    let verify = Coordinator::new(
        9000,
        ring.clone(),
        R,
        NetTransport::connect_sharded_unbounded(node_shards.clone()),
    );
    vals_rt::block_on(verify.refresh_catalog(1), reactor::poll_io).expect("refresh catalog");
    verify.set_range_map(ZID, map.clone());

    let s0 = Instant::now();
    let all = vals_rt::block_on(verify.scan_in(ZID, None, None, R, None), reactor::poll_io)
        .expect("full scan");
    let ssecs = s0.elapsed().as_secs_f64();
    let mut sorted_ok = all.len() == n;
    let mut prev: Option<Vec<u8>> = None;
    for (i, (k, v)) in all.iter().enumerate() {
        if let Some(p) = &prev {
            if k <= p {
                sorted_ok = false;
            }
        }
        prev = Some(k.clone());
        let idx = u64::from_le_bytes(v[..8].try_into().unwrap()) as usize;
        if k != &key(i) || idx != i {
            sorted_ok = false;
        }
    }
    println!(
        "  SCAN  : full scan {} keys in {ssecs:.2}s = {:>9.0} keys/s — {}{}",
        all.len(),
        all.len() as f64 / ssecs,
        if sorted_ok {
            "globally sorted + verified"
        } else {
            "MISMATCH"
        },
        if sorted_ok { "" } else { "  *** INVALID ***" }
    );

    fn raw_shard_count(addr: std::net::SocketAddr, zone: u32) -> usize {
        use std::io::{Read, Write};
        use std::net::TcpStream;
        let mut s = TcpStream::connect(addr).unwrap();
        s.write_all(&vals_cluster::proto::encode_hello()).unwrap();
        {
            let mut lenb = [0u8; 4];
            s.read_exact(&mut lenb).unwrap();
            let mut hello = vec![0u8; u32::from_le_bytes(lenb) as usize];
            s.read_exact(&mut hello).unwrap();
            vals_cluster::proto::decode_hello(&hello).expect("server hello");
        }
        let mut total = 0usize;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let payload =
                vals_cluster::wire::encode_request_payload(&vals_cluster::wire::Request::Scan {
                    epoch: vals_cluster::wire::INSPECT_EPOCH,
                    zone_id: zone,
                    lo: cursor.as_deref(),
                    hi: None,
                    max: 0,
                });
            let len = (8 + payload.len()) as u32;
            let mut frame = len.to_le_bytes().to_vec();
            frame.extend_from_slice(&1u64.to_le_bytes());
            frame.extend_from_slice(&payload);
            s.write_all(&frame).unwrap();
            let mut lenb = [0u8; 4];
            s.read_exact(&mut lenb).unwrap();
            let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
            s.read_exact(&mut block).unwrap();
            match vals_cluster::wire::decode_response(&block[8..]).unwrap() {
                vals_cluster::wire::Response::Scan { entries, more, .. } => {
                    total += entries.len();
                    if !more {
                        return total;
                    }
                    let mut next = entries
                        .last()
                        .expect("truncated page is non-empty")
                        .0
                        .clone();
                    next.push(0);
                    cursor = Some(next);
                }
                _ => return total,
            }
        }
    }
    let mut total_copies = 0usize;
    let mut min_used = usize::MAX;
    for node in 1..=nodes {
        let counts: Vec<usize> = all_shard_addrs
            .iter()
            .filter(|(nd, _, _)| *nd == node)
            .map(|(_, _, a)| raw_shard_count(*a, ZID))
            .collect();
        let used = counts.iter().filter(|&&c| c > 0).count();
        min_used = min_used.min(used);
        total_copies += counts.iter().sum::<usize>();
        println!("  drive distribution node {node}: {counts:?} ({used}/{dpn} drives used)");
    }
    let spread_ok = dpn == 1 || min_used > 1;
    println!(
        "  total replica copies across all drives: {total_copies} (expect n*RF={}) — spread {}{}",
        n * RF,
        if spread_ok { "OK" } else { "NOT SPREAD" },
        if spread_ok && total_copies == n * RF {
            ""
        } else {
            "  *** CHECK ***"
        }
    );

    let window = (n / map.num_ranges()).max(1);
    let scans_per = (8000 / ncoord).max(200);
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        let map = map.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Coordinator::new(
                5000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            );
            vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).expect("refresh catalog");
            coord.set_range_map(ZID, map);
            let coord = Rc::new(coord);
            barrier.wait();
            let next = Rc::new(Cell::new(0usize));
            let keys_read = Rc::new(Cell::new(0usize));
            let serr = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let t0 = Instant::now();
            for _ in 0..CONC {
                let (coord, next, keys_read, serr) =
                    (coord.clone(), next.clone(), keys_read.clone(), serr.clone());
                ex.spawn(async move {
                    loop {
                        let s = next.get();
                        if s >= scans_per {
                            break;
                        }
                        next.set(s + 1);
                        let lo = (s.wrapping_mul(2_654_435_761) % n.saturating_sub(window).max(1))
                            as usize;
                        let key = |i: usize| format!("sk-{i:09}").into_bytes();
                        match coord
                            .scan_in(ZID, Some(&key(lo)), Some(&key(lo + window)), R, None)
                            .await
                        {
                            Ok(rows) => keys_read.set(keys_read.get() + rows.len()),
                            Err(_) => serr.set(serr.get() + 1),
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            (t0.elapsed().as_secs_f64(), keys_read.get(), serr.get())
        }));
    }
    let (mut secs, mut kr, mut se) = (0f64, 0usize, 0usize);
    for h in handles {
        let (s, k, e) = h.join().unwrap();
        secs = secs.max(s);
        kr += k;
        se += e;
    }
    let total_scans = scans_per * ncoord;
    println!(
        "  RANGE-SCAN: {total_scans} window scans (~{window} keys each) in {secs:.2}s = \
         {:>8.0} scans/s, {:>9.0} keys/s ({se} errs){}",
        total_scans as f64 / secs,
        kr as f64 / secs,
        if se > 0 { "  *** INVALID ***" } else { "" }
    );

    for h in server_handles {
        h.shutdown();
    }
}

pub(super) fn workload_dist(n: usize, vsize: usize, read_conc: usize) {
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, ReplicaServer, Ring, VersionedStore,
    };
    use vals_store::{Engine, Ordering};

    const NODES: u32 = 5;
    const RF: usize = 3;
    const R: usize = 2;

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut addrs = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, vsize));
        let engine = Engine::create_prewiped(dev, bench_config(Ordering::Unsorted));
        let (addr, handle) =
            ReplicaServer::spawn_with_handle(VersionedStore::new(engine, node).unwrap())
                .expect("spawn replica");
        server_handles.push(handle);
        addrs.push((node, addr));
        ring.add_node(node);
    }
    let transport = if std::env::var_os("VALS_BENCH_BOUNDED_CLIENT").is_some() {
        NetTransport::connect(&addrs)
    } else {
        NetTransport::connect_unbounded(&addrs)
    };
    let coord = Coordinator::new(0, ring, R, transport);
    println!("  cluster      : {NODES} nodes, RF={RF}, R={R}, loopback TCP\n");

    let key_of = |i: usize| format!("k{i:09}").into_bytes();

    for i in 0..NODES as usize * 4 {
        let _ = vals_rt::block_on(
            coord.write_with(&key_of(i), &make_value(vsize, 0), WriteConcern::All),
            reactor::poll_io,
        );
    }

    for (label, tier) in [
        ("write NoSync (W=0)", WriteConcern::Unacknowledged),
        ("write OneNode (W=1)", WriteConcern::One),
        ("write Quorum (W=2)", WriteConcern::Quorum(2)),
        ("write Replicated (W=RF=3)", WriteConcern::All),
    ] {
        let mut lat = Vec::with_capacity(n);
        let start = Instant::now();
        for i in 0..n {
            let val = make_value(vsize, i as u64);
            let t = Instant::now();
            vals_rt::block_on(coord.write_with(&key_of(i), &val, tier), reactor::poll_io)
                .expect("write");
            lat.push(t.elapsed().as_nanos() as u64);
        }
        report(
            label,
            lat,
            start.elapsed().as_secs_f64(),
            (n * vsize) as u64,
        );
    }

    {
        let mut lat = Vec::with_capacity(n);
        let start = Instant::now();
        for i in 0..n {
            let t = Instant::now();
            vals_rt::block_on(coord.read(&key_of(i)), reactor::poll_io).expect("read");
            lat.push(t.elapsed().as_nanos() as u64);
        }
        report(
            "read R=2 (sequential)",
            lat,
            start.elapsed().as_secs_f64(),
            0,
        );
    }

    {
        use std::cell::Cell;
        use std::rc::Rc;
        use vals_rt::LocalExecutor;

        let coord = Rc::new(coord);
        let next = Rc::new(Cell::new(0usize));
        let ex = LocalExecutor::new();
        let start = Instant::now();
        for _ in 0..read_conc {
            let coord = coord.clone();
            let next = next.clone();
            ex.spawn(async move {
                loop {
                    let i = next.get();
                    if i >= n {
                        break;
                    }
                    next.set(i + 1);
                    let key = format!("k{i:09}").into_bytes();
                    let _ = coord.read(&key).await;
                }
            });
        }
        ex.run(reactor::poll_io);
        let secs = start.elapsed().as_secs_f64();
        println!("--- read R=2 (concurrent, {read_conc} workers) ---");
        println!("  ops          : {n}");
        println!("  wall         : {secs:.3} s");
        println!("  throughput   : {:.0} ops/s", n as f64 / secs);
    }

    for h in server_handles {
        h.shutdown();
    }
}

pub(super) fn workload_distmc(n: usize, vsize: usize, ncoord: usize) {
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, ReplicaServer, Ring, VersionedStore,
    };
    use vals_store::{Engine, Ordering};

    const NODES: u32 = 5;
    const RF: usize = 3;
    const R: usize = 2;
    let ncoord = ncoord.max(1);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut addrs = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, vsize));
        let engine = Engine::create_prewiped(dev, bench_config(Ordering::Unsorted));
        let (addr, handle) =
            ReplicaServer::spawn_with_handle(VersionedStore::new(engine, node).unwrap())
                .expect("spawn replica");
        server_handles.push(handle);
        addrs.push((node, addr));
        ring.add_node(node);
    }
    println!("  cluster      : {NODES} nodes, RF={RF}, R={R}, {ncoord} coordinators\n");

    let per = n / ncoord;
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let addrs = addrs.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let coord = Coordinator::new(
                1000 + c as u32,
                ring,
                R,
                NetTransport::connect_unbounded(&addrs),
            );
            let key = |i: usize| format!("c{c}-k{i:09}").into_bytes();
            let val = make_value(vsize, c as u64);

            for i in 0..NODES as usize {
                let _ = vals_rt::block_on(
                    coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
                    reactor::poll_io,
                );
            }
            barrier.wait();

            let w0 = Instant::now();
            for i in 0..per {
                vals_rt::block_on(
                    coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
                    reactor::poll_io,
                )
                .expect("write");
            }
            let w = w0.elapsed().as_secs_f64();

            let r0 = Instant::now();
            for i in 0..per {
                vals_rt::block_on(coord.read(&key(i)), reactor::poll_io).expect("read");
            }
            let r = r0.elapsed().as_secs_f64();
            (w, r)
        }));
    }

    let mut max_w = 0.0f64;
    let mut max_r = 0.0f64;
    for h in handles {
        let (w, r) = h.join().unwrap();
        max_w = max_w.max(w);
        max_r = max_r.max(r);
    }
    let total = per * ncoord;
    println!("--- distmc writes (Quorum W=2, {ncoord} coordinators) ---");
    println!("  ops          : {total}");
    println!("  wall         : {max_w:.3} s");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_w);
    println!("--- distmc reads (R=2, {ncoord} coordinators) ---");
    println!("  ops          : {total}");
    println!("  wall         : {max_r:.3} s");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_r);

    for h in server_handles {
        h.shutdown();
    }
}

pub(super) fn workload_distmcp(n: usize, vsize: usize, ncoord: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, Ring, ShardedServer, VersionedStore,
    };
    use vals_rt::LocalExecutor;

    const NODES: u32 = 3;
    const DPN: usize = 2;
    const RF: usize = 3;
    const R: usize = 2;
    const READ_CONC: usize = 64;
    let ncoord = ncoord.max(1);
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let stores: Vec<VersionedStore<MemBlockDevice>> = (0..DPN)
            .map(|_| {
                let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, vsize));
                VersionedStore::new(Engine::create_prewiped(dev, cfg), node).unwrap()
            })
            .collect();
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn node shards");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    println!(
        "--- distmcp (mem): {NODES} nodes x {DPN} shards, RF={RF}, R={R}, {ncoord} coordinators ---"
    );

    let per = n / ncoord;
    let barrier = Arc::new(Barrier::new(ncoord));
    let mut handles = Vec::new();
    for c in 0..ncoord {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let fanout = match std::env::var("VALS_READ_FANOUT").as_deref() {
                Ok("all") => vals_cluster::ReadFanout::All,
                Ok("quorum") => vals_cluster::ReadFanout::Quorum,
                _ => vals_cluster::ReadFanout::Digest,
            };
            let coord = Coordinator::new(
                1000 + c as u32,
                ring,
                R,
                NetTransport::connect_sharded_unbounded(node_shards),
            )
            .with_read_fanout(fanout);
            let val = make_value(vsize, c as u64);
            for i in 0..16 {
                let _ = vals_rt::block_on(
                    coord.write_with(
                        &format!("c{c}-k{i:09}").into_bytes(),
                        &val,
                        WriteConcern::Quorum(2),
                    ),
                    reactor::poll_io,
                );
            }
            barrier.wait();
            let coord = Rc::new(coord);

            let next = Rc::new(Cell::new(0usize));
            let werr = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let w0 = Instant::now();
            for _ in 0..READ_CONC {
                let coord = coord.clone();
                let next = next.clone();
                let werr = werr.clone();
                let val = val.clone();
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= per {
                            break;
                        }
                        next.set(i + 1);
                        if coord
                            .write_with(
                                &format!("c{c}-k{i:09}").into_bytes(),
                                &val,
                                WriteConcern::Quorum(2),
                            )
                            .await
                            .is_err()
                        {
                            werr.set(werr.get() + 1);
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            let wsecs = w0.elapsed().as_secs_f64();
            if c == 0 {
                println!("  [coord0] write errors: {}/{per}", werr.get());
            }

            let next = Rc::new(Cell::new(0usize));
            let rerr = Rc::new(Cell::new(0usize));
            let ex = LocalExecutor::new();
            let r0 = Instant::now();
            for _ in 0..READ_CONC {
                let coord = coord.clone();
                let next = next.clone();
                let rerr = rerr.clone();
                ex.spawn(async move {
                    loop {
                        let i = next.get();
                        if i >= per {
                            break;
                        }
                        next.set(i + 1);
                        if coord
                            .read(&format!("c{c}-k{i:09}").into_bytes())
                            .await
                            .is_err()
                        {
                            rerr.set(rerr.get() + 1);
                        }
                    }
                });
            }
            ex.run(reactor::poll_io);
            if c == 0 {
                println!("  [coord0] read errors: {}/{per}", rerr.get());
            }
            (wsecs, r0.elapsed().as_secs_f64())
        }));
    }
    let (mut maxw, mut maxr) = (0.0f64, 0.0f64);
    for h in handles {
        let (w, r) = h.join().unwrap();
        maxw = maxw.max(w);
        maxr = maxr.max(r);
    }
    let total = per * ncoord;
    println!(
        "  write Quorum(2)            : {:.0} ops/s",
        total as f64 / maxw
    );
    println!(
        "  read R=2 (conc {READ_CONC}/coord)  : {:.0} ops/s",
        total as f64 / maxr
    );
    let connects = vals_cluster::net::CONNECTS.load(std::sync::atomic::Ordering::Relaxed);
    let ops = (total * 2) as f64;
    println!(
        "  conns opened: {connects} ({:.3}/op)",
        connects as f64 / ops
    );

    for h in server_handles {
        h.shutdown();
    }
}

fn run_read_pass<T>(
    coord: std::rc::Rc<vals_cluster::Coordinator<T>>,
    keys: &[Vec<u8>],
    val: &[u8],
    conc: usize,
    hedge: Option<(std::rc::Rc<vals_cluster::coord::HedgeClock>, Duration)>,
) -> (Vec<u64>, usize, f64)
where
    T: vals_cluster::coord::Transport + 'static,
{
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use vals_cluster::reactor::{self, Notify, Timer};
    use vals_rt::{Either, LocalExecutor};

    let ex = LocalExecutor::new();
    let next = Rc::new(Cell::new(0usize));
    let errs = Rc::new(Cell::new(0usize));
    let lats: Rc<RefCell<Vec<u64>>> = Rc::new(RefCell::new(Vec::with_capacity(keys.len())));
    let keys: Rc<Vec<Vec<u8>>> = Rc::new(keys.to_vec());
    let val: Rc<Vec<u8>> = Rc::new(val.to_vec());
    let stop = Rc::new(Notify::new());
    let active = Rc::new(Cell::new(conc));
    let has_ticker = hedge.is_some();

    if let Some((clock, period)) = hedge {
        let stop = stop.clone();
        ex.spawn(async move {
            let timer = match Timer::every(period) {
                Ok(t) => t,
                Err(_) => return,
            };
            loop {
                match vals_rt::race(timer.tick(), stop.notified()).await {
                    Either::Left(()) => clock.advance(),
                    Either::Right(()) => return,
                }
            }
        });
    }

    let t0 = Instant::now();
    for _ in 0..conc {
        let coord = coord.clone();
        let next = next.clone();
        let errs = errs.clone();
        let lats = lats.clone();
        let keys = keys.clone();
        let val = val.clone();
        let active = active.clone();
        let stop = stop.clone();
        ex.spawn(async move {
            loop {
                let i = next.get();
                if i >= keys.len() {
                    break;
                }
                next.set(i + 1);
                let t = Instant::now();
                let r = coord.read(&keys[i]).await;
                lats.borrow_mut().push(t.elapsed().as_nanos() as u64);
                let ok = matches!(r, Ok(Some(ref s)) if s.value.as_deref() == Some(val.as_slice()));
                if !ok {
                    errs.set(errs.get() + 1);
                }
            }
            let rem = active.get() - 1;
            active.set(rem);
            if rem == 0 && has_ticker {
                stop.signal();
            }
        });
    }
    ex.run(reactor::poll_io);
    let secs = t0.elapsed().as_secs_f64();
    let lats = Rc::try_unwrap(lats)
        .expect("lats uniquely held")
        .into_inner();
    (lats, errs.get(), secs)
}

pub(super) fn workload_disthedge(n: usize, vsize: usize) {
    use std::rc::Rc;
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, ReadFanout, Ring, ShardedServer, VersionedStore,
    };

    const NODES: u32 = 3;
    const RF: usize = 3;
    const R: usize = 2;
    const READ_CONC: usize = 32;
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, vsize));
        let store = VersionedStore::new(Engine::create_prewiped(dev, cfg), node).unwrap();
        let (addrs, handle) = ShardedServer::spawn_with_handle(vec![store]).expect("spawn node");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }

    let (slow_node, slow_dur, slow_mode) =
        slow_node_env().unwrap_or((2, Duration::from_micros(5000), SlowMode::Digest));
    let hedge_delay = hedge_delay_env().unwrap_or(Duration::from_micros(1000));
    let mode_str = match slow_mode {
        SlowMode::Digest => "digest",
        SlowMode::All => "all",
    };
    println!(
        "--- disthedge (mem): {NODES} nodes x 1 shard, RF={RF}, R={R}, fanout=digest, \
         slow-node={slow_node} delay={} µs mode={mode_str}, hedge-delay={} µs, conc={READ_CONC} ---",
        slow_dur.as_micros(),
        hedge_delay.as_micros(),
    );

    let keys: Vec<Vec<u8>> = (0..n).map(|i| format!("k{i:09}").into_bytes()).collect();
    let val = make_value(vsize, 0);

    {
        let coord = Coordinator::new(
            1000,
            ring.clone(),
            R,
            NetTransport::connect_sharded_unbounded(node_shards.clone()),
        )
        .with_read_fanout(ReadFanout::Digest);
        for k in &keys {
            vals_rt::block_on(
                coord.write_with(k, &val, WriteConcern::Quorum(2)),
                reactor::poll_io,
            )
            .expect("populate write");
        }
    }

    let hedges = || vals_cluster::observe::counters().digest_hedges_fired.get();

    {
        let coord = Rc::new(
            Coordinator::new(
                1001,
                ring.clone(),
                R,
                NetTransport::connect_sharded_unbounded(node_shards.clone()),
            )
            .with_read_fanout(ReadFanout::Digest),
        );
        let h0 = hedges();
        let (lat, errs, secs) = run_read_pass(coord, &keys, &val, READ_CONC, None);
        report_tail("(a) healthy, hedge OFF", lat, secs, errs, hedges() - h0);
    }

    {
        let inner = NetTransport::connect_sharded_unbounded(node_shards.clone());
        let coord = Rc::new(
            Coordinator::new(
                1002,
                ring.clone(),
                R,
                SlowNode::new(inner, slow_node, slow_dur, slow_mode),
            )
            .with_read_fanout(ReadFanout::Digest),
        );
        let h0 = hedges();
        let (lat, errs, secs) = run_read_pass(coord, &keys, &val, READ_CONC, None);
        report_tail(
            "(b) slow replica, hedge OFF",
            lat,
            secs,
            errs,
            hedges() - h0,
        );
    }

    {
        let inner = NetTransport::connect_sharded_unbounded(node_shards.clone());
        let coord = Coordinator::new(
            1003,
            ring.clone(),
            R,
            SlowNode::new(inner, slow_node, slow_dur, slow_mode),
        )
        .with_read_fanout(ReadFanout::Digest)
        .with_digest_hedge(hedge_delay);
        let clock = coord.hedge_clock().expect("hedge configured");
        let period = coord.hedge_tick_period().expect("hedge configured");
        let coord = Rc::new(coord);
        let h0 = hedges();
        let (lat, errs, secs) = run_read_pass(coord, &keys, &val, READ_CONC, Some((clock, period)));
        report_tail("(c) slow replica, hedge ON", lat, secs, errs, hedges() - h0);
    }

    for h in server_handles {
        h.shutdown();
    }
}

pub(super) fn workload_distshard(n: usize, vsize: usize, shards: usize) {
    use std::sync::{Arc, Barrier};
    use vals_cluster::reactor;
    use vals_cluster::{
        Coordinator, WriteConcern, NetTransport, Ring, ShardedServer, VersionedStore,
    };
    use vals_store::{Engine, Ordering};

    const NODES: u32 = 5;
    const RF: usize = 3;
    const R: usize = 2;
    const NCOORD: usize = 8;
    let shards = shards.max(1);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut node_shards: Vec<(u32, Vec<std::net::SocketAddr>)> = Vec::new();
    let mut server_handles = Vec::new();
    for node in 1..=NODES {
        let stores: Vec<_> = (0..shards)
            .map(|_| {
                let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n / shards + 1, vsize));
                let engine = Engine::create_prewiped(dev, bench_config(Ordering::Unsorted));
                VersionedStore::new(engine, node).unwrap()
            })
            .collect();
        let (addrs, handle) = ShardedServer::spawn_with_handle(stores).expect("spawn shards");
        server_handles.push(handle);
        node_shards.push((node, addrs));
        ring.add_node(node);
    }
    println!(
        "  cluster      : {NODES} nodes x {shards} shards, RF={RF}, R={R}, {NCOORD} coordinators\n"
    );

    let per = n / NCOORD;
    let barrier = Arc::new(Barrier::new(NCOORD));
    let mut handles = Vec::new();
    for c in 0..NCOORD {
        let node_shards = node_shards.clone();
        let ring = ring.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            let transport = NetTransport::connect_sharded_unbounded(node_shards);
            let coord = Coordinator::new(1000 + c as u32, ring, R, transport);
            let key = |i: usize| format!("c{c}-k{i:09}").into_bytes();
            let val = make_value(vsize, c as u64);

            for i in 0..NODES as usize * shards {
                let _ = vals_rt::block_on(
                    coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
                    reactor::poll_io,
                );
            }
            barrier.wait();

            let w0 = Instant::now();
            for i in 0..per {
                vals_rt::block_on(
                    coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
                    reactor::poll_io,
                )
                .expect("write");
            }
            let w = w0.elapsed().as_secs_f64();

            let r0 = Instant::now();
            for i in 0..per {
                vals_rt::block_on(coord.read(&key(i)), reactor::poll_io).expect("read");
            }
            (w, r0.elapsed().as_secs_f64())
        }));
    }

    let (mut max_w, mut max_r) = (0.0f64, 0.0f64);
    for h in handles {
        let (w, r) = h.join().unwrap();
        max_w = max_w.max(w);
        max_r = max_r.max(r);
    }
    let total = per * NCOORD;
    println!("--- distshard writes (Quorum W=2, {shards} shards/node) ---");
    println!("  ops          : {total}");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_w);
    println!("--- distshard reads (R=2, {shards} shards/node) ---");
    println!("  ops          : {total}");
    println!("  throughput   : {:.0} ops/s", total as f64 / max_r);

    for h in server_handles {
        h.shutdown();
    }
}

