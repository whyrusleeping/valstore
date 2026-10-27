use std::net::SocketAddr;
use std::time::{Duration, Instant};

use vals_cluster::coord::Coordinator;
use vals_cluster::drive::{MemDriveSet, PartitionGeom};
use vals_cluster::net::NetTransport;
use vals_cluster::{reactor, Node, Ordering, Ring};

fn eventually(deadline: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + deadline;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn live_keys_gauge_tracks_multidrive_keys_across_flush() {
    let settings = vals_cluster::node_settings::NodeSettings { flush_tail_epochs: 0, ..Default::default() };

    let p = 2;
    let listeners = Node::bind_n(p).unwrap();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
    let shard_listeners: Vec<_> = listeners.into_iter().map(|(l, _)| l).collect();

    let ds = MemDriveSet::uniform(256, 4096, 2);
    let geom = PartitionGeom {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let ring = {
        let mut r = Ring::with_vnodes(1, 64);
        r.add_node(1);
        r
    };
    let peer_shards = vec![(1u32, addrs.clone())];

    let (_weight, handle) = Node::serve_multidrive_with_settings(
        1,
        shard_listeners,
        ds,
        512,
        geom,
        ring.clone(),
        peer_shards,
        Duration::from_millis(50),
        true,
        settings,
    )
    .unwrap();

    let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));

    const N: u64 = 100;
    for i in 0..N {
        let k = format!("k{i:04}");
        vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
    }

    let reached = eventually(Duration::from_secs(5), || {
        vals_cluster::observe::gauges().live_keys.get() == N
    });
    assert!(
        reached,
        "vals_live_keys never reached {N} (got {})",
        vals_cluster::observe::gauges().live_keys.get()
    );

    std::thread::sleep(Duration::from_millis(800));
    assert_eq!(
        vals_cluster::observe::gauges().live_keys.get(),
        N,
        "vals_live_keys must stay exact across flushes (keys in runs count)"
    );

    handle.shutdown();
    let drained = eventually(Duration::from_secs(2), || {
        vals_cluster::observe::gauges().live_keys.get() == 0
    });
    assert!(
        drained,
        "vals_live_keys should return to 0 after shutdown (got {})",
        vals_cluster::observe::gauges().live_keys.get()
    );
}
