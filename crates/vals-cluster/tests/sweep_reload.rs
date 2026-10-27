use std::time::{Duration, Instant};

use vals_cluster::coord::Coordinator;
use vals_cluster::net::NetTransport;
use vals_cluster::{reactor, Node, Ring, VersionedStore};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

fn store(node: u32) -> VersionedStore<MemBlockDevice> {
    let dev = MemBlockDevice::new(4096, 4096);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    VersionedStore::new(engine, node).unwrap()
}

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

fn live_keys() -> u64 {
    vals_cluster::observe::gauges().live_keys.get()
}

#[test]
fn set_sweep_every_retunes_gauge_sampling_cadence() {
    let (listener, addr) = Node::bind().unwrap();
    let ring = Ring::from_members(1, 64, [1]);

    let handle = Node::serve(
        1,
        listener,
        store(1),
        ring.clone(),
        vec![(1, addr)],
        Duration::from_millis(50),
    );

    let coord = Coordinator::new(0, ring, 1, NetTransport::connect(&[(1, addr)]));
    let write = |k: &str| {
        vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
    };

    const N: u64 = 30;
    for i in 0..N {
        write(&format!("k{i:04}"));
    }
    assert!(
        eventually(Duration::from_secs(5), || live_keys() == N),
        "gauge should reach {N} at the fast sweep cadence (got {})",
        live_keys()
    );

    handle.set_sweep_every(Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(400));

    const M: u64 = 20;
    for i in N..(N + M) {
        write(&format!("k{i:04}"));
    }
    let picked_up = eventually(Duration::from_millis(800), || live_keys() >= N + M);
    assert!(
        !picked_up,
        "the slow retune should freeze gauge sampling: gauge jumped to {} within 800ms \
         (the next sample should be ~10s out)",
        live_keys()
    );
    assert_eq!(
        live_keys(),
        N,
        "gauge must still read the pre-retune count until the (far off) next tick"
    );

    handle.shutdown();
    assert!(
        eventually(Duration::from_secs(2), || live_keys() == 0),
        "gauge should return to 0 after shutdown (got {})",
        live_keys()
    );
}
