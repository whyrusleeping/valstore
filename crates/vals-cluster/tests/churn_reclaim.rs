use std::net::SocketAddr;
use std::time::{Duration, Instant};

use vals_cluster::coord::{Coordinator, WriteConcern};
use vals_cluster::drive::{MemDriveSet, PartitionGeom};
use vals_cluster::net::NetTransport;
use vals_cluster::{reactor, Node, Ordering, Ring};

const BLOCK_SIZE: usize = 256;
const SEG_BLOCKS: u64 = 16;
const REGION_SEGMENTS: u64 = 2048;
const REGION_BLOCKS: u64 = REGION_SEGMENTS * SEG_BLOCKS;
const REGION_BYTES: u64 = REGION_BLOCKS * BLOCK_SIZE as u64;

#[test]
fn overwrite_churn_reclaims_before_the_region_wedges() {
    let listeners = Node::bind_n(1).unwrap();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
    let shard_listeners: Vec<_> = listeners.into_iter().map(|(l, _)| l).collect();

    let ds = MemDriveSet::uniform(BLOCK_SIZE, REGION_BLOCKS + 1024, 1);
    let geom = PartitionGeom {
        seg_blocks: SEG_BLOCKS,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let ring = {
        let mut r = Ring::with_vnodes(1, 64);
        r.add_node(1);
        r
    };
    let peer_shards = vec![(1u32, addrs.clone())];

    let (_weight, handle) = Node::serve_multidrive(
        1,
        shard_listeners,
        ds,
        REGION_BLOCKS,
        geom,
        ring.clone(),
        peer_shards,
        Duration::from_millis(50),
        true,
    )
    .unwrap();

    let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));

    const KEYS: u64 = 1000;
    const VSIZE: usize = 64;
    const BATCH: u64 = 100;
    const RECORD_EST: u64 = 110;
    let target_appends = 3 * REGION_BYTES;
    let total_writes = target_appends / RECORD_EST;

    let value = vec![0x5au8; VSIZE];
    let mut failed_batches = 0u64;
    let mut round = 0u64;
    let mut written = 0u64;
    while written < total_writes {
        let keys: Vec<String> = (0..BATCH)
            .map(|i| format!("k{:04}", (round * BATCH + i) % KEYS))
            .collect();
        let writes: Vec<(&[u8], &[u8])> = keys
            .iter()
            .map(|k| (k.as_bytes(), value.as_slice()))
            .collect();
        let res = vals_rt::block_on(
            coord.write_batch(&writes, WriteConcern::One),
            reactor::poll_io,
        );
        match res {
            Ok(r) if r.all_ok() => {}
            _ => failed_batches += 1,
        }
        written += BATCH;
        round += 1;
        std::thread::sleep(Duration::from_millis(2));
    }

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut recovered = false;
    while Instant::now() < deadline {
        let res = vals_rt::block_on(coord.write(b"probe", b"alive"), reactor::poll_io);
        if res.is_ok() {
            recovered = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let counters = vals_cluster::observe::counters();
    assert!(
        recovered,
        "zone wedged: writes never succeeded again after {}x-region overwrite churn \
         ({failed_batches} failed batches during churn; run_merges={} snapshots={} \
         segments_freed={})",
        target_appends / REGION_BYTES,
        counters.run_merges.get(),
        counters.snapshots.get(),
        counters.segments_freed.get(),
    );

    assert!(
        counters.run_merges.get() >= 1,
        "no run-merge ever committed (run_merges=0) — churn survival without \
         reclamation should be impossible",
    );
    assert!(
        counters.segments_freed.get() > 0,
        "run-merge committed but freed nothing (segments_freed=0)",
    );

    handle.shutdown();
}
