use std::sync::Arc;

use vals_spdk::MemBlockDevice;
use vals_store::alloc::{Allocator, SegmentId};
use vals_store::run::{self, DeviceRun, RunVersionStream, SidecarCursorStep};
use vals_store::vlog::ValuePtr;
use vals_store::{Config, Engine, Ordering, RunBuildOpts, SnapshotEntry};

fn entries() -> Vec<SnapshotEntry> {
    (0..400u64)
        .map(|i| SnapshotEntry {
            key: format!("at://did:plc:{:08}/app.bsky.graph.follow/{i:06}", i / 9).into_bytes(),
            ptr: ValuePtr { segment: SegmentId(0), offset: 0, len: 0 },
            seq: i,
            version: 1_000 + i as u128,
            tombstone: i % 5 == 0,
        })
        .collect()
}

fn stream_all(run: &DeviceRun<MemBlockDevice>, alloc: &Allocator) -> Vec<(Vec<u8>, u128)> {
    let mut stream = run.version_stream(alloc).unwrap().unwrap();
    let mut out = Vec::new();
    loop {
        match stream.next_step().unwrap() {
            SidecarCursorStep::Entry(e) => out.push(e),
            SidecarCursorStep::Done => return out,
            SidecarCursorStep::Checkpoint => {}
        }
    }
}

#[test]
fn write_version_4_keeps_sidecars_and_readers_handle_both_formats() {
    std::env::set_var("VALS_RUN_WRITE_VERSION", "4");
    assert_eq!(run::run_write_version(), run::RUN_SIDECAR_VERSION);

    let entries = entries();
    let opts = RunBuildOpts { block_target_bytes: 512, bloom_bits_per_key: 10, num_buckets: 16 };
    let dev = Arc::new(MemBlockDevice::new(512, 1 << 15));
    let mut alloc = Allocator::new(1 << 15, 16, 0);
    let legacy = run::build_run(&entries, opts);
    let current = run::build_run_versioned(&entries, opts, run::RUN_VERSION);
    assert!(legacy.len() > current.len());
    let h4 = run::write_run(&*dev, &mut alloc, &legacy).unwrap();
    let h5 = run::write_run(&*dev, &mut alloc, &current).unwrap();
    let v4 = DeviceRun::open(dev.clone(), &alloc, h4).unwrap();
    let v5 = DeviceRun::open(dev.clone(), &alloc, h5).unwrap();
    assert_eq!((v4.format_version(), v5.format_version()), (run::RUN_SIDECAR_VERSION, run::RUN_VERSION));
    assert!(v4.sidecar_len() > 0);
    assert_eq!(v5.sidecar_len(), 0);
    let model: Vec<(Vec<u8>, u128)> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
    assert_eq!(stream_all(&v4, &alloc), model);
    assert_eq!(stream_all(&v5, &alloc), model);
    let bucket = |k: &[u8]| vals_store::crc::crc32(k);
    for b in 0..opts.num_buckets {
        assert_eq!(
            v4.bucket_digests(&alloc, b, bucket).unwrap(),
            v5.bucket_digests(&alloc, b, bucket).unwrap()
        );
    }

    let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(512, 1 << 15), cfg);
    for round in 0..2u64 {
        for i in 0..200u64 {
            e.put(format!("key-{:06}", round * 100 + i).as_bytes(), b"v", (round * 1_000 + i) as u128 + 1).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
    }
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    let handles = e.run_handles();
    assert_eq!(handles.len(), 1);
    assert!(matches!(e.run_version_stream(&handles[0]).unwrap(), Some(RunVersionStream::Sidecar(_))));
    assert!(e.capacity_stats().sidecar_disk_bytes > 0);
    assert_eq!(e.version(b"key-000150").unwrap(), Some(1_051));
}
