use vals_spdk::MemBlockDevice;
use vals_store::alloc::Allocator;
use vals_store::run::{self, DeviceRun};
use vals_store::{Config, Engine, Ordering, RunBuildOpts};

#[test]
fn write_version_5_keeps_writing_v5_runs_without_block_bounds() {
    std::env::set_var("VALS_RUN_WRITE_VERSION", "5");
    assert_eq!(run::run_write_version(), run::RUN_BLOCK_VERSION);

    let blocks = 1 << 15;
    let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
    let dev = MemBlockDevice::new(512, blocks);
    let mut e = Engine::create_prewiped(dev.share(), cfg);
    for round in 0..2u64 {
        for i in 0..200u64 {
            e.put(format!("key-{:06}", round * 100 + i).as_bytes(), b"v", (round * 1_000 + i) as u128 + 1).unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
    }
    e.compact_runs(RunBuildOpts::default(), None).unwrap();
    let handles = e.run_handles();
    assert_eq!(handles.len(), 1);
    drop(e);

    let alloc = Allocator::new(blocks, cfg.seg_blocks, cfg.reserved_blocks);
    let merged = DeviceRun::open(std::sync::Arc::new(dev.share()), &alloc, handles[0].clone()).unwrap();
    assert_eq!(merged.format_version(), run::RUN_BLOCK_VERSION);
    assert!(!merged.supports_range_tombstones());
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.version(b"key-000150").unwrap(), Some(1_051));
}
