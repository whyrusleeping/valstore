use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering, RunBuildOpts};

fn opts() -> RunBuildOpts {
    RunBuildOpts {
        num_buckets: 1,
        ..Default::default()
    }
}

#[test]
fn truncate_then_flush_then_reopen_must_not_resurrect_pre_wipe_manifest() {
    let dev = MemBlockDevice::new(512, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev.share(), cfg);

    e.put(b"old-first", b"1", 5).unwrap();
    e.flush(opts()).unwrap();
    let big = vec![7u8; 1000];
    for i in 0..200u32 {
        e.put(format!("old-{i:04}").as_bytes(), &big, 10 + i as u128)
            .unwrap();
    }
    e.flush(opts()).unwrap();
    e.put(b"old-b", b"2", 1000).unwrap();
    e.flush(opts()).unwrap();

    e.truncate().unwrap();
    assert!(e.get(b"old-0000").unwrap().is_none());

    e.put(b"new", b"fresh", 42).unwrap();
    e.flush(opts()).unwrap();
    e.sync().unwrap();
    assert_eq!(e.get(b"new").unwrap().as_deref(), Some(&b"fresh"[..]));
    drop(e);

    let e2 = Engine::open(dev.share(), cfg).expect("reopen");
    assert_eq!(
        e2.get(b"new").unwrap().as_deref(),
        Some(&b"fresh"[..]),
        "post-wipe write must survive reboot"
    );
    assert!(
        e2.get(b"old-0000").unwrap().is_none(),
        "pre-wipe key old-0000 resurrected after reboot"
    );
    assert!(
        e2.get(b"old-b").unwrap().is_none(),
        "pre-wipe key old-b resurrected after reboot"
    );
}

#[test]
fn truncate_then_crash_before_flush_must_boot_empty() {
    let dev = MemBlockDevice::new(512, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev.share(), cfg);

    e.put(b"old-first", b"1", 5).unwrap();
    e.flush(opts()).unwrap();
    let big = vec![7u8; 1000];
    for i in 0..200u32 {
        e.put(format!("old-{i:04}").as_bytes(), &big, 10 + i as u128)
            .unwrap();
    }
    e.flush(opts()).unwrap();
    e.put(b"old-b", b"2", 1000).unwrap();
    e.flush(opts()).unwrap();

    e.truncate().unwrap();
    drop(e);

    let e2 = Engine::open(dev.share(), cfg).expect("reopen after wipe+crash");
    assert!(
        e2.get(b"old-0000").unwrap().is_none(),
        "pre-wipe key old-0000 resurrected after wipe+crash reboot"
    );
    assert!(
        e2.get(b"old-first").unwrap().is_none(),
        "pre-wipe key old-first resurrected after wipe+crash reboot"
    );
    assert!(
        e2.get(b"old-b").unwrap().is_none(),
        "pre-wipe key old-b resurrected after wipe+crash reboot"
    );
}
