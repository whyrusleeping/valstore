use std::collections::HashMap;

use vals_spdk::MemBlockDevice;
use vals_store::{scan_vlog_records, Config, Engine, Ordering, RecordKind, RunBuildOpts};

const BLOCK_SIZE: usize = 512;
const NUM_BLOCKS: u64 = 8192;

fn cfg() -> Config {
    Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    }
}

fn val(tag: &str, key: &str) -> Vec<u8> {
    format!("{tag}:{key}:{}", "x".repeat(280)).into_bytes()
}

fn write_corpus_a(dev: &MemBlockDevice) {
    let mut e = Engine::create_prewiped(dev.share(), cfg());
    let mut version = 1u128;
    for i in 0..100u32 {
        let k = format!("k{i:03}");
        e.put(k.as_bytes(), &val("A", &k), version).unwrap();
        version += 1;
    }
    for i in 0..50u32 {
        let k = format!("stale-{i:03}");
        e.put(k.as_bytes(), &val("A", &k), version).unwrap();
        version += 1;
    }
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"k000", &val("A-late", "k000"), version).unwrap();
    e.sync().unwrap();
}

fn assert_corpus_a_spans_many_segments(dev: &MemBlockDevice) {
    let stats = scan_vlog_records(dev, &cfg(), |_| Ok::<(), ()>(())).unwrap();
    assert!(
        stats.segments >= 20,
        "corpus A should span many segments (got {}) so stale segments survive a small corpus B",
        stats.segments
    );
}

#[test]
fn create_prewiped_over_dirty_device_resurrects_stale_records() {
    let dev = MemBlockDevice::new(BLOCK_SIZE, NUM_BLOCKS);
    write_corpus_a(&dev);
    assert_corpus_a_spans_many_segments(&dev);

    {
        let mut e = Engine::create_prewiped(dev.share(), cfg());
        for i in 0..20u32 {
            let k = format!("k{i:03}");
            e.put(k.as_bytes(), &val("B", &k), i as u128 + 1).unwrap();
        }
        e.sync().unwrap();
    }

    let e = Engine::open(dev.share(), cfg()).unwrap();

    assert_eq!(
        e.get(b"stale-000").unwrap().as_deref(),
        Some(val("A", "stale-000").as_slice()),
        "stale pre-format key should resurrect (pre-fence behavior)"
    );

    assert_eq!(
        e.get(b"k000").unwrap().as_deref(),
        Some(val("A-late", "k000").as_slice()),
        "stale record should WIN over the live corpus-B write (higher seq)"
    );

    assert_eq!(
        e.get(b"k099").unwrap().as_deref(),
        Some(val("A", "k099").as_slice()),
    );
}

#[test]
fn create_fences_stale_pre_format_segments() {
    let dev = MemBlockDevice::new(BLOCK_SIZE, NUM_BLOCKS);
    write_corpus_a(&dev);
    assert_corpus_a_spans_many_segments(&dev);

    {
        let mut e = Engine::create(dev.share(), cfg()).unwrap();
        for i in 0..20u32 {
            let k = format!("k{i:03}");
            e.put(k.as_bytes(), &val("B", &k), i as u128 + 1).unwrap();
        }
        e.sync().unwrap();
    }

    let e = Engine::open(dev.share(), cfg()).unwrap();

    for i in 0..50u32 {
        let k = format!("stale-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap(),
            None,
            "stale pre-format key {k} must not survive the format wipe"
        );
    }
    for i in 0..20u32 {
        let k = format!("k{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(val("B", &k).as_slice()),
            "overlapping key {k} must serve corpus B, not resurrected corpus A"
        );
    }
    for i in 20..100u32 {
        let k = format!("k{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap(),
            None,
            "corpus-A-only key {k} must not survive the format wipe"
        );
    }
    assert_eq!(e.run_count(), 0, "no pre-format run should be recovered");
}

#[test]
fn create_over_a_used_device_replays_no_record_past_the_new_tail() {
    let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
    let dev = MemBlockDevice::new(BLOCK_SIZE, 256);
    for round in 0..3u32 {
        let mut e = Engine::create(dev.share(), cfg).unwrap();
        for i in 0..40u32 {
            e.put(format!("old-{i:02}").as_bytes(), &[7; 100], 100 + i as u128).unwrap();
        }
        e.sync().unwrap();
        drop(e);
        if round == 1 {
            drop(Engine::open(dev.share(), cfg).unwrap());
        }

        let mut e = Engine::create(dev.share(), cfg).unwrap();
        e.put(b"new", b"v", 1).unwrap();
        e.sync().unwrap();
        drop(e);

        let e = Engine::open(dev.share(), cfg).unwrap();
        let back: Vec<u32> = (0..40).filter(|i| e.get(format!("old-{i:02}").as_bytes()).unwrap().is_some()).collect();
        assert!(back.is_empty(), "round {round}: old keys replayed after a format: {back:?}");
        assert_eq!(e.get(b"new").unwrap().as_deref(), Some(&b"v"[..]));
        let stats = scan_vlog_records(&dev, &cfg, |rec| {
            assert_eq!(rec.key, b"new", "dump surfaced a pre-format record");
            Ok::<(), ()>(())
        })
        .unwrap();
        assert_eq!(stats.records, 1);
    }
}

#[test]
fn dump_over_fenced_device_sees_only_live_records() {
    let dev = MemBlockDevice::new(BLOCK_SIZE, NUM_BLOCKS);
    write_corpus_a(&dev);

    {
        let mut e = Engine::create(dev.share(), cfg()).unwrap();
        for i in 0..20u32 {
            let k = format!("k{i:03}");
            e.put(k.as_bytes(), &val("B", &k), i as u128 + 1).unwrap();
        }
        e.sync().unwrap();
    }

    let mut last: HashMap<Vec<u8>, (RecordKind, Vec<u8>)> = HashMap::new();
    let stats = scan_vlog_records(&dev, &cfg(), |rec| {
        let k = String::from_utf8(rec.key.clone()).unwrap();
        let idx: u32 = k.strip_prefix('k').unwrap().parse().unwrap();
        assert!(idx < 20, "dump surfaced a non-live key: {k}");
        last.insert(rec.key.clone(), (rec.kind, rec.value.clone()));
        Ok::<(), ()>(())
    })
    .unwrap();

    assert_eq!(
        stats.records, 20,
        "exactly corpus B's records, no inflation"
    );
    assert_eq!(last.len(), 20);
    for i in 0..20u32 {
        let k = format!("k{i:03}");
        assert_eq!(
            last.get(k.as_bytes()).map(|(_, v)| v.as_slice()),
            Some(val("B", &k).as_slice()),
        );
    }
}
