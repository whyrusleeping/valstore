use super::*;

#[test]
fn pipelined_writes_drain_before_reads() {
    let dev = DelayDevice::new(4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let val =
        |i: usize| -> Vec<u8> { (0..1000).map(|b| (i as u8).wrapping_add(b as u8)).collect() };
    for i in 0..200 {
        e.put(format!("key-{i:04}").as_bytes(), &val(i), 0).unwrap();
    }
    for i in (0..200).rev() {
        assert_eq!(
            e.get(format!("key-{i:04}").as_bytes()).unwrap().as_deref(),
            Some(val(i).as_slice()),
            "read-before-sync must drain pending writes (key {i})"
        );
    }
    e.sync().unwrap();
    let dev = e.into_device();
    let e2 = Engine::open(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    )
    .unwrap();
    for i in 0..200 {
        assert_eq!(
            e2.get(format!("key-{i:04}").as_bytes()).unwrap().as_deref(),
            Some(val(i).as_slice()),
            "value must survive sync + reopen (key {i})"
        );
    }
}

#[test]
fn put_get_overwrite_delete() {
    let mut e = engine(Ordering::Unsorted);
    assert!(e.is_empty());
    e.put(b"k", b"v1", 0).unwrap();
    e.put(b"other", b"x", 0).unwrap();
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(e.len(), 2);

    e.put(b"k", b"v2-longer", 0).unwrap();
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v2-longer"[..]));
    assert_eq!(e.len(), 2);

    assert!(e.delete(b"k", 0).unwrap());
    assert_eq!(e.get(b"k").unwrap(), None);
    assert!(!e.delete(b"k", 0).unwrap());
    assert_eq!(e.len(), 1);

    assert_eq!(e.get(b"absent").unwrap(), None);
}

#[test]
fn multi_get_matches_individual_gets() {
    let mut e = engine(Ordering::Unsorted);
    for i in 0..50u32 {
        e.put(
            format!("k{i:03}").as_bytes(),
            format!("v-{i}").as_bytes(),
            0,
        )
        .unwrap();
    }
    e.delete(b"k010", 0).unwrap();

    let keys: Vec<Vec<u8>> = (0..52u32)
        .map(|i| format!("k{i:03}").into_bytes())
        .collect();
    let refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
    let got = e.multi_get(&refs).unwrap();

    assert_eq!(got.len(), refs.len());
    for (i, slot) in got.iter().enumerate() {
        let individual = e.get(refs[i]).unwrap();
        assert_eq!(*slot, individual, "key {i} mismatch vs get()");
    }
    assert_eq!(got[10], None, "deleted key must be None");
    assert_eq!(got[50], None, "absent key must be None");
    assert_eq!(got[5].as_deref(), Some(&b"v-5"[..]));
}

#[test]
fn scan_sorted_zone() {
    let mut e = engine(Ordering::Sorted);
    for k in ["banana", "apple", "cherry", "date"] {
        e.put(k.as_bytes(), format!("={k}").as_bytes(), 0).unwrap();
    }
    let all = e.scan(None, None).unwrap();
    let keys: Vec<_> = all
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert_eq!(keys, ["apple", "banana", "cherry", "date"]);
    assert_eq!(all[0].1, b"=apple");

    let mid = e.scan(Some(b"banana"), Some(b"date")).unwrap();
    let mid_keys: Vec<_> = mid
        .iter()
        .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
        .collect();
    assert_eq!(mid_keys, ["banana", "cherry"]);
}

#[test]
fn range_estimate_counts_and_midpoint_over_runs() {
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(BS, 8192),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
    );
    for i in 0..2_000u32 {
        e.put(format!("k{i:04}").as_bytes(), b"val", i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert!(e.runs[0].run.index_len() >= 3, "default blocks hold ~700 short keys");

    let (n, mid) = e.range_estimate(None, None);
    assert_eq!(
        n, 2_000,
        "whole-range count is exact when the window is the run"
    );
    let mid = mid.expect("a non-empty range yields a midpoint");
    assert!(
        mid.as_slice() >= b"k0500".as_ref() && mid.as_slice() < b"k1500".as_ref(),
        "midpoint {mid:?} lands in the 25th-75th percentile"
    );

    let (half, mid) = e.range_estimate(Some(b"k1000"), Some(b"k2000"));
    assert!(
        (600..=1_400).contains(&half),
        "sub-range estimate {half} within a block of 1000"
    );
    if let Some(m) = mid {
        assert!(m.as_slice() > b"k1000".as_ref() && m.as_slice() < b"k2000".as_ref());
    }

    assert_eq!(e.range_estimate(Some(b"k0100"), Some(b"k0100")), (0, None));
}

#[test]
fn range_midpoint_matches_range_estimate_midpoint() {
    let mut e = Engine::create_prewiped(
        MemBlockDevice::new(BS, 8192),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
    );
    for i in 0..300u32 {
        e.put(format!("m{i:04}").as_bytes(), b"v", i as u128 + 1).unwrap();
    }
    let bounds: Vec<Option<Vec<u8>>> = [None, Some("a"), Some("k0500"), Some("k1000"), Some("k1999"), Some("m"), Some("m0100"), Some("m0299"), Some("z")]
        .into_iter()
        .map(|b| b.map(|s| s.as_bytes().to_vec()))
        .collect();
    let check = |e: &Engine<MemBlockDevice>| {
        let mut memtable_midpoints = 0;
        for lo in &bounds {
            for hi in &bounds {
                let (lo, hi) = (lo.as_deref(), hi.as_deref());
                let mid = e.range_midpoint(lo, hi);
                assert_eq!(mid, e.range_estimate(lo, hi).1, "lo={lo:?} hi={hi:?}");
                memtable_midpoints += usize::from(mid.as_ref().is_some_and(|m| m.starts_with(b"m")));
            }
        }
        assert!(memtable_midpoints > 0);
    };
    check(&e);
    for i in 0..2_000u32 {
        e.put(format!("k{i:04}").as_bytes(), b"val", i as u128 + 1).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert!(e.runs[0].run.index_len() >= 3);
    for i in 0..300u32 {
        e.put(format!("m{i:04}").as_bytes(), b"v2", i as u128 + 5_000).unwrap();
    }
    check(&e);
}

#[test]
fn range_estimate_run_midpoint_needs_two_blocks() {
    let mut e = engine(Ordering::Sorted);
    for i in 0..200u32 {
        e.put(format!("k{i:04}").as_bytes(), b"val", i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.runs[0].run.index_len(), 1);
    assert_eq!(e.range_estimate(None, None), (200, None), "one block offers no interior fence key");
}

#[test]
fn range_estimate_over_memtable_only() {
    let mut e = engine(Ordering::Sorted);
    for i in 0..50u32 {
        e.put(format!("m{i:04}").as_bytes(), b"v", i as u128 + 1)
            .unwrap();
    }
    assert_eq!(e.run_count(), 0, "still in the memtable");
    let (n, mid) = e.range_estimate(None, None);
    assert_eq!(n, 50, "exact in-range memtable count");
    let mid = mid.expect("memtable fallback midpoint");
    assert!(mid.as_slice() > b"m0000".as_ref() && mid.as_slice() < b"m0049".as_ref());
}

#[test]
fn range_estimate_ignores_disjoint_run_below_lower_bound() {
    let mut e = engine(Ordering::Sorted);
    for i in 0..100u32 {
        e.put(format!("a{i:04}").as_bytes(), b"v", i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert!(e.scan(Some(b"b"), Some(b"c")).unwrap().is_empty());
    assert_eq!(e.range_estimate(Some(b"b"), Some(b"c")), (0, None));
}

#[test]
fn range_estimate_layout_difference_is_not_logical_difference() {
    let opts = RunBuildOpts {
        block_target_bytes: 256,
        ..RunBuildOpts::default()
    };
    let mut mixed = engine(Ordering::Sorted);
    let mut separate = engine(Ordering::Sorted);
    for i in 0..20u32 {
        let key = format!("a{i:03}");
        mixed.put(key.as_bytes(), b"v", i as u128 + 1).unwrap();
        separate.put(key.as_bytes(), b"v", i as u128 + 1).unwrap();
    }
    assert!(run_flush(&mut separate, opts).is_some());
    for i in 0..20u32 {
        let key = format!("c{i:03}-{}", "x".repeat(100));
        let version = i as u128 + 21;
        mixed.put(key.as_bytes(), b"v", version).unwrap();
        separate.put(key.as_bytes(), b"v", version).unwrap();
    }
    assert!(run_flush(&mut mixed, opts).is_some());
    assert!(run_flush(&mut separate, opts).is_some());

    assert_eq!(mixed.scan(None, None).unwrap(), separate.scan(None, None).unwrap());
    assert_eq!(mixed.scan(None, Some(b"b")).unwrap().len(), 20);
    assert_eq!(separate.scan(None, Some(b"b")).unwrap().len(), 20);
    assert_ne!(
        mixed.range_estimate(None, Some(b"b")).0,
        separate.range_estimate(None, Some(b"b")).0
    );
}

#[test]
fn scan_rejects_unsorted() {
    let e = engine(Ordering::Unsorted);
    assert!(matches!(e.scan(None, None), Err(EngineError::NotSorted)));
    assert!(matches!(
        e.scan_bounded(None, None, usize::MAX, usize::MAX),
        Err(EngineError::NotSorted)
    ));
}

#[test]
fn scan_bounded_pages_by_entries_and_bytes() {
    let mut e = engine(Ordering::Sorted);
    for k in ["a", "b", "c", "d"] {
        e.put(k.as_bytes(), format!("={k}").as_bytes(), 0).unwrap();
    }

    let (page, more) = e.scan_bounded(None, None, 3, usize::MAX).unwrap();
    assert_eq!(
        page.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(),
        [b"a", b"b", b"c"]
    );
    assert!(more);
    let (rest, more) = e.scan_bounded(Some(b"c\0"), None, 3, usize::MAX).unwrap();
    assert_eq!(rest, [(b"d".to_vec(), b"=d".to_vec())]);
    assert!(!more);

    let one = e.record_len(b"a").unwrap() as usize + 1;
    let (page, more) = e.scan_bounded(None, None, usize::MAX, one).unwrap();
    assert_eq!(page, [(b"a".to_vec(), b"=a".to_vec())]);
    assert!(more);
    let (page, more) = e.scan_bounded(None, None, usize::MAX, 1).unwrap();
    assert_eq!(page.len(), 1);
    assert!(more);

    let (page, more) = e.scan_bounded(Some(b"x"), None, 3, usize::MAX).unwrap();
    assert!(page.is_empty());
    assert!(!more);
}

#[test]
fn record_len_reports_live_records_only() {
    let mut e = engine(Ordering::Sorted);
    e.put(b"k", b"value", 0).unwrap();
    let len = e.record_len(b"k").unwrap();
    assert_eq!(len as usize, HEADER_LEN + 1 + 5);
    assert_eq!(e.record_len(b"missing"), None);
    e.delete(b"k", 0).unwrap();
    assert_eq!(e.record_len(b"k"), None);
}

#[test]
fn mutable_and_replay_keep_version_argmax() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 256), cfg);
    e.put(b"k", b"winner", 10).unwrap();
    e.put(b"k", b"loser", 5).unwrap();
    assert_eq!(e.version(b"k").unwrap(), Some(10));
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"winner"[..]));
    assert_eq!(
        e.scan_bounded_versioned(None, None, usize::MAX, usize::MAX)
            .unwrap()
            .0,
        vec![(b"k".to_vec(), b"winner".to_vec(), 10)]
    );
    e.sync().unwrap();
    let dev = e.into_device();
    let reopened = Engine::open(dev, cfg).unwrap();
    assert_eq!(reopened.version(b"k").unwrap(), Some(10));
    assert_eq!(
        reopened.get(b"k").unwrap().as_deref(),
        Some(&b"winner"[..])
    );
}

#[test]
fn scan_span_reads_coalesce_and_roundtrip() {
    let dev = MemBlockDevice::new(BS, 1 << 16);
    let cfg = Config {
        seg_blocks: 64,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let n = 500usize;
    for i in 0..n {
        let k = format!("k{i:05}");
        e.put(k.as_bytes(), format!("v{i}-old").as_bytes(), 1)
            .unwrap();
    }
    for i in (0..n).step_by(3).rev() {
        let k = format!("k{i:05}");
        e.put(k.as_bytes(), format!("v{i}-new").as_bytes(), 2)
            .unwrap();
    }
    e.sync().unwrap();

    let (keys, plan, _more) = e.scan_plan(None, None, usize::MAX, usize::MAX).unwrap();
    assert_eq!(keys.len(), n);
    assert!(
        plan.num_spans() * 4 < plan.num_records(),
        "expected heavy span coalescing: {} spans for {} records",
        plan.num_spans(),
        plan.num_records()
    );

    let mut got = Vec::new();
    let mut lo: Option<Vec<u8>> = None;
    loop {
        let (page, more) = e.scan_bounded(lo.as_deref(), None, 37, usize::MAX).unwrap();
        got.extend(page);
        if !more {
            break;
        }
        let mut next = got.last().unwrap().0.clone();
        next.push(0);
        lo = Some(next);
    }
    assert_eq!(got.len(), n);
    for (i, (k, v)) in got.iter().enumerate() {
        assert_eq!(k, format!("k{i:05}").as_bytes());
        let want = if i % 3 == 0 {
            format!("v{i}-new")
        } else {
            format!("v{i}-old")
        };
        assert_eq!(v, want.as_bytes(), "value for key {i}");
    }
}

#[test]
fn paged_scan_refills_the_memtable_cursor_past_the_chunk_budget() {
    let dev = MemBlockDevice::new(BS, 1 << 17);
    let cfg = Config {
        seg_blocks: 64,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let n = 10_000usize;
    for i in (0..n).step_by(2) {
        let k = format!("k{i:05}");
        e.put(k.as_bytes(), b"run", 1).unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap().unwrap();
    for i in 0..n {
        let k = format!("k{i:05}");
        e.put(k.as_bytes(), b"mem", 2).unwrap();
    }
    e.sync().unwrap();

    let mut got = 0usize;
    let mut lo: Option<Vec<u8>> = None;
    loop {
        let (page, more) = e
            .scan_bounded(lo.as_deref(), None, 1000, usize::MAX)
            .unwrap();
        for (i, (k, v)) in page.iter().enumerate() {
            assert_eq!(k, format!("k{:05}", got + i).as_bytes());
            assert_eq!(v, b"mem", "memtable v2 must win for {}", got + i);
        }
        got += page.len();
        if !more {
            break;
        }
        let mut next = page.last().unwrap().0.clone();
        next.push(0);
        lo = Some(next);
    }
    assert_eq!(got, n, "paged scan returned every key exactly once");
}

#[test]
fn scan_over_runs_pages_and_is_bounded() {
    let dev = CountingDevice {
        inner: MemBlockDevice::new(BS, 8192),
        reads: AtomicU64::new(0),
    };
    let cfg = Config {
        seg_blocks: 16,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    let n = 400u32;
    for i in 0..n {
        e.put(
            format!("k{i:04}").as_bytes(),
            format!("v{i:04}").as_bytes(),
            i as u128,
        )
        .unwrap();
    }
    e.flush(
        RunBuildOpts {
            block_target_bytes: 256,
            bloom_bits_per_key: 10,
            num_buckets: 16,
        },
    )
    .unwrap();

    let full = e.scan(None, None).unwrap();
    assert_eq!(full.len(), n as usize);

    e.device().reads.store(0, AtomicOrdering::Relaxed);
    let (page, more) = e.scan_bounded(None, None, 10, usize::MAX).unwrap();
    assert_eq!(page.len(), 10);
    assert!(more);
    let reads = e.device().reads.load(AtomicOrdering::Relaxed);
    assert!(reads < 40, "page should be O(page), did {reads} reads");

    let mut got = page.clone();
    let mut cursor = page.last().unwrap().0.clone();
    loop {
        let mut next = cursor.clone();
        next.push(0);
        let (pg, more) = e.scan_bounded(Some(&next), None, 10, usize::MAX).unwrap();
        if pg.is_empty() {
            assert!(!more);
            break;
        }
        cursor = pg.last().unwrap().0.clone();
        let last = !more;
        got.extend(pg);
        if last {
            break;
        }
    }
    assert_eq!(got, full, "the union of all pages must equal a full scan");
}

#[test]
fn scan_over_runs_honors_exclusive_upper_bound() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    for k in [b"a", b"c", b"e", b"g"] {
        e.put(k, b"run", 1).unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap();
    for k in [b"b", b"d", b"f"] {
        e.put(k, b"mem", 1).unwrap();
    }

    let page = e.scan(Some(b"c"), Some(b"f")).unwrap();
    let keys: Vec<_> = page.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]);
    assert_eq!(page[0].1, b"run");
    assert_eq!(page[1].1, b"mem");
}

#[test]
fn scan_picks_higher_version_across_two_runs() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    e.put(b"k", b"hi-ver", 9).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"k", b"lo-ver", 2).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();

    let all = e.scan(None, None).unwrap();
    assert_eq!(all, vec![(b"k".to_vec(), b"hi-ver".to_vec())]);
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"hi-ver"[..]));
}

#[test]
fn scan_and_get_agree_on_run_version_tie() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    e.put(b"k", b"older-run", 7).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"k", b"newer-run", 7).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();

    let scanned = e.scan(None, None).unwrap();
    let got = e.get(b"k").unwrap();
    assert_eq!(
        scanned,
        vec![(b"k".to_vec(), got.clone().unwrap())],
        "scan must agree with get"
    );
    assert_eq!(
        got.as_deref(),
        Some(&b"newer-run"[..]),
        "newest run wins the tie"
    );
}

#[test]
fn run_read_failure_is_error_never_false_absent() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mem = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        ReadFailDevice {
            inner: mem,
            armed: AtomicBool::new(false),
        },
        cfg,
    );

    e.put(b"k", b"v", 7).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.sync().unwrap();
    assert_eq!(
        e.len(),
        0,
        "memtable empty after flush — key is run-resident"
    );
    assert!(
        e.run_count() >= 1,
        "key must live in a run for the read path to be a device read"
    );
    assert_eq!(e.version(b"k").unwrap(), Some(7));
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));

    e.device().armed.store(true, AtomicOrdering::Relaxed);

    match e.version(b"k") {
        Err(_) => {}
        Ok(other) => panic!("version() on a run-read failure must be Err, got Ok({other:?})"),
    }
    match e.get(b"k") {
        Err(_) => {}
        Ok(other) => panic!("get() on a run-read failure must be Err, got Ok({other:?})"),
    }
}

#[test]
fn version_digest_page_filters_to_want_buckets() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };

    for k in [b"a" as &[u8], b"b", b"c", b"d"] {
        e.put(k, b"x", 1).unwrap();
    }
    e.flush(opts).unwrap();
    for k in [b"e" as &[u8], b"f", b"g", b"h"] {
        e.put(k, b"x", 1).unwrap();
    }

    let want0: std::collections::HashSet<u32> = [0].into_iter().collect();
    let (got, _) = page(&e, &want0, None, usize::MAX);
    let keys: Vec<&[u8]> = got.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"d" as &[u8], b"h"]);
    for (k, _) in &got {
        assert_eq!(vdp_bucket(k), 0);
    }

    let want13: std::collections::HashSet<u32> = [1, 3].into_iter().collect();
    let (got, _) = page(&e, &want13, None, usize::MAX);
    let keys: Vec<&[u8]> = got.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a" as &[u8], b"c", b"e", b"g"]);

    let (got, more) = page(&e, &std::collections::HashSet::new(), None, usize::MAX);
    assert!(got.is_empty() && !more);
}

#[test]
fn version_digest_page_after_resume_is_exclusive_and_gapless() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };

    for k in [b"a" as &[u8], b"b", b"c", b"d", b"e"] {
        e.put(k, b"x", 1).unwrap();
    }
    e.flush(opts).unwrap();
    for k in [b"f" as &[u8], b"g", b"h", b"i", b"j"] {
        e.put(k, b"x", 1).unwrap();
    }

    let want = all_buckets();
    let (full, _) = page(&e, &want, None, usize::MAX);

    let (tail, _) = page(&e, &want, Some(b"e"), usize::MAX);
    assert_eq!(tail, full[5..].to_vec());
    assert!(tail.iter().all(|(k, _)| k.as_slice() > b"e" as &[u8]));
    let (tail2, _) = page(&e, &want, Some(b"e0"), usize::MAX);
    assert_eq!(tail2, full[5..].to_vec());
}

#[test]
#[should_panic(expected = "different `after` than it was planned for")]
fn version_digest_page_plan_rejects_a_different_after_at_execution() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };
    for k in [b"a" as &[u8], b"b", b"c", b"d", b"e"] {
        e.put(k, b"x", 1).unwrap();
    }
    e.flush(opts).unwrap();
    let want = all_buckets();
    let plan = e
        .plan_version_digest_page(&want, vdp_bucket, Some(b"c"), usize::MAX, 0)
        .unwrap();
    let _ = vals_rt::block_on(
        Engine::version_digest_page_from_plan(plan, &want, vdp_bucket, Some(b"a"), usize::MAX, 0, e.device()),
        || {},
    );
}

#[test]
fn version_digest_page_byte_budget_pages_with_first_always_emitted() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };

    for k in [b"a" as &[u8], b"b", b"c", b"d"] {
        e.put(k, b"x", 1).unwrap();
    }
    e.flush(opts).unwrap();
    for k in [b"e" as &[u8], b"f", b"g", b"h"] {
        e.put(k, b"x", 1).unwrap();
    }

    let want = all_buckets();
    let (full, _) = e
        .version_digest_page(&want, vdp_bucket, None, usize::MAX, 0)
        .unwrap();
    assert_eq!(full.len(), 8);

    let mut acc: Vec<(Vec<u8>, u128)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let (pg, more) = e
            .version_digest_page(&want, vdp_bucket, after.as_deref(), 3, 0)
            .unwrap();
        assert!(!pg.is_empty(), "a page always makes progress");
        assert!(pg.len() <= 3);
        after = Some(pg.last().unwrap().0.clone());
        let last = !more;
        acc.extend(pg);
        if last {
            break;
        }
    }
    assert_eq!(acc, full, "paged result must equal the unbounded result");

    let (pg, more) = e
        .version_digest_page(&want, vdp_bucket, None, 0, 0)
        .unwrap();
    assert_eq!(pg.len(), 1);
    assert!(more);
}

#[test]
fn version_digest_page_argmax_across_two_runs_for_same_key() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };

    e.put(b"k", b"x", 3).unwrap();
    e.flush(opts).unwrap();
    e.put(b"k", b"x", 7).unwrap();
    e.flush(opts).unwrap();
    assert_eq!(e.len(), 0);

    let (got, _) = page(&e, &all_buckets(), None, usize::MAX);
    assert_eq!(got, vec![(b"k".to_vec(), 7)], "newer run's version wins");
}

#[test]
fn staged_probe_reads_all_candidate_runs() {
    let (e, reads) = two_run_counting_engine();
    let before = crate::attribution::lookup_snapshot();
    let (key, probes) = match e.probe_get(b"k") {
        StagedGet::Runs { key, probes, .. } => (key, probes),
        _ => panic!("expected a run probe for a flushed key"),
    };
    assert_eq!(
        probes.len(),
        2,
        "k must be a candidate in BOTH runs, else the early exit isn't exercised"
    );
    reads.store(0, AtomicOrdering::Relaxed);
    let entry = vals_rt::block_on(resolve_run_probe(&key, &probes, e.device()), || {})
        .unwrap()
        .expect("k is present");
    assert_eq!(entry.version, 7, "the newest run's copy is the winner");
    assert_eq!(
        reads.load(AtomicOrdering::Relaxed),
        2,
        "every candidate block is inspected before choosing by version"
    );

    let probe = e.probe_version(b"k");
    reads.store(0, AtomicOrdering::Relaxed);
    let v = vals_rt::block_on(Engine::version_from_probe(probe, e.device()), || {}).unwrap();
    assert_eq!(v, Some(7));
    assert_eq!(reads.load(AtomicOrdering::Relaxed), 2);

    let probe = e.probe_version(b"k");
    let (v, blocks) =
        vals_rt::block_on(Engine::version_from_probe_counted(probe, e.device()), || {})
            .unwrap();
    assert_eq!(v, Some(7));
    assert_eq!(blocks, 2, "the counted probe reports every candidate read");
    let after = crate::attribution::lookup_snapshot();
    assert_eq!(after[0][0] - before[0][0], 1);
    assert_eq!(after[0][1] - before[0][1], 2);
    assert_eq!(after[0][2] - before[0][2], 2);
    assert_eq!(after[3][0] - before[3][0], 2);
    assert_eq!(after[3][1] - before[3][1], 4);
    assert_eq!(after[3][2] - before[3][2], 4);
    assert_eq!(after[3][3] + after[3][4] - before[3][3] - before[3][4], 4);
}

#[test]
fn staged_probe_bloom_fp_falls_through_to_older_hit() {
    let reads = std::sync::Arc::new(AtomicU64::new(0));
    let dev = AsyncCountingDevice {
        inner: MemBlockDevice::new(BS, 4096),
        reads: reads.clone(),
        read_bytes: std::sync::Arc::new(AtomicU64::new(0)),
    };
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put(b"k", b"hit", 2).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"other", 9).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();

    let miss = e.runs[1]
        .run
        .locate_block(&e.alloc, b"a")
        .expect("run 1's block");
    let hit = e.runs[0]
        .run
        .locate_block(&e.alloc, b"k")
        .expect("run 0's block");
    let (miss2, hit2) = (miss.clone(), hit.clone());
    reads.store(0, AtomicOrdering::Relaxed);
    let entry = vals_rt::block_on(resolve_run_probe(b"k", &[miss, hit], e.device()), || {})
        .unwrap()
        .expect("the older run's copy must still be found");
    assert_eq!(entry.version, 2, "the fallthrough hit is returned");
    assert_eq!(
        reads.load(AtomicOrdering::Relaxed),
        2,
        "the FP block is read, then the scan continues to the real hit"
    );

    let (entry, blocks) = vals_rt::block_on(
        resolve_run_probe_counted(b"k", &[miss2, hit2], e.device()),
        || {},
    )
    .unwrap();
    assert_eq!(entry.expect("still found").version, 2);
    assert_eq!(blocks, 2, "the counted probe reports both block reads");
}

#[test]
fn value_pointers_outside_their_segment_are_refused_instead_of_panicking() {
    let mut e = engine(Ordering::Unsorted);
    e.put(b"k", b"v", 0).unwrap();
    let good = e.index.get(b"k").unwrap().ptr;
    let seg_bytes = (e.alloc.blocks_per_segment() * BS as u64) as u32;
    for ptr in [
        ValuePtr { segment: SegmentId(u64::MAX), ..good },
        ValuePtr { segment: SegmentId(e.alloc.num_segments()), ..good },
        ValuePtr { len: 0, ..good },
        ValuePtr { offset: seg_bytes, ..good },
    ] {
        assert!(
            matches!(
                e.plan_get_ptr(&ptr),
                Err(EngineError::Log(LogError::Corrupt("value pointer outside its segment")))
            ),
            "{ptr:?}"
        );
    }
    assert_eq!(e.plan_get_ptr(&good).unwrap().ptr(), good);
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}
