use super::*;

fn cfg() -> Config {
    Config { seg_blocks: 32, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn fresh() -> Engine<MemBlockDevice> {
    Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 16), cfg())
}

fn ver(n: u64) -> u128 {
    (n as u128) << 64 | 7
}

fn small_blocks() -> RunBuildOpts {
    RunBuildOpts { block_target_bytes: 256, ..RunBuildOpts::default() }
}

fn ascending(e: &Engine<MemBlockDevice>, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Vec<(Vec<u8>, IndexEntry)> {
    let (mut want, more) = e.scan_winners_page(lo, hi, usize::MAX, usize::MAX).unwrap();
    assert!(!more);
    want.reverse();
    want
}

fn check_pages(e: &Engine<MemBlockDevice>, lo: Option<&[u8]>, hi: Option<&[u8]>, entries: usize, bytes: usize) {
    let want = ascending(e, lo, hi);
    let mut at = 0;
    let mut cursor = hi.map(<[u8]>::to_vec);
    loop {
        let (page, more) = e.scan_winners_page_rev(lo, cursor.as_deref(), entries, bytes).unwrap();
        let staged = scan_rev_staged::page(e, lo, cursor.as_deref(), entries, bytes, scan_rev_staged::UNLIMITED);
        assert_eq!((&staged.winners, staged.more), (&page, more), "staged/sync reverse equivalence");
        assert!(staged.resume.is_none());
        let mut n = 0;
        let mut used = 0usize;
        for (key, entry) in &want[at..] {
            let sz = key.len() + entry.ptr.len as usize;
            if n >= entries || (n > 0 && used.saturating_add(sz) > bytes) { break; }
            n += 1;
            used = used.saturating_add(sz);
        }
        assert_eq!(page, want[at..at + n], "window {lo:?}..{hi:?}, entries {entries}, bytes {bytes}, offset {at}");
        at += n;
        assert_eq!(more, at < want.len(), "more at offset {at}, entries {entries}, bytes {bytes}");
        if !more || entries == 0 { break; }
        cursor = Some(page.last().expect("nonzero entry budget makes progress").0.clone());
    }
}

#[test]
fn reverse_pages_match_ascending_for_mixed_runs_memtable_and_tombstones() {
    for seed in [1u64, 42, 0x12345678] {
        let mut rng = seed;
        let mut next = || {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            rng
        };
        let mut e = fresh();
        let mut keys = vec![vec![], vec![0], vec![0, 0], vec![0, 255], vec![255], vec![255, 0], vec![255, 255]];
        keys.extend((0..80).map(|i| format!("prefix/{i:03}").into_bytes()));
        for (i, key) in keys.iter().enumerate() {
            e.put(key, &vec![i as u8; i % 35], ver(1)).unwrap();
        }
        for cap in 0..=keys.len() + 1 {
            check_pages(&e, None, None, cap, usize::MAX);
        }
        run::with_run_write_version(run::RUN_SIDECAR_VERSION, || e.flush(small_blocks()).unwrap());
        for round in 0..3 {
            for _ in 0..90 {
                let key = &keys[next() as usize % keys.len()];
                let v = ver(2 + next() % 15);
                let tombstone = next() % 5 == 0;
                e.put_kind(key, &vec![round; (next() % 50) as usize], v, tombstone).unwrap();
            }
            if round < 2 { e.flush(small_blocks()).unwrap(); }
        }
        assert!(e.runs.len() >= 3 && !e.index.is_empty());
        e.apply_range_delete(Some(b"prefix/011"), Some(b"prefix/063"), ver(8)).unwrap();
        e.apply_range_delete(Some(b"prefix/035"), Some(b"prefix/040"), ver(12)).unwrap();
        e.apply_range_delete(None, Some(&[0, 255]), ver(4)).unwrap();
        e.apply_range_delete(Some(&[255, 0]), None, ver(7)).unwrap();
        e.put_kind(b"prefix/079", b"", ver(30), true).unwrap();
        e.put(b"prefix/020", b"newer than cover", ver(31)).unwrap();
        let want = ascending(&e, None, None);
        assert!(want.iter().any(|(k, ie)| k == b"prefix/079" && ie.tombstone));
        assert!(want.iter().any(|(k, ie)| k == b"prefix/020" && ie.version == ver(31)));
        for build in [false, true] {
            if build { assert!(e.build_range_tombstone_max_trees() > 0); }
            for cap in 0..=want.len() + 1 {
                check_pages(&e, None, None, cap, usize::MAX);
            }
            for cap in 0..=200 {
                check_pages(&e, None, None, usize::MAX, cap);
            }
            for (lo, hi) in [
                (None, Some(b"".as_slice())),
                (Some(b"".as_slice()), Some(b"\0\0".as_slice())),
                (Some(b"prefix/010".as_slice()), Some(b"prefix/070".as_slice())),
                (Some(b"prefix/035".as_slice()), Some(b"prefix/040".as_slice())),
                (Some(b"prefix/020\0".as_slice()), Some(b"prefix/021\0".as_slice())),
                (Some(b"prefix/079".as_slice()), Some(b"prefix/079".as_slice())),
                (Some(b"z".as_slice()), Some(b"a".as_slice())),
                (Some(b"\xff\xff\xff".as_slice()), None),
                (None, Some(b"prefix/050".as_slice())),
            ] {
                for cap in [0, 1, 3, 17, usize::MAX] {
                    for bytes in [0, 63, usize::MAX] {
                        check_pages(&e, lo, hi, cap, bytes);
                    }
                }
            }
        }
        e.sync().unwrap();
        let dev = e.device().share();
        drop(e);
        let e = Engine::open(dev, cfg()).unwrap();
        check_pages(&e, None, None, 3, 100);
    }
}

#[test]
fn reverse_memtable_refill_crosses_4096_with_and_without_runs() {
    for runs in [false, true] {
        let mut e = fresh();
        if runs {
            e.put(b"k05000", b"old", ver(1)).unwrap();
            e.flush(small_blocks()).unwrap();
        }
        for i in 0..9000 {
            e.put(format!("k{i:05}").as_bytes(), b"value", ver(3)).unwrap();
        }
        e.apply_range_delete(Some(b"k02000"), Some(b"k03000"), ver(4)).unwrap();
        for cap in [1, 4095, 4096, 4097, 8193, usize::MAX] {
            check_pages(&e, None, None, cap, usize::MAX);
        }
    }
}

#[test]
fn reverse_run_cursors_seek_at_every_fence_and_stay_exhausted() {
    let mut e = fresh();
    for i in 0..160 {
        e.put(format!("k{i:05}").as_bytes(), b"value", ver(1)).unwrap();
    }
    e.flush(small_blocks()).unwrap();
    let r = &e.runs[0].run;
    assert!(r.index_len() > 20);
    let mut bounds: Vec<Option<Vec<u8>>> = vec![None, Some(vec![]), Some(b"z".to_vec())];
    for i in 0..r.index_len() {
        let k = r.block_first_key(i).to_vec();
        let mut after = k.clone();
        after.push(0);
        bounds.extend([Some(k), Some(after)]);
    }
    for hi in &bounds {
        let mut cursor = r.block_scan_rev(None, hi.as_deref());
        let mut reads = 0;
        let first = cursor.next_picking(&e.alloc, |end| { reads += 1; end }).unwrap();
        assert_eq!(reads, usize::from(first.is_some()), "the upper fence seeks directly to the first result: {hi:?}");
    }
    for lo in &bounds {
        for hi in &bounds {
            let mut forward = r.block_scan(lo.as_deref(), hi.as_deref());
            let mut want = Vec::new();
            while let Some(entry) = forward.next(&e.alloc).unwrap() { want.push(entry); }
            want.reverse();
            let mut reverse = r.block_scan_rev(lo.as_deref(), hi.as_deref());
            let mut got = Vec::new();
            while let Some(entry) = reverse.next(&e.alloc).unwrap() { got.push(entry); }
            assert_eq!(got, want, "{lo:?}..{hi:?}");
            assert_eq!(reverse.next(&e.alloc).unwrap(), None);
        }
    }
}

#[test]
fn reverse_covered_blocks_are_skipped_without_losing_boundary_or_same_hlc_winners() {
    let mut e = fresh();
    for i in 0..1600 {
        let version = if i == 700 { ver(5) + 1 } else { ver(1) };
        e.put(format!("k{i:05}").as_bytes(), b"value", version).unwrap();
    }
    e.flush(small_blocks()).unwrap();
    let run = &e.runs[0].run;
    let n = run.index_len();
    assert!(n > 100);
    let mut lo = run.block_first_key(5).to_vec();
    let mut hi = run.block_first_key(n - 6).to_vec();
    lo.push(0);
    hi.push(0);
    e.apply_range_delete(Some(&lo), Some(&hi), ver(5)).unwrap();
    let before = e.scan_winners_page_rev_counted(None, None, usize::MAX, usize::MAX).unwrap();
    assert_eq!(before.1.skipped_blocks, 0);
    assert!(e.build_range_tombstone_max_trees() > 0);
    let after = e.scan_winners_page_rev_counted(None, None, usize::MAX, usize::MAX).unwrap();
    assert_eq!(after.0, before.0);
    assert!(after.0.0.iter().any(|(k, v)| k == b"k00700" && v.version == ver(5) + 1));
    assert!(after.1.skipped_blocks > n as u64 - 20, "{:?}", after.1);
    assert!(after.1.block_reads < 20, "{:?}", after.1);
    check_pages(&e, None, None, 3, usize::MAX);
    check_pages(&e, Some(&lo), Some(&hi), 1, usize::MAX);
    e.apply_range_delete(None, None, ver(10)).unwrap();
    for (lo, hi) in [(None, None), (Some(b"k00400".as_slice()), Some(b"k00900".as_slice()))] {
        let (page, stats) = e.scan_winners_page_rev_counted(lo, hi, 1, usize::MAX).unwrap();
        assert_eq!(page, (vec![], false));
        assert_eq!(stats.block_reads, 0);
        assert!(stats.index_probes < 100, "skip uses the tree, not a linear block walk: {stats:?}");
    }
}

#[test]
fn reverse_empty_and_unsorted_engines() {
    let e = fresh();
    for entries in [0, 1, usize::MAX] {
        check_pages(&e, None, None, entries, 0);
    }
    let e = engine(Ordering::Unsorted);
    assert!(matches!(e.scan_winners_page_rev(None, None, 1, 0), Err(EngineError::NotSorted)));
}
