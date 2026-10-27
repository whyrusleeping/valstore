use super::*;

#[test]
fn server_scan_meter_counts_prime_and_refill_but_not_maintenance() {
    use crate::attribution::{SCAN_BLOCKS, SCAN_BYTES, SCAN_CURSORS};
    let e = engine_with_many_block_run(200);
    let before = (SCAN_CURSORS.get(), SCAN_BLOCKS.get(), SCAN_BYTES.get());
    staged_winners_counting_block_refills(&e, None, None, usize::MAX, usize::MAX);
    assert_eq!(before, (SCAN_CURSORS.get(), SCAN_BLOCKS.get(), SCAN_BYTES.get()));
    let StagedScanPage::Runs(mut merge) = e.stage_scan_page(None, None, usize::MAX, usize::MAX).unwrap() else { panic!() };
    merge.meter_server_scan();
    merge.meter_server_scan();
    vals_rt::block_on(async {
        loop {
            match merge.step(e.device()).await.unwrap() {
                StagedScanStep::Done => break,
                StagedScanStep::NeedMem { from } => e.scan_supply_mem(&mut merge, &from).unwrap(),
                StagedScanStep::NeedBlocks { cursor } => e.scan_supply_blocks(&mut merge, cursor).unwrap(),
            }
        }
    }, || {});
    assert_eq!(merge.finish().winners.len(), 200);
    assert_eq!(SCAN_CURSORS.get() - before.0, 1);
    assert_eq!(SCAN_BLOCKS.get() - before.1, 200);
    let bytes = SCAN_BYTES.get() - before.2;
    assert!(bytes >= 200 * BS as u64 && bytes < 400 * BS as u64);
    assert_eq!(bytes % BS as u64, 0);
}

#[test]
fn staged_scan_matches_sync_across_bounds_and_budgets() {
    let e = engine_with_runs_and_memtable();
    assert!(e.run_count() >= 3, "need multiple runs");

    #[allow(clippy::type_complexity)]
    let cases: &[(Option<&[u8]>, Option<&[u8]>, usize, usize)] = &[
        (None, None, usize::MAX, usize::MAX),
        (None, None, 1, usize::MAX),
        (None, None, 7, usize::MAX),
        (None, None, 10, 30),
        (None, None, usize::MAX, 1),
        (Some(b"k0050"), Some(b"k0110"), usize::MAX, usize::MAX),
        (Some(b"k0050"), Some(b"k0110"), 5, usize::MAX),
        (Some(b"k0000"), Some(b"k0001"), usize::MAX, usize::MAX),
        (Some(b"k0159"), None, usize::MAX, usize::MAX),
        (Some(b"k9999"), None, usize::MAX, usize::MAX),
        (Some(b"k0075"), Some(b"k0085"), usize::MAX, usize::MAX),
    ];
    for &(lo, hi, me, mb) in cases {
        assert_staged_eq_sync(&e, lo, hi, me, mb);
    }
}

#[test]
fn staged_scan_full_paged_walk_equals_full_scan() {
    let e = engine_with_runs_and_memtable();
    let (full, _) = e
        .scan_winners_page(None, None, usize::MAX, usize::MAX)
        .unwrap();

    let mut got: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
    let mut lo: Option<Vec<u8>> = None;
    loop {
        let (page, more) = staged_winners(&e, lo.as_deref(), None, 6, usize::MAX).unwrap();
        if page.is_empty() {
            assert!(!more);
            break;
        }
        let mut next = page.last().unwrap().0.clone();
        next.push(0);
        lo = Some(next);
        let last = !more;
        got.extend(page);
        if last {
            break;
        }
    }
    assert_eq!(got.len(), full.len(), "paged walk length");
    for (a, b) in full.iter().zip(&got) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1.version, b.1.version);
        assert_eq!(a.1.ptr, b.1.ptr);
    }
}

#[test]
fn staged_scan_forces_multiple_mem_refills() {
    let dev = MemBlockDevice::new(BS, 8192);
    let cfg = Config {
        seg_blocks: 16,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        block_target_bytes: 256,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    for i in (1..200u32).step_by(2) {
        let k = format!("k{i:04}");
        e.put(k.as_bytes(), format!("r-{i:04}").as_bytes(), i as u128)
            .unwrap();
    }
    run_flush(&mut e, opts);
    for i in (0..200u32).step_by(2) {
        let k = format!("k{i:04}");
        e.put(
            k.as_bytes(),
            format!("mt-{i:04}").as_bytes(),
            1000 + i as u128,
        )
        .unwrap();
    }
    assert_staged_eq_sync(&e, None, None, 3, usize::MAX);
    let (full, _) = e
        .scan_winners_page(None, None, usize::MAX, usize::MAX)
        .unwrap();
    let mut got: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
    let mut lo: Option<Vec<u8>> = None;
    loop {
        let (page, more) = staged_winners(&e, lo.as_deref(), None, 3, usize::MAX).unwrap();
        if page.is_empty() {
            break;
        }
        let mut next = page.last().unwrap().0.clone();
        next.push(0);
        lo = Some(next);
        let last = !more;
        got.extend(page);
        if last {
            break;
        }
    }
    assert_eq!(got.len(), full.len());
    for (a, b) in full.iter().zip(&got) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1.version, b.1.version);
    }
}

#[test]
fn staged_scan_memtable_only_fast_path_is_done() {
    let mut e = engine(Ordering::Sorted);
    for i in 0..20u32 {
        e.put(format!("k{i:02}").as_bytes(), b"v", i as u128)
            .unwrap();
    }
    assert_eq!(e.run_count(), 0);
    match e
        .stage_scan_page(None, None, usize::MAX, usize::MAX)
        .unwrap()
    {
        StagedScanPage::Done(_) => {}
        StagedScanPage::Runs(_) => panic!("memtable-only must be Done"),
    }
    assert_staged_eq_sync(&e, None, None, usize::MAX, usize::MAX);
    assert_staged_eq_sync(&e, Some(b"k05"), Some(b"k12"), 4, usize::MAX);
}

#[test]
fn sample_block_key_draws_stored_block_first_keys_across_runs() {
    let mut e = engine(Ordering::Sorted);
    e.put(b"k00", b"v", 1).unwrap();
    assert_eq!(e.sample_block_key(7), None, "memtable-only has no run blocks");
    let e = engine_with_runs_and_memtable();
    let (all, _) = e.scan_winners_page(None, None, usize::MAX, usize::MAX).unwrap();
    let stored: std::collections::BTreeSet<Vec<u8>> = all.into_iter().map(|(k, _)| k).collect();
    let picks: std::collections::BTreeSet<Vec<u8>> =
        (0..64).filter_map(|p| e.sample_block_key(p)).collect();
    assert!(picks.iter().all(|k| stored.contains(k)), "{picks:?}");
    assert!(picks.len() > 4, "draws spread over many blocks: {picks:?}");
    assert!(picks.iter().any(|k| k.as_slice() >= b"k0080".as_slice()), "the later run is drawn too: {picks:?}");
}

#[test]
fn staged_scan_generation_guard_detects_runset_change() {
    let mut e = engine_with_runs_and_memtable();
    let staged = e.stage_scan_page(None, None, 5, usize::MAX).unwrap();
    let merge = match staged {
        StagedScanPage::Runs(m) => m,
        StagedScanPage::Done(_) => panic!("expected runs"),
    };
    let captured = merge.generation();
    assert_eq!(
        captured,
        e.storage_generation(),
        "generation captured at stage"
    );
    e.put(b"k0300", b"newer", 9999).unwrap();
    run_flush(&mut e, RunBuildOpts::default());
    assert_ne!(
        merge.generation(),
        e.storage_generation(),
        "stale generation must be detected"
    );
}

#[test]
fn staged_scan_forces_multiple_block_window_refills() {
    let e = engine_with_many_block_run(4000);
    let (staged, more, refills) =
        staged_winners_counting_block_refills(&e, None, None, usize::MAX, usize::MAX);
    assert!(
        refills >= 2,
        "expected multiple block-window refills, got {refills}"
    );
    let (sync, sync_more) = e
        .scan_winners_page(None, None, usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(more, sync_more);
    assert_eq!(staged.len(), sync.len(), "windowed page length");
    for (a, b) in sync.iter().zip(&staged) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1.version, b.1.version);
        assert_eq!(a.1.ptr, b.1.ptr);
    }
    let (staged2, more2, _) = staged_winners_counting_block_refills(
        &e,
        Some(b"k01234"),
        Some(b"k03456"),
        usize::MAX,
        usize::MAX,
    );
    let (sync2, sync_more2) = e
        .scan_winners_page(Some(b"k01234"), Some(b"k03456"), usize::MAX, usize::MAX)
        .unwrap();
    assert_eq!(more2, sync_more2);
    assert_eq!(staged2.len(), sync2.len());
    for (a, b) in sync2.iter().zip(&staged2) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1.version, b.1.version);
    }
}

#[test]
fn staged_scan_generation_guard_detects_change_at_block_refill() {
    let mut e = engine_with_many_block_run(4000);
    let mut merge = match e
        .stage_scan_page(None, None, usize::MAX, usize::MAX)
        .unwrap()
    {
        StagedScanPage::Runs(m) => m,
        StagedScanPage::Done(_) => panic!("expected runs"),
    };
    let captured = merge.generation();
    let dev = e.device_handle();
    let reached_block_refill = loop {
        match vals_rt::block_on(merge.step(dev.as_ref()), || {}).unwrap() {
            StagedScanStep::NeedBlocks { .. } => break true,
            StagedScanStep::NeedMem { from } => {
                e.scan_supply_mem(&mut merge, &from).unwrap();
            }
            StagedScanStep::Done => break false,
        }
    };
    assert!(
        reached_block_refill,
        "a wide many-block page must hit NeedBlocks"
    );
    assert_eq!(
        captured,
        e.storage_generation(),
        "generation stable until mutation"
    );
    e.put(b"k99999", b"newer", 9_999_999).unwrap();
    run_flush(&mut e, RunBuildOpts::default());
    assert_ne!(
        merge.generation(),
        e.storage_generation(),
        "run-set change at a block refill must be detected"
    );
}


fn prefetched_count_page<D: BlockDevice + AsyncRead + 'static>(
    e: &Engine<D>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    max: usize,
    window: usize,
) -> (usize, Option<Vec<u8>>, bool) {
    let plan = e.winner_count_prefetch_plan(lo, hi, window).unwrap();
    assert_eq!(plan.storage_generation(), e.storage_generation());
    let blocks = vals_rt::block_on(plan.read(e.device()), || {});
    e.winner_count_page_prefetched(lo, hi, max, None, blocks)
        .unwrap()
        .expect("an unchanged engine accepts its own prefetch")
}

fn assert_prefetched_count_matches_sync<D: BlockDevice + AsyncRead + 'static>(
    e: &Engine<D>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    max: usize,
    window: usize,
    expected: usize,
) -> usize {
    let ctx = format!("lo={lo:?} hi={hi:?} max={max} window={window}");
    let mut from = lo.map(<[u8]>::to_vec);
    let mut total = 0usize;
    let mut pages = 0usize;
    let mut horizon_stops = 0usize;
    loop {
        let (n, last, more) = prefetched_count_page(e, from.as_deref(), hi, max, window);
        pages += 1;
        assert!(pages <= expected + 2, "prefetched pagination must progress: {ctx}");
        assert!(n <= max.max(1), "{ctx}");
        if n == 0 {
            assert!(last.is_none() && !more, "an empty page ends the walk: {ctx}");
            break;
        }
        let (sn, slast, _) = e.winner_count_page(from.as_deref(), hi, n, None).unwrap();
        assert_eq!((sn, slast.as_ref()), (n, last.as_ref()), "page is a sync prefix: {ctx}");
        if !more {
            let (rest, rlast, rmore) = e.winner_count_page(from.as_deref(), hi, usize::MAX, None).unwrap();
            assert_eq!((rest, rlast, rmore), (n, last, false), "final page matches sync: {ctx}");
            total += n;
            break;
        }
        total += n;
        if n < max {
            horizon_stops += 1;
        }
        let mut next = last.unwrap();
        next.push(0);
        from = Some(next);
    }
    assert_eq!(total, expected, "prefetched total: {ctx}");
    let mut sync_seq = Vec::new();
    let mut prefetch_seq = Vec::new();
    for (seq, prefetch) in [(&mut sync_seq, false), (&mut prefetch_seq, true)] {
        let mut from = lo.map(<[u8]>::to_vec);
        loop {
            let page = if prefetch {
                prefetched_count_page(e, from.as_deref(), hi, max, usize::MAX)
            } else {
                e.winner_count_page(from.as_deref(), hi, max, None).unwrap()
            };
            let (_, last, more) = page.clone();
            seq.push(page);
            if !more {
                break;
            }
            let mut next = last.unwrap();
            next.push(0);
            from = Some(next);
        }
    }
    assert_eq!(prefetch_seq, sync_seq, "an unbounded window paginates exactly like sync: {ctx}");
    horizon_stops
}

#[test]
fn prefetched_winner_count_matches_sync_over_randomized_layouts() {
    let key = |i: u64| format!("k{i:05}").into_bytes();
    let mut horizon_stops = 0usize;
    for seed in 1..=8u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng >> 11
        };
        let dev = MemBlockDevice::new(BS, 16384);
        let cfg = Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        let space = 300 + next() % 300;
        let mut distinct = std::collections::BTreeSet::new();
        let rounds = 2 + next() % 4;
        for round in 0..=rounds {
            let (lo_key, width) = match next() % 4 {
                0 => (next() % space, 1 + next() % 8),
                1 => (next() % (space / 2), space / 3),
                _ => (0, space),
            };
            let puts = 1 + next() % 160;
            for _ in 0..puts {
                let k = lo_key + next() % width.max(1);
                let tombstone = next() % 4 == 0;
                let version = (next() % 1000) as u128 + 1;
                e.put_kind(&key(k), b"v", version, tombstone).unwrap();
                distinct.insert(key(k));
            }
            if round < rounds {
                let opts = RunBuildOpts {
                    block_target_bytes: [64usize, 128, 256, 4096][(next() % 4) as usize],
                    bloom_bits_per_key: 10,
                    num_buckets: 16,
                };
                e.flush(opts).unwrap();
            }
        }
        assert!(e.run_count() >= 2, "seed {seed}: need several runs");
        #[allow(clippy::type_complexity)]
        let mut bounds: Vec<(Option<Vec<u8>>, Option<Vec<u8>>)> = vec![
            (None, None),
            (Some(key(space / 3)), Some(key(2 * space / 3))),
            (Some([key(space / 2), b"\0".to_vec()].concat()), None),
            (None, Some(key(1))),
            (Some(key(space + 10)), None),
        ];
        for _ in 0..4 {
            let a = next() % (space + 2);
            let b = a + next() % space;
            let mut l = key(a);
            if next() % 2 == 0 {
                l.push(b'~');
            }
            bounds.push((Some(l), Some(key(b))));
        }
        for (lo, hi) in &bounds {
            let expected = distinct
                .iter()
                .filter(|k| lo.as_ref().is_none_or(|l| *k >= l) && hi.as_ref().is_none_or(|h| *k < h))
                .count();
            for window in [2usize, 3, 8] {
                for max in [1usize, 7, 64, 100_000] {
                    horizon_stops += assert_prefetched_count_matches_sync(
                        &e,
                        lo.as_deref(),
                        hi.as_deref(),
                        max,
                        window,
                        expected,
                    );
                }
            }
        }
    }
    assert!(horizon_stops > 0, "windows must end mid-range in some layouts");
}

#[test]
fn prefetched_winner_versions_match_sync_winners_over_randomized_layouts() {
    let key = |i: u64| format!("k{i:05}").into_bytes();
    let mut horizon_stops = 0usize;
    for seed in 1..=8u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut next = move || {
            rng = rng
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            rng >> 11
        };
        let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 16384), cfg);
        let space = 300 + next() % 300;
        let rounds = 2 + next() % 4;
        for round in 0..=rounds {
            for _ in 0..1 + next() % 160 {
                let k = next() % space;
                e.put_kind(&key(k), b"v", (next() % 1000) as u128 + 1, next() % 4 == 0).unwrap();
            }
            if round < rounds {
                let opts = RunBuildOpts {
                    block_target_bytes: [64usize, 128, 256, 4096][(next() % 4) as usize],
                    bloom_bits_per_key: 10,
                    num_buckets: 16,
                };
                e.flush(opts).unwrap();
            }
        }
        let a = next() % space;
        for (lo, hi) in [(None, None), (Some(key(a)), Some(key(a + next() % space))), (Some(key(space / 2)), None)] {
            let (winners, _) = e.scan_winners_page(lo.as_deref(), hi.as_deref(), usize::MAX, usize::MAX).unwrap();
            let expected: Vec<(Vec<u8>, u128)> = winners.into_iter().map(|(k, w)| (k, w.version)).collect();
            assert_eq!(
                e.winner_version_page(lo.as_deref(), hi.as_deref(), usize::MAX, None).unwrap(),
                (expected.clone(), false),
                "seed {seed}: the sync version page returns every winner with its version",
            );
            for window in [2usize, 3, 8] {
                for max in [1usize, 7, 64] {
                    let mut from = lo.clone();
                    let mut walked = Vec::new();
                    loop {
                        let plan = e.winner_count_prefetch_plan(from.as_deref(), hi.as_deref(), window).unwrap();
                        let blocks = vals_rt::block_on(plan.read(e.device()), || {});
                        let (page, more) = e
                            .winner_version_page_prefetched(from.as_deref(), hi.as_deref(), max, None, blocks)
                            .unwrap()
                            .expect("an unchanged engine accepts its own prefetch");
                        assert!(page.len() <= max);
                        let sync = e.winner_version_page(from.as_deref(), hi.as_deref(), page.len().max(1), None).unwrap().0;
                        assert_eq!(page, sync[..page.len()], "seed {seed}: a prefetched page is a sync prefix");
                        if more && page.len() < max {
                            horizon_stops += 1;
                        }
                        let last = page.last().map(|(k, _)| k.clone());
                        walked.extend(page);
                        match last {
                            Some(mut last) if more => {
                                last.push(0);
                                from = Some(last);
                            }
                            _ => break,
                        }
                    }
                    assert_eq!(walked, expected, "seed {seed} window {window} max {max}");
                }
            }
        }
    }
    assert!(horizon_stops > 0, "windows must end mid-range in some layouts");
}

#[test]
fn prefetched_winner_count_horizon_edges() {
    let key = |i: u32| format!("k{i:04}").into_bytes();
    let dev = MemBlockDevice::new(BS, 16384);
    let cfg = Config {
        seg_blocks: 16,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut mem_only = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    for i in 0..5 {
        mem_only.put_kind(&key(i), b"", 1, i % 2 == 0).unwrap();
    }
    assert_eq!(mem_only.winner_count_prefetch_plan(None, None, 2).unwrap().block_reads(), 0);
    assert_eq!(prefetched_count_page(&mem_only, None, None, 3, 2), (3, Some(key(2)), true));
    assert_eq!(prefetched_count_page(&mem_only, Some(b"k0002\0"), None, 3, 2), (2, Some(key(4)), false));

    let mut e = Engine::create_prewiped(dev, cfg);
    let small = RunBuildOpts {
        block_target_bytes: 64,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    let single = RunBuildOpts {
        block_target_bytes: 1 << 20,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    for i in 0..200 {
        e.put(&key(i), b"many", 1).unwrap();
    }
    e.flush(small).unwrap();
    for i in [3u32, 50, 51, 199] {
        e.put_kind(&key(i), b"", 2, i == 50).unwrap();
    }
    e.flush(single).unwrap();
    for i in 500..520 {
        e.put(&key(i), b"far", 3).unwrap();
    }
    e.flush(small).unwrap();
    e.put_kind(&key(77), b"", 4, true).unwrap();
    e.put(&key(300), b"mt", 4).unwrap();
    assert_eq!(e.run_count(), 3);
    let distinct = 200 + 20 + 1;

    let plan = e.winner_count_prefetch_plan(Some(&key(10)), Some(&key(12)), 2).unwrap();
    assert!(plan.block_reads() >= 1, "a range that ends inside the first window still reads it");
    assert_eq!(prefetched_count_page(&e, Some(&key(10)), Some(&key(12)), 100, 2), (2, Some(key(11)), false));
    let plan = e.winner_count_prefetch_plan(Some(&key(600)), None, 2).unwrap();
    assert_eq!(plan.block_reads(), 0, "runs past lo are filtered by their key bounds");
    assert_eq!(prefetched_count_page(&e, Some(&key(600)), None, 100, 2), (0, None, false));
    let plan = e.winner_count_prefetch_plan(Some(&key(250)), Some(&key(400)), 2).unwrap();
    assert_eq!(plan.block_reads(), 0, "no run overlaps a gap between runs");
    assert_eq!(prefetched_count_page(&e, Some(&key(250)), Some(&key(400)), 100, 2), (1, Some(key(300)), false));
    let plan = e.winner_count_prefetch_plan(Some(b"k0100\x01"), None, 2).unwrap();
    assert!(plan.block_reads() <= 2 + 1 + 2);
    let (n, last, more) = prefetched_count_page(&e, Some(b"k0100\x01"), None, 10_000, 2);
    assert!(more && (1..100).contains(&n), "a two-block window stops at the horizon: {n}");
    assert!(last.unwrap().as_slice() > b"k0100".as_slice());
    let past = Some(std::time::Instant::now());
    let plan = e.winner_count_prefetch_plan(None, None, 10_000).unwrap();
    let blocks = vals_rt::block_on(plan.read(e.device()), || {});
    assert_eq!(
        e.winner_count_page_prefetched(None, None, 10_000, past, blocks).unwrap(),
        Some((32, Some(key(31)), true)),
        "an expired deadline ends the page at the first check"
    );
    for window in [2usize, 5, 64] {
        for max in [1usize, 16, 10_000] {
            assert_prefetched_count_matches_sync(&e, None, None, max, window, distinct);
            assert_prefetched_count_matches_sync(&e, Some(b"k0049\xff"), Some(&key(52)), max, window, 2);
        }
    }

    let plan = e.winner_count_prefetch_plan(Some(&key(40)), None, 2).unwrap();
    let blocks = vals_rt::block_on(plan.read(e.device()), || {});
    assert_eq!(
        e.winner_count_page_prefetched(Some(&key(41)), None, 100, None, blocks).unwrap(),
        None,
        "a prefetch for another page is discarded"
    );
    let plan = e.winner_count_prefetch_plan(Some(&key(40)), None, 2).unwrap();
    let blocks = vals_rt::block_on(plan.read(e.device()), || {});
    let generation = blocks.storage_generation();
    e.put(&key(41), b"newer", 9).unwrap();
    e.flush(small).unwrap();
    assert_ne!(e.storage_generation(), generation);
    assert_eq!(
        e.winner_count_page_prefetched(Some(&key(40)), None, 100, None, blocks).unwrap(),
        None,
        "a run install between plan and merge discards the prefetch"
    );
    let plan = e.winner_count_prefetch_plan(Some(&key(40)), None, 2).unwrap();
    let blocks = vals_rt::block_on(plan.read(e.device()), || {});
    e.put(&key(45), b"newest", 10).unwrap();
    e.put_kind(&key(46), b"", 10, true).unwrap();
    e.put(&key(451), b"", 10).unwrap();
    let (n, last, more) = e
        .winner_count_page_prefetched(Some(&key(40)), None, 100_000, None, blocks)
        .unwrap()
        .expect("memtable writes do not invalidate a prefetch");
    let (sn, slast, _) = e.winner_count_page(Some(&key(40)), None, n, None).unwrap();
    assert!(more);
    assert_eq!((n, last), (sn, slast), "memtable is read at merge time");
}
