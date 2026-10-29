use super::*;

#[test]
fn compact_commit_rejects_already_freed_input_before_install() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    e.put(b"a", b"one", 1).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put(b"b", b"two", 2).unwrap();
    run_flush(&mut e, opts).unwrap();

    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    let stale = plan.merged_inputs[0].handle.segs[0];
    let compact_dev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, compact_dev), || {}).unwrap();
    let outputs = written.output_segments();
    e.alloc.free(stale).unwrap();

    assert!(matches!(
        e.compact_commit(written),
        Err(EngineError::CompactionConflict)
    ));
    assert!(!e.compact_in_flight());
    assert!(outputs.iter().all(|seg| e.alloc.is_free(*seg)));
    assert_eq!(e.run_count(), 2);
}

#[test]
fn hot_key_orphan_vlog_segments_freed_by_merge() {
    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(dev, cfg);

    e.put(b"cold-a", b"a-value", 1).unwrap();
    e.put(b"cold-b", b"b-value", 2).unwrap();
    hammer_hot_key(&mut e, b"hot", 40, 100, 500);
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    hammer_hot_key(&mut e, b"hot", 40, 200, 500);
    e.put(b"cold-c", b"c-value", 3).unwrap();
    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert_eq!(e.run_count(), 2);

    let free_before = e.free_segments_for_test();
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(plan.co_compacts(), "merge must co-compact the value log");
    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert_eq!(stats.runs_merged, 2, "both runs merged");
    let free_after = e.free_segments_for_test();

    assert!(
        free_after >= free_before + 40,
        "co-compacting merge must free the hot-key orphan segments: \
         free before {free_before}, after {free_after}"
    );

    assert_eq!(
        e.get(b"hot").unwrap().as_deref(),
        Some(vec![200u8.wrapping_add(39); 500].as_slice())
    );
    assert_eq!(e.get(b"cold-a").unwrap().as_deref(), Some(&b"a-value"[..]));
    assert_eq!(e.get(b"cold-c").unwrap().as_deref(), Some(&b"c-value"[..]));
}

#[test]
fn compact_runs_preserves_recovery_watermark_across_merge_and_reopen() {
    let path = temp_path("compact-runs-watermark");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    let (seq_before, epoch_before);
    {
        let dev = FileBlockDevice::create(&path, BS, 2048).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for run in 0..3u32 {
            for i in 0..15u32 {
                let k = format!("key-{i:03}");
                let v = if i == 0 {
                    run as u128 + 1
                } else {
                    ver_of(k.as_bytes())
                };
                e.put(k.as_bytes(), format!("r{run}-{i}").as_bytes(), v)
                    .unwrap();
            }
            e.flush(RunBuildOpts::default()).unwrap();
        }
        assert_eq!(e.run_count(), 3, "three runs to merge");
        e.sync().unwrap();

        let (s, ep) = manifest_watermark(&path, cfg);
        assert!(
            s > 0 && ep > 0,
            "precondition: watermark must be non-zero before compaction (seq={s}, epoch={ep})"
        );
        seq_before = s;
        epoch_before = ep;

        let stats = e.compact_runs(RunBuildOpts::default(), None).unwrap();
        assert_eq!(stats.runs_merged, 3);
        assert_eq!(e.run_count(), 1);
        e.sync().unwrap();
    }

    let (seq_after, epoch_after) = manifest_watermark(&path, cfg);
    assert_eq!(
        seq_after, seq_before,
        "covered_seq must be preserved across compact_runs (was {seq_before}, now {seq_after})"
    );
    assert_eq!(
        epoch_after, epoch_before,
        "covered_epoch must be preserved across compact_runs (was {epoch_before}, now {epoch_after})"
    );

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.run_count(), 1, "reopen sees exactly the merged run");
    assert_eq!(e.get(b"key-000").unwrap().as_deref(), Some(&b"r2-0"[..]));
    for i in 1..15u32 {
        let k = format!("key-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("r2-{i}").as_bytes()),
            "{k} must recover from the merged run"
        );
    }
    let (seq_reopen, epoch_reopen) = manifest_watermark(&path, cfg);
    assert_eq!(
        seq_reopen, seq_before,
        "reopened watermark seq matches the merged manifest"
    );
    assert_eq!(
        epoch_reopen, epoch_before,
        "reopened watermark epoch matches the merged manifest"
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn compact_reclaims_dead_records_and_survives_reopen() {
    let path = temp_path("compact");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };

    let used_before;
    let stats;
    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..40u32 {
            e.put(b"hot", format!("v{i}").as_bytes(), 0).unwrap();
        }
        for k in ["a", "b", "c"] {
            e.put(k.as_bytes(), b"keep", 0).unwrap();
        }
        e.put(b"victim", b"bye", 0).unwrap();
        e.delete(b"victim", 0).unwrap();
        e.sync().unwrap();

        used_before = e.used_segments();
        stats = e.compact().unwrap();

        assert_eq!(e.get(b"hot").unwrap().as_deref(), Some(&b"v39"[..]));
        assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"keep"[..]));
        assert_eq!(e.get(b"victim").unwrap(), None);
        assert_eq!(e.len(), 4);

        e.put(b"after", b"z", 0).unwrap();
        e.sync().unwrap();
    }

    assert!(
        stats.dead_dropped >= 40,
        "dead_dropped={}",
        stats.dead_dropped
    );
    assert!(stats.tombstones_retained >= 1);
    assert!(stats.segments_freed > 0);
    assert!(
        e_used_after(&path, cfg) < used_before,
        "compaction should shrink used segments"
    );

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"hot").unwrap().as_deref(), Some(&b"v39"[..]));
    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"keep"[..]));
    assert_eq!(e.get(b"victim").unwrap(), None);
    assert_eq!(e.get(b"after").unwrap().as_deref(), Some(&b"z"[..]));
    assert_eq!(e.len(), 5);

    std::fs::remove_file(&path).ok();
}

#[test]
fn compact_step_proceeds_where_full_pass_refuses_headroom() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 64), cfg);
    for i in 0..36u32 {
        e.put(format!("k{i:04}xx").as_bytes(), &vec![7u8; 214], 0)
            .unwrap();
    }
    e.sync().unwrap();

    assert!(matches!(
        e.compact(),
        Err(EngineError::InsufficientHeadroom { .. })
    ));

    let (s, _done) = e.compact_step(2).unwrap();
    assert!(!e.is_poisoned());
    assert!(s.victims_scanned >= 1, "step made no progress");

    assert_eq!(
        e.get(b"k0000xx").unwrap().as_deref(),
        Some(&vec![7u8; 214][..])
    );
    assert_eq!(
        e.get(b"k0035xx").unwrap().as_deref(),
        Some(&vec![7u8; 214][..])
    );
}

#[test]
fn compaction_refuses_without_headroom() {
    let dev = MemBlockDevice::new(BS, 64);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for i in 0..36u32 {
        e.put(format!("k{i:04}xx").as_bytes(), &vec![7u8; 214], 0)
            .unwrap();
    }
    e.sync().unwrap();

    match e.compact() {
        Err(EngineError::InsufficientHeadroom {
            needed_segments,
            free_segments,
        }) => assert!(needed_segments > free_segments),
        other => panic!("expected InsufficientHeadroom, got {other:?}"),
    }
    assert!(!e.is_poisoned());
    assert_eq!(e.len(), 36);
    assert_eq!(
        e.get(b"k0000xx").unwrap().as_deref(),
        Some(&vec![7u8; 214][..])
    );
    e.put(b"still-writable", b"yes", 0).unwrap();
}

#[test]
fn reclaimable_fraction_tracks_dead_data() {
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let n = 2000u32;
    for i in 0..n {
        e.put(format!("k{i:05}").as_bytes(), &[7u8; 200], 0)
            .unwrap();
    }
    e.sync().unwrap();
    assert!(
        e.reclaimable_fraction() < 0.10,
        "append-only zone should read as nearly clean, got {}",
        e.reclaimable_fraction()
    );

    for i in 0..n {
        e.put(format!("k{i:05}").as_bytes(), &[9u8; 200], 0)
            .unwrap();
    }
    e.sync().unwrap();
    assert!(
        e.reclaimable_fraction() > 0.40,
        "after overwriting every key, ~half should be dead, got {}",
        e.reclaimable_fraction()
    );

    let before = e.used_segments();
    let stats = e.compact().unwrap();
    assert!(stats.segments_freed > 0);
    assert!(e.used_segments() < before);
    assert!(
        e.reclaimable_fraction() < 0.10,
        "post-compaction zone is clean again, got {}",
        e.reclaimable_fraction()
    );
    assert_eq!(e.len(), n as usize);
}

#[test]
fn maint_reserve_is_small_at_low_fill_and_never_rejects() {
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_maint_reserve_enabled(true);
    for i in 0..80u32 {
        e.put(format!("k{i:05}").as_bytes(), &[7u8; 200], i as u128 + 1)
            .expect("low-fill write never rejected");
    }
    e.sync().unwrap();
    let free = e.free_segments_for_test();
    let reserve = e.maint_reserve_for_test();
    assert!(
        reserve < free,
        "reserve ({reserve}) must stay below free ({free}) at low fill — no false positive"
    );
    assert!(e.free_fraction() > 0.5, "still plenty of free space");
    for round in 0..5u32 {
        for i in 0..80u32 {
            e.put(
                format!("k{i:05}").as_bytes(),
                &[9u8; 200],
                (round * 80 + i) as u128 + 100,
            )
            .expect("churn write succeeds while free stays ample");
        }
    }
    assert!(
        e.maint_reserve_for_test() < e.free_segments_for_test(),
        "reserve stays below free under churn with a small working set"
    );
}

#[test]
fn recurring_merge_shrinks_to_fit_manifest_headroom() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 32768);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for run_no in 0..4u32 {
        for i in 0..300u32 {
            let n = run_no * 300 + i;
            e.put(format!("k{n:05}").as_bytes(), &[5u8; 96], n as u128 + 1)
                .unwrap();
        }
        assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    }

    let seg_bytes = e.alloc.blocks_per_segment() as usize * e.block_size;
    let two_run_bytes: usize = e.runs[2..]
        .iter()
        .map(|or| or.meta.handle.len as usize)
        .sum();
    let target_free = e.manifest_headroom_segs()
        + two_run_bytes.div_ceil(seg_bytes).max(1) as u64;
    while e.alloc.free_segments() > target_free {
        e.alloc.allocate().unwrap();
    }
    let full_bytes: usize = e.runs
        .iter()
        .map(|or| or.meta.handle.len as usize)
        .sum();
    assert!(
        full_bytes.div_ceil(seg_bytes).max(1) as u64 + e.manifest_headroom_segs()
            > e.free_segments(),
        "the original full-output reservation cannot fit"
    );

    let plan = e
        .compact_plan(RunBuildOpts::default(), None)
        .expect("headroom shortage is not Snapshot(DeviceFull)")
        .expect("a two-run suffix fits");
    assert_eq!(plan.merged_inputs.len(), 2);
}

#[test]
fn floor_pressure_crosses_input_cap_for_minimal_two_run_progress() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 32768);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    for run_no in 0..2u32 {
        for i in 0..20u32 {
            e.put(
                format!("run-{run_no}-key-{i:04}").as_bytes(),
                &[run_no as u8; 3900],
                (run_no * 100 + i) as u128 + 1,
            )
            .unwrap();
        }
        assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    }
    let sizes: Vec<u64> = e.runs.iter().map(|run| run.meta.handle.len).collect();
    let cap = sizes[1].saturating_sub(1).max(1);
    e.merge_policy.size_cap = cap;
    assert_eq!(select_capped_suffix(&sizes, cap), 1);
    e.set_maint_reserve_enabled(true);
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }

    match e.compact_plan_pressure(RunBuildOpts::default(), None) {
        Ok(Some(plan)) => {
            assert_eq!(plan.merged_inputs.len(), 2);
            assert!(plan.input_bytes() > cap);
            e.free_compact_segs(&plan.segments());
        }
        Err(EngineError::InsufficientHeadroom {
            needed_segments,
            free_segments,
        }) => {
            assert!(needed_segments > free_segments);
            assert!(e.region_exhausted());
        }
        Ok(None) => panic!("floor pressure must progress or report terminal exhaustion"),
        Err(other) => panic!("unexpected floor-pressure error: {other:?}"),
    }
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn floor_pressure_prefers_a_viable_one_run_victim_over_two_runs() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    for i in 0..20u32 {
        e.put(format!("old-{i:04}").as_bytes(), &[1u8; 3900], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    e.put(b"victim-live", &[2u8; 100], 100).unwrap();
    for version in 0..10u128 {
        e.put(
            b"overwritten",
            &[3u8; 3900],
            version + 101,
        )
        .unwrap();
    }
    e.put(b"victim-live-2", &[6u8; 100], 150).unwrap();
    for version in 0..2u128 {
        e.put(b"overwritten-2", &[7u8; 3900], version + 151).unwrap();
    }
    for i in 0..8u32 {
        e.put(format!("new-{i:04}").as_bytes(), &[5u8; 3900], i as u128 + 200)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());

    let seg_bytes = e.alloc.blocks_per_segment() as usize * e.block_size;
    e.runs[0].meta.handle.len = (e.alloc.num_segments() / 2 + 8) * seg_bytes as u64;
    let sizes: Vec<u64> = e.runs.iter().map(|run| run.meta.handle.len).collect();
    let cap = sizes.iter().copied().sum::<u64>();
    e.merge_policy.size_cap = cap;
    assert_eq!(select_capped_suffix(&sizes, cap), 0);
    e.set_maint_reserve_enabled(true);
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    let two_run_needed = (e.runs.iter().map(|run| run.meta.handle.len).sum::<u64>()
        as usize)
        .div_ceil(seg_bytes)
        .max(1) as u64
        + e.manifest_headroom_segs();
    assert!(two_run_needed > e.free_segments());
    assert!(
        e.minimal_pressure_needed_segs()
            .is_some_and(|needed| needed <= e.free_segments())
    );
    assert!(!e.region_exhausted());

    let used_before = e.used_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .unwrap()
        .expect("one-run victim pressure plan");
    assert_eq!(plan.merged_inputs.len(), 1);
    assert!(plan.co_compacts());
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert!(stats.segments_freed > 0);
    assert!(e.used_segments() < used_before);
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn unknown_run_value_bytes_keeps_maintenance_floor() {
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for i in 0..32u32 {
        e.put(format!("k{i:04}").as_bytes(), &[3u8; 96], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    e.runs.last_mut().unwrap().meta.value_bytes = None;
    e.set_maint_reserve_enabled(true);

    assert!(e.index.is_empty());
    assert_eq!(e.lsm_dead_fraction(), None);
    assert!(
        e.maint_reserve_for_test() >= e.manifest_headroom_segs() + 5,
        "unknown value metadata must retain index, manifest, and victim-rewrite headroom"
    );
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    assert!(!e.region_exhausted(), "unknown metadata remains retryable");
}

#[test]
fn victim_lives_ascending_matches_reference() {
    fn run(segs: Option<Vec<u64>>, bytes: Option<Vec<u64>>) -> RunMeta {
        RunMeta {
            handle: crate::run::RunHandle {
                segs: vec![SegmentId(99)],
                len: 1,
            },
            min_key: Vec::new(),
            max_key: Vec::new(),
            entry_count: 0,
            level: 0,
            value_segs: segs.map(|segs| segs.into_iter().map(SegmentId).collect()),
            value_bytes: bytes.as_ref().map(|bytes| {
                bytes
                    .iter()
                    .copied()
                    .fold(0u64, u64::saturating_add)
            }),
            tombstone_min_version: None,
            value_seg_bytes: bytes,
        }
    }

    fn reference(
        inputs: &[RunMeta],
        victims: &std::collections::HashSet<SegmentId>,
    ) -> Option<Vec<(SegmentId, u64)>> {
        let mut lives = Vec::with_capacity(victims.len());
        for victim in victims.iter().copied() {
            let mut live = 0u64;
            for meta in inputs {
                let segs = meta.value_segs.as_ref()?;
                let bytes = meta.value_seg_bytes.as_ref()?;
                for (seg, value_bytes) in segs.iter().zip(bytes) {
                    if *seg == victim {
                        live = live.saturating_add(*value_bytes);
                    }
                }
            }
            lives.push((victim, live));
        }
        lives.sort_by_key(|&(seg, live)| (live, seg));
        Some(lives)
    }

    fn linear_candidate(
        inputs: &[RunMeta],
        victims: &std::collections::HashSet<SegmentId>,
    ) -> Option<Vec<(SegmentId, u64)>> {
        let mut lives: std::collections::HashMap<SegmentId, u64> = victims
            .iter()
            .copied()
            .map(|victim| (victim, 0))
            .collect();
        if lives.is_empty() {
            return Some(Vec::new());
        }
        for meta in inputs {
            let segs = meta.value_segs.as_ref()?;
            let bytes = meta.value_seg_bytes.as_ref()?;
            for (seg, value_bytes) in segs.iter().zip(bytes) {
                if let Some(live) = lives.get_mut(seg) {
                    *live = live.saturating_add(*value_bytes);
                }
            }
        }
        let mut lives: Vec<_> = lives.into_iter().collect();
        lives.sort_by_key(|&(seg, live)| (live, seg));
        Some(lives)
    }

    fn assert_case(
        inputs: &[RunMeta],
        victims: &std::collections::HashSet<SegmentId>,
    ) {
        let expected = reference(inputs, victims);
        let candidate = linear_candidate(inputs, victims);
        assert_eq!(candidate, expected);
        assert_eq!(
            Engine::<MemBlockDevice>::victim_lives_ascending(inputs, victims),
            expected
        );

        let candidate_smallest = candidate
            .as_ref()
            .and_then(|lives| lives.iter().copied().min_by_key(|(_, live)| *live));
        let current_smallest = Engine::<MemBlockDevice>::smallest_victim(inputs, victims);
        let min_live = expected
            .as_ref()
            .and_then(|lives| lives.iter().map(|(_, live)| *live).min());
        for smallest in [candidate_smallest, current_smallest] {
            match (smallest, min_live) {
                (None, None) => {}
                (Some((seg, live)), Some(min_live)) => {
                    assert_eq!(live, min_live);
                    assert!(expected.as_ref().is_some_and(|lives| {
                        lives
                            .iter()
                            .any(|&(tied_seg, tied_live)| tied_seg == seg && tied_live == min_live)
                    }));
                }
                other => panic!("smallest victim mismatch: {other:?}"),
            }
        }
    }

    let victim_set = |victims: &[u64]| {
        victims
            .iter()
            .copied()
            .map(SegmentId)
            .collect::<std::collections::HashSet<_>>()
    };
    let cases = vec![
        (Vec::new(), victim_set(&[])),
        (Vec::new(), victim_set(&[2, 7, 11])),
        (
            vec![
                run(Some(vec![7, 2, 9]), Some(vec![30, 10, 99])),
                run(Some(vec![2, 7]), Some(vec![5, 20])),
            ],
            victim_set(&[2, 7, 11]),
        ),
        (
            vec![run(Some(vec![8, 3, 5, 3]), Some(vec![11, 6, 11, 5]))],
            victim_set(&[3, 5, 8]),
        ),
        (
            vec![
                run(Some(vec![4, 4]), Some(vec![u64::MAX, 1])),
                run(Some(vec![4]), Some(vec![u64::MAX])),
            ],
            victim_set(&[4, 6]),
        ),
        (
            vec![run(Some(vec![2, 7, 9]), Some(vec![10]))],
            victim_set(&[2, 7, 9]),
        ),
        (
            vec![run(Some(vec![2]), Some(vec![10, 20, 30]))],
            victim_set(&[2, 7]),
        ),
        (vec![run(None, Some(vec![10]))], victim_set(&[2])),
        (vec![run(Some(vec![2]), None)], victim_set(&[2])),
        (vec![run(None, None)], victim_set(&[])),
    ];

    for (inputs, victims) in cases {
        assert_case(&inputs, &victims);
    }

    for seed in 0..64u64 {
        let inputs = (0..seed % 5 + 1)
            .map(|run_i| {
                let len = ((seed + run_i) % 7 + 1) as usize;
                let segs = (0..len)
                    .map(|i| (seed * 5 + run_i * 3 + i as u64 * 7) % 13)
                    .collect();
                let byte_len = if seed % 4 == 0 { len - 1 } else { len };
                let bytes = (0..byte_len)
                    .map(|i| (seed + run_i + i as u64) % 19)
                    .collect();
                run(Some(segs), Some(bytes))
            })
            .collect::<Vec<_>>();
        let victims = victim_set(&[
            seed % 13,
            (seed + 3) % 13,
            (seed + 7) % 13,
            (seed + 11) % 13,
        ]);
        assert_case(&inputs, &victims);
    }
}

#[test]
fn region_exhausted_identifies_a_zero_net_gain_single_victim() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    e.set_maint_reserve_enabled(true);
    e.put(b"victim-live", &[1u8; 100], 1).unwrap();
    e.put(b"overwritten", &[2u8; 3500], 2).unwrap();
    e.put(b"overwritten", &[3u8; 3900], 3).unwrap();
    let mut blocked = false;
    for i in 0..400u32 {
        match e.put(
            format!("full-{i:04}").as_bytes(),
            &[4u8; 3900],
            i as u128 + 10,
        ) {
            Ok(()) => {}
            Err(EngineError::Log(LogError::Backpressure)) => {
                blocked = true;
                break;
            }
            Err(other) => panic!("unexpected fill error: {other:?}"),
        }
    }
    assert!(blocked);
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());

    assert!(
        e.lsm_dead_fraction()
            .is_some_and(|dead| dead < MAINT_RESERVE_DEAD_MIN_FRACTION),
        "aggregate dead fraction is {:?}",
        e.lsm_dead_fraction()
    );
    let meta = e.runs[0].meta.clone();
    let usable = (e.alloc.blocks_per_segment() as usize * e.block_size)
        .saturating_sub(SEG_HEADER_LEN) as u64;
    assert!(
        e.select_victims(std::slice::from_ref(&meta), usable)
            .is_some_and(|(victims, _)| victims.len() == 1)
    );
    assert!(
        e.region_exhausted(),
        "a lone victim whose rewrite frees no net segments is terminal"
    );
    for _ in 0..3 {
        match e.compact_plan_pressure(RunBuildOpts::default(), None) {
            Err(EngineError::InsufficientHeadroom {
                needed_segments: _,
                free_segments: _,
            }) => {}
            Ok(Some(plan)) => panic!(
                "zero-net-gain victim must not be re-selected: {} inputs, co_compacts={}",
                plan.merged_inputs.len(),
                plan.co_compacts()
            ),
            Ok(None) => panic!("floor refusal must surface as InsufficientHeadroom"),
            Err(other) => panic!("unexpected pressure error: {other:?}"),
        }
        assert!(e.region_exhausted());
        assert!(!e.compact_in_flight);
    }
}

#[test]
fn one_run_pressure_refuses_when_victim_rewrite_cannot_fit() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    e.put(b"victim-live", &[1u8; 100], 1).unwrap();
    e.put(b"overwritten", &[2u8; 3500], 2).unwrap();
    e.put(b"overwritten", &[3u8; 3900], 3).unwrap();
    e.put(b"victim-live-2", &[6u8; 100], 4).unwrap();
    e.put(b"overwritten-2", &[7u8; 3500], 5).unwrap();
    e.put(b"overwritten-2", &[8u8; 3900], 6).unwrap();
    for i in 0..8u32 {
        e.put(format!("full-{i:03}").as_bytes(), &[4u8; 3900], i as u128 + 10)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    {
        let meta = &mut e.runs[0].meta;
        let kept: Vec<(SegmentId, u64)> = meta
            .value_segs
            .as_ref()
            .unwrap()
            .iter()
            .copied()
            .zip(meta.value_seg_bytes.as_ref().unwrap().iter().copied())
            .filter(|(_, bytes)| *bytes > 0)
            .collect();
        meta.value_segs = Some(kept.iter().map(|(seg, _)| *seg).collect());
        meta.value_seg_bytes = Some(kept.iter().map(|(_, bytes)| *bytes).collect());
    }
    let seg_bytes = e.alloc.blocks_per_segment() as usize * e.block_size;
    let output_segs = (e.runs[0].meta.handle.len as usize)
        .div_ceil(seg_bytes)
        .max(1) as u64;
    e.set_maint_reserve_enabled(true);
    let target_free = e.manifest_headroom_segs() + output_segs + 3;
    while e.free_segments() > target_free {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    let free_before = e.free_segments();
    let meta = e.runs[0].meta.clone();
    let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN) as u64;
    assert!(
        e.select_victims(std::slice::from_ref(&meta), usable)
            .is_some_and(|(victims, _)| victims.len() == 2)
    );

    match e.compact_plan_pressure(RunBuildOpts::default(), None) {
        Err(EngineError::InsufficientHeadroom { .. }) => {}
        Ok(Some(plan)) => panic!(
            "unexpected pressure plan: co_compacts={} reserved={}",
            plan.co_compacts(),
            plan.segments().len()
        ),
        Ok(None) => panic!("unexpected no-op pressure result"),
        Err(other) => panic!("unexpected pressure error: {other:?}"),
    }
    assert_eq!(e.free_segments(), free_before);
    assert!(!e.compact_in_flight);
}

#[test]
fn region_exhausted_identifies_an_all_live_single_run() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for i in 0..30u32 {
        e.put(format!("full-{i:04}").as_bytes(), &[5u8; 3900], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    e.set_maint_reserve_enabled(true);
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }

    assert!(e.region_exhausted());
}

#[test]
fn region_exhausted_covers_a_capacity_stalled_nonempty_memtable() {
    if co_compact_env(false) { return; }
    let mut e = stalled_mostly_live_floor_engine();
    e.set_orphan_reclaim_for_test(false);
    let usable = ((e.alloc.blocks_per_segment() as usize * e.block_size)
        .saturating_sub(SEG_HEADER_LEN)) as u64;
    assert!(
        e.shadowed_bytes_estimate_cached() < usable,
        "unique-key memtable must not register as shadow garbage: {}",
        e.shadowed_bytes_estimate_cached()
    );
    assert!(!e.region_exhausted(), "no failed plans yet");

    fail_pressure_plans(&mut e, 2);
    assert!(
        !e.region_exhausted(),
        "below the failure threshold the state stays retryable"
    );
    fail_pressure_plans(&mut e, 1);
    assert_eq!(e.pressure_infeasible_streak_for_test(), 3);
    assert!(
        e.region_exhausted(),
        "a persistently infeasible mostly-live floor engine with a non-empty \
         memtable is capacity-terminal"
    );

    e.free_compact_segs(&[]);
    assert_eq!(e.pressure_infeasible_streak_for_test(), 0);
    assert!(
        !e.region_exhausted(),
        "maintenance progress clears the stall signal"
    );
    fail_pressure_plans(&mut e, 3);
    assert!(e.region_exhausted(), "re-accumulated failures re-terminate");
}

#[test]
fn stalled_memtable_with_shadowed_garbage_stays_retryable() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    e.set_maint_reserve_enabled(true);
    for i in 0..100u32 {
        e.put(format!("full-{i:04}").as_bytes(), &[4u8; 3900], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in 0..100u32 {
        e.put(
            format!("full-{i:04}").as_bytes(),
            &[6u8; 100],
            i as u128 + 10_000,
        )
        .unwrap();
    }
    let mut blocked = false;
    for i in 0..400u32 {
        match e.put(
            format!("resident-{i:04}").as_bytes(),
            &[5u8; 3900],
            i as u128 + 20_000,
        ) {
            Ok(()) => {}
            Err(EngineError::Log(LogError::Backpressure)) => {
                blocked = true;
                break;
            }
            Err(other) => panic!("unexpected fill error: {other:?}"),
        }
    }
    assert!(blocked);
    assert!(!e.index.is_empty());
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    let _ = e.lsm_dead_fraction_gate();
    let usable = ((e.alloc.blocks_per_segment() as usize * e.block_size)
        .saturating_sub(SEG_HEADER_LEN)) as u64;
    assert!(
        e.shadowed_bytes_estimate_cached() >= usable,
        "fully overwritten run must register shadow garbage: {}",
        e.shadowed_bytes_estimate_cached()
    );

    fail_pressure_plans(&mut e, 4);
    assert!(e.pressure_infeasible_streak_for_test() >= 3);
    assert!(
        !e.region_exhausted(),
        "memtable-shadowed garbage is recoverable by flush+fold and must stay \
         retryable"
    );
}

#[test]
fn stalled_memtable_with_visible_dead_stays_retryable() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    e.set_maint_reserve_enabled(true);
    for i in 0..100u32 {
        e.put(format!("full-{i:04}").as_bytes(), &[4u8; 3900], i as u128 + 1)
            .unwrap();
    }
    for i in 0..50u32 {
        e.put(
            format!("full-{i:04}").as_bytes(),
            &[7u8; 3900],
            i as u128 + 10_000,
        )
        .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in 0..8u32 {
        e.put(
            format!("resident-{i:04}").as_bytes(),
            &[5u8; 100],
            i as u128 + 20_000,
        )
        .unwrap();
    }
    assert!(!e.index.is_empty());
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    assert!(
        e.lsm_dead_fraction()
            .is_some_and(|dead| dead >= MAINT_RESERVE_DEAD_MIN_FRACTION),
        "pre-flush overwrites must leave visible dead: {:?}",
        e.lsm_dead_fraction()
    );
    let _ = e.lsm_dead_fraction_gate();
    e.pressure_infeasible_streak = STALL_TERMINAL_MIN_PLAN_FAILURES + 2;
    assert!(
        !e.stalled_memtable_capacity(),
        "visible dead above the reserve gate must stay retryable"
    );
    assert!(!e.region_exhausted());
}

#[test]
fn stall_terminal_requires_shadow_estimator() {
    if co_compact_env(false) { return; }
    let mut e = stalled_mostly_live_floor_engine();
    e.set_orphan_reclaim_for_test(false);
    fail_pressure_plans(&mut e, 4);
    assert!(e.region_exhausted());
    std::env::set_var("VALS_SHADOW_EST", "0");
    assert!(
        !e.region_exhausted(),
        "without the estimator the non-empty path cannot discriminate and stays \
         Busy"
    );
    std::env::remove_var("VALS_SHADOW_EST");
}

#[test]
fn reserve_floor_rejection_is_backpressure_true_full_is_device_full() {
    {
        let dev = MemBlockDevice::new(BS, 8192);
        let mut e = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 64,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        e.set_maint_reserve_enabled(true);
        const KEYS: u32 = 64;
        let mut hit = false;
        'churn: for round in 0..4000u32 {
            for i in 0..KEYS {
                let ver = (round * KEYS + i) as u128 + 1;
                match e.put(format!("k{i:04}").as_bytes(), &[7u8; 200], ver) {
                    Ok(()) => {}
                    Err(e @ EngineError::Log(LogError::Backpressure)) => {
                        assert!(e.is_backpressure(), "is_backpressure() is true");
                        hit = true;
                        break 'churn;
                    }
                    Err(other) => panic!("expected Backpressure, got {other:?}"),
                }
            }
            if round == 0 {
                assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
            }
        }
        assert!(hit, "reserve floor must eventually reject with Backpressure");
        assert!(e.free_segments_for_test() > 0, "reserve keeps segments free");
    }

    {
        let dev = MemBlockDevice::new(BS, 8192);
        let mut e = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 64,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        let mut hit = false;
        for i in 0..100_000u32 {
            match e.put(format!("k{i:08}").as_bytes(), &[7u8; 4000], i as u128 + 1) {
                Ok(()) => {}
                Err(e @ EngineError::Log(LogError::DeviceFull)) => {
                    assert!(!e.is_backpressure(), "true full is not backpressure");
                    hit = true;
                    break;
                }
                Err(other) => panic!("expected DeviceFull, got {other:?}"),
            }
        }
        assert!(hit, "a disarmed region fills to genuine DeviceFull");
    }
}

#[test]
fn admission_shed_probability_ramps_across_the_band() {
    let mut e = engine(Ordering::Unsorted);
    e.maint_reserve_enabled = true;
    e.alloc.set_data_reserve(8);
    assert_eq!(e.admission_shed_probability(), 0.0);
    let mut held = Vec::new();
    while e.alloc.free_segments() > 11 {
        held.push(e.alloc.allocate().unwrap());
    }
    assert_eq!(
        e.admission_shed_probability(),
        0.0,
        "the band top still admits everything"
    );
    let mut last = 0.0;
    while e.alloc.free_segments() > 8 {
        held.push(e.alloc.allocate().unwrap());
        let p = e.admission_shed_probability();
        assert!(
            p > last && p <= 1.0,
            "probability must rise as free falls (free={}, p={p}, last={last})",
            e.alloc.free_segments(),
        );
        last = p;
    }
    assert_eq!(
        e.admission_shed_probability(),
        1.0,
        "every draw sheds at the floor"
    );
    held.push(e.alloc.allocate().unwrap());
    assert_eq!(e.admission_shed_probability(), 1.0, "and below it");
    for s in held {
        e.alloc.free(s).unwrap();
    }
    assert_eq!(e.admission_shed_probability(), 0.0);
}

#[test]
fn admission_shed_draws_match_probability() {
    let mut e = engine(Ordering::Unsorted);
    e.maint_reserve_enabled = true;
    e.alloc.set_data_reserve(8);
    assert!(
        !(0..1000).any(|_| e.admission_shed()),
        "no sheds above the band"
    );
    while e.alloc.free_segments() > 9 {
        e.alloc.allocate().unwrap();
    }
    let p = e.admission_shed_probability();
    assert!(p > 0.0 && p < 1.0, "mid-band probability is fractional ({p})");
    let sheds = (0..10_000).filter(|_| e.admission_shed()).count();
    let frac = sheds as f64 / 10_000.0;
    assert!(
        (frac - p).abs() < 0.15,
        "observed shed fraction {frac} must track p={p}"
    );
    while e.alloc.free_segments() > 8 {
        e.alloc.allocate().unwrap();
    }
    assert!(
        (0..1000).all(|_| e.admission_shed()),
        "at the floor every request sheds"
    );
}

#[test]
fn admission_shed_band_tuning() {
    let mut e = engine(Ordering::Unsorted);
    e.alloc.set_data_reserve(8);
    while e.alloc.free_segments() > 8 {
        e.alloc.allocate().unwrap();
    }
    assert_eq!(
        e.admission_shed_probability(),
        0.0,
        "a disarmed engine never shapes"
    );
    e.maint_reserve_enabled = true;
    assert_eq!(e.admission_shed_probability(), 1.0);

    let mut wide = engine(Ordering::Unsorted);
    wide.maint_reserve_enabled = true;
    wide.alloc.set_data_reserve(8);
    while wide.alloc.free_segments() > 20 {
        wide.alloc.allocate().unwrap();
    }
    assert_eq!(
        wide.admission_shed_probability(),
        0.0,
        "free=20 sits above the default band top"
    );
    crate::engine::merge::TEST_ADMIT_BAND_PCT.with(|v| v.set(Some(50)));
    assert!(
        wide.admission_shed_probability() > 0.0,
        "a wider band starts shaping earlier"
    );
    crate::engine::merge::TEST_ADMIT_BAND_PCT.with(|v| v.set(None));
}

#[test]
fn co_compaction_reclaims_dead_value_bytes_and_survives_reboot() {
    if bottommost_co_compact_env() { return; }
    let path = temp_path("co-compact-reclaim");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts {
        block_target_bytes: 4096,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    let used_after;
    {
        let dev = FileBlockDevice::create(&path, BS, 16384).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        let keys = 60u32;
        for r in 0..10u128 {
            for k in 0..keys {
                let key = format!("key-{k:03}");
                let val = format!("round-{r:02}-value-{k:03}-{}", "p".repeat(30));
                e.put(key.as_bytes(), val.as_bytes(), 1 + r * 1000 + k as u128)
                    .unwrap();
            }
            run_flush(&mut e, opts);
        }
        let used_before = e.used_segments();
        compact_off(&mut e, opts, None);
        used_after = e.used_segments();
        assert!(
            used_after * 2 < used_before,
            "co-compaction should reclaim the dead value bytes: \
             before={used_before} after={used_after}"
        );
        for k in 0..keys {
            let key = format!("key-{k:03}");
            let want = format!("round-09-value-{k:03}-{}", "p".repeat(30));
            assert_eq!(
                e.get(key.as_bytes()).unwrap().as_deref(),
                Some(want.as_bytes())
            );
        }
        e.sync().unwrap();
    }
    {
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let e = Engine::open(dev, cfg).unwrap();
        assert_eq!(
            e.used_segments(),
            used_after,
            "space reclaim must survive reboot"
        );
        for k in 0..60u32 {
            let key = format!("key-{k:03}");
            let want = format!("round-09-value-{k:03}-{}", "p".repeat(30));
            assert_eq!(
                e.get(key.as_bytes()).unwrap().as_deref(),
                Some(want.as_bytes())
            );
        }
    }
}

#[test]
#[ignore = "measurement: prints pressure-merge RSS and linear component scaling"]
fn pressure_merge_resident_measurement() {
    for entries in [10_000usize, 40_000, 160_000] {
        let path = temp_path("pressure-merge-rss");
        let dev = FileBlockDevice::create(&path, BS, 1 << 22).unwrap();
        let cfg = Config {
            seg_blocks: 4096,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut engine = Engine::create_prewiped(dev, cfg);
        let opts = RunBuildOpts::default();
        for i in 0..entries {
            engine
                .put(format!("key-{i:09}").as_bytes(), b"old", 1)
                .unwrap();
        }
        run_flush(&mut engine, opts).unwrap();
        for i in 0..entries {
            let key = format!("key-{i:09}");
            if i % 2 == 0 {
                engine.put_kind(key.as_bytes(), &[], 2, true).unwrap();
            } else {
                engine.put(key.as_bytes(), b"new", 2).unwrap();
            }
        }
        run_flush(&mut engine, opts).unwrap();

        let plan = engine
            .compact_plan_pressure(opts, Some(u128::MAX))
            .unwrap()
            .unwrap();
        let input_bytes = plan.input_bytes();
        let rss_before = measurement_rss();
        let peak = Arc::new(AtomicU64::new(rss_before));
        let done = Arc::new(AtomicBool::new(false));
        let sampler = {
            let peak = peak.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                while !done.load(AtomicOrdering::Relaxed) {
                    peak.fetch_max(measurement_rss(), AtomicOrdering::Relaxed);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                peak.fetch_max(measurement_rss(), AtomicOrdering::Relaxed);
            })
        };
        let wdev = Arc::new(engine.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        done.store(true, AtomicOrdering::Relaxed);
        sampler.join().unwrap();
        let rss_delta = peak
            .load(AtomicOrdering::Relaxed)
            .saturating_sub(rss_before);
        let n_upper = entries * 2;
        let bloom_bytes = (n_upper.max(1) * opts.bloom_bits_per_key)
            .next_multiple_of(512)
            / 8;
        let dropped_bytes: usize = written
            .dropped_tombstones
            .iter()
            .map(|(key, _)| key.capacity() + std::mem::size_of::<(Vec<u8>, u128)>())
            .sum();
        let parts_bytes = written.parts.estimated_resident_bytes();
        eprintln!(
            "pressure merge resident measurement: entries={entries} input_bytes={input_bytes} rss_delta={rss_delta} bloom_bytes={bloom_bytes} dropped_bytes={dropped_bytes} read_parts_bytes={parts_bytes} dropped_count={}",
            written.dropped_tombstones.len()
        );
        engine.compact_commit(written).unwrap();
        std::fs::remove_file(path).ok();
    }
}

fn drain_to_floor<D: BlockDevice>(engine: &mut Engine<D>, held: &mut Vec<SegmentId>) {
    engine.set_orphan_reclaim_for_test(false);
    engine.set_maint_reserve_enabled(true);
    while engine.free_segments() > engine.maint_reserve_for_test() {
        held.push(engine.alloc.allocate().unwrap());
    }
    assert!(engine.at_maintenance_floor());
}

#[test]
fn pressure_and_over_cap_tombstone_gc_are_capped() {
    if co_compact_env(false) { return; }
    let opts = RunBuildOpts::default();
    let mut engine = Engine::create_prewiped(
        MemBlockDevice::new(BS, 1 << 18),
        Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    for run in 0..3u32 {
        for key in 0..100u32 {
            engine
                .put(format!("run-{run}-key-{key:04}").as_bytes(), b"value", 1)
                .unwrap();
        }
        run_flush(&mut engine, opts).unwrap();
    }
    let sizes: Vec<u64> = engine.runs.iter().map(|run| run.meta.handle.len).collect();
    let cap = sizes[sizes.len() - 2].saturating_add(sizes[sizes.len() - 1]);
    engine.merge_policy.size_cap = cap;
    let run_count = engine.run_count();
    let gc = engine
        .compact_plan_lone_gc(opts, Some(u128::MAX))
        .unwrap()
        .expect("an over-cap tombstone GC takes a capped window");
    assert_eq!(gc.kind(), MergeKind::LoneGc);
    assert!(gc.merged_inputs.len() < run_count);
    assert!(gc.input_bytes() <= cap);
    engine.free_compact_segs(&gc.segments());
    assert!(
        engine.compact_plan_pressure(opts, None).unwrap().is_none(),
        "before the floor a pressure merge with nothing to reclaim falls back to the recurring policy"
    );
    drain_to_floor(&mut engine, &mut Vec::new());
    let pressure = engine
        .compact_plan_pressure(opts, None)
        .unwrap()
        .unwrap();
    assert!(pressure.merged_inputs.len() < run_count);
    assert!(pressure.merged_inputs.len() > 1);
    assert!(pressure.input_bytes() <= cap);
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn distinct_key_pressure_merge_stalls_without_single_run_victims() {
    if co_compact_env(false) { return; }
    let opts = RunBuildOpts::default();
    let mut engine = Engine::create_prewiped(
        MemBlockDevice::new(BS, 1 << 18),
        Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    for run in 0..5u32 {
        for key in 0..200u32 {
            engine
                .put(format!("run-{run}-key-{key:04}").as_bytes(), b"value", 1)
                .unwrap();
        }
        run_flush(&mut engine, opts).unwrap();
    }
    std::env::set_var("VALS_VICTIM_DEAD_FRACTION", "1");
    let sizes: Vec<u64> = engine.runs.iter().map(|run| run.meta.handle.len).collect();
    let cap = sizes[sizes.len() - 2].saturating_add(sizes[sizes.len() - 1]);
    engine.merge_policy.size_cap = cap;

    let runs_before = engine.run_count();
    assert!(engine.compact_plan_pressure(opts, None).unwrap().is_none());
    let mut held = Vec::new();
    drain_to_floor(&mut engine, &mut held);
    let plan = engine
        .compact_plan_pressure(opts, None)
        .unwrap()
        .unwrap();
    assert!(plan.merged_inputs.len() > 1);
    assert!(plan.input_bytes() <= cap);
    let wdev = Arc::new(engine.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    engine.compact_commit(written).unwrap();
    assert!(engine.run_count() < runs_before);

    let sizes: Vec<u64> = engine.runs.iter().map(|run| run.meta.handle.len).collect();
    assert_eq!(select_capped_suffix(&sizes, cap), sizes.len() - 1);
    for seg in held.drain(..) {
        engine.alloc.free(seg).unwrap();
    }
    assert!(engine.compact_plan_pressure(opts, None).unwrap().is_none());

    for run in 5..7u32 {
        for key in 0..200u32 {
            engine
                .put(format!("run-{run}-key-{key:04}").as_bytes(), b"value", 1)
                .unwrap();
        }
        run_flush(&mut engine, opts).unwrap();
    }
    let runs_before = engine.run_count();
    drain_to_floor(&mut engine, &mut held);
    let plan = engine
        .compact_plan_pressure(opts, None)
        .unwrap()
        .unwrap();
    assert!(plan.merged_inputs.len() > 1);
    assert!(plan.input_bytes() <= cap);
    let wdev = Arc::new(engine.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    engine.compact_commit(written).unwrap();
    assert!(engine.run_count() < runs_before);
    let sizes: Vec<u64> = engine.runs.iter().map(|run| run.meta.handle.len).collect();
    assert_eq!(select_capped_suffix(&sizes, cap), sizes.len() - 1);
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
    std::env::remove_var("VALS_VICTIM_DEAD_FRACTION");
}

#[test]
fn recurring_merge_is_index_only_preserves_ptrs_and_leaves_vlog_untouched() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_VLOG_CO_COMPACT", "");
    std::env::remove_var("VALS_VLOG_CO_COMPACT");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    for r in 0..6u128 {
        for k in 0..40u32 {
            let key = format!("k-{r}-{k:03}");
            e.put(
                key.as_bytes(),
                format!("v-{r}-{k:03}").as_bytes(),
                1 + r * 1000 + k as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
    }
    assert_eq!(e.run_count(), 6);
    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    assert!(
        !plan.co_compacts(),
        "recurring merge is index-only by default"
    );
    assert!(plan.paced, "an unforced recurring merge is paced");
    let n_inputs = plan.merged_inputs.len();
    assert!(
        (2..=6).contains(&n_inputs),
        "folds a subset, got {n_inputs}"
    );
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    assert!(!written.co_compacted, "no value bytes rewritten");
    e.compact_commit(written).unwrap();
    for r in 0..6u128 {
        for k in 0..40u32 {
            let key = format!("k-{r}-{k:03}");
            assert_eq!(
                e.get(key.as_bytes()).unwrap().as_deref(),
                Some(format!("v-{r}-{k:03}").as_bytes())
            );
        }
    }
    assert!(
        !e.last_run_value_segs_for_test().is_empty(),
        "merged run names its value segments"
    );
}

#[test]
fn index_only_merge_orphans_swept_at_commit() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    let stats = overwrite_rounds_then_index_only_merge(&mut e);
    assert!(stats.segments_freed > 0);
    assert!(
        e.orphan_segments_for_test().is_empty(),
        "index-only merge commit must leave no orphaned vlog segments"
    );
    assert_eq!(
        e.used_segments() as usize,
        e.referenced_segments().expect("metadata complete").len(),
        "space accounting must close after the sweep"
    );
    for k in 0..40u32 {
        let key = format!("k-{k:03}");
        assert_eq!(
            e.get(key.as_bytes()).unwrap().as_deref(),
            Some(format!("v-5-{k:03}").as_bytes()),
            "newest overwrite survives the sweep"
        );
    }
}

#[test]
fn orphan_reclaim_sweep_recovers_space_and_bumps_storage_generation() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    overwrite_rounds_then_index_only_merge(&mut e);
    let orphans = e.orphan_segments_for_test();
    assert!(
        !orphans.is_empty(),
        "index-only merges must fully dereference vlog segments here"
    );
    e.set_orphan_reclaim_for_test(true);
    let free_before = e.free_segments();
    let generation_before = e.storage_generation();
    let freed = e.reclaim_orphan_segments();
    assert_eq!(freed, orphans.len());
    assert_ne!(e.storage_generation(), generation_before);
    assert_eq!(e.free_segments(), free_before + freed as u64);
    assert!(e.orphan_segments_for_test().is_empty());
    for seg in orphans {
        assert!(!e.seg_allocated_for_test(seg));
    }
}

#[test]
fn pressure_plan_reclaims_fabricated_orphans() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    let opts = RunBuildOpts::default();
    for k in 0..80u32 {
        let key = format!("k-{k:03}");
        e.put(key.as_bytes(), &[9u8; 40], 1 + k as u128).unwrap();
    }
    run_flush(&mut e, opts);
    let meta = &mut e.runs[0].meta;
    let vs = meta.value_segs.as_mut().unwrap();
    assert!(vs.len() >= 2, "need >= 2 value segs, got {}", vs.len());
    let stolen: Vec<SegmentId> = vs.drain(..1).collect();
    meta.value_seg_bytes.as_mut().unwrap().drain(..1);
    for seg in &stolen {
        assert!(e.seg_allocated_for_test(*seg));
    }
    let _ = e.compact_plan_pressure(opts, None);
    for seg in &stolen {
        assert!(
            !e.seg_allocated_for_test(*seg),
            "pressure planning must reclaim orphaned segments"
        );
    }
}

#[test]
fn pressure_merge_reclaims_via_victims_and_reads_transparently() {
    if co_compact_env(false) { return; }
    std::env::remove_var("VALS_VLOG_CO_COMPACT");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts {
        block_target_bytes: 4096,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(dev, cfg);
    let keys = 40u32;
    for r in 0..10u128 {
        for k in 0..keys {
            let key = format!("key-{k:03}");
            let val = format!("round-{r:02}-{k:03}-{}", "x".repeat(20));
            e.put(key.as_bytes(), val.as_bytes(), 1 + r * 1000 + k as u128)
                .unwrap();
        }
    }
    run_flush(&mut e, opts);
    assert_eq!(e.run_count(), 1);
    e.rebuild_winner_directory().unwrap();
    let used_before = e.used_segments();
    let plan = e.compact_plan_pressure(opts, None).unwrap().unwrap();
    assert_eq!(plan.merged_inputs.len(), 1);
    assert!(plan.co_compacts(), "pressure plan reclaims the value log");
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    assert!(written.co_compacted);
    e.compact_commit(written).unwrap();
    let used_after = e.used_segments();
    assert!(
        used_after < used_before,
        "victim reclaim frees dead value segments: before={used_before} after={used_after}"
    );
    for k in 0..keys {
        let key = format!("key-{k:03}");
        let want = format!("round-09-{k:03}-{}", "x".repeat(20));
        assert_eq!(
            e.get(key.as_bytes()).unwrap().as_deref(),
            Some(want.as_bytes())
        );
        assert!(matches!(
            e.probe_get(key.as_bytes()),
            StagedGet::WinnerDirectory { .. }
        ));
    }
}

#[test]
fn pressure_floor_overwrite_churn_stays_live_without_zero_gain_merges() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 65536);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_maint_reserve_enabled(true);

    let nkeys = 8000usize;
    let slice = 400usize;
    let mut ver: u128 = 0;
    let mut zero_gain_commits = 0usize;
    let mut terminal = false;
    'outer: for round in 0..800 {
        let mut blocked = false;
        let (lo, hi) = if round == 0 {
            (0, nkeys)
        } else {
            let lo = ((round - 1) * slice) % nkeys;
            (lo, lo + slice)
        };
        for i in lo..hi {
            ver += 1;
            let key = format!("k{i:06}");
            match e.put(key.as_bytes(), &[7u8; 64], ver) {
                Ok(()) => {}
                Err(EngineError::Log(LogError::Backpressure)) => {
                    blocked = true;
                    break;
                }
                Err(other) => panic!("ingest error: {other:?}"),
            }
        }
        if !e.index.is_empty() {
            assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
        }
        while e.run_count() >= 4 {
            match e.compact_plan(RunBuildOpts::default(), None) {
                Ok(Some(plan)) => {
                    let wdev = Arc::new(e.device().sibling().unwrap());
                    let written =
                        vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                    e.compact_commit(written).unwrap();
                }
                _ => break,
            }
        }
        if !blocked {
            continue;
        }
        for _ in 0..8 {
            let free_before = e.free_segments();
            match e.compact_plan_pressure(RunBuildOpts::default(), None) {
                Ok(Some(plan)) => {
                    let wdev = Arc::new(e.device().sibling().unwrap());
                    let written =
                        vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                    e.compact_commit(written).unwrap();
                    if e.free_segments() <= free_before {
                        zero_gain_commits += 1;
                    }
                }
                Ok(None) => break,
                Err(EngineError::InsufficientHeadroom { .. }) => {
                    terminal = true;
                    break;
                }
                Err(other) => panic!("unexpected pressure error: {other:?}"),
            }
        }
        if terminal {
            break 'outer;
        }
    }

    assert!(
        zero_gain_commits <= 3,
        "unbounded zero-gain rewrites: {zero_gain_commits}"
    );
    assert!(
        !terminal,
        "reclaimable rotating-overwrite garbage must not classify terminal"
    );
    assert!(
        ver >= (nkeys + 400 * slice) as u128,
        "intake must keep progressing through floor visits: ver={ver}"
    );
    assert!(
        e.orphan_segments_for_test().is_empty(),
        "rotating-overwrite churn must not accumulate orphaned vlog segments"
    );
    assert_eq!(
        e.used_segments() as usize,
        e.referenced_segments().expect("metadata complete").len(),
        "space accounting must close: used == referenced"
    );
}

#[test]
fn pressure_floor_gainful_victims_reclaim_and_intake_resumes() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    let nkeys = 2000usize;
    for i in 0..nkeys {
        e.put(format!("k{i:06}").as_bytes(), &[1u8; 64], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in (0..nkeys).filter(|i| i % 3 != 0) {
        e.put(
            format!("k{i:06}").as_bytes(),
            &[2u8; 64],
            i as u128 + 1_000_000,
        )
        .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);

    e.set_maint_reserve_enabled(true);
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    assert!(!e.region_exhausted(), "reclaimable garbage stays retryable");

    let free_at_floor = e.free_segments();
    let mut saw_gainful_victims = false;
    for _ in 0..4 {
        match e.compact_plan_pressure(RunBuildOpts::default(), None) {
            Ok(Some(plan)) => {
                if plan.merged_inputs.len() == 1 {
                    assert!(
                        matches!(&plan.vlog_rewrite, VlogRewrite::Victims(v) if v.len() >= 2),
                        "single-run floor plan must carry gainful victims"
                    );
                    saw_gainful_victims = true;
                }
                let free_before = e.free_segments();
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written =
                    vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                let stats = e.compact_commit(written).unwrap();
                assert!(stats.segments_freed > 0);
                if saw_gainful_victims {
                    assert!(
                        e.free_segments() > free_before,
                        "gainful rewrite must net free segments: {} -> {}",
                        free_before,
                        e.free_segments()
                    );
                    break;
                }
            }
            Ok(None) => break,
            Err(EngineError::InsufficientHeadroom { .. }) => break,
            Err(other) => panic!("unexpected pressure error: {other:?}"),
        }
    }
    assert!(saw_gainful_victims, "floor reclaim never planned a victim rewrite");
    assert!(
        e.free_segments() > free_at_floor,
        "floor reclaim must recover net capacity: {} -> {}",
        free_at_floor,
        e.free_segments()
    );
    assert!(!e.at_maintenance_floor(), "net reclaim lifts the engine off the floor");
    e.put(b"probe-resumed", &[9u8; 64], 9_000_001)
        .expect("intake resumes after net reclaim");
}

#[test]
fn floor_multi_run_state_reclaims_via_aged_vlog_gc() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 65536);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    let nkeys = 4000usize;
    let mut ver: u128 = 0;
    for i in 0..nkeys {
        ver += 1;
        e.put(format!("k{i:06}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in (0..nkeys).filter(|i| i % 5 < 3) {
        ver += 1;
        e.put(format!("k{i:06}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    e.merge_policy.force_runs = 2;
    while e.run_count() >= 2 {
        match e.compact_plan(RunBuildOpts::default(), None) {
            Ok(Some(plan)) => {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written =
                    vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
            }
            _ => break,
        }
    }
    std::env::remove_var("VALS_MERGE_FORCE_RUNS");
    assert_eq!(e.run_count(), 1);
    e.merge_policy.size_cap = 32768;
    let base = e.runs[0].meta.handle.clone();
    for i in 0..300usize {
        ver += 1;
        e.put(format!("z{i:06}").as_bytes(), &[9u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);

    e.set_maint_reserve_enabled(true);
    while e.free_segments() > e.maint_reserve_for_test() {
        e.alloc.allocate().unwrap();
    }
    assert!(e.at_maintenance_floor());
    assert!(
        !e.region_exhausted(),
        "a floor state with an affordable aged-vlog GC is not terminal"
    );

    let free_at_floor = e.free_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .unwrap()
        .expect("floor aged-vlog GC plan");
    assert_eq!(plan.merged_inputs.len(), 1);
    assert_eq!(
        plan.merged_inputs[0].handle, base,
        "floor GC must target the base run, not the fresh suffix"
    );
    assert!(matches!(&plan.vlog_rewrite, VlogRewrite::Victims(v) if !v.is_empty()));
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert!(
        e.free_segments() > free_at_floor,
        "floor GC must net-reclaim: {} -> {}",
        free_at_floor,
        e.free_segments()
    );
    assert_eq!(e.run_count(), 2);
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn aged_dead_vlog_gc_reclaims_multi_run_state_pre_floor() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 65536);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let nkeys = 4000usize;
    let mut ver: u128 = 0;
    for i in 0..nkeys {
        ver += 1;
        e.put(format!("k{i:06}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in (0..nkeys).filter(|i| i % 5 < 3) {
        ver += 1;
        e.put(format!("k{i:06}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());

    e.merge_policy.force_runs = 2;
    while e.run_count() >= 2 {
        match e.compact_plan(RunBuildOpts::default(), None) {
            Ok(Some(plan)) => {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written =
                    vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
            }
            _ => break,
        }
    }
    std::env::remove_var("VALS_MERGE_FORCE_RUNS");
    assert_eq!(e.run_count(), 1);
    e.merge_policy.size_cap = 32768;
    let base = e.runs[0].meta.handle.clone();

    for i in 0..400usize {
        ver += 1;
        e.put(format!("z{i:06}").as_bytes(), &[9u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);
    assert!(
        e.orphan_segments_for_test().is_empty(),
        "aged dead vlog must be referenced garbage, not orphans"
    );

    e.set_maint_reserve_enabled(true);
    let minimal = e
        .minimal_pressure_needed_segs()
        .expect("minimal plan computable with value metadata present");
    assert!(
        e.maint_reserve_for_test() >= minimal.min(e.alloc.num_segments() / 2),
        "reserve floor must cover the minimal admissible pressure plan: reserve={} minimal={}",
        e.maint_reserve_for_test(),
        minimal,
    );
    assert!(!e.at_maintenance_floor());

    std::env::set_var("VALS_VLOG_GC", "0");
    let free_before = e.free_segments();
    match e.compact_plan_pressure(RunBuildOpts::default(), None) {
        Ok(None) => {}
        Ok(Some(plan)) => {
            let wdev = Arc::new(e.device().sibling().unwrap());
            let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
            e.compact_commit(written).unwrap();
            assert!(
                e.free_segments() < free_before.saturating_add(10),
                "with VALS_VLOG_GC=0 the suffix plan must not reach the aged dead vlog"
            );
        }
        Err(other) => panic!("unexpected pressure error: {other:?}"),
    }
    std::env::remove_var("VALS_VLOG_GC");

    let free_before = e.free_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .unwrap()
        .expect("aged-vlog GC plan");
    assert_eq!(
        plan.merged_inputs.len(),
        1,
        "aged-vlog GC targets a single run"
    );
    assert_eq!(
        plan.merged_inputs[0].handle, base,
        "aged-vlog GC must target the base run holding the dead vlog"
    );
    assert!(
        matches!(&plan.vlog_rewrite, VlogRewrite::Victims(v) if v.len() >= 32),
        "aged-vlog GC must carry the mostly-dead victims"
    );
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert!(
        e.free_segments() >= free_before + 40,
        "aged-vlog GC must net-reclaim the dead space: {} -> {}",
        free_before,
        e.free_segments()
    );
    assert_eq!(e.run_count(), 2, "the fresh run must survive untouched");
    assert!(e.orphan_segments_for_test().is_empty());
    assert_eq!(
        e.used_segments() as usize,
        e.referenced_segments().expect("metadata complete").len(),
        "space accounting must close: used == referenced"
    );
    ver += 1;
    e.put(b"probe-after-gc", &[1u8; 256], ver)
        .expect("intake continues after aged-vlog GC");
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
#[ignore]
fn shadow_wall_reproduction_without_fold() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_SHADOW_FOLD", "0");
    let mut r = shadow_repro(131072);
    let summary = drive_bootes_shaped_overwrites(&mut r);
    r.floor_autopsy();
    std::env::remove_var("VALS_SHADOW_FOLD");
    println!(
        "without fold: rejections={} max_true_dead={:.3} write_amp={:.2}",
        summary.rejections, summary.max_true_dead, summary.write_amp
    );
    assert!(
        summary.rejections > 0,
        "without the shadow fold the cap-stalled workload must hit the floor"
    );
    assert!(
        summary.max_true_dead >= 0.35,
        "the floor must be reached mostly-dead (the invisible-garbage wall): {:.3}",
        summary.max_true_dead
    );
}

#[test]
#[ignore]
fn shadow_prefloor_reclaim_bounds_occupancy() {
    if co_compact_env(false) { return; }
    let mut r = shadow_repro(131072);
    let summary = drive_bootes_shaped_overwrites(&mut r);
    r.floor_autopsy();
    println!(
        "with fold: rejections={} max_true_dead={:.3} write_amp={:.2} folds={} disarms={}",
        summary.rejections,
        summary.max_true_dead,
        summary.write_amp,
        r.e.shadow_folds(),
        r.e.shadow_fold_disarms(),
    );
    assert_eq!(
        summary.rejections, 0,
        "pre-floor reclaim must keep the workload admitted"
    );
    assert!(
        summary.max_true_dead < 0.35,
        "occupancy must stay bounded well short of floor-mostly-dead: {:.3}",
        summary.max_true_dead
    );
    assert!(
        r.e.shadow_folds() > 0,
        "the bounded occupancy must come from shadow folds actually firing"
    );
    assert!(
        summary.write_amp < 8.0,
        "reclaim churn must stay bounded: write_amp={:.2}",
        summary.write_amp
    );
}

#[test]
fn orphan_sweep_keeps_memtable_referenced_value_segments() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_FORCE_RUNS", "2");
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );

    e.put(b"a0", &[1u8; 6000], 1).unwrap();
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());

    e.put(b"k", &[2u8; 3000], 2).unwrap();
    let straggler_seg = e.index.get(b"k").unwrap().ptr.segment;
    e.put_kind(b"k", &[], 3, true).unwrap();
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);

    e.put(b"x", &[3u8; 500], 4).unwrap();
    assert_eq!(
        e.index.get(b"x").unwrap().ptr.segment,
        straggler_seg,
        "straggler must land in the segment that was open at flush time"
    );
    e.put(b"filler", &[4u8; 8000], 5).unwrap();
    assert_ne!(
        e.index.get(b"filler").unwrap().ptr.segment,
        straggler_seg,
        "the straggler segment must have sealed"
    );

    let plan = e
        .compact_plan(RunBuildOpts::default(), Some(100))
        .unwrap()
        .expect("bottommost fold of both runs");
    assert_eq!(plan.merged_inputs.len(), 2);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_eq!(e.run_count(), 1);
    assert!(
        e.runs[0]
            .meta
            .value_segs
            .as_ref()
            .is_some_and(|vs| !vs.contains(&straggler_seg)),
        "the fold must have dropped the straggler segment from run metadata"
    );

    let _ = e.compact_plan_pressure(RunBuildOpts::default(), None);
    assert!(
        e.seg_allocated_for_test(straggler_seg),
        "orphan sweep freed a segment still referenced by the memtable"
    );
    assert_eq!(
        e.get(b"x").unwrap().as_deref(),
        Some(&[3u8; 500][..]),
        "memtable value must remain readable after the sweep"
    );
    std::env::remove_var("VALS_MERGE_FORCE_RUNS");
}

#[test]
fn shadow_estimator_sees_cross_run_shadowing() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 6144);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let mut ver = 0u128;
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    let base_value_bytes = e.runs[0].meta.value_bytes.unwrap();
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE, "state must clear the estimation gate");

    let base = e.lsm_dead_fraction().unwrap();
    let refreshes = crate::attribution::SHADOW_UNGATED.get();
    let samples = crate::attribution::SHADOW_BLOCKS.get();
    let gate = e.lsm_dead_fraction_gate().unwrap();
    assert_eq!(crate::attribution::SHADOW_UNGATED.get(), refreshes + 1);
    assert!(crate::attribution::SHADOW_BLOCKS.get() > samples);
    let hits = crate::attribution::SHADOW_HITS.get();
    assert_eq!(e.lsm_dead_fraction_gate(), Some(gate));
    assert_eq!(crate::attribution::SHADOW_HITS.get(), hits + 1);
    assert_eq!(crate::attribution::SHADOW_UNGATED.get(), refreshes + 1);
    let est = e.shadowed_bytes_estimate_cached();
    assert!(
        est >= base_value_bytes / 2,
        "fully shadowed base run must be estimated as mostly garbage: est={est} vb={base_value_bytes}"
    );
    assert!(
        gate > base + 0.05,
        "the gate signal must rise above visible dead: base={base} gate={gate}"
    );
}

#[test]
fn shadow_estimator_stays_quiet_on_disjoint_keys() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 6144);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let mut ver = 0u128;
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("a{i:04}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    let base_value_bytes = e.runs[0].meta.value_bytes.unwrap();
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("b{i:04}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE);

    let base = e.lsm_dead_fraction().unwrap();
    let gate = e.lsm_dead_fraction_gate().unwrap();
    let est = e.shadowed_bytes_estimate_cached();
    assert!(
        est <= base_value_bytes / 10,
        "disjoint keys must not read as shadow garbage: est={est} vb={base_value_bytes}"
    );
    assert!(gate <= base + 0.02, "the gate must not open without garbage");

    e.merge_policy.size_cap = 1;
    match e.compact_plan_pressure(RunBuildOpts::default(), None) {
        Ok(Some(plan)) => {
            let inputs = plan.merged_inputs.len();
            let reserved: Vec<SegmentId> = plan
                .segs
                .iter()
                .chain(plan.vlog_segs.iter())
                .copied()
                .collect();
            e.free_compact_segs(&reserved);
            assert_eq!(inputs, 1, "no shadow extension without garbage");
        }
        Ok(None) => {}
        Err(err) => panic!("unexpected pressure error: {err:?}"),
    }
    assert_eq!(e.shadow_folds(), 0, "no shadow fold may be recorded");
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn shadow_fold_extends_pressure_suffix_and_reclaims() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 12288);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let mut ver = 0u128;
    for i in 0..2000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    let base_value_bytes = e.runs[0].meta.value_bytes.unwrap();
    for i in 0..2000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE);

    e.merge_policy.size_cap = 1;
    let live_before = e.total_run_value_bytes();
    let free_before = e.free_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .unwrap()
        .expect("shadow fold plan");
    assert_eq!(
        plan.merged_inputs.len(),
        1,
        "the capped fold rewrites only the shadowed base run"
    );
    assert_eq!(plan.merged_inputs[0].handle, e.runs[0].meta.handle);
    let streamed = plan.read_bytes() - plan.input_bytes();
    assert!(
        streamed > 0 && streamed <= e.runs[1].meta.handle.len,
        "the newer run's keys and versions are streamed read-only as a shadow source: {streamed}"
    );
    assert_eq!(e.shadow_folds(), 1);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();

    let reclaimed = live_before.saturating_sub(e.total_run_value_bytes());
    assert!(
        reclaimed >= base_value_bytes / 2,
        "the fold must drop the shadowed versions: reclaimed={reclaimed} vb={base_value_bytes}"
    );
    assert!(
        e.free_segments() > free_before,
        "wholly dead value segments must be swept at commit: {} -> {}",
        free_before,
        e.free_segments()
    );
    assert_eq!(e.shadow_fold_disarms(), 0, "an accurate prediction must not disarm");
    assert_eq!(e.shadow_backoff_for_test(), 0);
    assert!(
        e.shadow_cooldown_active_for_test(),
        "even a successful fold must set intake spacing"
    );
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn shadow_fold_disarms_on_missed_prediction() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 10240);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for i in 0..300usize {
        e.put(
            format!("k{i:04}").as_bytes(),
            &[7u8; 4000],
            1000 + i as u128,
        )
        .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in 0..300usize {
        e.put(format!("k{i:04}").as_bytes(), &[8u8; 16], 1 + i as u128)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);
    assert!(e.free_fraction() <= SHADOW_EST_FREE_GATE);

    e.merge_policy.size_cap = 1;
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .unwrap()
        .expect("shadow fold plan (overestimated by stale newer versions)");
    assert_eq!(plan.merged_inputs.len(), 1);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();

    assert_eq!(
        e.shadow_fold_disarms(),
        1,
        "a realized gain far below prediction must disarm the trigger"
    );
    assert!(e.shadow_backoff_for_test() >= 1);
    assert!(e.shadow_cooldown_active_for_test());
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
}

#[test]
fn shadow_machinery_leaves_reserve_and_terminal_unchanged() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 6144);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    let mut ver = 0u128;
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[7u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    for i in 0..1000usize {
        ver += 1;
        e.put(format!("k{i:04}").as_bytes(), &[8u8; 256], ver).unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    e.set_maint_reserve_enabled(true);

    let reserve_on = e.maint_reserve_for_test();
    let terminal_on = e.region_exhausted();
    std::env::set_var("VALS_SHADOW_EST", "0");
    std::env::set_var("VALS_SHADOW_FOLD", "0");
    e.refresh_data_reserve();
    assert_eq!(
        reserve_on,
        e.maint_reserve_for_test(),
        "shadow machinery must not price the reserve"
    );
    assert_eq!(
        terminal_on,
        e.region_exhausted(),
        "shadow machinery must not change terminal detection"
    );
    std::env::remove_var("VALS_SHADOW_EST");
    std::env::remove_var("VALS_SHADOW_FOLD");
}

#[test]
fn region_exhausted_clears_when_garbage_appears() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_orphan_reclaim_for_test(false);
    let nkeys = 2000usize;
    for i in 0..nkeys {
        e.put(format!("k{i:06}").as_bytes(), &[1u8; 64], i as u128 + 1)
            .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 1);
    e.set_maint_reserve_enabled(true);
    let mut drained: Vec<SegmentId> = Vec::new();
    while e.free_segments() > e.maint_reserve_for_test() {
        drained.push(e.alloc.allocate().unwrap());
    }
    assert!(e.at_maintenance_floor());
    assert!(e.region_exhausted(), "dense single run at the floor is terminal");
    assert!(matches!(
        e.compact_plan_pressure(RunBuildOpts::default(), None),
        Ok(None) | Err(EngineError::InsufficientHeadroom { .. })
    ));

    for _ in 0..120 {
        if let Some(seg) = drained.pop() {
            e.alloc.free(seg).unwrap();
        }
    }
    for i in (0..nkeys).filter(|i| i % 3 != 0) {
        e.put(
            format!("k{i:06}").as_bytes(),
            &[2u8; 64],
            i as u128 + 1_000_000,
        )
        .unwrap();
    }
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 2);
    while e.free_segments() > e.maint_reserve_for_test() {
        drained.push(e.alloc.allocate().unwrap());
    }
    assert!(e.at_maintenance_floor());
    assert!(
        !e.region_exhausted(),
        "new garbage must clear the terminal classification"
    );

    let mut made_progress = false;
    for _ in 0..4 {
        let free_before = e.free_segments();
        match e.compact_plan_pressure(RunBuildOpts::default(), None) {
            Ok(Some(plan)) => {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written =
                    vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
                if e.free_segments() > free_before {
                    made_progress = true;
                    break;
                }
            }
            Ok(None) => break,
            Err(EngineError::InsufficientHeadroom { .. }) => break,
            Err(other) => panic!("unexpected pressure error: {other:?}"),
        }
    }
    assert!(
        made_progress,
        "reclaim must net free segments once garbage exists"
    );
}

#[test]
fn frozen_runs_keep_overwrite_and_delete_garbage_bounded() {
    if co_compact_env(false) { return; }
    let mut r = shadow_repro(131072);
    let mut live: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for repo in 0..r.nrepos {
        for idx in 0..r.keys_per_repo {
            r.ver += 1;
            let key = format!("r{repo:04}/k{idx:05}").into_bytes();
            let val = r.val.clone();
            r.e.put(&key, &val, r.ver).unwrap();
            live.insert(key);
            r.puts += 1;
            if r.puts.is_multiple_of(64) {
                r.maintain(true).unwrap();
            }
        }
    }
    let force_runs = r.e.merge_policy.force_runs;
    r.e.merge_policy.force_runs = 2;
    while r.e.run_count() >= 2 {
        match run_compact_once(&mut r.e, false) {
            Ok(Some(_)) => {}
            _ => break,
        }
    }
    r.e.merge_policy.force_runs = force_runs;
    let cap = 1024 * 1024u64;
    r.e.merge_policy.size_cap = cap;
    assert!(
        r.e.frozen_run_count() >= 1,
        "the folded base must be frozen under the cap (largest run {} B)",
        r.e.max_run_bytes()
    );
    take_plan_log();

    let dead = |r: &ShadowRepro<MemBlockDevice>, live: usize| {
        let payload = ((r.e.alloc.blocks_per_segment() as usize * r.e.block_size)
            .saturating_sub(SEG_HEADER_LEN)) as u64;
        let capacity = r.e.alloc.num_segments() * payload;
        let used = r.e.alloc.used_segments() * payload;
        let mut held = live as u64 * r.val.len() as u64 + r.e.memtable_bytes();
        for or in &r.e.runs {
            held += or.meta.handle.len;
        }
        used.saturating_sub(held) as f64 / capacity as f64
    };
    let (mut rejections, mut max_dead, mut frozen_samples, mut samples) = (0u32, 0f64, 0u32, 0u32);
    for op in 0..200_000u32 {
        r.ver += 1;
        let key = r.key(true);
        let applied = if op % 5 == 4 {
            r.e.put_kind(&key, b"", r.ver, true).map(|_| live.remove(&key))
        } else {
            let val = r.val.clone();
            r.e.put(&key, &val, r.ver).map(|_| live.insert(key))
        };
        match applied {
            Ok(_) => r.puts += 1,
            Err(_) => rejections += 1,
        }
        if r.puts.is_multiple_of(64) {
            r.maintain(true).unwrap();
            while r.e.recurring_merge_selectable() {
                if run_compact_once(&mut r.e, false).unwrap().is_none() {
                    break;
                }
            }
        }
        if op % 2_000 == 0 {
            max_dead = max_dead.max(dead(&r, live.len()));
            frozen_samples += u32::from(r.e.frozen_run_count() > 0);
            samples += 1;
        }
    }
    r.report("frozen-overwrite-delete");
    let log = take_plan_log();
    let count = |kind: MergeKind| log.iter().filter(|p| p.kind == kind).count();
    println!(
        "dead<={max_dead:.3} folds={} vlog_gc={} pressure={} recurring={} forced={} frozen_samples={frozen_samples}/{samples}",
        count(MergeKind::ShadowFold),
        count(MergeKind::VlogGc),
        count(MergeKind::Pressure),
        count(MergeKind::Recurring),
        count(MergeKind::Forced),
    );
    std::env::remove_var("VALS_MERGE_INPUT_CAP_BYTES");
    assert_eq!(rejections, 0, "reclaim must keep the overwrite/delete workload admitted");
    assert!(max_dead < 0.35, "garbage must stay bounded with frozen runs present: {max_dead:.3}");
    assert!(frozen_samples * 4 >= samples * 3, "frozen runs present for most of the run: {frozen_samples}/{samples}");
    assert!(r.e.shadow_folds() > 0 && count(MergeKind::ShadowFold) > 0, "space reclaim came from shadow folds");
    assert!(
        log.iter().any(|p| matches!(p.kind, MergeKind::ShadowFold | MergeKind::Pressure) && p.frozen_inputs > 0),
        "space-reclaim paths still rewrite frozen runs"
    );
    assert!(count(MergeKind::Recurring) + count(MergeKind::Forced) > 0, "recurring merges ran beside the frozen runs");
    for p in log.iter().filter(|p| matches!(p.kind, MergeKind::Recurring | MergeKind::Forced)) {
        assert!(p.input_bytes <= cap && p.frozen_inputs == 0 && p.paced, "{p:?}");
    }
    for p in log.iter().filter(|p| p.kind == MergeKind::Pressure) {
        assert!(!p.paced, "{p:?}");
    }
    for p in log.iter().filter(|p| p.kind == MergeKind::ShadowFold) {
        assert!(p.input_bytes <= cap || p.frozen_inputs == 1, "shadow folds are capped or one run: {p:?}");
    }
    for key in live.iter().take(200) {
        assert_eq!(r.e.get(key).unwrap().as_deref(), Some(&r.val[..]));
    }
}

#[test]
fn cached_one_run_victim_needs_match_uncached_reference() {
    if co_compact_env(false) { return; }
    fn reference<D: BlockDevice>(e: &Engine<D>, meta: &RunMeta, seg_bytes: usize, headroom: u64) -> Option<u64> {
        reference_open(meta, seg_bytes, headroom, e.log.open_segment())
    }
    fn reference_open(meta: &RunMeta, seg_bytes: usize, headroom: u64, open: Option<SegmentId>) -> Option<u64> {
        type D = MemBlockDevice;
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let threshold = super::super::merge::victim_dead_fraction();
        let (victims, _) = Engine::<D>::select_victims_excluding(std::slice::from_ref(meta), usable as u64, open, threshold)?;
        let lives = Engine::<D>::victim_lives_ascending(std::slice::from_ref(meta), &victims)?;
        let k = Engine::<D>::gainful_victim_prefix_len(&lives, usable)?;
        let live: u64 = lives[..k].iter().map(|&(_, live)| live).sum();
        let vlog = if live == 0 { 0 } else { 2 * (live as usize).div_ceil(usable).max(1) as u64 + 2 };
        Some((meta.handle.len as usize).div_ceil(seg_bytes).max(1) as u64 + headroom + vlog)
    }
    fn check<D: BlockDevice>(e: &Engine<D>) -> (usize, usize) {
        let seg_bytes = e.alloc.blocks_per_segment() as usize * e.block_size;
        let threshold = super::super::merge::victim_dead_fraction();
        let (mut some, mut open_cached) = (0, 0);
        for or in &e.runs {
            for headroom in [0, 3] {
                let want = reference(e, &or.meta, seg_bytes, headroom);
                assert_eq!(e.one_run_victim_needed_segs(or, seg_bytes, headroom, threshold), want);
                some += usize::from(want.is_some());
            }
            if let Some((_, Some(lives))) = &*or.victim_lives.borrow() {
                open_cached += usize::from(lives.iter().any(|&(seg, _)| Some(seg) == e.log.open_segment()));
            }
        }
        (some, open_cached)
    }
    let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 8192), cfg);
    let opts = RunBuildOpts::default();
    let mut version = 1;
    let (mut found, mut open_cached) = (0, 0);
    for round in 0..6u32 {
        for key in 0..120u32 {
            let value = vec![b'v'; 40 + ((key * 7 + round) % 90) as usize];
            e.put(format!("k{:04}", round * 1000 + (key * 13) % 30).as_bytes(), &value, version).unwrap();
            version += 1;
            if key % 40 == 0 {
                let (some, open) = check(&e);
                found += some;
                open_cached += open;
            }
        }
        for _ in 0..9 {
            e.put(format!("hot{round}").as_bytes(), &[b'h'; 90], version).unwrap();
            version += 1;
        }
        run_flush(&mut e, opts).unwrap();
        let (some, open) = check(&e);
        found += some;
        open_cached += open;
    }
    assert!(found > 0, "the scenario must produce gainful one-run victim plans");
    let cached: usize = e.runs.iter().filter_map(|or| {
        or.victim_lives.borrow().as_ref().and_then(|(_, lives)| lives.as_ref()).map(|lives| lives.capacity())
    }).sum();
    assert!(cached > 0);
    let charged = e.capacity_stats().manifest_resident_bytes;
    for or in &e.runs {
        *or.victim_lives.borrow_mut() = None;
    }
    assert_eq!(charged - e.capacity_stats().manifest_resident_bytes, (cached * 16) as u64, "cached victim lists are charged as run metadata");
    check(&e);
    assert!(open_cached > 0, "a cached victim list must include the open segment");
    std::env::set_var("VALS_VICTIM_DEAD_FRACTION", "0.9");
    check(&e);
    std::env::remove_var("VALS_VICTIM_DEAD_FRACTION");
    check(&e);

    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 8192), cfg);
    for key in 0..25u32 {
        e.put(format!("live{key:02}").as_bytes(), &[b'l'; 90], 1).unwrap();
    }
    for version in 1..8 {
        e.put(b"hot", &[b'h'; 90], version).unwrap();
    }
    run_flush(&mut e, opts).unwrap();
    let open = e.log.open_segment();
    assert!(e.runs[0].meta.value_segs.as_ref().unwrap().iter().any(|&seg| Some(seg) == open));
    let seg_bytes = e.alloc.blocks_per_segment() as usize * e.block_size;
    let with_open = reference_open(&e.runs[0].meta, seg_bytes, 0, None);
    assert_ne!(reference(&e, &e.runs[0].meta, seg_bytes, 0), with_open, "the open segment must change the plan");
    assert_eq!(check(&e).1, 1);
    while e.log.open_segment() == open {
        e.put(b"filler", &[b'f'; 90], 1).unwrap();
    }
    check(&e);
    assert_eq!(reference(&e, &e.runs[0].meta, seg_bytes, 0), with_open, "once closed, the cached segment counts");
}
