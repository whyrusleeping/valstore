use super::*;

#[test]
fn open_fresh_device_is_empty() {
    let dev = MemBlockDevice::new(BS, 64);
    let e = Engine::open(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    )
    .unwrap();
    assert!(e.is_empty());
    assert_eq!(e.scan(None, None).unwrap().len(), 0);
}

#[test]
fn recovers_state_after_reopen() {
    let path = temp_path("recover");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 512).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..40u32 {
            e.put(
                format!("key-{i:03}").as_bytes(),
                format!("v{i}").as_bytes(),
                0,
            )
            .unwrap();
        }
        e.put(b"key-000", b"overwritten", 0).unwrap();
        e.delete(b"key-005", 0).unwrap();
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.len(), 39);
    assert_eq!(
        e.get(b"key-000").unwrap().as_deref(),
        Some(&b"overwritten"[..])
    );
    assert_eq!(e.get(b"key-005").unwrap(), None);
    assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"v17"[..]));

    let all = e.scan(None, None).unwrap();
    assert_eq!(all.len(), 39);
    let keys: Vec<_> = all.iter().map(|(k, _)| k.clone()).collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted);

    std::fs::remove_file(&path).ok();
}

#[test]
fn winner_is_permutation_invariant_across_tail_runs_and_reopen() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let candidates = [
        (3u128, false, &b"old"[..]),
        (9u128, true, &b"tomb"[..]),
        (6u128, false, &b"middle"[..]),
    ];
    let permutations = [
        [0usize, 1usize, 2usize],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for permutation in permutations {
        for flush_mask in 0..8u8 {
            let mut e =
                Engine::create_prewiped(MemBlockDevice::new(BS, 1024), cfg);
            for (position, candidate) in permutation.into_iter().enumerate() {
                let (version, tombstone, value) = candidates[candidate];
                e.put_kind(b"k", value, version, tombstone).unwrap();
                if flush_mask & (1 << position) != 0 {
                    e.flush(RunBuildOpts::default()).unwrap();
                }
            }
            assert_eq!(e.version(b"k").unwrap(), Some(9));
            let want = std::collections::HashSet::from([0u32]);
            assert_eq!(
                e.version_digest_page(&want, |_| 0, None, usize::MAX, 0)
                    .unwrap()
                    .0,
                vec![(b"k".to_vec(), 9)]
            );
            e.sync().unwrap();
            let dev = e.device().share();
            drop(e);
            let reopened = Engine::open(dev, cfg).unwrap();
            assert_eq!(reopened.version(b"k").unwrap(), Some(9));
        }
    }
}

#[test]
fn tail_epochs_reflects_recovered_span_before_any_write() {
    let path = temp_path("tail-after-recover");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..60u32 {
            e.put(format!("k{i:03}").as_bytes(), b"vvvvvvvvvvvvvvvv", 0)
                .unwrap();
        }
        e.sync().unwrap();
    }
    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert!(
        e.tail_epochs() > 1,
        "tail_epochs after recovery should reflect the recovered span, got {}",
        e.tail_epochs()
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn requested_directory_rebuild_failure_degrades_and_cannot_be_cleared() {
    if isolated_env_test(&[]) { return; }

    configure_winner_directory(true).unwrap();
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut failing = Engine::create_prewiped(
        ReadFailDevice {
            inner: MemBlockDevice::new(BS, 4096),
            armed: AtomicBool::new(false),
        },
        cfg,
    );
    failing.put(b"k", b"winner", 20).unwrap();
    failing.flush(RunBuildOpts::default()).unwrap();
    failing.rebuild_winner_directory().unwrap();
    assert!(winner_directory_operational());

    failing.device().armed.store(true, AtomicOrdering::Relaxed);
    assert!(failing.rebuild_winner_directory().is_err());
    assert!(!failing.winner_directory_enabled());
    assert!(!winner_directory_operational());

    failing.device().armed.store(false, AtomicOrdering::Relaxed);
    let mut scan = failing.cover_scan_plan().expect("degraded directory needs coverage");
    while !scan.step(failing.device(), 4096).unwrap() {}
    assert!(failing.cover_scan_commit(scan));
    assert_eq!(failing.key_filter_stats().covered_runs, 1);
    for version in 21..24 {
        failing.put_kind(b"deleted", b"", version, true).unwrap();
        failing.flush(RunBuildOpts::default()).unwrap();
        failing.compact_runs(RunBuildOpts::default(), Some(u128::MAX)).unwrap();
        assert!(!failing.winner_directory_enabled(), "merges must not revive a degraded directory");
        assert_eq!(failing.key_filter_stats().covered_runs, 1);
        assert_eq!(failing.key_filter_stats().uncovered_runs, 0);
        assert!(failing.cover_scan_plan().is_none());
        assert_eq!(failing.get(b"k").unwrap(), Some(b"winner".to_vec()));
    }
    assert_eq!(failing.key_filter_stats().cover_scans_committed, 1);

    let mut healthy = Engine::create_prewiped(MemBlockDevice::new(BS, 256), cfg);
    healthy.put(b"other", b"value", 30).unwrap();
    healthy.rebuild_winner_directory().unwrap();
    assert!(healthy.winner_directory_enabled());
    assert!(
        !winner_directory_operational(),
        "a successful rebuild in another engine must not clear degradation"
    );
}

#[test]
fn open_rejects_block_size_mismatch() {
    let path = temp_path("geom");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    {
        let dev = FileBlockDevice::create(&path, BS, 512).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put(b"k", b"v", 0).unwrap();
        e.sync().unwrap();
    }
    let dev = FileBlockDevice::open(&path, BS * 2).unwrap();
    match Engine::open(dev, cfg) {
        Err(EngineError::GeometryMismatch {
            recorded_block_size,
            open_block_size,
        }) => {
            assert_eq!(recorded_block_size, BS as u32);
            assert_eq!(open_block_size, BS * 2);
        }
        Err(other) => panic!("expected GeometryMismatch, got {other:?}"),
        Ok(_) => panic!("open must fail on a block-size mismatch"),
    }
    std::fs::remove_file(&path).ok();
}

#[test]
fn truncate_empties_the_store_and_survives_reopen() {
    let path = temp_path("truncate");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..50u32 {
            e.put(format!("k{i}").as_bytes(), b"value", 0).unwrap();
        }
        e.sync().unwrap();
        assert_eq!(e.len(), 50);
        assert!(e.used_segments() > 0);

        e.truncate().unwrap();

        assert_eq!(e.len(), 0);
        assert!(e.is_empty());
        assert_eq!(e.used_segments(), 0);
        assert_eq!(e.get(b"k0").unwrap(), None);

        e.put(b"fresh", b"after", 0).unwrap();
        e.sync().unwrap();
        assert_eq!(e.get(b"fresh").unwrap().as_deref(), Some(&b"after"[..]));
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.len(), 1);
    assert_eq!(e.get(b"fresh").unwrap().as_deref(), Some(&b"after"[..]));
    for i in 0..50u32 {
        assert_eq!(e.get(format!("k{i}").as_bytes()).unwrap(), None);
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn truncate_empty_is_a_noop() {
    let mut e = engine(Ordering::Unsorted);
    e.truncate().unwrap();
    assert!(e.is_empty());
    assert_eq!(e.used_segments(), 0);
}

#[test]
fn truncate_deep_queues_and_orders_header_wipe_phases() {
    let mut e = queued_truncate_engine();
    let (total, used) = arm_queued_truncate(&e, false);
    assert!(total - used > MAX_INFLIGHT_HEADER_WIPES);

    e.truncate().unwrap();

    let trace = e.device().trace.lock().unwrap();
    assert_eq!(trace.synchronous_writes, 0);
    assert_eq!(trace.submissions, total);
    assert_eq!(trace.max_inflight, MAX_INFLIGHT_HEADER_WIPES);
    assert_eq!(trace.pending_at_flush, vec![0, 0]);
    assert_eq!(trace.submissions_at_flush, vec![total - used, total]);
    assert!(!trace.phase_b_before_phase_a_flush);
}

#[test]
fn reused_segment_does_not_resurrect_block_aligned_stale_records() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mem = MemBlockDevice::new(BS, 64);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(mem, cfg);

    e.put(b"old1", &[1u8; 180], 0).unwrap();
    e.put(b"old2xx", &[2u8; 200], 0).unwrap();
    e.put(b"old3xx", &[3u8; 200], 0).unwrap();
    e.put(b"old4xx", &[4u8; 200], 0).unwrap();
    e.sync().unwrap();
    assert_eq!(e.used_segments(), 1, "test premise: gen 1 fits one segment");

    e.truncate().unwrap();

    e.put(b"new1", &[9u8; 180], 0).unwrap();
    e.sync().unwrap();
    assert_eq!(e.used_segments(), 1, "test premise: segment reused");

    drop(e);
    let e2 = Engine::open(disk, cfg).unwrap();
    assert_eq!(e2.get(b"new1").unwrap().as_deref(), Some(&[9u8; 180][..]));
    for stale in [&b"old2xx"[..], b"old3xx", b"old4xx", b"old1"] {
        assert_eq!(
            e2.get(stale).unwrap(),
            None,
            "stale prior-generation record must not resurrect"
        );
    }
    assert_eq!(e2.len(), 1);
}

#[test]
fn recovery_prefers_highest_seq_across_segments() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let dev = MemBlockDevice::new(BS, 64);
    let mut alloc = Allocator::new(64, 4, 0);

    let mut log1 = ValueLog::new(BS, 4, 5, 0);
    log1.append(&dev, &mut alloc, RecordKind::Put, 0, b"k", b"new")
        .unwrap();
    log1.append(&dev, &mut alloc, RecordKind::Put, 0, b"d", b"kept")
        .unwrap();
    log1.sync(&dev).unwrap();
    let mut log2 = ValueLog::new(BS, 4, 2, 1);
    log2.append(&dev, &mut alloc, RecordKind::Delete, 0, b"d", &[])
        .unwrap();
    log2.append(&dev, &mut alloc, RecordKind::Put, 0, b"k", b"old")
        .unwrap();
    log2.sync(&dev).unwrap();

    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(e.get(b"d").unwrap().as_deref(), Some(&b"kept"[..]));
    assert_eq!(e.len(), 2);
}

#[test]
fn writes_after_recovery_persist_across_a_second_reopen() {
    let path = temp_path("recover2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 512).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put(b"a", b"1", 0).unwrap();
        e.sync().unwrap();
    }
    {
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
        e.put(b"b", b"2", 0).unwrap();
        e.sync().unwrap();
    }
    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(e.get(b"b").unwrap().as_deref(), Some(&b"2"[..]));
    assert_eq!(e.len(), 2);

    std::fs::remove_file(&path).ok();
}

#[test]
fn resume_next_epoch_folds_the_manifest_watermark() {
    assert_eq!(resume_next_epoch(Some(9), Some(5)), 10);
    assert_eq!(resume_next_epoch(Some(3), Some(7)), 8);
    assert_eq!(resume_next_epoch(None, Some(7)), 8);
    assert_eq!(resume_next_epoch(Some(3), None), 4);
    assert_eq!(resume_next_epoch(None, None), 0);
}

#[test]
fn select_capped_suffix_limits_total_and_always_includes_newest() {
    assert_eq!(select_capped_suffix(&[], 100), 0);
    assert_eq!(select_capped_suffix(&[80, 70, 60, 50], 185), 1);
    assert_eq!(select_capped_suffix(&[80, 70, 60, 50], 260), 0);
    assert_eq!(select_capped_suffix(&[80, 300], 100), 1);
}

#[test]
fn select_capped_suffix_can_reach_base_when_outputs_shrink() {
    let mut sizes = vec![90, 80, 70, 60, 50];
    let cap = 190;
    let first = select_capped_suffix(&sizes, cap);
    assert_eq!(first, 2);
    sizes.drain(first..);
    sizes.push(40);
    let second = select_capped_suffix(&sizes, cap);
    assert_eq!(second, 1);
    sizes.drain(second..);
    sizes.push(30);
    assert_eq!(select_capped_suffix(&sizes, cap), 0);
}

#[test]
fn truncate_bumps_manifest_generation() {
    let mut e = engine_with_runs_and_memtable();
    assert!(e.run_count() > 0, "fixture must have runs to wipe");
    let gen_pre = e.manifest_generation();
    e.truncate().unwrap();
    assert!(
        e.manifest_generation() > gen_pre,
        "truncate must bump manifest_generation (monotone): {} !> {}",
        e.manifest_generation(),
        gen_pre
    );
    assert_eq!(e.run_count(), 0, "truncate empties the run set");
}

