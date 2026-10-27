use super::*;

#[test]
fn cancelling_partial_merge_preserves_inputs_and_reopens_cleanly() {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 32768), cfg);
    let opts = RunBuildOpts {
        num_buckets: 64,
        ..Default::default()
    };
    for batch in 0..2 {
        for i in 0..9000u32 {
            e.put(
                format!("k{i:05}").as_bytes(),
                format!("v{batch}-{i}").as_bytes(),
                (batch + 1) as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts).unwrap();
    }
    e.sync().unwrap();
    let inputs = e.run_count();
    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    let reserved = plan.segments();
    let cancel = AtomicBool::new(false);
    let mut yields = 0;
    let wdev = Arc::new(e.device().sibling().unwrap());
    let result = vals_rt::block_on(compact_and_write_cancellable(plan, wdev, &cancel), || {
        yields += 1;
        if yields == 2 {
            cancel.store(true, AtomicOrdering::Relaxed);
        }
    });
    let (segs, error) = match result {
        Err(v) => v,
        Ok(_) => panic!("merge did not cancel"),
    };
    assert!(matches!(error, EngineError::Cancelled));
    assert_eq!(segs, reserved);
    assert!(yields >= 2, "merge must process a chunk before cancellation");
    assert_eq!(e.run_count(), inputs);
    e.free_compact_segs(&segs);
    for seg in segs {
        assert!(e.alloc.is_free(seg));
    }
    assert_eq!(e.get(b"k00042").unwrap().as_deref(), Some(&b"v1-42"[..]));
    let dev = e.device().share();
    drop(e);
    let reopened = Engine::open(dev, cfg).unwrap();
    assert_eq!(reopened.get(b"k00042").unwrap().as_deref(), Some(&b"v1-42"[..]));
    assert_eq!(reopened.get(b"k08999").unwrap().as_deref(), Some(&b"v1-8999"[..]));
}

#[test]
#[ignore]
fn cancelled_rewrites_reclaim_orphans_without_losing_values() {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 32,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts { num_buckets: 64, ..Default::default() };
    for seed in 1..=3u64 {
        let mut rng = seed;
        let mut random = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 262144), cfg);
        let mut oracle = std::collections::BTreeMap::new();
        let mut saw_partial = false;
        for round in 0..3 {
            for batch in 0..2 {
                for _ in 0..18000 {
                    let i = (random() % 18000) as u32;
                    let key = format!("k{i:05}").into_bytes();
                    let value = vec![(random() & 255) as u8; 160 + (random() % 90) as usize];
                    let version = (round * 2 + batch + 1) as u128 * 100_000 + random() as u128;
                    let tombstone = random() % 19 == 0;
                    let value = if tombstone { Vec::new() } else { value };
                    e.put_kind(&key, &value, version, tombstone).unwrap();
                    if e.version(&key).unwrap() == Some(version) {
                        oracle.insert(key, value);
                    }
                }
                run_flush(&mut e, opts).unwrap();
            }
            let mut plan = e.compact_plan(opts, Some(u128::MAX)).unwrap().unwrap();
            assert!(plan.merged_inputs.iter().map(|m| m.entry_count).sum::<u64>() > 8192);
            if round < 2 {
                assert!(matches!(plan.vlog_rewrite, VlogRewrite::All), "seed={seed} round={round}");
            }
            if round % 2 == 1 {
                let victim = *plan.merged_inputs.iter()
                    .flat_map(|m| m.value_segs.as_ref().unwrap().iter())
                    .next().unwrap();
                plan.vlog_rewrite = VlogRewrite::Victims(std::iter::once(victim).collect());
            }
            let reserved = plan.segments();
            let stop_after = if round == 1 { 3 } else { 1 + random() % 3 };
            let cancel = AtomicBool::new(false);
            let mut yields = 0;
            let wdev = Arc::new(e.device().sibling().unwrap());
            let result = vals_rt::block_on(compact_and_write_cancellable(plan, wdev, &cancel), || {
                yields += 1;
                if yields == stop_after {
                    cancel.store(true, AtomicOrdering::Relaxed);
                }
            });
            let (segs, err) = result.err().expect("merge must cancel");
            assert!(matches!(err, EngineError::Cancelled), "seed={seed} round={round}: {err:?}");
            assert_eq!(segs, reserved);
            saw_partial |= yields >= 3;
            if round == 0 {
                let key = b"queued-flush";
                e.put(key, b"a queued value", u128::MAX).unwrap();
                oracle.insert(key.to_vec(), b"a queued value".to_vec());
                let _queued = e.flush_plan(opts).unwrap().unwrap();
            }
            e.sync().unwrap();
            let dev = e.device().share();
            drop(e);
            e = Engine::open(dev, cfg).unwrap();
            assert!(reserved.iter().all(|&s| e.alloc.is_free(s)), "orphan not reclaimed: seed={seed} round={round}");
            for (key, value) in &oracle {
                assert_eq!(e.get(key).unwrap().as_deref(), Some(value.as_slice()),
                           "seed={seed} round={round} key={key:?}");
            }
        }
        assert!(saw_partial, "seed={seed} never cancelled after writing a chunk");
    }
}

#[test]
fn nonblocking_compact_stream_is_read_equivalent_to_materialized_build() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(dev, cfg);

    let opts = RunBuildOpts {
        block_target_bytes: 256,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };

    let batches: [(u32, u32, &str, u128); 3] = [
        (0, 120, "vA", 100_000),
        (60, 180, "vB", 200_000),
        (150, 240, "vC", 300_000),
    ];
    for (lo, hi, tag, base) in batches {
        for i in lo..hi {
            let k = format!("key-{i:03}");
            let v = format!("{tag}-val-{i:03}");
            e.put(k.as_bytes(), v.as_bytes(), base + i as u128).unwrap();
        }
        run_flush(&mut e, opts).unwrap();
    }
    assert!(e.run_count() >= 2, "need multiple runs to merge");

    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    let seg_bytes = plan.seg_bytes;
    let reserved_segs = plan.segs.clone();

    let ref_dev = Arc::new(MemBlockDevice::new(BS, 4096));
    let merged_ref;
    let ref_run;
    let mut ref_alloc = Allocator::new(ref_dev.num_blocks(), cfg.seg_blocks, 0);
    {
        let read_dev = Arc::new(e.device().sibling().unwrap());
        let detached = Allocator::new(
            read_dev.num_blocks(),
            plan.blocks_per_segment,
            plan.reserved_blocks,
        );
        let mut opened = Vec::with_capacity(plan.merged_inputs.len());
        for meta in &plan.merged_inputs {
            opened.push(
                DeviceRun::open(read_dev.clone(), &detached, meta.handle.clone()).unwrap(),
            );
        }
        let (merged, _drp) = merge_runs(&opened, &detached, |_| false).unwrap();
        drop(opened);
        let ref_bytes = run::build_run(&merged, plan.opts);
        let ref_handle = run::write_run(&*ref_dev, &mut ref_alloc, &ref_bytes).unwrap();
        ref_run = DeviceRun::open(ref_dev.clone(), &ref_alloc, ref_handle).unwrap();
        merged_ref = merged;
    }

    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();

    assert_eq!(written.entry_count, merged_ref.len() as u64);
    assert_eq!(written.min_key, merged_ref.first().unwrap().key);
    assert_eq!(written.max_key, merged_ref.last().unwrap().key);
    let n_used = written.handle.segs.len();
    assert!(n_used > 1, "merged run should span multiple segments");
    assert_ne!(
        written.handle.len as usize % seg_bytes,
        0,
        "merged run should have a partial final segment"
    );
    assert!(
        reserved_segs.len() > n_used,
        "reservation should over-provision (an unused tail to free)"
    );

    e.compact_commit(written).unwrap();
    assert_eq!(e.run_count(), 1, "merge collapses all runs into one");

    for &s in &reserved_segs[..n_used] {
        assert!(
            !e.alloc.is_free(s),
            "used merged segment {s:?} must stay allocated"
        );
    }
    for &s in &reserved_segs[n_used..] {
        assert!(
            e.alloc.is_free(s),
            "over-reserved unused segment {s:?} must be freed after commit"
        );
    }

    let streamed = &e.runs[0].run;
    let mut scan = streamed.block_scan(None, None);
    let mut got: Vec<SnapshotEntry> = Vec::new();
    while let Some(entry) = scan.next(&e.alloc).unwrap() {
        got.push(entry);
    }
    assert_eq!(got, merged_ref, "streamed run scan differs from materialized");
    assert_eq!(streamed.entry_count(), ref_run.entry_count());

    for entry in &merged_ref {
        assert_eq!(
            streamed.lookup(&e.alloc, &entry.key).unwrap().as_ref(),
            Some(entry),
            "streamed run must resolve every merged key"
        );
    }
    assert_eq!(streamed.lookup(&e.alloc, b"key-999").unwrap(), None);

    let bk = |k: &[u8]| crate::crc::crc32(k) % opts.num_buckets;
    for b in 0..opts.num_buckets {
        assert_eq!(
            streamed.bucket_digests(&e.alloc, b, bk).unwrap(),
            ref_run.bucket_digests(&ref_alloc, b, bk).unwrap(),
            "bucket {b} digests differ streamed vs materialized"
        );
    }

    for i in 0..240u32 {
        let k = format!("key-{i:03}");
        let want_tag = if i >= 150 {
            "vC"
        } else if i >= 60 {
            "vB"
        } else {
            "vA"
        };
        let want = format!("{want_tag}-val-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(want.as_bytes()),
            "key {k} should read its highest-version winner"
        );
    }
    assert_eq!(e.get(b"key-999").unwrap(), None);
}

#[test]
fn merge_readahead_output_byte_identical_to_scan_path() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts {
        block_target_bytes: 256,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };

    let arm = |readahead: bool| -> Vec<u8> {
        std::env::set_var("VALS_MERGE_READAHEAD", if readahead { "1" } else { "0" });
        let dev = MemBlockDevice::new(BS, 4096);
        let mut e = Engine::create_prewiped(dev, cfg);
        let batches: [(u32, u32, &str, u128); 3] = [
            (0, 120, "vA", 100_000),
            (60, 180, "vB", 200_000),
            (150, 240, "vC", 300_000),
        ];
        for (lo, hi, tag, base) in batches {
            for i in lo..hi {
                let k = format!("key-{i:03}");
                let v = format!("{tag}-val-{i:03}");
                e.put(k.as_bytes(), v.as_bytes(), base + i as u128).unwrap();
            }
            run_flush(&mut e, opts).unwrap();
        }
        assert!(e.run_count() >= 2, "need multiple runs to merge");
        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        let seg_bytes = e.alloc.blocks_per_segment() as usize * BS;
        let rdev = e.device().sibling().unwrap();
        let mut bytes = Vec::new();
        for seg in &written.handle.segs {
            let (lba, _) = e.alloc.segment_range(*seg);
            let mut buf = vec![0u8; seg_bytes];
            rdev.read_at(lba, &mut buf).unwrap();
            bytes.extend_from_slice(&buf);
        }
        bytes.truncate(written.handle.len as usize);
        e.compact_commit(written).unwrap();
        assert_eq!(e.run_count(), 1);
        for i in 0..240u32 {
            let k = format!("key-{i:03}");
            assert!(e.get(k.as_bytes()).unwrap().is_some(), "key {k} lost");
        }
        bytes
    };

    let scan_bytes = arm(false);
    let readahead_bytes = arm(true);
    std::env::remove_var("VALS_MERGE_READAHEAD");
    assert!(!scan_bytes.is_empty());
    assert_eq!(
        readahead_bytes, scan_bytes,
        "merged run must be byte-identical with read-ahead on vs off"
    );
}

#[test]
#[ignore]
fn bench_merge_readahead_file_device() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 512,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let nruns = 4usize;
    let per_run = 100_000usize;

    let arm = |readahead: bool| -> (f64, u64) {
        std::env::set_var("VALS_MERGE_READAHEAD", if readahead { "1" } else { "0" });
        let path = temp_path("bench-merge-readahead");
        let dev =
            vals_spdk::FileBlockDevice::create(&path, BS, 512 * 1024).unwrap();
        let mut e = Engine::create_prewiped(dev, cfg);
        for r in 0..nruns {
            for i in 0..per_run {
                let k = format!("key-{:09}", i * nruns + r);
                let v = format!("value-{r}-{i:09}");
                e.put(k.as_bytes(), v.as_bytes(), (r * per_run + i) as u128 + 1)
                    .unwrap();
            }
            run_flush(&mut e, opts).unwrap();
        }
        assert_eq!(e.run_count(), nruns);
        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        let input_bytes: u64 = plan.merged_inputs.iter().map(|m| m.handle.len).sum();
        let wdev = Arc::new(e.device().sibling().unwrap());
        let t0 = std::time::Instant::now();
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        let dt = t0.elapsed().as_secs_f64();
        assert_eq!(written.entry_count, (nruns * per_run) as u64);
        drop(e);
        let _ = std::fs::remove_file(&path);
        (dt, input_bytes)
    };

    for round in 0..3 {
        let (t_off, bytes) = arm(false);
        let (t_on, _) = arm(true);
        println!(
            "round {round}: merge input {:.1} MiB — readahead OFF {:.3}s ({:.0} MB/s), \
             ON {:.3}s ({:.0} MB/s), speedup {:.2}x (whole compact_and_write incl. write side)",
            bytes as f64 / (1024.0 * 1024.0),
            t_off,
            bytes as f64 / t_off / 1e6,
            t_on,
            bytes as f64 / t_on / 1e6,
            t_off / t_on,
        );
    }
    std::env::remove_var("VALS_MERGE_READAHEAD");
}

#[test]
fn merge_stream_non_ascending_source_fails_closed_through_stream_builder() {
    let src = ScriptSource::new(vec![
        snap_entry(b"k1", 1),
        snap_entry(b"k2", 1),
        snap_entry(b"k1", 2),
    ]);
    let mut stream = MergeStream::new(vec![src], |_: &SnapshotEntry| false);

    let dev = MemBlockDevice::new(BS, 256);
    let mut alloc = Allocator::new(256, 8, 0);
    let seg_bytes = alloc.blocks_per_segment() as usize * BS;
    let segs = run::allocate_run_segs(&mut alloc, BS, 4 * seg_bytes).unwrap();
    let lbas: Vec<u64> = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    let sink = run::SegmentStreamSink::new(&dev, &segs, &lbas, seg_bytes, BS);
    let mut builder = run::RunStreamBuilder::new(sink, RunBuildOpts::default(), 3);

    let err = loop {
        match stream.next() {
            Ok(Some(entry)) => builder.push(&entry),
            Ok(None) => panic!("guard must reject the non-ascending source"),
            Err(e) => break e,
        }
    };
    match err {
        EngineError::Snapshot(SnapshotError::Corrupt(msg)) => {
            assert!(
                msg.contains("strictly ascending"),
                "unexpected corrupt message: {msg}"
            );
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn gc_defers_when_an_older_mutable_candidate_could_resurface() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1024), cfg);
    e.put(b"k", b"old", 1).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"k", b"tomb", 10, true).unwrap();
    e.flush(opts).unwrap();
    e.put(b"k", b"stale", 5).unwrap();

    assert_eq!(e.version(b"k").unwrap(), Some(10));
    let stats = e.compact_runs(opts, Some(u128::MAX)).unwrap();
    assert!(stats.current_dropped_tombstones.is_empty());
    assert_eq!(e.version(b"k").unwrap(), Some(10));

    e.sync().unwrap();
    let dev = e.device().share();
    drop(e);
    let reopened = Engine::open(dev, cfg).unwrap();
    assert_eq!(reopened.version(b"k").unwrap(), Some(10));
    assert_eq!(reopened.get(b"k").unwrap().as_deref(), Some(&b"tomb"[..]));
}

#[test]
fn gc_commit_rejects_a_lower_candidate_added_after_planning() {
    for flush_loser in [false, true] {
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let opts = RunBuildOpts::default();
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1024), cfg);
        e.put_kind(b"k", b"tomb", 10, true).unwrap();
        e.flush(opts).unwrap();
        let plan = e
            .compact_plan_lone_gc(opts, Some(u128::MAX))
            .unwrap()
            .expect("aged lone tombstone plans GC");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();

        e.put(b"k", b"stale", 5).unwrap();
        if flush_loser {
            e.flush(opts).unwrap();
        }
        assert!(matches!(
            e.compact_commit(written),
            Err(EngineError::CompactionConflict)
        ));
        assert_eq!(e.version(b"k").unwrap(), Some(10));
        assert!(!e.compact_in_flight());
    }
}

#[test]
fn tombstone_gc_over_the_input_cap_folds_windows_until_collected() {
    for first_window in [0, 1] {
        over_cap_tombstone_gc_converges(first_window);
    }
}

fn over_cap_tombstone_gc_converges(first_window: usize) {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 16384), cfg);
    for i in 0..300u32 {
        e.put(format!("base{i:04}").as_bytes(), b"v", 1).unwrap();
    }
    e.put(b"victim", b"live", 2).unwrap();
    e.flush(opts).unwrap();
    for i in 0..300u32 {
        e.put(format!("mid{i:04}").as_bytes(), b"v", 3).unwrap();
    }
    e.put_kind(b"victim", b"", 10, true).unwrap();
    e.put_kind(b"solo", b"", 11, true).unwrap();
    e.flush(opts).unwrap();
    for i in 0..300u32 {
        e.put(format!("top{i:04}").as_bytes(), b"v", 4).unwrap();
    }
    e.put_kind(b"fresh", b"", 500, true).unwrap();
    e.flush(opts).unwrap();
    assert_eq!(e.run_count(), 3);
    e.merge_policy.size_cap = e.runs.iter().map(|or| or.meta.handle.len).max().unwrap();
    let gc = Some(100u128);
    e.gc_fold_cursor = first_window;

    let mut passes = 0;
    while e.version(b"victim").unwrap().is_some() || e.version(b"solo").unwrap().is_some() {
        passes += 1;
        assert!(passes <= 2 * 3 + 1, "expired tombstones must converge within two rotations");
        let plan = e.compact_plan_lone_gc(opts, gc).unwrap().expect("an over-cap GC window");
        assert_eq!(plan.kind(), MergeKind::LoneGc);
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
        e.compact_commit(written).unwrap();
        assert_ne!(e.version(b"victim").unwrap(), Some(2), "pass {passes}: the deleted value resurfaced");
    }
    assert_eq!(e.version(b"fresh").unwrap(), Some(500), "a tombstone inside grace stays");
    for key in ["base0007", "mid0123", "top0299"] {
        assert!(e.get(key.as_bytes()).unwrap().is_some(), "{key}");
    }
}

#[test]
fn point_reads_merge_memtable_and_runs() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    e.put(b"a", b"a1", 1).unwrap();
    e.put(b"b", b"b1", 1).unwrap();
    e.put(b"c", b"c1", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"a2", 2).unwrap();
    e.put(b"d", b"d2", 2).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"b", b"b3", 3).unwrap();

    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"a2"[..]));
    assert_eq!(e.get(b"b").unwrap().as_deref(), Some(&b"b3"[..]));
    assert_eq!(e.get(b"c").unwrap().as_deref(), Some(&b"c1"[..]));
    assert_eq!(e.get(b"d").unwrap().as_deref(), Some(&b"d2"[..]));
    assert_eq!(e.get(b"z").unwrap(), None);

    assert_eq!(e.version(b"a").unwrap(), Some(2));
    assert_eq!(e.version(b"b").unwrap(), Some(3));
    assert_eq!(e.version(b"c").unwrap(), Some(1));
    assert_eq!(e.version(b"z").unwrap(), None);

    let got = e.multi_get(&[b"d", b"a", b"z", b"b"]).unwrap();
    assert_eq!(got[0].as_deref(), Some(&b"d2"[..]));
    assert_eq!(got[1].as_deref(), Some(&b"a2"[..]));
    assert_eq!(got[2], None);
    assert_eq!(got[3].as_deref(), Some(&b"b3"[..]));

    let all = e.scan(None, None).unwrap();
    assert_eq!(
        all,
        vec![
            (b"a".to_vec(), b"a2".to_vec()),
            (b"b".to_vec(), b"b3".to_vec()),
            (b"c".to_vec(), b"c1".to_vec()),
            (b"d".to_vec(), b"d2".to_vec()),
        ]
    );

    let mid = e.scan(Some(b"a"), Some(b"c")).unwrap();
    assert_eq!(
        mid,
        vec![
            (b"a".to_vec(), b"a2".to_vec()),
            (b"b".to_vec(), b"b3".to_vec())
        ]
    );
}

#[test]
fn scan_merges_multiple_runs_and_memtable_argmax() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    e.put(b"a", b"a-r1", 1).unwrap();
    e.put(b"c", b"c-r1", 1).unwrap();
    e.put(b"e", b"e-r1", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"a-r2", 2).unwrap();
    e.put(b"b", b"b-r2", 2).unwrap();
    e.put(b"e", b"e-r2", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"d", b"d-mt", 4).unwrap();
    e.put(b"c", b"c-mt", 5).unwrap();

    let all = e.scan(None, None).unwrap();
    assert_eq!(
        all,
        vec![
            (b"a".to_vec(), b"a-r2".to_vec()),
            (b"b".to_vec(), b"b-r2".to_vec()),
            (b"c".to_vec(), b"c-mt".to_vec()),
            (b"d".to_vec(), b"d-mt".to_vec()),
            (b"e".to_vec(), b"e-r2".to_vec()),
        ]
    );
}

#[test]
fn winner_count_page_paginates_distinct_winners_with_tombstones() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let key = |i: u32| format!("k{i:03}").into_bytes();
    for i in 0..40 {
        e.put(&key(i), b"r1", 1).unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap();
    for i in (0..60).step_by(2) {
        e.put_kind(&key(i), b"", 2, i % 4 == 0).unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap();
    for i in (0..80).step_by(3) {
        e.put_kind(&key(i), b"mt", 3, i % 9 == 0).unwrap();
    }
    let distinct: std::collections::BTreeSet<Vec<u8>> = (0..40)
        .chain((0..60).step_by(2))
        .chain((0..80).step_by(3))
        .map(key)
        .collect();
    let (winners, _) = e.scan_winners_page(None, None, usize::MAX, usize::MAX).unwrap();
    assert_eq!(winners.len(), distinct.len());
    assert!(winners.iter().any(|(_, w)| w.tombstone), "fixture must contain winning tombstones");
    for (lo, hi) in [
        (None, None),
        (Some(key(10)), Some(key(50))),
        (Some(key(41)), None),
        (None, Some(key(1))),
        (Some(key(90)), None),
    ] {
        let expected: Vec<&Vec<u8>> = distinct
            .iter()
            .filter(|k| lo.as_ref().is_none_or(|l| *k >= l) && hi.as_ref().is_none_or(|h| *k < h))
            .collect();
        for page in [1usize, 3, 7, 1000] {
            let mut covered = 0usize;
            let mut from = lo.clone();
            let mut pages = 0;
            loop {
                let (n, last, more) = e.winner_count_page(from.as_deref(), hi.as_deref(), page, None).unwrap();
                pages += 1;
                assert!(n <= page);
                covered += n;
                if n > 0 {
                    assert_eq!(last.as_ref(), Some(expected[covered - 1]));
                } else {
                    assert!(last.is_none() && !more);
                }
                if !more {
                    break;
                }
                let mut next = last.unwrap();
                next.push(0);
                from = Some(next);
            }
            assert_eq!(covered, expected.len());
            assert!(pages <= expected.len() / page + 1);
        }
    }
    let past = Some(std::time::Instant::now());
    let (n, last, more) = e.winner_count_page(None, None, 1000, past).unwrap();
    assert_eq!((n, more), (32, true), "an expired deadline ends the page at the first check");
    assert_eq!(last.as_ref(), distinct.iter().nth(31));
    let (rest, _, more) = e.winner_count_page(Some(b"k999"), None, 1000, past).unwrap();
    assert_eq!((rest, more), (0, false));
    let hint = e.range_size_hint(None, None, 4);
    assert!(hint >= 70 && hint <= 70 + 4, "runs by block window plus a capped memtable page: {hint}");
    assert_eq!(e.range_size_hint(Some(&key(50)), Some(&key(10)), 4), 0);
    let mut mem = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    for i in 0..5 {
        mem.put_kind(&key(i), b"", 1, i % 2 == 0).unwrap();
    }
    assert_eq!(mem.winner_count_page(None, None, 3, None).unwrap(), (3, Some(key(2)), true));
    assert_eq!(mem.winner_count_page(Some(b"k002\0"), None, 3, None).unwrap(), (2, Some(key(4)), false));
    assert_eq!(mem.winner_count_page(Some(b"k005"), None, 3, None).unwrap(), (0, None, false));
}

#[test]
fn compaction_is_a_noop_with_runs_and_recovery_stays_correct() {
    let path = temp_path("run-compact");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 2048).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..40u32 {
            e.put(
                format!("key-{i:03}").as_bytes(),
                format!("v{i}-v2").as_bytes(),
                i as u128,
            )
            .unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
        let man = e.live_manifest.clone().unwrap();

        e.put(b"key-000", b"v0-v3", 1000).unwrap();
        e.put(b"key-200", b"new", 1001).unwrap();
        e.sync().unwrap();

        let stats = e.compact().unwrap();
        assert_eq!(stats.segments_freed, 0, "no-op compaction frees nothing");
        assert!(
            man.segs.iter().all(|s| !e.alloc.is_free(*s)),
            "compaction freed manifest chain segments"
        );
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"key-000").unwrap().as_deref(), Some(&b"v0-v3"[..]));
    assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"v17-v2"[..]));
    assert_eq!(e.get(b"key-200").unwrap().as_deref(), Some(&b"new"[..]));

    std::fs::remove_file(&path).ok();
}

#[test]
fn compact_runs_merges_to_one_run_read_transparent() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let bk = |_: &[u8]| 0u32;

    e.put(b"a", b"a1", 1).unwrap();
    e.put(b"b", b"b1", 1).unwrap();
    e.put(b"c", b"c1", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"a2", 2).unwrap();
    e.put(b"c", b"c2", 2).unwrap();
    e.put(b"d", b"d2", 2).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"a3", 3).unwrap();
    e.put(b"e", b"e3", 3).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.run_count(), 3);

    let scan_before = e.scan(None, None).unwrap();
    let want: std::collections::HashSet<u32> = std::iter::once(0u32).collect();
    let digest_before = e
        .version_digest_page(&want, bk, None, usize::MAX, 8)
        .unwrap();
    let versions_before: std::collections::HashMap<Vec<u8>, u128> = ["a", "b", "c", "d", "e"]
        .iter()
        .map(|k| {
            (
                k.as_bytes().to_vec(),
                e.version(k.as_bytes()).unwrap().unwrap(),
            )
        })
        .collect();

    let stats = e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert_eq!(stats.runs_merged, 3);
    assert_eq!(stats.entries_written, 5, "5 distinct keys a..e");
    assert_eq!(e.run_count(), 1, "all runs merged into one");

    assert_eq!(e.runs[0].meta.entry_count, 5);

    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"a3"[..]));
    assert_eq!(e.get(b"b").unwrap().as_deref(), Some(&b"b1"[..]));
    assert_eq!(e.get(b"c").unwrap().as_deref(), Some(&b"c2"[..]));
    assert_eq!(e.get(b"d").unwrap().as_deref(), Some(&b"d2"[..]));
    assert_eq!(e.get(b"e").unwrap().as_deref(), Some(&b"e3"[..]));
    assert_eq!(e.get(b"z").unwrap(), None);

    for (k, v) in &versions_before {
        assert_eq!(e.version(k).unwrap(), Some(*v), "version changed for {k:?}");
    }
    assert_eq!(e.scan(None, None).unwrap(), scan_before, "scan changed");
    assert_eq!(
        e.version_digest_page(&want, bk, None, usize::MAX, 8)
            .unwrap(),
        digest_before,
        "digest page changed"
    );
}

#[test]
fn merge_sources_strictly_ascending_passes_through() {
    let src = ScriptSource::new(vec![
        snap_entry(b"a", 1),
        snap_entry(b"b", 1),
        snap_entry(b"c", 1),
    ]);
    let (merged, _) = merge_sources(vec![src], |_| false).unwrap();
    let keys: Vec<&[u8]> = merged.iter().map(|e| e.key.as_slice()).collect();
    assert_eq!(keys, vec![&b"a"[..], b"b", b"c"]);
}

#[test]
fn merge_sources_cross_source_duplicate_keys_do_not_trip_guard() {
    let older = ScriptSource::new(vec![snap_entry(b"a", 1), snap_entry(b"k", 1)]);
    let newer = ScriptSource::new(vec![snap_entry(b"k", 2), snap_entry(b"z", 2)]);
    let (merged, _) =
        merge_sources(vec![older, newer], |_| false).expect("cross-source dup is legal");
    let kv: Vec<(&[u8], u128)> = merged
        .iter()
        .map(|e| (e.key.as_slice(), e.version))
        .collect();
    assert_eq!(kv, vec![(&b"a"[..], 1), (b"k", 2), (b"z", 2)]);
}

#[test]
fn merge_sources_non_ascending_source_fails_closed() {
    let src = ScriptSource::new(vec![
        snap_entry(b"k1", 1),
        snap_entry(b"k2", 1),
        snap_entry(b"k1", 2),
    ]);
    let err =
        merge_sources(vec![src], |_| false).expect_err("guard must reject the regression");
    match err {
        EngineError::Snapshot(SnapshotError::Corrupt(msg)) => {
            assert!(
                msg.contains("strictly ascending"),
                "unexpected corrupt message: {msg}"
            );
        }
        other => panic!("expected Corrupt, got {other:?}"),
    }
}

#[test]
fn compact_runs_noop_below_two_runs() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    assert_eq!(
        e.compact_runs(RunBuildOpts::default(), None).unwrap(),
        CompactRunsStats::default()
    );
    e.put(b"k", b"v", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.run_count(), 1);
    assert_eq!(
        e.compact_runs(RunBuildOpts::default(), None).unwrap(),
        CompactRunsStats::default()
    );
    assert_eq!(e.run_count(), 1);
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn compact_runs_gcs_aged_tombstone_winner_only_when_cutoff_covers() {
    let opts = RunBuildOpts::default();
    let build = || {
        let dev = MemBlockDevice::new(BS, 4096);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut e = Engine::create_prewiped(dev, cfg);
        e.put_kind(b"live", b"v", 50, false).unwrap();
        e.put_kind(b"old_tomb", b"", 10, true).unwrap();
        e.flush(opts).unwrap();
        e.put_kind(b"fresh_tomb", b"", 200, true).unwrap();
        e.flush(opts).unwrap();
        assert_eq!(e.run_count(), 2);
        e
    };

    let mut e = build();
    let stats = e.compact_runs(opts, Some(100)).unwrap();
    assert_eq!(stats.runs_merged, 2);
    assert_eq!(
        stats.current_dropped_tombstones,
        vec![(b"old_tomb".to_vec(), 10u128)],
        "only the aged tombstone winner is collected + reported as current"
    );
    assert_eq!(
        stats.entries_written, 2,
        "old_tomb dropped from the merged run"
    );
    assert_eq!(
        e.version(b"old_tomb").unwrap(),
        None,
        "old_tomb GC'd from runs"
    );
    assert_eq!(e.version(b"fresh_tomb").unwrap(), Some(200), "fresh kept");
    assert_eq!(e.get(b"live").unwrap().as_deref(), Some(&b"v"[..]));

    let mut e = build();
    let stats = e.compact_runs(opts, None).unwrap();
    assert!(stats.current_dropped_tombstones.is_empty());
    assert_eq!(
        e.version(b"old_tomb").unwrap(),
        Some(10),
        "kept without cutoff"
    );
    assert_eq!(stats.entries_written, 3);

    let mut e = build();
    let stats = e.compact_runs(opts, Some(5)).unwrap();
    assert!(stats.current_dropped_tombstones.is_empty());
    assert_eq!(e.version(b"old_tomb").unwrap(), Some(10));
}

#[test]
fn compact_runs_superseded_tombstone_drop_reports_nothing() {
    let opts = RunBuildOpts::default();
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put_kind(b"k", b"", 10, true).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"k", b"v2", 100, false).unwrap();
    e.flush(opts).unwrap();

    let stats = e.compact_runs(opts, Some(50)).unwrap();
    assert!(
        stats.current_dropped_tombstones.is_empty(),
        "a superseded tombstone drop must not be reported"
    );
    assert_eq!(
        e.version(b"k").unwrap(),
        Some(100),
        "the live winner survives"
    );
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
}

#[test]
fn tombstone_gc_stays_collected_across_reopen() {
    let path = temp_path("tombstone-gc-reopen");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    {
        let dev = FileBlockDevice::create(&path, BS, 2048).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put_kind(b"gone", b"", 10, true).unwrap();
        e.put_kind(b"keep", b"v", 20, false).unwrap();
        e.flush(opts).unwrap();
        e.put_kind(b"filler", &vec![0u8; 900], 25, false).unwrap();
        e.put_kind(b"other", b"w", 30, false).unwrap();
        e.flush(opts).unwrap();
        let stats = e.compact_runs(opts, Some(100)).unwrap();
        assert_eq!(stats.current_dropped_tombstones.len(), 1);
        assert_eq!(e.version(b"gone").unwrap(), None);
        e.sync().unwrap();
    }
    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(
        e.version(b"gone").unwrap(),
        None,
        "stays deleted across recovery"
    );
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(e.get(b"other").unwrap().as_deref(), Some(&b"w"[..]));
}

#[test]
fn forget_keys_over_runs_drops_run_and_memtable_copies() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    e.put_kind(b"flushed", b"v", 10, false).unwrap();
    e.put_kind(b"both", b"old", 11, false).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"flushed2", b"v", 12, false).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"both", b"new", 20, false).unwrap();
    e.put_kind(b"memonly", b"v", 21, false).unwrap();
    e.rebuild_winner_directory().unwrap();

    let mut keys = std::collections::HashSet::new();
    keys.insert(b"flushed".to_vec());
    keys.insert(b"both".to_vec());
    keys.insert(b"memonly".to_vec());
    let stats = e.forget_keys(&keys, opts).unwrap().unwrap();

    assert_eq!(e.version(b"flushed").unwrap(), None);
    assert_eq!(e.version(b"both").unwrap(), None);
    assert_eq!(e.version(b"memonly").unwrap(), None);
    assert_eq!(e.get(b"flushed2").unwrap().as_deref(), Some(&b"v"[..]));
    let mut dropped = stats.dropped.clone();
    dropped.sort();
    assert_eq!(
        dropped,
        vec![
            (b"both".to_vec(), 20u128),
            (b"flushed".to_vec(), 10u128),
            (b"memonly".to_vec(), 21u128),
        ]
    );
}

fn post_manifest_fault_engine() -> (Engine<PostManifestReadFailDevice>, Arc<PostManifestReadFault>) {
    let fault = Arc::new(PostManifestReadFault::default());
    let dev = PostManifestReadFailDevice {
        inner: MemBlockDevice::new(BS, 4096),
        fault: fault.clone(),
    };
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    (Engine::create_prewiped(dev, cfg), fault)
}

#[test]
fn merge_all_runs_keeps_committed_drops_when_the_directory_rebuild_fails() {
    let opts = RunBuildOpts::default();
    let (mut e, fault) = post_manifest_fault_engine();
    e.put_kind(b"live", b"v", 50, false).unwrap();
    e.put_kind(b"old_tomb", b"", 10, true).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"fresh_tomb", b"", 200, true).unwrap();
    e.flush(opts).unwrap();
    e.rebuild_winner_directory().unwrap();
    assert_eq!(e.winner_directory_version(b"old_tomb"), Some(Some(10)));

    fault.arm();
    let merged = e.compact_runs(opts, Some(100));
    assert!(fault.disarm() > 0, "the post-commit directory rebuild read failed");
    let stats = merged.expect("a committed merge reports its drops when the directory rebuild fails");
    assert_eq!(stats.runs_merged, 2);
    assert_eq!(stats.current_dropped_tombstones, vec![(b"old_tomb".to_vec(), 10u128)]);
    assert_eq!(e.run_count(), 1);
    assert!(!e.winner_directory_enabled(), "the stale directory is dropped");
    assert_eq!(e.version(b"old_tomb").unwrap(), None);
    let mut versions = e.recovered_versions().unwrap();
    versions.sort();
    assert_eq!(
        versions,
        vec![(Box::from(&b"fresh_tomb"[..]), 200u128), (Box::from(&b"live"[..]), 50u128)]
    );
}

#[test]
fn compact_commit_keeps_committed_drops_when_the_directory_rebuild_fails() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    configure_winner_directory(true).unwrap();
    let opts = RunBuildOpts::default();
    let (mut e, fault) = post_manifest_fault_engine();
    e.put_kind(b"solo", b"", 11, true).unwrap();
    e.put_kind(b"keep", b"v", 12, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put_kind(b"filler", b"x", 13, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    assert_eq!(e.run_count(), 2);
    assert_eq!(e.winner_directory_version(b"solo"), Some(Some(11)));

    let plan = e.compact_plan(opts, Some(100)).unwrap().unwrap();
    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    fault.arm();
    let committed = e.compact_commit(written);
    assert!(fault.disarm() > 0, "the post-commit directory rebuild read failed");
    let stats = committed.expect("a committed merge reports its drops when the directory rebuild fails");
    assert_eq!(stats.current_dropped_tombstones, vec![(b"solo".to_vec(), 11u128)]);
    assert_eq!(e.run_count(), 1);
    assert!(!e.winner_directory_enabled());
    assert!(!winner_directory_operational(), "the requested directory degrades");
    assert_eq!(e.version(b"solo").unwrap(), None);
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn forget_keys_append_failure_after_the_run_commit_reports_exactly_the_removed_winners() {
    let opts = RunBuildOpts::default();
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let run_only: [(&[u8], u128); 3] = [(b"r0", 10), (b"r1", 11), (b"r2", 12)];
    let mutable: [(&[u8], u128); 3] = [(b"both", 30), (b"m0", 31), (b"m1", 32)];
    let poison: &[u8] = b"m0";
    let (mut unreached_run_key, mut processed_mutable, mut poison_first) = (false, false, false);
    for _ in 0..1_000 {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
        for (k, v) in run_only {
            e.put_kind(k, b"v", v, false).unwrap();
        }
        e.put_kind(b"both", b"old", 13, false).unwrap();
        e.flush(opts).unwrap();
        e.put_kind(b"keep", b"v", 14, false).unwrap();
        e.flush(opts).unwrap();
        for (k, v) in mutable {
            e.put_kind(k, b"new", v, false).unwrap();
        }
        e.rebuild_winner_directory().unwrap();
        e.fail_forget_append_for_test(Some(poison.to_vec()));

        let keys: std::collections::HashSet<Vec<u8>> =
            run_only.iter().chain(&mutable).map(|(k, _)| k.to_vec()).collect();
        let order: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let at = order.iter().position(|k| *k == poison).unwrap();
        let before = &order[..at];
        let mut want: Vec<(Vec<u8>, u128)> = run_only.iter().map(|(k, v)| (k.to_vec(), *v)).collect();
        want.extend(mutable.iter().filter(|(k, _)| before.contains(k)).map(|(k, v)| (k.to_vec(), *v)));
        want.sort();
        unreached_run_key |= run_only.iter().any(|(k, _)| order[at..].contains(k));
        processed_mutable |= mutable.iter().any(|(k, _)| before.contains(k));
        poison_first |= at == 0;

        match e.forget_keys(&keys, opts) {
            Err(EngineError::PartialForget { mut dropped, source }) => {
                dropped.sort();
                assert_eq!(dropped, want, "iteration order {order:?}");
                assert!(source.is_device_full(), "{source}");
            }
            Err(other) => panic!("expected PartialForget, got {other}"),
            Ok(stats) => panic!("expected PartialForget, got {stats:?}"),
        }
        for (k, _) in run_only {
            assert_eq!(e.version(k).unwrap(), None);
        }
        for (k, v) in mutable {
            assert_eq!(e.version(k).unwrap(), (!before.contains(&k)).then_some(v), "{order:?}");
        }
        assert_eq!(e.version(b"keep").unwrap(), Some(14));
        for k in keys.iter().map(|k| k.as_slice()).chain([&b"keep"[..]]) {
            assert_eq!(e.winner_directory_version(k), Some(e.version(k).unwrap()));
        }
        if unreached_run_key && processed_mutable && poison_first {
            break;
        }
    }
    assert!(unreached_run_key && processed_mutable && poison_first);
}

#[test]
fn compact_runs_offcore_gc_current_dropped_respects_survivor() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    e.put_kind(b"revived", b"", 10, true).unwrap();
    e.put_kind(b"solo", b"", 11, true).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put_kind(b"filler", b"x", 12, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    assert_eq!(e.run_count(), 2);

    let plan = e.compact_plan(opts, Some(100)).unwrap().unwrap();
    e.put_kind(b"revived", b"back", 200, false).unwrap();
    run_flush(&mut e, opts).unwrap();

    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();

    assert_eq!(
        stats.current_dropped_tombstones,
        vec![(b"solo".to_vec(), 11u128)],
        "only the truly-current tombstone drop is reported"
    );
    assert_eq!(e.version(b"solo").unwrap(), None);
    assert_eq!(e.get(b"revived").unwrap().as_deref(), Some(&b"back"[..]));
}

#[test]
fn compact_runs_frees_old_segments_which_become_reusable() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    for run in 0..3u32 {
        for i in 0..20u32 {
            let k = format!("k{i:03}");
            e.put(
                k.as_bytes(),
                format!("r{run}v{i}").as_bytes(),
                (run * 100 + i) as u128,
            )
            .unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
    }
    assert_eq!(e.run_count(), 3);
    let used_before = e.used_segments();

    let stats = e.compact_runs(RunBuildOpts::default(), None).unwrap();
    assert!(stats.segments_freed > 0, "old run segments must be freed");
    let used_after = e.used_segments();
    assert!(
        used_after < used_before,
        "used_segments must drop after merge: before {used_before}, after {used_after}"
    );

    e.put(b"k005", b"newest", 9999).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.get(b"k005").unwrap().as_deref(), Some(&b"newest"[..]));
    assert_eq!(e.get(b"k010").unwrap().as_deref(), Some(&b"r2v10"[..]));
}

#[test]
fn compact_runs_reopen_recovers_merged_run() {
    let path = temp_path("compact-runs-reopen");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

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
        assert_eq!(e.run_count(), 3);
        let stats = e.compact_runs(RunBuildOpts::default(), None).unwrap();
        assert_eq!(stats.runs_merged, 3);
        assert_eq!(e.run_count(), 1);
        e.sync().unwrap();
    }

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
    assert_eq!(e.scan(None, None).unwrap().len(), 15);
    std::fs::remove_file(&path).ok();
}

#[test]
fn compact_runs_torn_manifest_commit_keeps_prior_runs_on_reopen() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mem = MemBlockDevice::new(BS, 4096);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(
        ManifestCrashDevice {
            inner: mem,
            armed: AtomicBool::new(false),
        },
        cfg,
    );

    e.put(b"a", b"a1", 1).unwrap();
    e.put(b"b", b"b1", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"a", b"a2", 2).unwrap();
    e.put(b"c", b"c2", 2).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.run_count(), 2);

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    e.compact_runs(RunBuildOpts::default(), None).unwrap();

    drop(e);
    let e2 = Engine::open(disk, cfg).unwrap();
    assert_eq!(e2.get(b"a").unwrap().as_deref(), Some(&b"a2"[..]));
    assert_eq!(e2.get(b"b").unwrap().as_deref(), Some(&b"b1"[..]));
    assert_eq!(e2.get(b"c").unwrap().as_deref(), Some(&b"c2"[..]));
}

#[test]
fn compact_empty_is_noop() {
    let mut e = engine(Ordering::Unsorted);
    assert_eq!(e.compact().unwrap(), CompactStats::default());
}

#[test]
fn compact_filtered_drop_survives_a_crash_before_old_headers_are_wiped() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mem = MemBlockDevice::new(BS, 256);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(
        CrashDevice {
            inner: mem,
            armed: AtomicBool::new(false),
        },
        cfg,
    );

    e.put(b"forgotten", b"stale-value", 0).unwrap();
    e.put(b"keep", b"v", 0).unwrap();
    e.sync().unwrap();

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    let stats = e.compact_filtered(|k, _, _| k == b"forgotten").unwrap();
    assert_eq!(stats.gc_dropped, 1);

    drop(e);
    let e2 = Engine::open(disk, cfg).unwrap();
    assert_eq!(
        e2.get(b"forgotten").unwrap(),
        None,
        "a GC-dropped key must NOT resurrect after a crash mid-compaction"
    );
    assert_eq!(
        e2.get(b"keep").unwrap().as_deref(),
        Some(&b"v"[..]),
        "the survivor is intact"
    );
    assert_eq!(e2.len(), 1);
}

#[test]
fn compact_filtered_drops_flagged_live_records() {
    let mut e = engine(Ordering::Unsorted);
    e.put(b"keep", b"v1", 0).unwrap();
    e.put(b"drop", b"v2", 0).unwrap();
    e.sync().unwrap();

    let stats = e.compact_filtered(|k, _v, _| k == b"drop").unwrap();
    assert_eq!(stats.gc_dropped, 1);
    assert_eq!(stats.live_copied, 1);
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(e.get(b"drop").unwrap(), None);
    assert_eq!(e.len(), 1);

    let s2 = e.compact_filtered(|_, _, _| false).unwrap();
    assert_eq!(s2.gc_dropped, 0);
    assert_eq!(e.get(b"drop").unwrap(), None);
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v1"[..]));
}

#[test]
fn compact_step_bounded_and_equivalent_to_full_pass() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mut full = Engine::create_prewiped(MemBlockDevice::new(BS, 256), cfg);
    let mut step = Engine::create_prewiped(MemBlockDevice::new(BS, 256), cfg);
    fill_dirty_multi_segment(&mut full);
    fill_dirty_multi_segment(&mut step);

    let used_before = step.used_segments();
    assert!(
        used_before > 2,
        "test premise: multi-segment (got {used_before})"
    );

    let full_stats = full.compact().unwrap();
    let full_used = full.used_segments();

    const K: usize = 2;
    let mut steps = 0usize;
    let mut total_freed = 0usize;
    loop {
        let (s, done) = step.compact_step(K).unwrap();
        assert!(
            s.victims_scanned <= K,
            "step scanned {} > K",
            s.victims_scanned
        );
        total_freed += s.segments_freed;
        steps += 1;
        if done {
            break;
        }
        assert!(steps < 1000, "step compaction did not terminate");
    }
    assert!(steps > 1, "expected a multi-step sweep, took {steps}");
    assert!(total_freed > 0, "step sweep reclaimed nothing");

    for k in [
        "hot0", "hot1", "hot2", "hot3", "keepa", "keepb", "keepc", "keepd",
    ] {
        assert_eq!(
            step.get(k.as_bytes()).unwrap(),
            full.get(k.as_bytes()).unwrap(),
            "key {k} diverged"
        );
    }
    assert_eq!(step.get(b"gone").unwrap(), None);
    assert_eq!(step.len(), full.len());

    let step_used = step.used_segments();
    assert!(step_used < used_before, "step pass reclaimed nothing");
    assert!(
        step_used <= full_used + 2,
        "step_used={step_used} full_used={full_used}"
    );
    assert!(full_stats.dead_dropped > 0);
}

#[test]
fn compact_step_gc_matches_full_pass_and_survives_reopen() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mut full = Engine::create_prewiped(MemBlockDevice::new(BS, 256), cfg);
    let step_dev = MemBlockDevice::new(BS, 256);
    let step_disk = step_dev.share();
    let mut step = Engine::create_prewiped(step_dev, cfg);
    let fill = |e: &mut Engine<MemBlockDevice>| {
        e.put(b"keep", b"v1", 0).unwrap();
        e.put(b"aged", b"tomb", 0).unwrap();
        for i in 0..80u32 {
            e.put(b"churn", &vec![7u8; 180], i as u128).unwrap();
        }
        e.sync().unwrap();
    };
    fill(&mut full);
    fill(&mut step);

    let pred = |k: &[u8], _v: u128, _val: &[u8]| k == b"aged";
    let full_stats = full.compact_filtered(pred).unwrap();

    let mut gc_total = 0usize;
    loop {
        let (s, done) = step.compact_step_filtered(K_ONE, pred).unwrap();
        gc_total += s.gc_dropped;
        if done {
            break;
        }
    }
    assert_eq!(gc_total, full_stats.gc_dropped);
    assert_eq!(gc_total, 1, "exactly the aged key GC'd");
    assert_eq!(step.get(b"aged").unwrap(), None);
    assert_eq!(step.get(b"keep").unwrap().as_deref(), Some(&b"v1"[..]));

    drop(step);
    let disk = Engine::open(step_disk, cfg).unwrap();
    assert_eq!(disk.get(b"aged").unwrap(), None);
    assert_eq!(disk.get(b"keep").unwrap().as_deref(), Some(&b"v1"[..]));
}
#[test]
fn failed_compaction_rolls_back_and_a_retry_succeeds() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mem = MemBlockDevice::new(BS, 256);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(
        FailingDevice {
            inner: mem,
            armed: AtomicBool::new(false),
            fail_zero_too: false,
        },
        cfg,
    );

    for i in 0..30u32 {
        e.put(b"hot", format!("v{i}").as_bytes(), 0).unwrap();
    }
    e.put(b"keep", b"stable", 0).unwrap();
    e.sync().unwrap();
    let used_before = e.used_segments();

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    e.compact().unwrap_err();

    assert!(!e.is_poisoned());
    assert_eq!(e.get(b"hot").unwrap().as_deref(), Some(&b"v29"[..]));
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"stable"[..]));
    assert_eq!(
        e.used_segments(),
        used_before,
        "rollback must return the partial new log's segments"
    );

    e.device().armed.store(false, AtomicOrdering::Relaxed);
    e.put(b"hot", b"after-rollback", 0).unwrap();
    e.sync().unwrap();
    let stats = e.compact().unwrap();
    assert!(stats.segments_freed > 0);
    assert_eq!(
        e.get(b"hot").unwrap().as_deref(),
        Some(&b"after-rollback"[..])
    );

    drop(e);
    let e2 = Engine::open(disk, cfg).unwrap();
    assert_eq!(
        e2.get(b"hot").unwrap().as_deref(),
        Some(&b"after-rollback"[..])
    );
    assert_eq!(e2.get(b"keep").unwrap().as_deref(), Some(&b"stable"[..]));
    assert_eq!(e2.len(), 2);
}

#[test]
fn unrollbackable_compaction_poisons_but_reads_and_reopen_survive() {
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mem = MemBlockDevice::new(BS, 256);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(
        FailingDevice {
            inner: mem,
            armed: AtomicBool::new(false),
            fail_zero_too: true,
        },
        cfg,
    );
    for i in 0..10u32 {
        e.put(format!("k{i}").as_bytes(), b"value", 0).unwrap();
    }
    e.put(b"k0", b"latest", 0).unwrap();
    e.sync().unwrap();

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    e.compact().unwrap_err();
    assert!(e.is_poisoned());

    assert!(matches!(e.put(b"x", b"y", 0), Err(EngineError::Poisoned)));
    assert!(matches!(e.delete(b"k1", 0), Err(EngineError::Poisoned)));
    assert!(matches!(e.compact(), Err(EngineError::Poisoned)));
    assert!(matches!(e.truncate(), Err(EngineError::Poisoned)));
    assert_eq!(e.get(b"k0").unwrap().as_deref(), Some(&b"latest"[..]));
    assert_eq!(e.get(b"k5").unwrap().as_deref(), Some(&b"value"[..]));

    drop(e);
    let mut e2 = Engine::open(disk, cfg).unwrap();
    assert!(!e2.is_poisoned());
    assert_eq!(e2.get(b"k0").unwrap().as_deref(), Some(&b"latest"[..]));
    assert_eq!(e2.len(), 10);
    e2.put(b"x", b"y", 0).unwrap();
}

#[test]
fn version_digest_page_merges_memtable_and_runs_argmax() {
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

    for (k, v) in [(b"a", 1u128), (b"b", 1), (b"c", 1), (b"d", 1)] {
        e.put(k, b"x", v).unwrap();
    }
    e.flush(opts).unwrap();
    e.put(b"a", b"x", 5).unwrap();
    e.put(b"e", b"x", 2).unwrap();
    e.flush(opts).unwrap();
    e.put(b"f", b"x", 3).unwrap();
    e.put(b"c", b"x", 9).unwrap();

    let (got, more) = page(&e, &all_buckets(), None, usize::MAX);
    assert!(!more, "unbounded page has no continuation");
    assert_eq!(
        got,
        vec![
            (b"a".to_vec(), 5),
            (b"b".to_vec(), 1),
            (b"c".to_vec(), 9),
            (b"d".to_vec(), 1),
            (b"e".to_vec(), 2),
            (b"f".to_vec(), 3),
        ]
    );
    assert!(got.windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn recurring_merge_selectable_tracks_tiered_policy() {
    if co_compact_env(false) { return; }
    let dev = MemBlockDevice::new(BS, 8192);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    for r in 0..4u32 {
        assert!(
            !e.recurring_merge_selectable(),
            "below min fan-in ({} runs) nothing is selectable",
            e.run_count()
        );
        for i in 0..8u32 {
            e.put(
                format!("k{i:02}").as_bytes(),
                b"value",
                u128::from(r * 100 + i + 1),
            )
            .unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
    }
    assert_eq!(e.run_count(), 4);
    assert!(
        e.recurring_merge_selectable(),
        "min fan-in of similar-size runs is selectable"
    );
    assert!(e
        .compact_plan(RunBuildOpts::default(), None)
        .unwrap()
        .is_some());
    assert!(
        !e.recurring_merge_selectable(),
        "an in-flight compaction is never selectable"
    );
}

#[test]
fn co_compaction_key_orders_values_span_coalesces_and_reads_transparently() {
    if co_compact_env(true) { return; }
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        block_target_bytes: 4096,
        bloom_bits_per_key: 10,
        num_buckets: 64,
    };

    let n = 600u32;
    let mut expected: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for (idx, &k) in shuffled(n, 0x1234).iter().enumerate() {
        let key = format!("key-{k:04}");
        let val = format!("value-for-key-{k:04}-{}", "x".repeat(40));
        e.put(key.as_bytes(), val.as_bytes(), 1000 + k as u128)
            .unwrap();
        expected.insert(key, val);
        if idx % 120 == 119 {
            run_flush(&mut e, opts);
        }
    }
    run_flush(&mut e, opts);
    assert!(e.run_count() >= 2, "need multiple runs to merge");

    let (spans_before, recs_before) = paged_scan_span_count(&e, 64);

    let stats = compact_off(&mut e, opts, None);
    assert_eq!(e.run_count(), 1, "merged to one run");
    assert!(stats.entries_written as u32 == n);

    for (k, v) in &expected {
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(v.as_bytes()),
            "key {k} value after co-compaction"
        );
    }

    let (spans_after, recs_after) = paged_scan_span_count(&e, 64);
    assert_eq!(recs_after as u32, n);
    assert!(
        spans_after * 5 < spans_before,
        "co-compaction should collapse paged-scan spans: \
         before={spans_before}/{recs_before} recs, after={spans_after}/{recs_after} recs"
    );
    assert!(
        recs_after / spans_after.max(1) >= 8,
        "expected dense span packing, got {} recs/span",
        recs_after / spans_after.max(1)
    );
}

#[test]
fn select_merge_suffix_folds_similar_newest_runs_up_to_k() {
    let sizes = vec![100u64; 8];
    let start = select_merge_suffix(&sizes, 4, 1.5, u64::MAX);
    assert_eq!(start, 4, "k=4 selects runs[4..8], leaving the older four");
    assert!(start != 0, "not bottommost");
}

#[test]
fn select_merge_suffix_graduates_base_when_newer_reaches_its_size() {
    let sizes = vec![400u64, 100, 100, 100, 100];
    let start = select_merge_suffix(&sizes, 8, 1.5, u64::MAX);
    assert_eq!(
        start, 0,
        "base graduates once newer cumulative ≈ its own size"
    );
}

#[test]
fn select_merge_suffix_stops_at_a_much_bigger_base() {
    let sizes = vec![10_000u64, 100, 100];
    let start = select_merge_suffix(&sizes, 8, 1.5, u64::MAX);
    assert_eq!(start, 1, "the huge base is excluded (waits for graduation)");
}

#[test]
fn select_merge_suffix_respects_size_cap() {
    let sizes = vec![100u64; 8];
    let start = select_merge_suffix(&sizes, 8, 1.5, 250);
    assert_eq!(start, 6, "cap stops the fold at 2 runs");
}

#[test]
fn select_recurring_merge_skips_lone_pair_and_waits_for_min_fanin() {
    let p = MergePolicy {
        k: 6,
        min_k: 4,
        size_ratio: 1.5,
        size_cap: u64::MAX,
        force_runs: 12,
        freeze_bytes: u64::MAX,
    };

    assert_eq!(
        select_recurring_merge(&[1000, 100], &p),
        None,
        "a lone newest run over a big base must not force-fold"
    );
    assert_eq!(
        select_recurring_merge(&[1000, 100, 100, 100], &p),
        None,
        "3 similar runs is below min fan-in — wait for the 4th"
    );
    assert_eq!(
        select_recurring_merge(&[1000, 100, 100, 100, 100], &p),
        Some(MergeWindow { start: 1, end: 5, forced: false }),
        "4 similar newest runs fold together (fan-in 4), base excluded"
    );
    assert_eq!(
        select_recurring_merge(&[100, 100, 100, 100], &p),
        Some(MergeWindow { start: 0, end: 4, forced: false }),
        "four equal runs fold bottommost at fan-in 4"
    );
}

#[test]
fn select_recurring_merge_valve_bounds_runs_on_pathological_sizes() {
    let p = MergePolicy {
        k: 6,
        min_k: 4,
        size_ratio: 1.5,
        size_cap: u64::MAX,
        force_runs: 20,
        freeze_bytes: u64::MAX,
    };
    let sizes: Vec<u64> = (0..12).map(|i| 1u64 << (12 - i)).collect();
    assert_eq!(
        select_recurring_merge(&sizes, &p),
        None,
        "below the valve a pathological sequence just accumulates"
    );
    let w = select_recurring_merge(&sizes, &MergePolicy { force_runs: 12, ..p })
        .expect("valve must fire at force_runs");
    assert!(w.forced, "the valve path reports itself as forced");
    assert!(
        w.end - w.start >= 2 && w.end - w.start <= p.k,
        "valve folds between 2 and k runs, got {w:?}"
    );
}

#[test]
fn select_recurring_merge_steady_cadence_bounds_runs_and_merge_input() {
    let (k, min_k, ratio, cap) = (6usize, 4usize, 1.5f64, u64::MAX);
    let p = MergePolicy {
        k,
        min_k,
        size_ratio: ratio,
        size_cap: cap,
        force_runs: 12,
        freeze_bytes: u64::MAX,
    };
    const FLUSH: u64 = 100;

    let simulate = |steps: usize, post_fix: bool| -> (f64, usize, usize, usize) {
        let mut runs: Vec<u64> = Vec::new();
        let (mut ingested, mut merge_input) = (0u64, 0u64);
        let (mut max_runs, mut min_fanin, mut valve_trips) = (0usize, usize::MAX, 0usize);
        for _ in 0..steps {
            runs.push(FLUSH);
            ingested += FLUSH;
            loop {
                let (start, end) = if post_fix {
                    match select_recurring_merge(&runs, &p) {
                        Some(w) => {
                            if w.forced {
                                valve_trips += 1;
                            }
                            (w.start, w.end)
                        }
                        None => break,
                    }
                } else {
                    if runs.len() < 2 {
                        break;
                    }
                    (select_merge_suffix(&runs, k, ratio, cap).min(runs.len() - 2), runs.len())
                };
                let fanin = end - start;
                min_fanin = min_fanin.min(fanin);
                let folded: u64 = runs[start..end].iter().sum();
                merge_input += folded;
                runs.splice(start..end, [folded]);
                if !post_fix {
                    break;
                }
            }
            max_runs = max_runs.max(runs.len());
        }
        (merge_input as f64 / ingested as f64, max_runs, min_fanin, valve_trips)
    };

    let (wa_new_100, _, _, _) = simulate(100, true);
    let (wa_new_200, runs_new, min_fanin, valve_trips) = simulate(200, true);
    let (wa_old_100, _, _, _) = simulate(100, false);
    let (wa_old_200, runs_old, _, _) = simulate(200, false);

    assert!(
        min_fanin >= min_k,
        "every fold reaches the minimum fan-in, min was {min_fanin}"
    );
    assert_eq!(valve_trips, 0, "steady same-size cadence must not trip the valve");
    assert!(runs_new <= 10, "run count stays bounded, peaked at {runs_new}");
    assert!(
        wa_old_200 > 40.0 && wa_old_200 > 1.8 * wa_old_100,
        "old clamp WA is O(N²) — grows ~linearly with N: {wa_old_100:.2} → {wa_old_200:.2}"
    );
    assert!(
        wa_new_200 < 6.0,
        "new policy WA stays a small tiered constant: {wa_new_100:.2} → {wa_new_200:.2}"
    );
    let growth_new = wa_new_200 / wa_new_100;
    let growth_old = wa_old_200 / wa_old_100;
    assert!(
        growth_new < growth_old,
        "new policy WA grows slower than old with N: {growth_new:.2}× vs {growth_old:.2}×"
    );
    assert!(
        wa_new_200 < 0.1 * wa_old_200,
        "new policy slashes cumulative merge input >10×: new {wa_new_200:.2} vs old \
         {wa_old_200:.2} (× ingested), old runs peaked {runs_old}"
    );
}

#[test]
fn recurring_merge_bounds_run_count_and_reads_transparently() {
    if co_compact_env(false) { return; }
    std::env::remove_var("VALS_VLOG_CO_COMPACT");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 65536);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    let mut max_runs = 0usize;
    for cycle in 0..40u128 {
        for k in 0..50u32 {
            let key = format!("key-{k:03}");
            e.put(
                key.as_bytes(),
                format!("c{cycle:02}-{k:03}").as_bytes(),
                1 + cycle * 100 + k as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
        while e.run_count() >= 4 {
            if let Some(plan) = e.compact_plan(opts, None).unwrap() {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
            } else {
                break;
            }
        }
        max_runs = max_runs.max(e.run_count());
    }
    assert!(
        max_runs <= 8,
        "run count stays bounded, peaked at {max_runs}"
    );
    for k in 0..50u32 {
        let key = format!("key-{k:03}");
        let want = format!("c39-{k:03}");
        assert_eq!(
            e.get(key.as_bytes()).unwrap().as_deref(),
            Some(want.as_bytes()),
            "key {key} reads newest value after tiered merges"
        );
    }
}

#[test]
fn recurring_merge_cumulative_input_bounded_per_ingested_byte() {
    if co_compact_env(false) { return; }
    std::env::remove_var("VALS_VLOG_CO_COMPACT");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 262144);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    let mut ingested: u64 = 0;
    let mut merge_input: u64 = 0;
    let mut max_runs = 0usize;
    let mut seq = 0u128;
    const CYCLES: u32 = 60;
    for cycle in 0..CYCLES {
        for k in 0..50u32 {
            seq += 1;
            let key = format!("k-{cycle:03}-{k:03}");
            e.put(key.as_bytes(), format!("v-{cycle}-{k:03}").as_bytes(), seq)
                .unwrap();
        }
        run_flush(&mut e, opts);
        ingested += e.runs.last().unwrap().meta.handle.len;
        loop {
            match e.compact_plan(opts, None).unwrap() {
                Some(plan) => {
                    merge_input +=
                        plan.merged_inputs.iter().map(|m| m.handle.len).sum::<u64>();
                    let wdev = Arc::new(e.device().sibling().unwrap());
                    let written =
                        vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                    e.compact_commit(written).unwrap();
                }
                None => break,
            }
        }
        max_runs = max_runs.max(e.run_count());
    }
    assert!(
        max_runs <= 12,
        "resident run count stays bounded under the tiered policy, peaked at {max_runs}"
    );
    let wa = merge_input as f64 / ingested as f64;
    assert!(
        wa > 0.5,
        "the tiered policy must actually fire merges on this growth run (wa={wa:.2})"
    );
    assert!(
        wa < 3.0,
        "cumulative merge input per ingested byte {wa:.2} stays bounded (O(log N) \
         tiered) — the old base-refold clamp grew this with N"
    );
    for cycle in (0..CYCLES).step_by(7) {
        for k in (0..50u32).step_by(11) {
            let key = format!("k-{cycle:03}-{k:03}");
            assert_eq!(
                e.get(key.as_bytes()).unwrap().as_deref(),
                Some(format!("v-{cycle}-{k:03}").as_bytes()),
                "key {key} reads back after tiered merges"
            );
        }
    }
}

#[test]
fn non_bottommost_merge_never_gc_drops_a_tombstone() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_K", "3");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 16384);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    e.put_kind(b"x", b"old", 1, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    for r in 1..5u128 {
        e.put_kind(format!("f{r}").as_bytes(), b"v", 10 + r, false)
            .unwrap();
        run_flush(&mut e, opts).unwrap();
    }
    e.put_kind(b"x", b"", 500, true).unwrap();
    run_flush(&mut e, opts).unwrap();
    assert_eq!(e.run_count(), 6);
    assert_eq!(
        e.version(b"x").unwrap(),
        Some(500),
        "x's current version is the tombstone (v500) before the merge"
    );

    let plan = e.compact_plan(opts, Some(100_000)).unwrap().unwrap();
    assert!(
        plan.merged_inputs.len() < 6,
        "non-bottommost subset (run0 excluded), got {} inputs",
        plan.merged_inputs.len()
    );
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert!(
        stats.current_dropped_tombstones.is_empty(),
        "no tombstone GC on a non-bottommost merge"
    );
    assert_eq!(
        e.version(b"x").unwrap(),
        Some(500),
        "the tombstone (v500) is retained, not the older live copy (v1) — no resurrection"
    );
    assert_ne!(
        e.get(b"x").unwrap().as_deref(),
        Some(&b"old"[..]),
        "x must not resurrect to the older live value"
    );
    std::env::remove_var("VALS_MERGE_TIER_K");
}

#[test]
fn co_compaction_crash_before_commit_recovers_pre_compaction_state() {
    if co_compact_env(true) { return; }
    let path = temp_path("co-compact-crash-pre");
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
    let (used_pre, gen_pre);
    {
        let dev = FileBlockDevice::create(&path, BS, 16384).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for r in 0..4u128 {
            for k in 0..40u32 {
                let key = format!("key-{k:03}");
                let val = format!("round-{r}-{k:03}-{}", "v".repeat(30));
                e.put(key.as_bytes(), val.as_bytes(), 1 + r * 1000 + k as u128)
                    .unwrap();
            }
            run_flush(&mut e, opts);
        }
        e.sync().unwrap();
        used_pre = e.used_segments();
        gen_pre = e.manifest_gen;

        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        let wdev = Arc::new(e.device().sibling().unwrap());
        let _written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    }
    {
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let e = Engine::open(dev, cfg).unwrap();
        assert_eq!(e.manifest_gen, gen_pre, "old manifest still authoritative");
        assert_eq!(
            e.used_segments(),
            used_pre,
            "crashed co-compaction output must be reclaimed to pre-compaction state"
        );
        for k in 0..40u32 {
            let key = format!("key-{k:03}");
            let want = format!("round-3-{k:03}-{}", "v".repeat(30));
            assert_eq!(
                e.get(key.as_bytes()).unwrap().as_deref(),
                Some(want.as_bytes())
            );
        }
    }
}

#[test]
fn co_compaction_error_frees_run_and_vlog_reservations() {
    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        block_target_bytes: 4096,
        bloom_bits_per_key: 10,
        num_buckets: 16,
    };
    for r in 0..3u128 {
        for k in 0..30u32 {
            e.put(
                format!("key-{k:03}").as_bytes(),
                format!("r{r}-{k}").as_bytes(),
                1 + r * 100 + k as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
    }
    let free_before = e.alloc.free_segments();
    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    assert!(e.compact_in_flight());
    assert!(
        e.alloc.free_segments() < free_before,
        "plan reserved segments"
    );
    let reserved = plan.segments();
    e.free_compact_segs(&reserved);
    assert!(!e.compact_in_flight(), "gate cleared");
    assert_eq!(
        e.alloc.free_segments(),
        free_before,
        "both run + vlog reservations freed"
    );
}

#[test]
fn co_compaction_kill_switch_falls_back_to_plain_merge() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    for r in 0..3u128 {
        for k in 0..30u32 {
            e.put(
                format!("key-{k:03}").as_bytes(),
                format!("r{r}-{k:03}").as_bytes(),
                1 + r * 100 + k as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts);
    }
    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    assert!(!plan.co_compacts(), "kill-switch disables co-compaction");
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    assert!(!written.co_compacted);
    e.compact_commit(written).unwrap();
    for k in 0..30u32 {
        let key = format!("key-{k:03}");
        let want = format!("r2-{k:03}");
        assert_eq!(
            e.get(key.as_bytes()).unwrap().as_deref(),
            Some(want.as_bytes())
        );
    }
}


struct IndexFlipDevice {
    inner: MemBlockDevice,
    armed: Arc<std::sync::atomic::AtomicBool>,
}

impl IndexFlipDevice {
    fn footer_at(buf: &[u8]) -> Option<usize> {
        let n = run::FOOTER_LEN;
        (17..=buf.len().saturating_sub(n)).find(|&p| {
            buf[p..p + 4] == run::RUN_MAGIC.to_le_bytes()
                && crate::crc::crc32(&buf[p..p + n - 4]).to_le_bytes() == buf[p + n - 4..p + n]
        })
    }
}

impl BlockDevice for IndexFlipDevice {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        if self.armed.load(std::sync::atomic::Ordering::Relaxed) {
            if let Some(p) = Self::footer_at(buf) {
                self.armed.store(false, std::sync::atomic::Ordering::Relaxed);
                let mut bad = buf.to_vec();
                bad[p - 17] ^= 0x80;
                return self.inner.write_at(lba, &bad);
            }
        }
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        self.inner.flush()
    }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(IndexFlipDevice { inner: self.inner.share(), armed: self.armed.clone() })
    }
}

#[test]
fn merge_output_index_flip_fails_build_verify_and_is_not_published() {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let armed = Arc::new(AtomicBool::new(false));
    let dev = IndexFlipDevice { inner: MemBlockDevice::new(BS, 32768), armed: armed.clone() };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: 64,
        ..Default::default()
    };
    for batch in 0..2 {
        for i in 0..9000u32 {
            e.put(
                format!("k{i:05}").as_bytes(),
                format!("v{batch}-{i}").as_bytes(),
                (batch + 1) as u128,
            )
            .unwrap();
        }
        run_flush(&mut e, opts).unwrap();
    }
    e.sync().unwrap();
    let inputs = e.run_count();
    let free_before = e.alloc.free_segments();
    let (_, failures_before) = run::run_build_verify_totals();
    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    let reserved = plan.segments();
    armed.store(true, AtomicOrdering::Relaxed);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let (segs, error) = match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
        Err(v) => v,
        Ok(_) => panic!("a merge output with a flipped index bit was published"),
    };
    let (_, failures_after) = run::run_build_verify_totals();
    assert!(!armed.load(AtomicOrdering::Relaxed), "index flip was not injected");
    assert!(
        matches!(error, EngineError::Snapshot(SnapshotError::Corrupt("index crc mismatch"))),
        "{error:?}"
    );
    assert!(failures_after > failures_before);
    assert_eq!(segs, reserved);
    assert_eq!(e.run_count(), inputs);
    e.free_compact_segs(&segs);
    assert_eq!(e.alloc.free_segments(), free_before);
    assert_eq!(e.get(b"k00042").unwrap().as_deref(), Some(&b"v1-42"[..]));

    compact_off(&mut e, opts, None);
    assert_eq!(e.run_count(), 1);
    assert_eq!(e.get(b"k08999").unwrap().as_deref(), Some(&b"v1-8999"[..]));
    let dev = e.device().sibling().unwrap();
    drop(e);
    let reopened = Engine::open(dev, cfg).unwrap();
    assert_eq!(reopened.run_count(), 1);
    assert_eq!(reopened.get(b"k00042").unwrap().as_deref(), Some(&b"v1-42"[..]));
}

#[test]
fn merge_output_encode_flips_fail_retryably_and_publish_nothing() {
    use run::encode_test_seams::{self as seams, with_knockouts, ORDER, SOURCE_HASH};

    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let opts = RunBuildOpts::default();
    let key = |i: u32| format!("at://did:plc:tpghvdx6/app.bsky.feed.post/{:06}", i * 10).into_bytes();
    let setup = || {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 32768), cfg);
        for batch in 0..2u32 {
            for i in 0..2000u32 {
                e.put(&key(i), format!("v{batch}-{i}").as_bytes(), (batch + 1) as u128).unwrap();
            }
            run_flush(&mut e, opts).unwrap();
        }
        e.sync().unwrap();
        e
    };
    let last = key(0).len() - 1;
    let field = 12 + key(0).len() + 16;
    type Arm = fn(usize, usize);
    let cases: [(&str, Arm, &str, &str); 3] = [
        ("after-hash", |_, last| seams::arm_stream_entry_flip(700, last, 0x01, false), "source-hash", "stage=source-copy"),
        ("before-hash", |_, _| seams::arm_stream_entry_flip(700, 5, 0x04, true), "order", "not strictly above"),
        ("buffer", |field, _| seams::arm_buffer_flip(0, field, 0x80), "source-hash", "stage=encode"),
    ];
    for (name, arm, check, detail) in cases {
        let mut e = setup();
        let free = e.alloc.free_segments();
        let before = run::encode_integrity_failures()[1].1;
        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        let reserved = plan.segments();
        arm(field, last);
        let wdev = Arc::new(e.device().sibling().unwrap());
        let (segs, err) = match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
            Err(v) => v,
            Ok(_) => panic!("{name}: a merge output with a flipped entry was published"),
        };
        assert!(!seams::stream_entry_flip_armed() && !seams::buffer_flip_armed(), "{name}");
        let enc = err.encode_integrity().unwrap_or_else(|| panic!("{name}: {err}"));
        assert_eq!((enc.path, enc.check), (run::EncodePath::Merge, check), "{name}: {enc}");
        assert!(enc.to_string().contains(detail), "{name}: {enc}");
        if check == "source-hash" {
            assert!(enc.single_bit_xor().is_some(), "{name}: {enc}");
        }
        assert!(run::encode_integrity_failures()[1].1 > before);
        assert_eq!(segs, reserved);
        assert_eq!(e.run_count(), 2);
        e.free_compact_segs(&segs);
        assert_eq!(e.alloc.free_segments(), free);
        compact_off(&mut e, opts, None);
        assert_eq!(e.run_count(), 1);
        for i in [0u32, 700, 1999] {
            assert_eq!(e.get(&key(i)).unwrap().as_deref(), Some(format!("v1-{i}").as_bytes()), "{name}");
        }
    }

    for (knock, arm) in [
        (SOURCE_HASH, cases[0].1),
        (ORDER, cases[1].1),
        (SOURCE_HASH, cases[2].1),
    ] {
        let mut e = setup();
        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        arm(field, last);
        let wdev = Arc::new(e.device().sibling().unwrap());
        let written = with_knockouts(knock, || vals_rt::block_on(compact_and_write(plan, wdev), || {}))
            .unwrap_or_else(|(_, e)| panic!("knockout {knock} still failed: {e}"));
        e.compact_commit(written).unwrap();
        assert_eq!(e.run_count(), 1);
    }
}

struct ReadBytesDevice {
    inner: MemBlockDevice,
    read: Arc<AtomicU64>,
}

impl BlockDevice for ReadBytesDevice {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.read.fetch_add(buf.len() as u64, AtomicOrdering::Relaxed);
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        self.inner.flush()
    }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(ReadBytesDevice { inner: self.inner.share(), read: self.read.clone() })
    }
}

#[test]
fn failed_streamed_merge_stops_reading_its_inputs() {
    use run::encode_test_seams as seams;

    if isolated_env_test(&[
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_MERGE_READAHEAD", Some("0")),
        ("VALS_MERGE_TIER_K", None),
        ("VALS_MERGE_TIER_MIN_K", Some("2")),
        ("VALS_MERGE_FORCE_RUNS", None),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
        ("VALS_SHADOW_FOLD", None),
    ]) {
        return;
    }
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let opts = RunBuildOpts::default();
    let read = Arc::new(AtomicU64::new(0));
    let dev = ReadBytesDevice { inner: MemBlockDevice::new(BS, 131072), read: read.clone() };
    let mut e = Engine::create_prewiped(dev, cfg);
    let key = |i: u32| format!("at://did:plc:tpghvdx6/app.bsky.feed.post/{i:06}").into_bytes();
    for batch in 0..2u32 {
        for i in 0..30000u32 {
            e.put(&key(i * 2 + batch), format!("v{i}").as_bytes(), (batch + 1) as u128).unwrap();
        }
        run_flush(&mut e, opts).unwrap();
    }
    e.sync().unwrap();

    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    let input_bytes: u64 = plan.merged_inputs.iter().map(|m| m.handle.len).sum();
    seams::arm_stream_entry_flip(700, key(0).len() - 1, 0x01, false);
    read.store(0, AtomicOrdering::Relaxed);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let (segs, err) = match vals_rt::block_on(compact_and_write(plan, wdev), || {}) {
        Err(v) => v,
        Ok(_) => panic!("a merge output with a flipped entry was published"),
    };
    let failed_read = read.load(AtomicOrdering::Relaxed);
    assert!(err.encode_integrity().is_some(), "{err}");
    e.free_compact_segs(&segs);

    let plan = e.compact_plan(opts, None).unwrap().unwrap();
    read.store(0, AtomicOrdering::Relaxed);
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap_or_else(|(_, e)| panic!("{e}"));
    let clean_read = read.load(AtomicOrdering::Relaxed);
    e.compact_commit(written).unwrap();
    assert_eq!(e.run_count(), 1);

    assert!(clean_read >= input_bytes, "clean merge read {clean_read} of {input_bytes} input bytes");
    assert!(
        failed_read * 3 < input_bytes,
        "failed merge read {failed_read} of {input_bytes} input bytes (clean merge read {clean_read})"
    );
}

#[test]
fn sync_flush_and_merge_all_check_the_new_run_before_the_manifest() {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let armed = Arc::new(AtomicBool::new(false));
    let dev = IndexFlipDevice { inner: MemBlockDevice::new(BS, 32768), armed: armed.clone() };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    for batch in 0..2 {
        for i in 0..3000u32 {
            e.put(format!("k{i:05}").as_bytes(), format!("v{batch}-{i}").as_bytes(), (batch + 1) as u128)
                .unwrap();
        }
        let free_before = e.alloc.free_segments();
        armed.store(true, AtomicOrdering::Relaxed);
        let err = e.flush(opts).expect_err("a flushed run with a flipped index bit was published");
        assert!(!armed.load(AtomicOrdering::Relaxed));
        assert!(matches!(err, EngineError::Snapshot(SnapshotError::Corrupt("index crc mismatch"))), "{err:?}");
        assert_eq!(e.run_count(), batch);
        assert_eq!(e.alloc.free_segments(), free_before);
        e.flush(opts).unwrap().unwrap();
        assert_eq!(e.run_count(), batch + 1);
    }
    let free_before = e.alloc.free_segments();
    armed.store(true, AtomicOrdering::Relaxed);
    let err = e.compact_runs(opts, None).expect_err("a merged run with a flipped index bit was published");
    assert!(!armed.load(AtomicOrdering::Relaxed));
    assert!(matches!(err, EngineError::Snapshot(SnapshotError::Corrupt("index crc mismatch"))), "{err:?}");
    assert_eq!(e.run_count(), 2);
    assert_eq!(e.alloc.free_segments(), free_before);
    assert_eq!(e.get(b"k00042").unwrap().as_deref(), Some(&b"v1-42"[..]));
    let dev = e.device().sibling().unwrap();
    drop(e);
    let mut reopened = Engine::open(dev, cfg).unwrap();
    assert_eq!(reopened.run_count(), 2);
    reopened.compact_runs(opts, None).unwrap();
    assert_eq!(reopened.run_count(), 1);
    assert_eq!(reopened.get(b"k02999").unwrap().as_deref(), Some(&b"v1-2999"[..]));
}

fn merge_policy_env(force_runs: Option<&str>) -> bool {
    isolated_env_test(&[
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_MERGE_TIER_K", None),
        ("VALS_MERGE_TIER_MIN_K", None),
        ("VALS_MERGE_TIER_SIZE_RATIO", None),
        ("VALS_MERGE_FORCE_RUNS", force_runs),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
        ("VALS_MERGE_FREEZE_BYTES", None),
        ("VALS_MERGE_READAHEAD", None),
        ("VALS_VLOG_GC", None),
        ("VALS_SHADOW_EST", None),
        ("VALS_SHADOW_FOLD", None),
    ])
}

#[test]
fn forced_window_merges_an_inner_window_in_place_and_reads_stay_lww() {
    if merge_policy_env(Some("3")) { return; }
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 65536), cfg);
    let opts = RunBuildOpts::default();
    for i in 0..1000u32 {
        e.put(format!("k{i:05}").as_bytes(), b"a", 1).unwrap();
    }
    run_flush(&mut e, opts).unwrap();
    for i in 0..1000u32 {
        if i == 1 {
            e.put_kind(b"k00001", b"", 2, true).unwrap();
        } else {
            e.put(format!("k{i:05}").as_bytes(), b"b", 2).unwrap();
        }
    }
    run_flush(&mut e, opts).unwrap();
    for i in 500..4500u32 {
        e.put(format!("k{i:05}").as_bytes(), b"c", 3).unwrap();
    }
    run_flush(&mut e, opts).unwrap();
    let handles: Vec<_> = e.runs.iter().map(|or| or.meta.handle.clone()).collect();
    let sizes: Vec<u64> = handles.iter().map(|h| h.len).collect();
    assert_eq!(
        select_recurring_merge(&sizes, &MergePolicy::from_env()),
        Some(MergeWindow { start: 0, end: 2, forced: true }),
        "sizes {sizes:?} must select the older balanced pair, not the newest suffix"
    );

    let plan = e.compact_plan(opts, Some(u128::MAX)).unwrap().expect("forced window plan");
    assert_eq!(plan.kind(), MergeKind::Forced);
    assert!(plan.paced, "bounded forced merges are paced");
    let inputs: Vec<_> = plan.merged_inputs.iter().map(|m| m.handle.clone()).collect();
    assert_eq!(inputs, handles[..2].to_vec());
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert_eq!(stats.runs_merged, 2);
    assert!(stats.current_dropped_tombstones.is_empty(), "an inner window never GCs tombstones");

    assert_eq!(e.run_count(), 2);
    assert_eq!(e.runs[1].meta.handle, handles[2], "the newest run stays newest");
    assert!(
        e.lookup_runs(b"k00001").unwrap().is_some_and(|t| t.tombstone && t.version == 2),
        "the tombstone survives a window that excludes the newest run"
    );
    assert_eq!(e.version(b"k00001").unwrap(), Some(2), "no resurrection of the v1 copy");
    assert_eq!(e.get(b"k00000").unwrap().as_deref(), Some(&b"b"[..]));
    assert_eq!(e.get(b"k00499").unwrap().as_deref(), Some(&b"b"[..]));
    assert_eq!(e.get(b"k00500").unwrap().as_deref(), Some(&b"c"[..]));
    assert_eq!(e.get(b"k04499").unwrap().as_deref(), Some(&b"c"[..]));
    let scanned = e.scan(None, None).unwrap();
    assert!(scanned.len() >= 4499, "scan lost keys: {}", scanned.len());
    assert!(scanned.windows(2).all(|w| w[0].0 < w[1].0), "scan output sorted and unique");
}

#[test]
fn frozen_runs_are_never_recurring_inputs_and_bounded_merges_are_paced() {
    if merge_policy_env(Some("2")) { return; }
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 262144), cfg);
    let opts = RunBuildOpts::default();
    let mut ver = 0u128;
    for i in 0..6000u32 {
        ver += 1;
        e.put(format!("base{i:05}").as_bytes(), b"v", ver).unwrap();
    }
    run_flush(&mut e, opts).unwrap();
    let base = e.runs[0].meta.handle.clone();
    e.merge_policy.size_cap = base.len;
    assert_eq!(e.frozen_run_count(), 1, "a run at the cap is frozen");
    assert_eq!(e.max_run_bytes(), base.len);
    take_plan_log();
    for batch in 0..60u32 {
        for i in 0..150u32 {
            ver += 1;
            let key = if i % 3 == 0 { format!("base{:05}", batch * 50 + i) } else { format!("b{batch:03}-{i:03}") };
            e.put(key.as_bytes(), b"w", ver).unwrap();
        }
        run_flush(&mut e, opts).unwrap();
        while run_compact_once(&mut e, false).unwrap().is_some() {}
        assert_eq!(e.runs[0].meta.handle, base, "batch {batch}: the frozen base was rewritten");
        let sizes: Vec<u64> = e.runs.iter().map(|or| or.meta.handle.len).collect();
        assert!(
            select_recurring_merge(&sizes, &e.merge_policy).is_none(),
            "batch {batch}: merges drained to a fixpoint"
        );
    }
    let log = take_plan_log();
    assert!(log.iter().any(|r| r.kind == MergeKind::Forced), "the valve fired: {log:?}");
    for r in &log {
        assert!(matches!(r.kind, MergeKind::Recurring | MergeKind::Forced), "{r:?}");
        assert!(r.input_bytes <= base.len, "merge input over the cap: {r:?}");
        assert_eq!(r.frozen_inputs, 0, "{r:?}");
        assert!(r.paced, "{r:?}");
    }
    assert!(e.frozen_run_count() >= 1);
    assert!(e.max_run_bytes() >= base.len);
    for batch in 0..60u32 {
        assert_eq!(e.get(format!("b{batch:03}-001").as_bytes()).unwrap().as_deref(), Some(&b"w"[..]));
        assert_eq!(e.get(format!("base{:05}", batch * 50).as_bytes()).unwrap().as_deref(), Some(&b"w"[..]));
    }
    assert_eq!(e.get(b"base05999").unwrap().as_deref(), Some(&b"v"[..]));
}

struct WriteGate {
    open: std::sync::Mutex<bool>,
    cv: std::sync::Condvar,
    arrived: std::sync::atomic::AtomicBool,
}

struct GatedDev {
    inner: MemBlockDevice,
    gate: Option<Arc<WriteGate>>,
    shared_gate: Arc<WriteGate>,
}

impl BlockDevice for GatedDev {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }
    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        if let Some(gate) = &self.gate {
            gate.arrived.store(true, std::sync::atomic::Ordering::SeqCst);
            let mut open = gate.open.lock().unwrap();
            while !*open {
                open = gate.cv.wait(open).unwrap();
            }
        }
        self.inner.write_at(lba, buf)
    }
    fn flush(&self) -> vals_spdk::Result<()> {
        Ok(())
    }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(GatedDev { inner: self.inner.share(), gate: Some(self.shared_gate.clone()), shared_gate: self.shared_gate.clone() })
    }
}

#[test]
fn truncate_waits_for_an_in_flight_off_core_merge() {
    if merge_policy_env(Some("2")) { return; }
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let gate = Arc::new(WriteGate {
        open: std::sync::Mutex::new(false),
        cv: std::sync::Condvar::new(),
        arrived: std::sync::atomic::AtomicBool::new(false),
    });
    let dev = GatedDev { inner: MemBlockDevice::new(BS, 16384), gate: None, shared_gate: gate.clone() };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts::default();
    for round in 0..3u128 {
        for i in 0..300u32 {
            e.put(format!("old{i:05}").as_bytes(), &[round as u8; 48], round * 1000 + i as u128 + 1).unwrap();
        }
        let plan = e.flush_plan(opts).unwrap().unwrap();
        let ungated = GatedDev { inner: e.device().inner.share(), gate: None, shared_gate: gate.clone() };
        let written = vals_rt::block_on(build_and_write_flush(plan, &ungated), || {}).unwrap();
        e.flush_commit(written).unwrap();
    }
    let plan = e.compact_plan(opts, None).unwrap().expect("a merge plan");
    let wdev = Arc::new(e.device().sibling().unwrap());
    let merge = std::thread::spawn(move || vals_rt::block_on(compact_and_write(plan, wdev), || {}));
    while !gate.arrived.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::yield_now();
    }
    let truncated = e.truncate();
    let fresh = |e: &mut Engine<GatedDev>| {
        for i in 0..600u32 {
            e.put(format!("new{i:05}").as_bytes(), format!("fresh-{i:05}-{}", "x".repeat(40)).as_bytes(), 10_000 + i as u128)
                .unwrap();
        }
    };
    if truncated.is_ok() {
        fresh(&mut e);
    }
    *gate.open.lock().unwrap() = true;
    gate.cv.notify_all();
    let committed = match merge.join().unwrap() {
        Ok(written) => e.compact_commit(written).map(|_| ()),
        Err((segs, err)) => {
            e.free_compact_segs(&segs);
            Err(err)
        }
    };
    if truncated.is_err() {
        committed.unwrap();
        e.truncate().unwrap();
        fresh(&mut e);
    }
    for i in 0..600u32 {
        assert_eq!(
            e.get(format!("new{i:05}").as_bytes()).ok().flatten().as_deref(),
            Some(format!("fresh-{i:05}-{}", "x".repeat(40)).as_bytes()),
            "post-truncate data corrupted by the in-flight merge (truncate {truncated:?})"
        );
    }
    assert_eq!(e.get(b"old00001").unwrap(), None);
    e.storage_audit_for_test().unwrap();
    assert!(matches!(truncated, Err(EngineError::MaintenanceInFlight)), "{truncated:?}");
}

#[test]
fn offcore_forget_commit_refreshes_an_unrequested_directory_and_keeps_failed_forgets_live() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    assert!(!winner_directory_requested());
    let opts = RunBuildOpts::default();
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    e.put_kind(b"gone", b"v", 10, false).unwrap();
    e.put_kind(b"keep", b"v", 11, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put_kind(b"filler", b"x", 12, false).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put_kind(b"mem-gone", b"v", 20, false).unwrap();
    e.put_kind(b"mem-stuck", b"v", 21, false).unwrap();
    e.rebuild_winner_directory().unwrap();
    assert_eq!(e.winner_directory_version(b"gone"), Some(Some(10)));
    assert_eq!(e.winner_directory_version(b"mem-gone"), Some(Some(20)));
    e.fail_forget_append_for_test(Some(b"mem-stuck".to_vec()));

    let forget = std::collections::HashMap::from([
        (b"gone".to_vec(), 10u128),
        (b"mem-gone".to_vec(), 20u128),
        (b"mem-stuck".to_vec(), 21u128),
    ]);
    let failures = COMMIT_FORGET_FAILURES.load(AtomicOrdering::Relaxed);
    let plan = e.forget_plan(opts, &forget).unwrap().unwrap();
    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert_eq!(e.run_count(), 1);
    assert!(e.winner_directory_enabled());
    assert_eq!(e.winner_directory_version(b"gone"), Some(None), "a forgotten run key leaves the hand-built directory");
    assert_eq!(e.winner_directory_version(b"mem-gone"), Some(None), "a forgotten memtable key leaves the directory");
    assert_eq!(e.version(b"gone").unwrap(), None);
    assert_eq!(e.version(b"mem-gone").unwrap(), None);
    assert!(stats.current_dropped_tombstones.contains(&(b"mem-gone".to_vec(), 20u128)));
    assert!(!stats.current_dropped_tombstones.iter().any(|(k, _)| k == b"mem-stuck"));
    assert_eq!(e.version(b"mem-stuck").unwrap(), Some(21), "a failed forget append leaves the key live");
    assert_eq!(e.winner_directory_version(b"mem-stuck"), Some(Some(21)));
    assert_eq!(COMMIT_FORGET_FAILURES.load(AtomicOrdering::Relaxed), failures + 1);
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn a_key_forgotten_out_of_a_run_stays_forgotten_after_reopen_with_its_record_in_the_replayed_tail() {
    if co_compact_env(false) { return; }
    let opts = RunBuildOpts::default();
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    e.put_kind(b"gone", b"v", 10, false).unwrap();
    e.put_kind(b"keep", b"v", 11, false).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"late", b"v", 12, false).unwrap();
    let forget = std::collections::HashMap::from([(b"gone".to_vec(), 10u128)]);
    let plan = e.forget_plan(opts, &forget).unwrap().unwrap();
    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_eq!(e.version(b"gone").unwrap(), None);
    e.sync().unwrap();
    let dev = e.device().share();
    drop(e);

    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.version(b"gone").unwrap(), None, "a forgotten run key must not be replayed back from the tail");
    assert_eq!(e.get(b"keep").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(e.get(b"late").unwrap().as_deref(), Some(&b"v"[..]), "an unflushed tail record is still replayed");
    assert_eq!(e.len(), 1, "only records after the flush watermark return to the memtable");
}

#[test]
fn a_key_forgotten_out_of_a_run_above_the_flush_watermark_stays_forgotten_after_reopen() {
    if co_compact_env(false) { return; }
    let opts = RunBuildOpts::default();
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let forget = |e: &mut Engine<MemBlockDevice>, key: &[u8], version: u128, sync_path: bool| -> Result<(), EngineError> {
        if sync_path {
            let keys = std::collections::HashSet::from([key.to_vec()]);
            e.forget_keys(&keys, opts)?.unwrap();
        } else {
            let plan = e.forget_plan(opts, &std::collections::HashMap::from([(key.to_vec(), version)]))?.unwrap();
            let wdev = Arc::new(e.device().sibling().expect("sibling"));
            let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).map_err(|(_, e)| e)?;
            e.compact_commit(written)?;
        }
        Ok(())
    };
    let reopen = |mut e: Engine<MemBlockDevice>| {
        e.sync().unwrap();
        let dev = e.device().share();
        drop(e);
        Engine::open(dev, cfg).unwrap()
    };
    for sync_path in [false, true] {
        let mut single = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
        single.put_kind(b"gone", b"v", 10, false).unwrap();
        single.flush(opts).unwrap();
        assert_eq!(single.last_flush_seq, 0, "premise: the flush covers only seq 0");
        forget(&mut single, b"gone", 10, sync_path).unwrap();
        let single = reopen(single);
        assert_eq!(single.version(b"gone").unwrap(), None, "forgotten seq 0 under a covered_seq 0 manifest (sync path: {sync_path})");

        let mut paged = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
        paged.put_kind(b"a", b"v", 10, false).unwrap();
        paged.put_kind(b"z", b"v", 11, false).unwrap();
        assert!(matches!(paged.flush_plan_step(opts, 1).unwrap(), Some(FlushPlanStep::Pending)));
        paged.put_kind(b"z", b"new", 12, false).unwrap();
        let Some(FlushPlanStep::Ready(plan)) = paged.flush_plan_step(opts, 1).unwrap() else { panic!("expected a ready flush plan") };
        let wdev = paged.device().sibling().expect("sibling");
        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
        paged.flush_commit(written).unwrap();
        assert!(paged.last_flush_seq < 2, "premise: z's seq 2 is above the flush watermark");
        assert_eq!(paged.mutable_entry(b"z").map(|m| m.seq), Some(2), "a paged flush leaves a write past its watermark in the memtable");
        forget(&mut paged, b"z", 12, sync_path).unwrap();
        let paged = reopen(paged);
        assert_eq!(paged.version(b"z").unwrap(), None, "a forgotten write made past the paged flush's watermark (sync path: {sync_path})");
        assert_eq!(paged.get(b"a").unwrap().as_deref(), Some(&b"v"[..]));

        let mut cold = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
        cold.put_kind(b"z", b"winner", 11, false).unwrap();
        cold.flush(opts).unwrap();
        cold.put_kind(b"a", b"v", 12, false).unwrap();
        cold.put_kind(b"b", b"v", 13, false).unwrap();
        assert!(matches!(cold.flush_plan_step(opts, 1).unwrap(), Some(FlushPlanStep::Pending)));
        cold.put_kind(b"z", b"stale", 9, false).unwrap();
        let Some(FlushPlanStep::Ready(plan)) = cold.flush_plan_step(opts, 2).unwrap() else { panic!("expected a ready flush plan") };
        let wdev = cold.device().sibling().expect("sibling");
        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
        cold.flush_commit(written).unwrap();
        assert_eq!(cold.mutable_entry(b"z").map(|m| m.version), Some(9), "premise: the cold stale write stays in the memtable");
        match forget(&mut cold, b"z", 11, sync_path) {
            Ok(()) | Err(EngineError::CompactionConflict) => {}
            Err(e) => panic!("cold forget: {e}"),
        }
        let live = cold.version(b"z").unwrap();
        let cold = reopen(cold);
        assert_eq!(cold.version(b"z").unwrap(), live, "a reopen after the forget reads what the live engine read (sync path: {sync_path})");

        let mut blind = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
        blind.put_kind(b"a", b"v", 10, false).unwrap();
        blind.put_kind(b"z", b"winner", 11, false).unwrap();
        assert!(matches!(blind.flush_plan_step(opts, 1).unwrap(), Some(FlushPlanStep::Pending)));
        let next_seq = blind.log.next_seq();
        blind.put_kind(b"z", b"stale", 9, false).unwrap();
        assert_eq!(blind.log.next_seq(), next_seq, "a put that loses to the memtable winner is not logged");
        assert_eq!(blind.mutable_entry(b"z").map(|m| m.version), Some(11), "premise: the stale append loses to the memtable winner");
        let Some(FlushPlanStep::Ready(plan)) = blind.flush_plan_step(opts, 1).unwrap() else { panic!("expected a ready flush plan") };
        let wdev = blind.device().sibling().expect("sibling");
        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
        blind.flush_commit(written).unwrap();
        assert!(blind.mutable_entry(b"z").is_none());
        forget(&mut blind, b"z", 11, sync_path).unwrap();
        let blind = reopen(blind);
        assert_eq!(blind.version(b"z").unwrap(), None, "a losing append past the watermark stays dead (sync path: {sync_path})");
    }
}

#[test]
fn a_losing_append_past_the_flush_watermark_stays_dead_after_its_tombstone_is_collected_and_reopen() {
    if co_compact_env(false) { return; }
    let opts = RunBuildOpts::default();
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    e.put_kind(b"b", b"v", 8, false).unwrap();
    e.flush(opts).unwrap();
    e.put_kind(b"a", b"v", 10, false).unwrap();
    e.put_kind(b"z", b"", 11, true).unwrap();
    assert!(matches!(e.flush_plan_step(opts, 1).unwrap(), Some(FlushPlanStep::Pending)));
    e.put_kind(b"z", b"stale", 9, false).unwrap();
    let Some(FlushPlanStep::Ready(plan)) = e.flush_plan_step(opts, 1).unwrap() else { panic!("expected a ready flush plan") };
    let wdev = e.device().sibling().expect("sibling");
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
    e.flush_commit(written).unwrap();
    e.put_kind(b"c", b"v", 13, false).unwrap();
    let stats = e.compact_runs(opts, Some(u128::MAX)).unwrap();
    assert!(stats.current_dropped_tombstones.contains(&(b"z".to_vec(), 11u128)), "premise: the tombstone is collected");
    assert_eq!(e.get(b"z").unwrap(), None);
    e.sync().unwrap();
    let dev = e.device().share();
    drop(e);

    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"z").unwrap(), None, "the stale value must not come back over the collected tombstone");
    assert_eq!(e.get(b"c").unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn gc_rewrite_refuses_a_wrong_record_pointer_unpublished_and_excludes_the_segment() {
    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    std::env::set_var("VALS_VICTIM_DEAD_FRACTION", "0");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts {
        num_buckets: 16,
        ..Default::default()
    };
    let key = |k: u32| format!("key-{k:03}").into_bytes();
    let ptr = |e: &Engine<MemBlockDevice>, k: &[u8]| {
        e.plan_get(k).unwrap().unwrap().ptr()
    };
    for (case, reason) in [("key", "record key mismatch"), ("version", "record version mismatch")] {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 8192), cfg);
        for k in 0..40u32 {
            e.put(&key(k), format!("r0-{k:03}").as_bytes(), 1).unwrap();
        }
        let wrong = if case == "key" {
            e.index.get(b"key-031").unwrap().ptr
        } else {
            let stale = e.index.get(b"key-007").unwrap().ptr;
            e.put(b"key-007", b"r1-007", 2).unwrap();
            stale
        };
        e.set_mutable_ptr_for_test(b"key-007", wrong);
        run_flush(&mut e, opts).unwrap();
        for k in 40..80u32 {
            e.put(&key(k), format!("r1-{k:03}").as_bytes(), 3).unwrap();
        }
        run_flush(&mut e, opts).unwrap();
        let (runs, generation) = (e.run_count(), e.manifest_gen);
        let tail_before = ptr(&e, &key(79));

        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        assert!(matches!(plan.vlog_rewrite, VlogRewrite::All), "{case}");
        let wdev = Arc::new(e.device().sibling().unwrap());
        let (segs, err) = match vals_rt::block_on(compact_and_write(plan, wdev.clone()), || {}) {
            Err(v) => v,
            Ok(_) => panic!("{case}: the merge rewrote a wrong-record pointer"),
        };
        let found = err.corrupt_value_record().unwrap_or_else(|| panic!("{case}: {err:?}"));
        assert_eq!(found, (&b"key-007"[..], wrong, reason), "{case}");
        e.free_compact_segs(&segs);
        assert_eq!((e.run_count(), e.manifest_gen), (runs, generation), "{case}: nothing published");
        assert_eq!(ptr(&e, b"key-007"), wrong, "{case}");

        let metas: Vec<RunMeta> = e.runs.iter().map(|or| or.meta.clone()).collect();
        let usable = (e.alloc.blocks_per_segment() as usize * BS - SEG_HEADER_LEN) as u64;
        assert!(e.select_victims(&metas, usable).unwrap().0.contains(&wrong.segment), "{case}");
        assert!(e.exclude_from_value_gc(wrong.segment));
        assert!(!e.exclude_from_value_gc(wrong.segment), "{case}: excluded once");
        assert!(!e.select_victims(&metas, usable).unwrap().0.contains(&wrong.segment), "{case}");

        let plan = e.compact_plan(opts, None).unwrap().unwrap();
        let written = vals_rt::block_on(compact_and_write(plan, wdev), || {})
            .unwrap_or_else(|(_, err)| panic!("{case}: other segments must still rewrite: {err}"));
        e.compact_commit(written).unwrap();
        assert_eq!(e.run_count(), 1, "{case}");
        assert_eq!(ptr(&e, b"key-007"), wrong, "{case}: the damaged pointer is kept, never laundered");
        let read = e.get(b"key-007").unwrap_err().to_string();
        assert!(read.contains(reason), "{case}: reads still fail: {read}");
        assert_ne!(ptr(&e, &key(79)), tail_before, "{case}: values outside the excluded segment rewrite");
        for k in (0..80u32).filter(|&k| k != 7) {
            let round = if k < 40 { 0 } else { 1 };
            assert_eq!(e.get(&key(k)).unwrap(), Some(format!("r{round}-{k:03}").into_bytes()), "{case} {k}");
        }
    }
}
