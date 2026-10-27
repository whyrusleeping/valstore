use super::*;

#[test]
fn shared_prefix_bit_flip_aborts_flush_without_writing_a_run() {
    for sliced in [false, true] {
        let mut e = engine(Ordering::Sorted);
        for i in 0..3 {
            let key = format!("at://did:plc:22niu5w3iw4cghdpdqomndhd/app.bsky.feed.post/3mee7ygcz4k2b\0at://did:plc:{i:024x}/app.bsky.feed.like/3mee7ygcz4k2b\0subject.uri");
            assert_eq!(key.as_bytes()[73], b':');
            e.put(key.as_bytes(), b"value", i).unwrap();
        }
        e.index.flip_packed_bit(0, 2 + 73, 0x10);
        let altered = e.index.scan(None, None).unwrap();
        assert!(altered.iter().all(|(key, _)| key[73] == b'*'));
        let before = crate::index::integrity_failures();
        let used = e.used_segments();
        let error = if sliced {
            e.flush_plan_step(RunBuildOpts::default(), 1).err().unwrap()
        } else {
            e.flush(RunBuildOpts::default()).unwrap_err()
        };
        let integrity = error.memtable_integrity().expect("memtable integrity error");
        assert_eq!(integrity.finding.bit_flip_xor(), Some(0x10));
        assert!(error.to_string().contains("front-coded index integrity mismatch block_first_key=["));
        assert!(crate::index::integrity_failures() > before);
        assert_eq!(e.index.len(), 3);
        assert!(e.runs.is_empty());
        assert_eq!(e.used_segments(), used);
        assert!(!e.flush_in_flight);
        assert!(e.flush_snapshot.is_none());
        assert!(e.flush_value_segs.is_empty());
    }
}

#[test]
fn integrity_failure_cleans_up_a_partial_flush_snapshot() {
    let mut e = engine(Ordering::Sorted);
    for i in 0..100 {
        e.put(format!("shared/{i:04}").as_bytes(), b"value", i).unwrap();
    }
    assert!(matches!(e.flush_plan_step(RunBuildOpts::default(), 1).unwrap(),
        Some(FlushPlanStep::Pending)));
    e.index.flip_packed_bit(1, 1 + 3, 0x10);
    let used = e.used_segments();
    assert!(e.flush_plan(RunBuildOpts::default()).is_err());
    assert!(!e.flush_in_flight);
    assert!(e.flush_snapshot.is_none());
    assert!(!e.flush_cleanup.is_empty());
    assert!(e.flush_value_segs.is_empty());
    assert!(e.runs.is_empty());
    assert_eq!(e.used_segments(), used);
    assert_eq!(e.index.len(), 100);
    assert!(e.flush_plan(RunBuildOpts::default()).is_err());
}

#[test]
fn persistent_memtable_flip_is_diagnosed_and_reopen_replays_the_intact_value_log() {
    let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted };
    let mem = MemBlockDevice::new(BS, 256);
    let disk = mem.share();
    let mut e = Engine::create_prewiped(mem, cfg);
    let keys: Vec<Vec<u8>> = (0..40)
        .map(|i| format!("did:plc:sim36rxr2peojm7mthz5us7s/{i:03}").into_bytes())
        .collect();
    for (i, key) in keys.iter().enumerate() {
        e.put(key, format!("value-{i}").as_bytes(), 0).unwrap();
    }
    e.sync().unwrap();
    assert_eq!(keys[0][19], b'o');
    e.flip_memtable_bit_for_test(0, 1 + 19, 0x10);
    for _ in 0..4 {
        let error = e.flush_plan(RunBuildOpts::default()).err().unwrap();
        let integrity = error.memtable_integrity().expect("memtable integrity error");
        assert_eq!(integrity.first_key.as_ref(), keys[0].as_slice());
        assert_eq!(integrity.entries, 40);
        match &integrity.finding {
            crate::index::IntegrityFinding::BitFlip { region, key_index, offset, actual, expected, candidates, .. } => {
                assert_eq!((*region, *key_index, *offset, *actual, *expected, *candidates), ("key", 0, 19, 0x7f, b'o', 1));
            }
            other => panic!("unexpected finding {other:?}"),
        }
        assert!(error.to_string().contains("region=key key_index=0 offset=19 actual=0x7f expected=0x6f xor=0x10"), "{error}");
        assert!(e.runs.is_empty());
    }
    drop(e);
    let mut e = Engine::open(disk, cfg).unwrap();
    assert!(e.flush(RunBuildOpts::default()).unwrap().is_some());
    assert_eq!(e.runs.len(), 1);
    assert!(e.index.is_empty());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(e.get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
        let mut flipped = key.clone();
        flipped[19] = 0x7f;
        assert_eq!(e.get(&flipped).unwrap(), None);
    }
}

fn encode_flip_engine(keys: &[Vec<u8>]) -> Engine<MemBlockDevice> {
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 8192), cfg);
    for (i, key) in keys.iter().enumerate() {
        e.put(key, format!("value-{i}").as_bytes(), 1).unwrap();
    }
    e.sync().unwrap();
    e
}

fn encode_flip_keys() -> Vec<Vec<u8>> {
    (0..120).map(|i| format!("at://did:plc:tpghvdx6/app.bsky.feed.post/{:05}", i * 10).into_bytes()).collect()
}

fn failed_flush(e: &mut Engine<MemBlockDevice>, sliced: bool) -> EngineError {
    if sliced {
        let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
        let wdev = e.device().sibling().unwrap();
        let (segs, err) = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {})
            .err()
            .expect("a flush with a flipped source entry was published");
        e.free_flush_segs(&segs);
        err
    } else {
        e.flush(RunBuildOpts::default()).expect_err("a flush with a flipped source entry was published")
    }
}

fn flush_ok(e: &mut Engine<MemBlockDevice>, sliced: bool) {
    if sliced {
        run_flush(e, RunBuildOpts::default()).unwrap();
    } else {
        e.flush(RunBuildOpts::default()).unwrap().unwrap();
    }
}

#[test]
fn flush_source_flips_after_the_memtable_check_abort_retryably_and_publish_nothing() {
    let keys = encode_flip_keys();
    let last = keys[60].len() - 1;
    for sliced in [true, false] {
        for (byte, mask, check) in [(last, 0x01u8, "source-hash"), (5, 0x04, "order")] {
            let mut e = encode_flip_engine(&keys);
            let free = e.alloc.free_segments();
            let before = run::encode_integrity_failures()[0].1;
            e.set_flush_source_flip_for_test(Some((60, byte, mask)));
            for _ in 0..2 {
                let err = failed_flush(&mut e, sliced);
                assert!(err.memtable_integrity().is_none());
                let enc = err.encode_integrity().expect("encode integrity error");
                assert_eq!((enc.path, enc.check), (run::EncodePath::Flush, check), "{enc}");
                if check == "source-hash" {
                    assert!(enc.detail.contains("stage=source"), "{enc}");
                    assert_eq!(enc.entry, 60);
                    assert_eq!(enc.single_bit_xor(), Some(mask));
                    assert!(enc.to_string().contains(&format!("region=key key_index=60 offset={last}")), "{enc}");
                } else {
                    assert_eq!(enc.entry, 60);
                    assert!(enc.detail.contains("not strictly above the previous encoded key"), "{enc}");
                }
                assert!(err.to_string().starts_with("run encode integrity mismatch path=flush"));
                assert!(e.runs.is_empty());
                assert_eq!(e.index.len(), keys.len());
                assert_eq!(e.alloc.free_segments(), free);
                assert!(!e.flush_in_flight);
            }
            assert!(run::encode_integrity_failures()[0].1 >= before + 2);
            e.set_flush_source_flip_for_test(None);
            flush_ok(&mut e, sliced);
            assert_eq!(e.runs.len(), 1);
            assert!(e.index.is_empty());
            for (i, key) in keys.iter().enumerate() {
                assert_eq!(e.get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
            }
            let mut flipped = keys[60].clone();
            flipped[byte] ^= mask;
            assert_eq!(e.get(&flipped).unwrap(), None);
        }
    }
}

#[test]
fn flush_encode_buffer_flip_before_the_block_crc_aborts_retryably() {
    let keys = encode_flip_keys();
    let offset_field = 12 + keys[0].len() + 8;
    for sliced in [true, false] {
        let mut e = encode_flip_engine(&keys);
        let free = e.alloc.free_segments();
        run::encode_test_seams::arm_buffer_flip(0, offset_field, 0x01);
        let err = failed_flush(&mut e, sliced);
        assert!(!run::encode_test_seams::buffer_flip_armed());
        let enc = err.encode_integrity().expect("encode integrity error");
        assert_eq!((enc.path, enc.check, enc.block, enc.entry), (run::EncodePath::Flush, "source-hash", 0, 0));
        assert!(enc.detail.contains("stage=encode"), "{enc}");
        assert!(enc.to_string().contains("region=entry.offset key_index=0 offset=8"), "{enc}");
        assert_eq!(enc.single_bit_xor(), Some(0x01));
        assert!(e.runs.is_empty());
        assert_eq!(e.alloc.free_segments(), free);
        flush_ok(&mut e, sliced);
        assert_eq!(e.runs.len(), 1);
        assert_eq!(e.get(&keys[0]).unwrap().as_deref(), Some(&b"value-0"[..]));
    }
}

#[test]
fn knocked_out_flush_encode_checks_publish_the_corrupt_run() {
    use run::encode_test_seams::{with_knockouts, ORDER, SOURCE_HASH};
    let keys = encode_flip_keys();
    let last = keys[60].len() - 1;
    let mut e = encode_flip_engine(&keys);
    e.set_flush_source_flip_for_test(Some((60, last, 0x01)));
    with_knockouts(SOURCE_HASH, || run_flush(&mut e, RunBuildOpts::default())).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.index.keys(), vec![keys[60].clone()]);
    let mut flipped = keys[60].clone();
    flipped[last] ^= 0x01;
    let err = e.get(&flipped).unwrap_err().to_string();
    assert!(err.contains("record key mismatch"), "{err}");

    let mut e = encode_flip_engine(&keys);
    e.set_flush_source_flip_for_test(Some((60, 5, 0x04)));
    let err = with_knockouts(ORDER, || failed_flush(&mut e, true));
    assert_eq!(err.encode_integrity().unwrap().check, "source-hash");
    with_knockouts(ORDER | SOURCE_HASH, || run_flush(&mut e, RunBuildOpts::default())).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert_eq!(e.index.keys(), vec![keys[60].clone()]);

    let mut e = encode_flip_engine(&keys);
    run::encode_test_seams::arm_buffer_flip(0, 12 + keys[0].len() + 8, 0x01);
    with_knockouts(SOURCE_HASH, || run_flush(&mut e, RunBuildOpts::default())).unwrap();
    assert_eq!(e.runs.len(), 1);
    assert!(!matches!(e.get(&keys[0]), Ok(Some(v)) if v == b"value-0"));
}

#[test]
fn survives_sync_and_reads_back() {
    let mut e = engine(Ordering::Unsorted);
    for i in 0..50u32 {
        let k = format!("key-{i}");
        let v = format!("value-{i}-{}", "x".repeat(i as usize));
        e.put(k.as_bytes(), v.as_bytes(), 0).unwrap();
    }
    e.sync().unwrap();
    for i in 0..50u32 {
        let k = format!("key-{i}");
        let v = format!("value-{i}-{}", "x".repeat(i as usize));
        assert_eq!(e.get(k.as_bytes()).unwrap().as_deref(), Some(v.as_bytes()));
    }
}

#[test]
fn reads_without_sync_span_blocks_and_segments() {
    let mut e = engine(Ordering::Unsorted);
    let mut kv = Vec::new();
    for i in 0..60u32 {
        let k = format!("k{i:03}");
        let v = format!("val-{i:03}-payload");
        e.put(k.as_bytes(), v.as_bytes(), 0).unwrap();
        kv.push((k, v));
    }
    assert!(e.used_segments() > 1, "should have rolled across segments");
    for (k, v) in &kv {
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(v.as_bytes()),
            "key {k} mismatch (no sync)"
        );
    }
}

#[test]
fn multi_get_async_matches_sync() {
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

    let sync_res = e.multi_get(&refs).unwrap();
    let async_res = vals_rt::block_on(e.multi_get_async(&refs), || {}).unwrap();
    assert_eq!(async_res, sync_res);
    assert_eq!(async_res[10], None);
    assert_eq!(async_res[50], None);
    assert_eq!(async_res[5].as_deref(), Some(&b"v-5"[..]));
}

#[test]
fn flush_then_tail_recovers_all() {
    let path = temp_path("flush-tail");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..30u32 {
            let k = format!("key-{i:03}");
            e.put(
                k.as_bytes(),
                format!("val-{i:03}").as_bytes(),
                ver_of(k.as_bytes()),
            )
            .unwrap();
        }
        let meta = e.flush(RunBuildOpts::default()).unwrap().unwrap();
        assert_eq!(meta.entry_count, 30);
        assert_eq!(e.len(), 0, "memtable is empty after a flush");
        assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"val-017"[..]));

        e.put(b"key-000", b"val-OVERWRITE", ver_of(b"key-000") + 1)
            .unwrap();
        e.put(b"key-099", b"val-099", ver_of(b"key-099")).unwrap();
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(
        e.get(b"key-000").unwrap().as_deref(),
        Some(&b"val-OVERWRITE"[..])
    );
    assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"val-017"[..]));
    assert_eq!(e.get(b"key-099").unwrap().as_deref(), Some(&b"val-099"[..]));
    assert_eq!(e.get(b"key-500").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn paced_flush_build_sleeps_under_a_tiny_duty_share() {
    if isolated_env_test(&[("VALS_MAINT_DUTY_PCT", Some("1"))]) { return; }
    let dev = MemBlockDevice::new(BS, 8192);
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    for i in 0..500u32 {
        e.put(
            format!("pace-key-{i:05}").as_bytes(),
            &[0x5a; 64],
            i as u128 + 1,
        )
        .unwrap();
    }
    let (_, slept_before) = crate::pace::maint_pace_totals();
    let flushed = run_flush(&mut e, RunBuildOpts::default());
    assert!(flushed.is_some());
    let (_, slept_after) = crate::pace::maint_pace_totals();
    assert!(
        slept_after > slept_before,
        "a paced flush at 1% duty must sleep ({slept_before} -> {slept_after})"
    );
}

fn drain_flush_cleanup<D: BlockDevice>(e: &mut Engine<D>, expected: usize, budget: usize) {
    assert_eq!(e.flush_cleanup.len(), expected);
    assert!(e.flush_cleanup.capacity() >= expected);
    while !e.flush_cleanup.is_empty() {
        let before = e.flush_cleanup.len();
        assert!(matches!(
            e.flush_plan_step(RunBuildOpts::default(), budget).unwrap(),
            Some(FlushPlanStep::Pending)
        ));
        assert!(
            before - e.flush_cleanup.len() <= budget,
            "one cleanup step destroyed more than its entry budget"
        );
        assert!(!e.flush_in_flight(), "deferred cleanup does not hold the gate");
    }
    assert_eq!(e.flush_cleanup.capacity(), 0);
}

#[test]
fn flush_protection_deduplicates_segments_without_entry_sized_capacity() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 32768),
        Config { seg_blocks: 256, reserved_blocks: 0, ordering: Ordering::Unsorted });
    for i in 0..5_000u64 { e.put(&i.to_be_bytes(), b"value", i as u128).unwrap(); }
    let mut expected = std::collections::HashSet::new();
    e.index.for_each_value_segment(&mut |segment| { expected.insert(segment); });
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    assert_eq!(e.flush_value_segs, expected);
    assert!(e.flush_value_segs.capacity() < 1000);
    let dev = e.device().sibling().unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, &dev), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert!(e.flush_value_segs.is_empty());
    assert_eq!(e.memtable_bytes(), 0);
    assert!(e.capacity_stats().mutable_allocated_bytes > 0);
    assert_eq!(e.get(&2_500u64.to_be_bytes()).unwrap().as_deref(), Some(b"value".as_slice()));
}

#[test]
fn cooperative_flush_pages_keep_gate_and_interleaved_keys() {
    let mut e = engine(Ordering::Unsorted);
    for i in 0..10u32 {
        e.put(format!("k{i:02}").as_bytes(), b"old", i as u128 + 1)
            .unwrap();
    }

    assert!(matches!(
        e.flush_plan_step(RunBuildOpts::default(), 3).unwrap(),
        Some(FlushPlanStep::Pending)
    ));
    assert!(e.flush_in_flight(), "gate arms on the first snapshot page");
    e.put(b"k00", b"new", 100).unwrap();
    e.put(b"z-new", b"tail", 101).unwrap();

    let plan = loop {
        match e
            .flush_plan_step(RunBuildOpts::default(), 3)
            .unwrap()
            .unwrap()
        {
            FlushPlanStep::Pending => assert!(e.flush_in_flight()),
            FlushPlanStep::Ready(plan) => break plan,
        }
    };
    let wdev = e.device().sibling().unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
    let mut commit = e.flush_commit_start(written).unwrap();
    assert!(e.flush_in_flight(), "gate remains armed after manifest install");
    let mut steps = 0;
    let mut prior_remaining = commit.remaining_entries();
    loop {
        steps += 1;
        let step = e.flush_commit_step(&mut commit, 2);
        assert!(
            prior_remaining - commit.remaining_entries() <= 2,
            "a commit page destroyed more than its entry budget"
        );
        prior_remaining = commit.remaining_entries();
        match step {
            FlushCommitStep::Pending => {
                assert!(e.flush_in_flight());
                assert!(commit.remaining_entries() > 0);
            }
            FlushCommitStep::Ready(_) => {
                assert_eq!(
                    commit.remaining_entries(),
                    0,
                    "Ready must not leave an O(n) snapshot to destroy"
                );
                break;
            }
        }
    }
    assert!(steps > 1, "drop pass was genuinely paged");
    assert!(!e.flush_in_flight());
    assert_eq!(e.get(b"k00").unwrap().as_deref(), Some(&b"new"[..]));
    assert_eq!(e.get(b"z-new").unwrap().as_deref(), Some(&b"tail"[..]));
}

#[test]
fn cooperative_snapshot_new_key_behind_cursor_waits_for_next_flush() {
    let mut e = engine(Ordering::Sorted);
    for key in [b"m".as_slice(), b"n", b"o"] {
        e.put(key, b"v", 1).unwrap();
    }
    assert!(matches!(
        e.flush_plan_step(RunBuildOpts::default(), 1).unwrap(),
        Some(FlushPlanStep::Pending)
    ));
    e.put(b"a", b"late", 2).unwrap();
    let plan = loop {
        match e
            .flush_plan_step(RunBuildOpts::default(), 1)
            .unwrap()
            .unwrap()
        {
            FlushPlanStep::Pending => {}
            FlushPlanStep::Ready(plan) => break plan,
        }
    };
    let wdev = e.device().sibling().unwrap();
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert_eq!(e.get(b"a").unwrap().as_deref(), Some(&b"late"[..]));
    assert!(e.memtable_bytes() > 0, "behind-cursor key waits for next flush");
}

#[test]
fn flush_committed_after_a_reordered_compaction_names_only_live_runs_and_survives_reopen() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let dev = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: 1,
        ..Default::default()
    };

    e.put(b"a0", b"a0v", 1).unwrap();
    e.put(b"shared", b"s1", 1).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put(b"a1", b"a1v", 2).unwrap();
    e.put(b"shared", b"s2", 2).unwrap();
    run_flush(&mut e, opts).unwrap();
    assert_eq!(e.run_count(), 2);
    let input_segs: Vec<SegmentId> = e
        .runs
        .iter()
        .flat_map(|or| or.meta.handle.segs.iter().copied())
        .collect();

    e.put(b"fresh", b"fv", 10).unwrap();
    e.put(b"shared", b"s10", 10).unwrap();
    let flush_plan = e.flush_plan(opts).unwrap().unwrap();
    assert!(e.flush_in_flight);

    let compact_plan = e.compact_plan(opts, None).unwrap().unwrap();
    let cwdev = Arc::new(e.device().sibling().unwrap());
    let cwritten = vals_rt::block_on(compact_and_write(compact_plan, cwdev), || {}).unwrap();
    e.compact_commit(cwritten).unwrap();
    assert_eq!(e.run_count(), 1, "merged installed, inputs freed");
    for s in &input_segs {
        assert!(
            e.alloc.is_free(*s),
            "input segment {s:?} freed by compaction"
        );
    }

    let fwdev = e.device().sibling().unwrap();
    let fwritten = vals_rt::block_on(build_and_write_flush(flush_plan, &fwdev), || {}).unwrap();
    e.flush_commit(fwritten).unwrap();

    assert_eq!(e.run_count(), 2, "merged + flush survivor");
    for or in &e.runs {
        for s in &or.meta.handle.segs {
            assert!(
                !input_segs.contains(s),
                "the committed manifest must not name a freed input segment {s:?}",
            );
        }
    }
    let expect = |e: &Engine<MemBlockDevice>| {
        assert_eq!(e.get(b"a0").unwrap().as_deref(), Some(&b"a0v"[..]));
        assert_eq!(e.get(b"a1").unwrap().as_deref(), Some(&b"a1v"[..]));
        assert_eq!(e.get(b"fresh").unwrap().as_deref(), Some(&b"fv"[..]));
        assert_eq!(e.get(b"shared").unwrap().as_deref(), Some(&b"s10"[..]));
        assert_eq!(e.get(b"absent").unwrap(), None);
    };
    expect(&e);

    let reopen_dev = e.device().sibling().unwrap();
    drop(e);
    let e2 = Engine::open(reopen_dev, cfg).unwrap();
    assert_eq!(e2.run_count(), 2, "reopen recovers [merged, flush-run]");
    expect(&e2);
}

#[test]
fn memtable_bytes_grows_with_puts_and_drops_after_flush() {
    let mut e = engine(Ordering::Sorted);
    assert_eq!(
        e.memtable_bytes(),
        0,
        "empty engine has no memtable footprint"
    );

    let mut expected_keys: u64 = 0;
    for i in 0..20u32 {
        let k = format!("key-{i:04}");
        expected_keys += k.len() as u64;
        e.put(k.as_bytes(), b"value", ver_of(k.as_bytes())).unwrap();
    }
    let before = e.memtable_bytes();
    assert!(
        before > expected_keys,
        "footprint includes per-entry overhead"
    );

    let meta = run_flush(&mut e, RunBuildOpts::default()).unwrap();
    assert_eq!(meta.entry_count, 20);
    assert!(e.run_metadata_resident_bytes() > 0);
    assert_eq!(e.run_metadata_resident_bytes(), e.capacity_stats().run_resident_bytes);
    assert_eq!(e.len(), 0, "memtable empty after a committed flush");
    assert_eq!(
        e.memtable_bytes(),
        0,
        "flush released the resident memtable bytes"
    );
}

#[test]
fn nonblocking_flush_then_tail_recovers_all() {
    let path = temp_path("nb-flush-tail");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..30u32 {
            let k = format!("key-{i:03}");
            e.put(
                k.as_bytes(),
                format!("val-{i:03}").as_bytes(),
                ver_of(k.as_bytes()),
            )
            .unwrap();
        }
        let meta = run_flush(&mut e, RunBuildOpts::default()).unwrap();
        assert_eq!(meta.entry_count, 30);
        assert_eq!(e.len(), 0, "memtable empty after a committed flush");
        assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"val-017"[..]));

        e.put(b"key-000", b"val-OVERWRITE", ver_of(b"key-000") + 1)
            .unwrap();
        e.put(b"key-099", b"val-099", ver_of(b"key-099")).unwrap();
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(
        e.get(b"key-000").unwrap().as_deref(),
        Some(&b"val-OVERWRITE"[..])
    );
    assert_eq!(e.get(b"key-017").unwrap().as_deref(), Some(&b"val-017"[..]));
    assert_eq!(e.get(b"key-099").unwrap().as_deref(), Some(&b"val-099"[..]));
    assert_eq!(e.get(b"key-500").unwrap(), None);

    std::fs::remove_file(&path).ok();
}

#[test]
fn vlog_compact_refuses_while_an_offcore_flush_is_in_flight() {
    let dev = MemBlockDevice::new(BS, 256);
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    for round in 0..4u128 {
        for i in 0..20u32 {
            let k = format!("k{i:02}");
            e.put(
                k.as_bytes(),
                format!("v{round}-{i:02}").as_bytes(),
                round + 1,
            )
            .unwrap();
        }
    }
    e.sync().unwrap();
    assert!(
        e.reclaimable_fraction() > 0.0,
        "setup: there is dead vlog to reclaim"
    );

    let plan = e
        .flush_plan(RunBuildOpts::default())
        .unwrap()
        .expect("non-empty memtable plans a flush");
    assert!(e.flush_in_flight(), "flush_plan set the gate");

    let stats = e
        .compact()
        .expect("compact must not error during an in-flight flush");
    assert_eq!(
        stats,
        CompactStats::default(),
        "vlog compaction is skipped, not run"
    );
    assert_eq!(e.run_count(), 0, "no run committed yet");

    let wdev = e.device().sibling().expect("mem sibling");
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert_eq!(e.run_count(), 1, "flush committed a run");
    assert_eq!(
        e.get(b"k05").unwrap().as_deref(),
        Some(&b"v3-05"[..]),
        "latest value intact"
    );
}

#[test]
fn delete_refuses_while_a_flush_is_in_flight() {
    let dev = MemBlockDevice::new(BS, 256);
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    for i in 0..8u32 {
        let k = format!("k{i:02}");
        e.put(k.as_bytes(), format!("v{i:02}").as_bytes(), 1)
            .unwrap();
    }
    e.sync().unwrap();

    let plan = e
        .flush_plan(RunBuildOpts::default())
        .unwrap()
        .expect("non-empty memtable plans a flush");
    assert!(e.flush_in_flight(), "flush_plan set the gate");

    let segs_before = e.used_segments();
    assert!(
        matches!(e.delete(b"k03", 2), Err(EngineError::FlushInFlight)),
        "delete must refuse while a flush is in flight"
    );
    assert_eq!(
        e.get(b"k03").unwrap().as_deref(),
        Some(&b"v03"[..]),
        "refused delete leaves the key readable"
    );
    assert_eq!(
        e.version(b"k03").unwrap(),
        Some(1),
        "refused delete leaves the version untouched (no record appended)"
    );
    assert_eq!(
        e.used_segments(),
        segs_before,
        "refused delete writes nothing to the log"
    );

    let wdev = e.device().sibling().expect("mem sibling");
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
    e.flush_commit(written).unwrap();
    assert!(!e.flush_in_flight(), "flush committed, gate cleared");
    e.put(b"fresh", b"fv", 2).unwrap();
    assert!(
        e.delete(b"fresh", 3).unwrap(),
        "delete succeeds once the flush is no longer in flight"
    );
    assert_eq!(
        e.get(b"fresh").unwrap(),
        None,
        "key is gone after the delete"
    );
}

#[test]
fn flush_keeps_keys_overwritten_during_the_flush() {
    let path = temp_path("flush-overwrite-race");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put(b"raced", b"old", 1).unwrap();
        e.put(b"quiet", b"q", 1).unwrap();

        let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();

        e.put(b"raced", b"new", 2).unwrap();

        let wdev = e.device().sibling().expect("file sibling");
        let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();

        let meta = e.flush_commit(written).unwrap();
        assert_eq!(meta.entry_count, 2, "the run holds both snapshotted keys");

        assert_eq!(e.get(b"raced").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(e.version(b"raced").unwrap(), Some(2));
        assert_eq!(
            e.len(),
            1,
            "only the overwritten key stayed in the memtable"
        );
        assert_eq!(e.get(b"quiet").unwrap().as_deref(), Some(&b"q"[..]));

        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(
        e.get(b"raced").unwrap().as_deref(),
        Some(&b"new"[..]),
        "overwrite recovers"
    );
    assert_eq!(
        e.get(b"quiet").unwrap().as_deref(),
        Some(&b"q"[..]),
        "run-only key recovers"
    );

    std::fs::remove_file(&path).ok();
}

#[test]
fn flush_plan_reserves_segments_freeable_on_abandon() {
    let dev = MemBlockDevice::new(BS, 256);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    for i in 0..20u32 {
        e.put(format!("k{i:03}").as_bytes(), b"v", i as u128 + 1)
            .unwrap();
    }
    let before = e.used_segments();
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    let segs: Vec<_> = plan.segments().to_vec();
    assert!(!segs.is_empty(), "the run reserved at least one segment");
    assert!(
        e.used_segments() > before,
        "reserved segments are held allocated"
    );
    drop(plan);
    e.free_flush_segs(&segs);
    assert_eq!(
        e.used_segments(),
        before,
        "freed back to the pre-plan footprint"
    );
    assert_eq!(e.get(b"k005").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(e.len(), 20);
}

#[test]
fn randomized_version_reference_survives_flush_merge_and_reopen() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 16384), cfg);
    let mut reference: std::collections::BTreeMap<Vec<u8>, (u128, Vec<u8>, bool)> =
        std::collections::BTreeMap::new();
    let mut rng = 0x173d_5eed_cafe_babeu64;
    for step in 0..240u128 {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let key = format!("k{:02}", rng % 17).into_bytes();
        let version = (rng as u128 % 37) * 1000 + step;
        let tombstone = rng & 7 == 0;
        let value = if tombstone {
            Vec::new()
        } else {
            format!("v{step}-{rng}").into_bytes()
        };
        e.put_kind(&key, &value, version, tombstone).unwrap();
        if reference
            .get(&key)
            .is_none_or(|(current, _, _)| version > *current)
        {
            reference.insert(key, (version, value, tombstone));
        }

        if step % 11 == 0 {
            e.flush(opts).unwrap();
        }
        if step % 29 == 0 {
            if let Some(plan) = e.compact_plan(opts, None).unwrap() {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written =
                    vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
            }
        }
        if step % 61 == 0 {
            e.sync().unwrap();
            let dev = e.device().share();
            drop(e);
            e = Engine::open(dev, cfg).unwrap();
        }
    }

    let scan = e
        .scan_bounded_versioned(None, None, usize::MAX, usize::MAX)
        .unwrap()
        .0;
    assert_eq!(scan.len(), reference.len());
    for ((key, value, version), (expected_key, (expected_version, expected_value, _))) in
        scan.into_iter().zip(reference.iter())
    {
        assert_eq!(&key, expected_key);
        assert_eq!(version, *expected_version);
        assert_eq!(&value, expected_value);
        assert_eq!(e.version(&key).unwrap(), Some(*expected_version));
    }
}

#[test]
fn flush_version_recovers_via_open_and_recovered_versions() {
    let path = temp_path("flush-ver");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Unsorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..25u32 {
            let k = format!("k{i:03}");
            e.put(
                k.as_bytes(),
                format!("VV-{i:03}").as_bytes(),
                ver_of(k.as_bytes()),
            )
            .unwrap();
        }
        e.flush(RunBuildOpts::default()).unwrap();
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    let versions = e.recovered_versions().unwrap();
    assert_eq!(versions.len(), 25);
    for (handle, version) in &versions {
        assert_eq!(*version, ver_of(handle), "version mismatch for {handle:?}");
    }

    std::fs::remove_file(&path).ok();
}

#[test]
fn second_flush_supersedes_first_by_version() {
    let path = temp_path("flush-gen");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };

    {
        let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..10u32 {
            e.put(format!("g{i}").as_bytes(), b"first", 1).unwrap();
        }
        let m1 = e.flush(RunBuildOpts::default()).unwrap().unwrap();
        for i in 0..10u32 {
            e.put(format!("g{i}").as_bytes(), b"second", 2).unwrap();
        }
        e.put(b"g99", b"second", 2).unwrap();
        let m2 = e.flush(RunBuildOpts::default()).unwrap().unwrap();
        assert_eq!(m1.entry_count, 10);
        assert_eq!(m2.entry_count, 11);
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.get(b"g3").unwrap().as_deref(), Some(&b"second"[..]));
    assert_eq!(e.get(b"g99").unwrap().as_deref(), Some(&b"second"[..]));

    std::fs::remove_file(&path).ok();
}

#[test]
fn tombstone_flag_survives_flush_and_recovery() {
    let path = temp_path("tombstone-flag-recovery");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    {
        let dev = FileBlockDevice::create(&path, BS, 2048).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put_kind(b"mem_tomb", b"", 5, true).unwrap();
        e.put_kind(b"mem_live", b"v", 6, false).unwrap();
        e.sync().unwrap();
    }
    {
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        assert_eq!(e.version(b"mem_tomb").unwrap(), Some(5), "recovered");
        e.flush(opts).unwrap();
        e.put_kind(b"filler", b"y", 9, false).unwrap();
        e.flush(opts).unwrap();
        let stats = e.compact_runs(opts, Some(100)).unwrap();
        assert!(
            stats
                .current_dropped_tombstones
                .iter()
                .any(|(k, _)| k == b"mem_tomb"),
            "recovered memtable tombstone kept its flag through flush + GC"
        );
        assert_eq!(e.get(b"mem_live").unwrap().as_deref(), Some(&b"v"[..]));
    }
}

#[test]
fn forget_keys_defers_while_a_flush_is_in_flight() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put_kind(b"k", b"v", 1, false).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    e.put_kind(b"m", b"w", 2, false).unwrap();

    let _plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    assert!(e.flush_in_flight());
    let mut keys = std::collections::HashSet::new();
    keys.insert(b"k".to_vec());
    assert!(
        e.forget_keys(&keys, RunBuildOpts::default())
            .unwrap()
            .is_none(),
        "forget_keys must defer while a flush is in flight"
    );
}

#[test]
fn compact_commit_pins_inflight_flush_value_segments() {
    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 8192), cfg);

    e.put(b"seed-a", &[1u8; 100], 1).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put(b"seed-b", &[2u8; 100], 2).unwrap();
    run_flush(&mut e, opts).unwrap();
    e.put(b"pending", &[3u8; 100], 3).unwrap();
    e.put(b"zz-pending", &[4u8; 100], 3).unwrap();
    assert!(matches!(
        e.flush_plan_step(opts, 1).unwrap(),
        Some(FlushPlanStep::Pending)
    ));
    let captured = e
        .flush_snapshot
        .as_ref()
        .unwrap()
        .entries
        .iter()
        .find(|entry| entry.key == b"pending")
        .unwrap()
        .ptr;
    assert!(e.runs.iter().any(|run| {
        run.meta
            .value_segs
            .as_ref()
            .is_some_and(|segs| segs.contains(&captured.segment))
    }));

    for version in 4..8u128 {
        e.put(b"pending", &[version as u8; 700], version).unwrap();
    }
    assert_ne!(e.index.get(b"pending").unwrap().ptr.segment, captured.segment);
    assert_ne!(e.log.open_segment(), Some(captured.segment));

    let compact_plan = e.compact_plan(opts, None).unwrap().unwrap();
    assert!(compact_plan.co_compacts());
    let compact_dev = Arc::new(e.device().sibling().unwrap());
    let compact_written =
        vals_rt::block_on(compact_and_write(compact_plan, compact_dev), || {}).unwrap();
    e.compact_commit(compact_written).unwrap();

    assert!(e.seg_allocated_for_test(captured.segment));
    let (seg_start, _) = e.alloc.segment_range(captured.segment);
    let captured_record = e.log.read(&e.dev, seg_start, &captured).unwrap();
    assert_eq!(captured_record.value, vec![3u8; 100]);

    let flush_plan = loop {
        match e.flush_plan_step(opts, 1).unwrap().unwrap() {
            FlushPlanStep::Pending => {}
            FlushPlanStep::Ready(plan) => break plan,
        }
    };
    let flush_dev = e.device().sibling().unwrap();
    let flush_written =
        vals_rt::block_on(build_and_write_flush(flush_plan, &flush_dev), || {}).unwrap();
    e.flush_commit(flush_written).unwrap();
    assert_eq!(e.get(b"pending").unwrap(), Some(vec![7u8; 700]));
}

#[test]
fn hot_key_orphan_flush_value_segs_survive_reopen_then_merge() {
    if co_compact_env(true) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let path = temp_path("hot-key-orphan-reopen");

    {
        let dev = FileBlockDevice::create(&path, BS, 4096).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        e.put(b"cold-a", b"a-value", 1).unwrap();
        hammer_hot_key(&mut e, b"hot", 40, 100, 500);
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
        hammer_hot_key(&mut e, b"hot", 40, 200, 500);
        run_flush(&mut e, RunBuildOpts::default()).unwrap();
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let mut e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.run_count(), 2);
    assert_eq!(e.get(b"cold-a").unwrap().as_deref(), Some(&b"a-value"[..]));
    assert_eq!(
        e.get(b"hot").unwrap().as_deref(),
        Some(vec![200u8.wrapping_add(39); 500].as_slice())
    );

    let free_before = e.free_segments_for_test();
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert!(plan.co_compacts(), "post-reopen merge must co-compact");
    let wdev = Arc::new(e.device().sibling().expect("sibling"));
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    let free_after = e.free_segments_for_test();
    assert!(
        free_after >= free_before + 40,
        "post-reopen merge must still free the orphan segments: \
         before {free_before}, after {free_after}"
    );
    assert_eq!(
        e.get(b"hot").unwrap().as_deref(),
        Some(vec![200u8.wrapping_add(39); 500].as_slice())
    );
    std::fs::remove_file(&path).ok();
}

#[test]
fn compact_step_frees_never_pinned_by_later_flush() {
    if co_compact_env(true) { return; }
    let cfg = Config {
        seg_blocks: 4,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let path = temp_path("compact-step-never-pinned");
    let dev = FileBlockDevice::create(&path, BS, 4096).unwrap();
    let mut e = Engine::open(dev, cfg).unwrap();

    hammer_hot_key(&mut e, b"hot", 40, 100, 500);
    e.put(b"cold", b"c", 1).unwrap();

    let mut total_freed = 0u64;
    loop {
        let (stats, done) = e.compact_step(4).unwrap();
        total_freed += stats.segments_freed as u64;
        if done {
            break;
        }
    }
    assert!(total_freed > 0, "compact_step must free some dead segments");

    run_flush(&mut e, RunBuildOpts::default()).unwrap();
    for seg in e.last_run_value_segs_for_test() {
        assert!(
            e.seg_allocated_for_test(seg),
            "flush run value_segs must be ⊆ allocated (seg {seg:?} is free)"
        );
    }

    e.sync().unwrap();
    drop(e);
    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let e2 = Engine::open(dev, cfg).unwrap();
    assert_eq!(
        e2.get(b"hot").unwrap().as_deref(),
        Some(vec![100u8.wrapping_add(39); 500].as_slice())
    );
    assert_eq!(e2.get(b"cold").unwrap().as_deref(), Some(&b"c"[..]));
}

#[test]
fn flush_plan_tail_write_failure_defers_snapshot_cleanup_and_retries() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let armed = Arc::new(AtomicBool::new(false));
    let mut e = Engine::create_prewiped(
        TailWriteFailDevice {
            inner: MemBlockDevice::new(BS, 4096),
            armed: armed.clone(),
        },
        cfg,
    );
    for i in 0..40u32 {
        e.put(
            format!("key-{i:03}").as_bytes(),
            format!("val-{i:03}").as_bytes(),
            i as u128 + 1,
        )
        .unwrap();
    }

    armed.store(true, AtomicOrdering::Relaxed);
    assert!(
        e.flush_plan_step(RunBuildOpts::default(), usize::MAX)
            .is_err(),
        "the injected tail write failure must propagate"
    );
    assert!(!e.flush_in_flight());
    drain_flush_cleanup(&mut e, 40, 3);

    armed.store(false, AtomicOrdering::Relaxed);
    let meta = run_flush(&mut e, RunBuildOpts::default()).expect("retry produces a run");
    assert_eq!(meta.entry_count, 40);
    assert_eq!(e.len(), 0);
    for i in 0..40u32 {
        let k = format!("key-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("val-{i:03}").as_bytes())
        );
    }
}

#[test]
fn flush_plan_allocation_failure_defers_snapshot_cleanup_and_retries() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 4096), cfg);
    e.set_orphan_reclaim_for_test(false);
    for i in 0..40u32 {
        e.put(
            format!("key-{i:03}").as_bytes(),
            format!("val-{i:03}").as_bytes(),
            i as u128 + 1,
        )
        .unwrap();
    }
    let mut held = Vec::new();
    while let Some(seg) = e.alloc.allocate() {
        held.push(seg);
    }

    assert!(
        e.flush_plan_step(RunBuildOpts::default(), usize::MAX)
            .is_err(),
        "an exhausted allocator must reject the run reservation"
    );
    assert!(!e.flush_in_flight());
    drain_flush_cleanup(&mut e, 40, 3);
    for seg in held {
        e.alloc.free(seg).unwrap();
    }

    let meta = run_flush(&mut e, RunBuildOpts::default()).expect("retry produces a run");
    assert_eq!(meta.entry_count, 40);
    assert_eq!(e.len(), 0);
    for i in 0..40u32 {
        let k = format!("key-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("val-{i:03}").as_bytes())
        );
    }
}

#[test]
fn flush_commit_manifest_write_failure_clears_gate_and_reclaims_segments() {
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mem = MemBlockDevice::new(BS, 4096);
    let mut e = Engine::create_prewiped(
        ManifestWriteFailDevice {
            inner: mem,
            armed: AtomicBool::new(false),
        },
        cfg,
    );

    for i in 0..40u32 {
        e.put(
            format!("key-{i:03}").as_bytes(),
            format!("val-{i:03}").as_bytes(),
            i as u128 + 1,
        )
        .unwrap();
    }

    let used_before_plan = e.used_segments();
    let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
    assert!(e.flush_in_flight(), "flush_plan arms the in-flight gate");
    let wdev = e.device().sibling().expect("mem sibling");
    let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    let res = e.flush_commit(written);
    assert!(
        res.is_err(),
        "a failed manifest write must propagate from flush_commit"
    );

    assert!(
        !e.flush_in_flight(),
        "a failed flush_commit must clear flush_in_flight (else flushes are disabled forever)"
    );
    assert_eq!(
        e.used_segments(),
        used_before_plan,
        "the failed flush's run AND manifest-chain segments must all be reclaimed"
    );
    drain_flush_cleanup(&mut e, 40, 3);

    assert_eq!(
        e.len(),
        40,
        "no key dropped from the memtable by the failed flush"
    );
    for i in 0..40u32 {
        let k = format!("key-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("val-{i:03}").as_bytes()),
            "{k} still served from the memtable after the failed flush"
        );
    }

    e.device().armed.store(false, AtomicOrdering::Relaxed);
    let meta = run_flush(&mut e, RunBuildOpts::default())
        .expect("the second flush produces a run");
    assert_eq!(meta.entry_count, 40);
    assert_eq!(e.len(), 0, "memtable drained after the successful flush");
    for i in 0..40u32 {
        let k = format!("key-{i:03}");
        assert_eq!(
            e.get(k.as_bytes()).unwrap().as_deref(),
            Some(format!("val-{i:03}").as_bytes()),
            "{k} resolves via the run after the successful flush"
        );
    }
}

#[test]
fn torn_flush_manifest_commit_keeps_prior_state_on_reopen() {
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
    e.put(b"c", b"c1", 1).unwrap();
    assert_eq!(e.run_count(), 0, "no run before the flush");

    e.device().armed.store(true, AtomicOrdering::Relaxed);
    let _ = e.flush(RunBuildOpts::default());

    drop(e);
    let e2 = Engine::open(disk, cfg).unwrap();
    assert_eq!(e2.run_count(), 0, "torn flush must NOT add a run on reopen");
    assert_eq!(e2.get(b"a").unwrap().as_deref(), Some(&b"a1"[..]));
    assert_eq!(e2.get(b"b").unwrap().as_deref(), Some(&b"b1"[..]));
    assert_eq!(e2.get(b"c").unwrap().as_deref(), Some(&b"c1"[..]));
    assert_eq!(
        e2.get(b"missing").unwrap(),
        None,
        "no spurious resurrection"
    );
    assert_eq!(e2.version(b"a").unwrap(), Some(1));
}

#[test]
fn truncate_with_flushed_runs_empties_the_store() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);

    for i in 0..40u32 {
        e.put(format!("k{i:03}").as_bytes(), b"v", i as u128 + 1)
            .unwrap();
    }
    e.flush(RunBuildOpts::default()).unwrap();
    e.put(b"mem", b"v", 999).unwrap();
    assert_eq!(
        e.get(b"k017").unwrap().as_deref(),
        Some(&b"v"[..]),
        "flushed key reads pre-wipe"
    );

    e.truncate().unwrap();

    assert!(e.is_empty());
    assert_eq!(e.used_segments(), 0);
    assert_eq!(
        e.get(b"k017").unwrap(),
        None,
        "flushed key gone after wipe (run cleared)"
    );
    assert_eq!(e.get(b"mem").unwrap(), None);
    assert!(
        e.recovered_versions().unwrap().is_empty(),
        "no versions survive the wipe"
    );
    assert_eq!(e.scan(None, None).unwrap().len(), 0);

    e.put(b"after", b"x", 1).unwrap();
    e.flush(RunBuildOpts::default()).unwrap();
    assert_eq!(e.get(b"after").unwrap().as_deref(), Some(&b"x"[..]));
    assert_eq!(e.get(b"k017").unwrap(), None);
}

#[test]
fn truncate_drains_async_writes_before_returning_completion_error() {
    let mut e = queued_truncate_engine();
    arm_queued_truncate(&e, true);

    assert!(matches!(e.truncate(), Err(EngineError::Io(_))));

    let trace = e.device().trace.lock().unwrap();
    assert_eq!(trace.synchronous_writes, 0);
    assert!(trace.pending.is_empty());
    assert_eq!(trace.max_inflight, MAX_INFLIGHT_HEADER_WIPES);
    assert!(trace.submissions_at_flush.is_empty());
}

#[test]
fn file_floor_flush_preserves_pressure_reclaim_and_resumes_writes() {
    if co_compact_env(false) { return; }
    let path = temp_path("floor-flush-pressure");
    let dev = FileBlockDevice::create(&path, BS, 8192).unwrap();
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
    let mut blocked = false;
    'churn: for round in 0..2000u32 {
        for i in 0..KEYS {
            let ver = (round * KEYS + i) as u128 + 1;
            match e.put(format!("k{i:04}").as_bytes(), &[7u8; 200], ver) {
                Ok(()) => {}
                Err(EngineError::Log(LogError::Backpressure)) => {
                    blocked = true;
                    break 'churn;
                }
                Err(other) => panic!("unexpected put error: {other:?}"),
            }
        }
        if round <= 1 {
            assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
        }
    }
    e.sync().unwrap();
    assert!(
        blocked,
        "unbounded churn must eventually hit the reserve floor (free={}, reserve={})",
        e.free_segments_for_test(),
        e.maint_reserve_for_test(),
    );
    assert!(e.free_segments_for_test() > 0, "reserve keeps segments free");
    let reserve = e.maint_reserve_for_test();
    assert!(reserve > 0, "a churned zone reserves reclaim-merge headroom");
    assert!(!e.index.is_empty(), "the floor was reached with a pending memtable");
    assert!(!e.region_exhausted(), "a pending flush remains retryable");
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    let used_before = e.used_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .expect("plan ok")
        .expect("post-flush pressure merge remains plannable");
    assert!(!plan.paced, "pressure merges bypass the pacer");
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert!(stats.segments_freed > 0);
    assert!(e.used_segments() < used_before);
    e.put(b"writes-resume", b"ok", u128::MAX).unwrap();
    assert_eq!(e.get(b"writes-resume").unwrap().as_deref(), Some(&b"ok"[..]));
    std::fs::remove_file(path).ok();
}

#[test]
fn file_first_floor_flush_preserves_one_run_victim_reclaim() {
    if co_compact_env(false) { return; }
    let path = temp_path("first-floor-flush-pressure");
    let dev = FileBlockDevice::create(&path, BS, 8192).unwrap();
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 64,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_maint_reserve_enabled(true);

    let mut blocked = false;
    'churn: for round in 0..4000u32 {
        for i in 0..64u32 {
            match e.put(
                format!("k{i:04}").as_bytes(),
                &[round as u8; 200],
                (round * 64 + i) as u128 + 1,
            ) {
                Ok(()) => {}
                Err(EngineError::Log(LogError::Backpressure)) => {
                    blocked = true;
                    break 'churn;
                }
                Err(other) => panic!("unexpected churn error: {other:?}"),
            }
        }
    }
    assert!(blocked);
    assert_eq!(e.run_count(), 0);
    assert!(!e.index.is_empty());
    assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
    assert_eq!(e.run_count(), 1);

    let used_before = e.used_segments();
    let plan = e
        .compact_plan_pressure(RunBuildOpts::default(), None)
        .expect("first post-flush pressure plan has headroom")
        .expect("one-run victim reclaim is viable");
    assert!(e.free_segments_for_test() >= e.manifest_headroom_segs(), "plan left the manifest commit headroom");
    assert_eq!(plan.merged_inputs.len(), 1);
    assert!(plan.co_compacts());
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    let stats = e.compact_commit(written).unwrap();
    assert!(stats.segments_freed > 0);
    assert!(e.used_segments() < used_before);
    e.put(b"writes-resume", b"ok", u128::MAX).unwrap();
    std::fs::remove_file(path).ok();
}

#[test]
fn file_unique_flush_cycles_preserve_compaction_headroom() {
    if co_compact_env(false) { return; }
    let path = temp_path("unique-flush-floor");
    let dev = FileBlockDevice::create(&path, BS, 8192).unwrap();
    let mut e = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 32,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    e.set_maint_reserve_enabled(true);

    let mut next = 0u32;
    let mut ingested_bytes = 0u64;
    let mut blocked = false;
    for _ in 0..1000 {
        for _ in 0..24 {
            let key = format!("unique-{next:08}");
            match e.put(key.as_bytes(), &[7u8; 96], next as u128 + 1) {
                Ok(()) => {
                    ingested_bytes += key.len() as u64 + 96;
                    next += 1;
                }
                Err(EngineError::Log(LogError::Backpressure)) => {
                    blocked = true;
                    break;
                }
                Err(other) => panic!("unexpected unique ingest error: {other:?}"),
            }
        }
        if blocked {
            break;
        }
        assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
        if e.run_count() >= 4 {
            if let Some(plan) = e.compact_plan(RunBuildOpts::default(), None).unwrap() {
                let wdev = Arc::new(e.device().sibling().unwrap());
                let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                e.compact_commit(written).unwrap();
            }
        }
    }

    assert!(blocked, "small unique-key region reaches its reserve floor");
    assert_eq!(
        e.used_segments() + e.free_segments(),
        e.alloc.num_segments(),
        "allocator accounting closes at the floor"
    );
    assert!(e.free_segments() > 0, "armed region never reaches free=0");
    assert!(
        e.lsm_dead_fraction()
            .is_some_and(|dead| dead < MAINT_RESERVE_DEAD_MIN_FRACTION),
        "unique-key fill remains below the dead-space pressure gate"
    );

    let mut terminal = false;
    for _ in 0..1000 {
        if !e.index.is_empty() {
            assert!(run_flush(&mut e, RunBuildOpts::default()).is_some());
        }
        loop {
            match e.compact_plan_pressure(RunBuildOpts::default(), None) {
                Ok(Some(plan)) => {
                    let wdev = Arc::new(e.device().sibling().unwrap());
                    let written =
                        vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
                    e.compact_commit(written).unwrap();
                }
                Ok(None) => break,
                Err(EngineError::InsufficientHeadroom { .. }) => {
                    terminal = true;
                    break;
                }
                Err(EngineError::Snapshot(SnapshotError::DeviceFull)) => {
                    panic!("pressure planning must not fail as Snapshot(DeviceFull)")
                }
                Err(other) => panic!("unexpected pressure planning error: {other:?}"),
            }
        }
        if terminal || e.region_exhausted() {
            terminal = true;
            break;
        }
        loop {
            let key = format!("unique-{next:08}");
            match e.put(key.as_bytes(), &[7u8; 96], next as u128 + 1) {
                Ok(()) => {
                    ingested_bytes += key.len() as u64 + 96;
                    next += 1;
                }
                Err(EngineError::Log(LogError::Backpressure)) => break,
                Err(other) => panic!("unexpected resumed ingest error: {other:?}"),
            }
        }
    }
    if !terminal {
        match e.compact_plan_pressure(RunBuildOpts::default(), None) {
            Ok(Some(plan)) => panic!(
                "unexpected remaining plan with {} inputs and {} segments",
                plan.merged_inputs.len(),
                plan.segments().len()
            ),
            Ok(None) => {}
            Err(EngineError::InsufficientHeadroom { .. }) => terminal = true,
            Err(other) => panic!("unexpected final pressure error: {other:?}"),
        }
    }
    assert!(
        terminal,
        "unique live capacity reaches planner-level terminal headroom: next={next} free={} reserve={} runs={} pending={} compact={}",
        e.free_segments(),
        e.maint_reserve_for_test(),
        e.run_count(),
        !e.index.is_empty(),
        e.compact_in_flight
    );
    assert!(e.region_exhausted(), "client-visible terminal predicate is true");
    let seg_bytes = e.alloc.blocks_per_segment() * e.block_size as u64;
    assert!(
        e.used_segments() * seg_bytes < ingested_bytes * 4,
        "segment amplification exceeds 4x: used={} logical={ingested_bytes}",
        e.used_segments() * seg_bytes
    );
    std::fs::remove_file(path).ok();
}

#[test]
fn armed_flush_reserve_is_always_on_below_pressure() {
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
    for i in 0..8u32 {
        e.put(format!("k{i:04}").as_bytes(), &[7u8; 200], i as u128 + 1)
            .unwrap();
    }
    assert!(
        e.free_fraction() > 0.50,
        "precondition: the test must stay well below region pressure (free={})",
        e.free_fraction(),
    );
    assert!(
        matches!(e.lsm_dead_fraction(), Some(d) if d < MAINT_RESERVE_DEAD_MIN_FRACTION),
        "precondition: distinct keys leave the zone below the dead-fraction gate (dead={:?})",
        e.lsm_dead_fraction(),
    );
    assert!(
        e.maint_reserve_for_test() > 0,
        "an armed, non-empty memtable reserves the flush term even below pressure",
    );
    e.set_maint_reserve_enabled(false);
    assert_eq!(
        e.maint_reserve_for_test(),
        0,
        "a disarmed engine reserves nothing regardless of memtable state",
    );
}

#[test]
fn async_get_matches_sync_memtable_flushed_mixed() {
    let (e, _) = mixed_engine();
    for key in [b"a" as &[u8], b"b", b"c", b"d", b"e", b"f", b"z", b"a0"] {
        assert_eq!(
            async_get(&e, key),
            e.get(key).unwrap(),
            "async_get != sync get for {:?}",
            key
        );
    }
}

#[test]
fn async_version_matches_sync_memtable_flushed_mixed() {
    let (e, _) = mixed_engine();
    for key in [b"a" as &[u8], b"b", b"c", b"d", b"e", b"f", b"z"] {
        assert_eq!(
            async_version(&e, key),
            e.version(key).unwrap(),
            "async_version != sync version for {:?}",
            key
        );
    }
}

#[test]
fn async_get_pure_memtable_and_pure_flushed() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    for i in 0..20u32 {
        e.put(
            format!("k{i:02}").as_bytes(),
            format!("v{i}").as_bytes(),
            i as u128,
        )
        .unwrap();
    }
    for i in 0..20u32 {
        let k = format!("k{i:02}");
        assert_eq!(async_get(&e, k.as_bytes()), e.get(k.as_bytes()).unwrap());
    }

    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };
    e.flush(opts).unwrap();
    assert_eq!(e.len(), 0, "memtable empty after flush");
    for i in 0..20u32 {
        let k = format!("k{i:02}");
        assert_eq!(
            async_get(&e, k.as_bytes()),
            e.get(k.as_bytes()).unwrap(),
            "flushed-only async_get != sync for {k}"
        );
        assert_eq!(
            async_version(&e, k.as_bytes()),
            e.version(k.as_bytes()).unwrap()
        );
    }
    assert_eq!(async_get(&e, b"absent"), None);
}

#[test]
fn async_digest_page_matches_sync_unbounded_and_paged() {
    let (e, _) = mixed_engine();
    let want = all_buckets();

    let sync_full = e
        .version_digest_page(&want, vdp_bucket, None, usize::MAX, 0)
        .unwrap();
    let async_full = async_digest_page(&e, &want, None, usize::MAX);
    assert_eq!(
        async_full, sync_full,
        "async digest page != sync (unbounded)"
    );

    let mut acc: Vec<(Vec<u8>, u128)> = Vec::new();
    let mut after: Option<Vec<u8>> = None;
    loop {
        let sync_pg = e
            .version_digest_page(&want, vdp_bucket, after.as_deref(), 4, 0)
            .unwrap();
        let async_pg = async_digest_page(&e, &want, after.as_deref(), 4);
        assert_eq!(
            async_pg, sync_pg,
            "async page != sync page at cursor {after:?}"
        );
        let (pg, more) = async_pg;
        assert!(!pg.is_empty());
        after = Some(pg.last().unwrap().0.clone());
        acc.extend(pg);
        if !more {
            break;
        }
    }
    assert_eq!(acc, async_full.0, "paged async == unbounded async");
}

#[test]
fn sync_sidecar_stream_is_windowed_ordered_and_crc_checked() {
    use std::sync::atomic::AtomicBool;
    struct TamperDevice {
        inner: MemBlockDevice,
        bytes: std::sync::Arc<AtomicU64>,
        corrupt: std::sync::Arc<AtomicBool>,
    }
    impl BlockDevice for TamperDevice {
        fn block_size(&self) -> usize { self.inner.block_size() }
        fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)?;
            self.bytes.fetch_add(buf.len() as u64, AtomicOrdering::Relaxed);
            if self.corrupt.load(AtomicOrdering::Relaxed) && buf.len() > 16 {
                let mid = buf.len() / 2;
                buf[mid] ^= 1;
            }
            Ok(())
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.inner.write_at(lba, buf) }
        fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    }
    for (ordering, version) in [
        (Ordering::Sorted, run::RUN_SIDECAR_VERSION),
        (Ordering::Unsorted, run::RUN_SIDECAR_VERSION),
        (Ordering::Sorted, run::RUN_VERSION),
        (Ordering::Unsorted, run::RUN_VERSION),
    ] {
        let bytes = std::sync::Arc::new(AtomicU64::new(0));
        let corrupt = std::sync::Arc::new(AtomicBool::new(false));
        let dev = TamperDevice {
            inner: MemBlockDevice::new(BS, 65536), bytes: bytes.clone(), corrupt: corrupt.clone(),
        };
        let cfg = Config { seg_blocks: 256, reserved_blocks: 0, ordering };
        let mut e = Engine::create_prewiped(dev, cfg);
        for i in 0..2_000u32 {
            let mut key = format!("key-{i:05}").into_bytes();
            key.resize(256, b'x');
            e.put(&key, b"v", i as u128 + 1).unwrap();
        }
        run::with_run_write_version(version, || e.flush(RunBuildOpts::default())).unwrap();
        assert_eq!(e.runs[0].run.format_version(), version);
        let handle = e.run_handles().pop().unwrap();
        bytes.store(0, AtomicOrdering::Relaxed);
        let mut stream = e.run_version_stream(&handle).unwrap().unwrap();
        let mut i = 0u32;
        loop {
            let before = bytes.load(AtomicOrdering::Relaxed);
            match stream.next_step().unwrap() {
                run::SidecarCursorStep::Entry((key, version)) => {
                    assert!(key.starts_with(format!("key-{i:05}").as_bytes()));
                    assert_eq!(version, i as u128 + 1);
                    i += 1;
                }
                run::SidecarCursorStep::Done => break,
                run::SidecarCursorStep::Checkpoint => {}
            }
            assert!(bytes.load(AtomicOrdering::Relaxed) - before <=
                (run::SIDECAR_CURSOR_WINDOW + 16 * BS) as u64);
            assert!(stream.resident_bound_bytes() <= (run::SIDECAR_CURSOR_WINDOW + 4 * BS) as u64,
                "version {version} resident bound {}", stream.resident_bound_bytes());
        }
        assert_eq!(i, 2_000);
        corrupt.store(true, AtomicOrdering::Relaxed);
        let mut stream = e.run_version_stream(&handle).unwrap().unwrap();
        let mut failed = false;
        for _ in 0..3_000 {
            if matches!(stream.next_step(), Err(SnapshotError::Corrupt(_))) {
                failed = true;
                break;
            }
        }
        assert!(failed, "a changed sidecar body must fail before publication");
    }
}

#[test]
fn async_digest_page_reads_only_bounded_sidecar_windows() {
    for version in [run::RUN_SIDECAR_VERSION, run::RUN_VERSION] {
        run::with_run_write_version(version, || async_digest_page_bounded_windows(version));
    }
}

fn async_digest_page_bounded_windows(version: u16) {
    let reads = std::sync::Arc::new(AtomicU64::new(0));
    let read_bytes = std::sync::Arc::new(AtomicU64::new(0));
    let dev = AsyncCountingDevice {
        inner: MemBlockDevice::new(BS, 65536),
        reads,
        read_bytes: read_bytes.clone(),
    };
    let cfg = Config {
        seg_blocks: 256,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };
    for run_version in 1..=3u128 {
        for i in 0..2_500u32 {
            let mut key = format!("feed.like/{i:08}/").into_bytes();
            key.resize(512, b'x');
            e.put(&key, b"v", run_version).unwrap();
        }
        e.flush(opts).unwrap();
    }
    assert!(e.runs.iter().all(|open| open.run.format_version() == version));
    let total_sidecar_bytes = e
        .runs
        .iter()
        .map(|open| open.run.version_source(&e.alloc, None).stored_len())
        .sum::<usize>();
    assert!(
        total_sidecar_bytes > run::SIDECAR_CURSOR_WINDOW * e.runs.len() * 3,
        "corpus sidecars must substantially exceed one cursor window per run"
    );

    read_bytes.store(0, AtomicOrdering::Relaxed);
    let (page, more) = async_digest_page(&e, &all_buckets(), None, 4096);
    assert!(!page.is_empty());
    assert!(more);
    assert!(page.iter().all(|(_, version)| *version == 3));
    let got = read_bytes.load(AtomicOrdering::Relaxed) as usize;
    let bound = e.runs.len() * (run::SIDECAR_CURSOR_WINDOW + 3 * BS);
    assert!(got <= bound, "read {got} bytes, bounded-window limit {bound}");
    assert!(got * 3 < total_sidecar_bytes);

    struct Invalidate(usize);
    impl DigestCheckpoint for Invalidate {
        async fn checkpoint(&mut self) -> bool {
            self.0 += 1;
            false
        }
    }
    let want = all_buckets();
    let plan = e
        .plan_version_digest_page(&want, vdp_bucket, None, 4096, 0)
        .unwrap();
    let resident = plan.resident_bound_bytes(4096) as usize;
    assert!(resident >= e.runs.len() * 2 * run::SIDECAR_CURSOR_WINDOW);
    let per_run: usize = e
        .runs
        .iter()
        .map(|open| 2 * run::SIDECAR_CURSOR_WINDOW + (64 * BS).max(2 * open.run.max_block_bytes()))
        .sum();
    assert!(resident <= per_run + 4096,
        "version {version} digest plan resident bound {resident}");
    let mut invalidate = Invalidate(0);
    let stale = vals_rt::block_on(
        Engine::version_digest_page_from_plan_cooperative(
            plan,
            &want,
            vdp_bucket,
            None,
            4096,
            0,
            e.device(),
            &mut invalidate,
        ),
        || {},
    )
    .unwrap();
    assert!(stale.is_none());
    assert_eq!(invalidate.0, 1);
}


#[test]
fn expired_digest_pages_resume_without_gaps_or_duplicates() {
    for version in [run::RUN_SIDECAR_VERSION, run::RUN_VERSION] {
        run::with_run_write_version(version, || expired_digest_pages_resume(version));
    }
}

fn expired_digest_pages_resume(version: u16) {
    struct Expired;
    impl DigestCheckpoint for Expired {
        async fn checkpoint(&mut self) -> bool {
            true
        }
        fn expired(&self) -> bool {
            true
        }
    }
    let key = |i: u32| {
        let mut key = format!("feed.like/{i:08}/").into_bytes();
        let mut x = u64::from(i).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        while key.len() < 512 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            key.push(b'a' + (x % 26) as u8);
        }
        key
    };
    let read_bytes = std::sync::Arc::new(AtomicU64::new(0));
    let dev = AsyncCountingDevice {
        inner: MemBlockDevice::new(BS, 262144),
        reads: std::sync::Arc::new(AtomicU64::new(0)),
        read_bytes: read_bytes.clone(),
    };
    let cfg = Config {
        seg_blocks: 256,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };
    for run_version in 1..=5u128 {
        let first = 300 * run_version as u32;
        for i in (first..5_000u32).filter(|i| (i + run_version as u32) % 4 != 0) {
            e.put(&key(i), b"v", run_version).unwrap();
        }
        e.flush(opts).unwrap();
    }
    for i in (0..5_000u32).step_by(250) {
        e.put(&key(i), b"m", 9).unwrap();
    }
    assert!(e.runs.iter().all(|open| open.run.format_version() == version));
    let want: std::collections::HashSet<u32> = [1].into();
    for (modulus, max_bytes) in [(2_500, usize::MAX), (2_500, 1024), (37, usize::MAX), (37, 4096)] {
        let sparse = move |k: &[u8]| -> u32 {
            let id: u32 = std::str::from_utf8(&k[10..18]).unwrap().parse().unwrap();
            u32::from(id % modulus == 0)
        };
        let (expected, more) = e.version_digest_page(&want, sparse, None, usize::MAX, 0).unwrap();
        assert!(!more);
        assert!(expected.len() >= 2);
        let mut got = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        let (mut pages, mut resumed, mut empty) = (0usize, 0usize, 0usize);
        loop {
            let plan = e
                .plan_version_digest_page(&want, sparse, after.as_deref(), max_bytes, 0)
                .unwrap();
            let (page, more, resume) = vals_rt::block_on(
                Engine::version_digest_page_from_plan_cooperative(
                    plan,
                    &want,
                    sparse,
                    after.as_deref(),
                    max_bytes,
                    0,
                    e.device(),
                    &mut Expired,
                ),
                || {},
            )
            .unwrap()
            .unwrap();
            pages += 1;
            assert!(pages < 1_000, "resumable digest paging must terminate");
            assert!(page
                .iter()
                .all(|(k, _)| after.as_deref().is_none_or(|a| k.as_slice() > a)));
            if !more {
                assert!(resume.is_none());
                got.extend(page);
                break;
            }
            if let Some(resume) = &resume {
                resumed += 1;
                empty += usize::from(page.is_empty());
                assert!(page.iter().all(|(k, _)| k <= resume));
            }
            let next = resume.or_else(|| page.last().map(|(k, _)| k.clone())).unwrap();
            assert!(after.as_deref().is_none_or(|a| next.as_slice() > a));
            got.extend(page);
            after = Some(next);
        }
        assert_eq!(got, expected, "version {version} modulus {modulus} max_bytes {max_bytes}");
        assert!(
            max_bytes < usize::MAX || resumed > 1,
            "version {version} modulus {modulus} resumed {resumed}"
        );
        assert!(modulus < 2_500 || empty > 0, "version {version} max_bytes {max_bytes} empty {empty}");
    }
    let want_all: std::collections::HashSet<u32> = [0, 1].into();
    let any = |_: &[u8]| 0u32;
    let late = key(4_000);
    assert!(e
        .runs
        .iter()
        .all(|open| matches!(open.run.version_source(&e.alloc, Some(&late)), run::VersionSource::Blocks(_))));
    let total_bytes: usize = e
        .runs
        .iter()
        .map(|open| open.run.version_source(&e.alloc, None).stored_len())
        .sum();
    read_bytes.store(0, AtomicOrdering::Relaxed);
    let plan = e.plan_version_digest_page(&want_all, any, Some(&late), usize::MAX, 0).unwrap();
    let (_, more, resume) = vals_rt::block_on(
        Engine::version_digest_page_from_plan_cooperative(
            plan,
            &want_all,
            any,
            Some(&late),
            usize::MAX,
            0,
            e.device(),
            &mut Expired,
        ),
        || {},
    )
    .unwrap()
    .unwrap();
    assert!(more && resume.is_some_and(|r| r > late));
    let got = read_bytes.load(AtomicOrdering::Relaxed) as usize;
    let bound = e.runs.len() * 3 * (run::SIDECAR_CURSOR_WINDOW + 3 * BS);
    assert!(got <= bound && got * 2 < total_bytes, "version {version}: late resume read {got} bytes (bound {bound}, runs {total_bytes})");
}
