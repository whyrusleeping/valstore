use super::*;
use crate::attribution as a;
use crate::keyfilter::KeyFilterParams;
use std::time::Instant;

fn run_count_env() -> bool {
    isolated_env_test(&[
        ("VALS_VLOG_CO_COMPACT", Some("0")),
        ("VALS_SHADOW_EST", Some("1")),
        ("VALS_SHADOW_SAMPLE_BLOCKS", Some("2")),
        ("VALS_SHADOW_FOLD", Some("0")),
        ("VALS_MERGE_FORCE_RUNS", Some("3")),
        ("VALS_MERGE_TIER_K", None),
        ("VALS_MERGE_TIER_MIN_K", None),
        ("VALS_MERGE_TIER_SIZE_RATIO", None),
        ("VALS_MERGE_INPUT_CAP_BYTES", None),
        ("VALS_MERGE_FREEZE_BYTES", None),
    ])
}

fn fixture() -> Engine<MemBlockDevice> {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(4096, 8192), Config {
        seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Unsorted,
    });
    e.key_filter.set_params(KeyFilterParams { enabled: true, min_keys: 1024,
        ..KeyFilterParams::from_env() });
    e.set_orphan_reclaim_for_test(false);
    e
}

fn ungate(e: &mut Engine<MemBlockDevice>) {
    while e.free_fraction() > SHADOW_EST_FREE_GATE { e.alloc.allocate().unwrap(); }
}

fn batch(e: &mut Engine<MemBlockDevice>, round: usize, n: usize, stride: usize) {
    for i in 0..n {
        let key = format!("k{:08}", i * stride + round);
        e.put(key.as_bytes(), b"value", (round + 1) as u128).unwrap();
    }
    e.flush(RunBuildOpts { block_target_bytes: 4096, bloom_bits_per_key: 10,
        ..RunBuildOpts::default() }).unwrap().unwrap();
}

fn assert_shadow_reference(e: &Engine<MemBlockDevice>) {
    e.refresh_shadow_estimates();
    let c = e.shadow.cache.borrow();
    assert!(!c.gated);
    let reference: Vec<u64> = e.runs.iter().enumerate().map(|(i, or)| {
        e.shadow_sample_from_scratch(i, &c.samples[&or.id])
    }).collect();
    assert_eq!(c.per_run, reference);
    assert_eq!(c.total, reference.iter().sum::<u64>());
    assert_eq!(c.samples.len(), e.run_count());
}

#[test]
fn cached_shadow_samples_increase_run_metadata_charge() {
    if run_count_env() { return; }
    let mut e = fixture();
    ungate(&mut e);
    batch(&mut e, 0, 256, 1);
    let before = e.run_metadata_resident_bytes();
    e.refresh_shadow_estimates();
    let cached = e.run_metadata_resident_bytes();
    assert!(cached > before, "cached samples must increase the serving-memory charge");
    assert_eq!(e.capacity_stats().run_resident_bytes, cached);
    e.refresh_shadow_estimates();
    assert_eq!(e.run_metadata_resident_bytes(), cached, "cache hits retain the same allocations");
    std::env::set_var("VALS_SHADOW_EST", "0");
    e.refresh_shadow_estimates();
    assert_eq!(e.run_metadata_resident_bytes(), before, "dropping samples releases their charge");
}

#[test]
fn incremental_shadow_matches_cached_sample_reference() {
    if run_count_env() { return; }
    let mut e = fixture();
    ungate(&mut e);
    batch(&mut e, 0, 256, 1);
    assert_shadow_reference(&e);
    let first_id = e.runs[0].id;
    let first_keys: Vec<_> = e.shadow.cache.borrow().samples[&first_id].entries.iter()
        .map(|e| e.key.clone()).collect();
    let reads = a::SHADOW_BLOCKS.get();
    batch(&mut e, 1, 256, 1);
    assert_shadow_reference(&e);
    assert_eq!(a::SHADOW_BLOCKS.get() - reads, shadow_sample_blocks().min(e.runs[1].run.index_len()) as u64,
        "only the new run is sampled");
    assert_eq!(first_keys, e.shadow.cache.borrow().samples[&first_id].entries.iter()
        .map(|e| e.key.clone()).collect::<Vec<_>>());
    let reads = a::SHADOW_BLOCKS.get();
    e.refresh_shadow_estimates();
    assert_eq!(a::SHADOW_BLOCKS.get(), reads);
    batch(&mut e, 2, 1024, 1);
    assert_shadow_reference(&e);
    let sizes: Vec<_> = e.runs.iter().map(|or| or.meta.handle.len).collect();
    assert_eq!(select_recurring_merge(&sizes, &e.merge_policy).unwrap().end, 2);
    compact_off(&mut e, RunBuildOpts::default(), None);
    assert_shadow_reference(&e);
    e.put(b"k00000002", b"mutable", 100).unwrap();
    assert_shadow_reference(&e);
    let reads = a::SHADOW_BLOCKS.get();
    e.forget_keys(&[b"k00000002".to_vec()].into_iter().collect(), RunBuildOpts::default()).unwrap().unwrap();
    assert_shadow_reference(&e);
    assert!(a::SHADOW_BLOCKS.get() >= reads);
    e.truncate().unwrap();
    ungate(&mut e);
    assert_shadow_reference(&e);
    batch(&mut e, 0, 256, 1);
    assert_shadow_reference(&e);
    let sampled_key = e.shadow.cache.borrow().samples[&e.runs[0].id].entries[0].key.clone();
    e.put(&sampled_key, b"new", 1000).unwrap();
    assert_shadow_reference(&e);
    assert!(e.shadowed_bytes_estimate_cached() > 0);
    e.index.remove(&sampled_key);
    e.bump_mutable_generation();
    assert_shadow_reference(&e);
    assert_eq!(e.shadowed_bytes_estimate_cached(), 0);
}

#[test]
fn shadow_reprobes_survivor_when_a_newer_run_disappears() {
    if run_count_env() { return; }
    let mut e = fixture();
    ungate(&mut e);
    batch(&mut e, 0, 128, 1);
    assert_shadow_reference(&e);
    batch(&mut e, 0, 128, 1);
    assert_shadow_reference(&e);
    assert!(e.shadowed_bytes_estimate_cached() > 0);
    let reads = a::SHADOW_BLOCKS.get();
    e.runs.pop();
    e.manifest_gen += 1;
    assert_shadow_reference(&e);
    assert_eq!(e.shadowed_bytes_estimate_cached(), 0);
    assert_eq!(a::SHADOW_BLOCKS.get(), reads, "removal re-probes cached entries without IO");
}

#[test]
fn probe_get_filter_negative_skips_only_covered_runs() {
    if run_count_env() { return; }
    let mut e = fixture();
    for round in 0..3 { batch(&mut e, round, 256, 3); }
    assert!(e.runs.iter().all(|or| or.covered));
    let key = (0..10_000).map(|i| format!("k{i:08}-absent").into_bytes()).find(|key| {
        e.key_filter.excludes(&run::BloomKey::new(key))
            && e.runs.iter().any(|or| or.run.locate_block(&e.alloc, key).is_some())
    }).unwrap();
    let before = a::lookup_snapshot()[0];
    assert!(matches!(e.probe_get(&key), StagedGet::Absent));
    assert_eq!(a::lookup_snapshot()[0][1], before[1], "no covered bloom probes on a negative");
    let filter = std::mem::replace(&mut e.key_filter, crate::keyfilter::KeyFilter::new(KeyFilterParams::from_env()));
    assert!(matches!(e.probe_get(&key), StagedGet::Runs { .. }), "knockout must expose a false positive");
    assert!(a::lookup_snapshot()[0][1] > before[1]);
    e.key_filter = filter;
    let idx = e.runs.iter().position(|or| or.run.locate_block(&e.alloc, &key).is_some()).unwrap();
    e.runs[idx].covered = false;
    let probe = e.probe_get(&key);
    let StagedGet::Runs { probes, .. } = probe else { panic!("uncovered run was skipped"); };
    assert_eq!(probes.len(), 1);
    assert!(vals_rt::block_on(resolve_run_probe(&key, &probes, e.device()), || {}).unwrap().is_none());
    assert!(matches!(e.probe_get(b"k00000000"), StagedGet::Runs { .. }));
}

#[test]
fn cover_scan_avoids_inner_forced_window_and_commit_counts_it() {
    if run_count_env() { return; }
    let mut e = fixture();
    batch(&mut e, 0, 256, 1);
    batch(&mut e, 1, 256, 1);
    batch(&mut e, 2, 1024, 1);
    e.reset_key_filter_for_test();
    let mut cover = e.cover_scan_plan().unwrap();
    assert_eq!(cover.run_id, e.runs[2].id);
    let dev = e.device().sibling().unwrap();
    while !cover.step(&dev, 1 << 20).unwrap() {}
    assert!(e.cover_scan_commit(cover));
    let count = inner_window_merges_total();
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    assert_eq!(inner_window_merges_total(), count);
    let reserved = plan.segments();
    drop(plan);
    e.free_compact_segs(&reserved);
    assert_eq!(inner_window_merges_total(), count, "aborted plans are not merges");
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap().unwrap();
    let written = vals_rt::block_on(compact_and_write(plan, Arc::new(dev)), || {}).unwrap();
    assert_eq!(inner_window_merges_total(), count);
    e.runs[0].meta.entry_count += 1;
    assert!(matches!(e.compact_commit(written), Err(EngineError::CompactionConflict)));
    assert_eq!(inner_window_merges_total(), count, "failed commits are not merges");
    e.runs[0].meta.entry_count -= 1;
    compact_off(&mut e, RunBuildOpts::default(), None);
    assert_eq!(inner_window_merges_total(), count + 1);
}

#[test]
fn stranded_runs_count_the_non_frozen_prefix() {
    if run_count_env() { return; }
    let mut e = fixture();
    for i in 0..6 { batch(&mut e, i, 1, 10); }
    let gib = 1 << 30;
    e.merge_policy.size_cap = 2 * gib;
    e.merge_policy.freeze_bytes = 2 * gib;
    for (or, tenth) in e.runs.iter_mut().zip([1, 10, 3, 11, 10, 9]) {
        or.meta.handle.len = tenth * gib / 10;
    }
    assert_eq!(e.stranded_run_count(), 0);
    e.runs[4].meta.handle.len = 19 * gib / 10;
    e.runs.pop();
    assert_eq!(e.frozen_run_count(), 1);
    assert_eq!(e.stranded_run_count(), 4);
    std::env::set_var("VALS_MERGE_FREEZE_BYTES", "1");
    assert_eq!(e.frozen_run_count(), 1, "policy is fixed at open");
}

fn staged_read(e: &Engine<MemBlockDevice>, key: &[u8]) -> (Option<Vec<u8>>, u64) {
    match e.probe_get(key) {
        StagedGet::Absent => (None, 0),
        StagedGet::Runs { probes, mutable, .. } => {
            let (run, blocks) = vals_rt::block_on(resolve_run_probe_counted(key, &probes, e.device()), || {}).unwrap();
            let entry = run.map(|r| IndexEntry { ptr: r.ptr, version: r.version, seq: r.seq, tombstone: r.tombstone });
            let best = entry.into_iter().chain(mutable).max_by_key(|e| e.version);
            let value = best.map(|entry| {
                let plan = e.plan_get_ptr(&entry.ptr).unwrap();
                vals_rt::block_on(plan.execute(e.device()), || {}).unwrap().value
            });
            (value, blocks)
        }
        _ => panic!("expected immutable runs"),
    }
}

#[test]
#[ignore]
fn run_count_microbench_100_runs() {
    if run_count_env() { return; }
    let mut old_us = Vec::new();
    let mut new_us = Vec::new();
    let mut old_counts = [0u64; 2];
    let mut new_counts = [0u64; 2];
    for _ in 0..20 {
        let mut e = fixture();
        for round in 0..99 { batch(&mut e, round, 256, 100); }
        ungate(&mut e);
        e.refresh_shadow_estimates();
        batch(&mut e, 99, 256, 100);
        assert_eq!(e.run_count(), 100);
        let before = [a::SHADOW_BLOCKS.get(), a::SHADOW_BLOOMS.get()];
        let start = Instant::now();
        e.refresh_shadow_estimates();
        new_us.push(start.elapsed().as_nanos() as u64);
        for (i, after) in [a::SHADOW_BLOCKS.get(), a::SHADOW_BLOOMS.get()].into_iter().enumerate() { new_counts[i] += after - before[i]; }
        let before = [a::SHADOW_BLOCKS.get(), a::SHADOW_BLOOMS.get()];
        let start = Instant::now();
        let total: u64 = (0..e.run_count()).map(|i| {
            let sample = e.sample_shadow_run(i);
            e.shadow_sample_from_scratch(i, &sample)
        }).sum();
        std::hint::black_box(total);
        old_us.push(start.elapsed().as_nanos() as u64);
        for (i, after) in [a::SHADOW_BLOCKS.get(), a::SHADOW_BLOOMS.get()].into_iter().enumerate() { old_counts[i] += after - before[i]; }
    }
    println!("shadow runs=100 n=20 old_us={:.3} new_us={:.3} old_blocks={:.1} new_blocks={:.1} old_blooms={:.1} new_blooms={:.1}",
        old_us.iter().sum::<u64>() as f64 / 20_000.0, new_us.iter().sum::<u64>() as f64 / 20_000.0,
        old_counts[0] as f64 / 20.0, new_counts[0] as f64 / 20.0, old_counts[1] as f64 / 20.0, new_counts[1] as f64 / 20.0);
    let mut e = fixture();
    for round in 0..100 { batch(&mut e, round, 256, 100); }
    assert_eq!(e.run_count(), 100);
    for hit in [false, true] {
        for filter in [false, true] {
            let saved = if filter { None } else {
                Some(std::mem::replace(&mut e.key_filter, crate::keyfilter::KeyFilter::new(KeyFilterParams::from_env())))
            };
            let before = a::lookup_snapshot()[0][1];
            let mut ns = Vec::new();
            let mut blocks = 0u64;
            for i in 0..20_000 {
                let k = (i * 7919) % 25_600;
                let key = if hit { format!("k{k:08}") } else { format!("k{k:08}-absent") };
                let start = Instant::now();
                let (value, n) = staged_read(&e, key.as_bytes());
                ns.push(start.elapsed().as_nanos() as u64);
                assert_eq!(value.is_some(), hit);
                blocks += n;
            }
            ns.sort_unstable();
            println!("get runs=100 hit={hit} filter={filter} n=20000 p50_us={:.3} p99_us={:.3} index_blocks/get={:.4} blooms/get={:.4}",
                ns[10_000] as f64 / 1000.0, ns[19_800] as f64 / 1000.0, blocks as f64 / 20_000.0,
                (a::lookup_snapshot()[0][1] - before) as f64 / 20_000.0);
            if let Some(saved) = saved { e.key_filter = saved; }
        }
    }
}
