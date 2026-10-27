use super::*;

fn pin_cfg() -> Config {
    Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn pin_opts() -> RunBuildOpts {
    RunBuildOpts { num_buckets: 64, ..Default::default() }
}

fn fill_runs(e: &mut Engine<MemBlockDevice>, runs: u32, per_run: u32, stamp: u128) {
    for r in 0..runs {
        for i in 0..per_run {
            let key = format!("k{:04}", (r * 7 + i * 3) % (per_run * 2));
            e.put(key.as_bytes(), format!("v{r}-{i}").as_bytes(), stamp + (r * per_run + i) as u128)
                .unwrap();
        }
        run_flush(e, pin_opts()).unwrap();
    }
}

fn stream_all(sources: &[run::RunStreamSource<MemBlockDevice>]) -> Vec<Vec<(Vec<u8>, u128)>> {
    sources
        .iter()
        .map(|source| {
            let mut out = Vec::new();
            match source.version_stream().unwrap() {
                Some(mut stream) => loop {
                    match stream.next_step().unwrap() {
                        run::SidecarCursorStep::Entry(entry) => out.push(entry),
                        run::SidecarCursorStep::Checkpoint => {}
                        run::SidecarCursorStep::Done => break,
                    }
                },
                None => {
                    let mut versions = source.sidecar_versions().unwrap();
                    versions.fold_page(usize::MAX, |key, raw| out.push((key.to_vec(), raw))).unwrap();
                }
            }
            out
        })
        .collect()
}

fn churn(e: &mut Engine<MemBlockDevice>, rounds: u32, stamp: u128) {
    for round in 0..rounds {
        for i in 0..40u32 {
            let key = format!("churn{round:03}-{i:03}");
            e.put(key.as_bytes(), b"churn-value-padding-padding", stamp + (round * 40 + i) as u128).unwrap();
        }
        run_flush(e, pin_opts()).unwrap();
        e.compact_runs(pin_opts(), None).unwrap();
    }
}

#[test]
fn pinned_runs_survive_merges_until_released() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 4, 60, 1);
    let pinned = e.pin_runs(true);
    let pin = pinned.pin.as_ref().unwrap();
    let before = stream_all(&pinned.sources);
    let pinned_segs: Vec<SegmentId> =
        e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    let epoch = e.runs_removed_epoch();
    churn(&mut e, 12, 10_000);
    assert!(e.runs_removed_epoch() > epoch);
    assert!(e.run_pin_valid(pin));
    for seg in &pinned_segs {
        assert!(!e.alloc.is_free(*seg), "a pinned run segment is never freed while pinned");
    }
    assert!(e.protected_segments().iter().filter(|s| pinned_segs.contains(s)).count() == pinned_segs.len());
    assert!(e.orphan_segments_for_test().iter().all(|s| !pinned_segs.contains(s)));
    assert_eq!(e.pinned_retired_bytes(), pinned_segs.len() as u64 * 8 * BS as u64);
    assert_eq!(stream_all(&pinned.sources), before, "pinned snapshot reads back unchanged after merges");
    assert!(!e.run_pins_reapable());
    assert_eq!(e.reap_run_pins(), 0);
    let runs = std::mem::take(&mut e.runs);
    assert_eq!(e.compact().unwrap(), CompactStats::default(), "no log rewrite while retired runs are pinned");
    assert_eq!(e.compact_step(usize::MAX).unwrap(), (CompactStats::default(), true));
    e.runs = runs;
    for seg in &pinned_segs {
        assert!(!e.alloc.is_free(*seg));
    }
    assert_eq!(stream_all(&pinned.sources), before);

    drop(pinned);
    assert!(e.run_pins_reapable());
    let free_before = e.alloc.free_segments();
    assert_eq!(e.reap_run_pins(), pinned_segs.len());
    assert_eq!(e.alloc.free_segments(), free_before + pinned_segs.len() as u64);
    assert_eq!(e.pinned_retired_bytes(), 0);
    for seg in &pinned_segs {
        assert!(e.alloc.is_free(*seg));
    }
    let mut control = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut control, 4, 60, 1);
    churn(&mut control, 12, 10_000);
    assert_eq!(e.alloc.used_segments(), control.alloc.used_segments(), "no segment leaks after release");
    let dev = e.device().share();
    drop(e);
    let reopened = Engine::open(dev, pin_cfg()).unwrap();
    assert_eq!(reopened.get(b"churn011-039").unwrap().as_deref(), Some(&b"churn-value-padding-padding"[..]));
}

#[test]
fn unpinned_and_post_pin_runs_are_freed_at_commit() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 3, 40, 1);
    let unpinned = e.pin_runs(false);
    assert!(unpinned.pin.is_none());
    let segs: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    e.compact_runs(pin_opts(), None).unwrap();
    assert!(segs.iter().all(|s| e.alloc.is_free(*s)), "no pin: inputs free at commit as before");
    assert_eq!(e.pinned_retired_bytes(), 0);

    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    fill_runs(&mut e, 2, 30, 50_000);
    let later: Vec<SegmentId> = e.run_handles()[1..].iter().flat_map(|h| h.segs.iter().copied()).collect();
    e.compact_runs(pin_opts(), None).unwrap();
    assert!(held.iter().all(|s| !e.alloc.is_free(*s)));
    assert!(later.iter().all(|s| e.alloc.is_free(*s)), "runs flushed after the pin are not held");
    drop(pinned);
    e.reap_run_pins();
    assert!(held.iter().all(|s| e.alloc.is_free(*s)));
}

#[test]
fn truncate_and_pressure_revoke_pins() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 3, 40, 1);
    let pinned = e.pin_runs(true);
    churn(&mut e, 2, 10_000);
    assert!(e.pinned_retired_bytes() > 0);
    e.truncate().unwrap();
    assert!(!e.run_pin_valid(pinned.pin.as_ref().unwrap()), "truncate revokes outstanding pins");
    assert_eq!(e.pinned_retired_bytes(), 0);
    assert_eq!(e.alloc.used_segments(), 0);
    drop(pinned);

    fill_runs(&mut e, 3, 40, 1);
    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    churn(&mut e, 1, 20_000);
    assert!(e.run_pin_valid(pinned.pin.as_ref().unwrap()));
    let revocations = e.run_pin_revocations();
    e.set_run_pin_pressure_for_test(true);
    assert!(e.run_pins_reapable());
    assert_eq!(e.reap_run_pins(), held.len());
    assert!(!e.run_pin_valid(pinned.pin.as_ref().unwrap()), "region pressure revokes the pin");
    assert_eq!(e.run_pin_revocations(), revocations + 1);
    assert!(held.iter().all(|s| e.alloc.is_free(*s)));
    e.set_run_pin_pressure_for_test(false);
    drop(pinned);

    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    e.set_run_pin_pressure_for_test(true);
    churn(&mut e, 1, 30_000);
    assert!(!e.run_pin_valid(pinned.pin.as_ref().unwrap()), "a merge under pressure revokes instead of holding");
    assert!(held.iter().all(|s| e.alloc.is_free(*s)));
    assert_eq!(e.pinned_retired_bytes(), 0);
}

#[test]
fn async_merge_commit_holds_pinned_inputs() {
    if co_compact_env(false) { return; }
    std::env::set_var("VALS_MERGE_TIER_MIN_K", "2");
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 2, 60, 1);
    let pinned = e.pin_runs(true);
    let before = stream_all(&pinned.sources);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    let plan = e.compact_plan(pin_opts(), None).unwrap().unwrap();
    let wdev = Arc::new(e.device().sibling().unwrap());
    let written = vals_rt::block_on(compact_and_write(plan, wdev), || {}).unwrap();
    e.compact_commit(written).unwrap();
    assert_eq!(e.run_count(), 1);
    assert!(held.iter().all(|s| !e.alloc.is_free(*s)));
    for i in 0..400u32 {
        e.put(format!("fill{i:04}").as_bytes(), b"overwrite-pressure-on-free-segments", 90_000 + i as u128).unwrap();
    }
    run_flush(&mut e, pin_opts()).unwrap();
    assert_eq!(stream_all(&pinned.sources), before);
    drop(pinned);
    e.reap_run_pins();
    assert!(held.iter().all(|s| e.alloc.is_free(*s)));
    assert_eq!(e.pinned_retired_bytes(), 0);
}

#[test]
fn pin_limit_revokes_at_commit_and_revoke_bumps_storage_generation() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 3, 40, 1);
    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    let bytes = held.len() as u64 * 8 * BS as u64;
    e.set_run_pin_limit(Some(bytes));
    churn(&mut e, 1, 10_000);
    assert_eq!(e.run_pin_state(pinned.pin.as_ref().unwrap()), RunPinState::Valid, "at the limit is not over it");
    assert_eq!(e.held_retired_bytes(), bytes);
    assert_eq!(e.pinned_retired_bytes(), bytes);
    drop(pinned);
    assert_eq!(e.held_retired_bytes(), 0, "a released pin holds nothing before the reap");
    assert_eq!(e.pinned_retired_bytes(), bytes);
    assert_eq!(e.reap_run_pins(), held.len());

    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    e.set_run_pin_limit(Some(held.len() as u64 * 8 * BS as u64 - 1));
    let (revocations, limit_revocations) = (e.run_pin_revocations(), e.run_pin_limit_revocations());
    churn(&mut e, 1, 20_000);
    assert_eq!(e.run_pin_state(pinned.pin.as_ref().unwrap()), RunPinState::OverLimit, "over the limit at the retiring commit");
    assert!(held.iter().all(|s| e.alloc.is_free(*s)), "the over-limit commit frees instead of holding");
    assert_eq!(e.pinned_retired_bytes(), 0);
    assert_eq!((e.run_pin_revocations(), e.run_pin_limit_revocations()), (revocations, limit_revocations + 1));
    drop(pinned);
    e.set_run_pin_limit(None);

    let pinned = e.pin_runs(true);
    churn(&mut e, 1, 30_000);
    assert!(e.pinned_retired_bytes() > 0);
    let generation = e.storage_generation();
    assert!(e.revoke_run_pins() > 0);
    assert!(e.storage_generation() > generation, "a public revoke bumps the storage generation");
    assert_eq!(e.run_pin_state(pinned.pin.as_ref().unwrap()), RunPinState::Revoked);
}

#[test]
fn held_v5_run_index_is_charged_until_the_pin_is_released() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 4, 60, 1);
    let index_bytes: u64 = e.runs.iter().map(|or| crate::engine::pins::retained_index_bytes(&or.run)).sum();
    assert!(index_bytes > 0);
    let pinned = e.pin_runs(true);
    churn(&mut e, 3, 10_000);
    assert_eq!(e.held_retired_index_bytes(), index_bytes);
    let charged = e.capacity_stats().run_resident_bytes;
    drop(pinned);
    assert_eq!(e.held_retired_index_bytes(), 0, "released sources drop the index before the reap");
    assert_eq!(e.capacity_stats().run_resident_bytes, charged - index_bytes);

    run::with_run_write_version(run::RUN_SIDECAR_VERSION, || {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
        fill_runs(&mut e, 4, 60, 1);
        assert!(e.runs.iter().all(|or| or.run.has_sidecar()));
        let pinned = e.pin_runs(true);
        churn(&mut e, 3, 10_000);
        assert!(e.held_retired_bytes() > 0);
        assert_eq!(e.held_retired_index_bytes(), 0, "sidecar sources do not keep the block index");
        drop(pinned);
    });
}

#[test]
fn commit_limit_counts_only_bytes_still_held() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 15), pin_cfg());
    fill_runs(&mut e, 3, 40, 1);
    let first = e.pin_runs(true);
    churn(&mut e, 1, 10_000);
    drop(first);
    let released = e.pinned_retired_bytes();
    assert!(released > 0);
    assert_eq!(e.held_retired_bytes(), 0);

    let second = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    let bytes = held.len() as u64 * 8 * BS as u64;
    e.set_run_pin_limit(Some(bytes));
    churn(&mut e, 1, 20_000);
    assert_eq!(
        e.run_pin_state(second.pin.as_ref().unwrap()),
        RunPinState::Valid,
        "an earlier pin's released, unreaped bytes do not count against this pin's limit"
    );
    assert_eq!(e.held_retired_bytes(), bytes);
    assert_eq!(e.pinned_retired_bytes(), released + bytes);
    assert!(held.iter().all(|s| !e.alloc.is_free(*s)));
}

#[test]
fn real_region_pressure_revokes_a_pin_and_frees_its_runs() {
    let mut e = Engine::create_prewiped(MemBlockDevice::new(BS, 1 << 11), pin_cfg());
    e.set_maint_reserve_enabled(true);
    fill_runs(&mut e, 4, 60, 1);
    let pinned = e.pin_runs(true);
    let held: Vec<SegmentId> = e.run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    let revocations = e.run_pin_revocations();
    let mut rounds = 0u32;
    let mut shed_seen = false;
    while e.run_pin_valid(pinned.pin.as_ref().unwrap()) {
        assert!(rounds < 400, "pressure never revoked the pin");
        for i in 0..40u32 {
            let key = format!("press{rounds:03}-{i:03}");
            e.put(key.as_bytes(), b"pressure-value-padding", 10_000 + (rounds * 40 + i) as u128).unwrap();
        }
        run_flush(&mut e, pin_opts()).unwrap();
        shed_seen = e.admission_shed_probability() > 0.0 || e.at_maintenance_floor();
        e.compact_runs(pin_opts(), None).unwrap();
        rounds += 1;
    }
    assert!(rounds > 1, "the pin rode at least one merge before pressure");
    assert!(shed_seen, "the revoking merge ran inside the admission band or at the floor, not under a test hook");
    assert_eq!(e.run_pin_state(pinned.pin.as_ref().unwrap()), RunPinState::Revoked);
    assert_eq!(e.run_pin_revocations(), revocations + 1);
    assert_eq!(e.pinned_retired_bytes(), 0);
    assert!(held.iter().all(|s| e.alloc.is_free(*s)), "revocation frees the held runs");
    let last = format!("press{:03}-039", rounds - 1);
    assert_eq!(e.get(last.as_bytes()).unwrap().as_deref(), Some(&b"pressure-value-padding"[..]));
}
