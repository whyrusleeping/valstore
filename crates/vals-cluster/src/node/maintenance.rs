use super::*;

const COMPACT_MAX_STEPS_PER_ZONE_PER_TICK: usize = 64;

const PRESSURE_FREE_FRACTION: f64 = 0.50;

const PRESSURE_DEAD_MIN_FRACTION: f64 = 0.10;

pub(super) struct MaintSignals {
    pub(super) tail_epochs: u64,
    pub(super) memtable_bytes: u64,
    pub(super) flush_in_flight: bool,
    pub(super) flush_cleanup_pending: bool,
    pub(super) run_count: usize,
    pub(super) merge_selectable: bool,
    pub(super) compact_in_flight: bool,
    pub(super) free_fraction: f64,
    pub(super) at_maintenance_floor: bool,
    pub(super) lsm_dead_fraction: Option<f64>,
}

impl MaintSignals {
    fn sample<D: BlockDevice>(s: &VersionedStore<D>) -> Self {
        MaintSignals {
            tail_epochs: s.tail_epochs(),
            memtable_bytes: s.memtable_bytes(),
            flush_in_flight: s.flush_in_flight(),
            flush_cleanup_pending: s.engine().flush_cleanup_pending(),
            run_count: s.run_count(),
            merge_selectable: s.recurring_merge_selectable(),
            compact_in_flight: s.compact_in_flight(),
            free_fraction: s.engine().free_fraction(),
            at_maintenance_floor: s.engine().at_maintenance_floor(),
            lsm_dead_fraction: s.engine().lsm_dead_fraction_gate(),
        }
    }
}

#[derive(Default)]
pub(super) struct MaintDue {
    pub(super) flush: bool,
    pub(super) compact: bool,
    pub(super) tombstone_gc: bool,
    pub(super) pressure: bool,
}

pub(super) fn zone_maint_due(
    s: &MaintSignals,
    flush_tail_epochs: u64,
    flush_max_bytes: u64,
    run_threshold: usize,
) -> MaintDue {
    let pressure = s.at_maintenance_floor
        || (s.free_fraction <= PRESSURE_FREE_FRACTION
            && s.lsm_dead_fraction
                .is_some_and(|d| d >= PRESSURE_DEAD_MIN_FRACTION));
    let flush = s.flush_cleanup_pending
        || ((s.tail_epochs >= flush_tail_epochs
            || s.memtable_bytes >= flush_max_bytes
            || (pressure && s.tail_epochs >= 1))
            && !s.flush_in_flight);
    let compact = (s.merge_selectable
        || s.run_count >= run_threshold
        || (pressure && s.run_count >= 1))
        && !s.compact_in_flight;
    MaintDue {
        flush,
        compact,
        tombstone_gc: false,
        pressure,
    }
}

pub(super) fn zone_store_due<D: BlockDevice>(
    s: &VersionedStore<D>,
    flush_tail_epochs: u64,
    flush_max_bytes: u64,
    run_threshold: usize,
    now: u64,
) -> MaintDue {
    let mut d = zone_maint_due(&MaintSignals::sample(s), flush_tail_epochs, flush_max_bytes, run_threshold);
    d.tombstone_gc = !s.compact_in_flight() && s.tombstone_gc_due(now);
    d.flush |= !s.flush_in_flight() && s.tombstone_gc_flush_due(now);
    d
}

pub(super) fn plan_zone_merge<D: BlockDevice>(
    s: &mut VersionedStore<D>,
    due: &MaintDue,
    now: u64,
) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
    let planned = if !due.compact {
        Ok(None)
    } else if due.pressure {
        s.compact_plan_pressure(now)
    } else {
        s.compact_plan(now)
    };
    match planned {
        Ok(None) if due.tombstone_gc => s.compact_plan_tombstone_gc(now),
        planned => planned,
    }
}

pub(super) fn reserve_merge(
    kind: vals_store::MergeKind,
    owner: usize,
    bytes: u64,
) -> Option<crate::ram_broker::Reservation> {
    match kind {
        vals_store::MergeKind::Recurring | vals_store::MergeKind::Forced => {
            crate::ram_broker::try_reserve(crate::ram_broker::Class::Merge, owner, bytes)
        }
        _ => Some(crate::ram_broker::reserve_overdraw(crate::ram_broker::Class::Merge, bytes)),
    }
}

const COMPACT_MIN_RECLAIM_FRACTION: f64 = 0.20;

pub(super) const COMPACT_QUIESCE_WAIT: Duration = Duration::from_millis(100);

pub(super) const COMPACT_QUIESCE_WAIT_MAX: Duration = Duration::from_secs(2);

pub(super) const CLEANUP_QUIESCE_WAIT_MAX: Duration = Duration::from_millis(250);

const CLEANUP_QUIESCE_BACKOFF_FACTOR: u32 = 8;

pub(super) fn quiesce_wait(consecutive_timeouts: u32) -> Duration {
    let base = COMPACT_QUIESCE_WAIT.as_millis() as u64;
    let factor = 1u64.checked_shl(consecutive_timeouts).unwrap_or(u64::MAX);
    let millis = base.saturating_mul(factor);
    Duration::from_millis(millis).min(COMPACT_QUIESCE_WAIT_MAX)
}

pub(super) fn cleanup_quiesce_wait(consecutive_timeouts: u32) -> Duration {
    quiesce_wait(consecutive_timeouts).min(CLEANUP_QUIESCE_WAIT_MAX)
}

pub(super) async fn compact_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    every: Duration,
) {
    let mut every = every;
    let mut timer = match Timer::every(every) {
        Ok(t) => t,
        Err(e) => {
            log::error!(
                "node {}: timerfd creation failed ({e}); background compaction DISABLED \
                 — value-log garbage will not be reclaimed",
                shared.node_id
            );
            return;
        }
    };
    let stop = shared.add_shutdown_notify();
    let mut consecutive_timeouts: u32 = 0;
    let mut last_sweep = shared.sweep_every();
    let mut last_headroom_warn: std::collections::HashMap<ZoneId, std::time::Instant> =
        std::collections::HashMap::new();
    loop {
        if let Either::Right(()) = vals_rt::race(timer.tick(), stop.notified()).await {
            return;
        }
        if shared.is_shutdown() {
            return;
        }
        let cur = shared.sweep_every();
        if cur != last_sweep {
            last_sweep = cur;
            let want = shared.settings.compact_interval(cur);
            match Timer::every(want) {
                Ok(t) => {
                    timer = t;
                    every = want;
                    log::info!("node {}: compaction cadence now {every:?}", shared.node_id);
                }
                Err(e) => log::error!(
                    "node {}: re-arming compaction timer at {want:?} failed ({e}); \
                     keeping {every:?}",
                    shared.node_id
                ),
            }
        }
        let any_compactable = shared.sweep_zone_ids(false).into_iter().any(|zone_id| {
            shared.zone_store(zone_id).is_ok_and(|store| {
                let store = store.borrow();
                store.run_count() == 0
                    && !store.flush_in_flight()
                    && store.engine().reclaimable_fraction() >= COMPACT_MIN_RECLAIM_FRACTION
            })
        });
        if !any_compactable {
            continue;
        }
        let now = physical_now_ms();
        let step_segs = shared.settings.compact_step_segments;
        'zones: for zone_id in shared.sweep_zone_ids(false) {
            let Ok(store) = shared.zone_store(zone_id) else {
                continue;
            };
            let reclaimable_before = {
                let s = store.borrow();
                if s.run_count() > 0 || s.flush_in_flight() {
                    continue;
                }
                let reclaimable = s.engine().reclaimable_fraction();
                if reclaimable < COMPACT_MIN_RECLAIM_FRACTION {
                    crate::observe::counters().compaction_skipped_clean.inc();
                    continue;
                }
                reclaimable
            };
            let mut steps = 0usize;
            let mut zone_freed: u64 = 0;
            let mut zone_touched = false;
            loop {
                let stepped = {
                    let _pause = shared.read_gate().pause();
                    let wait = quiesce_wait(consecutive_timeouts);
                    match drain_reads(&shared, Some(&stop), wait).await {
                        Drain::Drained => consecutive_timeouts = 0,
                        Drain::TimedOut => {
                            consecutive_timeouts = consecutive_timeouts.saturating_add(1);
                            log::warn!(
                                "node {}: compaction step skipped — {} in-flight read(s) did \
                                 not drain within {wait:?} (retrying next tick)",
                                shared.node_id,
                                shared.inflight()
                            );
                            crate::observe::counters().compaction_quiesce_timeouts.inc();
                            continue 'zones;
                        }
                        Drain::Shutdown => return,
                    }
                    if shared.is_shutdown() {
                        return;
                    }
                    store.borrow_mut().compact_step(now, step_segs)
                };
                match stepped {
                    Ok((stats, done)) => {
                        zone_freed += stats.segments_freed as u64;
                        zone_touched = true;
                        crate::observe::counters()
                            .segments_freed
                            .add(stats.segments_freed as u64);
                        if done {
                            break;
                        }
                    }
                    Err(EngineError::InsufficientHeadroom {
                        needed_segments,
                        free_segments,
                    }) => {
                        crate::observe::counters().compaction_headroom_skips.inc();
                        let should_warn = last_headroom_warn
                            .get(&zone_id)
                            .is_none_or(|last| last.elapsed() >= Duration::from_secs(60));
                        if should_warn {
                            last_headroom_warn.insert(zone_id, std::time::Instant::now());
                            let used = store.borrow().engine().used_segments();
                            log::warn!(
                                "node {}: zone {zone_id} compaction step skipped — region too full \
                                 (a bounded step needs ~{needed_segments} free segments, \
                                 {free_segments} free, {used} used); reclamation is paused until \
                                 space frees or region_mib is raised",
                                shared.node_id
                            );
                        }
                        break;
                    }
                    Err(e) => {
                        log::error!(
                            "node {}: zone {zone_id} compaction step failed \
                             (retrying next tick): {e}",
                            shared.node_id
                        );
                        crate::observe::counters().compaction_errors.inc();
                        break;
                    }
                }
                steps += 1;
                if steps >= COMPACT_MAX_STEPS_PER_ZONE_PER_TICK {
                    break;
                }
                if shared.is_shutdown() {
                    return;
                }
                vals_rt::yield_now().await;
                if shared.is_shutdown() {
                    return;
                }
                if !shared.zone_is_current(zone_id, &store) {
                    continue 'zones;
                }
            }
            if zone_touched {
                crate::observe::counters().compactions.inc();
                if zone_freed > 0 {
                    log::info!(
                        "node {}: maintenance owner {} zone {zone_id} incrementally compacted: \
                         {zone_freed} segment(s) freed over {steps} step(s), \
                         reclaimable_before={reclaimable_before:.3}",
                        shared.node_id,
                        shared.shard().0
                    );
                }
            }
        }
    }
}

pub(super) async fn maint_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    handle: crate::maint::MaintHandle<D>,
) {
    let owner = handle.owner_id();
    let resubmit = handle.submitter();
    let stop = shared.add_shutdown_notify();
    let mut deferred = std::collections::VecDeque::new();
    loop {
        if deferred.is_empty() {
            if let Either::Right(()) = vals_rt::race(handle.wake().notified(), stop.notified()).await {
                return;
            }
        }
        if shared.is_shutdown() {
            return;
        }
        deferred.extend(handle.drain());
        while let Some(done) = deferred.pop_front() {
            match done {
                #[cfg(test)]
                crate::maint::MaintDone::Echoed(n) => {
                    log::trace!("node {}: maint echo {n} returned", shared.node_id);
                }
                crate::maint::MaintDone::Flushed(zone_id, result) => {
                    let Ok(store) = shared.zone_store(zone_id) else {
                        shared.finish_flush_build(zone_id);
                        shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Flush);
                        log::warn!(
                            "node {}: zone {zone_id} flush returned but the zone is gone; dropping",
                            shared.node_id
                        );
                        continue;
                    };
                    match result {
                        Ok(written) => {
                            let commit_t = std::time::Instant::now();
                            let started = store.borrow_mut().flush_commit_start(written);
                            crate::observe::histograms()
                                .flush_commit_us
                                .observe_us(commit_t.elapsed().as_micros() as u64);
                            match started {
                                Ok(mut commit) => {
                                    shared.integrity_watch.borrow_mut().cleared(zone_id);
                                    vals_rt::yield_now().await;
                                    loop {
                                        let step_t = std::time::Instant::now();
                                        let step = store.borrow_mut().flush_commit_step(
                                            &mut commit,
                                            vals_store::FLUSH_COMMIT_SLICE_ENTRIES,
                                        );
                                        crate::observe::histograms()
                                            .flush_commit_us
                                            .observe_us(step_t.elapsed().as_micros() as u64);
                                        match step {
                                            vals_store::FlushCommitStep::Pending => {
                                                vals_rt::yield_now().await;
                                                relay_cover_scan_chunks(&shared, &handle, &resubmit, &mut deferred);
                                                if shared.is_shutdown()
                                                    || !shared.zone_is_current(zone_id, &store)
                                                {
                                                    break;
                                                }
                                            }
                                            vals_store::FlushCommitStep::Ready(meta) => {
                                                crate::observe::counters().snapshots.inc();
                                                log::info!(
                                                    "node {}: maintenance owner {owner} zone {zone_id} flushed off-core ({} run entries)",
                                                    shared.node_id,
                                                    meta.entry_count,
                                                );
                                                break;
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    let policy = crate::integrity::FailFastPolicy::from_settings(&shared.settings);
                                    let trip = shared.integrity_watch.borrow_mut().flush_failure(
                                        policy, zone_id, Rc::as_ptr(&store) as usize, &e, std::time::Instant::now(),
                                    );
                                    if let Some(trip) = trip { integrity_fail_fast(&shared, policy, &trip); }
                                    crate::observe::counters().snapshot_errors.inc();
                                    log::warn!(
                                        "node {}: zone {zone_id} off-core flush commit failed (retrying next tick): {e}",
                                        shared.node_id
                                    );
                                }
                            }
                        }
                        Err((segs, e)) => {
                            store.borrow_mut().free_flush_segs(&segs);
                            crate::observe::counters().snapshot_errors.inc();
                            log::warn!(
                                "node {}: zone {zone_id} off-core flush write failed (retrying next tick): {e}",
                                shared.node_id
                            );
                            let policy = crate::integrity::FailFastPolicy::from_settings(&shared.settings);
                            let trip = shared.integrity_watch.borrow_mut().flush_failure(
                                policy, zone_id, Rc::as_ptr(&store) as usize, &e, std::time::Instant::now(),
                            );
                            if let Some(trip) = trip { integrity_fail_fast(&shared, policy, &trip); }
                        }
                    }
                    shared.finish_flush_build(zone_id);
                    shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Flush);
                }
                crate::maint::MaintDone::CleanupCompacted(zone_id, fingerprint, merge_dur, result) => {
                    let Ok(store) = shared.zone_store(zone_id) else {
                        shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Merge);
                        continue;
                    };
                    let valid = with_cleanup_placement(&shared, zone_id, |base, map| {
                        store.borrow().forget_plan_placement_valid(base, map)
                    })
                    .unwrap_or(false);
                    match result {
                        Ok(written) if valid => {
                            let kind = written.kind();
                            match store.borrow_mut().compact_commit(written) {
                                Err(e) => {
                                    log::warn!("node {}: cleanup compact commit failed: {e}", shared.node_id);
                                    crate::observe::counters().cleanup_errors.inc();
                                }
                                Ok(stats) => {
                                    crate::observe::record_merge_kind(kind, stats.input_bytes, stats.output_bytes);
                                    shared.bump_zone_forget_gen(zone_id);
                                    let keys = stats.current_dropped_tombstones.len();
                                    let merge_ms = merge_dur.as_millis() as u64;
                                    crate::observe::counters().run_merge_ms.add(merge_ms);
                                    crate::observe::counters().cleanup_forget_compactions.inc();
                                    crate::observe::counters().cleanup_forget_keys.add(keys as u64);
                                    log::info!(
                                        "node {}: zone {zone_id} cleanup forget compact committed ({keys} keys, {} in, {} out, {merge_ms} ms)",
                                        shared.node_id, stats.input_bytes, stats.output_bytes
                                    );
                                    shared.kick_sweep();
                                }
                            }
                        }
                        Ok(written) => {
                            log::debug!(
                                "node {}: zone {zone_id} cleanup forget plan from placement {fingerprint:?} \
                                 was overtaken by a placement change over its keys; aborted",
                                shared.node_id
                            );
                            let segs = written.output_segments();
                            store.borrow_mut().abort_forget_plan(&segs);
                            shared.kick_sweep();
                        }
                        Err((segs, e)) => {
                            store.borrow_mut().abort_forget_plan(&segs);
                            crate::integrity::corrupt_merge_record(
                                shared.read_site(zone_id),
                                store.borrow_mut().engine_mut(),
                                &e,
                            );
                            log::debug!("node {}: cleanup compact failed: {e}", shared.node_id);
                            crate::observe::counters().cleanup_errors.inc();
                            shared.kick_sweep();
                        }
                    }
                    shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Merge);
                }
                crate::maint::MaintDone::Compacted(zone_id, merge_dur, result) => {
                    let Ok(store) = shared.zone_store(zone_id) else {
                        shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Merge);
                        log::warn!(
                            "node {}: zone {zone_id} compaction returned but the zone is gone; dropping",
                            shared.node_id
                        );
                        continue;
                    };
                    match result {
                        Ok(written) => {
                            let kind = written.kind();
                            let commit_t = std::time::Instant::now();
                            let committed = store.borrow_mut().compact_commit(written);
                            crate::observe::histograms()
                                .compact_commit_us
                                .observe_us(commit_t.elapsed().as_micros() as u64);
                            match committed {
                                Ok(stats) => {
                                    let merge_ms = merge_dur.as_millis() as u64;
                                    crate::observe::counters().run_merges.inc();
                                    crate::observe::counters()
                                        .segments_freed
                                        .add(stats.segments_freed as u64);
                                    crate::observe::counters().run_merge_ms.add(merge_ms);
                                    crate::observe::counters()
                                        .run_merge_input_bytes
                                        .add(stats.input_bytes);
                                    crate::observe::counters()
                                        .run_merge_output_bytes
                                        .add(stats.output_bytes);
                                    crate::observe::record_merge_kind(kind, stats.input_bytes, stats.output_bytes);
                                    log::info!(
                                        "node {}: maintenance owner {owner} zone {zone_id} merged {} runs off-core ({}, {} entries, {} in, {} out, {} segs freed, {} ms)",
                                        shared.node_id,
                                        stats.runs_merged,
                                        kind.label(),
                                        stats.entries_written,
                                        stats.input_bytes,
                                        stats.output_bytes,
                                        stats.segments_freed,
                                        merge_ms,
                                    );
                                }
                                Err(e) => {
                                    crate::observe::counters().snapshot_errors.inc();
                                    log::warn!(
                                        "node {}: zone {zone_id} off-core compaction commit failed (retrying next tick): {e}",
                                        shared.node_id
                                    );
                                }
                            }
                        }
                        Err((segs, e)) => {
                            store.borrow_mut().free_compact_segs(&segs);
                            crate::integrity::corrupt_merge_record(
                                shared.read_site(zone_id),
                                store.borrow_mut().engine_mut(),
                                &e,
                            );
                            crate::observe::counters().snapshot_errors.inc();
                            log::warn!(
                                "node {}: zone {zone_id} off-core compaction write failed (retrying next tick): {e}",
                                shared.node_id
                            );
                        }
                    }
                    shared.finish_reserved_maint_job(zone_id, crate::ram_broker::Class::Merge);
                }
                crate::maint::MaintDone::CoverScanned(zone_id, plan, result) => {
                    finish_cover_scan_chunk(&shared, &resubmit, zone_id, plan, result);
                }
                #[cfg(test)]
                crate::maint::MaintDone::Stalled(_) => {}
            }
        }
    }
}

fn relay_cover_scan_chunks<D: BlockDevice + AsyncRead + 'static>(
    shared: &ServerShared<D>,
    handle: &crate::maint::MaintHandle<D>,
    resubmit: &crate::maint::MaintSubmit<D>,
    deferred: &mut std::collections::VecDeque<crate::maint::MaintDone<D>>,
) {
    for done in handle.drain() {
        match done {
            crate::maint::MaintDone::CoverScanned(zone_id, plan, result) => {
                finish_cover_scan_chunk(shared, resubmit, zone_id, plan, result);
            }
            other => deferred.push_back(other),
        }
    }
}

pub(crate) fn finish_cover_scan_chunk<D: BlockDevice + AsyncRead + 'static>(
    shared: &ServerShared<D>,
    resubmit: &crate::maint::MaintSubmit<D>,
    zone_id: ZoneId,
    plan: Box<vals_store::CoverScanPlan<D>>,
    result: Result<bool, EngineError>,
) {
    let counters = crate::observe::counters();
    counters.key_filter_cover_scan_chunks.inc();
    let Ok(store) = shared.zone_store(zone_id) else {
        counters.key_filter_cover_scans_discarded.inc();
        shared.finish_maint_job(zone_id);
        return;
    };
    let mut store = store.borrow_mut();
    let engine = store.engine_mut();
    if matches!(result, Ok(false)) && engine.cover_scan_current(&plan)
        && !shared.is_shutdown() && !shared.maintenance_paused_for_wipe()
    {
        match resubmit.submit(crate::maint::MaintJob::CoverScan(zone_id, plan)) {
            Ok(()) => return,
            Err(e) => {
                if let crate::maint::MaintJob::CoverScan(_, plan) = e.0 {
                    engine.cover_scan_abort(&plan);
                }
                counters.key_filter_cover_scans_discarded.inc();
            }
        }
        drop(store);
        shared.finish_maint_job(zone_id);
        return;
    }
    counters.key_filter_cover_scan_bytes.add(plan.bytes_read());
    counters.key_filter_cover_scan_us.add(plan.busy().as_micros() as u64);
    match result {
        Ok(true) => {
            let (keys, run_bytes) = (plan.keys(), plan.run_bytes());
            if engine.cover_scan_commit(*plan) {
                counters.key_filter_cover_scans_committed.inc();
                log::info!(
                    "node {}: zone {zone_id} key filter covered a {run_bytes}-byte run ({keys} keys)",
                    shared.node_id
                );
            } else {
                counters.key_filter_cover_scans_discarded.inc();
            }
        }
        Ok(false) => {
            engine.cover_scan_abort(&plan);
            counters.key_filter_cover_scans_discarded.inc();
        }
        Err(e) => {
            if engine.cover_scan_current(&plan) && !matches!(e, EngineError::Cancelled) {
                counters.key_filter_cover_scan_errors.inc();
                engine.cover_scan_fail(&plan);
                log::warn!(
                    "node {}: zone {zone_id} key filter cover scan of a {}-byte run failed (run backs off, others proceed): {e}",
                    shared.node_id,
                    plan.run_bytes()
                );
            } else {
                engine.cover_scan_abort(&plan);
                counters.key_filter_cover_scans_discarded.inc();
            }
        }
    }
    drop(store);
    shared.finish_maint_job(zone_id);
}

pub(super) async fn snapshot_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    every: Duration,
    maint: crate::maint::MaintSubmit<D>,
) {
    let mut every = every;
    let mut timer = match Timer::every(every) {
        Ok(t) => t,
        Err(e) => {
            log::warn!(
                "node {}: timerfd creation failed ({e}); periodic index snapshots DISABLED \
                 — boots will fall back to full-log replay",
                shared.node_id
            );
            return;
        }
    };
    let stop = shared.add_shutdown_notify();
    let mut last_sweep = shared.sweep_every();
    let mut last_plan_warn: std::collections::HashMap<ZoneId, std::time::Instant> =
        std::collections::HashMap::new();
    loop {
        if let Either::Right(()) = vals_rt::race(timer.tick(), stop.notified()).await {
            return;
        }
        if shared.is_shutdown() {
            return;
        }
        let cur = shared.sweep_every();
        if cur != last_sweep {
            last_sweep = cur;
            let want = shared.settings.flush_interval(cur);
            match Timer::every(want) {
                Ok(t) => {
                    timer = t;
                    every = want;
                    log::info!(
                        "node {}: flush/snapshot cadence now {every:?}",
                        shared.node_id
                    );
                }
                Err(e) => log::error!(
                    "node {}: re-arming snapshot timer at {want:?} failed ({e}); \
                     keeping {every:?}",
                    shared.node_id
                ),
            }
        }
        'zones: for zone_id in shared.sweep_zone_ids(false) {
            if let Ok(store) = shared.zone_store(zone_id) {
                store.borrow_mut().reap_seed_pins();
                store.borrow_mut().engine_mut().dealloc_step(shared.settings.device_dealloc_segments_per_tick as usize);
                let now = physical_now_ms();
                shared.persist_gc_watermark(now);
                if shared.maintenance_paused_for_wipe() {
                    shared.check_wipe_timeout(std::time::Instant::now());
                    continue;
                }
                let flush_budget = crate::ram::per_engine_flush_budget(
                    shared.settings.flush_max_bytes,
                    crate::ram::zone_engine_count(),
                );
                let due = zone_store_due(
                    &store.borrow(),
                    shared.settings.flush_tail_epochs,
                    flush_budget,
                    shared.settings.run_compact_threshold,
                    now,
                );
                if due.pressure && (due.flush || due.compact) {
                    let c = crate::observe::counters();
                    if due.flush {
                        c.pressure_flushes.inc();
                    }
                    if due.compact {
                        c.pressure_merges.inc();
                    }
                    log::debug!(
                        "node {}: zone {zone_id} under region pressure — scheduling \
                         {}{}{}",
                        shared.node_id,
                        if due.flush { "flush" } else { "" },
                        if due.flush && due.compact { " + " } else { "" },
                        if due.compact { "run-merge" } else { "" },
                    );
                }
                if due.compact || due.tombstone_gc {
                    let planned = plan_zone_merge(&mut store.borrow_mut(), &due, now);
                    match planned {
                        Ok(None) => {}
                        Ok(Some(plan)) => {
                            let reservation = reserve_merge(
                                plan.kind(),
                                Rc::as_ptr(&store) as usize,
                                plan.working_set_bytes(),
                            );
                            if let Some(reservation) = reservation {
                                match maint.submit(crate::maint::MaintJob::Compact(zone_id, plan)) {
                                    Ok(()) => shared.begin_reserved_maint_job(zone_id, reservation),
                                    Err(e) => {
                                        let segs = match e.0 {
                                            crate::maint::MaintJob::Compact(_, p) => p.segments(),
                                            _ => Vec::new(),
                                        };
                                        store.borrow_mut().free_compact_segs(&segs);
                                        store.borrow_mut().retry_tombstone_gc_soon();
                                        crate::observe::counters().snapshot_errors.inc();
                                        log::warn!(
                                            "node {}: zone {zone_id} compaction submit failed (worker gone): freeing reserved segs",
                                            shared.node_id
                                        );
                                    }
                                }
                            } else {
                                crate::ram_broker::note_deferred_merge_input(Rc::as_ptr(&store) as usize, plan.input_bytes());
                                store.borrow_mut().free_compact_segs(&plan.segments());
                                if plan.kind() == vals_store::MergeKind::LoneGc {
                                    store.borrow_mut().retry_tombstone_gc_soon();
                                }
                            }
                        }
                        Err(e) => {
                            let counters = crate::observe::counters();
                            if matches!(&e, EngineError::InsufficientHeadroom { .. }) {
                                counters.compaction_headroom_skips.inc();
                            } else {
                                counters.snapshot_errors.inc();
                            }
                            counters.compaction_plan_failures.inc();
                            let should_warn = last_plan_warn
                                .get(&zone_id)
                                .is_none_or(|last| last.elapsed() >= Duration::from_secs(60));
                            if should_warn {
                                last_plan_warn.insert(zone_id, std::time::Instant::now());
                                log::warn!(
                                    "node {}: zone {zone_id} run compaction plan failed (retrying): {e}",
                                    shared.node_id
                                );
                            }
                        }
                    }
                }
                if !due.pressure {
                    let plan = store.borrow_mut().engine_mut().cover_scan_plan();
                    if let Some(plan) = plan {
                        match maint.submit(crate::maint::MaintJob::CoverScan(zone_id, Box::new(plan))) {
                            Ok(()) => shared.begin_maint_job(zone_id),
                            Err(e) => {
                                if let crate::maint::MaintJob::CoverScan(_, plan) = e.0 {
                                    store.borrow_mut().engine_mut().cover_scan_abort(&plan);
                                }
                            }
                        }
                    }
                }
                if due.flush {
                    let flush_build_bytes = store.borrow().memtable_bytes();
                    let unpaced = due.pressure
                        || flush_build_bytes >= flush_budget.saturating_mul(2);
                    let reservation = if unpaced {
                        Some(crate::ram_broker::reserve_overdraw(
                            crate::ram_broker::Class::Flush, flush_build_bytes))
                    } else {
                        crate::ram_broker::try_reserve(
                            crate::ram_broker::Class::Flush, Rc::as_ptr(&store) as usize, flush_build_bytes)
                    };
                    let Some(reservation) = reservation else { continue 'zones; };
                    loop {
                        let plan_t = std::time::Instant::now();
                        let planned = store
                            .borrow_mut()
                            .flush_plan_step(vals_store::FLUSH_SLICE_ENTRIES);
                        let plan_us = plan_t.elapsed().as_micros() as u64;
                        match planned {
                            Ok(None) => {
                                shared.integrity_watch.borrow_mut().cleared(zone_id);
                                break;
                            }
                            Ok(Some(vals_store::FlushPlanStep::Pending)) => {
                                crate::observe::histograms().flush_plan_us.observe_us(plan_us);
                                vals_rt::yield_now().await;
                                if shared.is_shutdown() {
                                    store.borrow_mut().free_flush_segs(&[]);
                                    return;
                                }
                                if !shared.zone_is_current(zone_id, &store)
                                    || shared.maintenance_paused_for_wipe()
                                {
                                    store.borrow_mut().free_flush_segs(&[]);
                                    continue 'zones;
                                }
                            }
                            Ok(Some(vals_store::FlushPlanStep::Ready(plan))) => {
                                let mut plan = plan;
                                if due.pressure
                                    || flush_build_bytes >= flush_budget.saturating_mul(2)
                                {
                                    plan.set_unpaced();
                                }
                                let (sync_us, snapshot_us) = plan.phase_us();
                                let hists = crate::observe::histograms();
                                hists.flush_plan_us.observe_us(plan_us);
                                hists.flush_plan_sync_us.observe_us(sync_us);
                                hists.flush_plan_snapshot_us.observe_us(snapshot_us);
                                match maint.submit(crate::maint::MaintJob::Flush(zone_id, plan)) {
                                    Ok(()) => {
                                        shared.begin_reserved_maint_job(zone_id, reservation);
                                        shared.begin_flush_build(zone_id, flush_build_bytes);
                                    }
                                    Err(e) => {
                                        let segs = match e.0 {
                                            crate::maint::MaintJob::Flush(_, p) => {
                                                p.segments().to_vec()
                                            }
                                            _ => Vec::new(),
                                        };
                                        store.borrow_mut().free_flush_segs(&segs);
                                        shared.integrity_watch.borrow_mut().cleared(zone_id);
                                        crate::observe::counters().snapshot_errors.inc();
                                        log::warn!(
                                            "node {}: zone {zone_id} flush submit failed (worker gone): freeing reserved segs",
                                            shared.node_id
                                        );
                                    }
                                }
                                break;
                            }
                            Err(e) => {
                                crate::observe::counters().snapshot_errors.inc();
                                log::warn!(
                                    "node {}: zone {zone_id} memtable flush plan failed (retrying next tick): {e}",
                                    shared.node_id
                                );
                                let policy = crate::integrity::FailFastPolicy::from_settings(&shared.settings);
                                let trip = shared.integrity_watch.borrow_mut().flush_failure(
                                    policy, zone_id, Rc::as_ptr(&store) as usize, &e, std::time::Instant::now(),
                                );
                                if let Some(trip) = trip { integrity_fail_fast(&shared, policy, &trip); }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
}

fn integrity_fail_fast<D: BlockDevice>(
    shared: &ServerShared<D>,
    policy: crate::integrity::FailFastPolicy,
    trip: &crate::integrity::FailFastTrip,
) {
    crate::observe::counters().integrity_fail_fast_total.inc();
    log::error!(
        "node {}: zone {} persistent in-RAM memtable integrity failure: the same source engine failed its \
         checksum or its flush encode verification on {} consecutive flush attempts over {:?} \
         (fail-fast policy attempts={} window={:?}; single-bit signature: {}); \
         failing fast: shutting down without flushing this memtable and exiting with code {} so the supervisor \
         restarts the node and it replays the intact value log; evidence: {}",
        shared.node_id,
        trip.zone,
        trip.attempts,
        trip.elapsed,
        policy.attempts,
        policy.window,
        trip.single_bit_xor.map_or_else(
            || "none".to_string(),
            |x| format!("xor=0x{x:02x}{}", if trip.single_bit_partial { " (partial search)" } else { "" }),
        ),
        crate::integrity::INTEGRITY_FAIL_FAST_EXIT_CODE,
        trip.evidence,
    );
    log::logger().flush();
    crate::integrity::request_fail_fast();
}

enum Drain {
    Drained,
    TimedOut,
    Shutdown,
}

async fn drain_reads<D: BlockDevice + AsyncRead + 'static>(
    shared: &ServerShared<D>,
    stop: Option<&crate::reactor::Notify>,
    wait: Duration,
) -> Drain {
    if shared.inflight() == 0 {
        return Drain::Drained;
    }
    if shared.is_shutdown() {
        return Drain::Shutdown;
    }
    let Ok(deadline) = Timer::every(wait) else {
        return Drain::TimedOut;
    };
    loop {
        let woke = match stop {
            Some(stop) => {
                vals_rt::race(
                    shared.read_gate().drained(),
                    vals_rt::race(deadline.tick(), stop.notified()),
                )
                .await
            }
            None => match vals_rt::race(shared.read_gate().drained(), deadline.tick()).await {
                Either::Left(()) => Either::Left(()),
                Either::Right(()) => Either::Right(Either::Left(())),
            },
        };
        match woke {
            Either::Left(()) => {
                if shared.inflight() == 0 {
                    return Drain::Drained;
                }
            }
            Either::Right(Either::Left(())) => {
                if shared.is_shutdown() {
                    return Drain::Shutdown;
                }
                return if shared.inflight() == 0 {
                    Drain::Drained
                } else {
                    Drain::TimedOut
                };
            }
            Either::Right(Either::Right(())) => return Drain::Shutdown,
        }
    }
}

pub(super) struct QuiesceGate<'a, D: BlockDevice> {
    pub(super) shared: &'a ServerShared<D>,
    pub(super) consecutive_timeouts: &'a Cell<u32>,
    pub(super) next_attempt_ms: &'a Cell<u64>,
}

impl<'a, D: BlockDevice + AsyncRead + 'static> ForgetGate for QuiesceGate<'a, D> {
    type Hold = ReadPause<'a>;
    async fn acquire(&mut self) -> Option<ReadPause<'a>> {
        let now = physical_now_ms();
        if now < self.next_attempt_ms.get() {
            return None;
        }
        let pause = self.shared.read_gate().pause();
        let wait = cleanup_quiesce_wait(self.consecutive_timeouts.get());
        match drain_reads(self.shared, None, wait).await {
            Drain::Drained => {
                self.consecutive_timeouts.set(0);
                self.next_attempt_ms.set(0);
                Some(pause)
            }
            Drain::TimedOut => {
                self.consecutive_timeouts
                    .set(self.consecutive_timeouts.get().saturating_add(1));
                log::warn!(
                    "node {}: cleanup forget deferred — {} in-flight read(s) did not \
                     drain within {wait:?} (retrying next tick)",
                    self.shared.node_id,
                    self.shared.inflight()
                );
                crate::observe::counters().cleanup_quiesce_timeouts.inc();
                let backoff = wait.saturating_mul(CLEANUP_QUIESCE_BACKOFF_FACTOR);
                let backoff_ms = backoff.as_millis().try_into().unwrap_or(u64::MAX);
                self.next_attempt_ms
                    .set(physical_now_ms().saturating_add(backoff_ms));
                drop(pause);
                None
            }
            Drain::Shutdown => None,
        }
    }
}

