use super::*;
use crate::keyfilter::{KeyFilterSink, KeyFilterStats};
use std::time::{Duration, Instant};

pub const COVER_SCAN_RETRY_BASE: Duration = Duration::from_secs(60);
pub const COVER_SCAN_RETRY_MAX: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SinkKind {
    Flush,
    Merge,
    CoverScan,
    RebuildScan,
}

#[cfg(test)]
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct KeyFilterKnockouts {
    pub flush: bool,
    pub merge: bool,
    pub cover_scan: bool,
    pub inputs_covered: bool,
    pub rebuild: bool,
    pub rebuild_scan: bool,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CoverScanBackoff {
    failures: u32,
    retry_at: Instant,
}

pub struct CoverScanPlan<D: BlockDevice> {
    token: u64,
    pub(super) run_id: u64,
    source: run::RunKeySource,
    sink: KeyFilterSink,
    dev: Arc<D>,
    run_bytes: u64,
    pub(super) keys: u64,
    busy: Duration,
    done: bool,
    rebuild_scan: bool,
}

impl<D: BlockDevice> CoverScanPlan<D> {
    pub fn serving_dev(&self) -> &D {
        &self.dev
    }

    pub fn run_bytes(&self) -> u64 {
        self.run_bytes
    }

    pub fn bytes_read(&self) -> u64 {
        self.source.bytes_read()
    }

    pub fn keys(&self) -> u64 {
        self.keys
    }

    pub fn busy(&self) -> Duration {
        self.busy
    }

    pub fn is_done(&self) -> bool {
        self.done
    }

    pub fn is_rebuild_scan(&self) -> bool {
        self.rebuild_scan
    }

    pub fn step(&mut self, dev: &D, max_bytes: usize) -> Result<bool, EngineError> {
        if self.done {
            return Ok(true);
        }
        let started = Instant::now();
        let sink = &mut self.sink;
        let mut keys = 0u64;
        let stepped = self.source.step(dev, max_bytes, |key| {
            sink.insert(key);
            keys += 1;
        });
        self.keys += keys;
        let elapsed = started.elapsed();
        self.busy += elapsed;
        if self.rebuild_scan {
            crate::pace::maint_pace_charge(elapsed);
        } else {
            crate::pace::maint_unpaced_charge(elapsed);
        }
        self.done = stepped?;
        Ok(self.done)
    }
}

impl<D: BlockDevice> Engine<D> {
    fn engine_key_count(&self) -> u64 {
        self.index.len() as u64 + self.runs.iter().map(|or| or.meta.entry_count).sum::<u64>()
    }

    pub(super) fn key_filter_active(&self) -> bool {
        self.key_filter.enabled() && self.winner_directory.is_none()
    }

    pub(super) fn key_filter_sink(&mut self, kind: SinkKind, expected_keys: u64) -> Option<KeyFilterSink> {
        self.key_filter_sink_lanes(kind, expected_keys, expected_keys)
    }

    fn key_filter_sink_lanes(&mut self, kind: SinkKind, active_keys: u64, rebuild_keys: u64) -> Option<KeyFilterSink> {
        if !self.key_filter_active() {
            return None;
        }
        let (active_keys, rebuild_keys) = match kind {
            SinkKind::CoverScan => (active_keys.max(1), rebuild_keys.max(1)),
            SinkKind::RebuildScan => (0, rebuild_keys.max(1)),
            _ => (active_keys, rebuild_keys),
        };
        let engine_keys = self.engine_key_count();
        #[allow(unused_mut)]
        let mut sink = self.key_filter.sink_lanes(active_keys, rebuild_keys, engine_keys);
        #[cfg(test)]
        {
            let k = self.test_kf_knockouts;
            let persistent = match kind {
                SinkKind::Flush => k.flush,
                SinkKind::Merge => k.merge,
                SinkKind::CoverScan => k.cover_scan,
                SinkKind::RebuildScan => k.rebuild_scan,
            };
            if persistent || std::mem::take(&mut self.test_key_filter_knockout) {
                if let Some(s) = sink.as_mut() {
                    s.knock_out_generation(s.generation());
                }
            }
        }
        #[cfg(test)]
        if self.test_kf_knockouts.rebuild {
            if let (Some(s), Some(r)) = (sink.as_mut(), self.key_filter.rebuild_filter()) {
                s.knock_out_generation(r.generation());
            }
        }
        let _ = kind;
        sink
    }

    pub(super) fn key_filter_inputs_covered(&self, inputs: &[RunMeta]) -> bool {
        #[cfg(test)]
        if self.test_kf_knockouts.inputs_covered {
            return true;
        }
        !inputs.is_empty()
            && inputs
                .iter()
                .all(|m| self.runs.iter().any(|or| or.meta.handle == m.handle && or.covered))
    }

    pub(super) fn key_filter_rebuild_inputs_covered(&self, inputs: &[RunMeta]) -> bool {
        self.key_filter.rebuilding() && !inputs.is_empty()
            && inputs.iter().all(|m| self.runs.iter().any(|or| or.meta.handle == m.handle && or.rebuild_covered))
    }

    pub(super) fn key_filter_merge_sink(&mut self, inputs: &[RunMeta]) -> Option<KeyFilterSink> {
        let expected = inputs.iter().map(|m| m.entry_count).sum::<u64>().max(1);
        let active = if self.key_filter_inputs_covered(inputs) { 0 } else { expected };
        let rebuild = if self.key_filter.rebuilding() && !self.key_filter_rebuild_inputs_covered(inputs) {
            expected
        } else { 0 };
        self.key_filter_sink_lanes(SinkKind::Merge, active, rebuild)
    }

    fn key_filter_drop_generation(&mut self) {
        self.key_filter.reset();
        for or in &mut self.runs {
            or.covered = false;
            or.rebuild_covered = false;
        }
    }

    pub(super) fn key_filter_maintain(&mut self) {
        self.key_filter.prune_retired();
        if !self.key_filter_active() {
            self.key_filter.cancel_rebuild_wait();
            if self.key_filter.rebuilding() || !self.key_filter.sub_filters().is_empty() || self.runs.iter().any(|or| or.covered) {
                self.key_filter_drop_generation();
            }
        } else {
            let live: u64 = self.runs.iter().map(|or| or.meta.entry_count).sum();
            if self.key_filter.rebuilding() && self.runs.iter().all(|or| or.rebuild_covered) {
                self.key_filter.complete_rebuild();
                for or in &mut self.runs {
                    or.covered = true;
                    or.rebuild_covered = false;
                }
            }
            let params = self.key_filter.params();
            let stale = self.key_filter.stale(live);
            let eligible = params.rebuild && !self.key_filter.rebuilding() && self.cover_scan_token.is_none()
                && self.runs.iter().all(|or| or.covered)
                && (stale || self.key_filter.sub_filters().len() as u64 >= params.rebuild_subs);
            if eligible {
                let estimate = self.key_filter.inserted_keys().min(live).saturating_add(self.index.len() as u64);
                if self.key_filter.start_rebuild(estimate) {
                    for or in &mut self.runs {
                        or.rebuild_covered = false;
                    }
                }
            } else {
                self.key_filter.cancel_rebuild_wait();
            }
            if !params.rebuild && stale {
                self.key_filter_drop_generation();
                self.key_filter.resets += 1;
            }
            self.key_filter.note_rebuild_peak();
        }
        if !self.cover_scan_backoff.is_empty() {
            let runs = &self.runs;
            self.cover_scan_backoff
                .retain(|id, _| runs.iter().any(|or| or.id == *id));
        }
    }

    #[inline]
    pub(super) fn key_filter_excludes(&self, hk: &run::BloomKey) -> bool {
        !self.key_filter.sub_filters().is_empty() && self.key_filter.excludes(hk)
    }

    pub fn key_filter_stats(&self) -> KeyFilterStats {
        let mut stats = self.key_filter.stats();
        for or in &self.runs {
            stats.rebuild_covered_runs += or.rebuild_covered as u64;
            if or.covered {
                stats.covered_runs += 1;
            } else {
                stats.uncovered_runs += 1;
                stats.uncovered_run_bytes += or.meta.handle.len;
            }
        }
        let now = Instant::now();
        stats.cover_scan_backoff_runs =
            self.cover_scan_backoff.values().filter(|b| b.retry_at > now).count() as u64;
        stats
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn reset_key_filter_for_test(&mut self) {
        self.key_filter_drop_generation();
    }

    pub fn cover_scan_in_flight(&self) -> bool {
        self.cover_scan_token.is_some()
    }

    fn cover_scan_merge_soon_window(&self) -> std::ops::Range<usize> {
        let sizes: Vec<u64> = self.runs.iter().map(|or| or.meta.handle.len).collect();
        select_recurring_merge(&sizes, &self.merge_policy)
            .map_or(0..0, |w| w.start..w.end)
    }

    pub fn cover_scan_plan(&mut self) -> Option<CoverScanPlan<D>> {
        self.cover_scan_plan_at(Instant::now())
    }

    pub fn cover_scan_plan_at(&mut self, now: Instant) -> Option<CoverScanPlan<D>> {
        static NEXT_TOKEN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        if self.poisoned || self.cover_scan_token.is_some() || !self.key_filter_active() {
            return None;
        }
        if self.key_filter.params().rebuild {
            self.key_filter_maintain();
        }
        if !self.runs.iter().any(|or| !or.covered || (self.key_filter.rebuilding() && !or.rebuild_covered)) {
            return None;
        }
        let merge_soon = self.cover_scan_merge_soon_window();
        let backoff = &self.cover_scan_backoff;
        let idx = self
            .runs
            .iter()
            .enumerate()
            .filter(|(_, or)| !or.covered && backoff.get(&or.id).is_none_or(|b| b.retry_at <= now))
            .max_by_key(|(i, or)| (!merge_soon.contains(i), or.meta.handle.len))
            .map(|(i, _)| i);
        let (idx, rebuild_scan) = match idx {
            Some(i) => (i, false),
            None if self.key_filter.rebuilding() => {
                let i = self.runs.iter().enumerate()
                    .filter(|(_, or)| !or.rebuild_covered && backoff.get(&or.id).is_none_or(|b| b.retry_at <= now))
                    .max_by_key(|(i, or)| (!merge_soon.contains(i), or.meta.handle.len))?.0;
                (i, true)
            }
            None => return None,
        };
        let expected = self.runs[idx].meta.entry_count;
        let sink = if rebuild_scan {
            self.key_filter_sink_lanes(SinkKind::RebuildScan, 0, expected)?
        } else {
            self.key_filter_sink(SinkKind::CoverScan, expected)?
        };
        let or = &self.runs[idx];
        let token = NEXT_TOKEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let plan = CoverScanPlan {
            token,
            run_id: or.id,
            source: or.run.key_source(&self.alloc),
            sink,
            dev: self.dev.clone(),
            run_bytes: or.meta.handle.len,
            keys: 0,
            busy: Duration::ZERO,
            done: false,
            rebuild_scan,
        };
        self.cover_scan_token = Some(token);
        Some(plan)
    }

    pub fn cover_scan_current(&self, plan: &CoverScanPlan<D>) -> bool {
        self.cover_scan_token == Some(plan.token)
            && if plan.rebuild_scan {
                self.key_filter.owns_rebuild(&plan.sink)
                    && self.runs.iter().any(|or| or.id == plan.run_id && !or.rebuild_covered)
            } else {
                self.key_filter.owns(&plan.sink)
                    && self.runs.iter().any(|or| or.id == plan.run_id && !or.covered)
            }
    }

    fn note_cover_scan_failure(&mut self, run_id: u64) {
        if !self.runs.iter().any(|or| or.id == run_id) {
            return;
        }
        self.key_filter.cover_scan_failures += 1;
        let entry = self.cover_scan_backoff.entry(run_id).or_insert(CoverScanBackoff {
            failures: 0,
            retry_at: Instant::now(),
        });
        entry.failures = entry.failures.saturating_add(1);
        let delay = COVER_SCAN_RETRY_BASE
            .saturating_mul(1u32 << (entry.failures - 1).min(16))
            .min(COVER_SCAN_RETRY_MAX);
        entry.retry_at = Instant::now() + delay;
    }

    pub fn cover_scan_commit(&mut self, plan: CoverScanPlan<D>) -> bool {
        let current = plan.done && self.cover_scan_current(&plan);
        if self.cover_scan_token == Some(plan.token) {
            self.cover_scan_token = None;
        }
        let (run_id, keys) = (plan.run_id, plan.keys);
        let active = self.key_filter.owns(&plan.sink);
        let rebuild = self.key_filter.owns_rebuild(&plan.sink);
        drop(plan);
        if !current {
            self.key_filter.cover_scans_discarded += 1;
            return false;
        }
        let entries = self.runs.iter().find(|or| or.id == run_id).map_or(0, |or| or.meta.entry_count);
        if keys != entries {
            self.key_filter.cover_scans_discarded += 1;
            self.key_filter.cover_scan_mismatches += 1;
            self.note_cover_scan_failure(run_id);
            log::warn!(
                "key filter cover scan streamed {keys} keys from a run holding {entries} entries; discarded, run stays uncovered"
            );
            return false;
        }
        for or in &mut self.runs {
            if or.id == run_id {
                or.covered |= active;
                or.rebuild_covered |= rebuild;
            }
        }
        self.cover_scan_backoff.remove(&run_id);
        self.key_filter.cover_scans_committed += 1;
        self.key_filter_maintain();
        true
    }

    pub fn cover_scan_abort(&mut self, plan: &CoverScanPlan<D>) {
        if self.cover_scan_token == Some(plan.token) {
            self.cover_scan_token = None;
            self.key_filter.cover_scans_discarded += 1;
        }
    }

    pub fn cover_scan_fail(&mut self, plan: &CoverScanPlan<D>) -> bool {
        if self.cover_scan_token != Some(plan.token) {
            return false;
        }
        self.cover_scan_token = None;
        if !self.runs.iter().any(|or| or.id == plan.run_id) {
            self.key_filter.cover_scans_discarded += 1;
            return false;
        }
        self.note_cover_scan_failure(plan.run_id);
        true
    }
}
