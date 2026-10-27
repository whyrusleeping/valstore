use super::*;
use vals_store::range_tombstone::{Fragment, FragmentSet};

pub(super) const FORGET_BATCH_CAP_BYTES: usize = 32 * 1024 * 1024;
const FORGET_ENTRY_OVERHEAD: usize = 64;

type CleanupSpan = (Vec<u8>, Option<Vec<u8>>);

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct CleanupPlacement {
    pub(crate) base: (u64, u64, u64),
    pub(crate) map: Option<std::sync::Arc<RangeMap>>,
}

impl CleanupPlacement {
    pub(crate) fn changes_to(
        &self,
        base: (u64, u64, u64),
        map: Option<&RangeMap>,
    ) -> Option<Vec<CleanupSpan>> {
        if self.base != base {
            return None;
        }
        match (self.map.as_deref(), map) {
            (None, None) => Some(Vec::new()),
            (Some(old), Some(new)) if std::ptr::eq(old, new) || old == new => Some(Vec::new()),
            (Some(old), Some(new)) => Some(range_placement_changes(old, new)),
            _ => None,
        }
    }

    pub(crate) fn unchanged_at(&self, base: (u64, u64, u64), map: Option<&RangeMap>) -> bool {
        self.changes_to(base, map).is_some_and(|spans| spans.is_empty())
    }
}

fn range_placement_changes(old: &RangeMap, new: &RangeMap) -> Vec<CleanupSpan> {
    let mut changed: Vec<CleanupSpan> = Vec::new();
    let (mut i, mut j) = (0, 0);
    let mut lo: Vec<u8> = Vec::new();
    loop {
        let old_hi = old.range_bounds(i).1;
        let new_hi = new.range_bounds(j).1;
        let hi = match (old_hi, new_hi) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if old.token_of(i) != new.token_of(j) || old.pending_of(i) != new.pending_of(j) {
            match changed.last_mut() {
                Some((_, last_hi)) if last_hi.as_deref() == Some(lo.as_slice()) => {
                    *last_hi = hi.map(<[u8]>::to_vec);
                }
                _ => changed.push((lo.clone(), hi.map(<[u8]>::to_vec))),
            }
        }
        let Some(hi) = hi else {
            break;
        };
        if old_hi == Some(hi) {
            i += 1;
        }
        if new_hi == Some(hi) {
            j += 1;
        }
        lo = hi.to_vec();
    }
    changed
}

fn spans_hold(spans: &[CleanupSpan], key: &[u8]) -> bool {
    let at = spans.partition_point(|(lo, _)| lo.as_slice() <= key);
    at > 0 && spans[at - 1].1.as_deref().is_none_or(|hi| key < hi)
}

fn spans_meet(spans: &[CleanupSpan], lo: &[u8], hi: Option<&[u8]>) -> bool {
    let at = spans.partition_point(|(_, span_hi)| span_hi.as_deref().is_some_and(|span_hi| span_hi <= lo));
    spans
        .get(at)
        .is_some_and(|(span_lo, _)| hi.is_none_or(|hi| span_lo.as_slice() < hi))
}

impl<D: BlockDevice> VersionedStore<D> {
    pub fn flush(&mut self) -> Result<Option<vals_store::RunMeta>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        self.engine.flush(opts)
    }

    pub fn flush_plan(&mut self) -> Result<Option<vals_store::FlushPlan<D>>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        self.engine.flush_plan(opts)
    }

    pub fn flush_plan_step(
        &mut self,
        max_entries: usize,
    ) -> Result<Option<vals_store::FlushPlanStep<D>>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        self.engine.flush_plan_step(opts, max_entries)
    }

    pub fn device_sibling(&self) -> vals_spdk::Result<D> {
        self.engine.device().sibling()
    }

    pub fn engine_config(&self) -> vals_store::Config {
        self.engine.config()
    }

    pub fn flush_commit(
        &mut self,
        written: vals_store::WrittenFlush<D>,
    ) -> Result<Option<vals_store::RunMeta>, EngineError> {
        self.engine.flush_commit(written).map(Some)
    }

    pub fn flush_commit_start(
        &mut self,
        written: vals_store::WrittenFlush<D>,
    ) -> Result<vals_store::FlushCommit, EngineError> {
        self.engine.flush_commit_start(written)
    }

    pub fn flush_commit_step(
        &mut self,
        commit: &mut vals_store::FlushCommit,
        max_entries: usize,
    ) -> vals_store::FlushCommitStep {
        self.engine.flush_commit_step(commit, max_entries)
    }

    pub fn free_flush_segs(&mut self, segs: &[vals_store::alloc::SegmentId]) {
        self.engine.free_flush_segs(segs)
    }

    pub fn run_count(&self) -> usize {
        self.engine.run_count()
    }

    pub fn recurring_merge_selectable(&self) -> bool {
        self.engine.recurring_merge_selectable()
    }

    pub fn flush_in_flight(&self) -> bool {
        self.engine.flush_in_flight()
    }

    pub fn compact_runs(
        &mut self,
        wall_ms: u64,
    ) -> Result<vals_store::CompactRunsStats, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        let gc = self.gc_collect_before(wall_ms);
        let stats = self.engine.compact_runs(opts, gc)?;
        self.sync_pinned_bytes();
        self.note_collected_range_tombstones(&stats.collected_range_tombstones);
        for (k, v) in &stats.current_dropped_tombstones {
            let group = self.group_of(k);
            self.retire_winner(k, &group, unpack_version(*v));
        }
        Ok(stats)
    }

    pub fn compact_plan(
        &mut self,
        wall_ms: u64,
    ) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        let gc = self.gc_collect_before(wall_ms);
        self.engine
            .compact_plan(opts, gc)
    }

    pub fn tombstone_gc_due(&self, wall_ms: u64) -> bool {
        self.tombstone_gc_trigger(wall_ms)
            .is_some_and(|trigger| self.engine.runs_tombstone_min_version().is_some_and(|min| min < trigger))
    }

    pub fn tombstone_gc_flush_due(&self, wall_ms: u64) -> bool {
        self.tombstone_gc_trigger(wall_ms)
            .is_some_and(|trigger| self.engine.mutable_tombstone_min_version().is_some_and(|min| min < trigger))
    }

    pub fn compact_plan_tombstone_gc(
        &mut self,
        wall_ms: u64,
    ) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        let gc = self.gc_collect_before(wall_ms);
        self.last_tombstone_gc_ms = Some(wall_ms);
        self.engine.compact_plan_lone_gc(opts, gc)
    }

    pub fn retry_tombstone_gc_soon(&mut self) {
        self.last_tombstone_gc_ms = None;
    }

    pub fn compact_plan_pressure(
        &mut self,
        wall_ms: u64,
    ) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        let gc = self.gc_collect_before(wall_ms);
        self.engine
            .compact_plan_pressure(opts, gc)
    }

    fn clear_accumulating_forget(&mut self) {
        self.accumulating_forget.clear();
        self.accumulating_released = FragmentSet::with_max_fragments(usize::MAX);
        self.accumulating_forget_placement = None;
        self.accumulating_forget_bytes = 0;
        self.accumulating_forget_wrapped = false;
    }

    fn discard_inflight_forget(&mut self) {
        if !self.pending_forget.is_empty() || !self.pending_released.is_empty() {
            crate::observe::counters().cleanup_forget_discards.inc();
            self.pending_forget.clear();
            self.pending_released = FragmentSet::default();
        }
        self.pending_forget_placement = None;
    }

    pub fn reset_pending_forget(&mut self) {
        if !self.accumulating_forget.is_empty() || !self.accumulating_released.is_empty() {
            crate::observe::counters().cleanup_forget_discards.inc();
        }
        self.clear_accumulating_forget();
        self.discard_inflight_forget();
    }

    pub(crate) fn set_cleanup_placement(&mut self, placement: CleanupPlacement) {
        self.cleanup_placement = placement;
        self.validate_forget_placement();
    }

    pub(crate) fn forget_plan_placement_valid(
        &self,
        base: (u64, u64, u64),
        map: Option<&RangeMap>,
    ) -> bool {
        let Some(placement) = &self.pending_forget_placement else {
            return false;
        };
        let Some(spans) = placement.changes_to(base, map) else {
            return false;
        };
        spans.is_empty()
            || (!self.pending_forget.keys().any(|key| spans_hold(&spans, key))
                && !self
                    .pending_released
                    .fragments()
                    .any(|piece| spans_meet(&spans, &piece.lo, piece.hi.as_deref())))
    }

    pub(crate) fn cleanup_forget_pending(&self) -> bool {
        !self.accumulating_forget.is_empty()
            || !self.accumulating_released.is_empty()
            || !self.pending_forget.is_empty()
            || !self.pending_released.is_empty()
    }

    fn accumulating_forget_is_empty(&self) -> bool {
        self.accumulating_forget.is_empty() && self.accumulating_released.is_empty()
    }

    pub(crate) fn accumulate_released(&mut self, released: &[Fragment]) {
        self.validate_forget_placement();
        for piece in released {
            if self.accumulating_forget_is_empty() {
                self.accumulating_forget_placement = Some(self.cleanup_placement.clone());
            }
            self.accumulating_released
                .overlay(Some(&piece.lo), piece.hi.as_deref(), piece.version)
                .expect("an uncapped overlay never refuses");
            self.accumulating_forget_bytes += piece.lo.len() + piece.hi.as_ref().map_or(0, Vec::len) + FORGET_ENTRY_OVERHEAD;
        }
    }

    pub(crate) fn accumulate_forget(&mut self, keys: &[(Vec<u8>, Version)], wrapped: bool) {
        self.validate_forget_placement();
        for (key, version) in keys {
            if self.pending_forget.get(key) == Some(version) {
                continue;
            }
            if self.accumulating_forget_is_empty() {
                self.accumulating_forget_placement = Some(self.cleanup_placement.clone());
            }
            match self.accumulating_forget.entry(key.clone()) {
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(*version);
                    self.accumulating_forget_bytes += key.len() + FORGET_ENTRY_OVERHEAD;
                }
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    if *version > *entry.get() {
                        entry.insert(*version);
                    }
                }
            }
        }
        if !self.accumulating_forget_is_empty() {
            self.accumulating_forget_wrapped |= wrapped;
        }
    }

    fn validate_forget_placement(&mut self) {
        let changes = match &self.accumulating_forget_placement {
            None => return,
            Some(placement) if *placement == self.cleanup_placement => return,
            Some(placement) => {
                placement.changes_to(self.cleanup_placement.base, self.cleanup_placement.map.as_deref())
            }
        };
        match changes {
            None => {
                crate::observe::counters().cleanup_forget_discards.inc();
                self.clear_accumulating_forget();
            }
            Some(spans) => {
                self.drop_accumulating_forget_in(&spans);
                if !self.accumulating_forget_is_empty() {
                    self.accumulating_forget_placement = Some(self.cleanup_placement.clone());
                }
            }
        }
    }

    fn drop_accumulating_forget_in(&mut self, spans: &[CleanupSpan]) {
        if spans.is_empty() {
            return;
        }
        let mut dropped = 0usize;
        let mut dropped_bytes = 0usize;
        self.accumulating_forget.retain(|key, _| {
            let hit = spans_hold(spans, key);
            if hit {
                dropped += 1;
                dropped_bytes += key.len() + FORGET_ENTRY_OVERHEAD;
            }
            !hit
        });
        if self
            .accumulating_released
            .fragments()
            .any(|piece| spans_meet(spans, &piece.lo, piece.hi.as_deref()))
        {
            let mut kept = FragmentSet::with_max_fragments(usize::MAX);
            for piece in self.accumulating_released.fragments() {
                if spans_meet(spans, &piece.lo, piece.hi.as_deref()) {
                    dropped += 1;
                    dropped_bytes += piece.lo.len() + piece.hi.as_ref().map_or(0, Vec::len) + FORGET_ENTRY_OVERHEAD;
                } else {
                    kept.overlay(Some(&piece.lo), piece.hi.as_deref(), piece.version)
                        .expect("an uncapped overlay never refuses");
                }
            }
            self.accumulating_released = kept;
        }
        if dropped > 0 {
            crate::observe::counters().cleanup_forget_discards.inc();
            self.accumulating_forget_bytes = self.accumulating_forget_bytes.saturating_sub(dropped_bytes);
            if self.accumulating_forget_is_empty() {
                self.clear_accumulating_forget();
            }
        }
    }

    pub(crate) fn cleanup_forget_due(&self) -> bool {
        if self.accumulating_forget_is_empty() || !self.pending_forget.is_empty() || !self.pending_released.is_empty() {
            return false;
        }
        if self.run_count() == 0 {
            return !self.flush_in_flight() && !self.compact_in_flight();
        }
        #[cfg(test)]
        let cap = self.forget_batch_cap;
        #[cfg(not(test))]
        let cap = FORGET_BATCH_CAP_BYTES;
        self.engine.can_plan_forget()
            && (self.accumulating_forget_wrapped || self.accumulating_forget_bytes >= cap)
    }

    pub(crate) fn cleanup_forget_batch(
        &mut self,
    ) -> Result<(usize, Option<vals_store::CompactPlan<D>>), EngineError> {
        self.validate_forget_placement();
        if !self.cleanup_forget_due() {
            return Ok((0, None));
        }
        if self.run_count() == 0 {
            let mut keys = std::collections::HashSet::new();
            for (key, version) in &self.accumulating_forget {
                if self.get_versioned(key)?.is_some_and(|cur| cur.version == *version) {
                    keys.insert(key.clone());
                }
            }
            let released = self.forget(&keys)?;
            let spans = self.engine.release_range_tombstones(&self.accumulating_released)?;
            if spans > 0 {
                self.refresh_accounted_from_records();
            }
            if released == keys.len() {
                self.clear_accumulating_forget();
            }
            return Ok((released + spans, None));
        }
        let keys = std::mem::take(&mut self.accumulating_forget).into_iter().collect::<Vec<_>>();
        let released = std::mem::take(&mut self.accumulating_released);
        self.clear_accumulating_forget();
        let plan = self.forget_plan_releasing(&keys, released)?;
        if plan.is_none() {
            crate::observe::counters().cleanup_forget_discards.inc();
        } else {
            self.pending_forget_placement = Some(self.cleanup_placement.clone());
        }
        Ok((0, plan))
    }

    #[cfg(test)]
    pub(super) fn pending_forget_len(&self) -> usize {
        self.pending_forget.len()
    }

    #[cfg(test)]
    pub(crate) fn set_forget_batch_cap(&mut self, bytes: usize) {
        self.forget_batch_cap = bytes;
    }

    pub fn forget_plan(
        &mut self,
        keys: &[(Vec<u8>, Version)],
    ) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
        self.forget_plan_releasing(keys, FragmentSet::default())
    }

    fn forget_plan_releasing(
        &mut self,
        keys: &[(Vec<u8>, Version)],
        released: FragmentSet,
    ) -> Result<Option<vals_store::CompactPlan<D>>, EngineError> {
        if !self.engine.can_plan_forget() || !self.pending_forget.is_empty() || !self.pending_released.is_empty() {
            return Ok(None);
        }
        self.pending_forget.extend(keys.iter().map(|(k, v)| (k.clone(), *v)));
        let exact = self
            .pending_forget
            .iter()
            .map(|(k, v)| (k.clone(), pack_version(*v)))
            .collect();
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        match self.engine.forget_plan_releasing(opts, &exact, &released) {
            Ok(Some(plan)) => {
                self.pending_released = released;
                Ok(Some(plan))
            }
            Ok(None) => {
                self.pending_forget.clear();
                Ok(None)
            }
            Err(e) => {
                self.discard_inflight_forget();
                Err(e)
            }
        }
    }

    pub fn compact_commit(
        &mut self,
        written: vals_store::WrittenCompact<D>,
    ) -> Result<vals_store::CompactRunsStats, EngineError> {
        let committed = self.engine.compact_commit(written);
        self.sync_pinned_bytes();
        match committed {
            Ok(stats) => {
                self.note_collected_range_tombstones(&stats.collected_range_tombstones);
                for (k, v) in &stats.current_dropped_tombstones {
                    let version = unpack_version(*v);
                    let group = self.group_of(k);
                    self.retire_winner(k, &group, version);
                }
                if stats.released_range_tombstones > 0 {
                    self.refresh_accounted_from_records();
                }
                self.pending_forget.clear();
                self.pending_released = FragmentSet::default();
                self.pending_forget_placement = None;
                Ok(stats)
            }
            Err(e) => {
                self.discard_inflight_forget();
                Err(e)
            }
        }
    }

    pub fn free_compact_segs(&mut self, segs: &[vals_store::alloc::SegmentId]) {
        self.engine.free_compact_segs(segs)
    }

    pub fn abort_forget_plan(&mut self, segs: &[vals_store::alloc::SegmentId]) {
        self.engine.free_compact_segs(segs);
        self.discard_inflight_forget();
    }

    pub fn compact_in_flight(&self) -> bool {
        self.engine.compact_in_flight()
    }

    pub fn compact(&mut self, _wall_ms: u64) -> Result<vals_store::CompactStats, EngineError> {
        let cutoff = self.durable_gc_cutoff_ms();
        let dropped = std::cell::RefCell::new(Vec::new());
        let stats = self.engine.compact_filtered(|key, record_version, raw| {
            let Some(cutoff) = cutoff else {
                return false;
            };
            let version = unpack_version(record_version);
            let (kind, _) = decode(raw);
            let drop = kind == KIND_TOMB && version.physical_ms() < cutoff;
            if drop {
                dropped.borrow_mut().push((key.to_vec(), version));
            }
            drop
        });
        let stats = self.retire_gc_dropped(stats)?;
        for (k, version) in dropped.into_inner() {
            let group = self.group_of(&k);
            self.retire_winner(&k, &group, version);
        }
        Ok(stats)
    }

    fn retire_gc_dropped<T>(&self, result: Result<T, EngineError>) -> Result<T, EngineError> {
        match result {
            Err(EngineError::PartialForget { dropped, source }) => {
                for (k, version) in dropped {
                    let group = self.group_of(&k);
                    self.retire_winner(&k, &group, unpack_version(version));
                }
                Err(*source)
            }
            other => other,
        }
    }

    pub fn compact_step(
        &mut self,
        wall_ms: u64,
        max_segments: usize,
    ) -> Result<(vals_store::CompactStats, bool), EngineError> {
        let grace = self.gc_grace_ms;
        let eff_now = wall_ms.min(self.high_water_ms());
        let dropped = std::cell::RefCell::new(Vec::new());
        let stepped =
            self.engine
                .compact_step_filtered(max_segments, |key, record_version, raw| {
                    if grace == 0 {
                        return false;
                    }
                    let version = unpack_version(record_version);
                    let (kind, _) = decode(raw);
                    let drop =
                        kind == KIND_TOMB && eff_now.saturating_sub(version.physical_ms()) > grace;
                    if drop {
                        dropped.borrow_mut().push((key.to_vec(), version));
                    }
                    drop
                });
        let (stats, done) = self.retire_gc_dropped(stepped)?;
        for (k, version) in dropped.into_inner() {
            let group = self.group_of(&k);
            self.retire_winner(&k, &group, version);
        }
        Ok((stats, done))
    }

    pub fn forget(
        &mut self,
        keys: &std::collections::HashSet<Vec<u8>>,
    ) -> Result<usize, EngineError> {
        if keys.is_empty() {
            return Ok(0);
        }
        let opts = vals_store::RunBuildOpts {
            num_buckets: INDEX_BUCKETS,
            ..Default::default()
        };
        let forgot = self.engine.forget_keys(keys, opts);
        self.sync_pinned_bytes();
        let stats = match forgot {
            Ok(Some(stats)) => stats,
            Ok(None) => return Ok(0),
            Err(EngineError::PartialForget { dropped, source }) => {
                for (k, version) in dropped {
                    let group = self.group_of(&k);
                    self.retire_winner(&k, &group, unpack_version(version));
                }
                return Err(*source);
            }
            Err(e) => return Err(e),
        };
        let present = stats.dropped.len();
        for (k, version) in stats.dropped {
            let group = self.group_of(&k);
            self.retire_winner(&k, &group, unpack_version(version));
        }
        Ok(present)
    }

    pub fn clear(&mut self) -> Result<(), EngineError> {
        let truncated = self.engine.truncate();
        self.sync_pinned_bytes();
        truncated?;
        self.reset_pending_forget();
        *self.regroup.borrow_mut() = None;
        self.versions.clear();
        self.clear_accounted();
        if let Some(state) = self.logical_range_counts.borrow_mut().as_mut() {
            state.counts = Some(vec![0; state.map.num_ranges()]);
            state.recounts.clear();
        }
        self.reset_seed_progress(SeedEnd::Dropped);
        self.seed_restart_count.set(0);
        self.seed_started.set(None);
        self.seed_recovery.set(false);
        Ok(())
    }

    pub fn tracked_keys(&self) -> usize {
        self.engine.total_entries()
    }

    pub fn live_keys(&self) -> u64 {
        if self.versions.seeded.get() {
            self.versions.live.get()
        } else {
            self.engine.total_entries() as u64
        }
    }

    pub(crate) fn exact_live_keys(&self) -> Option<u64> {
        (self.versions.seeded.get() && !self.versions.has_dirty())
            .then(|| self.versions.live.get())
    }
}
