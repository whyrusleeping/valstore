use super::*;
use crate::range_tombstone::{decode_rt_value, encode_rt_value, CoverDelta, DEFAULT_MAX_FRAGMENTS};

#[derive(Default)]
pub(super) struct RtState {
    pub(super) fragments: std::sync::Arc<FragmentSet>,
    pub(super) tail: Vec<(RtRecord, ValuePtr)>,
    pub(super) accounted_seq: std::cell::Cell<u64>,
    pub(super) completed_flush_seq: u64,
    pub(super) runs_through: std::cell::RefCell<Option<(u64, FragmentSet)>>,
}

fn seq_window(rts: &[RtRecord], after: Option<u64>, through: u64) -> &[RtRecord] {
    let start = after.map_or(0, |a| rts.partition_point(|r| r.seq <= a));
    let end = rts.partition_point(|r| r.seq <= through).max(start);
    &rts[start..end]
}

impl RtState {
    pub(super) fn recover<'a>(
        persisted: impl Iterator<Item = &'a RtRecord>,
        tail: impl Iterator<Item = (RtRecord, ValuePtr)>,
        completed_flush_seq: u64,
    ) -> Self {
        let mut tail: Vec<(RtRecord, ValuePtr)> = tail.collect();
        tail.sort_by_key(|(r, _)| r.seq);
        let mut records: Vec<&RtRecord> = persisted.collect();
        records.extend(tail.iter().map(|(r, _)| r));
        let fragments = FragmentSet::from_records(records);
        RtState {
            fragments: std::sync::Arc::new(fragments),
            tail,
            accounted_seq: std::cell::Cell::new(0),
            completed_flush_seq,
            runs_through: std::cell::RefCell::new(None),
        }
    }

    fn tail_window(&self, after: Option<u64>, through: u64) -> &[(RtRecord, ValuePtr)] {
        let start = after.map_or(0, |a| self.tail.partition_point(|(r, _)| r.seq <= a));
        let end = self.tail.partition_point(|(r, _)| r.seq <= through).max(start);
        &self.tail[start..end]
    }
}

pub(super) struct RtCompaction {
    pub(super) filter: Option<FragmentSet>,
    pub(super) floor: u64,
    pub(super) accounted_seq: u64,
    pub(super) keep: Vec<RtRecord>,
    pub(super) collected: Vec<RtRecord>,
}

static COVERED_DROPPED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static COLLECTED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static DOMINATED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn rt_covered_dropped_total() -> u64 {
    COVERED_DROPPED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn rt_collected_total() -> u64 {
    COLLECTED.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn rt_dominated_dropped_total() -> u64 {
    DOMINATED.load(std::sync::atomic::Ordering::Relaxed)
}

pub(super) fn note_dominated_dropped(n: usize) {
    if n > 0 {
        DOMINATED.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(super) fn note_covered_dropped(n: u64) {
    if n > 0 {
        COVERED_DROPPED.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(super) fn is_covered(cover: Option<u128>, version: u128) -> bool {
    cover.is_some_and(|c| version <= c)
}

pub(super) fn split_covered(
    entries: Vec<SnapshotEntry>,
    groups: &mut [crate::index::SourceGroup],
    filter: &FragmentSet,
) -> (Vec<SnapshotEntry>, Vec<SnapshotEntry>) {
    let mut cursor = filter.cursor_from(&[]);
    let mut kept = Vec::with_capacity(entries.len());
    let mut covered = Vec::new();
    let (mut g, mut left) = (0usize, groups.first().map_or(0, |g| g.entries));
    for e in entries {
        while left == 0 && g + 1 < groups.len() {
            g += 1;
            left = groups[g].entries;
        }
        left = left.saturating_sub(1);
        if !is_covered(cursor.cover(&e.key), e.version) {
            kept.push(e);
            continue;
        }
        if let Some(group) = groups.get_mut(g).filter(|group| group.entries > 0) {
            group.entries -= 1;
            group.sum = group.sum.wrapping_sub(crate::index::snapshot_entry_hash(&e));
        }
        covered.push(e);
    }
    (kept, covered)
}

pub(super) fn merge_sorted_entries(a: Vec<SnapshotEntry>, b: Vec<SnapshotEntry>) -> Vec<SnapshotEntry> {
    if b.is_empty() {
        return a;
    }
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut a, mut b) = (a.into_iter().peekable(), b.into_iter().peekable());
    loop {
        let take_a = match (a.peek(), b.peek()) {
            (Some(x), Some(y)) => x.key <= y.key,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return out,
        };
        out.push(if take_a { a.next() } else { b.next() }.unwrap());
    }
}

fn span_meets_run(r: &RtRecord, meta: &RunMeta) -> bool {
    meta.entry_count > 0
        && r.lo.as_deref().is_none_or(|lo| meta.max_key.as_slice() >= lo)
        && r.hi.as_deref().is_none_or(|hi| meta.min_key.as_slice() < hi)
}

pub(super) fn decode_record(rec: &Record) -> Result<RtRecord, EngineError> {
    if !rec.key.is_empty() {
        return Err(EngineError::Log(LogError::Corrupt("range tombstone record carries a key")));
    }
    let (lo, hi) = decode_rt_value(&rec.value).map_err(|why| EngineError::Log(LogError::Corrupt(why)))?;
    Ok(RtRecord { lo, hi, version: rec.version, seq: rec.seq })
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RtScanStats {
    pub filtered_entries: u64,
    pub skipped_blocks: u64,
    pub block_reads: u64,
    pub index_probes: u64,
}

fn search_steps(n: usize) -> u64 {
    (usize::BITS - n.leading_zeros()) as u64
}

pub(super) fn rt_next_block<D: BlockDevice>(
    or: &OpenRun<D>,
    set: &FragmentSet,
    lo: Option<&[u8]>,
    mut i: usize,
    stats: &mut RtScanStats,
) -> usize {
    let run = &or.run;
    let n = run.index_len();
    if let Some(tree) = or.max_tree.as_ref() {
        while i < n {
            let first = run.block_first_key(i);
            let at = match lo {
                Some(l) if l > first => l,
                _ => first,
            };
            let Some((_, fhi, version)) = set.fragment_at(at) else { break };
            let h = crate::range_tombstone::version_hlc(version);
            if run.block_max_hlc(i).is_none_or(|m| m >= h) {
                break;
            }
            let end = match fhi {
                None => n,
                Some(fhi) => {
                    stats.index_probes += search_steps(n);
                    let p = run.blocks_starting_at_or_below(fhi);
                    if p == n && or.meta.max_key.as_slice() < fhi { n } else { p.saturating_sub(1) }
                }
            };
            if end <= i {
                break;
            }
            let j = tree.first_at_least(i, end, h, |b| run.block_max_hlc(b).unwrap_or(u64::MAX), &mut stats.index_probes);
            stats.skipped_blocks += (j - i) as u64;
            i = j;
            if j < end {
                break;
            }
        }
    }
    if i < n {
        stats.block_reads += 1;
    }
    i
}

pub(super) fn rt_prev_block<D: BlockDevice>(
    or: &OpenRun<D>,
    set: &FragmentSet,
    hi: Option<&[u8]>,
    mut end: usize,
    stats: &mut RtScanStats,
) -> usize {
    let run = &or.run;
    let n = run.index_len();
    if let Some(tree) = or.max_tree.as_ref() {
        while end > 0 {
            let first = run.block_first_key(end - 1);
            let Some((flo, fhi, version)) = set.fragment_at(first) else { break };
            if fhi.is_some_and(|fhi| {
                hi.is_none_or(|h| h > fhi)
                    && if end < n { run.block_first_key(end) > fhi } else { or.meta.max_key.as_slice() >= fhi }
            }) {
                break;
            }
            let h = crate::range_tombstone::version_hlc(version);
            if run.block_max_hlc(end - 1).is_none_or(|m| m >= h) {
                break;
            }
            stats.index_probes += search_steps(n);
            let start = run.blocks_starting_below(flo);
            let next = tree.last_at_least(start, end, h, |b| run.block_max_hlc(b).unwrap_or(u64::MAX), &mut stats.index_probes);
            let new_end = next.map_or(start, |b| b + 1);
            stats.skipped_blocks += (end - new_end) as u64;
            end = new_end;
            if next.is_some() {
                break;
            }
        }
    }
    if end > 0 {
        stats.block_reads += 1;
    }
    end
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeDeleteApplied {
    pub seq: Option<u64>,
    pub delta: Vec<CoverDelta>,
}

impl<D: BlockDevice> Engine<D> {
    pub fn apply_range_delete(
        &mut self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        version: u128,
    ) -> Result<RangeDeleteApplied, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }
        if matches!((lo, hi), (Some(l), Some(h)) if l >= h) {
            return Err(EngineError::InvalidRange);
        }
        if !run::version_supports_range_tombstones(run::run_write_version()) {
            return Err(EngineError::RangeTombstonesUnsupported);
        }
        let Some(plan) = self.rts.fragments.plan_overlay(lo, hi, version).map_err(EngineError::RangeTombstoneCap)? else {
            return Ok(RangeDeleteApplied { seq: None, delta: Vec::new() });
        };
        self.ensure_range_tombstone_fence()?;
        self.log.skip_seqs_below(self.last_flush_seq + 1);
        let epoch_before = self.log.next_epoch();
        let (seq, ptr) = self.log.append(
            &self.dev,
            &mut self.alloc,
            RecordKind::RangeDelete,
            version,
            &[],
            &encode_rt_value(lo, hi),
        )?;
        let delta = std::sync::Arc::make_mut(&mut self.rts.fragments).apply_overlay(plan);
        let at = self.rts.tail.partition_point(|(r, _)| r.seq <= seq);
        self.rts.tail.insert(at, (RtRecord { lo: lo.map(<[u8]>::to_vec), hi: hi.map(<[u8]>::to_vec), version, seq }, ptr));
        self.bump_storage_generation();
        self.bump_mutable_generation();
        if self.log.next_epoch() != epoch_before || self.rts.tail.len() == 1 {
            self.refresh_data_reserve();
        }
        Ok(RangeDeleteApplied { seq: Some(seq), delta })
    }

    pub fn build_range_tombstone_max_trees(&mut self) -> u64 {
        if self.rts.fragments.is_empty() {
            for or in &mut self.runs {
                or.max_tree = None;
            }
            return 0;
        }
        self.runs.iter_mut().map(OpenRun::build_max_tree).sum()
    }

    pub fn remove_covered_mutable(&mut self, lo: Option<&[u8]>, hi: Option<&[u8]>, cover: &FragmentSet) -> usize {
        if cover.is_empty() {
            return 0;
        }
        let Some(entries) = self.index.scan(lo, hi) else { return 0 };
        let mut cursor = cover.cursor_from(lo.unwrap_or(&[]));
        let mut removed = 0;
        for (key, entry) in entries {
            if !is_covered(cursor.cover(&key), entry.version) || !self.index.remove_if_eq(&key, &entry) {
                continue;
            }
            if let Some(directory) = self.winner_directory.as_mut() {
                if directory.get(&key) == Some(entry) {
                    directory.remove(&key);
                }
            }
            removed += 1;
        }
        if removed > 0 {
            self.bump_mutable_generation();
        }
        removed
    }

    pub fn range_tombstone_max_tree_blocks(&self) -> u64 {
        self.runs.iter().filter_map(|or| or.max_tree.as_ref()).map(|t| t.blocks() as u64).sum()
    }

    pub fn range_tombstones(&self) -> &FragmentSet {
        &self.rts.fragments
    }

    pub fn range_tombstone_cover(&self, key: &[u8]) -> Option<u128> {
        if self.rts.fragments.is_empty() {
            return None;
        }
        self.rts.fragments.cover(key)
    }

    pub fn set_range_tombstone_max_fragments(&mut self, max: usize) {
        std::sync::Arc::make_mut(&mut self.rts.fragments).set_max_fragments(max);
    }

    pub fn advance_range_tombstone_accounted_seq(&self, seq: u64) {
        self.rts.accounted_seq.set(self.rts.accounted_seq.get().max(seq));
    }

    pub fn range_tombstone_accounted_seq(&self) -> u64 {
        self.rts.accounted_seq.get()
    }

    pub fn range_tombstone_max_seq(&self) -> u64 {
        self.rt_max_seq_through(u64::MAX)
    }

    pub fn range_tombstones_through(&self, seq: u64) -> FragmentSet {
        if self.range_tombstone_max_seq() <= seq {
            return (*self.rts.fragments).clone();
        }
        self.rt_set_through(seq, seq)
    }

    fn max_run_rt_seq(&self) -> u64 {
        self.runs.iter().filter_map(|or| or.rts.last()).map(|r| r.seq).max().unwrap_or(0)
    }

    fn rt_max_seq_through(&self, tail_through: u64) -> u64 {
        let tail = self.rts.tail_window(None, tail_through).last().map_or(0, |(r, _)| r.seq);
        self.max_run_rt_seq().max(tail)
    }

    fn run_rts_between(&self, after: Option<u64>, through: u64) -> impl Iterator<Item = &RtRecord> + '_ {
        self.runs.iter().flat_map(move |or| seq_window(&or.rts, after, through))
    }

    fn rt_records_through(&self, limit: u64, tail_through: u64) -> impl Iterator<Item = &RtRecord> + '_ {
        let tail = self.rts.tail_window(None, limit.min(tail_through));
        self.run_rts_between(None, limit).chain(tail.iter().map(|(r, _)| r))
    }

    fn run_rt_set_through(&self, limit: u64) -> FragmentSet {
        let limit = limit.min(self.max_run_rt_seq());
        let mut cache = self.rts.runs_through.borrow_mut();
        match cache.as_mut() {
            Some((at, set)) if *at <= limit => {
                for r in self.run_rts_between(Some(*at), limit) {
                    set.overlay(r.lo.as_deref(), r.hi.as_deref(), r.version).expect("an uncapped overlay never refuses");
                }
                *at = limit;
            }
            _ => {
                let mut set = FragmentSet::from_records(self.run_rts_between(None, limit));
                set.set_max_fragments(usize::MAX);
                *cache = Some((limit, set));
            }
        }
        cache.as_ref().expect("filled above").1.clone()
    }

    fn rt_set_through(&self, limit: u64, tail_through: u64) -> FragmentSet {
        let mut set = self.run_rt_set_through(limit);
        for (r, _) in self.rts.tail_window(None, limit.min(tail_through)) {
            set.overlay(r.lo.as_deref(), r.hi.as_deref(), r.version).expect("an uncapped overlay never refuses");
        }
        set.set_max_fragments(DEFAULT_MAX_FRAGMENTS);
        debug_assert!(set == FragmentSet::from_records(self.rt_records_through(limit, tail_through)), "cached RT filter set drifted");
        set
    }

    pub(super) fn note_run_rts_added(&self, rts: &[RtRecord]) {
        if let Some((at, set)) = self.rts.runs_through.borrow_mut().as_mut() {
            for r in seq_window(rts, None, *at) {
                set.overlay(r.lo.as_deref(), r.hi.as_deref(), r.version).expect("an uncapped overlay never refuses");
            }
        }
    }

    pub(super) fn note_run_rts_replaced(&mut self, released: bool, before: usize) {
        if released || self.run_rt_count() != before {
            *self.rts.runs_through.get_mut() = None;
        }
    }

    pub(super) fn run_rt_count(&self) -> usize {
        self.runs.iter().map(|or| or.rts.len()).sum()
    }

    pub(super) fn rt_filter_set(&self, limit: u64, tail_through: u64) -> Option<std::borrow::Cow<'_, FragmentSet>> {
        if self.rts.fragments.is_empty() || self.rts.accounted_seq.get() == 0 {
            return None;
        }
        let tail_limit = limit.min(tail_through);
        let run_min = self.runs.iter().filter_map(|or| or.rts.first()).map(|r| r.seq).min();
        let every = self.max_run_rt_seq() <= limit && self.rts.tail.last().is_none_or(|(r, _)| r.seq <= tail_limit);
        let any = run_min.is_some_and(|s| s <= limit) || self.rts.tail.first().is_some_and(|(r, _)| r.seq <= tail_limit);
        if !any {
            return None;
        }
        if every {
            return Some(std::borrow::Cow::Borrowed(&*self.rts.fragments));
        }
        Some(std::borrow::Cow::Owned(self.rt_set_through(limit, tail_through)))
    }

    pub(super) fn rt_accounted_set(&self) -> Option<std::borrow::Cow<'_, FragmentSet>> {
        self.rt_filter_set(self.rts.accounted_seq.get(), u64::MAX)
    }

    pub(super) fn rt_flush_filter(&self, covered_seq: u64) -> (Option<FragmentSet>, u64) {
        let filter = self.rt_filter_set(self.rts.accounted_seq.get().min(covered_seq), covered_seq).map(|f| f.into_owned());
        let persisted = self.rt_max_seq_through(covered_seq);
        (filter, self.rts.accounted_seq.get().min(persisted))
    }

    pub(super) fn rt_merge_plan(&self, inputs: &[RunMeta], gc_collect_before: Option<u128>) -> RtCompaction {
        let is_input = |or: &OpenRun<D>| inputs.iter().any(|m| m.handle == or.meta.handle);
        let filter = self.rt_filter_set(self.rts.accounted_seq.get(), 0).map(|f| f.into_owned());
        let persisted = self.rt_max_seq_through(0);
        let mut keep = Vec::new();
        let mut collected = Vec::new();
        for r in self.input_rts(inputs) {
            if self.rt_collectible(&r, gc_collect_before, &is_input) {
                collected.push(r);
            } else {
                keep.push(r);
            }
        }
        let accounted_seq = self.rts.accounted_seq.get();
        RtCompaction { filter, floor: accounted_seq.min(persisted), accounted_seq, keep, collected }
    }

    fn rt_collectible(&self, r: &RtRecord, gc_collect_before: Option<u128>, is_input: &dyn Fn(&OpenRun<D>) -> bool) -> bool {
        gc_collect_before.is_some_and(|cutoff| r.version < cutoff)
            && r.seq <= self.rts.accounted_seq.get()
            && r.seq <= self.rts.completed_flush_seq
            && self.rt_outside_runs_filtered(r, is_input)
    }

    pub(super) fn rt_outside_runs_filtered(&self, r: &RtRecord, is_input: &dyn Fn(&OpenRun<D>) -> bool) -> bool {
        self.runs
            .iter()
            .filter(|or| !is_input(or) && span_meets_run(r, &or.meta))
            .all(|or| or.run.rt_seq_floor() >= r.seq)
    }

    pub(super) fn finish_rt_collection(&mut self, collected: &[RtRecord]) {
        if collected.is_empty() {
            return;
        }
        let max = self.rts.fragments.max_fragments();
        let records = self.range_tombstone_records();
        let mut fragments = FragmentSet::from_records(&records);
        fragments.set_max_fragments(max);
        self.rts.fragments = std::sync::Arc::new(fragments);
        if self.rts.fragments.is_empty() {
            self.build_range_tombstone_max_trees();
        }
        COLLECTED.fetch_add(collected.len() as u64, std::sync::atomic::Ordering::Relaxed);
        self.bump_mutable_generation();
    }

    pub(super) fn releasable_range_tombstones(&self, released: &FragmentSet) -> FragmentSet {
        let mut out = FragmentSet::with_max_fragments(usize::MAX);
        for piece in released.fragments() {
            let (lo, hi) = (piece.lo.as_slice(), piece.hi.as_deref());
            if self.rts.fragments.covers_span(lo, hi, piece.version)
                && self.rts.fragments.clipped(lo, hi).iter().all(|f| f.version == piece.version)
            {
                out.overlay(Some(&piece.lo), piece.hi.as_deref(), piece.version).expect("an uncapped overlay never refuses");
            }
        }
        out
    }

    pub(super) fn remove_released_mutable(&mut self, released: &FragmentSet) -> usize {
        released
            .fragments()
            .map(|piece| self.remove_covered_mutable(Some(&piece.lo), piece.hi.as_deref(), released))
            .sum()
    }

    pub(super) fn clip_released_range_tombstones(&mut self, released: &FragmentSet) {
        if released.is_empty() {
            return;
        }
        if !self.flush_in_flight {
            self.rts.tail = std::mem::take(&mut self.rts.tail)
                .into_iter()
                .flat_map(|(r, ptr)| r.without(released).into_iter().map(move |piece| (piece, ptr)))
                .collect();
        }
        let max = self.rts.fragments.max_fragments();
        let mut fragments = FragmentSet::from_records(&self.range_tombstone_records());
        fragments.set_max_fragments(max);
        self.rts.fragments = std::sync::Arc::new(fragments);
        if self.rts.fragments.is_empty() {
            self.build_range_tombstone_max_trees();
        }
        self.bump_storage_generation();
        self.bump_mutable_generation();
    }

    pub fn release_range_tombstones(&mut self, released: &FragmentSet) -> Result<usize, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.flush_in_flight || self.compact_in_flight {
            return Ok(0);
        }
        let released = self.releasable_range_tombstones(released);
        self.remove_released_mutable(&released);
        self.clip_released_range_tombstones(&released);
        Ok(released.len())
    }

    pub fn range_tombstone_records(&self) -> Vec<RtRecord> {
        self.range_tombstone_records_between(0, u64::MAX)
    }

    pub fn range_tombstone_records_between(&self, after: u64, through: u64) -> Vec<RtRecord> {
        let mut out: Vec<RtRecord> = self
            .run_rts_between(Some(after), through)
            .chain(self.rts.tail_window(Some(after), through).iter().map(|(r, _)| r))
            .cloned()
            .collect();
        out.sort_by_key(|r| r.seq);
        out
    }

    pub fn range_tombstone_record_count(&self) -> usize {
        self.run_rt_count() + self.rts.tail.len()
    }

    pub fn has_unflushed_range_tombstones(&self) -> bool {
        !self.rts.tail.is_empty()
    }

    pub(super) fn unflushed_rts_through(&self, covered_seq: u64) -> Vec<RtRecord> {
        self.rts.tail.iter().filter(|(r, _)| r.seq <= covered_seq).map(|(r, _)| r.clone()).collect()
    }

    pub(super) fn retire_flushed_rts(&mut self, covered_seq: u64) {
        self.rts.tail.retain(|(r, _)| r.seq > covered_seq);
    }

    pub(super) fn flush_run_version(&self, rts: &[RtRecord]) -> u16 {
        let configured = run::run_write_version();
        if rts.is_empty() { configured } else { configured.max(run::RUN_VERSION) }
    }

    pub(super) fn input_rts(&self, inputs: &[RunMeta]) -> Vec<RtRecord> {
        let mut out: Vec<RtRecord> = inputs
            .iter()
            .filter_map(|m| self.runs.iter().find(|or| or.meta.handle == m.handle))
            .flat_map(|or| or.rts.iter().cloned())
            .collect();
        out.sort_by_key(|r| r.seq);
        out
    }

    pub(super) fn for_each_mutable_value_segment(&self, f: &mut dyn FnMut(SegmentId)) {
        self.index.for_each_value_segment(f);
        for (_, ptr) in &self.rts.tail {
            f(ptr.segment);
        }
    }

    pub(super) fn has_range_tombstone_fence(&self) -> bool {
        self.runs.iter().any(|or| or.run.supports_range_tombstones())
    }

    fn ensure_range_tombstone_fence(&mut self) -> Result<(), EngineError> {
        if self.has_range_tombstone_fence() {
            return Ok(());
        }
        let (bytes, _) = run::build_run_checked_rts(
            &[],
            RunBuildOpts::default(),
            run::RUN_VERSION,
            run::EncodePath::Flush,
            run::RunSource::Inline,
            run::RunRts::NONE,
        )
        .map_err(EngineError::EncodeIntegrity)?;
        let handle = run::write_run_source(&*self.dev, &mut self.alloc, &bytes, 3)?;
        let opened = DeviceRun::open(self.dev.clone(), &self.alloc, handle.clone());
        let run = match opened {
            Ok(run) => run,
            Err(e) => {
                for s in &handle.segs {
                    let _ = self.alloc.free(*s);
                }
                return Err(e.into());
            }
        };
        let meta = RunMeta {
            handle: handle.clone(),
            min_key: Vec::new(),
            max_key: Vec::new(),
            entry_count: 0,
            level: 0,
            value_segs: Some(Vec::new()),
            value_bytes: Some(0),
            tombstone_min_version: None,
            value_seg_bytes: Some(Vec::new()),
        };
        let generation = self.manifest_gen + 1;
        let mut run_metas: Vec<RunMeta> = self.runs.iter().map(|or| or.meta.clone()).collect();
        run_metas.push(meta.clone());
        let manifest = Manifest {
            generation,
            covered_seq: self.last_flush_seq,
            covered_epoch: self.last_flush_epoch,
            runs: run_metas,
        };
        let live = match snapshot_io::write_manifest(
            &*self.dev,
            &mut self.alloc,
            &manifest,
            generation,
            self.live_manifest.as_ref(),
        ) {
            Ok(live) => live,
            Err(e) => {
                drop(run);
                for s in &handle.segs {
                    let _ = self.alloc.free(*s);
                }
                return Err(e.into());
            }
        };
        self.manifest_gen = generation;
        self.live_manifest = Some(live);
        self.runs.push(OpenRun::new(meta, run, false));
        self.bump_storage_generation();
        self.key_filter_maintain();
        self.refresh_data_reserve();
        Ok(())
    }
}
