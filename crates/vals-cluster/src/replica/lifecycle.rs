use super::*;

type KeySpan = (Option<Vec<u8>>, Option<Vec<u8>>);

fn placement_equivalent(
    previous: &SeedPlacementInputs,
    next: &SeedPlacementInputs,
    ring: &crate::ring::Ring,
) -> bool {
    changed_placement_spans(previous, next, ring).is_some_and(|spans| spans.is_empty())
}

fn changed_placement_spans(
    previous: &SeedPlacementInputs,
    next: &SeedPlacementInputs,
    ring: &crate::ring::Ring,
) -> Option<Vec<KeySpan>> {
    let SeedPlacementInputs {
        ring_epoch, catalog_epoch, zone_id, config, range_map: old, rf, vnodes, members,
    } = previous;
    if (ring_epoch, catalog_epoch, zone_id, config, rf, vnodes, members)
        != (&next.ring_epoch, &next.catalog_epoch, &next.zone_id, &next.config, &next.rf,
            &next.vnodes, &next.members)
    {
        return None;
    }
    let (Some(old), Some(new), Some(cfg)) = (old, &next.range_map, config) else {
        return None;
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return None;
    }
    let scope = cfg.scope.as_ref();
    let mut bounds: Vec<&[u8]> = std::iter::once(&[][..])
        .chain(old.splits().iter().map(Vec::as_slice))
        .chain(new.splits().iter().map(Vec::as_slice))
        .collect();
    bounds.sort_unstable();
    bounds.dedup();
    let mut changed: Vec<KeySpan> = Vec::new();
    for (i, key) in bounds.iter().enumerate() {
        let mut before = old.replicas_for_key_while_rehoming(ring, *zone_id, key, cfg.rf, scope);
        let mut after = new.replicas_for_key_while_rehoming(ring, *zone_id, key, cfg.rf, scope);
        before.sort_unstable();
        after.sort_unstable();
        if before == after {
            continue;
        }
        let lo = (!key.is_empty()).then(|| key.to_vec());
        let hi = bounds.get(i + 1).map(|h| h.to_vec());
        match changed.last_mut() {
            Some((_, last_hi)) if last_hi.as_deref() == lo.as_deref() => *last_hi = hi,
            _ => changed.push((lo, hi)),
        }
    }
    Some(changed)
}

fn range_holds(map: &RangeMap, index: usize, key: &[u8]) -> bool {
    let (lo, hi) = map.range_bounds(index);
    lo.is_none_or(|lo| key >= lo) && hi.is_none_or(|hi| key < hi)
}

fn strictly_inside(lo: Option<&[u8]>, hi: Option<&[u8]>, key: &[u8]) -> bool {
    lo.is_none_or(|lo| key > lo) && hi.is_none_or(|hi| key < hi)
}

impl RegroupSpan {
    fn routes_old(&self, key: &[u8]) -> bool {
        self.lo.as_deref().is_none_or(|lo| key >= lo)
            && self.hi.as_deref().is_none_or(|hi| key < hi)
            && self.after.as_deref().is_none_or(|after| key > after)
    }

    fn page_lo(&self) -> Option<Vec<u8>> {
        match &self.after {
            Some(after) => Some([after.as_ref(), &[0]].concat()),
            None => self.lo.as_deref().map(<[u8]>::to_vec),
        }
    }
}

pub(super) const MAX_REGROUP_COMPOSITIONS: u32 = 8;

fn union_spans(mut spans: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Vec<RegroupSpan> {
    spans.retain(|(lo, hi)| hi.as_ref().is_none_or(|hi| lo < hi));
    spans.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut merged: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(spans.len());
    for (lo, hi) in spans {
        match merged.last_mut() {
            Some((_, last)) if last.as_ref().is_none_or(|last| lo <= *last) => {
                if last.as_ref().is_some_and(|last| hi.as_ref().is_none_or(|hi| hi > last)) {
                    *last = hi;
                }
            }
            _ => merged.push((lo, hi)),
        }
    }
    merged
        .into_iter()
        .map(|(lo, hi)| RegroupSpan {
            lo: (!lo.is_empty()).then(|| lo.into_boxed_slice()),
            hi: hi.map(Vec::into_boxed_slice),
            after: None,
        })
        .collect()
}

const RECOUNT_FRESH: RecountChild = RecountChild::Scan { after: None, covered: 0, done: false };

impl SplitRecount {
    fn contains(&self, key: &[u8]) -> bool {
        self.lo.as_deref().is_none_or(|lo| key >= lo)
            && self.hi.as_deref().is_none_or(|hi| key < hi)
    }

    fn child_of(&self, key: &[u8]) -> usize {
        self.cuts.partition_point(|cut| **cut <= *key)
    }

    fn child_bounds(&self, i: usize) -> (Option<&[u8]>, Option<&[u8]>) {
        let lo = if i == 0 { self.lo.as_deref() } else { Some(&*self.cuts[i - 1]) };
        let hi = if i == self.cuts.len() { self.hi.as_deref() } else { Some(&*self.cuts[i]) };
        (lo, hi)
    }

    fn page_bounds(&self, i: usize) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
        let (lo, hi) = self.child_bounds(i);
        let page_lo = match &self.children[i] {
            RecountChild::Scan { after: Some(after), .. } => Some([after.as_ref(), &[0]].concat()),
            _ => lo.map(<[u8]>::to_vec),
        };
        (page_lo, hi.map(<[u8]>::to_vec))
    }

    pub(super) fn next_scan(&self) -> Option<usize> {
        self.children
            .iter()
            .position(|child| matches!(child, RecountChild::Scan { done: false, .. }))
    }

    fn covered_sum(&self) -> Option<u64> {
        self.children.iter().try_fold(0u64, |sum, child| match child {
            RecountChild::Scan { covered, .. } => sum.checked_add(*covered),
            RecountChild::Derived => Some(sum),
        })
    }

    fn note(&mut self, key: &[u8], insert: bool) {
        self.total = if insert {
            self.total.saturating_add(1)
        } else {
            self.total.saturating_sub(1)
        };
        let i = self.child_of(key);
        if let RecountChild::Scan { after, covered, done } = &mut self.children[i] {
            if *done || after.as_deref().is_some_and(|after| key <= after) {
                *covered = if insert {
                    covered.saturating_add(1)
                } else {
                    covered.saturating_sub(1)
                };
            }
        }
    }
}

type SizeHint<'a> = &'a dyn Fn(Option<&[u8]>, Option<&[u8]>) -> u64;

enum RemapChild {
    Known(u64),
    Partial(RecountChild),
    Derived,
    Fresh,
}

#[derive(Default)]
struct Remap {
    counts: Vec<u64>,
    spans: Vec<SplitRecount>,
    created: u64,
    empty: u64,
    merges: u64,
    repartitions: u64,
}

impl LogicalRangeCounts {
    fn note_winner(&mut self, key: &[u8], insert: bool) {
        let Some(counts) = self.counts.as_mut() else { return };
        if !self.recounts.is_empty() {
            if let Some(recount) = self.recounts.iter_mut().find(|r| r.contains(key)) {
                recount.note(key, insert);
                return;
            }
        }
        let index = self.map.range_index(key);
        counts[index] = if insert {
            counts[index].saturating_add(1)
        } else {
            counts[index].saturating_sub(1)
        };
    }

    pub(super) fn span_child(&self, lo: Option<&[u8]>) -> Option<(usize, usize)> {
        self.recounts.iter().enumerate().find_map(|(s, r)| {
            let hit = match lo {
                None => r.lo.is_none(),
                Some(key) => r.contains(key),
            };
            hit.then(|| (s, lo.map_or(0, |key| r.child_of(key))))
        })
    }

    fn count_bounds(&self) -> Option<Vec<(u64, u64)>> {
        let counts = self.counts.as_ref()?;
        let mut bounds: Vec<(u64, u64)> = counts.iter().map(|&n| (n, n)).collect();
        for r in &self.recounts {
            let base = r.lo.as_deref().map_or(0, |lo| self.map.range_index(lo));
            let covered = r.covered_sum()?;
            for (i, child) in r.children.iter().enumerate() {
                let slot = bounds.get_mut(base + i)?;
                *slot = match child {
                    RecountChild::Scan { covered: n, done: true, .. } => (*n, *n),
                    RecountChild::Scan { covered: n, .. } => {
                        (*n, r.total.saturating_sub(covered - n))
                    }
                    RecountChild::Derived => (0, r.total.saturating_sub(covered)),
                };
            }
        }
        Some(bounds)
    }

    fn remap(&self, map: &RangeMap, hint: SizeHint<'_>) -> Option<Remap> {
        let counts = self.counts.as_ref()?;
        if counts.len() != self.map.num_ranges() {
            return None;
        }
        let (old, new) = (self.map.splits(), map.splits());
        let mut seps = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < old.len() && j < new.len() {
            match old[i].cmp(&new[j]) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    let cut = old[i].as_slice();
                    if !self
                        .recounts
                        .iter()
                        .any(|r| strictly_inside(r.lo.as_deref(), r.hi.as_deref(), cut))
                    {
                        seps.push((i + 1, j + 1));
                    }
                    i += 1;
                    j += 1;
                }
            }
        }
        seps.push((old.len() + 1, new.len() + 1));
        let mut out = Remap { counts: Vec::with_capacity(map.num_ranges()), ..Remap::default() };
        let (mut o0, mut n0) = (0, 0);
        for (o1, n1) in seps {
            if o1 - o0 == 1 && n1 - n0 == 1 {
                out.counts.push(counts[o0]);
            } else if o1 - o0 == n1 - n0 && old[o0..o1 - 1] == new[n0..n1 - 1] {
                let (s, _) = self.span_child(self.map.range_bounds(o0).0)?;
                out.counts.extend_from_slice(&counts[o0..o1]);
                out.spans.push(self.recounts[s].clone());
            } else {
                self.remap_group(map, (o0, o1), (n0, n1), hint, &mut out)?;
            }
            (o0, n0) = (o1, n1);
        }
        (out.counts.len() == map.num_ranges()).then_some(out)
    }

    fn remap_group(
        &self,
        map: &RangeMap,
        (o0, o1): (usize, usize),
        (n0, n1): (usize, usize),
        hint: SizeHint<'_>,
        out: &mut Remap,
    ) -> Option<()> {
        let counts = self.counts.as_ref()?;
        let old = self.map.splits();
        let piece = |o: usize| -> RemapChild {
            match self.span_child(self.map.range_bounds(o).0) {
                None => RemapChild::Known(counts[o]),
                Some((s, c)) => match &self.recounts[s].children[c] {
                    RecountChild::Scan { covered, done: true, .. } => RemapChild::Known(*covered),
                    RecountChild::Derived => RemapChild::Derived,
                    partial => RemapChild::Partial(partial.clone()),
                },
            }
        };
        let mut total = 0u64;
        let mut spans: Vec<usize> = Vec::new();
        for (o, &count) in (o0..o1).zip(&counts[o0..o1]) {
            match self.span_child(self.map.range_bounds(o).0) {
                None => total = total.checked_add(count)?,
                Some((s, _)) if !spans.contains(&s) => {
                    spans.push(s);
                    total = total.checked_add(self.recounts[s].total)?;
                }
                Some(_) => {}
            }
        }
        if !spans.is_empty() {
            out.repartitions += 1;
        }
        if n1 - n0 == 1 {
            out.merges += 1;
            out.counts.push(total);
            return Some(());
        }
        if total == 0 {
            out.empty += 1;
            out.counts.extend(std::iter::repeat_n(0, n1 - n0));
            return Some(());
        }
        let old_index = |key: Option<&[u8]>, end: usize| match key {
            None => Some(end),
            Some(key) => old.binary_search_by(|s| s.as_slice().cmp(key)).ok().map(|p| p + 1),
        };
        let mut children = Vec::with_capacity(n1 - n0);
        for n in n0..n1 {
            let (lo, hi) = map.range_bounds(n);
            let span = old_index(lo, 0).zip(old_index(hi, old.len() + 1));
            let class = match span {
                Some((a, b)) if b == a + 1 => piece(a),
                Some((a, b)) if b > a + 1 => {
                    let mut known = Some(0u64);
                    let mut derived = 0;
                    for o in a..b {
                        match piece(o) {
                            RemapChild::Known(n) => known = known.and_then(|k| k.checked_add(n)),
                            RemapChild::Derived => derived += 1,
                            _ => derived += 2,
                        }
                    }
                    match (known, derived) {
                        (Some(n), 0) => RemapChild::Known(n),
                        (Some(_), 1) => RemapChild::Derived,
                        _ => RemapChild::Fresh,
                    }
                }
                _ => RemapChild::Fresh,
            };
            children.push(class);
        }
        let unknown: Vec<usize> = (0..children.len())
            .filter(|&c| !matches!(children[c], RemapChild::Known(_)))
            .collect();
        let known = children.iter().try_fold(0u64, |sum, child| match child {
            RemapChild::Known(n) => sum.checked_add(*n),
            _ => Some(sum),
        })?;
        if unknown.len() <= 1 {
            let rest = total.checked_sub(known)?;
            if unknown.is_empty() && rest != 0 {
                return None;
            }
            out.counts.extend(children.iter().map(|child| match child {
                RemapChild::Known(n) => *n,
                _ => rest,
            }));
            return Some(());
        }
        let carried: Vec<usize> = unknown
            .iter()
            .copied()
            .filter(|&c| matches!(children[c], RemapChild::Derived))
            .collect();
        let candidates = if carried.is_empty() { &unknown } else { &carried };
        let derived = if candidates.len() == 1 {
            candidates[0]
        } else {
            *candidates.iter().max_by_key(|&&c| {
                let (lo, hi) = map.range_bounds(n0 + c);
                let progress = match &children[c] {
                    RemapChild::Partial(RecountChild::Scan { covered, .. }) => *covered,
                    _ => 0,
                };
                hint(lo, hi).saturating_sub(progress)
            })?
        };
        let children = children
            .into_iter()
            .enumerate()
            .map(|(c, child)| match child {
                _ if c == derived => RecountChild::Derived,
                RemapChild::Known(n) => RecountChild::Scan { after: None, covered: n, done: true },
                RemapChild::Partial(partial) => partial,
                RemapChild::Derived | RemapChild::Fresh => RECOUNT_FRESH,
            })
            .collect();
        let (lo, _) = map.range_bounds(n0);
        let (_, hi) = map.range_bounds(n1 - 1);
        out.spans.push(SplitRecount {
            lo: lo.map(Box::from),
            hi: hi.map(Box::from),
            cuts: new_cuts(map, n0, n1),
            children,
            total,
        });
        out.created += 1;
        out.counts.extend(std::iter::repeat_n(0, n1 - n0));
        Some(())
    }
}

enum SeedRanges {
    Skip,
    Publish(u64, Vec<u64>),
    Keep,
    Remap(u64, Remap),
}

fn new_cuts(map: &RangeMap, n0: usize, n1: usize) -> Vec<Box<[u8]>> {
    map.splits()[n0..n1 - 1].iter().map(|cut| Box::from(cut.as_slice())).collect()
}

#[cfg(not(test))]
pub(super) const SPLIT_RECOUNT_PAGE: usize = 1024;
#[cfg(test)]
pub(super) const SPLIT_RECOUNT_PAGE: usize = 64;
pub(super) const SPLIT_RECOUNT_TURN_BUDGET: std::time::Duration = std::time::Duration::from_millis(1);
const SPLIT_RECOUNT_HINT_MEMTABLE: usize = 1024;

const TOMBSTONE_GC_MAX_INTERVAL_MS: u64 = 3_600_000;

fn gc_cutoff_version(cutoff_ms: u64) -> u128 {
    pack_version(Version {
        hlc: cutoff_ms << crate::hlc::COUNTER_BITS,
        node: 0,
    })
}
#[cfg(not(test))]
pub(super) const RECOUNT_PREFETCH_WINDOW_BYTES: usize = 64 * 1024;
#[cfg(test)]
pub(super) const RECOUNT_PREFETCH_WINDOW_BYTES: usize = 1;
pub(super) const RECOUNT_PREFETCH_ATTEMPTS: u32 = 4;

type RecountSpan = (Option<Box<[u8]>>, Option<Box<[u8]>>);

pub(crate) struct RecountPrefetchPlan {
    span: RecountSpan,
    child: usize,
    plan: vals_store::WinnerCountPlan,
}

impl RecountPrefetchPlan {
    pub(crate) fn block_reads(&self) -> usize {
        self.plan.block_reads()
    }

    pub(crate) async fn read<D: BlockDevice + AsyncRead>(self, dev: &D) -> RecountPrefetched {
        RecountPrefetched {
            span: self.span,
            child: self.child,
            blocks: self.plan.read(dev).await,
        }
    }
}

pub(crate) struct RecountPrefetched {
    span: RecountSpan,
    child: usize,
    blocks: vals_store::WinnerCountBlocks,
}

impl<D: BlockDevice> VersionedStore<D> {
    /// Wraps a freshly created engine as replica `node`'s store.
    pub fn new(engine: Engine<D>, node: u32) -> Result<Self, EngineError> {
        Ok(Self::lazy(engine, node))
    }

    pub(super) fn lazy(engine: Engine<D>, node: u32) -> Self {
        let vi = VersionIndex::new();
        debug_assert!(engine.run_count() != 0 || engine.range_tombstones().is_empty(), "range tombstones imply a fence run");
        let high_ms = if engine.run_count() == 0 {
            vi.seed_pass(engine.index_versions(), |_| Vec::new())
        } else {
            let mut h = 0u64;
            for (_handle, raw) in engine.index_versions() {
                h = h.max(unpack_version(raw).physical_ms());
            }
            h
        };
        let seed_recovery = std::cell::Cell::new(!vi.seeded.get());
        VersionedStore {
            engine,
            hlc: Hlc::new(node),
            gc_grace_ms: 0,
            durable_gc_watermark_ms: std::cell::Cell::new(0),
            live_admission_cutoff_ms: std::cell::Cell::new(0),
            last_tombstone_gc_ms: None,
            applied_high_ms: std::cell::Cell::new(high_ms),
            versions: vi,
            node,
            placement: std::cell::RefCell::new(None),
            placement_ranges: std::cell::RefCell::new(None),
            placement_generation: std::cell::Cell::new(0),
            seed_placement: std::cell::RefCell::new(None),
            regroup: std::cell::RefCell::new(None),
            rt_accounting: std::cell::RefCell::new(Default::default()),
            #[cfg(test)]
            regroup_knockout: std::cell::Cell::new(RegroupKnockout::None),
            logical_range_counts: std::cell::RefCell::new(None),
            #[cfg(test)]
            split_recount_disabled: std::cell::Cell::new(false),
            #[cfg(test)]
            split_recount_visited: std::cell::Cell::new(0),
            #[cfg(test)]
            split_recount_fallbacks: std::cell::Cell::new(0),
            pending_forget: std::collections::HashMap::new(),
            accumulating_forget: std::collections::HashMap::new(),
            pending_released: Default::default(),
            accumulating_released: vals_store::range_tombstone::FragmentSet::with_max_fragments(usize::MAX),
            cleanup_placement: Default::default(),
            accumulating_forget_placement: None,
            pending_forget_placement: None,
            accumulating_forget_bytes: 0,
            accumulating_forget_wrapped: false,
            #[cfg(test)]
            forget_batch_cap: super::maintenance::FORGET_BATCH_CAP_BYTES,
            seed_progress: std::cell::RefCell::new(None),
            stream_seed: std::cell::RefCell::new(None),
            seed_restart_count: std::cell::Cell::new(0),
            seed_started: std::cell::Cell::new(None),
            seed_deferred: std::cell::Cell::new(false),
            seed_driver: std::cell::Cell::new(false),
            seed_recovery,
            pinned_reported: std::cell::Cell::new(0),
            seed_pin_ticket: std::cell::Cell::new(0),
            #[cfg(test)]
            seed_generation_resets: std::cell::Cell::new(0),
            #[cfg(test)]
            seed_run_causes: std::cell::Cell::new([0; 4]),
            #[cfg(test)]
            seed_folds: std::cell::Cell::new(0),
            #[cfg(test)]
            seed_spills: std::cell::Cell::new(0),
        }
    }

    pub(super) fn ensure_seeded(&self) -> Result<(), EngineError> {
        if self.versions.seeded.get() {
            return Ok(());
        }
        while !self.seed_step_stream(false, true)? {}
        Ok(())
    }

    pub fn is_leaf_seeded(&self) -> bool {
        self.versions.seeded.get()
    }

    pub(crate) fn set_seed_driver(&self, attached: bool) {
        self.seed_driver.set(attached);
    }

    #[cfg(test)]
    pub(crate) fn seed_driver_attached(&self) -> bool {
        self.seed_driver.get()
    }

    pub(crate) fn seed_not_ready_for_caller(&self) -> bool {
        self.seed_driver.get() && !self.versions.seeded.get() && seed_policy::isolation()
    }

    pub(crate) fn regroup_not_ready_for_caller(&self) -> bool {
        self.seed_driver.get() && self.regroup_pending() && seed_policy::isolation()
    }

    pub(crate) fn regroup_pending(&self) -> bool {
        self.regroup.borrow().is_some()
    }

    pub(crate) fn recovery_seed_pending(&self) -> bool {
        self.seed_recovery.get() && !self.versions.seeded.get()
    }

    pub(crate) fn seed_restarts(&self) -> u32 {
        self.seed_restart_count.get()
    }

    #[cfg(test)]
    pub(crate) fn set_seed_restarts_for_test(&self, restarts: u32) {
        self.seed_restart_count.set(restarts);
    }

    #[cfg(test)]
    pub(crate) fn seed_run_causes(&self) -> [u64; 4] {
        self.seed_run_causes.get()
    }

    #[cfg(test)]
    fn note_seed_run_cause(&self, cause: usize) {
        let mut causes = self.seed_run_causes.get();
        causes[cause] += 1;
        self.seed_run_causes.set(causes);
    }

    pub(crate) fn sync_pinned_bytes(&self) {
        let now = self.engine.pinned_retired_bytes();
        seed_policy::adjust_pinned_node(self.pinned_reported.replace(now), now);
        if self.seed_pin_ticket.get() != 0 {
            seed_policy::note_pin_held(self.seed_pin_ticket.get(), self.engine.held_retired_bytes());
        }
    }

    fn pinned_node_view(&self) -> u64 {
        self.sync_pinned_bytes();
        let unheld = self.engine.pinned_retired_bytes().saturating_sub(self.engine.held_retired_bytes());
        seed_policy::pinned_node_bytes().saturating_sub(unheld)
    }

    pub(crate) fn reap_seed_pins(&mut self) -> usize {
        let reaped = if self.engine.run_pins_reapable() { self.engine.reap_run_pins() } else { 0 };
        crate::observe::counters().ae_seed_pin_reaped_segments.add(reaped as u64);
        self.sync_pinned_bytes();
        reaped
    }

    #[cfg(test)]
    pub(crate) fn seed_test_counts(&self) -> (u64, u64, u64) {
        (self.seed_generation_resets.get(), self.seed_folds.get(), self.seed_spills.get())
    }

    #[cfg(test)]
    pub(crate) fn seed_journal_bytes(&self) -> u64 {
        self.versions.journal_bytes()
    }

    pub(super) fn reset_seed_progress(&self, end: SeedEnd) {
        if self.stream_seed.borrow_mut().take().is_some() {
            self.note_seed_end(end);
        }
        *self.seed_progress.borrow_mut() = None;
        self.versions.seeding.set(false);
        self.versions.clear_journal();
        self.reset_seed_accounting();
    }

    pub(super) fn note_seed_end(&self, end: SeedEnd) {
        let c = crate::observe::counters();
        match end {
            SeedEnd::Restart => c.ae_seed_restarts.inc(),
            SeedEnd::PlacementReset => c.ae_seed_placement_resets.inc(),
            SeedEnd::GenerationReset => c.ae_seed_generation_resets.inc(),
            SeedEnd::RamShed => c.ae_seed_ram_sheds.inc(),
            SeedEnd::CorruptSidecar => c.ae_seed_corrupt_sidecars.inc(),
            SeedEnd::Error => c.ae_seed_error_resets.inc(),
            SeedEnd::Dropped => c.ae_seed_dropped.inc(),
        }
    }

    pub fn seed_step(&self) -> Result<bool, EngineError> {
        self.seed_deferred.set(false);
        self.seed_step_stream(true, false)
    }

    pub(crate) fn seed_step_cooperative(&self) -> Result<bool, EngineError> {
        self.seed_deferred.set(false);
        self.seed_step_stream(false, false)
    }

    pub(crate) fn seed_deferred(&self) -> bool {
        self.seed_deferred.get()
    }

    fn seed_step_stream(&self, allow_sync_fallback: bool, ignore_ram: bool) -> Result<bool, EngineError> {
        if self.versions.seeded.get() {
            if self.stream_seed.borrow_mut().take().is_some() {
                self.note_seed_end(SeedEnd::Dropped);
            }
            *self.seed_progress.borrow_mut() = None;
            self.reset_seed_accounting();
            return Ok(true);
        }
        if !ignore_ram && self.seed_progress.borrow().as_ref().is_some_and(|sp| sp.reservation_failed) {
            self.reset_seed_progress(SeedEnd::RamShed);
            crate::replica::seed_permit::shed();
            crate::replica::seed_permit::trim_after_seed();
            self.seed_deferred.set(true);
            return Ok(false);
        }
        if !ignore_ram && crate::ram::ram_over_budget() {
            if self.stream_seed.borrow().is_some() {
                self.reset_seed_progress(SeedEnd::RamShed);
                crate::replica::seed_permit::shed();
                crate::replica::seed_permit::trim_after_seed();
            }
            self.seed_deferred.set(true);
            return Ok(false);
        }
        let mut progress = self.seed_progress.borrow_mut();
        let mut stream = self.stream_seed.borrow_mut();
        macro_rules! shed_reserved_seed {
            () => {{
                drop(stream);
                drop(progress);
                self.reset_seed_progress(SeedEnd::RamShed);
                crate::replica::seed_permit::shed();
                crate::replica::seed_permit::trim_after_seed();
                self.seed_deferred.set(true);
                return Ok(false);
            }};
        }
        let permit = if stream.is_none() && !ignore_ram {
            let acquired = if self.seed_driver.get() && seed_policy::isolation() {
                crate::replica::seed_permit::SeedPermit::try_acquire_driver()
            } else {
                crate::replica::seed_permit::SeedPermit::try_acquire()
            };
            match acquired {
                Some(permit) => Some(permit),
                None => {
                    self.seed_deferred.set(true);
                    return Ok(false);
                }
            }
        } else { None };
        let reservation = if stream.is_none() {
            let estimate = self.engine.memtable_bytes().saturating_mul(2)
                .saturating_add(2 * 1024 * 1024);
            if ignore_ram {
                Some(crate::ram_broker::reserve_overdraw(crate::ram_broker::Class::Seed, estimate))
            } else {
                match crate::ram_broker::try_reserve(crate::ram_broker::Class::Seed, self as *const Self as usize, estimate) {
                    Some(reservation) => Some(reservation),
                    None => { self.seed_deferred.set(true); return Ok(false); }
                }
            }
        } else { None };
        crate::observe::counters().ae_seed_steps.inc();
        if stream.is_none() {
            crate::observe::counters().ae_seed_starts.inc();
            if self.seed_started.get().is_none() {
                self.seed_started.set(Some(std::time::Instant::now()));
            }
            let mut mem_pending = self.engine.index_versions();
            mem_pending.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            let mem_pending_bytes = (mem_pending.capacity()
                * std::mem::size_of::<(Box<[u8]>, u128)>()) as u64
                + mem_pending.iter().map(|(key, _)| key.len() as u64 + 16).sum::<u64>();
            let mut reservation = reservation.unwrap();
            if mem_pending_bytes > reservation.bytes() {
                let extra = mem_pending_bytes - reservation.bytes();
                if ignore_ram { reservation.overdraw_grow(extra); }
                else if !reservation.try_grow(extra) {
                    drop(mem_pending);
                    shed_reserved_seed!();
                }
            }
            let range = self.logical_range_seed();
            let logical_range_generation = range.as_ref().map(|(generation, _)| *generation);
            let logical_range_counts = range.as_ref().map_or_else(Vec::new, |(_, map)| vec![0; map.num_ranges()]);
            let mut sp = SeedProgress {
                _permit: permit,
                reservation_floor: reservation.bytes(),
                reservation, reservation_failed: false,
                force_overdraw: ignore_ram,
                mem_pending, mem_pending_bytes,
                projection_generation: self.placement_generation.get(),
                logical_range_generation, logical_range_counts,
                range_map: range.map(|(_, map)| map), spill: None,
                journal_pos: 0, staging: std::collections::HashMap::new(),
                bucket_counts: vec![0; INDEX_BUCKETS as usize].into_boxed_slice(),
                bucket_dirty: vec![None; INDEX_BUCKETS as usize].into_boxed_slice(),
                live: 0, high_ms: 0, estimated_bytes: 0,
                cover: self.start_seed_accounting(),
            };
            sp.adjust_estimate(0, mem_pending_bytes);
            let node = self.pinned_node_view();
            let pinned = self.engine.pin_runs(seed_policy::pin_admits(node));
            let ticket = pinned.pin.as_ref().map(|_| seed_policy::pin_ticket());
            self.seed_pin_ticket.set(ticket.as_ref().map_or(0, |t| t.id()));
            if let Some(t) = &ticket {
                self.engine.set_run_pin_limit(seed_policy::pin_limit(t.id(), 0, node).commit);
            }
            *stream = Some(StreamSeed {
                cursors: Vec::new(), heads: std::collections::BinaryHeap::new(), refill: vec![0],
                finished: vec![false], pending: pinned.sources,
                pin: pinned.pin, ticket, mem_pos: 0, runs_epoch: self.engine.runs_removed_epoch(), windows_bytes: 0,
                head_bytes: 0, publish_ranges: true,
            });
            *progress = Some(sp);
            self.versions.seeding.set(true);
            self.versions.clear_journal();
            return Ok(false);
        }
        let sp = progress.as_mut().unwrap();
        if ignore_ram { sp.force_overdraw = true; }
        let ss = stream.as_mut().unwrap();
        self.sync_pinned_bytes();
        if let Some(pin) = &ss.pin {
            let state = self.engine.run_pin_state(pin);
            let revoked = state == vals_store::engine::RunPinState::Revoked;
            let mut over_cap = state == vals_store::engine::RunPinState::OverLimit;
            if state == vals_store::engine::RunPinState::Valid {
                let held = self.engine.held_retired_bytes();
                let ticket = ss.ticket.as_ref().map_or(0, |t| t.id());
                let limit = seed_policy::pin_limit(ticket, held, self.pinned_node_view());
                self.engine.set_run_pin_limit(limit.commit);
                if limit.exempt_over_cap && held > 0 {
                    crate::observe::counters().ae_seed_pin_cap_exempt_steps.inc();
                }
                over_cap = held > 0 && limit.step.is_some_and(|bytes| held > bytes);
            }
            if revoked || over_cap {
                let c = crate::observe::counters();
                if revoked {
                    c.ae_seed_pin_revocations.inc();
                } else {
                    c.ae_seed_pin_cap_fallbacks.inc();
                }
                #[cfg(test)]
                self.note_seed_run_cause(if revoked { 3 } else { 2 });
                drop(stream);
                drop(progress);
                return self.restart_or_fallback(allow_sync_fallback);
            }
        }
        if self.engine.runs_removed_epoch() != ss.runs_epoch {
            if ss.pin.is_some() {
                let epoch = self.engine.runs_removed_epoch();
                crate::observe::counters().ae_seed_merge_carries.add(epoch.wrapping_sub(ss.runs_epoch));
                ss.runs_epoch = epoch;
                #[cfg(test)]
                self.note_seed_run_cause(0);
            } else {
                crate::observe::counters().ae_seed_merge_restarts.inc();
                #[cfg(test)]
                self.note_seed_run_cause(1);
                drop(stream);
                drop(progress);
                return self.restart_or_fallback(allow_sync_fallback);
            }
        }
        if sp.projection_generation != self.placement_generation.get() {
            crate::observe::counters().ae_seed_projection_retries.inc();
            drop(stream);
            drop(progress);
            self.reset_seed_progress(SeedEnd::PlacementReset);
            return Ok(false);
        }
        let range_generation = self.logical_range_generation();
        if sp.logical_range_generation != range_generation
            && !self.seed_carries_ranges(sp, range_generation)
        {
            drop(stream);
            drop(progress);
            self.note_seed_generation_reset();
            return Ok(false);
        }
        if let Some(handle) = ss.pending.pop() {
            let window = vals_store::run::SIDECAR_CURSOR_WINDOW as u64;
            if !sp.try_adjust_estimate(0, window) { shed_reserved_seed!(); }
            match handle.version_stream().map_err(EngineError::from) {
                Ok(Some(cursor)) => {
                    let bytes = cursor.resident_bound_bytes();
                    ss.windows_bytes += bytes;
                    sp.adjust_estimate(window, bytes);
                    ss.cursors.push(SeedCursor::Stream(cursor));
                    ss.refill.push(ss.cursors.len());
                    ss.finished.push(false);
                    return Ok(false);
                }
                Ok(None) => {
                    let materialized = (|| -> Result<Option<SeedCursor<D>>, EngineError> {
                        let mut sidecar = handle.sidecar_versions()?;
                        let body_bytes = sidecar.body_len() as u64;
                        let capacity = sidecar.body_len() / 20;
                        let estimate = body_bytes.saturating_mul(2)
                            .saturating_add((capacity * std::mem::size_of::<(Vec<u8>, u128)>()) as u64);
                        if !sp.try_adjust_estimate(window, estimate) { return Ok(None); }
                        let mut entries = Vec::with_capacity(capacity);
                        sidecar.fold_page(usize::MAX, |key, raw| entries.push((key.to_vec(), raw)))?;
                        drop(sidecar);
                        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                        let bytes = (entries.capacity() * std::mem::size_of::<(Vec<u8>, u128)>()) as u64
                            + entries.iter().map(|(key, _)| key.capacity() as u64).sum::<u64>();
                        if !sp.try_adjust_estimate(estimate, bytes) { return Ok(None); }
                        Ok(Some(SeedCursor::Mem { entries, pos: 0, bytes }))
                    })();
                    match materialized {
                        Ok(Some(cursor)) => {
                            ss.windows_bytes += cursor.resident_bound_bytes();
                            ss.cursors.push(cursor);
                            ss.refill.push(ss.cursors.len());
                            ss.finished.push(false);
                            return Ok(false);
                        }
                        Ok(None) if sp.reservation_failed => { shed_reserved_seed!(); }
                        Ok(None) => {
                            drop(stream);
                            drop(progress);
                            return self.restart_or_fallback(allow_sync_fallback);
                        }
                        Err(e) => {
                            let corrupt = matches!(e, EngineError::Snapshot(vals_store::SnapshotError::Corrupt(_)));
                            drop(stream);
                            drop(progress);
                            self.reset_seed_progress(if corrupt { SeedEnd::CorruptSidecar } else { SeedEnd::Error });
                            return Err(e);
                        }
                    }
                }
                Err(e) => {
                    let corrupt = matches!(e, EngineError::Snapshot(vals_store::SnapshotError::Corrupt(_)));
                    drop(stream);
                    drop(progress);
                    self.reset_seed_progress(if corrupt { SeedEnd::CorruptSidecar } else { SeedEnd::Error });
                    return Err(e);
                }
            }
        }
        let placement_ranges = if self.regroup.borrow().is_none() {
            self.placement_ranges.borrow().clone()
        } else { None };
        let mut group_cache: Option<(usize, Box<[NodeId]>)> = None;
        let mut range_cache: Option<usize> = None;
        let mut projected = 0;
        'merge: while projected < SEED_MEMORY_OPS_PER_STEP {
            while let Some(&slot) = ss.refill.last() {
                if slot == 0 {
                    if let Some(bytes) = sp.mem_pending.get(ss.mem_pos).map(|(key, _)| key.len() as u64) {
                        if !sp.try_adjust_estimate(0, bytes) { shed_reserved_seed!(); }
                        let (key, raw) = &sp.mem_pending[ss.mem_pos];
                        let (key, raw) = (key.to_vec(), *raw);
                        ss.head_bytes += key.capacity() as u64;
                        sp.adjust_estimate(bytes, key.capacity() as u64);
                        ss.heads.push(std::cmp::Reverse((key, 0, raw)));
                        ss.mem_pos += 1;
                    } else {
                        ss.finished[0] = true;
                        sp.mem_pending = Vec::new();
                        sp.adjust_estimate(sp.mem_pending_bytes, 0);
                        sp.mem_pending_bytes = 0;
                    }
                    ss.refill.pop();
                    continue;
                }
                let i = slot - 1;
                let before = ss.cursors[i].resident_bound_bytes();
                let mut turn_extra = 0u64;
                let step = ss.cursors[i].next_step_reserving(|bytes| {
                    if sp.try_adjust_estimate(0, bytes) {
                        turn_extra = turn_extra.saturating_add(bytes);
                        true
                    } else { false }
                });
                if matches!(step, Ok(None)) { shed_reserved_seed!(); }
                let after = ss.cursors[i].resident_bound_bytes();
                let head_bytes = match &step {
                    Ok(Some(vals_store::run::SidecarCursorStep::Entry(head))) => head.0.capacity() as u64,
                    _ => 0,
                };
                let old = turn_extra.saturating_add(before.saturating_sub(after));
                let new = after.saturating_sub(before).saturating_add(head_bytes);
                if !sp.try_adjust_estimate(old, new) { shed_reserved_seed!(); }
                ss.windows_bytes = ss.windows_bytes.saturating_sub(before).saturating_add(after);
                match step {
                    Ok(Some(vals_store::run::SidecarCursorStep::Entry((key, raw)))) => {
                        ss.head_bytes += head_bytes;
                        ss.heads.push(std::cmp::Reverse((key, slot, raw)));
                        ss.refill.pop();
                    }
                    Ok(Some(vals_store::run::SidecarCursorStep::Done)) => {
                        ss.finished[slot] = true;
                        ss.refill.pop();
                    }
                    Ok(Some(vals_store::run::SidecarCursorStep::Checkpoint)) => break 'merge,
                    Ok(None) => unreachable!(),
                    Err(e) => {
                        let corrupt = matches!(e, vals_store::SnapshotError::Corrupt(_));
                        drop(stream);
                        drop(progress);
                        self.reset_seed_progress(if corrupt { SeedEnd::CorruptSidecar } else { SeedEnd::Error });
                        return Err(e.into());
                    }
                }
            }
            let Some(std::cmp::Reverse((key, slot, mut raw))) = ss.heads.pop() else { break };
            let key_bytes = key.capacity() as u64;
            ss.head_bytes -= key_bytes;
            ss.refill.push(slot);
            while ss.heads.peek().is_some_and(|std::cmp::Reverse((head, _, _))| *head == key) {
                let std::cmp::Reverse((head, slot, head_raw)) = ss.heads.pop().unwrap();
                ss.head_bytes -= head.capacity() as u64;
                sp.adjust_estimate(head.capacity() as u64, 0);
                raw = raw.max(head_raw);
                ss.refill.push(slot);
            }
            sp.high_ms = sp.high_ms.max(unpack_version(raw).physical_ms());
            if sp.cover.as_deref().is_some_and(|cover| rt_accounting::covers(cover, &key, raw)) {
                drop(key);
                sp.adjust_estimate(key_bytes, 0);
                projected += 1;
                continue;
            }
            let b = VersionIndex::slot(&key);
            let uncached;
            let group: &[NodeId] = match &placement_ranges {
                Some(map) => {
                    if !group_cache.as_ref().is_some_and(|(index, _)| range_holds(map, *index, &key)) {
                        group_cache = Some((map.range_index(&key), self.group_of(&key).into_boxed_slice()));
                    }
                    &group_cache.as_ref().unwrap().1
                }
                None => {
                    uncached = self.group_of(&key);
                    &uncached
                }
            };
            let Some(leaves) = sp.try_group_mut(group) else { shed_reserved_seed!(); };
            leaves[b] ^= crate::antientropy::entry_digest(&key, unpack_version(raw));
            sp.bucket_counts[b] += 1;
            sp.live += 1;
            if sp.logical_range_generation.is_some() {
                if let Some(map) = &sp.range_map {
                    let index = match range_cache {
                        Some(index) if range_holds(map, index, &key) => index,
                        _ => map.range_index(&key),
                    };
                    range_cache = Some(index);
                    sp.logical_range_counts[index] += 1;
                }
            }
            drop(key);
            sp.adjust_estimate(key_bytes, 0);
            projected += 1;
        }
        crate::observe::counters().ae_seed_fold_ops.add(projected as u64);
        crate::observe::counters().ae_seed_projection_ops.add(projected as u64);
        #[cfg(test)]
        self.seed_folds.set(self.seed_folds.get() + projected as u64);
        if !ss.finished.iter().all(|done| *done) { return Ok(false); }

        let journal_len = self.versions.journal.borrow().len();
        let end = (sp.journal_pos + SEED_MEMORY_OPS_PER_STEP).min(journal_len);
        for i in sp.journal_pos..end {
            let op = self.versions.take_journal(i);
            crate::observe::counters().ae_seed_journal_ops.inc();
            match op {
                LeafOp::Apply { key, old, new } => {
                    let b = VersionIndex::slot(&key);
                    match old {
                        OldWinner::Unknown => {
                            sp.bucket_dirty[b].get_or_insert_with(std::time::Instant::now);
                            ss.publish_ranges = false;
                        }
                        OldWinner::Exact(previous) if sp.bucket_dirty[b].is_none() => {
                            let digest = crate::antientropy::entry_digest(&key, new);
                            let group = self.group_of(&key);
                            let Some(leaves) = sp.try_group_mut(&group) else { shed_reserved_seed!(); };
                            if let Some(previous) = previous {
                                leaves[b] ^= crate::antientropy::entry_digest(&key, previous);
                            } else {
                                sp.bucket_counts[b] += 1;
                                sp.live += 1;
                            }
                            let group = self.group_of(&key);
                            let Some(leaves) = sp.try_group_mut(&group) else { shed_reserved_seed!(); };
                            leaves[b] ^= digest;
                        }
                        OldWinner::Exact(_) => {}
                    }
                    if matches!(old, OldWinner::Exact(None)) && sp.logical_range_generation.is_some() {
                        if let Some(map) = &sp.range_map {
                            sp.logical_range_counts[map.range_index(&key)] += 1;
                        }
                    }
                    sp.high_ms = sp.high_ms.max(new.physical_ms());
                }
                LeafOp::Retire { key, version } => {
                    let b = VersionIndex::slot(&key);
                    if sp.bucket_dirty[b].is_none() {
                        let group = self.group_of(&key);
                        let Some(leaves) = sp.try_group_mut(&group) else { shed_reserved_seed!(); };
                        leaves[b] ^= crate::antientropy::entry_digest(&key, version);
                        sp.bucket_counts[b] = sp.bucket_counts[b].saturating_sub(1);
                        sp.live = sp.live.saturating_sub(1);
                    }
                    if sp.logical_range_generation.is_some() {
                        if let Some(map) = &sp.range_map {
                            let index = map.range_index(&key);
                            sp.logical_range_counts[index] = sp.logical_range_counts[index].saturating_sub(1);
                        }
                    }
                }
            }
        }
        sp.journal_pos = end;
        self.versions.sync_journal_bytes(false);
        if end != journal_len || sp.journal_pos != self.versions.journal.borrow().len() {
            return Ok(false);
        }
        if sp.projection_generation != self.placement_generation.get() {
            return Ok(false);
        }
        if let Some(spill) = sp.spill.take() {
            for (count, delta) in sp.bucket_counts.iter_mut().zip(spill.buckets.iter()) {
                *count = apply_delta(*count, *delta);
            }
            sp.live = apply_delta(sp.live, spill.live);
            for (count, delta) in sp.logical_range_counts.iter_mut().zip(&spill.ranges) {
                *count = apply_delta(*count, *delta);
            }
            if spill.ranges_unknown {
                ss.publish_ranges = false;
            }
            sp.adjust_estimate(spill.bytes(), 0);
        }
        if !ss.publish_ranges {
            sp.logical_range_generation = None;
        }
        debug_assert_eq!(ss.head_bytes, 0);
        sp.adjust_estimate(ss.windows_bytes, 0);
        if !ignore_ram && sp.reservation_failed {
            drop(stream);
            drop(progress);
            self.reset_seed_progress(SeedEnd::RamShed);
            crate::replica::seed_permit::shed();
            crate::replica::seed_permit::trim_after_seed();
            self.seed_deferred.set(true);
            return Ok(false);
        }
        let (accounted, accounted_seq, accounting_changed) = self.seed_accounted_set();
        let Some(ranges) = self.resolve_seed_ranges(sp, accounting_changed) else {
            drop(stream);
            drop(progress);
            self.note_seed_generation_reset();
            return Ok(false);
        };
        *stream = None;
        let mut sp = progress.take().unwrap();
        drop(progress);
        drop(stream);
        self.publish_accounted(accounted, accounted_seq);
        self.publish_seed(&mut sp, ranges);
        self.versions.clear_journal();
        drop(sp);
        crate::replica::seed_permit::trim_after_seed();
        Ok(true)
    }

    pub(super) fn restart_or_fallback(&self, allow_sync_fallback: bool) -> Result<bool, EngineError> {
        self.reset_seed_progress(SeedEnd::Restart);
        let n = self.seed_restart_count.get().saturating_add(1);
        self.seed_restart_count.set(n);
        if allow_sync_fallback && n > MAX_SEED_RESTARTS {
            log::warn!(
                "cooperative Merkle-leaf seed restarted {n} times (run churn outpacing the \
                 cooperative seed); falling back to the one-shot synchronous ensure_seeded — \
                 accepting a serving stall to guarantee forward progress"
            );
            crate::observe::counters().ae_seed_sync_fallbacks.inc();
            self.ensure_seeded()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn seed_carries_ranges(&self, sp: &SeedProgress, current: Option<u64>) -> bool {
        seed_policy::isolation()
            && sp.logical_range_generation.is_some()
            && current.is_some()
            && sp.range_map.is_some()
            && self.engine.ordering() == vals_store::Ordering::Sorted
            && self.split_recount_enabled()
    }

    fn note_seed_generation_reset(&self) {
        crate::observe::counters().ae_seed_projection_retries.inc();
        #[cfg(test)]
        self.seed_generation_resets
            .set(self.seed_generation_resets.get() + 1);
        self.reset_seed_progress(SeedEnd::GenerationReset);
    }

    fn resolve_seed_ranges(&self, sp: &mut SeedProgress, accounting_changed: bool) -> Option<SeedRanges> {
        let Some(generation) = sp.logical_range_generation else {
            return Some(SeedRanges::Skip);
        };
        let counts = std::mem::take(&mut sp.logical_range_counts);
        let accounting = self.logical_range_counts.borrow();
        let Some(state) = accounting.as_ref() else {
            return Some(SeedRanges::Skip);
        };
        if state.generation == generation {
            return Some(SeedRanges::Publish(generation, counts));
        }
        if state.counts.is_some() && !accounting_changed {
            return Some(SeedRanges::Keep);
        }
        let seeded = LogicalRangeCounts {
            epoch: 0,
            map: sp.range_map.take()?,
            counts: Some(counts),
            generation,
            recounts: Vec::new(),
        };
        let hint = |lo: Option<&[u8]>, hi: Option<&[u8]>| {
            self.engine.range_size_hint(lo, hi, SPLIT_RECOUNT_HINT_MEMTABLE)
        };
        seeded
            .remap(&state.map, &hint)
            .map(|remap| SeedRanges::Remap(state.generation, remap))
    }

    fn publish_seed_ranges(&self, ranges: SeedRanges) {
        match ranges {
            SeedRanges::Skip => {}
            SeedRanges::Publish(generation, counts) => {
                self.publish_logical_range_seed(generation, counts);
            }
            SeedRanges::Keep => {
                crate::observe::counters().ae_seed_generation_carries.inc();
            }
            SeedRanges::Remap(generation, remap) => {
                let c = crate::observe::counters();
                c.ae_seed_generation_carries.inc();
                c.logical_range_split_recounts.add(remap.created);
                c.logical_range_split_empty.add(remap.empty);
                c.logical_range_merge_sums.add(remap.merges);
                c.logical_range_split_recount_repartitions.add(remap.repartitions);
                let mut accounting = self.logical_range_counts.borrow_mut();
                if let Some(state) = accounting.as_mut().filter(|state| state.generation == generation) {
                    state.counts = Some(remap.counts);
                    state.recounts = remap.spans;
                }
            }
        }
    }

    fn publish_seed(&self, sp: &mut SeedProgress, ranges: SeedRanges) {
        sp.release_staging_estimate();
        *self.versions.leaves.borrow_mut() = std::mem::take(&mut sp.staging);
        let buckets: Vec<BucketMeta> = sp
            .bucket_counts
            .iter()
            .map(|&logical_keys| BucketMeta {
                generation: 0,
                logical_keys,
            })
            .collect();
        *self.versions.buckets.borrow_mut() = buckets.into_boxed_slice();
        *self.versions.dirty.borrow_mut() = sp
            .bucket_dirty
            .iter()
            .enumerate()
            .filter_map(|(slot, &since)| since.map(|since| (slot, since)))
            .collect();
        self.versions.live.set(sp.live);
        self.publish_seed_ranges(ranges);
        self.versions.seeded.set(true);
        self.versions.seeding.set(false);
        self.versions.refresh_dirty_gauges();
        if sp.high_ms > self.applied_high_ms.get() {
            self.applied_high_ms.set(sp.high_ms);
        }
        self.seed_restart_count.set(0);
        self.seed_recovery.set(false);
        crate::observe::counters().ae_seed_completions.inc();
        if let Some(started) = self.seed_started.take() {
            let elapsed = started.elapsed();
            crate::observe::counters()
                .ae_seed_wall_us
                .add(elapsed.as_micros() as u64);
            crate::observe::histograms()
                .ae_seed_duration_ms
                .observe_us(elapsed.as_millis() as u64);
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.node
    }

    pub(super) fn group_of(&self, key: &[u8]) -> Vec<NodeId> {
        let regroup = self.regroup.borrow();
        let old = regroup.as_ref().filter(|r| self.routes_old(r, key)).map(|r| &r.old);
        match old.or(self.placement.borrow().as_ref()) {
            Some(f) => {
                let mut g = f(key);
                g.sort_unstable();
                g
            }
            None => Vec::new(),
        }
    }

    pub(crate) fn prepare_zone_seed(
        &self,
        epochs: (u64, u64, u64),
        ring: &crate::ring::Ring,
        zone_id: crate::zone::ZoneId,
        config: Option<crate::zone::ZoneConfig>,
        range_map: Option<RangeMap>,
    ) {
        let (ring_epoch, catalog_epoch, range_epoch) = epochs;
        if let Some(map) = &range_map {
            self.set_logical_range_map(range_epoch, map.clone());
        }
        let inputs = SeedPlacementInputs {
            ring_epoch,
            catalog_epoch,
            zone_id,
            config: config.clone(),
            range_map: range_map.clone(),
            rf: ring.rf(),
            vnodes: ring.vnodes(),
            members: ring.members_weighted().collect(),
        };
        let mut local_reuse = false;
        let mut regroup_spans = None;
        {
            let mut previous = self.seed_placement.borrow_mut();
            if let Some(previous) = previous.as_mut() {
                if previous.inputs == inputs
                    && (previous.range_epoch == range_epoch
                        || (previous.range_epoch != 0 && range_epoch > previous.range_epoch))
                {
                    if previous.range_epoch != range_epoch {
                        crate::observe::counters().ae_seed_epoch_suppressions.inc();
                    }
                    previous.range_epoch = range_epoch;
                    return;
                }
                if previous.range_epoch != 0
                    && range_epoch > previous.range_epoch
                    && placement_equivalent(&previous.inputs, &inputs, ring)
                {
                    previous.range_epoch = range_epoch;
                    previous.inputs = inputs;
                    crate::observe::counters().ae_seed_placement_reuses.inc();
                    return;
                }
                if previous.range_epoch != 0 && range_epoch > previous.range_epoch {
                    if let Some(spans) = changed_placement_spans(&previous.inputs, &inputs, ring) {
                        local_reuse = spans.iter().all(|(lo, hi)| {
                            self.engine
                                .winner_count_page(lo.as_deref(), hi.as_deref(), 1, None)
                                .is_ok_and(|(n, _, _)| n == 0)
                        });
                        regroup_spans = Some(spans);
                    }
                }
            }
        }
        let old = self.placement.borrow().clone();
        let ring = ring.clone();
        let mut ranges = None;
        let placement: PlacementFn = match config {
            None => std::sync::Arc::new(move |key: &[u8]| ring.replicas(key)),
            Some(cfg) => {
                let range_map = range_map.map(std::sync::Arc::new);
                ranges = range_map.clone();
                std::sync::Arc::new(move |key: &[u8]| match &range_map {
                    Some(map) => map.replicas_for_key_while_rehoming(
                        &ring, zone_id, key, cfg.rf, cfg.scope.as_ref(),
                    ),
                    None => ring.replicas_in_zone(zone_id, key, cfg.rf, cfg.scope.as_ref()),
                })
            }
        };
        self.install_placement(placement);
        *self.placement_ranges.borrow_mut() = ranges;
        *self.seed_placement.borrow_mut() = Some(SeedPlacement { range_epoch, inputs });
        if local_reuse {
            crate::observe::counters().ae_seed_placement_local_reuses.inc();
            return;
        }
        if let (Some(spans), Some(old)) = (regroup_spans, old) {
            if self.start_regroup(old, spans) {
                crate::observe::counters().ae_seed_placement_regroups.inc();
                return;
            }
        }
        crate::observe::counters().ae_seed_placement_invalidations.inc();
        self.invalidate_leaf_seed();
    }

    fn start_regroup(&self, current: PlacementFn, spans: Vec<KeySpan>) -> bool {
        if !seed_policy::span_regroup()
            || !self.versions.seeded.get()
            || self.versions.seeding.get()
            || self.seed_progress.borrow().is_some()
        {
            return false;
        }
        let changed = spans.into_iter().map(|(lo, hi)| (lo.unwrap_or_default(), hi));
        let mut regroup = self.regroup.borrow_mut();
        let Some(active) = regroup.as_mut() else {
            *regroup = Some(Regroup { old: current, spans: union_spans(changed.collect()), depth: 0 });
            return true;
        };
        #[cfg(test)]
        if self.regroup_knockout.get() == RegroupKnockout::NoCompose {
            return false;
        }
        if active.depth >= MAX_REGROUP_COMPOSITIONS {
            crate::observe::counters().ae_regroup_depth_fallbacks.inc();
            return false;
        }
        let unscanned = active.spans.iter().map(|span| {
            (span.page_lo().unwrap_or_default(), span.hi.as_deref().map(<[u8]>::to_vec))
        });
        #[cfg(test)]
        let unscanned = unscanned.filter(|_| self.regroup_knockout.get() != RegroupKnockout::ComposeNewSpansOnly);
        let merged = union_spans(unscanned.chain(changed).collect());
        let frozen = std::mem::take(&mut active.spans);
        let old = active.old.clone();
        active.old = std::sync::Arc::new(move |key: &[u8]| {
            if frozen.iter().any(|span| span.routes_old(key)) { old(key) } else { current(key) }
        });
        active.spans = merged;
        active.depth += 1;
        crate::observe::counters().ae_regroup_compositions.inc();
        true
    }

    fn routes_old(&self, regroup: &Regroup, key: &[u8]) -> bool {
        #[cfg(test)]
        if self.regroup_knockout.get() == RegroupKnockout::Misroute {
            return false;
        }
        regroup.spans.iter().any(|span| span.routes_old(key))
    }

    pub(crate) fn regroup_step(&self) -> Result<RecountStep, EngineError> {
        self.regroup_page(None)
            .expect("a synchronous regroup page is never stale")
    }

    #[cfg(test)]
    pub(crate) fn finish_regroup(&self) -> Result<(), EngineError> {
        while self.regroup_step()? != RecountStep::Done {}
        Ok(())
    }

    pub(crate) fn regroup_prefetch_plan(
        &self,
    ) -> Result<Option<vals_store::WinnerCountPlan>, EngineError> {
        let regroup = self.regroup.borrow();
        let Some(span) = regroup.as_ref().and_then(|r| r.spans.first()) else {
            return Ok(None);
        };
        let page_lo = span.page_lo();
        self.engine
            .winner_count_prefetch_plan_bytes(
                page_lo.as_deref(),
                span.hi.as_deref(),
                RECOUNT_PREFETCH_WINDOW_BYTES,
            )
            .map(Some)
    }

    pub(crate) fn regroup_prefetched_step(
        &self,
        blocks: vals_store::WinnerCountBlocks,
    ) -> Option<Result<RecountStep, EngineError>> {
        self.regroup_page(Some(blocks))
    }

    fn regroup_page(
        &self,
        prefetched: Option<vals_store::WinnerCountBlocks>,
    ) -> Option<Result<RecountStep, EngineError>> {
        let started = std::time::Instant::now();
        let c = crate::observe::counters();
        let mut regroup = self.regroup.borrow_mut();
        let Some(state) = regroup.as_mut() else { return Some(Ok(RecountStep::Done)) };
        if let Some(span) = state.spans.first() {
            let page_lo = span.page_lo();
            let hi = span.hi.as_deref();
            let deadline = Some(started + SPLIT_RECOUNT_TURN_BUDGET);
            let sync = || {
                self.engine
                    .winner_version_page(page_lo.as_deref(), hi, SPLIT_RECOUNT_PAGE, deadline)
                    .map(Some)
            };
            let page = match prefetched {
                None => sync(),
                Some(blocks) => match self.engine.winner_version_page_prefetched(
                    page_lo.as_deref(),
                    hi,
                    SPLIT_RECOUNT_PAGE,
                    deadline,
                    blocks,
                ) {
                    Err(_) => {
                        c.ae_regroup_prefetch_errors.inc();
                        sync()
                    }
                    page => page,
                },
            };
            let (entries, more) = match page {
                Ok(Some(page)) => page,
                Ok(None) => return None,
                Err(e) => {
                    drop(regroup);
                    self.invalidate_leaf_seed();
                    return Some(Err(e));
                }
            };
            let Some(current) = self.placement.borrow().clone() else {
                drop(regroup);
                self.invalidate_leaf_seed();
                return Some(Ok(RecountStep::Abandoned));
            };
            let dirty = self.versions.dirty.borrow();
            let mut groups = self.versions.leaves.borrow_mut();
            let mut moves = 0u64;
            for (key, raw) in &entries {
                let b = VersionIndex::slot(key);
                if dirty.contains_key(&b) || self.accounted_covers(key, *raw) {
                    continue;
                }
                let (mut from, mut to) = ((state.old)(key), current(key));
                from.sort_unstable();
                to.sort_unstable();
                if from == to {
                    continue;
                }
                let digest = crate::antientropy::entry_digest(key, unpack_version(*raw));
                debug_assert!(groups.contains_key(from.as_slice()), "a clean winner's digest is in its routed group");
                match groups.get_mut(from.as_slice()) {
                    Some(leaves) => leaves[b] ^= digest,
                    None => groups.entry(from.into_boxed_slice()).or_insert_with(VersionIndex::empty_group)[b] ^= digest,
                }
                match groups.get_mut(to.as_slice()) {
                    Some(leaves) => leaves[b] ^= digest,
                    None => groups.entry(to.into_boxed_slice()).or_insert_with(VersionIndex::empty_group)[b] ^= digest,
                }
                moves += 1;
            }
            drop(groups);
            drop(dirty);
            c.ae_regroup_pages.inc();
            c.ae_regroup_keys.add(entries.len() as u64);
            c.ae_regroup_moves.add(moves);
            match entries.into_iter().next_back() {
                Some((last, _)) if more => state.spans[0].after = Some(last.into_boxed_slice()),
                _ => {
                    state.spans.remove(0);
                }
            }
        }
        if !state.spans.is_empty() {
            return Some(Ok(RecountStep::More));
        }
        *regroup = None;
        self.versions.leaves.borrow_mut().retain(|_, leaves| leaves.iter().any(|&leaf| leaf != 0));
        c.ae_regroup_completions.inc();
        Some(Ok(RecountStep::Done))
    }

    pub fn set_placement(&self, f: PlacementFn) {
        if self.regroup.borrow().is_some() {
            self.invalidate_leaf_seed();
        }
        self.install_placement(f);
    }

    fn install_placement(&self, f: PlacementFn) {
        *self.seed_placement.borrow_mut() = None;
        *self.placement_ranges.borrow_mut() = None;
        *self.placement.borrow_mut() = Some(f);
        self.placement_generation
            .set(self.placement_generation.get().wrapping_add(1));
    }

    pub(crate) fn set_logical_range_map(&self, epoch: u64, map: RangeMap) {
        let mut fallback = false;
        'transition: {
            let mut accounting = self.logical_range_counts.borrow_mut();
            if let Some(state) = accounting.as_mut() {
                if state.map == map {
                    if state.epoch == epoch {
                        return;
                    }
                    if state.epoch != 0 && epoch > state.epoch {
                        state.epoch = epoch;
                        crate::observe::counters().logical_range_epoch_rebases.inc();
                        return;
                    }
                } else if state.epoch != 0
                    && epoch > state.epoch
                    && state.map.splits() == map.splits()
                {
                    state.map = map;
                    state.epoch = epoch;
                    crate::observe::counters().logical_range_boundary_rebases.inc();
                    return;
                }
                if state.map.splits() != map.splits()
                    && state.epoch != 0
                    && epoch > state.epoch
                    && state.counts.is_some()
                    && self.engine.ordering() == vals_store::Ordering::Sorted
                {
                    let hint = |lo: Option<&[u8]>, hi: Option<&[u8]>| {
                        self.engine.range_size_hint(lo, hi, SPLIT_RECOUNT_HINT_MEMTABLE)
                    };
                    let remap = self
                        .split_recount_enabled()
                        .then(|| state.remap(&map, &hint))
                        .flatten();
                    let Some(remap) = remap else {
                        fallback = true;
                        break 'transition;
                    };
                    let c = crate::observe::counters();
                    c.logical_range_split_recounts.add(remap.created);
                    c.logical_range_split_empty.add(remap.empty);
                    c.logical_range_merge_sums.add(remap.merges);
                    c.logical_range_split_recount_repartitions.add(remap.repartitions);
                    state.recounts = remap.spans;
                    state.counts = Some(remap.counts);
                    state.map = map;
                    state.epoch = epoch;
                    state.generation = state.generation.wrapping_add(1);
                    return;
                }
            }
        }
        if fallback {
            self.note_split_recount_fallback();
        }
        crate::observe::counters().logical_range_map_invalidations.inc();
        let generation = self
            .logical_range_counts
            .borrow()
            .as_ref()
            .map_or(1, |state| state.generation.wrapping_add(1));
        *self.logical_range_counts.borrow_mut() = Some(LogicalRangeCounts {
            epoch,
            counts: None,
            map,
            generation,
            recounts: Vec::new(),
        });
        self.invalidate_leaf_seed();
    }

    fn split_recount_enabled(&self) -> bool {
        #[cfg(test)]
        if self.split_recount_disabled.get() {
            return false;
        }
        true
    }

    pub(crate) fn logical_recount_pending(&self) -> bool {
        self.logical_range_counts
            .borrow()
            .as_ref()
            .is_some_and(|state| !state.recounts.is_empty())
    }

    pub(crate) fn logical_recount_step(&self) -> Result<RecountStep, EngineError> {
        self.logical_recount_page(None, std::time::Duration::ZERO)
            .expect("a synchronous recount page is never stale")
    }

    pub(crate) fn logical_recount_prefetch_plan(
        &self,
    ) -> Result<Option<RecountPrefetchPlan>, EngineError> {
        let accounting = self.logical_range_counts.borrow();
        let Some(recount) = accounting.as_ref().and_then(|state| state.recounts.first()) else {
            return Ok(None);
        };
        if crate::ram::ram_over_budget() {
            return Ok(None);
        }
        let Some(i) = recount.next_scan() else { return Ok(None) };
        let (page_lo, hi) = recount.page_bounds(i);
        let plan = self.engine.winner_count_prefetch_plan_bytes(
            page_lo.as_deref(),
            hi.as_deref(),
            RECOUNT_PREFETCH_WINDOW_BYTES,
        )?;
        Ok(Some(RecountPrefetchPlan {
            span: (recount.lo.clone(), recount.hi.clone()),
            child: i,
            plan,
        }))
    }

    pub(crate) fn logical_recount_prefetched_step(
        &self,
        fetched: RecountPrefetched,
        planned: std::time::Duration,
    ) -> Option<Result<RecountStep, EngineError>> {
        self.logical_recount_page(Some(fetched), planned)
    }

    fn logical_recount_page(
        &self,
        prefetched: Option<RecountPrefetched>,
        planned: std::time::Duration,
    ) -> Option<Result<RecountStep, EngineError>> {
        let started = std::time::Instant::now();
        let c = crate::observe::counters();
        let mut accounting = self.logical_range_counts.borrow_mut();
        let Some(state) = accounting.as_mut() else { return Some(Ok(RecountStep::Done)) };
        let Some(recount) = state.recounts.first_mut() else { return Some(Ok(RecountStep::Done)) };
        if crate::ram::ram_over_budget() {
            c.logical_range_split_recount_ram_pauses.inc();
            return Some(Ok(RecountStep::Paused));
        }
        if let Some(i) = recount.next_scan() {
            let (page_lo, hi) = recount.page_bounds(i);
            let deadline = Some(started + SPLIT_RECOUNT_TURN_BUDGET);
            let prefetch = prefetched.is_some();
            let page = match prefetched {
                None => self.accounted_count_page(page_lo.as_deref(), hi.as_deref(), deadline, None),
                Some(fetched)
                    if fetched.child == i
                        && fetched.span.0 == recount.lo
                        && fetched.span.1 == recount.hi =>
                {
                    self.accounted_count_page(page_lo.as_deref(), hi.as_deref(), deadline, Some(fetched.blocks))
                }
                Some(_) => Ok(None),
            };
            let (n, last, more) = match page {
                Ok(Some(page)) => page,
                Ok(None) => return None,
                Err(e) => {
                    drop(accounting);
                    self.abandon_split_recounts();
                    return Some(Err(e));
                }
            };
            c.logical_range_split_recount_pages.inc();
            if prefetch {
                c.logical_range_split_recount_prefetch_pages.inc();
            }
            c.logical_range_split_recount_keys.add(n as u64);
            #[cfg(test)]
            self.split_recount_visited
                .set(self.split_recount_visited.get() + n as u64);
            if let RecountChild::Scan { after, covered, done } = &mut recount.children[i] {
                *covered += n as u64;
                if let Some(last) = last {
                    *after = Some(last.into_boxed_slice());
                }
                *done = !more;
            }
        } else if prefetched.is_some() {
            return None;
        }
        if recount.next_scan().is_some() {
            drop(accounting);
            crate::observe::histograms()
                .logical_range_split_recount_turn_us
                .observe(started.elapsed() + planned);
            return Some(Ok(RecountStep::More));
        }
        let recount = state.recounts.remove(0);
        let base = recount.lo.as_deref().map_or(0, |lo| state.map.range_index(lo));
        let aligned = (0..recount.children.len()).all(|i| {
            base + i < state.map.num_ranges()
                && state.map.range_bounds(base + i) == recount.child_bounds(i)
        });
        let rest = recount
            .covered_sum()
            .and_then(|covered| recount.total.checked_sub(covered));
        let finalized = match (state.counts.as_mut(), rest) {
            (Some(counts), Some(rest)) if aligned && counts.len() == state.map.num_ranges() => {
                for (i, child) in recount.children.iter().enumerate() {
                    counts[base + i] = match child {
                        RecountChild::Scan { covered, .. } => *covered,
                        RecountChild::Derived => rest,
                    };
                }
                true
            }
            _ => false,
        };
        let done = state.recounts.is_empty();
        drop(accounting);
        crate::observe::histograms()
            .logical_range_split_recount_turn_us
            .observe(started.elapsed() + planned);
        if !finalized {
            if rest.is_none() {
                c.logical_range_split_recount_mismatches.inc();
            }
            self.abandon_split_recounts();
            return Some(Ok(RecountStep::Abandoned));
        }
        c.logical_range_split_recount_done.inc();
        Some(Ok(if done { RecountStep::Done } else { RecountStep::More }))
    }

    fn abandon_split_recounts(&self) {
        {
            let mut accounting = self.logical_range_counts.borrow_mut();
            let Some(state) = accounting.as_mut() else { return };
            state.recounts.clear();
            state.counts = None;
        }
        self.note_split_recount_fallback();
        self.invalidate_leaf_seed();
    }

    #[cfg(test)]
    pub(crate) fn understate_split_recount_totals_for_test(&self) {
        if let Some(state) = self.logical_range_counts.borrow_mut().as_mut() {
            for recount in &mut state.recounts {
                recount.total = 0;
            }
        }
    }

    fn note_split_recount_fallback(&self) {
        crate::observe::counters()
            .logical_range_split_recount_fallbacks
            .inc();
        #[cfg(test)]
        self.split_recount_fallbacks
            .set(self.split_recount_fallbacks.get() + 1);
    }

    pub(crate) fn logical_range_counts(&self, epoch: u64) -> Option<Vec<u64>> {
        self.logical_range_counts
            .borrow()
            .as_ref()
            .filter(|state| state.epoch == epoch && state.recounts.is_empty() && !self.rt_fold_pending())
            .and_then(|state| state.counts.clone())
    }

    pub(crate) fn logical_range_count_bounds(&self, epoch: u64) -> Option<Vec<(u64, u64)>> {
        self.logical_range_counts
            .borrow()
            .as_ref()
            .filter(|state| state.epoch == epoch && !self.rt_fold_pending())
            .and_then(LogicalRangeCounts::count_bounds)
    }

    fn logical_range_seed(&self) -> Option<(u64, RangeMap)> {
        self.logical_range_counts
            .borrow()
            .as_ref()
            .map(|state| (state.generation, state.map.clone()))
    }

    fn logical_range_generation(&self) -> Option<u64> {
        self.logical_range_counts
            .borrow()
            .as_ref()
            .map(|state| state.generation)
    }

    fn publish_logical_range_seed(&self, generation: u64, counts: Vec<u64>) {
        let mut accounting = self.logical_range_counts.borrow_mut();
        let Some(state) = accounting.as_mut() else { return };
        if state.generation == generation && counts.len() == state.map.num_ranges() {
            state.counts = Some(counts);
            state.recounts.clear();
        }
    }

    pub(super) fn note_logical_winner_apply(&self, key: &[u8], old: OldWinner) {
        let mut invalidate = false;
        let mut abandoned = false;
        if let Some(state) = self.logical_range_counts.borrow_mut().as_mut() {
            if state.counts.is_some() {
                match old {
                    OldWinner::Exact(None) => state.note_winner(key, true),
                    OldWinner::Exact(Some(_)) => {}
                    OldWinner::Unknown => {
                        crate::observe::counters().logical_range_unknown_invalidations.inc();
                        abandoned = !state.recounts.is_empty();
                        state.recounts.clear();
                        state.counts = None;
                        invalidate = true;
                    }
                }
            }
        }
        if abandoned {
            self.note_split_recount_fallback();
        }
        if invalidate {
            self.invalidate_leaf_seed();
        }
    }

    pub(super) fn note_logical_winner_retire(&self, key: &[u8]) {
        if let Some(state) = self.logical_range_counts.borrow_mut().as_mut() {
            state.note_winner(key, false);
        }
    }

    pub(super) fn retire_winner(&self, key: &[u8], group: &[NodeId], version: Version) {
        let (leaf, live) = self.accounted_old(key, Some(version));
        if leaf.is_some() && self.versions.retire_leaf(key, group, version) {
            self.spill_seed_op(key, group, None, version);
        }
        if live.is_some() {
            self.note_logical_winner_retire(key);
        }
    }

    pub(super) fn apply_winner_leaf(&self, key: &[u8], group: &[NodeId], old: OldWinner, new: Version) {
        if self.versions.apply_leaf(key, group, old, new) {
            self.spill_seed_op(key, group, Some(old), new);
        }
    }

    fn spill_seed_op(&self, key: &[u8], group: &[NodeId], apply: Option<OldWinner>, version: Version) {
        let Ok(mut progress) = self.seed_progress.try_borrow_mut() else {
            let op = match apply {
                Some(old) => LeafOp::Apply { key: Box::from(key), old, new: version },
                None => LeafOp::Retire { key: Box::from(key), version },
            };
            self.versions.push_journal_forced(op, key.len());
            return;
        };
        debug_assert!(
            progress.is_some(),
            "seed journal spill without an in-flight seed drops a leaf op"
        );
        let Some(sp) = progress.as_mut() else { return };
        crate::observe::counters().ae_seed_journal_spill_ops.inc();
        let forced = std::mem::replace(&mut sp.force_overdraw, true);
        if sp.spill.is_none() {
            let spill = SeedSpill::new(sp.logical_range_counts.len());
            sp.adjust_estimate(0, spill.bytes());
            sp.spill = Some(spill);
            #[cfg(test)]
            self.seed_spills.set(self.seed_spills.get() + 1);
        }
        let b = VersionIndex::slot(key);
        let range = sp
            .logical_range_generation
            .and(sp.range_map.as_ref())
            .map(|map| map.range_index(key));
        let clean = sp.bucket_dirty[b].is_none();
        let (digest, keys, ranges) = match apply {
            Some(OldWinner::Unknown) => {
                sp.bucket_dirty[b].get_or_insert_with(std::time::Instant::now);
                if let Some(spill) = sp.spill.as_mut() {
                    spill.ranges_unknown = true;
                }
                (0, 0, 0)
            }
            Some(OldWinner::Exact(previous)) => {
                let mut digest = crate::antientropy::entry_digest(key, version);
                if let Some(previous) = previous {
                    digest ^= crate::antientropy::entry_digest(key, previous);
                }
                let added = i64::from(previous.is_none());
                (digest, added, added)
            }
            None => (crate::antientropy::entry_digest(key, version), -1, -1),
        };
        if apply.is_some() {
            sp.high_ms = sp.high_ms.max(version.physical_ms());
        }
        if clean && !matches!(apply, Some(OldWinner::Unknown)) {
            if let Some(leaves) = sp.try_group_mut(group) {
                leaves[b] ^= digest;
            }
        }
        sp.force_overdraw = forced;
        let Some(spill) = sp.spill.as_mut() else { return };
        if clean {
            spill.buckets[b] += keys;
            spill.live += keys;
        }
        if let Some(index) = range {
            spill.ranges[index] += ranges;
        }
    }

    pub fn has_placement(&self) -> bool {
        self.placement.borrow().is_some()
    }

    pub fn invalidate_leaf_seed(&self) {
        if self.regroup.borrow_mut().take().is_some() {
            crate::observe::counters().ae_regroup_fallbacks.inc();
        }
        if self.versions.seeding.get() && self.seed_progress.borrow().is_some() {
            self.versions.leaves.borrow_mut().clear();
            self.versions.reset_bucket_meta();
            self.versions.seeded.set(false);
            crate::observe::counters().ae_seed_placement_coalesces.inc();
            log::debug!(
                "coalescing placement generation {} into in-flight Merkle-leaf seed",
                self.placement_generation.get()
            );
            return;
        }
        self.versions.invalidate();
        self.reset_seed_progress(SeedEnd::Dropped);
        self.seed_restart_count.set(0);
        self.seed_started.set(None);
    }

    /// Recovers replica `node`'s store from `dev` (see
    /// `vals_store::Engine::open`; the same `cfg`, block size and node id as
    /// before are required).
    pub fn open(dev: D, cfg: vals_store::Config, node: u32) -> Result<Self, EngineError> {
        let engine = Engine::open(dev, cfg)?;
        Ok(Self::lazy(engine, node))
    }

    pub fn high_water_ms(&self) -> u64 {
        self.applied_high_ms.get()
    }

    pub(super) fn note_version(&self, v: Version) {
        let ms = v.physical_ms();
        if ms > self.applied_high_ms.get() {
            self.applied_high_ms.set(ms);
        }
    }

    pub fn with_max_skew_ms(mut self, ms: u64) -> Self {
        self.hlc.set_max_skew_ms(ms);
        self
    }

    pub fn with_gc_grace_ms(mut self, ms: u64) -> Self {
        self.set_gc_grace_ms(ms);
        self
    }

    pub fn advance_durable_gc_watermark(&self, watermark_ms: u64) {
        if watermark_ms > self.durable_gc_watermark_ms.get() {
            self.durable_gc_watermark_ms.set(watermark_ms);
        }
        if self.gc_grace_ms != 0 {
            let cutoff = self
                .durable_gc_watermark_ms
                .get()
                .saturating_sub(self.gc_grace_ms);
            if cutoff > self.live_admission_cutoff_ms.get() {
                self.live_admission_cutoff_ms.set(cutoff);
            }
        }
    }

    pub(super) fn advance_live_admission_cutoff(&self, wall_ms: u64) {
        if self.gc_grace_ms == 0 {
            return;
        }
        let cutoff = wall_ms
            .min(self.high_water_ms())
            .saturating_sub(self.gc_grace_ms);
        if cutoff > self.live_admission_cutoff_ms.get() {
            self.live_admission_cutoff_ms.set(cutoff);
        }
    }

    pub fn admission_floor_ms(&self, wall_ms: u64) -> u64 {
        self.advance_live_admission_cutoff(wall_ms);
        if self.gc_grace_ms == 0 { 0 } else { self.live_admission_cutoff_ms.get() }
    }

    pub(super) fn durable_gc_cutoff_ms(&self) -> Option<u64> {
        if self.gc_grace_ms == 0 {
            return None;
        }
        let cutoff = self
            .durable_gc_watermark_ms
            .get()
            .saturating_sub(self.gc_grace_ms);
        (cutoff != 0).then_some(cutoff)
    }

    pub(super) fn gc_collect_before(&self, _wall_ms: u64) -> Option<u128> {
        self.durable_gc_cutoff_ms().map(gc_cutoff_version)
    }

    pub(super) fn tombstone_gc_trigger(&self, wall_ms: u64) -> Option<u128> {
        let interval = self.gc_grace_ms.min(TOMBSTONE_GC_MAX_INTERVAL_MS);
        if self.last_tombstone_gc_ms.is_some_and(|last| wall_ms < last.saturating_add(interval)) {
            return None;
        }
        self.gc_collect_before(wall_ms)
    }

    pub fn gc_grace_ms(&self) -> u64 {
        self.gc_grace_ms
    }

    pub fn set_gc_grace_ms(&mut self, ms: u64) {
        self.gc_grace_ms = ms;
        self.advance_durable_gc_watermark(self.durable_gc_watermark_ms.get());
    }

    pub fn engine(&self) -> &Engine<D> {
        &self.engine
    }

    pub fn engine_mut(&mut self) -> &mut Engine<D> {
        &mut self.engine
    }

    pub fn sync(&mut self) -> Result<(), EngineError> {
        self.engine.sync()
    }

    pub fn sync_prepare(&mut self) -> Result<(), EngineError> {
        self.engine.sync_prepare()
    }

    pub fn tail_epochs(&self) -> u64 {
        self.engine.tail_epochs()
    }

    pub fn memtable_bytes(&self) -> u64 {
        self.engine.memtable_bytes()
    }

}

pub async fn ensure_seeded_cooperative_cell<D: BlockDevice>(
    store: &std::cell::RefCell<VersionedStore<D>>,
) -> Result<bool, EngineError> {
    if store.borrow().seed_not_ready_for_caller() {
        crate::observe::counters().ae_seed_not_ready_busy.inc();
        return Ok(false);
    }
    loop {
        let step = store.borrow().seed_step()?;
        if step {
            return Ok(true);
        }
        if store.borrow().seed_deferred() {
            return Ok(false);
        }
        vals_rt::yield_now().await;
    }
}

trait StoreAccess<D: BlockDevice> {
    fn access<R>(&self, f: impl FnOnce(&VersionedStore<D>) -> R) -> R;

    fn access_mut<R>(&self, f: impl FnOnce(&mut VersionedStore<D>) -> R) -> Option<R>;
}

impl<D: BlockDevice> StoreAccess<D> for VersionedStore<D> {
    fn access<R>(&self, f: impl FnOnce(&VersionedStore<D>) -> R) -> R {
        f(self)
    }

    fn access_mut<R>(&self, _: impl FnOnce(&mut VersionedStore<D>) -> R) -> Option<R> {
        None
    }
}

impl<D: BlockDevice> StoreAccess<D> for std::cell::RefCell<VersionedStore<D>> {
    fn access<R>(&self, f: impl FnOnce(&VersionedStore<D>) -> R) -> R {
        f(&self.borrow())
    }

    fn access_mut<R>(&self, f: impl FnOnce(&mut VersionedStore<D>) -> R) -> Option<R> {
        self.try_borrow_mut().ok().map(|mut store| f(&mut store))
    }
}

struct DirtyRebuildCheckpoint<'a, D: BlockDevice, S: StoreAccess<D>> {
    pub(super) store: &'a S,
    pub(super) snapshot: &'a DirtySnapshot,
    pub(super) marker: std::marker::PhantomData<D>,
}

impl<D: BlockDevice + AsyncRead, S: StoreAccess<D>> vals_store::DigestCheckpoint
    for DirtyRebuildCheckpoint<'_, D, S>
{
    async fn checkpoint(&mut self) -> bool {
        vals_rt::yield_now().await;
        !crate::ram::ram_over_budget()
            && self
                .store
                .access(|store| store.dirty_snapshot_valid(self.snapshot))
    }
}

struct DirtyRebuildTimer(std::time::Instant);

impl Drop for DirtyRebuildTimer {
    fn drop(&mut self) {
        crate::observe::histograms()
            .ae_dirty_rebuild_us
            .observe(self.0.elapsed());
    }
}

async fn ensure_tree_ready_inner<D: BlockDevice + AsyncRead, S: StoreAccess<D>>(
    store: &S,
    scope_g: u32,
    scope: &std::collections::HashSet<u32>,
) -> Result<bool, EngineError> {
    if store.access(VersionedStore::seed_not_ready_for_caller) {
        crate::observe::counters().ae_seed_not_ready_busy.inc();
        return Ok(false);
    }
    loop {
        if store.access(|store| store.seed_step())? {
            break;
        }
        if store.access(|store| store.seed_deferred()) {
            return Ok(false);
        }
        vals_rt::yield_now().await;
    }
    if store.access(VersionedStore::regroup_not_ready_for_caller) {
        crate::observe::counters().ae_regroup_not_ready_busy.inc();
        return Ok(false);
    }
    while store.access(|store| store.regroup_step())? != RecountStep::Done {
        if !store.access(VersionedStore::is_leaf_seeded) {
            return Ok(false);
        }
        vals_rt::yield_now().await;
    }
    if store.access(VersionedStore::rt_fold_not_ready_for_caller) {
        crate::observe::counters().rt_fold_not_ready_busy.inc();
        return Ok(false);
    }
    loop {
        let step = match store.access_mut(VersionedStore::rt_fold_step) {
            Some(step) => step?,
            None => store.access(VersionedStore::rt_fold_step_inline)?,
        };
        if step == RecountStep::Done {
            break;
        }
        if !store.access(VersionedStore::is_leaf_seeded) {
            return Ok(false);
        }
        vals_rt::yield_now().await;
    }
    if store
        .access(|store| store.dirty_snapshot(scope_g, scope))
        .is_none()
    {
        return Ok(true);
    }
    crate::observe::counters().ae_dirty_rebuild_requests.inc();
    let _timer = DirtyRebuildTimer(std::time::Instant::now());
    let dev = store.access(VersionedStore::device_handle);
    for attempt in 0..MAX_DIRTY_REBUILD_ATTEMPTS {
        if store.access(VersionedStore::regroup_pending) || store.access(VersionedStore::rt_fold_pending) {
            return Ok(false);
        }
        if crate::ram::ram_over_budget() {
            crate::observe::counters().ae_dirty_rebuild_errors.inc();
            return Ok(false);
        }
        let Some(snapshot) = store.access(|store| store.dirty_snapshot(scope_g, scope)) else {
            return Ok(true);
        };
        let ids: Vec<u32> = snapshot.buckets.iter().map(|&(slot, _)| slot).collect();
        let mut digests: std::collections::HashMap<
            Box<[NodeId]>,
            std::collections::HashMap<u32, u64>,
        > = std::collections::HashMap::new();
        let mut counts: std::collections::HashMap<u32, u64> =
            std::collections::HashMap::new();
        let mut after: Option<Vec<u8>> = None;
        let mut invalidated = false;
        loop {
            let plan = match store.access(|store| {
                store.plan_bucket_digest_page(
                    INDEX_BUCKETS,
                    &ids,
                    after.as_deref(),
                    DIRTY_REBUILD_PAGE_BYTES,
                )
            }) {
                Ok(plan) => plan,
                Err(e) => {
                    crate::observe::counters().ae_dirty_rebuild_errors.inc();
                    return Err(e);
                }
            };
            let mut checkpoint = DirtyRebuildCheckpoint {
                store,
                snapshot: &snapshot,
                marker: std::marker::PhantomData,
            };
            let page = VersionedStore::bucket_digest_page_from_plan_cooperative(
                plan,
                INDEX_BUCKETS,
                &ids,
                after.as_deref(),
                DIRTY_REBUILD_PAGE_BYTES,
                dev.as_ref(),
                &mut checkpoint,
            )
            .await;
            let ((entries, more), _) = match page {
                Ok(Some(page))
                    if store.access(|store| store.dirty_snapshot_valid(&snapshot)) =>
                {
                    page
                }
                Ok(Some(_)) | Ok(None) => {
                    invalidated = true;
                    break;
                }
                Err(_)
                    if !store.access(|store| store.dirty_snapshot_valid(&snapshot)) =>
                {
                    invalidated = true;
                    break;
                }
                Err(e) => {
                    crate::observe::counters().ae_dirty_rebuild_errors.inc();
                    return Err(e);
                }
            };
            if more && entries.is_empty() {
                invalidated = true;
                break;
            }
            let mut metadata_bytes = 0u64;
            for (key, version) in &entries {
                let slot = VersionIndex::slot(key) as u32;
                let group = store.access(|store| store.group_of(key)).into_boxed_slice();
                *digests
                    .entry(group)
                    .or_default()
                    .entry(slot)
                    .or_default() ^= crate::antientropy::entry_digest(key, *version);
                *counts.entry(slot).or_default() += 1;
                metadata_bytes = metadata_bytes.saturating_add(key.len() as u64 + 16);
            }
            crate::observe::counters()
                .ae_dirty_rebuild_winner_outputs
                .add(entries.len() as u64);
            crate::observe::counters()
                .ae_dirty_rebuild_winner_output_bytes
                .add(metadata_bytes);
            if !more {
                break;
            }
            after = entries.last().map(|(key, _)| key.clone());
        }
        if !invalidated
            && store.access(|store| store.publish_dirty_rebuild(&snapshot, digests, &counts))
        {
            return Ok(true);
        }
        if attempt + 1 < MAX_DIRTY_REBUILD_ATTEMPTS {
            crate::observe::counters().ae_dirty_rebuild_retries.inc();
            vals_rt::yield_now().await;
        }
    }
    crate::observe::counters().ae_dirty_rebuild_errors.inc();
    Ok(false)
}

pub async fn ensure_tree_ready_cooperative<D: BlockDevice + AsyncRead>(
    store: &VersionedStore<D>,
    scope_g: u32,
    scope: &std::collections::HashSet<u32>,
) -> Result<bool, EngineError> {
    ensure_tree_ready_inner(store, scope_g, scope).await
}

pub async fn ensure_tree_ready_cooperative_cell<D: BlockDevice + AsyncRead>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    scope_g: u32,
    scope: &std::collections::HashSet<u32>,
) -> Result<bool, EngineError> {
    ensure_tree_ready_inner(store, scope_g, scope).await
}

pub(crate) async fn regroup_turn_with<D: BlockDevice + AsyncRead>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    gate: Option<&crate::net::ReadGate>,
    mut in_flight: impl FnMut(),
) -> Result<RecountStep, EngineError> {
    if !store.borrow().regroup_pending() {
        return Ok(RecountStep::Done);
    }
    if crate::ram::ram_over_budget() {
        return Ok(RecountStep::Paused);
    }
    let c = crate::observe::counters();
    for _ in 0..RECOUNT_PREFETCH_ATTEMPTS {
        let guard = match gate {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let plan = store.borrow().regroup_prefetch_plan();
        let plan = match plan {
            Ok(Some(plan)) => plan,
            Ok(None) | Err(_) => {
                drop(guard);
                return store.borrow().regroup_step();
            }
        };
        let dev = store.borrow().device_handle();
        let fetched = plan.read(dev.as_ref()).await;
        drop(guard);
        in_flight();
        let step = store.borrow().regroup_prefetched_step(fetched);
        match step {
            Some(step) => return step,
            None => c.ae_regroup_prefetch_discards.inc(),
        }
    }
    store.borrow().regroup_step()
}

pub(crate) async fn logical_recount_turn_with<D: BlockDevice + AsyncRead>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    gate: Option<&crate::net::ReadGate>,
    mut in_flight: impl FnMut(),
) -> Result<RecountStep, EngineError> {
    let c = crate::observe::counters();
    for _ in 0..RECOUNT_PREFETCH_ATTEMPTS {
        let guard = match gate {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let started = std::time::Instant::now();
        let plan = store.borrow().logical_recount_prefetch_plan();
        let plan = match plan {
            Ok(Some(plan)) => plan,
            Ok(None) | Err(_) => {
                drop(guard);
                return store.borrow().logical_recount_step();
            }
        };
        let planned = started.elapsed();
        c.logical_range_split_recount_prefetch_blocks
            .add(plan.block_reads() as u64);
        let dev = store.borrow().device_handle();
        let fetched = plan.read(dev.as_ref()).await;
        drop(guard);
        in_flight();
        let step = store.borrow().logical_recount_prefetched_step(fetched, planned);
        match step {
            Some(step) => return step,
            None => c.logical_range_split_recount_prefetch_discards.inc(),
        }
    }
    c.logical_range_split_recount_sync_fallbacks.inc();
    store.borrow().logical_recount_step()
}

