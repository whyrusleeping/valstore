use super::lifecycle::{
    RECOUNT_PREFETCH_ATTEMPTS, RECOUNT_PREFETCH_WINDOW_BYTES, SPLIT_RECOUNT_PAGE, SPLIT_RECOUNT_TURN_BUDGET,
};
use super::*;
use std::sync::Arc;
use vals_store::range_tombstone::FragmentSet;

type Span = (Option<Vec<u8>>, Option<Vec<u8>>);

struct RtFold {
    target: Arc<FragmentSet>,
    seq: u64,
    spans: Vec<Span>,
    cursor: Vec<u8>,
}

impl RtFold {
    fn page_bounds(&self) -> Option<Span> {
        let span = self.spans.iter().find(|(_, hi)| hi.as_deref().is_none_or(|hi| hi > self.cursor.as_slice()))?;
        let lo = match &span.0 {
            Some(lo) if lo.as_slice() >= self.cursor.as_slice() => Some(lo.clone()),
            None if self.cursor.is_empty() => None,
            _ => Some(self.cursor.clone()),
        };
        Some((lo, span.1.clone()))
    }
}

#[derive(Default)]
pub(super) struct RtAccounting {
    accounted: Arc<FragmentSet>,
    seq: u64,
    seed: Option<(Option<Arc<FragmentSet>>, u64)>,
    queued: u64,
    fold: Option<RtFold>,
    epoch: u64,
    #[cfg(test)]
    fold_winners: u64,
}

impl RtAccounting {
    fn acct(&self, key: &[u8]) -> &FragmentSet {
        match &self.fold {
            Some(fold) if key < fold.cursor.as_slice() => &fold.target,
            _ => &self.accounted,
        }
    }

    fn pending(&self) -> bool {
        self.fold.is_some() || self.queued > self.seq
    }
}

pub(crate) struct RtFoldRemoval {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    cover: Arc<FragmentSet>,
}

type RtFoldPage = (RecountStep, Option<RtFoldRemoval>);

pub(super) fn covers(set: &FragmentSet, key: &[u8], raw: u128) -> bool {
    !set.is_empty() && set.cover(key).is_some_and(|cover| raw <= cover)
}

fn union_spans(mut spans: Vec<Span>) -> Vec<Span> {
    spans.sort_by(|a, b| a.0.cmp(&b.0));
    let mut out: Vec<Span> = Vec::with_capacity(spans.len());
    for (lo, hi) in spans {
        match out.last_mut() {
            Some((_, last)) if last.as_ref().is_none_or(|last| lo.as_ref().is_none_or(|lo| lo <= last)) => {
                if last.is_some() && hi.as_ref().is_none_or(|hi| Some(hi) > last.as_ref()) {
                    *last = hi;
                }
            }
            _ => out.push((lo, hi)),
        }
    }
    out
}

impl<D: BlockDevice> VersionedStore<D> {
    pub(super) fn accounted_old(&self, key: &[u8], old: Option<Version>) -> (Option<Version>, Option<Version>) {
        let Some(version) = old else { return (None, None) };
        let raw = pack_version(version);
        let acct = self.rt_accounting.borrow();
        let live = !covers(acct.acct(key), key, raw);
        let leaf = match &acct.seed {
            Some((cover, _)) if self.versions.seeding.get() => {
                !cover.as_deref().is_some_and(|cover| covers(cover, key, raw))
            }
            _ => live,
        };
        (leaf.then_some(version), live.then_some(version))
    }

    pub(super) fn accounted_covers(&self, key: &[u8], raw: u128) -> bool {
        let acct = self.rt_accounting.borrow();
        covers(acct.acct(key), key, raw)
    }

    pub(super) fn digest_cover(&self) -> Option<Arc<FragmentSet>> {
        let acct = self.rt_accounting.borrow();
        (!acct.accounted.is_empty()).then(|| acct.accounted.clone())
    }

    pub(super) fn filter_digest_page(&self, digests: &mut Vec<(Vec<u8>, Version)>) -> bool {
        let acct = self.rt_accounting.borrow();
        if acct.accounted.is_empty() {
            return false;
        }
        let before = digests.last().map(|(key, _)| key.clone());
        digests.retain(|(key, version)| !covers(&acct.accounted, key, pack_version(*version)));
        before.is_some() && digests.last().map(|(key, _)| key) != before.as_ref()
    }

    pub(super) fn accounted_count_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        deadline: Option<std::time::Instant>,
        prefetched: Option<vals_store::WinnerCountBlocks>,
    ) -> Result<Option<(usize, Option<Vec<u8>>, bool)>, EngineError> {
        let page = SPLIT_RECOUNT_PAGE;
        let acct = self.rt_accounting.borrow();
        if acct.accounted.is_empty() && acct.fold.is_none() {
            return match prefetched {
                None => self.engine.winner_count_page(lo, hi, page, deadline).map(Some),
                Some(blocks) => self.engine.winner_count_page_prefetched(lo, hi, page, deadline, blocks),
            };
        }
        let versions = match prefetched {
            None => Some(self.engine.winner_version_page(lo, hi, page, deadline)?),
            Some(blocks) => self.engine.winner_version_page_prefetched(lo, hi, page, deadline, blocks)?,
        };
        Ok(versions.map(|(entries, more)| {
            let n = entries.iter().filter(|(key, raw)| !covers(acct.acct(key), key, *raw)).count();
            (n, entries.into_iter().next_back().map(|(key, _)| key), more)
        }))
    }

    pub(super) fn start_seed_accounting(&self) -> Option<Arc<FragmentSet>> {
        let seq = self.engine.range_tombstone_max_seq();
        let fragments = self.engine.range_tombstones();
        let cover = (!fragments.is_empty()).then(|| Arc::new(fragments.clone()));
        self.rt_accounting.borrow_mut().seed = Some((cover.clone(), seq));
        cover
    }

    pub(super) fn reset_seed_accounting(&self) {
        self.rt_accounting.borrow_mut().seed = None;
    }

    pub(super) fn seed_accounted_set(&self) -> (FragmentSet, u64, bool) {
        let acct = self.rt_accounting.borrow();
        let seed_seq = acct.seed.as_ref().map_or(0, |(_, seq)| *seq);
        let seq = acct.seq.max(seed_seq);
        let set = self.engine.range_tombstones_through(seq);
        let changed = set != *acct.accounted;
        (set, seq, changed)
    }

    pub(super) fn publish_accounted(&self, set: FragmentSet, seq: u64) {
        let mut acct = self.rt_accounting.borrow_mut();
        acct.accounted = Arc::new(set);
        acct.seq = seq;
        acct.seed = None;
        acct.fold = None;
        acct.epoch += 1;
        drop(acct);
        self.engine.advance_range_tombstone_accounted_seq(seq);
    }

    pub(super) fn queue_range_tombstone(&self, seq: u64) {
        let mut acct = self.rt_accounting.borrow_mut();
        acct.queued = acct.queued.max(seq);
    }

    pub(super) fn note_collected_range_tombstones(&self, collected: &[vals_store::range_tombstone::RtRecord]) {
        if !collected.is_empty() {
            self.refresh_accounted_from_records();
        }
    }

    pub(super) fn refresh_accounted_from_records(&self) {
        let mut acct = self.rt_accounting.borrow_mut();
        let accounted = self.engine.range_tombstones_through(acct.seq);
        if let Some(through) = acct.fold.as_ref().map(|fold| fold.seq) {
            let (target, _) = self.fold_target(&accounted, acct.seq, through);
            acct.fold.as_mut().unwrap().target = Arc::new(target);
        }
        acct.accounted = Arc::new(accounted);
        acct.epoch += 1;
        let seeding = acct.seed.is_some();
        drop(acct);
        if seeding {
            self.reset_seed_progress(SeedEnd::Restart);
        }
    }

    pub(crate) fn rt_release_accounted(&self, lo: &[u8], hi: Option<&[u8]>, version: u128) -> bool {
        let acct = self.rt_accounting.borrow();
        if !self.versions.seeded.get() || self.versions.seeding.get() || acct.seed.is_some() {
            return false;
        }
        acct.accounted.covers_span(lo, hi, version)
    }

    pub(super) fn clear_accounted(&self) {
        let mut acct = self.rt_accounting.borrow_mut();
        let epoch = acct.epoch + 1;
        *acct = RtAccounting { epoch, ..RtAccounting::default() };
    }

    pub(crate) fn accounted_epoch(&self) -> u64 {
        self.rt_accounting.borrow().epoch
    }

    fn fold_target(&self, accounted: &FragmentSet, from: u64, through: u64) -> (FragmentSet, Vec<Span>) {
        let mut target = accounted.clone();
        target.set_max_fragments(usize::MAX);
        let mut spans = Vec::new();
        for r in self.engine.range_tombstone_records_between(from, through) {
            let delta = target
                .overlay(r.lo.as_deref(), r.hi.as_deref(), r.version)
                .expect("an uncapped overlay never refuses");
            spans.extend(delta.into_iter().map(|d| ((!d.lo.is_empty()).then_some(d.lo), d.hi)));
        }
        target.set_max_fragments(accounted.max_fragments());
        (target, union_spans(spans))
    }

    pub(crate) fn rt_fold_pending(&self) -> bool {
        self.rt_accounting.borrow().pending()
    }

    pub(crate) fn rt_fold_not_ready_for_caller(&self) -> bool {
        self.seed_driver.get() && self.rt_fold_pending()
    }

    fn rt_fold_runnable(&self) -> bool {
        self.versions.seeded.get()
            && !self.versions.seeding.get()
            && self.seed_progress.borrow().is_none()
            && self.rt_fold_pending()
    }

    fn begin_rt_fold(&self) -> bool {
        if !self.rt_fold_runnable() {
            return false;
        }
        let mut acct = self.rt_accounting.borrow_mut();
        if acct.fold.is_some() {
            return false;
        }
        let (target, spans) = self.fold_target(&acct.accounted, acct.seq, acct.queued);
        acct.fold = Some(RtFold { target: Arc::new(target), seq: acct.queued, spans, cursor: Vec::new() });
        true
    }

    fn rt_fold_page(
        &self,
        prefetched: Option<vals_store::WinnerCountBlocks>,
    ) -> Option<Result<RtFoldPage, EngineError>> {
        if !self.rt_fold_runnable() {
            return prefetched.is_none().then_some(Ok((RecountStep::Done, None)));
        }
        self.begin_rt_fold();
        let started = std::time::Instant::now();
        let c = crate::observe::counters();
        let acct = self.rt_accounting.borrow();
        let fold = acct.fold.as_ref().expect("a runnable fold has begun");
        let (accounted, target) = (acct.accounted.clone(), fold.target.clone());
        let Some((lo, hi)) = fold.page_bounds() else {
            drop(acct);
            return prefetched.is_none().then(|| Ok((self.complete_rt_fold(), None)));
        };
        drop(acct);
        let deadline = Some(started + SPLIT_RECOUNT_TURN_BUDGET);
        let sync = || {
            self.engine
                .winner_version_page_skipping(lo.as_deref(), hi.as_deref(), SPLIT_RECOUNT_PAGE, deadline, &accounted)
                .map(Some)
        };
        let sync_page = prefetched.is_none();
        let page = match prefetched {
            None => sync(),
            Some(blocks) => match self.engine.winner_version_page_prefetched(
                lo.as_deref(),
                hi.as_deref(),
                SPLIT_RECOUNT_PAGE,
                deadline,
                blocks,
            ) {
                Err(_) => sync(),
                page => page,
            },
        };
        let (entries, more) = match page {
            Ok(Some(page)) => page,
            Ok(None) => return None,
            Err(e) => {
                self.invalidate_leaf_seed();
                return Some(Err(e));
            }
        };
        if more && entries.is_empty() {
            return sync_page.then_some(Ok((RecountStep::More, None)));
        }
        c.rt_fold_pages.inc();
        c.rt_fold_winners_total.add(entries.len() as u64);
        let mut retired = 0u64;
        for (key, raw) in &entries {
            if covers(&accounted, key, *raw) || !covers(&target, key, *raw) {
                continue;
            }
            let group = self.group_of(key);
            self.versions.retire_leaf(key, &group, unpack_version(*raw));
            self.note_logical_winner_retire(key);
            retired += 1;
        }
        c.rt_fold_retired.add(retired);
        let end = match entries.last() {
            Some((last, _)) if more => Some([last.as_slice(), &[0]].concat()),
            _ => hi.clone(),
        };
        let mut acct = self.rt_accounting.borrow_mut();
        #[cfg(test)]
        {
            acct.fold_winners += entries.len() as u64;
        }
        let fold = acct.fold.as_mut().expect("the fold survives its own page");
        let done = match &end {
            Some(end) => {
                fold.cursor = end.clone();
                fold.page_bounds().is_none()
            }
            None => true,
        };
        drop(acct);
        let step = if done { self.complete_rt_fold() } else { RecountStep::More };
        Some(Ok((step, Some(RtFoldRemoval { lo, hi: end, cover: target }))))
    }

    fn complete_rt_fold(&self) -> RecountStep {
        let mut acct = self.rt_accounting.borrow_mut();
        let fold = acct.fold.take().expect("a completing fold");
        acct.accounted = fold.target;
        acct.seq = fold.seq;
        acct.epoch += 1;
        let pending = acct.pending();
        drop(acct);
        self.engine.advance_range_tombstone_accounted_seq(fold.seq);
        crate::observe::counters().rt_fold_completions.inc();
        if pending { RecountStep::More } else { RecountStep::Done }
    }

    fn finish_rt_fold_page(&mut self, page: RtFoldPage) -> RecountStep {
        let (step, removal) = page;
        if let Some(removal) = removal {
            self.remove_rt_folded(removal);
        }
        step
    }

    fn remove_rt_folded(&mut self, removal: RtFoldRemoval) {
        let removed = self.engine.remove_covered_mutable(removal.lo.as_deref(), removal.hi.as_deref(), &removal.cover);
        crate::observe::counters().rt_fold_memtable_removed.add(removed as u64);
    }

    fn build_rt_fold_trees(&mut self) {
        self.engine.build_range_tombstone_max_trees();
    }

    pub(crate) fn rt_fold_step_inline(&self) -> Result<RecountStep, EngineError> {
        self.rt_fold_page(None)
            .expect("a synchronous fold page is never stale")
            .map(|(step, _)| step)
    }

    pub(crate) fn begin_rt_fold_with_trees(&mut self) {
        if self.begin_rt_fold() {
            self.build_rt_fold_trees();
        }
    }

    pub(crate) fn rt_fold_step(&mut self) -> Result<RecountStep, EngineError> {
        self.begin_rt_fold_with_trees();
        let page = self.rt_fold_page(None).expect("a synchronous fold page is never stale")?;
        Ok(self.finish_rt_fold_page(page))
    }

    pub(crate) fn rt_fold_prefetch_plan(&self) -> Result<Option<vals_store::WinnerCountPlan>, EngineError> {
        if !self.rt_fold_runnable() {
            return Ok(None);
        }
        let acct = self.rt_accounting.borrow();
        let Some((lo, hi)) = acct.fold.as_ref().and_then(RtFold::page_bounds) else { return Ok(None) };
        self.engine
            .winner_count_prefetch_plan_bytes_skipping(
                lo.as_deref(),
                hi.as_deref(),
                RECOUNT_PREFETCH_WINDOW_BYTES,
                &acct.accounted,
            )
            .map(Some)
    }

    #[cfg(test)]
    pub(crate) fn rt_fold_prefetched_step(
        &mut self,
        blocks: vals_store::WinnerCountBlocks,
    ) -> Option<Result<RecountStep, EngineError>> {
        let page = self.rt_fold_page(Some(blocks))?;
        Some(page.map(|page| self.finish_rt_fold_page(page)))
    }

    #[cfg(test)]
    pub(crate) fn accounted_range_tombstones(&self) -> (FragmentSet, u64) {
        let acct = self.rt_accounting.borrow();
        ((*acct.accounted).clone(), acct.seq)
    }

    #[cfg(test)]
    pub(crate) fn rt_fold_state(&self) -> Option<(FragmentSet, u64, Vec<u8>)> {
        let acct = self.rt_accounting.borrow();
        acct.fold.as_ref().map(|fold| ((*fold.target).clone(), fold.seq, fold.cursor.clone()))
    }

    #[cfg(test)]
    pub(crate) fn rt_fold_winners(&self) -> u64 {
        self.rt_accounting.borrow().fold_winners
    }
}

fn finish_shared<D: BlockDevice>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    page: Result<RtFoldPage, EngineError>,
) -> Result<RecountStep, EngineError> {
    let (step, removal) = page?;
    if let Some(removal) = removal {
        match store.try_borrow_mut() {
            Ok(mut store) => store.remove_rt_folded(removal),
            Err(_) => crate::observe::counters().rt_fold_memtable_deferred.inc(),
        }
    }
    Ok(step)
}

pub(crate) async fn rt_fold_turn_with<D: BlockDevice + AsyncRead>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    gate: Option<&crate::net::ReadGate>,
    mut in_flight: impl FnMut(),
) -> Result<RecountStep, EngineError> {
    if !store.borrow().rt_fold_runnable() {
        return Ok(RecountStep::Done);
    }
    if crate::ram::ram_over_budget() {
        return Ok(RecountStep::Paused);
    }
    if store.borrow().begin_rt_fold() {
        if let Ok(mut store) = store.try_borrow_mut() {
            store.build_rt_fold_trees();
        }
    }
    let c = crate::observe::counters();
    for _ in 0..RECOUNT_PREFETCH_ATTEMPTS {
        let guard = match gate {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let plan = store.borrow().rt_fold_prefetch_plan();
        let plan = match plan {
            Ok(Some(plan)) => plan,
            Ok(None) | Err(_) => {
                drop(guard);
                break;
            }
        };
        let dev = store.borrow().device_handle();
        let fetched = plan.read(dev.as_ref()).await;
        drop(guard);
        in_flight();
        let page = store.borrow().rt_fold_page(Some(fetched));
        match page {
            Some(page) => return finish_shared(store, page),
            None => c.rt_fold_prefetch_discards.inc(),
        }
    }
    let page = store.borrow().rt_fold_page(None).expect("a synchronous fold page is never stale");
    finish_shared(store, page)
}
