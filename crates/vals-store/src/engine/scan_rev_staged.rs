use super::*;

const MEM_REFILL_ENTRIES: usize = 4096;
const SCAN_BLOCK_WINDOW: usize = 64;

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub fn stage_scan_page_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        budget: ScanCoveredBudget,
    ) -> Result<StagedScanRevPage, EngineError> {
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }
        if matches!((lo, hi), (Some(l), Some(h)) if l >= h) {
            return Ok(StagedScanRevPage::Done(StagedScanDone {
                winners: Vec::new(), more: false, resume: None, rt: RtScanStats::default(),
            }));
        }
        let set = (!self.rts.fragments.is_empty()).then(|| self.rts.fragments.clone());
        if self.runs.is_empty() && set.is_none() {
            let (winners, more) = self.index.scan_page_rev(lo, hi, max_entries, max_bytes).expect("sorted index");
            return Ok(StagedScanRevPage::Done(StagedScanDone {
                winners, more, resume: None, rt: RtScanStats::default(),
            }));
        }
        let refill = if set.is_some() { MEM_REFILL_ENTRIES } else { MEM_REFILL_ENTRIES.min(max_entries.max(1)) };
        let (mem, mem_more) = self.index.scan_page_rev(lo, hi, refill, usize::MAX).expect("sorted index");
        let mut stats = RtScanStats::default();
        let mut cursors = Vec::new();
        for (run_idx, or) in self.runs.iter().enumerate() {
            if or.meta.entry_count == 0
                || lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let end = hi.map_or(or.run.index_len(), |h| or.run.blocks_starting_below(h));
            let (locators, resume_block, has_more_blocks) = locate_window(
                or, &self.alloc, end, lo, hi, set.as_deref(), &mut stats,
            );
            if locators.is_empty() && !has_more_blocks { continue; }
            cursors.push(AsyncRunRevCursor {
                run_idx, locators, resume_block, has_more_blocks,
                lo: lo.map(<[u8]>::to_vec), hi: hi.map(<[u8]>::to_vec),
                next_block: 0, cur: Vec::new(), needs_window: false, needs_prime: false,
                reads: 0, metered: false,
            });
        }
        let counted = vec![0; cursors.len()];
        Ok(StagedScanRevPage::Runs(StagedScanRevMerge {
            lo: lo.map(<[u8]>::to_vec), hi: hi.map(<[u8]>::to_vec),
            max_entries, max_bytes, refill,
            storage_generation: self.storage_gen, mutable_generation: self.mutable_gen,
            cursors, heads: Vec::new(), primed: false,
            mem, mem_more, mt: 0, out: Vec::new(), bytes: 0, more: false,
            cover: set, budget, stats, counted, popped: Vec::new(), covered_blocks: 0, resume: None,
        }))
    }

    pub fn scan_supply_mem_rev(&self, merge: &mut StagedScanRevMerge, from: &[u8]) -> Result<(), EngineError> {
        let (mem, mem_more) = self.index.scan_page_rev(merge.lo.as_deref(), Some(from), merge.refill, usize::MAX)
            .ok_or(EngineError::NotSorted)?;
        merge.mem = mem;
        merge.mem_more = mem_more;
        merge.mt = 0;
        Ok(())
    }

    pub fn scan_supply_blocks_rev(&self, merge: &mut StagedScanRevMerge, cursor: usize) -> Result<(), EngineError> {
        let c = &mut merge.cursors[cursor];
        let (locators, resume_block, has_more_blocks) = locate_window(
            &self.runs[c.run_idx], &self.alloc, c.resume_block,
            merge.lo.as_deref(), merge.hi.as_deref(), merge.cover.as_deref(), &mut merge.stats,
        );
        c.locators = locators;
        c.next_block = 0;
        c.resume_block = resume_block;
        c.has_more_blocks = has_more_blocks;
        c.needs_window = false;
        c.needs_prime = true;
        Ok(())
    }
}

fn locate_window<D: BlockDevice>(
    or: &OpenRun<D>,
    alloc: &Allocator,
    end: usize,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    cover: Option<&FragmentSet>,
    stats: &mut RtScanStats,
) -> (Vec<run::RangeLocator>, usize, bool) {
    or.run.locate_blocks_window_rev_picking(alloc, end, lo, SCAN_BLOCK_WINDOW, |end| {
        let Some(set) = cover else { return end };
        let mut picks = RtScanStats::default();
        let end = range_delete::rt_prev_block(or, set, hi, end, &mut picks);
        stats.skipped_blocks += picks.skipped_blocks;
        stats.index_probes += picks.index_probes;
        end
    })
}

struct AsyncRunRevCursor {
    run_idx: usize,
    locators: Vec<run::RangeLocator>,
    resume_block: usize,
    has_more_blocks: bool,
    needs_window: bool,
    needs_prime: bool,
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    next_block: usize,
    cur: Vec<SnapshotEntry>,
    reads: u64,
    metered: bool,
}

impl AsyncRunRevCursor {
    fn meter_read(&self, loc: &run::RangeLocator) {
        if self.metered {
            crate::attribution::SCAN_BLOCKS.add(1);
            crate::attribution::SCAN_BYTES.add(loc.read_bytes() as u64);
        }
    }

    async fn read_first<D: AsyncRead + BlockDevice>(&self, dev: &D) -> Result<Option<Vec<u8>>, EngineError> {
        match self.locators.first() {
            Some(loc) => {
                self.meter_read(loc);
                Ok(Some(loc.read(dev).await.map_err(EngineError::Snapshot)?))
            }
            None => Ok(None),
        }
    }

    fn seed_first(&mut self, bytes: Option<Vec<u8>>) -> Result<(), EngineError> {
        if let Some(bytes) = bytes {
            self.cur = run::decode_block(&bytes).map_err(|e| EngineError::Snapshot(SnapshotError::Corrupt(e)))?;
            self.next_block = 1;
            self.reads += 1;
        }
        Ok(())
    }

    async fn next<D: AsyncRead + BlockDevice>(&mut self, dev: &D) -> Result<Option<SnapshotEntry>, EngineError> {
        loop {
            if let Some(e) = self.cur.pop() {
                if self.hi.as_deref().is_some_and(|hi| e.key.as_slice() >= hi) { continue; }
                if self.lo.as_deref().is_some_and(|lo| e.key.as_slice() < lo) {
                    self.cur.clear();
                    self.next_block = self.locators.len();
                    self.has_more_blocks = false;
                    return Ok(None);
                }
                return Ok(Some(e));
            }
            if self.next_block >= self.locators.len() {
                self.needs_window = self.has_more_blocks;
                return Ok(None);
            }
            let loc = &self.locators[self.next_block];
            self.meter_read(loc);
            let bytes = loc.read(dev).await.map_err(EngineError::Snapshot)?;
            self.next_block += 1;
            self.reads += 1;
            self.cur = run::decode_block(&bytes).map_err(|e| EngineError::Snapshot(SnapshotError::Corrupt(e)))?;
        }
    }
}

pub enum StagedScanRevPage {
    Done(StagedScanDone),
    Runs(StagedScanRevMerge),
}

pub struct StagedScanRevMerge {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    max_entries: usize,
    max_bytes: usize,
    refill: usize,
    storage_generation: u64,
    mutable_generation: u64,
    cursors: Vec<AsyncRunRevCursor>,
    heads: Vec<Option<SnapshotEntry>>,
    primed: bool,
    mem: Vec<(Vec<u8>, IndexEntry)>,
    mem_more: bool,
    mt: usize,
    out: Vec<(Vec<u8>, IndexEntry)>,
    bytes: usize,
    more: bool,
    cover: Option<std::sync::Arc<FragmentSet>>,
    budget: ScanCoveredBudget,
    stats: RtScanStats,
    counted: Vec<u64>,
    popped: Vec<(usize, u64)>,
    covered_blocks: u64,
    resume: Option<Vec<u8>>,
}

impl StagedScanRevMerge {
    pub fn meter_server_scan(&mut self) {
        for cursor in &mut self.cursors {
            if !cursor.metered {
                crate::attribution::SCAN_CURSORS.add(1);
                cursor.metered = true;
            }
        }
    }

    pub fn generation(&self) -> u64 { self.storage_generation }

    pub fn mutable_generation(&self) -> u64 { self.mutable_generation }

    pub fn aggregate_generation(&self) -> (u64, u64) { (self.storage_generation, self.mutable_generation) }

    pub fn finish(mut self) -> StagedScanDone {
        if self.cover.is_some() {
            self.stats.block_reads = self.cursors.iter().map(|c| c.reads).sum();
        }
        StagedScanDone { winners: self.out, more: self.more, resume: self.resume, rt: self.stats }
    }

    pub async fn step<D: AsyncRead + BlockDevice>(&mut self, dev: &D) -> Result<StagedScanStep, EngineError> {
        if !self.primed {
            let reads: Vec<_> = self.cursors.iter().map(|c| c.read_first(dev)).collect();
            let firsts = vals_rt::join_all(reads).await;
            for (c, bytes) in self.cursors.iter_mut().zip(firsts) { c.seed_first(bytes?)?; }
            self.heads = Vec::with_capacity(self.cursors.len());
            for c in &mut self.cursors { self.heads.push(c.next(dev).await?); }
            self.primed = true;
        }
        loop {
            for i in 0..self.cursors.len() {
                if self.cursors[i].needs_prime {
                    self.cursors[i].needs_prime = false;
                    self.heads[i] = self.cursors[i].next(dev).await?;
                }
            }
            if let Some(cursor) = self.cursors.iter().position(|c| c.needs_window) {
                return Ok(StagedScanStep::NeedBlocks { cursor });
            }
            if self.mt == self.mem.len() && self.mem_more {
                let from = self.mem.last().expect("drained chunk is non-empty").0.clone();
                return Ok(StagedScanStep::NeedMem { from });
            }
            let mut max_key = self.mem.get(self.mt).map(|(k, _)| k.as_slice());
            for h in self.heads.iter().flatten() {
                if max_key.is_none_or(|m| h.key.as_slice() > m) { max_key = Some(h.key.as_slice()); }
            }
            let Some(key) = max_key else { return Ok(StagedScanStep::Done) };
            let key = key.to_vec();
            let mut win = None;
            if let Some((k, e)) = self.mem.get(self.mt) {
                if *k == key {
                    win = Some(*e);
                    self.mt += 1;
                }
            }
            for i in (0..self.cursors.len()).rev() {
                if self.heads[i].as_ref().is_some_and(|h| h.key == key) {
                    let h = self.heads[i].take().unwrap();
                    if win.as_ref().is_none_or(|w: &IndexEntry| h.version > w.version) {
                        win = Some(IndexEntry { ptr: h.ptr, seq: h.seq, version: h.version, tombstone: h.tombstone });
                    }
                    if self.cover.is_some() { self.popped.push((i, self.cursors[i].reads)); }
                    self.heads[i] = self.cursors[i].next(dev).await?;
                }
            }
            let entry = win.expect("max_key came from some source");
            if let Some(set) = &self.cover {
                let filtered = set.cover(&key).is_some_and(|v| entry.version <= v);
                for (i, block) in self.popped.drain(..) {
                    if filtered && self.counted[i] != block {
                        self.counted[i] = block;
                        self.covered_blocks += 1;
                    }
                }
                if filtered {
                    self.stats.filtered_entries += 1;
                    if self.stats.filtered_entries >= self.budget.filtered_entries || self.covered_blocks >= self.budget.block_reads {
                        self.resume = Some(key);
                        self.more = true;
                        return Ok(StagedScanStep::Done);
                    }
                    continue;
                }
            }
            let size = key.len() + entry.ptr.len as usize;
            if self.out.len() >= self.max_entries || (!self.out.is_empty() && self.bytes.saturating_add(size) > self.max_bytes) {
                self.more = true;
                return Ok(StagedScanStep::Done);
            }
            self.bytes = self.bytes.saturating_add(size);
            self.out.push((key, entry));
        }
    }
}
