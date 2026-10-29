use super::*;

impl<D: BlockDevice> Engine<D> {
    /// Every key in `[lo, hi)` with its greatest version's value, in key
    /// order. `None` bounds are open. Only for [`Ordering::Sorted`] engines
    /// (otherwise [`EngineError::NotSorted`]); the whole result is held in
    /// memory, so use [`scan_bounded`](Self::scan_bounded) to page.
    pub fn scan(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<Vec<KvPair>, EngineError> {
        Ok(self.scan_bounded(lo, hi, usize::MAX, usize::MAX)?.0)
    }

    /// Like [`scan`](Self::scan), stopping after `max_entries` keys or about
    /// `max_bytes`; the flag is `true` if keys remain. Resume from the last
    /// key returned plus a `0x00` byte.
    pub fn scan_bounded(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(Vec<KvPair>, bool), EngineError> {
        let (entries, more) = self.scan_winners_page(lo, hi, max_entries, max_bytes)?;
        let items = self.value_items(entries.iter().map(|(_, e)| &e.ptr))?;
        let recs = self
            .log
            .plan_read_spans(&self.dev, &items)?
            .execute_sync(&self.dev)?;
        let mut out = Vec::with_capacity(entries.len());
        for ((key, _), rec) in entries.into_iter().zip(recs) {
            if rec.kind == RecordKind::Put {
                out.push((key, rec.value));
            }
        }
        Ok((out, more))
    }

    pub fn scan_bounded_versioned(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<VersionedScanPage, EngineError> {
        let (entries, more) = self.scan_winners_page(lo, hi, max_entries, max_bytes)?;
        let items = self.value_items(entries.iter().map(|(_, e)| &e.ptr))?;
        let recs = self
            .log
            .plan_read_spans(&self.dev, &items)?
            .execute_sync(&self.dev)?;
        let mut out = Vec::with_capacity(entries.len());
        for ((key, e), rec) in entries.into_iter().zip(recs) {
            if rec.kind == RecordKind::Put {
                out.push((key, rec.value, e.version));
            }
        }
        Ok((out, more))
    }

    pub fn winner_count_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<(usize, Option<Vec<u8>>, bool), EngineError> {
        let (entries, more) =
            self.scan_winners_page_until(lo, hi, max_entries.max(1), usize::MAX, deadline)?;
        let n = entries.len();
        Ok((n, entries.into_iter().next_back().map(|(key, _)| key), more))
    }

    pub fn winner_count_prefetch_plan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        window: usize,
    ) -> Result<WinnerCountPlan, EngineError> {
        self.winner_count_prefetch_plan_with(lo, hi, |_| window, None)
    }

    pub fn winner_count_prefetch_plan_bytes(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        window_bytes: usize,
    ) -> Result<WinnerCountPlan, EngineError> {
        self.winner_count_prefetch_plan_with(lo, hi, |run| run.blocks_for_bytes(window_bytes), None)
    }

    pub fn winner_count_prefetch_plan_bytes_skipping(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        window_bytes: usize,
        skip: &FragmentSet,
    ) -> Result<WinnerCountPlan, EngineError> {
        let skip = (!skip.is_empty()).then_some(skip);
        self.winner_count_prefetch_plan_with(lo, hi, |run| run.blocks_for_bytes(window_bytes), skip)
    }

    fn winner_count_prefetch_plan_with(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        window_for: impl Fn(&run::DeviceRun<D>) -> usize,
        skip: Option<&FragmentSet>,
    ) -> Result<WinnerCountPlan, EngineError> {
        let mut plan = WinnerCountPlan {
            lo: lo.map(<[u8]>::to_vec),
            hi: hi.map(<[u8]>::to_vec),
            storage_generation: self.storage_gen,
            horizon: None,
            runs: Vec::new(),
        };
        if self.runs.is_empty() {
            return Ok(plan);
        }
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }
        let mut starts: Vec<(usize, usize, usize)> = Vec::new();
        for (run_idx, or) in self.runs.iter().enumerate() {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let window = window_for(&or.run).max(2);
            let start = or.run.straddling_start_block(lo);
            let start = match skip {
                Some(set) => range_delete::rt_next_block(or, set, lo, start, &mut RtScanStats::default()),
                None => start,
            };
            let edge = start.saturating_add(window);
            if edge < or.run.index_len() {
                let first = or.run.block_first_key(edge);
                if hi.is_none_or(|h| first < h)
                    && plan.horizon.as_deref().is_none_or(|x| first < x)
                {
                    plan.horizon = Some(first.to_vec());
                }
            }
            starts.push((run_idx, start, window));
        }
        let bound = plan.horizon.as_deref().or(hi);
        for (run_idx, start, window) in starts {
            let (locators, _, _) =
                self.runs[run_idx]
                    .run
                    .locate_blocks_window(&self.alloc, start, bound, window);
            if !locators.is_empty() {
                plan.runs.push(locators);
            }
        }
        Ok(plan)
    }

    pub fn winner_version_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<WinnerVersionPage, EngineError> {
        let (entries, more) =
            self.scan_winners_page_until(lo, hi, max_entries.max(1), usize::MAX, deadline)?;
        Ok((entries.into_iter().map(|(key, e)| (key, e.version)).collect(), more))
    }

    pub fn winner_version_page_skipping(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
        skip: &FragmentSet,
    ) -> Result<WinnerVersionPage, EngineError> {
        let skip = (!skip.is_empty()).then_some(skip);
        let (entries, more) = self.scan_winners_page_covered(
            lo,
            hi,
            max_entries.max(1),
            usize::MAX,
            deadline,
            None,
            skip,
            &mut RtScanStats::default(),
        )?;
        Ok((entries.into_iter().map(|(key, e)| (key, e.version)).collect(), more))
    }

    pub fn winner_count_page_prefetched(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
        blocks: WinnerCountBlocks,
    ) -> Result<Option<WinnerCount>, EngineError> {
        self.winner_page_prefetched(lo, hi, max_entries, deadline, blocks, |_, _| {})
    }

    pub fn winner_version_page_prefetched(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
        blocks: WinnerCountBlocks,
    ) -> Result<Option<WinnerVersionPage>, EngineError> {
        let mut out = Vec::new();
        let page = self.winner_page_prefetched(lo, hi, max_entries, deadline, blocks, |key, version| {
            out.push((key.to_vec(), version))
        })?;
        Ok(page.map(|(_, _, more)| (out, more)))
    }

    fn winner_page_prefetched(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        deadline: Option<std::time::Instant>,
        blocks: WinnerCountBlocks,
        mut sink: impl FnMut(&[u8], u128),
    ) -> Result<Option<WinnerCount>, EngineError> {
        const DEADLINE_CHECK_EVERY: usize = 32;
        const MEM_REFILL_ENTRIES: usize = 4096;
        if blocks.storage_generation != self.storage_gen
            || blocks.lo.as_deref() != lo
            || blocks.hi.as_deref() != hi
        {
            return Ok(None);
        }
        let max_entries = max_entries.max(1);
        if self.runs.is_empty() {
            let (entries, more) = self
                .index
                .scan_page(lo, hi, max_entries, usize::MAX)
                .ok_or(EngineError::NotSorted)?;
            for (key, e) in &entries {
                sink(key, e.version);
            }
            let n = entries.len();
            return Ok(Some((n, entries.into_iter().next_back().map(|(key, _)| key), more)));
        }
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }
        let WinnerCountBlocks { horizon, runs, .. } = blocks;
        let bound = horizon.as_deref().or(hi);
        let refill = MEM_REFILL_ENTRIES.min(max_entries);
        let (mut mem, mut mem_more) = self
            .index
            .scan_page(lo, bound, refill, usize::MAX)
            .ok_or(EngineError::NotSorted)?;
        let mut mt = 0usize;
        let mut cursors: Vec<PrefetchedRunCursor> = runs
            .into_iter()
            .map(|blocks| PrefetchedRunCursor {
                blocks: blocks.into_iter(),
                cur: run::BlockCursor::default(),
            })
            .collect();
        let mut heads: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize)>> =
            std::collections::BinaryHeap::with_capacity(cursors.len());
        let mut head_versions = vec![0u128; cursors.len()];
        for (i, c) in cursors.iter_mut().enumerate() {
            if let Some((key, version)) = c.next(lo, bound)? {
                head_versions[i] = version;
                heads.push(std::cmp::Reverse((key, i)));
            }
        }
        let mut n = 0usize;
        let mut last: Option<Vec<u8>> = None;
        let mut more = horizon.is_some();
        loop {
            if mt == mem.len() && mem_more {
                let mut next = mem.last().expect("drained chunk is non-empty").0.clone();
                next.push(0);
                let (m, mm) = self
                    .index
                    .scan_page(Some(&next), bound, refill, usize::MAX)
                    .ok_or(EngineError::NotSorted)?;
                mem = m;
                mem_more = mm;
                mt = 0;
            }
            let from_mem = match (mem.get(mt), heads.peek()) {
                (None, None) => break,
                (Some(_), None) => true,
                (None, Some(_)) => false,
                (Some((m, _)), Some(std::cmp::Reverse((h, _)))) => m <= h,
            };
            if n >= max_entries
                || (n > 0
                    && n.is_multiple_of(DEADLINE_CHECK_EVERY)
                    && deadline.is_some_and(|d| std::time::Instant::now() >= d))
            {
                more = true;
                break;
            }
            let (min_key, mut version) = if from_mem {
                mt += 1;
                (mem[mt - 1].0.clone(), mem[mt - 1].1.version)
            } else {
                let std::cmp::Reverse((key, i)) = heads.pop().expect("peeked above");
                let version = head_versions[i];
                if let Some((next, v)) = cursors[i].next(lo, bound)? {
                    head_versions[i] = v;
                    heads.push(std::cmp::Reverse((next, i)));
                }
                (key, version)
            };
            while heads.peek().is_some_and(|std::cmp::Reverse((h, _))| *h == min_key) {
                let std::cmp::Reverse((_, i)) = heads.pop().expect("peeked above");
                version = version.max(head_versions[i]);
                if let Some((next, v)) = cursors[i].next(lo, bound)? {
                    head_versions[i] = v;
                    heads.push(std::cmp::Reverse((next, i)));
                }
            }
            sink(&min_key, version);
            n += 1;
            last = Some(min_key);
        }
        if n == 0 && more {
            return Ok(None);
        }
        Ok(Some((n, last, more)))
    }

    pub fn scan_winners_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<WinnersPage, EngineError> {
        Ok(self.scan_winners_page_counted(lo, hi, max_entries, max_bytes)?.0)
    }

    pub fn scan_winners_page_counted(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(WinnersPage, RtScanStats), EngineError> {
        let mut stats = RtScanStats::default();
        let cover = (!self.rts.fragments.is_empty()).then_some(&*self.rts.fragments);
        let page = self.scan_winners_page_covered(lo, hi, max_entries, max_bytes, None, cover, cover, &mut stats)?;
        Ok((page, stats))
    }

    fn scan_winners_page_until(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        deadline: Option<std::time::Instant>,
    ) -> Result<WinnersPage, EngineError> {
        self.scan_winners_page_covered(lo, hi, max_entries, max_bytes, deadline, None, None, &mut RtScanStats::default())
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_winners_page_covered(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        deadline: Option<std::time::Instant>,
        cover: Option<&FragmentSet>,
        skip: Option<&FragmentSet>,
        stats: &mut RtScanStats,
    ) -> Result<WinnersPage, EngineError> {
        const DEADLINE_CHECK_EVERY: usize = 32;
        if self.runs.is_empty() && cover.is_none() {
            return self
                .index
                .scan_page(lo, hi, max_entries, max_bytes)
                .ok_or(EngineError::NotSorted);
        }
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }

        const MEM_REFILL_ENTRIES: usize = 4096;
        let refill = MEM_REFILL_ENTRIES.min(max_entries.max(1));
        let (mut mem, mut mem_more) = self
            .index
            .scan_page(lo, hi, refill, usize::MAX)
            .ok_or(EngineError::NotSorted)?;
        let mut mt = 0usize;

        let mut cursors: Vec<(run::RunScan<'_, D>, &OpenRun<D>)> = Vec::new();
        let mut heads: Vec<Option<SnapshotEntry>> = Vec::new();
        let advance = |c: &mut (run::RunScan<'_, D>, &OpenRun<D>), stats: &mut RtScanStats| match skip {
            None => c.0.next(&self.alloc),
            Some(set) => c.0.next_picking(&self.alloc, |b| range_delete::rt_next_block(c.1, set, lo, b, stats)),
        };
        for or in &self.runs {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let mut c = (or.run.block_scan(lo, hi), or);
            let head = advance(&mut c, stats)?;
            cursors.push(c);
            heads.push(head);
        }
        let mut covers = cover.map(|set| set.cursor_from(lo.unwrap_or(&[])));

        let mut out: Vec<(Vec<u8>, IndexEntry)> = Vec::new();
        let mut bytes = 0usize;
        let mut more = false;
        loop {
            if mt == mem.len() && mem_more {
                let mut next = mem.last().expect("drained chunk is non-empty").0.clone();
                next.push(0);
                let (m, mm) = self
                    .index
                    .scan_page(Some(&next), hi, refill, usize::MAX)
                    .ok_or(EngineError::NotSorted)?;
                mem = m;
                mem_more = mm;
                mt = 0;
            }
            let mut min_key: Option<&[u8]> = mem.get(mt).map(|(k, _)| k.as_slice());
            for h in heads.iter().flatten() {
                if min_key.is_none_or(|m| h.key.as_slice() < m) {
                    min_key = Some(h.key.as_slice());
                }
            }
            let Some(min_key) = min_key else { break };
            let min_key = min_key.to_vec();
            let mut win_key: Vec<u8> = Vec::new();
            let mut win: Option<IndexEntry> = None;
            if let Some((k, e)) = mem.get(mt) {
                if k.as_slice() == min_key.as_slice() {
                    win_key = k.clone();
                    win = Some(*e);
                    mt += 1;
                }
            }
            for i in (0..cursors.len()).rev() {
                if heads[i].as_ref().is_some_and(|h| h.key == min_key) {
                    let h = heads[i].take().unwrap();
                    if win.as_ref().is_none_or(|w| h.version > w.version) {
                        win_key = h.key;
                        win = Some(IndexEntry {
                            ptr: h.ptr,
                            seq: h.seq,
                            version: h.version,
                            tombstone: h.tombstone,
                        });
                    }
                    heads[i] = advance(&mut cursors[i], stats)?;
                }
            }
            let entry = win.expect("min_key came from some source");
            if covers.as_mut().is_some_and(|cc| cc.cover(&win_key).is_some_and(|c| entry.version <= c)) {
                stats.filtered_entries += 1;
                continue;
            }
            let sz = win_key.len() + entry.ptr.len as usize;
            if out.len() >= max_entries
                || (!out.is_empty() && bytes.saturating_add(sz) > max_bytes)
                || (!out.is_empty()
                    && out.len().is_multiple_of(DEADLINE_CHECK_EVERY)
                    && deadline.is_some_and(|d| std::time::Instant::now() >= d))
            {
                more = true;
                break;
            }
            bytes = bytes.saturating_add(sz);
            out.push((win_key, entry));
        }
        Ok((out, more))
    }

    pub fn sample_block_key(&self, pick: u64) -> Option<Vec<u8>> {
        let total: u64 = self.runs.iter().map(|or| or.run.index_len() as u64).sum();
        if total == 0 {
            return None;
        }
        let mut at = pick % total;
        for or in &self.runs {
            let blocks = or.run.index_len() as u64;
            if at < blocks {
                return Some(or.run.block_first_key(at as usize).to_vec());
            }
            at -= blocks;
        }
        None
    }
}

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub fn scan_plan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<ScanSpanPage, EngineError> {
        let (entries, more) = self.scan_winners_page(lo, hi, max_entries, max_bytes)?;
        let items = self.value_items(entries.iter().map(|(_, e)| &e.ptr))?;
        let plan = self.log.plan_read_spans(&self.dev, &items)?;
        let keys = entries.into_iter().map(|(k, e)| (k, e.version)).collect();
        Ok((keys, plan, more))
    }

    pub fn stage_scan_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<StagedScanPage, EngineError> {
        if self.runs.is_empty() {
            let (entries, more) = if self.ordering == Ordering::Sorted {
                self.index
                    .scan_page(lo, hi, max_entries, max_bytes)
                    .expect("sorted index")
            } else if hi.is_none() {
                self.index.maintenance_page(lo, max_entries)
            } else {
                return Err(EngineError::NotSorted);
            };
            return Ok(StagedScanPage::Done(StagedScanDone {
                winners: entries,
                more,
                resume: None,
                rt: RtScanStats::default(),
            }));
        }
        let maintenance = self.ordering != Ordering::Sorted && hi.is_none();
        if self.ordering != Ordering::Sorted && !maintenance {
            return Err(EngineError::NotSorted);
        }

        let refill = MEM_REFILL_ENTRIES.min(max_entries.max(1));
        let (mem, mem_more) = if maintenance {
            self.index.maintenance_page(lo, refill)
        } else {
            self.index
                .scan_page(lo, hi, refill, usize::MAX)
                .expect("sorted index")
        };

        let mut cursors: Vec<AsyncRunCursor> = Vec::new();
        for (run_idx, or) in self.runs.iter().enumerate() {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let start = or.run.straddling_start_block(lo);
            let (locators, resume, has_more) =
                or.run
                    .locate_blocks_window(&self.alloc, start, hi, SCAN_BLOCK_WINDOW);
            if locators.is_empty() && !has_more {
                continue;
            }
            cursors.push(AsyncRunCursor::new(
                run_idx,
                locators,
                resume,
                has_more,
                lo.map(|l| l.to_vec()),
                hi.map(|h| h.to_vec()),
                maintenance,
            ));
        }

        Ok(StagedScanPage::Runs(StagedScanMerge {
            hi: hi.map(|h| h.to_vec()),
            maintenance,
            max_entries,
            max_bytes,
            refill,
            storage_generation: self.storage_gen,
            mutable_generation: self.mutable_gen,
            cursors,
            heads: Vec::new(),
            primed: false,
            mem,
            mem_more,
            mt: 0,
            out: Vec::new(),
            bytes: 0,
            more: false,
            cover: None,
        }))
    }

    pub fn stage_scan_page_covered(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        budget: ScanCoveredBudget,
    ) -> Result<StagedScanPage, EngineError> {
        if self.rts.fragments.is_empty() || self.ordering != Ordering::Sorted {
            return self.stage_scan_page(lo, hi, max_entries, max_bytes);
        }
        let set = self.rts.fragments.clone();
        let mut picks = RtScanStats::default();
        let (mem, mem_more) = self
            .index
            .scan_page(lo, hi, MEM_REFILL_ENTRIES, usize::MAX)
            .expect("sorted index");
        let mut cursors: Vec<AsyncRunCursor> = Vec::new();
        for (run_idx, or) in self.runs.iter().enumerate() {
            if or.meta.entry_count == 0
                || lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let start = or.run.straddling_start_block(lo);
            let (locators, resume, has_more) = or.run.locate_blocks_window_picking(
                &self.alloc,
                start,
                hi,
                SCAN_BLOCK_WINDOW,
                |b| pick_block(or, &set, lo, b, &mut picks),
            );
            if locators.is_empty() && !has_more {
                continue;
            }
            cursors.push(AsyncRunCursor::new(
                run_idx,
                locators,
                resume,
                has_more,
                lo.map(|l| l.to_vec()),
                hi.map(|h| h.to_vec()),
                false,
            ));
        }
        let runs = cursors.len();
        Ok(StagedScanPage::Runs(StagedScanMerge {
            hi: hi.map(|h| h.to_vec()),
            maintenance: false,
            max_entries,
            max_bytes,
            refill: MEM_REFILL_ENTRIES,
            storage_generation: self.storage_gen,
            mutable_generation: self.mutable_gen,
            cursors,
            heads: Vec::new(),
            primed: false,
            mem,
            mem_more,
            mt: 0,
            out: Vec::new(),
            bytes: 0,
            more: false,
            cover: Some(Box::new(StagedCover {
                set,
                budget,
                region_end: Some(Vec::new()),
                region_cover: None,
                filtered: 0,
                covered_blocks: 0,
                counted: vec![0; runs],
                popped: Vec::new(),
                picks,
                resume: None,
            })),
        }))
    }

    pub fn scan_mutable_entries(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Option<Vec<(Vec<u8>, IndexEntry)>> {
        self.index.scan(lo, hi)
    }

    pub fn scan_supply_mem(
        &self,
        merge: &mut StagedScanMerge,
        from: &[u8],
    ) -> Result<(), EngineError> {
        let (mem, mem_more) = if merge.maintenance {
            self.index.maintenance_page(Some(from), merge.refill)
        } else {
            self.index
                .scan_page(Some(from), merge.hi.as_deref(), merge.refill, usize::MAX)
                .expect("sorted index")
        };
        merge.mem = mem;
        merge.mem_more = mem_more;
        merge.mt = 0;
        Ok(())
    }

    pub fn scan_supply_blocks(
        &self,
        merge: &mut StagedScanMerge,
        cursor: usize,
    ) -> Result<(), EngineError> {
        let run_idx = merge.cursors[cursor].run_idx;
        let resume = merge.cursors[cursor].resume_block;
        let hi = merge.hi.as_deref();
        let run = &self.runs[run_idx];
        let (locators, next_resume, has_more) = match merge.cover.as_deref_mut() {
            None => run
                .run
                .locate_blocks_window(&self.alloc, resume, hi, SCAN_BLOCK_WINDOW),
            Some(cover) => {
                let lo = merge.cursors[cursor].lo.as_deref();
                let (set, picks) = (&cover.set, &mut cover.picks);
                run.run.locate_blocks_window_picking(&self.alloc, resume, hi, SCAN_BLOCK_WINDOW, |b| {
                    pick_block(run, set, lo, b, picks)
                })
            }
        };
        let c = &mut merge.cursors[cursor];
        c.locators = locators;
        c.next_block = 0;
        c.resume_block = next_resume;
        c.has_more_blocks = has_more;
        c.needs_window = false;
        c.needs_prime = true;
        Ok(())
    }

    pub fn finish_staged_versions(
        &self,
        done: StagedScanDone,
    ) -> Result<(Vec<(Vec<u8>, u128)>, bool), EngineError> {
        let StagedScanDone { winners, more, .. } = done;
        Ok((
            winners.into_iter().map(|(k, e)| (k, e.version)).collect(),
            more,
        ))
    }

    pub fn plan_staged_scan(&self, done: StagedScanDone) -> Result<ScanSpanPage, EngineError> {
        let StagedScanDone { winners, more, .. } = done;
        let items = self.value_items(winners.iter().map(|(_, e)| &e.ptr))?;
        let plan = self.log.plan_read_spans(&self.dev, &items)?;
        let keys = winners.into_iter().map(|(k, e)| (k, e.version)).collect();
        Ok((keys, plan, more))
    }

}

pub type WinnerCount = (usize, Option<Vec<u8>>, bool);

pub type WinnerVersionPage = (Vec<(Vec<u8>, u128)>, bool);

pub struct WinnerCountPlan {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    storage_generation: u64,
    horizon: Option<Vec<u8>>,
    runs: Vec<Vec<run::RangeLocator>>,
}

impl WinnerCountPlan {
    pub fn storage_generation(&self) -> u64 {
        self.storage_generation
    }

    pub fn block_reads(&self) -> usize {
        self.runs.iter().map(Vec::len).sum()
    }

    pub async fn read<D: AsyncRead + BlockDevice>(self, dev: &D) -> WinnerCountBlocks {
        let reads: Vec<_> = self
            .runs
            .iter()
            .flat_map(|locators| locators.iter().map(|l| l.read(dev)))
            .collect();
        let mut done = vals_rt::join_all(reads).await.into_iter();
        let runs = self
            .runs
            .iter()
            .map(|locators| {
                locators
                    .iter()
                    .map(|_| done.next().expect("one result per locator"))
                    .collect()
            })
            .collect();
        WinnerCountBlocks {
            lo: self.lo,
            hi: self.hi,
            storage_generation: self.storage_generation,
            horizon: self.horizon,
            runs,
        }
    }
}

pub struct WinnerCountBlocks {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    storage_generation: u64,
    horizon: Option<Vec<u8>>,
    runs: Vec<Vec<Result<Vec<u8>, SnapshotError>>>,
}

impl WinnerCountBlocks {
    pub fn storage_generation(&self) -> u64 {
        self.storage_generation
    }
}

struct PrefetchedRunCursor {
    blocks: std::vec::IntoIter<Result<Vec<u8>, SnapshotError>>,
    cur: run::BlockCursor,
}

impl PrefetchedRunCursor {
    fn next(
        &mut self,
        lo: Option<&[u8]>,
        bound: Option<&[u8]>,
    ) -> Result<Option<(Vec<u8>, u128)>, EngineError> {
        let corrupt = |e| EngineError::Snapshot(SnapshotError::Corrupt(e));
        loop {
            let Some(e) = self.cur.next_entry().map_err(corrupt)? else {
                let Some(bytes) = self.blocks.next() else {
                    return Ok(None);
                };
                let bytes = bytes.map_err(EngineError::Snapshot)?;
                self.cur = run::BlockCursor::new(bytes, lo, false).map_err(corrupt)?;
                continue;
            };
            if bound.is_some_and(|b| e.key.as_slice() >= b) {
                self.blocks = Vec::new().into_iter();
                self.cur = run::BlockCursor::default();
                return Ok(None);
            }
            if lo.is_some_and(|l| e.key.as_slice() < l) {
                continue;
            }
            return Ok(Some((e.key, e.version)));
        }
    }
}

struct AsyncRunCursor {
    run_idx: usize,
    locators: Vec<run::RangeLocator>,
    resume_block: usize,
    has_more_blocks: bool,
    needs_window: bool,
    needs_prime: bool,
    lo: Option<Vec<u8>>,
    exclusive_lo: bool,
    hi: Option<Vec<u8>>,
    next_block: usize,
    cur: run::BlockCursor,
    reads: u64,
    metered: bool,
}

impl AsyncRunCursor {
    fn new(
        run_idx: usize,
        locators: Vec<run::RangeLocator>,
        resume_block: usize,
        has_more_blocks: bool,
        lo: Option<Vec<u8>>,
        hi: Option<Vec<u8>>,
        exclusive_lo: bool,
    ) -> Self {
        AsyncRunCursor {
            run_idx,
            locators,
            resume_block,
            has_more_blocks,
            needs_window: false,
            needs_prime: false,
            lo,
            exclusive_lo,
            hi,
            next_block: 0,
            cur: run::BlockCursor::default(),
            reads: 0,
            metered: false,
        }
    }

    async fn read_first<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
    ) -> Result<Option<Vec<u8>>, EngineError> {
        match self.locators.first() {
            Some(loc) => {
                self.meter_read(loc);
                Ok(Some(loc.read(dev).await.map_err(EngineError::Snapshot)?))
            }
            None => Ok(None),
        }
    }

    fn meter_read(&self, loc: &run::RangeLocator) {
        if self.metered {
            crate::attribution::SCAN_BLOCKS.add(1);
            crate::attribution::SCAN_BYTES.add(loc.read_bytes() as u64);
        }
    }

    fn open_block(&self, bytes: Vec<u8>) -> Result<run::BlockCursor, EngineError> {
        run::BlockCursor::new(bytes, self.lo.as_deref(), self.exclusive_lo)
            .map_err(|e| EngineError::Snapshot(SnapshotError::Corrupt(e)))
    }

    fn seed_first(&mut self, bytes: Option<Vec<u8>>) -> Result<(), EngineError> {
        if let Some(b) = bytes {
            self.cur = self.open_block(b)?;
            self.next_block = 1;
            self.reads += 1;
        }
        Ok(())
    }

    async fn next<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
    ) -> Result<Option<SnapshotEntry>, EngineError> {
        loop {
            let next = self
                .cur
                .next_entry()
                .map_err(|e| EngineError::Snapshot(SnapshotError::Corrupt(e)))?;
            let Some(e) = next else {
                if self.next_block >= self.locators.len() {
                    if self.has_more_blocks {
                        self.needs_window = true;
                    }
                    return Ok(None);
                }
                self.meter_read(&self.locators[self.next_block]);
                let bytes = self.locators[self.next_block]
                    .read(dev)
                    .await
                    .map_err(EngineError::Snapshot)?;
                self.next_block += 1;
                self.reads += 1;
                self.cur = self.open_block(bytes)?;
                continue;
            };
            if self
                .hi
                .as_ref()
                .is_some_and(|h| e.key.as_slice() >= h.as_slice())
            {
                return Ok(None);
            }
            if self
                .lo
                .as_ref()
                .is_some_and(|l| {
                    if self.exclusive_lo {
                        e.key.as_slice() <= l.as_slice()
                    } else {
                        e.key.as_slice() < l.as_slice()
                    }
                })
            {
                continue;
            }
            return Ok(Some(e));
        }
    }
}

pub enum StagedScanPage {
    Done(StagedScanDone),
    Runs(StagedScanMerge),
}

pub struct StagedScanDone {
    pub(super) winners: Vec<(Vec<u8>, IndexEntry)>,
    pub(super) more: bool,
    pub(super) resume: Option<Vec<u8>>,
    pub(super) rt: RtScanStats,
}

impl StagedScanDone {
    pub fn into_winners(self) -> (Vec<(Vec<u8>, IndexEntry)>, bool) {
        (self.winners, self.more)
    }

    pub fn resume(&self) -> Option<&[u8]> {
        self.resume.as_deref()
    }

    pub fn rt_stats(&self) -> RtScanStats {
        self.rt
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanCoveredBudget {
    pub filtered_entries: u64,
    pub block_reads: u64,
}

pub const SCAN_COVERED_BUDGET: ScanCoveredBudget = ScanCoveredBudget { filtered_entries: 65_536, block_reads: 64 };

const MEM_REFILL_ENTRIES: usize = 4096;

fn pick_block<D: BlockDevice>(or: &OpenRun<D>, set: &FragmentSet, lo: Option<&[u8]>, b: usize, picks: &mut RtScanStats) -> usize {
    let mut s = RtScanStats::default();
    let next = range_delete::rt_next_block(or, set, lo, b, &mut s);
    picks.skipped_blocks += s.skipped_blocks;
    picks.index_probes += s.index_probes;
    next
}

struct StagedCover {
    set: std::sync::Arc<FragmentSet>,
    budget: ScanCoveredBudget,
    region_end: Option<Vec<u8>>,
    region_cover: Option<u128>,
    filtered: u64,
    covered_blocks: u64,
    counted: Vec<u64>,
    popped: Vec<(usize, u64)>,
    picks: RtScanStats,
    resume: Option<Vec<u8>>,
}

impl StagedCover {
    fn covers(&mut self, key: &[u8], version: u128) -> bool {
        if self.region_end.as_deref().is_none_or(|end| key >= end) {
            let (cover, end) = self.set.cover_region(key);
            self.region_cover = cover;
            self.region_end = end;
        }
        self.region_cover.is_some_and(|c| version <= c)
    }
}

pub enum StagedScanStep {
    Done,
    NeedMem {
        from: Vec<u8>,
    },
    NeedBlocks {
        cursor: usize,
    },
}

const SCAN_BLOCK_WINDOW: usize = 64;

pub struct StagedScanMerge {
    hi: Option<Vec<u8>>,
    maintenance: bool,
    max_entries: usize,
    max_bytes: usize,
    refill: usize,
    storage_generation: u64,
    mutable_generation: u64,
    cursors: Vec<AsyncRunCursor>,
    heads: Vec<Option<SnapshotEntry>>,
    primed: bool,
    mem: Vec<(Vec<u8>, IndexEntry)>,
    mem_more: bool,
    mt: usize,
    out: Vec<(Vec<u8>, IndexEntry)>,
    bytes: usize,
    more: bool,
    cover: Option<Box<StagedCover>>,
}

impl StagedScanMerge {
    pub fn meter_server_scan(&mut self) {
        for cursor in &mut self.cursors {
            if !cursor.metered {
                crate::attribution::SCAN_CURSORS.add(1);
                cursor.metered = true;
            }
        }
    }

    pub fn generation(&self) -> u64 {
        self.storage_generation
    }

    pub fn mutable_generation(&self) -> u64 {
        self.mutable_generation
    }

    pub fn aggregate_generation(&self) -> (u64, u64) {
        (self.storage_generation, self.mutable_generation)
    }

    pub fn finish(self) -> StagedScanDone {
        let (resume, rt) = match self.cover {
            None => (None, RtScanStats::default()),
            Some(c) => (
                c.resume,
                RtScanStats {
                    filtered_entries: c.filtered,
                    skipped_blocks: c.picks.skipped_blocks,
                    block_reads: self.cursors.iter().map(|r| r.reads).sum(),
                    index_probes: c.picks.index_probes,
                },
            ),
        };
        StagedScanDone {
            winners: self.out,
            more: self.more,
            resume,
            rt,
        }
    }

    pub async fn step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
    ) -> Result<StagedScanStep, EngineError> {
        if !self.primed {
            let first_reads: Vec<_> = self.cursors.iter().map(|c| c.read_first(dev)).collect();
            let firsts = vals_rt::join_all(first_reads).await;
            for (c, bytes) in self.cursors.iter_mut().zip(firsts) {
                c.seed_first(bytes?)?;
            }
            self.heads = Vec::with_capacity(self.cursors.len());
            for c in &mut self.cursors {
                let h = c.next(dev).await?;
                self.heads.push(h);
            }
            self.primed = true;
        }

        loop {
            for i in 0..self.cursors.len() {
                if self.cursors[i].needs_prime {
                    self.cursors[i].needs_prime = false;
                    let h = self.cursors[i].next(dev).await?;
                    self.heads[i] = h;
                }
            }
            if let Some(cursor) = self.cursors.iter().position(|c| c.needs_window) {
                return Ok(StagedScanStep::NeedBlocks { cursor });
            }
            if self.mt == self.mem.len() && self.mem_more {
                let mut from = self
                    .mem
                    .last()
                    .expect("drained chunk is non-empty")
                    .0
                    .clone();
                if !self.maintenance {
                    from.push(0);
                }
                return Ok(StagedScanStep::NeedMem { from });
            }
            let mut min_key: Option<&[u8]> = self.mem.get(self.mt).map(|(k, _)| k.as_slice());
            for h in self.heads.iter().flatten() {
                if min_key.is_none_or(|m| h.key.as_slice() < m) {
                    min_key = Some(h.key.as_slice());
                }
            }
            let Some(min_key) = min_key else {
                return Ok(StagedScanStep::Done);
            };
            let min_key = min_key.to_vec();
            let mut win_key: Vec<u8> = Vec::new();
            let mut win: Option<IndexEntry> = None;
            if let Some((k, e)) = self.mem.get(self.mt) {
                if k.as_slice() == min_key.as_slice() {
                    win_key = k.clone();
                    win = Some(*e);
                    self.mt += 1;
                }
            }
            for i in (0..self.cursors.len()).rev() {
                if self.heads[i].as_ref().is_some_and(|h| h.key == min_key) {
                    let h = self.heads[i].take().unwrap();
                    if win.as_ref().is_none_or(|w| h.version > w.version) {
                        win_key = h.key;
                        win = Some(IndexEntry {
                            ptr: h.ptr,
                            seq: h.seq,
                            version: h.version,
                            tombstone: h.tombstone,
                        });
                    }
                    if let Some(cover) = self.cover.as_deref_mut() {
                        cover.popped.push((i, self.cursors[i].reads));
                    }
                    let next = self.cursors[i].next(dev).await?;
                    self.heads[i] = next;
                }
            }
            let entry = win.expect("min_key came from some source");
            if let Some(cover) = self.cover.as_deref_mut() {
                let filtered = cover.covers(&win_key, entry.version);
                let StagedCover { popped, counted, covered_blocks, .. } = cover;
                for (i, block) in popped.drain(..) {
                    if filtered && counted[i] != block {
                        counted[i] = block;
                        *covered_blocks += 1;
                    }
                }
                if filtered {
                    cover.filtered += 1;
                    if cover.filtered >= cover.budget.filtered_entries
                        || cover.covered_blocks >= cover.budget.block_reads
                    {
                        win_key.push(0);
                        cover.resume = Some(win_key);
                        self.more = true;
                        return Ok(StagedScanStep::Done);
                    }
                    continue;
                }
            }
            let sz = win_key.len() + entry.ptr.len as usize;
            if self.out.len() >= self.max_entries
                || (!self.out.is_empty() && self.bytes.saturating_add(sz) > self.max_bytes)
            {
                self.more = true;
                return Ok(StagedScanStep::Done);
            }
            self.bytes = self.bytes.saturating_add(sz);
            self.out.push((win_key, entry));
        }
    }
}

