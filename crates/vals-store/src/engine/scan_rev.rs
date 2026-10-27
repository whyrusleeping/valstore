use super::*;

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub fn scan_plan_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<ScanSpanPage, EngineError> {
        let ((winners, more), rt) = self.scan_winners_page_rev_counted(lo, hi, max_entries, max_bytes)?;
        self.plan_staged_scan(StagedScanDone { winners, more, resume: None, rt })
    }
}

impl<D: BlockDevice> Engine<D> {
    pub fn scan_winners_page_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<WinnersPage, EngineError> {
        Ok(self.scan_winners_page_rev_counted(lo, hi, max_entries, max_bytes)?.0)
    }

    pub fn scan_winners_page_rev_counted(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<(WinnersPage, RtScanStats), EngineError> {
        let mut stats = RtScanStats::default();
        if self.ordering != Ordering::Sorted {
            return Err(EngineError::NotSorted);
        }
        if matches!((lo, hi), (Some(l), Some(h)) if l >= h) {
            return Ok(((Vec::new(), false), stats));
        }
        let cover = (!self.rts.fragments.is_empty()).then_some(&*self.rts.fragments);
        if self.runs.is_empty() && cover.is_none() {
            let page = self.index.scan_page_rev(lo, hi, max_entries, max_bytes).ok_or(EngineError::NotSorted)?;
            return Ok((page, stats));
        }
        let refill = 4096.min(max_entries.max(1));
        let (mut mem, mut mem_more) = self.index.scan_page_rev(lo, hi, refill, usize::MAX).ok_or(EngineError::NotSorted)?;
        let mut mt = 0;
        let mut cursors: Vec<(run::RunScanRev<'_, D>, &OpenRun<D>)> = Vec::new();
        let mut heads: Vec<Option<SnapshotEntry>> = Vec::new();
        let advance = |c: &mut (run::RunScanRev<'_, D>, &OpenRun<D>), stats: &mut RtScanStats| match cover {
            None => c.0.next(&self.alloc),
            Some(set) => c.0.next_picking(&self.alloc, |end| range_delete::rt_prev_block(c.1, set, hi, end, stats)),
        };
        for or in &self.runs {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let mut c = (or.run.block_scan_rev(lo, hi), or);
            heads.push(advance(&mut c, &mut stats)?);
            cursors.push(c);
        }
        let mut out = Vec::new();
        let mut bytes = 0usize;
        loop {
            if mt == mem.len() && mem_more {
                let next = mem.last().expect("drained chunk is non-empty").0.clone();
                (mem, mem_more) = self.index.scan_page_rev(lo, Some(&next), refill, usize::MAX).ok_or(EngineError::NotSorted)?;
                mt = 0;
            }
            let mut max_key = mem.get(mt).map(|(k, _)| k.as_slice());
            for h in heads.iter().flatten() {
                if max_key.is_none_or(|m| h.key.as_slice() > m) {
                    max_key = Some(h.key.as_slice());
                }
            }
            let Some(max_key) = max_key else { return Ok(((out, false), stats)) };
            let key = max_key.to_vec();
            let mut win = None;
            if let Some((k, e)) = mem.get(mt) {
                if *k == key {
                    win = Some(*e);
                    mt += 1;
                }
            }
            for i in (0..cursors.len()).rev() {
                if heads[i].as_ref().is_some_and(|h| h.key == key) {
                    let h = heads[i].take().unwrap();
                    if win.as_ref().is_none_or(|w: &IndexEntry| h.version > w.version) {
                        win = Some(IndexEntry { ptr: h.ptr, seq: h.seq, version: h.version, tombstone: h.tombstone });
                    }
                    heads[i] = advance(&mut cursors[i], &mut stats)?;
                }
            }
            let entry = win.expect("max_key came from some source");
            if cover.is_some_and(|set| set.cover(&key).is_some_and(|v| entry.version <= v)) {
                stats.filtered_entries += 1;
                continue;
            }
            let sz = key.len() + entry.ptr.len as usize;
            if out.len() >= max_entries || (!out.is_empty() && bytes.saturating_add(sz) > max_bytes) {
                return Ok(((out, true), stats));
            }
            bytes = bytes.saturating_add(sz);
            out.push((key, entry));
        }
    }
}
