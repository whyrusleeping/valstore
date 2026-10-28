use super::*;

pub enum FlushPlanStep<D: BlockDevice> {
    Pending,
    Ready(FlushPlan<D>),
}

pub enum FlushCommitStep {
    Pending,
    Ready(RunMeta),
}

pub struct FlushCommit {
    entries: Vec<SnapshotEntry>,
    meta: RunMeta,
    covered_seq: u64,
}

impl FlushCommit {
    pub fn remaining_entries(&self) -> usize {
        self.entries.len()
    }
}

pub const FLUSH_SLICE_ENTRIES: usize = 4 * 1024;
pub const FLUSH_COMMIT_SLICE_ENTRIES: usize = 512;

pub(super) struct FlushSnapshotState {
    opts: RunBuildOpts,
    pub(super) entries: Vec<SnapshotEntry>,
    groups: Vec<crate::index::SourceGroup>,
    after: Option<Vec<u8>>,
    key_bytes: usize,
    max_key: usize,
    covered_seq: u64,
    covered_epoch: u64,
    snapshot_us: u64,
}

pub struct FlushPlan<D: BlockDevice> {
    entries: Vec<SnapshotEntry>,
    groups: Vec<crate::index::SourceGroup>,
    opts: RunBuildOpts,
    run_version: u16,
    segs: Vec<SegmentId>,
    seg_lbas: Vec<u64>,
    seg_bytes: usize,
    block_size: usize,
    covered_seq: u64,
    covered_epoch: u64,
    dev: Arc<D>,
    sync_us: u64,
    snapshot_us: u64,
    paced: bool,
    key_sink: Option<crate::keyfilter::KeyFilterSink>,
    rts: Vec<RtRecord>,
    rt_filter: Option<FragmentSet>,
    rt_floor: u64,
    rt_accounted_seq: u64,
    #[cfg(any(test, feature = "test-fixtures"))]
    test_source_flip: Option<(usize, usize, u8)>,
}

pub struct WrittenFlush<D: BlockDevice> {
    handle: crate::run::RunHandle,
    entries: Vec<SnapshotEntry>,
    covered_seq: u64,
    covered_epoch: u64,
    min_key: Vec<u8>,
    max_key: Vec<u8>,
    entry_count: u64,
    value_segs: Vec<SegmentId>,
    value_seg_bytes: Vec<u64>,
    value_bytes: u64,
    tombstone_min_version: Option<u128>,
    unused_segs: Vec<SegmentId>,
    parts: crate::run::RunParts,
    dev: Arc<D>,
    key_sink: Option<crate::keyfilter::KeyFilterSink>,
    rts: Vec<RtRecord>,
    rt_dominated: usize,
    covered: Vec<SnapshotEntry>,
}

pub async fn build_and_write_flush<D: BlockDevice>(
    mut plan: FlushPlan<D>,
    write_dev: &D,
) -> Result<WrittenFlush<D>, (Vec<SegmentId>, EngineError)> {
    #[cfg(any(test, feature = "test-fixtures"))]
    if let Some((entry, byte, mask)) = plan.test_source_flip {
        plan.entries[entry].key[byte] ^= mask;
    }
    let covered = match plan.rt_filter.take() {
        Some(filter) => {
            let (kept, covered) = range_delete::split_covered(std::mem::take(&mut plan.entries), &mut plan.groups, &filter);
            plan.entries = kept;
            covered
        }
        None => Vec::new(),
    };
    let rt_dominated = crate::range_tombstone::drop_dominated(&mut plan.rts, plan.rt_accounted_seq);
    let entry_count = plan.entries.len() as u64;
    let (value_segs, value_seg_bytes, value_bytes) = value_liveness(&plan.entries);
    let tombstone_min_version = tombstone_min_version(&plan.entries);
    if let Some(sink) = plan.key_sink.as_mut() {
        for e in &plan.entries {
            sink.insert(&e.key);
        }
    }

    vals_rt::yield_now().await;
    let (handle, built) = {
        let mut sink = run::SegmentStreamSink::new(
            write_dev,
            &plan.segs,
            &plan.seg_lbas,
            plan.seg_bytes,
            plan.block_size,
        )
        .write_source(3)
        .paced(plan.paced)
        .flush_every(if plan.paced {
            crate::pace::maint_flush_bytes()
        } else {
            0
        });
        let built = match run::build_run_into_checked_rts(
            &plan.entries,
            plan.opts,
            plan.run_version,
            run::EncodePath::Flush,
            run::RunSource::Attested(&plan.groups),
            run::RunRts { seq_floor: plan.rt_floor, records: &plan.rts },
            &mut sink,
        ) {
            Ok(b) => b,
            Err(e) => return Err((plan.segs, EngineError::EncodeIntegrity(e))),
        };
        match sink.finish().await {
            Ok(h) => (h, built),
            Err(e) => return Err((plan.segs, EngineError::from(e))),
        }
    };
    let n_used = handle.segs.len();
    let unused_segs = plan.segs[n_used..].to_vec();
    let parts_t = std::time::Instant::now();
    let parts =
        match run::read_run_parts(write_dev, &handle, &plan.seg_lbas[..n_used], plan.seg_bytes) {
            Ok(p) => p,
            Err(e) => return Err((plan.segs, EngineError::from(e))),
        };
    if plan.paced {
        crate::pace::maint_pace_charge(parts_t.elapsed());
    }
    Ok(WrittenFlush {
        handle,
        entries: plan.entries,
        covered_seq: plan.covered_seq,
        covered_epoch: plan.covered_epoch,
        min_key: built.min_key,
        max_key: built.max_key,
        entry_count,
        value_segs,
        value_seg_bytes,
        value_bytes,
        tombstone_min_version,
        unused_segs,
        parts,
        dev: plan.dev,
        key_sink: plan.key_sink,
        rts: plan.rts,
        rt_dominated,
        covered,
    })
}

#[cfg(test)]
impl<D: BlockDevice> WrittenFlush<D> {
    pub(crate) fn key_sink_for_test(&self) -> Option<&crate::keyfilter::KeyFilterSink> {
        self.key_sink.as_ref()
    }
}

impl<D: BlockDevice> FlushPlan<D> {
    pub fn segments(&self) -> &[SegmentId] {
        &self.segs
    }

    pub fn serving_dev(&self) -> &D {
        &self.dev
    }

    pub fn phase_us(&self) -> (u64, u64) {
        (self.sync_us, self.snapshot_us)
    }

    pub fn set_unpaced(&mut self) {
        self.paced = false;
    }
}

impl<D: BlockDevice> Engine<D> {
    /// Writes out the buffered value log and flushes the device, so every
    /// earlier successful write survives a crash. How durable a device flush
    /// is depends on the device (for a file, `fsync`; a memory device has no
    /// durability). On error, treat the writes since the last successful sync
    /// as possibly lost.
    pub fn sync(&mut self) -> Result<(), EngineError> {
        self.log.sync(&self.dev)?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn flip_memtable_bit_for_test(&mut self, block: usize, offset: usize, mask: u8) {
        self.index.flip_packed_bit(block, offset, mask);
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn set_flush_source_flip_for_test(&mut self, flip: Option<(usize, usize, u8)>) {
        self.test_flush_source_flip = flip;
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn set_mutable_ptr_for_test(&mut self, key: &[u8], ptr: ValuePtr) {
        let entry = self.index.get(key).expect("key is in the memtable");
        self.index.put(key, IndexEntry { ptr, ..entry });
    }

    /// Everything `sync` does before the device cache flush: after this
    /// returns, every record appended so far is written (not yet flushed).
    pub fn sync_prepare(&mut self) -> Result<(), EngineError> {
        self.log.drain_and_flush_tail(&self.dev)?;
        Ok(())
    }

    pub fn flush(&mut self, opts: RunBuildOpts) -> Result<Option<RunMeta>, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.index.is_empty() && !self.has_unflushed_range_tombstones() {
            return Ok(None);
        }
        self.log.sync(&self.dev)?;

        let covered_seq = self.log.next_seq().saturating_sub(1);
        let covered_epoch = self.log.next_epoch().saturating_sub(1);
        let mut rts = self.unflushed_rts_through(covered_seq);
        let rt_dominated = crate::range_tombstone::drop_dominated(&mut rts, self.rts.accounted_seq.get());
        let (rt_filter, rt_floor) = self.rt_flush_filter(covered_seq);

        let mut groups = Vec::new();
        let (page, _) = self.index.flush_page_attested(None, usize::MAX, covered_seq, &mut groups)
            .map_err(|e| {
                log::error!("memtable flush aborted owner={:?}: {e}", std::thread::current().name());
                EngineError::MemtableIntegrity(Box::new(e))
            })?;
        #[allow(unused_mut)]
        let mut entries: Vec<SnapshotEntry> = page.into_iter().map(|(key, entry)| SnapshotEntry {
            key,
            ptr: entry.ptr,
            seq: entry.seq,
            version: entry.version,
            tombstone: entry.tombstone,
        }).collect();
        #[cfg(any(test, feature = "test-fixtures"))]
        if let Some((entry, byte, mask)) = self.test_flush_source_flip {
            entries[entry].key[byte] ^= mask;
        }
        let (entries, covered_entries) = match rt_filter {
            Some(filter) => range_delete::split_covered(entries, &mut groups, &filter),
            None => (entries, Vec::new()),
        };
        let entry_count = entries.len() as u64;

        let mut key_sink = self.key_filter_sink(SinkKind::Flush, entry_count);
        if let Some(sink) = key_sink.as_mut() {
            for e in &entries {
                sink.insert(&e.key);
            }
        }
        let (run_bytes, built) = run::build_run_checked_rts(
            &entries,
            opts,
            self.flush_run_version(&rts),
            run::EncodePath::Flush,
            run::RunSource::Attested(&groups),
            run::RunRts { seq_floor: rt_floor, records: &rts },
        )
        .map_err(EngineError::EncodeIntegrity)?;
        let (min_key, max_key) = (built.min_key, built.max_key);
        let handle = run::write_run_source(&*self.dev, &mut self.alloc, &run_bytes, 3)?;
        let run = match DeviceRun::open(self.dev.clone(), &self.alloc, handle.clone()) {
            Ok(r) => r,
            Err(e) => {
                for s in &handle.segs {
                    let _ = self.alloc.free(*s);
                }
                return Err(e.into());
            }
        };
        let (mut value_segs, mut value_seg_bytes, value_bytes) = value_liveness(&entries);
        {
            let mut seen: std::collections::HashSet<SegmentId> =
                value_segs.iter().copied().collect();
            for seg in self.log.tail_segs_through(covered_epoch) {
                if seen.insert(seg) {
                    value_segs.push(seg);
                    value_seg_bytes.push(0);
                }
            }
        }
        let meta = RunMeta {
            handle: handle.clone(),
            min_key,
            max_key,
            entry_count,
            level: 0,
            value_segs: Some(value_segs),
            value_bytes: Some(value_bytes),
            tombstone_min_version: tombstone_min_version(&entries),
            value_seg_bytes: Some(value_seg_bytes),
        };

        let generation = self.manifest_gen + 1;
        let mut run_metas: Vec<RunMeta> = self.runs.iter().map(|r| r.meta.clone()).collect();
        run_metas.push(meta.clone());
        let manifest = Manifest {
            generation,
            covered_seq,
            covered_epoch,
            runs: run_metas,
        };
        let live = snapshot_io::write_manifest(
            &*self.dev,
            &mut self.alloc,
            &manifest,
            generation,
            self.live_manifest.as_ref(),
        )
        .map_err(EngineError::from)?;
        self.manifest_gen = generation;
        self.live_manifest = Some(live);
        self.last_flush_epoch = covered_epoch;
        self.last_flush_seq = covered_seq;
        if self.mutable_tombstones.is_some_and(|(_, seq)| seq <= covered_seq) {
            self.mutable_tombstones = None;
        }
        self.log.prune_tail_segs_through(covered_epoch);
        self.retire_flushed_rts(covered_seq);
        range_delete::note_covered_dropped(covered_entries.len() as u64);
        range_delete::note_dominated_dropped(rt_dominated);

        let covered = key_sink.as_ref().is_some_and(|s| self.key_filter.owns(s));
        let rebuild_covered = key_sink.as_ref().is_some_and(|s| self.key_filter.owns_rebuild(s));
        drop(key_sink);
        let mut opened = OpenRun::new(meta.clone(), run, covered).with_rts(rts).with_max_tree(!self.rts.fragments.is_empty());
        opened.rebuild_covered = rebuild_covered;
        self.note_run_rts_added(&opened.rts);
        self.runs.push(opened);
        for e in entries.iter().chain(&covered_entries) {
            let captured = IndexEntry {
                ptr: e.ptr,
                seq: e.seq,
                version: e.version,
                tombstone: e.tombstone,
            };
            self.index.remove_if_eq(&e.key, &captured);
        }
        self.bump_storage_generation();
        self.bump_mutable_generation();
        self.key_filter_maintain();
        self.rts.completed_flush_seq = covered_seq;
        Ok(Some(meta))
    }

    pub fn flush_plan(&mut self, opts: RunBuildOpts) -> Result<Option<FlushPlan<D>>, EngineError> {
        loop {
            match self.flush_plan_step(opts, FLUSH_SLICE_ENTRIES)? {
                None => return Ok(None),
                Some(FlushPlanStep::Pending) => {}
                Some(FlushPlanStep::Ready(plan)) => return Ok(Some(plan)),
            }
        }
    }

    pub fn flush_plan_step(
        &mut self,
        opts: RunBuildOpts,
        max_entries: usize,
    ) -> Result<Option<FlushPlanStep<D>>, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if !self.flush_cleanup.is_empty() {
            self.flush_cleanup_step(max_entries);
            return Ok(Some(FlushPlanStep::Pending));
        }
        if self.flush_snapshot.is_none() {
            if (self.index.is_empty() && !self.has_unflushed_range_tombstones()) || self.flush_in_flight {
                return Ok(None);
            }
            self.flush_in_flight = true;
            self.flush_value_segs.clear();
            self.flush_snapshot = Some(FlushSnapshotState {
                opts,
                entries: Vec::with_capacity(self.index.len()),
                groups: Vec::new(),
                after: None,
                key_bytes: 0,
                max_key: 0,
                covered_seq: self.log.next_seq().saturating_sub(1),
                covered_epoch: self.log.next_epoch().saturating_sub(1),
                snapshot_us: 0,
            });
        }

        let mut state = self.flush_snapshot.take().expect("snapshot state armed");
        let snap_t = std::time::Instant::now();
        let (page, more) = match self.index.flush_page_attested(
            state.after.as_deref(),
            max_entries.max(1),
            state.covered_seq,
            &mut state.groups,
        ) {
            Ok(page) => page,
            Err(e) => {
                log::error!("memtable flush aborted owner={:?}: {e}", std::thread::current().name());
                self.defer_flush_cleanup(state.entries);
                self.flush_value_segs.clear();
                self.flush_in_flight = false;
                return Err(EngineError::MemtableIntegrity(Box::new(e)));
            }
        };
        for (key, entry) in page {
            state.key_bytes += key.len();
            state.max_key = state.max_key.max(key.len());
            state.after = Some(key.clone());
            self.flush_value_segs.insert(entry.ptr.segment);
            state.entries.push(SnapshotEntry {
                key,
                ptr: entry.ptr,
                seq: entry.seq,
                version: entry.version,
                tombstone: entry.tombstone,
            });
        }
        state.snapshot_us += snap_t.elapsed().as_micros() as u64;
        if more {
            self.flush_snapshot = Some(state);
            return Ok(Some(FlushPlanStep::Pending));
        }

        let sync_t = std::time::Instant::now();
        if let Err(e) = self.log.drain_and_flush_tail(&self.dev) {
            self.defer_flush_cleanup(state.entries);
            self.flush_value_segs.clear();
            self.flush_in_flight = false;
            return Err(e.into());
        }
        let sync_us = sync_t.elapsed().as_micros() as u64;
        let rts = self.unflushed_rts_through(state.covered_seq);
        let (rt_filter, rt_floor) = self.rt_flush_filter(state.covered_seq);
        let run_version = self.flush_run_version(&rts);
        let bound = run::run_len_upper_bound_agg(
            state.entries.len(),
            state.key_bytes,
            state.max_key,
            state.opts,
            run_version,
        ) + crate::range_tombstone::rt_block_len(&rts);
        let segs = match run::allocate_run_segs(&mut self.alloc, self.block_size, bound) {
            Ok(segs) => segs,
            Err(e) => {
                self.defer_flush_cleanup(state.entries);
                self.flush_value_segs.clear();
                self.flush_in_flight = false;
                return Err(e.into());
            }
        };
        let seg_lbas = segs
            .iter()
            .map(|s| self.alloc.segment_range(*s).0)
            .collect();
        let key_sink = self.key_filter_sink(SinkKind::Flush, state.entries.len() as u64);
        let plan = FlushPlan {
            entries: state.entries,
            groups: state.groups,
            opts: state.opts,
            run_version,
            segs,
            seg_lbas,
            seg_bytes: self.alloc.blocks_per_segment() as usize * self.block_size,
            block_size: self.block_size,
            covered_seq: state.covered_seq,
            covered_epoch: state.covered_epoch,
            dev: self.dev.clone(),
            sync_us,
            snapshot_us: state.snapshot_us,
            paced: true,
            key_sink,
            rts,
            rt_filter,
            rt_floor,
            rt_accounted_seq: self.rts.accounted_seq.get(),
            #[cfg(any(test, feature = "test-fixtures"))]
            test_source_flip: self.test_flush_source_flip,
        };
        Ok(Some(FlushPlanStep::Ready(plan)))
    }

    pub fn flush_commit(&mut self, written: WrittenFlush<D>) -> Result<RunMeta, EngineError> {
        let mut commit = self.flush_commit_start(written)?;
        loop {
            match self.flush_commit_step(&mut commit, FLUSH_SLICE_ENTRIES) {
                FlushCommitStep::Pending => {}
                FlushCommitStep::Ready(meta) => return Ok(meta),
            }
        }
    }

    pub fn flush_commit_start(
        &mut self,
        mut written: WrittenFlush<D>,
    ) -> Result<FlushCommit, EngineError> {
        for s in &written.unused_segs {
            let _ = self.alloc.free(*s);
        }
        let covered_dropped = written.covered.len() as u64;
        written.entries = range_delete::merge_sorted_entries(std::mem::take(&mut written.entries), std::mem::take(&mut written.covered));

        let mut value_segs = written.value_segs;
        let mut value_seg_bytes = written.value_seg_bytes;
        let value_bytes = written.value_bytes;
        {
            let mut seen: std::collections::HashSet<SegmentId> =
                value_segs.iter().copied().collect();
            for seg in self.log.tail_segs_through(written.covered_epoch) {
                if seen.insert(seg) {
                    value_segs.push(seg);
                    value_seg_bytes.push(0);
                }
            }
        }
        let meta = RunMeta {
            handle: written.handle.clone(),
            min_key: written.min_key,
            max_key: written.max_key,
            entry_count: written.entry_count,
            level: 0,
            value_segs: Some(value_segs),
            value_bytes: Some(value_bytes),
            tombstone_min_version: written.tombstone_min_version,
            value_seg_bytes: Some(value_seg_bytes),
        };

        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.dev.block_size();
        let run = DeviceRun::from_parts(
            written.dev,
            seg_bytes,
            written.handle.clone(),
            written.parts,
        );

        let generation = self.manifest_gen + 1;
        let mut run_metas: Vec<RunMeta> = self.runs.iter().map(|r| r.meta.clone()).collect();
        run_metas.push(meta.clone());
        let manifest = Manifest {
            generation,
            covered_seq: written.covered_seq,
            covered_epoch: written.covered_epoch,
            runs: run_metas,
        };
        let live = match snapshot_io::write_manifest(
            &*self.dev,
            &mut self.alloc,
            &manifest,
            generation,
            self.live_manifest.as_ref(),
        ) {
            Ok(l) => l,
            Err(e) => {
                drop(run);
                self.defer_flush_cleanup(std::mem::take(&mut written.entries));
                self.free_flush_segs(&written.handle.segs);
                return Err(EngineError::from(e));
            }
        };

        self.manifest_gen = generation;
        self.live_manifest = Some(live);
        self.last_flush_epoch = written.covered_epoch;
        self.last_flush_seq = written.covered_seq;
        if self.mutable_tombstones.is_some_and(|(_, seq)| seq <= written.covered_seq) {
            self.mutable_tombstones = None;
        }
        self.log.prune_tail_segs_through(written.covered_epoch);
        self.retire_flushed_rts(written.covered_seq);
        range_delete::note_covered_dropped(covered_dropped);
        range_delete::note_dominated_dropped(written.rt_dominated);
        let covered = written.key_sink.as_ref().is_some_and(|s| self.key_filter.owns(s));
        let rebuild_covered = written.key_sink.as_ref().is_some_and(|s| self.key_filter.owns_rebuild(s));
        drop(written.key_sink.take());
        let mut opened = OpenRun::new(meta.clone(), run, covered)
            .with_rts(std::mem::take(&mut written.rts))
            .with_max_tree(!self.rts.fragments.is_empty());
        opened.rebuild_covered = rebuild_covered;
        self.note_run_rts_added(&opened.rts);
        self.runs.push(opened);
        self.flush_value_segs.clear();
        if self.flush_value_segs.capacity() > 4096 {
            self.flush_value_segs.shrink_to(4096);
        }
        self.bump_storage_generation();
        self.key_filter_maintain();

        Ok(FlushCommit {
            entries: written.entries,
            meta,
            covered_seq: written.covered_seq,
        })
    }

    pub fn flush_commit_step(
        &mut self,
        commit: &mut FlushCommit,
        max_entries: usize,
    ) -> FlushCommitStep {
        let start = commit.entries.len().saturating_sub(max_entries.max(1));
        let removed = self.index.remove_if_eq_rev(&commit.entries[start..]);
        commit.entries.truncate(start);
        for _ in 0..removed {
            self.bump_mutable_generation();
        }
        if !commit.entries.is_empty() {
            FlushCommitStep::Pending
        } else {
            self.flush_in_flight = false;
            self.pressure_infeasible_streak = 0;
            self.rts.completed_flush_seq = commit.covered_seq;
            self.refresh_data_reserve();
            FlushCommitStep::Ready(commit.meta.clone())
        }
    }

    pub fn free_flush_segs(&mut self, segs: &[SegmentId]) {
        self.free_unreferenced_staged_segs(segs);
        if let Some(snapshot) = self.flush_snapshot.take() {
            self.defer_flush_cleanup(snapshot.entries);
        }
        self.flush_value_segs.clear();
        self.flush_in_flight = false;
        self.pressure_infeasible_streak = 0;
    }

    pub fn flush_cleanup_pending(&self) -> bool {
        !self.flush_cleanup.is_empty()
    }

    fn defer_flush_cleanup(&mut self, entries: Vec<SnapshotEntry>) {
        debug_assert!(self.flush_cleanup.is_empty());
        self.flush_cleanup = entries;
    }

    fn flush_cleanup_step(&mut self, max_entries: usize) {
        for _ in 0..max_entries.max(1) {
            if self.flush_cleanup.pop().is_none() {
                break;
            }
        }
        if self.flush_cleanup.is_empty() {
            self.flush_cleanup = Vec::new();
        }
    }

    pub fn free_compact_segs(&mut self, segs: &[SegmentId]) {
        self.free_unreferenced_staged_segs(segs);
        self.compact_in_flight = false;
        self.shadow.pending = None;
        self.pressure_infeasible_streak = 0;
    }

    fn free_unreferenced_staged_segs(&mut self, segs: &[SegmentId]) {
        let protected = self.protected_segments();
        for s in segs {
            if !protected.contains(s) {
                let _ = self.alloc.free(*s);
            }
        }
    }

}

