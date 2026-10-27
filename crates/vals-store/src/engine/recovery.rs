use super::*;

struct EngineState<D: BlockDevice> {
    runs: Vec<OpenRun<D>>,
    manifest_gen: u64,
    live_manifest: Option<LiveSnapshot>,
    last_flush_epoch: u64,
    last_flush_seq: u64,
    winner_directory: Option<WinnerDirectory>,
    rts: RtState,
}

impl<D: BlockDevice> Engine<D> {
    fn from_parts(
        dev: Arc<D>,
        alloc: Allocator,
        log: ValueLog,
        index: Box<dyn Index>,
        ordering: Ordering,
        state: EngineState<D>,
    ) -> Self {
        let block_size = dev.block_size();
        Engine {
            dev,
            alloc,
            log,
            index,
            block_size,
            ordering,
            poisoned: false,
            dealloc_error_warned: false,
            runs: state.runs,
            manifest_gen: state.manifest_gen,
            storage_gen: state.manifest_gen,
            mutable_gen: 0,
            live_manifest: state.live_manifest,
            last_flush_epoch: state.last_flush_epoch,
            last_flush_seq: state.last_flush_seq,
            mutable_tombstones: None,
            gc_fold_cursor: 0,
            flush_in_flight: false,
            flush_snapshot: None,
            flush_cleanup: Vec::new(),
            flush_value_segs: std::collections::HashSet::new(),
            compact_in_flight: false,
            runs_removed_epoch: 0,
            compact_cursor: SegmentId(0),
            maint_reserve_enabled: false,
            shadow: ShadowState::default(),
            merge_policy: MergePolicy::from_env(),
            pressure_infeasible_streak: 0,
            gc_excluded_segs: std::collections::HashSet::new(),
            shed_rng: 0x9E37_79B9_7F4A_7C15,
            winner_directory: state.winner_directory,
            key_filter: crate::keyfilter::KeyFilter::new(crate::keyfilter::KeyFilterParams::from_env()),
            cover_scan_token: None,
            cover_scan_backoff: std::collections::HashMap::new(),
            run_pins: std::cell::RefCell::new(pins::RunPins::default()),
            rts: state.rts,
            #[cfg(test)]
            test_orphan_reclaim: true,
            #[cfg(test)]
            test_run_pin_pressure: false,
            #[cfg(test)]
            test_key_filter_knockout: false,
            #[cfg(test)]
            test_kf_knockouts: Default::default(),
            #[cfg(any(test, feature = "test-fixtures"))]
            test_flush_source_flip: None,
            #[cfg(any(test, feature = "test-fixtures"))]
            test_fail_forget_append: None,
        }
    }

    pub fn create(dev: D, cfg: Config) -> Result<Self, EngineError> {
        Self::format_device(&dev, &cfg)?;
        Ok(Self::create_prewiped(dev, cfg))
    }

    fn format_device(dev: &D, cfg: &Config) -> Result<(), EngineError> {
        let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
        let num_segments = alloc.num_segments();
        if num_segments >= 1 << 16 {
            log::info!(
                "engine: formatting device ({num_segments} segment headers, \
                 {seg_blocks}-block segments) before first use",
                seg_blocks = cfg.seg_blocks,
            );
        }
        Self::wipe_segment_headers(dev, &alloc, (0..num_segments).map(SegmentId))?;
        dev.flush()?;
        Ok(())
    }

    fn wipe_segment_headers<I>(
        dev: &D,
        alloc: &Allocator,
        segments: I,
    ) -> Result<(), EngineError>
    where
        I: IntoIterator<Item = SegmentId>,
    {
        let zero = vec![0u8; dev.block_size()];
        let mut inflight: Vec<WriteHandle> = Vec::new();
        let mut error: Option<BlockError> = None;
        let reap = |inflight: &mut Vec<WriteHandle>, error: &mut Option<BlockError>| {
            let mut i = 0;
            while i < inflight.len() {
                if inflight[i].is_done() {
                    let h = inflight.swap_remove(i);
                    if let Err(e) = h.into_result() {
                        if error.is_none() {
                            *error = Some(e);
                        }
                    }
                } else {
                    i += 1;
                }
            }
        };

        'submit: for seg in segments {
            while inflight.len() >= MAX_INFLIGHT_HEADER_WIPES {
                let head_done = inflight.first().is_some_and(|h| h.is_done());
                if dev.poll_writes() > 0 || head_done {
                    reap(&mut inflight, &mut error);
                    if error.is_some() {
                        break 'submit;
                    }
                }
            }
            let (start, _) = alloc.segment_range(seg);
            crate::attribution::write_bytes(6, zero.len());
            inflight.push(dev.submit_write(start, &zero));
        }
        while !inflight.is_empty() {
            let head_done = inflight.first().is_some_and(|h| h.is_done());
            if dev.poll_writes() > 0 || head_done {
                reap(&mut inflight, &mut error);
            }
        }

        match error {
            Some(e) => Err(EngineError::Io(e)),
            None => Ok(()),
        }
    }

    pub fn create_prewiped(dev: D, cfg: Config) -> Self {
        let block_size = dev.block_size();
        let alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
        let log = ValueLog::new(block_size, cfg.seg_blocks, 0, 0);
        let index = new_index(cfg.ordering);
        Self::from_parts(
            Arc::new(dev),
            alloc,
            log,
            index,
            cfg.ordering,
            EngineState {
                runs: Vec::new(),
                manifest_gen: 0,
                live_manifest: None,
                last_flush_epoch: 0,
                last_flush_seq: 0,
                winner_directory: winner_directory_requested().then(WinnerDirectory::default),
                rts: RtState::default(),
            },
        )
    }

    pub fn open(dev: D, cfg: Config) -> Result<Self, EngineError> {
        Self::open_inner(dev, cfg)
    }

    fn open_inner(dev: D, cfg: Config) -> Result<Self, EngineError> {
        let block_size = dev.block_size();
        let mut alloc = Allocator::new(dev.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
        let mut index = new_index(cfg.ordering);
        let seg_bytes = cfg.seg_blocks as usize * block_size;
        let mut buf = vec![0u8; seg_bytes];

        let mut log_segs = crate::dump::scan_log_segments(&dev, &alloc, block_size)?;
        log_segs.sort_by_key(|(_, ep, _)| *ep);

        let manifest = match snapshot_io::load_manifest(&dev, &alloc) {
            Ok(m) => m,
            Err(snapshot_io::SnapshotError::Io(e)) => return Err(EngineError::Io(e)),
            Err(e @ snapshot_io::SnapshotError::Unsupported(_)) => return Err(e.into()),
            Err(_) => None,
        };

        let max_epoch_all: Option<u64> = log_segs.iter().map(|(_, e, _)| *e).max();

        let dev = Arc::new(dev);
        let mut runs: Vec<OpenRun<D>> = Vec::new();
        let (min_replay_epoch, covered_seq, manifest_gen, live_manifest, manifest_driven) =
            match manifest {
                Some((man, live)) => {
                    let covered_epoch = man.covered_epoch;
                    let covered_seq = man.covered_seq;
                    let generation = live.generation;
                    let manifest_driven = man.runs.iter().all(|m| m.value_segs.is_some());

                    let mut allocated: std::collections::HashSet<SegmentId> =
                        std::collections::HashSet::new();
                    for s in &live.segs {
                        allocated.insert(*s);
                    }
                    for meta in &man.runs {
                        for s in &meta.handle.segs {
                            allocated.insert(*s);
                        }
                        if manifest_driven {
                            if let Some(vs) = &meta.value_segs {
                                allocated.extend(vs.iter().copied());
                            }
                        }
                    }
                    if manifest_driven {
                        for (seg, ep, not_tail) in &log_segs {
                            if !*not_tail && *ep >= covered_epoch {
                                allocated.insert(*seg);
                            }
                        }
                    } else {
                        for (seg, _ep, _nt) in &log_segs {
                            allocated.insert(*seg);
                        }
                    }
                    for s in &allocated {
                        alloc
                            .mark_allocated(*s)
                            .expect("recover: segment marked twice");
                    }
                    for meta in man.runs {
                        let run = DeviceRun::open(dev.clone(), &alloc, meta.handle.clone())?;
                        let rts = run.read_range_tombstones(&alloc)?;
                        runs.push(OpenRun::new(meta, run, false).with_rts(rts));
                    }
                    (
                        Some(covered_epoch),
                        Some(covered_seq),
                        generation,
                        Some(live),
                        manifest_driven,
                    )
                }
                None => (None, None, 0, None, false),
            };

        let mut winners: Winners = std::collections::HashMap::new();
        let mut replayed_rts: Vec<(RtRecord, ValuePtr)> = Vec::new();
        let (max_seq, _max_epoch) = Self::replay_winners(
            &dev,
            &mut alloc,
            &mut buf,
            &log_segs,
            &mut winners,
            &mut replayed_rts,
            min_replay_epoch,
            /* mark */ !manifest_driven,
        )?;
        let replay_cover = (!replayed_rts.is_empty()).then(|| FragmentSet::from_records(replayed_rts.iter().map(|(r, _)| r)));
        let rts = RtState::recover(
            runs.iter().flat_map(|or| or.rts.iter()),
            replayed_rts.into_iter().filter(|(r, _)| covered_seq.is_none_or(|c| r.seq > c)),
            covered_seq.unwrap_or(0),
        );
        let flushed_through_seq = covered_seq.filter(|&c| c > 0);
        let mut mutable_tombstones: Option<(u128, u64)> = None;
        for (key, (seq, kind, tombstone, ptr, version)) in winners {
            if kind == RecordKind::Put
                && !flushed_through_seq.is_some_and(|c| seq <= c)
                && !replay_cover.as_ref().is_some_and(|c| range_delete::is_covered(c.cover(&key), version))
            {
                if tombstone {
                    mutable_tombstones = Some(note_mutable_tombstone(mutable_tombstones, version, seq));
                }
                index.put(
                    &key,
                    IndexEntry {
                        ptr,
                        seq,
                        version,
                        tombstone,
                    },
                );
            }
        }

        let max_seq = match (max_seq, covered_seq) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        let next_seq = max_seq.map_or(0, |m| m + 1);
        let next_epoch = resume_next_epoch(max_epoch_all, min_replay_epoch);
        let mut log = ValueLog::new(block_size, cfg.seg_blocks, next_seq, next_epoch);
        {
            let watermark = min_replay_epoch.unwrap_or(0);
            log.seed_tail_segs(
                log_segs
                    .iter()
                    .filter(|(_, ep, not_tail)| !*not_tail && *ep >= watermark)
                    .map(|(seg, ep, _)| (*ep, *seg)),
            );
        }

        let mut engine = Self::from_parts(
            dev,
            alloc,
            log,
            index,
            cfg.ordering,
            EngineState {
                runs,
                manifest_gen,
                live_manifest,
                last_flush_epoch: min_replay_epoch.unwrap_or(0),
                last_flush_seq: covered_seq.unwrap_or(0),
                winner_directory: None,
                rts,
            },
        );
        engine.mutable_tombstones = mutable_tombstones;
        engine.refresh_data_reserve();
        engine.build_range_tombstone_max_trees();
        if winner_directory_requested() {
            let directory = engine.build_winner_directory()?;
            engine.winner_directory = Some(directory);
        }
        Ok(engine)
    }

    pub(super) fn runs_bucket_granularity(&self) -> u32 {
        self.runs.first().map_or(0, |or| or.run.num_buckets())
    }

    #[allow(clippy::too_many_arguments)]
    fn replay_winners(
        dev: &D,
        alloc: &mut Allocator,
        buf: &mut [u8],
        log_segs: &[(SegmentId, u64, bool)],
        winners: &mut Winners,
        rts: &mut Vec<(RtRecord, ValuePtr)>,
        min_replay_epoch: Option<u64>,
        mark: bool,
    ) -> Result<(Option<u64>, Option<u64>), EngineError> {
        let mut max_seq: Option<u64> = None;
        let mut max_epoch: Option<u64> = None;
        for (seg, seg_epoch, not_tail) in log_segs {
            if mark {
                alloc
                    .mark_allocated(*seg)
                    .expect("recover: segment marked twice");
            }
            max_epoch = Some(max_epoch.map_or(*seg_epoch, |m| m.max(*seg_epoch)));
            if *not_tail {
                continue;
            }
            if min_replay_epoch.is_some_and(|min| *seg_epoch < min) {
                continue;
            }
            let (start, _) = alloc.segment_range(*seg);
            dev.read_at(start, buf)?;
            let (_hdr, recs) = replay_segment(buf)?;
            for (off, rec) in recs {
                let ptr = ValuePtr {
                    segment: *seg,
                    offset: off,
                    len: (HEADER_LEN + rec.key.len() + rec.value.len()) as u32,
                };
                max_seq = Some(max_seq.map_or(rec.seq, |m| m.max(rec.seq)));
                if rec.kind == RecordKind::RangeDelete {
                    rts.push((range_delete::decode_record(&rec)?, ptr));
                    continue;
                }
                match winners.entry(rec.key) {
                    std::collections::hash_map::Entry::Occupied(mut o) => {
                        if rec.version > o.get().4
                            || (rec.version == o.get().4 && rec.seq > o.get().0)
                        {
                            o.insert((rec.seq, rec.kind, rec.tombstone, ptr, rec.version));
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(v) => {
                        v.insert((rec.seq, rec.kind, rec.tombstone, ptr, rec.version));
                    }
                }
            }
        }
        Ok((max_seq, max_epoch))
    }

    pub fn truncate(&mut self) -> Result<(), EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.flush_in_flight || self.compact_in_flight {
            return Err(EngineError::MaintenanceInFlight);
        }
        let old_used = self.alloc.used_list();
        let used_set: std::collections::HashSet<SegmentId> = old_used.iter().copied().collect();
        let non_used = (0..self.alloc.num_segments())
            .map(SegmentId)
            .filter(|seg| !used_set.contains(seg));
        Self::wipe_segment_headers(self.dev.as_ref(), &self.alloc, non_used)?;
        self.dev.flush()?;

        Self::wipe_segment_headers(self.dev.as_ref(), &self.alloc, old_used.iter().copied())?;
        self.dev.flush()?;
        for &seg in &old_used {
            self.alloc
                .free(seg)
                .expect("truncate: freeing a used segment");
        }
        self.index = new_index(self.ordering);
        if let Some(directory) = self.winner_directory.as_mut() {
            directory.clear();
        }
        self.log = ValueLog::new(
            self.block_size,
            self.alloc.blocks_per_segment(),
            self.log.next_seq(),
            self.log.next_epoch(),
        );
        self.manifest_gen += 1;
        self.bump_storage_generation();
        self.bump_mutable_generation();
        self.runs.clear();
        self.rts = RtState::default();
        self.forget_run_pins_on_truncate();
        self.runs_removed_epoch += 1;
        self.key_filter.reset();
        self.cover_scan_backoff.clear();
        self.live_manifest = None;
        self.last_flush_epoch = 0;
        self.last_flush_seq = 0;
        self.mutable_tombstones = None;
        self.refresh_data_reserve();
        Ok(())
    }

    pub fn device(&self) -> &D {
        self.dev.as_ref()
    }

    pub fn config(&self) -> Config {
        Config {
            seg_blocks: self.alloc.blocks_per_segment(),
            reserved_blocks: self.alloc.reserved_blocks(),
            ordering: self.ordering,
        }
    }

    pub fn device_handle(&self) -> Arc<D> {
        self.dev.clone()
    }

    pub fn vlog_inflight_writes(&self) -> usize {
        self.log.inflight_writes()
    }

    #[cfg(test)]
    pub(super) fn into_device(self) -> D {
        Arc::try_unwrap(self.dev)
            .ok()
            .expect("into_device: a device handle is still shared")
    }

    pub fn manifest_generation(&self) -> u64 {
        self.manifest_gen
    }

    pub fn storage_generation(&self) -> u64 {
        self.storage_gen
    }

    pub fn mutable_generation(&self) -> u64 {
        self.mutable_gen
    }

    pub fn mutable_entry(&self, key: &[u8]) -> Option<IndexEntry> {
        self.index.get(key)
    }

    pub(super) fn bump_storage_generation(&mut self) {
        self.storage_gen = self.storage_gen.wrapping_add(1);
    }

    pub(super) fn bump_mutable_generation(&mut self) {
        self.mutable_gen = self.mutable_gen.wrapping_add(1);
    }
}

