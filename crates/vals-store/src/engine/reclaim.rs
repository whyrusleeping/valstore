use super::*;

fn failed_after_gc_drops(dropped: Vec<(Vec<u8>, u128)>, source: EngineError) -> EngineError {
    if dropped.is_empty() {
        source
    } else {
        EngineError::PartialForget { dropped, source: Box::new(source) }
    }
}

impl<D: BlockDevice> Engine<D> {
    pub(super) fn protected_segments(&self) -> std::collections::HashSet<SegmentId> {
        let mut keep: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        for or in &self.runs {
            if let Some(vs) = &or.meta.value_segs {
                keep.extend(vs.iter().copied());
            }
            keep.extend(or.meta.handle.segs.iter().copied());
        }
        if let Some(live) = &self.live_manifest {
            keep.extend(live.segs.iter().copied());
        }
        self.extend_pinned_segments(&mut keep);
        if let Some(open) = self.log.open_segment() {
            keep.insert(open);
        }
        keep.extend(self.log.tail_segs_through(u64::MAX));
        keep.extend(self.flush_value_segs.iter().copied());
        self.for_each_mutable_value_segment(&mut |seg| {
                keep.insert(seg);
            });
        keep
    }

    pub(super) fn referenced_segments(&self) -> Option<std::collections::HashSet<SegmentId>> {
        if self.runs.iter().any(|or| or.meta.value_segs.is_none()) {
            return None;
        }
        Some(self.protected_segments())
    }

    pub(super) fn reclaim_orphan_segments(&mut self) -> usize {
        #[cfg(test)]
        if !self.test_orphan_reclaim {
            return 0;
        }
        if self.poisoned || self.flush_in_flight || self.compact_in_flight {
            return 0;
        }
        let Some(keep) = self.referenced_segments() else {
            return 0;
        };
        let mut freed = 0usize;
        for seg in self.alloc.used_list() {
            if !keep.contains(&seg) {
                self.alloc
                    .free(seg)
                    .expect("reclaim_orphan_segments: freeing an unreferenced segment");
                freed += 1;
            }
        }
        if freed > 0 {
            self.bump_storage_generation();
            self.refresh_data_reserve();
        }
        freed
    }

    pub fn forget_keys(
        &mut self,
        keys: &std::collections::HashSet<Vec<u8>>,
        opts: RunBuildOpts,
    ) -> Result<Option<ForgetRunsStats>, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if keys.is_empty() {
            return Ok(Some(ForgetRunsStats::default()));
        }
        if self.flush_in_flight || self.compact_in_flight {
            return Ok(None);
        }

        if self.runs.is_empty() {
            let mut out = ForgetRunsStats::default();
            for key in keys {
                if let Some(e) = self.index.get(key) {
                    if let Err(e) = self.append_forget_delete(key, e.version) {
                        return Err(EngineError::PartialForget {
                            dropped: out.dropped,
                            source: Box::new(EngineError::from(e)),
                        });
                    }
                    self.index.remove(key);
                    if let Some(directory) = self.winner_directory.as_mut() {
                        directory.remove(key.as_slice());
                    }
                    self.bump_mutable_generation();
                    out.dropped.push((key.clone(), e.version));
                }
            }
            return Ok(Some(out));
        }

        let floor = self.last_flush_seq;
        let mut tail_forgets: Vec<(Vec<u8>, u128)> = Vec::new();
        let run_dropped: std::collections::HashMap<Vec<u8>, u128> = if self.runs.is_empty() {
            std::collections::HashMap::new()
        } else {
            let (_stats, dropped) = self.merge_all_runs(opts, None, |e| {
                let forgotten = keys.contains(&e.key);
                if forgotten && super::merge::replays_after_flush(e.seq, floor) {
                    tail_forgets.push((e.key.clone(), e.version));
                }
                forgotten
            })?;
            dropped.into_iter().collect()
        };
        self.append_tail_forget_deletes(&tail_forgets);

        let mut out = ForgetRunsStats::default();
        for key in keys {
            if let Some(e) = self.index.get(key) {
                if let Err(err) = self.append_forget_delete(key, e.version) {
                    let unreached: Vec<(Vec<u8>, u128)> = {
                        let reported: std::collections::HashSet<&[u8]> =
                            out.dropped.iter().map(|(k, _)| k.as_slice()).collect();
                        run_dropped
                            .iter()
                            .filter(|(k, _)| {
                                !reported.contains(k.as_slice()) && self.index.get(k).is_none()
                            })
                            .map(|(k, v)| (k.clone(), *v))
                            .collect()
                    };
                    out.dropped.extend(unreached);
                    return Err(EngineError::PartialForget {
                        dropped: out.dropped,
                        source: Box::new(EngineError::from(err)),
                    });
                }
                self.index.remove(key);
                if let Some(directory) = self.winner_directory.as_mut() {
                    directory.remove(key.as_slice());
                }
                self.bump_mutable_generation();
                out.dropped.push((key.clone(), e.version));
            } else if let Some(&v) = run_dropped.get(key) {
                out.dropped.push((key.clone(), v));
            }
        }
        Ok(Some(out))
    }

    pub(super) fn append_forget_delete(&mut self, key: &[u8], version: u128) -> Result<(), LogError> {
        #[cfg(any(test, feature = "test-fixtures"))]
        if self.test_fail_forget_append.as_deref() == Some(key) {
            return Err(LogError::DeviceFull);
        }
        self.log
            .append(&self.dev, &mut self.alloc, RecordKind::Delete, version, key, &[])
            .map(|_| ())
    }

    pub(super) fn append_tail_forget_deletes(&mut self, forgotten: &[(Vec<u8>, u128)]) {
        for (i, (key, version)) in forgotten.iter().enumerate() {
            if let Err(e) = self.append_forget_delete(key, *version) {
                log::warn!(
                    "engine: could not append {} forget delete record(s) for keys dropped from runs ({e}); \
                     their records in the replayed log tail bring them back after a restart",
                    forgotten.len() - i
                );
                return;
            }
        }
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn fail_forget_append_for_test(&mut self, key: Option<Vec<u8>>) {
        self.test_fail_forget_append = key;
    }

    pub fn compact(&mut self) -> Result<CompactStats, EngineError> {
        self.compact_filtered(|_, _, _| false)
    }

    pub fn compact_filtered(
        &mut self,
        drop_live: impl Fn(&[u8], u128, &[u8]) -> bool,
    ) -> Result<CompactStats, EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.flush_in_flight {
            return Ok(CompactStats::default());
        }
        if !self.runs.is_empty() || self.has_pinned_retired_runs() {
            return Ok(CompactStats::default());
        }
        let reserved_segs: std::collections::HashSet<SegmentId> = self
            .live_manifest
            .as_ref()
            .map(|l| l.segs.iter().copied().collect())
            .unwrap_or_default();
        let old_used: Vec<SegmentId> = self
            .alloc
            .used_list()
            .into_iter()
            .filter(|s| !reserved_segs.contains(s))
            .collect();
        if old_used.is_empty() {
            return Ok(CompactStats::default());
        }

        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let payload = (seg_bytes - SEG_HEADER_LEN) as u64;
        let needed = self.index.live_bytes() / payload + 1;
        if self.alloc.free_segments() < needed {
            return Err(EngineError::InsufficientHeadroom {
                needed_segments: needed,
                free_segments: self.alloc.free_segments(),
            });
        }

        self.sync()?;

        let mut newlog = ValueLog::new(
            self.block_size,
            self.alloc.blocks_per_segment(),
            self.log.next_seq(),
            self.log.next_epoch(),
        );
        newlog.carry_epoch_fence(&self.log);

        let (mut stats, updates, gc_dropped) =
            match self.copy_live_records(&mut newlog, &old_used, drop_live) {
                Ok(out) => out,
                Err(e) => {
                    self.rollback_failed_compaction(newlog, &old_used);
                    return Err(e);
                }
            };

        for (key, ptr, seq, version, tombstone) in updates {
            let entry = IndexEntry {
                ptr,
                seq,
                version,
                tombstone,
            };
            self.index.put(&key, entry);
            if let Some(directory) = self.winner_directory.as_mut() {
                directory.insert_max(&key, entry);
            }
        }
        let mut removed = Vec::with_capacity(gc_dropped.len());
        for key in gc_dropped {
            let entry = self.index.remove(&key);
            if let Some(directory) = self.winner_directory.as_mut() {
                directory.remove(key.as_slice());
            }
            if let Some(entry) = entry {
                removed.push((key, entry.version));
            }
        }
        self.bump_storage_generation();
        self.bump_mutable_generation();

        let zero = vec![0u8; self.block_size];
        for &seg in &old_used {
            let (start, _) = self.alloc.segment_range(seg);
            crate::attribution::write_bytes(6, zero.len());
            if let Err(e) = self.dev.write_at(start, &zero) {
                self.poisoned = true;
                return Err(failed_after_gc_drops(removed, e.into()));
            }
        }
        if let Err(e) = self.dev.flush() {
            self.poisoned = true;
            return Err(failed_after_gc_drops(removed, e.into()));
        }
        for &seg in &old_used {
            self.alloc
                .free(seg)
                .expect("compact: freeing a used segment");
            stats.segments_freed += 1;
        }

        self.log = newlog;
        Ok(stats)
    }

    pub fn compact_step(
        &mut self,
        max_segments: usize,
    ) -> Result<(CompactStats, bool), EngineError> {
        self.compact_step_filtered(max_segments, |_, _, _| false)
    }

    pub fn compact_step_filtered(
        &mut self,
        max_segments: usize,
        drop_live: impl Fn(&[u8], u128, &[u8]) -> bool,
    ) -> Result<(CompactStats, bool), EngineError> {
        if self.poisoned {
            return Err(EngineError::Poisoned);
        }
        if self.flush_in_flight || !self.runs.is_empty() || self.has_pinned_retired_runs() {
            return Ok((CompactStats::default(), true));
        }
        if max_segments == 0 {
            return Ok((CompactStats::default(), true));
        }

        let reserved_segs: std::collections::HashSet<SegmentId> = self
            .live_manifest
            .as_ref()
            .map(|l| l.segs.iter().copied().collect())
            .unwrap_or_default();
        let open_seg = self.log.open_segment();
        let mut candidates: Vec<SegmentId> = self
            .alloc
            .used_list()
            .into_iter()
            .filter(|s| !reserved_segs.contains(s) && Some(*s) != open_seg)
            .filter(|s| *s >= self.compact_cursor)
            .collect();
        candidates.sort_unstable_by_key(|s| s.0);
        if candidates.is_empty() {
            self.compact_cursor = SegmentId(0);
            return Ok((CompactStats::default(), true));
        }

        let free = self.alloc.free_segments();
        if free < 2 {
            return Err(EngineError::InsufficientHeadroom {
                needed_segments: 2,
                free_segments: free,
            });
        }
        let victim_cap = (free - 1) as usize;
        let take = max_segments.min(victim_cap).min(candidates.len());
        let victims: Vec<SegmentId> = candidates[..take].to_vec();

        self.sync()?;

        let (mut stats, updates, gc_dropped) = {
            let Engine {
                dev,
                alloc,
                log,
                index,
                block_size,
                ..
            } = self;
            Self::copy_step_records(
                dev,
                alloc,
                log,
                index.as_ref(),
                *block_size,
                &victims,
                drop_live,
            )?
        };
        stats.victims_scanned = victims.len();

        self.log.sync(&self.dev)?;

        for (key, ptr, seq, version, tombstone) in updates {
            let entry = IndexEntry {
                ptr,
                seq,
                version,
                tombstone,
            };
            self.index.put(&key, entry);
            if let Some(directory) = self.winner_directory.as_mut() {
                directory.insert_max(&key, entry);
            }
        }
        let mut removed = Vec::with_capacity(gc_dropped.len());
        for key in gc_dropped {
            let entry = self.index.remove(&key);
            if let Some(directory) = self.winner_directory.as_mut() {
                directory.remove(key.as_slice());
            }
            if let Some(entry) = entry {
                removed.push((key, entry.version));
            }
        }
        self.bump_storage_generation();
        self.bump_mutable_generation();

        let zero = vec![0u8; self.block_size];
        for &seg in &victims {
            let (start, _) = self.alloc.segment_range(seg);
            crate::attribution::write_bytes(6, zero.len());
            if let Err(e) = self.dev.write_at(start, &zero) {
                self.poisoned = true;
                return Err(failed_after_gc_drops(removed, e.into()));
            }
        }
        if let Err(e) = self.dev.flush() {
            self.poisoned = true;
            return Err(failed_after_gc_drops(removed, e.into()));
        }
        for &seg in &victims {
            self.alloc
                .free(seg)
                .expect("compact_step: freeing a used segment");
            self.log.forget_tail_seg(seg);
            stats.segments_freed += 1;
        }

        let last = victims.last().unwrap().0;
        self.compact_cursor = SegmentId(last + 1);
        let done = take == candidates.len();
        if done {
            self.compact_cursor = SegmentId(0);
        }
        Ok((stats, done))
    }

    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn copy_step_records(
        dev: &Arc<D>,
        alloc: &mut Allocator,
        log: &mut ValueLog,
        index: &dyn Index,
        block_size: usize,
        victims: &[SegmentId],
        drop_live: impl Fn(&[u8], u128, &[u8]) -> bool,
    ) -> Result<
        (
            CompactStats,
            Vec<(Vec<u8>, ValuePtr, u64, u128, bool)>,
            Vec<Vec<u8>>,
        ),
        EngineError,
    > {
        let mut stats = CompactStats::default();
        let mut updates: Vec<(Vec<u8>, ValuePtr, u64, u128, bool)> = Vec::new();
        let mut gc_dropped: Vec<Vec<u8>> = Vec::new();
        let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
        let mut buf = vec![0u8; seg_bytes];

        for &seg in victims {
            let (start, _) = alloc.segment_range(seg);
            dev.read_at(start, &mut buf)?;
            let (_, recs) = replay_segment(&buf)?;
            for (off, rec) in recs {
                match rec.kind {
                    RecordKind::Put => {
                        let live = index
                            .get(&rec.key)
                            .map(|e| e.ptr.segment == seg && e.ptr.offset == off)
                            .unwrap_or(false);
                        if live && drop_live(&rec.key, rec.version, &rec.value) {
                            log.append(
                                dev.as_ref(),
                                alloc,
                                RecordKind::Delete,
                                rec.version,
                                &rec.key,
                                &[],
                            )?;
                            gc_dropped.push(rec.key);
                            stats.gc_dropped += 1;
                        } else if live {
                            let (seq, ptr) = log.append_tombstone(
                                dev.as_ref(),
                                alloc,
                                RecordKind::Put,
                                rec.tombstone,
                                rec.version,
                                &rec.key,
                                &rec.value,
                            )?;
                            updates.push((rec.key, ptr, seq, rec.version, rec.tombstone));
                            stats.live_copied += 1;
                        } else {
                            stats.dead_dropped += 1;
                        }
                    }
                    RecordKind::RangeDelete => return Err(legacy_compaction_range_tombstone()),
                    RecordKind::Delete => {
                        if index.get(&rec.key).is_none() {
                            log.append(
                                dev.as_ref(),
                                alloc,
                                RecordKind::Delete,
                                rec.version,
                                &rec.key,
                                &[],
                            )?;
                            stats.tombstones_retained += 1;
                        } else {
                            stats.dead_dropped += 1;
                        }
                    }
                }
            }
        }
        Ok((stats, updates, gc_dropped))
    }

    #[allow(clippy::type_complexity)]
    fn copy_live_records(
        &mut self,
        newlog: &mut ValueLog,
        old_used: &[SegmentId],
        drop_live: impl Fn(&[u8], u128, &[u8]) -> bool,
    ) -> Result<
        (
            CompactStats,
            Vec<(Vec<u8>, ValuePtr, u64, u128, bool)>,
            Vec<Vec<u8>>,
        ),
        EngineError,
    > {
        let mut stats = CompactStats::default();
        let mut updates: Vec<(Vec<u8>, ValuePtr, u64, u128, bool)> = Vec::new();
        let mut gc_dropped: Vec<Vec<u8>> = Vec::new();
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let mut buf = vec![0u8; seg_bytes];

        for &seg in old_used {
            let (start, _) = self.alloc.segment_range(seg);
            self.dev.read_at(start, &mut buf)?;
            let (_, recs) = replay_segment(&buf)?;
            for (off, rec) in recs {
                match rec.kind {
                    RecordKind::Put => {
                        let live = self
                            .index
                            .get(&rec.key)
                            .map(|e| e.ptr.segment == seg && e.ptr.offset == off)
                            .unwrap_or(false);
                        if live && drop_live(&rec.key, rec.version, &rec.value) {
                            newlog.append(
                                &self.dev,
                                &mut self.alloc,
                                RecordKind::Delete,
                                rec.version,
                                &rec.key,
                                &[],
                            )?;
                            gc_dropped.push(rec.key);
                            stats.gc_dropped += 1;
                        } else if live {
                            let (seq, ptr) = newlog.append_tombstone(
                                &self.dev,
                                &mut self.alloc,
                                RecordKind::Put,
                                rec.tombstone,
                                rec.version,
                                &rec.key,
                                &rec.value,
                            )?;
                            let tombstone = rec.tombstone;
                            updates.push((rec.key, ptr, seq, rec.version, tombstone));
                            stats.live_copied += 1;
                        } else {
                            stats.dead_dropped += 1;
                        }
                    }
                    RecordKind::RangeDelete => return Err(legacy_compaction_range_tombstone()),
                    RecordKind::Delete => {
                        if self.index.get(&rec.key).is_none() {
                            newlog.append(
                                &self.dev,
                                &mut self.alloc,
                                RecordKind::Delete,
                                rec.version,
                                &rec.key,
                                &[],
                            )?;
                            stats.tombstones_retained += 1;
                        } else {
                            stats.dead_dropped += 1;
                        }
                    }
                }
            }
        }

        newlog.sync(&self.dev)?;
        Ok((stats, updates, gc_dropped))
    }

    fn rollback_failed_compaction(&mut self, newlog: ValueLog, old_used: &[SegmentId]) {
        newlog.quiesce(&self.dev);
        let old: std::collections::HashSet<SegmentId> = old_used.iter().copied().collect();
        let fresh: Vec<SegmentId> = self
            .alloc
            .used_list()
            .into_iter()
            .filter(|s| !old.contains(s))
            .collect();

        let zero = vec![0u8; self.block_size];
        let mut wiped = true;
        for &seg in &fresh {
            let (start, _) = self.alloc.segment_range(seg);
            crate::attribution::write_bytes(6, zero.len());
            if self.dev.write_at(start, &zero).is_err() {
                wiped = false;
            }
        }
        if self.dev.flush().is_err() {
            wiped = false;
        }

        let carried_tail = self.log.take_tail_segs();
        self.log = ValueLog::new(
            self.block_size,
            self.alloc.blocks_per_segment(),
            newlog.next_seq(),
            newlog.next_epoch(),
        );
        self.log.carry_epoch_fence(&newlog);
        self.log.seed_tail_segs(carried_tail);

        if wiped {
            for &seg in &fresh {
                self.alloc
                    .free(seg)
                    .expect("rollback: freeing a fresh segment");
            }
        } else {
            self.poisoned = true;
        }
    }

    #[cfg(test)]
    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned
    }

}

fn legacy_compaction_range_tombstone() -> EngineError {
    EngineError::Log(LogError::Corrupt("range tombstone record in a run-less value-log compaction"))
}
