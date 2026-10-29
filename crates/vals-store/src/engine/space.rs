use super::*;

impl<D: BlockDevice> Engine<D> {
    pub fn tail_epochs(&self) -> u64 {
        let (gap_above, gap) = self.tail_epoch_gap;
        self.log
            .next_epoch()
            .saturating_sub(1)
            .saturating_sub(self.last_flush_epoch)
            .saturating_sub(if self.last_flush_epoch == gap_above { gap } else { 0 })
    }

    pub fn next_log_seq(&self) -> u64 {
        self.log.next_seq()
    }

    pub fn memtable_bytes(&self) -> u64 {
        const PER_ENTRY_OVERHEAD: u64 = 64;
        self.index.key_bytes() + self.index.len() as u64 * PER_ENTRY_OVERHEAD
    }

    pub fn run_metadata_resident_bytes(&self) -> u64 {
        self.capacity_stats().run_resident_bytes
    }

    pub fn capacity_stats(&self) -> CapacityStats {
        let mut stats = CapacityStats {
            mutable_entries: self.index.len() as u64,
            mutable_logical_bytes: self.memtable_bytes(),
            mutable_allocated_bytes: self.index.allocated_bytes(),
            flush_protection_bytes: if self.flush_value_segs.capacity() == 0 { 0 } else {
                ((self.flush_value_segs.capacity() * 8 / 7).next_power_of_two()
                    * (std::mem::size_of::<SegmentId>() + 1) + 16) as u64
            },
            run_count: self.runs.len() as u64,
            winner_directory_bytes: self.winner_directory_bytes(),
            ..CapacityStats::default()
        };
        stats.manifest_resident_bytes =
            (self.runs.capacity() * std::mem::size_of::<OpenRun<D>>()) as u64;
        if let Some(live) = &self.live_manifest {
            stats.manifest_resident_bytes +=
                (std::mem::size_of::<LiveSnapshot>()
                    + live.segs.capacity() * std::mem::size_of::<SegmentId>()) as u64;
        }
        for open in &self.runs {
            stats.run_entries += open.meta.entry_count;
            stats.run_disk_bytes += open.meta.handle.len;
            stats.sidecar_disk_bytes += open.run.sidecar_len();
            stats.live_value_bytes += open.meta.value_bytes.unwrap_or(0);
            let run = open.run.resident_stats();
            stats.fence_resident_bytes += run.fence_bytes;
            stats.fence_payload_bytes += run.fence_payload_bytes;
            stats.filter_resident_bytes += run.filter_bytes;
            stats.run_filter_resident_bytes += run.filter_bytes;
            stats.run_filter_payload_bytes += run.filter_payload_bytes;
            stats.run_resident_bytes += run.total_bytes
                - std::mem::size_of::<run::DeviceRun<D>>() as u64;
            stats.manifest_resident_bytes += (open.meta.min_key.capacity()
                + open.meta.max_key.capacity()
                + open.meta.handle.segs.capacity() * std::mem::size_of::<SegmentId>()
                + open.meta.value_segs.as_ref().map_or(0, |v| {
                    v.capacity() * std::mem::size_of::<SegmentId>()
                })
                + open.meta.value_seg_bytes.as_ref().map_or(0, |v| {
                    v.capacity() * std::mem::size_of::<u64>()
                })
                + open.victim_lives.borrow().as_ref().and_then(|(_, lives)| lives.as_ref()).map_or(0, |lives| {
                    lives.capacity() * std::mem::size_of::<(SegmentId, u64)>()
                })) as u64;
        }
        let key_filter_bytes = self.key_filter.resident_bytes();
        stats.filter_resident_bytes += key_filter_bytes;
        stats.run_resident_bytes += stats.manifest_resident_bytes + key_filter_bytes;
        let shadow = self.shadow.cache.borrow();
        let buckets = (shadow.samples.capacity() * 8).div_ceil(7);
        let mut shadow_bytes = (shadow.run_ids.capacity() + shadow.per_run.capacity() + shadow.per_run_fold.capacity())
            * std::mem::size_of::<u64>();
        if buckets != 0 {
            shadow_bytes += buckets * (std::mem::size_of::<(u64, ShadowSample)>() + 1) + 16;
        }
        for sample in shadow.samples.values() {
            shadow_bytes += sample.entries.capacity() * std::mem::size_of::<ShadowSampleEntry>();
            shadow_bytes += sample.tombstones.capacity() * std::mem::size_of::<u128>();
            shadow_bytes += sample.entries.iter().map(|e| e.key.capacity()).sum::<usize>();
        }
        stats.run_resident_bytes += shadow_bytes as u64 + self.held_retired_index_bytes();
        stats
    }

    pub fn used_segments(&self) -> u64 {
        self.alloc.used_segments()
    }

    pub fn free_segments(&self) -> u64 {
        self.alloc.free_segments()
    }

    pub fn at_maintenance_floor(&self) -> bool {
        self.maint_reserve_enabled && self.alloc.free_segments() <= self.alloc.maint_reserve()
    }

    pub fn admission_shed_probability(&self) -> f64 {
        if !self.maint_reserve_enabled {
            return 0.0;
        }
        let floor = self.alloc.maint_reserve();
        if floor == 0 {
            return 0.0;
        }
        let free = self.alloc.free_segments();
        let band = (self.alloc.num_segments() * admit_band_pct() / 100).max(1);
        let top = floor + band;
        if free >= top {
            return 0.0;
        }
        if free <= floor {
            return 1.0;
        }
        let x = (top - free) as f64 / band as f64;
        x * x
    }

    pub fn admission_shed(&mut self) -> bool {
        let p = self.admission_shed_probability();
        if p <= 0.0 {
            return false;
        }
        if p >= 1.0 {
            return true;
        }
        let mut s = self.shed_rng;
        s ^= s >> 12;
        s ^= s << 25;
        s ^= s >> 27;
        self.shed_rng = s;
        let u = (s.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64;
        u < p
    }

    pub(super) fn one_run_victim_needed_segs(
        &self,
        or: &OpenRun<D>,
        seg_bytes: usize,
        commit_headroom: u64,
        threshold: f64,
    ) -> Option<u64> {
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let key = (threshold.to_bits(), usable);
        let mut cache = or.victim_lives.borrow_mut();
        if cache.as_ref().is_none_or(|(cached, _)| *cached != key) {
            let meta = std::slice::from_ref(&or.meta);
            let lives = Self::select_victims_excluding(meta, usable as u64, None, threshold)
                .and_then(|(victims, _)| Self::victim_lives_ascending(meta, &victims));
            *cache = Some((key, lives));
        }
        let lives = cache.as_ref().and_then(|(_, lives)| lives.as_ref())?;
        let open = self.log.open_segment();
        let output_segs = OpenRun::rewrite_output_bound(std::iter::once(or)).div_ceil(seg_bytes).max(1) as u64;
        let (_, rewrite_live) = Self::gainful_victim_prefix(
            lives
                .iter()
                .filter(|&&(seg, _)| Some(seg) != open && !self.gc_excluded_segs.contains(&seg))
                .map(|&(_, live)| live),
            usable,
        )?;
        let vlog_segs = if rewrite_live == 0 {
            0
        } else {
            2 * (rewrite_live as usize).div_ceil(usable).max(1) as u64 + 2
        };
        Some(
            output_segs
                .saturating_add(commit_headroom)
                .saturating_add(vlog_segs),
        )
    }

    pub(super) fn minimal_pressure_needed_segs(&self) -> Option<u64> {
        self.runs.last()?;
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let commit_headroom = self
            .manifest_headroom_segs()
            .saturating_add(self.flush_headroom_segs(seg_bytes));
        let threshold = super::merge::victim_dead_fraction();
        let one_run = if vlog_gc_enabled() {
            self.runs
                .iter()
                .filter(|or| or.meta.value_bytes.is_some())
                .filter_map(|or| {
                    self.one_run_victim_needed_segs(or, seg_bytes, commit_headroom, threshold)
                })
                .min()
        } else {
            self.runs.last().and_then(|newest| {
                newest.meta.value_bytes.and_then(|_| {
                    self.one_run_victim_needed_segs(newest, seg_bytes, commit_headroom, threshold)
                })
            })
        };
        let two_run = if self.runs.len() >= 2 {
            let output_bytes = OpenRun::rewrite_output_bound(self.runs[self.runs.len() - 2..].iter());
            Some(
                (output_bytes.div_ceil(seg_bytes).max(1) as u64)
                    .saturating_add(commit_headroom),
            )
        } else {
            None
        };
        match (one_run, two_run) {
            (Some(one), Some(two)) => Some(one.min(two)),
            (Some(one), None) => Some(one),
            (None, Some(two)) => Some(two),
            (None, None) => None,
        }
    }

    pub fn region_exhausted(&self) -> bool {
        if !self.at_maintenance_floor()
            || self.flush_in_flight
            || self.compact_in_flight
        {
            return false;
        }
        if !self.index.is_empty() && !self.stalled_memtable_capacity() {
            return false;
        }
        if self.aged_vlog_gc_feasible() {
            return false;
        }
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let commit_headroom = self
            .manifest_headroom_segs()
            .saturating_add(self.flush_headroom_segs(seg_bytes));
        if self.shadow_fold_window(seg_bytes, commit_headroom, None).is_some() {
            return false;
        }
        if self
            .minimal_pressure_needed_segs()
            .is_some_and(|needed| needed > self.alloc.free_segments())
        {
            return true;
        }
        if self.runs.len() != 1 {
            return false;
        }
        let meta = self.runs[0].meta.clone();
        if meta.value_bytes.is_none() {
            return false;
        }
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let Some((victims, _)) = self.select_victims(std::slice::from_ref(&meta), usable as u64)
        else {
            return false;
        };
        if victims.is_empty() {
            return true;
        }
        match Self::victim_lives_ascending(std::slice::from_ref(&meta), &victims) {
            Some(lives) => Self::gainful_victim_prefix_len(&lives, usable).is_none(),
            None => false,
        }
    }

    pub(super) fn stalled_memtable_capacity(&self) -> bool {
        if !shadow_est_enabled() {
            return false;
        }
        if self.pressure_infeasible_streak < STALL_TERMINAL_MIN_PLAN_FAILURES {
            return false;
        }
        if !self
            .lsm_dead_fraction()
            .is_some_and(|dead| dead < MAINT_RESERVE_DEAD_MIN_FRACTION)
        {
            return false;
        }
        let usable = ((self.alloc.blocks_per_segment() as usize * self.block_size)
            .saturating_sub(SEG_HEADER_LEN)) as u64;
        let c = self.shadow.cache.borrow();
        c.computed && c.gen == self.manifest_gen && c.mutable_gen == self.mutable_gen
            && !c.gated && c.total < usable
    }

    #[cfg(test)]
    pub(super) fn set_orphan_reclaim_for_test(&mut self, enabled: bool) {
        self.test_orphan_reclaim = enabled;
    }

    #[cfg(test)]
    pub(super) fn free_segments_for_test(&self) -> u64 {
        self.free_segments()
    }

    #[cfg(test)]
    pub(super) fn pressure_infeasible_streak_for_test(&self) -> u32 {
        self.pressure_infeasible_streak
    }

    #[cfg(test)]
    pub(super) fn last_run_value_segs_for_test(&self) -> Vec<SegmentId> {
        self.runs
            .last()
            .expect("last_run_value_segs_for_test: no runs")
            .meta
            .value_segs
            .clone()
            .expect("value_segs populated")
    }

    #[cfg(test)]
    pub(super) fn seg_allocated_for_test(&self, seg: SegmentId) -> bool {
        !self.alloc.is_free(seg)
    }

    #[cfg(test)]
    pub(super) fn orphan_segments_for_test(&self) -> Vec<SegmentId> {
        let Some(keep) = self.referenced_segments() else {
            return Vec::new();
        };
        self.alloc
            .used_list()
            .into_iter()
            .filter(|s| !keep.contains(s))
            .collect()
    }

    #[cfg(test)]
    pub(super) fn maint_reserve_for_test(&self) -> u64 {
        self.alloc.maint_reserve()
    }

    #[cfg(test)]
    pub(super) fn shadow_backoff_for_test(&self) -> u32 {
        self.shadow.backoff_shift
    }

    #[cfg(test)]
    pub(super) fn shadow_cooldown_active_for_test(&self) -> bool {
        self.log.next_epoch() < self.shadow.cooldown_until_epoch
    }

    pub fn refresh_data_reserve(&mut self) {
        let reserve = if self.maint_reserve_enabled {
            self.maint_reserve_segs()
        } else {
            0
        };
        self.alloc.set_data_reserve(reserve);
    }

    pub fn set_maint_reserve_enabled(&mut self, on: bool) {
        self.maint_reserve_enabled = on;
        self.refresh_data_reserve();
    }

    pub(super) fn manifest_headroom_segs(&self) -> u64 {
        self.live_manifest
            .as_ref()
            .map_or(1, |live| live.segs.len() as u64 + 1)
    }

    fn flush_headroom_bytes(&self) -> usize {
        if self.index.is_empty() && !self.has_unflushed_range_tombstones() {
            return 0;
        }
        let rt_bytes = crate::range_tombstone::rt_block_len_of(self.rts.tail.iter().map(|(r, _)| r));
        let version = if rt_bytes > 0 { run::run_write_version().max(run::RUN_VERSION) } else { run::run_write_version() };
        run::run_len_upper_bound_agg(
            self.index.len(),
            self.index.key_bytes() as usize,
            self.index.max_key_len() as usize,
            RunBuildOpts::default(),
            version,
        ) + rt_bytes
    }

    pub(super) fn flush_headroom_segs(&self, seg_bytes: usize) -> u64 {
        if seg_bytes == 0 {
            return 0;
        }
        self.flush_headroom_bytes().div_ceil(seg_bytes) as u64
    }

    fn maint_reserve_segs(&self) -> u64 {
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        if seg_bytes == 0 {
            return 0;
        }

        let flush_bytes = self.flush_headroom_bytes();
        let flush_segs = (flush_bytes.div_ceil(seg_bytes)) as u64;
        let mut projected_sizes: Vec<u64> =
            {
                let out_version = run::merge_write_version(self.runs.iter().map(|or| or.run.format_version()));
                self.runs.iter().map(|or| or.rewrite_bound(out_version) as u64).collect()
            };
        if flush_bytes > 0 {
            projected_sizes.push(flush_bytes as u64);
        }
        let merge_index_segs = if projected_sizes.is_empty() {
            0
        } else {
            let mut start = select_capped_suffix(&projected_sizes, merge_input_cap_bytes());
            if projected_sizes.len() >= 2 {
                start = start.min(projected_sizes.len() - 2);
            }
            let bytes: u64 = projected_sizes[start..].iter().copied().sum();
            (bytes as usize).div_ceil(seg_bytes).max(1) as u64
        };
        let victim_rewrite_segs = if projected_sizes.is_empty() { 0 } else { 4 };

        let computed = flush_segs
            .saturating_add(self.manifest_headroom_segs())
            .saturating_add(merge_index_segs)
            .saturating_add(victim_rewrite_segs);
        let computed = match self.minimal_pressure_needed_segs() {
            Some(min_plan) => {
                computed.max(min_plan.saturating_add(MAINT_RESERVE_PLAN_MARGIN_SEGS))
            }
            None => computed,
        };
        let cap = self.alloc.num_segments() / 2;
        computed.min(cap)
    }

    pub fn free_fraction(&self) -> f64 {
        let total = self.alloc.num_segments();
        if total == 0 {
            return 1.0;
        }
        self.alloc.free_segments() as f64 / total as f64
    }

    pub fn lsm_dead_fraction(&self) -> Option<f64> {
        let payload = ((self.alloc.blocks_per_segment() as usize * self.block_size)
            .saturating_sub(SEG_HEADER_LEN)) as u64;
        let capacity = self.alloc.num_segments().saturating_mul(payload);
        if capacity == 0 {
            return Some(0.0);
        }
        Some(self.visible_dead_bytes()? as f64 / capacity as f64)
    }

    fn visible_dead_bytes(&self) -> Option<u64> {
        let payload = ((self.alloc.blocks_per_segment() as usize * self.block_size)
            .saturating_sub(SEG_HEADER_LEN)) as u64;
        let used = self.alloc.used_segments().saturating_mul(payload);
        let mut live = self.index.live_bytes();
        for or in &self.runs {
            live = live
                .saturating_add(or.meta.handle.len)
                .saturating_add(or.meta.value_bytes?);
        }
        Some(used.saturating_sub(live))
    }

    pub fn lsm_dead_fraction_gate(&self) -> Option<f64> {
        let base = self.lsm_dead_fraction()?;
        if !shadow_est_enabled() {
            return Some(base);
        }
        self.refresh_shadow_estimates();
        if !shadow_fold_enabled() {
            return Some(base);
        }
        if self.log.next_epoch() < self.shadow.cooldown_until_epoch {
            return Some(base);
        }
        let total = self.shadow.cache.borrow().total;
        if total == 0 {
            return Some(base);
        }
        let payload = ((self.alloc.blocks_per_segment() as usize * self.block_size)
            .saturating_sub(SEG_HEADER_LEN)) as u64;
        let capacity = self.alloc.num_segments().saturating_mul(payload);
        if capacity == 0 {
            return Some(base);
        }
        Some(base + total as f64 / capacity as f64)
    }

    pub fn shadowed_bytes_estimate_cached(&self) -> u64 {
        self.shadow.cache.borrow().total
    }

    pub fn shadow_folds(&self) -> u64 {
        self.shadow.folds
    }

    pub fn shadow_fold_disarms(&self) -> u64 {
        self.shadow.disarms
    }

    pub(super) fn refresh_shadow_estimates(&self) {
        if !shadow_est_enabled() {
            let mut c = self.shadow.cache.borrow_mut();
            *c = ShadowCache::default();
            return;
        }
        use crate::attribution as a;
        let gated = self.free_fraction() > SHADOW_EST_FREE_GATE;
        {
            let c = self.shadow.cache.borrow();
            if c.computed && c.gen == self.manifest_gen
                && c.mutable_gen == self.mutable_gen && c.gated == gated
                && c.accounted_seq == self.rts.accounted_seq.get() {
                a::SHADOW_HITS.add(1);
                return;
            }
            if !c.computed { a::SHADOW_FIRST.add(1); }
            else if c.gen != self.manifest_gen || c.mutable_gen != self.mutable_gen
                || c.accounted_seq != self.rts.accounted_seq.get() { a::SHADOW_GENERATION.add(1); }
            else { a::SHADOW_GATE.add(1); }
        }
        if gated { a::SHADOW_GATED.add(1); } else { a::SHADOW_UNGATED.add(1); }
        let _timer = a::Timer::new(&a::SHADOW_WALL, Some(&a::SHADOW_CPU));
        let mut c = self.shadow.cache.borrow_mut();
        let current: std::collections::HashSet<u64> = self.runs.iter().map(|or| or.id).collect();
        c.samples.retain(|id, _| current.contains(id));
        c.per_run = vec![0u64; self.runs.len()];
        c.per_run_fold = vec![0u64; self.runs.len()];
        c.total = 0;
        if !gated {
            let previous: std::collections::HashSet<u64> = c.run_ids.iter().copied().collect();
            let added: Vec<_> = self.runs.iter().enumerate()
                .filter(|(_, or)| !previous.contains(&or.id)).collect();
            let mut reset = std::collections::HashSet::new();
            let mut removed_newer = false;
            for id in c.run_ids.iter().rev() {
                if !current.contains(id) { removed_newer = true; }
                else if removed_newer { reset.insert(*id); }
            }
            for (i, or) in self.runs.iter().enumerate() {
                if !c.samples.contains_key(&or.id) {
                    reset.insert(or.id);
                    let sample = self.sample_shadow_run(i);
                    c.samples.insert(or.id, sample);
                }
            }
            let accounted = self.rt_accounted_set();
            let mut pass = vec![1.0f64; self.runs.len() + 1];
            for (i, or) in self.runs.iter().enumerate().rev() {
                pass[i] = pass[i + 1] * (1.0 - c.samples[&or.id].fpr.clamp(0.0, 1.0));
            }
            for (i, or) in self.runs.iter().enumerate() {
                let sample = c.samples.get_mut(&or.id).expect("sampled above");
                let reprobe = reset.contains(&or.id);
                let (mut memtable, mut matched, mut rest, mut covered) = (0u64, 0u64, 0u64, 0u64);
                for e in &mut sample.entries {
                    if reprobe {
                        e.run_shadowed = self.runs[i + 1..].iter()
                            .any(|newer| Self::shadow_sample_matches(e, newer));
                    } else if !e.run_shadowed {
                        e.run_shadowed = added.iter().filter(|(j, _)| *j > i)
                            .any(|(_, newer)| Self::shadow_sample_matches(e, newer));
                    }
                    if accounted.as_ref().is_some_and(|a| range_delete::is_covered(a.cover(&e.key), e.version)) {
                        covered = covered.saturating_add(e.len);
                    } else if self.index.get(&e.key).is_some_and(|m| m.version > e.version) {
                        memtable = memtable.saturating_add(e.len);
                    } else {
                        rest = rest.saturating_add(e.len);
                        if e.run_shadowed {
                            matched = matched.saturating_add(e.len);
                        }
                    }
                }
                let by_runs = Self::shadow_fp_corrected(matched, rest, 1.0 - pass[i + 1]).saturating_add(covered);
                let scale = |bytes: u64| if sample.sampled_bytes == 0 { 0 } else {
                    (sample.value_bytes as u128 * bytes as u128 / sample.sampled_bytes as u128) as u64
                };
                let est = scale(memtable.saturating_add(by_runs));
                c.per_run_fold[i] = scale(by_runs);
                c.per_run[i] = est;
                c.total = c.total.saturating_add(est);
            }
            c.run_ids = self.runs.iter().map(|or| or.id).collect();
        }
        c.gen = self.manifest_gen;
        c.mutable_gen = self.mutable_gen;
        c.accounted_seq = self.rts.accounted_seq.get();
        c.computed = true;
        c.gated = gated;
    }

    pub(super) fn sample_shadow_run(&self, i: usize) -> ShadowSample {
        let or = &self.runs[i];
        let mut sample = ShadowSample {
            value_bytes: or.meta.value_bytes.unwrap_or(0),
            fpr: or.run.filter_fpr(),
            ..ShadowSample::default()
        };
        if sample.value_bytes == 0 { return sample; }
        let nblocks = or.run.index_len();
        let want = shadow_sample_blocks().min(nblocks);
        let mut seed = or.id.wrapping_mul(0x9e3779b97f4a7c15);
        for k in 0..want {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let b = if want == nblocks { k } else { (seed >> 11) as usize % nblocks };
            crate::attribution::SHADOW_BLOCKS.add(1);
            crate::attribution::SHADOW_BYTES.add(or.run.block_read_bytes(&self.alloc, b) as u64);
            let Ok(entries) = or.run.read_block_entries(&self.alloc, b) else { continue; };
            for e in entries {
                sample.entries_sampled += 1;
                if e.tombstone {
                    sample.tombstones.push(e.version);
                    continue;
                }
                if e.ptr.len == 0 { continue; }
                let len = e.ptr.len as u64;
                sample.sampled_bytes = sample.sampled_bytes.saturating_add(len);
                sample.entries.push(ShadowSampleEntry {
                    bloom: run::BloomKey::new(&e.key),
                    key: e.key,
                    version: e.version,
                    len,
                    run_shadowed: false,
                });
            }
        }
        sample
    }

    fn shadow_sample_matches(e: &ShadowSampleEntry, or: &OpenRun<D>) -> bool {
        crate::attribution::SHADOW_RANGES.add(1);
        if e.key.as_slice() < or.meta.min_key.as_slice() || e.key.as_slice() > or.meta.max_key.as_slice() {
            return false;
        }
        crate::attribution::SHADOW_BLOOMS.add(1);
        or.run.might_contain_hashed(&e.bloom)
    }

    pub(super) fn shadow_fp_corrected(matched: u64, rest: u64, fp: f64) -> u64 {
        if matched == 0 || !(0.0..0.99).contains(&fp) {
            return 0;
        }
        let corrected = (matched as f64 - fp * rest as f64) / (1.0 - fp);
        corrected.clamp(0.0, rest as f64) as u64
    }

    #[cfg(test)]
    pub(super) fn shadow_sample_from_scratch(&self, i: usize, sample: &ShadowSample) -> u64 {
        let fp = 1.0 - self.runs[i + 1..].iter().map(|or| 1.0 - or.run.filter_fpr()).product::<f64>();
        let (mut memtable, mut matched, mut rest) = (0u64, 0u64, 0u64);
        for e in &sample.entries {
            if self.index.get(&e.key).is_some_and(|m| m.version > e.version) {
                memtable += e.len;
            } else {
                rest += e.len;
                if self.runs[i + 1..].iter().any(|or| Self::shadow_sample_matches(e, or)) {
                    matched += e.len;
                }
            }
        }
        let shadowed = memtable + Self::shadow_fp_corrected(matched, rest, fp);
        if sample.sampled_bytes == 0 { return 0; }
        (sample.value_bytes as u128 * shadowed as u128 / sample.sampled_bytes as u128) as u64
    }

    #[cfg(test)]
    pub(super) fn total_run_value_bytes(&self) -> u64 {
        self.runs
            .iter()
            .map(|or| or.meta.value_bytes.unwrap_or(0))
            .sum()
    }

    pub(super) fn shadow_fold_window(
        &self,
        seg_bytes: usize,
        commit_headroom: u64,
        gc: Option<u128>,
    ) -> Option<ShadowFoldWindow> {
        if !shadow_est_enabled() || !shadow_fold_enabled() || self.runs.len() < 2 || seg_bytes == 0 {
            return None;
        }
        if self.log.next_epoch() < self.shadow.cooldown_until_epoch {
            return None;
        }
        if self.runs.iter().any(|or| or.run.format_version() < 3) {
            return None;
        }
        self.refresh_shadow_estimates();
        let c = self.shadow.cache.borrow();
        if !c.computed || c.per_run_fold.len() != self.runs.len() {
            return None;
        }
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1) as u64;
        let free = self.alloc.free_segments();
        let cap = self.merge_policy.size_cap;
        let garbage = |i: usize| {
            let shadowed = c.per_run_fold[i];
            let meta = &self.runs[i].meta;
            let index_share = match meta.value_bytes {
                Some(values) if values > 0 => {
                    (meta.handle.len as u128 * shadowed.min(values) as u128 / values as u128) as u64
                }
                _ => 0,
            };
            let tombstones = match (gc, c.samples.get(&self.runs[i].id)) {
                (Some(cutoff), Some(sample)) if sample.entries_sampled > 0 => {
                    let below = sample.tombstones.iter().filter(|&&v| v < cutoff).count() as u128;
                    (meta.handle.len as u128 * below / sample.entries_sampled as u128) as u64
                }
                _ => 0,
            };
            shadowed.saturating_add(index_share).saturating_add(tombstones)
        };
        let mut best: Option<ShadowFoldWindow> = None;
        for t in 0..self.runs.len() {
            let first = garbage(t);
            if first < usable {
                continue;
            }
            let mut end = t + 1;
            let mut bytes = self.runs[t].meta.handle.len;
            while end < self.runs.len() && bytes.saturating_add(self.runs[end].meta.handle.len) <= cap {
                bytes += self.runs[end].meta.handle.len;
                end += 1;
            }
            let output_segs = (bytes as usize).div_ceil(seg_bytes).max(1) as u64;
            if output_segs.saturating_add(commit_headroom) > free {
                continue;
            }
            let gain: u64 = first + (t + 1..end).map(garbage).sum::<u64>();
            if gain / usable < shadow_fold_min_gain_segs().max(output_segs.div_ceil(2)) {
                continue;
            }
            if best.is_none_or(|b| gain > b.predicted_bytes) {
                best = Some(ShadowFoldWindow { start: t, end, predicted_bytes: gain });
            }
        }
        best
    }

    pub fn reclaimable_fraction(&self) -> f64 {
        let payload = (self.alloc.blocks_per_segment() as usize * self.block_size)
            .saturating_sub(SEG_HEADER_LEN) as u64;
        let used = self.alloc.used_segments().saturating_mul(payload);
        if used == 0 {
            return 0.0;
        }
        let live = self.index.live_bytes().min(used);
        (used - live) as f64 / used as f64
    }

}

