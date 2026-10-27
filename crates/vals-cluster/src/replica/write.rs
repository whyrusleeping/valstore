use super::*;

impl<D: BlockDevice> VersionedStore<D> {
    pub(super) fn stamp(&mut self, old: Option<Version>, phys_ms: u64) -> Version {
        match old {
            Some(cur) => self.hlc.observe(cur.hlc, phys_ms),
            None => self.hlc.now(phys_ms),
        }
    }

    pub(super) fn store(
        &mut self,
        key: &[u8],
        old: Option<Version>,
        version: Version,
        kind: u8,
        val: &[u8],
    ) -> Result<(), EngineError> {
        let write = self.engine.put_kind_authoritative(
            key,
            &encode(kind, val),
            pack_version(version),
            kind == KIND_TOMB,
        )?;
        if write.written {
            let group = self.group_of(key);
            let (leaf_old, count_old) = self.accounted_old(key, old);
            self.apply_winner_leaf(key, &group, OldWinner::Exact(leaf_old), version);
            self.note_logical_winner_apply(key, OldWinner::Exact(count_old));
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn note_unknown_old_winner(&self, key: &[u8], version: Version) {
        let group = self.group_of(key);
        self.apply_winner_leaf(key, &group, OldWinner::Unknown, version);
        self.note_logical_winner_apply(key, OldWinner::Unknown);
    }

    #[cfg(test)]
    pub(crate) fn append_unknown_for_test(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<(), EngineError> {
        let (kind, value) = value.map_or((KIND_TOMB, &[][..]), |value| (KIND_PUT, value));
        self.engine.put_kind(
            key,
            &encode(kind, value),
            pack_version(version),
            kind == KIND_TOMB,
        )?;
        self.note_unknown_old_winner(key, version);
        Ok(())
    }

    pub fn apply(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, EngineError> {
        self.apply_at(key, value, version, physical_now_ms())
    }

    pub fn apply_at(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
    ) -> Result<Applied, EngineError> {
        let skew = self.hlc.max_skew_ms();
        if skew != 0 && version.physical_ms() > wall_ms.saturating_add(skew) {
            return Ok(Applied::Rejected);
        }
        self.advance_live_admission_cutoff(wall_ms);
        if self.gc_grace_ms != 0
            && value.is_some()
            && version.physical_ms() < self.live_admission_cutoff_ms.get()
        {
            return Ok(Applied::Rejected);
        }
        self.note_version(version);
        self.hlc.observe(version.hlc, wall_ms);
        if let Some(cover) = self.covering(key, version) {
            return Ok(Applied::Lost(cover));
        }
        let old = self.physical_version(key)?;
        self.decide_and_store(key, value, version, old, wall_ms)
    }

    pub fn apply_range_delete(
        &mut self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
    ) -> Result<Applied, EngineError> {
        let skew = self.hlc.max_skew_ms();
        if skew != 0 && version.physical_ms() > wall_ms.saturating_add(skew) {
            return Ok(Applied::Rejected);
        }
        self.note_version(version);
        self.hlc.observe(version.hlc, wall_ms);
        if let Some(seq) = self.engine.apply_range_delete(lo, hi, pack_version(version))?.seq {
            self.queue_range_tombstone(seq);
        }
        Ok(Applied::Won)
    }

    fn covering(&self, key: &[u8], version: Version) -> Option<Version> {
        let cover = self.engine.range_tombstone_cover(key)?;
        (pack_version(version) <= cover).then(|| unpack_version(cover))
    }

    fn physical_version(&self, key: &[u8]) -> Result<Option<Version>, EngineError> {
        Ok(self.engine.physical_version(key)?.map(unpack_version))
    }

    pub(crate) fn apply_unconditional_at(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
        mode: ApplyMode,
    ) -> Result<Applied, EngineError> {
        match mode {
            ApplyMode::Legacy => self.apply_at(key, value, version, wall_ms),
            ApplyMode::Blind => self.apply_blind_at(key, value, version, wall_ms),
        }
    }

    pub fn apply_blind_at(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
    ) -> Result<Applied, EngineError> {
        let skew = self.hlc.max_skew_ms();
        if skew != 0 && version.physical_ms() > wall_ms.saturating_add(skew) {
            crate::observe::counters().blind_rejected_skew.inc();
            return Ok(Applied::Rejected);
        }
        self.advance_live_admission_cutoff(wall_ms);
        if self.gc_grace_ms != 0
            && value.is_some()
            && version.physical_ms() < self.live_admission_cutoff_ms.get()
        {
            crate::observe::counters().blind_rejected_gc_floor.inc();
            return Ok(Applied::Rejected);
        }
        self.note_version(version);
        self.hlc.observe(version.hlc, wall_ms);
        if let Some(cover) = self.covering(key, version) {
            return Ok(Applied::Lost(cover));
        }
        if self.engine.winner_directory_enabled() {
            if self.expired_tombstone(value, version)
                && self.engine.winner_directory_version(key) == Some(None)
            {
                crate::observe::counters().expired_tombstones_skipped.inc();
                return Ok(Applied::Won);
            }
            let (kind, val) = value.map_or((KIND_TOMB, &[][..]), |value| (KIND_PUT, value));
            let write = match self.engine.put_kind_authoritative(
                key,
                &encode(kind, val),
                pack_version(version),
                kind == KIND_TOMB,
            ) {
                Ok(write) => write,
                Err(e) => {
                    crate::observe::counters().blind_append_errors.inc();
                    return Err(e);
                }
            };
            if write.written {
                let old = write.old_version.map(unpack_version);
                let group = self.group_of(key);
                let (leaf_old, count_old) = self.accounted_old(key, old);
                self.apply_winner_leaf(key, &group, OldWinner::Exact(leaf_old), version);
                self.note_logical_winner_apply(key, OldWinner::Exact(count_old));
                crate::observe::counters().blind_candidates_appended.inc();
            }
            return Ok(Applied::Won);
        }
        let (kind, val) = value.map_or((KIND_TOMB, &[][..]), |value| (KIND_PUT, value));
        if let Err(e) = self.engine.put_kind(
            key,
            &encode(kind, val),
            pack_version(version),
            kind == KIND_TOMB,
        ) {
            crate::observe::counters().blind_append_errors.inc();
            return Err(e);
        }
        self.note_unknown_old_winner(key, version);
        crate::observe::counters().blind_candidates_appended.inc();
        Ok(Applied::Won)
    }

    fn expired_tombstone(&self, value: Option<&[u8]>, version: Version) -> bool {
        self.gc_grace_ms != 0
            && value.is_none()
            && version.physical_ms() < self.live_admission_cutoff_ms.get()
    }

    fn decide_and_store(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        old: Option<Version>,
        _wall_ms: u64,
    ) -> Result<Applied, EngineError> {
        if let Some(cur) = old {
            if !version.wins_over(&cur) {
                return Ok(Applied::Lost(cur));
            }
        } else if self.expired_tombstone(value, version) {
            crate::observe::counters().expired_tombstones_skipped.inc();
            return Ok(Applied::Won);
        }
        let (kind, val) = match value {
            Some(v) => (KIND_PUT, v),
            None => (KIND_TOMB, &[][..]),
        };
        self.store(key, old, version, kind, val)?;
        Ok(Applied::Won)
    }

    pub fn apply_stage(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
    ) -> Result<StagedApply, EngineError>
    where
        D: AsyncRead,
    {
        let skew = self.hlc.max_skew_ms();
        if skew != 0 && version.physical_ms() > wall_ms.saturating_add(skew) {
            return Ok(StagedApply::Done(Applied::Rejected));
        }
        self.advance_live_admission_cutoff(wall_ms);
        if self.gc_grace_ms != 0
            && value.is_some()
            && version.physical_ms() < self.live_admission_cutoff_ms.get()
        {
            return Ok(StagedApply::Done(Applied::Rejected));
        }
        self.note_version(version);
        self.hlc.observe(version.hlc, wall_ms);
        if let Some(cover) = self.covering(key, version) {
            return Ok(StagedApply::Done(Applied::Lost(cover)));
        }
        match self.engine.probe_physical_version(key) {
            vals_store::StagedVersion::Memtable(v) => {
                let old = Some(unpack_version(v));
                Ok(StagedApply::Done(
                    self.decide_and_store(key, value, version, old, wall_ms)?,
                ))
            }
            vals_store::StagedVersion::Absent => Ok(StagedApply::Done(
                self.decide_and_store(key, value, version, None, wall_ms)?,
            )),
            probe @ vals_store::StagedVersion::Runs { .. } => Ok(StagedApply::NeedsRunRead {
                probe,
                gen: self.engine.storage_generation(),
            }),
        }
    }

    pub(crate) fn apply_unconditional_stage(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
        mode: ApplyMode,
    ) -> Result<StagedApply, EngineError>
    where
        D: AsyncRead,
    {
        match mode {
            ApplyMode::Legacy => self.apply_stage(key, value, version, wall_ms),
            ApplyMode::Blind => self
                .apply_blind_at(key, value, version, wall_ms)
                .map(StagedApply::Done),
        }
    }

    pub fn apply_commit_staged(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        run_old: Option<Version>,
        gen: u64,
        wall_ms: u64,
    ) -> Result<StagedApplyOutcome, EngineError>
    where
        D: AsyncRead,
    {
        if self.engine.storage_generation() != gen {
            return Ok(StagedApplyOutcome::Retry);
        }
        Ok(StagedApplyOutcome::Applied(
            self.apply_commit_staged_snapshot(key, value, version, run_old, wall_ms)?,
        ))
    }

    pub(crate) fn apply_commit_staged_snapshot(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        run_old: Option<Version>,
        wall_ms: u64,
    ) -> Result<Applied, EngineError>
    where
        D: AsyncRead,
    {
        if let Some(cover) = self.covering(key, version) {
            return Ok(Applied::Lost(cover));
        }
        let mutable = match self.engine.probe_physical_version(key) {
            vals_store::StagedVersion::Memtable(v)
            | vals_store::StagedVersion::Runs {
                mutable: Some(v), ..
            } => Some(unpack_version(v)),
            _ => None,
        };
        let old = mutable.into_iter().chain(run_old).max();
        self.decide_and_store(key, value, version, old, wall_ms)
    }

    fn stamp_and_store(
        &mut self,
        key: &[u8],
        kind: u8,
        value: &[u8],
        phys_ms: u64,
    ) -> Result<Version, EngineError> {
        let old = self.physical_version(key)?;
        let cover = self.engine.range_tombstone_cover(key).map(unpack_version);
        let v = self.stamp(old.max(cover), phys_ms);
        self.note_version(v);
        self.store(key, old, v, kind, value)?;
        Ok(v)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<Version, EngineError> {
        self.put_at(key, value, physical_now_ms())
    }

    pub fn put_at(
        &mut self,
        key: &[u8],
        value: &[u8],
        phys_ms: u64,
    ) -> Result<Version, EngineError> {
        self.stamp_and_store(key, KIND_PUT, value, phys_ms)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<Version, EngineError> {
        self.delete_at(key, physical_now_ms())
    }

    pub fn delete_at(&mut self, key: &[u8], phys_ms: u64) -> Result<Version, EngineError> {
        self.stamp_and_store(key, KIND_TOMB, &[], phys_ms)
    }

    pub fn cas(
        &mut self,
        key: &[u8],
        value: &[u8],
        expected: Option<Version>,
    ) -> Result<CasResult, EngineError> {
        self.cas_at(key, value, expected, physical_now_ms())
    }

    pub fn cas_at(
        &mut self,
        key: &[u8],
        value: &[u8],
        expected: Option<Version>,
        phys_ms: u64,
    ) -> Result<CasResult, EngineError> {
        let current = self.current_version(key)?;
        if current != expected {
            return Ok(CasResult::Mismatch { current });
        }
        let v = self.stamp(current, phys_ms);
        self.note_version(v);
        let old = match self.engine.range_tombstone_cover(key) {
            None => current,
            Some(_) => self.physical_version(key)?,
        };
        self.store(key, old, v, KIND_PUT, value)?;
        Ok(CasResult::Applied(v))
    }
}

