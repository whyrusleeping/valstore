use super::*;

enum Winner {
    Entry(IndexEntry),
    Covered(u128),
}

fn with_cover(best: Option<IndexEntry>, cover: Option<u128>) -> Option<Winner> {
    match (best, cover) {
        (Some(e), Some(c)) if e.version <= c => Some(Winner::Covered(c)),
        (Some(e), _) => Some(Winner::Entry(e)),
        (None, Some(c)) => Some(Winner::Covered(c)),
        (None, None) => None,
    }
}

fn valid_midpoint(midpoint: Option<Vec<u8>>, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Vec<u8>> {
    midpoint.filter(|m| {
        !m.is_empty() && lo.is_none_or(|l| m.as_slice() > l) && hi.is_none_or(|h| m.as_slice() < h)
    })
}

impl<D: BlockDevice> Engine<D> {
    pub fn ordering(&self) -> Ordering {
        self.ordering
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn keys(&self) -> Vec<Vec<u8>> {
        self.index.keys()
    }

    pub fn index_versions(&self) -> RecoveredVersions {
        self.index.versions()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }

    pub fn total_entries(&self) -> usize {
        self.index.len()
            + self
                .runs
                .iter()
                .map(|r| r.meta.entry_count as usize)
                .sum::<usize>()
    }

    pub fn range_size_hint(&self, lo: Option<&[u8]>, hi: Option<&[u8]>, mem_cap: usize) -> u64 {
        if let (Some(l), Some(h)) = (lo, hi) {
            if l >= h {
                return 0;
            }
        }
        let mut approx: u64 = 0;
        for or in &self.runs {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let total = or.run.index_len();
            if total == 0 {
                continue;
            }
            let (start, end) = or.run.range_block_window(lo, hi);
            approx += or.run.entry_count() * end.saturating_sub(start) as u64 / total as u64;
        }
        let mem = self
            .index
            .scan_page(lo, hi, mem_cap.max(1), usize::MAX)
            .map_or(0, |(page, _)| page.len());
        approx + mem as u64
    }

    pub fn range_estimate(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> (u64, Option<Vec<u8>>) {
        if let (Some(l), Some(h)) = (lo, hi) {
            if l >= h {
                return (0, None);
            }
        }
        let (mut approx, mut midpoint) = self.run_range_estimate(lo, hi);
        let (mem, mem_midpoint) = self.index.range_count_median(lo, hi).unwrap_or_default();
        approx += mem as u64;
        if midpoint.is_none() {
            midpoint = mem_midpoint;
        }
        (approx, valid_midpoint(midpoint, lo, hi))
    }

    pub fn range_midpoint(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Option<Vec<u8>> {
        if let (Some(l), Some(h)) = (lo, hi) {
            if l >= h {
                return None;
            }
        }
        let midpoint = match self.run_range_estimate(lo, hi).1 {
            Some(m) => Some(m),
            None => self.index.range_count_median(lo, hi).and_then(|(_, m)| m),
        };
        valid_midpoint(midpoint, lo, hi)
    }

    fn run_range_estimate(&self, lo: Option<&[u8]>, hi: Option<&[u8]>) -> (u64, Option<Vec<u8>>) {
        let mut approx: u64 = 0;
        let mut best: Option<(u64, Vec<u8>)> = None;
        for or in &self.runs {
            if lo.is_some_and(|l| or.meta.max_key.as_slice() < l)
                || hi.is_some_and(|h| or.meta.min_key.as_slice() >= h)
            {
                continue;
            }
            let run = &or.run;
            let total = run.index_len();
            if total == 0 {
                continue;
            }
            let (start, end) = run.range_block_window(lo, hi);
            let window = end.saturating_sub(start);
            if window == 0 {
                continue;
            }
            let run_approx = run.entry_count() * window as u64 / total as u64;
            approx += run_approx;
            if window >= 2 {
                let mid_block = (start + window / 2).clamp(start + 1, end - 1);
                let mid = run.block_first_key(mid_block).to_vec();
                if best.as_ref().is_none_or(|(w, _)| run_approx > *w) {
                    best = Some((run_approx, mid));
                }
            }
        }
        (approx, best.map(|(_, m)| m))
    }

    pub(super) fn lookup_runs(&self, key: &[u8]) -> Result<Option<SnapshotEntry>, EngineError> {
        crate::attribution::record_lookup(crate::attribution::LookupCaller::Exists);
        self.lookup_runs_for(key, crate::attribution::LookupCaller::Exists)
    }

    fn lookup_runs_for(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> Result<Option<SnapshotEntry>, EngineError> {
        let mut best: Option<SnapshotEntry> = None;
        let hk = crate::run::BloomKey::new(key);
        let skip_covered = self.key_filter_excludes(&hk);
        for or in self.runs.iter() {
            if !(skip_covered && or.covered)
                && key >= or.meta.min_key.as_slice() && key <= or.meta.max_key.as_slice() {
                or.run.prefetch_filter(&hk);
            }
        }
        for or in self.runs.iter().rev() {
            if skip_covered && or.covered {
                continue;
            }
            if key < or.meta.min_key.as_slice() || key > or.meta.max_key.as_slice() {
                continue;
            }
            let (positive, result) = or.run.lookup_hashed_counted(&self.alloc, key, &hk);
            let p = u64::from(positive);
            crate::attribution::record_blooms(caller, 1, p, if or.covered { p } else { 0 }, if or.covered { 0 } else { p });
            if let Some(e) = result? {
                if best.as_ref().is_none_or(|b| e.version > b.version) {
                    best = Some(e);
                }
            }
        }
        Ok(best)
    }

    fn resolve_slow(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> Result<Option<IndexEntry>, EngineError> {
        let mut best = self.index.get(key);
        if let Some(e) = self.lookup_runs_for(key, caller)? {
            if best.as_ref().is_none_or(|b| e.version > b.version) {
                best = Some(IndexEntry {
                    ptr: e.ptr,
                    seq: e.seq,
                    version: e.version,
                    tombstone: e.tombstone,
                });
            }
        }
        Ok(best)
    }

    fn resolve_for(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> Result<Option<IndexEntry>, EngineError> {
        crate::attribution::record_lookup(caller);
        match &self.winner_directory {
            Some(directory) => Ok(directory.get(key)),
            None => self.resolve_slow(key, caller),
        }
    }

    fn resolve_logical(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> Result<Option<Winner>, EngineError> {
        let cover = self.range_tombstone_cover(key);
        Ok(with_cover(self.resolve_for(key, caller)?, cover))
    }

    /// The value of `key`'s greatest version, or `None` if the key has no
    /// entry or is covered by a newer range tombstone. Reads unsynced writes.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let entry = match self.resolve_logical(key, crate::attribution::LookupCaller::Get)? {
            None | Some(Winner::Covered(_)) => return Ok(None),
            Some(Winner::Entry(e)) => e,
        };
        let (seg_start, _) = self.alloc.segment_range(entry.ptr.segment);
        let rec = self
            .log
            .read_checked(&self.dev, seg_start, &entry.ptr, key, entry.version)?;
        match rec.kind {
            RecordKind::Put => Ok(Some(rec.value)),
            RecordKind::Delete | RecordKind::RangeDelete => Ok(None),
        }
    }

    /// The greatest version stored for `key` (or of a range tombstone
    /// covering it), or `None` if there is none.
    pub fn version(&self, key: &[u8]) -> Result<Option<u128>, EngineError> {
        self.version_for(key, crate::attribution::LookupCaller::Exists)
    }

    pub fn physical_version(&self, key: &[u8]) -> Result<Option<u128>, EngineError> {
        Ok(self.resolve_for(key, crate::attribution::LookupCaller::Exists)?.map(|e| e.version))
    }

    pub fn version_for(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> Result<Option<u128>, EngineError> {
        Ok(self.resolve_logical(key, caller)?.map(|w| match w {
            Winner::Entry(e) => e.version,
            Winner::Covered(c) => c,
        }))
    }

    pub fn record_len(&self, key: &[u8]) -> Option<u32> {
        self.index.get(key).map(|e| e.ptr.len)
    }

    fn resolve_items(&self, keys: &[&[u8]]) -> Result<ResolvedItems, EngineError> {
        let mut items: Vec<(u64, ValuePtr)> = Vec::new();
        let mut slots: Vec<(usize, u128)> = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            if let Some(Winner::Entry(e)) = self.resolve_logical(k, crate::attribution::LookupCaller::Get)? {
                items.push((self.alloc.segment_range(e.ptr.segment).0, e.ptr));
                slots.push((i, e.version));
            }
        }
        Ok((items, slots))
    }

    fn scatter(recs: Vec<Record>, slots: &[(usize, u128)], n: usize) -> Vec<Option<Vec<u8>>> {
        let mut out = vec![None; n];
        for (rec, &(i, _)) in recs.into_iter().zip(slots) {
            if rec.kind == RecordKind::Put {
                out[i] = Some(rec.value);
            }
        }
        out
    }

    pub fn multi_get(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let (items, slots) = self.resolve_items(keys)?;
        let expected: Vec<(&[u8], u128)> = slots
            .iter()
            .map(|&(slot, version)| (keys[slot], version))
            .collect();
        let recs = self.log.read_many_checked(&self.dev, &items, &expected)?;
        Ok(Self::scatter(recs, &slots, keys.len()))
    }

}

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub async fn multi_get_async(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, EngineError> {
        let (items, slots) = self.resolve_items(keys)?;
        let expected: Vec<(&[u8], u128)> = slots
            .iter()
            .map(|&(slot, version)| (keys[slot], version))
            .collect();
        let recs = self
            .log
            .read_many_async_checked(&self.dev, &items, &expected)
            .await?;
        Ok(Self::scatter(recs, &slots, keys.len()))
    }

    pub fn plan_get(&self, key: &[u8]) -> Result<Option<crate::vlog::ReadPlan>, EngineError> {
        let Some(Winner::Entry(e)) = self.resolve_logical(key, crate::attribution::LookupCaller::Get)? else {
            return Ok(None);
        };
        let seg_start = self.alloc.segment_range(e.ptr.segment).0;
        Ok(Some(self.log.plan_read(&self.dev, seg_start, &e.ptr)?))
    }

    pub fn probe_get(&self, key: &[u8]) -> StagedGet {
        crate::attribution::record_lookup(crate::attribution::LookupCaller::Get);
        let cover = self.range_tombstone_cover(key);
        let covered = || cover.map_or(StagedGet::Absent, StagedGet::Covered);
        if let Some(directory) = &self.winner_directory {
            let Some(token) = directory.read_token(key).filter(|t| cover.is_none_or(|c| t.entry.version > c)) else {
                return covered();
            };
            let seg_start = self.alloc.segment_range(token.entry.ptr.segment).0;
            return match self.log.plan_read(&self.dev, seg_start, &token.entry.ptr) {
                Ok(plan) => StagedGet::WinnerDirectory {
                    plan: Box::new(plan),
                    token,
                },
                Err(e) => StagedGet::Err(EngineError::Log(e)),
            };
        }
        let mutable = self.index.get(key);
        let mut probes = ProbeList::new();
        let hk = crate::run::BloomKey::new(key);
        let skip_covered = self.key_filter_excludes(&hk);
        for or in self.runs.iter() {
            if !(skip_covered && or.covered)
                && key >= or.meta.min_key.as_slice() && key <= or.meta.max_key.as_slice() {
                or.run.prefetch_filter(&hk);
            }
        }
        let mut bloom_probes = 0;
        let mut bloom_positives = 0;
        let mut covered_positives = 0;
        for or in self.runs.iter().rev() {
            if (skip_covered && or.covered)
                || key < or.meta.min_key.as_slice() || key > or.meta.max_key.as_slice() {
                continue;
            }
            bloom_probes += 1;
            let (positive, loc) = or.run.locate_block_hashed_counted(&self.alloc, key, &hk);
            bloom_positives += u64::from(positive);
            if or.covered { covered_positives += u64::from(positive); }
            if let Some(loc) = loc { probes.push(loc); }
        }
        crate::attribution::record_blooms(crate::attribution::LookupCaller::Get, bloom_probes, bloom_positives,
            covered_positives, bloom_positives - covered_positives);
        if probes.is_empty() {
            let Some(e) = mutable.filter(|e| cover.is_none_or(|c| e.version > c)) else {
                return covered();
            };
            let seg_start = self.alloc.segment_range(e.ptr.segment).0;
            return match self.log.plan_read(&self.dev, seg_start, &e.ptr) {
                Ok(plan) => StagedGet::Memtable(Box::new(plan)),
                Err(e) => StagedGet::Err(EngineError::Log(e)),
            };
        }
        StagedGet::Runs {
            key: key.to_vec(),
            probes,
            mutable,
            generation: self.storage_gen,
            cover,
        }
    }

    pub fn plan_get_ptr(&self, ptr: &ValuePtr) -> Result<crate::vlog::ReadPlan, EngineError> {
        let seg_start = self.alloc.segment_range(ptr.segment).0;
        Ok(self.log.plan_read(&self.dev, seg_start, ptr)?)
    }
}

pub async fn resolve_run_probe<D: AsyncRead + BlockDevice>(
    key: &[u8],
    probes: &[run::RangeLocator],
    dev: &D,
) -> Result<Option<SnapshotEntry>, EngineError> {
    Ok(resolve_run_probe_counted(key, probes, dev).await?.0)
}

pub async fn resolve_run_probe_counted<D: AsyncRead + BlockDevice>(
    key: &[u8],
    probes: &[run::RangeLocator],
    dev: &D,
) -> Result<(Option<SnapshotEntry>, u64), EngineError> {
    let mut blocks_read = 0u64;
    let mut best: Option<SnapshotEntry> = None;
    for loc in probes {
        let block = loc.read(dev).await?;
        blocks_read += 1;
        if let Some(e) = run::DeviceRun::<D>::block_lookup_bytes(&block, key)? {
            if best.as_ref().is_none_or(|b| e.version > b.version) {
                best = Some(e);
            }
        }
    }
    Ok((best, blocks_read))
}

pub type ProbeList = smallvec::SmallVec<[run::RangeLocator; 4]>;

#[allow(clippy::large_enum_variant)]
pub enum StagedGet {
    Memtable(Box<crate::vlog::ReadPlan>),
    WinnerDirectory {
        plan: Box<crate::vlog::ReadPlan>,
        token: WinnerReadToken,
    },
    Runs {
        key: Vec<u8>,
        probes: ProbeList,
        mutable: Option<IndexEntry>,
        generation: u64,
        cover: Option<u128>,
    },
    Covered(u128),
    Absent,
    Err(EngineError),
}

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub async fn version_from_probe(
        probe: StagedVersion,
        dev: &D,
    ) -> Result<Option<u128>, EngineError> {
        Ok(Self::version_from_probe_counted(probe, dev).await?.0)
    }

    pub async fn version_from_probe_counted(
        probe: StagedVersion,
        dev: &D,
    ) -> Result<(Option<u128>, u64), EngineError> {
        match probe {
            StagedVersion::Memtable(v) => Ok((Some(v), 0)),
            StagedVersion::Absent => Ok((None, 0)),
            StagedVersion::Runs {
                key,
                probes,
                mutable,
                cover,
                ..
            } => {
                let (entry, blocks) = resolve_run_probe_counted(&key, &probes, dev).await?;
                let version = entry
                    .map(|e| e.version)
                    .into_iter()
                    .chain(mutable)
                    .chain(cover)
                    .max();
                Ok((version, blocks))
            }
        }
    }

    pub fn probe_version(&self, key: &[u8]) -> StagedVersion {
        self.probe_version_for(key, crate::attribution::LookupCaller::Apply)
    }

    pub fn probe_version_for(&self, key: &[u8], caller: crate::attribution::LookupCaller) -> StagedVersion {
        self.probe_version_with(key, caller, self.range_tombstone_cover(key))
    }

    pub fn probe_physical_version(&self, key: &[u8]) -> StagedVersion {
        self.probe_version_with(key, crate::attribution::LookupCaller::Apply, None)
    }

    fn probe_version_with(&self, key: &[u8], caller: crate::attribution::LookupCaller, cover: Option<u128>) -> StagedVersion {
        crate::attribution::record_lookup(caller);
        if let Some(directory) = &self.winner_directory {
            return directory
                .get(key)
                .map(|entry| entry.version)
                .max(cover)
                .map_or(StagedVersion::Absent, StagedVersion::Memtable);
        }
        let mutable = self.index.get(key).map(|e| e.version);
        let mut probes = ProbeList::new();
        let hk = crate::run::BloomKey::new(key);
        let filtered = !self.key_filter.sub_filters().is_empty();
        let skip_covered = filtered && self.key_filter.excludes(&hk);
        if filtered {
            self.key_filter.probes.set(self.key_filter.probes.get() + 1);
            if skip_covered {
                self.key_filter.negatives.set(self.key_filter.negatives.get() + 1);
            }
        }
        for or in self.runs.iter() {
            if !(skip_covered && or.covered) {
                or.run.prefetch_filter(&hk);
            }
        }
        let mut run_probes = 0u64;
        let mut positives = 0;
        let mut covered_positives = 0;
        let mut uncovered_positives = 0;
        for or in self.runs.iter().rev() {
            if skip_covered && or.covered {
                continue;
            }
            if key < or.meta.min_key.as_slice() || key > or.meta.max_key.as_slice() {
                continue;
            }
            run_probes += 1;
            let (positive, loc) = or.run.locate_block_hashed_counted(&self.alloc, key, &hk);
            positives += u64::from(positive);
            if positive {
                if or.covered { covered_positives += 1; } else { uncovered_positives += 1; }
            }
            if let Some(loc) = loc { probes.push(loc); }
        }
        crate::attribution::record_blooms(caller, run_probes, positives, covered_positives, uncovered_positives);
        self.key_filter.run_probes.set(self.key_filter.run_probes.get() + run_probes);
        if probes.is_empty() {
            return mutable.max(cover).map_or(StagedVersion::Absent, StagedVersion::Memtable);
        }
        StagedVersion::Runs {
            key: key.to_vec(),
            probes,
            mutable,
            generation: self.storage_gen,
            cover,
        }
    }
}

