use super::*;

pub(super) type Winners = std::collections::HashMap<Vec<u8>, (u64, RecordKind, bool, ValuePtr, u128)>;

const WINNER_EMPTY: u32 = u32::MAX;
pub(super) const WINNER_DELETED: u32 = u32::MAX - 1;
const WINNER_CTRL_EMPTY: u8 = u8::MAX;
pub(super) const WINNER_CTRL_DELETED: u8 = u8::MAX - 1;

#[inline]
fn prefetch_read<T>(ptr: *const T) {
    #[cfg(target_arch = "x86_64")]
    // SAFETY: prefetch is only a non-faulting cache hint; callers derive pointers from live vectors.
    unsafe {
        std::arch::x86_64::_mm_prefetch(ptr.cast(), std::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(not(target_arch = "x86_64"))]
    let _ = ptr;
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct WinnerProbeProfile {
    pub(super) probes: usize,
    pub(super) fingerprint_matches: usize,
    pub(super) full_key_comparisons: usize,
    pub(super) baseline_full_key_comparisons: usize,
    pub(super) found: bool,
}

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct WinnerProbeDistribution {
    pub(super) queries: usize,
    pub(super) found: usize,
    pub(super) profiles: Vec<WinnerProbeProfile>,
    pub(super) fingerprint_matches: usize,
    pub(super) baseline_full_key_comparisons: usize,
    pub(super) full_key_comparisons: usize,
}

#[cfg(test)]
impl WinnerProbeDistribution {
    pub(super) fn record(&mut self, profile: WinnerProbeProfile) {
        self.queries += 1;
        self.found += usize::from(profile.found);
        self.profiles.push(profile);
        self.fingerprint_matches += profile.fingerprint_matches;
        self.baseline_full_key_comparisons += profile.baseline_full_key_comparisons;
        self.full_key_comparisons += profile.full_key_comparisons;
    }

    pub(super) fn summary(values: impl Iterator<Item = usize>) -> (usize, f64, usize, usize, usize) {
        let mut sorted: Vec<usize> = values.collect();
        let total: usize = sorted.iter().sum();
        let count = sorted.len();
        sorted.sort_unstable();
        let percentile = |p: usize| sorted[(sorted.len() - 1) * p / 100];
        (
            total,
            total as f64 / count as f64,
            percentile(50),
            percentile(99),
            *sorted.last().unwrap(),
        )
    }
}

#[derive(Clone, Copy)]
pub(super) struct PackedWinner {
    key_offset: u64,
    key_len: u32,
    entry: PackedEntry,
}

const _: () = assert!(std::mem::size_of::<PackedWinner>() == 48);

#[derive(Default)]
pub(super) struct WinnerDirectory {
    pub(super) keys: Vec<u8>,
    pub(super) entries: Vec<PackedWinner>,
    pub(super) table: Vec<u32>,
    pub(super) controls: Vec<u8>,
    pub(super) deleted_buckets: usize,
    pub(super) live_key_bytes: usize,
    dead_key_bytes: usize,
}

impl WinnerDirectory {
    #[cfg(test)]
    pub(super) fn with_capacity(capacity: usize) -> Self {
        let mut directory = WinnerDirectory {
            entries: Vec::with_capacity(capacity),
            ..WinnerDirectory::default()
        };
        if capacity > 0 {
            directory.rebuild_table(Self::table_capacity(capacity));
        }
        directory
    }

    fn table_capacity(entries: usize) -> usize {
        entries
            .saturating_mul(10)
            .div_ceil(7)
            .max(8)
            .next_power_of_two()
    }

    #[inline]
    pub(super) fn hash(key: &[u8]) -> u64 {
        let mut hasher = FxBuildHasher::default().build_hasher();
        hasher.write(key);
        hasher.finish()
    }

    #[inline]
    pub(super) fn fingerprint(hash: u64) -> u8 {
        (hash >> 57) as u8
    }

    #[inline]
    fn key_at(&self, entry: &PackedWinner) -> &[u8] {
        let start = entry.key_offset as usize;
        &self.keys[start..start + entry.key_len as usize]
    }

    pub(super) fn find(&self, key: &[u8]) -> Option<(usize, usize)> {
        self.find_hashed(key, Self::hash(key))
    }

    fn find_hashed(&self, key: &[u8], hash: u64) -> Option<(usize, usize)> {
        if self.table.is_empty() {
            return None;
        }
        let fingerprint = Self::fingerprint(hash);
        let mask = self.table.len() - 1;
        let mut bucket = hash as usize & mask;
        for _ in 0..self.table.len() {
            match self.controls[bucket] {
                WINNER_CTRL_EMPTY => return None,
                WINNER_CTRL_DELETED => {}
                control if control == fingerprint => {
                    let index = self.table[bucket] as usize;
                    if self.key_at(&self.entries[index]) == key {
                        return Some((bucket, index));
                    }
                }
                _ => {}
            }
            bucket = (bucket + 1) & mask;
        }
        None
    }

    #[cfg(test)]
    pub(super) fn probe_profile(&self, key: &[u8]) -> WinnerProbeProfile {
        let mut profile = WinnerProbeProfile::default();
        if self.table.is_empty() {
            return profile;
        }
        let hash = Self::hash(key);
        let fingerprint = Self::fingerprint(hash);
        let mask = self.table.len() - 1;
        let mut bucket = hash as usize & mask;
        for _ in 0..self.table.len() {
            profile.probes += 1;
            match self.controls[bucket] {
                WINNER_CTRL_EMPTY => return profile,
                WINNER_CTRL_DELETED => {}
                control => {
                    profile.baseline_full_key_comparisons += 1;
                    if control == fingerprint {
                        profile.fingerprint_matches += 1;
                        profile.full_key_comparisons += 1;
                        let index = self.table[bucket] as usize;
                        if self.key_at(&self.entries[index]) == key {
                            profile.found = true;
                            return profile;
                        }
                    }
                }
            }
            bucket = (bucket + 1) & mask;
        }
        profile
    }

    pub(super) fn get(&self, key: &[u8]) -> Option<IndexEntry> {
        self.find(key)
            .map(|(_, index)| self.entries[index].entry.unpack())
    }

    pub(super) fn read_token(&self, key: &[u8]) -> Option<WinnerReadToken> {
        self.find(key).map(|(_, index)| WinnerReadToken {
            index: index as u32,
            entry: self.entries[index].entry.unpack(),
        })
    }

    pub(super) fn read_tokens(&self, keys: &[&[u8]]) -> Vec<Option<WinnerReadToken>> {
        if self.table.is_empty() {
            return vec![None; keys.len()];
        }
        let hashes: Vec<u64> = keys.iter().map(|key| Self::hash(key)).collect();
        let mask = self.table.len() - 1;
        for hash in &hashes {
            let bucket = *hash as usize & mask;
            prefetch_read(self.controls.as_ptr().wrapping_add(bucket));
            prefetch_read(self.table.as_ptr().wrapping_add(bucket));
        }
        let mut candidates = Vec::with_capacity(keys.len());
        for hash in &hashes {
            let bucket = *hash as usize & mask;
            let control = self.controls[bucket];
            let candidate = if control == WINNER_CTRL_EMPTY {
                WINNER_EMPTY
            } else if control == Self::fingerprint(*hash) {
                let index = self.table[bucket];
                prefetch_read(self.entries.as_ptr().wrapping_add(index as usize));
                index
            } else {
                WINNER_DELETED
            };
            candidates.push(candidate);
        }
        for &index in &candidates {
            if index < WINNER_DELETED {
                let winner = &self.entries[index as usize];
                prefetch_read(self.keys.as_ptr().wrapping_add(winner.key_offset as usize));
            }
        }
        keys.iter()
            .zip(hashes)
            .zip(candidates)
            .map(|((key, hash), index)| {
                if index == WINNER_EMPTY {
                    return None;
                }
                let found = if index < WINNER_DELETED
                    && self.key_at(&self.entries[index as usize]) == *key
                {
                    Some(index as usize)
                } else {
                    self.find_hashed(key, hash).map(|(_, index)| index)
                };
                found.map(|index| WinnerReadToken {
                    index: index as u32,
                    entry: self.entries[index].entry.unpack(),
                })
            })
            .collect()
    }

    pub(super) fn read_tokens_profiled_into<'a>(
        &self,
        count: usize,
        mut key_at: impl FnMut(usize) -> &'a [u8],
        scratch: &mut WinnerReadScratch,
    ) -> WinnerProbeStats {
        scratch.clear();
        scratch.hashes.reserve(count);
        scratch.candidates.reserve(count);
        scratch.tokens.resize(count, None);
        let mut stats = WinnerProbeStats {
            lookups: count as u64,
            ..WinnerProbeStats::default()
        };
        if self.table.is_empty() {
            stats.empty_misses = count as u64;
            stats.probe_len_le_1 = count as u64;
            return stats;
        }
        let mask = self.table.len() - 1;
        for i in 0..count {
            let hash = Self::hash(key_at(i));
            scratch.hashes.push(hash);
            let bucket = hash as usize & mask;
            prefetch_read(self.controls.as_ptr().wrapping_add(bucket));
            prefetch_read(self.table.as_ptr().wrapping_add(bucket));
        }
        for &hash in &scratch.hashes {
            let bucket = hash as usize & mask;
            let control = self.controls[bucket];
            let candidate = if control == WINNER_CTRL_EMPTY {
                WINNER_EMPTY
            } else if control == Self::fingerprint(hash) {
                let index = self.table[bucket];
                prefetch_read(self.entries.as_ptr().wrapping_add(index as usize));
                index
            } else {
                WINNER_DELETED
            };
            scratch.candidates.push(candidate);
        }
        for &index in &scratch.candidates {
            if index < WINNER_DELETED {
                let winner = &self.entries[index as usize];
                prefetch_read(self.keys.as_ptr().wrapping_add(winner.key_offset as usize));
            }
        }
        for i in 0..count {
            let key = key_at(i);
            let hash = scratch.hashes[i];
            let fingerprint = Self::fingerprint(hash);
            let home = hash as usize & mask;
            let candidate = scratch.candidates[i];
            let (found, probes) = if candidate == WINNER_EMPTY {
                stats.empty_misses += 1;
                (None, 1)
            } else if candidate < WINNER_DELETED
                && self.key_at(&self.entries[candidate as usize]) == key
            {
                stats.home_slot_hits += 1;
                (Some(candidate as usize), 1)
            } else {
                let control = self.controls[home];
                if control == WINNER_CTRL_DELETED {
                    stats.deleted_buckets += 1;
                } else if control != fingerprint {
                    stats.fingerprint_mismatches += 1;
                } else {
                    stats.full_key_collisions += 1;
                }
                let mut bucket = (home + 1) & mask;
                let mut probes = 1u64;
                let found = loop {
                    probes += 1;
                    let control = self.controls[bucket];
                    if control == WINNER_CTRL_EMPTY {
                        stats.empty_misses += 1;
                        break None;
                    }
                    if control == WINNER_CTRL_DELETED {
                        stats.deleted_buckets += 1;
                    } else if control != fingerprint {
                        stats.fingerprint_mismatches += 1;
                    } else {
                        let index = self.table[bucket] as usize;
                        if self.key_at(&self.entries[index]) == key {
                            break Some(index);
                        }
                        stats.full_key_collisions += 1;
                    }
                    bucket = (bucket + 1) & mask;
                };
                (found, probes)
            };
            stats.total_probe_len += probes;
            if probes > 1 {
                stats.collision_fallbacks += 1;
            }
            stats.record_probe_len(probes);
            scratch.tokens[i] = found.map(|index| WinnerReadToken {
                index: index as u32,
                entry: self.entries[index].entry.unpack(),
            });
        }
        stats
    }

    fn read_tokens_profiled(
        &self,
        keys: &[&[u8]],
    ) -> (Vec<Option<WinnerReadToken>>, WinnerProbeStats) {
        let mut scratch = WinnerReadScratch::default();
        let stats = self.read_tokens_profiled_into(keys.len(), |i| keys[i], &mut scratch);
        (scratch.tokens, stats)
    }

    fn token_matches(&self, key: &[u8], token: WinnerReadToken) -> bool {
        let Some(winner) = self.entries.get(token.index as usize) else {
            return false;
        };
        winner.entry.unpack() == token.entry && self.key_at(winner) == key
    }

    pub(super) fn insert(&mut self, key: &[u8], entry: IndexEntry) -> Option<IndexEntry> {
        if let Some((_, index)) = self.find(key) {
            let old = self.entries[index].entry.unpack();
            self.entries[index].entry = PackedEntry::pack(&entry);
            return Some(old);
        }
        self.reserve_insert();
        assert!(key.len() <= u32::MAX as usize, "winner-directory key is too long");
        assert!(self.entries.len() < WINNER_DELETED as usize, "winner directory is too large");
        let key_offset = self.keys.len() as u64;
        self.keys.extend_from_slice(key);
        self.live_key_bytes += key.len();
        let index = self.entries.len();
        self.entries.push(PackedWinner {
            key_offset,
            key_len: key.len() as u32,
            entry: PackedEntry::pack(&entry),
        });
        self.insert_bucket(index);
        None
    }

    pub(super) fn insert_max(&mut self, key: &[u8], candidate: IndexEntry) -> bool {
        if let Some((_, index)) = self.find(key) {
            if candidate.version >= self.entries[index].entry.unpack().version {
                self.entries[index].entry = PackedEntry::pack(&candidate);
                return true;
            }
            return false;
        }
        self.insert(key, candidate);
        true
    }

    pub(super) fn remove(&mut self, key: &[u8]) -> Option<IndexEntry> {
        let (bucket, index) = self.find(key)?;
        let removed = self.entries[index];
        self.table[bucket] = WINNER_DELETED;
        self.controls[bucket] = WINNER_CTRL_DELETED;
        self.deleted_buckets += 1;
        self.live_key_bytes -= removed.key_len as usize;
        self.dead_key_bytes += removed.key_len as usize;

        let last = self.entries.len() - 1;
        if index != last {
            let moved = self.entries[last];
            let moved_key = self.key_at(&moved);
            let moved_bucket = self
                .find(moved_key)
                .map(|(bucket, found)| {
                    debug_assert_eq!(found, last);
                    bucket
                })
                .expect("winner-directory moved entry disappeared");
            self.entries[index] = moved;
            self.table[moved_bucket] = index as u32;
        }
        self.entries.pop();
        self.compact_after_remove();
        Some(removed.entry.unpack())
    }

    fn reserve_insert(&mut self) {
        if self.table.is_empty() {
            self.rebuild_table(8);
        } else if (self.entries.len() + self.deleted_buckets + 1).saturating_mul(10)
            >= self.table.len().saturating_mul(7)
        {
            let capacity = Self::table_capacity(self.entries.len() + 1);
            self.rebuild_table(capacity.max(self.table.len().saturating_mul(2)));
        }
    }

    fn insert_bucket(&mut self, index: usize) {
        let hash = Self::hash(self.key_at(&self.entries[index]));
        let fingerprint = Self::fingerprint(hash);
        let mask = self.table.len() - 1;
        let mut bucket = hash as usize & mask;
        let mut deleted = None;
        loop {
            match self.controls[bucket] {
                WINNER_CTRL_EMPTY => {
                    let target = deleted.unwrap_or(bucket);
                    if deleted.is_some() {
                        self.deleted_buckets -= 1;
                    }
                    self.table[target] = index as u32;
                    self.controls[target] = fingerprint;
                    return;
                }
                WINNER_CTRL_DELETED => {
                    deleted.get_or_insert(bucket);
                }
                _ => {}
            }
            bucket = (bucket + 1) & mask;
        }
    }

    fn rebuild_table(&mut self, capacity: usize) {
        self.table = vec![WINNER_EMPTY; capacity.max(8).next_power_of_two()];
        self.controls = vec![WINNER_CTRL_EMPTY; self.table.len()];
        self.deleted_buckets = 0;
        for index in 0..self.entries.len() {
            self.insert_bucket(index);
        }
    }

    fn compact_after_remove(&mut self) {
        if self.entries.is_empty() {
            self.clear();
            return;
        }
        if self.dead_key_bytes >= 4096 && self.dead_key_bytes >= self.live_key_bytes / 2 {
            let mut keys = Vec::with_capacity(self.live_key_bytes);
            for entry in &mut self.entries {
                let start = entry.key_offset as usize;
                let end = start + entry.key_len as usize;
                entry.key_offset = keys.len() as u64;
                keys.extend_from_slice(&self.keys[start..end]);
            }
            self.keys = keys;
            self.dead_key_bytes = 0;
        }
        let desired = Self::table_capacity(self.entries.len());
        if self.deleted_buckets > self.entries.len() / 2 || desired < self.table.len() / 2 {
            if desired < self.table.len() / 2 {
                self.entries.shrink_to_fit();
            }
            self.rebuild_table(desired);
        }
    }

    pub(super) fn clear(&mut self) {
        *self = WinnerDirectory::default();
    }

    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(super) fn allocated_bytes(&self) -> u64 {
        self.keys.capacity() as u64
            + (self.entries.capacity() * std::mem::size_of::<PackedWinner>()) as u64
            + (self.table.capacity() * std::mem::size_of::<u32>()) as u64
            + self.controls.capacity() as u64
    }

    pub(super) fn finish_build(&mut self) {
        if self.entries.is_empty() {
            self.clear();
            return;
        }
        self.keys.shrink_to_fit();
        self.entries.shrink_to_fit();
        self.rebuild_table(Self::table_capacity(self.entries.len()));
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&[u8], IndexEntry)> {
        self.entries
            .iter()
            .map(|entry| (self.key_at(entry), entry.entry.unpack()))
    }
}

pub type RecoveredVersions = Vec<(Box<[u8]>, u128)>;

pub struct AuthoritativePut {
    pub old_version: Option<u128>,
    pub written: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WinnerReadToken {
    index: u32,
    pub entry: IndexEntry,
}

#[derive(Default)]
pub(super) struct WinnerReadScratch {
    pub(super) hashes: Vec<u64>,
    pub(super) candidates: Vec<u32>,
    pub(super) tokens: Vec<Option<WinnerReadToken>>,
}

impl WinnerReadScratch {
    #[cfg(test)]
    pub(super) fn with_capacity(capacity: usize) -> Self {
        WinnerReadScratch {
            hashes: Vec::with_capacity(capacity),
            candidates: Vec::with_capacity(capacity),
            tokens: Vec::with_capacity(capacity),
        }
    }

    #[cfg(test)]
    pub(super) fn tokens(&self) -> &[Option<WinnerReadToken>] {
        &self.tokens
    }

    pub(super) fn clear(&mut self) {
        self.hashes.clear();
        self.candidates.clear();
        self.tokens.clear();
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WinnerProbeStats {
    pub lookups: u64,
    pub home_slot_hits: u64,
    pub empty_misses: u64,
    pub fingerprint_mismatches: u64,
    pub deleted_buckets: u64,
    pub collision_fallbacks: u64,
    pub full_key_collisions: u64,
    pub total_probe_len: u64,
    pub probe_len_le_1: u64,
    pub probe_len_2_4: u64,
    pub probe_len_5_8: u64,
    pub probe_len_gt_8: u64,
}

impl WinnerProbeStats {
    fn record_probe_len(&mut self, probes: u64) {
        match probes {
            0 | 1 => self.probe_len_le_1 += 1,
            2..=4 => self.probe_len_2_4 += 1,
            5..=8 => self.probe_len_5_8 += 1,
            _ => self.probe_len_gt_8 += 1,
        }
    }
}

static WINNER_DIRECTORY_REQUESTED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
static WINNER_DIRECTORY_DEGRADED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn configure_winner_directory(enabled: bool) -> Result<(), bool> {
    match WINNER_DIRECTORY_REQUESTED.set(enabled) {
        Ok(()) => Ok(()),
        Err(requested) if WINNER_DIRECTORY_REQUESTED.get() == Some(&requested) => Ok(()),
        Err(_) => Err(*WINNER_DIRECTORY_REQUESTED.get().unwrap()),
    }
}

pub fn winner_directory_requested() -> bool {
    *WINNER_DIRECTORY_REQUESTED.get().unwrap_or(&false)
}

pub fn winner_directory_operational() -> bool {
    winner_directory_requested()
        && !WINNER_DIRECTORY_DEGRADED.load(std::sync::atomic::Ordering::Acquire)
}

pub fn degrade_winner_directory() {
    if winner_directory_requested()
        && !WINNER_DIRECTORY_DEGRADED.swap(true, std::sync::atomic::Ordering::AcqRel)
    {
        log::error!(
            "winner directory degraded after a runtime rebuild failure; blind append is disabled until process restart"
        );
    }
}

impl<D: BlockDevice> Engine<D> {
    pub fn recovered_versions(&self) -> Result<RecoveredVersions, EngineError> {
        if let Some(directory) = &self.winner_directory {
            return Ok(directory
                .iter()
                .map(|(key, entry)| (Box::from(key), entry.version))
                .collect());
        }
        let cap = self.index.len()
            + self
                .runs
                .iter()
                .map(|r| r.meta.entry_count as usize)
                .sum::<usize>();
        let mut by_key: std::collections::HashMap<Box<[u8]>, u128> =
            std::collections::HashMap::with_capacity(cap);
        by_key.extend(self.index.versions());
        for or in &self.runs {
            or.run.for_each_bucket_digest(&self.alloc, |key, v| {
                if let Some(cur) = by_key.get_mut(key) {
                    if v > *cur {
                        *cur = v;
                    }
                } else {
                    by_key.insert(Box::from(key), v);
                }
            })?;
        }
        Ok(by_key.into_iter().collect())
    }

    pub fn rebuild_winner_directory(&mut self) -> Result<usize, EngineError> {
        let rebuilt = self.build_winner_directory();
        match rebuilt {
            Ok(directory) => {
                let len = directory.len();
                self.winner_directory = Some(directory);
                self.key_filter_maintain();
                Ok(len)
            }
            Err(e) => {
                if winner_directory_requested() {
                    degrade_winner_directory();
                    self.winner_directory = None;
                }
                Err(e)
            }
        }
    }

    pub(super) fn refresh_winner_directory_after_commit(&mut self) {
        if WINNER_DIRECTORY_DEGRADED.load(std::sync::atomic::Ordering::Acquire) {
            self.winner_directory = None;
            self.key_filter_maintain();
            return;
        }
        if let Err(e) = self.rebuild_winner_directory() {
            self.winner_directory = None;
            degrade_winner_directory();
            self.key_filter_maintain();
            log::warn!(
                "engine: winner directory rebuild after a committed run-set change failed: {e}; continuing without the directory"
            );
        }
    }

    pub(super) fn build_winner_directory(&self) -> Result<WinnerDirectory, EngineError> {
        let mut directory = WinnerDirectory::default();
        for or in &self.runs {
            let mut scan = or.run.block_scan(None, None);
            while let Some(entry) = scan.next(&self.alloc)? {
                let candidate = IndexEntry {
                    ptr: entry.ptr,
                    seq: entry.seq,
                    version: entry.version,
                    tombstone: entry.tombstone,
                };
                directory.insert_max(&entry.key, candidate);
            }
        }
        for key in self.index.keys() {
            let entry = self.index.get(&key).expect("winner-directory index key disappeared");
            directory.insert_max(&key, entry);
        }
        directory.finish_build();
        Ok(directory)
    }

    pub fn winner_directory_enabled(&self) -> bool {
        self.winner_directory.is_some()
    }

    pub fn winner_directory_version(&self, key: &[u8]) -> Option<Option<u128>> {
        self.winner_directory
            .as_ref()
            .map(|directory| directory.get(key).map(|entry| entry.version).max(self.range_tombstone_cover(key)))
    }

    #[cfg(test)]
    pub(super) fn winner_directory_entry(&self, key: &[u8]) -> Option<IndexEntry> {
        self.winner_directory
            .as_ref()
            .and_then(|directory| directory.get(key))
    }

    pub fn winner_read_tokens(&self, keys: &[&[u8]]) -> Option<Vec<Option<WinnerReadToken>>> {
        self.winner_directory
            .as_ref()
            .filter(|_| self.rts.fragments.is_empty())
            .map(|directory| directory.read_tokens(keys))
    }

    pub fn winner_read_tokens_profiled(
        &self,
        keys: &[&[u8]],
    ) -> Option<(Vec<Option<WinnerReadToken>>, WinnerProbeStats)> {
        self.winner_directory
            .as_ref()
            .filter(|_| self.rts.fragments.is_empty())
            .map(|directory| directory.read_tokens_profiled(keys))
    }

    pub fn winner_read_token_matches(&self, key: &[u8], token: WinnerReadToken) -> bool {
        self.winner_directory
            .as_ref()
            .is_some_and(|directory| directory.token_matches(key, token))
    }

    pub fn plan_winner_read(
        &self,
        token: WinnerReadToken,
    ) -> Result<crate::vlog::ReadPlan, EngineError> {
        let seg_start = self.alloc.segment_range(token.entry.ptr.segment).0;
        Ok(self.log.plan_read(&self.dev, seg_start, &token.entry.ptr)?)
    }

    pub fn winner_directory_len(&self) -> u64 {
        self.winner_directory
            .as_ref()
            .map_or(0, |directory| directory.len() as u64)
    }

    pub fn winner_directory_bytes(&self) -> u64 {
        self.winner_directory
            .as_ref()
            .map_or(0, WinnerDirectory::allocated_bytes)
    }

}

