use super::*;

type LeafGroups = std::collections::HashMap<Box<[NodeId]>, Box<[u64]>>;

pub(super) const INDEX_BUCKETS: u32 = crate::antientropy::DEFAULT_BUCKETS;

pub(super) const DIGEST_WIRE_OVERHEAD: usize = 4 + 16;
pub(super) const DIRTY_REBUILD_PAGE_BYTES: usize = 256 * 1024;
pub(super) const MAX_DIRTY_REBUILD_ATTEMPTS: usize = 3;

#[derive(Clone, Copy)]
pub(super) enum OldWinner {
    Exact(Option<Version>),
    Unknown,
}

pub(super) enum LeafOp {
    Apply {
        key: Box<[u8]>,
        old: OldWinner,
        new: Version,
    },
    Retire {
        key: Box<[u8]>,
        version: Version,
    },
}

#[derive(Clone, Copy)]
pub(super) struct BucketMeta {
    pub(super) generation: u64,
    pub(super) logical_keys: u64,
}

impl BucketMeta {
    pub(super) fn clean() -> Self {
        BucketMeta {
            generation: 0,
            logical_keys: 0,
        }
    }
}

#[derive(Debug)]
pub enum LeafDigestError {
    Engine(EngineError),
    Dirty,
}

impl std::fmt::Display for LeafDigestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeafDigestError::Engine(e) => write!(f, "{e}"),
            LeafDigestError::Dirty => write!(f, "logical-winner Merkle bucket is dirty"),
        }
    }
}

impl From<EngineError> for LeafDigestError {
    fn from(value: EngineError) -> Self {
        LeafDigestError::Engine(value)
    }
}

#[cfg(not(test))]
pub(super) const SEED_MEMORY_OPS_PER_STEP: usize = 4096;
#[cfg(test)]
pub(super) const SEED_MEMORY_OPS_PER_STEP: usize = 64;

pub(super) const MAX_SEED_RESTARTS: u32 = 8;

#[cfg(not(test))]
const JOURNAL_CHARGE_STEP: u64 = 256 * 1024;
#[cfg(test)]
const JOURNAL_CHARGE_STEP: u64 = 1024;

#[cfg(not(test))]
const JOURNAL_RESERVE_CHUNK: u64 = 64 * 1024;
#[cfg(test)]
const JOURNAL_RESERVE_CHUNK: u64 = 256;

#[cfg(not(test))]
static JOURNAL_NODE_RESERVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
thread_local! {
    static JOURNAL_NODE_RESERVED: std::sync::atomic::AtomicU64 =
        const { std::sync::atomic::AtomicU64::new(0) };
}

fn with_journal_reserved<R>(f: impl FnOnce(&std::sync::atomic::AtomicU64) -> R) -> R {
    #[cfg(not(test))]
    return f(&JOURNAL_NODE_RESERVED);
    #[cfg(test)]
    return JOURNAL_NODE_RESERVED.with(f);
}

fn reserve_on(
    counter: &std::sync::atomic::AtomicU64,
    min: u64,
    want: u64,
    cap: Option<u64>,
) -> Option<u64> {
    use std::sync::atomic::Ordering;
    let mut granted = 0;
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            let room = cap.map_or(u64::MAX, |cap| cap.saturating_sub(current));
            granted = want.max(min).min(room);
            (granted >= min).then(|| current + granted)
        })
        .ok()?;
    Some(granted)
}

fn reserve_journal_node(min: u64, want: u64, cap: Option<u64>) -> Option<u64> {
    with_journal_reserved(|counter| reserve_on(counter, min, want, cap))
}

fn release_journal_node(bytes: u64) {
    if bytes != 0 {
        with_journal_reserved(|counter| {
            counter.fetch_sub(bytes, std::sync::atomic::Ordering::AcqRel);
        });
    }
}

#[cfg(test)]
pub(super) fn journal_node_bytes_for_test() -> u64 {
    with_journal_reserved(|counter| counter.load(std::sync::atomic::Ordering::Acquire))
}

#[cfg(test)]
mod reserve_tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn concurrent_reservations_never_exceed_the_cap() {
        let counter = AtomicU64::new(0);
        let cap = 1_000_000u64;
        let granted = AtomicU64::new(0);
        std::thread::scope(|scope| {
            for t in 0..8u64 {
                let (counter, granted) = (&counter, &granted);
                scope.spawn(move || {
                    for i in 0..20_000u64 {
                        let min = 1 + (i * 7 + t) % 97;
                        if let Some(got) = reserve_on(counter, min, min + 300, Some(cap)) {
                            assert!(got >= min);
                            granted.fetch_add(got, Ordering::Relaxed);
                            assert!(counter.load(Ordering::Acquire) <= cap);
                        }
                    }
                });
            }
        });
        assert_eq!(counter.load(Ordering::Relaxed), granted.load(Ordering::Relaxed));
        assert!(counter.load(Ordering::Relaxed) <= cap);
        assert!(counter.load(Ordering::Relaxed) > cap - 400, "the cap is filled, not undershot");
        assert_eq!(reserve_on(&counter, 1_000, 1_000, Some(cap)), None);
        assert_eq!(reserve_on(&counter, 1_000, 1_000, None), Some(1_000), "the forced path overdraws");
    }
}

pub(super) struct SeedSpill {
    pub(super) buckets: Box<[i64]>,
    pub(super) live: i64,
    pub(super) ranges: Vec<i64>,
    pub(super) ranges_unknown: bool,
}

impl SeedSpill {
    pub(super) fn new(ranges: usize) -> Self {
        SeedSpill {
            buckets: vec![0; INDEX_BUCKETS as usize].into_boxed_slice(),
            live: 0,
            ranges: vec![0; ranges],
            ranges_unknown: false,
        }
    }

    pub(super) fn bytes(&self) -> u64 {
        ((self.buckets.len() + self.ranges.len()) * std::mem::size_of::<i64>()) as u64
    }
}

pub(super) fn apply_delta(value: u64, delta: i64) -> u64 {
    if delta >= 0 {
        value.saturating_add(delta as u64)
    } else {
        value.saturating_sub(delta.unsigned_abs())
    }
}

pub(super) struct SeedProgress {
    pub(super) _permit: Option<super::seed_permit::SeedPermit>,
    pub(super) reservation: crate::ram_broker::Reservation,
    pub(super) reservation_floor: u64,
    pub(super) reservation_failed: bool,
    pub(super) force_overdraw: bool,
    pub(super) mem_pending: vals_store::RecoveredVersions,
    pub(super) mem_pending_bytes: u64,
    pub(super) projection_generation: u64,
    pub(super) logical_range_generation: Option<u64>,
    pub(super) logical_range_counts: Vec<u64>,
    pub(super) range_map: Option<RangeMap>,
    pub(super) spill: Option<SeedSpill>,
    pub(super) journal_pos: usize,
    pub(super) staging: LeafGroups,
    pub(super) bucket_counts: Box<[u64]>,
    pub(super) bucket_dirty: Box<[Option<std::time::Instant>]>,
    pub(super) live: u64,
    pub(super) high_ms: u64,
    pub(super) estimated_bytes: u64,
    pub(super) cover: Option<std::sync::Arc<vals_store::range_tombstone::FragmentSet>>,
}

impl SeedProgress {
    pub(super) fn group_bytes(group: &[NodeId], leaves: &[u64]) -> u64 {
        (std::mem::size_of::<(Box<[NodeId]>, Box<[u64]>)>()
            + group.len() * std::mem::size_of::<NodeId>()
            + leaves.len() * std::mem::size_of::<u64>()) as u64
    }

    pub(super) fn try_adjust_estimate(&mut self, old: u64, new: u64) -> bool {
        let next = self.estimated_bytes.saturating_sub(old).saturating_add(new);
        let target = next.max(self.reservation_floor);
        if target > self.reservation.bytes() {
            let growth = target - self.reservation.bytes();
            if self.force_overdraw {
                self.reservation.overdraw_grow(growth);
            } else if !self.reservation.try_grow(growth) {
                self.reservation_failed = true;
                return false;
            }
        } else if target < self.reservation.bytes() {
            self.reservation.shrink(self.reservation.bytes() - target);
        }
        self.estimated_bytes = next;
        crate::observe::gauges().ae_seed_staged_bytes.adjust(old, new);
        true
    }

    pub(super) fn adjust_estimate(&mut self, old: u64, new: u64) {
        let _ = self.try_adjust_estimate(old, new);
    }

    pub(super) fn try_group_mut(&mut self, group: &[NodeId]) -> Option<&mut [u64]> {
        if !self.staging.contains_key(group) {
            let bytes = Self::group_bytes(group, &[])
                + INDEX_BUCKETS as u64 * std::mem::size_of::<u64>() as u64;
            if !self.try_adjust_estimate(0, bytes) { return None; }
            self.staging.insert(group.into(), VersionIndex::empty_group());
        }
        self.staging.get_mut(group).map(|leaves| leaves.as_mut())
    }

    pub(super) fn release_staging_estimate(&mut self) {
        let bytes: u64 = self
            .staging
            .iter()
            .map(|(group, leaves)| Self::group_bytes(group, leaves))
            .sum();
        self.adjust_estimate(bytes, 0);
    }
}

impl Drop for SeedProgress {
    fn drop(&mut self) {
        crate::observe::gauges()
            .ae_seed_staged_bytes
            .adjust(self.estimated_bytes, 0);
    }
}

#[derive(Clone, Copy)]
pub(super) struct DirtyGaugeState {
    pub(super) count: u64,
    pub(super) oldest: Option<std::time::Instant>,
}

type DirtyGaugeRegistry =
    std::sync::Mutex<std::collections::HashMap<u64, DirtyGaugeState>>;

pub(super) fn dirty_gauge_registry() -> &'static DirtyGaugeRegistry {
    static REGISTRY: std::sync::OnceLock<DirtyGaugeRegistry> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

fn publish_dirty_gauges(id: u64, state: Option<DirtyGaugeState>) {
    let mut registry = dirty_gauge_registry().lock().unwrap_or_else(|e| e.into_inner());
    match state {
        Some(state) => {
            registry.insert(id, state);
        }
        None => {
            registry.remove(&id);
        }
    }
    let count = registry.values().map(|s| s.count).sum();
    let now = std::time::Instant::now();
    let monotonic_now = crate::observe::monotonic_ms();
    let oldest_since_ms = registry
        .values()
        .filter_map(|s| s.oldest)
        .map(|since| {
            monotonic_now
                .saturating_sub(now.saturating_duration_since(since).as_millis() as u64)
                .max(1)
        })
        .min()
        .unwrap_or(0);
    crate::observe::gauges().ae_dirty_buckets.set(count);
    crate::observe::gauges()
        .ae_dirty_oldest_since_ms
        .set(oldest_since_ms);
}

pub(super) struct VersionIndex {
    pub(super) leaves: std::cell::RefCell<LeafGroups>,
    pub(super) buckets: std::cell::RefCell<Box<[BucketMeta]>>,
    pub(super) dirty: std::cell::RefCell<std::collections::HashMap<usize, std::time::Instant>>,
    pub(super) live: std::cell::Cell<u64>,
    pub(super) seeded: std::cell::Cell<bool>,
    pub(super) seeding: std::cell::Cell<bool>,
    pub(super) journal: std::cell::RefCell<Vec<Option<LeafOp>>>,
    pub(super) journal_key_bytes: std::cell::Cell<u64>,
    pub(super) journal_gauged: std::cell::Cell<u64>,
    pub(super) journal_charged: std::cell::Cell<u64>,
    pub(super) journal_reserved: std::cell::Cell<u64>,
    pub(super) journal_spill: std::cell::Cell<bool>,
    pub(super) gauge_id: u64,
}

impl VersionIndex {
    pub(super) fn new() -> Self {
        static NEXT_GAUGE_ID: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(1);
        let gauge_id = NEXT_GAUGE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        publish_dirty_gauges(
            gauge_id,
            Some(DirtyGaugeState {
                count: 0,
                oldest: None,
            }),
        );
        VersionIndex {
            leaves: std::cell::RefCell::new(std::collections::HashMap::new()),
            buckets: std::cell::RefCell::new(
                vec![BucketMeta::clean(); INDEX_BUCKETS as usize].into_boxed_slice(),
            ),
            dirty: std::cell::RefCell::new(std::collections::HashMap::new()),
            live: std::cell::Cell::new(0),
            seeded: std::cell::Cell::new(false),
            seeding: std::cell::Cell::new(false),
            journal: std::cell::RefCell::new(Vec::new()),
            journal_key_bytes: std::cell::Cell::new(0),
            journal_gauged: std::cell::Cell::new(0),
            journal_charged: std::cell::Cell::new(0),
            journal_reserved: std::cell::Cell::new(0),
            journal_spill: std::cell::Cell::new(false),
            gauge_id,
        }
    }

    pub(super) fn refresh_dirty_gauges(&self) {
        let dirty = self.dirty.borrow();
        let count = dirty.len() as u64;
        let oldest = dirty.values().copied().min();
        publish_dirty_gauges(self.gauge_id, Some(DirtyGaugeState { count, oldest }));
    }

    pub(super) fn reset_bucket_meta(&self) {
        self.buckets.borrow_mut().fill(BucketMeta::clean());
        self.dirty.borrow_mut().clear();
        self.refresh_dirty_gauges();
    }

    pub(super) fn dirty_bucket(&self, slot: usize) {
        let mut buckets = self.buckets.borrow_mut();
        buckets[slot].generation = buckets[slot].generation.wrapping_add(1);
        drop(buckets);
        let first_dirty = {
            let mut dirty = self.dirty.borrow_mut();
            if let std::collections::hash_map::Entry::Vacant(entry) = dirty.entry(slot) {
                entry.insert(std::time::Instant::now());
                true
            } else {
                false
            }
        };
        if first_dirty {
            self.refresh_dirty_gauges();
        }
    }

    pub(super) fn slot(key: &[u8]) -> usize {
        crate::antientropy::bucket_of(key, INDEX_BUCKETS) as usize
    }

    pub(super) fn empty_group() -> Box<[u64]> {
        vec![0u64; INDEX_BUCKETS as usize].into_boxed_slice()
    }

    pub(super) fn journal_bytes(&self) -> u64 {
        self.journal.borrow().capacity() as u64 * std::mem::size_of::<Option<LeafOp>>() as u64
            + self.journal_key_bytes.get()
    }

    pub(super) fn sync_journal_bytes(&self, force: bool) {
        let bytes = self.journal_bytes();
        let gauged = self.journal_gauged.get();
        if !force && bytes.abs_diff(gauged) < JOURNAL_CHARGE_STEP {
            return;
        }
        self.journal_gauged.set(bytes);
        crate::observe::gauges().ae_seed_journal_bytes.adjust(gauged, bytes);
        let charge = if super::seed_policy::isolation() { bytes } else { 0 };
        let charged = self.journal_charged.replace(charge);
        if charged != charge {
            crate::ram_broker::charge(charged, charge);
        }
    }

    fn reserve_journal(&self, key_len: usize, cap: Option<u64>) -> bool {
        let slot = std::mem::size_of::<Option<LeafOp>>() as u64;
        let keys = self.journal_key_bytes.get() + key_len as u64;
        let mut journal = self.journal.borrow_mut();
        let len = journal.len() as u64;
        let capacity = journal.capacity() as u64;
        let reserved = self.journal_reserved.get();
        let (min_slots, want_slots) = if len == capacity {
            (len + 1, len.saturating_mul(2).max(4))
        } else {
            (capacity, capacity)
        };
        let need = |slots: u64| (slots * slot + keys).saturating_sub(reserved);
        let (min, want) = (need(min_slots), need(want_slots) + JOURNAL_RESERVE_CHUNK);
        let granted = if min == 0 {
            0
        } else {
            match reserve_journal_node(min, want, cap) {
                Some(granted) => granted,
                None => return false,
            }
        };
        let reserved = reserved + granted;
        self.journal_reserved.set(reserved);
        if len == capacity {
            let slots = ((reserved - keys) / slot).min(want_slots);
            journal.reserve_exact((slots - len) as usize);
        }
        true
    }

    fn push_journal(&self, op: LeafOp, key_len: usize) -> bool {
        if let Some(cap) = super::seed_policy::journal_cap() {
            if !self.reserve_journal(key_len, Some(cap)) {
                self.journal_spill.set(true);
                crate::observe::counters().ae_seed_journal_spills.inc();
                return false;
            }
        }
        self.journal.borrow_mut().push(Some(op));
        self.journal_key_bytes.set(self.journal_key_bytes.get() + key_len as u64);
        self.sync_journal_bytes(false);
        true
    }

    pub(super) fn push_journal_forced(&self, op: LeafOp, key_len: usize) {
        if super::seed_policy::journal_cap().is_some() {
            self.reserve_journal(key_len, None);
        }
        self.journal.borrow_mut().push(Some(op));
        self.journal_key_bytes.set(self.journal_key_bytes.get() + key_len as u64);
        self.sync_journal_bytes(false);
    }

    pub(super) fn take_journal(&self, i: usize) -> LeafOp {
        let op = self.journal.borrow_mut()[i].take().unwrap();
        let key_len = match &op {
            LeafOp::Apply { key, .. } | LeafOp::Retire { key, .. } => key.len() as u64,
        };
        self.journal_key_bytes.set(self.journal_key_bytes.get().saturating_sub(key_len));
        op
    }

    pub(super) fn clear_journal(&self) {
        *self.journal.borrow_mut() = Vec::new();
        self.journal_key_bytes.set(0);
        self.journal_spill.set(false);
        release_journal_node(self.journal_reserved.replace(0));
        self.sync_journal_bytes(true);
    }

    pub(super) fn apply_leaf(&self, key: &[u8], group: &[NodeId], old: OldWinner, new: Version) -> bool {
        if !self.seeded.get() {
            if self.seeding.get() {
                if self.journal_spill.get() {
                    return true;
                }
                let op = LeafOp::Apply { key: Box::from(key), old, new };
                if !self.push_journal(op, key.len()) {
                    return true;
                }
            }
            return false;
        }
        let b = Self::slot(key);
        if matches!(old, OldWinner::Unknown) {
            self.dirty_bucket(b);
            return false;
        }
        let mut buckets = self.buckets.borrow_mut();
        let bucket = &mut buckets[b];
        bucket.generation = bucket.generation.wrapping_add(1);
        if self.dirty.borrow().contains_key(&b) {
            return false;
        }
        let OldWinner::Exact(old) = old else { unreachable!() };
        let mut groups = self.leaves.borrow_mut();
        let leaves = groups
            .entry(Box::from(group))
            .or_insert_with(Self::empty_group);
        if let Some(old) = old {
            leaves[b] ^= crate::antientropy::entry_digest(key, old);
        } else {
            bucket.logical_keys += 1;
            self.live.set(self.live.get() + 1);
        }
        leaves[b] ^= crate::antientropy::entry_digest(key, new);
        false
    }

    pub(super) fn retire_leaf(&self, key: &[u8], group: &[NodeId], version: Version) -> bool {
        if !self.seeded.get() {
            if self.seeding.get() {
                if self.journal_spill.get() {
                    return true;
                }
                let op = LeafOp::Retire { key: Box::from(key), version };
                if !self.push_journal(op, key.len()) {
                    return true;
                }
            }
            return false;
        }
        let b = Self::slot(key);
        let mut buckets = self.buckets.borrow_mut();
        let bucket = &mut buckets[b];
        bucket.generation = bucket.generation.wrapping_add(1);
        if self.dirty.borrow().contains_key(&b) {
            return false;
        }
        let mut groups = self.leaves.borrow_mut();
        let leaves = groups
            .entry(Box::from(group))
            .or_insert_with(Self::empty_group);
        leaves[b] ^= crate::antientropy::entry_digest(key, version);
        bucket.logical_keys = bucket.logical_keys.saturating_sub(1);
        debug_assert!(
            self.live.get() > 0,
            "retire_leaf underflow: no resident keys to retire"
        );
        self.live.set(self.live.get().saturating_sub(1));
        false
    }

    pub(super) fn seed_pass(
        &self,
        versions: RecoveredVersions,
        group_of: impl Fn(&[u8]) -> Vec<NodeId>,
    ) -> u64 {
        let mut groups = self.leaves.borrow_mut();
        groups.clear();
        let mut buckets = vec![BucketMeta::clean(); INDEX_BUCKETS as usize].into_boxed_slice();
        let mut live = 0u64;
        let mut high_ms = 0u64;
        for (handle, raw) in versions {
            let version = unpack_version(raw);
            let b = Self::slot(&handle);
            let g = group_of(&handle);
            let leaves = groups
                .entry(g.into_boxed_slice())
                .or_insert_with(Self::empty_group);
            leaves[b] ^= crate::antientropy::entry_digest(&handle, version);
            buckets[b].logical_keys += 1;
            live += 1;
            high_ms = high_ms.max(version.physical_ms());
        }
        *self.buckets.borrow_mut() = buckets;
        self.dirty.borrow_mut().clear();
        self.live.set(live);
        self.seeded.set(true);
        drop(groups);
        self.refresh_dirty_gauges();
        high_ms
    }

    pub(super) fn clear(&self) {
        self.leaves.borrow_mut().clear();
        self.reset_bucket_meta();
        self.live.set(0);
        self.seeded.set(true);
        self.seeding.set(false);
        self.clear_journal();
    }

    pub(super) fn invalidate(&self) {
        self.leaves.borrow_mut().clear();
        self.reset_bucket_meta();
        self.seeded.set(false);
        self.seeding.set(false);
        self.clear_journal();
    }

    pub(super) fn has_dirty(&self) -> bool {
        self.refresh_dirty_gauges();
        !self.dirty.borrow().is_empty()
    }

    pub(super) fn leaf_digests(&self, num_buckets: u32) -> Result<Vec<u64>, LeafDigestError> {
        debug_assert!(self.seeded.get(), "leaf_digests on an unseeded index");
        if self.has_dirty() {
            return Err(LeafDigestError::Dirty);
        }
        let groups = self.leaves.borrow();
        if num_buckets == INDEX_BUCKETS {
            let mut out = vec![0u64; INDEX_BUCKETS as usize];
            for leaves in groups.values() {
                for (i, &leaf) in leaves.iter().enumerate() {
                    out[i] ^= leaf;
                }
            }
            Ok(out)
        } else if num_buckets != 0 && INDEX_BUCKETS.is_multiple_of(num_buckets) {
            let mut out = vec![0u64; num_buckets as usize];
            for leaves in groups.values() {
                for (i, &leaf) in leaves.iter().enumerate() {
                    out[i % num_buckets as usize] ^= leaf;
                }
            }
            Ok(out)
        } else {
            panic!(
                "leaf_digests: num_buckets {num_buckets} does not divide INDEX_BUCKETS \
                 {INDEX_BUCKETS}; only divisor granularities are supported now the \
                 per-key version map is out-of-core"
            )
        }
    }

    pub(super) fn scoped_leaf_digests(
        &self,
        level: u32,
        scope_g: u32,
        scope: &std::collections::HashSet<u32>,
        keep_group: impl Fn(&[NodeId]) -> bool,
    ) -> Result<Vec<(u32, u64)>, LeafDigestError> {
        debug_assert!(
            self.seeded.get(),
            "scoped_leaf_digests on an unseeded index"
        );
        debug_assert!(level != 0 && INDEX_BUCKETS.is_multiple_of(level));
        debug_assert!(scope_g != 0 && level.is_multiple_of(scope_g));
        self.refresh_dirty_gauges();
        if self
            .dirty
            .borrow()
            .keys()
            .any(|&p| scope.contains(&(p as u32 % scope_g)))
        {
            return Err(LeafDigestError::Dirty);
        }
        let mut acc = vec![0u64; level as usize];
        for (group, leaves) in self.leaves.borrow().iter() {
            if !keep_group(group) {
                continue;
            }
            for (p, &leaf) in leaves.iter().enumerate() {
                if scope.contains(&(p as u32 % scope_g)) {
                    acc[p % level as usize] ^= leaf;
                }
            }
        }
        Ok((0..level)
            .filter(|f| scope.contains(&(f % scope_g)))
            .map(|f| (f, acc[f as usize]))
            .collect())
    }
}

impl Drop for VersionIndex {
    fn drop(&mut self) {
        publish_dirty_gauges(self.gauge_id, None);
        self.clear_journal();
    }
}

