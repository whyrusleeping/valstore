use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use crate::run::{prefetch_read, splitmix64, BloomKey};

const BLOCK_BITS: u64 = 512;
const BLOCK_WORDS: usize = 8;
const PAGE_BYTES: u64 = 4096;
const POSITION_BITS: u64 = 9;
const POSITIONS_PER_WORD: u64 = 64 / POSITION_BITS;
const SINK_FLUSH_EVERY: u64 = 1024;

pub const DEFAULT_MIN_KEYS: u64 = 16 << 20;
pub const DEFAULT_BITS_PER_KEY: u64 = 12;
pub const DEFAULT_MAX_BITS_PER_KEY: u64 = 12;
pub const MIN_BITS_PER_KEY: u64 = 4;
pub const MAX_BITS_PER_KEY: u64 = 18;
pub const DEFAULT_GROWTH_PCT: u64 = 200;

static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);
const REBUILD_WAITER_STALE: Duration = Duration::from_secs(60);
static REBUILD_REGISTRY: OnceLock<Mutex<RebuildRegistry>> = OnceLock::new();

struct RebuildWaiter {
    priority: u64,
    urgent: bool,
    started: Instant,
    refreshed: Instant,
}

#[derive(Default)]
struct RebuildRegistry {
    waiters: HashMap<u64, RebuildWaiter>,
    in_flight: u64,
    completed_wait: Duration,
    urgent_grants: u64,
}

impl RebuildRegistry {
    fn refresh(&mut self, id: u64, priority: u64, urgent_fpr_ppm: u64, now: Instant) {
        let urgent = urgent_fpr_ppm > 0 && priority >= urgent_fpr_ppm;
        let waiter = self.waiters.entry(id).or_insert(RebuildWaiter {
            priority, urgent, started: now, refreshed: now,
        });
        waiter.priority = priority;
        waiter.urgent = urgent;
        waiter.refreshed = now;
    }

    fn remove(&mut self, id: u64, now: Instant) {
        if let Some(waiter) = self.waiters.remove(&id) {
            self.completed_wait += now.duration_since(waiter.started);
        }
    }

    fn grant(&mut self, id: u64, limit: u64, extra: u64, now: Instant, stale: Duration) -> bool {
        let urgent_only = self.in_flight >= limit;
        if self.in_flight >= limit.saturating_add(extra) {
            return false;
        }
        let top = self.waiters.iter()
            .filter(|(_, w)| now.duration_since(w.refreshed) < stale && (!urgent_only || w.urgent))
            .max_by(|(aid, a), (bid, b)| a.priority.cmp(&b.priority)
                .then_with(|| b.started.cmp(&a.started)).then_with(|| bid.cmp(aid)))
            .map(|(&id, _)| id);
        if top != Some(id) {
            return false;
        }
        self.remove(id, now);
        if urgent_only {
            self.urgent_grants += 1;
        }
        self.in_flight += 1;
        true
    }

    fn total_wait(&self, now: Instant) -> Duration {
        self.completed_wait + self.waiters.values().map(|w| now.duration_since(w.started)).sum::<Duration>()
    }
}

fn rebuild_registry() -> &'static Mutex<RebuildRegistry> {
    REBUILD_REGISTRY.get_or_init(|| Mutex::new(RebuildRegistry::default()))
}

pub fn rebuild_wait_seconds_total() -> u64 {
    rebuild_registry().lock().unwrap_or_else(|e| e.into_inner()).total_wait(Instant::now()).as_secs()
}

pub fn rebuild_urgent_grants() -> u64 {
    rebuild_registry().lock().unwrap_or_else(|e| e.into_inner()).urgent_grants
}

struct RebuildPermit;

impl Drop for RebuildPermit {
    fn drop(&mut self) {
        rebuild_registry().lock().unwrap_or_else(|e| e.into_inner()).in_flight -= 1;
    }
}

struct Rebuild {
    filter: Box<KeyFilter>,
    started: Instant,
    _permit: RebuildPermit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyFilterParams {
    pub enabled: bool,
    pub min_keys: u64,
    pub bits_per_key: u64,
    pub max_bits_per_key: u64,
    pub growth_pct: u64,
    pub rebuild: bool,
    pub step_pct: u64,
    pub rebuild_subs: u64,
    pub rebuild_concurrency: u64,
    pub rebuild_urgent_fpr_ppm: u64,
    pub rebuild_urgent_extra: u64,
}

impl KeyFilterParams {
    pub fn from_env() -> Self {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let num = |name: &str| lookup(name).and_then(|s| s.trim().parse::<u64>().ok());
        let bits_per_key = num("VALS_KEY_FILTER_BITS_PER_KEY")
            .map(|b| b.clamp(MIN_BITS_PER_KEY, MAX_BITS_PER_KEY))
            .unwrap_or(DEFAULT_BITS_PER_KEY);
        let max_bits_per_key = num("VALS_KEY_FILTER_MAX_BITS_PER_KEY")
            .unwrap_or(DEFAULT_MAX_BITS_PER_KEY)
            .clamp(bits_per_key, MAX_BITS_PER_KEY);
        let rebuild_subs = num("VALS_KEY_FILTER_REBUILD_SUBS").unwrap_or(4).clamp(2, 16);
        let urgent_fpr_ppm = num("VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM").unwrap_or(25_000);
        KeyFilterParams {
            enabled: lookup("VALS_KEY_FILTER").is_none_or(|v| v.trim() != "0"),
            min_keys: num("VALS_KEY_FILTER_MIN_KEYS").filter(|&n| n > 0).unwrap_or(DEFAULT_MIN_KEYS),
            bits_per_key,
            max_bits_per_key,
            growth_pct: num("VALS_KEY_FILTER_GROWTH_PCT")
                .map(|g| g.clamp(110, 400))
                .unwrap_or(DEFAULT_GROWTH_PCT),
            rebuild: lookup("VALS_KEY_FILTER_REBUILD").is_none_or(|v| v.trim() != "0"),
            step_pct: num("VALS_KEY_FILTER_STEP_PCT").unwrap_or(20).clamp(5, 100),
            rebuild_subs,
            rebuild_concurrency: num("VALS_KEY_FILTER_REBUILD_CONCURRENCY").unwrap_or(1).clamp(1, 64),
            rebuild_urgent_fpr_ppm: if urgent_fpr_ppm == 0 { 0 } else { urgent_fpr_ppm.clamp(1_000, 500_000) },
            rebuild_urgent_extra: num("VALS_KEY_FILTER_REBUILD_URGENT_EXTRA").unwrap_or(1).min(8),
        }
    }
}

fn zeroed_words(n: usize) -> Box<[AtomicU64]> {
    let n = n.max(1);
    let layout = std::alloc::Layout::array::<AtomicU64>(n).expect("key filter size overflow");
    // SAFETY: the layout is non-zero-sized; an all-zero bit pattern is a valid
    // AtomicU64; the allocation is exactly `Layout::array::<AtomicU64>(n)`, which
    // is the layout `Box<[AtomicU64]>` deallocates with.
    unsafe {
        let ptr = std::alloc::alloc_zeroed(layout) as *mut AtomicU64;
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }
        Box::from_raw(std::ptr::slice_from_raw_parts_mut(ptr, n))
    }
}

const HUGE_PAGE_BYTES: usize = 2 << 20;

struct MappedWords {
    ptr: std::ptr::NonNull<AtomicU64>,
    len: usize,
    map_bytes: usize,
    huge_bytes: usize,
}

// SAFETY: MappedWords exclusively owns its private anonymous mapping and only
// exposes it as `[AtomicU64]`, which is Send and Sync.
unsafe impl Send for MappedWords {}
// SAFETY: as above; shared access goes through atomics only.
unsafe impl Sync for MappedWords {}

impl Drop for MappedWords {
    fn drop(&mut self) {
        // SAFETY: ptr/map_bytes are exactly the window this value mapped and
        // still owns; no references outlive self.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.map_bytes);
        }
    }
}

#[cfg(target_os = "linux")]
fn mapped_words(n: usize) -> Option<MappedWords> {
    let bytes = n.checked_mul(std::mem::size_of::<AtomicU64>())?;
    if bytes < HUGE_PAGE_BYTES {
        return None;
    }
    let map_bytes = bytes.checked_next_multiple_of(PAGE_BYTES as usize)?;
    let reserve = map_bytes.checked_add(HUGE_PAGE_BYTES)?;
    // SAFETY: a fresh private anonymous mapping (zero-filled on first touch,
    // so all-zero AtomicU64s); the unused head and tail around the 2 MiB
    // aligned window are unmapped, and the window is only advised, never
    // remapped. MappedWords owns the window and unmaps it on drop.
    unsafe {
        let raw = libc::mmap(std::ptr::null_mut(), reserve, libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1, 0);
        if raw == libc::MAP_FAILED {
            return None;
        }
        let base = raw as usize;
        let start = base.next_multiple_of(HUGE_PAGE_BYTES);
        if start > base {
            libc::munmap(raw, start - base);
        }
        let tail = base + reserve - (start + map_bytes);
        if tail > 0 {
            libc::munmap((start + map_bytes) as *mut libc::c_void, tail);
        }
        let huge = bytes / HUGE_PAGE_BYTES * HUGE_PAGE_BYTES;
        let advised = libc::madvise(start as *mut libc::c_void, huge, libc::MADV_HUGEPAGE) == 0;
        Some(MappedWords {
            ptr: std::ptr::NonNull::new(start as *mut AtomicU64)?,
            len: n,
            map_bytes,
            huge_bytes: if advised { huge } else { 0 },
        })
    }
}

#[cfg(not(target_os = "linux"))]
fn mapped_words(_n: usize) -> Option<MappedWords> {
    None
}

enum Words {
    Heap(Box<[AtomicU64]>),
    Mapped(MappedWords),
}

impl Words {
    fn new(n: usize) -> Words {
        mapped_words(n).map_or_else(|| Words::Heap(zeroed_words(n)), Words::Mapped)
    }

    fn huge_bytes(&self) -> usize {
        match self {
            Words::Heap(_) => 0,
            Words::Mapped(m) => m.huge_bytes,
        }
    }
}

impl std::ops::Deref for Words {
    type Target = [AtomicU64];

    #[inline]
    fn deref(&self) -> &[AtomicU64] {
        match self {
            Words::Heap(b) => b,
            // SAFETY: the mapping holds `len` zero-initialized AtomicU64s and
            // lives as long as self.
            Words::Mapped(m) => unsafe { std::slice::from_raw_parts(m.ptr.as_ptr(), m.len) },
        }
    }
}

fn touched_bytes(region: u64, page: u64, keys: f64) -> u64 {
    if region == 0 {
        return 0;
    }
    let pages = region.div_ceil(page).max(1);
    let touched = pages as f64 * (1.0 - (-keys / pages as f64).exp());
    ((touched.ceil() as u64).saturating_mul(page)).min(region)
}

pub struct SubFilter {
    words: Words,
    blocks: u64,
    k: u32,
    seed: u64,
    capacity: u64,
    bits_per_key: u64,
    inserted: AtomicU64,
    reserved: AtomicU64,
}

impl SubFilter {
    pub fn new(capacity: u64, bits_per_key: u64, seed: u64) -> SubFilter {
        let capacity = capacity.max(1);
        let blocks = capacity.saturating_mul(bits_per_key).div_ceil(BLOCK_BITS).max(1);
        SubFilter {
            words: Words::new(blocks as usize * BLOCK_WORDS),
            blocks,
            k: ((bits_per_key * 3 + 2) / 5).clamp(4, 2 * POSITIONS_PER_WORD) as u32,
            seed,
            capacity,
            bits_per_key,
            inserted: AtomicU64::new(0),
            reserved: AtomicU64::new(0),
        }
    }

    #[inline]
    fn locate(&self, hk: &BloomKey) -> (usize, u64, u64) {
        let g = splitmix64(hk.z() ^ self.seed);
        let block = ((g as u128 * self.blocks as u128) >> 64) as usize;
        let h1 = splitmix64(g);
        (block * BLOCK_WORDS, h1, splitmix64(h1))
    }

    #[inline]
    fn position(h1: u64, h2: u64, i: u64) -> u64 {
        let src = if i < POSITIONS_PER_WORD { h1 >> (POSITION_BITS * i) } else { h2 >> (POSITION_BITS * (i - POSITIONS_PER_WORD)) };
        src & (BLOCK_BITS - 1)
    }

    #[inline]
    pub fn prefetch(&self, hk: &BloomKey) {
        let (base, _, _) = self.locate(hk);
        prefetch_read(self.words.as_ptr().wrapping_add(base));
    }

    #[inline]
    pub fn contains(&self, hk: &BloomKey) -> bool {
        let (base, h1, h2) = self.locate(hk);
        (0..self.k as u64).all(|i| {
            let pos = Self::position(h1, h2, i);
            self.words[base + (pos / 64) as usize].load(Ordering::Relaxed) & (1u64 << (pos % 64)) != 0
        })
    }

    pub fn insert(&self, hk: &BloomKey) -> bool {
        let (base, h1, h2) = self.locate(hk);
        let mut fresh = false;
        for i in 0..self.k as u64 {
            let pos = Self::position(h1, h2, i);
            let bit = 1u64 << (pos % 64);
            let word = &self.words[base + (pos / 64) as usize];
            if word.load(Ordering::Relaxed) & bit == 0 && word.fetch_or(bit, Ordering::Relaxed) & bit == 0 {
                fresh = true;
            }
        }
        fresh
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn inserted(&self) -> u64 {
        self.inserted.load(Ordering::Relaxed)
    }

    pub fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Relaxed)
    }

    pub fn bits_per_key(&self) -> u64 {
        self.bits_per_key
    }

    pub fn bytes(&self) -> u64 {
        (self.words.len() * std::mem::size_of::<AtomicU64>()) as u64
    }

    pub fn resident_bytes(&self) -> u64 {
        let bytes = self.bytes();
        let huge = (self.words.huge_bytes() as u64).min(bytes);
        let keys = self.inserted() as f64;
        let huge_keys = keys * huge as f64 / bytes.max(1) as f64;
        touched_bytes(huge, HUGE_PAGE_BYTES as u64, huge_keys)
            + touched_bytes(bytes - huge, PAGE_BYTES, keys - huge_keys)
    }

    pub fn estimated_fpr(&self) -> f64 {
        let k = self.k as f64;
        let lambda = self.inserted() as f64 / self.blocks as f64;
        let block_fpr = |keys: f64| (1.0 - (1.0 - 1.0 / BLOCK_BITS as f64).powf(k * keys)).powf(k);
        if lambda > 500.0 {
            return block_fpr(lambda);
        }
        let mut p = (-lambda).exp();
        let mut fpr = p * block_fpr(0.0);
        let last = (lambda + 10.0 * lambda.sqrt() + 20.0) as u64;
        for j in 1..=last {
            p *= lambda / j as f64;
            fpr += p * block_fpr(j as f64);
        }
        fpr.min(1.0)
    }
}

pub struct KeyFilterSink {
    sub: Arc<SubFilter>,
    generation: u64,
    pending: u64,
    reserved: u64,
    secondary: Option<Box<KeyFilterSink>>,
    #[cfg(test)]
    pub(crate) knockout: bool,
}

impl KeyFilterSink {
    #[inline]
    pub fn insert(&mut self, key: &[u8]) {
        self.insert_hashed(&BloomKey::new(key));
    }

    #[inline]
    pub fn insert_hashed(&mut self, hk: &BloomKey) {
        if let Some(s) = &mut self.secondary {
            s.insert_hashed(hk);
        }
        #[cfg(test)]
        if self.knockout {
            return;
        }
        if self.sub.insert(hk) {
            self.pending += 1;
            if self.pending >= SINK_FLUSH_EVERY {
                self.flush_count();
            }
        }
    }

    fn flush_count(&mut self) {
        if self.pending > 0 {
            self.sub.inserted.fetch_add(self.pending, Ordering::Relaxed);
            let release = self.pending.min(self.reserved);
            if release > 0 {
                self.sub.reserved.fetch_sub(release, Ordering::Relaxed);
                self.reserved -= release;
            }
            self.pending = 0;
        }
    }

    fn has_generation(&self, generation: u64) -> bool {
        self.generation == generation || self.secondary.as_ref().is_some_and(|s| s.has_generation(generation))
    }

    #[cfg(test)]
    pub(crate) fn knock_out_generation(&mut self, generation: u64) {
        if self.generation == generation {
            self.knockout = true;
        }
        if let Some(s) = &mut self.secondary {
            s.knock_out_generation(generation);
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn sub_filter(&self) -> &Arc<SubFilter> {
        &self.sub
    }
}

impl Drop for KeyFilterSink {
    fn drop(&mut self) {
        self.flush_count();
        if self.reserved > 0 {
            self.sub.reserved.fetch_sub(self.reserved, Ordering::Relaxed);
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub struct KeyFilterStats {
    pub probes: u64,
    pub negatives: u64,
    pub run_probes: u64,
    pub sub_filters: u64,
    pub filter_bytes: u64,
    pub resident_bytes: u64,
    pub inserted_keys: u64,
    pub capacity_keys: u64,
    pub covered_runs: u64,
    pub uncovered_runs: u64,
    pub uncovered_run_bytes: u64,
    pub estimated_fpr: f64,
    pub resets: u64,
    pub cover_scans_committed: u64,
    pub cover_scans_discarded: u64,
    pub cover_scan_failures: u64,
    pub cover_scan_mismatches: u64,
    pub cover_scan_backoff_runs: u64,
    pub retired_sub_filters: u64,
    pub retired_bytes: u64,
    pub retired_resident_bytes: u64,
    pub rebuild_bytes: u64,
    pub rebuild_resident_bytes: u64,
    pub rebuilding: u64,
    pub rebuild_covered_runs: u64,
    pub rebuilds_started: u64,
    pub rebuilds_completed: u64,
    pub rebuilds_abandoned: u64,
    pub rebuild_peak_bytes: u64,
    pub rebuild_wait_seconds: u64,
    pub rebuild_open_seconds: u64,
}


pub struct KeyFilter {
    params: KeyFilterParams,
    generation: u64,
    waiter_id: u64,
    wait_started: Option<Instant>,
    subs: Vec<Arc<SubFilter>>,
    retired: Vec<Arc<SubFilter>>,
    rebuild: Option<Rebuild>,
    rebuilds_started: u64,
    rebuilds_completed: u64,
    rebuilds_abandoned: u64,
    rebuild_peak_bytes: Cell<u64>,
    pub(crate) probes: Cell<u64>,
    pub(crate) negatives: Cell<u64>,
    pub(crate) run_probes: Cell<u64>,
    pub(crate) resets: u64,
    pub(crate) cover_scans_committed: u64,
    pub(crate) cover_scans_discarded: u64,
    pub(crate) cover_scan_failures: u64,
    pub(crate) cover_scan_mismatches: u64,
}

#[cfg(test)]
#[path = "keyfilter_tests.rs"]
mod tests;

impl Drop for KeyFilter {
    fn drop(&mut self) {
        self.cancel_rebuild_wait();
    }
}

impl KeyFilter {
    pub fn new(params: KeyFilterParams) -> KeyFilter {
        KeyFilter {
            params,
            generation: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            waiter_id: NEXT_GENERATION.fetch_add(1, Ordering::Relaxed),
            wait_started: None,
            subs: Vec::new(),
            retired: Vec::new(),
            rebuild: None,
            rebuilds_started: 0,
            rebuilds_completed: 0,
            rebuilds_abandoned: 0,
            rebuild_peak_bytes: Cell::new(0),
            probes: Cell::new(0),
            negatives: Cell::new(0),
            run_probes: Cell::new(0),
            resets: 0,
            cover_scans_committed: 0,
            cover_scans_discarded: 0,
            cover_scan_failures: 0,
            cover_scan_mismatches: 0,
        }
    }

    pub fn params(&self) -> KeyFilterParams {
        self.params
    }

    #[cfg(test)]
    pub(crate) fn set_params(&mut self, params: KeyFilterParams) {
        self.params = params;
        self.reset();
    }

    #[cfg(test)]
    pub(crate) fn subs_for_bench(&mut self) -> &mut Vec<Arc<SubFilter>> {
        &mut self.subs
    }

    pub fn enabled(&self) -> bool {
        self.params.enabled
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn owns(&self, sink: &KeyFilterSink) -> bool {
        sink.has_generation(self.generation)
    }

    pub fn sub_filters(&self) -> &[Arc<SubFilter>] {
        &self.subs
    }

    pub(crate) fn cancel_rebuild_wait(&mut self) {
        if self.wait_started.take().is_some() {
            rebuild_registry().lock().unwrap_or_else(|e| e.into_inner()).remove(self.waiter_id, Instant::now());
        }
    }

    pub fn reset(&mut self) {
        self.cancel_rebuild_wait();
        if let Some(mut r) = self.rebuild.take() {
            self.retired.extend(std::mem::take(&mut r.filter.subs));
            self.rebuilds_abandoned += 1;
        }
        self.generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
        let subs = std::mem::take(&mut self.subs);
        self.retired.extend(subs);
        self.prune_retired();
    }

    pub(crate) fn prune_retired(&mut self) {
        self.retired.retain(|s| Arc::strong_count(s) > 1);
    }

    pub fn rebuilding(&self) -> bool {
        self.rebuild.is_some()
    }

    pub fn owns_rebuild(&self, sink: &KeyFilterSink) -> bool {
        self.rebuild.as_ref().is_some_and(|r| r.filter.owns(sink))
    }

    #[cfg(test)]
    pub(crate) fn rebuild_filter(&self) -> Option<&KeyFilter> {
        self.rebuild.as_ref().map(|r| &*r.filter)
    }

    pub(crate) fn start_rebuild(&mut self, estimate: u64) -> bool {
        if self.rebuilding() || !self.params.rebuild {
            return false;
        }
        let priority = (self.estimated_fpr() * 1_000_000.0).ceil() as u64;
        let mut registry = rebuild_registry().lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        registry.refresh(self.waiter_id, priority, self.params.rebuild_urgent_fpr_ppm, now);
        self.wait_started.get_or_insert(now);
        if !registry.grant(self.waiter_id, self.params.rebuild_concurrency,
            self.params.rebuild_urgent_extra, now, REBUILD_WAITER_STALE)
        {
            return false;
        }
        self.wait_started = None;
        drop(registry);
        let permit = RebuildPermit;
        let mut filter = KeyFilter::new(self.params);
        let capacity = self.params.min_keys.max(estimate.saturating_add(estimate / 10));
        filter.subs.push(Arc::new(SubFilter::new(capacity, self.params.bits_per_key, splitmix64(filter.generation))));
        self.rebuild = Some(Rebuild { filter: Box::new(filter), started: now, _permit: permit });
        self.rebuilds_started += 1;
        self.note_rebuild_peak();
        true
    }

    pub(crate) fn complete_rebuild(&mut self) {
        self.note_rebuild_peak();
        if let Some(mut r) = self.rebuild.take() {
            self.retired.extend(std::mem::replace(&mut self.subs, std::mem::take(&mut r.filter.subs)));
            self.generation = r.filter.generation;
            self.rebuilds_completed += 1;
            self.prune_retired();
        }
    }

    pub(crate) fn note_rebuild_peak(&self) {
        if self.rebuilding() {
            self.rebuild_peak_bytes.set(self.rebuild_peak_bytes.get().max(self.filter_bytes()));
        }
    }

    pub fn sink(&mut self, expected_keys: u64, engine_keys: u64) -> Option<KeyFilterSink> {
        if !self.rebuilding() {
            self.sink_single(expected_keys, engine_keys)
        } else {
            self.sink_lanes(expected_keys, expected_keys, engine_keys)
        }
    }

    pub(crate) fn sink_lanes(&mut self, active_keys: u64, rebuild_keys: u64, engine_keys: u64) -> Option<KeyFilterSink> {
        let active = if active_keys > 0 { self.sink_single(active_keys, engine_keys) } else { None };
        let rebuild = self.rebuild.as_mut().and_then(|r| {
            if rebuild_keys > 0 { r.filter.sink_single(rebuild_keys, 0) } else { None }
        });
        let sink = match (active, rebuild) {
            (Some(mut a), Some(r)) => {
                a.secondary = Some(Box::new(r));
                Some(a)
            }
            (a, r) => a.or(r),
        };
        self.note_rebuild_peak();
        sink
    }

    fn sink_single(&mut self, expected_keys: u64, engine_keys: u64) -> Option<KeyFilterSink> {
        if !self.params.enabled {
            return None;
        }
        self.prune_retired();
        let need_new = self.subs.last().is_none_or(|s| {
            s.inserted().saturating_add(s.reserved()).saturating_add(expected_keys) > s.capacity()
        });
        if need_new {
            let capacity = match self.subs.last() {
                None => self
                    .params
                    .min_keys
                    .max(engine_keys.saturating_add(engine_keys / 4))
                    .max(expected_keys),
                Some(_) if self.params.rebuild => (self.subs.iter().map(|s| s.capacity()).sum::<u64>()
                    .saturating_mul(self.params.step_pct) / 100).max(expected_keys),
                Some(last) => (last.capacity().saturating_mul(self.params.growth_pct) / 100)
                    .max(expected_keys),
            };
            let bits = (self.params.bits_per_key + self.subs.len() as u64).min(self.params.max_bits_per_key);
            let seed = splitmix64(self.generation ^ (self.subs.len() as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
            self.subs.push(Arc::new(SubFilter::new(capacity, bits, seed)));
        }
        let sub = self.subs.last().unwrap().clone();
        sub.reserved.fetch_add(expected_keys, Ordering::Relaxed);
        Some(KeyFilterSink {
            sub,
            generation: self.generation,
            pending: 0,
            reserved: expected_keys,
            secondary: None,
            #[cfg(test)]
            knockout: false,
        })
    }

    #[inline]
    pub fn excludes(&self, hk: &BloomKey) -> bool {
        for s in &self.subs {
            s.prefetch(hk);
        }
        !self.subs.iter().any(|s| s.contains(hk))
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        !self.excludes(&BloomKey::new(key))
    }

    pub fn inserted_keys(&self) -> u64 {
        self.subs.iter().map(|s| s.inserted()).sum()
    }

    pub fn filter_bytes(&self) -> u64 {
        self.subs.iter().chain(&self.retired).map(|s| s.bytes()).sum::<u64>()
            + self.rebuild.as_ref().map_or(0, |r| r.filter.filter_bytes())
    }

    pub fn resident_bytes(&self) -> u64 {
        self.subs.iter().chain(&self.retired).map(|s| s.resident_bytes()).sum::<u64>()
            + self.rebuild.as_ref().map_or(0, |r| r.filter.resident_bytes())
    }

    pub fn estimated_fpr(&self) -> f64 {
        1.0 - self.subs.iter().map(|s| 1.0 - s.estimated_fpr()).product::<f64>()
    }

    pub fn stale(&self, live_entries: u64) -> bool {
        let inserted = self.inserted_keys();
        inserted > self.params.min_keys && inserted > live_entries.saturating_mul(2)
    }

    pub fn stats(&self) -> KeyFilterStats {
        self.note_rebuild_peak();
        KeyFilterStats {
            rebuild_bytes: self.rebuild.as_ref().map_or(0, |r| r.filter.filter_bytes()),
            rebuild_resident_bytes: self.rebuild.as_ref().map_or(0, |r| r.filter.resident_bytes()),
            rebuilding: self.rebuilding() as u64,
            rebuilds_started: self.rebuilds_started,
            rebuilds_completed: self.rebuilds_completed,
            rebuilds_abandoned: self.rebuilds_abandoned,
            rebuild_peak_bytes: self.rebuild_peak_bytes.get(),
            rebuild_wait_seconds: self.wait_started.map_or(0, |t| t.elapsed().as_secs()),
            rebuild_open_seconds: self.rebuild.as_ref().map_or(0, |r| r.started.elapsed().as_secs()),
            probes: self.probes.get(),
            negatives: self.negatives.get(),
            run_probes: self.run_probes.get(),
            sub_filters: self.subs.len() as u64,
            filter_bytes: self.filter_bytes(),
            resident_bytes: self.resident_bytes(),
            inserted_keys: self.inserted_keys(),
            capacity_keys: self.subs.iter().map(|s| s.capacity()).sum(),
            estimated_fpr: self.estimated_fpr(),
            resets: self.resets,
            cover_scans_committed: self.cover_scans_committed,
            cover_scans_discarded: self.cover_scans_discarded,
            cover_scan_failures: self.cover_scan_failures,
            cover_scan_mismatches: self.cover_scan_mismatches,
            retired_sub_filters: self.retired.len() as u64,
            retired_bytes: self.retired.iter().map(|s| s.bytes()).sum(),
            retired_resident_bytes: self.retired.iter().map(|s| s.resident_bytes()).sum(),
            ..KeyFilterStats::default()
        }
    }
}
