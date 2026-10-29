use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
#[repr(align(64))]
pub struct Counter(AtomicU64);

impl Counter {
    pub const fn new() -> Self { Self(AtomicU64::new(0)) }
    pub fn add(&self, n: u64) { self.0.fetch_add(n, Ordering::Relaxed); }
    pub fn get(&self) -> u64 { self.0.load(Ordering::Relaxed) }
}

macro_rules! counters {
    ($($field:ident => ($name:literal, $help:literal)),* $(,)?) => {
        $(pub static $field: Counter = Counter::new();)*
        pub fn counters() -> Vec<(&'static str, &'static str, u64)> {
            vec![$(($name, $help, $field.get())),*]
        }
    };
}

counters! {
    SCAN_CURSORS => ("scan_run_cursors_total", "Run cursors opened by server staged scans, including restarted attempts; excludes synchronous fallback scans and maintenance scans."),
    SCAN_BLOCKS => ("scan_run_blocks_total", "Run data-block read attempts by server staged scans; a run block can span multiple device blocks. Excludes synchronous fallbacks."),
    SCAN_BYTES => ("scan_run_read_bytes_total", "LBA-aligned bytes requested by server staged scan run-block reads, including failed attempts. Excludes synchronous fallbacks."),
    SCAN_VALUES => ("scan_value_records_total", "Value-log records requested by server scan span plans, including retried pages; excludes finalize point-read patches."),
    SCAN_SPANS => ("scan_value_spans_total", "Value-log span reads submitted by server scans, including retried pages; excludes finalize point-read patches."),
    SHADOW_HITS => ("shadow_refresh_cache_hits_total", "Shadow-estimate refresh requests served by the manifest/memtable-generation and gate cache."),
    SHADOW_GATED => ("shadow_refresh_gated_total", "Shadow-estimate recomputes above the free-space gate; no run sampling."),
    SHADOW_UNGATED => ("shadow_refresh_ungated_total", "Shadow-estimate recomputes at or below the free-space gate."),
    SHADOW_FIRST => ("shadow_refresh_first_total", "Shadow-estimate recomputes with no previously computed cache."),
    SHADOW_GENERATION => ("shadow_refresh_generation_total", "Shadow-estimate recomputes caused by manifest or memtable generation change; takes precedence over a simultaneous gate flip."),
    SHADOW_GATE => ("shadow_refresh_gate_total", "Shadow-estimate recomputes caused only by a free-space gate flip."),
    SHADOW_WALL => ("shadow_refresh_wall_ns_total", "Wall nanoseconds in shadow-estimate recomputes, excluding cache hits and disabled requests."),
    SHADOW_CPU => ("shadow_refresh_cpu_ns_total", "Thread CPU nanoseconds in shadow-estimate recomputes (Linux CLOCK_THREAD_CPUTIME_ID; zero if unavailable)."),
    SHADOW_BLOCKS => ("shadow_sample_blocks_total", "Shadow-estimate data-block read attempts, including repeated samples and failed reads."),
    SHADOW_BYTES => ("shadow_sample_read_bytes_total", "LBA-aligned bytes requested by shadow-estimate sampled data-block reads, including failed reads."),
    SHADOW_RANGES => ("shadow_newer_run_range_checks_total", "Newer-run key-range checks during shadow estimation."),
    SHADOW_BLOOMS => ("shadow_newer_run_bloom_probes_total", "In-range newer-run bloom probes during shadow estimation."),
    SHADOW_FOLD_SOURCE_BYTES => ("shadow_fold_source_run_bytes_total", "Sidecar or index-block bytes of the read-only source runs a capped shadow fold streams (keys and versions only) to find shadowed window entries; planned at fold start, an upper bound on device reads."),
    SHADOW_FOLD_DROPPED => ("shadow_fold_dropped_entries_total", "Window entries a capped shadow fold dropped because a source run holds a newer version of the key."),
    SHADOW_FOLD_COLLECTED => ("shadow_fold_collected_tombstones_total", "Tombstones a capped shadow fold collected because no other run holds the key and the tombstone is below the GC cutoff."),
    DIGEST_LOOKUPS => ("digest_lookups_total", "Server Digest lookup requests, including directory-fast requests and synchronous control requests; retries within a request excluded."),
    APPLY_PROBE_WALL => ("apply_run_probe_wall_ns_total", "Wall nanoseconds resolving staged version run probes, including device waits and failed probes; concurrent probes overlap."),
    MERGE_ATTEMPTS => ("merge_work_attempts_total", "Off-core compact-and-write attempts, including cancelled and failed work; denominator for merge phase time totals."),
    MERGE_WALL => ("merge_work_wall_ns_total", "Wall nanoseconds in off-core compact-and-write attempts, including pacing, IO waits, cancellation and failure."),
    MERGE_CPU => ("merge_work_cpu_ns_total", "Thread CPU nanoseconds in off-core compact-and-write attempts (Linux; zero if unavailable), excluding sleeps and device waits."),
    MERGE_INPUT => ("merge_input_read_ns_total", "Wall nanoseconds advancing merge input cursors, including input decode and selection; not pure device latency."),
    MERGE_VERIFY => ("merge_build_verify_ns_total", "Wall nanoseconds in merge-output device readback verification, including IO waits; excludes encode attestation."),
}

static MERGE_METADATA: AtomicU64 = AtomicU64::new(0);

pub fn merge_metadata_bytes() -> u64 { MERGE_METADATA.load(Ordering::Relaxed) }

#[derive(Default)]
pub(crate) struct MergeMetadata(u64);

impl MergeMetadata {
    pub(crate) fn set(&mut self, bytes: u64) {
        if bytes >= self.0 { MERGE_METADATA.fetch_add(bytes - self.0, Ordering::Relaxed); }
        else { MERGE_METADATA.fetch_sub(self.0 - bytes, Ordering::Relaxed); }
        self.0 = bytes;
    }
}

impl Drop for MergeMetadata {
    fn drop(&mut self) { MERGE_METADATA.fetch_sub(self.0, Ordering::Relaxed); }
}

pub const WRITE_SOURCES: [&str; 7] = ["vlog_normal", "vlog_tail", "vlog_maint_rewrite", "flush_run", "merge_run", "catalog", "other"];
static WRITE_BYTES: [Counter; 7] = [const { Counter::new() }; 7];
pub fn write_bytes(source: usize, bytes: usize) { WRITE_BYTES[source].add(bytes as u64); }
pub(crate) fn normal_vlog_bytes() -> u64 { WRITE_BYTES[0].get() }
pub fn writes() -> Vec<(&'static str, u64)> {
    WRITE_SOURCES.iter().enumerate().map(|(i, s)| (*s, WRITE_BYTES[i].get())).collect()
}

static RUN_PARTS: [[Counter; 5]; 2] = [const { [const { Counter::new() }; 5] }; 2];
pub fn run_parts(path: usize, data: u64, index: u64, filter: u64, footer: u64, sidecar: u64) {
    for (c, n) in RUN_PARTS[path].iter().zip([data, index, filter, footer, sidecar]) { c.add(n); }
}
pub fn parts() -> Vec<(&'static str, &'static str, u64)> {
    ["flush", "merge"].iter().enumerate().flat_map(|(i, path)| {
        ["data", "index", "filter", "footer", "sidecar"].iter().enumerate()
            .map(move |(j, part)| (*path, *part, RUN_PARTS[i][j].get()))
    }).collect()
}

#[derive(Clone, Copy)]
pub enum LookupCaller { Get, Digest, Exists, Apply }
impl LookupCaller {
    pub fn index(self) -> usize { match self { Self::Get => 0, Self::Digest => 1, Self::Exists => 2, Self::Apply => 3 } }
}
pub const LOOKUP_CALLERS: [&str; 4] = ["get", "digest", "exists", "apply"];
struct LookupStats {
    counts: [std::cell::Cell<u64>; 4],
    probes: [std::cell::Cell<u64>; 4],
    positives: [std::cell::Cell<u64>; 4],
    covered: [std::cell::Cell<u64>; 4],
    uncovered: [std::cell::Cell<u64>; 4],
}
thread_local! {
    static LOOKUPS: LookupStats = const { LookupStats {
        counts: [const { std::cell::Cell::new(0) }; 4],
        probes: [const { std::cell::Cell::new(0) }; 4],
        positives: [const { std::cell::Cell::new(0) }; 4],
        covered: [const { std::cell::Cell::new(0) }; 4],
        uncovered: [const { std::cell::Cell::new(0) }; 4],
    } };
}
pub fn record_lookup(caller: LookupCaller) { LOOKUPS.with(|s| s.lookup(caller)); }
pub fn record_blooms(caller: LookupCaller, probes: u64, positives: u64, covered: u64, uncovered: u64) {
    LOOKUPS.with(|s| s.blooms(caller, probes, positives, covered, uncovered));
}
pub fn lookup_snapshot() -> [[u64; 5]; 4] { LOOKUPS.with(LookupStats::snapshot) }
impl LookupStats {
    pub fn lookup(&self, caller: LookupCaller) {
        let c = &self.counts[caller.index()]; c.set(c.get() + 1);
    }
    pub fn blooms(&self, caller: LookupCaller, probes: u64, positives: u64, covered: u64, uncovered: u64) {
        let i = caller.index();
        for (c, n) in [(&self.probes[i], probes), (&self.positives[i], positives),
            (&self.covered[i], covered), (&self.uncovered[i], uncovered)] { c.set(c.get() + n); }
    }
    pub fn snapshot(&self) -> [[u64; 5]; 4] {
        std::array::from_fn(|i| [self.counts[i].get(), self.probes[i].get(), self.positives[i].get(),
            self.covered[i].get(), self.uncovered[i].get()])
    }
}

pub fn thread_cpu_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        let mut ts = libc::timespec { tv_sec: 0, tv_nsec: 0 };
        // SAFETY: ts is a valid writable timespec; the clock has no other preconditions.
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0 {
            return ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64;
        }
    }
    0
}

pub struct Timer {
    wall: std::time::Instant,
    cpu: u64,
    wall_counter: &'static Counter,
    cpu_counter: Option<&'static Counter>,
}
impl Timer {
    pub fn new(wall_counter: &'static Counter, cpu_counter: Option<&'static Counter>) -> Self {
        Self { wall: std::time::Instant::now(), cpu: if cpu_counter.is_some() { thread_cpu_ns() } else { 0 }, wall_counter, cpu_counter }
    }
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.wall_counter.add(self.wall.elapsed().as_nanos() as u64);
        if let Some(c) = self.cpu_counter { c.add(thread_cpu_ns().saturating_sub(self.cpu)); }
    }
}
