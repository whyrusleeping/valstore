use super::*;

use std::os::unix::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

const OVERWRITE_EVERY: usize = 100;
const TOMBSTONE_OFFSET: usize = 50;
const FRESH_DIVISOR: usize = 20;
const OVERWRITE_SALT: u64 = 0x9e37_79b9_7f4a_7c15;

struct RssSampler {
    peak: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> Self {
        let peak = Arc::new(AtomicU64::new(process_rss()));
        let stop = Arc::new(AtomicBool::new(false));
        let sample_peak = peak.clone();
        let sample_stop = stop.clone();
        let worker = std::thread::spawn(move || {
            while !sample_stop.load(AtomicOrdering::Relaxed) {
                sample_peak.fetch_max(process_rss(), AtomicOrdering::Relaxed);
                std::thread::sleep(Duration::from_millis(2));
            }
            sample_peak.fetch_max(process_rss(), AtomicOrdering::Relaxed);
        });
        RssSampler { peak, stop, worker: Some(worker) }
    }

    fn reset(&self) {
        self.peak.store(process_rss(), AtomicOrdering::Relaxed);
    }

    fn report(&self, phase: &str) {
        let current = trimmed_rss();
        self.peak.fetch_max(current, AtomicOrdering::Relaxed);
        println!(
            "CAPACITY MEMORY phase={phase} trimmed_rss_bytes={current} sampled_peak_bytes={}",
            self.peak.load(AtomicOrdering::Relaxed)
        );
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.stop.store(true, AtomicOrdering::Relaxed);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone, Copy, Default)]
struct ProcIo {
    read_bytes: u64,
    write_bytes: u64,
}

fn process_rss() -> u64 {
    vals_cluster::observe::process_resident_bytes().unwrap_or(0)
}

fn trimmed_rss() -> u64 {
    malloc_trim();
    process_rss()
}

fn proc_io() -> ProcIo {
    let mut out = ProcIo::default();
    if let Ok(text) = std::fs::read_to_string("/proc/self/io") {
        for line in text.lines() {
            if let Some(value) = line.strip_prefix("read_bytes: ") {
                out.read_bytes = value.parse().unwrap_or(0);
            } else if let Some(value) = line.strip_prefix("write_bytes: ") {
                out.write_bytes = value.parse().unwrap_or(0);
            }
        }
    }
    out
}

fn process_cpu_ns() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return 0;
    }
    let usage = unsafe { usage.assume_init() };
    let user = usage.ru_utime.tv_sec as u64 * 1_000_000_000
        + usage.ru_utime.tv_usec as u64 * 1_000;
    let system = usage.ru_stime.tv_sec as u64 * 1_000_000_000
        + usage.ru_stime.tv_usec as u64 * 1_000;
    user + system
}

fn capacity_key(i: usize) -> Vec<u8> {
    format!(
        "app.bsky.feed.post/did:plc:{:016x}/{:016x}",
        i / 64,
        i
    )
    .into_bytes()
}

fn fresh_count(n: usize) -> usize {
    (n / FRESH_DIVISOR).max(1)
}

fn overwrite_count(n: usize) -> usize {
    n.div_ceil(OVERWRITE_EVERY)
}

fn tombstone_count(n: usize) -> usize {
    if n <= TOMBSTONE_OFFSET {
        0
    } else {
        (n - TOMBSTONE_OFFSET).div_ceil(OVERWRITE_EVERY)
    }
}

fn capacity_version(physical: u64) -> vals_cluster::Version {
    vals_cluster::Version { hlc: physical << 16, node: 1 }
}

fn apply_capacity(
    store: &mut vals_cluster::VersionedStore<FileBlockDevice>,
    key: &[u8],
    value: Option<&[u8]>,
    physical: u64,
) {
    let applied = store
        .apply_at(key, value, capacity_version(physical), physical)
        .expect("capacity mutation");
    assert!(matches!(applied, vals_cluster::Applied::Won), "capacity mutation lost");
}

fn expected(i: usize, n: usize, vsize: usize) -> (Option<Vec<u8>>, u64) {
    let overwrites = overwrite_count(n);
    let tombstones = tombstone_count(n);
    let (value, physical) = if i >= n {
        let j = i - n;
        (
            Some(make_value(vsize, i as u64)),
            n + overwrites + tombstones + j + 1,
        )
    } else if i.is_multiple_of(OVERWRITE_EVERY) {
        (
            Some(make_value(vsize, i as u64 ^ OVERWRITE_SALT)),
            n + i / OVERWRITE_EVERY + 1,
        )
    } else if i % OVERWRITE_EVERY == TOMBSTONE_OFFSET {
        (None, n + overwrites + (i - TOMBSTONE_OFFSET) / OVERWRITE_EVERY + 1)
    } else {
        (Some(make_value(vsize, i as u64)), i + 1)
    };
    (value, (physical as u64) << 16)
}

fn report_engine_stats<D: BlockDevice>(phase: &str, engine: &Engine<D>) {
    let s = engine.capacity_stats();
    println!(
        "CAPACITY STRUCTURES phase={phase} mutable_entries={} mutable_logical_bytes={} run_count={} run_entries={} run_disk_bytes={} sidecar_disk_bytes={} live_value_bytes={} run_resident_bytes={} fence_resident_bytes={} filter_resident_bytes={} manifest_resident_bytes={} winner_directory_bytes={} used_segments={}",
        s.mutable_entries,
        s.mutable_logical_bytes,
        s.run_count,
        s.run_entries,
        s.run_disk_bytes,
        s.sidecar_disk_bytes,
        s.live_value_bytes,
        s.run_resident_bytes,
        s.fence_resident_bytes,
        s.filter_resident_bytes,
        s.manifest_resident_bytes,
        s.winner_directory_bytes,
        engine.used_segments(),
    );
}

fn flush_daemon_path(store: &mut vals_cluster::VersionedStore<FileBlockDevice>) {
    let mut plan = loop {
        match store
            .flush_plan_step(vals_store::FLUSH_SLICE_ENTRIES)
            .expect("capacity stepped flush plan")
        {
            Some(vals_store::FlushPlanStep::Pending) => {}
            Some(vals_store::FlushPlanStep::Ready(plan)) => break plan,
            None => panic!("capacity flush unexpectedly found an empty memtable"),
        }
    };
    plan.set_unpaced();
    let write_dev = store.device_sibling().expect("capacity flush sibling");
    let written = vals_rt::block_on(
        vals_store::build_and_write_flush(plan, &write_dev),
        || {
            write_dev.poll_device();
        },
    )
    .unwrap_or_else(|(_, error)| panic!("capacity flush write: {error}"));
    let mut commit = store.flush_commit_start(written).expect("capacity flush commit start");
    while let vals_store::FlushCommitStep::Pending =
        store.flush_commit_step(&mut commit, vals_store::FLUSH_SLICE_ENTRIES)
    {}
}

fn evict_file(path: &str) {
    let file = std::fs::File::open(path).expect("open capacity image for cache eviction");
    file.sync_all().expect("sync capacity image before cache eviction");
    #[cfg(target_os = "linux")]
    {
        let rc = unsafe { libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        assert_eq!(rc, 0, "posix_fadvise(DONTNEED) failed");
    }
    #[cfg(not(target_os = "linux"))]
    panic!("capacity cache eviction uses posix_fadvise(DONTNEED), which needs Linux");
}

fn exceeds_lateness(now: Instant, due: Instant, budget: Duration) -> bool {
    now.checked_duration_since(due).is_some_and(|lateness| lateness > budget)
}

fn cold_probes(
    store: &vals_cluster::VersionedStore<FileBlockDevice>,
    path: &str,
    n: usize,
    vsize: usize,
    probes: usize,
) {
    let offered_rate: u64 = std::env::var("VALS_CAPACITY_OFFERED_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    let late_us: u64 = std::env::var("VALS_CAPACITY_LATE_US")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    assert!(offered_rate > 0, "VALS_CAPACITY_OFFERED_OPS must be positive");
    evict_file(path);
    let io0 = proc_io();
    let cpu0 = process_cpu_ns();
    let started = Instant::now();
    let mut rng = Rng(0xd1b5_4a32_d192_ed03);
    let total = n + fresh_count(n);
    let mut completed = 0usize;
    let mut harness_lateness_drops = 0usize;
    let mut hits = 0usize;
    let mut misses = 0usize;
    let mut arrival_latencies = Vec::with_capacity(probes);
    let mut service_latencies = Vec::with_capacity(probes);
    let lateness_budget = Duration::from_micros(late_us);
    for offered in 0..probes {
        let due = started + Duration::from_nanos(
            (offered as u64).saturating_mul(1_000_000_000) / offered_rate,
        );
        let now = Instant::now();
        if now < due {
            std::thread::sleep(due - now);
        }
        if exceeds_lateness(Instant::now(), due, lateness_budget) {
            harness_lateness_drops += 1;
            continue;
        }
        let positive = offered % 2 == 0;
        let index = if positive {
            rng.below(total as u64) as usize
        } else {
            total + offered
        };
        let key = capacity_key(index);
        let began = Instant::now();
        let got = store.get_versioned(&key).expect("cold capacity probe");
        let finished = Instant::now();
        service_latencies.push(finished.duration_since(began).as_nanos() as u64);
        arrival_latencies.push(finished.saturating_duration_since(due).as_nanos() as u64);
        if positive {
            let (want_value, want_hlc) = expected(index, n, vsize);
            let got = got.expect("cold positive probe missed");
            assert_eq!(got.value, want_value, "cold probe value/tombstone mismatch");
            assert_eq!(got.version.hlc, want_hlc, "cold probe version mismatch");
            assert_eq!(got.version.node, 1, "cold probe writer identity mismatch");
            hits += 1;
        } else {
            assert!(got.is_none(), "cold negative probe unexpectedly hit");
            misses += 1;
        }
        completed += 1;
    }
    let elapsed = started.elapsed();
    let io1 = proc_io();
    let cpu = process_cpu_ns().saturating_sub(cpu0);
    arrival_latencies.sort_unstable();
    service_latencies.sort_unstable();
    println!(
        "CAPACITY OPEN_LOOP offered={} completed={} rejected={} rejection_kind=harness_lateness_drop harness_lateness_drops={} server_rejected=0 offered_ops_s={} lateness_budget_us={} wall_s={:.6} hits={} misses={} arrival_p50_us={:.3} arrival_p99_us={:.3} arrival_p999_us={:.3} service_p50_us={:.3} service_p99_us={:.3} service_p999_us={:.3} cpu_ns_per_completed={:.1} read_bytes={} read_bytes_per_completed={:.1}",
        probes,
        completed,
        harness_lateness_drops,
        harness_lateness_drops,
        offered_rate,
        late_us,
        elapsed.as_secs_f64(),
        hits,
        misses,
        percentile(&arrival_latencies, 50.0) as f64 / 1000.0,
        percentile(&arrival_latencies, 99.0) as f64 / 1000.0,
        percentile(&arrival_latencies, 99.9) as f64 / 1000.0,
        percentile(&service_latencies, 50.0) as f64 / 1000.0,
        percentile(&service_latencies, 99.0) as f64 / 1000.0,
        percentile(&service_latencies, 99.9) as f64 / 1000.0,
        cpu as f64 / completed.max(1) as f64,
        io1.read_bytes.saturating_sub(io0.read_bytes),
        io1.read_bytes.saturating_sub(io0.read_bytes) as f64 / completed.max(1) as f64,
    );
}

fn verify_scan(
    store: &vals_cluster::VersionedStore<FileBlockDevice>,
    n: usize,
    vsize: usize,
    page_size: usize,
) {
    let total = n + fresh_count(n);
    let mut cursor: Option<Vec<u8>> = None;
    let mut next_index = 0usize;
    let mut seen = 0usize;
    let mut seen_tombstones = 0usize;
    let mut pages = 0usize;
    let started = Instant::now();
    loop {
        let (page, more) = store
            .scan_page(cursor.as_deref(), None, page_size, 8 << 20)
            .expect("capacity versioned scan");
        pages += 1;
        for (key, stored) in &page {
            assert!(next_index < total, "scan returned an extra key");
            let want_key = capacity_key(next_index);
            let (want_value, want_hlc) = expected(next_index, n, vsize);
            assert_eq!(key, &want_key, "scan key order/continuation mismatch");
            assert_eq!(stored.value, want_value, "scan winner value/tombstone mismatch");
            assert_eq!(stored.version.hlc, want_hlc, "scan winner version mismatch");
            assert_eq!(stored.version.node, 1, "scan writer identity mismatch");
            seen_tombstones += usize::from(stored.value.is_none());
            next_index += 1;
            seen += 1;
        }
        if !more {
            break;
        }
        let mut next = page.last().expect("non-final scan page is non-empty").0.clone();
        next.push(0);
        cursor = Some(next);
    }
    assert_eq!(next_index, total, "scan omitted a suffix");
    assert_eq!(seen, total, "scan logical cardinality mismatch");
    assert_eq!(seen_tombstones, tombstone_count(n), "scan tombstone count mismatch");
    println!(
        "CAPACITY CORRECTNESS scan=ok ordered=ok continuation=ok versions=ok writer=ok tombstones=ok values=ok logical={} live={} tombstones={} pages={} wall_s={:.6}",
        seen,
        seen - seen_tombstones,
        seen_tombstones,
        pages,
        started.elapsed().as_secs_f64(),
    );
}

pub(super) fn workload_capacity(backend: &str, path: &str, n: usize, vsize: usize, probes: usize) {
    if backend != "file" {
        eprintln!("capacity workload requires the file backend");
        std::process::exit(2);
    }
    assert!(n > TOMBSTONE_OFFSET, "capacity workload needs more than 50 keys");
    let sampler = RssSampler::start();
    let io0 = proc_io();
    let cpu0 = process_cpu_ns();
    println!(
        "CAPACITY CONFIG keys={} value_bytes={} writer_id=1 key_order=monotonic_shared_prefix history_overwrite_pct=1 history_tombstone_pct=1 history_fresh_pct=5 probes={} block_bytes={} bloom_bits_per_key={} ae_buckets={}",
        n,
        vsize,
        probes,
        BLOCK_SIZE,
        RunBuildOpts::default().bloom_bits_per_key,
        RunBuildOpts::default().num_buckets,
    );
    sampler.report("baseline");
    sampler.reset();

    let blocks = device_blocks(n.saturating_mul(2), vsize);
    let dev = FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("create capacity image");
    let engine = Engine::create_prewiped(dev, bench_config(Ordering::Sorted));
    let mut store = vals_cluster::VersionedStore::new(engine, 1).expect("capacity versioned store");
    let initial_started = Instant::now();
    for i in 0..n {
        let key = capacity_key(i);
        let value = make_value(vsize, i as u64);
        apply_capacity(&mut store, &key, Some(&value), (i + 1) as u64);
    }
    println!(
        "CAPACITY PHASE phase=initial_insert operations={} wall_s={:.6} cpu_s={:.6}",
        n,
        initial_started.elapsed().as_secs_f64(),
        process_cpu_ns().saturating_sub(cpu0) as f64 / 1e9,
    );
    report_engine_stats("initial_mutable", store.engine());
    sampler.report("initial_mutable");
    sampler.reset();

    let flush1 = Instant::now();
    flush_daemon_path(&mut store);
    store.sync().expect("sync initial flush");
    println!("CAPACITY PHASE phase=initial_flush wall_s={:.6}", flush1.elapsed().as_secs_f64());
    report_engine_stats("initial_flushed", store.engine());
    sampler.report("initial_flush");
    sampler.reset();

    let history_started = Instant::now();
    let mut physical = n as u64;
    for i in (0..n).step_by(OVERWRITE_EVERY) {
        physical += 1;
        let key = capacity_key(i);
        let value = make_value(vsize, i as u64 ^ OVERWRITE_SALT);
        apply_capacity(&mut store, &key, Some(&value), physical);
    }
    for i in (TOMBSTONE_OFFSET..n).step_by(OVERWRITE_EVERY) {
        physical += 1;
        let key = capacity_key(i);
        apply_capacity(&mut store, &key, None, physical);
    }
    for i in n..n + fresh_count(n) {
        physical += 1;
        let key = capacity_key(i);
        let value = make_value(vsize, i as u64);
        apply_capacity(&mut store, &key, Some(&value), physical);
    }
    flush_daemon_path(&mut store);
    store.sync().expect("sync history flush");
    let history_ops = overwrite_count(n) + tombstone_count(n) + fresh_count(n);
    println!(
        "CAPACITY PHASE phase=history_flush operations={} wall_s={:.6}",
        history_ops,
        history_started.elapsed().as_secs_f64(),
    );
    report_engine_stats("history_flushed", store.engine());
    sampler.report("history_flush");
    let io_loaded = proc_io();
    let mutation_cpu = process_cpu_ns().saturating_sub(cpu0);
    let logical_bytes = (0..n)
        .map(|i| capacity_key(i).len() as u64 + vsize as u64)
        .sum::<u64>()
        + (0..n)
            .step_by(OVERWRITE_EVERY)
            .map(|i| capacity_key(i).len() as u64 + vsize as u64)
            .sum::<u64>()
        + (TOMBSTONE_OFFSET..n)
            .step_by(OVERWRITE_EVERY)
            .map(|i| capacity_key(i).len() as u64)
            .sum::<u64>()
        + (n..n + fresh_count(n))
            .map(|i| capacity_key(i).len() as u64 + vsize as u64)
            .sum::<u64>();
    let written = io_loaded.write_bytes.saturating_sub(io0.write_bytes);
    println!(
        "CAPACITY WRITE_IO logical_mutation_bytes={} process_write_bytes={} write_amplification={:.3} cpu_ns_per_mutation={:.1} cpu_timer_scope=process_start_through_history_sync_includes_phase_diagnostics_excludes_logical_byte_recomputation",
        logical_bytes,
        written,
        written as f64 / logical_bytes.max(1) as f64,
        mutation_cpu as f64 / (n + history_ops) as f64,
    );

    drop(store);
    let _ = trimmed_rss();
    sampler.reset();
    evict_file(path);
    let reopen_io0 = proc_io();
    let reopen_cpu0 = process_cpu_ns();
    let reopen_started = Instant::now();
    let dev = FileBlockDevice::open(path, BLOCK_SIZE).expect("open capacity image");
    let engine = Engine::open(dev, bench_config(Ordering::Sorted)).expect("reopen capacity engine");
    let reopen_wall = reopen_started.elapsed();
    let reopen_io1 = proc_io();
    println!(
        "CAPACITY REOPEN wall_s={:.6} cpu_s={:.6} read_bytes={}",
        reopen_wall.as_secs_f64(),
        process_cpu_ns().saturating_sub(reopen_cpu0) as f64 / 1e9,
        reopen_io1.read_bytes.saturating_sub(reopen_io0.read_bytes),
    );
    report_engine_stats("reopen", &engine);
    sampler.report("reopen");

    let store = vals_cluster::VersionedStore::new(engine, 1).expect("capacity versioned store");
    sampler.reset();
    let seed_io0 = proc_io();
    let seed_cpu0 = process_cpu_ns();
    let seed_before = vals_cluster::observe::snapshot();
    let seed_started = Instant::now();
    let interference = std::env::var_os("VALS_CAPACITY_SEED_INTERFERENCE").is_some();
    let mut store = store;
    let mut seed_steps = 0u64;
    loop {
        seed_steps += 1;
        if interference && seed_steps == 3 {
            for i in 0..64 {
                let key = format!("interference-{i:04}");
                apply_capacity(&mut store, key.as_bytes(), Some(b"write"), (n * 3 + i) as u64);
            }
            flush_daemon_path(&mut store);
            let before = store.engine().run_count();
            store.compact_runs(0).expect("capacity concurrent compaction");
            println!("CAPACITY INTERFERENCE writes=64 flushes=1 runs_before_compact={} runs_after_compact={}",
                before, store.engine().run_count());
        }
        if store.seed_step().expect("capacity AE seed step") {
            break;
        }
    }
    let seed_io1 = proc_io();
    let seed_after = vals_cluster::observe::snapshot();
    println!(
        "CAPACITY SEED mode=stream interference={} steps={} wall_s={:.6} cpu_s={:.6} read_bytes={} seeded={} restarts={} fallbacks={}",
        interference,
        seed_steps,
        seed_started.elapsed().as_secs_f64(),
        process_cpu_ns().saturating_sub(seed_cpu0) as f64 / 1e9,
        seed_io1.read_bytes.saturating_sub(seed_io0.read_bytes),
        store.is_leaf_seeded(),
        seed_after.ae_seed_restarts - seed_before.ae_seed_restarts,
        seed_after.ae_seed_sync_fallbacks - seed_before.ae_seed_sync_fallbacks,
    );
    sampler.report("ae_seed");
    report_engine_stats("seeded", store.engine());
    if interference { return; }

    sampler.reset();
    cold_probes(&store, path, n, vsize, probes);
    sampler.report("cold_probes");
    sampler.reset();
    verify_scan(&store, n, vsize, 4096);
    sampler.report("full_scan");

    let metadata = std::fs::metadata(path).expect("capacity image metadata");
    let physical = metadata.blocks().saturating_mul(512);
    let logical_keys = n + fresh_count(n) - tombstone_count(n);
    let final_stats = store.engine().capacity_stats();
    println!(
        "CAPACITY FOOTPRINT file_length_bytes={} file_allocated_bytes={} allocated_bytes_per_live_key={:.3} live_keys={} run_entries={} maintenance_run_count={} recovered_tail_mutable_entries={} maintenance_debt_qualification=two_fully_flushed_runs_below_default_run_threshold_reopen_replays_conservative_tail_no_forced_compaction",
        metadata.len(),
        physical,
        physical as f64 / logical_keys as f64,
        logical_keys,
        final_stats.run_entries,
        store.engine().run_count(),
        store.engine().len(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lateness_budget_is_inclusive() {
        let due = Instant::now();
        let budget = Duration::from_micros(10);
        assert!(!exceeds_lateness(due - Duration::from_micros(1), due, budget));
        assert!(!exceeds_lateness(due + budget, due, budget));
        assert!(exceeds_lateness(due + budget + Duration::from_nanos(1), due, budget));
    }
}
