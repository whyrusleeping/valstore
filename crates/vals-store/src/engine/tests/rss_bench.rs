use super::*;
use std::cell::RefCell;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;
use std::time::Instant;

thread_local! {
    static HOOK: RefCell<Option<(Sender<Event>, Receiver<()>)>> = const { RefCell::new(None) };
}

pub(crate) fn merge_snapshot(stage: &'static str, bloom: u64, fence: u64, builder: u64) {
    HOOK.with(|hook| {
        if let Some((tx, ack)) = hook.borrow().as_ref() {
            tx.send(Event::Snapshot(stage, bloom, fence, builder)).unwrap();
            ack.recv().unwrap();
        }
    });
}

enum Event {
    Snapshot(&'static str, u64, u64, u64),
    Done(WrittenCompact<FileBlockDevice>),
}

struct Worker {
    events: Receiver<Event>,
    ack: Sender<()>,
    thread: JoinHandle<()>,
    seed: super::super::pins::PinnedRuns<FileBlockDevice>,
    retained_fences: u64,
    started: Instant,
}

fn allocator_snapshot(label: &str) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let field = |name: &str| -> u64 {
        status.lines().find(|l| l.starts_with(name)).and_then(|l| l.split_whitespace().nth(1)).unwrap().parse::<u64>().unwrap() * 1024
    };
    // SAFETY: mallinfo2 reads glibc allocator statistics and takes no pointers.
    let info = unsafe { libc::mallinfo2() };
    let mut xml_ptr = std::ptr::null_mut();
    let mut xml_len = 0;
    // SAFETY: open_memstream initializes the provided output pointer and length; its stream is closed before reading and freeing the buffer.
    let arenas = unsafe {
        let file = libc::open_memstream(&mut xml_ptr, &mut xml_len);
        if file.is_null() { 0 } else {
            libc::malloc_info(0, file);
            libc::fclose(file);
            let xml = std::slice::from_raw_parts(xml_ptr.cast::<u8>(), xml_len);
            let count = xml.windows(6).filter(|w| *w == b"<heap ").count();
            libc::free(xml_ptr.cast());
            count
        }
    };
    #[cfg(feature = "heap-profile")]
    let (requested, live_count, allocs, reallocs, internal) = crate::heap_profile::counts();
    #[cfg(not(feature = "heap-profile"))]
    let (requested, live_count, allocs, reallocs, internal) = (0, 0, 0, 0, 0);
    eprintln!("RSS397 allocator label={label} rss={} anon={} arena={} arenas={arenas} ordblks={} uordblks={} fordblks={} hblkhd={} keepcost={} requested={requested} live_count={live_count} allocs={allocs} reallocs={reallocs} profiler_internal={internal}",
        field("VmRSS:"), field("RssAnon:"), info.arena, info.ordblks, info.uordblks, info.fordblks, info.hblkhd, info.keepcost);
}

fn snapshot(e: &Engine<FileBlockDevice>, stored: u64, stage: &str, worker_bloom: u64, worker_fence: u64, builder: u64, seed: u64) {
    let stats = e.capacity_stats();
    let kf = e.key_filter_stats();
    let label = format!("{stage}-{stored}");
    eprintln!("RSS397 serving label={label} stored={stored} runs={} run_entries={} metadata={} key_filter={} key_filter_allocated={} bloom={} bloom_payload={} fence={} fence_payload={} memtable={} mutable_entries={} winner={} flush_cleanup={} worker_bloom={worker_bloom} worker_fence={worker_fence} builder={builder} seed_retained_fence={seed}",
        stats.run_count, stats.run_entries, stats.run_resident_bytes, kf.resident_bytes, kf.filter_bytes,
        stats.run_filter_resident_bytes, stats.run_filter_payload_bytes, stats.fence_resident_bytes,
        stats.fence_payload_bytes, stats.mutable_logical_bytes, stats.mutable_entries, stats.winner_directory_bytes,
        e.flush_cleanup.capacity() * std::mem::size_of::<SnapshotEntry>() + e.flush_cleanup.iter().map(|entry| entry.key.capacity()).sum::<usize>());
    eprintln!("RSS397 capacity label={label} memtable_allocated={} flush_protection={} hash_buckets={}", stats.mutable_allocated_bytes, stats.flush_protection_bytes, e.index.hash_bucket_count());
    allocator_snapshot(&label);
    #[cfg(feature = "heap-profile")]
    if stored >= env_number("VALS_RSS_DUMP_KEYS", 90_000_000) || stored == 0 {
        crate::heap_profile::dump(&label);
    }
}

fn native_sample(started: Instant, ingesting: bool) -> (u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let field = |name: &str| -> u64 {
        status.lines().find(|l| l.starts_with(name)).and_then(|l| l.split_whitespace().nth(1)).unwrap().parse::<u64>().unwrap() * 1024
    };
    // SAFETY: mallinfo2 takes no pointers and glibc serializes allocator access.
    let info = unsafe { libc::mallinfo2() };
    let rss = field("VmRSS:");
    let anon = field("RssAnon:");
    eprintln!("RSS397 sample seconds={:.6} ingesting={} rss={rss} anon={anon} native_in_use={} fordblks={} arena={} hblkhd={}",
        started.elapsed().as_secs_f64(), u8::from(ingesting), info.uordblks + info.hblkhd, info.fordblks, info.arena, info.hblkhd);
    (rss, anon)
}

fn start_allocator_sampler(interval: u64, trim_nice: u64) -> (Sender<()>, Arc<std::sync::atomic::AtomicBool>, JoinHandle<()>) {
    let (stop, rx) = mpsc::channel();
    let ingesting = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let active = ingesting.clone();
    let thread = std::thread::spawn(move || {
        if interval != 0 {
            // SAFETY: on Linux, pid 0 selects only this thread; raising niceness needs no privilege.
            assert_eq!(unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, i32::try_from(trim_nice).unwrap()) }, 0);
        }
        let started = Instant::now();
        let mut last_trim = started;
        loop {
            let (before, _) = native_sample(started, active.load(std::sync::atomic::Ordering::Relaxed));
            if interval != 0 && last_trim.elapsed().as_secs() >= interval {
                let trim_start = Instant::now();
                // SAFETY: malloc_trim releases only free pages; live ingest/worker allocations remain valid.
                let result = unsafe { libc::malloc_trim(0) };
                let elapsed = trim_start.elapsed().as_secs_f64();
                let (after, _) = native_sample(started, active.load(std::sync::atomic::Ordering::Relaxed));
                eprintln!("RSS397 background_trim seconds={elapsed:.6} at_s={:.6} before={before} after={after} returned={} result={result}",
                    started.elapsed().as_secs_f64(), before.saturating_sub(after));
                last_trim = Instant::now();
            }
            match rx.recv_timeout(std::time::Duration::from_secs(1)) {
                Err(mpsc::RecvTimeoutError::Timeout) => {},
                _ => { native_sample(started, active.load(std::sync::atomic::Ordering::Relaxed)); break; },
            }
        }
    });
    (stop, ingesting, thread)
}

fn env_number(name: &str, default: u64) -> u64 {
    std::env::var(name).ok().and_then(|s| s.parse().ok()).unwrap_or(default)
}

fn start_worker(e: &mut Engine<FileBlockDevice>) -> Option<Worker> {
    let plan = e.compact_plan(RunBuildOpts::default(), None).unwrap()?;
    let retained_fences = e.runs.iter().filter(|r| plan.merged_inputs.iter().any(|m| m.handle == r.meta.handle) && !r.run.has_sidecar()).map(|r| r.run.resident_stats().fence_bytes).sum();
    let seed = e.pin_runs(true);
    let dev = Arc::new(e.device().sibling().unwrap());
    let (tx, events) = mpsc::channel();
    let (ack, rx) = mpsc::channel();
    let started = Instant::now();
    let thread = std::thread::spawn(move || {
        HOOK.with(|hook| *hook.borrow_mut() = Some((tx.clone(), rx)));
        let written = vals_rt::block_on(compact_and_write(plan, dev), || {}).unwrap_or_else(|(_, err)| panic!("{err}"));
        HOOK.with(|hook| *hook.borrow_mut() = None);
        tx.send(Event::Done(written)).unwrap();
    });
    Some(Worker { events, ack, thread, seed, retained_fences, started })
}

fn service_worker(e: &mut Engine<FileBlockDevice>, worker: &mut Option<Worker>, stored: u64, wait: bool, merge_seconds: &mut f64, merges: &mut u64) {
    loop {
        let Some(w) = worker.as_ref() else { return; };
        let event = if wait { w.events.recv().unwrap() } else {
            match w.events.try_recv() {
                Ok(event) => event,
                Err(mpsc::TryRecvError::Empty) => return,
                Err(err) => panic!("merge worker disconnected: {err}"),
            }
        };
        match event {
            Event::Snapshot(stage, bloom, fence, builder) => {
                snapshot(e, stored, stage, bloom, fence, builder, 0);
                w.ack.send(()).unwrap();
            }
            Event::Done(written) => {
                let w = worker.take().unwrap();
                w.thread.join().unwrap();
                *merge_seconds += w.started.elapsed().as_secs_f64();
                e.compact_commit(written).unwrap();
                *merges += 1;
                snapshot(e, stored, "commit-seed-held", 0, 0, 0, w.retained_fences);
                drop(w.seed);
                e.reap_run_pins();
                snapshot(e, stored, "seed-released", 0, 0, 0, 0);
                return;
            }
        }
    }
}

fn scans(e: &mut Engine<FileBlockDevice>) -> u64 {
    let mut bytes = 0;
    while let Some(mut plan) = e.cover_scan_plan() {
        let dev = e.device().sibling().unwrap();
        let thread = std::thread::spawn(move || {
            while !plan.step(&dev, 16 << 20).unwrap() {}
            plan
        });
        let plan = thread.join().unwrap();
        bytes += plan.bytes_read();
        assert!(e.cover_scan_commit(plan));
    }
    bytes
}

fn measure_key(id: u64, replay: bool, buffer: &mut [u8; 192]) -> &[u8] {
    if !replay {
        buffer[..8].copy_from_slice(&(id * 2).to_be_bytes());
        return &buffer[..8];
    }
    use std::io::Write;
    let mut out = buffer.as_mut_slice();
    if id.is_multiple_of(2) {
        write!(out, "app.bsky.graph.follow/did:plc:{:024x}/{id:013x}", id / 1000).unwrap();
    } else {
        write!(out, "at://did:plc:{:024x}/app.bsky.feed.post/{:013x}\0at://did:plc:{:024x}/app.bsky.feed.like/{id:013x}\0subject.uri", id / 1000, id / 1000, id / 1000 + 1).unwrap();
    }
    let len = 192 - out.len();
    &buffer[..len]
}

fn measure_value(id: u64, replay: bool, updated: bool) -> &'static [u8] {
    if replay {
        let value = if updated { &[b'u'; 64] } else { &[b'v'; 64] };
        &value[..if id.is_multiple_of(2) { 64 } else { 8 }]
    } else if updated { b"updated" } else { b"value" }
}

fn ingest(e: &mut Engine<FileBlockDevice>, round: u64, rounds: u64, range: std::ops::Range<u64>, replay: bool) -> u64 {
    let mut requests = 0;
    let mut buffer = [0; 192];
    for i in range {
        let id = i * rounds + round;
        let k = measure_key(id, replay, &mut buffer);
        let probe = e.probe_version(k);
        assert!(vals_rt::block_on(Engine::version_from_probe(probe, e.device()), || {}).unwrap().is_none());
        e.put_kind_authoritative(k, measure_value(id, replay, false), (round + 1) as u128, false).unwrap();
        requests += 1;
        if round > 0 && i % 100 == 0 {
            let id = i * rounds + round - 1;
            let old = measure_key(id, replay, &mut buffer);
            e.put_kind(old, measure_value(id, replay, true), (round + 1) as u128, i % 1000 == 0).unwrap();
            requests += 1;
        }
    }
    requests
}

#[test]
#[ignore]
fn rss_remainder_file_ingest_measurement() {
    #[cfg(feature = "heap-profile")]
    crate::heap_profile::enable();
    let total = env_number("VALS_RSS_MEASURE_KEYS", 100_000_000);
    let batch = env_number("VALS_RSS_MEASURE_BATCH", 1_000_000).min(total);
    assert!(batch > 0);
    assert_eq!(total % batch, 0);
    let rounds = total / batch;
    let overlap_pct = env_number("VALS_RSS_OVERLAP_PCT", 0);
    assert!(overlap_pct <= 100);
    let overlap = batch * overlap_pct / 100;
    let ordering = match std::env::var("VALS_RSS_ORDERING").as_deref().unwrap_or("unsorted") {
        "sorted" => Ordering::Sorted,
        "unsorted" => Ordering::Unsorted,
        other => panic!("unknown ordering: {other}"),
    };
    let replay = match std::env::var("VALS_RSS_KEY_SHAPE").as_deref().unwrap_or("short") {
        "replay" => true,
        "short" => false,
        other => panic!("unknown key shape: {other}"),
    };
    let mmap_threshold = env_number("VALS_RSS_MMAP_THRESHOLD", 0);
    if mmap_threshold != 0 {
        // SAFETY: mallopt changes glibc allocator policy without invalidating live allocations.
        assert_eq!(unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, i32::try_from(mmap_threshold).unwrap()) }, 1);
    }
    let background_trim = env_number("VALS_RSS_BACKGROUND_TRIM_SECONDS", 0);
    let trim_nice = env_number("VALS_RSS_BACKGROUND_TRIM_NICE", 19);
    eprintln!("RSS397 config total={total} batch={batch} overlap_pct={overlap_pct} sorted={} replay={} mmap_threshold={mmap_threshold} background_trim={background_trim} trim_nice={trim_nice}", u8::from(ordering == Ordering::Sorted), u8::from(replay));
    let path = temp_path("rss397-file-measure");
    let dev = FileBlockDevice::create(&path, 4096, (total * 512 / 4096 + 8192) * 3).unwrap();
    let mut e = Engine::create_prewiped(dev, Config { seg_blocks: 256, reserved_blocks: 0, ordering });
    snapshot(&e, 0, "baseline", 0, 0, 0, 0);
    let wall = Instant::now();
    let (sampler_stop, ingesting, sampler) = start_allocator_sampler(background_trim, trim_nice);
    let mut put_seconds = 0.0;
    let mut flush_seconds = 0.0;
    let mut merge_seconds = 0.0;
    let mut scan_seconds = 0.0;
    let mut scan_bytes = 0;
    let mut requests = 0;
    let mut merges = 0;
    let mut worker = None;
    let trim_interval = env_number("VALS_RSS_TRIM_SECONDS", 0);
    let mut last_trim = Instant::now();
    let mut trim_calls = 0;
    let mut trim_seconds = 0.0;
    let mut trim_max_seconds = 0.0f64;
    for round in 0..rounds {
        service_worker(&mut e, &mut worker, round * batch, false, &mut merge_seconds, &mut merges);
        let started = Instant::now();
        requests += ingest(&mut e, round, rounds, if round == 0 { 0 } else { overlap }..batch, replay);
        put_seconds += started.elapsed().as_secs_f64();
        let started = Instant::now();
        if overlap == 0 {
            run_flush(&mut e, RunBuildOpts::default()).unwrap();
            flush_seconds += started.elapsed().as_secs_f64();
        } else {
            let plan = e.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
            let wdev = e.device().sibling().unwrap();
            let written = vals_rt::block_on(build_and_write_flush(plan, &wdev), || {}).unwrap();
            flush_seconds += started.elapsed().as_secs_f64();
            if round + 1 < rounds {
                let started = Instant::now();
                requests += ingest(&mut e, round + 1, rounds, 0..overlap, replay);
                put_seconds += started.elapsed().as_secs_f64();
            }
            let started = Instant::now();
            let mut commit = e.flush_commit_start(written).unwrap();
            while matches!(e.flush_commit_step(&mut commit, 4096), FlushCommitStep::Pending) {}
            flush_seconds += started.elapsed().as_secs_f64();
            assert_eq!(e.index.len(), if round + 1 < rounds { (overlap + overlap.div_ceil(100)) as usize } else { 0 });
        }
        let stored = (round + 1) * batch;
        eprintln!("RSS397 mutable stored={stored} entries={} allocated={} hash_buckets={} requests={requests} put_s={put_seconds:.6} runs={}", e.index.len(), e.index.allocated_bytes(), e.index.hash_bucket_count(), e.runs.len());
        service_worker(&mut e, &mut worker, stored, false, &mut merge_seconds, &mut merges);
        let started = Instant::now();
        scan_bytes += scans(&mut e);
        scan_seconds += started.elapsed().as_secs_f64();
        if trim_interval != 0 && last_trim.elapsed().as_secs() >= trim_interval {
            let label = format!("cadence-before-{stored}");
            allocator_snapshot(&label);
            let trim_start = Instant::now();
            // SAFETY: malloc_trim releases only free pages, including while the merge worker owns live allocations.
            unsafe { libc::malloc_trim(0); }
            let elapsed = trim_start.elapsed().as_secs_f64();
            trim_calls += 1;
            trim_seconds += elapsed;
            trim_max_seconds = trim_max_seconds.max(elapsed);
            last_trim = Instant::now();
            eprintln!("RSS397 trim stored={stored} seconds={elapsed:.6}");
            allocator_snapshot(&format!("cadence-after-{stored}"));
        }
        if worker.is_none() { worker = start_worker(&mut e); }
        if (round + 1) % (rounds / 10).max(1) == 0 {
            eprintln!("RSS397 progress stored={stored} requests={requests} merges={merges} put_s={put_seconds:.6} flush_s={flush_seconds:.6} scan_s={scan_seconds:.6} wall_s={:.6}", wall.elapsed().as_secs_f64());
        }
    }
    ingesting.store(false, std::sync::atomic::Ordering::Relaxed);
    eprintln!("RSS397 ingest_end seconds={:.6}", wall.elapsed().as_secs_f64());
    service_worker(&mut e, &mut worker, total, true, &mut merge_seconds, &mut merges);
    while let Some(w) = start_worker(&mut e) {
        worker = Some(w);
        service_worker(&mut e, &mut worker, total, true, &mut merge_seconds, &mut merges);
        let started = Instant::now();
        scan_bytes += scans(&mut e);
        scan_seconds += started.elapsed().as_secs_f64();
    }
    let started = Instant::now();
    scan_bytes += scans(&mut e);
    scan_seconds += started.elapsed().as_secs_f64();
    e.sync().unwrap();
    let seconds = wall.elapsed().as_secs_f64();
    sampler_stop.send(()).unwrap();
    sampler.join().unwrap();
    snapshot(&e, total, "settle-before-trim", 0, 0, 0, 0);
    let settle_trim_started = Instant::now();
    // SAFETY: malloc_trim asks glibc to return free pages; it does not invalidate live allocations.
    let trimmed = unsafe { libc::malloc_trim(0) };
    let settle_trim_seconds = settle_trim_started.elapsed().as_secs_f64();
    snapshot(&e, total, "settle-after-trim", 0, 0, 0, 0);
    eprintln!("RSS397 timing stored={total} requests={requests} merges={merges} put_s={put_seconds:.6} flush_s={flush_seconds:.6} merge_worker_s={merge_seconds:.6} scan_s={scan_seconds:.6} scan_bytes={scan_bytes} wall_s={seconds:.6} puts_per_s={:.3} ingest_per_s={:.3} trimmed={trimmed} trim_calls={trim_calls} trim_s={trim_seconds:.6} trim_max_s={trim_max_seconds:.6} settle_trim_s={settle_trim_seconds:.6}", requests as f64 / put_seconds, requests as f64 / seconds);
    for round in [0, rounds / 2, rounds - 1] {
        for i in [0, 1, batch / 2, batch - 1] {
            let id = i * rounds + round;
            let mut buffer = [0; 192];
            let k = measure_key(id, replay, &mut buffer);
            let updated = round + 1 < rounds && i % 100 == 0;
            let tombstone = updated && i % 1000 == 0;
            let StagedGet::Runs { probes, .. } = e.probe_get(k) else { panic!("expected settled run lookup"); };
            let entry = vals_rt::block_on(resolve_run_probe(k, &probes, e.device()), || {}).unwrap().unwrap();
            assert_eq!(entry.tombstone, tombstone);
            assert_eq!(entry.version, (round + 1 + u64::from(updated)) as u128);
            let value = e.get(k).unwrap();
            assert_eq!(value.as_deref(), Some(measure_value(id, replay, updated)));
        }
    }
    drop(e);
    allocator_snapshot("engine-dropped");
    std::fs::remove_file(path).unwrap();
}
