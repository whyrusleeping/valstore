use super::*;

pub(super) fn workload_write(e: &mut Engine<Box<dyn BlockDevice>>, n: usize, vsize: usize) {
    let keys: Vec<String> = (0..n).map(|i| format!("key-{i:012}")).collect();
    let val = make_value(vsize, 0);
    let mut lat = Vec::with_capacity(n);
    let start = Instant::now();
    for k in &keys {
        let t = Instant::now();
        e.put(k.as_bytes(), &val, 0).unwrap();
        lat.push(t.elapsed().as_nanos() as u64);
    }
    let sync_t = Instant::now();
    e.sync().unwrap();
    let sync_secs = sync_t.elapsed().as_secs_f64();
    let total = start.elapsed().as_secs_f64();
    report("write (append)", lat, total, (n * vsize) as u64);
    println!("  final sync   : {sync_secs:.3} s");

    assert_eq!(
        e.get(keys[n / 2].as_bytes()).unwrap().as_deref(),
        Some(val.as_slice()),
        "read-back mismatch"
    );
    println!("  correctness  : ok ({} live keys)", e.len());
}

pub(super) fn workload_read(e: &mut Engine<Box<dyn BlockDevice>>, n: usize, vsize: usize) {
    let keys: Vec<String> = (0..n).map(|i| format!("key-{i:012}")).collect();
    let val = make_value(vsize, 0);
    for k in &keys {
        e.put(k.as_bytes(), &val, 0).unwrap();
    }
    e.sync().unwrap();

    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    let idxs: Vec<usize> = (0..n).map(|_| rng.below(n as u64) as usize).collect();
    let mut lat = Vec::with_capacity(n);
    let start = Instant::now();
    for &i in &idxs {
        let t = Instant::now();
        let got = e.get(keys[i].as_bytes()).unwrap();
        lat.push(t.elapsed().as_nanos() as u64);
        debug_assert!(got.is_some());
    }
    report(
        "read (random)",
        lat,
        start.elapsed().as_secs_f64(),
        (n * vsize) as u64,
    );
}

pub(super) fn workload_batchread(e: &mut Engine<Box<dyn BlockDevice>>, n: usize, vsize: usize, batch: usize) {
    let all_keys: Vec<String> = (0..n).map(|i| format!("key-{i:012}")).collect();
    let val = make_value(vsize, 0);
    for k in &all_keys {
        e.put(k.as_bytes(), &val, 0).unwrap();
    }
    e.sync().unwrap();

    let mut rng = Rng(0x2545_F491_4F6C_DD1D);
    let idxs: Vec<usize> = (0..n).map(|_| rng.below(n as u64) as usize).collect();
    let mut lat = Vec::with_capacity(n);
    let start = Instant::now();
    let mut done = 0;
    while done < n {
        let this = batch.min(n - done);
        let refs: Vec<&[u8]> = idxs[done..done + this]
            .iter()
            .map(|&i| all_keys[i].as_bytes())
            .collect();
        let t = Instant::now();
        let got = e.multi_get(&refs).unwrap();
        let per_key = t.elapsed().as_nanos() as u64 / this as u64;
        for _ in 0..this {
            lat.push(per_key);
        }
        debug_assert!(got.iter().all(|g| g.is_some()));
        done += this;
    }
    report(
        &format!("batchread (batch={batch})"),
        lat,
        start.elapsed().as_secs_f64(),
        (n * vsize) as u64,
    );
}

pub(super) fn workload_mixed(e: &mut Engine<Box<dyn BlockDevice>>, n: usize, vsize: usize) {
    let universe = (n / 2).max(1);
    for i in 0..universe {
        let key = format!("key-{i:012}");
        e.put(key.as_bytes(), &make_value(vsize, i as u64), 0)
            .unwrap();
    }
    e.sync().unwrap();

    let mut rng = Rng(0xD1B5_4A32_D192_ED03);
    let mut lat = Vec::with_capacity(n);
    let (mut reads, mut writes) = (0u64, 0u64);
    let start = Instant::now();
    for _ in 0..n {
        let i = rng.below(universe as u64);
        let key = format!("key-{i:012}");
        let t = Instant::now();
        if rng.next() & 1 == 0 {
            e.get(key.as_bytes()).unwrap();
            reads += 1;
        } else {
            e.put(key.as_bytes(), &make_value(vsize, i), 0).unwrap();
            writes += 1;
        }
        lat.push(t.elapsed().as_nanos() as u64);
    }
    e.sync().unwrap();
    report("mixed (50/50)", lat, start.elapsed().as_secs_f64(), 0);
    println!("  reads/writes : {reads}/{writes}");
}

pub(super) fn workload_recover(backend: &str, path: &str, n: usize, vsize: usize) {
    if backend != "file" {
        eprintln!("recover workload requires the 'file' backend");
        std::process::exit(2);
    }
    let cfg = bench_config(Ordering::Unsorted);
    let blocks = device_blocks(n, vsize);
    {
        let dev = FileBlockDevice::create(path, BLOCK_SIZE, blocks).unwrap();
        let mut e = Engine::open(dev, cfg).unwrap();
        for i in 0..n {
            let key = format!("key-{i:012}");
            e.put(key.as_bytes(), &make_value(vsize, i as u64), 0)
                .unwrap();
        }
        e.sync().unwrap();
    }
    let dev = FileBlockDevice::open(path, BLOCK_SIZE).unwrap();
    let start = Instant::now();
    let e = Engine::open(dev, cfg).unwrap();
    let secs = start.elapsed().as_secs_f64();
    println!("--- recover ---");
    println!("  keys         : {}", e.len());
    println!("  replay time  : {secs:.3} s");
    println!("  replay rate  : {:.0} keys/s", n as f64 / secs);
    assert_eq!(e.len(), n, "recovered key count mismatch");
    let probe = format!("key-{:012}", n - 1);
    assert_eq!(
        e.get(probe.as_bytes()).unwrap().as_deref(),
        Some(make_value(vsize, (n - 1) as u64).as_slice())
    );
    println!("  correctness  : ok");
}

fn rss_now() -> u64 {
    malloc_trim();
    vals_cluster::observe::process_resident_bytes().unwrap_or(0)
}

fn memprof_key(i: usize, rng: &mut Rng) -> Vec<u8> {
    format!("app.bsky.feed.post/did:plc:{:020x}/{i:013x}", rng.next()).into_bytes()
}

pub(super) fn workload_memprof(backend: &str, path: &str, n: usize, vsize: usize) {
    use vals_cluster::VersionedStore;

    let blocks = device_blocks(n, vsize);
    let dev: Box<dyn BlockDevice> = match backend {
        "mem" => Box::new(MemBlockDevice::new(BLOCK_SIZE, blocks)),
        "file" => Box::new(FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("create file")),
        other => {
            eprintln!("memprof: backend '{other}' not supported (use mem | file)");
            std::process::exit(2);
        }
    };
    let engine = Engine::create_prewiped(dev, bench_config(Ordering::Sorted));
    let mut store = VersionedStore::new(engine, 1).expect("versioned store");

    let val = make_value(vsize, 0);
    let mut rng = Rng(0x1234_5678_9ABC_DEF0);
    let keys: Vec<Vec<u8>> = (0..n).map(|i| memprof_key(i, &mut rng)).collect();
    let avg_key = keys.iter().map(Vec::len).sum::<usize>() as f64 / n as f64;

    let rss_before = rss_now();
    let start = Instant::now();
    for k in &keys {
        store.put(k, &val).unwrap();
    }
    store.sync().unwrap();
    let secs = start.elapsed().as_secs_f64();
    let rss_after = rss_now();

    let live = store.engine().len().max(1);
    let delta = rss_after.saturating_sub(rss_before);
    println!("--- memprof ({backend}, sorted) ---");
    println!("  live keys        : {}", store.engine().len());
    println!("  avg key bytes    : {avg_key:.1}");
    println!("  value bytes      : {vsize}");
    println!(
        "  load wall        : {secs:.3} s ({:.0} keys/s)",
        n as f64 / secs
    );
    println!("  rss before       : {} MiB", rss_before >> 20);
    println!("  rss after        : {} MiB", rss_after >> 20);
    println!("  rss delta        : {} MiB", delta >> 20);
    println!("  bytes/key (delta): {:.1}", delta as f64 / live as f64);
}

pub(super) fn workload_scanlsm(
    backend: &str,
    path: &str,
    n: usize,
    vsize: usize,
    page: usize,
    nruns: usize,
    order: &str,
) {
    let blocks = device_blocks(n, vsize);
    let dev: Box<dyn BlockDevice> = match backend {
        "mem" => Box::new(MemBlockDevice::new(BLOCK_SIZE, blocks)),
        "file" => Box::new(FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("create file")),
        other => {
            eprintln!("scanlsm: backend '{other}' not supported (use mem | file)");
            std::process::exit(2);
        }
    };
    let mut e = Engine::create_prewiped(dev, bench_config(Ordering::Sorted));

    let mut rng = Rng(0x5EED_5EED_5EED_5EED);
    let mut keys: Vec<Vec<u8>> = (0..n).map(|i| memprof_key(i, &mut rng)).collect();
    match order {
        "sorted" => keys.sort_unstable(),
        _ => {
            for i in (1..keys.len()).rev() {
                let j = rng.below(i as u64 + 1) as usize;
                keys.swap(i, j);
            }
        }
    }
    let val = make_value(vsize, 0);
    let chunks = nruns + 1;
    let per_chunk = n.div_ceil(chunks);
    let w0 = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        e.put(k, &val, (i as u128) << 16).unwrap();
        if (i + 1) % per_chunk == 0 && (i + 1) / per_chunk <= nruns {
            e.flush(RunBuildOpts::default())
                .expect("flush")
                .expect("non-empty flush");
        }
    }
    e.sync().unwrap();
    println!(
        "  setup  : {} keys, {} runs + memtable({}) in {:.1}s",
        n,
        e.run_count(),
        e.len(),
        w0.elapsed().as_secs_f64()
    );

    const REPLY_BUDGET: usize = 8 * 1024 * 1024;
    let repeat: usize = std::env::var("VALS_SCAN_REPEAT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let cap = if page == 0 { usize::MAX } else { page };
    for _ in 0..repeat {
        let mut lo: Option<Vec<u8>> = None;
        let mut total = 0usize;
        let mut bytes = 0u64;
        let mut pages = 0usize;
        let s0 = Instant::now();
        loop {
            let (pg, more) = e
                .scan_bounded(lo.as_deref(), None, cap, REPLY_BUDGET)
                .expect("scan page");
            pages += 1;
            total += pg.len();
            bytes += pg
                .iter()
                .map(|(k, v)| (k.len() + v.len()) as u64)
                .sum::<u64>();
            if !more {
                break;
            }
            let mut next = pg.last().expect("truncated page is non-empty").0.clone();
            next.push(0);
            lo = Some(next);
        }
        let secs = s0.elapsed().as_secs_f64();
        assert_eq!(total, n, "scan returned every key exactly once");
        println!(
            "  scanlsm: {total} keys / {pages} pages (page cap {cap}) in {secs:.2}s = {:>9.0} keys/s, {:.0} MiB/s",
            total as f64 / secs,
            bytes as f64 / secs / (1 << 20) as f64
        );
    }
}

fn key_hash(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

const GEN2_SALT: u64 = 0x9E37_79B9_7F4A_7C15;

fn cocompact_gen2(key: &[u8]) -> bool {
    key_hash(key).is_multiple_of(4)
}

fn cocompact_expected(key: &[u8], vsize: usize) -> Vec<u8> {
    let seed = key_hash(key) ^ if cocompact_gen2(key) { GEN2_SALT } else { 0 };
    make_value(vsize, seed)
}

fn cocompact_blocks(n: usize, vsize: usize) -> u64 {
    let data = (n as u64) * (vsize as u64 + 96);
    (data * 8).max(256 << 20).div_ceil(BLOCK_SIZE as u64)
}

fn cocompact_scan<D: BlockDevice + AsyncRead + 'static>(
    e: &Engine<D>,
    n: usize,
    vsize: usize,
    cap: usize,
    budget: usize,
    qd: usize,
    label: &str,
) -> (f64, usize) {
    use vals_store::{StagedScanPage, StagedScanStep};
    let dev = e.device();
    let qd = qd.max(1);
    let mut lo: Option<Vec<u8>> = None;
    let mut total = 0usize;
    let mut bytes = 0u64;
    let mut mism = 0u64;
    let mut spans_total = 0usize;
    let s0 = Instant::now();
    loop {
        let done = match e
            .stage_scan_page(lo.as_deref(), None, cap, budget)
            .expect("stage_scan_page")
        {
            StagedScanPage::Done(d) => d,
            StagedScanPage::Runs(mut merge) => {
                vals_rt::block_on(
                    async {
                        loop {
                            match merge.step(dev).await? {
                                StagedScanStep::Done => break,
                                StagedScanStep::NeedMem { from } => {
                                    e.scan_supply_mem(&mut merge, &from)?;
                                }
                                StagedScanStep::NeedBlocks { cursor } => {
                                    e.scan_supply_blocks(&mut merge, cursor)?;
                                }
                            }
                        }
                        Ok::<(), vals_store::EngineError>(())
                    },
                    || {
                        dev.poll_device();
                    },
                )
                .expect("staged scan step");
                merge.finish()
            }
        };
        let (keys, plan, more) = e.plan_staged_scan(done).expect("plan_staged_scan");
        let nspans = plan.num_spans();
        spans_total += nspans;
        let mut recs: Vec<Option<vals_store::Record>> = Vec::new();
        recs.resize_with(keys.len(), || None);
        let mut i = 0;
        while i < nspans {
            let hi_w = (i + qd).min(nspans);
            let futs: Vec<_> = (i..hi_w).map(|j| plan.execute_span(j, dev)).collect();
            let results = vals_rt::block_on(vals_rt::join_all(futs), || {
                dev.poll_device();
            });
            for r in results {
                for (slot, rec) in r.expect("span read") {
                    recs[slot] = Some(rec);
                }
            }
            i = hi_w;
        }
        for ((k, _v), rec) in keys.iter().zip(recs) {
            let rec = rec.expect("every winner slot decoded");
            if rec.kind == vals_store::RecordKind::Put {
                if rec.value.as_slice() != cocompact_expected(k, vsize).as_slice() {
                    mism += 1;
                }
                bytes += (k.len() + rec.value.len()) as u64;
                total += 1;
            }
        }
        if !more {
            break;
        }
        let mut next = keys.last().expect("non-empty page").0.clone();
        next.push(0);
        lo = Some(next);
    }
    let secs = s0.elapsed().as_secs_f64();
    let kps = total as f64 / secs;
    println!(
        "  {label:<9}: {total} keys in {secs:.2}s = {kps:>9.0} keys/s, {:.0} MiB/s, {spans_total} device-reads (QD{qd}), {mism} mismatches",
        bytes as f64 / secs / (1 << 20) as f64
    );
    assert_eq!(total, n, "{label}: scan must return every key exactly once");
    assert_eq!(mism, 0, "{label}: value-verify mismatches (stale/corrupt values)");
    (kps, spans_total)
}

fn serve_reads<D: BlockDevice>(
    e: &Engine<D>,
    keys: &[Vec<u8>],
    max_dur: Duration,
    stop: Option<&std::sync::atomic::AtomicBool>,
) -> (f64, u64, u64) {
    use std::sync::atomic::Ordering as AtomicOrdering;
    let mut rng = Rng(0xA5A5_1234_5678_9ABC);
    let mut lat: Vec<u64> = Vec::new();
    let start = Instant::now();
    let mut ops = 0u64;
    'outer: loop {
        for _ in 0..256 {
            let k = &keys[rng.below(keys.len() as u64) as usize];
            let t = Instant::now();
            let _ = e.get(k).expect("serving get");
            lat.push(t.elapsed().as_nanos() as u64);
            ops += 1;
        }
        if let Some(s) = stop {
            if s.load(AtomicOrdering::SeqCst) {
                break 'outer;
            }
        }
        if start.elapsed() >= max_dur {
            break 'outer;
        }
    }
    let secs = start.elapsed().as_secs_f64().max(1e-9);
    lat.sort_unstable();
    (
        ops as f64 / secs,
        percentile(&lat, 50.0),
        percentile(&lat, 99.0),
    )
}

#[allow(clippy::too_many_arguments)]
fn cocompact_core<D: BlockDevice + AsyncRead + 'static>(
    backend: &str,
    mut e: Engine<D>,
    n: usize,
    vsize: usize,
    page: usize,
    nruns: usize,
    order: &str,
    reopen: impl FnOnce() -> Engine<D>,
) {
    use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
    use std::sync::Arc;
    const REPLY_BUDGET: usize = 8 * 1024 * 1024;
    const SCAN_QD: usize = 64;
    let cap = if page == 0 { usize::MAX } else { page };

    if order == "sorted" {
        let _ = reopen;
        let mut rng = Rng(0x5EED_C0C0_5EED_C0C0);
        let mut keys: Vec<Vec<u8>> = (0..n).map(|i| memprof_key(i, &mut rng)).collect();
        keys.sort_unstable();
        let w0 = Instant::now();
        for (i, k) in keys.iter().enumerate() {
            let v = cocompact_expected(k, vsize);
            e.put(k, &v, (i as u128) << 16).unwrap();
        }
        e.flush(RunBuildOpts::default())
            .expect("flush")
            .expect("non-empty flush");
        e.sync().unwrap();
        println!(
            "  age(sort): {n} keys key-ordered, {} run(s), used_segments={} in {:.1}s",
            e.run_count(),
            e.used_segments(),
            w0.elapsed().as_secs_f64()
        );
        let (rate, spans) = cocompact_scan(&e, n, vsize, cap, REPLY_BUDGET, SCAN_QD, "ctl-scan");
        println!(
            "COCOMPACT-CONTROL: OK (backend={backend} key-ordered-write full-scan = {rate:.0} keys/s, \
             {spans} device-reads, 0 mismatches) — the ceiling the post-co-compaction scan targets"
        );
        return;
    }

    let mut rng = Rng(0x5EED_C0C0_5EED_C0C0);
    let mut keys: Vec<Vec<u8>> = (0..n).map(|i| memprof_key(i, &mut rng)).collect();
    for i in (1..keys.len()).rev() {
        let j = rng.below(i as u64 + 1) as usize;
        keys.swap(i, j);
    }
    let chunks = nruns + 1;
    let per_chunk = n.div_ceil(chunks);
    let w0 = Instant::now();
    for (i, k) in keys.iter().enumerate() {
        let v = make_value(vsize, key_hash(k));
        e.put(k, &v, (i as u128) << 16).unwrap();
        if (i + 1) % per_chunk == 0 && (i + 1) / per_chunk <= nruns {
            e.flush(RunBuildOpts::default())
                .expect("flush")
                .expect("non-empty flush");
        }
    }
    let gen2_base: u128 = (n as u128) << 40;
    let mut overwritten = 0usize;
    for (i, k) in keys.iter().enumerate() {
        if cocompact_gen2(k) {
            let v = make_value(vsize, key_hash(k) ^ GEN2_SALT);
            e.put(k, &v, gen2_base + i as u128).unwrap();
            overwritten += 1;
        }
    }
    e.flush(RunBuildOpts::default())
        .expect("gen2 flush")
        .expect("non-empty gen2 flush");
    e.sync().unwrap();
    let used_before = e.used_segments();
    println!(
        "  age      : {n} keys ({overwritten} overwritten), {} runs, used_segments={used_before} in {:.1}s",
        e.run_count(),
        w0.elapsed().as_secs_f64()
    );

    let (pre, spans_pre) = cocompact_scan(&e, n, vsize, cap, REPLY_BUDGET, SCAN_QD, "pre-scan");

    let base = serve_reads(&e, &keys, Duration::from_secs(3), None);
    println!(
        "  serve base : {:>8.0} ops/s  p50={} us  p99={} us",
        base.0,
        base.1 / 1000,
        base.2 / 1000
    );

    let opts = RunBuildOpts {
        num_buckets: 4096,
        ..Default::default()
    };
    if std::env::var_os("VALS_VLOG_CO_COMPACT").is_none() {
        std::env::set_var("VALS_VLOG_CO_COMPACT", "1");
    }
    let plan = e
        .compact_plan(opts, None)
        .expect("compact_plan");
    let Some(plan) = plan else {
        eprintln!(
            "CHECK: compact_plan returned None — the tiered recurring selector found no \
             eligible fan-in. Raise nruns or n so enough similar-sized runs reach the \
             configured minimum fan-in and graduation gates."
        );
        std::process::exit(3);
    };
    if !plan.co_compacts() {
        eprintln!(
            "CHECK: selected a PLAIN merge — co-compaction did NOT engage. The tiered \
             selector may have chosen a non-bottommost suffix; co-compaction requires a \
             bottommost graduation merge, VALS_VLOG_CO_COMPACT enabled, and sufficient \
             vlog-output headroom (bump cocompact_blocks)."
        );
        std::process::exit(3);
    }

    let done = Arc::new(AtomicBool::new(false));
    let done_w = done.clone();
    let c0 = Instant::now();
    let worker = std::thread::spawn(move || {
        let write_dev = Arc::new(
            plan.serving_dev()
                .sibling()
                .expect("device sibling (handle-B) for off-core compaction"),
        );
        let res = vals_rt::block_on(vals_store::compact_and_write(plan, write_dev.clone()), || {
            write_dev.poll_device();
        });
        done_w.store(true, AtomicOrdering::SeqCst);
        res
    });
    let during = serve_reads(&e, &keys, Duration::from_secs(3600), Some(&done));
    let written = worker
        .join()
        .expect("compaction worker panicked")
        .expect("compact_and_write failed");
    let compact_secs = c0.elapsed().as_secs_f64();
    let stats = e.compact_commit(written).expect("compact_commit");
    e.sync().unwrap();
    let used_after = e.used_segments();
    println!(
        "  serve dur  : {:>8.0} ops/s  p50={} us  p99={} us",
        during.0,
        during.1 / 1000,
        during.2 / 1000
    );
    println!(
        "  compact  : {compact_secs:.2}s wall, {} runs merged, {} entries, {} run-segs freed; \
         used_segments {used_before} -> {used_after}",
        stats.runs_merged, stats.entries_written, stats.segments_freed
    );
    let dropped = used_after < used_before;

    let (post, spans_post) = cocompact_scan(&e, n, vsize, cap, REPLY_BUDGET, SCAN_QD, "post-scan");
    let ratio = if pre > 0.0 { post / pre } else { 0.0 };
    let span_reduction = if spans_post > 0 {
        spans_pre as f64 / spans_post as f64
    } else {
        0.0
    };
    println!("  ratio    : post/pre = {ratio:.2}x (pre {pre:.0} -> post {post:.0} keys/s)");
    println!(
        "  reads    : device-reads {spans_pre} -> {spans_post} = {span_reduction:.1}x fewer (coalescing win)"
    );

    drop(e);
    let e2 = reopen();
    let used_reopen = e2.used_segments();
    let persisted = used_reopen < used_before && used_reopen <= used_after;
    println!(
        "  reopen   : used_segments={used_reopen} (before {used_before}, after {used_after}) persisted={persisted}"
    );
    let _ = cocompact_scan(&e2, n, vsize, cap, REPLY_BUDGET, SCAN_QD, "rescan");

    let speedup = ratio.max(span_reduction);
    let speedup_ok = backend != "spdk" || speedup >= 5.0;
    if backend == "spdk" && !speedup_ok {
        eprintln!(
            "CHECK: spdk scan speedup {speedup:.2}x < 5x (rate {ratio:.2}x, device-reads {span_reduction:.1}x)"
        );
    }
    if !dropped {
        eprintln!("CHECK: used_segments did NOT drop ({used_before} -> {used_after}) — no space reclaimed");
    }
    if !persisted {
        eprintln!("CHECK: reclaim did NOT persist across reopen (used_segments={used_reopen}, was {used_after})");
    }
    if dropped && persisted && speedup_ok {
        let cleared = if ratio >= 5.0 && span_reduction >= 5.0 {
            "rate + device-reads"
        } else if ratio >= 5.0 {
            "wall-time rate"
        } else if span_reduction >= 5.0 {
            "device-reads"
        } else {
            "neither (informational; spdk-only bar)"
        };
        println!(
            "COCOMPACT: OK (backend={backend}) — scan speedup: wall-time rate {ratio:.2}x, \
             device-reads {span_reduction:.1}x fewer; >=5x cleared by {cleared}. \
             space reclaim: used_segments {used_before}->{used_after} (persisted across reboot)."
        );
    } else {
        eprintln!(
            "COCOMPACT: FAIL (dropped={dropped} persisted={persisted} speedup_ok={speedup_ok})"
        );
        std::process::exit(4);
    }
}

pub(super) fn workload_cocompact(
    backend: &str,
    path: &str,
    n: usize,
    vsize: usize,
    page: usize,
    nruns: usize,
    order: &str,
) {
    let cfg = bench_config(Ordering::Sorted);
    let blocks = cocompact_blocks(n, vsize);
    match backend {
        "mem" => {
            let dev = MemBlockDevice::new(BLOCK_SIZE, blocks);
            let reopen_dev = dev.sibling().expect("mem sibling for reopen");
            let e = Engine::create_prewiped(dev, cfg);
            cocompact_core(backend, e, n, vsize, page, nruns, order, move || {
                Engine::open(reopen_dev, cfg).expect("reopen mem engine")
            });
        }
        "file" => {
            let dev = FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("create file");
            let e = Engine::create_prewiped(dev, cfg);
            let path = path.to_string();
            cocompact_core(backend, e, n, vsize, page, nruns, order, move || {
                let d = FileBlockDevice::open(&path, BLOCK_SIZE).expect("reopen file");
                Engine::open(d, cfg).expect("reopen file engine")
            });
        }
        #[cfg(feature = "spdk")]
        "spdk" => {
            use vals_spdk::spdk::{SpdkConfig, SpdkController};
            let scfg = SpdkConfig {
                pci_addr: path.to_string(),
                nsid: 1,
            };
            let e = {
                let ctrlr = SpdkController::attach(&scfg).expect("attach spdk controller");
                let nblk = ctrlr.num_blocks();
                let dev = ctrlr.open_shard(0, nblk).expect("open spdk shard");
                Engine::create(dev, cfg).unwrap()
            };
            cocompact_core(backend, e, n, vsize, page, nruns, order, move || {
                let ctrlr = SpdkController::attach(&scfg).expect("reattach spdk controller");
                let nblk = ctrlr.num_blocks();
                let d = ctrlr.open_shard(0, nblk).expect("reopen spdk shard");
                Engine::open(d, cfg).expect("reopen spdk engine")
            });
        }
        other => {
            eprintln!(
                "cocompact: backend '{other}' not supported (use mem | file | spdk; \
                 spdk needs --features spdk + a PCIe addr as the path arg)"
            );
            std::process::exit(2);
        }
    }
}

