use super::*;

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};

/// One node's shards on one device, reduced to the engine commit path (task 0546).
///
/// `threads` writers, each owning an engine on its own device range (SPDK: its
/// own qpair via `SpdkController::open_shard`, like a node shard). Each loops:
/// put `batch` records (48 B keys, `vsize` B values), then commit: the engine's
/// `sync_prepare` (value-log drain and partial tail) followed by a device cache
/// flush shared through one flush domain, as the server's group commit does (one
/// flush in flight; a waiter is covered by any flush claimed after its prepare).
/// `VALS_BENCH_BG_MBPS=<read>,<write>` adds a background thread per direction
/// on its own qpair and device range doing paced 128 KiB reads/writes, standing
/// in for the node's merge, flush, cover-scan and verify traffic. Writers cycle
/// over `VALS_BENCH_KEYSPACE` keys each (default 1M), so the memtable stays
/// bounded while the value log keeps appending; a per-10 s table shows when the
/// drive reaches steady state.
pub(super) fn workload_commitwrite(
    backend: &str,
    path: &str,
    n: usize,
    vsize: usize,
    batch: usize,
    threads: usize,
) {
    let threads = threads.max(1);
    let per_thread = n / threads;
    let keyspace: usize = std::env::var("VALS_BENCH_KEYSPACE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let blocks = device_blocks(per_thread, vsize + 48);
    let domain = Arc::new(FlushDomain::default());
    #[cfg(feature = "spdk")]
    let controller = (backend == "spdk").then(|| {
        vals_spdk::spdk::SpdkController::attach(&vals_spdk::spdk::SpdkConfig {
            pci_addr: path.to_string(),
            nsid: 1,
        })
        .expect("attach spdk controller")
    });
    // One extra device range for the background load.
    #[cfg(feature = "spdk")]
    let shard_blocks = controller
        .as_ref()
        .map(|c| c.num_blocks() / (threads as u64 + 1));
    let bg: Vec<f64> = std::env::var("VALS_BENCH_BG_MBPS")
        .ok()
        .map(|v| v.split(',').filter_map(|x| x.parse().ok()).collect())
        .unwrap_or_default();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    #[cfg(feature = "spdk")]
    let bg_handles: Vec<_> = match (&controller, shard_blocks) {
        (Some(c), Some(per)) => bg
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, mbps)| *mbps > 0.0)
            .map(|(dir, mbps)| {
                let c = c.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let dev = c
                        .open_shard(threads as u64 * per, per)
                        .expect("open background shard");
                    background_io(&dev, dir == 1, mbps, per, &stop)
                })
            })
            .collect(),
        _ => Vec::new(),
    };
    if !bg.is_empty() {
        println!("  background read/write MB/s target: {bg:?}");
    }
    println!("  threads {threads}, {per_thread} records/thread, batch {batch}, value {vsize} B");
    let stats0 = vals_spdk::spdk_io_stats();
    let start = Instant::now();
    let handles: Vec<_> = (0..threads)
        .map(|t| {
            let domain = domain.clone();
            let backend = backend.to_string();
            let path = path.to_string();
            #[cfg(feature = "spdk")]
            let shard = controller.clone().map(|c| (c, shard_blocks.unwrap()));
            std::thread::spawn(move || {
                let dev: Box<dyn BlockDevice> = match backend.as_str() {
                    "mem" => Box::new(MemBlockDevice::new(BLOCK_SIZE, blocks)),
                    "file" => Box::new(
                        FileBlockDevice::create(format!("{path}.{t}"), BLOCK_SIZE, blocks)
                            .expect("create file"),
                    ),
                    #[cfg(feature = "spdk")]
                    "spdk" => {
                        let (c, per) = shard.expect("spdk controller");
                        Box::new(c.open_shard(t as u64 * per, per).expect("open shard"))
                    }
                    other => panic!("commitwrite: unsupported backend {other}"),
                };
                let mut e = Engine::create(dev, bench_config(Ordering::Unsorted)).expect("engine");
                run_writer(
                    &mut e, &domain, t, per_thread, vsize, batch, keyspace, start,
                )
            })
        })
        .collect();
    // Device counters every 10 s while the writers run.
    let mut samples = vec![(0.0, stats0.api_write_ops, stats0.api_write_bytes)];
    while !handles.iter().all(|h| h.is_finished()) {
        std::thread::sleep(Duration::from_millis(250));
        let t = start.elapsed().as_secs_f64();
        if t - samples.last().unwrap().0 >= 10.0 {
            let st = vals_spdk::spdk_io_stats();
            samples.push((t, st.api_write_ops, st.api_write_bytes));
        }
    }
    let mut prepare = Vec::new();
    let mut flush = Vec::new();
    let mut apply = Vec::new();
    let mut commits = Vec::new();
    for h in handles {
        let w = h.join().expect("writer thread");
        prepare.extend(w.prepare_ns.iter().copied());
        flush.extend(w.flush_ns.iter().copied());
        apply.extend(w.apply_ns.iter().copied());
        commits.extend(w.at_ms.iter().zip(&w.prepare_ns).map(|(t, p)| (*t, *p)));
    }
    println!("  window(s)  records/s  dev MB/s  dev ops/s  prepare p50/p99 ms");
    for pair in samples.windows(2) {
        let (t0, o0, b0) = pair[0];
        let (t1, o1, b1) = pair[1];
        let mut p: Vec<u64> = commits
            .iter()
            .filter(|(t, _)| (*t as f64 / 1e3) >= t0 && (*t as f64 / 1e3) < t1)
            .map(|(_, p)| *p)
            .collect();
        p.sort_unstable();
        let q = |f: f64| {
            p.get(((p.len() as f64 * f) as usize).min(p.len().saturating_sub(1)))
                .copied()
                .unwrap_or(0) as f64
                / 1e6
        };
        println!(
            "  {:>4.0}-{:<4.0} {:>10.0} {:>9.1} {:>10.0}  {:.3}/{:.3}",
            t0,
            t1,
            p.len() as f64 * batch as f64 / (t1 - t0),
            (b1 - b0) as f64 / (t1 - t0) / 1e6,
            (o1 - o0) as f64 / (t1 - t0),
            q(0.5),
            q(0.99)
        );
    }
    let secs = start.elapsed().as_secs_f64();
    let stats1 = vals_spdk::spdk_io_stats();
    stop.store(true, AtomicOrdering::Relaxed);
    #[cfg(feature = "spdk")]
    for h in bg_handles {
        let (reads, writes) = h.join().expect("background thread");
        println!(
            "  background   : read {:.1} MB/s, write {:.1} MB/s",
            reads as f64 / secs / 1e6,
            writes as f64 / secs / 1e6
        );
    }
    let records = per_thread * threads;
    let commits = prepare.len();
    println!("--- commitwrite ---");
    println!("  records/s    : {:.0}", records as f64 / secs);
    println!("  commits/s    : {:.0}", commits as f64 / secs);
    println!(
        "  value MB/s   : {:.1}",
        (records * vsize) as f64 / secs / 1e6
    );
    let ops = stats1.api_write_ops - stats0.api_write_ops;
    let bytes = stats1.api_write_bytes - stats0.api_write_bytes;
    if ops > 0 {
        println!(
            "  device writes: {:.0}/s, {:.1} MB/s, mean {:.0} B",
            ops as f64 / secs,
            bytes as f64 / secs / 1e6,
            bytes as f64 / ops as f64
        );
    }
    println!(
        "  device flushes: {}",
        domain.completed.load(AtomicOrdering::Relaxed)
    );
    for (name, mut v) in [
        ("apply", apply),
        ("prepare", prepare),
        ("flush wait", flush),
    ] {
        v.sort_unstable();
        let pct = |p: f64| {
            v.get(((v.len() as f64 * p) as usize).min(v.len().saturating_sub(1)))
                .copied()
                .unwrap_or(0)
        };
        println!(
            "  {name:<12} : p50 {:.3} ms  p90 {:.3} ms  p99 {:.3} ms",
            pct(0.5) as f64 / 1e6,
            pct(0.9) as f64 / 1e6,
            pct(0.99) as f64 / 1e6
        );
    }
    println!("  seconds      : {secs:.1}");
}

/// Paced 128 KiB sequential reads (or writes) over `blocks`, at `mbps`, until `stop`.
#[cfg(feature = "spdk")]
fn background_io(
    dev: &dyn BlockDevice,
    write: bool,
    mbps: f64,
    blocks: u64,
    stop: &std::sync::atomic::AtomicBool,
) -> (u64, u64) {
    const CHUNK: usize = 128 * 1024;
    let per_chunk = (CHUNK / BLOCK_SIZE) as u64;
    let mut buf = make_value(CHUNK, 0x5A);
    let start = Instant::now();
    let (mut done, mut lba) = (0u64, 0u64);
    while !stop.load(AtomicOrdering::Relaxed) {
        let due = start.elapsed().as_secs_f64() * mbps * 1e6;
        if (done as f64) >= due {
            std::thread::sleep(Duration::from_micros(200));
            continue;
        }
        if write {
            dev.write_at(lba, &buf).expect("background write");
        } else {
            dev.read_at(lba, &mut buf).expect("background read");
        }
        done += CHUNK as u64;
        lba = (lba + per_chunk) % (blocks - per_chunk);
    }
    if write {
        (0, done)
    } else {
        (done, 0)
    }
}

#[derive(Default)]
struct FlushDomain {
    claimed: AtomicU64,
    completed: AtomicU64,
    lock: Mutex<()>,
}

struct WriterTimes {
    at_ms: Vec<u64>,
    apply_ns: Vec<u64>,
    prepare_ns: Vec<u64>,
    flush_ns: Vec<u64>,
}

fn run_writer(
    e: &mut Engine<Box<dyn BlockDevice>>,
    domain: &FlushDomain,
    t: usize,
    n: usize,
    vsize: usize,
    batch: usize,
    keyspace: usize,
    start: Instant,
) -> WriterTimes {
    let val = make_value(vsize, t as u64);
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (t as u64 + 1).wrapping_mul(0x1000_0001B));
    let mut out = WriterTimes {
        at_ms: Vec::new(),
        apply_ns: Vec::new(),
        prepare_ns: Vec::new(),
        flush_ns: Vec::new(),
    };
    let ids: Vec<u64> = (0..keyspace.max(1)).map(|_| rng.next()).collect();
    let mut done = 0;
    while done < n {
        let k = batch.min(n - done);
        let t0 = Instant::now();
        for _ in 0..k {
            // 48 B keys spread like hashed ids, so the memtable sees realistic key order.
            let slot = done % ids.len();
            let key = format!("t{t:02}/{:016x}/{slot:027}", ids[slot]);
            e.put(key.as_bytes(), &val, done as u128 + 1).expect("put");
            done += 1;
        }
        let t1 = Instant::now();
        e.sync_prepare().expect("sync_prepare");
        let t2 = Instant::now();
        let need = domain.claimed.load(AtomicOrdering::SeqCst) + 1;
        while domain.completed.load(AtomicOrdering::SeqCst) < need {
            if let Ok(_g) = domain.lock.try_lock() {
                if domain.completed.load(AtomicOrdering::SeqCst) >= need {
                    break;
                }
                let number = domain.claimed.fetch_add(1, AtomicOrdering::SeqCst) + 1;
                e.device().flush().expect("device flush");
                domain.completed.fetch_max(number, AtomicOrdering::SeqCst);
            } else {
                std::hint::spin_loop();
            }
        }
        let t3 = Instant::now();
        out.at_ms.push((t3 - start).as_millis() as u64);
        out.apply_ns.push((t1 - t0).as_nanos() as u64);
        out.prepare_ns.push((t2 - t1).as_nanos() as u64);
        out.flush_ns.push((t3 - t2).as_nanos() as u64);
    }
    out
}
