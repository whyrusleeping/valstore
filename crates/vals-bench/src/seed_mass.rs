use super::*;
use std::sync::{Arc, Barrier};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

fn rss() -> u64 {
    vals_cluster::observe::process_resident_bytes().unwrap_or(0)
}

pub(super) fn workload_seed_mass(path: &str, keys: usize, stores: usize, threads: usize) {
    assert!(keys > 0 && stores > 0 && threads > 0);
    let config = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let blocks = device_blocks(keys.saturating_mul(2), 8);
    let device = FileBlockDevice::create(path, BLOCK_SIZE, blocks).expect("mass seed image");
    let mut source = vals_cluster::VersionedStore::new(Engine::create_prewiped(device, config), 1).unwrap();
    for i in 0..keys {
        let key = format!("mass-seed-{i:016x}");
        source.apply_at(key.as_bytes(), Some(b"v"), vals_cluster::Version { hlc: (i as u64 + 1) << 16, node: 1 }, i as u64 + 1).unwrap();
    }
    source.flush().expect("mass seed flush");
    source.sync().expect("mass seed sync");
    drop(source);

    let ready = Arc::new(Barrier::new(threads + 1));
    let start = Arc::new(Barrier::new(threads + 1));
    let completed = Arc::new(Barrier::new(threads + 1));
    let release = Arc::new(Barrier::new(threads + 1));
    let stop = Arc::new(AtomicBool::new(false));
    let sample_stop = stop.clone();
    let sampler = std::thread::spawn(move || {
        let mut samples = Vec::new();
        while !sample_stop.load(AtomicOrdering::Relaxed) {
            samples.push(rss());
            std::thread::sleep(Duration::from_millis(5));
        }
        samples.push(rss());
        samples
    });
    let before = vals_cluster::observe::snapshot();
    let deadline_ms: u64 = std::env::var("VALS_SEED_MASS_DEADLINE_MS").ok()
        .and_then(|value| value.parse().ok()).unwrap_or(120_000);
    let begun = Arc::new(std::sync::OnceLock::<Instant>::new());
    let mut workers = Vec::new();
    for shard in 0..threads {
        let path = path.to_string();
        let ready = ready.clone();
        let start = start.clone();
        let completed = completed.clone();
        let release = release.clone();
        let begun = begun.clone();
        workers.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            for _ in (shard..stores).step_by(threads) {
                let device = FileBlockDevice::open(&path, BLOCK_SIZE).unwrap();
                local.push(vals_cluster::VersionedStore::new(Engine::open(device, config).unwrap(), 1).unwrap());
            }
            ready.wait();
            start.wait();
            let mut done = vec![false; local.len()];
            let mut finished = 0;
            while finished < local.len() && begun.get().unwrap().elapsed().as_millis() < deadline_ms as u128 {
                let mut advanced = false;
                for (i, store) in local.iter().enumerate() {
                    if done[i] { continue; }
                    if store.seed_step().unwrap() {
                        done[i] = true;
                        finished += 1;
                        advanced = true;
                    }
                }
                if !advanced { std::thread::sleep(Duration::from_millis(1)); }
            }
            completed.wait();
            release.wait();
            finished
        }));
    }
    ready.wait();
    let baseline = rss();
    let started = Instant::now();
    begun.set(started).unwrap();
    start.wait();
    completed.wait();
    let wall = started.elapsed();
    let settle_ms: u64 = std::env::var("VALS_SEED_MASS_SETTLE_MS").ok()
        .and_then(|value| value.parse().ok()).unwrap_or(0);
    std::thread::sleep(Duration::from_millis(settle_ms));
    let retained = rss();
    let trimmed = if std::env::var_os("VALS_SEED_MASS_TRIM").is_some() {
        let trim_started = Instant::now();
        let after = std::thread::spawn(|| { malloc_trim(); rss() }).join().unwrap();
        println!("SEED_MASS_TRIM before={retained} after={after} wall_ms={:.3}", trim_started.elapsed().as_secs_f64() * 1000.0);
        Some(after)
    } else { None };
    release.wait();
    let seeded: usize = workers.into_iter().map(|worker| worker.join().unwrap()).sum();
    stop.store(true, AtomicOrdering::Relaxed);
    let mut samples = sampler.join().unwrap();
    samples.sort_unstable();
    let peak = *samples.last().unwrap();
    let p99 = samples[(samples.len() * 99 / 100).min(samples.len() - 1)];
    let after = vals_cluster::observe::snapshot();
    println!("SEED_MASS keys_per_store={keys} stores={stores} threads={threads} seeded={seeded} baseline_rss={baseline} peak_rss={peak} p99_rss={p99} retained_rss={retained} trimmed_rss={} samples={} wall_s={:.3} starts={} sheds={} restarts={} fallbacks={} deferrals={}",
        trimmed.unwrap_or(retained),
        samples.len(), wall.as_secs_f64(), after.ae_seed_starts - before.ae_seed_starts,
        after.ae_seed_ram_sheds - before.ae_seed_ram_sheds,
        after.ae_seed_restarts - before.ae_seed_restarts,
        after.ae_seed_sync_fallbacks - before.ae_seed_sync_fallbacks,
        after.ae_seed_permit_deferrals - before.ae_seed_permit_deferrals);
    assert!(seeded == stores || wall.as_millis() >= deadline_ms as u128);
}
