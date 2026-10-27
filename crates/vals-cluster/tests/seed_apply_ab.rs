use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use vals_client::hlc::COUNTER_BITS;
use vals_cluster::replica::{ensure_seeded_cooperative_cell, VersionedStore};
use vals_cluster::Version;
use vals_rt::LocalExecutor;
use vals_spdk::{BlockDevice, MemBlockDevice};
use vals_store::{Config, Engine, Ordering};

const BUILD_RUNS: u64 = 6;
const KEYS_PER_BUILD_RUN: u64 = 100_000;
const PENDING_KEYS: u64 = 8_192;

struct CountingDevice {
    inner: MemBlockDevice,
    reads: Arc<AtomicU64>,
}

impl BlockDevice for CountingDevice {
    fn block_size(&self) -> usize {
        self.inner.block_size()
    }

    fn num_blocks(&self) -> u64 {
        self.inner.num_blocks()
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.reads.fetch_add(1, AtomicOrdering::Relaxed);
        self.inner.read_at(lba, buf)
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        self.inner.write_at(lba, buf)
    }

    fn flush(&self) -> vals_spdk::Result<()> {
        self.inner.flush()
    }
}

fn version(stamp: u64) -> Version {
    Version {
        hlc: stamp << COUNTER_BITS,
        node: 7,
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    sorted[(sorted.len() - 1) * pct / 100]
}

#[test]
#[ignore = "measurement: prints concurrent apply latency while production-budget seeding runs"]
fn seed_apply_latency_during_cooperative_seed() {
    let mem = MemBlockDevice::new(vals_spdk::DEFAULT_BLOCK_SIZE, 1 << 20);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut source = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
    for run in 0..BUILD_RUNS {
        for i in 0..KEYS_PER_BUILD_RUN {
            let key = format!("app.bsky.feed/{run:02}/{i:06}");
            source
                .apply(
                    key.as_bytes(),
                    Some(b"v"),
                    version(1_000 + run * KEYS_PER_BUILD_RUN + i),
                )
                .unwrap();
        }
        source.flush().unwrap();
    }
    assert_eq!(source.engine().run_count(), BUILD_RUNS as usize);
    source.compact_runs(0).unwrap();
    assert_eq!(source.engine().run_count(), 1);
    source.sync().unwrap();
    drop(source);

    let reads = Arc::new(AtomicU64::new(0));
    let dev = CountingDevice {
        inner: mem.share(),
        reads: reads.clone(),
    };
    let mut reopened = VersionedStore::open(dev, cfg, 7).unwrap();
    assert_eq!(reopened.engine().run_count(), 1);
    for i in 0..PENDING_KEYS {
        let key = format!("pending-{i:06}");
        reopened
            .apply(key.as_bytes(), Some(b"v"), version(700_000 + i))
            .unwrap();
    }
    let store = Rc::new(RefCell::new(reopened));
    let done = Rc::new(Cell::new(false));
    let seed_wall = Rc::new(Cell::new(Duration::ZERO));
    let apply_latencies = Rc::new(RefCell::new(Vec::<u64>::new()));
    let apply_gaps = Rc::new(RefCell::new(Vec::<u64>::new()));
    let sidecar_load_gaps = Rc::new(RefCell::new(Vec::<u64>::new()));

    let ex = LocalExecutor::new();
    {
        let store = store.clone();
        let done = done.clone();
        let seed_wall = seed_wall.clone();
        ex.spawn(async move {
            let started = Instant::now();
            ensure_seeded_cooperative_cell(&store).await.unwrap();
            seed_wall.set(started.elapsed());
            done.set(true);
        });
    }
    {
        let store = store.clone();
        let done = done.clone();
        let apply_latencies = apply_latencies.clone();
        let apply_gaps = apply_gaps.clone();
        let sidecar_load_gaps = sidecar_load_gaps.clone();
        let reads = reads.clone();
        ex.spawn(async move {
            let mut i = 0u64;
            while !done.get() {
                let started = Instant::now();
                let key = format!("live-{i:08}");
                store
                    .borrow_mut()
                    .apply(key.as_bytes(), Some(b"v"), version(1_000_000 + i))
                    .unwrap();
                let reads_before = reads.load(AtomicOrdering::Relaxed);
                let gap_started = Instant::now();
                vals_rt::yield_now().await;
                let gap_us = gap_started.elapsed().as_micros() as u64;
                if reads.load(AtomicOrdering::Relaxed) > reads_before {
                    sidecar_load_gaps.borrow_mut().push(gap_us);
                }
                apply_gaps.borrow_mut().push(gap_us);
                apply_latencies
                    .borrow_mut()
                    .push(started.elapsed().as_micros() as u64);
                i += 1;
            }
        });
    }
    ex.run(|| {});

    let mut latencies = apply_latencies.borrow().clone();
    let gaps = apply_gaps.borrow();
    assert!(!latencies.is_empty());
    latencies.sort_unstable();
    let max_gap = gaps.iter().copied().max().unwrap();
    let max_sidecar_load_gap = sidecar_load_gaps.borrow().iter().copied().max().unwrap();
    eprintln!(
        "seed apply A/B: run_keys={} runs=1 build_runs={BUILD_RUNS} pending_keys={PENDING_KEYS} applies={} \
         apply_p50_us={} apply_p99_us={} apply_max_us={} seed_wall={:?} max_gap_us={max_gap} \
         max_sidecar_load_gap_us={max_sidecar_load_gap}",
        BUILD_RUNS * KEYS_PER_BUILD_RUN,
        latencies.len(),
        percentile(&latencies, 50),
        percentile(&latencies, 99),
        latencies.last().unwrap(),
        seed_wall.get(),
    );
}
