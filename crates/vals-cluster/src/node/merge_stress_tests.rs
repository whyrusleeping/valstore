use super::*;
use crate::hlc::Version;
use crate::node_settings::NodeSettings;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Instant;
use vals_spdk::{MemBlockDevice, WriteHandle, WriteState};
use vals_store::{Config, Engine, MergeKind, Ordering as SOrdering};

const RBS: usize = 512;

struct Disk {
    data: std::sync::Mutex<Vec<u8>>,
}

type PendingWrite = (u64, Vec<u8>, Arc<WriteState>);

struct RegionDev {
    disk: Arc<Disk>,
    base: u64,
    blocks: u64,
    deferred: bool,
    pending: std::sync::Mutex<Vec<PendingWrite>>,
    rng: AtomicU64,
}

impl RegionDev {
    fn new(disk: &Arc<Disk>, base: u64, blocks: u64, deferred: bool, seed: u64) -> Self {
        RegionDev {
            disk: disk.clone(),
            base,
            blocks,
            deferred,
            pending: std::sync::Mutex::new(Vec::new()),
            rng: AtomicU64::new(seed | 1),
        }
    }

    fn range(&self, lba: u64, len: usize) -> std::ops::Range<usize> {
        assert!(len % RBS == 0 && lba + (len / RBS) as u64 <= self.blocks, "out of region: lba {lba} len {len}");
        let off = ((self.base + lba) as usize) * RBS;
        off..off + len
    }

    fn next(&self) -> u64 {
        let mut s = self.rng.load(AtomicOrdering::Relaxed);
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        self.rng.store(s, AtomicOrdering::Relaxed);
        s
    }

    fn complete_some(&self) -> i32 {
        let mut pending = self.pending.lock().unwrap();
        let mut done = 0;
        let mut i = 0;
        while i < pending.len() {
            if self.next() % 3 == 0 {
                let j = (self.next() as usize) % pending.len();
                let (lba, buf, state) = pending.swap_remove(j);
                let range = self.range(lba, buf.len());
                self.disk.data.lock().unwrap()[range].copy_from_slice(&buf);
                state.complete(true);
                done += 1;
            } else {
                i += 1;
            }
        }
        done
    }
}

impl BlockDevice for RegionDev {
    fn block_size(&self) -> usize {
        RBS
    }

    fn num_blocks(&self) -> u64 {
        self.blocks
    }

    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        let range = self.range(lba, buf.len());
        buf.copy_from_slice(&self.disk.data.lock().unwrap()[range]);
        Ok(())
    }

    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        let range = self.range(lba, buf.len());
        self.disk.data.lock().unwrap()[range].copy_from_slice(buf);
        Ok(())
    }

    fn flush(&self) -> vals_spdk::Result<()> {
        Ok(())
    }

    fn deallocate(&self, ranges: &[(u64, u64)]) -> vals_spdk::Result<bool> {
        let mut data = self.disk.data.lock().unwrap();
        for &(lba, blocks) in ranges {
            let range = self.range(lba, blocks as usize * RBS);
            data[range].fill(0xdd);
        }
        Ok(true)
    }

    fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
        if !self.deferred {
            return WriteHandle::ready(self.write_at(lba, buf));
        }
        self.range(lba, buf.len());
        let state = WriteState::pending();
        self.pending.lock().unwrap().push((lba, buf.to_vec(), state.clone()));
        WriteHandle::pending(state)
    }

    fn poll_writes(&self) -> i32 {
        self.complete_some()
    }

    fn poll_device(&self) -> i32 {
        self.complete_some()
    }

    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(RegionDev::new(&self.disk, self.base, self.blocks, false, self.next()))
    }
}

impl AsyncRead for RegionDev {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        self.read_at(lba, buf)
    }
}

fn run_loops<D: BlockDevice + AsyncRead + 'static, F>(shared: &Rc<ServerShared<D>>, orchestrate: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    let (handle, mut join) = crate::maint::spawn::<D>();
    let ex = LocalExecutor::new();
    ex.spawn(orchestrate);
    ex.spawn(snapshot_loop(shared.clone(), Duration::from_millis(2), handle.submitter()));
    ex.spawn(maint_loop(shared.clone(), handle));
    ex.run({
        let shared = shared.clone();
        move || server_poll(&shared)
    });
    join.join();
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (self.0 >> 11) % n.max(1)
    }
}

#[derive(Debug, Default)]
struct NodeStressStats {
    applies: u64,
    rejected: u64,
    audits: u64,
    audited_entries: u64,
}

type Model = BTreeMap<u32, Option<Vec<u8>>>;

fn node_stress(seed: u64, zones: usize, ops: u64, deferred: bool) -> NodeStressStats {
    const REGION: u64 = 6_144;
    const KEYS: u64 = 1_500;
    let disk = Arc::new(Disk { data: std::sync::Mutex::new(vec![0u8; zones * REGION as usize * RBS]) });
    let store_for = |z: usize| {
        let cfg = Config {
            seg_blocks: 16,
            reserved_blocks: 0,
            ordering: if z % 2 == 0 { SOrdering::Sorted } else { SOrdering::Unsorted },
        };
        let dev = RegionDev::new(&disk, z as u64 * REGION, REGION, deferred, seed ^ (z as u64 + 1) * 0x9e37_79b9);
        let mut store = VersionedStore::new(Engine::create_prewiped(dev, cfg), 1).unwrap();
        store.engine_mut().set_maint_reserve_enabled(true);
        store.engine_mut().set_device_dealloc(true);
        store
    };
    let settings = NodeSettings {
        flush_tail_epochs: 0,
        device_dealloc_segments_per_tick: 16,
        ..NodeSettings::default()
    };
    let shared = Rc::new(
        ServerShared::single_zone_on(1, store_for(0), Ring::from_members(1, 64, [1])).with_settings(settings),
    );
    let mut stores = vec![shared.zone_store(DEFAULT_ZONE).unwrap()];
    for z in 1..zones {
        stores.push(shared.insert_zone_for_test(z as ZoneId, store_for(z)));
    }
    let models: Rc<RefCell<Vec<Model>>> = Rc::new(RefCell::new(vec![Model::new(); zones]));
    let stats = Rc::new(RefCell::new(NodeStressStats::default()));
    let orchestrator = shared.clone();
    let (orch_stores, orch_models, orch_stats) = (stores.clone(), models.clone(), stats.clone());
    run_loops(&shared, async move {
        let mut rng = Rng(seed.wrapping_mul(0x2545_F491_4F6C_DD1D) | 1);
        let mut hlc = 0u64;
        let mut until_yield = 0u64;
        let mut burst = (0usize, 0u64);
        for op in 1..=ops {
            if burst.1 == 0 && rng.below(40) == 0 {
                burst = (rng.below(zones as u64) as usize, 100 + rng.below(500));
            }
            let z = if burst.1 > 0 {
                burst.1 -= 1;
                burst.0
            } else {
                rng.below(zones as u64) as usize
            };
            let key = rng.below(KEYS) as u32;
            hlc += 1;
            let tomb = rng.below(10) == 0 && orch_models.borrow()[z].contains_key(&key);
            let value: Option<Vec<u8>> = (!tomb).then(|| {
                let tag = format!("z{z}/k{key}/h{hlc}/");
                tag.bytes().cycle().take(24 + rng.below(360) as usize).collect()
            });
            let applied = orch_stores[z].borrow_mut().apply(
                format!("key{key:06}").as_bytes(),
                value.as_deref(),
                Version { hlc, node: 1 },
            );
            match applied {
                Ok(_) => {
                    orch_models.borrow_mut()[z].insert(key, value);
                    orch_stats.borrow_mut().applies += 1;
                }
                Err(_) => orch_stats.borrow_mut().rejected += 1,
            }
            if until_yield == 0 && burst.1 == 0 {
                until_yield = 1 + rng.below(24);
                vals_rt::yield_now().await;
            }
            until_yield = until_yield.saturating_sub(1);
            if op % 2_000 == 0 {
                for (z, store) in orch_stores.iter().enumerate() {
                    let entries = store
                        .borrow()
                        .engine()
                        .storage_audit_for_test()
                        .unwrap_or_else(|e| panic!("seed {seed} zone {z} op {op}: {e}"));
                    let mut s = orch_stats.borrow_mut();
                    s.audits += 1;
                    s.audited_entries += entries;
                }
            }
        }
        let deadline = Instant::now() + Duration::from_secs(60);
        while orch_stores.iter().any(|s| {
            let s = s.borrow();
            s.compact_in_flight() || s.flush_in_flight() || s.memtable_bytes() > 0 || s.recurring_merge_selectable()
        }) {
            assert!(Instant::now() < deadline, "seed {seed}: maintenance never drained");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    for (z, store) in stores.iter().enumerate() {
        let store = store.borrow();
        store
            .engine()
            .storage_audit_for_test()
            .unwrap_or_else(|e| panic!("seed {seed} zone {z} final: {e}"));
        for (key, value) in &models.borrow()[z] {
            assert_eq!(
                store.get(format!("key{key:06}").as_bytes()).unwrap().as_deref(),
                value.as_deref(),
                "seed {seed} zone {z} key {key}"
            );
        }
        for key in (0..KEYS as u32).filter(|k| !models.borrow()[z].contains_key(k)).take(50) {
            assert_eq!(store.get(format!("key{key:06}").as_bytes()).unwrap(), None, "seed {seed} zone {z} key {key}");
        }
    }
    Rc::try_unwrap(stats).unwrap().into_inner()
}

#[test]
fn node_maintenance_lanes_never_corrupt_in_place_forced_windows() {
    if crate::testutil::isolated_env_test(&[
        ("VALS_MERGE_FORCE_RUNS", "3"),
        ("VALS_MERGE_TIER_MIN_K", "4"),
        ("VALS_MERGE_INPUT_CAP_BYTES", "131072"),
        ("VALS_RUN_BUILD_VERIFY", "1"),
        ("VALS_MAINT_DUTY_PCT", "0"),
        ("VALS_VLOG_CO_COMPACT", "0"),
    ]) {
        return;
    }
    let counters = crate::observe::counters();
    let errors_before = counters.snapshot_errors.get();
    let merges_before = counters.run_merges.get();
    let kinds_before = crate::observe::merge_kind_metrics();
    let inner_before = vals_store::inner_window_merges_total();
    let (verify_bytes_before, verify_failures_before) = vals_store::run::run_build_verify_totals();
    for (seed, deferred) in [(11u64, true), (12, false), (13, true)] {
        let stats = node_stress(seed, 6, 20_000, deferred);
        println!("seed {seed} deferred={deferred}: {stats:?}");
        assert!(stats.audits > 0 && stats.applies > 0);
    }
    let (verify_bytes, verify_failures) = vals_store::run::run_build_verify_totals();
    let kinds: Vec<_> = crate::observe::merge_kind_metrics()
        .into_iter()
        .zip(kinds_before)
        .map(|((kind, merges, bytes), (_, m0, b0))| (kind, merges - m0, bytes - b0))
        .collect();
    let inner = vals_store::inner_window_merges_total() - inner_before;
    println!(
        "merges={} by kind={kinds:?} inner_windows={inner} verify_bytes={} verify_failures={} snapshot_errors={}",
        counters.run_merges.get() - merges_before,
        verify_bytes - verify_bytes_before,
        verify_failures - verify_failures_before,
        counters.snapshot_errors.get() - errors_before,
    );
    assert_eq!(verify_failures, verify_failures_before, "build verify failures");
    assert!(verify_bytes > verify_bytes_before);
    assert_eq!(counters.snapshot_errors.get(), errors_before, "off-core flush or merge failures");
    assert!(inner > 0, "no inner forced window was exercised");
    assert!(kinds.iter().any(|(kind, merges, _)| *kind == MergeKind::Forced.label() && *merges > 0), "{kinds:?}");
}

#[test]
fn recurring_and_forced_merges_reserve_normally_while_reclaim_overdraws() {
    let _rss = crate::ram::override_for_test(0, u64::MAX);
    let _broker = crate::ram_broker::override_for_test(Some(u64::MAX / 2), 1);
    for kind in [MergeKind::Recurring, MergeKind::Forced] {
        assert!(reserve_merge(kind, 7, 4096).is_none(), "{kind:?} must defer when the broker says no");
    }
    for kind in [MergeKind::Pressure, MergeKind::ShadowFold, MergeKind::VlogGc, MergeKind::LoneGc, MergeKind::Forget] {
        let reservation = reserve_merge(kind, 7, 4096).unwrap_or_else(|| panic!("{kind:?} must overdraw"));
        assert_eq!(reservation.bytes(), 4096);
    }
}

#[test]
fn many_run_engine_defers_recurring_merges_under_ram_pressure_until_capacity_returns() {
    let _rss = crate::ram::override_for_test(0, u64::MAX);
    let broker = crate::ram_broker::override_for_test(Some(u64::MAX / 2), 1);
    const CFG: Config = Config { seg_blocks: 64, reserved_blocks: 0, ordering: SOrdering::Sorted };
    let mut store = VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(512, 65_536), CFG), 1).unwrap();
    for round in 0..12u64 {
        for i in 0..40u64 {
            store
                .apply(format!("k{i:03}").as_bytes(), Some(format!("r{round}-{i}").as_bytes()), Version { hlc: round * 100 + i + 1, node: 1 })
                .unwrap();
        }
        assert!(store.flush().unwrap().is_some());
    }
    assert!(store.run_count() >= NodeSettings::default().run_compact_threshold);
    assert!(store.recurring_merge_selectable());
    let shared = Rc::new(
        ServerShared::single_zone_on(1, store, Ring::from_members(1, 64, [1]))
            .with_settings(NodeSettings { flush_tail_epochs: 0, ..NodeSettings::default() }),
    );
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let store = orchestrator.zone_store(DEFAULT_ZONE).unwrap();
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(300) {
            assert_eq!(store.borrow().run_count(), 12, "a recurring merge ran despite the RAM broker refusing it");
            vals_rt::yield_now().await;
        }
        assert!(!store.borrow().compact_in_flight(), "a deferred plan must release its reservation of segments");
        drop(broker);
        while store.borrow().run_count() == 12 {
            assert!(start.elapsed() < Duration::from_secs(20), "the merge never ran after capacity returned");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert!(store.borrow().run_count() < 12);
    for i in 0..40u64 {
        assert_eq!(store.borrow().get(format!("k{i:03}").as_bytes()).unwrap().as_deref(), Some(format!("r11-{i}").as_bytes()));
    }
}

#[test]
fn liveness_wipe_defers_while_an_off_core_merge_is_in_flight() {
    const CFG: Config = Config { seg_blocks: 64, reserved_blocks: 0, ordering: SOrdering::Sorted };
    let mut store = VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(512, 65_536), CFG), 1).unwrap();
    for round in 0..4u64 {
        for i in 0..40u64 {
            store
                .apply(format!("k{i:03}").as_bytes(), Some(b"v"), Version { hlc: round * 100 + i + 1, node: 1 })
                .unwrap();
        }
        assert!(store.flush().unwrap().is_some());
    }
    let shared = ServerShared::single_zone_on(1, store, Ring::from_members(1, 64, [1]));
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    let plan = store.borrow_mut().compact_plan(physical_now_ms()).unwrap().expect("a merge plan");
    let wipes = crate::observe::counters().wipes.get();
    let refused = shared.wipe_all_zones(physical_now_ms(), &[]);
    assert!(matches!(refused, Err(EngineError::MaintenanceInFlight)), "{refused:?}");
    assert_eq!(crate::observe::counters().wipes.get(), wipes, "a refused wipe must not be recorded");
    assert_eq!(store.borrow().run_count(), 4);
    assert_eq!(store.borrow().get(b"k001").unwrap().as_deref(), Some(&b"v"[..]));
    let wdev = Arc::new(store.borrow().device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
    store.borrow_mut().compact_commit(written).unwrap();
    shared.wipe_all_zones(physical_now_ms(), &[]).unwrap();
    assert_eq!(store.borrow().run_count(), 0);
    assert_eq!(store.borrow().get(b"k001").unwrap(), None);
}
