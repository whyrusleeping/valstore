use super::*;
use crate::hlc::Version;
use crate::integrity::{FailFastTrip, INTEGRITY_FAIL_FAST_EXIT_CODE};
use crate::node_settings::NodeSettings;
use std::time::Instant;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

const CFG: Config = Config { seg_blocks: 64, reserved_blocks: 0, ordering: SOrdering::Sorted };

fn keys() -> Vec<Vec<u8>> {
    (0..40).map(|i| format!("did:plc:sim36rxr2peojm7mthz5us7s/{i:03}").into_bytes()).collect()
}

fn sorted_store(mem: MemBlockDevice) -> VersionedStore<MemBlockDevice> {
    let mut s = VersionedStore::new(Engine::create_prewiped(mem, CFG), 1).unwrap();
    for (i, key) in keys().iter().enumerate() {
        s.apply(key, Some(format!("value-{i}").as_bytes()), Version { hlc: i as u64 + 1, node: 1 }).unwrap();
    }
    s.sync().unwrap();
    s
}

fn settings(attempts: u32, secs: u64) -> NodeSettings {
    NodeSettings {
        flush_tail_epochs: 0,
        integrity_fail_fast_attempts: attempts,
        integrity_fail_fast_secs: secs,
        ..NodeSettings::default()
    }
}

fn shared_for(store: VersionedStore<MemBlockDevice>, settings: NodeSettings) -> Rc<ServerShared<MemBlockDevice>> {
    Rc::new(ServerShared::single_zone_on(1, store, Ring::from_members(1, 64, [1])).with_settings(settings))
}

fn run_loops<F>(shared: &Rc<ServerShared<MemBlockDevice>>, orchestrate: F)
where
    F: std::future::Future<Output = ()> + 'static,
{
    let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
    let ex = LocalExecutor::new();
    ex.spawn(orchestrate);
    ex.spawn(snapshot_loop(shared.clone(), Duration::from_millis(5), handle.submitter()));
    ex.spawn(maint_loop(shared.clone(), handle));
    ex.run({
        let shared = shared.clone();
        move || server_poll(&shared)
    });
    join.join();
}

fn flip_key_byte(shared: &ServerShared<MemBlockDevice>) {
    shared.zone_store(DEFAULT_ZONE).unwrap().borrow_mut().engine_mut().flip_memtable_bit_for_test(0, 1 + 19, 0x10);
}

fn tripped(shared: &ServerShared<MemBlockDevice>) -> Option<FailFastTrip> {
    shared.integrity_watch.borrow().tripped().cloned()
}

#[test]
fn persistent_memtable_integrity_failure_fails_fast_and_reopen_recovers() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let mem = MemBlockDevice::new(512, 65_536);
    let disk = mem.share();
    let shared = shared_for(sorted_store(mem), settings(3, 30));
    flip_key_byte(&shared);
    let before = crate::observe::counters().integrity_fail_fast_total.get();
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        while tripped(&orchestrator).is_none() {
            assert!(Instant::now() < deadline, "persistent integrity failure never failed fast");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    let trip = tripped(&shared).unwrap();
    assert_eq!((trip.zone, trip.attempts, trip.single_bit_xor), (DEFAULT_ZONE, 3, Some(0x10)));
    assert!(trip.evidence.contains("front-coded index integrity mismatch block_first_key=["), "{}", trip.evidence);
    assert!(trip.evidence.contains("region=key key_index=0 offset=19 actual=0x7f expected=0x6f xor=0x10"), "{}", trip.evidence);
    assert!(crate::integrity::fail_fast_requested());
    assert!(crate::observe::counters().integrity_fail_fast_total.get() > before);
    assert_eq!(INTEGRITY_FAIL_FAST_EXIT_CODE, 75);
    let failures = vals_store::index::integrity_failures();
    shared.finalize_shutdown();
    assert_eq!(vals_store::index::integrity_failures(), failures, "shutdown must not attempt a final corrupt memtable flush");
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert_eq!(store.borrow().run_count(), 0);
    assert!(!store.borrow().flush_in_flight());
    drop(store);
    drop(shared);

    let mut reopened = VersionedStore::new(Engine::open(disk, CFG).unwrap(), 1).unwrap();
    assert_eq!(reopened.run_count(), 0, "shutdown persisted no run from the corrupt memtable");
    assert!(reopened.flush().unwrap().is_some());
    assert_eq!(reopened.run_count(), 1);
    for (i, key) in keys().iter().enumerate() {
        assert_eq!(reopened.get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
        let mut flipped = key.clone();
        flipped[19] = 0x7f;
        assert_eq!(reopened.get(&flipped).unwrap(), None);
    }
}

#[test]
fn replacement_and_wipe_clear_streaks_even_when_the_rc_is_unchanged() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = shared_for(sorted_store(MemBlockDevice::new(512, 65_536)), settings(3, 30));
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    let identity = Rc::as_ptr(&store) as usize;
    let record = || {
        flip_key_byte(&shared);
        let error = store.borrow_mut().engine_mut().flush_plan(vals_store::run::RunBuildOpts::default()).err().unwrap();
        let policy = crate::integrity::FailFastPolicy::from_settings(&shared.settings);
        assert!(shared.integrity_watch.borrow_mut().flush_failure(policy, DEFAULT_ZONE, identity, &error, Instant::now()).is_none());
        assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 1);
    };
    record();
    shared.commit_default_zone_swap(sorted_store(MemBlockDevice::new(512, 65_536)));
    assert_eq!(Rc::as_ptr(&shared.zone_store(DEFAULT_ZONE).unwrap()) as usize, identity);
    assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 0);
    record();
    shared.wipe_all_zones(100, &[]).unwrap();
    assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 0);
}

#[test]
fn transient_memtable_integrity_failure_cleared_by_a_retry_does_not_fail_fast() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = shared_for(sorted_store(MemBlockDevice::new(512, 65_536)), settings(3, 30));
    flip_key_byte(&shared);
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        while orchestrator.integrity_watch.borrow().streak(DEFAULT_ZONE) == 0 {
            assert!(Instant::now() < deadline, "the injected flip was never detected");
            vals_rt::yield_now().await;
        }
        flip_key_byte(&orchestrator);
        let store = orchestrator.zone_store(DEFAULT_ZONE).unwrap();
        while store.borrow().run_count() == 0 {
            assert!(Instant::now() < deadline, "the flush never succeeded after the flip cleared");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    assert!(tripped(&shared).is_none());
    assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 0);
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert_eq!(store.borrow().run_count(), 1);
    for (i, key) in keys().iter().enumerate() {
        assert_eq!(store.borrow().get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
    }
}

#[test]
fn disabled_policy_keeps_retrying_the_corrupt_flush_forever() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = shared_for(sorted_store(MemBlockDevice::new(512, 65_536)), settings(0, 0));
    flip_key_byte(&shared);
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        while orchestrator.integrity_watch.borrow().streak(DEFAULT_ZONE) < 20 {
            assert!(Instant::now() < deadline, "the flush stopped retrying");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    assert!(tripped(&shared).is_none());
    assert!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE) >= 20);
    assert_eq!(shared.zone_store(DEFAULT_ZONE).unwrap().borrow().run_count(), 0);
}

#[test]
fn on_disk_run_crc_failures_never_fail_fast() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let mem = MemBlockDevice::new(512, 65_536);
    let disk = mem.share();
    let mut store = sorted_store(mem);
    assert!(store.flush().unwrap().is_some());
    for round in 1..12u64 {
        for (i, key) in keys().iter().enumerate() {
            store.apply(key, Some(format!("round-{round}-{i}").as_bytes()), Version { hlc: 100 * round + i as u64, node: 1 }).unwrap();
        }
        assert!(store.flush().unwrap().is_some());
    }
    assert_eq!(store.run_count(), 12);
    for handle in store.engine().run_handles() {
        let lba = handle.segs[0].0 * CFG.seg_blocks;
        let mut block = vec![0u8; 512];
        disk.read_at(lba, &mut block).unwrap();
        for b in block.iter_mut().skip(16).take(64) {
            *b ^= 0x10;
        }
        disk.write_at(lba, &block).unwrap();
    }
    let shared = shared_for(store, NodeSettings { run_compact_threshold: 2, ..settings(2, 1) });
    let errors_before = crate::observe::counters().snapshot_errors.get();
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let start = Instant::now();
        while crate::observe::counters().snapshot_errors.get() < errors_before + 3
            || start.elapsed() < Duration::from_secs(1)
        {
            assert!(start.elapsed() < Duration::from_secs(20), "the corrupt runs never failed a merge");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    assert!(tripped(&shared).is_none());
    assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 0);
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert_eq!(store.borrow().run_count(), 12);
    let error = store.borrow_mut().compact_runs(physical_now_ms()).unwrap_err();
    assert!(error.memtable_integrity().is_none());
    assert!(error.to_string().contains("crc"), "{error}");
}

fn arm_flush_source_flip(shared: &ServerShared<MemBlockDevice>, flip: Option<(usize, usize, u8)>) {
    shared.zone_store(DEFAULT_ZONE).unwrap().borrow_mut().engine_mut().set_flush_source_flip_for_test(flip);
}

#[test]
fn persistent_flush_encode_integrity_failure_fails_fast_and_reopen_recovers() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let mem = MemBlockDevice::new(512, 65_536);
    let disk = mem.share();
    let shared = shared_for(sorted_store(mem), settings(3, 30));
    arm_flush_source_flip(&shared, Some((0, 35, 0x10)));
    let before = vals_store::run::encode_integrity_failures()[0].1;
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        while tripped(&orchestrator).is_none() {
            assert!(Instant::now() < deadline, "persistent encode integrity failure never failed fast");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    let trip = tripped(&shared).unwrap();
    assert_eq!((trip.zone, trip.attempts, trip.single_bit_xor), (DEFAULT_ZONE, 3, Some(0x10)));
    assert!(trip.evidence.contains("run encode integrity mismatch path=flush check=source-hash"), "{}", trip.evidence);
    assert!(trip.evidence.contains("region=key key_index=0 offset=35 actual=0x20 expected=0x30 xor=0x10"), "{}", trip.evidence);
    assert!(crate::integrity::fail_fast_requested());
    assert!(vals_store::run::encode_integrity_failures()[0].1 >= before + 3);
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert_eq!(store.borrow().run_count(), 0);
    assert!(!store.borrow().flush_in_flight());
    drop(store);
    drop(shared);

    let mut reopened = VersionedStore::new(Engine::open(disk, CFG).unwrap(), 1).unwrap();
    assert!(reopened.flush().unwrap().is_some());
    for (i, key) in keys().iter().enumerate() {
        assert_eq!(reopened.get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
    }
}

#[test]
fn transient_flush_encode_integrity_failure_cleared_by_a_retry_does_not_fail_fast() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = shared_for(sorted_store(MemBlockDevice::new(512, 65_536)), settings(3, 30));
    arm_flush_source_flip(&shared, Some((0, 35, 0x10)));
    let orchestrator = shared.clone();
    run_loops(&shared, async move {
        let deadline = Instant::now() + Duration::from_secs(20);
        while orchestrator.integrity_watch.borrow().streak(DEFAULT_ZONE) == 0 {
            assert!(Instant::now() < deadline, "the injected flip was never detected");
            vals_rt::yield_now().await;
        }
        arm_flush_source_flip(&orchestrator, None);
        let store = orchestrator.zone_store(DEFAULT_ZONE).unwrap();
        while store.borrow().run_count() == 0 {
            assert!(Instant::now() < deadline, "the flush never succeeded after the flip cleared");
            vals_rt::yield_now().await;
        }
        orchestrator.begin_shutdown();
    });
    assert!(tripped(&shared).is_none());
    assert_eq!(shared.integrity_watch.borrow().streak(DEFAULT_ZONE), 0);
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    assert_eq!(store.borrow().run_count(), 1);
    for (i, key) in keys().iter().enumerate() {
        assert_eq!(store.borrow().get(key).unwrap().as_deref(), Some(format!("value-{i}").as_bytes()));
    }
}
