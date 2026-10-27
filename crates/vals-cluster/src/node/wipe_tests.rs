use super::*;
use crate::hlc::Version;
use crate::liveness::MemLivenessLog;
use std::collections::HashMap;
use std::time::Instant;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

fn store() -> VersionedStore<MemBlockDevice> {
    let cfg = Config { seg_blocks: 64, reserved_blocks: 0, ordering: SOrdering::Sorted };
    VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(512, 65_536), cfg), 1).unwrap()
}

fn write_round(store: &mut VersionedStore<MemBlockDevice>, round: u64) {
    for i in 0..40u64 {
        store.apply(format!("k{i:03}").as_bytes(), Some(b"v"),
            Version { hlc: round * 100 + i + 1, node: 1 }).unwrap();
    }
}

#[test]
fn pending_liveness_wipe_drains_under_sustained_ingest() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = ServerShared::single_zone_on(1, store(), Ring::from_members(1, 64, [1]));
    let other = DEFAULT_ZONE + 1;
    shared.insert_zone_for_test(other, store());
    let zones = [DEFAULT_ZONE, other];
    let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
    let submit = handle.submitter();
    let first = shared.zone_store(DEFAULT_ZONE).unwrap();
    write_round(&mut first.borrow_mut(), 0);
    let mut plan = first.borrow_mut().flush_plan().unwrap().unwrap();
    plan.set_unpaced();
    assert!(submit.submit(crate::maint::MaintJob::Flush(DEFAULT_ZONE, plan)).is_ok());
    assert!(matches!(shared.wipe_all_zones(physical_now_ms(), &[]), Err(EngineError::MaintenanceInFlight)));
    let mut previous = vec![DEFAULT_ZONE];
    let mut completed = HashMap::new();
    let mut wiped_at = None;
    for round in 1..=4 {
        let mut started = Vec::new();
        for zone in zones {
            let store = shared.zone_store(zone).unwrap();
            write_round(&mut store.borrow_mut(), round);
            if !shared.maintenance_paused_for_wipe() && !store.borrow().flush_in_flight() {
                let mut plan = store.borrow_mut().flush_plan().unwrap().unwrap();
                plan.set_unpaced();
                assert!(submit.submit(crate::maint::MaintJob::Flush(zone, plan)).is_ok());
                started.push(zone);
            }
        }
        match shared.wipe_all_zones(physical_now_ms(), &[]) {
            Ok(()) => {
                wiped_at = Some(round);
                break;
            }
            Err(EngineError::MaintenanceInFlight) => {}
            Err(e) => panic!("unexpected wipe failure: {e}"),
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while previous.iter().any(|zone| !completed.contains_key(zone)) {
            assert!(Instant::now() < deadline, "maintenance never completed");
            for done in handle.drain() {
                if let crate::maint::MaintDone::Flushed(zone, result) = done {
                    completed.insert(zone, result.unwrap());
                } else {
                    panic!("unexpected maintenance completion");
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        for zone in previous {
            shared.zone_store(zone).unwrap().borrow_mut()
                .flush_commit(completed.remove(&zone).unwrap()).unwrap();
        }
        previous = started;
    }
    drop(submit);
    drop(handle);
    join.join();
    assert_eq!(wiped_at, Some(2), "a pending wipe must complete within two sweeps despite ongoing writes");
    assert!(!shared.maintenance_paused_for_wipe());
    assert_eq!(crate::observe::gauges().wipe_pending.get(), 0);
    for zone in zones {
        let store = shared.zone_store(zone).unwrap();
        assert_eq!(store.borrow().get(b"k001").unwrap(), None);
        assert_eq!(store.borrow().run_count(), 0);
    }
}

#[test]
fn obsolete_liveness_wipe_resumes_maintenance() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    for case in 0..4 {
        let shared = ServerShared::single_zone_on(1, store(), Ring::from_members(1, 64, [1]))
            .with_liveness(100, Box::new(MemLivenessLog::new()));
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        write_round(&mut store.borrow_mut(), 0);
        let plan = store.borrow_mut().flush_plan().unwrap().unwrap();
        assert!(matches!(shared.wipe_all_zones(physical_now_ms(), &[]), Err(EngineError::MaintenanceInFlight)));
        assert_eq!(crate::observe::gauges().wipe_pending.get(), 1);
        let peers = [shared.watermark() + 101];
        assert!(shared.liveness_wipe_wanted(true, &peers));
        assert!(shared.maintenance_paused_for_wipe());
        match case {
            0 => shared.record_contact(physical_now_ms()),
            1 => shared.begin_resync(),
            _ => {}
        }
        let peers = if case == 2 { &[][..] } else { &peers[..] };
        assert!(!shared.liveness_wipe_wanted(case != 3, peers));
        assert!(!shared.maintenance_paused_for_wipe());
        assert_eq!(crate::observe::gauges().wipe_pending.get(), 0);
        store.borrow_mut().free_flush_segs(plan.segments());
    }
}

#[test]
fn lost_maintenance_job_requests_restart_after_wipe_deadline() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    let shared = ServerShared::single_zone_on(1, store(), Ring::from_members(1, 64, [1]));
    let store = shared.zone_store(DEFAULT_ZONE).unwrap();
    write_round(&mut store.borrow_mut(), 0);
    let plan = store.borrow_mut().flush_plan().unwrap().unwrap();
    assert!(!crate::integrity::fail_fast_requested());
    let before = Instant::now();
    assert!(matches!(shared.wipe_all_zones(physical_now_ms(), &[]), Err(EngineError::MaintenanceInFlight)));
    shared.check_wipe_timeout(before + Duration::from_secs(599));
    assert!(!crate::integrity::fail_fast_requested());
    assert!(matches!(shared.wipe_all_zones(physical_now_ms(), &[]), Err(EngineError::MaintenanceInFlight)));
    shared.check_wipe_timeout(Instant::now() + Duration::from_secs(601));
    assert!(crate::integrity::fail_fast_requested());
    assert_eq!(crate::integrity::INTEGRITY_FAIL_FAST_EXIT_CODE, 75);
    drop(plan);
    drop(shared);
    assert_eq!(crate::observe::gauges().wipe_pending.get(), 0);
}
