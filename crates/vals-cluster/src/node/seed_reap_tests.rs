use super::*;
use crate::hlc::Version;
use std::time::Instant;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

fn seed_holding_retired_runs() -> VersionedStore<MemBlockDevice> {
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: SOrdering::Sorted };
    let mut s = VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(256, 1 << 14), cfg), 1).unwrap();
    for i in 0..600u64 {
        s.apply(format!("k{i:04}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 }).unwrap();
        if i % 150 == 149 {
            s.flush().unwrap();
        }
    }
    s.invalidate_leaf_seed();
    assert!(!s.seed_step_cooperative().unwrap());
    assert_eq!(s.forget(&std::collections::HashSet::from([b"k0001".to_vec()])).unwrap(), 1);
    assert!(s.engine().pinned_retired_bytes() > 0, "the in-flight seed pins the forget's removed runs");
    s
}

fn run_until_reaped(
    shared: &Rc<ServerShared<MemBlockDevice>>,
    spawn_loops: impl FnOnce(&LocalExecutor) -> Option<crate::maint::MaintJoin>,
) -> (bool, u64) {
    let ex = LocalExecutor::new();
    let join = spawn_loops(&ex);
    let observer = shared.clone();
    let outcome = Rc::new(Cell::new((false, u64::MAX)));
    let outcome_out = outcome.clone();
    ex.spawn(async move {
        let store = observer.zone_store(DEFAULT_ZONE).unwrap();
        observer.kick_logical_accounting();
        let deadline = Instant::now() + Duration::from_secs(10);
        while (!store.borrow().is_leaf_seeded() || store.borrow().engine().pinned_retired_bytes() > 0)
            && Instant::now() < deadline
        {
            vals_rt::yield_now().await;
        }
        for _ in 0..50 {
            vals_rt::yield_now().await;
        }
        outcome_out.set((store.borrow().is_leaf_seeded(), store.borrow().engine().pinned_retired_bytes()));
        observer.begin_shutdown();
    });
    ex.run(crate::reactor::poll_io_nonblocking);
    if let Some(mut join) = join {
        join.join();
    }
    outcome.get()
}

#[test]
fn seed_driver_reaps_released_pins_after_its_completing_step() {
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = crate::replica::seed_policy::pin_cap_for_test(1 << 40);
    let s = seed_holding_retired_runs();
    let shared = Rc::new(ServerShared::single_zone_on(1, s, Ring::from_members(1, 64, [1])));
    let (seeded, pinned) = run_until_reaped(&shared, |ex| {
        ex.spawn(logical_accounting_loop(shared.clone()));
        None
    });
    assert!(seeded, "the accounting task finished the seed");
    assert_eq!(pinned, 0, "the seed driver reaped the retired runs once the seed released its pin");
}

#[test]
fn snapshot_tick_reaps_released_pins_without_a_seed_driver() {
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = crate::replica::seed_policy::pin_cap_for_test(1 << 40);
    let s = seed_holding_retired_runs();
    while !s.seed_step_cooperative().unwrap() {}
    assert!(s.engine().pinned_retired_bytes() > 0, "publishing releases the pin but does not reap");
    let shared = Rc::new(ServerShared::single_zone_on(1, s, Ring::from_members(1, 64, [1])));
    let (seeded, pinned) = run_until_reaped(&shared, |ex| {
        let (handle, join) = crate::maint::spawn::<MemBlockDevice>();
        ex.spawn(snapshot_loop(shared.clone(), Duration::from_millis(5), handle.submitter()));
        ex.spawn(maint_loop(shared.clone(), handle));
        Some(join)
    });
    assert!(seeded);
    assert_eq!(pinned, 0, "the snapshot tick reaped the retired runs");
}
