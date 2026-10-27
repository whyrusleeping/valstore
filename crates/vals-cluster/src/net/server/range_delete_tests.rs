use super::*;
use crate::hlc::Version;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering as SOrdering};

fn v(hlc: u64, node: u32) -> Version {
    Version { hlc, node: node.into() }
}

fn rt_shared(
    node: NodeId,
    ring: Ring,
    ordering: crate::zone::Ordering,
    map: Option<RangeMap>,
) -> Rc<ServerShared<MemBlockDevice>> {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 8192),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: SOrdering::Sorted },
    );
    let mut store = VersionedStore::new(engine, 1).unwrap();
    for k in [b"a" as &[u8], b"b", b"c", b"d"] {
        store.apply(k, Some(b"v1"), v(1, 1)).unwrap();
    }
    let shared = ServerShared::single_zone_on(node, store, ring);
    let store = shared.zones.borrow_mut().remove(&DEFAULT_ZONE).unwrap();
    shared.zones.borrow_mut().insert(7, store);
    let dev = shared.devices.borrow_mut().remove(&DEFAULT_ZONE).unwrap();
    shared.devices.borrow_mut().insert(7, dev);
    let mut catalog = Catalog::new();
    catalog.insert(7, ZoneConfig { name: "rt".into(), rf: 1, ordering, scope: None });
    *shared.catalog.borrow_mut() = catalog;
    if let Some(map) = map {
        shared.range_maps.borrow_mut().insert(7, map);
        shared.range_map_epoch.set(11);
    }
    Rc::new(shared)
}

fn owned(lo: Option<&[u8]>, hi: Option<&[u8]>) -> wire::OwnedSlice {
    (lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec))
}

fn accounting_kicked(shared: &ServerShared<MemBlockDevice>) -> bool {
    let notified = std::pin::pin!(shared.logical_accounting_notified());
    notified.poll(&mut Context::from_waker(Waker::noop())).is_ready()
}

fn split_with_distinct_owners(ring: &Ring) -> (Vec<u8>, NodeId, NodeId) {
    let mut boundary = b"m".to_vec();
    while ring.replicas_in_zone(7, b"", 1, None) == ring.replicas_in_zone(7, &boundary, 1, None) {
        boundary.push(b'x');
    }
    let first = ring.replicas_in_zone(7, b"", 1, None)[0];
    let second = ring.replicas_in_zone(7, &boundary, 1, None)[0];
    (boundary, first, second)
}

#[test]
fn applied_slices_hide_keys_wait_for_group_commit_and_kick_accounting() {
    let mut map = RangeMap::single();
    assert!(map.split(b"c"));
    let shared = rt_shared(1, Ring::from_members(1, 64, [1]), crate::zone::Ordering::Sorted, Some(map));
    let store = shared.zone_store(7).unwrap();
    let slices = vec![owned(None, Some(b"b")), owned(Some(b"c"), Some(b"d"))];
    assert!(fence_range_delete(&shared, 7, &slices).is_ok());
    assert!(!accounting_kicked(&shared));
    let applied_before = crate::observe::counters().range_deletes_applied_total.get();
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, true, v(20, 2), &slices),
        crate::reactor::poll_io,
    );
    assert_eq!(response, Response::Batch(vec![Applied::Won, Applied::Won]));
    assert!(crate::observe::counters().range_deletes_applied_total.get() >= applied_before + 2);
    assert_eq!(shared.sync_groups.borrow().get(&7).map(|g| g.commits), Some(1));
    assert!(store.borrow().rt_fold_pending());
    assert!(accounting_kicked(&shared));
    let tombstone = Some(Stored { version: v(20, 2), value: None });
    for key in [b"a" as &[u8], b"c", b"0"] {
        assert_eq!(store.borrow().get_versioned(key).unwrap(), tombstone);
    }
    for key in [b"b" as &[u8], b"d"] {
        assert_eq!(store.borrow().get_versioned(key).unwrap().unwrap().value.as_deref(), Some(b"v1" as &[u8]));
    }

    let future = Version { hlc: (crate::hlc::physical_now_ms() + 60_000) << 16, node: 2 };
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, false, future, &[owned(Some(b"d"), None)]),
        crate::reactor::poll_io,
    );
    assert_eq!(response, Response::Batch(vec![Applied::Rejected]));
    assert_eq!(store.borrow().get_versioned(b"d").unwrap().unwrap().value.as_deref(), Some(b"v1" as &[u8]));
    assert_eq!(shared.sync_groups.borrow().get(&7).map(|g| g.commits), Some(1));
}

#[test]
fn fence_refuses_the_whole_request_when_any_slice_meets_a_foreign_range() {
    let ring = Ring::from_members(1, 64, [1, 2, 3]);
    let (boundary, first, second) = split_with_distinct_owners(&ring);
    let map = RangeMap::from_splits([boundary.clone()]);
    let shared = rt_shared(first, ring.clone(), crate::zone::Ordering::Sorted, Some(map.clone()));
    let mine = owned(None, Some(&boundary));
    assert!(fence_range_delete(&shared, 7, std::slice::from_ref(&mine)).is_ok());
    let below = [boundary.as_slice(), b"/0"].concat();
    for slices in [
        vec![mine.clone(), owned(Some(&boundary), None)],
        vec![owned(Some(b"a"), Some(&below))],
        vec![owned(None, None)],
    ] {
        assert_eq!(fence_range_delete(&shared, 7, &slices), Err(Response::StaleEpoch(11)));
    }
    let foreign = rt_shared(second, ring, crate::zone::Ordering::Sorted, Some(map));
    assert!(fence_range_delete(&foreign, 7, &[owned(Some(&boundary), None)]).is_ok());
    assert_eq!(
        fence_range_delete(&foreign, 7, std::slice::from_ref(&mine)),
        Err(Response::StaleEpoch(11))
    );
    assert!(foreign.zone_store(7).unwrap().borrow().engine().range_tombstones().is_empty());

    let unsorted = rt_shared(1, Ring::from_members(1, 64, [1]), crate::zone::Ordering::Unsorted, None);
    assert!(matches!(fence_range_delete(&unsorted, 7, &[mine.clone()]), Err(Response::Err(_))));
    assert!(matches!(fence_range_delete(&unsorted, 8, &[mine]), Err(Response::Err(_))));
    let sorted = rt_shared(1, Ring::from_members(1, 64, [1]), crate::zone::Ordering::Sorted, None);
    assert!(matches!(
        fence_range_delete(&sorted, 7, &[owned(Some(b"b"), Some(b"b"))]),
        Err(Response::Err(_))
    ));
}

#[test]
fn pending_owner_mirror_failure_is_an_error_and_is_recorded() {
    let ring = Ring::from_members(1, 64, [1, 2]);
    let (boundary, current, _) = split_with_distinct_owners(&ring);
    let mut map = RangeMap::single();
    assert!(map.split(&boundary));
    assert!(map.set_pending(1, boundary.clone()));
    let shared = rt_shared(current, ring, crate::zone::Ordering::Sorted, Some(map));
    let store = shared.zone_store(7).unwrap();
    let below = owned(Some(b"a"), Some(b"b"));
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, false, v(20, 2), std::slice::from_ref(&below)),
        crate::reactor::poll_io,
    );
    assert_eq!(response, Response::Batch(vec![Applied::Won]));
    assert!(shared.rehome_mirrors.borrow().get(&(7, boundary.clone())).is_none_or(|s| s.failures == 0));

    let inside = owned(Some(&boundary), None);
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, false, v(21, 2), &[below, inside]),
        crate::reactor::poll_io,
    );
    assert!(matches!(response, Response::Err(ref e) if e.contains("mirror")), "{response:?}");
    assert_eq!(shared.rehome_mirrors.borrow()[&(7, boundary.clone())].failures, 1);
    assert_eq!(
        store.borrow().engine().range_tombstone_cover(&boundary),
        Some(crate::replica::pack_version(v(21, 2)))
    );
}

#[test]
fn a_request_queued_across_an_ownership_change_is_refused_with_nothing_applied() {
    let shared = rt_shared(1, Ring::from_members(1, 64, [1]), crate::zone::Ordering::Sorted, Some(RangeMap::single()));
    let store = shared.zone_store(7).unwrap();
    let slices = vec![owned(None, None)];
    assert!(fence_range_delete(&shared, 7, &slices).is_ok());
    *shared.ring.borrow_mut() = Ring::from_members(1, 64, [2]);
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, false, v(20, 2), &slices),
        crate::reactor::poll_io,
    );
    assert_eq!(response, Response::StaleEpoch(11));
    assert!(store.borrow().engine().range_tombstones().is_empty());
}

#[test]
fn a_seeded_replica_folds_a_small_range_delete_before_it_replies() {
    let seeded = |s: &VersionedStore<MemBlockDevice>| {
        for _ in 0..1_000_000 {
            if s.seed_step_cooperative().unwrap() {
                break;
            }
        }
        assert!(s.is_leaf_seeded());
    };
    let shared = rt_shared(1, Ring::from_members(1, 64, [1]), crate::zone::Ordering::Sorted, Some(RangeMap::single()));
    let store = shared.zone_store(7).unwrap();
    seeded(&store.borrow());
    assert_eq!(store.borrow().exact_live_keys(), Some(4));
    let response = vals_rt::block_on(
        serve_range_delete(shared.clone(), 7, &store, false, v(20, 2), &[owned(None, Some(b"c"))]),
        crate::reactor::poll_io,
    );
    assert_eq!(response, Response::Batch(vec![Applied::Won]));
    assert!(!store.borrow().rt_fold_pending(), "the reply waits for the fold of a small range delete");
    assert!(!accounting_kicked(&shared));
    assert_eq!(store.borrow().exact_live_keys(), Some(2));

    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 8192),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: SOrdering::Sorted },
    );
    let mut fresh = VersionedStore::new(engine, 1).unwrap();
    for k in [b"c" as &[u8], b"d"] {
        fresh.apply(k, Some(b"v1"), v(1, 1)).unwrap();
    }
    seeded(&fresh);
    let all = std::collections::HashSet::from([0]);
    assert_eq!(
        store.borrow().scoped_leaf_digests(1, 1, &all, None).unwrap(),
        fresh.scoped_leaf_digests(1, 1, &all, None).unwrap()
    );
}
