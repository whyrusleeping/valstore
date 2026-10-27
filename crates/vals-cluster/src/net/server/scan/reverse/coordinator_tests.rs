use super::*;
use crate::coord::{Coordinator, LocalCluster, RingView, ScanPage, Transport, TransportError};
use crate::hlc::Version;
use crate::partition::RangeMap;
use crate::replica::Applied;
use crate::ring::Ring;
use crate::zone::{Catalog, ZoneConfig, ZoneId};
use std::cell::Cell;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine};

type Call = (NodeId, Option<Vec<u8>>, Option<Vec<u8>>, u32, bool);

struct Cluster {
    inner: LocalCluster<MemBlockDevice>,
    map: RangeMap,
    zone: ZoneId,
    cap: Cell<u32>,
    down: Cell<Option<NodeId>>,
    calls: RefCell<Vec<Call>>,
    empty_pages: Cell<usize>,
}

impl Drop for Cluster {
    fn drop(&mut self) {
        set_zone_scan_reply_budget(self.zone, None);
        set_scan_covered_budget(None);
    }
}

impl Transport for Cluster {
    async fn apply(&self, n: NodeId, z: ZoneId, k: &[u8], v: Option<&[u8]>, ver: Version) -> Result<Applied, TransportError> {
        self.inner.apply(n, z, k, v, ver).await
    }
    async fn read(&self, n: NodeId, z: ZoneId, k: &[u8]) -> Result<Option<Stored>, TransportError> {
        self.inner.read(n, z, k).await
    }
    async fn members(&self, n: NodeId) -> Result<RingView, TransportError> { self.inner.members(n).await }
    async fn zones(&self, n: NodeId) -> Result<Catalog, TransportError> { self.inner.zones(n).await }
    async fn range_maps(&self, _: NodeId) -> Result<Vec<(ZoneId, RangeMap)>, TransportError> {
        Ok(vec![(self.zone, self.map.clone())])
    }
    async fn scan(&self, n: NodeId, z: ZoneId, lo: Option<&[u8]>, hi: Option<&[u8]>, max: u32) -> Result<ScanPage, TransportError> {
        self.inner.scan(n, z, lo, hi, max).await
    }
    async fn scan_rev(&self, n: NodeId, z: ZoneId, lo: Option<&[u8]>, hi: Option<&[u8]>, max: u32, keys: bool) -> Result<ScanPage, TransportError> {
        assert_eq!(z, self.zone);
        self.calls.borrow_mut().push((n, lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec), max, keys));
        assert!(self.calls.borrow().len() < 100_000, "pagination must terminate");
        if self.down.get() == Some(n) { return Err(TransportError::Unavailable); }
        let s = self.inner.store_zone(n, z).unwrap();
        let dev = s.borrow().device_handle();
        let cap = if max == 0 { self.cap.get() } else { max.min(self.cap.get()) };
        let (response, _) = serve_scan_rev(s, &*dev, &ReadGate::new(), lo, hi, cap, true, keys,
            crate::integrity::ReadSite { zone: z, shard: 0 }).await;
        match response {
            Response::Scan { entries, more, fragments, resume } => {
                if entries.is_empty() && more { self.empty_pages.set(self.empty_pages.get() + 1); }
                Ok(ScanPage { entries, more, fragments, resume })
            }
            other => panic!("unexpected response: {other:?}"),
        }
    }
}

fn run<F: std::future::Future>(f: F) -> F::Output { vals_rt::block_on(f, || {}) }
fn ver(hlc: u64) -> Version { Version { hlc, node: 1 } }

fn cluster(rf: usize, nodes: u32, map: impl FnOnce(&Ring, ZoneId) -> RangeMap) -> Coordinator<Cluster> {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(490000);
    let zone = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut ring = Ring::with_vnodes(rf, 64);
    let mut inner = LocalCluster::new();
    inner.define_zone(zone, ZoneConfig { name: "reverse".into(), rf, ordering: crate::zone::Ordering::Sorted, scope: None });
    for n in 1..=nodes {
        ring.add_node(n);
        let e = Engine::create_prewiped(MemBlockDevice::new(256, 32768),
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted });
        inner.insert_zone(n, zone, VersionedStore::new(e, n).unwrap());
    }
    let map = map(&ring, zone);
    let t = Cluster { inner, map, zone, cap: Cell::new(u32::MAX), down: Cell::new(None), calls: RefCell::new(Vec::new()), empty_pages: Cell::new(0) };
    let c = Coordinator::new(99, ring, rf, t);
    run(c.refresh_catalog(1)).unwrap();
    c
}

fn put(c: &Coordinator<Cluster>, node: NodeId, key: &[u8], value: Option<&[u8]>, hlc: u64) {
    c.transport().inner.store_zone(node, c.transport().zone).unwrap().borrow_mut()
        .apply_at(key, value, ver(hlc), 10000).unwrap();
}

#[test]
fn reverse_max_coverage_keeps_dense_replica_versions_and_each_query_cursor() {
    let c = cluster(3, 3, |_, _| RangeMap::single());
    let z = c.transport().zone;
    c.transport().cap.set(2);
    for prefix in [b'a', b'z'] {
        for n in 1..=3 {
            for suffix in [b'/', b'0', b'9'] { put(&c, n, &[prefix, suffix], Some(b"old"), 10); }
        }
        let holder = if prefix == b'a' { 1 } else { 2 };
        for suffix in [b'0', b'7', b'8'] { put(&c, holder, &[prefix, suffix], Some(b"new"), 20); }
    }
    let scans = [(Some(&b"a"[..]), Some(&b"b"[..]), None), (Some(&b"z"[..]), None, None)];
    for keys_only in [false, true] {
        let got = run(c.scan_many_in_rev(z, &scans, 3, keys_only)).unwrap();
        for (i, prefix) in [b'a', b'z'].into_iter().enumerate() {
            assert_eq!(got[i].iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
                [b'9', b'8', b'7', b'0', b'/'].map(|s| vec![prefix, s]));
            assert_eq!(got[i][3].1.version, ver(20));
            assert_eq!(got[i][3].1.value.as_deref(), Some(if keys_only { &b""[..] } else { &b"new"[..] }));
        }
    }
    let got = run(c.scan_in_rev(z, Some(b"a"), Some(b"b"), 3, Some(3))).unwrap();
    assert_eq!(got, vec![(b"a9".to_vec(), b"old".to_vec()), (b"a8".to_vec(), b"new".to_vec()), (b"a7".to_vec(), b"new".to_vec())]);
}

#[test]
fn reverse_quorum_pages_match_ascending_across_splits_tombstones_and_small_budgets() {
    let c = cluster(3, 3, |_, _| RangeMap::from_splits(vec![b"k0040".to_vec(), b"k0080".to_vec()]));
    let z = c.transport().zone;
    let mut keys: Vec<_> = [b"".as_slice(), b"\0", b"k", b"k\0", b"z", b"z\0"].into_iter().map(<[u8]>::to_vec).collect();
    keys.extend((0..120).map(|i| format!("k{i:04}").into_bytes()));
    for n in 1..=3 {
        for k in &keys { put(&c, n, k, Some(k), 10); }
        c.transport().inner.store_zone(n, z).unwrap().borrow_mut().flush().unwrap();
    }
    for i in 0..120 {
        let k = format!("k{i:04}");
        if i % 7 == 0 { put(&c, 1, k.as_bytes(), None, 30); }
        if i % 9 == 0 { put(&c, 2, k.as_bytes(), Some(b"new"), 40); }
    }
    c.transport().inner.store_zone(3, z).unwrap().borrow_mut()
        .apply_range_delete(Some(b"k0010"), Some(b"k0090"), ver(20), 10000).unwrap();
    set_scan_covered_budget(Some(vals_store::ScanCoveredBudget { filtered_entries: 3, block_reads: u64::MAX }));
    let windows = [(None, None), (Some(&b"k0020"[..]), Some(&b"k0100"[..])),
        (None, Some(&b"\0"[..])), (Some(&b"z"[..]), None)];
    for budget in [1, 90, 350, SCAN_REPLY_BUDGET] {
        set_zone_scan_reply_budget(z, Some(budget));
        for cap in [1, 4, 1000] {
            c.transport().cap.set(cap);
            for &(lo, hi) in &windows {
                let mut reference = run(c.scan_in(z, lo, hi, 3, None)).unwrap();
                reference.reverse();
                for limit in [None, Some(0), Some(1), Some(7), Some(1000)] {
                    let got = run(c.scan_in_rev(z, lo, hi, 3, limit)).unwrap();
                    let want = &reference[..limit.unwrap_or(reference.len()).min(reference.len())];
                    assert_eq!(got, want, "budget={budget} cap={cap} lo={lo:?} hi={hi:?} limit={limit:?}");
                }
            }
        }
    }
    assert!(c.transport().empty_pages.get() > 0);
    let scans = [(None, None, Some(5)), (Some(&b"k0040"[..]), Some(&b"k0080"[..]), None),
        (Some(&b"z"[..]), Some(&b"a"[..]), None), (None, None, Some(0))];
    let got = run(c.scan_many_in_rev(z, &scans, 3, true)).unwrap();
    for (i, &(lo, hi, limit)) in scans.iter().enumerate() {
        let expected = run(c.scan_in_rev(z, lo, hi, 3, limit)).unwrap();
        assert_eq!(got[i].iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), expected.into_iter().map(|(k, _)| k).collect::<Vec<_>>());
        assert!(got[i].iter().all(|(_, s)| s.value == Some(Vec::new())));
    }
}

#[test]
fn reverse_majority_intersects_writes_and_tombstones_with_any_one_replica_down() {
    let c = cluster(3, 3, |_, _| RangeMap::from_splits(vec![b"m".to_vec()]));
    let t = c.transport();
    let z = t.zone;
    for n in 1..=3 {
        for k in [b"a", b"b", b"n", b"p", b"z"] { put(&c, n, k, Some(b"old"), 10); }
    }
    for n in [2, 3] {
        put(&c, n, b"a", None, 20);
        put(&c, n, b"b", Some(b"new"), 30);
        t.inner.store_zone(n, z).unwrap().borrow_mut()
            .apply_range_delete(Some(b"n"), Some(b"z"), ver(20), 10000).unwrap();
        put(&c, n, b"p", Some(b"newer"), 30);
    }
    t.cap.set(1);
    set_zone_scan_reply_budget(z, Some(1));
    for down in [None, Some(1), Some(2), Some(3)] {
        t.down.set(down);
        assert_eq!(run(c.scan_in_rev(z, None, None, 2, None)).unwrap(),
            vec![(b"z".to_vec(), b"old".to_vec()), (b"p".to_vec(), b"newer".to_vec()), (b"b".to_vec(), b"new".to_vec())], "down={down:?}");
    }
}

#[test]
fn reverse_pending_rehome_uses_committed_owners_and_visits_newest_slice_first() {
    let c = cluster(2, 4, |ring, z| {
        let owners = ring.replicas_in_zone(z, b"", 2, None);
        let split = (0..100).map(|i| format!("m{i:03}").into_bytes()).find(|s| {
            ring.replicas_in_zone(z, s, 2, None).iter().any(|n| !owners.contains(n))
        }).unwrap();
        RangeMap::from_splits_tokens_and_pending(vec![split.clone()], vec![Vec::new(), Vec::new()], vec![None, Some(split)]).unwrap()
    });
    let t = c.transport();
    let z = t.zone;
    let split = t.map.splits()[0].clone();
    let old = c.ring().replicas_in_zone(z, b"", 2, None);
    let targets = c.ring().replicas_in_zone(z, &split, 2, None);
    let mut expected = Vec::new();
    for prefix in [b"a".as_slice(), split.as_slice()] {
        for i in 0..9 {
            let k = [prefix, format!("/{i}").as_bytes()].concat();
            expected.push((k.clone(), k.clone()));
            for &n in &old { put(&c, n, &k, Some(&k), 10); }
        }
    }
    for n in targets.into_iter().filter(|n| !old.contains(n)) {
        put(&c, n, b"zz-pending-only", Some(b"wrong"), 100);
    }
    expected.reverse();
    t.cap.set(2);
    set_zone_scan_reply_budget(z, Some(90));
    for limit in [Some(1), Some(9), Some(10), None] {
        t.calls.borrow_mut().clear();
        let got = run(c.scan_in_rev(z, None, None, 2, limit)).unwrap();
        assert_eq!(got, expected[..limit.unwrap_or(expected.len())]);
        let calls = t.calls.borrow();
        assert!(calls.iter().all(|(n, _, _, _, _)| old.contains(n)), "pending owners are not scan authorities");
        assert_eq!(calls[0].1.as_deref(), Some(split.as_slice()));
        if limit.is_some_and(|n| n <= 9) {
            assert!(calls.iter().all(|(_, lo, _, _, _)| lo.as_deref() == Some(split.as_slice())), "limited scans must not touch older slices");
        } else {
            let first_low = calls.iter().position(|(_, lo, _, _, _)| lo.is_none()).unwrap();
            assert!(calls[first_low..].iter().all(|(_, lo, _, _, _)| lo.is_none()));
        }
    }
}
