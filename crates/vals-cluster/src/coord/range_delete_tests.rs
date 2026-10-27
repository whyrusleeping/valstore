use super::*;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

use std::cell::RefCell;

use crate::hlc::Version;
use crate::partition::RangeMap;
use crate::replica::{Applied, CasResult, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::wire::{OwnedSlice, RtFragment, WireSlice};
use crate::zone::{Catalog, Ordering as ZOrdering, ZoneConfig, ZoneId, DEFAULT_ZONE};

const ZONE: ZoneId = 2;
const UNSORTED: ZoneId = 3;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, || {})
}

fn rt_replica(node: NodeId, ordering: Ordering) -> VersionedStore<MemBlockDevice> {
    let engine = Engine::create_prewiped(
        MemBlockDevice::new(256, 8192),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering },
    );
    VersionedStore::new(engine, node).unwrap()
}

type DeleteCall = (NodeId, bool, Version, Vec<OwnedSlice>);

struct RtCluster {
    inner: LocalCluster<MemBlockDevice>,
    maps: Vec<(ZoneId, RangeMap)>,
    unsupported: Option<NodeId>,
    stale_once: RefCell<Vec<NodeId>>,
    held: RefCell<Option<NodeId>>,
    held_slices: RefCell<Vec<(Version, Vec<OwnedSlice>)>>,
    deletes: RefCell<Vec<DeleteCall>>,
}

impl RtCluster {
    fn release_held(&self) {
        let node = self.held.borrow_mut().take().expect("a held node");
        for (version, slices) in self.held_slices.borrow_mut().drain(..) {
            let wire: Vec<WireSlice> = slices.iter().map(|(lo, hi)| (lo.as_deref(), hi.as_deref())).collect();
            let acks = run(self.inner.range_delete(node, ZONE, false, version, &wire));
            assert!(acks.iter().all(|a| *a == BatchAck::Applied(Applied::Won)), "{acks:?}");
        }
    }

    fn store(&self, node: NodeId) -> &RefCell<VersionedStore<MemBlockDevice>> {
        self.inner.store_zone(node, ZONE).unwrap()
    }
}

impl Transport for RtCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        self.inner.apply(node, zone_id, key, value, version).await
    }

    async fn read(&self, node: NodeId, zone_id: ZoneId, key: &[u8]) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }

    async fn scan(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> Result<ScanPage, TransportError> {
        self.inner.scan(node, zone_id, lo, hi, max).await
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }

    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }

    async fn range_maps(&self, _node: NodeId) -> Result<Vec<(ZoneId, RangeMap)>, TransportError> {
        Ok(self.maps.clone())
    }

    async fn apply_batch(&self, node: NodeId, zone_id: ZoneId, sync: bool, writes: &[BatchEntry<'_>]) -> Vec<BatchAck> {
        self.inner.apply_batch(node, zone_id, sync, writes).await
    }

    fn supports_range_tombstones(&self, node: NodeId) -> Option<bool> {
        Some(self.unsupported != Some(node))
    }

    async fn range_delete(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: &[WireSlice<'_>],
    ) -> Vec<BatchAck> {
        let owned: Vec<OwnedSlice> = slices.iter().map(|(lo, hi)| (lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec))).collect();
        self.deletes.borrow_mut().push((node, sync, version, owned.clone()));
        let stale_at = self.stale_once.borrow().iter().position(|n| *n == node);
        if let Some(at) = stale_at {
            self.stale_once.borrow_mut().remove(at);
            return vec![BatchAck::Stale(5); slices.len()];
        }
        if *self.held.borrow() == Some(node) {
            self.held_slices.borrow_mut().push((version, owned));
            return vec![BatchAck::Unreachable; slices.len()];
        }
        self.inner.range_delete(node, zone_id, sync, version, slices).await
    }
}

fn rt_cluster(splits: &[&[u8]]) -> Coordinator<RtCluster> {
    let mut lc = LocalCluster::new();
    lc.define_zone(ZONE, ZoneConfig { name: "s".into(), rf: 3, ordering: ZOrdering::Sorted, scope: None });
    lc.define_zone(UNSORTED, ZoneConfig { name: "u".into(), rf: 3, ordering: ZOrdering::Unsorted, scope: None });
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3u32 {
        lc.insert_zone(node, ZONE, rt_replica(node, Ordering::Sorted));
        lc.insert_zone(node, UNSORTED, rt_replica(node, Ordering::Unsorted));
        ring.add_node(node);
    }
    let mut map = RangeMap::single();
    for at in splits {
        assert!(map.split(at));
    }
    let transport = RtCluster {
        inner: lc,
        maps: vec![(ZONE, map)],
        unsupported: None,
        stale_once: RefCell::new(Vec::new()),
        held: RefCell::new(None),
        held_slices: RefCell::new(Vec::new()),
        deletes: RefCell::new(Vec::new()),
    };
    let c = Coordinator::new(0, ring, 2, transport);
    run(c.refresh_catalog(1)).unwrap();
    assert_eq!(c.range_map(ZONE).num_ranges(), splits.len() + 1);
    c
}

fn key(i: u32) -> Vec<u8> {
    format!("m/{i:03}").into_bytes()
}

fn write(c: &Coordinator<RtCluster>, k: &[u8], v: &[u8]) -> Version {
    run(c.write_in(ZONE, k, v, WriteConcern::All)).unwrap()
}

fn scan_keys(c: &Coordinator<RtCluster>, r: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    run(c.scan_in(ZONE, None, None, r, None)).unwrap()
}

fn owned(lo: &[u8], hi: &[u8]) -> OwnedSlice {
    (Some(lo.to_vec()), Some(hi.to_vec()))
}

#[test]
fn multi_scan_min_coverage_is_independent_for_each_subscan() {
    let c = rt_cluster(&[]);
    c.transport.inner.set_scan_page_entries(2);
    for prefix in [b'a', b'z'] {
        for node in 1..=3 {
            let mut s = c.transport.store(node).borrow_mut();
            for suffix in [b'0', b'9'] {
                s.apply_at(&[prefix, suffix], Some(b"old"), Version { hlc: 10, node: 1 }, 1000).unwrap();
            }
        }
        let holder = if prefix == b'a' { 1 } else { 2 };
        let mut s = c.transport.store(holder).borrow_mut();
        for suffix in [b'1', b'2', b'9'] {
            s.apply_at(&[prefix, suffix], Some(b"new"), Version { hlc: 20, node: 1 }, 1000).unwrap();
        }
    }
    let scans = [(Some(&b"a"[..]), Some(&b"b"[..]), None), (Some(&b"z"[..]), None, None)];
    for keys_only in [false, true] {
        let got = run(c.scan_many_in(ZONE, &scans, 3, keys_only)).unwrap();
        for (i, &(lo, hi, _)) in scans.iter().enumerate() {
            let reference = run(c.scan_in(ZONE, lo, hi, 3, None)).unwrap();
            assert_eq!(got[i].iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), reference.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>());
            assert_eq!(got[i].last().unwrap().1.version.hlc, 20, "a far-ahead old page must not escape the other replica's smaller coverage");
        }
    }
}

#[test]
fn range_delete_hides_older_keys_and_newer_writes_win_before_and_after_the_rt() {
    let c = rt_cluster(&[b"m/030"]);
    for i in 0..60 {
        write(&c, &key(i), b"old");
    }
    write(&c, b"a", b"old");
    write(&c, b"z", b"old");
    let old = run(c.read_in(ZONE, &key(15))).unwrap().unwrap().version;

    let lagging = 3;
    *c.transport.held.borrow_mut() = Some(lagging);
    let version = run(c.delete_range_in(ZONE, Some(&key(10)), Some(&key(50)), WriteConcern::Majority)).unwrap();
    let calls = c.transport.deletes.borrow().clone();
    assert_eq!(calls.len(), 3, "one request per replica: {calls:?}");
    for (_, sync, v, slices) in &calls {
        assert!(!sync);
        assert_eq!(*v, version, "one version for every slice");
        assert_eq!(slices, &vec![owned(&key(10), b"m/030"), owned(b"m/030", &key(50))], "the span is clipped per range");
    }

    let before_rt = write(&c, &key(40), b"before-rt");
    assert!(before_rt > version);
    for i in 0..3 {
        let want: Vec<Vec<u8>> = [b"a".to_vec()]
            .into_iter()
            .chain((0..10).map(key))
            .chain(std::iter::once(key(40)))
            .chain((50..60).map(key))
            .chain(std::iter::once(b"z".to_vec()))
            .collect();
        c.transport.inner.set_scan_page_entries(if i == 0 { usize::MAX } else { 4 });
        for r in [1, 2, 3] {
            let got: Vec<Vec<u8>> = scan_keys(&c, r).into_iter().map(|(k, _)| k).collect();
            assert_eq!(got, want, "R {r}, page {i}: the lagging replica's covered keys are hidden by the holders' fragments");
        }
    }
    c.transport.release_held();
    let after_rt = write(&c, &key(20), b"after-rt");
    assert!(after_rt > version);

    let tombstone = Some(Stored { version, value: None });
    for node in 1..=3 {
        let s = c.transport.store(node).borrow();
        assert_eq!(s.get_versioned(&key(15)).unwrap(), tombstone, "node {node}");
        assert_eq!(s.get(&key(40)).unwrap().as_deref(), Some(&b"before-rt"[..]), "node {node}");
        assert_eq!(s.get(&key(20)).unwrap().as_deref(), Some(&b"after-rt"[..]), "node {node}");
    }
    let got = scan_keys(&c, 2);
    let want: Vec<(Vec<u8>, Vec<u8>)> = [(b"a".to_vec(), b"old".to_vec())]
        .into_iter()
        .chain((0..10).map(|i| (key(i), b"old".to_vec())))
        .chain([(key(20), b"after-rt".to_vec()), (key(40), b"before-rt".to_vec())])
        .chain((50..60).map(|i| (key(i), b"old".to_vec())))
        .chain(std::iter::once((b"z".to_vec(), b"old".to_vec())))
        .collect();
    assert_eq!(got, want);
    assert_eq!(run(c.scan_in(ZONE, Some(&key(10)), Some(&key(50)), 2, Some(1))).unwrap(), vec![(key(20), b"after-rt".to_vec())]);
    assert_eq!(run(c.read_in(ZONE, &key(15))).unwrap(), None);
    assert_eq!(run(c.read_in(ZONE, &key(40))).unwrap().unwrap().value.as_deref(), Some(&b"before-rt"[..]));
    assert_eq!(run(c.read_in(ZONE, b"m/012-never-written")).unwrap(), None);

    assert_eq!(
        run(c.cas_in(ZONE, &key(15), b"cas", Some(old))).unwrap(),
        CasResult::Mismatch { current: Some(version) },
        "CAS sees a covered key as a tombstone at the range delete's version"
    );
    assert!(matches!(run(c.cas_in(ZONE, &key(15), b"cas", Some(version))).unwrap(), CasResult::Applied(_)));
    assert_eq!(run(c.read_in(ZONE, &key(15))).unwrap().unwrap().value.as_deref(), Some(&b"cas"[..]));
}

#[test]
fn stragglers_older_than_the_range_delete_lose_on_every_apply_path() {
    let c = rt_cluster(&[]);
    for i in 0..20 {
        write(&c, &key(i), b"old");
    }
    let version = run(c.delete_range_in(ZONE, Some(b"m/"), Some(b"m0"), WriteOptions::device_durable(WriteConcern::All))).unwrap();
    assert!(c.transport.deletes.borrow().iter().all(|(_, sync, _, _)| *sync), "Persistence::Device syncs the replicas");
    let straggler = Version { hlc: version.hlc - 1, node: 77 };
    let never = b"m/never-held".to_vec();
    for node in 1..=3 {
        for k in [key(5), never.clone()] {
            assert_eq!(
                run(c.transport.apply(node, ZONE, &k, Some(b"late"), straggler)).unwrap(),
                Applied::Lost(version),
                "node {node} {k:?}"
            );
        }
        let batch = [BatchEntry { key: &key(6), value: Some(b"late"), version: straggler }, BatchEntry { key: &never, value: None, version: straggler }];
        assert_eq!(run(c.transport.apply_batch(node, ZONE, true, &batch)), vec![BatchAck::Applied(Applied::Lost(version)); 2]);
        assert_eq!(c.transport.store(node).borrow().engine().physical_version(&never).unwrap(), None, "nothing is stored for a key the replica never held");
    }
    assert!(scan_keys(&c, 3).is_empty());
    assert_eq!(run(c.read_in(ZONE, &key(5))).unwrap(), None);
    assert_eq!(run(c.read_in(ZONE, &never)).unwrap(), None);
    let newer = write(&c, &never, b"newer");
    assert!(newer > version);
    assert_eq!(scan_keys(&c, 3), vec![(never, b"newer".to_vec())]);
}

#[test]
fn an_r2_scan_hides_every_covered_key_over_a_multi_page_window_when_a_replica_missed_the_rt() {
    let c = rt_cluster(&[b"m/100"]);
    for i in 0..200 {
        write(&c, &key(i), b"old");
    }
    let lagging = c.replicas_for(ZONE, &key(0)).unwrap()[0];
    c.transport.inner.set_down(lagging, true);
    run(c.delete_range_in(ZONE, Some(&key(5)), Some(&key(190)), WriteConcern::Majority)).unwrap();
    c.transport.inner.set_down(lagging, false);
    assert_eq!(c.transport.store(lagging).borrow().get(&key(100)).unwrap().as_deref(), Some(&b"old"[..]), "the lagging replica still holds covered keys");
    let want: Vec<Vec<u8>> = (0..5).chain(190..200).map(key).collect();
    for page in [1, 3, 7] {
        c.transport.inner.set_scan_page_entries(page);
        for (r, prefetch) in [(2, false), (2, true), (3, false)] {
            c.set_scan_prefetch(prefetch);
            let got: Vec<Vec<u8>> = scan_keys(&c, r).into_iter().map(|(k, _)| k).collect();
            assert_eq!(got, want, "page {page} R {r} prefetch {prefetch}");
            let limited = run(c.scan_in(ZONE, Some(&key(3)), None, r, Some(4))).unwrap();
            assert_eq!(limited.into_iter().map(|(k, _)| k).collect::<Vec<_>>(), vec![key(3), key(4), key(190), key(191)]);
        }
    }
}

#[test]
fn range_delete_refuses_bad_spans_unsorted_zones_and_unsupported_replicas_before_stamping() {
    let c = rt_cluster(&[]);
    for (lo, hi) in [(Some(&b"b"[..]), Some(&b"a"[..])), (Some(b"a"), Some(b"a")), (None, Some(b""))] {
        assert_eq!(run(c.delete_range_in(ZONE, lo, hi, WriteConcern::Majority)), Err(CoordError::InvalidRange));
    }
    assert_eq!(run(c.delete_range_in(DEFAULT_ZONE, None, None, WriteConcern::Majority)), Err(CoordError::NotSorted(DEFAULT_ZONE)));
    assert_eq!(run(c.delete_range_in(UNSORTED, None, None, WriteConcern::Majority)), Err(CoordError::NotSorted(UNSORTED)));
    assert_eq!(run(c.delete_range_in(9, None, None, WriteConcern::Majority)), Err(CoordError::UnknownZone(9)));
    for options in [
        WriteOptions::buffered(WriteConcern::Quorum(0)),
        WriteOptions::buffered(WriteConcern::Quorum(4)),
        WriteOptions::device_durable(WriteConcern::Unacknowledged),
    ] {
        assert_eq!(
            run(c.delete_range_in(ZONE, None, None, options)),
            Err(CoordError::InvalidWriteOptions { options, replicas: 3 })
        );
    }
    assert!(c.transport.deletes.borrow().is_empty());

    let mut c = c;
    c.transport.unsupported = Some(2);
    write(&c, b"k", b"v");
    assert_eq!(run(c.delete_range_in(ZONE, None, None, WriteConcern::One)), Err(CoordError::Unsupported(2)));
    assert!(c.transport.deletes.borrow().is_empty(), "no replica is sent anything");
    assert_eq!(scan_keys(&c, 3).len(), 1);
}

#[test]
fn a_range_delete_that_misses_w_names_its_slices_and_keeps_what_landed() {
    let c = rt_cluster(&[b"m/010"]);
    for i in 0..20 {
        write(&c, &key(i), b"old");
    }
    c.transport.inner.set_down(3, true);
    let err = run(c.delete_range_in(ZONE, Some(&key(5)), Some(&key(15)), WriteConcern::All)).unwrap_err();
    let CoordError::RangeDeleteIncomplete { version, missed } = err else {
        panic!("expected RangeDeleteIncomplete, got {err:?}");
    };
    assert_eq!(
        missed,
        vec![
            RangeDeleteMiss { lo: Some(key(5)), hi: Some(key(10)), error: CoordError::QuorumFailed { needed: 3, got: 2 } },
            RangeDeleteMiss { lo: Some(key(10)), hi: Some(key(15)), error: CoordError::QuorumFailed { needed: 3, got: 2 } },
        ]
    );
    for node in [1, 2] {
        assert_eq!(c.transport.store(node).borrow().get_versioned(&key(7)).unwrap(), Some(Stored { version, value: None }));
    }
    c.transport.inner.set_down(3, false);
    let retried = run(c.delete_range_in(ZONE, Some(&key(5)), Some(&key(15)), WriteConcern::All)).unwrap();
    assert!(retried > version, "a retry stamps a newer version that covers the same keys");
    let got: Vec<Vec<u8>> = scan_keys(&c, 3).into_iter().map(|(k, _)| k).collect();
    assert_eq!(got, (0..5).chain(15..20).map(key).collect::<Vec<_>>());
}

#[test]
fn a_stale_epoch_refusal_refreshes_and_retries_the_missed_slices_with_the_same_version() {
    let c = rt_cluster(&[b"m/010"]);
    for i in 0..20 {
        write(&c, &key(i), b"old");
    }
    c.transport.stale_once.borrow_mut().push(1);
    let version = run(c.delete_range_in(ZONE, Some(&key(5)), Some(&key(15)), WriteConcern::All)).unwrap();
    let calls = c.transport.deletes.borrow().clone();
    assert_eq!(calls.iter().filter(|(n, ..)| *n == 1).count(), 2, "{calls:?}");
    assert!(calls.iter().all(|(_, _, v, _)| *v == version), "the retry reuses the version");
    for node in 1..=3 {
        assert_eq!(c.transport.store(node).borrow().get_versioned(&key(12)).unwrap(), Some(Stored { version, value: None }));
    }

    let c = c.with_manual_refresh();
    c.transport.stale_once.borrow_mut().push(1);
    let err = run(c.delete_range_in(ZONE, Some(&key(5)), Some(&key(15)), WriteConcern::All)).unwrap_err();
    assert!(
        matches!(&err, CoordError::RangeDeleteIncomplete { missed, .. } if missed.iter().all(|m| m.error == CoordError::StaleRing { epoch: 5 })),
        "{err:?}"
    );
}

type Frag<'a> = (&'a [u8], Option<&'a [u8]>, u64);

fn page(more: bool, entries: &[(&[u8], u64)], fragments: &[Frag<'_>], resume: Option<&[u8]>) -> ScanPage {
    ScanPage {
        more,
        entries: entries
            .iter()
            .map(|(k, hlc)| (k.to_vec(), Stored { version: Version { hlc: *hlc, node: 1 }, value: Some(b"v".to_vec()) }))
            .collect(),
        fragments: fragments
            .iter()
            .map(|(lo, hi, hlc)| RtFragment {
                lo: (!lo.is_empty()).then(|| lo.to_vec()),
                hi: hi.map(<[u8]>::to_vec),
                version: Version { hlc: *hlc, node: 1 },
            })
            .collect(),
        resume: resume.map(<[u8]>::to_vec),
    }
}

#[test]
fn scan_merge_uses_resume_keys_as_coverage_and_hides_winners_under_any_responders_fragment() {
    assert_eq!(scan_merge_bound(&[page(true, &[], &[(b"", None, 5)], Some(b"q"))]), Some(b"q".to_vec()));
    assert_eq!(
        scan_merge_bound(&[page(true, &[(b"a", 1), (b"z", 1)], &[], None), page(true, &[], &[], Some(b"m"))]),
        Some(b"m".to_vec()),
        "a resume key bounds the round like any coverage"
    );
    let merged = scan_merge_lww(
        vec![
            page(false, &[(b"a", 1), (b"c", 1), (b"d", 9), (b"f", 1)], &[], None),
            page(false, &[(b"b", 1)], &[(b"c", Some(b"e"), 5)], None),
            page(false, &[], &[(b"", Some(b"b"), 1), (b"f", None, 3)], None),
        ],
        None,
    );
    assert_eq!(merged.keys().cloned().collect::<Vec<_>>(), vec![b"b".to_vec(), b"d".to_vec()], "a cover equal to the winner's version hides it; a newer winner survives");
}

struct ScriptedScan {
    inner: LocalCluster<MemBlockDevice>,
    pages: RefCell<Vec<ScanPage>>,
    requests: RefCell<Vec<Option<Vec<u8>>>>,
}

impl Transport for ScriptedScan {
    async fn apply(&self, node: NodeId, zone_id: ZoneId, key: &[u8], value: Option<&[u8]>, version: Version) -> Result<Applied, TransportError> {
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(&self, node: NodeId, zone_id: ZoneId, key: &[u8]) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn scan(&self, _node: NodeId, _zone_id: ZoneId, lo: Option<&[u8]>, _hi: Option<&[u8]>, _max: u32) -> Result<ScanPage, TransportError> {
        self.requests.borrow_mut().push(lo.map(<[u8]>::to_vec));
        let mut pages = self.pages.borrow_mut();
        Ok(if pages.is_empty() { ScanPage::default() } else { pages.remove(0) })
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

#[test]
fn a_single_responder_round_filters_with_its_own_fragments_and_follows_empty_resume_pages() {
    let mut lc = LocalCluster::new();
    lc.define_zone(ZONE, ZoneConfig { name: "s".into(), rf: 1, ordering: ZOrdering::Sorted, scope: None });
    lc.insert_zone(1, ZONE, rt_replica(1, Ordering::Sorted));
    let transport = ScriptedScan {
        inner: lc,
        pages: RefCell::new(vec![
            page(true, &[(b"a", 1), (b"b", 4)], &[(b"b", Some(b"k"), 5)], Some(b"k")),
            page(true, &[], &[(b"k", None, 5)], Some(b"p")),
            page(false, &[(b"p", 1), (b"q", 9)], &[(b"k", None, 5)], None),
        ]),
        requests: RefCell::new(Vec::new()),
    };
    let c = Coordinator::new(0, Ring::from_members(1, 64, [1]), 1, transport);
    run(c.refresh_catalog(1)).unwrap();
    let got: Vec<Vec<u8>> = run(c.scan_in(ZONE, None, None, 1, None)).unwrap().into_iter().map(|(k, _)| k).collect();
    assert_eq!(got, vec![b"a".to_vec(), b"q".to_vec()], "an entry the replica examined before the RT arrived is hidden by its own fragment");
    assert_eq!(*c.transport.requests.borrow(), vec![None, Some(b"k".to_vec()), Some(b"p".to_vec())], "resume keys are the next cursor, even on an empty page");

    c.transport.pages.borrow_mut().extend([page(true, &[], &[], None)]);
    assert!(matches!(run(c.scan_in(ZONE, None, None, 1, None)), Err(CoordError::ScanQuorumFailed { .. })), "an empty truncated page without a resume key is still a violation");
    c.transport.pages.borrow_mut().extend([page(true, &[], &[], Some(b""))]);
    assert!(matches!(run(c.scan_in(ZONE, None, None, 1, None)), Err(CoordError::ScanQuorumFailed { .. })), "a resume key must advance");
}

fn ahead_of_clock(ms: u64) -> Version {
    Version { hlc: (crate::hlc::physical_now_ms() + ms) << 16, node: 99 }
}

fn plant_ahead(c: &Coordinator<RtCluster>, k: &[u8], value: Option<&[u8]>, ahead: Version) {
    for node in 1..=3 {
        assert_eq!(run(c.transport.apply(node, ZONE, k, value, ahead)).unwrap(), Applied::Won, "node {node}");
    }
}

#[test]
fn a_point_delete_after_scanning_a_key_written_ahead_of_the_coordinator_clock_wins() {
    for (r, splits) in [(1, &[][..]), (3, &[][..]), (2, &[&b"m/010"[..]][..])] {
        let c = rt_cluster(splits);
        let ahead = ahead_of_clock(300);
        plant_ahead(&c, &key(15), Some(b"v"), ahead);
        assert_eq!(scan_keys(&c, r), vec![(key(15), b"v".to_vec())], "R {r}");
        let deleted = run(c.delete_in(ZONE, &key(15), WriteConcern::All)).unwrap();
        assert!(deleted > ahead, "R {r}: the delete is stamped above the version the scan returned: {deleted:?} vs {ahead:?}");
        assert!(scan_keys(&c, 3).is_empty(), "R {r}: the scanned-then-deleted key stays deleted");
        assert_eq!(run(c.read_in(ZONE, &key(15))).unwrap(), None, "R {r}");
        for node in 1..=3 {
            assert_eq!(c.transport.store(node).borrow().get_versioned(&key(15)).unwrap(), Some(Stored { version: deleted, value: None }), "R {r} node {node}");
        }
    }
}

#[test]
fn a_range_delete_after_scanning_a_key_written_ahead_of_the_coordinator_clock_covers_it() {
    for (r, splits) in [(1, &[][..]), (3, &[][..]), (2, &[&b"m/010"[..]][..])] {
        let c = rt_cluster(splits);
        let ahead = ahead_of_clock(300);
        plant_ahead(&c, &key(15), Some(b"v"), ahead);
        assert_eq!(scan_keys(&c, r), vec![(key(15), b"v".to_vec())], "R {r}");
        let deleted = run(c.delete_range_in(ZONE, Some(&key(0)), Some(&key(100)), WriteConcern::All)).unwrap();
        assert!(deleted > ahead, "R {r}: the range delete is stamped above the version the scan returned: {deleted:?} vs {ahead:?}");
        assert!(scan_keys(&c, 3).is_empty(), "R {r}: the range delete covers the scanned key");
        assert_eq!(run(c.read_in(ZONE, &key(15))).unwrap(), None, "R {r}");
    }
}

#[test]
fn a_scan_that_sees_only_a_tombstone_or_a_fragment_ahead_of_the_coordinator_clock_lets_the_next_write_win() {
    for r in [1, 3] {
        let c = rt_cluster(&[]);
        let ahead = ahead_of_clock(300);
        plant_ahead(&c, &key(15), None, ahead);
        assert!(scan_keys(&c, r).is_empty(), "R {r}");
        let written = write(&c, &key(15), b"new");
        assert!(written > ahead, "R {r}: a write after scanning a tombstone is stamped above it: {written:?} vs {ahead:?}");
        assert_eq!(scan_keys(&c, 3), vec![(key(15), b"new".to_vec())], "R {r}");

        let c = rt_cluster(&[]);
        let ahead = ahead_of_clock(300);
        let span: Vec<WireSlice> = vec![(Some(&b"m/"[..]), Some(&b"m0"[..]))];
        for node in 1..=3 {
            let acks = run(c.transport.range_delete(node, ZONE, false, ahead, &span));
            assert_eq!(acks, vec![BatchAck::Applied(Applied::Won)], "node {node}");
        }
        assert!(scan_keys(&c, r).is_empty(), "R {r}");
        let written = write(&c, &key(15), b"new");
        assert!(written > ahead, "R {r}: a write after scanning a fragment is stamped above it: {written:?} vs {ahead:?}");
        assert_eq!(scan_keys(&c, 3), vec![(key(15), b"new".to_vec())], "R {r}");
    }
}
