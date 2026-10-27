use std::cell::RefCell;
use std::collections::HashSet;

use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

use crate::antientropy::{reconcile_scoped_zone, AePeer, LocalPeer, DEFAULT_BUCKETS};
use crate::coord::TransportError;
use crate::hlc::Version;
use crate::partition::RangeMap;
use crate::replica::{Applied, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::rt_sync::{exchange, local_fragments, respond, RtExchange, RtScope};
use crate::zone::ZoneId;

type Store = VersionedStore<MemBlockDevice>;

const ZONE: ZoneId = 42;
const RF: usize = 3;

fn k(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn at(i: u64) -> Version {
    Version { hlc: i << crate::hlc::COUNTER_BITS, node: 1 }
}

fn ring() -> Ring {
    Ring::from_members(RF, 64, [1, 2, 3])
}

fn map() -> RangeMap {
    RangeMap::from_splits([k(250), k(500), k(800)])
}

fn zone() -> crate::zone::ZoneConfig {
    crate::zone::ZoneConfig { name: "rt-sync".into(), rf: RF, ordering: crate::zone::Ordering::Sorted, scope: None }
}

fn store(node: NodeId) -> Store {
    let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
    let s = Store::new(Engine::create_prewiped(MemBlockDevice::new(256, 1 << 16), cfg), node)
        .unwrap()
        .with_max_skew_ms(0);
    s.prepare_zone_seed((1, 1, 1), &ring(), ZONE, Some(zone()), Some(map()));
    s
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, || {})
}

fn tree_ready(s: &Store) {
    assert!(block_on(crate::replica::ensure_tree_ready_cooperative(s, 1, &HashSet::from([0]))).unwrap());
}

struct Peer<'a> {
    store: &'a RefCell<Store>,
    node: NodeId,
    ring: &'a Ring,
    map: &'a RangeMap,
}

impl AePeer for Peer<'_> {
    async fn tree(
        &self,
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &[u32],
        requester: Option<NodeId>,
    ) -> Result<Vec<(u32, u64)>, TransportError> {
        let s = self.store.borrow();
        LocalPeer(&s).tree(zone_id, level, scope_g, scope, requester).await
    }

    async fn bucket_digests(
        &self,
        zone_id: ZoneId,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
    ) -> Result<crate::replica::DigestCursorPage, TransportError> {
        let s = self.store.borrow();
        LocalPeer(&s).bucket_digests(zone_id, num_buckets, ids, after).await
    }

    async fn values(&self, zone_id: ZoneId, keys: &[Vec<u8>]) -> Result<crate::replica::EntryPage, TransportError> {
        let s = self.store.borrow();
        LocalPeer(&s).values(zone_id, keys).await
    }

    async fn range_tombstones(
        &self,
        zone_id: ZoneId,
        requester: Option<NodeId>,
        digest: u64,
        after: Option<&[u8]>,
    ) -> Result<Option<vals_client::ae::RtSyncPage>, TransportError> {
        let scope = RtScope::new(self.map, self.ring, zone_id, RF, None, self.node, requester, (0, 1));
        let fragments = local_fragments(&self.store.borrow());
        Ok(Some(respond(fragments, Some(&scope), digest, after)))
    }
}

struct Pass {
    pulls: usize,
    exchanges: Vec<RtExchange>,
}

fn ae_pass(nodes: &[(NodeId, RefCell<Store>)], ring: &Ring, map: &RangeMap, rt_exchange: bool) -> Pass {
    let mut pass = Pass { pulls: 0, exchanges: Vec::new() };
    for (node, local) in nodes {
        for (peer_node, peer_store) in nodes {
            if peer_node == node {
                continue;
            }
            let peer = Peer { store: peer_store, node: *peer_node, ring, map };
            if rt_exchange {
                let scope = RtScope::new(map, ring, ZONE, RF, None, *node, Some(*peer_node), (0, 1));
                pass.exchanges.push(block_on(exchange(local, &peer, &scope)).unwrap());
            }
            tree_ready(&local.borrow());
            let (applied, _) = block_on(reconcile_scoped_zone(
                local,
                &peer,
                DEFAULT_BUCKETS,
                ring,
                *node,
                *peer_node,
                ZONE,
                RF,
                None,
                Some(map),
                (0, 1),
                None,
            ))
            .unwrap();
            pass.pulls += applied;
        }
    }
    pass
}

fn converge(rt_exchange: bool) -> (Vec<(NodeId, RefCell<Store>)>, Vec<Pass>) {
    let rt = at(5_000);
    let (mut s1, mut s2, mut s3) = (store(1), store(2), store(3));
    for i in 0..1000 {
        for (n, s) in [(1, &mut s1), (2, &mut s2), (3, &mut s3)] {
            if n == 2 && (100..400).contains(&i) && i % 2 == 1 {
                continue;
            }
            assert_eq!(s.apply_at(&k(i), Some(b"v"), at(i + 1), 0).unwrap(), Applied::Won);
        }
        if i % 150 == 149 {
            s1.flush().unwrap();
        }
    }
    s3.flush().unwrap();
    for i in 900..920 {
        s3.apply_at(&k(i), Some(b"v3"), at(4_000 + i), 0).unwrap();
    }
    for s in [&mut s1, &mut s2] {
        assert_eq!(s.apply_range_delete(Some(&k(100)), Some(&k(400)), rt, 0).unwrap(), Applied::Won);
    }
    s1.apply_at(&k(200), Some(b"newer"), at(6_000), 0).unwrap();
    tree_ready(&s1);
    s1.flush().unwrap();
    s1.compact_runs(0).unwrap();
    assert!(
        s1.engine().physical_version(&k(150)).unwrap().is_none(),
        "node 1 compacted its covered entries away"
    );
    assert!(s2.engine().physical_version(&k(150)).unwrap().is_some(), "node 2 still holds them in RAM");

    let nodes: Vec<(NodeId, RefCell<Store>)> =
        vec![(1, RefCell::new(s1)), (2, RefCell::new(s2)), (3, RefCell::new(s3))];
    let (ring, map) = (ring(), map());
    let passes = (0..4).map(|_| ae_pass(&nodes, &ring, &map, rt_exchange)).collect();
    (nodes, passes)
}

#[test]
fn ae_converges_leaves_and_fragments_and_stops_pulling_when_a_replica_missed_the_range_delete() {
    let (nodes, passes) = converge(true);
    let pulls: Vec<usize> = passes.iter().map(|p| p.pulls).collect();
    assert!(pulls[0] > 0, "the first pass repairs uncovered keys: {pulls:?}");
    assert_eq!(&pulls[2..], &[0, 0], "pulls stop once F and the leaves converge: {pulls:?}");
    assert!(passes[0].exchanges.iter().any(|e| matches!(e, RtExchange::Pulled { fragments, .. } if *fragments > 0)));
    assert!(
        passes[3].exchanges.iter().all(|e| *e == RtExchange::Same),
        "{:?}",
        passes[3].exchanges
    );

    let rt = at(5_000);
    let f1 = local_fragments(&nodes[0].1.borrow());
    assert_eq!(f1.len(), 1);
    for (node, s) in &nodes {
        let s = s.borrow();
        assert_eq!(local_fragments(&s), f1, "node {node} F");
        for (peer, _) in &nodes {
            if peer == node {
                continue;
            }
            let mine = s.scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(*peer)).unwrap();
            let theirs = nodes[*peer as usize - 1]
                .1
                .borrow()
                .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(*node))
                .unwrap();
            assert_eq!(mine, theirs, "leaves of node {node} vs node {peer}");
        }
        for i in [100, 151, 199, 201, 399] {
            assert_eq!(
                s.get_versioned(&k(i)).unwrap(),
                Some(Stored { version: rt, value: None }),
                "node {node} k{i}"
            );
        }
        assert_eq!(s.get(&k(200)).unwrap().as_deref(), Some(&b"newer"[..]), "node {node}");
        assert_eq!(s.get(&k(910)).unwrap().as_deref(), Some(&b"v3"[..]), "node {node}");
        assert_eq!(s.get(&k(400)).unwrap().as_deref(), Some(&b"v"[..]), "node {node}");
    }
    let s2 = nodes[1].1.borrow();
    for i in (101..400).step_by(2) {
        assert_eq!(s2.engine().physical_version(&k(i)).unwrap(), None, "node 2 stored covered k{i}");
    }
}

#[test]
fn replicas_that_dropped_dominated_records_converge_with_replicas_that_kept_or_missed_them() {
    let (mut s1, mut s2, mut s3) = (store(1), store(2), store(3));
    for i in 0..1000 {
        for s in [&mut s1, &mut s2, &mut s3] {
            assert_eq!(s.apply_at(&k(i), Some(b"v"), at(i + 1), 0).unwrap(), Applied::Won);
        }
    }
    for s in [&mut s1, &mut s3] {
        tree_ready(s);
        s.flush().unwrap();
    }
    for j in 1..=40u64 {
        for (n, s) in [(1, &mut s1), (2, &mut s2), (3, &mut s3)] {
            if n == 3 && j > 10 {
                continue;
            }
            assert_eq!(s.apply_range_delete(Some(&k(0)), Some(&k(5 * j)), at(2_000 + j), 0).unwrap(), Applied::Won);
            assert_eq!(s.apply_range_delete(Some(&k(850)), Some(&k(900)), at(3_000 + j), 0).unwrap(), Applied::Won);
        }
        if j % 10 == 0 {
            tree_ready(&s1);
            s1.flush().unwrap();
        }
    }
    s3.flush().unwrap();
    s1.compact_runs(0).unwrap();
    assert_eq!(s1.engine().range_tombstone_record_count(), 2, "node 1 keeps one record per delete series");
    assert_eq!(s2.engine().range_tombstone_record_count(), 80, "node 2 keeps every record in its tail");
    assert_eq!(s3.engine().range_tombstone_record_count(), 20, "node 3 missed the later deletes");
    assert!(s1.engine().physical_version(&k(150)).unwrap().is_none(), "node 1 compacted its covered entries away");

    let nodes: Vec<(NodeId, RefCell<Store>)> = vec![(1, RefCell::new(s1)), (2, RefCell::new(s2)), (3, RefCell::new(s3))];
    let (ring, map) = (ring(), map());
    let passes: Vec<Pass> = (0..3).map(|_| ae_pass(&nodes, &ring, &map, true)).collect();
    let pulls: Vec<usize> = passes.iter().map(|p| p.pulls).collect();
    assert_eq!(&pulls[1..], &[0, 0], "pulls stop once F and the leaves converge: {pulls:?}");
    assert!(passes[2].exchanges.iter().all(|e| *e == RtExchange::Same), "{:?}", passes[2].exchanges);
    let f1 = local_fragments(&nodes[0].1.borrow());
    assert_eq!(f1.len(), 2);
    for (node, s) in &nodes {
        let s = s.borrow();
        assert_eq!(local_fragments(&s), f1, "node {node} F");
        for (peer, _) in &nodes {
            if peer != node {
                let mine = s.scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(*peer)).unwrap();
                let theirs = nodes[*peer as usize - 1].1.borrow().scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(*node)).unwrap();
                assert_eq!(mine, theirs, "leaves of node {node} vs node {peer}");
            }
        }
        for (i, cover) in [(0, Some(at(2_040))), (199, Some(at(2_040))), (870, Some(at(3_040)))] {
            assert_eq!(s.get_versioned(&k(i)).unwrap(), cover.map(|version| Stored { version, value: None }), "node {node} k{i}");
        }
        for i in [200, 849, 900] {
            assert_eq!(s.get(&k(i)).unwrap().as_deref(), Some(&b"v"[..]), "node {node} k{i}");
        }
    }
}

#[test]
fn without_the_exchange_a_replica_that_missed_the_range_delete_never_converges() {
    let (nodes, _) = converge(false);
    let s1 = nodes[0].1.borrow();
    let s3 = nodes[2].1.borrow();
    assert!(local_fragments(&s3).is_empty());
    assert_ne!(
        s1.scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(3)).unwrap(),
        s3.scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(1)).unwrap(),
    );
    assert_eq!(s3.get(&k(151)).unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn exchange_scopes_fragments_to_ranges_both_nodes_replicate_and_pages_them() {
    let ring = Ring::from_members(1, 64, [1, 2]);
    let map = RangeMap::from_splits((1..40).map(|i| k(i * 25)));
    let s1 = RefCell::new(store(1));
    let s2 = RefCell::new(store(2));
    for i in 0..2_000u64 {
        let rt = at(10_000 + i);
        s1.borrow_mut().apply_range_delete(Some(&k(i)), Some(&[k(i), b"~".to_vec()].concat()), rt, 0).unwrap();
    }
    let both = RtScope::new(&map, &ring, ZONE, 1, None, 2, Some(1), (0, 1));
    assert!(both.clip(local_fragments(&s1.borrow())).is_empty(), "RF 1: no range has both nodes");

    let ring2 = Ring::from_members(2, 64, [1, 2]);
    let scope = RtScope::new(&map, &ring2, ZONE, 2, None, 2, Some(1), (0, 1));
    let peer = Peer { store: &s1, node: 1, ring: &ring2, map: &map };
    let pulled = block_on(exchange(&s2, &peer, &scope)).unwrap();
    assert_eq!(pulled, RtExchange::Pulled { fragments: 2_000, rejected: 0 }, "more than one page");
    assert_eq!(local_fragments(&s2.borrow()), local_fragments(&s1.borrow()));
    assert_eq!(block_on(exchange(&s2, &peer, &scope)).unwrap(), RtExchange::Same);

    let mut sharded = 0;
    for shard in 0..2 {
        let half = RtScope::new(&map, &ring2, ZONE, 2, None, 2, Some(1), (shard, 2));
        let clipped = half.clip(local_fragments(&s1.borrow()));
        for f in &clipped {
            let index = map.range_index(&f.lo);
            assert_eq!((crate::ring::hash64(map.token_of(index)) % 2) as usize, shard);
        }
        sharded += clipped.len();
    }
    assert_eq!(sharded, 2_000, "each fragment routes to exactly one shard");
}

#[test]
fn exchange_pages_large_fragments_by_bytes_until_the_whole_set_converges() {
    let cfg = Config { seg_blocks: 64, reserved_blocks: 0, ordering: Ordering::Sorted };
    let big = |node| {
        let s = Store::new(Engine::create_prewiped(MemBlockDevice::new(4096, 1 << 14), cfg.clone()), node)
            .unwrap()
            .with_max_skew_ms(0);
        s.prepare_zone_seed((1, 1, 1), &ring(), ZONE, Some(zone()), Some(map()));
        RefCell::new(s)
    };
    let (s1, s2) = (big(1), big(2));
    let pad = vec![b'p'; 16 * 1024];
    for i in 0..100u64 {
        let lo = [k(i * 9), pad.clone()].concat();
        let hi = [lo.clone(), b"~".to_vec()].concat();
        s1.borrow_mut().apply_range_delete(Some(&lo), Some(&hi), at(10_000 + i), 0).unwrap();
    }
    let (ring, map) = (ring(), map());
    let mut pages = 0;
    let mut after: Option<Vec<u8>> = None;
    loop {
        let page = respond(local_fragments(&s1.borrow()), None, 0, after.as_deref());
        pages += 1;
        assert!(!page.fragments.is_empty() && page.fragments.len() < 40, "{}", page.fragments.len());
        after = page.fragments.last().map(|f| f.lo.clone().unwrap_or_default());
        if !page.more {
            break;
        }
    }
    assert!(pages >= 3, "a 1 MiB page holds about 32 of these fragments: {pages} pages");

    let scope = RtScope::new(&map, &ring, ZONE, RF, None, 2, Some(1), (0, 1));
    let peer = Peer { store: &s1, node: 1, ring: &ring, map: &map };
    assert_eq!(
        block_on(exchange(&s2, &peer, &scope)).unwrap(),
        RtExchange::Pulled { fragments: 100, rejected: 0 }
    );
    assert_eq!(local_fragments(&s2.borrow()), local_fragments(&s1.borrow()));
    assert_eq!(block_on(exchange(&s2, &peer, &scope)).unwrap(), RtExchange::Same);
}

struct OneFragmentPages<'a>(&'a RefCell<Store>, std::cell::Cell<usize>);

impl AePeer for OneFragmentPages<'_> {
    async fn tree(&self, _: ZoneId, _: u32, _: u32, _: &[u32], _: Option<NodeId>) -> Result<Vec<(u32, u64)>, TransportError> {
        Err(TransportError::Busy)
    }

    async fn bucket_digests(
        &self,
        _: ZoneId,
        _: u32,
        _: &[u32],
        _: Option<&[u8]>,
    ) -> Result<crate::replica::DigestCursorPage, TransportError> {
        Err(TransportError::Busy)
    }

    async fn values(&self, _: ZoneId, _: &[Vec<u8>]) -> Result<crate::replica::EntryPage, TransportError> {
        Err(TransportError::Busy)
    }

    async fn range_tombstones(
        &self,
        _: ZoneId,
        _: Option<NodeId>,
        digest: u64,
        after: Option<&[u8]>,
    ) -> Result<Option<vals_client::ae::RtSyncPage>, TransportError> {
        self.1.set(self.1.get() + 1);
        let mut page = respond(local_fragments(&self.0.borrow()), None, digest, after);
        if page.fragments.len() > 1 {
            page.fragments.truncate(1);
            page.more = true;
        }
        Ok(Some(page))
    }
}

#[test]
fn exchange_follows_hundreds_of_pages_to_the_end() {
    let (s1, s2) = (RefCell::new(store(1)), RefCell::new(store(2)));
    for i in 0..300u64 {
        let hi = [k(i), b"~".to_vec()].concat();
        s1.borrow_mut().apply_range_delete(Some(&k(i)), Some(&hi), at(10_000 + i), 0).unwrap();
    }
    let (ring, map) = (ring(), map());
    let scope = RtScope::new(&map, &ring, ZONE, RF, None, 2, Some(1), (0, 1));
    let peer = OneFragmentPages(&s1, std::cell::Cell::new(0));
    assert_eq!(
        block_on(exchange(&s2, &peer, &scope)).unwrap(),
        RtExchange::Pulled { fragments: 300, rejected: 0 }
    );
    assert_eq!(peer.1.get(), 300);
    assert_eq!(local_fragments(&s2.borrow()), local_fragments(&s1.borrow()));
}
