use super::*;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering, RtScanStats};

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use crate::hlc::{physical_now_ms, Version};
use crate::replica::{Applied, RecountStep, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::wire::{Response, WireSlice};
use crate::zone::{Catalog, Ordering as ZOrdering, ZoneConfig, ZoneId};

const ZONE: ZoneId = 7;
const LIMIT: usize = 10;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, crate::reactor::poll_io)
}

fn cfg() -> Config {
    Config { seg_blocks: 64, reserved_blocks: 0, ordering: Ordering::Sorted }
}

type Store = VersionedStore<MemBlockDevice>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Tally {
    rounds: u64,
    reply_bytes: u64,
    resume_pages: u64,
    filtered_entries: u64,
    skipped_blocks: u64,
    block_reads: u64,
    index_probes: u64,
}

impl Tally {
    fn add(&mut self, stats: RtScanStats) {
        self.filtered_entries += stats.filtered_entries;
        self.skipped_blocks += stats.skipped_blocks;
        self.block_reads += stats.block_reads;
        self.index_probes += stats.index_probes;
    }
}

struct ServeCluster {
    stores: RefCell<Vec<(NodeId, Rc<RefCell<Store>>)>>,
    catalog: Catalog,
    tally: Cell<Tally>,
}

impl ServeCluster {
    fn store(&self, node: NodeId) -> Rc<RefCell<Store>> {
        self.stores.borrow().iter().find(|(n, _)| *n == node).map(|(_, s)| s.clone()).expect("a store for the node")
    }

    fn each(&self, mut f: impl FnMut(&mut Store)) {
        for (_, s) in self.stores.borrow().iter() {
            f(&mut s.borrow_mut());
        }
    }

    fn reopen(&self) {
        for (node, slot) in self.stores.borrow_mut().iter_mut() {
            let dev = {
                let mut s = slot.borrow_mut();
                s.flush().unwrap();
                s.sync().unwrap();
                s.engine().device().share()
            };
            *slot = Rc::new(RefCell::new(VersionedStore::open(dev, cfg(), *node).unwrap()));
        }
        self.each(|s| {
            for _ in 0..1_000_000 {
                if s.seed_step_cooperative().unwrap() {
                    break;
                }
            }
            assert!(!s.rt_fold_pending(), "the reseed accounts the range tombstone");
        });
    }
}

impl Transport for ServeCluster {
    async fn apply(&self, node: NodeId, _zone_id: ZoneId, key: &[u8], value: Option<&[u8]>, version: Version) -> Result<Applied, TransportError> {
        self.store(node).borrow_mut().apply(key, value, version).map_err(|e| TransportError::Engine(e.to_string()))
    }

    async fn read(&self, node: NodeId, _zone_id: ZoneId, key: &[u8]) -> Result<Option<Stored>, TransportError> {
        self.store(node).borrow().get_versioned(key).map_err(|e| TransportError::Engine(e.to_string()))
    }

    async fn scan(&self, node: NodeId, _zone_id: ZoneId, lo: Option<&[u8]>, hi: Option<&[u8]>, max: u32) -> Result<ScanPage, TransportError> {
        let store = self.store(node);
        let dev = store.borrow().device_handle();
        let gate = crate::net::ReadGate::new();
        let (response, stats) =
            crate::net::serve_scan_counted(&store, dev.as_ref(), &gate, lo, hi, max, true, Default::default()).await;
        let mut tally = self.tally.get();
        tally.rounds += 1;
        tally.reply_bytes += crate::wire::encode_response_payload(&response).len() as u64;
        tally.add(stats);
        let page = match response {
            Response::Scan { entries, more, fragments, resume } => {
                tally.resume_pages += resume.is_some() as u64;
                Ok(ScanPage { entries, more, fragments, resume })
            }
            Response::Busy => Err(TransportError::Busy),
            other => Err(TransportError::Remote(format!("{other:?}"))),
        };
        self.tally.set(tally);
        page
    }

    async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
        Ok(RingView::default())
    }

    async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
        Ok(self.catalog.clone())
    }

    fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
        Some(true)
    }

    async fn range_delete(&self, node: NodeId, _zone_id: ZoneId, _sync: bool, version: Version, slices: &[WireSlice<'_>]) -> Vec<BatchAck> {
        let store = self.store(node);
        let mut s = store.borrow_mut();
        slices
            .iter()
            .map(|&(lo, hi)| BatchAck::Applied(s.apply_range_delete(lo, hi, version, physical_now_ms()).unwrap()))
            .collect()
    }
}

fn span_key(i: u32) -> Vec<u8> {
    format!("q/{i:07}").into_bytes()
}

fn live_key(prefix: &str, i: u32) -> Vec<u8> {
    format!("{prefix}/{i:03}").into_bytes()
}

fn build_replica(node: NodeId, n: u32) -> Store {
    let blocks = if n > 50_000 { 1 << 14 } else { 1 << 12 };
    let mut s = VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(4096, blocks), cfg()), node).unwrap();
    let old = Version { hlc: (physical_now_ms() - 60_000) << crate::hlc::COUNTER_BITS, node: 50 };
    let wall = physical_now_ms();
    let put = |s: &mut Store, k: &[u8]| {
        assert_eq!(s.apply_at(k, Some(b"value-0123456789"), old, wall).unwrap(), Applied::Won);
    };
    const EDGE: u32 = 500;
    for i in 0..10 {
        put(&mut s, &live_key("a", i));
    }
    for i in 0..EDGE {
        put(&mut s, &span_key(i));
    }
    s.flush().unwrap();
    for i in EDGE..n - EDGE {
        put(&mut s, &span_key(i));
    }
    s.flush().unwrap();
    for i in n - EDGE..n {
        put(&mut s, &span_key(i));
    }
    for i in 0..50 {
        put(&mut s, &live_key("z", i));
    }
    s.flush().unwrap();
    s
}

fn cluster(nodes: u32, r: usize, n: u32) -> Coordinator<ServeCluster> {
    let mut catalog = Catalog::new();
    catalog.insert(ZONE, ZoneConfig { name: "queue".into(), rf: nodes as usize, ordering: ZOrdering::Sorted, scope: None });
    let transport = ServeCluster {
        stores: RefCell::new((1..=nodes).map(|node| (node, Rc::new(RefCell::new(build_replica(node, n))))).collect()),
        catalog,
        tally: Cell::new(Tally::default()),
    };
    let c = Coordinator::new(0, Ring::from_members(nodes as usize, 64, 1..=nodes), r, transport);
    run(c.refresh_catalog(1)).unwrap();
    c
}

fn tree_blocks(c: &Coordinator<ServeCluster>) -> Vec<u64> {
    let mut out = Vec::new();
    c.transport.each(|s| out.push(s.engine().range_tombstone_max_tree_blocks()));
    out
}

fn measure(c: &Coordinator<ServeCluster>, r: usize, phase: &str) -> Tally {
    let want: Vec<Vec<u8>> = (0..LIMIT as u32).map(|i| live_key("z", i)).collect();
    for _ in 0..5 {
        let trees = tree_blocks(c);
        let built = vals_store::range_tombstone::rt_max_tree_built_blocks_total();
        c.transport.tally.set(Tally::default());
        let got = run(c.scan_in(ZONE, Some(b"q/"), None, r, Some(LIMIT))).unwrap();
        let tally = c.transport.tally.get();
        assert_eq!(got.into_iter().map(|(k, _)| k).collect::<Vec<_>>(), want, "{phase}");
        assert_eq!(tree_blocks(c), trees, "{phase}: the scan path builds no max tree");
        if vals_store::range_tombstone::rt_max_tree_built_blocks_total() == built {
            return tally;
        }
    }
    panic!("{phase}: vals_rt_max_tree_built_blocks_total moved during every measured scan");
}

fn fold(c: &Coordinator<ServeCluster>) {
    c.transport.each(|s| {
        for _ in 0..1_000_000 {
            if s.rt_fold_step().unwrap() == RecountStep::Done {
                break;
            }
        }
        assert!(!s.rt_fold_pending());
    });
}

fn head_of_queue(nodes: u32, r: usize, n: u32) -> (Tally, Vec<(&'static str, Tally)>) {
    let c = cluster(nodes, r, n);
    run(c.delete_range_in(ZONE, Some(b"q/"), Some(b"q0"), WriteConcern::All)).unwrap();
    let before_fold = measure(&c, r, "before the fold");
    fold(&c);
    assert!(tree_blocks(&c).iter().all(|&b| b > 0), "the first fold builds the trees");
    let mut out = vec![("after the fold", measure(&c, r, "after the fold"))];
    c.transport.reopen();
    out.push(("cold reopen", measure(&c, r, "cold reopen")));
    c.transport.each(|s| {
        s.compact_runs(physical_now_ms()).unwrap();
    });
    out.push(("after compaction", measure(&c, r, "after compaction")));
    c.transport.reopen();
    out.push(("cold reopen after compaction", measure(&c, r, "cold reopen after compaction")));
    (before_fold, out)
}

fn assert_head_of_queue_cost_is_independent_of_n(nodes: u32, r: usize) {
    let (_, small) = head_of_queue(nodes, r, 10_000);
    let (before_fold, large) = head_of_queue(nodes, r, 200_000);
    let budget = vals_store::SCAN_COVERED_BUDGET;
    eprintln!("head-of-queue {nodes} node(s) R {r}, before the fold, N=200k: {before_fold:?}");
    for ((phase, s), (_, l)) in small.iter().zip(&large) {
        eprintln!("head-of-queue {nodes} node(s) R {r}, {phase}: N=10k {s:?} N=200k {l:?}");
    }
    assert_eq!(before_fold.filtered_entries, nodes as u64 * 200_000, "before the fold, runs without a max tree are filtered entry by entry");
    assert!(before_fold.resume_pages > 0, "{before_fold:?}");
    assert!(
        before_fold.filtered_entries <= before_fold.rounds * budget.filtered_entries
            && before_fold.block_reads <= before_fold.rounds * budget.block_reads,
        "the covered-work budget caps every page: {before_fold:?}"
    );
    for ((phase, s), (_, l)) in small.iter().zip(&large) {
        let ctx = format!("{nodes} node(s) R {r}, {phase}: 10k {s:?} vs 200k {l:?}");
        assert_eq!(s.rounds, nodes as u64, "{ctx}: one page round per responder");
        assert_eq!((s.rounds, s.block_reads, s.filtered_entries, s.reply_bytes, s.resume_pages), (l.rounds, l.block_reads, l.filtered_entries, l.reply_bytes, l.resume_pages), "{ctx}");
        assert!(l.index_probes <= s.index_probes + 16 * nodes as u64, "{ctx}: index probes grow only logarithmically");
        if !phase.contains("compaction") {
            assert!(l.skipped_blocks > s.skipped_blocks && s.skipped_blocks > 0, "{ctx}: the covered span is skipped");
        }
    }
}

#[test]
fn head_of_queue_scan_cost_does_not_depend_on_the_deleted_span_one_node() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    assert_head_of_queue_cost_is_independent_of_n(1, 1);
}

#[test]
fn head_of_queue_scan_cost_does_not_depend_on_the_deleted_span_three_nodes_r2() {
    if crate::testutil::isolated_env_test(&[]) { return; }
    assert_head_of_queue_cost_is_independent_of_n(3, 2);
}
