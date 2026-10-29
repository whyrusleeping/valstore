use super::*;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use crate::hints::HintStore;
use crate::hlc::{physical_now_ms, Version};
use crate::partition::RangeMap;
use crate::replica::{Applied, CasResult, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::zone::{Catalog, ZoneId, DEFAULT_ZONE};

const BS: usize = 256;

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, || {})
}

fn replica(node: NodeId) -> VersionedStore<MemBlockDevice> {
    let dev = MemBlockDevice::new(BS, 256);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        },
    );
    VersionedStore::new(engine, node).unwrap()
}

fn cluster(n: u32, rf: usize, r: usize) -> Coordinator<LocalCluster<MemBlockDevice>> {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(rf, 64);
    for node in 1..=n {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    Coordinator::new(0, ring, r, lc)
}

struct PersistenceRecorder {
    inner: LocalCluster<MemBlockDevice>,
    singles: RefCell<Vec<Persistence>>,
    batches: RefCell<Vec<bool>>,
}

impl Transport for PersistenceRecorder {
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

    async fn apply_persistent(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        persistence: Persistence,
    ) -> Result<Applied, TransportError> {
        self.singles.borrow_mut().push(persistence);
        if persistence == Persistence::Buffered {
            return self.apply(node, zone_id, key, value, version).await;
        }
        let entry = [BatchEntry {
            key,
            value,
            version,
        }];
        match self.apply_batch(node, zone_id, true, &entry).await[0] {
            BatchAck::Applied(applied) => Ok(applied),
            BatchAck::Stale(epoch) => Err(TransportError::StaleEpoch(epoch)),
            BatchAck::Busy => Err(TransportError::Busy),
            BatchAck::Timeout => Err(TransportError::Timeout),
            BatchAck::Overloaded => Err(TransportError::Overloaded),
            BatchAck::Remote => Err(TransportError::Remote("batch apply failed".into())),
            BatchAck::Unreachable => Err(TransportError::Unavailable),
        }
    }

    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }

    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }

    async fn apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> Vec<BatchAck> {
        self.batches.borrow_mut().push(sync);
        self.inner.apply_batch(node, zone_id, sync, writes).await
    }
}

#[test]
fn single_and_batch_persistence_mapping_is_consistent() {
    let mut inner = LocalCluster::new();
    inner.insert(1, replica(1));
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let c = Coordinator::new(
        0,
        ring,
        1,
        PersistenceRecorder {
            inner,
            singles: RefCell::new(Vec::new()),
            batches: RefCell::new(Vec::new()),
        },
    );

    run(c.write_with(b"single-buffered", b"v", WriteConcern::One)).unwrap();
    run(c.write_with(
        b"single-device",
        b"v",
        WriteOptions::device_durable(WriteConcern::One),
    ))
    .unwrap();
    run(c.write_batch(&[(b"batch-buffered", b"v")], WriteConcern::One)).unwrap();
    run(c.write_batch(
        &[(b"batch-device", b"v")],
        WriteOptions::device_durable(WriteConcern::One),
    ))
    .unwrap();

    assert_eq!(
        *c.transport.singles.borrow(),
        vec![Persistence::Buffered, Persistence::Device]
    );
    assert_eq!(*c.transport.batches.borrow(), vec![true, false, true]);
}

struct SlowCluster {
    inner: LocalCluster<MemBlockDevice>,
    yields: usize,
}

impl Transport for SlowCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        for _ in 0..self.yields {
            vals_rt::yield_now().await;
        }
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn slow_coord(yields: usize, max_ops: usize) -> Coordinator<SlowCluster> {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    Coordinator::new(0, ring, 2, SlowCluster { inner: lc, yields }).with_credits(max_ops, 1 << 30)
}

#[derive(Clone, Default)]
struct Gate(std::rc::Rc<RefCell<GateInner>>);

#[derive(Default)]
struct GateInner {
    open: bool,
    wakers: Vec<Waker>,
}

impl Gate {
    fn open(&self) {
        let mut g = self.0.borrow_mut();
        g.open = true;
        for w in g.wakers.drain(..) {
            w.wake();
        }
    }

    async fn wait(&self) {
        std::future::poll_fn(|cx| {
            let mut g = self.0.borrow_mut();
            if g.open {
                Poll::Ready(())
            } else {
                g.wakers.push(cx.waker().clone());
                Poll::Pending
            }
        })
        .await
    }
}

struct StallCluster {
    inner: LocalCluster<MemBlockDevice>,
    stalled: NodeId,
    gate: Gate,
}

impl Transport for StallCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        if node == self.stalled {
            self.gate.wait().await;
        }
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

struct GatedSend {
    inner: LocalCluster<MemBlockDevice>,
    gate: Gate,
}

impl Transport for GatedSend {
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
    async fn apply_send(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _key: &[u8],
        _value: Option<&[u8]>,
        _version: Version,
    ) -> Result<(), TransportError> {
        self.gate.wait().await;
        Ok(())
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

struct CountingCluster {
    inner: LocalCluster<MemBlockDevice>,
    applies: RefCell<HashMap<NodeId, usize>>,
}

impl Transport for CountingCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        *self.applies.borrow_mut().entry(node).or_default() += 1;
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

struct SkewCluster {
    inner: LocalCluster<MemBlockDevice>,
    reject: Vec<NodeId>,
}

impl Transport for SkewCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        if self.reject.contains(&node) {
            return Ok(Applied::Rejected);
        }
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn skew_coord(reject: Vec<NodeId>, r: usize) -> Coordinator<SkewCluster> {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=5 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    Coordinator::new(0, ring, r, SkewCluster { inner: lc, reject })
}

#[test]
fn write_failing_on_a_skew_split_surfaces_partial_commit_not_clean_quorum_failure() {
    let reps = {
        let c = skew_coord(vec![], 2);
        let reps = c.ring().replicas(b"k");
        reps
    };
    let rejecter = reps[0];
    let c = skew_coord(vec![rejecter], 2);
    let err = run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap_err();
    assert_eq!(
        err,
        CoordError::SkewRejected {
            needed: 3,
            got: 2,
            rejected: 1,
        },
        "a skew split must report partial commit + the skew cause, not a bare QuorumFailed"
    );
    let committed = reps[1..]
        .iter()
        .filter(|&&n| {
            c.transport
                .inner
                .store(n)
                .unwrap()
                .borrow()
                .get(b"k")
                .unwrap()
                .is_some()
        })
        .count();
    assert_eq!(
        committed, 2,
        "the write durably landed on the two healthy replicas"
    );
}

#[test]
fn an_unreachable_replica_without_skew_still_reports_plain_quorum_failure() {
    let c = skew_coord(vec![], 2);
    let reps = c.ring().replicas(b"k");
    c.transport.inner.set_down(reps[0], true);
    c.transport.inner.set_down(reps[1], true);
    assert_eq!(
        run(c.write_with(b"k", b"v", WriteConcern::All)),
        Err(CoordError::QuorumFailed { needed: 3, got: 1 }),
    );
}

#[test]
fn batch_write_surfaces_a_skew_split_per_write() {
    let reps = {
        let c = skew_coord(vec![], 2);
        let reps = c.ring().replicas(b"k");
        reps
    };
    let c = skew_coord(vec![reps[0]], 2);
    let res = run(c.write_batch(&[(b"k", b"v")], WriteConcern::All)).unwrap();
    match &res.outcomes[0] {
        WriteOutcome::Failed {
            error:
                CoordError::SkewRejected {
                    needed,
                    got,
                    rejected,
                },
            acks,
        } => {
            assert_eq!((*needed, *got, *rejected), (3, 2, 1));
            assert_eq!(*acks, 2, "two replicas committed the batched write");
        }
        other => panic!("expected a SkewRejected partial commit, got {other:?}"),
    }
}

fn fd_coord(
    threshold: u32,
    cooldown: u64,
) -> (Coordinator<CountingCluster>, std::rc::Rc<Cell<u64>>) {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let clock = std::rc::Rc::new(Cell::new(0u64));
    let cc = CountingCluster {
        inner: lc,
        applies: RefCell::new(HashMap::new()),
    };
    let c = Coordinator::new(0, ring, 2, cc)
        .with_failure_detector(crate::failure::FdConfig {
            failure_threshold: threshold,
            open_cooldown_ms: cooldown,
        })
        .with_clock({
            let cl = clock.clone();
            move || cl.get()
        });
    (c, clock)
}

fn apply_count(c: &Coordinator<CountingCluster>, node: NodeId) -> usize {
    c.transport
        .applies
        .borrow()
        .get(&node)
        .copied()
        .unwrap_or(0)
}

#[test]
fn failure_detector_trips_then_fast_fails_a_down_replica() {
    let (c, _clock) = fd_coord(3, 1000);
    let reps = c.ring().replicas(b"k");
    let down = reps[0];
    c.transport.inner.set_down(down, true);
    assert!(!c.is_node_down(down), "starts healthy");

    for _ in 0..3 {
        run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    }
    assert!(c.is_node_down(down), "tripped after 3 consecutive failures");
    assert_eq!(c.down_nodes(), vec![down]);
    assert!(
        c.pending_hints(down) > 0,
        "a tripped replica is still hinted"
    );

    let before = apply_count(&c, down);
    for _ in 0..5 {
        run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    }
    assert_eq!(
        apply_count(&c, down),
        before,
        "tripped breaker short-circuits — no apply RPC to the down node"
    );
    assert!(apply_count(&c, reps[1]) >= 8);
}

#[test]
fn failure_detector_recovers_via_a_probe_after_cooldown() {
    let (c, clock) = fd_coord(1, 1000);
    let reps = c.ring().replicas(b"k");
    let down = reps[0];

    c.transport.inner.set_down(down, true);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert!(c.is_node_down(down), "tripped");

    c.transport.inner.set_down(down, false);
    let cnt = apply_count(&c, down);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(
        apply_count(&c, down),
        cnt,
        "still fast-failed inside cooldown"
    );
    assert!(c.is_node_down(down));

    clock.set(1000);
    let cnt = apply_count(&c, down);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert!(!c.is_node_down(down), "recovered after a successful probe");
    assert!(apply_count(&c, down) > cnt, "the probe reached the node");
    assert_eq!(
        c.pending_hints(down),
        0,
        "recovered node's hints auto-drained"
    );

    let cnt = apply_count(&c, down);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(apply_count(&c, down), cnt + 1, "fully back in rotation");
}

#[test]
fn disabled_detector_never_short_circuits() {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let cc = CountingCluster {
        inner: lc,
        applies: RefCell::new(HashMap::new()),
    };
    let c = Coordinator::new(0, ring, 2, cc);
    let reps = c.ring().replicas(b"k");
    let down = reps[0];
    c.transport.inner.set_down(down, true);
    for _ in 0..6 {
        run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    }
    assert!(!c.is_node_down(down), "no detector → never marked down");
    assert_eq!(
        apply_count(&c, down),
        6,
        "every write re-attempts the down node"
    );
}

#[test]
fn write_backpressure_bounds_in_flight_and_blocks() {
    use std::rc::Rc;
    use vals_rt::LocalExecutor;
    let coord = Rc::new(slow_coord(3, 2));
    let ex = LocalExecutor::new();
    let done = Rc::new(Cell::new(0usize));
    for i in 0..6 {
        let coord = coord.clone();
        let done = done.clone();
        ex.spawn(async move {
            let k = format!("k{i}").into_bytes();
            coord
                .write_with(&k, b"v", WriteConcern::Quorum(2))
                .await
                .unwrap();
            done.set(done.get() + 1);
        });
    }
    ex.run(|| {});

    assert_eq!(done.get(), 6, "all writes completed (none dropped)");
    let st = coord.credit_stats();
    assert_eq!(st.in_flight_ops, 0, "every credit released");
    assert_eq!(st.peak_ops, 2, "in-flight never exceeded the 2-op bound");
    assert!(
        st.blocked_total >= 4,
        "≥4 of 6 writes had to wait for a credit, got {}",
        st.blocked_total
    );
}

#[test]
fn try_write_sheds_load_when_pool_exhausted() {
    let c = cluster(3, 3, 2).with_credits(1, 1 << 30);
    let held = c.try_acquire(1, 8).expect("first credit is free");
    assert_eq!(
        run(c.try_write(b"k", b"v", WriteConcern::Quorum(2))),
        Err(CoordError::Overloaded)
    );
    drop(held);
    assert!(run(c.try_write(b"k", b"v", WriteConcern::Quorum(2))).is_ok());
}

#[test]
fn partial_quorum_failure_releases_write_credits() {
    let c = cluster(3, 3, 2).with_credits(1, 1024).with_node_credits(1, 1024);
    let down = c.ring().replicas(b"key")[0];
    c.transport().set_down(down, true);
    assert_eq!(
        run(c.write_with(b"key", b"value", WriteConcern::All)),
        Err(CoordError::QuorumFailed { needed: 3, got: 2 })
    );
    let stats = c.credit_stats();
    assert_eq!(stats.in_flight_ops, 0);
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.waiters, 0);
}

#[test]
fn write_budget_counts_keys_and_rejects_without_leaking() {
    let c = cluster(3, 3, 2).with_credits(8, 3);
    assert_eq!(
        run(c.write_with(b"key", b"v", WriteConcern::Majority)),
        Err(CoordError::Overloaded)
    );
    let stats = c.credit_stats();
    assert_eq!(stats.in_flight_ops, 0);
    assert_eq!(stats.in_flight_bytes, 0);
    assert_eq!(stats.waiters, 0);
}

#[test]
fn batch_budget_counts_keys_and_enforces_per_node_limit() {
    let writes = [(b"same".as_slice(), b"a".as_slice()), (b"same".as_slice(), b"b".as_slice())];
    let bytes: usize = writes.iter().map(|(key, value)| key.len() + value.len()).sum();

    let global = cluster(3, 3, 2).with_credits(8, bytes - 1);
    assert!(matches!(
        run(global.write_batch(&writes, WriteConcern::Majority)),
        Err(CoordError::Overloaded)
    ));
    assert_eq!(global.credit_stats().in_flight_ops, 0);

    let per_node = cluster(3, 3, 2)
        .with_credits(8, 1024)
        .with_node_credits(1, 1024);
    assert!(matches!(
        run(per_node.write_batch(&writes, WriteConcern::Majority)),
        Err(CoordError::Overloaded)
    ));
    assert_eq!(per_node.credit_stats().in_flight_ops, 0);
}

#[test]
fn unbounded_pool_never_blocks() {
    let c = cluster(3, 3, 2);
    for i in 0..10 {
        let k = format!("k{i}").into_bytes();
        run(c.write_with(&k, b"v", WriteConcern::Quorum(2))).unwrap();
    }
    let st = c.credit_stats();
    assert_eq!(st.blocked_total, 0, "unbounded pool never parks a write");
    assert_eq!(st.max_ops, usize::MAX);
    assert_eq!(st.in_flight_ops, 0);
}

#[test]
fn credit_pool_rejects_oversized_when_idle() {
    let pool = Rc::new(RefCell::new(CreditPool::new(2, 100)));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);

    let mut big = Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes: 500,
        node_costs: Vec::new(),
        ticket: None,
        deadline_at: None,
    };
    assert!(matches!(
        Pin::new(&mut big).poll(&mut cx),
        Poll::Ready(Err(CoordError::Overloaded))
    ));
    let p = pool.borrow();
    assert!(p.fits(2, 100));
    assert!(p.waiters.is_empty());
}

#[test]
fn credit_pool_is_fifo() {
    let pool = Rc::new(RefCell::new(CreditPool::new(1, 1 << 20)));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mk = |ticket: Option<u64>| Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes: 1,
        node_costs: Vec::new(),
        ticket,
        deadline_at: None,
    };
    let mut a = mk(None);
    let a_credit = match Pin::new(&mut a).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("first acquire fits"),
    };
    let (mut b, mut c) = (mk(None), mk(None));
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Pending));
    assert!(matches!(Pin::new(&mut c).poll(&mut cx), Poll::Pending));
    drop(a_credit);
    assert!(matches!(Pin::new(&mut c).poll(&mut cx), Poll::Pending));
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Ready(_)));
}

#[test]
fn cancelled_parked_acquire_does_not_strand_the_pool() {
    let pool = Rc::new(RefCell::new(CreditPool::new(1, 1 << 20)));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mk = || Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes: 1,
        node_costs: Vec::new(),
        ticket: None,
        deadline_at: None,
    };
    let mut a = mk();
    let a_credit = match Pin::new(&mut a).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("first acquire fits"),
    };
    let mut b = mk();
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Pending));
    drop(b);
    assert!(
        pool.borrow().waiters.is_empty(),
        "cancelled waiter removed its ticket"
    );
    drop(a_credit);

    let mut c = mk();
    let c_credit = match Pin::new(&mut c).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("acquire after a cancelled waiter must succeed"),
    };
    drop(c_credit);
    {
        let p = pool.borrow();
        assert!(
            p.waiters.is_empty() && p.fits(1, 1),
            "try_acquire viable again"
        );
    }
    drop(a);
    drop(c);
    assert!(pool.borrow().waiters.is_empty());
}

#[test]
fn credit_pool_per_node_sublimit_bypasses_a_stalled_node() {
    let mut p = CreditPool::new(100, 1 << 20);
    p.set_node_limits(1, 1 << 20);
    let pool = Rc::new(RefCell::new(p));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mk = |nodes: Vec<NodeId>| Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes: 1,
        node_costs: nodes
            .into_iter()
            .map(|node| NodeCreditCost { node, ops: 1, bytes: 1 })
            .collect(),
        ticket: None,
        deadline_at: None,
    };

    let mut a = mk(vec![7]);
    let a_credit = match Pin::new(&mut a).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("an idle node admits"),
    };
    let mut b = mk(vec![7]);
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Pending));
    let mut c = mk(vec![9]);
    let c_credit = match Pin::new(&mut c).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("a healthy-node write must bypass a per-node-blocked waiter"),
    };
    drop(a_credit);
    let b_credit = match Pin::new(&mut b).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("b admits once node 7's sub-limit frees"),
    };
    drop(b_credit);
    drop(c_credit);
    let mut d = mk(vec![7]);
    assert!(
        matches!(Pin::new(&mut d).poll(&mut cx), Poll::Ready(_)),
        "per-node accounting returned to zero after every credit dropped"
    );
    assert!(pool.borrow().waiters.is_empty());
}

#[test]
fn credit_pool_rejects_oversized_per_node_request() {
    let mut p = CreditPool::new(100, 1 << 20);
    p.set_node_limits(1, 1 << 20);
    let pool = Rc::new(RefCell::new(p));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mut big = Acquire {
        pool: pool.clone(),
        ops: 3,
        bytes: 1,
        node_costs: vec![NodeCreditCost { node: 5, ops: 3, bytes: 1 }],
        ticket: None,
        deadline_at: None,
    };
    assert!(matches!(
        Pin::new(&mut big).poll(&mut cx),
        Poll::Ready(Err(CoordError::Overloaded))
    ));
    let p = pool.borrow();
    assert!(p.fits(100, 1 << 20));
    assert!(p.waiters.is_empty());
}

#[test]
fn credit_pool_global_blocked_earlier_waiter_still_gates() {
    let mut p = CreditPool::new(100, 10);
    p.set_node_limits(100, 100);
    let pool = Rc::new(RefCell::new(p));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mk = |bytes: usize, node: NodeId| Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes,
        node_costs: vec![NodeCreditCost { node, ops: 1, bytes }],
        ticket: None,
        deadline_at: None,
    };

    let mut a = mk(5, 1);
    let a_credit = match Pin::new(&mut a).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("first write fits"),
    };
    let mut b = mk(8, 2);
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Pending));
    let mut c = mk(4, 3);
    assert!(
        matches!(Pin::new(&mut c).poll(&mut cx), Poll::Pending),
        "a global-blocked earlier waiter must still gate a later fitting write"
    );
    drop(a_credit);
    let b_credit = match Pin::new(&mut b).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("b admits once the global budget frees"),
    };
    assert!(
        matches!(Pin::new(&mut c).poll(&mut cx), Poll::Pending),
        "c still can't fit while b holds 8 of the 10 global bytes"
    );
    drop(b_credit);
    assert!(matches!(Pin::new(&mut c).poll(&mut cx), Poll::Ready(_)));
}

#[test]
fn cancelled_per_node_parked_acquire_does_not_strand() {
    let mut p = CreditPool::new(100, 1 << 20);
    p.set_node_limits(1, 1 << 20);
    let pool = Rc::new(RefCell::new(p));
    let w = Waker::noop();
    let mut cx = Context::from_waker(w);
    let mk = || Acquire {
        pool: pool.clone(),
        ops: 1,
        bytes: 1,
        node_costs: vec![NodeCreditCost { node: 7, ops: 1, bytes: 1 }],
        ticket: None,
        deadline_at: None,
    };
    let mut a = mk();
    let a_credit = match Pin::new(&mut a).poll(&mut cx) {
        Poll::Ready(c) => c,
        _ => panic!("first write to node 7 admits"),
    };
    let mut b = mk();
    assert!(matches!(Pin::new(&mut b).poll(&mut cx), Poll::Pending));
    drop(b);
    assert!(pool.borrow().waiters.is_empty(), "cancelled waiter cleaned up");
    drop(a_credit);
    let mut c = mk();
    assert!(matches!(Pin::new(&mut c).poll(&mut cx), Poll::Ready(_)));
}

#[test]
fn per_node_sublimit_isolates_a_stalled_replica() {
    use std::rc::Rc;
    use vals_rt::LocalExecutor;
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(1, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let stalled = 1;
    let gate = Gate::default();
    let coord = Rc::new(
        Coordinator::new(
            0,
            ring,
            1,
            StallCluster {
                inner: lc,
                stalled,
                gate: gate.clone(),
            },
        )
        .with_credits(100, 1 << 30)
        .with_node_credits(1, 1 << 30),
    );

    let mut stalled_keys: Vec<Vec<u8>> = Vec::new();
    let mut healthy_keys: Vec<Vec<u8>> = Vec::new();
    let mut i = 0;
    while stalled_keys.len() < 3 || healthy_keys.len() < 3 {
        let k = format!("k{i}").into_bytes();
        let owner = coord.ring().primary(&k).unwrap();
        if owner == stalled {
            if stalled_keys.len() < 3 {
                stalled_keys.push(k);
            }
        } else if healthy_keys.len() < 3 {
            healthy_keys.push(k);
        }
        i += 1;
    }

    let ex = LocalExecutor::new();
    let stalled_done = Rc::new(Cell::new(0usize));
    for k in stalled_keys {
        let coord = coord.clone();
        let d = stalled_done.clone();
        ex.spawn(async move {
            coord.write_with(&k, b"v", WriteConcern::One).await.unwrap();
            d.set(d.get() + 1);
        });
    }
    ex.run_until(
        {
            let coord = coord.clone();
            let healthy_keys = healthy_keys.clone();
            async move {
                for k in &healthy_keys {
                    coord.write_with(k, b"v", WriteConcern::One).await.unwrap();
                }
            }
        },
        || {},
    );

    assert_eq!(
        stalled_done.get(),
        0,
        "writes to the stalled node are still parked"
    );
    let st = coord.credit_stats();
    assert_eq!(
        st.in_flight_ops, 1,
        "only the stalled node's one-op sub-limit is held (healthy writes drained)"
    );
    assert_eq!(
        st.waiters, 2,
        "the other two stalled-node writes park on its sub-limit"
    );

    gate.open();
    ex.run(|| {});
    assert_eq!(stalled_done.get(), 3, "every stalled write eventually completes");
    let st = coord.credit_stats();
    assert_eq!(st.in_flight_ops, 0, "every credit released");
    assert_eq!(st.waiters, 0);
    assert_eq!(st.node_max_ops, 1, "sub-limit surfaced in stats");
}

#[test]
fn w0_credit_releases_only_after_sends_complete() {
    use std::rc::Rc;
    use vals_rt::LocalExecutor;
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let gate = Gate::default();
    let coord = Rc::new(
        Coordinator::new(
            0,
            ring,
            2,
            GatedSend {
                inner: lc,
                gate: gate.clone(),
            },
        )
        .with_credits(1, 1 << 30),
    );

    let ex = LocalExecutor::new();
    let done = Rc::new(Cell::new(0usize));
    for k in [b"a".as_slice(), b"b".as_slice()] {
        let coord = coord.clone();
        let done = done.clone();
        let k = k.to_vec();
        ex.spawn(async move {
            coord.write_with(&k, b"v", WriteConcern::Unacknowledged).await.unwrap();
            done.set(done.get() + 1);
        });
    }
    ex.run_until(
        async {
            for _ in 0..4 {
                vals_rt::yield_now().await;
            }
        },
        || {},
    );

    assert_eq!(done.get(), 0, "neither W=0 write has completed");
    let st = coord.credit_stats();
    assert_eq!(
        st.in_flight_ops, 1,
        "the first write still holds the credit — its sends are incomplete"
    );
    assert_eq!(st.waiters, 1, "the second write is parked awaiting the credit");
    assert!(st.blocked_total >= 1);

    gate.open();
    ex.run(|| {});
    assert_eq!(done.get(), 2, "both W=0 writes complete once sends finish");
    assert_eq!(coord.credit_stats().in_flight_ops, 0, "credit released");
}

#[test]
fn w0_down_replica_becomes_a_hint_without_stalling() {
    let c = cluster(3, 3, 2);
    let down = c.ring().replicas(b"k")[0];
    c.transport().set_down(down, true);
    run(c.write_with(b"k", b"v", WriteConcern::Unacknowledged)).unwrap();
    assert_eq!(
        c.pending_hints(down),
        1,
        "the down replica's W=0 send became a hint"
    );
}

#[test]
fn refresh_ring_adopts_cluster_membership() {
    let mut lc = LocalCluster::new();
    for node in 1..=5 {
        lc.insert(node, replica(node));
    }
    let mut ring = Ring::with_vnodes(3, 64);
    ring.set_members([1, 2]);
    let c = Coordinator::new(0, ring, 2, lc);
    assert_eq!(c.ring().members().collect::<Vec<_>>(), vec![1, 2]);

    let now = run(c.refresh_ring(1)).unwrap();
    assert_eq!(now, vec![1, 2, 3, 4, 5]);
    assert_eq!(c.ring().members().collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
    let reps = c.ring().replicas(b"some-key");
    assert!(reps.iter().any(|n| ![1, 2].contains(n)) || reps.len() == 3);
}

#[test]
fn write_then_read_roundtrips() {
    let c = cluster(5, 3, 2);
    let v = run(c.write(b"k", b"hello")).unwrap();
    let got = run(c.read(b"k")).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"hello"[..]));
    assert_eq!(got.version, v);
    assert_eq!(run(c.read(b"absent")).unwrap(), None);
}

#[test]
fn same_clock_same_caller_writes_converge_in_opposite_delivery_order() {
    let writer_a = Coordinator::new_with_writer_for_test(
        77,
        Ring::with_vnodes(1, 1),
        1,
        LocalCluster::<MemBlockDevice>::new(),
        0x8000_0000_0000_0011,
    )
    .with_clock(|| 1_000);
    let writer_b = Coordinator::new_with_writer_for_test(
        77,
        Ring::with_vnodes(1, 1),
        1,
        LocalCluster::<MemBlockDevice>::new(),
        0x8000_0000_0000_0022,
    )
    .with_clock(|| 1_000);
    let va = writer_a.stamp_at(1_000);
    let vb = writer_b.stamp_at(1_000);
    assert_eq!(va.hlc, vb.hlc);
    assert_ne!(va, vb);
    let (winner_version, winner_value) = if va > vb {
        (va, &b"a"[..])
    } else {
        (vb, &b"b"[..])
    };

    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        let mut store = replica(node);
        if node % 2 == 0 {
            store.apply(b"k", Some(b"a"), va).unwrap();
            store.apply(b"k", Some(b"b"), vb).unwrap();
        } else {
            store.apply(b"k", Some(b"b"), vb).unwrap();
            store.apply(b"k", Some(b"a"), va).unwrap();
        }
        let local = store.get_versioned(b"k").unwrap().unwrap();
        assert_eq!(local.version, winner_version);
        assert_eq!(local.value.as_deref(), Some(winner_value));
        lc.insert(node, store);
        ring.add_node(node);
    }

    let reader = Coordinator::new(88, ring, 2, lc);
    let quorum = run(reader.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(quorum.version, winner_version);
    assert_eq!(quorum.value.as_deref(), Some(winner_value));
}

#[test]
fn read_any_returns_value_spreads_and_fails_over() {
    let c = cluster(5, 3, 2);
    let v = run(c.write(b"k", b"hello")).unwrap();
    let got = run(c.read_any(b"k")).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"hello"[..]));
    assert_eq!(got.version, v);
    assert_eq!(run(c.read_any(b"absent")).unwrap(), None);

    let reps = c.ring().replicas(b"k");
    let mut hits = std::collections::HashSet::new();
    for _ in 0..reps.len() * 3 {
        let _ = run(c.read_any(b"k")).unwrap();
        hits.insert(c.read_rotor.get() % reps.len());
    }
    assert_eq!(hits.len(), reps.len(), "rotor should visit every replica");

    c.transport.set_down(reps[0], true);
    for _ in 0..reps.len() {
        let got = run(c.read_any(b"k")).unwrap().unwrap();
        assert_eq!(got.value.as_deref(), Some(&b"hello"[..]));
    }
    for &n in &reps {
        c.transport.set_down(n, true);
    }
    assert!(run(c.read_any(b"k")).is_err());
}

#[test]
fn write_lands_on_all_preference_list_replicas() {
    let c = cluster(5, 3, 2);
    let v = run(c.write(b"k", b"v")).unwrap();
    let reps = c.ring().replicas(b"k");
    assert_eq!(reps.len(), 3);
    for node in reps {
        let s = c.transport.store(node).unwrap().borrow();
        assert_eq!(s.get_versioned(b"k").unwrap().unwrap().version, v);
    }
}

#[test]
fn write_quorum_tolerates_failures_up_to_rf_minus_w() {
    let c = cluster(5, 3, 2);
    let reps = c.ring().replicas(b"k");
    c.transport.set_down(reps[0], true);
    assert!(run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).is_ok());
    c.transport.set_down(reps[1], true);
    assert_eq!(
        run(c.write_with(b"k", b"v2", WriteConcern::Quorum(2))),
        Err(CoordError::QuorumFailed { needed: 2, got: 1 })
    );
}

#[test]
fn nosync_tier_acks_even_with_all_replicas_down() {
    let c = cluster(3, 3, 1);
    for node in c.ring().replicas(b"k") {
        c.transport.set_down(node, true);
    }
    assert!(run(c.write_with(b"k", b"v", WriteConcern::Unacknowledged)).is_ok());
}

#[test]
fn replicated_tier_needs_every_replica() {
    let c = cluster(5, 3, 2);
    let reps = c.ring().replicas(b"k");
    c.transport.set_down(reps[0], true);
    assert_eq!(
        run(c.write_with(b"k", b"v", WriteConcern::All)),
        Err(CoordError::QuorumFailed { needed: 3, got: 2 })
    );
}

#[test]
fn majority_tier_resolves_to_a_strict_majority_w() {
    for (rf, want) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4), (7, 4)] {
        let w = WriteOptions::buffered(WriteConcern::Majority).required_acks(rf).unwrap();
        assert_eq!(w, want, "Majority W for rf={rf}");
        assert!(
            w + w > rf,
            "majority overlap must hold at rf={rf}: {w}+{w} > {rf}"
        );
    }
}

#[test]
fn majority_tier_needs_a_real_majority_at_even_rf() {
    let c = cluster(4, 4, 2);
    let reps = c.ring().replicas(b"k");
    assert_eq!(reps.len(), 4, "RF=4 preference list is all four nodes");
    c.transport.set_down(reps[0], true);
    c.transport.set_down(reps[1], true);

    assert!(run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).is_ok());

    assert_eq!(
        run(c.write_with(b"k", b"v2", WriteConcern::Majority)),
        Err(CoordError::QuorumFailed { needed: 3, got: 2 })
    );
}

#[test]
fn majority_write_is_seen_by_a_majority_read_at_even_rf() {
    let r = WriteOptions::buffered(WriteConcern::Majority).required_acks(4).unwrap();
    let c = cluster(4, 4, r);
    let v = run(c.write_with(b"k", b"v", WriteConcern::Majority)).unwrap();
    let got = run(c.read_quorum(b"k", r)).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(got.version, v);
}

#[test]
fn read_concern_resolves_and_clamps_per_replica_count() {
    for rf in 1..=9usize {
        let majority_w = WriteOptions::buffered(WriteConcern::Majority).required_acks(rf).unwrap();
        let majority_r = ReadConcern::Majority.required_replies(rf);
        assert_eq!(majority_r, rf / 2 + 1, "Majority R at rf={rf}");
        assert!(majority_w + majority_r > rf, "majority W and R must overlap at rf={rf}");
        assert_eq!(ReadConcern::One.required_replies(rf), 1);
        assert_eq!(ReadConcern::All.required_replies(rf), rf);
        assert_eq!(ReadConcern::Quorum(0).required_replies(rf), 1, "0 reads one replica");
        assert_eq!(ReadConcern::Quorum(rf + 3).required_replies(rf), rf, "capped at RF");
        assert_eq!(ReadConcern::from(2), ReadConcern::Quorum(2));
    }
    for concern in [ReadConcern::One, ReadConcern::Majority, ReadConcern::All, ReadConcern::Quorum(2)] {
        assert_eq!(concern.required_replies(0), 0);
    }
}

#[test]
fn a_zero_read_count_reads_one_replica_instead_of_reporting_absent() {
    let c = cluster(3, 3, 0);
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    assert_eq!(run(c.read(b"k")).unwrap().unwrap().value.as_deref(), Some(&b"v"[..]));
    assert_eq!(
        run(c.read_quorum(b"k", 0)).unwrap().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );
    assert_eq!(run(c.read_many(&[b"k".to_vec()])).unwrap()[0].as_ref().unwrap().as_ref().unwrap().value.as_deref(), Some(&b"v"[..]));
}

const MIXED_RF_ZONES: [(ZoneId, usize, bool); 4] = [(11, 1, false), (13, 3, false), (15, 5, false), (25, 5, true)];

fn mixed_rf_cluster(read: impl Into<ReadConcern>) -> Coordinator<LocalCluster<MemBlockDevice>> {
    let mut lc = LocalCluster::new();
    for (zone, rf, sorted) in MIXED_RF_ZONES {
        lc.define_zone(
            zone,
            ZoneConfig {
                name: format!("z{zone}"),
                rf,
                ordering: if sorted { ZOrdering::Sorted } else { ZOrdering::Unsorted },
                scope: None,
            },
        );
    }
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=5u32 {
        lc.insert(node, replica(node));
        for (zone, _, sorted) in MIXED_RF_ZONES {
            lc.insert_zone(node, zone, if sorted { sorted_replica(node) } else { replica(node) });
        }
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, read, lc);
    run(c.refresh_catalog(1)).unwrap();
    c
}

fn set_down(c: &Coordinator<LocalCluster<MemBlockDevice>>, nodes: &[NodeId], down: bool) {
    for &node in nodes {
        c.transport.set_down(node, down);
    }
}

fn majority_write_on_last_replicas(
    c: &Coordinator<LocalCluster<MemBlockDevice>>,
    zone: ZoneId,
    key: &[u8],
) -> (Vec<NodeId>, usize, Version) {
    let reps = c.replicas_for(zone, key).unwrap();
    let w = reps.len() / 2 + 1;
    let skipped = &reps[..reps.len() - w];
    set_down(c, skipped, true);
    let v = run(c.write_in(zone, key, b"v", WriteOptions::device_durable(WriteConcern::Majority)))
        .unwrap();
    set_down(c, skipped, false);
    (reps, w, v)
}

#[test]
fn majority_reads_see_majority_writes_in_zones_of_every_rf() {
    let c = mixed_rf_cluster(ReadConcern::Majority);
    for (zone, rf, sorted) in MIXED_RF_ZONES {
        let key = format!("key-{zone}").into_bytes();
        let (reps, w, v) = majority_write_on_last_replicas(&c, zone, &key);
        assert_eq!(reps.len(), rf, "zone {zone}");
        let holders = &reps[reps.len() - w..];
        set_down(&c, &holders[..w - 1], true);

        let got = run(c.read_in(zone, &key)).unwrap().expect("one holder answers");
        assert_eq!(got.version, v, "zone {zone} rf {rf}");
        let many = run(c.read_many_in(zone, std::slice::from_ref(&key))).unwrap();
        assert_eq!(many[0].as_ref().unwrap().as_ref().unwrap().version, v, "zone {zone} read_many");
        if sorted {
            let rows = run(c.scan_in(zone, None, None, ReadConcern::Majority, None)).unwrap();
            assert_eq!(rows, vec![(key.clone(), b"v".to_vec())], "zone {zone} scan");
        }
        set_down(&c, &holders[..w - 1], false);
    }
}

#[test]
fn majority_reads_fail_rather_than_miss_a_write_a_fixed_count_misses() {
    let c = mixed_rf_cluster(2);
    for (zone, sorted) in [(15, false), (25, true)] {
        let key = b"rf5-key".to_vec();
        let (reps, w, _) = majority_write_on_last_replicas(&c, zone, &key);
        let holders = &reps[reps.len() - w..];
        set_down(&c, holders, true);

        assert_eq!(run(c.read_in(zone, &key)), Ok(None), "R=2 at RF 5 misses the write");
        assert_eq!(
            run(c.read_quorum_in(zone, &key, ReadConcern::Majority)),
            Err(CoordError::QuorumFailed { needed: 3, got: 2 })
        );
        if sorted {
            assert_eq!(run(c.scan_in(zone, None, None, 2, None)), Ok(Vec::new()));
            assert!(matches!(
                run(c.scan_in(zone, None, None, ReadConcern::Majority, None)),
                Err(CoordError::ScanQuorumFailed { needed: 3, got: 2, .. })
            ));
        }
        set_down(&c, holders, false);
    }
}

#[test]
fn read_quorum_fails_when_too_few_respond() {
    let c = cluster(5, 3, 2);
    let reps = c.ring().replicas(b"k");
    c.transport.set_down(reps[0], true);
    c.transport.set_down(reps[1], true);
    assert_eq!(
        run(c.read_quorum(b"k", 2)),
        Err(CoordError::QuorumFailed { needed: 2, got: 1 })
    );
}

#[test]
fn w_plus_r_greater_than_rf_reads_latest() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let v = run(c.write_with(b"k", b"v1", WriteConcern::Quorum(2))).unwrap();

    c.transport.set_down(reps[0], true);
    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.version, v);
    assert_eq!(got.value.as_deref(), Some(&b"v1"[..]));
}

#[test]
fn read_fanout_quorum_still_reads_latest() {
    let c = cluster(3, 3, 2).with_read_fanout(ReadFanout::Quorum);
    let reps = c.ring().replicas(b"k");
    let v = run(c.write_with(b"k", b"v1", WriteConcern::Quorum(2))).unwrap();
    c.transport.set_down(reps[0], true);
    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.version, v);
    assert_eq!(got.value.as_deref(), Some(&b"v1"[..]));
}

#[test]
fn read_fanout_quorum_escalates_past_a_down_primary() {
    let c = cluster(3, 3, 2).with_read_fanout(ReadFanout::Quorum);
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    c.transport.set_down(reps[0], true);
    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"v"[..]));
    c.transport.set_down(reps[1], true);
    assert_eq!(
        run(c.read_quorum(b"k", 2)),
        Err(CoordError::QuorumFailed { needed: 2, got: 1 })
    );
}

#[test]
fn digest_read_falls_back_when_value_primary_is_stale() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v1", WriteConcern::All)).unwrap();

    c.transport.set_down(reps[0], true);
    let v2 = run(c.write_with(b"k", b"v2", WriteConcern::Quorum(2))).unwrap();
    c.transport.set_down(reps[0], false);

    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.version, v2);
    assert_eq!(got.value.as_deref(), Some(&b"v2"[..]));
}

#[test]
fn digest_read_many_preserves_positions_falls_back_and_keeps_tombstones() {
    let c = cluster(3, 3, 2);
    let a_reps = c.ring().replicas(b"a");
    run(c.write_with(b"a", b"old", WriteConcern::All)).unwrap();
    run(c.write_with(b"b", b"gone", WriteConcern::All)).unwrap();
    run(c.delete(b"b", WriteConcern::All)).unwrap();

    c.transport.set_down(a_reps[0], true);
    let fresh = run(c.write_with(b"a", b"fresh", WriteConcern::Quorum(2))).unwrap();
    c.transport.set_down(a_reps[0], false);

    let keys = vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec(), b"missing".to_vec()];
    let got = run(c.read_many(&keys)).unwrap();
    assert_eq!(got.len(), keys.len());
    for i in [0, 2] {
        let stored = got[i].as_ref().unwrap().as_ref().unwrap();
        assert_eq!(stored.version, fresh);
        assert_eq!(stored.value.as_deref(), Some(&b"fresh"[..]));
    }
    assert_eq!(got[1], Ok(None));
    assert_eq!(got[3], Ok(None));
    let repaired = c.transport.store(a_reps[0]).unwrap().borrow();
    assert_eq!(repaired.get_versioned(b"a").unwrap().unwrap().version, fresh);
}

struct PartialMixedReads;

impl Transport for PartialMixedReads {
    async fn apply(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _key: &[u8],
        _value: Option<&[u8]>,
        _version: Version,
    ) -> Result<Applied, TransportError> {
        Ok(Applied::Rejected)
    }

    async fn read(
        &self,
        node: NodeId,
        _zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        if key == b"failed" && node != 1 {
            return Err(TransportError::Unavailable);
        }
        if key == b"stale" && node != 1 {
            return Err(TransportError::StaleEpoch(42));
        }
        Ok(Some(Stored {
            version: Version { hlc: 7, node: 1 },
            value: Some(key.to_vec()),
        }))
    }

    async fn digest(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Version>, TransportError> {
        self.read(node, zone_id, key)
            .await
            .map(|stored| stored.map(|s| s.version))
    }

    async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
        Ok(RingView::default())
    }

    async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
        Ok(Catalog::new())
    }
}

#[test]
fn digest_read_many_keeps_partial_failures_per_key() {
    let mut ring = Ring::new(3);
    ring.set_members([1, 2, 3]);
    let c = Coordinator::new(9, ring, 2, PartialMixedReads).with_manual_refresh();
    let keys = vec![b"failed".to_vec(), b"healthy".to_vec(), b"stale".to_vec()];
    let got = run(c.read_many(&keys)).unwrap();
    assert!(matches!(
        got[0],
        Err(CoordError::QuorumFailed { needed: 2, got: 1 })
    ));
    assert_eq!(
        got[1].as_ref().unwrap().as_ref().unwrap().value.as_deref(),
        Some(&b"healthy"[..])
    );
    assert!(matches!(
        got[2],
        Err(CoordError::StaleRing { epoch: 42 })
    ));
}

struct EqualWinnerFallbacks {
    primary: NodeId,
    failed: NodeId,
    wrong: NodeId,
    good: NodeId,
    value_reads: RefCell<Vec<NodeId>>,
}

impl Transport for EqualWinnerFallbacks {
    async fn apply(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _key: &[u8],
        _value: Option<&[u8]>,
        _version: Version,
    ) -> Result<Applied, TransportError> {
        Ok(Applied::Won)
    }

    async fn read(
        &self,
        node: NodeId,
        _zone_id: ZoneId,
        _key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.value_reads.borrow_mut().push(node);
        if node == self.primary {
            return Ok(Some(Stored {
                version: Version { hlc: 1, node: node.into() },
                value: Some(b"stale".to_vec()),
            }));
        }
        if node == self.failed {
            return Err(TransportError::Unavailable);
        }
        if node == self.wrong {
            return Ok(Some(Stored {
                version: Version { hlc: 1, node: node.into() },
                value: Some(b"wrong".to_vec()),
            }));
        }
        assert_eq!(node, self.good);
        Ok(Some(Stored {
            version: Version { hlc: 2, node: self.good.into() },
            value: Some(b"winner".to_vec()),
        }))
    }

    async fn digest(
        &self,
        node: NodeId,
        _zone_id: ZoneId,
        _key: &[u8],
    ) -> Result<Option<Version>, TransportError> {
        Ok(Some(if node == self.primary {
            Version { hlc: 1, node: node.into() }
        } else {
            Version {
                hlc: 2,
                node: self.good.into(),
            }
        }))
    }

    async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
        Ok(RingView::default())
    }

    async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
        Ok(Catalog::new())
    }
}

#[test]
fn digest_read_many_tries_every_equal_winner_fallback_owner() {
    let mut ring = Ring::new(4);
    ring.set_members([1, 2, 3, 4]);
    let reps = ring.replicas(b"k");
    let transport = EqualWinnerFallbacks {
        primary: reps[0],
        failed: reps[1],
        wrong: reps[2],
        good: reps[3],
        value_reads: RefCell::new(Vec::new()),
    };
    let c = Coordinator::new(9, ring, 4, transport).with_manual_refresh();
    let got = run(c.read_many(&[b"k".to_vec()])).unwrap();
    assert_eq!(
        got[0].as_ref().unwrap().as_ref().unwrap().value.as_deref(),
        Some(&b"winner"[..])
    );
    let reads = c.transport.value_reads.borrow();
    assert!(reads.contains(&reps[1]));
    assert!(reads.contains(&reps[2]));
    assert!(reads.contains(&reps[3]));
}

#[test]
fn read_repair_heals_a_lagging_replica() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let laggard = reps[2];

    c.transport.set_down(laggard, true);
    let v = run(c.write_with(b"k", b"fresh", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(
        c.transport
            .store(laggard)
            .unwrap()
            .borrow()
            .get(b"k")
            .unwrap(),
        None,
        "laggard missed the write"
    );

    c.transport.set_down(laggard, false);
    run(c.read_quorum(b"k", 3)).unwrap();
    let healed = c.transport.store(laggard).unwrap().borrow();
    assert_eq!(healed.get(b"k").unwrap().as_deref(), Some(&b"fresh"[..]));
    assert_eq!(healed.get_versioned(b"k").unwrap().unwrap().version, v);
}

#[test]
fn deleted_key_reads_as_absent_and_repairs_tombstone() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();

    let laggard = reps[2];
    c.transport.set_down(laggard, true);
    run(c.delete(b"k", WriteConcern::Quorum(2))).unwrap();
    c.transport.set_down(laggard, false);

    assert_eq!(run(c.read_quorum(b"k", 3)).unwrap(), None);
    let s = c.transport.store(laggard).unwrap().borrow();
    assert_eq!(
        s.get(b"k").unwrap(),
        None,
        "tombstone repaired onto laggard"
    );
    assert!(
        s.get_versioned(b"k").unwrap().unwrap().value.is_none(),
        "laggard now holds a tombstone, not the old value"
    );
}

#[test]
fn hinted_handoff_delivers_after_recovery() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let down = reps[2];

    c.transport.set_down(down, true);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(c.pending_hints(down), 1);
    assert_eq!(
        c.transport.store(down).unwrap().borrow().get(b"k").unwrap(),
        None,
        "down replica missed the write"
    );

    c.transport.set_down(down, false);
    assert_eq!(run(c.replay_hints(down)), 1);
    assert_eq!(c.pending_hints(down), 0);
    assert_eq!(
        c.transport
            .store(down)
            .unwrap()
            .borrow()
            .get(b"k")
            .unwrap()
            .as_deref(),
        Some(&b"v"[..]),
        "hint delivered the write on recovery"
    );
}

#[test]
fn durable_hints_persist_to_disk() {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "vals-coord-hints-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));

    let store = crate::hints::FileHintStore::open(&path, 100).unwrap();
    let c = cluster(3, 3, 2).with_hint_store(Box::new(store));
    let reps = c.ring().replicas(b"k");
    let down = reps[2];

    c.transport.set_down(down, true);
    run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(c.pending_hints(down), 1);

    let reopened = crate::hints::FileHintStore::open(&path, 100).unwrap();
    assert_eq!(reopened.pending(down), 1);
    let _ = std::fs::remove_file(&path);
}

#[test]
fn hints_auto_replay_on_next_write_to_recovered_node() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let down = reps[2];

    c.transport.set_down(down, true);
    run(c.write_with(b"k", b"v1", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(c.pending_hints(down), 1);

    c.transport.set_down(down, false);
    run(c.write_with(b"k", b"v2", WriteConcern::Quorum(2))).unwrap();
    assert_eq!(c.pending_hints(down), 0, "hints drained automatically");
    assert_eq!(
        c.transport
            .store(down)
            .unwrap()
            .borrow()
            .get(b"k")
            .unwrap()
            .as_deref(),
        Some(&b"v2"[..]),
        "recovered replica converged to the latest value"
    );
}

#[test]
fn hints_only_held_for_successful_writes() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    c.transport.set_down(reps[1], true);
    c.transport.set_down(reps[2], true);
    assert!(run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).is_err());
    assert_eq!(c.pending_hints(reps[1]), 0);
    assert_eq!(c.pending_hints(reps[2]), 0);
}

#[test]
fn sloppy_quorum_recruits_fallback_nodes_when_home_is_down() {
    let c = cluster(5, 3, 2);
    let order = c.ring().replicas_n(b"k", 5);
    let home = c.ring().replicas(b"k");
    assert_eq!(&order[..3], &home[..], "home is the first RF of the walk");
    let fallbacks: Vec<NodeId> = order[3..].to_vec();
    assert_eq!(fallbacks.len(), 2);

    c.transport.set_down(home[0], true);
    c.transport.set_down(home[1], true);
    assert!(matches!(
        run(c.write_with(b"k", b"v", WriteConcern::All)),
        Err(CoordError::QuorumFailed { .. })
    ));

    let ack = run(c.write_sloppy(b"k", b"v", WriteConcern::All)).unwrap();

    assert_eq!(ack.home_acks, 1, "one home replica reachable");
    assert_eq!(ack.fallback_acks, 2, "two fallback acks made up the quorum");
    assert_eq!(ack.hinted, 2, "both down home owners hinted");
    assert!(!ack.is_strict(), "committed partly off-home");

    assert_eq!(
        c.transport
            .store(home[2])
            .unwrap()
            .borrow()
            .get_versioned(b"k")
            .unwrap()
            .unwrap()
            .version,
        ack.version
    );
    let on_fallbacks = fallbacks
        .iter()
        .filter(|&&n| {
            c.transport
                .store(n)
                .unwrap()
                .borrow()
                .get(b"k")
                .unwrap()
                .is_some()
        })
        .count();
    assert!(
        on_fallbacks >= 2,
        "fallbacks absorbed the write: {on_fallbacks}"
    );

    assert_eq!(c.pending_hints(home[0]), 1);
    assert_eq!(c.pending_hints(home[1]), 1);
}

#[test]
fn sloppy_write_hints_migrate_home_on_recovery() {
    let c = cluster(5, 3, 2);
    let home = c.ring().replicas(b"k");
    c.transport.set_down(home[0], true);
    c.transport.set_down(home[1], true);
    let ack = run(c.write_sloppy(b"k", b"hi", WriteConcern::All)).unwrap();
    assert_eq!(c.pending_hints(home[0]), 1);
    assert_eq!(c.pending_hints(home[1]), 1);
    assert_eq!(ack.hinted, 2, "provenance reports the two pending hints");

    c.transport.set_down(home[0], false);
    c.transport.set_down(home[1], false);
    assert_eq!(run(c.replay_hints(home[0])), 1);
    assert_eq!(run(c.replay_hints(home[1])), 1);
    for &n in &home[..2] {
        let s = c.transport.store(n).unwrap().borrow();
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"hi"[..]));
        assert_eq!(s.get_versioned(b"k").unwrap().unwrap().version, ack.version);
    }
}

#[test]
fn sloppy_write_with_healthy_home_behaves_like_strict() {
    let c = cluster(5, 3, 2);
    let order = c.ring().replicas_n(b"k", 5);
    let ack = run(c.write_sloppy(b"k", b"v", WriteConcern::All)).unwrap();
    assert_eq!(ack.home_acks, 3, "all RF home replicas acked");
    assert_eq!(ack.fallback_acks, 0, "no fallback recruited");
    assert_eq!(ack.hinted, 0, "no hints when home is healthy");
    assert!(
        ack.is_strict(),
        "healthy-home sloppy write is strict-readable"
    );
    for &n in &order[3..] {
        assert!(
            c.transport
                .store(n)
                .unwrap()
                .borrow()
                .get(b"k")
                .unwrap()
                .is_none(),
            "fallback {n} must be untouched when home is healthy"
        );
    }
    for &n in &order {
        assert_eq!(c.pending_hints(n), 0, "no hints when home is healthy");
    }
    for &n in &order[..3] {
        assert_eq!(
            c.transport
                .store(n)
                .unwrap()
                .borrow()
                .get_versioned(b"k")
                .unwrap()
                .unwrap()
                .version,
            ack.version
        );
    }
}

#[test]
fn sloppy_write_still_fails_without_enough_reachable_nodes() {
    let c = cluster(5, 3, 2);
    let order = c.ring().replicas_n(b"k", 5);
    for &n in &order[1..] {
        c.transport.set_down(n, true);
    }
    assert!(matches!(
        run(c.write_sloppy(b"k", b"v", WriteConcern::All)),
        Err(CoordError::QuorumFailed { .. })
    ));
}

#[test]
fn sloppy_delete_spills_onto_fallbacks() {
    let c = cluster(5, 3, 2);
    let home = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    c.transport.set_down(home[0], true);
    c.transport.set_down(home[1], true);
    let ack = run(c.delete_sloppy(b"k", WriteConcern::All)).unwrap();
    assert_eq!(ack.home_acks, 1);
    assert_eq!(ack.fallback_acks, 2);
    assert_eq!(ack.hinted, 2);
    assert!(!ack.is_strict());
    assert!(matches!(
        c.transport
            .store(home[2])
            .unwrap()
            .borrow()
            .get_versioned(b"k")
            .unwrap(),
        Some(s) if s.value.is_none()
    ));
    assert_eq!(c.pending_hints(home[0]), 1);
    assert_eq!(c.pending_hints(home[1]), 1);
}

#[test]
fn write_batch_all_ok_lands_on_every_replica() {
    let c = cluster(5, 3, 2);
    let writes: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3")];
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();

    assert!(res.all_ok());
    assert_eq!(res.outcomes.len(), 3);
    assert_eq!(res.failures().count(), 0);
    for (i, &(key, val)) in writes.iter().enumerate() {
        let version = match &res.outcomes[i] {
            WriteOutcome::Ok { version, acks } => {
                assert_eq!(*acks, 3);
                *version
            }
            other => panic!("expected Ok, got {other:?}"),
        };
        let got = run(c.read(key)).unwrap().unwrap();
        assert_eq!(got.value.as_deref(), Some(val));
        assert_eq!(got.version, version);
        for node in c.ring().replicas(key) {
            let s = c.transport.store(node).unwrap().borrow();
            assert_eq!(s.get_versioned(key).unwrap().unwrap().version, version);
        }
    }
}

#[test]
fn write_batch_stamps_distinct_versions_even_for_duplicate_keys() {
    let c = cluster(3, 3, 2);
    let writes: &[(&[u8], &[u8])] = &[(b"k", b"first"), (b"k", b"second")];
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();
    assert!(res.all_ok());
    let (v0, v1) = match (&res.outcomes[0], &res.outcomes[1]) {
        (WriteOutcome::Ok { version: a, .. }, WriteOutcome::Ok { version: b, .. }) => (*a, *b),
        other => panic!("expected two Ok, got {other:?}"),
    };
    assert!(v1.wins_over(&v0), "later duplicate must dominate");
    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"second"[..])
    );
}

#[test]
fn write_batch_reports_per_write_partial_failure() {
    let c = cluster(5, 3, 2);
    let writes: &[(&[u8], &[u8])] = &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")];

    c.transport.set_down(2, true);
    c.transport.set_down(4, true);
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();
    assert_eq!(res.outcomes.len(), 4);

    for (i, &(key, _)) in writes.iter().enumerate() {
        let reps = c.ring().replicas(key);
        let reachable = reps.iter().filter(|n| ![2, 4].contains(n)).count();
        match &res.outcomes[i] {
            WriteOutcome::Ok { acks, .. } => {
                assert_eq!(*acks, reps.len(), "Replicated Ok means all RF acked");
                assert_eq!(reachable, reps.len());
            }
            WriteOutcome::Failed { acks, error } => {
                assert_eq!(*acks, reachable, "Failed still reports partial acks");
                assert!(matches!(error, CoordError::QuorumFailed { .. }));
                assert!(reachable < reps.len());
            }
        }
    }
}

#[test]
fn write_batch_holds_hints_for_unreached_replicas() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    let down = reps[2];
    c.transport.set_down(down, true);

    let writes: &[(&[u8], &[u8])] = &[(b"k", b"v")];
    let res = run(c.write_batch(writes, WriteConcern::Quorum(2))).unwrap();
    assert!(res.all_ok(), "W=2 of 3 reachable -> committed");
    assert_eq!(c.pending_hints(down), 1, "hint held for the down replica");

    c.transport.set_down(down, false);
    assert_eq!(run(c.replay_hints(down)), 1);
    assert_eq!(
        c.transport
            .store(down)
            .unwrap()
            .borrow()
            .get(b"k")
            .unwrap()
            .as_deref(),
        Some(&b"v"[..])
    );
}

struct StaleNodeCluster {
    inner: LocalCluster<MemBlockDevice>,
    stale_node: NodeId,
    epoch: u64,
}

impl Transport for StaleNodeCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        if node == self.stale_node {
            return Err(TransportError::StaleEpoch(self.epoch));
        }
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
    async fn apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> Vec<BatchAck> {
        if node == self.stale_node {
            return vec![BatchAck::Stale(self.epoch); writes.len()];
        }
        self.inner.apply_batch(node, zone_id, sync, writes).await
    }
}

fn stale_node_cluster(epoch: u64) -> (Coordinator<StaleNodeCluster>, NodeId) {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let stale_node = ring.replicas(b"k")[2];
    let c = Coordinator::new(
        0,
        ring,
        2,
        StaleNodeCluster {
            inner: lc,
            stale_node,
            epoch,
        },
    );
    (c, stale_node)
}

#[test]
fn batch_does_not_hint_a_healthy_stale_refusing_replica() {
    let (c, stale_node) = stale_node_cluster(42);
    let writes: &[(&[u8], &[u8])] = &[(b"k", b"v")];
    let res = run(c.write_batch(writes, WriteConcern::Quorum(2))).unwrap();
    assert!(res.all_ok(), "W=2 of 3 reachable -> committed");
    assert_eq!(
        c.pending_hints(stale_node),
        0,
        "a healthy stale-ring refusal must not be hinted"
    );
}

#[test]
fn batch_quorum_failure_with_a_stale_refusal_surfaces_stale_ring() {
    let (c, _) = stale_node_cluster(42);
    let writes: &[(&[u8], &[u8])] = &[(b"k", b"v")];
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();
    match &res.outcomes[0] {
        WriteOutcome::Failed { error, acks } => {
            assert_eq!(*acks, 2, "the two fresh replicas still acked");
            assert_eq!(
                *error,
                CoordError::StaleRing { epoch: 42 },
                "a stale refusal must outrank QuorumFailed in the per-write error"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

#[test]
fn an_epoch_zero_stale_refusal_still_surfaces_stale_ring() {
    let (c, _) = stale_node_cluster(0);
    assert_eq!(
        run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap_err(),
        CoordError::StaleRing { epoch: 0 },
        "a held replica that has no epoch yet refuses with StaleEpoch(0), not a dead quorum"
    );
    let writes: &[(&[u8], &[u8])] = &[(b"k", b"v")];
    let res = run(c.write_batch(writes, WriteConcern::All)).unwrap();
    match &res.outcomes[0] {
        WriteOutcome::Failed { error, .. } => {
            assert_eq!(*error, CoordError::StaleRing { epoch: 0 });
        }
        other => panic!("expected Failed, got {other:?}"),
    }
}

struct BusyCluster {
    inner: LocalCluster<MemBlockDevice>,
    busy_left: RefCell<u32>,
}

impl Transport for BusyCluster {
    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        {
            let mut left = self.busy_left.borrow_mut();
            if *left > 0 {
                if *left != u32::MAX {
                    *left -= 1;
                }
                return Err(TransportError::Busy);
            }
        }
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn busy_coord(busy_left: u32, threshold: u32) -> Coordinator<BusyCluster> {
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    Coordinator::new(
        0,
        ring,
        2,
        BusyCluster {
            inner: lc,
            busy_left: RefCell::new(busy_left),
        },
    )
    .with_failure_detector(crate::failure::FdConfig {
        failure_threshold: threshold,
        open_cooldown_ms: 1000,
    })
}

#[test]
fn reserve_floor_busy_fails_with_coorderror_busy_no_hints_no_fd_trip() {
    let c = busy_coord(u32::MAX, 1);
    let reps = c.ring().replicas(b"k");
    let err = run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))).unwrap_err();
    match err {
        CoordError::Busy { needed, got, busy } => {
            assert_eq!((needed, got, busy), (2, 0, 3),
                "a backpressure quorum miss reports needed/got/busy, not QuorumFailed");
        }
        other => panic!("expected CoordError::Busy, got {other:?}"),
    }
    for &n in &reps {
        assert_eq!(
            c.pending_hints(n),
            0,
            "a healthy backpressure refusal must NOT store a hint"
        );
        assert!(
            !c.is_node_down(n),
            "a healthy backpressure refusal must NOT trip the failure detector"
        );
    }
}

#[test]
fn a_client_backing_off_on_busy_rides_out_the_merge_window() {
    let c = busy_coord(6, 1);
    let reps = c.ring().replicas(b"k");
    let mut attempts = 0;
    loop {
        attempts += 1;
        match run(c.write_with(b"k", b"v", WriteConcern::Quorum(2))) {
            Ok(_) => break,
            Err(CoordError::Busy { .. }) => {
                assert!(attempts < 50, "must ride out the window in a few retries");
                continue;
            }
            Err(other) => panic!("only Busy is retryable here, got {other:?}"),
        }
    }
    assert!(attempts > 1, "the merge window forced at least one backoff+retry");
    assert_eq!(
        run(c.read(b"k")).unwrap().and_then(|s| s.value),
        Some(b"v".to_vec()),
        "the retried write is durably present"
    );
    for &n in &reps {
        assert_eq!(c.pending_hints(n), 0, "no hints from backpressure retries");
        assert!(!c.is_node_down(n), "the FD never tripped on backpressure");
    }
}

#[test]
fn empty_batch_is_ok_and_empty() {
    let c = cluster(3, 3, 2);
    let res = run(c.write_batch(&[], WriteConcern::All)).unwrap();
    assert!(res.all_ok());
    assert!(res.outcomes.is_empty());
}

#[test]
fn no_replicas_when_ring_empty() {
    let lc = LocalCluster::<MemBlockDevice>::new();
    let c = Coordinator::new(0, Ring::with_vnodes(3, 16), 1, lc);
    assert_eq!(run(c.write(b"k", b"v")), Err(CoordError::NoReplicas));
    assert_eq!(run(c.read(b"k")), Err(CoordError::NoReplicas));
}

use crate::zone::{Ordering as ZOrdering, ZoneConfig};

fn zoned_cluster() -> Coordinator<LocalCluster<MemBlockDevice>> {
    let mut lc = LocalCluster::new();
    lc.define_zone(
        1,
        ZoneConfig {
            name: "z1".into(),
            rf: 2,
            ordering: ZOrdering::Unsorted,
            scope: None,
        },
    );
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3u32 {
        lc.insert(node, replica(node));
        lc.insert_zone(node, 1, replica(node));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, lc);
    run(c.refresh_catalog(1)).unwrap();
    c
}

fn sorted_replica(node: NodeId) -> VersionedStore<MemBlockDevice> {
    let dev = MemBlockDevice::new(BS, 256);
    let engine = Engine::create_prewiped(
        dev,
        Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        },
    );
    VersionedStore::new(engine, node).unwrap()
}

fn sorted_cluster() -> Coordinator<LocalCluster<MemBlockDevice>> {
    let mut lc = LocalCluster::new();
    lc.define_zone(
        2,
        ZoneConfig {
            name: "s".into(),
            rf: 3,
            ordering: ZOrdering::Sorted,
            scope: None,
        },
    );
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3u32 {
        lc.insert_zone(node, 2, sorted_replica(node));
        ring.add_node(node);
    }
    let c = Coordinator::new(0, ring, 2, lc);
    run(c.refresh_catalog(1)).unwrap();
    c
}

#[test]
fn scan_returns_ordered_live_results_and_hides_tombstones() {
    let c = sorted_cluster();
    for (k, v) in [
        (&b"banana"[..], &b"y"[..]),
        (b"apple", b"r"),
        (b"date", b"br"),
        (b"cherry", b"d"),
    ] {
        run(c.write_in(2, k, v, WriteConcern::All)).unwrap();
    }
    run(c.delete_in(2, b"cherry", WriteConcern::All)).unwrap();

    let all = run(c.scan_in(2, None, None, 2, None)).unwrap();
    let got: Vec<(&[u8], &[u8])> = all.iter().map(|(k, v)| (&k[..], &v[..])).collect();
    assert_eq!(
        got,
        vec![
            (&b"apple"[..], &b"r"[..]),
            (b"banana", b"y"),
            (b"date", b"br"),
        ]
    );

    let mid = run(c.scan_in(2, Some(b"banana"), Some(b"date"), 2, None)).unwrap();
    let mid_keys: Vec<&[u8]> = mid.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(mid_keys, vec![&b"banana"[..]]);

    let limited = run(c.scan_in(2, None, None, 2, Some(2))).unwrap();
    assert_eq!(limited.len(), 2);
    assert_eq!(limited[0].0, b"apple");
    assert_eq!(limited[1].0, b"banana");
}

#[test]
fn scan_merges_replicas_by_lww() {
    let c = sorted_cluster();
    run(c.write_in(2, b"k", b"v1", WriteConcern::All)).unwrap();
    let reps = c.replicas_for(2, b"k").unwrap();
    c.transport.set_down(reps[2], true);
    run(c.write_in(2, b"k", b"v2", WriteConcern::Quorum(2))).unwrap();
    c.transport.set_down(reps[2], false);

    let all = run(c.scan_in(2, None, None, 3, None)).unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].1, b"v2");
}

#[test]
fn paged_scan_equals_unpaged_scan() {
    let c = sorted_cluster();
    for k in ["a", "b", "c", "d", "e", "f"] {
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    run(c.delete_in(2, b"d", WriteConcern::All)).unwrap();
    let unpaged = run(c.scan_in(2, None, None, 2, None)).unwrap();

    c.transport.set_scan_page_entries(1);
    assert_eq!(run(c.scan_in(2, None, None, 2, None)).unwrap(), unpaged);
    let mid = run(c.scan_in(2, Some(b"b"), Some(b"e"), 2, None)).unwrap();
    let mid_keys: Vec<&[u8]> = mid.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(mid_keys, vec![&b"b"[..], b"c"]);
    let limited = run(c.scan_in(2, None, None, 2, Some(3))).unwrap();
    assert_eq!(&limited[..], &unpaged[..3]);
}

fn divergent_two_node_cluster() -> Coordinator<LocalCluster<MemBlockDevice>> {
    let mut lc = LocalCluster::new();
    lc.define_zone(
        2,
        ZoneConfig {
            name: "s".into(),
            rf: 2,
            ordering: ZOrdering::Sorted,
            scope: None,
        },
    );
    let mut ring = Ring::with_vnodes(2, 64);
    for node in 1..=2u32 {
        lc.insert_zone(node, 2, sorted_replica(node));
        ring.add_node(node);
    }
    let ver = |hlc: u64| Version { hlc, node: 9 };
    {
        let s1 = lc.store_zone(1, 2).unwrap();
        s1.borrow_mut().apply(b"a", Some(b"a"), ver(10)).unwrap();
        s1.borrow_mut().apply(b"b", Some(b"b"), ver(10)).unwrap();
        s1.borrow_mut().apply(b"c", Some(b"new"), ver(20)).unwrap();
        let s2 = lc.store_zone(2, 2).unwrap();
        s2.borrow_mut()
            .apply(b"c", Some(b"stale"), ver(10))
            .unwrap();
    }
    let c = Coordinator::new(0, ring, 2, lc);
    run(c.refresh_catalog(1)).unwrap();
    c
}

#[test]
fn paged_scan_merges_lww_only_below_the_min_covered_bound() {
    let c = divergent_two_node_cluster();
    c.transport.set_scan_page_entries(2);
    let all = run(c.scan_in(2, None, None, 2, None)).unwrap();
    assert_eq!(
        all,
        vec![
            (b"a".to_vec(), b"a".to_vec()),
            (b"b".to_vec(), b"b".to_vec()),
            (b"c".to_vec(), b"new".to_vec()),
        ]
    );

    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(keys, vec![&b"a"[..], b"b", b"c"]);
}

#[test]
fn paged_scan_limit_spans_tombstone_heavy_pages() {
    let c = sorted_cluster();
    for k in ["a", "b", "c", "d", "e"] {
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    for k in ["b", "c", "d"] {
        run(c.delete_in(2, k.as_bytes(), WriteConcern::All)).unwrap();
    }
    c.transport.set_scan_page_entries(2);
    let two = run(c.scan_in(2, None, None, 2, Some(2))).unwrap();
    assert_eq!(
        two,
        vec![
            (b"a".to_vec(), b"a".to_vec()),
            (b"e".to_vec(), b"e".to_vec())
        ]
    );
}

fn sorted_inner() -> (LocalCluster<MemBlockDevice>, Ring) {
    let mut lc = LocalCluster::new();
    lc.define_zone(
        2,
        ZoneConfig {
            name: "s".into(),
            rf: 3,
            ordering: ZOrdering::Sorted,
            scope: None,
        },
    );
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3u32 {
        lc.insert_zone(node, 2, sorted_replica(node));
        ring.add_node(node);
    }
    (lc, ring)
}

#[test]
fn paged_unlimited_scan_with_prefetch_matches_the_serial_path() {
    let c = sorted_cluster();
    for i in 0..50u32 {
        let k = format!("key{i:03}");
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    for i in [7u32, 13, 28, 41] {
        let k = format!("key{i:03}");
        run(c.delete_in(2, k.as_bytes(), WriteConcern::All)).unwrap();
    }
    let unpaged = run(c.scan_in(2, None, None, 2, None)).unwrap();

    c.transport.set_scan_page_entries(3);

    c.set_scan_prefetch(false);
    let off = run(c.scan_in(2, None, None, 2, None)).unwrap();
    c.set_scan_prefetch(true);
    let on = run(c.scan_in(2, None, None, 2, None)).unwrap();

    assert_eq!(off, unpaged, "serial paged scan == unpaged scan");
    assert_eq!(
        on, off,
        "prefetched scan is byte-identical to the serial path"
    );

    c.set_scan_prefetch(false);
    let off_mid = run(c.scan_in(2, Some(b"key010"), Some(b"key040"), 2, None)).unwrap();
    c.set_scan_prefetch(true);
    let on_mid = run(c.scan_in(2, Some(b"key010"), Some(b"key040"), 2, None)).unwrap();
    assert_eq!(on_mid, off_mid);
}

#[derive(Clone)]
enum ScanFault {
    Stale(u64),
    Unavailable,
    PerNode(Vec<(NodeId, TransportError)>),
}

struct ScanFaultCluster {
    inner: LocalCluster<MemBlockDevice>,
    fail_nodes: Vec<NodeId>,
    from_key: Vec<u8>,
    fault: ScanFault,
    heal_on_refresh: bool,
    refreshed: Cell<bool>,
}

impl Transport for ScanFaultCluster {
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
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
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
        let healed = self.heal_on_refresh && self.refreshed.get();
        let past_boundary = lo.is_some_and(|l| l >= &self.from_key[..]);
        if !healed && past_boundary && self.fail_nodes.contains(&node) {
            return match &self.fault {
                ScanFault::Stale(ep) => Err(TransportError::StaleEpoch(*ep)),
                ScanFault::Unavailable => Err(TransportError::Unavailable),
                ScanFault::PerNode(errors) => Err(errors
                    .iter()
                    .find(|(n, _)| *n == node)
                    .map(|(_, e)| e.clone())
                    .unwrap()),
            };
        }
        self.inner.scan(node, zone_id, lo, hi, max).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.refreshed.set(true);
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn scan_fault_coord(
    fail_nodes: Vec<NodeId>,
    from_key: &[u8],
    fault: ScanFault,
    heal: bool,
) -> Coordinator<ScanFaultCluster> {
    let (lc, ring) = sorted_inner();
    let c = Coordinator::new(
        0,
        ring,
        2,
        ScanFaultCluster {
            inner: lc,
            fail_nodes,
            from_key: from_key.to_vec(),
            fault,
            heal_on_refresh: heal,
            refreshed: Cell::new(false),
        },
    );
    run(c.refresh_catalog(1)).unwrap();
    c
}

#[test]
fn prefetched_scan_recovers_from_a_mid_scan_stale_refusal() {
    let c = scan_fault_coord(vec![1, 2], b"key015", ScanFault::Stale(9), true);
    let mut expected = Vec::new();
    for i in 0..40u32 {
        let k = format!("key{i:03}");
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
        expected.push((k.as_bytes().to_vec(), k.as_bytes().to_vec()));
    }
    c.transport.inner.set_scan_page_entries(3);
    c.set_scan_prefetch(true);
    let got = run(c.scan_in(2, None, None, 2, None)).unwrap();
    assert_eq!(
        got, expected,
        "scan completes correctly after the single stale retry"
    );
}

#[test]
fn prefetched_scan_surfaces_a_mid_scan_quorum_failure() {
    let c = scan_fault_coord(vec![1, 2], b"key015", ScanFault::Unavailable, false);
    for i in 0..40u32 {
        let k = format!("key{i:03}");
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    c.transport.inner.set_scan_page_entries(3);
    c.set_scan_prefetch(true);
    let err = run(c.scan_in(2, None, None, 2, None)).unwrap_err();
    let CoordError::ScanQuorumFailed {
        zone_id: 2,
        cursor: Some(cursor),
        needed: 2,
        got: 1,
        replicas,
    } = &err
    else {
        panic!("expected ScanQuorumFailed, got {err:?}");
    };
    assert!(cursor.as_slice() >= b"key015".as_slice(), "{err}");
    let mut nodes: Vec<_> = replicas.iter().map(|r| (r.node, r.error.clone())).collect();
    nodes.sort_by_key(|(n, _)| *n);
    assert_eq!(
        nodes,
        vec![(1, TransportError::Unavailable), (2, TransportError::Unavailable)]
    );
}

#[test]
fn scan_quorum_failure_names_each_failed_replica_and_its_cause() {
    let corrupt = "corrupt record: length mismatch";
    let c = scan_fault_coord(
        vec![1, 2],
        b"key015",
        ScanFault::PerNode(vec![
            (1, TransportError::Remote(corrupt.into())),
            (2, TransportError::Busy),
        ]),
        false,
    );
    for i in 0..40u32 {
        let k = format!("key{i:03}");
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    c.transport.inner.set_scan_page_entries(3);
    let err = run(c.scan_in(2, None, None, 2, None)).unwrap_err();
    let CoordError::ScanQuorumFailed {
        zone_id: 2,
        cursor: Some(cursor),
        needed: 2,
        got: 1,
        replicas,
    } = &err
    else {
        panic!("expected ScanQuorumFailed, got {err:?}");
    };
    let mut causes: Vec<_> = replicas.iter().map(|r| (r.node, r.error.clone())).collect();
    causes.sort_by_key(|(n, _)| *n);
    assert_eq!(
        causes,
        vec![
            (1, TransportError::Remote(corrupt.into())),
            (2, TransportError::Busy),
        ]
    );
    let text = err.to_string();
    let hex: String = cursor.iter().map(|b| format!("{b:02x}")).collect();
    for want in [
        "scan quorum failed: needed 2, got 1 (zone 2, cursor ",
        hex.as_str(),
        &format!("node 1: remote: {corrupt}"),
        "node 2: busy",
    ] {
        assert!(text.contains(want), "{text:?} lacks {want:?}");
    }
}

struct CountingScanCluster {
    inner: LocalCluster<MemBlockDevice>,
    scans: Cell<usize>,
}

impl Transport for CountingScanCluster {
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
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
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
        self.scans.set(self.scans.get() + 1);
        self.inner.scan(node, zone_id, lo, hi, max).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

#[test]
fn limited_scan_does_not_prefetch() {
    let (lc, ring) = sorted_inner();
    lc.set_scan_page_entries(2);
    let c = Coordinator::new(
        0,
        ring,
        2,
        CountingScanCluster {
            inner: lc,
            scans: Cell::new(0),
        },
    );
    run(c.refresh_catalog(1)).unwrap();
    for i in 0..20u32 {
        let k = format!("key{i:03}");
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }

    c.set_scan_prefetch(false);
    c.transport.scans.set(0);
    let off = run(c.scan_in(2, None, None, 2, Some(5))).unwrap();
    let off_calls = c.transport.scans.get();

    c.set_scan_prefetch(true);
    c.transport.scans.set(0);
    let on = run(c.scan_in(2, None, None, 2, Some(5))).unwrap();
    let on_calls = c.transport.scans.get();

    assert_eq!(
        on, off,
        "limited scan result unaffected by the prefetch flag"
    );
    assert_eq!(
        on_calls, off_calls,
        "limited scan must issue no extra (prefetched) scan RPCs"
    );
    assert_eq!(on.len(), 5);
}

#[test]
fn scan_across_multiple_ranges_is_globally_sorted() {
    let c = sorted_cluster();
    c.set_range_map(2, RangeMap::from_splits([b"m".to_vec(), b"t".to_vec()]));
    for k in ["a", "f", "m", "p", "t", "z"] {
        run(c.write_in(2, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    let all = run(c.scan_in(2, None, None, 2, None)).unwrap();
    let keys: Vec<&[u8]> = all.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(keys, vec![&b"a"[..], b"f", b"m", b"p", b"t", b"z"]);

    let span = run(c.scan_in(2, Some(b"f"), Some(b"u"), 2, None)).unwrap();
    let span_keys: Vec<&[u8]> = span.iter().map(|(k, _)| &k[..]).collect();
    assert_eq!(span_keys, vec![&b"f"[..], b"m", b"p", b"t"]);
}

#[test]
fn scan_rejects_an_unsorted_zone() {
    let c = cluster(3, 3, 2);
    assert_eq!(
        run(c.scan_in(DEFAULT_ZONE, None, None, 2, None)),
        Err(CoordError::NotSorted(DEFAULT_ZONE))
    );
}

#[test]
fn zone_isolation_same_key_independent_values() {
    let c = zoned_cluster();
    run(c.write_with(b"k", b"default-val", WriteConcern::Quorum(2))).unwrap();
    run(c.write_in(1, b"k", b"zone1-val", WriteConcern::Quorum(2))).unwrap();

    assert_eq!(
        run(c.read(b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"default-val"[..])
    );
    assert_eq!(
        run(c.read_in(1, b"k")).unwrap().unwrap().value.as_deref(),
        Some(&b"zone1-val"[..])
    );
}

#[test]
fn per_zone_rf_places_on_the_right_count() {
    let c = zoned_cluster();
    let reps = c.replicas_for(1, b"k").unwrap();
    assert_eq!(reps.len(), 2, "zone 1 rf=2");
    run(c.write_in(1, b"k", b"v", WriteConcern::All)).unwrap();
    let mut holders = 0;
    for node in 1..=3u32 {
        let s = c.transport.store_zone(node, 1).unwrap().borrow();
        if s.get(b"k").unwrap().is_some() {
            holders += 1;
        }
    }
    assert_eq!(holders, 2, "exactly rf=2 nodes hold the zone-1 key");
}

#[test]
fn unknown_zone_write_errors_after_refresh() {
    let c = zoned_cluster();
    assert_eq!(
        run(c.write_in(9, b"k", b"v", WriteConcern::Quorum(2))),
        Err(CoordError::UnknownZone(9))
    );
}

#[test]
fn cas_insert_only_succeeds_once_on_an_absent_key() {
    let c = cluster(3, 3, 2);
    let v = match run(c.cas(b"k", b"v1", None)) {
        Ok(CasResult::Applied(v)) => v,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert_eq!(
        run(c.read(b"k")).unwrap().map(|s| s.value),
        Some(Some(b"v1".to_vec())),
    );
    assert_eq!(
        run(c.cas(b"k", b"v2", None)),
        Ok(CasResult::Mismatch { current: Some(v) }),
    );
    assert_eq!(
        run(c.read(b"k")).unwrap().map(|s| s.value),
        Some(Some(b"v1".to_vec())),
    );
}

#[test]
fn cas_applies_on_a_matching_version_and_rejects_a_stale_one() {
    let c = cluster(3, 3, 2);
    run(c.write_with(b"k", b"v1", WriteConcern::All)).unwrap();
    let v1 = run(c.read(b"k")).unwrap().unwrap().version;

    let v2 = match run(c.cas(b"k", b"v2", Some(v1))) {
        Ok(CasResult::Applied(v2)) => v2,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert!(v2 > v1, "the CAS write got a newer version");
    assert_eq!(
        run(c.read(b"k")).unwrap().map(|s| s.value),
        Some(Some(b"v2".to_vec())),
    );

    assert_eq!(
        run(c.cas(b"k", b"v3", Some(v1))),
        Ok(CasResult::Mismatch { current: Some(v2) }),
    );
    assert_eq!(
        run(c.read(b"k")).unwrap().map(|s| s.value),
        Some(Some(b"v2".to_vec())),
    );
}

#[test]
fn cas_dominates_a_current_version_from_an_ahead_coordinator_clock() {
    let c = cluster(3, 3, 2);
    let ahead = Version {
        hlc: (physical_now_ms() + 300) << 16,
        node: 99,
    };
    for node in 1..=3 {
        assert!(matches!(
            run(c
                .transport
                .apply(node, DEFAULT_ZONE, b"k", Some(b"v0"), ahead)),
            Ok(Applied::Won)
        ));
    }

    let cur = run(c.read(b"k")).unwrap().unwrap().version;
    assert_eq!(cur, ahead);
    let v2 = match run(c.cas(b"k", b"v1", Some(cur))) {
        Ok(CasResult::Applied(v2)) => v2,
        other => panic!("expected Applied, got {other:?}"),
    };
    assert!(v2 > cur, "the CAS stamp dominates the version it validated");
    let got = run(c.read(b"k")).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"v1"[..]));
    assert_eq!(got.version, v2);
}

#[test]
fn cas_insert_if_absent_mismatches_against_a_tombstone() {
    let c = cluster(3, 3, 2);
    run(c.write_with(b"k", b"v1", WriteConcern::All)).unwrap();
    let del = run(c.delete(b"k", WriteConcern::All)).unwrap();
    assert_eq!(run(c.read(b"k")).unwrap(), None);
    assert_eq!(
        run(c.cas(b"k", b"v2", None)),
        Ok(CasResult::Mismatch { current: Some(del) }),
    );
    match run(c.cas(b"k", b"v2", Some(del))) {
        Ok(CasResult::Applied(_)) => {}
        other => panic!("expected Applied, got {other:?}"),
    }
    assert_eq!(
        run(c.read(b"k")).unwrap().map(|s| s.value),
        Some(Some(b"v2".to_vec())),
    );
}

#[test]
fn cas_fails_quorum_when_too_few_replicas_are_up() {
    let c = cluster(3, 3, 2);
    let reps = c.ring().replicas(b"k");
    c.transport.set_down(reps[0], true);
    c.transport.set_down(reps[1], true);
    assert_eq!(
        run(c.cas(b"k", b"v", None)),
        Err(CoordError::QuorumFailed { needed: 2, got: 1 }),
    );
}

fn page(more: bool, entries: &[(&[u8], u64)]) -> ScanPage {
    ScanPage {
        more,
        fragments: Vec::new(),
        resume: None,
        entries: entries
            .iter()
            .map(|(k, hlc)| {
                (
                    k.to_vec(),
                    Stored {
                        version: Version { hlc: *hlc, node: 1 },
                        value: Some(b"v".to_vec()),
                    },
                )
            })
            .collect(),
    }
}

#[test]
fn scan_merge_bound_is_the_min_last_key_over_truncated_pages() {
    assert_eq!(scan_merge_bound(&[page(false, &[(b"a", 1)])]), None);
    assert_eq!(
        scan_merge_bound(&[page(true, &[(b"a", 1), (b"c", 1)])]),
        Some(b"c\0".to_vec())
    );
    assert_eq!(
        scan_merge_bound(&[
            page(true, &[(b"a", 1), (b"m", 1)]),
            page(true, &[(b"a", 1), (b"e", 1)]),
        ]),
        Some(b"e\0".to_vec())
    );
}

#[test]
fn scan_merge_lww_picks_winners_below_bound_and_drops_the_rest() {
    let merged = scan_merge_lww(
        vec![page(false, &[(b"k", 5)]), page(false, &[(b"k", 9)])],
        None,
    );
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[b"k".as_slice()].version.hlc, 9);

    let merged = scan_merge_lww(
        vec![page(true, &[(b"a", 1), (b"c", 1), (b"z", 1)])],
        Some(b"c\0"),
    );
    assert_eq!(
        merged.keys().cloned().collect::<Vec<_>>(),
        vec![b"a".to_vec(), b"c".to_vec()]
    );
}

struct MutableCluster {
    inner: LocalCluster<MemBlockDevice>,
    view: RefCell<RingView>,
}

impl MutableCluster {
    fn new(view: RingView) -> Self {
        let mut inner = LocalCluster::new();
        for node in 1..=5 {
            inner.insert(node, replica(node));
        }
        Self {
            inner,
            view: RefCell::new(view),
        }
    }
    fn set_view(&self, view: RingView) {
        *self.view.borrow_mut() = view;
    }
}

impl Transport for MutableCluster {
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
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
        Ok(self.view.borrow().clone())
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn ring_view(epoch: u64, n: NodeId) -> RingView {
    RingView {
        epoch,
        vnodes: 64,
        rf: 3,
        members: (1..=n).map(|node| (node, 1)).collect(),
        addrs: Vec::new(),
    }
}

fn mutable_coord() -> Coordinator<MutableCluster> {
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        ring.add_node(node);
    }
    Coordinator::new(0, ring, 2, MutableCluster::new(ring_view(0, 3)))
}

#[test]
fn adopt_from_any_member_replaces_the_ring_with_no_op_issued() {
    let coord = mutable_coord();
    assert_eq!(coord.ring_epoch(), 0);
    coord.transport().set_view(ring_view(1, 5));
    assert!(run(coord.adopt_from_any_member()));
    let mut members: Vec<NodeId> = coord.ring().members().collect();
    members.sort();
    assert_eq!(members, vec![1, 2, 3, 4, 5]);
    assert_eq!(coord.ring_epoch(), 1);
}

#[test]
fn proactive_refresh_loop_adopts_a_new_ring_in_the_background() {
    use crate::reactor::Timer;
    use std::time::Duration;

    let svc =
        CoordinatorService::new(mutable_coord()).with_proactive_refresh(Duration::from_millis(1));
    svc.coordinator().transport().set_view(ring_view(1, 5));

    let fg = Timer::every(Duration::from_millis(1)).expect("foreground timerfd");
    for _ in 0..2000 {
        if svc.coordinator().ring_epoch() >= 1 {
            break;
        }
        svc.run(fg.tick());
    }
    svc.shutdown();

    assert_eq!(
        svc.coordinator().ring_epoch(),
        1,
        "the background loop should adopt the new ring without any op issued"
    );
    let mut members: Vec<NodeId> = svc.coordinator().ring().members().collect();
    members.sort();
    assert_eq!(members, vec![1, 2, 3, 4, 5]);
}

struct HedgeCluster {
    inner: LocalCluster<MemBlockDevice>,
    digests: RefCell<HashMap<NodeId, usize>>,
    slow: Cell<Option<NodeId>>,
    clock: RefCell<Option<std::rc::Rc<HedgeClock>>>,
}

impl HedgeCluster {
    fn set_slow(&self, node: Option<NodeId>) {
        self.slow.set(node);
    }
    fn set_clock(&self, clock: std::rc::Rc<HedgeClock>) {
        *self.clock.borrow_mut() = Some(clock);
    }
    fn digest_count(&self, node: NodeId) -> usize {
        self.digests.borrow().get(&node).copied().unwrap_or(0)
    }
}

struct StalledDigest {
    clock: Option<std::rc::Rc<HedgeClock>>,
}
impl Future for StalledDigest {
    type Output = Result<Option<Version>, TransportError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(c) = &self.clock {
            c.advance();
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Transport for HedgeCluster {
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
    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.inner.read(node, zone_id, key).await
    }
    async fn digest(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Version>, TransportError> {
        *self.digests.borrow_mut().entry(node).or_default() += 1;
        if self.slow.get() == Some(node) {
            let clock = self.clock.borrow().clone();
            return StalledDigest { clock }.await;
        }
        self.inner.digest(node, zone_id, key).await
    }
    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        self.inner.members(node).await
    }
    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn hedge_coord() -> Coordinator<HedgeCluster> {
    use std::time::Duration;
    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(3, 64);
    for node in 1..=3 {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let transport = HedgeCluster {
        inner: lc,
        digests: RefCell::new(HashMap::new()),
        slow: Cell::new(None),
        clock: RefCell::new(None),
    };
    let coord = Coordinator::new(0, ring, 2, transport).with_digest_hedge(Duration::from_millis(4));
    let clock = coord.hedge_clock().expect("hedge configured");
    coord.transport.set_clock(clock);
    coord
}

#[test]
fn digest_hedge_activates_reserve_on_a_slow_replica() {
    let c = hedge_coord();
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();

    c.transport.set_slow(Some(reps[1]));
    let before = vals_client::observe::snapshot().digest_hedges_fired;

    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(
        got.value.as_deref(),
        Some(&b"v"[..]),
        "correct value returned"
    );

    let after = vals_client::observe::snapshot().digest_hedges_fired;
    assert!(
        c.transport.digest_count(reps[2]) >= 1,
        "the reserve replica received a hedged digest"
    );
    assert!(
        after > before,
        "the hedge fired (digest_hedges_fired rose: {before} -> {after})"
    );
}

#[test]
fn digest_read_many_hedges_a_grouped_slow_probe_on_the_timer() {
    let c = hedge_coord();
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    c.transport.set_slow(Some(reps[1]));
    let before = vals_client::observe::snapshot().digest_hedges_fired;

    let got = run(c.read_many(&[b"k".to_vec()])).unwrap();
    assert_eq!(
        got[0].as_ref().unwrap().as_ref().unwrap().value.as_deref(),
        Some(&b"v"[..])
    );
    assert!(c.transport.digest_count(reps[2]) >= 1);
    assert!(vals_client::observe::snapshot().digest_hedges_fired > before);
}

#[test]
fn digest_read_does_not_hedge_when_all_replicas_are_fast() {
    let c = hedge_coord();
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();

    c.transport.set_slow(None);
    let got = run(c.read_quorum(b"k", 2)).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"v"[..]));
    assert_eq!(
        c.transport.digest_count(reps[2]),
        0,
        "a fast read never escalates to the reserve replica"
    );
}

#[test]
fn digest_read_many_does_not_activate_grouped_reserve_when_fast() {
    let c = hedge_coord();
    let reps = c.ring().replicas(b"k");
    run(c.write_with(b"k", b"v", WriteConcern::All)).unwrap();
    let got = run(c.read_many(&[b"k".to_vec()])).unwrap();
    assert!(got[0].as_ref().unwrap().is_some());
    assert_eq!(c.transport.digest_count(reps[2]), 0);
}

#[test]
fn digest_hedge_ticker_advances_the_clock_under_the_service() {
    use crate::reactor::Timer;
    use std::time::Duration;

    let svc = CoordinatorService::new(hedge_coord());
    let clock = svc.coordinator().hedge_clock().expect("hedge configured");
    let start = clock.now();

    let fg = Timer::every(Duration::from_millis(1)).expect("foreground timerfd");
    for _ in 0..2000 {
        if clock.now() > start {
            break;
        }
        svc.run(fg.tick());
    }
    svc.shutdown();

    assert!(
        clock.now() > start,
        "the shared hedge ticker advanced the clock ({start} -> {})",
        clock.now()
    );
}

#[test]
fn submit_write_batch_outcome_matches_blocking() {
    let svc = CoordinatorService::new(cluster(5, 3, 2));
    let coord = svc.coordinator().clone();
    let writes = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
    ];

    let handle = svc.submit_write_batch(writes.clone(), WriteConcern::All);
    let _ = svc.run(coord.read(b"unrelated")).unwrap();
    let res = svc.run(handle).unwrap();

    assert!(res.all_ok());
    assert_eq!(res.outcomes.len(), 3);
    for o in &res.outcomes {
        match o {
            WriteOutcome::Ok { acks, .. } => assert_eq!(*acks, 3),
            other => panic!("expected Ok, got {other:?}"),
        }
    }
    for (k, v) in &writes {
        let got = svc.run(coord.read(k)).unwrap().unwrap();
        assert_eq!(got.value.as_deref(), Some(v.as_slice()));
    }
}

#[test]
fn submit_write_batch_reports_replica_errors() {
    let svc = CoordinatorService::new(cluster(5, 3, 2));
    let coord = svc.coordinator().clone();
    coord.transport().set_down(2, true);
    coord.transport().set_down(4, true);
    let writes = vec![
        (b"a".to_vec(), b"1".to_vec()),
        (b"b".to_vec(), b"2".to_vec()),
        (b"c".to_vec(), b"3".to_vec()),
        (b"d".to_vec(), b"4".to_vec()),
    ];

    let handle = svc.submit_write_batch(writes.clone(), WriteConcern::All);
    let res = svc.run(handle).unwrap();
    assert_eq!(res.outcomes.len(), 4);

    for (i, (key, _)) in writes.iter().enumerate() {
        let reps = coord.ring().replicas(key);
        let reachable = reps.iter().filter(|n| ![2, 4].contains(n)).count();
        match &res.outcomes[i] {
            WriteOutcome::Ok { acks, .. } => {
                assert_eq!(*acks, reps.len(), "Replicated Ok means all RF acked");
                assert_eq!(reachable, reps.len());
            }
            WriteOutcome::Failed { acks, error } => {
                assert_eq!(*acks, reachable, "Failed still reports partial acks");
                assert!(matches!(error, CoordError::QuorumFailed { .. }));
                assert!(reachable < reps.len());
            }
        }
    }
}

#[test]
fn submit_write_batch_blocks_on_credit_at_submit() {
    let svc = CoordinatorService::new(slow_coord(100, 1));

    let first = svc.submit_write_batch(vec![(b"k1".to_vec(), b"v1".to_vec())], WriteConcern::Quorum(2));
    let st = svc.coordinator().credit_stats();
    assert_eq!(st.in_flight_ops, 1, "first write holds the one credit, still parked");
    assert_eq!(st.blocked_total, 0, "nothing has had to wait yet");

    let second =
        svc.submit_write_batch(vec![(b"k2".to_vec(), b"v2".to_vec())], WriteConcern::Quorum(2));

    let st = svc.coordinator().credit_stats();
    assert!(
        st.blocked_total >= 1,
        "the second submit had to wait for a credit, blocked_total={}",
        st.blocked_total
    );
    assert_eq!(
        st.peak_ops, 1,
        "the one-op pool never held two writes at once"
    );

    assert!(svc.run(first).unwrap().all_ok());
    assert!(svc.run(second).unwrap().all_ok());
    let coord = svc.coordinator().clone();
    assert_eq!(
        svc.run(coord.read(b"k1")).unwrap().unwrap().value.as_deref(),
        Some(&b"v1"[..])
    );
    assert_eq!(
        svc.run(coord.read(b"k2")).unwrap().unwrap().value.as_deref(),
        Some(&b"v2"[..])
    );
}

#[test]
fn dropped_batch_handle_still_completes_the_write() {
    let svc = CoordinatorService::new(slow_coord(4, 8));
    let coord = svc.coordinator().clone();

    let handle = svc.submit_write_batch(vec![(b"k".to_vec(), b"v".to_vec())], WriteConcern::Quorum(2));
    drop(handle);

    svc.shutdown();

    let got = svc.run(coord.read(b"k")).unwrap().unwrap();
    assert_eq!(got.value.as_deref(), Some(&b"v"[..]));
}

#[test]
fn submit_empty_batch_resolves_empty() {
    let svc = CoordinatorService::new(cluster(3, 3, 2));
    let handle = svc.submit_write_batch(Vec::new(), WriteConcern::All);
    let res = svc.run(handle).unwrap();
    assert!(res.all_ok());
    assert!(res.outcomes.is_empty());
}
