use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;

use vals_cluster::antientropy::{reconcile_scoped, LocalPeer, DEFAULT_BUCKETS};
use vals_cluster::{
    Applied, Catalog, CoordError, Coordinator, WriteConcern, LocalCluster, NodeId, Ring, RingView,
    Stored, Transport, TransportError, Version, VersionedStore, ZoneId,
};
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

const BS: usize = 256;
const N_NODES: u32 = 3;
const RF: usize = 3;
const R: usize = 2;
const KEYS: u64 = 48;
const N_OPS: usize = 150;
const MAX_AE_ROUNDS: usize = 8;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn chance(&mut self, permille: u32) -> bool {
        (self.below(1000) as u32) < permille
    }
}

#[derive(Default, Clone)]
struct Faults {
    p_drop_req: u32,
    p_drop_reply: u32,
    p_delay: u32,
    p_dup: u32,
    down: BTreeSet<NodeId>,
    partition_req: BTreeSet<NodeId>,
    partition_reply: BTreeSet<NodeId>,
}

struct Event {
    node: NodeId,
    kind: EventKind,
    start: usize,
    end: usize,
}

enum EventKind {
    Down,
    PartitionReq,
    PartitionReply,
}

struct SimNet {
    inner: LocalCluster<MemBlockDevice>,
    rng: RefCell<Rng>,
    faults: RefCell<Faults>,
    cluster_epoch: Cell<u64>,
    route_epoch: Cell<u64>,
    stale_refusals: Cell<u64>,
    members_view: RefCell<Option<Vec<NodeId>>>,
}

impl SimNet {
    fn new(inner: LocalCluster<MemBlockDevice>, rng: Rng) -> Self {
        SimNet {
            inner,
            rng: RefCell::new(rng),
            faults: RefCell::new(Faults::default()),
            cluster_epoch: Cell::new(0),
            route_epoch: Cell::new(0),
            stale_refusals: Cell::new(0),
            members_view: RefCell::new(None),
        }
    }

    fn set_authoritative(&self, epoch: u64, members: Vec<NodeId>) {
        self.cluster_epoch.set(epoch);
        *self.members_view.borrow_mut() = Some(members);
    }

    fn stale_refusals(&self) -> u64 {
        self.stale_refusals.get()
    }

    fn set_faults(&self, f: Faults) {
        *self.faults.borrow_mut() = f;
    }

    fn heal(&self) {
        *self.faults.borrow_mut() = Faults::default();
    }

    fn store(&self, node: NodeId) -> &RefCell<VersionedStore<MemBlockDevice>> {
        self.inner.store(node).expect("replica store exists")
    }
}

impl Transport for SimNet {
    fn set_route_epoch(&self, epoch: u64) {
        self.route_epoch.set(epoch);
    }

    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> Result<Applied, TransportError> {
        let ce = self.cluster_epoch.get();
        if ce > 0 && self.route_epoch.get() < ce {
            self.stale_refusals.set(self.stale_refusals.get() + 1);
            return Err(TransportError::StaleEpoch(ce));
        }

        let (drop_req, delay, dup, drop_reply) = {
            let mut rng = self.rng.borrow_mut();
            let f = self.faults.borrow();
            let hard_down = f.down.contains(&node);
            let drop_req = hard_down || f.partition_req.contains(&node) || rng.chance(f.p_drop_req);
            let delay = if rng.chance(f.p_delay) {
                1 + rng.below(3)
            } else {
                0
            };
            let dup = rng.chance(f.p_dup);
            let drop_reply =
                hard_down || f.partition_reply.contains(&node) || rng.chance(f.p_drop_reply);
            (drop_req, delay, dup, drop_reply)
        };

        if drop_req {
            return Err(TransportError::Unavailable);
        }
        for _ in 0..delay {
            vals_rt::yield_now().await;
        }
        let res = self.inner.apply(node, zone_id, key, value, version).await;
        if dup {
            let _ = self.inner.apply(node, zone_id, key, value, version).await;
        }
        if drop_reply && res.is_ok() {
            return Err(TransportError::Unavailable);
        }
        res
    }

    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        {
            let f = self.faults.borrow();
            if f.down.contains(&node) || f.partition_req.contains(&node) {
                return Err(TransportError::Unavailable);
            }
        }
        self.inner.read(node, zone_id, key).await
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        if let Some(members) = self.members_view.borrow().clone() {
            return Ok(RingView {
                epoch: self.cluster_epoch.get(),
                vnodes: 64,
                rf: RF as u32,
                members: members.into_iter().map(|n| (n, 1)).collect(),
                addrs: Vec::new(),
            });
        }
        self.inner.members(node).await
    }

    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        self.inner.zones(node).await
    }
}

fn run<F: Future>(f: F) -> F::Output {
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

type Dump = BTreeMap<Vec<u8>, (Version, Option<Vec<u8>>)>;

fn dump(store: &RefCell<VersionedStore<MemBlockDevice>>) -> Dump {
    store
        .borrow()
        .entries()
        .expect("entries")
        .into_iter()
        .map(|(k, s)| (k, (s.version, s.value)))
        .collect()
}

fn pull(sim: &SimNet, ring: &Ring, a: NodeId, b: NodeId) {
    let local = sim.store(a);
    let peer_cell = sim.store(b);
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let peer_ref = peer_cell.borrow();
        let (_, next) = run(reconcile_scoped(
            local,
            &LocalPeer(&peer_ref),
            DEFAULT_BUCKETS,
            ring,
            a,
            b,
            (0, 1),
            resume.as_deref(),
        ))
        .expect("reconcile");
        drop(peer_ref);
        match next {
            Some(cursor) => resume = Some(cursor),
            None => break,
        }
    }
}

fn schedule(rng: &mut Rng) -> (Faults, Vec<Event>) {
    let base = Faults {
        p_drop_req: rng.below(350) as u32,
        p_drop_reply: rng.below(220) as u32,
        p_delay: rng.below(500) as u32,
        p_dup: rng.below(160) as u32,
        ..Faults::default()
    };

    let n_events = rng.below(4) as usize;
    let mut events = Vec::with_capacity(n_events);
    for _ in 0..n_events {
        let node = 1 + rng.below(N_NODES as u64) as NodeId;
        let kind = match rng.below(3) {
            0 => EventKind::Down,
            1 => EventKind::PartitionReq,
            _ => EventKind::PartitionReply,
        };
        let start = rng.below(N_OPS as u64) as usize;
        let len = 5 + rng.below(40) as usize;
        let end = (start + len).min(N_OPS);
        events.push(Event {
            node,
            kind,
            start,
            end,
        });
    }
    (base, events)
}

fn faults_at(base: &Faults, events: &[Event], op: usize) -> Faults {
    let mut f = base.clone();
    for e in events {
        if op >= e.start && op < e.end {
            match e.kind {
                EventKind::Down => {
                    f.down.insert(e.node);
                }
                EventKind::PartitionReq => {
                    f.partition_req.insert(e.node);
                }
                EventKind::PartitionReply => {
                    f.partition_reply.insert(e.node);
                }
            }
        }
    }
    f
}

fn run_scenario(seed: u64) -> Dump {
    let mut rng = Rng::new(seed);
    let (base, events) = schedule(&mut rng);

    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(RF, 64);
    for node in 1..=N_NODES {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let clock = Rc::new(Cell::new(1u64));
    let sim = SimNet::new(lc, Rng::new(seed ^ 0xA5A5_A5A5_5A5A_5A5A));
    let coord =
        Coordinator::new_with_writer_for_test(0, ring.clone(), R, sim, seed | (1 << 63))
            .with_clock({
                let c = clock.clone();
                move || c.get()
            });

    let mut acked: Dump = BTreeMap::new();

    for op in 0..N_OPS {
        coord.transport().set_faults(faults_at(&base, &events, op));
        clock.set(clock.get() + 1);

        let key = format!("k{:02}", rng.below(KEYS));
        let is_delete = rng.chance(200);
        if is_delete {
            if let Ok(v) = run(coord.delete(key.as_bytes(), WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, None);
            }
        } else {
            let val = format!("v{op}").into_bytes();
            if let Ok(v) = run(coord.write_with(key.as_bytes(), &val, WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, Some(val));
            }
        }
    }

    heal_converge_check(seed, coord.transport(), &ring, &acked)
}

fn heal_converge_check(seed: u64, sim: &SimNet, ring: &Ring, acked: &Dump) -> Dump {
    sim.heal();
    let mut converged = false;
    for _ in 0..MAX_AE_ROUNDS {
        for a in 1..=N_NODES {
            for b in 1..=N_NODES {
                if a != b {
                    pull(sim, ring, a, b);
                }
            }
        }
        let d1 = dump(sim.store(1));
        if (2..=N_NODES).all(|n| dump(sim.store(n)) == d1) {
            converged = true;
            break;
        }
    }

    let d1 = dump(sim.store(1));
    for n in 2..=N_NODES {
        let dn = dump(sim.store(n));
        if dn != d1 {
            let key = first_diff(&d1, &dn);
            panic!(
                "[netsim] seed {seed}: nodes 1 and {n} did not converge \
                 (converged={converged}); first differing key: {key:?} \
                 node1={:?} node{n}={:?}",
                key.as_ref().and_then(|k| d1.get(k)),
                key.as_ref().and_then(|k| dn.get(k)),
            );
        }
    }
    assert!(
        converged,
        "[netsim] seed {seed}: stores equal but AE never reported a fixed point"
    );

    for (key, (aver, aval)) in acked {
        match d1.get(key) {
            None => panic!(
                "[netsim] seed {seed}: acknowledged write to {key:?} at {aver:?} was lost \
                 (no entry after convergence)"
            ),
            Some((cver, cval)) => {
                if cver < aver {
                    panic!(
                        "[netsim] seed {seed}: acknowledged write to {key:?} regressed: \
                         acked {aver:?} but converged to older {cver:?}"
                    );
                }
                if cver == aver && cval != aval {
                    panic!(
                        "[netsim] seed {seed}: acknowledged write to {key:?} at {aver:?} kept the \
                         wrong value: acked {aval:?}, converged {cval:?}"
                    );
                }
            }
        }
    }

    d1
}

type OpFut<'a> = Pin<Box<dyn Future<Output = Result<Version, CoordError>> + 'a>>;

fn run_concurrent(seed: u64) -> Dump {
    let mut rng = Rng::new(seed);
    let (base, events) = schedule(&mut rng);

    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(RF, 64);
    for node in 1..=N_NODES {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let clock = Rc::new(Cell::new(1u64));
    let sim = SimNet::new(lc, Rng::new(seed ^ 0xA5A5_A5A5_5A5A_5A5A));
    let coord =
        Coordinator::new_with_writer_for_test(0, ring.clone(), R, sim, seed | (1 << 63))
            .with_clock({
                let c = clock.clone();
                move || c.get()
            });

    let mut acked: Dump = BTreeMap::new();

    let mut op = 0usize;
    while op < N_OPS {
        let batch = (1 + rng.below(4) as usize).min(N_OPS - op);
        coord.transport().set_faults(faults_at(&base, &events, op));
        clock.set(clock.get() + 1);

        let mut specs: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::with_capacity(batch);
        for j in 0..batch {
            let key = format!("k{:02}", rng.below(KEYS)).into_bytes();
            let is_delete = rng.chance(200);
            let val = if is_delete {
                None
            } else {
                Some(format!("v{}-{}", op, j).into_bytes())
            };
            specs.push((key, val));
        }
        let futs: Vec<OpFut> = specs
            .iter()
            .map(|(k, v)| -> OpFut {
                match v {
                    Some(val) => Box::pin(coord.write_with(k, val, WriteConcern::Majority)),
                    None => Box::pin(coord.delete(k, WriteConcern::Majority)),
                }
            })
            .collect();
        let results = run(vals_rt::join_all(futs));
        for ((k, v), r) in specs.iter().zip(results) {
            if let Ok(ver) = r {
                record(&mut acked, k.clone(), ver, v.clone());
            }
        }
        op += batch;
    }

    heal_converge_check(seed, coord.transport(), &ring, &acked)
}

fn run_crash(seed: u64) -> Dump {
    let mut rng = Rng::new(seed);
    let (base, events) = schedule(&mut rng);

    let mut lc = LocalCluster::new();
    let mut ring = Ring::with_vnodes(RF, 64);
    for node in 1..=N_NODES {
        lc.insert(node, replica(node));
        ring.add_node(node);
    }
    let clock = Rc::new(Cell::new(1u64));
    let sim = SimNet::new(lc, Rng::new(seed ^ 0xA5A5_A5A5_5A5A_5A5A));
    let coord =
        Coordinator::new_with_writer_for_test(0, ring.clone(), R, sim, seed | (1 << 63))
            .with_clock({
                let c = clock.clone();
                move || c.get()
            });

    let loss_node = 1 + rng.below(N_NODES as u64) as NodeId;
    let n_wipes = 1 + rng.below(2) as usize;
    let mut wipes: Vec<usize> = (0..n_wipes)
        .map(|_| 10 + rng.below((N_OPS - 20) as u64) as usize)
        .collect();
    wipes.sort_unstable();

    let mut acked: Dump = BTreeMap::new();

    for op in 0..N_OPS {
        if wipes.contains(&op) {
            *coord.transport().store(loss_node).borrow_mut() = replica(loss_node);
        }
        coord.transport().set_faults(faults_at(&base, &events, op));
        clock.set(clock.get() + 1);

        let key = format!("k{:02}", rng.below(KEYS));
        let is_delete = rng.chance(200);
        if is_delete {
            if let Ok(v) = run(coord.delete(key.as_bytes(), WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, None);
            }
        } else {
            let val = format!("v{op}").into_bytes();
            if let Ok(v) = run(coord.write_with(key.as_bytes(), &val, WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, Some(val));
            }
        }
    }

    heal_converge_check(seed, coord.transport(), &ring, &acked)
}

const RESHAPE_NODES: u32 = 4;

fn run_reshape(seed: u64) -> Vec<Dump> {
    let mut rng = Rng::new(seed);
    let (base, events) = schedule(&mut rng);

    let mut lc = LocalCluster::new();
    for node in 1..=RESHAPE_NODES {
        lc.insert(node, replica(node));
    }
    let mut ring = Ring::with_vnodes(RF, 64);
    for node in 1..=N_NODES {
        ring.add_node(node);
    }
    let clock = Rc::new(Cell::new(1u64));
    let sim = SimNet::new(lc, Rng::new(seed ^ 0xA5A5_A5A5_5A5A_5A5A));
    sim.set_authoritative(1, (1..=N_NODES).collect());
    let coord = Coordinator::new_with_writer_for_test(0, ring, R, sim, seed | (1 << 63))
        .with_clock({
            let c = clock.clone();
            move || c.get()
        });

    run(coord.refresh_ring(1)).expect("adopt epoch 1");
    assert_eq!(
        coord.ring_epoch(),
        1,
        "[netsim] seed {seed}: coordinator did not adopt epoch 1 at startup"
    );

    let mut acked: Dump = BTreeMap::new();
    let reshape_at = 10 + rng.below((N_OPS - 20) as u64) as usize;

    for op in 0..N_OPS {
        if op == reshape_at {
            coord
                .transport()
                .set_authoritative(2, (1..=RESHAPE_NODES).collect());
        }
        coord.transport().set_faults(faults_at(&base, &events, op));
        clock.set(clock.get() + 1);

        let key = format!("k{:02}", rng.below(KEYS));
        let is_delete = rng.chance(200);
        if is_delete {
            if let Ok(v) = run(coord.delete(key.as_bytes(), WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, None);
            }
        } else {
            let val = format!("v{op}").into_bytes();
            if let Ok(v) = run(coord.write_with(key.as_bytes(), &val, WriteConcern::Majority)) {
                record(&mut acked, key.into_bytes(), v, Some(val));
            }
        }
    }

    assert!(
        coord.transport().stale_refusals() > 0,
        "[netsim] seed {seed}: the stale-coordinator refusal path was never exercised"
    );
    assert_eq!(
        coord.ring_epoch(),
        2,
        "[netsim] seed {seed}: coordinator did not converge onto epoch 2 after the reshape"
    );

    let sim = coord.transport();
    sim.heal();
    let final_ring = coord.ring().clone();
    let mut converged = false;
    for _ in 0..MAX_AE_ROUNDS {
        for a in 1..=RESHAPE_NODES {
            for b in 1..=RESHAPE_NODES {
                if a != b {
                    pull(sim, &final_ring, a, b);
                }
            }
        }
        if ownership_violation(sim, &final_ring, RESHAPE_NODES).is_none() {
            converged = true;
            break;
        }
    }

    if let Some((key, per_owner)) = ownership_violation(sim, &final_ring, RESHAPE_NODES) {
        panic!(
            "[netsim] seed {seed}: final owners of {key:?} disagree \
             (converged={converged}): {per_owner:?}"
        );
    }
    assert!(
        converged,
        "[netsim] seed {seed}: owners agree but AE never reached an ownership-scoped fixed point"
    );

    let dumps: Vec<Dump> = (1..=RESHAPE_NODES).map(|n| dump(sim.store(n))).collect();
    for (key, (aver, aval)) in &acked {
        for owner in final_ring.replicas(key) {
            match dumps[(owner - 1) as usize].get(key) {
                None => panic!(
                    "[netsim] seed {seed}: acked write to {key:?} at {aver:?} lost on final \
                     owner {owner} after the reshape"
                ),
                Some((cver, cval)) => {
                    if cver < aver {
                        panic!(
                            "[netsim] seed {seed}: acked write to {key:?} regressed on final \
                             owner {owner}: acked {aver:?} but holds older {cver:?}"
                        );
                    }
                    if cver == aver && cval != aval {
                        panic!(
                            "[netsim] seed {seed}: acked write to {key:?} at {aver:?} kept the \
                             wrong value on final owner {owner}: acked {aval:?}, holds {cval:?}"
                        );
                    }
                }
            }
        }
    }

    dumps
}

type OwnerEntry = (Version, Option<Vec<u8>>);
type OwnerDisagreement = (Vec<u8>, Vec<(NodeId, Option<OwnerEntry>)>);
fn ownership_violation(sim: &SimNet, ring: &Ring, nodes: u32) -> Option<OwnerDisagreement> {
    let dumps: Vec<Dump> = (1..=nodes).map(|n| dump(sim.store(n))).collect();
    let keys: BTreeSet<Vec<u8>> = dumps.iter().flat_map(|d| d.keys().cloned()).collect();
    for key in keys {
        let owners = ring.replicas(&key);
        let per_owner: Vec<(NodeId, Option<OwnerEntry>)> = owners
            .iter()
            .map(|&o| (o, dumps[(o - 1) as usize].get(&key).cloned()))
            .collect();
        let first = per_owner.first().map(|(_, e)| e.clone());
        if let Some(first) = first {
            if per_owner.iter().any(|(_, e)| *e != first) {
                return Some((key, per_owner));
            }
        }
    }
    None
}

fn record(acked: &mut Dump, key: Vec<u8>, version: Version, value: Option<Vec<u8>>) {
    match acked.get(&key) {
        Some((v, _)) if *v >= version => {}
        _ => {
            acked.insert(key, (version, value));
        }
    }
}

fn first_diff(a: &Dump, b: &Dump) -> Option<Vec<u8>> {
    a.keys()
        .chain(b.keys())
        .find(|k| a.get(*k) != b.get(*k))
        .cloned()
}

const SEED_BASE: u64 = 0x5EED_0046_C0FF_EE00;
const SEED_BASE_CONCURRENT: u64 = 0x5EED_0052_C0FF_EE01;
const SEED_BASE_CRASH: u64 = 0x5EED_0052_C0FF_EE02;
const SEED_BASE_RESHAPE: u64 = 0x5EED_0052_C0FF_EE03;

fn seed_with(base: u64, i: u64) -> u64 {
    base.wrapping_add(i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

fn seed_for(i: u64) -> u64 {
    seed_with(SEED_BASE, i)
}

fn replay_seed() -> Option<u64> {
    std::env::var("VALS_SIM_SEED")
        .ok()
        .map(|s| s.parse().expect("VALS_SIM_SEED must be a u64"))
}

#[test]
fn netsim_convergence_many_seeds() {
    if let Some(seed) = replay_seed() {
        run_scenario(seed);
        return;
    }
    for i in 0..80u64 {
        run_scenario(seed_for(i));
    }
}

#[test]
fn netsim_concurrent_ops_many_seeds() {
    if let Some(seed) = replay_seed() {
        run_concurrent(seed);
        return;
    }
    for i in 0..60u64 {
        run_concurrent(seed_with(SEED_BASE_CONCURRENT, i));
    }
}

#[test]
fn netsim_crash_state_loss_many_seeds() {
    if let Some(seed) = replay_seed() {
        run_crash(seed);
        return;
    }
    for i in 0..60u64 {
        run_crash(seed_with(SEED_BASE_CRASH, i));
    }
}

#[test]
fn netsim_epoch_reshape_many_seeds() {
    if let Some(seed) = replay_seed() {
        run_reshape(seed);
        return;
    }
    for i in 0..60u64 {
        run_reshape(seed_with(SEED_BASE_RESHAPE, i));
    }
}

#[test]
fn netsim_deterministic_replay() {
    for i in [0u64, 7, 42, 1000] {
        let seed = seed_for(i);
        assert_eq!(
            run_scenario(seed),
            run_scenario(seed),
            "[netsim] seed {seed}: base replay diverged — non-determinism leaked in"
        );

        let cseed = seed_with(SEED_BASE_CONCURRENT, i);
        assert_eq!(
            run_concurrent(cseed),
            run_concurrent(cseed),
            "[netsim] seed {cseed}: concurrent-op replay diverged"
        );

        let kseed = seed_with(SEED_BASE_CRASH, i);
        assert_eq!(
            run_crash(kseed),
            run_crash(kseed),
            "[netsim] seed {kseed}: crash-state-loss replay diverged"
        );

        let rseed = seed_with(SEED_BASE_RESHAPE, i);
        assert_eq!(
            run_reshape(rseed),
            run_reshape(rseed),
            "[netsim] seed {rseed}: reshape replay diverged"
        );
    }
}
