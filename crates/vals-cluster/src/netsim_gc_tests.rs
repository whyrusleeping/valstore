use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::rc::Rc;

use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};

use crate::antientropy::{cleanup_scoped_zone, reconcile_scoped_zone, AePeer, LocalPeer, DEFAULT_BUCKETS};
use crate::coord::{
    BatchAck, BatchEntry, Coordinator, ReadConcern, RingView, Transport, TransportError,
    WriteConcern, WriteOptions,
};
use crate::hlc::Version;
use crate::partition::RangeMap;
use crate::replica::{Applied, Stored, VersionedStore};
use crate::ring::{NodeId, Ring};
use crate::rt_sync::{exchange, local_fragments, respond, RtScope};
use crate::wire::WireSlice;
use crate::zone::{Catalog, Ordering as ZoneOrdering, ZoneConfig, ZoneId};

type Store = VersionedStore<MemBlockDevice>;

const BS: usize = 256;
const DEV_BLOCKS: u64 = 4096;
const ZONE: ZoneId = 7;
const RF: usize = 3;
const VNODES: u32 = 64;
const NODES: u32 = 4;
const FIRST_MEMBERS: u32 = 3;
const KEYS: u64 = 24;
const N_OPS: usize = 240;
const STEP_MS: u64 = 10;
const START_MS: u64 = 1_000_000;
const GRACE_MS: u64 = 1000;
const AE_EVERY: usize = 12;
const CLEANUP_EVERY: usize = 8;
const LATE_MIN_OPS: u64 = 4;
const LATE_SPAN_OPS: u64 = 120;
const HEAL_OPS: usize = 130;
const FINAL_ROUNDS: usize = 12;
const DEFAULT_SEEDS: u64 = 2;
const SEED_BASE: u64 = 0x5EED_0534_C0FF_EE00;

struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }

    fn chance(&mut self, permille: u32) -> bool {
        (self.below(1000) as u32) < permille
    }
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    vals_rt::block_on(f, || {})
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:02}").into_bytes()
}

fn keyspace() -> Vec<Vec<u8>> {
    (0..KEYS).map(key).collect()
}

fn engine_config() -> Config {
    Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn zone_config() -> ZoneConfig {
    ZoneConfig { name: "netsim".into(), rf: RF, ordering: ZoneOrdering::Sorted, scope: None }
}

fn range_map() -> RangeMap {
    RangeMap::from_splits((1..8).map(|i| key(i * 3)))
}

fn show(k: &[u8]) -> String {
    String::from_utf8_lossy(k).into_owned()
}

fn covers(lo: &[u8], hi: Option<&[u8]>, k: &[u8]) -> bool {
    k >= lo && hi.is_none_or(|hi| k < hi)
}

fn value_op(value: &[u8]) -> usize {
    std::str::from_utf8(value)
        .ok()
        .and_then(|s| s.strip_prefix('v'))
        .and_then(|s| s.parse().ok())
        .expect("sim values are v<op>")
}

#[derive(Default, Clone)]
struct Faults {
    p_drop_req: u32,
    p_drop_reply: u32,
    p_dup: u32,
    p_late: u32,
    down: BTreeSet<NodeId>,
    cut_req: BTreeSet<NodeId>,
    cut_reply: BTreeSet<NodeId>,
}

#[derive(Clone, Copy)]
enum WindowKind {
    Down,
    CutReq,
    CutReply,
}

struct Window {
    node: NodeId,
    kind: WindowKind,
    start: usize,
    end: usize,
}

#[derive(Clone, Copy)]
struct Every {
    period: usize,
    phase: usize,
}

impl Every {
    fn pick(rng: &mut Rng, lo: u64, span: u64) -> Self {
        let period = (lo + rng.below(span)) as usize;
        Every { period, phase: rng.below(period as u64) as usize }
    }

    fn due(self, op: usize) -> bool {
        op % self.period == self.phase
    }
}

#[derive(Clone, Copy)]
struct Maint {
    persist: Every,
    sync: Every,
    flush: Option<Every>,
    compact: Every,
    merge: Every,
}

struct Plan {
    base: Faults,
    windows: Vec<Window>,
    crash_node: NodeId,
    crashes: Vec<usize>,
    join_at: usize,
    range_deletes: bool,
    maint: Vec<Maint>,
}

impl Plan {
    fn new(rng: &mut Rng) -> Self {
        let base = Faults {
            p_drop_req: rng.below(200) as u32,
            p_drop_reply: rng.below(150) as u32,
            p_dup: rng.below(100) as u32,
            p_late: 20 + rng.below(100) as u32,
            ..Faults::default()
        };
        let mut windows = Vec::new();
        for _ in 0..rng.below(4) {
            let node = 1 + rng.below(NODES as u64) as NodeId;
            let kind = match rng.below(3) {
                0 => WindowKind::Down,
                1 => WindowKind::CutReq,
                _ => WindowKind::CutReply,
            };
            let start = rng.below(N_OPS as u64) as usize;
            let end = (start + 3 + rng.below(8) as usize).min(N_OPS);
            windows.push(Window { node, kind, start, end });
        }
        let crash_node = 1 + rng.below(NODES as u64) as NodeId;
        let mut crashes: Vec<usize> = (0..4 + rng.below(7)).map(|_| 10 + rng.below((N_OPS - 10) as u64) as usize).collect();
        crashes.sort_unstable();
        crashes.dedup();
        let join_at = N_OPS / 4 + rng.below((N_OPS / 2) as u64) as usize;
        let range_deletes = rng.chance(400);
        let maint = (0..NODES)
            .map(|_| Maint {
                persist: Every::pick(rng, 5, 36),
                sync: Every::pick(rng, 5, 36),
                flush: (!rng.chance(700)).then(|| Every::pick(rng, 6, 20)),
                compact: Every::pick(rng, 1, 5),
                merge: Every::pick(rng, 8, 30),
            })
            .collect();
        Plan { base, windows, crash_node, crashes, join_at, range_deletes, maint }
    }

    fn faults_at(&self, op: usize) -> Faults {
        let mut f = self.base.clone();
        for w in &self.windows {
            if op >= w.start && op < w.end {
                match w.kind {
                    WindowKind::Down => f.down.insert(w.node),
                    WindowKind::CutReq => f.cut_req.insert(w.node),
                    WindowKind::CutReply => f.cut_reply.insert(w.node),
                };
            }
        }
        f
    }
}

type Stats = BTreeMap<&'static str, usize>;

enum Deferred {
    Puts(Vec<(Vec<u8>, Option<Vec<u8>>, Version)>),
    Ranges(Version, Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>),
}

struct Late {
    at: usize,
    node: NodeId,
    epoch: u64,
    sync: bool,
    msg: Deferred,
}

enum Route {
    Lost,
    Late(usize),
    Now { dup: bool, reply_lost: bool },
}

#[derive(Default)]
struct Landed {
    puts: HashSet<(Vec<u8>, Version, Vec<u8>)>,
    deletes: Vec<(Vec<u8>, Option<Vec<u8>>, Version)>,
}

impl Landed {
    fn delete_after(&self, k: &[u8], after: Version) -> bool {
        self.deletes.iter().any(|(lo, hi, v)| *v > after && covers(lo, hi.as_deref(), k))
    }
}

struct SimNode {
    dev: MemBlockDevice,
    store: RefCell<Store>,
    persisted_wm: Cell<u64>,
}

struct Cluster {
    seed: u64,
    nodes: Vec<SimNode>,
    now: Cell<u64>,
    op: Cell<usize>,
    epoch: Cell<u64>,
    ring: RefCell<Ring>,
    map: RangeMap,
    faults: RefCell<Faults>,
    rng: RefCell<Rng>,
    late: RefCell<Vec<Late>>,
    landed: RefCell<Landed>,
    stats: RefCell<Stats>,
}

impl Cluster {
    fn new(seed: u64) -> Self {
        let nodes = (1..=NODES)
            .map(|n| {
                let dev = MemBlockDevice::new(BS, DEV_BLOCKS);
                let store = VersionedStore::new(Engine::create_prewiped(dev.share(), engine_config()), n)
                    .expect("store")
                    .with_gc_grace_ms(GRACE_MS);
                SimNode { dev, store: RefCell::new(store), persisted_wm: Cell::new(0) }
            })
            .collect();
        let c = Cluster {
            seed,
            nodes,
            now: Cell::new(START_MS),
            op: Cell::new(0),
            epoch: Cell::new(1),
            ring: RefCell::new(Ring::from_members(RF, VNODES, 1..=FIRST_MEMBERS)),
            map: range_map(),
            faults: RefCell::new(Faults::default()),
            rng: RefCell::new(Rng(seed ^ 0xA5A5_A5A5_5A5A_5A5A)),
            late: RefCell::new(Vec::new()),
            landed: RefCell::new(Landed::default()),
            stats: RefCell::new(Stats::new()),
        };
        for n in 1..=NODES {
            c.place(&c.store(n).borrow());
        }
        c
    }

    fn count(&self, name: &'static str, n: usize) {
        *self.stats.borrow_mut().entry(name).or_default() += n;
    }

    fn store(&self, n: NodeId) -> &RefCell<Store> {
        &self.nodes[(n - 1) as usize].store
    }

    fn members(&self) -> Vec<NodeId> {
        self.ring.borrow().members().collect()
    }

    fn place(&self, store: &Store) {
        store.prepare_zone_seed((self.epoch.get(), 1, 1), &self.ring.borrow(), ZONE, Some(zone_config()), Some(self.map.clone()));
    }

    fn owners(&self, k: &[u8]) -> Vec<NodeId> {
        self.ring.borrow().replicas_in_zone(ZONE, self.map.range_token(k), RF, None)
    }

    fn join(&self, n: NodeId) {
        self.ring.borrow_mut().add_node(n);
        self.epoch.set(self.epoch.get() + 1);
        for m in 1..=NODES {
            self.place(&self.store(m).borrow());
        }
    }

    fn reachable(&self, n: NodeId) -> bool {
        let f = self.faults.borrow();
        !f.down.contains(&n) && !f.cut_req.contains(&n) && !f.cut_reply.contains(&n)
    }

    fn route(&self, node: NodeId) -> Route {
        let f = self.faults.borrow();
        let mut rng = self.rng.borrow_mut();
        let drop_req = rng.chance(f.p_drop_req);
        let late = rng.chance(f.p_late);
        let delay = LATE_MIN_OPS + rng.below(LATE_SPAN_OPS);
        let dup = rng.chance(f.p_dup);
        let drop_reply = rng.chance(f.p_drop_reply);
        if f.down.contains(&node) || f.cut_req.contains(&node) || drop_req {
            return Route::Lost;
        }
        if late {
            return Route::Late(self.op.get() + delay as usize);
        }
        Route::Now { dup, reply_lost: f.cut_reply.contains(&node) || drop_reply }
    }

    fn defer(&self, at: usize, node: NodeId, epoch: u64, sync: bool, msg: Deferred) {
        self.count("late_sent", 1);
        self.late.borrow_mut().push(Late { at, node, epoch, sync, msg });
    }

    fn put(&self, node: NodeId, k: &[u8], value: Option<&[u8]>, version: Version) -> Result<Applied, TransportError> {
        let applied = self
            .store(node)
            .borrow_mut()
            .apply_at(k, value, version, self.now.get())
            .map_err(|e| TransportError::Engine(e.to_string()))?;
        if applied == Applied::Won {
            let mut landed = self.landed.borrow_mut();
            match value {
                Some(v) => {
                    let now = self.now.get();
                    if let Some((_, _, delete)) = landed
                        .deletes
                        .iter()
                        .find(|(lo, hi, d)| *d > version && d.physical_ms() + GRACE_MS <= now && covers(lo, hi.as_deref(), k))
                    {
                        panic!(
                            "[netsim-gc] seed {}: resurrection at op {}: node {node} stored {} at {version:?}, older \
                             than the delete {delete:?} that is past the grace",
                            self.seed,
                            self.op.get(),
                            show(k)
                        );
                    }
                    landed.puts.insert((k.to_vec(), version, v.to_vec()));
                }
                None => landed.deletes.push((k.to_vec(), Some([k, &[0]].concat()), version)),
            }
        }
        Ok(applied)
    }

    fn range(&self, node: NodeId, lo: Option<&[u8]>, hi: Option<&[u8]>, version: Version) -> BatchAck {
        match self.store(node).borrow_mut().apply_range_delete(lo, hi, version, self.now.get()) {
            Ok(applied) => {
                if applied == Applied::Won {
                    let lo = lo.unwrap_or_default().to_vec();
                    self.landed.borrow_mut().deletes.push((lo, hi.map(<[u8]>::to_vec), version));
                }
                BatchAck::Applied(applied)
            }
            Err(_) => BatchAck::Remote,
        }
    }

    fn sync(&self, node: NodeId) {
        self.store(node).borrow_mut().sync().expect("sync");
    }

    fn deliver_due(&self, op: usize) {
        let due: Vec<Late> = {
            let mut late = self.late.borrow_mut();
            let (due, keep): (Vec<Late>, Vec<Late>) = late.drain(..).partition(|l| l.at <= op);
            *late = keep;
            due
        };
        for l in due {
            if l.epoch < self.epoch.get() {
                self.count("late_stale_refused", 1);
                continue;
            }
            if self.faults.borrow().down.contains(&l.node) || self.faults.borrow().cut_req.contains(&l.node) {
                continue;
            }
            match &l.msg {
                Deferred::Puts(puts) => {
                    for (k, v, version) in puts {
                        let applied = self.put(l.node, k, v.as_deref(), *version).expect("late apply");
                        self.count(if applied == Applied::Rejected { "late_floor_refused" } else { "late_applied" }, 1);
                    }
                }
                Deferred::Ranges(version, slices) => {
                    for (lo, hi) in slices {
                        self.range(l.node, lo.as_deref(), hi.as_deref(), *version);
                        self.count("late_applied", 1);
                    }
                }
            }
            if l.sync {
                self.sync(l.node);
            }
        }
    }

    fn maintain(&self, n: NodeId, op: usize, m: &Maint) {
        let node = &self.nodes[(n - 1) as usize];
        let now = self.now.get();
        let seed = self.seed;
        if m.persist.due(op) {
            let store = node.store.borrow();
            let wm = now.min(store.high_water_ms().max(node.persisted_wm.get()));
            if wm > node.persisted_wm.get() {
                node.persisted_wm.set(wm);
            }
            store.advance_durable_gc_watermark(node.persisted_wm.get());
        }
        if m.sync.due(op) {
            self.sync(n);
        }
        if m.flush.is_some_and(|f| f.due(op)) {
            node.store.borrow_mut().flush().unwrap_or_else(|e| panic!("[netsim-gc] seed {seed}: node {n} flush: {e}"));
        }
        let compacting = m.compact.due(op) || (m.merge.due(op) && node.store.borrow().run_count() >= 2);
        let before = compacting.then(|| self.effective(n));
        if m.compact.due(op) {
            let mut store = node.store.borrow_mut();
            for _ in 0..64 {
                let (stats, done) = store
                    .compact_step(now, 2)
                    .unwrap_or_else(|e| panic!("[netsim-gc] seed {seed}: node {n} compact_step: {e}"));
                self.count("gc_dropped", stats.gc_dropped);
                if done {
                    break;
                }
            }
        }
        if m.merge.due(op) && node.store.borrow().run_count() >= 2 {
            let stats = node
                .store
                .borrow_mut()
                .compact_runs(now)
                .unwrap_or_else(|e| panic!("[netsim-gc] seed {seed}: node {n} compact_runs: {e}"));
            self.count("gc_dropped", stats.current_dropped_tombstones.len());
            self.count("rt_collected", stats.collected_range_tombstones.len());
        }
        if let Some(before) = before {
            let cutoff = node.persisted_wm.get().saturating_sub(GRACE_MS);
            for (k, after) in self.effective(n) {
                let was = &before[&k];
                if *was == after {
                    continue;
                }
                let collected = matches!((was, &after), (Some(was), None) if was.value.is_none());
                assert!(
                    collected,
                    "[netsim-gc] seed {seed}: node {n} compaction at op {op} changed {} from {was:?} to {after:?}",
                    show(&k)
                );
                let was = was.as_ref().expect("collected");
                assert!(
                    was.version.physical_ms() < cutoff,
                    "[netsim-gc] seed {seed}: node {n} compaction at op {op} collected {} = {was:?} at or above its \
                     persisted GC cutoff {cutoff} (watermark {}): a restart could readmit older values",
                    show(&k),
                    node.persisted_wm.get()
                );
            }
        }
    }

    fn effective(&self, n: NodeId) -> BTreeMap<Vec<u8>, Option<Stored>> {
        let store = self.store(n).borrow();
        keyspace()
            .into_iter()
            .map(|k| {
                let s = store.get_versioned(&k).expect("get_versioned");
                (k, s)
            })
            .collect()
    }

    fn crash(&self, n: NodeId) {
        let seed = self.seed;
        let node = &self.nodes[(n - 1) as usize];
        let before = self.effective(n);
        let reopened = VersionedStore::open(node.dev.share(), engine_config(), n)
            .unwrap_or_else(|e| panic!("[netsim-gc] seed {seed}: node {n} reopen: {e}"))
            .with_gc_grace_ms(GRACE_MS);
        self.place(&reopened);
        reopened.advance_durable_gc_watermark(node.persisted_wm.get());
        *node.store.borrow_mut() = reopened;
        let after = self.effective(n);
        let lost = before.iter().filter(|(k, s)| after.get(*k) != Some(*s)).count();
        self.count("crashes", 1);
        self.count("crash_lost_keys", lost);
    }

    fn in_sync(&self, a: NodeId, b: NodeId, ring: &Ring) -> bool {
        let (sa, sb) = (self.store(a).borrow(), self.store(b).borrow());
        let scope_a = RtScope::new(&self.map, ring, ZONE, RF, None, a, Some(b), (0, 1));
        let scope_b = RtScope::new(&self.map, ring, ZONE, RF, None, b, Some(a), (0, 1));
        if scope_a.clip(local_fragments(&sa)) != scope_b.clip(local_fragments(&sb)) {
            return false;
        }
        keyspace().iter().all(|k| {
            let owners = self.owners(k);
            !owners.contains(&a)
                || !owners.contains(&b)
                || sa.engine().physical_version(k).expect("physical_version")
                    == sb.engine().physical_version(k).expect("physical_version")
        })
    }

    fn ae_pass(&self) -> usize {
        let ring = self.ring.borrow().clone();
        let members = self.members();
        let mut pulls = 0;
        for &a in &members {
            self.place(&self.store(a).borrow());
        }
        for &a in &members {
            for &b in &members {
                if a == b || !self.reachable(a) || !self.reachable(b) || self.in_sync(a, b, &ring) {
                    continue;
                }
                let peer = SimPeer { store: self.store(b), node: b, ring: &ring, map: &self.map };
                let scope = RtScope::new(&self.map, &ring, ZONE, RF, None, a, Some(b), (0, 1));
                if run(exchange(self.store(a), &peer, &scope)).is_err() {
                    self.count("ae_errors", 1);
                    continue;
                }
                let all = HashSet::from([0]);
                if !run(crate::replica::ensure_tree_ready_cooperative(&self.store(a).borrow(), 1, &all)).unwrap_or(false) {
                    self.count("ae_errors", 1);
                    continue;
                }
                let mut resume: Option<Vec<u8>> = None;
                for _ in 0..16 {
                    match run(reconcile_scoped_zone(
                        self.store(a),
                        &peer,
                        DEFAULT_BUCKETS,
                        &ring,
                        a,
                        b,
                        ZONE,
                        RF,
                        None,
                        Some(&self.map),
                        (0, 1),
                        resume.as_deref(),
                    )) {
                        Ok((applied, next)) => {
                            pulls += applied;
                            match next {
                                Some(cursor) => resume = Some(cursor),
                                None => break,
                            }
                        }
                        Err(_) => {
                            self.count("ae_errors", 1);
                            break;
                        }
                    }
                }
            }
        }
        self.count("ae_pulls", pulls);
        pulls
    }

    fn cleanup_pass(self: &Rc<Self>) {
        let ring = self.ring.borrow().clone();
        let link = Link { c: self.clone(), route_epoch: Cell::new(self.epoch.get()) };
        for n in self.members() {
            if !self.reachable(n) {
                continue;
            }
            let mut resume: Option<Vec<u8>> = None;
            for _ in 0..8 {
                let progress = run(cleanup_scoped_zone(
                    self.store(n),
                    &link,
                    &ring,
                    n,
                    ZONE,
                    RF,
                    None,
                    2,
                    Some(&self.map),
                    (0, 1),
                    resume.as_deref(),
                    None,
                    || false,
                    |_: &[u8]| false,
                    || false,
                    || false,
                ));
                let Ok(progress) = progress else { break };
                self.count("handoff_rpcs", progress.handoff_rpcs);
                match progress.next_cursor {
                    Some(cursor) if !progress.wrapped => resume = Some(cursor),
                    _ => break,
                }
            }
        }
    }
}

struct SimPeer<'a> {
    store: &'a RefCell<Store>,
    node: NodeId,
    ring: &'a Ring,
    map: &'a RangeMap,
}

impl AePeer for SimPeer<'_> {
    async fn tree(
        &self,
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &[u32],
        requester: Option<NodeId>,
    ) -> Result<Vec<(u32, u64)>, TransportError> {
        let s = self.store.borrow();
        run(LocalPeer(&s).tree(zone_id, level, scope_g, scope, requester))
    }

    async fn bucket_digests(
        &self,
        zone_id: ZoneId,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
    ) -> Result<crate::replica::DigestCursorPage, TransportError> {
        let s = self.store.borrow();
        run(LocalPeer(&s).bucket_digests(zone_id, num_buckets, ids, after))
    }

    async fn values(&self, zone_id: ZoneId, keys: &[Vec<u8>]) -> Result<crate::replica::EntryPage, TransportError> {
        let s = self.store.borrow();
        run(LocalPeer(&s).values(zone_id, keys))
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

struct Link {
    c: Rc<Cluster>,
    route_epoch: Cell<u64>,
}

impl Link {
    fn stale(&self) -> Option<u64> {
        let epoch = self.c.epoch.get();
        if self.route_epoch.get() < epoch {
            self.c.count("stale_refusals", 1);
            Some(epoch)
        } else {
            None
        }
    }

    fn deliver(
        &self,
        node: NodeId,
        n: usize,
        sync: bool,
        late: impl FnOnce() -> Deferred,
        apply: impl Fn() -> Vec<BatchAck>,
    ) -> Vec<BatchAck> {
        match self.c.route(node) {
            Route::Lost => vec![BatchAck::Unreachable; n],
            Route::Late(at) => {
                self.c.defer(at, node, self.route_epoch.get(), sync, late());
                vec![BatchAck::Timeout; n]
            }
            Route::Now { dup, reply_lost } => {
                let acks = match self.stale() {
                    Some(epoch) => vec![BatchAck::Stale(epoch); n],
                    None => {
                        let acks = apply();
                        if dup {
                            apply();
                        }
                        if sync {
                            self.c.sync(node);
                        }
                        acks
                    }
                };
                if reply_lost {
                    vec![BatchAck::Timeout; n]
                } else {
                    acks
                }
            }
        }
    }
}

impl Transport for Link {
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
        match self.apply_batch(node, zone_id, false, &[BatchEntry { key, value, version }]).await[0] {
            BatchAck::Applied(applied) => Ok(applied),
            BatchAck::Stale(epoch) => Err(TransportError::StaleEpoch(epoch)),
            BatchAck::Timeout => Err(TransportError::Timeout),
            _ => Err(TransportError::Unavailable),
        }
    }

    async fn apply_batch(&self, node: NodeId, _zone_id: ZoneId, sync: bool, writes: &[BatchEntry<'_>]) -> Vec<BatchAck> {
        self.deliver(
            node,
            writes.len(),
            sync,
            || Deferred::Puts(writes.iter().map(|w| (w.key.to_vec(), w.value.map(<[u8]>::to_vec), w.version)).collect()),
            || {
                writes
                    .iter()
                    .map(|w| match self.c.put(node, w.key, w.value, w.version) {
                        Ok(applied) => BatchAck::Applied(applied),
                        Err(_) => BatchAck::Remote,
                    })
                    .collect()
            },
        )
    }

    fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
        Some(true)
    }

    async fn range_delete(
        &self,
        node: NodeId,
        _zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: &[WireSlice<'_>],
    ) -> Vec<BatchAck> {
        self.deliver(
            node,
            slices.len(),
            sync,
            || Deferred::Ranges(version, slices.iter().map(|(lo, hi)| (lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec))).collect()),
            || slices.iter().map(|(lo, hi)| self.c.range(node, *lo, *hi, version)).collect(),
        )
    }

    async fn read(&self, node: NodeId, _zone_id: ZoneId, key: &[u8]) -> Result<Option<Stored>, TransportError> {
        let p_drop = self.c.faults.borrow().p_drop_req;
        if self.c.rng.borrow_mut().chance(p_drop) || !self.c.reachable(node) {
            return Err(TransportError::Unavailable);
        }
        if let Some(epoch) = self.stale() {
            return Err(TransportError::StaleEpoch(epoch));
        }
        self.c.store(node).borrow().get_versioned(key).map_err(|e| TransportError::Engine(e.to_string()))
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        if self.c.faults.borrow().down.contains(&node) {
            return Err(TransportError::Unavailable);
        }
        Ok(RingView {
            epoch: self.c.epoch.get(),
            vnodes: VNODES,
            rf: RF as u32,
            members: self.c.members().into_iter().map(|n| (n, 1)).collect(),
            addrs: Vec::new(),
        })
    }

    async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
        let mut cat = Catalog::new();
        cat.insert(ZONE, zone_config());
        Ok(cat)
    }

    async fn range_maps(&self, _node: NodeId) -> Result<Vec<(ZoneId, RangeMap)>, TransportError> {
        Ok(vec![(ZONE, self.c.map.clone())])
    }
}

struct Acked {
    op: usize,
    version: Version,
    value: Option<Vec<u8>>,
    durable: bool,
    epoch: u64,
}

#[derive(Default)]
struct History {
    acked: BTreeMap<Vec<u8>, Vec<Acked>>,
}

impl History {
    fn ack(&mut self, k: &[u8], a: Acked) {
        self.acked.entry(k.to_vec()).or_default().push(a);
    }

    fn check(&self, seed: u64, what: &str, k: &[u8], got: Option<&Stored>, landed: &Landed, filter: impl Fn(&Acked) -> bool) {
        let live = got.filter(|s| s.value.is_some());
        let what = format!("{what} of {}", show(k));
        if let Some(s) = live {
            let value = s.value.as_ref().expect("live");
            assert!(
                landed.puts.contains(&(k.to_vec(), s.version, value.clone())),
                "[netsim-gc] seed {seed}: {what} returned {s:?}, which no replica ever stored (phantom)"
            );
        }
        for a in self.acked.get(k).into_iter().flatten().filter(|a| filter(a)) {
            match live {
                Some(s) if s.version < a.version => {
                    let kind = if a.value.is_none() { "resurrection: an acknowledged delete" } else { "an acknowledged write" };
                    panic!(
                        "[netsim-gc] seed {seed}: {what}: {kind} (op {}, {:?}) lost to the older value of op {} ({:?})",
                        a.op,
                        a.version,
                        value_op(s.value.as_ref().expect("live")),
                        s.version
                    );
                }
                Some(s) if s.version == a.version => assert_eq!(
                    s.value, a.value,
                    "[netsim-gc] seed {seed}: {what}: version {:?} carries the wrong value",
                    a.version
                ),
                Some(_) => {}
                None => {
                    if a.value.is_some() {
                        assert!(
                            landed.delete_after(k, a.version),
                            "[netsim-gc] seed {seed}: {what}: the acknowledged write of op {} ({:?}) is gone \
                             and no newer delete ever reached a replica",
                            a.op,
                            a.version
                        );
                    }
                }
            }
        }
    }
}

type Snapshot = Vec<BTreeMap<Vec<u8>, Option<(Version, Vec<u8>)>>>;

fn live_view(c: &Cluster) -> Snapshot {
    (1..=NODES)
        .map(|n| {
            c.effective(n)
                .into_iter()
                .map(|(k, s)| (k, s.and_then(|s| s.value.map(|v| (s.version, v)))))
                .collect()
        })
        .collect()
}

fn owner_disagreement(c: &Cluster) -> Option<String> {
    let view = live_view(c);
    for k in keyspace() {
        let owners = c.owners(&k);
        let first = &view[(owners[0] - 1) as usize][&k];
        for &o in &owners[1..] {
            let other = &view[(o - 1) as usize][&k];
            if other != first {
                return Some(format!("{}: node {} has {first:?}, node {o} has {other:?}", show(&k), owners[0]));
            }
        }
    }
    None
}

fn step(c: &Rc<Cluster>, plan: &Plan, op: usize) {
    c.op.set(op);
    c.now.set(START_MS + op as u64 * STEP_MS);
    c.deliver_due(op);
    for n in 1..=NODES {
        c.maintain(n, op, &plan.maint[(n - 1) as usize]);
    }
    if op.is_multiple_of(AE_EVERY) {
        c.ae_pass();
    }
    if op.is_multiple_of(CLEANUP_EVERY) {
        c.cleanup_pass();
    }
}

fn run_sim(seed: u64) -> (Stats, Snapshot) {
    let mut rng = Rng(seed);
    let plan = Plan::new(&mut rng);
    let c = Rc::new(Cluster::new(seed));
    let link = Link { c: c.clone(), route_epoch: Cell::new(0) };
    let coord = Coordinator::new_with_writer_for_test(
        0,
        Ring::from_members(RF, VNODES, 1..=FIRST_MEMBERS),
        ReadConcern::Majority,
        link,
        seed | (1 << 63),
    )
    .with_clock({
        let c = c.clone();
        move || c.now.get()
    });
    run(coord.refresh_ring(1)).expect("adopt the first ring");
    run(coord.refresh_catalog(1)).expect("adopt the catalog");

    let keys = keyspace();
    let mut h = History::default();
    for op in 0..N_OPS {
        *c.faults.borrow_mut() = plan.faults_at(op);
        if op == plan.join_at {
            c.join(NODES);
        }
        if plan.crashes.contains(&op) {
            c.crash(plan.crash_node);
        }
        step(&c, &plan, op);

        let k = keys[rng.below(KEYS) as usize].clone();
        let durable = rng.chance(300);
        let options = if durable {
            WriteOptions::device_durable(WriteConcern::Majority)
        } else {
            WriteOptions::buffered(WriteConcern::Majority)
        };
        let kind = rng.below(100);
        if kind < 58 {
            let value = format!("v{op}").into_bytes();
            if let Ok(version) = run(coord.write_in(ZONE, &k, &value, options)) {
                c.count("acked_puts", 1);
                h.ack(&k, Acked { op, version, value: Some(value), durable, epoch: coord.ring_epoch() });
            }
        } else if kind < 72 {
            if let Ok(version) = run(coord.delete_in(ZONE, &k, options)) {
                c.count("acked_deletes", 1);
                h.ack(&k, Acked { op, version, value: None, durable, epoch: coord.ring_epoch() });
            }
        } else if kind < 78 && plan.range_deletes {
            let lo = rng.below(KEYS);
            let hi = lo + 1 + rng.below(4);
            let (lo, hi) = (key(lo), (hi < KEYS).then(|| key(hi)));
            if let Ok(version) = run(coord.delete_range_in(ZONE, Some(&lo), hi.as_deref(), options)) {
                c.count("acked_range_deletes", 1);
                for k in keys.iter().filter(|k| covers(&lo, hi.as_deref(), k)) {
                    h.ack(k, Acked { op, version, value: None, durable, epoch: coord.ring_epoch() });
                }
            }
        } else {
            let before = coord.ring_epoch();
            if let Ok(got) = run(coord.read_quorum_in(ZONE, &k, ReadConcern::Majority)) {
                let after = coord.ring_epoch();
                if before == after {
                    c.count("reads_checked", 1);
                    let landed = c.landed.borrow();
                    h.check(seed, &format!("majority read at op {op}"), &k, got.as_ref(), &landed, |a| {
                        a.durable && a.epoch == after && a.op < op
                    });
                }
            }
        }
    }

    *c.faults.borrow_mut() = Faults::default();
    for op in N_OPS..N_OPS + HEAL_OPS {
        step(&c, &plan, op);
    }
    assert!(c.late.borrow().is_empty(), "[netsim-gc] seed {seed}: late messages outlived the heal phase");

    let mut converged = false;
    for round in 0..FINAL_ROUNDS {
        let op = N_OPS + HEAL_OPS + round;
        c.op.set(op);
        c.now.set(START_MS + op as u64 * STEP_MS);
        let before: Vec<_> = (1..=NODES).map(|n| c.effective(n)).collect();
        c.cleanup_pass();
        c.ae_pass();
        let after: Vec<_> = (1..=NODES).map(|n| c.effective(n)).collect();
        if before == after && owner_disagreement(&c).is_none() {
            converged = true;
            break;
        }
    }
    if let Some(diff) = owner_disagreement(&c) {
        panic!("[netsim-gc] seed {seed}: final owners disagree on {diff}");
    }
    assert!(converged, "[netsim-gc] seed {seed}: anti-entropy never reached a fixed point");

    let landed = c.landed.borrow();
    for k in &keys {
        let owner = c.owners(k)[0];
        let got = c.store(owner).borrow().get_versioned(k).expect("get_versioned");
        h.check(seed, "the converged state", k, got.as_ref(), &landed, |_| true);
    }
    let stats = c.stats.borrow().clone();
    (stats, live_view(&c))
}

fn sim_seeds() -> Vec<u64> {
    if let Some(seed) = std::env::var("VALS_SIM_SEED").ok().map(|s| s.parse().expect("VALS_SIM_SEED must be a u64")) {
        return vec![seed];
    }
    let n = std::env::var("VALS_SIM_SEEDS")
        .ok()
        .map(|s| s.parse().expect("VALS_SIM_SEEDS must be a count"))
        .unwrap_or(DEFAULT_SEEDS);
    (0..n).map(|i| SEED_BASE.wrapping_add(i.wrapping_mul(0x9E37_79B9_7F4A_7C15))).collect()
}

#[test]
fn netsim_gc_sorted_zone_with_crashes_and_a_join_keeps_lww_and_deletes() {
    let seeds = sim_seeds();
    let mut total = Stats::new();
    for &seed in &seeds {
        for (name, n) in run_sim(seed).0 {
            *total.entry(name).or_default() += n;
        }
    }
    let t = |name: &str| total.get(name).copied().unwrap_or(0);
    eprintln!("[netsim-gc] {} seeds: {total:?}", seeds.len());
    if seeds.len() > 1 {
        assert!(t("acked_range_deletes") > 0 && t("acked_deletes") > 0, "{total:?}");
        assert!(t("gc_dropped") > 0 && t("rt_collected") > 0, "no tombstone was ever collected: {total:?}");
        assert!(t("late_floor_refused") > 0, "the admission floor never refused a late write: {total:?}");
        assert!(t("crash_lost_keys") > 0, "no crash ever lost unsynced state: {total:?}");
        assert!(t("stale_refusals") > 0 && t("handoff_rpcs") > 0, "the join never moved data: {total:?}");
    }
}

#[test]
fn netsim_gc_replay_is_deterministic() {
    let seed = SEED_BASE ^ 0x77;
    assert!(run_sim(seed) == run_sim(seed), "[netsim-gc] seed {seed}: replay diverged");
}
