use super::*;

mod mocktime {
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Waker};

    thread_local! {
        static CLOCK: Cell<u64> = const { Cell::new(0) };
        static NEXT_ID: Cell<u64> = const { Cell::new(0) };
        static PENDING: RefCell<HashMap<u64, (u64, Waker)>> = RefCell::new(HashMap::new());
    }

    pub fn elapsed_us() -> u64 {
        CLOCK.with(|c| c.get())
    }
    pub fn reset() {
        CLOCK.with(|c| c.set(0));
        PENDING.with(|p| p.borrow_mut().clear());
    }

    pub struct Delay {
        deadline: u64,
        id: Option<u64>,
    }
    impl Future for Delay {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            let this = self.get_mut();
            if CLOCK.with(|c| c.get()) >= this.deadline {
                if let Some(id) = this.id.take() {
                    PENDING.with(|p| p.borrow_mut().remove(&id));
                }
                Poll::Ready(())
            } else {
                let id = *this
                    .id
                    .get_or_insert_with(|| NEXT_ID.with(|n| n.replace(n.get() + 1)));
                PENDING.with(|p| {
                    p.borrow_mut()
                        .insert(id, (this.deadline, cx.waker().clone()));
                });
                Poll::Pending
            }
        }
    }
    impl Drop for Delay {
        fn drop(&mut self) {
            if let Some(id) = self.id {
                PENDING.with(|p| p.borrow_mut().remove(&id));
            }
        }
    }
    pub fn delay_us(micros: u64) -> Delay {
        Delay {
            deadline: CLOCK.with(|c| c.get()) + micros,
            id: None,
        }
    }

    pub fn drive() {
        let ready: Vec<Waker> = PENDING.with(|p| {
            let mut m = p.borrow_mut();
            let Some(next) = m.values().map(|(d, _)| *d).min() else {
                return Vec::new();
            };
            CLOCK.with(|c| c.set(next));
            let due: Vec<u64> = m
                .iter()
                .filter(|(_, (d, _))| *d <= next)
                .map(|(id, _)| *id)
                .collect();
            due.into_iter().map(|id| m.remove(&id).unwrap().1).collect()
        });
        for w in ready {
            w.wake();
        }
    }
}

struct Latency<T> {
    inner: T,
    latency_us: u64,
}
impl<T: vals_cluster::Transport> vals_cluster::Transport for Latency<T> {
    async fn apply(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: vals_cluster::Version,
    ) -> Result<vals_cluster::Applied, vals_cluster::TransportError> {
        mocktime::delay_us(self.latency_us).await;
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
    ) -> Result<Option<vals_cluster::Stored>, vals_cluster::TransportError> {
        mocktime::delay_us(self.latency_us).await;
        self.inner.read(node, zone_id, key).await
    }
    async fn members(
        &self,
        node: vals_cluster::NodeId,
    ) -> Result<vals_cluster::RingView, vals_cluster::TransportError> {
        self.inner.members(node).await
    }
    async fn zones(
        &self,
        node: vals_cluster::NodeId,
    ) -> Result<vals_cluster::Catalog, vals_cluster::TransportError> {
        self.inner.zones(node).await
    }
}

pub(super) fn workload_pipewrite(n: usize, vsize: usize, conc: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use vals_cluster::{Coordinator, WriteConcern, LocalCluster, Ring, VersionedStore};
    use vals_rt::LocalExecutor;
    use vals_store::{Engine, Ordering};

    const NODES: u32 = 3;
    const RF: usize = 3;
    const R: usize = 2;
    let conc = conc.max(1);
    let latency_us = if vsize == 0 { 20 } else { vsize as u64 };
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut lc = LocalCluster::new();
    for node in 1..=NODES {
        let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, 200));
        lc.insert(
            node,
            VersionedStore::new(Engine::create_prewiped(dev, cfg), node).unwrap(),
        );
        ring.add_node(node);
    }
    let coord = Coordinator::new(
        0,
        ring,
        R,
        Latency {
            inner: lc,
            latency_us,
        },
    );
    let val = make_value(200, 0);
    let key = |i: usize| format!("k{i:09}").into_bytes();
    let sim_ops = |ops: usize, us: u64| ops as f64 / (us.max(1) as f64 / 1e6);
    println!(
        "--- pipewrite: {NODES} nodes RF={RF}, mock RPC latency {latency_us}µs, 1 coordinator (simulated time) ---"
    );

    mocktime::reset();
    for i in 0..n {
        vals_rt::block_on(
            coord.write_with(&key(i), &val, WriteConcern::Quorum(2)),
            mocktime::drive,
        )
        .expect("write");
    }
    let seq = sim_ops(n, mocktime::elapsed_us());

    mocktime::reset();
    let coord = Rc::new(coord);
    let next = Rc::new(Cell::new(0usize));
    let ex = LocalExecutor::new();
    for _ in 0..conc {
        let coord = coord.clone();
        let next = next.clone();
        let val = val.clone();
        ex.spawn(async move {
            loop {
                let i = next.get();
                if i >= n {
                    break;
                }
                next.set(i + 1);
                let _ = coord
                    .write_with(
                        &format!("k{i:09}").into_bytes(),
                        &val,
                        WriteConcern::Quorum(2),
                    )
                    .await;
            }
        });
    }
    ex.run(mocktime::drive);
    let pipe = sim_ops(n, mocktime::elapsed_us());

    println!("  sequential          : {seq:.0} ops/s");
    println!(
        "  pipelined (conc={conc}) : {pipe:.0} ops/s  ({:.1}x)",
        pipe / seq
    );
}

struct ServerState {
    latency_us: u64,
    qdepth: std::cell::Cell<usize>,
    slots: std::cell::RefCell<std::collections::HashMap<vals_cluster::NodeId, Vec<u64>>>,
}
impl ServerState {
    fn reset(&self, qdepth: usize) {
        self.qdepth.set(qdepth.max(1));
        self.slots.borrow_mut().clear();
    }
}
struct ServerModel<T> {
    inner: T,
    state: std::rc::Rc<ServerState>,
}
impl<T: vals_cluster::Transport> vals_cluster::Transport for ServerModel<T> {
    async fn apply(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: vals_cluster::Version,
    ) -> Result<vals_cluster::Applied, vals_cluster::TransportError> {
        self.inner.apply(node, zone_id, key, value, version).await
    }
    async fn read(
        &self,
        node: vals_cluster::NodeId,
        zone_id: vals_cluster::ZoneId,
        key: &[u8],
    ) -> Result<Option<vals_cluster::Stored>, vals_cluster::TransportError> {
        let now = mocktime::elapsed_us();
        let finish = {
            let q = self.state.qdepth.get();
            let mut slots = self.state.slots.borrow_mut();
            let v = slots.entry(node).or_insert_with(|| vec![0u64; q]);
            let idx = (0..v.len()).min_by_key(|&i| v[i]).unwrap();
            let start = now.max(v[idx]);
            let finish = start + self.state.latency_us;
            v[idx] = finish;
            finish
        };
        mocktime::delay_us(finish - now).await;
        self.inner.read(node, zone_id, key).await
    }
    async fn members(
        &self,
        node: vals_cluster::NodeId,
    ) -> Result<vals_cluster::RingView, vals_cluster::TransportError> {
        self.inner.members(node).await
    }
    async fn zones(
        &self,
        node: vals_cluster::NodeId,
    ) -> Result<vals_cluster::Catalog, vals_cluster::TransportError> {
        self.inner.zones(node).await
    }
}

pub(super) fn workload_mcread(n: usize, vsize: usize, batch: usize) {
    use std::cell::Cell;
    use std::rc::Rc;
    use vals_cluster::{Coordinator, WriteConcern, LocalCluster, Ring, VersionedStore};
    use vals_rt::LocalExecutor;
    use vals_store::{Engine, Ordering};

    const NODES: u32 = 3;
    const RF: usize = 3;
    const R: usize = 2;
    let latency_us = if vsize == 0 { 20 } else { vsize as u64 };
    let qdepth = batch.max(1);
    let cfg = bench_config(Ordering::Unsorted);

    let mut ring = Ring::with_vnodes(RF, 128);
    let mut lc = LocalCluster::new();
    for node in 1..=NODES {
        let dev = MemBlockDevice::new(BLOCK_SIZE, device_blocks(n, 200));
        lc.insert(
            node,
            VersionedStore::new(Engine::create_prewiped(dev, cfg), node).unwrap(),
        );
        ring.add_node(node);
    }
    let state = Rc::new(ServerState {
        latency_us,
        qdepth: Cell::new(qdepth),
        slots: std::cell::RefCell::new(std::collections::HashMap::new()),
    });
    let coord = Rc::new(Coordinator::new(
        0,
        ring,
        R,
        ServerModel {
            inner: lc,
            state: state.clone(),
        },
    ));
    let val = make_value(200, 0);
    let key = |i: usize| format!("k{i:09}").into_bytes();

    for i in 0..n {
        vals_rt::block_on(
            coord.write_with(&key(i), &val, WriteConcern::All),
            mocktime::drive,
        )
        .expect("seed write");
    }

    let sim_ops = |ops: usize, us: u64| ops as f64 / (us.max(1) as f64 / 1e6);
    let run = |conc: usize, qd: usize| -> f64 {
        state.reset(qd);
        mocktime::reset();
        let next = Rc::new(Cell::new(0usize));
        let ex = LocalExecutor::new();
        for _ in 0..conc {
            let coord = coord.clone();
            let next = next.clone();
            ex.spawn(async move {
                loop {
                    let i = next.get();
                    if i >= n {
                        break;
                    }
                    next.set(i + 1);
                    let _ = coord.read(&format!("k{i:09}").into_bytes()).await;
                }
            });
        }
        ex.run(mocktime::drive);
        sim_ops(n, mocktime::elapsed_us())
    };

    println!(
        "--- mcread: {NODES} nodes RF={RF} R={R}, device read L={latency_us}µs, \
         qdepth Q={qdepth} (simulated time) ---"
    );
    println!("  concurrency   serial (Q=1)    pipelined (Q={qdepth})   speedup");
    for &conc in &[1usize, 8, 64, 256] {
        let serial = run(conc, 1);
        let pipe = run(conc, qdepth);
        println!(
            "  {conc:<11}   {serial:>10.0}    {pipe:>14.0}     {:.1}x",
            pipe / serial
        );
    }
}

