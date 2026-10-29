use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};

use vals_spdk::{AsyncRead, BlockDevice, MaintenanceCacheKey};
use vals_store::{
    build_and_write_flush, compact_and_write_cancellable, CompactPlan, CoverScanPlan, EngineError,
    FlushPlan, WrittenCompact, WrittenFlush,
};

use crate::observe;
use crate::reactor::Notify;
use crate::zone::ZoneId;
use vals_store::alloc::SegmentId;

#[non_exhaustive]
pub enum MaintJob<D: BlockDevice> {
    #[cfg(test)]
    Echo(u64),
    Flush(ZoneId, FlushPlan<D>),
    Compact(ZoneId, CompactPlan<D>),
    CleanupCompact(ZoneId, (u64, u64, u64, u64), CompactPlan<D>),
    CoverScan(ZoneId, Box<CoverScanPlan<D>>),
    #[cfg(test)]
    Stall(u64, std::time::Duration),
}

pub const COVER_SCAN_CHUNK_BYTES: usize = 16 * 1024 * 1024;

pub const COVER_SCAN_LINGER: std::time::Duration = std::time::Duration::from_millis(100);

const CLASS_URGENT: usize = 0;
const CLASS_COVER: usize = 1;
const CLASS_SCAN: usize = 2;
const CLASS_NORMAL: usize = 3;
const JOB_CLASSES: usize = 4;
const COVER_SCAN_DUTY_CHUNKS: usize = 4;

fn job_class<D: BlockDevice>(job: &MaintJob<D>) -> usize {
    match job {
        MaintJob::Compact(_, plan) if plan.is_pressure() => CLASS_URGENT,
        MaintJob::CoverScan(_, plan) if plan.is_rebuild_scan() => CLASS_SCAN,
        MaintJob::CoverScan(..) => CLASS_COVER,
        _ => CLASS_NORMAL,
    }
}

#[non_exhaustive]
pub enum MaintDone<D: BlockDevice> {
    #[cfg(test)]
    Echoed(u64),
    Flushed(
        ZoneId,
        Result<WrittenFlush<D>, (Vec<SegmentId>, EngineError)>,
    ),
    Compacted(
        ZoneId,
        std::time::Duration,
        Result<WrittenCompact<D>, (Vec<SegmentId>, EngineError)>,
    ),
    CleanupCompacted(
        ZoneId,
        (u64, u64, u64, u64),
        std::time::Duration,
        Result<WrittenCompact<D>, (Vec<SegmentId>, EngineError)>,
    ),
    CoverScanned(ZoneId, Box<CoverScanPlan<D>>, Result<bool, EngineError>),
    #[cfg(test)]
    Stalled(u64),
}

struct ZoneState<D: BlockDevice> {
    write_dev: Arc<D>,
}

type SiblingKey = (usize, ZoneId, Option<MaintenanceCacheKey>);

struct ActiveMerge {
    input_bytes: u64,
}

impl ActiveMerge {
    fn new(input_bytes: u64) -> Self {
        let gauges = crate::observe::gauges();
        gauges.merge_in_flight.adjust(0, 1);
        gauges.merge_in_flight_input_bytes.adjust(0, input_bytes);
        ActiveMerge { input_bytes }
    }
}

impl Drop for ActiveMerge {
    fn drop(&mut self) {
        let gauges = crate::observe::gauges();
        gauges.merge_in_flight.adjust(1, 0);
        gauges
            .merge_in_flight_input_bytes
            .adjust(self.input_bytes, 0);
    }
}

fn run_job<D: BlockDevice + AsyncRead>(
    owner: usize,
    job: MaintJob<D>,
    siblings: &mut HashMap<SiblingKey, ZoneState<D>>,
    cancel: &AtomicBool,
) -> MaintDone<D> {
    match job {
        #[cfg(test)]
        MaintJob::Echo(n) => {
            std::thread::sleep(std::time::Duration::from_millis(5));
            MaintDone::Echoed(n)
        }
        MaintJob::Flush(zone, plan) => {
            let write_dev = match resolve_handle_b(owner, zone, plan.serving_dev(), siblings) {
                Ok(d) => d,
                Err(e) => {
                    return MaintDone::Flushed(
                        zone,
                        Err((plan.segments().to_vec(), EngineError::from(e))),
                    );
                }
            };
            let res = vals_rt::block_on(build_and_write_flush(plan, &*write_dev), || {
                write_dev.poll_device();
            });
            MaintDone::Flushed(zone, res)
        }
        MaintJob::Compact(zone, plan) => {
            let input_bytes = plan.input_bytes();
            let write_dev = match resolve_handle_b(owner, zone, plan.serving_dev(), siblings) {
                Ok(d) => d,
                Err(e) => {
                    return MaintDone::Compacted(
                        zone,
                        std::time::Duration::ZERO,
                        Err((plan.segments().to_vec(), EngineError::from(e))),
                    );
                }
            };
            let _active = ActiveMerge::new(input_bytes);
            let start = std::time::Instant::now();
            let res = vals_rt::block_on(
                compact_and_write_cancellable(plan, write_dev.clone(), cancel),
                || {
                    write_dev.poll_device();
                },
            );
            MaintDone::Compacted(zone, start.elapsed(), res)
        }
        MaintJob::CleanupCompact(zone, fingerprint, plan) => {
            let input_bytes = plan.input_bytes();
            let write_dev = match resolve_handle_b(owner, zone, plan.serving_dev(), siblings) {
                Ok(d) => d,
                Err(e) => {
                    return MaintDone::CleanupCompacted(
                        zone,
                        fingerprint,
                        std::time::Duration::ZERO,
                        Err((plan.segments(), EngineError::from(e))),
                    );
                }
            };
            let _active = ActiveMerge::new(input_bytes);
            let start = std::time::Instant::now();
            let res = vals_rt::block_on(
                compact_and_write_cancellable(plan, write_dev.clone(), cancel),
                || {
                    write_dev.poll_device();
                },
            );
            MaintDone::CleanupCompacted(zone, fingerprint, start.elapsed(), res)
        }
        MaintJob::CoverScan(zone, mut plan) => {
            let write_dev = match resolve_handle_b(owner, zone, plan.serving_dev(), siblings) {
                Ok(d) => d,
                Err(e) => return MaintDone::CoverScanned(zone, plan, Err(EngineError::from(e))),
            };
            let res = if cancel.load(Ordering::Relaxed) {
                Err(EngineError::Cancelled)
            } else {
                plan.step(&*write_dev, COVER_SCAN_CHUNK_BYTES)
            };
            MaintDone::CoverScanned(zone, plan, res)
        }
        #[cfg(test)]
        MaintJob::Stall(n, dur) => {
            std::thread::sleep(dur);
            MaintDone::Stalled(n)
        }
    }
}

fn resolve_handle_b<D: BlockDevice>(
    owner: usize,
    zone: ZoneId,
    serving_dev: &D,
    siblings: &mut HashMap<SiblingKey, ZoneState<D>>,
) -> Result<Arc<D>, vals_spdk::BlockError> {
    let key = (owner, zone, serving_dev.maintenance_sibling_cache_key());
    #[allow(clippy::map_entry)]
    if !siblings.contains_key(&key) {
        let write_dev = Arc::new(serving_dev.maintenance_sibling().inspect_err(|_| {
            crate::observe::counters().maint_qpair_errors.inc();
        })?);
        siblings.insert(key, ZoneState { write_dev });
    }
    Ok(siblings.get(&key).unwrap().write_dev.clone())
}

type OwnedJob<D> = (usize, MaintJob<D>);

struct DoneRoute<D: BlockDevice> {
    done: mpsc::Sender<MaintDone<D>>,
    wake: Arc<Notify>,
}

impl<D: BlockDevice> Clone for DoneRoute<D> {
    fn clone(&self) -> Self {
        DoneRoute {
            done: self.done.clone(),
            wake: self.wake.clone(),
        }
    }
}

pub struct MaintHandle<D: BlockDevice> {
    owner: usize,
    jobs_flush: mpsc::Sender<OwnedJob<D>>,
    jobs_compact: mpsc::Sender<OwnedJob<D>>,
    done: mpsc::Receiver<MaintDone<D>>,
    wake: Arc<Notify>,
}

#[allow(clippy::result_large_err)]
fn submit_owned<D: BlockDevice>(
    owner: usize,
    jobs_flush: &mpsc::Sender<OwnedJob<D>>,
    jobs_compact: &mpsc::Sender<OwnedJob<D>>,
    job: MaintJob<D>,
) -> Result<(), mpsc::SendError<MaintJob<D>>> {
    let tx = match job {
        MaintJob::Compact(..) | MaintJob::CleanupCompact(..) | MaintJob::CoverScan(..) => jobs_compact,
        MaintJob::Flush(..) => jobs_flush,
        #[cfg(test)]
        MaintJob::Echo(..) => jobs_flush,
        #[cfg(test)]
        MaintJob::Stall(..) => jobs_compact,
    };
    vals_store::maint_job_queued();
    tx.send((owner, job)).map_err(|e| {
        vals_store::maint_job_started();
        mpsc::SendError(e.0.1)
    })
}

impl<D: BlockDevice> MaintHandle<D> {
    #[cfg(test)]
    #[allow(clippy::result_large_err)]
    pub fn submit(&self, job: MaintJob<D>) -> Result<(), mpsc::SendError<MaintJob<D>>> {
        submit_owned(
            self.owner,
            &self.jobs_flush,
            &self.jobs_compact,
            job,
        )
    }

    pub fn owner_id(&self) -> usize {
        self.owner
    }

    pub fn drain(&self) -> Vec<MaintDone<D>> {
        self.done.try_iter().collect()
    }

    pub fn wake(&self) -> &Arc<Notify> {
        &self.wake
    }

    pub fn submitter(&self) -> MaintSubmit<D> {
        MaintSubmit {
            owner: self.owner,
            jobs_flush: self.jobs_flush.clone(),
            jobs_compact: self.jobs_compact.clone(),
        }
    }
}

#[derive(Clone)]
pub struct MaintSubmit<D: BlockDevice> {
    owner: usize,
    jobs_flush: mpsc::Sender<OwnedJob<D>>,
    jobs_compact: mpsc::Sender<OwnedJob<D>>,
}

impl<D: BlockDevice> MaintSubmit<D> {
    #[allow(clippy::result_large_err)]
    pub fn submit(&self, job: MaintJob<D>) -> Result<(), mpsc::SendError<MaintJob<D>>> {
        submit_owned(
            self.owner,
            &self.jobs_flush,
            &self.jobs_compact,
            job,
        )
    }
}

pub const COMPACT_LANES_DEFAULT: usize = 2;

pub const COMPACT_LANES_MAX: usize = 4;

pub fn compact_lanes() -> usize {
    COMPACT_LANES_DEFAULT
}

#[cfg(test)]
pub fn spawn<D: BlockDevice + AsyncRead + 'static>() -> (MaintHandle<D>, MaintJoin) {
    let (mut handles, join) = spawn_pool(1);
    (handles.pop().unwrap(), join)
}

pub fn spawn_pool<D: BlockDevice + AsyncRead + 'static>(
    owners: usize,
) -> (Vec<MaintHandle<D>>, MaintJoin) {
    spawn_pool_lanes(owners, compact_lanes())
}

pub fn spawn_pool_lanes<D: BlockDevice + AsyncRead + 'static>(
    owners: usize,
    compact_lanes: usize,
) -> (Vec<MaintHandle<D>>, MaintJoin) {
    let compact_lanes = compact_lanes.clamp(1, COMPACT_LANES_MAX);
    let (flush_tx, flush_rx) = mpsc::channel::<OwnedJob<D>>();
    let (compact_tx, compact_rx) = mpsc::channel::<OwnedJob<D>>();
    let mut handles = Vec::with_capacity(owners);
    let mut routes = Vec::with_capacity(owners);
    for owner in 0..owners {
        let (done_tx, done_rx) = mpsc::channel::<MaintDone<D>>();
        let wake = Arc::new(Notify::new());
        routes.push(DoneRoute {
            done: done_tx,
            wake: wake.clone(),
        });
        handles.push(MaintHandle {
            owner,
            jobs_flush: flush_tx.clone(),
            jobs_compact: compact_tx.clone(),
            done: done_rx,
            wake,
        });
    }
    drop(flush_tx);
    drop(compact_tx);
    let cancel = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicUsize::new(0));
    let cancelled = Arc::new(AtomicUsize::new(0));
    let flush_routes = routes.clone();
    let flush_dispatch = Arc::new(Mutex::new(Dispatch::new(
        flush_rx, owners, cancel.clone(), dropped.clone(),
    )));
    let mut joins = Vec::with_capacity(1 + compact_lanes);
    let flush_cancel = cancel.clone();
    let flush_dropped = dropped.clone();
    let flush_cancelled = cancelled.clone();
    joins.push(Some(observe::spawn_abort_on_panic(
        "vals-maint-flush",
        move || {
            worker_loop(
                flush_dispatch, 0, flush_routes, flush_cancel, flush_dropped, flush_cancelled,
            )
        },
    )));
    let compact_dispatch = Arc::new(Mutex::new(compaction_dispatch(
        compact_rx, owners, cancel.clone(), dropped.clone(), compact_lanes,
    )));
    for lane in 0..compact_lanes {
        let dispatch = compact_dispatch.clone();
        let lane_routes = routes.clone();
        let lane_cancel = cancel.clone();
        let lane_dropped = dropped.clone();
        let lane_cancelled = cancelled.clone();
        joins.push(Some(observe::spawn_abort_on_panic(
            &format!("vals-maint-cmp{lane}"),
            move || {
                worker_loop(dispatch, lane, lane_routes, lane_cancel, lane_dropped, lane_cancelled)
            },
        )));
    }
    log::info!(
        "maintenance pool registered {owners} owners on 1 flush + {compact_lanes} compaction I/O lanes"
    );
    (handles, MaintJoin {
        joins, cancel, dropped, cancelled,
    })
}

fn compaction_dispatch<D: BlockDevice>(
    rx: mpsc::Receiver<OwnedJob<D>>,
    owners: usize,
    cancel: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    compact_lanes: usize,
) -> Dispatch<MaintJob<D>> {
    let linger = if compact_lanes >= 2 { COVER_SCAN_LINGER } else { std::time::Duration::ZERO };
    let scan_duty_limit = (compact_lanes == 1).then_some(COVER_SCAN_DUTY_CHUNKS);
    Dispatch::new(rx, owners, cancel, dropped)
        .with_classes(job_class::<D>, 1, linger, scan_duty_limit)
}

pub struct MaintJoin {
    joins: Vec<Option<std::thread::JoinHandle<()>>>,
    cancel: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
}

impl MaintJoin {
    pub fn cancel(&self) {
        self.cancel.store(true, Ordering::Relaxed);
    }

    pub fn cancellation_counts(&self) -> (usize, usize) {
        (
            self.dropped.load(Ordering::Relaxed),
            self.cancelled.load(Ordering::Relaxed),
        )
    }

    pub fn join(&mut self) {
        for h in &mut self.joins {
            if let Some(h) = h.take() {
                let _ = h.join();
            }
        }
    }
}

const INGRESS_DRAIN_LIMIT: usize = 256;

fn next_pending<T>(pending: &[VecDeque<T>], next_owner: usize) -> Option<usize> {
    (0..pending.len()).find_map(|offset| {
        let owner = (next_owner + offset) % pending.len();
        (!pending[owner].is_empty()).then_some(owner)
    })
}

struct ScanClock {
    epoch: std::time::Instant,
    last_done_ns: AtomicU64,
}

impl ScanClock {
    fn new() -> Self {
        ScanClock { epoch: std::time::Instant::now(), last_done_ns: AtomicU64::new(0) }
    }

    fn mark(&self) {
        let now = self.epoch.elapsed().as_nanos().min(u64::MAX as u128 - 1) as u64;
        self.last_done_ns.store(now + 1, Ordering::Relaxed);
    }

    fn within(&self, window: std::time::Duration) -> bool {
        let last = self.last_done_ns.load(Ordering::Relaxed);
        if last == 0 || window.is_zero() {
            return false;
        }
        let now = self.epoch.elapsed().as_nanos().min(u64::MAX as u128 - 1) as u64 + 1;
        now.saturating_sub(last) < window.as_nanos().min(u64::MAX as u128) as u64
    }
}

enum Pick<T> {
    Job(usize, usize, T),
    Wait,
    Closed,
}

struct Dispatch<T> {
    rx: mpsc::Receiver<(usize, T)>,
    pending: [Vec<VecDeque<T>>; JOB_CLASSES],
    next_owner: [usize; JOB_CLASSES],
    classify: fn(&T) -> usize,
    scan_lanes: usize,
    scan_chunks: usize,
    scan_duty_limit: Option<usize>,
    linger: std::time::Duration,
    scan_clock: Arc<ScanClock>,
    connected: bool,
    cancel: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
}

fn normal_class<T>(_: &T) -> usize {
    CLASS_NORMAL
}

impl<T> Dispatch<T> {
    fn new(
        rx: mpsc::Receiver<(usize, T)>,
        owners: usize,
        cancel: Arc<AtomicBool>,
        dropped: Arc<AtomicUsize>,
    ) -> Self {
        Dispatch {
            rx,
            pending: std::array::from_fn(|_| (0..owners.max(1)).map(|_| VecDeque::new()).collect()),
            next_owner: [0; JOB_CLASSES],
            classify: normal_class::<T>,
            scan_lanes: 0,
            scan_chunks: 0,
            scan_duty_limit: None,
            linger: std::time::Duration::ZERO,
            scan_clock: Arc::new(ScanClock::new()),
            connected: true,
            cancel,
            dropped,
        }
    }

    fn with_classes(mut self, classify: fn(&T) -> usize, scan_lanes: usize, linger: std::time::Duration, scan_duty_limit: Option<usize>) -> Self {
        self.classify = classify;
        self.scan_lanes = scan_lanes;
        self.linger = linger;
        self.scan_duty_limit = scan_duty_limit;
        self
    }

    fn queued(&self) -> usize {
        self.pending.iter().flatten().map(VecDeque::len).sum()
    }

    fn push(&mut self, owner: usize, job: T) {
        let class = (self.classify)(&job).min(JOB_CLASSES - 1);
        self.pending[class][owner].push_back(job);
    }

    fn drain_ingress(&mut self) {
        for _ in 0..INGRESS_DRAIN_LIMIT {
            match self.rx.try_recv() {
                Ok((owner, job)) => self.push(owner, job),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => {
                    self.connected = false;
                    break;
                }
            }
        }
    }

    #[cfg(test)]
    fn scan_done(&mut self) {
        self.scan_clock.mark();
    }

    #[cfg(test)]
    fn next_job(&mut self) -> Option<(usize, T)> {
        loop {
            match self.pick(usize::MAX) {
                Pick::Job(owner, _, job) => return Some((owner, job)),
                Pick::Wait => {}
                Pick::Closed => return None,
            }
        }
    }

    fn pick(&mut self, lane: usize) -> Pick<T> {
        loop {
            if self.cancel.load(Ordering::Relaxed) {
                let n = self.queued() + self.rx.try_iter().count();
                self.pending.iter_mut().flatten().for_each(VecDeque::clear);
                for _ in 0..n {
                    vals_store::maint_job_started();
                }
                self.dropped.fetch_add(n, Ordering::Relaxed);
                return Pick::Closed;
            }
            if self.queued() == 0 && self.connected {
                match self.rx.recv_timeout(std::time::Duration::from_millis(10)) {
                    Ok((owner, job)) => self.push(owner, job),
                    Err(mpsc::RecvTimeoutError::Disconnected) => self.connected = false,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            self.drain_ingress();
            let scan_lane = lane < self.scan_lanes;
            let scan_first = scan_lane && self.scan_duty_limit.is_none_or(|limit| self.scan_chunks < limit);
            let lingering = scan_first && self.scan_clock.within(self.linger);
            let order = if scan_first {
                [CLASS_URGENT, CLASS_COVER, CLASS_SCAN, CLASS_NORMAL]
            } else if scan_lane {
                [CLASS_URGENT, CLASS_COVER, CLASS_NORMAL, CLASS_SCAN]
            } else {
                [CLASS_URGENT, CLASS_NORMAL, CLASS_COVER, CLASS_SCAN]
            };
            for class in order {
                if class == CLASS_NORMAL && lingering {
                    continue;
                }
                if let Some(owner) = next_pending(&self.pending[class], self.next_owner[class]) {
                    self.next_owner[class] = (owner + 1) % self.pending[class].len();
                    let job = self.pending[class][owner].pop_front().unwrap();
                    if let Some(limit) = self.scan_duty_limit {
                        if lane < self.scan_lanes {
                            if class == CLASS_SCAN {
                                self.scan_chunks = (self.scan_chunks + 1).min(limit);
                            } else if class == CLASS_NORMAL {
                                self.scan_chunks = 0;
                            }
                        }
                    }
                    return Pick::Job(owner, class, job);
                }
            }
            if lingering && self.queued() > 0 {
                return Pick::Wait;
            }
            if !self.connected && self.queued() == 0 {
                return Pick::Closed;
            }
        }
    }
}

fn worker_loop<D: BlockDevice + AsyncRead + 'static>(
    dispatch: Arc<Mutex<Dispatch<MaintJob<D>>>>,
    lane: usize,
    routes: Vec<DoneRoute<D>>,
    cancel: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
) {
    let mut siblings: HashMap<SiblingKey, ZoneState<D>> = HashMap::new();
    let lock = || dispatch.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    let scan_clock = lock().scan_clock.clone();
    loop {
        let next = lock().pick(lane);
        let (owner, class, job) = match next {
            Pick::Job(owner, class, job) => (owner, class, job),
            Pick::Wait => {
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            Pick::Closed => break,
        };
        vals_store::maint_job_started();
        if cancel.load(Ordering::Relaxed) {
            dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let done = run_job(owner, job, &mut siblings, &cancel);
        if class == CLASS_SCAN || class == CLASS_COVER {
            scan_clock.mark();
        }
        if matches!(&done,
            MaintDone::Compacted(_, _, Err((_, EngineError::Cancelled)))
            | MaintDone::CleanupCompacted(_, _, _, Err((_, EngineError::Cancelled)))
        ) {
            cancelled.fetch_add(1, Ordering::Relaxed);
        }
        if routes[owner].done.send(done).is_ok() {
            routes[owner].wake.signal();
        }
    }
}

#[cfg(test)]
mod cover_scan_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reactor::poll_io_nonblocking;
    use crate::zonestore::SubDev;
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;
    use vals_rt::LocalExecutor;
    use vals_spdk::MemBlockDevice;

    const TEST_BS: usize = 512;

    #[test]
    fn cancel_discards_queued_jobs_without_waiting_for_the_queue() {
        let rendezvous = Rendezvous::new(2, Duration::from_secs(10));
        let mut engine = gated_engine(
            Arc::new(Mutex::new(HashSet::new())), 0, rendezvous.clone(),
        );
        engine.put(b"key", b"value", 1).unwrap();
        let plan = engine.flush_plan(RunBuildOpts::default()).unwrap().unwrap();
        let (mut handles, mut join) = spawn_pool_lanes::<CountingMaintDevice>(1, 1);
        let handle = handles.pop().unwrap();
        handle.submit(MaintJob::Flush(0, plan)).unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while rendezvous.arrived.load(AtomicOrdering::SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline, "flush did not start");
            std::thread::yield_now();
        }
        for n in 0..200 {
            handle.submit(MaintJob::Echo(n)).unwrap();
        }
        let start = std::time::Instant::now();
        join.cancel();
        rendezvous.arrive();
        join.join();
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(join.cancellation_counts(), (200, 0));
        assert!(matches!(handle.drain().as_slice(), [MaintDone::Flushed(..)]));
    }

    #[test]
    fn every_owner_progresses_during_a_continuous_owner_flood() {
        const OWNERS: usize = 8;
        let (tx, rx) = mpsc::channel::<(usize, u64)>();
        for n in 0..4096 {
            tx.send((0, n)).unwrap();
        }
        for owner in 1..OWNERS {
            tx.send((owner, owner as u64)).unwrap();
        }
        let flooding = Arc::new(AtomicBool::new(true));
        let sent = Arc::new(AtomicUsize::new(0));
        let flood_tx = tx.clone();
        let flood_flag = flooding.clone();
        let flood_sent = sent.clone();
        let flood = std::thread::spawn(move || {
            let mut n = 4096u64;
            while flood_flag.load(AtomicOrdering::Relaxed) {
                if flood_tx.send((0, n)).is_err() {
                    break;
                }
                n += 1;
                flood_sent.fetch_add(1, AtomicOrdering::Relaxed);
            }
        });
        while sent.load(AtomicOrdering::Relaxed) < 4096 {
            std::thread::yield_now();
        }
        drop(tx);

        let (progress_tx, progress_rx) = mpsc::channel();
        let scheduler = std::thread::spawn(move || {
            let mut dispatch = Dispatch::new(
                rx, OWNERS, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)),
            );
            let mut seen = HashSet::new();
            while seen.len() < OWNERS - 1 {
                let Some((owner, _job)) = dispatch.next_job() else {
                    break;
                };
                progress_tx.send(owner).unwrap();
                if owner != 0 {
                    seen.insert(owner);
                }
            }
        });

        let deadline = std::time::Instant::now() + Duration::from_millis(250);
        let mut progressed = HashSet::new();
        while progressed.len() < OWNERS - 1 {
            let Some(left) = deadline.checked_duration_since(std::time::Instant::now()) else {
                break;
            };
            match progress_rx.recv_timeout(left) {
                Ok(owner) if owner != 0 => {
                    progressed.insert(owner);
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        flooding.store(false, AtomicOrdering::Relaxed);
        flood.join().unwrap();
        scheduler.join().unwrap();
        assert_eq!(progressed, (1..OWNERS).collect());
    }

    #[test]
    fn serving_executor_stays_live_across_an_offloaded_echo() {
        let (handle, _join) = spawn::<MemBlockDevice>();

        let ex = LocalExecutor::new();

        const WITNESS_TURNS: u64 = 1000;
        let counter = Rc::new(Cell::new(0u64));
        {
            let counter = counter.clone();
            ex.spawn(async move {
                for _ in 0..WITNESS_TURNS {
                    counter.set(counter.get() + 1);
                    vals_rt::yield_now().await;
                }
            });
        }

        let counter_at_submit = Cell::new(u64::MAX);
        let counter_at_done = Cell::new(0u64);

        const N: u64 = 0xC0FFEE;
        let got = ex.run_until(
            async {
                counter_at_submit.set(counter.get());
                handle.submit(MaintJob::Echo(N)).expect("worker alive");
                loop {
                    handle.wake().notified().await;
                    if let Some(MaintDone::Echoed(v)) = handle.drain().pop() {
                        counter_at_done.set(counter.get());
                        break v;
                    }
                }
            },
            poll_io_nonblocking,
        );

        assert_eq!(got, N, "echo round-tripped through the worker");
        assert!(
            counter_at_done.get() > counter_at_submit.get(),
            "serving task must advance while the echo is in flight \
             (witness at submit {}, at done {})",
            counter_at_submit.get(),
            counter_at_done.get(),
        );

        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Mutex;
    use vals_store::{Config, Engine, Ordering, RunBuildOpts};

    fn mem_engine() -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(TEST_BS, 4096);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        Engine::create_prewiped(dev, cfg)
    }

    struct CountingMaintDevice {
        inner: MemBlockDevice,
        maintenance_threads: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
        maintenance_failures: Arc<AtomicUsize>,
        rendezvous: Arc<Rendezvous>,
    }

    struct Rendezvous {
        need: usize,
        patience: Duration,
        arrived: AtomicUsize,
        timeouts: AtomicUsize,
    }

    impl Rendezvous {
        fn new(need: usize, patience: Duration) -> Arc<Self> {
            Arc::new(Rendezvous {
                need,
                patience,
                arrived: AtomicUsize::new(0),
                timeouts: AtomicUsize::new(0),
            })
        }

        fn arrive(&self) {
            if self.need == 0 {
                return;
            }
            self.arrived.fetch_add(1, AtomicOrdering::SeqCst);
            let deadline = std::time::Instant::now() + self.patience;
            while self.arrived.load(AtomicOrdering::SeqCst) < self.need {
                if std::time::Instant::now() >= deadline {
                    self.timeouts.fetch_add(1, AtomicOrdering::SeqCst);
                    return;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    impl BlockDevice for CountingMaintDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
        fn submit_write(&self, lba: u64, buf: &[u8]) -> vals_spdk::WriteHandle {
            self.inner.submit_write(lba, buf)
        }
        fn poll_writes(&self) -> i32 {
            self.inner.poll_writes()
        }
        fn poll_device(&self) -> i32 {
            self.inner.poll_device()
        }
        fn take_scratch(&self, len: usize) -> vals_spdk::Scratch {
            self.inner.take_scratch(len)
        }
        fn release_scratch(&self, buf: vals_spdk::Scratch) {
            self.inner.release_scratch(buf)
        }
        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(CountingMaintDevice {
                inner: self.inner.share(),
                maintenance_threads: self.maintenance_threads.clone(),
                maintenance_failures: self.maintenance_failures.clone(),
                rendezvous: self.rendezvous.clone(),
            })
        }
        fn maintenance_sibling(&self) -> vals_spdk::Result<Self> {
            self.maintenance_threads
                .lock()
                .unwrap()
                .insert(std::thread::current().id());
            self.rendezvous.arrive();
            if self
                .maintenance_failures
                .fetch_update(
                    AtomicOrdering::Relaxed,
                    AtomicOrdering::Relaxed,
                    |n| if n > 0 { Some(n - 1) } else { None },
                )
                .is_ok()
            {
                return Err(vals_spdk::BlockError::Io(std::io::Error::other(
                    "injected maintenance sibling failure",
                )));
            }
            self.sibling()
        }
    }

    impl AsyncRead for CountingMaintDevice {
        async fn read_async(
            &self,
            lba: u64,
            buf: &mut [u8],
        ) -> vals_spdk::Result<()> {
            self.read_at(lba, buf)
        }
    }

    fn counting_engine(
        maintenance_threads: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
        maintenance_failures: usize,
    ) -> Engine<CountingMaintDevice> {
        gated_engine(
            maintenance_threads,
            maintenance_failures,
            Rendezvous::new(0, Duration::ZERO),
        )
    }

    fn gated_engine(
        maintenance_threads: Arc<Mutex<HashSet<std::thread::ThreadId>>>,
        maintenance_failures: usize,
        rendezvous: Arc<Rendezvous>,
    ) -> Engine<CountingMaintDevice> {
        let dev = CountingMaintDevice {
            inner: MemBlockDevice::new(TEST_BS, 4096),
            maintenance_threads,
            maintenance_failures: Arc::new(AtomicUsize::new(maintenance_failures)),
            rendezvous,
        };
        Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        )
    }

    fn run_offcore_flush(
        engine: &RefCell<Engine<MemBlockDevice>>,
        handle: &MaintHandle<MemBlockDevice>,
        between_plan_and_park: impl FnOnce(),
    ) -> u64 {
        let ex = LocalExecutor::new();
        let entry_count = Cell::new(0u64);
        ex.run_until(
            async {
                let plan = engine
                    .borrow_mut()
                    .flush_plan(
                        RunBuildOpts {
                            num_buckets: 1,
                            ..Default::default()
                        },
                    )
                    .unwrap()
                    .expect("non-empty memtable plans a flush");
                handle
                    .submit(MaintJob::Flush(0, plan))
                    .expect("worker alive");
                between_plan_and_park();
                loop {
                    handle.wake().notified().await;
                    if let Some(MaintDone::Flushed(_zone, res)) = handle.drain().pop() {
                        let written = res.expect("off-core flush write ok");
                        let meta = engine.borrow_mut().flush_commit(written).unwrap();
                        entry_count.set(meta.entry_count);
                        break;
                    }
                }
            },
            poll_io_nonblocking,
        );
        entry_count.get()
    }

    fn run_offcore_flush_meta<D: BlockDevice + AsyncRead>(
        engine: &RefCell<Engine<D>>,
        handle: &MaintHandle<D>,
        zone: ZoneId,
    ) -> vals_store::RunMeta {
        let ex = LocalExecutor::new();
        let meta_cell: RefCell<Option<vals_store::RunMeta>> = RefCell::new(None);
        ex.run_until(
            async {
                let plan = engine
                    .borrow_mut()
                    .flush_plan(RunBuildOpts {
                        num_buckets: 1,
                        ..Default::default()
                    })
                    .unwrap()
                    .expect("non-empty memtable plans a flush");
                handle
                    .submit(MaintJob::Flush(zone, plan))
                    .expect("worker alive");
                loop {
                    if let Some(MaintDone::Flushed(_zone, res)) = handle.drain().pop() {
                        let written = res.expect("off-core flush write ok");
                        let meta = engine.borrow_mut().flush_commit(written).unwrap();
                        *meta_cell.borrow_mut() = Some(meta);
                        break;
                    }
                    vals_rt::yield_now().await;
                }
            },
            poll_io_nonblocking,
        );
        meta_cell.into_inner().expect("flush committed a run")
    }

    #[test]
    fn offcore_flush_snapshots_unsorted_and_derives_correct_run_meta() {
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        let order = [7u32, 0, 3, 9, 1, 5, 2, 8, 4, 6];
        for &i in &order {
            engine
                .borrow_mut()
                .put(
                    format!("k{i:03}").as_bytes(),
                    format!("v{i:03}").as_bytes(),
                    i as u128 + 1,
                )
                .unwrap();
        }

        let meta = run_offcore_flush_meta(&engine, &handle, 0);
        assert_eq!(meta.entry_count, order.len() as u64);
        assert_eq!(&meta.min_key[..], b"k000");
        assert_eq!(&meta.max_key[..], b"k009");
        assert_eq!(engine.borrow().len(), 0, "memtable drained");
        for &i in &order {
            let key = format!("k{i:03}");
            let want = format!("v{i:03}");
            assert_eq!(
                engine.borrow().get(key.as_bytes()).unwrap().as_deref(),
                Some(want.as_bytes()),
                "key {key} reads back through the run",
            );
        }
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn reused_zone_id_flushes_to_the_new_subdevice_region() {
        const ZONE: ZoneId = 41;
        const REGION_BLOCKS: u64 = 4096;
        const OLD_BASE: u64 = 64;
        const NEW_BASE: u64 = OLD_BASE + REGION_BLOCKS;
        let parent = Arc::new(MemBlockDevice::new(
            TEST_BS,
            NEW_BASE + REGION_BLOCKS + 64,
        ));
        let (handle, mut join) = spawn::<SubDev<MemBlockDevice>>();
        let config = || Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let read_region = |base| {
            let mut bytes = vec![0; REGION_BLOCKS as usize * TEST_BS];
            parent.read_at(base, &mut bytes).unwrap();
            bytes
        };

        let old_engine = RefCell::new(Engine::create_prewiped(
            SubDev::new(parent.clone(), OLD_BASE, REGION_BLOCKS),
            config(),
        ));
        old_engine
            .borrow_mut()
            .put(b"old-region-key", b"old-region-value", 1)
            .unwrap();
        run_offcore_flush_meta(&old_engine, &handle, ZONE);
        drop(old_engine);
        let old_after_drop = read_region(OLD_BASE);

        let new_engine = RefCell::new(Engine::create_prewiped(
            SubDev::new(parent.clone(), NEW_BASE, REGION_BLOCKS),
            config(),
        ));
        new_engine
            .borrow_mut()
            .put(b"new-region-key", b"new-region-value", 2)
            .unwrap();
        let new_before_flush = read_region(NEW_BASE);
        run_offcore_flush_meta(&new_engine, &handle, ZONE);

        assert!(
            read_region(OLD_BASE) == old_after_drop,
            "the reused zone id must not redirect the new flush into the old region",
        );
        assert!(
            read_region(NEW_BASE) != new_before_flush,
            "maintenance output must land in the new region",
        );
        assert_eq!(
            new_engine.borrow().get(b"new-region-key").unwrap().as_deref(),
            Some(&b"new-region-value"[..]),
        );

        drop(handle);
        join.join();
    }

    #[test]
    fn offcore_flush_then_get_returns_flushed_value() {
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        for i in 0..40u32 {
            engine
                .borrow_mut()
                .put(
                    format!("k{i:03}").as_bytes(),
                    format!("v{i:03}").as_bytes(),
                    i as u128 + 1,
                )
                .unwrap();
        }

        let n = run_offcore_flush(&engine, &handle, || {});
        assert_eq!(n, 40, "the run holds all 40 keys");
        assert_eq!(engine.borrow().len(), 0, "memtable empty after commit");
        assert_eq!(
            engine.borrow().get(b"k017").unwrap().as_deref(),
            Some(&b"v017"[..]),
            "flushed key reads back through the run after the off-core flush",
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn offcore_flush_keeps_key_overwritten_while_in_flight() {
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        engine.borrow_mut().put(b"raced", b"old", 1).unwrap();
        engine.borrow_mut().put(b"quiet", b"q", 1).unwrap();

        let n = run_offcore_flush(&engine, &handle, || {
            engine.borrow_mut().put(b"raced", b"new", 2).unwrap();
        });
        assert_eq!(n, 2, "the run captured both snapshotted keys");

        assert_eq!(
            engine.borrow().get(b"raced").unwrap().as_deref(),
            Some(&b"new"[..])
        );
        assert_eq!(engine.borrow().version(b"raced").unwrap(), Some(2));
        assert_eq!(engine.borrow().len(), 1, "only the overwritten key stayed");
        assert_eq!(
            engine.borrow().get(b"quiet").unwrap().as_deref(),
            Some(&b"q"[..])
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn serving_advances_while_an_offcore_flush_is_in_flight() {
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        for i in 0..400u32 {
            engine
                .borrow_mut()
                .put(
                    format!("key-{i:04}").as_bytes(),
                    b"value-payload-bytes",
                    i as u128 + 1,
                )
                .unwrap();
        }

        let ex = LocalExecutor::new();
        const WITNESS_TURNS: u64 = 100_000;
        let counter = Rc::new(Cell::new(0u64));
        {
            let counter = counter.clone();
            ex.spawn(async move {
                for _ in 0..WITNESS_TURNS {
                    counter.set(counter.get() + 1);
                    vals_rt::yield_now().await;
                }
            });
        }

        let counter_at_submit = Cell::new(u64::MAX);
        let counter_at_done = Cell::new(0u64);
        ex.run_until(
            async {
                let plan = engine
                    .borrow_mut()
                    .flush_plan(
                        RunBuildOpts {
                            num_buckets: 1,
                            ..Default::default()
                        },
                    )
                    .unwrap()
                    .expect("plans a flush");
                counter_at_submit.set(counter.get());
                handle
                    .submit(MaintJob::Flush(0, plan))
                    .expect("worker alive");
                loop {
                    handle.wake().notified().await;
                    if let Some(MaintDone::Flushed(_z, res)) = handle.drain().pop() {
                        let written = res.expect("flush ok");
                        engine.borrow_mut().flush_commit(written).unwrap();
                        counter_at_done.set(counter.get());
                        break;
                    }
                }
            },
            poll_io_nonblocking,
        );

        assert!(
            counter_at_done.get() > counter_at_submit.get(),
            "serving witness must advance while the off-core flush is in flight \
             (at submit {}, at done {})",
            counter_at_submit.get(),
            counter_at_done.get(),
        );
        assert_eq!(engine.borrow().len(), 0, "memtable drained after commit");
        assert_eq!(
            engine.borrow().get(b"key-0200").unwrap().as_deref(),
            Some(&b"value-payload-bytes"[..]),
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    fn flush_run(engine: &RefCell<Engine<MemBlockDevice>>) {
        engine
            .borrow_mut()
            .flush(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
            )
            .unwrap();
    }

    fn run_offcore_compact(
        engine: &RefCell<Engine<MemBlockDevice>>,
        handle: &MaintHandle<MemBlockDevice>,
        plan: vals_store::CompactPlan<MemBlockDevice>,
    ) -> vals_store::CompactRunsStats {
        let ex = LocalExecutor::new();
        let stats = RefCell::new(None);
        ex.run_until(
            async {
                handle
                    .submit(MaintJob::Compact(0, plan))
                    .expect("worker alive");
                loop {
                    handle.wake().notified().await;
                    if let Some(MaintDone::Compacted(_z, _dur, res)) = handle.drain().pop() {
                        let written = res.expect("off-core compaction write ok");
                        *stats.borrow_mut() =
                            Some(engine.borrow_mut().compact_commit(written).unwrap());
                        break;
                    }
                }
            },
            poll_io_nonblocking,
        );
        stats.into_inner().unwrap()
    }

    #[test]
    fn cover_scan_jobs_cover_recovered_runs_on_a_compaction_lane() {
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        for round in 0..3u32 {
            for i in 0..300u32 {
                let key = format!("cover-{:05}", i * 3 + round);
                engine.borrow_mut().put(key.as_bytes(), b"v", 1 + (i * 3 + round) as u128).unwrap();
            }
            flush_run(&engine);
        }
        engine.borrow_mut().reset_key_filter_for_test();
        assert_eq!(engine.borrow().key_filter_stats().uncovered_runs, 3);
        let ex = LocalExecutor::new();
        let chunks = Cell::new(0usize);
        ex.run_until(
            async {
                loop {
                    let plan = engine.borrow_mut().cover_scan_plan();
                    let Some(plan) = plan else { break };
                    let job = MaintJob::CoverScan(0, Box::new(plan));
                    assert_eq!(job_class(&job), CLASS_COVER, "a reopened run's scan is an active cover chunk");
                    handle.submit(job).expect("worker alive");
                    loop {
                        handle.wake().notified().await;
                        let Some(MaintDone::CoverScanned(_z, plan, res)) = handle.drain().pop() else {
                            continue;
                        };
                        chunks.set(chunks.get() + 1);
                        if res.expect("cover scan chunk ok") {
                            assert!(engine.borrow_mut().cover_scan_commit(*plan));
                            break;
                        }
                        assert!(engine.borrow().cover_scan_current(&plan));
                        handle.submit(MaintJob::CoverScan(0, plan)).expect("worker alive");
                    }
                }
            },
            poll_io_nonblocking,
        );
        let stats = engine.borrow().key_filter_stats();
        assert_eq!((stats.covered_runs, stats.uncovered_runs), (3, 0));
        assert!(chunks.get() >= 3);
        for i in 0..900u32 {
            let key = format!("cover-{i:05}");
            assert_eq!(engine.borrow().version(key.as_bytes()).unwrap(), Some(1 + i as u128));
            let absent = format!("cover-{i:05}-absent");
            assert_eq!(engine.borrow().version(absent.as_bytes()).unwrap(), None);
        }
    }

    #[test]
    fn dispatch_reserves_a_scan_lane_and_lingers_between_chunks() {
        let (tx, rx) = mpsc::channel::<(usize, u64)>();
        let mut dispatch = Dispatch::new(rx, 1, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)))
            .with_classes(|job: &u64| [CLASS_URGENT, CLASS_SCAN, CLASS_NORMAL][(*job / 100) as usize], 1, Duration::from_millis(200), None);
        let job = |p: Pick<u64>| match p {
            Pick::Job(_, class, job) => Some((class, job)),
            Pick::Wait => None,
            Pick::Closed => panic!("dispatch closed"),
        };
        for n in [200, 201, 202, 203, 100] {
            tx.send((0, n)).unwrap();
        }
        assert_eq!(job(dispatch.pick(1)), Some((CLASS_NORMAL, 200)), "other lanes keep merging");
        assert_eq!(job(dispatch.pick(0)), Some((CLASS_SCAN, 100)), "the reserved lane takes the scan");
        dispatch.scan_done();
        assert_eq!(job(dispatch.pick(0)), None, "the reserved lane lingers for the next chunk");
        tx.send((0, 101)).unwrap();
        assert_eq!(job(dispatch.pick(0)), Some((CLASS_SCAN, 101)));
        tx.send((0, 7)).unwrap();
        assert_eq!(job(dispatch.pick(0)), Some((CLASS_URGENT, 7)), "pressure work never waits behind scans");
        tx.send((0, 102)).unwrap();
        assert_eq!(job(dispatch.pick(1)), Some((CLASS_NORMAL, 201)));
        assert_eq!(job(dispatch.pick(1)), Some((CLASS_NORMAL, 202)));
        assert_eq!(job(dispatch.pick(1)), Some((CLASS_NORMAL, 203)));
        assert_eq!(job(dispatch.pick(1)), Some((CLASS_SCAN, 102)), "idle merge lanes help scanning");
        tx.send((0, 204)).unwrap();
        dispatch.scan_done();
        std::thread::sleep(Duration::from_millis(250));
        assert_eq!(job(dispatch.pick(0)), Some((CLASS_NORMAL, 204)), "after the linger the lane merges again");
    }

    #[test]
    fn cover_scans_progress_while_merges_are_continuously_queued() {
        const QUEUED_STALLS: usize = 8;
        let (tx, rx) = mpsc::channel();
        let mut dispatch = compaction_dispatch(rx, 1, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)), 2);
        let cancel = AtomicBool::new(false);
        let mut siblings = HashMap::new();
        let engine = RefCell::new(mem_engine());
        for round in 0..4u32 {
            for i in 0..200u32 {
                let key = format!("busy-{:05}", i * 4 + round);
                engine.borrow_mut().put(key.as_bytes(), b"v", 1 + (i * 4 + round) as u128).unwrap();
            }
            flush_run(&engine);
        }
        engine.borrow_mut().reset_key_filter_for_test();
        let runs = engine.borrow().key_filter_stats().uncovered_runs;
        assert_eq!(runs, 4);
        for n in 0..QUEUED_STALLS as u64 {
            tx.send((0, MaintJob::Stall(n, Duration::ZERO))).unwrap();
        }
        let mut next_stall = QUEUED_STALLS as u64;
        let mut chunks = 0;
        loop {
            let Some(mut plan) = engine.borrow_mut().cover_scan_plan() else { break };
            loop {
                tx.send((0, MaintJob::CoverScan(0, Box::new(plan)))).unwrap();
                let Pick::Job(owner, class, job) = dispatch.pick(1) else { panic!("merge lane idle") };
                assert_eq!(class, CLASS_NORMAL, "merge lane keeps making progress");
                let MaintDone::Stalled(n) = run_job(owner, job, &mut siblings, &cancel) else { panic!("merge lane did not run its job") };
                assert!(n < next_stall);
                tx.send((0, MaintJob::Stall(next_stall, Duration::ZERO))).unwrap();
                next_stall += 1;
                let Pick::Job(owner, class, job) = dispatch.pick(0) else { panic!("cover lane idle") };
                assert_eq!(class, CLASS_COVER, "cover chunks precede the continuously queued merges");
                let MaintDone::CoverScanned(_, scanned, res) = run_job(owner, job, &mut siblings, &cancel) else {
                    panic!("cover lane did not scan")
                };
                chunks += 1;
                assert!(chunks < 100, "cover scans must finish in bounded chunks");
                if res.expect("cover scan chunk ok") {
                    assert!(engine.borrow_mut().cover_scan_commit(*scanned));
                    break;
                }
                plan = *scanned;
            }
        }
        let stats = engine.borrow().key_filter_stats();
        assert_eq!((stats.covered_runs, stats.uncovered_runs), (4, 0));
        assert!(chunks >= 4);
    }

    #[test]
    fn flush_completes_while_a_slow_compaction_is_in_flight() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());

        for i in 0..2000u32 {
            engine
                .borrow_mut()
                .put(format!("k{i:05}").as_bytes(), b"payload-one", i as u128 + 1)
                .unwrap();
        }
        flush_run(&engine);
        for i in 0..2000u32 {
            engine
                .borrow_mut()
                .put(
                    format!("k{i:05}").as_bytes(),
                    b"payload-two",
                    i as u128 + 100_000,
                )
                .unwrap();
        }
        flush_run(&engine);
        assert_eq!(engine.borrow().run_count(), 2);

        let compact_plan = engine
            .borrow_mut()
            .compact_plan(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
            .expect("two runs plan a compaction");

        for i in 0..5u32 {
            engine
                .borrow_mut()
                .put(format!("b{i}").as_bytes(), b"bee", i as u128 + 1_000_000)
                .unwrap();
        }
        let flush_plan = engine
            .borrow_mut()
            .flush_plan(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
            )
            .unwrap()
            .expect("non-empty memtable plans a flush");

        let ex = LocalExecutor::new();
        let flush_before_compact = Cell::new(false);
        ex.run_until(
            async {
                handle
                    .submit(MaintJob::Compact(0, compact_plan))
                    .expect("compact worker alive");
                handle
                    .submit(MaintJob::Flush(0, flush_plan))
                    .expect("flush worker alive");
                let mut committed = 0u32;
                let mut compact_seen = false;
                while committed < 2 {
                    handle.wake().notified().await;
                    for done in handle.drain() {
                        match done {
                            MaintDone::Flushed(_z, res) => {
                                if !compact_seen {
                                    flush_before_compact.set(true);
                                }
                                let written = res.expect("flush write ok");
                                engine.borrow_mut().flush_commit(written).unwrap();
                                committed += 1;
                            }
                            MaintDone::Compacted(_z, _dur, res) => {
                                compact_seen = true;
                                let written = res.expect("compaction write ok");
                                engine.borrow_mut().compact_commit(written).unwrap();
                                committed += 1;
                            }
                            MaintDone::Echoed(_) | MaintDone::CleanupCompacted(..) | MaintDone::CoverScanned(..)
                            | MaintDone::Stalled(_) => {
                                unreachable!("no echo/cleanup submitted")
                            }
                        }
                    }
                }
            },
            poll_io_nonblocking,
        );

        assert!(
            flush_before_compact.get(),
            "the tiny flush must complete before the slow compaction — proving the \
             flush worker is not serialized behind the run-merge",
        );
        assert_eq!(engine.borrow().run_count(), 2, "merged + flushed survivor");
        assert_eq!(
            engine.borrow().get(b"k01000").unwrap().as_deref(),
            Some(&b"payload-two"[..]),
            "merged run keeps the newest version",
        );
        assert_eq!(
            engine.borrow().get(b"b3").unwrap().as_deref(),
            Some(&b"bee"[..]),
            "the flushed survivor's keys read back",
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn offcore_compact_keeps_run_flushed_during_merge_and_frees_only_inputs() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());

        engine.borrow_mut().put(b"shared", b"v1", 1).unwrap();
        engine.borrow_mut().put(b"k0", b"k0v", 1).unwrap();
        flush_run(&engine);
        engine.borrow_mut().put(b"shared", b"v2", 2).unwrap();
        engine.borrow_mut().put(b"k1", b"k1v", 2).unwrap();
        flush_run(&engine);
        assert_eq!(engine.borrow().run_count(), 2);

        let plan = engine
            .borrow_mut()
            .compact_plan(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
            .expect("two runs plan a compaction");
        assert!(
            engine.borrow().compact_in_flight(),
            "gate set by compact_plan"
        );

        engine.borrow_mut().put(b"shared", b"v3-from-B", 3).unwrap();
        engine.borrow_mut().put(b"b-only", b"bv", 3).unwrap();
        flush_run(&engine);
        assert_eq!(
            engine.borrow().run_count(),
            3,
            "B committed during the merge"
        );
        let used_before = engine.borrow().used_segments();

        let stats = run_offcore_compact(&engine, &handle, plan);
        assert_eq!(stats.runs_merged, 2, "only the two captured inputs merged");

        assert_eq!(engine.borrow().run_count(), 2, "merged + B survivor");
        assert!(
            !engine.borrow().compact_in_flight(),
            "gate cleared by commit"
        );

        assert_eq!(
            engine.borrow().get(b"shared").unwrap().as_deref(),
            Some(&b"v3-from-B"[..])
        );
        assert_eq!(engine.borrow().version(b"shared").unwrap(), Some(3));
        assert_eq!(
            engine.borrow().get(b"b-only").unwrap().as_deref(),
            Some(&b"bv"[..])
        );
        assert_eq!(
            engine.borrow().get(b"k0").unwrap().as_deref(),
            Some(&b"k0v"[..])
        );
        assert_eq!(
            engine.borrow().get(b"k1").unwrap().as_deref(),
            Some(&b"k1v"[..])
        );
        assert!(
            engine.borrow().used_segments() < used_before,
            "A0+A1 segments freed: before {used_before}, after {}",
            engine.borrow().used_segments()
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn offcore_compact_is_read_transparent() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());

        engine.borrow_mut().put(b"a", b"a1", 1).unwrap();
        engine.borrow_mut().put(b"b", b"b1", 1).unwrap();
        flush_run(&engine);
        engine.borrow_mut().put(b"a", b"a2", 2).unwrap();
        engine.borrow_mut().put(b"c", b"c2", 2).unwrap();
        flush_run(&engine);
        engine.borrow_mut().put(b"a", b"a3", 3).unwrap();
        engine.borrow_mut().put(b"d", b"d3", 3).unwrap();
        flush_run(&engine);
        assert_eq!(engine.borrow().run_count(), 3);

        let scan_before = engine.borrow().scan(None, None).unwrap();
        let versions_before: Vec<(Vec<u8>, u128)> = ["a", "b", "c", "d"]
            .iter()
            .map(|k| {
                (
                    k.as_bytes().to_vec(),
                    engine.borrow().version(k.as_bytes()).unwrap().unwrap(),
                )
            })
            .collect();

        let plan = engine
            .borrow_mut()
            .compact_plan(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
            .expect("three runs plan a compaction");
        let stats = run_offcore_compact(&engine, &handle, plan);
        assert_eq!(stats.runs_merged, 3);
        assert_eq!(stats.entries_written, 4, "4 distinct keys a..d");
        assert_eq!(
            engine.borrow().run_count(),
            1,
            "all merged (no concurrent flush)"
        );

        assert_eq!(
            engine.borrow().scan(None, None).unwrap(),
            scan_before,
            "scan changed"
        );
        for (k, v) in &versions_before {
            assert_eq!(
                engine.borrow().version(k).unwrap(),
                Some(*v),
                "version changed for {k:?}"
            );
        }
        assert_eq!(
            engine.borrow().get(b"a").unwrap().as_deref(),
            Some(&b"a3"[..])
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn offcore_compact_abort_frees_segs_and_leaves_runs_intact() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        engine.borrow_mut().put(b"a", b"a1", 1).unwrap();
        engine.borrow_mut().put(b"b", b"b1", 1).unwrap();
        flush_run(&engine);
        engine.borrow_mut().put(b"a", b"a2", 2).unwrap();
        flush_run(&engine);
        assert_eq!(engine.borrow().run_count(), 2);
        let used_before = engine.borrow().used_segments();

        let plan = engine
            .borrow_mut()
            .compact_plan(
                RunBuildOpts {
                    num_buckets: 1,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
            .expect("two runs plan a compaction");
        assert!(engine.borrow().compact_in_flight());
        let reserved = plan.segments().to_vec();
        drop(plan);
        engine.borrow_mut().free_compact_segs(&reserved);

        assert!(
            !engine.borrow().compact_in_flight(),
            "gate cleared on abort"
        );
        assert_eq!(engine.borrow().run_count(), 2, "both input runs kept");
        assert_eq!(
            engine.borrow().get(b"a").unwrap().as_deref(),
            Some(&b"a2"[..])
        );
        assert_eq!(
            engine.borrow().get(b"b").unwrap().as_deref(),
            Some(&b"b1"[..])
        );
        assert_eq!(
            engine.borrow().used_segments(),
            used_before,
            "reserved output segs returned to the allocator (no leak, no run freed)"
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn serving_advances_while_an_offcore_compaction_is_in_flight() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (handle, _join) = spawn::<MemBlockDevice>();
        let engine = RefCell::new(mem_engine());
        for i in 0..300u32 {
            engine
                .borrow_mut()
                .put(
                    format!("k{i:04}").as_bytes(),
                    b"payload-bytes",
                    i as u128 + 1,
                )
                .unwrap();
        }
        flush_run(&engine);
        for i in 0..300u32 {
            engine
                .borrow_mut()
                .put(
                    format!("k{i:04}").as_bytes(),
                    b"payload-2",
                    i as u128 + 1000,
                )
                .unwrap();
        }
        flush_run(&engine);
        assert_eq!(engine.borrow().run_count(), 2);

        let ex = LocalExecutor::new();
        const WITNESS_TURNS: u64 = 100_000;
        let counter = Rc::new(Cell::new(0u64));
        {
            let counter = counter.clone();
            ex.spawn(async move {
                for _ in 0..WITNESS_TURNS {
                    counter.set(counter.get() + 1);
                    vals_rt::yield_now().await;
                }
            });
        }
        let counter_at_submit = Cell::new(u64::MAX);
        let counter_at_done = Cell::new(0u64);
        ex.run_until(
            async {
                let plan = engine
                    .borrow_mut()
                    .compact_plan(
                        RunBuildOpts {
                            num_buckets: 1,
                            ..Default::default()
                        },
                        None,
                    )
                    .unwrap()
                    .expect("two runs plan a compaction");
                counter_at_submit.set(counter.get());
                handle
                    .submit(MaintJob::Compact(0, plan))
                    .expect("worker alive");
                loop {
                    handle.wake().notified().await;
                    if let Some(MaintDone::Compacted(_z, _dur, res)) = handle.drain().pop() {
                        let written = res.expect("compaction ok");
                        engine.borrow_mut().compact_commit(written).unwrap();
                        counter_at_done.set(counter.get());
                        break;
                    }
                }
            },
            poll_io_nonblocking,
        );
        assert!(
            counter_at_done.get() > counter_at_submit.get(),
            "serving witness must advance while the off-core compaction is in flight \
             (at submit {}, at done {})",
            counter_at_submit.get(),
            counter_at_done.get(),
        );
        assert_eq!(engine.borrow().run_count(), 1, "merged to one");
        assert_eq!(
            engine.borrow().get(b"k0150").unwrap().as_deref(),
            Some(&b"payload-2"[..])
        );
        drop(handle);
        std::thread::sleep(Duration::from_millis(5));
    }

    #[test]
    fn eight_owners_route_both_lanes_and_shutdown_with_two_maintenance_owners() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        const OWNERS: usize = 8;
        let maintenance_threads = Arc::new(Mutex::new(HashSet::new()));
        let mut engines: Vec<Engine<CountingMaintDevice>> = (0..OWNERS)
            .map(|_| counting_engine(maintenance_threads.clone(), 0))
            .collect();
        let (handles, mut join) = spawn_pool_lanes::<CountingMaintDevice>(OWNERS, 1);

        for (owner, engine) in engines.iter_mut().enumerate() {
            engine.put(b"key", b"v1", 1).unwrap();
            engine.flush(RunBuildOpts::default()).unwrap();
            engine.put(b"key", b"v2", 2).unwrap();
            engine.flush(RunBuildOpts::default()).unwrap();
            let plan = engine
                .compact_plan(RunBuildOpts::default(), None)
                .unwrap()
                .expect("two runs plan a merge");
            handles[owner]
                .submit(MaintJob::Compact(owner as ZoneId, plan))
                .expect("compact lane alive");
        }
        for (owner, handle) in handles.iter().enumerate() {
            let done = handle
                .done
                .recv_timeout(Duration::from_secs(5))
                .expect("every owner receives its compact completion");
            match done {
                MaintDone::Compacted(zone, _, result) => {
                    assert_eq!(zone, owner as ZoneId);
                    engines[owner]
                        .compact_commit(result.expect("compact write succeeds"))
                        .unwrap();
                }
                _ => panic!("completion crossed maintenance lanes"),
            }
        }

        for (owner, engine) in engines.iter_mut().enumerate() {
            engine.put(b"flush", b"value", 3).unwrap();
            let plan = engine
                .flush_plan(RunBuildOpts::default())
                .unwrap()
                .expect("non-empty memtable plans a flush");
            handles[owner]
                .submit(MaintJob::Flush(owner as ZoneId, plan))
                .expect("flush lane alive");
        }
        for (owner, handle) in handles.iter().enumerate() {
            let done = handle
                .done
                .recv_timeout(Duration::from_secs(5))
                .expect("every owner receives its flush completion");
            match done {
                MaintDone::Flushed(zone, result) => {
                    assert_eq!(zone, owner as ZoneId);
                    engines[owner]
                        .flush_commit(result.expect("flush write succeeds"))
                        .unwrap();
                }
                _ => panic!("completion crossed maintenance lanes"),
            }
        }

        assert_eq!(
            maintenance_threads.lock().unwrap().len(),
            2,
            "eight owners use only the flush and compact worker I/O owners"
        );
        drop(handles);
        join.join();
    }

    #[test]
    fn one_owner_sibling_error_does_not_block_other_owners_and_retries() {
        const OWNERS: usize = 8;
        const FAILING: usize = 3;
        let maintenance_threads = Arc::new(Mutex::new(HashSet::new()));
        let mut engines: Vec<Engine<CountingMaintDevice>> = (0..OWNERS)
            .map(|owner| {
                counting_engine(
                    maintenance_threads.clone(),
                    usize::from(owner == FAILING),
                )
            })
            .collect();
        let (handles, mut join) = spawn_pool::<CountingMaintDevice>(OWNERS);

        for (owner, engine) in engines.iter_mut().enumerate() {
            engine.put(b"key", b"value", 1).unwrap();
            let plan = engine
                .flush_plan(RunBuildOpts::default())
                .unwrap()
                .expect("non-empty memtable plans a flush");
            handles[owner]
                .submit(MaintJob::Flush(owner as ZoneId, plan))
                .expect("flush lane alive");
        }
        for (owner, handle) in handles.iter().enumerate() {
            let done = handle
                .done
                .recv_timeout(Duration::from_secs(5))
                .expect("one owner's error does not stop completion routing");
            let MaintDone::Flushed(zone, result) = done else {
                panic!("unexpected maintenance lane")
            };
            assert_eq!(zone, owner as ZoneId);
            if owner == FAILING {
                let (segs, error) = match result {
                    Err(error) => error,
                    Ok(_) => panic!("selected owner must fail once"),
                };
                assert!(error.to_string().contains("injected maintenance sibling failure"));
                engines[owner].free_flush_segs(&segs);
            } else {
                engines[owner]
                    .flush_commit(result.expect("unaffected owner flush succeeds"))
                    .unwrap();
            }
        }

        let retry = engines[FAILING]
            .flush_plan(RunBuildOpts::default())
            .unwrap()
            .expect("failed owner remains retryable");
        handles[FAILING]
            .submit(MaintJob::Flush(FAILING as ZoneId, retry))
            .expect("flush lane remains alive");
        let done = handles[FAILING]
            .done
            .recv_timeout(Duration::from_secs(5))
            .expect("retry completes");
        let MaintDone::Flushed(zone, result) = done else {
            panic!("unexpected maintenance lane")
        };
        assert_eq!(zone, FAILING as ZoneId);
        engines[FAILING]
            .flush_commit(result.expect("retry succeeds"))
            .unwrap();

        assert_eq!(maintenance_threads.lock().unwrap().len(), 1);
        drop(handles);
        join.join();
    }

    fn two_run_merge_plan(
        engine: &mut Engine<CountingMaintDevice>,
    ) -> CompactPlan<CountingMaintDevice> {
        engine.put(b"key", b"v1", 1).unwrap();
        engine.flush(RunBuildOpts::default()).unwrap();
        engine.put(b"key", b"v2", 2).unwrap();
        engine.flush(RunBuildOpts::default()).unwrap();
        engine
            .compact_plan(RunBuildOpts::default(), None)
            .unwrap()
            .expect("two runs plan a merge")
    }

    fn merge_rendezvous(lanes: usize, patience: Duration) -> (usize, usize) {
        const OWNERS: usize = 2;
        let maintenance_threads = Arc::new(Mutex::new(HashSet::new()));
        let rendezvous = Rendezvous::new(OWNERS, patience);
        let mut engines: Vec<Engine<CountingMaintDevice>> = (0..OWNERS)
            .map(|_| gated_engine(maintenance_threads.clone(), 0, rendezvous.clone()))
            .collect();
        let (handles, mut join) = spawn_pool_lanes::<CountingMaintDevice>(OWNERS, lanes);
        for (owner, engine) in engines.iter_mut().enumerate() {
            let plan = two_run_merge_plan(engine);
            handles[owner]
                .submit(MaintJob::Compact(owner as ZoneId, plan))
                .expect("compact lane alive");
        }
        for (owner, handle) in handles.iter().enumerate() {
            let done = handle
                .done
                .recv_timeout(Duration::from_secs(30))
                .expect("every owner receives its compact completion");
            let MaintDone::Compacted(zone, _, result) = done else {
                panic!("completion crossed maintenance lanes")
            };
            assert_eq!(zone, owner as ZoneId);
            engines[owner]
                .compact_commit(result.expect("compact write succeeds"))
                .unwrap();
            assert_eq!(engines[owner].run_count(), 1);
            assert_eq!(
                engines[owner].get(b"key").unwrap().as_deref(),
                Some(&b"v2"[..])
            );
        }
        drop(handles);
        join.join();
        let threads = maintenance_threads.lock().unwrap().len();
        (threads, rendezvous.timeouts.load(AtomicOrdering::SeqCst))
    }

    #[test]
    fn two_compaction_lanes_run_merges_of_different_engines_concurrently() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (threads, timeouts) = merge_rendezvous(2, Duration::from_secs(10));
        assert_eq!(
            timeouts, 0,
            "both merges must be in flight at the same time"
        );
        assert_eq!(
            threads, 2,
            "each lane opens its maintenance qpair on its own thread"
        );
    }

    #[test]
    fn one_compaction_lane_serializes_merges() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        let (threads, timeouts) = merge_rendezvous(1, Duration::from_millis(200));
        assert_eq!(timeouts, 1, "the first merge waits alone on a single lane");
        assert_eq!(threads, 1);
    }

    #[test]
    fn compaction_lanes_drain_every_owner_and_shut_down() {
        if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2")]) { return; }
        const OWNERS: usize = 8;
        const ENGINES_PER_OWNER: usize = 3;
        const LANES: usize = 3;
        let maintenance_threads = Arc::new(Mutex::new(HashSet::new()));
        let mut engines: Vec<Vec<Engine<CountingMaintDevice>>> = (0..OWNERS)
            .map(|_| {
                (0..ENGINES_PER_OWNER)
                    .map(|_| counting_engine(maintenance_threads.clone(), 0))
                    .collect()
            })
            .collect();
        let (handles, mut join) = spawn_pool_lanes::<CountingMaintDevice>(OWNERS, LANES);
        for (owner, zones) in engines.iter_mut().enumerate() {
            for (zone, engine) in zones.iter_mut().enumerate() {
                let plan = two_run_merge_plan(engine);
                handles[owner]
                    .submit(MaintJob::Compact(zone as ZoneId, plan))
                    .expect("compact lane alive");
            }
        }
        for (owner, handle) in handles.iter().enumerate() {
            let mut seen = HashSet::new();
            for _ in 0..ENGINES_PER_OWNER {
                let done = handle
                    .done
                    .recv_timeout(Duration::from_secs(10))
                    .expect("every engine receives its compact completion");
                let MaintDone::Compacted(zone, _, result) = done else {
                    panic!("completion crossed maintenance lanes")
                };
                assert!(seen.insert(zone), "one completion per engine");
                engines[owner][zone as usize]
                    .compact_commit(result.expect("compact write succeeds"))
                    .unwrap();
            }
            assert!(handle.done.try_recv().is_err(), "no duplicate completions");
        }
        let threads = maintenance_threads.lock().unwrap().len();
        assert!(
            (1..=LANES).contains(&threads),
            "only compaction lanes open maintenance qpairs ({threads})"
        );
        drop(handles);
        join.join();
    }

}
