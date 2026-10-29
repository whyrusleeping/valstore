use super::*;

pub(super) fn engine_space_rejection<D: BlockDevice>(engine: &vals_store::Engine<D>) -> Response {
    if engine.region_exhausted() {
        crate::observe::counters().region_exhausted_rejections.inc();
        Response::Err(
            "zone region exhausted: grow region_mib, enable re-home, or reduce load".to_string(),
        )
    } else {
        crate::observe::counters().busy_rejections.inc();
        Response::Busy
    }
}

pub(super) fn space_rejection<D: BlockDevice>(store: &RefCell<VersionedStore<D>>) -> Response {
    engine_space_rejection(store.borrow().engine())
}

pub(super) fn admission_shed_rejection<D: BlockDevice>(
    store: &RefCell<VersionedStore<D>>,
) -> Option<Response> {
    if !store.borrow_mut().engine_mut().admission_shed() {
        return None;
    }
    let c = crate::observe::counters();
    c.admit_shed_rejections.inc();
    c.busy_rejections.inc();
    Some(Response::Busy)
}

pub(super) async fn serve_apply<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    key: &[u8],
    value: Option<&[u8]>,
    version: crate::hlc::Version,
    wall_ms: u64,
    mode: ApplyMode,
) -> Response {
    let applied = crate::replica::apply_staged_with_retry(
        store,
        dev,
        gate,
        key,
        value,
        version,
        wall_ms,
        crate::replica::ApplyRetryMode::Unconditional(mode),
        || {
            #[cfg(test)]
            staged_apply_commit_hook();
        },
    )
    .await;
    match applied {
        Ok(Some(applied)) => Response::Apply(applied),
        Ok(None) => Response::Busy,
        Err(crate::replica::ApplyRetryError::Apply(e))
            if e.is_backpressure() || e.is_device_full() =>
        {
            space_rejection(store)
        }
        Err(crate::replica::ApplyRetryError::Apply(e))
        | Err(crate::replica::ApplyRetryError::Probe(e)) => Response::Err(format!("{e}")),
    }
}

#[cfg(test)]
thread_local! {
    static STAGED_BATCH_PROBE_HOOK: RefCell<Option<Box<dyn FnMut(u64, u64)>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_staged_batch_probe_hook(hook: Option<Box<dyn FnMut(u64, u64)>>) {
    STAGED_BATCH_PROBE_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
fn staged_batch_probe_hook(runs: u64, blocks: u64) {
    STAGED_BATCH_PROBE_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f(runs, blocks);
        }
    });
}

pub(super) async fn group_commit<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
) -> SyncResult {
    let waiter = Rc::new(SyncWaiter::new());
    shared
        .sync_groups
        .borrow_mut()
        .entry(zone_id)
        .or_default()
        .waiters
        .push(waiter.clone());
    loop {
        if let Some(result) = waiter.result.borrow_mut().take() {
            return result;
        }
        let lead = {
            let mut groups = shared.sync_groups.borrow_mut();
            let group = groups.entry(zone_id).or_default();
            !std::mem::replace(&mut group.running, true)
        };
        if lead {
            group_commit_round(shared, zone_id, store).await;
            continue;
        }
        std::future::poll_fn(|cx| {
            if waiter.result.borrow().is_some() {
                return Poll::Ready(());
            }
            let running = shared
                .sync_groups
                .borrow()
                .get(&zone_id)
                .is_some_and(|group| group.running);
            if !running {
                return Poll::Ready(());
            }
            *waiter.waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        })
        .await;
    }
}

async fn group_commit_round<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
) {
    let mut guard = SyncLeaderGuard {
        shared,
        zone_id,
        armed: true,
        batch: Vec::new(),
    };
    vals_rt::yield_now().await;
    guard.batch = std::mem::take(
        &mut shared
            .sync_groups
            .borrow_mut()
            .get_mut(&zone_id)
            .expect("active sync group")
            .waiters,
    );
    let started = std::time::Instant::now();
    let result = sync_store(shared, store).await;
    crate::observe::histograms()
        .sync_flush_us
        .observe(started.elapsed());
    crate::observe::counters().sync_commits.inc();
    let successors = {
        let mut groups = shared.sync_groups.borrow_mut();
        let group = groups.get_mut(&zone_id).expect("active sync group");
        group.running = false;
        group.commits += 1;
        group.waiters.clone()
    };
    guard.armed = false;
    for pending in std::mem::take(&mut guard.batch) {
        *pending.result.borrow_mut() = Some(result.clone());
        if let Some(waker) = pending.waker.borrow_mut().take() {
            waker.wake();
        }
    }
    for next in successors {
        if let Some(waker) = next.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

/// Node-wide group commit for shards whose stores share one device namespace.
/// `claimed` numbers flushes in claim order; a waiter whose writes completed
/// before it read `claimed == n` is durable once any flush numbered > n has
/// completed, whichever shard's queue pair carried it (an NVMe flush covers
/// every command the controller completed before the flush was submitted).
/// At most one flush per domain is in flight, so waiters that arrive during
/// it share the next one. A claim abandoned before it resolves (its owning
/// shard state dropped, or no asynchronous flush available) clears `inflight`
/// without advancing `completed`, so an unresolved waiter claims a fresh,
/// later-numbered flush instead of trusting it.
pub(super) struct FlushDomain {
    pub(super) claimed: std::sync::atomic::AtomicU64,
    pub(super) completed: std::sync::atomic::AtomicU64,
    pub(super) failed: std::sync::atomic::AtomicU64,
    pub(super) inflight: std::sync::atomic::AtomicBool,
}

impl FlushDomain {
    pub(super) fn get(key: usize) -> Arc<FlushDomain> {
        static DOMAINS: OnceLock<Mutex<HashMap<usize, Arc<FlushDomain>>>> = OnceLock::new();
        DOMAINS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| {
                Arc::new(FlushDomain {
                    claimed: 0.into(),
                    completed: 0.into(),
                    failed: 0.into(),
                    inflight: false.into(),
                })
            })
            .clone()
    }
}

pub(super) struct DomainFlush {
    domain: Option<Arc<FlushDomain>>,
    number: u64,
    handle: Option<vals_spdk::WriteHandle>,
    started: Option<std::time::Instant>,
}

impl DomainFlush {
    pub(super) fn publish_if_done(&mut self) -> bool {
        if self.handle.as_ref().is_some_and(|handle| !handle.is_done()) {
            return true;
        }
        self.publish();
        false
    }

    fn publish(&mut self) {
        use std::sync::atomic::Ordering::SeqCst;
        let Some(handle) = self.handle.take() else {
            return;
        };
        let Some(domain) = self.domain.as_ref() else {
            return;
        };
        let published = std::time::Instant::now();
        if let Some(started) = self.started.take() {
            crate::observe::counters().domain_flush_busy_us_total
                .add(published.duration_since(started).as_micros() as u64);
            if handle.is_done() {
                let h = crate::observe::histograms();
                h.domain_flush_latency_us.observe(published.duration_since(started));
                if let Some(completed) = handle.completed_at() {
                    h.domain_flush_device_us.observe(completed.saturating_duration_since(started));
                    h.domain_flush_publish_us.observe(published.saturating_duration_since(completed));
                }
            }
        }
        if handle.is_done() {
            if handle.into_result().is_ok() {
                domain.completed.fetch_max(self.number, SeqCst);
            } else {
                domain.failed.fetch_max(self.number, SeqCst);
            }
        }
        domain.inflight.store(false, SeqCst);
    }
}

impl Drop for DomainFlush {
    fn drop(&mut self) {
        self.publish();
    }
}

const DOMAIN_FLUSH_STALL: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(test)]
thread_local! {
    static DOMAIN_FLUSH_STALL_OVERRIDE: Cell<Option<std::time::Duration>> =
        const { Cell::new(None) };
}

#[cfg(test)]
pub(super) fn set_domain_flush_stall(stall: Option<std::time::Duration>) {
    DOMAIN_FLUSH_STALL_OVERRIDE.with(|s| s.set(stall));
}

#[cfg(test)]
fn domain_flush_stall() -> std::time::Duration {
    DOMAIN_FLUSH_STALL_OVERRIDE.with(|s| s.get()).unwrap_or(DOMAIN_FLUSH_STALL)
}

#[cfg(not(test))]
fn domain_flush_stall() -> std::time::Duration {
    DOMAIN_FLUSH_STALL
}

async fn domain_flush<D: BlockDevice>(
    shared: &ServerShared<D>,
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    key: usize,
) -> Option<SyncResult> {
    use std::sync::atomic::Ordering::SeqCst;
    let domain = FlushDomain::get(key);
    let need = domain.claimed.load(SeqCst) + 1;
    let started = std::time::Instant::now();
    loop {
        dev.poll_writes();
        shared.publish_domain_flushes();
        if domain.failed.load(SeqCst) >= need {
            return Some(SyncResult::Err("device flush failed".into()));
        }
        if domain.completed.load(SeqCst) >= need {
            return Some(SyncResult::Ok);
        }
        if !domain.inflight.load(SeqCst)
            && domain
                .inflight
                .compare_exchange(false, true, SeqCst, SeqCst)
                .is_ok()
        {
            if domain.failed.load(SeqCst) >= need || domain.completed.load(SeqCst) >= need {
                domain.inflight.store(false, SeqCst);
                continue;
            }
            let number = domain.claimed.fetch_add(1, SeqCst) + 1;
            let writes_inflight = shared.zones.borrow().values().map(|store| {
                let store = store.borrow();
                let engine = store.engine();
                if engine.device_handle().flush_domain() == Some(key) {
                    engine.vlog_inflight_writes() as u64
                } else {
                    0
                }
            }).sum();
            let flush_started = std::time::Instant::now();
            match dev.submit_flush() {
                Some(handle) => shared.domain_flushes.borrow_mut().push(DomainFlush {
                    domain: Some(domain.clone()),
                    number,
                    handle: Some(handle),
                    started: Some(flush_started),
                }),
                None => {
                    domain.inflight.store(false, SeqCst);
                    return None;
                }
            }
            crate::observe::counters().domain_flushes.inc();
            crate::observe::domain_flush_vlog_writes_inflight().observe(writes_inflight);
            continue;
        }
        if started.elapsed() > domain_flush_stall() {
            crate::observe::counters().domain_flush_stalls.inc();
            return Some(match store.borrow_mut().sync() {
                Ok(()) => SyncResult::Ok,
                Err(e) if e.is_backpressure() || e.is_device_full() => SyncResult::Space,
                Err(e) => SyncResult::Err(format!("{e}")),
            });
        }
        vals_rt::yield_now().await;
    }
}

struct DeviceWait<'a>(&'a Cell<usize>);

impl<'a> DeviceWait<'a> {
    fn new(waits: &'a Cell<usize>) -> Self {
        waits.set(waits.get() + 1);
        DeviceWait(waits)
    }
}

impl Drop for DeviceWait<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

struct FlushWait<'a, D: BlockDevice> {
    shared: &'a ServerShared<D>,
    handle: Option<vals_spdk::WriteHandle>,
}

impl<D: BlockDevice> Drop for FlushWait<'_, D> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take().filter(|handle| !handle.is_done()) {
            self.shared.domain_flushes.borrow_mut().push(DomainFlush {
                domain: None,
                number: 0,
                handle: Some(handle),
                started: None,
            });
        }
    }
}

async fn sync_store<D: BlockDevice>(
    shared: &ServerShared<D>,
    store: &RefCell<VersionedStore<D>>,
) -> SyncResult {
    fn classify(e: vals_store::EngineError) -> SyncResult {
        if e.is_backpressure() || e.is_device_full() {
            SyncResult::Space
        } else {
            SyncResult::Err(format!("{e}"))
        }
    }
    let dev = store.borrow().device_handle();
    if let Err(e) = store.borrow_mut().sync_prepare() {
        return classify(e);
    }
    if let Some(key) = dev.flush_domain() {
        let _wait = DeviceWait::new(&shared.device_waits);
        if let Some(result) = domain_flush(shared, store, dev.as_ref(), key).await {
            return result;
        }
    }
    let Some(flush) = dev.submit_flush() else {
        return match store.borrow_mut().sync() {
            Ok(()) => SyncResult::Ok,
            Err(e) => classify(e),
        };
    };
    let _wait = DeviceWait::new(&shared.device_waits);
    let mut flush = FlushWait {
        shared,
        handle: Some(flush),
    };
    while flush.handle.as_ref().is_some_and(|handle| !handle.is_done()) {
        dev.poll_writes();
        if flush.handle.as_ref().is_some_and(|handle| handle.is_done()) {
            break;
        }
        vals_rt::yield_now().await;
    }
    match flush.handle.take().expect("flush handle").into_result() {
        Ok(()) => SyncResult::Ok,
        Err(e) => SyncResult::Err(format!("device flush failed: {e}")),
    }
}

const BATCH_READ_CONCURRENCY: usize = 64;

pub(super) fn single_apply_record_bytes(applied: Applied, value_bytes: usize) -> Option<u64> {
    (applied == Applied::Won).then_some(value_bytes as u64)
}

pub(super) fn note_single_apply_written(applied: Applied, value_bytes: usize) {
    if let Some(value_bytes) = single_apply_record_bytes(applied, value_bytes) {
        let counters = crate::observe::counters();
        counters.records_written.inc();
        counters.record_bytes_written.add(value_bytes);
    }
}

pub(super) async fn serve_mirrored_batch_apply<D: BlockDevice + AsyncRead>(
    shared: Rc<ServerShared<D>>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    writes: &[crate::wire::OwnedWrite],
    sync: bool,
    mode: ApplyMode,
    floor: LiveFloor,
    fenced: Option<&FencedWrites>,
) -> Response {
    let response = serve_batch_apply_with(
        Some(shared.as_ref()),
        zone_id,
        store,
        dev,
        gate,
        writes,
        sync,
        mode,
        floor,
    )
    .await;
    mirror_batch_response(shared, zone_id, writes, response, sync, fenced).await
}

#[cfg(test)]
pub(super) async fn serve_batch_apply<D: BlockDevice + AsyncRead>(
    shared: Option<&ServerShared<D>>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    writes: &[crate::wire::OwnedWrite],
    sync: bool,
    mode: ApplyMode,
) -> Response {
    serve_batch_apply_with(shared, zone_id, store, dev, gate, writes, sync, mode, LiveFloor::Enforce).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn serve_batch_apply_with<D: BlockDevice + AsyncRead>(
    shared: Option<&ServerShared<D>>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    writes: &[crate::wire::OwnedWrite],
    sync: bool,
    mode: ApplyMode,
    floor: LiveFloor,
) -> Response {
    let started = std::time::Instant::now();
    let wall_ms = crate::hlc::physical_now_ms();
    let value_bytes: u64 = writes
        .iter()
        .map(|(_, v, _)| v.as_ref().map_or(0, |v| v.len() as u64))
        .sum();

    if mode == ApplyMode::Blind {
        let mut results = Vec::with_capacity(writes.len());
        {
            let mut s = store.borrow_mut();
            for (key, value, version) in writes {
                match s.apply_blind_with(key, value.as_deref(), *version, wall_ms, floor) {
                    Ok(a) => results.push(a),
                    Err(e) if e.is_backpressure() || e.is_device_full() => {
                        return engine_space_rejection(s.engine());
                    }
                    Err(e) => return Response::Err(format!("{e}")),
                }
            }
        }
        if sync {
            let synced = match shared {
                Some(shared) => group_commit(shared, zone_id, store).await,
                None => match store.borrow_mut().sync() {
                    Ok(()) => SyncResult::Ok,
                    Err(e) if e.is_backpressure() || e.is_device_full() => SyncResult::Space,
                    Err(e) => SyncResult::Err(format!("{e}")),
                },
            };
            match synced {
                SyncResult::Ok => {}
                SyncResult::Space => {
                    crate::observe::counters().blind_sync_errors.inc();
                    return space_rejection(store);
                }
                SyncResult::Err(e) => {
                    crate::observe::counters().blind_sync_errors.inc();
                    return Response::Err(e);
                }
            }
            crate::observe::counters()
                .synced_writes
                .add(writes.len() as u64);
            if let Some(shared) = shared { shared.record_synced_writes(writes.len() as u64); }
        }
        let c = crate::observe::counters();
        c.records_written.add(writes.len() as u64);
        c.record_bytes_written.add(value_bytes);
        crate::observe::histograms()
            .batch_apply_latency_us
            .observe(started.elapsed());
        return Response::Batch(results);
    }

    let mut results: Vec<Option<crate::replica::Applied>> = vec![None; writes.len()];
    let mut pending: Vec<usize> = (0..writes.len()).collect();
    let mut cold_reads: u64 = 0;

    struct Cold {
        idx: usize,
        gen: u64,
    }

    const MAX_ATTEMPTS: usize = 8;
    'attempts: for _ in 0..MAX_ATTEMPTS {
        if pending.is_empty() {
            break;
        }
        let _guard = gate.acquire().await;
        let mut cold: Vec<Cold> = Vec::new();
        let mut probes: Vec<vals_store::StagedVersion> = Vec::new();
        {
            let mut s = store.borrow_mut();
            for &idx in &pending {
                let (key, value, version) = &writes[idx];
                match s.apply_stage_with(key, value.as_deref(), *version, wall_ms, floor) {
                    Ok(StagedApply::Done(a)) => results[idx] = Some(a),
                    Ok(StagedApply::NeedsRunRead { probe, gen }) => {
                        cold.push(Cold { idx, gen });
                        probes.push(probe);
                    }
                    Err(e) if e.is_backpressure() || e.is_device_full() => {
                        return engine_space_rejection(s.engine());
                    }
                    Err(e) => return Response::Err(format!("{e}")),
                }
            }
        }
        if cold.is_empty() {
            pending.clear();
            break;
        }
        cold_reads += cold.len() as u64;
        let probe_runs: u64 = probes
            .iter()
            .map(|p| match p {
                vals_store::StagedVersion::Runs { probes, .. } => probes.len() as u64,
                _ => 0,
            })
            .sum();
        let probe_bytes: u64 = probes.iter().map(vals_store::StagedVersion::probe_bytes).sum();
        let mut probe_blocks = 0u64;
        let mut run_olds: Vec<Option<crate::hlc::Version>> = Vec::with_capacity(cold.len());
        let mut probes = probes.into_iter();
        let probe_timer = vals_store::attribution::Timer::new(&vals_store::attribution::APPLY_PROBE_WALL, None);
        loop {
            let window: Vec<vals_store::StagedVersion> =
                probes.by_ref().take(BATCH_READ_CONCURRENCY).collect();
            if window.is_empty() {
                break;
            }
            let results_win = {
                let futs: Vec<_> = window
                    .into_iter()
                    .map(|probe| VersionedStore::version_from_probe_counted(probe, dev))
                    .collect();
                vals_rt::join_all(futs).await
            };
            for r in results_win {
                match r {
                    Ok((v, blocks)) => {
                        probe_blocks += blocks;
                        run_olds.push(v);
                    }
                    Err(e) => return Response::Err(format!("{e}")),
                }
            }
        }
        drop(probe_timer);
        let c = crate::observe::counters();
        c.apply_probe_runs.add(probe_runs);
        c.apply_probe_blocks.add(probe_blocks);
        c.apply_probe_bytes.add(probe_bytes);
        #[cfg(test)]
        staged_batch_probe_hook(probe_runs, probe_blocks);
        let mut next_pending: Vec<usize> = Vec::new();
        #[cfg(test)]
        staged_batch_commit_hook();
        {
            let mut s = store.borrow_mut();
            let commit_generation = s.storage_generation();
            for (c, run_old) in cold.iter().zip(run_olds) {
                if c.gen != commit_generation {
                    next_pending.push(c.idx);
                    continue;
                }
                let (key, value, version) = &writes[c.idx];
                match s.apply_commit_staged_snapshot(
                    key,
                    value.as_deref(),
                    *version,
                    run_old,
                    wall_ms,
                ) {
                    Ok(a) => results[c.idx] = Some(a),
                    Err(e) if e.is_backpressure() || e.is_device_full() => {
                        return engine_space_rejection(s.engine());
                    }
                    Err(e) => return Response::Err(format!("{e}")),
                }
            }
        }
        pending = next_pending;
        if pending.is_empty() {
            break 'attempts;
        }
    }
    if !pending.is_empty() {
        return Response::Busy;
    }

    if sync {
        let synced = match shared {
            Some(shared) => group_commit(shared, zone_id, store).await,
            None => match store.borrow_mut().sync() {
                Ok(()) => SyncResult::Ok,
                Err(e) if e.is_backpressure() || e.is_device_full() => SyncResult::Space,
                Err(e) => SyncResult::Err(format!("{e}")),
            },
        };
        match synced {
            SyncResult::Ok => {}
            SyncResult::Space => return space_rejection(store),
            SyncResult::Err(e) => return Response::Err(e),
        }
        crate::observe::counters()
            .synced_writes
            .add(writes.len() as u64);
        if let Some(shared) = shared { shared.record_synced_writes(writes.len() as u64); }
    }
    let c = crate::observe::counters();
    c.records_written.add(writes.len() as u64);
    c.record_bytes_written.add(value_bytes);
    if cold_reads != 0 {
        c.batch_apply_run_reads.add(cold_reads);
    }
    crate::observe::histograms()
        .batch_apply_latency_us
        .observe(started.elapsed());
    let results = results
        .into_iter()
        .map(|r| r.expect("every batch entry resolved to a result"))
        .collect();
    Response::Batch(results)
}

