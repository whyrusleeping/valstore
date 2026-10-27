use super::*;

pub(super) fn spawn_shard<D, F>(
    listener: TcpListener,
    peers: Vec<(NodeId, SocketAddr)>,
    sweep_every: Duration,
    maint: crate::maint::MaintHandle<D>,
    build: F,
) -> ShardHandle
where
    D: BlockDevice + AsyncRead + Send + 'static,
    F: FnOnce() -> ServerShared<D> + Send + 'static,
{
    let peer_shards: Vec<(NodeId, Vec<SocketAddr>)> = peers.iter()
        .map(|(node, addr)| (*node, vec![*addr]))
        .collect();
    spawn_shard_at(
        listener,
        peers,
        0,
        sweep_every,
        maint,
        peer_shards,
        Vec::new(),
        build,
    )
}

fn bind_uds(port: u16) -> Option<std::os::unix::net::UnixListener> {
    let path = crate::net::uds_path_for_port(port)?;
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::remove_file(&path);
    match std::os::unix::net::UnixListener::bind(&path) {
        Ok(l) => {
            let _ = l.set_nonblocking(true);
            log::info!("shard on :{port} also serving UDS at {}", path.display());
            Some(l)
        }
        Err(e) => {
            log::warn!(
                "UDS bind {} failed ({e}); shard serves TCP only",
                path.display()
            );
            None
        }
    }
}

pub(super) fn spawn_shard_at<D, F>(
    listener: TcpListener,
    peers: Vec<(NodeId, SocketAddr)>,
    shard_index: usize,
    sweep_every: Duration,
    maint: crate::maint::MaintHandle<D>,
    peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
    own_shard_addrs: Vec<SocketAddr>,
    build: F,
) -> ShardHandle
where
    D: BlockDevice + AsyncRead + Send + 'static,
    F: FnOnce() -> ServerShared<D> + Send + 'static,
{
    let stop = Arc::new(crate::reactor::Notify::new());
    let stop_rx = stop.clone();
    let (ctl_tx, ctl_rx) = mpsc::channel::<NodeCtl>();
    let ctl_notify = Arc::new(crate::reactor::Notify::new());
    let ctl_notify_rx = ctl_notify.clone();
    let dump_threads: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>> =
        Arc::new(Mutex::new(Vec::new()));
    let dump_threads_loop = dump_threads.clone();
    let join = crate::observe::spawn_abort_on_panic("vals-shard", move || {
        let shared = Rc::new(build());
        shared.bind_device_owners();
        shared.arm_maint_reserve();
        shared.set_sweep_every(sweep_every);
        let addr = listener.local_addr().ok();
        let mut ex = LocalExecutor::new();
        if let Some(uds) = addr.and_then(|a| bind_uds(a.port())) {
            ex.spawn(accept_loop(
                crate::net::Listener::Unix(uds),
                shared.clone(),
                ex.spawner(),
            ));
        }
        ex.spawn(accept_loop(
            crate::net::Listener::Tcp(listener),
            shared.clone(),
            ex.spawner(),
        ));
        ex.spawn(periodic_loop(shared.clone(), sweep_every));
        ex.spawn(compact_loop(shared.clone(), shared.settings.compact_interval(sweep_every)));
        ex.spawn(snapshot_loop(
            shared.clone(),
            shared.settings.flush_interval(sweep_every),
            maint.submitter(),
        ));
        ex.spawn(rehome_copy_loop(shared.clone(), peer_shards.clone()));
        ex.spawn(logical_accounting_loop(shared.clone()));
        ex.spawn(sweep_loop(
            shared.clone(),
            peers,
            shard_index,
            peer_shards,
            own_shard_addrs,
            maint.submitter(),
        ));
        ex.spawn(maint_loop(shared.clone(), maint));
        ex.spawn(ctl_loop(
            shared.clone(),
            ctl_rx,
            ctl_notify_rx,
            dump_threads_loop,
        ));
        let (_, canceled) = ex.run_until_and_cancel(
            shutdown_loop(stop_rx, shared.clone(), addr),
            {
                let shared = shared.clone();
                move || server_poll(&shared)
            },
        );
        log::info!(
            "node {}: connection drain complete; canceled {canceled} unfinished task(s), \
             syncing engines",
            shared.node_id
        );
        for th in dump_threads.lock().unwrap().drain(..) {
            let _ = th.join();
        }
        shared.finalize_shutdown();
    });
    ShardHandle {
        stop,
        ctl: ctl_tx,
        ctl_notify,
        join,
    }
}

const SHUTDOWN_DRAIN_DEADLINE: Duration = Duration::from_secs(2);

pub(crate) async fn shutdown_loop<D: BlockDevice + AsyncRead + 'static>(
    stop: Arc<crate::reactor::Notify>,
    shared: Rc<ServerShared<D>>,
    addr: Option<SocketAddr>,
) {
    stop.notified().await;
    log::info!(
        "node {}: graceful shutdown requested; draining",
        shared.node_id
    );
    shared.begin_shutdown();
    if let Some(addr) = addr {
        let _ = std::net::TcpStream::connect(addr);
        if let Some(path) = crate::net::uds_path_for_port(addr.port()) {
            let _ = std::os::unix::net::UnixStream::connect(&path);
            let _ = std::fs::remove_file(&path);
        }
    }
    let deadline = physical_now_ms() + SHUTDOWN_DRAIN_DEADLINE.as_millis() as u64;
    if let Ok(tick) = Timer::every(Duration::from_millis(1)) {
        while shared.inflight() > 0 && physical_now_ms() < deadline {
            tick.tick().await;
        }
    }
    if shared.inflight() > 0 {
        log::warn!(
            "node {}: {} device read(s) still in flight at the shutdown drain \
             deadline; closing connections anyway",
            shared.node_id,
            shared.inflight()
        );
    }
    if shared.wake_conns() > 0 {
        if let Ok(tick) = Timer::every(Duration::from_millis(1)) {
            while shared.wake_conns() > 0 && physical_now_ms() < deadline {
                tick.tick().await;
            }
        }
        shared.force_close_conns();
        shared.wake_conns();
    }
}

async fn ctl_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    ctl_rx: mpsc::Receiver<NodeCtl>,
    ctl_notify: Arc<crate::reactor::Notify>,
    dump_threads: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
) {
    let stop = shared.add_shutdown_notify();
    loop {
        if let Either::Right(()) = vals_rt::race(ctl_notify.notified(), stop.notified()).await {
            return;
        }
        if shared.is_shutdown() {
            return;
        }
        for ctl in ctl_rx.try_iter() {
            match ctl {
                NodeCtl::Dump {
                    zone,
                    out,
                    skip_tombstones,
                    parallel,
                    rate_mib,
                    reply,
                } => {
                    start_dump(
                        &shared,
                        &dump_threads,
                        zone,
                        out,
                        skip_tombstones,
                        parallel,
                        rate_mib,
                        reply,
                    );
                }
                NodeCtl::ZoneStats { reply } => {
                    let _ = reply.send(
                        shared
                            .zone_segment_stats()
                            .into_iter()
                            .map(
                                |(zone_id, used_segments, free_segments, live_keys, resident_runs)| {
                                    ZoneSegmentStats {
                                        zone_id,
                                        used_segments,
                                        free_segments,
                                        live_keys,
                                        resident_runs,
                                    }
                                },
                            )
                            .collect(),
                    );
                }
                #[cfg(test)]
                NodeCtl::SweepCounts { reply } => {
                    let _ = reply.send((shared.sweep_pass_count(), shared.sweep_held_count()));
                }
                NodeCtl::SetSweepEvery(d) => {
                    let old = shared.sweep_every();
                    shared.set_sweep_every(d);
                    log::info!(
                        "node {}: sweep cadence retuned {old:?} -> {d:?} (config reload)",
                        shared.node_id
                    );
                }
                NodeCtl::SwapStore {
                    dev,
                    geom,
                    node,
                    prepared,
                    commit,
                    reply,
                } => {
                    let res = swap_store(&shared, dev, geom, node, &stop, prepared, commit).await;
                    let _ = reply.send(res);
                }
            }
        }
    }
}

const SWAP_QUIESCE_DEADLINE: Duration = Duration::from_secs(5);

async fn swap_store<D: BlockDevice + AsyncRead + 'static>(
    shared: &Rc<ServerShared<D>>,
    dev: Box<dyn std::any::Any + Send>,
    geom: crate::drive::PartitionGeom,
    node: NodeId,
    stop: &crate::reactor::Notify,
    prepared: mpsc::Sender<Result<(), String>>,
    commit: mpsc::Receiver<()>,
) -> Result<(), String> {
    if shared.is_shutdown() {
        return Err("node is shutting down".into());
    }
    let dev = *dev
        .downcast::<D>()
        .map_err(|_| "swap device type mismatch (drive set device type != served)".to_string())?;
    let store = shared
        .zone_store(DEFAULT_ZONE)
        .map_err(|e| format!("default zone unavailable: {e}"))?;
    let pause = shared.read_gate().pause();
    let Ok(tick) = Timer::every(Duration::from_millis(1)) else {
        drop(pause);
        return Err("no timerfd for the swap quiesce wait".into());
    };
    let deadline = physical_now_ms() + SWAP_QUIESCE_DEADLINE.as_millis() as u64;
    loop {
        let quiet = shared.inflight() == 0 && {
            let s = store.borrow();
            !s.flush_in_flight() && !s.compact_in_flight()
        };
        if quiet {
            break;
        }
        if shared.is_shutdown() {
            drop(pause);
            return Err("node is shutting down".into());
        }
        if physical_now_ms() >= deadline {
            drop(pause);
            return Err("timed out waiting for reads/maintenance to quiesce".into());
        }
        if let Either::Right(()) = vals_rt::race(tick.tick(), stop.notified()).await {
            drop(pause);
            return Err("node is shutting down".into());
        }
    }
    let new_store = match shared.prepare_default_zone_swap(dev, geom.config(), node) {
        Ok(s) => s,
        Err(e) => {
            drop(pause);
            let msg = format!("swap prepare failed: {e}");
            let _ = prepared.send(Err(msg.clone()));
            return Err(msg);
        }
    };
    if prepared.send(Ok(())).is_err() {
        drop(pause);
        return Err("swap prepared channel dropped (orchestrator gone?)".into());
    }
    match commit.recv() {
        Ok(()) => {
            shared.commit_default_zone_swap(new_store);
            drop(pause);
            Ok(())
        }
        Err(_) => {
            drop(new_store);
            drop(pause);
            Err("swap commit channel dropped (journal persist failed?)".into())
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn start_dump<D: BlockDevice + AsyncRead + 'static>(
    shared: &Rc<ServerShared<D>>,
    dump_threads: &Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
    zone: String,
    out: PathBuf,
    skip_tombstones: bool,
    parallel: usize,
    rate_mib: Option<u64>,
    reply: mpsc::Sender<Result<String, String>>,
) {
    if shared.is_shutdown() {
        let _ = reply.send(Err("node is shutting down".into()));
        return;
    }
    let numeric = zone.parse::<ZoneId>().ok();
    let zone_id = match numeric {
        Some(id) => id,
        None => match shared.zone_id_by_name(&zone) {
            Some(id) => id,
            None => {
                let _ = reply.send(Err(format!(
                    "zone {zone:?} not found (not a numeric id, and no catalog zone by that name)"
                )));
                return;
            }
        },
    };
    let store = match shared.zone_store(zone_id) {
        Ok(s) => s,
        Err(e) => {
            let _ = reply.send(Err(format!("zone {zone_id}: {e}")));
            return;
        }
    };
    let (dev, cfg) = {
        let s = store.borrow();
        (s.device_handle(), s.engine_config())
    };
    let name = match numeric {
        Some(_) => format!("zone-{zone_id}"),
        None => zone,
    };
    let reply_err = reply.clone();
    let th = std::thread::Builder::new()
        .name("vals-dump".into())
        .spawn(move || {
            let result = (|| -> Result<String, String> {
                let sib =
                    BlockDevice::sibling(&*dev).map_err(|e| format!("open dump handle: {e}"))?;
                let stats = crate::dump::emit_dump(
                    &sib,
                    &cfg,
                    &name,
                    &out,
                    skip_tombstones,
                    parallel,
                    rate_mib,
                )?;
                Ok(stats.summary(&name, &out, skip_tombstones, parallel, rate_mib))
            })();
            let _ = reply.send(result);
        });
    match th {
        Ok(handle) => {
            let mut guard = dump_threads.lock().unwrap();
            guard.retain(|h| !h.is_finished());
            guard.push(handle);
        }
        Err(e) => {
            let _ = reply_err.send(Err(format!("spawn dump thread: {e}")));
        }
    }
}

async fn periodic_loop<D: BlockDevice + 'static>(shared: Rc<ServerShared<D>>, period: Duration) {
    let mut period = period;
    let mut timer = match Timer::every(period) {
        Ok(t) => t,
        Err(e) => {
            log::error!(
                "node {}: timerfd creation failed ({e}); periodic anti-entropy sweep \
                 DISABLED — only membership/catalog kicks will drive the sweep",
                shared.node_id
            );
            return;
        }
    };
    let stop = shared.add_shutdown_notify();
    let mut last_sweep = shared.sweep_every();
    loop {
        if let Either::Right(()) = vals_rt::race(timer.tick(), stop.notified()).await {
            return;
        }
        if shared.is_shutdown() {
            return;
        }
        shared.sample_gauges();
        shared.signal_periodic();
        let cur = shared.sweep_every();
        if cur != last_sweep {
            last_sweep = cur;
            match Timer::every(cur) {
                Ok(t) => {
                    timer = t;
                    period = cur;
                    log::info!(
                        "node {}: anti-entropy sweep cadence now {period:?}",
                        shared.node_id
                    );
                }
                Err(e) => log::error!(
                    "node {}: re-arming sweep timer at {cur:?} failed ({e}); \
                     keeping {period:?}",
                    shared.node_id
                ),
            }
        }
    }
}

