use super::*;
use crate::replica::seed_policy;
use crate::replica::RecountStep;

const RECOUNT_RAM_PAUSE: Duration = Duration::from_millis(100);
const SEED_SCAN_YIELD_MAX: Duration = Duration::from_millis(10);

fn seed_scan_priority() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let enabled = *ENABLED.get_or_init(|| crate::env_parse("VALS_SEED_SCAN_PRIORITY", 1u8) != 0);
    #[cfg(test)]
    if let Some(override_enabled) = SEED_SCAN_PRIORITY_OVERRIDE.with(|value| value.get()) {
        return override_enabled;
    }
    enabled
}

#[cfg(test)]
thread_local! {
    static SEED_SCAN_PRIORITY_OVERRIDE: Cell<Option<bool>> = const { Cell::new(None) };
    static SEED_SCAN_TEST_STEPS: Cell<u64> = const { Cell::new(0) };
    static SEED_SCAN_TEST_YIELDS: Cell<u64> = const { Cell::new(0) };
    static SEED_STEP_TEST_SPIN: Cell<Duration> = const { Cell::new(Duration::ZERO) };
    static SEED_DUTY_TEST_TOTALS: Cell<(Duration, Duration)> = const { Cell::new((Duration::ZERO, Duration::ZERO)) };
}

#[cfg(test)]
pub(super) fn set_seed_step_test_spin(spin: Duration) {
    SEED_STEP_TEST_SPIN.with(|value| value.set(spin));
    SEED_DUTY_TEST_TOTALS.with(|value| value.set((Duration::ZERO, Duration::ZERO)));
}

#[cfg(test)]
pub(super) fn seed_duty_test_totals() -> (Duration, Duration) {
    SEED_DUTY_TEST_TOTALS.with(Cell::get)
}

struct SeedDriver<D: BlockDevice>(Rc<ServerShared<D>>);

impl<D: BlockDevice> SeedDriver<D> {
    fn attach(shared: &Rc<ServerShared<D>>) -> Self {
        shared.attach_seed_driver(true);
        crate::observe::gauges().ae_seed_drivers.adjust(0, 1);
        SeedDriver(shared.clone())
    }
}

impl<D: BlockDevice> Drop for SeedDriver<D> {
    fn drop(&mut self) {
        if !self.0.is_shutdown() {
            self.0.attach_seed_driver(false);
        }
        crate::observe::gauges().ae_seed_drivers.adjust(1, 0);
    }
}

#[derive(Default)]
struct SeedDuty {
    signal: seed_policy::ForegroundSignal,
    budget: seed_policy::DutyBudget,
}

impl SeedDuty {
    fn observe<D: BlockDevice>(&mut self, shared: &ServerShared<D>) {
        self.signal.busy(
            std::time::Instant::now(),
            shared.foreground_ops(),
            shared.seed_foreground_inflight(),
        );
    }

    fn pause<D: BlockDevice>(
        &mut self,
        shared: &ServerShared<D>,
        cost: Duration,
        restarts: u32,
    ) -> Duration {
        crate::observe::counters().ae_seed_busy_us.add(cost.as_micros() as u64);
        let policy = seed_policy::policy();
        if !policy.isolation {
            return Duration::ZERO;
        }
        let now = std::time::Instant::now();
        let busy = self.signal.busy(
            now,
            shared.foreground_ops(),
            shared.seed_foreground_inflight(),
        );
        let pct = seed_policy::effective_duty_pct(policy.duty_pct, restarts);
        self.budget.charge(now, cost, busy, pct)
    }
}

async fn seed_duty_pause(pause: Duration, stop: &crate::reactor::Notify) {
    #[cfg(test)]
    SEED_DUTY_TEST_TOTALS.with(|value| {
        let (busy, paused) = value.get();
        value.set((busy, paused + pause));
    });
    if pause.is_zero() {
        vals_rt::yield_now().await;
        return;
    }
    let c = crate::observe::counters();
    c.ae_seed_duty_pauses.inc();
    c.ae_seed_duty_pause_us.add(pause.as_micros() as u64);
    match Timer::every(pause) {
        Ok(timer) => {
            vals_rt::race(timer.tick(), stop.notified()).await;
        }
        Err(_) => vals_rt::yield_now().await,
    }
}

#[cfg(test)]
pub(super) fn set_seed_scan_priority_override(enabled: Option<bool>) {
    SEED_SCAN_PRIORITY_OVERRIDE.with(|value| value.set(enabled));
}

#[cfg(test)]
pub(super) fn reset_seed_scan_test_counts() {
    SEED_SCAN_TEST_STEPS.with(|value| value.set(0));
    SEED_SCAN_TEST_YIELDS.with(|value| value.set(0));
}

#[cfg(test)]
pub(super) fn seed_scan_test_counts() -> (u64, u64) {
    (
        SEED_SCAN_TEST_STEPS.with(Cell::get),
        SEED_SCAN_TEST_YIELDS.with(Cell::get),
    )
}

const SEED_ERROR_WARN_EVERY: Duration = Duration::from_secs(60);
const AE_SYNC_ERROR_LOG_EVERY: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct LogEvery {
    last: Option<std::time::Instant>,
    suppressed: u64,
}

impl LogEvery {
    pub(super) fn note(&mut self, now: std::time::Instant, every: Duration) -> Option<u64> {
        if self.last.is_some_and(|last| now.saturating_duration_since(last) < every) {
            self.suppressed += 1;
            return None;
        }
        self.last = Some(now);
        Some(std::mem::take(&mut self.suppressed))
    }
}

struct LapTally {
    placement: crate::replica::CleanupPlacement,
    start_gen: u64,
    saw_copy_hold: bool,
    moved: bool,
    kept: u64,
    samples: Vec<Vec<u8>>,
}

#[derive(Debug)]
pub(super) struct CleanupLap {
    origin: Option<Vec<u8>>,
    lapped: bool,
    cursor: Option<Vec<u8>>,
}

impl CleanupLap {
    pub(super) fn starting_at(origin: Option<Vec<u8>>) -> Self {
        CleanupLap {
            lapped: origin.is_none(),
            cursor: origin.clone(),
            origin,
        }
    }

    pub(super) fn resume(&self) -> Option<&[u8]> {
        self.cursor.as_deref()
    }

    pub(super) fn end(&self) -> Option<&[u8]> {
        self.origin.as_deref().filter(|_| self.lapped)
    }

    pub(super) fn advance(&mut self, next_cursor: Option<Vec<u8>>, wrapped: bool) -> bool {
        match next_cursor {
            Some(cursor) => self.cursor = Some(cursor),
            None if wrapped && self.lapped => return true,
            None if wrapped => {
                self.lapped = true;
                self.cursor = None;
            }
            None => {}
        }
        false
    }
}

#[cfg(test)]
thread_local! {
    static CLEANUP_ORIGIN_OVERRIDE: std::cell::RefCell<Option<Option<Vec<u8>>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_cleanup_origin_override(origin: Option<Option<Vec<u8>>>) {
    CLEANUP_ORIGIN_OVERRIDE.with(|value| *value.borrow_mut() = origin);
}

fn cleanup_origin<D: BlockDevice>(
    store: &VersionedStore<D>,
    node_id: NodeId,
    shard_index: usize,
    zone_id: ZoneId,
) -> Option<Vec<u8>> {
    #[cfg(test)]
    if let Some(origin) = CLEANUP_ORIGIN_OVERRIDE.with(|value| value.borrow().clone()) {
        return origin;
    }
    store.engine().sample_block_key(cleanup_origin_pick(node_id, shard_index, zone_id))
}

fn cleanup_origin_pick(node_id: NodeId, shard_index: usize, zone_id: ZoneId) -> u64 {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    let mut buf = Vec::with_capacity(28);
    buf.extend_from_slice(&nanos.to_le_bytes());
    buf.extend_from_slice(&node_id.to_le_bytes());
    buf.extend_from_slice(&(shard_index as u64).to_le_bytes());
    buf.extend_from_slice(&zone_id.to_le_bytes());
    crate::ring::hash64(&buf)
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

pub(super) fn congruent_shard_addrs(
    addrs: &[SocketAddr],
    shard_index: usize,
    shard_count: usize,
) -> Vec<SocketAddr> {
    if addrs.is_empty() {
        return Vec::new();
    }
    let g = gcd(shard_count.max(1), addrs.len());
    addrs
        .iter()
        .enumerate()
        .filter(|(i, _)| i % g == shard_index % g)
        .map(|(_, &a)| a)
        .collect()
}

fn cleanup_ring_hash(ring: &Ring) -> u64 {
    let mut mw: Vec<(NodeId, u32)> = ring.members_weighted().collect();
    mw.sort_unstable();
    let mut buf = Vec::with_capacity(mw.len() * 8 + 4);
    buf.extend_from_slice(&(ring.rf() as u32).to_le_bytes());
    for (n, w) in mw {
        buf.extend_from_slice(&n.to_le_bytes());
        buf.extend_from_slice(&w.to_le_bytes());
    }
    crate::ring::hash64(&buf)
}

fn cleanup_zone_hash(zone_id: ZoneId, cfg: Option<&crate::zone::ZoneConfig>) -> u64 {
    if zone_id == DEFAULT_ZONE {
        return 0;
    }
    let Some(cfg) = cfg else {
        return u64::MAX;
    };
    let mut buf = Vec::with_capacity(10 + 4 * cfg.scope.as_ref().map_or(0, |s| s.len()));
    buf.extend_from_slice(&(cfg.rf as u64).to_le_bytes());
    buf.push(u8::from(cfg.ordering == crate::zone::Ordering::Sorted));
    match &cfg.scope {
        None => buf.push(0),
        Some(scope) => {
            buf.push(1);
            for n in scope {
                buf.extend_from_slice(&n.to_le_bytes());
            }
        }
    }
    crate::ring::hash64(&buf)
}

pub(super) fn cleanup_placement_base<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
) -> (u64, u64, u64) {
    let ring_hash = cleanup_ring_hash(&shared.ring().borrow());
    let cfg = if zone_id == DEFAULT_ZONE { None } else { shared.zone_config(zone_id) };
    (shared.ring_epoch(), cleanup_zone_hash(zone_id, cfg.as_ref()), ring_hash)
}

pub(super) fn cleanup_stamp<D: BlockDevice>(shared: &ServerShared<D>) -> (u64, u64, u64, u64) {
    let (ring_epoch, _, ring_hash) = cleanup_placement_base(shared, DEFAULT_ZONE);
    (ring_epoch, shared.catalog_epoch(), shared.range_map_epoch(), ring_hash)
}

pub(super) fn with_cleanup_placement<D: BlockDevice, R>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    f: impl FnOnce((u64, u64, u64), Option<&crate::partition::RangeMap>) -> R,
) -> Option<R> {
    let base = cleanup_placement_base(shared, zone_id);
    if zone_id == DEFAULT_ZONE {
        return Some(f(base, None));
    }
    if shared.zone_config(zone_id)?.ordering != crate::zone::Ordering::Sorted {
        return Some(f(base, None));
    }
    shared.with_sorted_range_map(zone_id, |map| f(base, Some(map)))
}

pub(super) async fn logical_accounting_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
) {
    let all_buckets = std::collections::HashSet::from([0]);
    let stop = shared.add_shutdown_notify();
    let _driver = SeedDriver::attach(&shared);
    let mut duty = SeedDuty::default();
    let mut last_seed_warn: std::collections::HashMap<ZoneId, std::time::Instant> =
        std::collections::HashMap::new();
    loop {
        shared.logical_accounting_notified().await;
        if shared.is_shutdown() {
            return;
        }
        for zone_id in shared.sweep_zone_ids(false) {
            if shared.is_shutdown() {
                return;
            }
            let Ok(store) = shared.zone_store(zone_id) else {
                continue;
            };
            'zone: loop {
                let mut last_seed_step = std::time::Instant::now() - SEED_SCAN_YIELD_MAX;
                loop {
                    if shared.is_shutdown() || !shared.zone_is_current(zone_id, &store) {
                        break 'zone;
                    }
                    if seed_scan_priority()
                        && shared.scans_inflight() > 0
                        && last_seed_step.elapsed() < SEED_SCAN_YIELD_MAX
                    {
                        crate::observe::counters().ae_seed_scan_yields.inc();
                        #[cfg(test)]
                        SEED_SCAN_TEST_YIELDS.with(|value| value.set(value.get() + 1));
                        vals_rt::yield_now().await;
                        continue;
                    }
                    #[cfg(test)]
                    SEED_SCAN_TEST_STEPS.with(|value| value.set(value.get() + 1));
                    duty.observe(&shared);
                    let started = std::time::Instant::now();
                    let step = { store.borrow().seed_step_cooperative() };
                    if store.borrow().engine().run_pins_reapable() {
                        if let Ok(mut s) = store.try_borrow_mut() {
                            s.reap_seed_pins();
                        }
                    }
                    #[cfg(test)]
                    {
                        let spin = SEED_STEP_TEST_SPIN.with(Cell::get);
                        while started.elapsed() < spin {
                            std::hint::spin_loop();
                        }
                    }
                    last_seed_step = std::time::Instant::now();
                    let cost = last_seed_step.saturating_duration_since(started);
                    #[cfg(test)]
                    SEED_DUTY_TEST_TOTALS.with(|value| {
                        let (busy, paused) = value.get();
                        value.set((busy + cost, paused));
                    });
                    let restarts = store.borrow().seed_restarts();
                    let pause = duty.pause(&shared, cost, restarts);
                    match step {
                        Ok(true) => break,
                        Ok(false) if store.borrow().seed_deferred() => break 'zone,
                        Ok(false) => seed_duty_pause(pause, &stop).await,
                        Err(e) => {
                            let now = std::time::Instant::now();
                            if last_seed_warn.get(&zone_id).is_none_or(|last| now.duration_since(*last) >= SEED_ERROR_WARN_EVERY) {
                                last_seed_warn.insert(zone_id, now);
                                log::warn!(
                                    "node {}: local logical accounting seed of zone {zone_id} failed and was reset (retried on a later pass): {e}",
                                    shared.node_id
                                );
                            }
                            break;
                        }
                    }
                }
                let mut reseed = false;
                loop {
                    if shared.is_shutdown() || !shared.zone_is_current(zone_id, &store) {
                        break;
                    }
                    duty.observe(&shared);
                    let started = std::time::Instant::now();
                    let resumed = Cell::new(None);
                    let step = crate::replica::rt_fold_turn_with(
                        &store,
                        Some(shared.read_gate()),
                        || resumed.set(Some(std::time::Instant::now())),
                    )
                    .await;
                    let cost = resumed.get().unwrap_or(started).elapsed();
                    let pause = duty.pause(&shared, cost, 0);
                    match step {
                        Ok(RecountStep::Done) => break,
                        Ok(RecountStep::More) => seed_duty_pause(pause, &stop).await,
                        Ok(RecountStep::Paused) => {
                            if let Ok(pause) = Timer::every(RECOUNT_RAM_PAUSE) {
                                vals_rt::race(pause.tick(), stop.notified()).await;
                            } else {
                                vals_rt::yield_now().await;
                            }
                        }
                        Ok(RecountStep::Abandoned) | Err(_) => {
                            if let Err(e) = step {
                                log::debug!(
                                    "node {}: range-tombstone fold of zone {zone_id} failed: {e}",
                                    shared.node_id
                                );
                            }
                            reseed = true;
                            break;
                        }
                    }
                }
                if reseed {
                    if shared.is_shutdown() {
                        break;
                    }
                    continue;
                }
                loop {
                    if shared.is_shutdown() || !shared.zone_is_current(zone_id, &store) {
                        break;
                    }
                    duty.observe(&shared);
                    let started = std::time::Instant::now();
                    let resumed = Cell::new(None);
                    let step = crate::replica::regroup_turn_with(
                        &store,
                        Some(shared.read_gate()),
                        || resumed.set(Some(std::time::Instant::now())),
                    )
                    .await;
                    let cost = resumed.get().unwrap_or(started).elapsed();
                    let pause = duty.pause(&shared, cost, 0);
                    match step {
                        Ok(RecountStep::Done) => break,
                        Ok(RecountStep::More) => seed_duty_pause(pause, &stop).await,
                        Ok(RecountStep::Paused) => {
                            if let Ok(pause) = Timer::every(RECOUNT_RAM_PAUSE) {
                                vals_rt::race(pause.tick(), stop.notified()).await;
                            } else {
                                vals_rt::yield_now().await;
                            }
                        }
                        Ok(RecountStep::Abandoned) => {
                            reseed = true;
                            break;
                        }
                        Err(e) => {
                            log::debug!(
                                "node {}: span regroup of zone {zone_id} failed: {e}",
                                shared.node_id
                            );
                            reseed = true;
                            break;
                        }
                    }
                }
                if reseed {
                    if shared.is_shutdown() {
                        break;
                    }
                    continue;
                }
                loop {
                    if shared.is_shutdown() || !shared.zone_is_current(zone_id, &store) {
                        break;
                    }
                    duty.observe(&shared);
                    let started = std::time::Instant::now();
                    let resumed = Cell::new(None);
                    let step = crate::replica::logical_recount_turn_with(
                        &store,
                        Some(shared.read_gate()),
                        || resumed.set(Some(std::time::Instant::now())),
                    )
                    .await;
                    let cost = resumed.get().unwrap_or(started).elapsed();
                    let pause = duty.pause(&shared, cost, 0);
                    match step {
                        Ok(RecountStep::Done) => break,
                        Ok(RecountStep::More) => seed_duty_pause(pause, &stop).await,
                        Ok(RecountStep::Paused) => {
                            if let Ok(pause) = Timer::every(RECOUNT_RAM_PAUSE) {
                                vals_rt::race(pause.tick(), stop.notified()).await;
                            } else {
                                vals_rt::yield_now().await;
                            }
                        }
                        Ok(RecountStep::Abandoned) => {
                            reseed = true;
                            break;
                        }
                        Err(e) => {
                            log::debug!(
                                "node {}: split recount of zone {zone_id} failed: {e}",
                                shared.node_id
                            );
                            reseed = true;
                            break;
                        }
                    }
                }
                if !reseed || shared.is_shutdown() {
                    break;
                }
            }
            if shared.is_shutdown() {
                return;
            }
            if !shared.zone_is_current(zone_id, &store) || !store.borrow().is_leaf_seeded() {
                continue;
            }
            match crate::replica::ensure_tree_ready_cooperative_cell(
                &store,
                1,
                &all_buckets,
            )
            .await
            {
                Ok(true) => {}
                Ok(false) => log::debug!(
                    "node {}: local logical accounting rebuild of zone {zone_id} was deferred",
                    shared.node_id
                ),
                Err(e) => log::debug!(
                    "node {}: local logical accounting rebuild of zone {zone_id} failed: {e}",
                    shared.node_id
                ),
            }
        }
    }
}

pub(super) async fn sweep_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    peers: Vec<(NodeId, SocketAddr)>,
    shard_index: usize,
    peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
    own_shard_addrs: Vec<SocketAddr>,
    maint: crate::maint::MaintSubmit<D>,
) {
    let node_id = shared.node_id;
    let (_, shard_count) = shared.shard();
    let reconcile_nodes: Vec<(NodeId, Vec<SocketAddr>)> = if peer_shards.is_empty() {
        peers.iter().map(|&(n, a)| (n, vec![a])).collect()
    } else {
        peer_shards
            .iter()
            .filter_map(|(n, addrs)| {
                let sub = congruent_shard_addrs(addrs, shard_index, shard_count);
                (!sub.is_empty()).then_some((*n, sub))
            })
            .collect()
    };
    let transport =
        NetTransport::connect_sharded(reconcile_nodes).with_rpc_deadline(NODE_PEER_RPC_DEADLINE);
    let handoff = {
        let mut nodes: Vec<(NodeId, Vec<SocketAddr>)> = if shard_count > 1 {
            peer_shards.clone()
        } else {
            peers.iter().map(|&(n, a)| (n, vec![a])).collect()
        };
        if shard_count > 1 && !own_shard_addrs.is_empty() {
            nodes.push((node_id, own_shard_addrs.clone()));
        }
        NetTransport::connect_sharded(nodes).with_rpc_deadline(NODE_PEER_RPC_DEADLINE)
    };
    shared.load_persisted_watermark();
    shared.kick_sweep();
    let cleanup_quiesce_timeouts = Cell::new(0u32);
    let cleanup_quiesce_next_attempt_ms = Cell::new(0u64);
    let mut ae_cursors: std::collections::HashMap<(ZoneId, NodeId, usize), Vec<u8>> =
        std::collections::HashMap::new();
    let mut cleanup_laps: std::collections::HashMap<ZoneId, CleanupLap> =
        std::collections::HashMap::new();
    let mut lap_tallies: std::collections::HashMap<ZoneId, LapTally> =
        std::collections::HashMap::new();
    let mut ae_error_log = LogEvery::default();
    let mut cleanup_dirty: std::collections::HashSet<ZoneId> =
        std::collections::HashSet::new();
    let mut cleanup_done: std::collections::HashSet<ZoneId> =
        std::collections::HashSet::new();
    let mut cleanup_placements: std::collections::HashMap<ZoneId, crate::replica::CleanupPlacement> =
        std::collections::HashMap::new();
    let mut last_cleanup_stamp: Option<(u64, u64, u64, u64)> = None;
    let mut last_recheck_gen = 0u64;
    loop {
        shared.sweep_notified().await;
        if shared.is_shutdown() {
            return;
        }
        let eager = shared.take_rebalance_pending();
        shared.note_sweep_pass();
        let recheck_gen = shared.cleanup_recheck_gen();
        if recheck_gen != last_recheck_gen {
            cleanup_done.clear();
            last_recheck_gen = recheck_gen;
        }
        let stamp = cleanup_stamp(&shared);
        if last_cleanup_stamp != Some(stamp) {
            cleanup_done.clear();
            cleanup_dirty.extend(cleanup_laps.keys().copied());
            last_cleanup_stamp = Some(stamp);
        }
        let min_confirms = shared.settings.cleanup_min_confirms.unwrap_or(usize::MAX);
        let snapshot = shared.ring().borrow().clone();
        crate::coord::Transport::set_route_epoch(&transport, shared.ring_epoch());
        for n in snapshot.members().filter(|&n| n != node_id) {
            let full = shared.peer_addrs_of(n).or_else(|| {
                peer_shards
                    .iter()
                    .find(|(p, _)| *p == n)
                    .map(|(_, a)| a.clone())
            });
            match full.as_deref() {
                Some(addrs) => {
                    let sub = congruent_shard_addrs(addrs, shard_index, shard_count);
                    if sub.is_empty() {
                        log::debug!(
                            "node {node_id}: member {n} has no known address yet; \
                             sweep can't reach it this pass"
                        );
                    } else {
                        transport.add_peer(n, &sub);
                    }
                }
                None => log::debug!(
                    "node {node_id}: member {n} has no known address yet; \
                     sweep can't reach it this pass"
                ),
            }
        }
        let netpeers: Vec<NodeId> = snapshot.members().filter(|&n| n != node_id).collect();

        if shared.liveness_grace_ms().is_some() {
            let now = physical_now_ms();
            let mut peer_wms = Vec::new();
            for peer_node in &netpeers {
                let wm = if transport.has_peer(*peer_node) {
                    transport.watermark(*peer_node).await
                } else {
                    continue;
                };
                if let Ok(wm) = wm {
                    peer_wms.push(wm);
                }
            }
            let contact_possible = !peer_wms.is_empty()
                || (netpeers.is_empty() && shared.lone_unfence_allowed());
            let wipe_wanted = shared.liveness_wipe_wanted(contact_possible, &peer_wms);
            if contact_possible {
                if wipe_wanted {
                    shared.check_wipe_timeout(std::time::Instant::now());
                    if shared.inflight() != 0 {
                        continue;
                    }
                    if let Err(e) = shared.wipe_all_zones(now, &peer_wms) {
                        log::error!(
                            "node {node_id}: liveness wipe failed ({e}); staying fenced \
                             and retrying next sweep"
                        );
                        continue;
                    }
                    shared.begin_resync();
                }
                if shared.resync_pending() {
                    shared.record_contact_fenced(now);
                } else {
                    shared.record_contact(now);
                }
            } else if netpeers.is_empty() {
                log::debug!(
                    "node {node_id}: holding born-fence — membership not yet adopted \
                     and a prior watermark exists; waiting for a peer or a 1-member push"
                );
            } else {
                shared.refence_if_stale(now);
            }
        } else {
            shared.clear_wipe_pending();
        }

        let cleanup_ring = shared.ring().borrow().clone();
        let cleanup_ring_epoch = shared.ring_epoch();
        let cleanup_ring_hash = cleanup_ring_hash(&cleanup_ring);
        crate::coord::Transport::set_route_epoch(&handoff, cleanup_ring_epoch);
        crate::coord::Transport::set_sorted_ranges(&handoff, &shared.sorted_routes());
        for n in cleanup_ring.members().filter(|&n| n != node_id) {
            let full = shared.peer_addrs_of(n).or_else(|| {
                peer_shards
                    .iter()
                    .find(|(p, _)| *p == n)
                    .map(|(_, a)| a.clone())
            });
            if let Some(addrs) = full {
                handoff.add_peer(n, &addrs);
            }
        }
        let mut prepared_zones = Vec::new();
        let mut placement_held = false;
        for zone_id in shared.sweep_zone_ids(eager) {
            let Ok(store) = shared.zone_store(zone_id) else {
                continue;
            };
            let (cfg, range_map) = if zone_id == DEFAULT_ZONE {
                (None, None)
            } else if let Some(cfg) = shared.zone_config(zone_id) {
                if cfg.ordering != crate::zone::Ordering::Sorted {
                    (Some(cfg), None)
                } else if let Some(map) = shared.sorted_range_map(zone_id) {
                    (Some(cfg), Some(Arc::new(map)))
                } else {
                    shared.note_sweep_sorted_zone_held();
                    placement_held = true;
                    continue;
                }
            } else {
                continue;
            };
            {
                let s = store.borrow();
                s.prepare_zone_seed(
                    (shared.ring_epoch(), shared.catalog_epoch(), shared.range_map_epoch()),
                    &snapshot,
                    zone_id,
                    cfg.clone(),
                    range_map.as_deref().cloned(),
                );
                if s.logical_recount_pending() || !s.is_leaf_seeded() || s.regroup_pending() || s.rt_fold_pending() {
                    shared.kick_logical_accounting();
                }
            }
            let placement = crate::replica::CleanupPlacement {
                base: (cleanup_ring_epoch, cleanup_zone_hash(zone_id, cfg.as_ref()), cleanup_ring_hash),
                map: range_map.clone(),
            };
            let moved = cleanup_placements
                .get(&zone_id)
                .is_none_or(|last| !last.unchanged_at(placement.base, placement.map.as_deref()));
            if moved {
                cleanup_done.remove(&zone_id);
                if cleanup_laps.contains_key(&zone_id) {
                    cleanup_dirty.insert(zone_id);
                }
                if let Some(tally) = lap_tallies.get_mut(&zone_id) {
                    tally.moved = true;
                }
            }
            cleanup_placements.insert(zone_id, placement.clone());
            shared.note_cleanup_placement(zone_id, moved, (stamp.0, stamp.1, stamp.2));
            store.borrow_mut().set_cleanup_placement(placement.clone());
            prepared_zones.push((zone_id, cfg, range_map, placement));
        }
        if eager {
            let all_buckets = std::collections::HashSet::from([0]);
            for (zone_id, _, _, _) in &prepared_zones {
                let Ok(store) = shared.zone_store(*zone_id) else {
                    continue;
                };
                match crate::replica::ensure_tree_ready_cooperative_cell(
                    &store,
                    1,
                    &all_buckets,
                )
                .await
                {
                    Ok(true) => {}
                    Ok(false) => log::debug!(
                        "node {node_id}: eager local accounting rebuild of zone {zone_id} was deferred"
                    ),
                    Err(e) => log::debug!(
                        "node {node_id}: eager local accounting rebuild of zone {zone_id} failed: {e}"
                    ),
                }
                if !shared.zone_is_current(*zone_id, &store) {
                    continue;
                }
            }
        }

        let mut refill_ok = !placement_held;
        let burst_budget = Duration::from_millis(shared.settings.cleanup_idle_burst_ms)
            .min(shared.sweep_every() / 2);
        let mut burst_spent = Duration::ZERO;
        for (zone_id, cfg, range_map, placement) in prepared_zones {
            if shared.is_shutdown() {
                return;
            }
            let Ok(store) = shared.zone_store(zone_id) else {
                continue;
            };
            let mut zone_pulled = false;
            let mut rt_synced = true;
            let mut rt_rejected = false;
            if let (Some(cfg), Some(map)) = (&cfg, &range_map) {
                rt_synced = netpeers.is_empty();
                for peer_node in &netpeers {
                    if !shared.zone_is_current(zone_id, &store) {
                        break;
                    }
                    let rt_scope = crate::rt_sync::RtScope::new(
                        map,
                        &snapshot,
                        zone_id,
                        cfg.rf,
                        cfg.scope.as_ref(),
                        node_id,
                        Some(*peer_node),
                        shared.shard(),
                    );
                    for i in 0..transport.peer_shard_count(*peer_node) {
                        if shared.is_shutdown() {
                            return;
                        }
                        if !shared.zone_is_current(zone_id, &store) {
                            break;
                        }
                        let peer = transport.peer_shard(*peer_node, i);
                        match crate::rt_sync::exchange(&store, &peer, &rt_scope).await {
                            Ok(crate::rt_sync::RtExchange::Unsupported) => rt_synced = true,
                            Ok(crate::rt_sync::RtExchange::Same) => {
                                rt_synced = true;
                                crate::observe::note_rt_exchange(crate::observe::RtExchangeResult::Same);
                            }
                            Ok(crate::rt_sync::RtExchange::Pulled { fragments, rejected }) => {
                                rt_synced = true;
                                rt_rejected |= rejected > 0;
                                crate::observe::note_rt_exchange(crate::observe::RtExchangeResult::Pulled);
                                if rejected > 0 {
                                    log::debug!(
                                        "node {node_id}: {rejected} of {} range-tombstone fragments of zone \
                                         {zone_id} from node {peer_node} shard {i} were beyond the clock-skew \
                                         bound; retried next pass",
                                        fragments + rejected
                                    );
                                }
                            }
                            Err(e) => {
                                crate::observe::note_rt_exchange(crate::observe::RtExchangeResult::Error);
                                log::debug!(
                                    "node {node_id}: range-tombstone exchange of zone {zone_id} with node \
                                     {peer_node} shard {i} failed (retrying next pass): {e}"
                                );
                            }
                        }
                    }
                }
                if store.borrow().rt_fold_pending() {
                    shared.kick_logical_accounting();
                }
            }
            let seed_ready = !store.borrow().seed_not_ready_for_caller()
                && !store.borrow().regroup_not_ready_for_caller()
                && !store.borrow().rt_fold_not_ready_for_caller();
            if !seed_ready && !netpeers.is_empty() {
                crate::observe::counters().ae_seed_reconcile_skips.inc();
            }
            for peer_node in &netpeers {
                if !seed_ready {
                    break;
                }
                if *peer_node == node_id {
                    continue;
                }
                if !shared.zone_is_current(zone_id, &store) {
                    break;
                }
                let peer_shard_count = transport.peer_shard_count(*peer_node);
                for i in 0..peer_shard_count {
                    if shared.is_shutdown() {
                        return;
                    }
                    if !shared.zone_is_current(zone_id, &store) {
                        break;
                    }
                    let peer = transport.peer_shard(*peer_node, i);
                    let resume = ae_cursors.get(&(zone_id, *peer_node, i)).map(Vec::as_slice);
                    let synced = match &cfg {
                        None => {
                            reconcile_scoped(
                                &store,
                                &peer,
                                DEFAULT_BUCKETS,
                                &snapshot,
                                node_id,
                                *peer_node,
                                shared.shard(),
                                resume,
                                Some(shared.read_gate()),
                            )
                            .await
                        }
                        Some(cfg) => {
                            reconcile_scoped_zone(
                                &store,
                                &peer,
                                DEFAULT_BUCKETS,
                                &snapshot,
                                node_id,
                                *peer_node,
                                zone_id,
                                cfg.rf,
                                cfg.scope.as_ref(),
                                range_map.as_deref(),
                                shared.shard(),
                                resume,
                                Some(shared.read_gate()),
                            )
                            .await
                        }
                    };
                    match synced {
                        Ok((applied, next_cursor)) => {
                            zone_pulled = true;
                            if applied > 0 {
                                shared.invalidate_zone_write_marks(zone_id);
                            }
                            match next_cursor {
                                Some(c) => {
                                    ae_cursors.insert((zone_id, *peer_node, i), c);
                                }
                                None => {
                                    ae_cursors.remove(&(zone_id, *peer_node, i));
                                }
                            }
                        }
                        Err(e) => {
                            crate::observe::counters().ae_sync_errors.inc();
                            match ae_error_log.note(std::time::Instant::now(), AE_SYNC_ERROR_LOG_EVERY) {
                                Some(suppressed) => log::info!(
                                    "node {node_id}: anti-entropy reconcile of zone {zone_id} \
                                     against node {peer_node} shard {i} failed; retried next \
                                     pass from the same cursor: {e} ({suppressed} more \
                                     reconcile failures on this shard since the last report)"
                                ),
                                None => log::debug!(
                                    "node {node_id}: anti-entropy reconcile of zone {zone_id} \
                                     against node {peer_node} shard {i} failed (retrying next \
                                     pass): {e}"
                                ),
                            }
                        }
                    }
                }
            }
            if (!zone_pulled || !rt_synced || rt_rejected) && !netpeers.is_empty() {
                refill_ok = false;
            }
            if cleanup_done.contains(&zone_id) {
                continue;
            }
            if shared.is_shutdown() {
                return;
            }
            if !shared.zone_is_current(zone_id, &store) {
                continue;
            }
            let lap = cleanup_laps.entry(zone_id).or_insert_with(|| {
                CleanupLap::starting_at(cleanup_origin(&store.borrow(), node_id, shard_index, zone_id))
            });
            let tally = lap_tallies
                .entry(zone_id)
                .or_insert_with(|| LapTally {
                    placement: placement.clone(),
                    start_gen: shared.cleanup_recheck_gen(),
                    saw_copy_hold: false,
                    moved: false,
                    kept: 0,
                    samples: Vec::new(),
                });
            let burst_left = burst_budget.saturating_sub(burst_spent);
            let burst_started = std::time::Instant::now();
            let burst_ops = shared.foreground_ops();
            let mut lap_done = false;
            loop {
                tally.saw_copy_hold |= shared.zone_copy_hold_active(zone_id, physical_now_ms());
                let gate = QuiesceGate {
                    shared: &shared,
                    consecutive_timeouts: &cleanup_quiesce_timeouts,
                    next_attempt_ms: &cleanup_quiesce_next_attempt_ms,
                };
                let placement_valid = || {
                    with_cleanup_placement(&shared, zone_id, |base, map| placement.unchanged_at(base, map))
                        .unwrap_or(false)
                };
                let cleanup_resume = lap.resume();
                let cleanup_end = lap.end();
                let cleaned = match &cfg {
                    None => {
                        cleanup_scoped(
                            &store,
                            &handoff,
                            &cleanup_ring,
                            node_id,
                            min_confirms,
                            shared.shard(),
                            cleanup_resume,
                            cleanup_end,
                            gate,
                            placement_valid,
                            || {
                                shared.is_shutdown()
                                    || !shared.zone_is_current(zone_id, &store)
                            },
                        )
                        .await
                    }
                    Some(cfg) => {
                        let hold_now_ms = physical_now_ms();
                        cleanup_scoped_zone(
                            &store,
                            &handoff,
                            &cleanup_ring,
                            node_id,
                            zone_id,
                            cfg.rf,
                            cfg.scope.as_ref(),
                            min_confirms,
                            range_map.as_deref(),
                            shared.shard(),
                            cleanup_resume,
                            cleanup_end,
                            gate,
                            |key| {
                                range_map.as_ref().is_some_and(|map| {
                                    let index = map.range_index(key);
                                    let lower = map.range_bounds(index).0.unwrap_or_default();
                                    shared.rehome_copy_hold_active(zone_id, lower, hold_now_ms)
                                })
                            },
                            placement_valid,
                            || {
                                shared.is_shutdown()
                                    || !shared.zone_is_current(zone_id, &store)
                            },
                        )
                        .await
                    }
                };
                if !shared.zone_is_current(zone_id, &store) {
                    break;
                }
                let mut burst_more = false;
                tally.saw_copy_hold |= shared.zone_copy_hold_active(zone_id, physical_now_ms());
                match cleaned {
                    Ok(mut progress) => {
                        crate::observe::counters()
                            .cleanup_scanned_keys
                            .add(progress.scanned as u64);
                        crate::observe::counters()
                            .cleanup_probe_released_keys
                            .add(progress.probe_released as u64);
                        if progress.released > 0 {
                            shared.bump_zone_forget_gen(zone_id);
                        }
                        if progress.kept > 0 {
                            tally.kept += progress.kept as u64;
                            for key in progress.kept_samples.drain(..) {
                                if tally.samples.len() < crate::antientropy::CLEANUP_KEPT_SAMPLES {
                                    tally.samples.push(key);
                                }
                            }
                            shared.note_cleanup_kept(zone_id, tally.kept, &tally.samples);
                        }
                        if shared.maintenance_paused_for_wipe() {
                            if let Some(plan) = progress.forget_plan.take() {
                                store.borrow_mut().abort_forget_plan(&plan.segments());
                                progress.deferred = true;
                            }
                        }
                        let mut plan_submitted = false;
                        if let Some(plan) = progress.forget_plan.take() {
                            let reservation = crate::ram_broker::reserve_overdraw(
                                crate::ram_broker::Class::Merge, plan.working_set_bytes());
                            match maint.submit(crate::maint::MaintJob::CleanupCompact(
                                zone_id,
                                stamp,
                                plan,
                            )) {
                                Ok(()) => {
                                    shared.begin_reserved_maint_job(zone_id, reservation);
                                    plan_submitted = true;
                                }
                                Err(e) => {
                                    let segs = match e.0 {
                                        crate::maint::MaintJob::CleanupCompact(_, _, p) => p.segments(),
                                        _ => Vec::new(),
                                    };
                                    store.borrow_mut().abort_forget_plan(&segs);
                                    crate::observe::counters().cleanup_errors.inc();
                                    progress.deferred = true;
                                }
                            }
                        }
                        if progress.deferred {
                            cleanup_dirty.insert(zone_id);
                        }
                        if lap.advance(progress.next_cursor, progress.wrapped) {
                            crate::observe::counters().cleanup_traversals.inc();
                            lap_done = true;
                            let held = !tally.saw_copy_hold
                                && !tally.moved
                                && with_cleanup_placement(&shared, zone_id, |base, map| {
                                    tally.placement.unchanged_at(base, map)
                                })
                                .unwrap_or(false);
                            shared.note_cleanup_lap(
                                zone_id,
                                held.then_some(tally.start_gen),
                                (stamp.0, stamp.1, stamp.2),
                                tally.kept,
                                std::mem::take(&mut tally.samples),
                                store.borrow().cleanup_forget_pending(),
                            );
                            let rechecked = tally.start_gen >= shared.cleanup_recheck_gen();
                            if !cleanup_dirty.remove(&zone_id) && rechecked && !tally.saw_copy_hold {
                                cleanup_done.insert(zone_id);
                            }
                        } else {
                            burst_more = !progress.deferred && !plan_submitted;
                        }
                    }
                    Err(e) => {
                        log::debug!(
                            "node {node_id}: over-replication cleanup of zone {zone_id} failed \
                             (retrying next pass near its cursor): {e}"
                        );
                        crate::observe::counters().cleanup_errors.inc();
                        cleanup_dirty.insert(zone_id);
                    }
                }
                if !burst_more
                    || burst_started.elapsed() >= burst_left
                    || shared.foreground_ops() != burst_ops
                    || shared.seed_foreground_inflight()
                    || shared.is_shutdown()
                    || !shared.zone_is_current(zone_id, &store)
                {
                    break;
                }
                crate::observe::counters().cleanup_idle_burst_pages.inc();
            }
            burst_spent += burst_started.elapsed();
            if lap_done {
                cleanup_laps.remove(&zone_id);
                lap_tallies.remove(&zone_id);
            }
        }
        if shared.resync_pending() && refill_ok {
            shared.finish_resync(physical_now_ms());
        }
        if shared.is_shutdown() {
            return;
        }
    }
}

