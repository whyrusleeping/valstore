use super::*;

#[derive(Clone)]
pub(super) struct PdMirrors {
    pub(super) members: Arc<Mutex<AddressedMembers>>,
    pub(super) zones: Arc<Mutex<Catalog>>,
    pub(super) range_maps: Arc<Mutex<HashMap<ZoneId, RangeMap>>>,
}

impl PdMirrors {
    pub(super) fn new() -> Self {
        PdMirrors {
            members: Arc::new(Mutex::new(Vec::new())),
            zones: Arc::new(Mutex::new(Catalog::new())),
            range_maps: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

/// A running PD (placement driver) replica: the Raft-replicated control
/// plane that owns membership, the zone catalog and sorted-zone range maps,
/// and pushes them to data nodes.
///
/// Start one with `spawn_single_durable` (one PD, no PD fault tolerance) or
/// `spawn_durable` (one voter of a fixed multi-PD group); the non-durable
/// `spawn`/`spawn_single` keep Raft state in memory and are for tests.
/// Growing a single PD into a group in place is not supported. Membership
/// and zone methods propose a change and return once the leader accepted
/// the proposal, not when it is committed or pushed: followers answer
/// `NotLeader`, and a new leader may answer `NotReady`. Watch
/// [`members`](Self::members), [`catalog`](Self::catalog) or
/// [`range_map`](Self::range_map) for the committed state. Call
/// [`shutdown`](Self::shutdown) before the process exits.
pub struct PdDaemon {
    pub(super) cmd: mpsc::Sender<PdCmd>,
    pub(super) admin_timeout_ms: Arc<std::sync::atomic::AtomicU64>,
    pub(super) mirrors: PdMirrors,
    pub(super) drive_join: std::thread::JoinHandle<()>,
    pub(super) push_join: std::thread::JoinHandle<()>,
    pub(super) push_state: Arc<Mutex<PushState>>,
    pub(super) accept_join: Option<std::thread::JoinHandle<()>>,
    pub(super) accept_stop: Arc<std::sync::atomic::AtomicBool>,
    pub(super) raft_addr: Option<SocketAddr>,
}

pub(crate) enum PdCmd {
    Admin(AdminMutation, std::time::Instant, mpsc::Sender<Result<AdminOutcome, PartitionError>>),
    Status(mpsc::Sender<PdStatus>),
    Inspect(mpsc::Sender<PdInspection>, bool),
    Bootstrap(AddressedMembers),
    Add(NodeId, u32, Vec<SocketAddr>),
    CreateZone(ZoneConfig, Vec<Vec<u8>>),
    SplitZone(ZoneId, Vec<u8>, SplitMode),
    MergeZone(ZoneId, Vec<u8>),
    MergeZones(ZoneId, Vec<Vec<u8>>),
    PrepareRehome(ZoneId, Vec<u8>),
    PrepareBalance(BalanceProposal),
    EnableOpaque(BalanceMapUpgrade),
    AbortRehome(ZoneId, Vec<u8>),
    CommitRehome(ZoneId, Vec<u8>),
    RestoreSeed(u32),
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdminOutcome {
    Committed { index: u64, term: u64 },
    Unchanged,
    Queued,
    CommittedMapQueued { index: u64, term: u64 },
    CommittedMapDropped { index: u64, term: u64 },
}

#[derive(Clone, Debug)]
pub struct PdStatus {
    pub id: u64,
    pub role: &'static str,
    pub term: u64,
    pub leader: Option<u64>,
    pub commit: u64,
    pub applied: u64,
    pub caught_up: bool,
    pub incarnation: Option<u128>,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
}

#[derive(Clone, Debug)]
pub struct PdInspection {
    pub status: PdStatus,
    pub authoritative: Option<u64>,
    pub members: AddressedMembers,
    pub ring_rf: usize,
    pub zones: Catalog,
    pub range_maps: HashMap<ZoneId, RangeMap>,
    pub state: Option<Vec<(Vec<u8>, Vec<u8>)>>,
}

#[derive(Clone)]
pub enum AdminMutation {
    Add(NodeId, u32, Vec<SocketAddr>),
    Remove(NodeId),
    CreateZone(ZoneConfig, Vec<Vec<u8>>),
    DropZone(ZoneId),
    SplitZone(ZoneId, Vec<u8>, SplitMode),
    MergeZone(ZoneId, Vec<u8>),
    AddPdLearner(u64, SocketAddr),
    RemovePdLearner(u64),
    ReplacePdVoter(u64, u64),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SplitMode {
    Inherit,
    Canonical,
}

#[derive(Clone, Copy)]
pub(super) enum BalancePreparePhase {
    Acquire,
    AcquireSubmitted(u64),
    IntentReflected,
    MapSubmitted(u64),
    Clearing(u64),
}

#[derive(Clone)]
pub(super) enum CanonicalTarget {
    Unresolved,
    Legacy,
    Opaque(Vec<u8>),
}

#[derive(Clone)]
pub(super) enum InitialMapPlacement {
    Unresolved,
    Legacy,
    Opaque { member_epoch: u64 },
}

#[derive(Clone)]
pub(super) enum RangeOpKind {
    Split(SplitMode, CanonicalTarget),
    InstallInitialMap {
        map: RangeMap,
        legacy: RangeMap,
        cfg: ZoneConfig,
        creation_index: u64,
        creation_term: u64,
        authorized: bool,
        placement: InitialMapPlacement,
    },
    Merge,
    MergeAll(Vec<Vec<u8>>),
    PrepareRehome(CanonicalTarget),
    PrepareBalance(BalanceProposal, BalancePreparePhase),
    EnableOpaque(BalanceMapUpgrade),
    AbortRehome,
    CommitRehome,
}

#[derive(Clone)]
pub(super) struct RangeEntryEstimate {
    pub(super) entries: u64,
    pub(super) observed_at: std::time::Instant,
}

#[derive(Clone)]
pub(super) struct RangeEntryEstimates {
    pub(super) member_epoch: u64,
    pub(super) zone_epoch: u64,
    pub(super) range_map_epoch: u64,
    pub(super) ranges: HashMap<(ZoneId, Vec<u8>), RangeEntryEstimate>,
}

pub(crate) struct RangeEntryPoll {
    pub(super) member_epoch: u64,
    pub(super) zone_epoch: u64,
    pub(super) range_map_epoch: u64,
    pub(super) observed_at: std::time::Instant,
    pub(super) ranges: HashMap<(ZoneId, Vec<u8>), u64>,
}

#[derive(Clone)]
pub(crate) struct PushState {
    pub(crate) member_epoch: u64,
    pub(crate) members: AddressedMembers,
    pub(crate) zone_epoch: u64,
    pub(crate) zones: Catalog,
    pub(crate) range_map_epoch: u64,
    pub(crate) range_maps: HashMap<ZoneId, RangeMap>,
    pub(crate) leader: bool,
    pub(crate) incarnation: Option<u128>,
    pub(crate) auto_split_threshold: u64,
    pub(crate) auto_load_split_ops: u64,
    pub(crate) auto_merge_threshold: u64,
    pub(crate) auto_merge_covered: bool,
    pub(crate) auto_split_poll_ms: u64,
    pub(crate) auto_splits_per_poll: usize,
    pub(crate) single_range_warn_entries: u64,
    pub(crate) range_concentration_warn_pct: u64,
    pub(crate) rehome_enabled: bool,
    pub(crate) max_concurrent_rehomes: usize,
    pub(crate) rehome_report_deadline_ms: u64,
    pub(crate) rehome_recovery_defer_ms: u64,
    pub(crate) opaque_range_maps_enabled: bool,
    pub(crate) opaque_capability_certified: Option<(u64, u64, u64)>,
    pub(crate) balance_execution_enabled: bool,
    pub(crate) balance_high_pct: u64,
    pub(crate) balance_low_pct: u64,
    pub(crate) balance_min_improvement_pct: u64,
    pub(crate) balance_token_search_limit: u64,
    pub(crate) balance_prepare_result: Option<(u64, bool)>,
    pub(super) range_entry_estimates: Arc<Mutex<Option<RangeEntryEstimates>>>,
    pub(crate) stop: bool,
}

impl PushState {
    pub(crate) fn new() -> Self {
        PushState {
            member_epoch: 0,
            members: Vec::new(),
            zone_epoch: 0,
            zones: Catalog::new(),
            range_map_epoch: 0,
            range_maps: HashMap::new(),
            leader: false,
            incarnation: None,
            auto_split_threshold: DEFAULT_RANGE_SPLIT_THRESHOLD,
            auto_load_split_ops: DEFAULT_RANGE_LOAD_SPLIT_OPS,
            auto_merge_threshold: DEFAULT_RANGE_MERGE_THRESHOLD,
            auto_merge_covered: true,
            auto_split_poll_ms: DEFAULT_RANGE_SPLIT_POLL_MS,
            auto_splits_per_poll: DEFAULT_RANGE_SPLITS_PER_POLL,
            single_range_warn_entries: DEFAULT_SINGLE_RANGE_WARN_ENTRIES,
            range_concentration_warn_pct: DEFAULT_RANGE_CONCENTRATION_WARN_PCT,
            rehome_enabled: false,
            max_concurrent_rehomes: 1,
            rehome_report_deadline_ms: 60_000,
            rehome_recovery_defer_ms: 1_800_000,
            opaque_range_maps_enabled: false,
            opaque_capability_certified: None,
            balance_execution_enabled: false,
            balance_high_pct: DEFAULT_BALANCE_HIGH_PCT,
            balance_low_pct: DEFAULT_BALANCE_LOW_PCT,
            balance_min_improvement_pct: DEFAULT_BALANCE_MIN_IMPROVEMENT_PCT,
            balance_token_search_limit: DEFAULT_BALANCE_TOKEN_SEARCH_LIMIT,
            balance_prepare_result: None,
            range_entry_estimates: Arc::new(Mutex::new(None)),
            stop: false,
        }
    }

    pub(crate) fn opaque_capability_current(&self) -> bool {
        self.leader
            && self.opaque_capability_certified == Some((
                self.member_epoch,
                self.zone_epoch,
                self.range_map_epoch,
            ))
    }

    pub(super) fn apply_range_entry_poll(&mut self, poll: RangeEntryPoll) {
        if !self.leader
            || poll.member_epoch != self.member_epoch
            || poll.zone_epoch != self.zone_epoch
            || poll.range_map_epoch != self.range_map_epoch
        {
            return;
        }
        let max_age = Duration::from_millis(
            self.auto_split_poll_ms.saturating_mul(RANGE_ENTRY_ESTIMATE_TTL_POLLS),
        );
        let mut retained = self.range_entry_estimates.lock().unwrap();
        let estimates = retained.get_or_insert_with(|| RangeEntryEstimates {
            member_epoch: poll.member_epoch,
            zone_epoch: poll.zone_epoch,
            range_map_epoch: poll.range_map_epoch,
            ranges: HashMap::new(),
        });
        if estimates.member_epoch != poll.member_epoch
            || estimates.zone_epoch != poll.zone_epoch
            || estimates.range_map_epoch != poll.range_map_epoch
        {
            *estimates = RangeEntryEstimates {
                member_epoch: poll.member_epoch,
                zone_epoch: poll.zone_epoch,
                range_map_epoch: poll.range_map_epoch,
                ranges: HashMap::new(),
            };
        }
        estimates.ranges.retain(|_, estimate| {
            poll.observed_at.checked_duration_since(estimate.observed_at)
                .is_some_and(|age| age <= max_age)
        });
        for (range, entries) in poll.ranges {
            estimates.ranges.insert(range, RangeEntryEstimate {
                entries,
                observed_at: poll.observed_at,
            });
        }
    }
}

struct StatsPoll {
    fetched: mpsc::Receiver<RangeStatsFetch>,
    snap: PushState,
    targets: Vec<(NodeId, Vec<SocketAddr>)>,
    range_maps_acked: bool,
    opaque_capability_ready: bool,
}

pub(super) fn pusher_loop(
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    ring: RingConfig,
    state: &Mutex<PushState>,
    balance_poll_authority: &Mutex<BalancePollAuthority>,
    cmd: &Sender<PdCmd>,
) {
    let mut ledger = PushLedger::default();
    let mut lanes = PushLanes::default();
    let mut beacon: Option<mpsc::Receiver<Vec<BeaconProbe>>> = None;
    let mut stats_poll: Option<StatsPoll> = None;
    let mut next_beacon = std::time::Instant::now() + BEACON_INTERVAL;
    let mut next_split_poll = std::time::Instant::now();
    let mut last_single_range_warn: HashMap<ZoneId, std::time::Instant> = HashMap::new();
    let mut rehome_reports: HashMap<(ZoneId, Vec<u8>), RehomeReportState> = HashMap::new();
    let mut rehome_backoff: HashMap<(ZoneId, Vec<u8>), (std::time::Instant, u32)> = HashMap::new();
    let mut balance_executor = BalanceExecutorState::default();
    let mut recovery_hold = RecoveryHold::default();
    loop {
        let mut snap = state.lock().unwrap().clone();
        if snap.stop {
            return;
        }
        if snap.leader && snap.incarnation.is_some() {
            let targets = push_targets(data_nodes, &snap.members);
            lanes.collect(&mut ledger);
            lanes.dispatch(&targets, &snap, ring, &mut ledger);
            if let Some(probes) = beacon.as_ref().map(|probes| probes.try_recv()) {
                if !matches!(probes, Err(TryRecvError::Empty)) {
                    beacon = None;
                    next_beacon = std::time::Instant::now() + BEACON_INTERVAL;
                }
                if let Ok(probes) = probes {
                    let restarted = apply_beacon(
                        probes,
                        &mut ledger.members,
                        &mut ledger.zones,
                        &mut ledger.range_maps,
                        &mut ledger.range_map_features,
                    );
                    let now = std::time::Instant::now();
                    for node in restarted {
                        recovery_hold.note_restart(node, now);
                    }
                }
            } else if std::time::Instant::now() >= next_beacon {
                let probes = beacon_probes(&targets, &ledger.members, &ledger.zones);
                let (tx, rx) = mpsc::channel();
                beacon = Some(rx);
                thread::spawn(move || {
                    let _ = tx.send(run_beacon_probes(probes));
                });
            }
            if let Some(poll) = stats_poll.as_ref() {
                let fetched = poll.fetched.try_recv();
                if !matches!(fetched, Err(TryRecvError::Empty)) {
                    let poll = stats_poll.take().unwrap();
                    if let Ok(fetched) = fetched {
                        let entry_poll = range_stats_apply(
                            fetched,
                            &poll.targets,
                            &poll.snap,
                            cmd,
                            &mut last_single_range_warn,
                            &ledger.range_maps,
                            poll.range_maps_acked,
                            poll.opaque_capability_ready,
                            ring,
                            &mut rehome_reports,
                            &mut rehome_backoff,
                            &mut balance_executor,
                            balance_poll_authority,
                            &mut recovery_hold,
                        );
                        state.lock().unwrap().apply_range_entry_poll(entry_poll);
                        if poll.snap.balance_prepare_result.is_some() {
                            let mut current = state.lock().unwrap();
                            if current.balance_prepare_result == poll.snap.balance_prepare_result {
                                current.balance_prepare_result = None;
                            }
                        }
                    }
                    next_split_poll = std::time::Instant::now()
                        + Duration::from_millis(poll.snap.auto_split_poll_ms.max(1));
                }
            } else if std::time::Instant::now() >= next_split_poll {
                let range_maps_acked = targets.iter().all(|(_, addrs)| {
                    !addrs.is_empty()
                        && addrs.iter().all(|addr| {
                            range_maps_confirmed_at(&ledger.range_maps, addr, snap.range_map_epoch)
                        })
                });
                let opaque_capability_ready = snap.opaque_range_maps_enabled
                    && range_maps_acked
                    && targets.iter().all(|(_, addrs)| {
                        !addrs.is_empty() && addrs.iter().all(|addr| {
                            ledger.range_map_features.get(addr).copied().unwrap_or(0)
                                & crate::proto::FEATURE_OPAQUE_RANGE_MAP != 0
                        })
                    });
                let certified = opaque_capability_ready.then_some((
                    snap.member_epoch,
                    snap.zone_epoch,
                    snap.range_map_epoch,
                ));
                snap.opaque_capability_certified = certified;
                {
                    let mut current = state.lock().unwrap();
                    if current.leader
                        && current.member_epoch == snap.member_epoch
                        && current.zone_epoch == snap.zone_epoch
                        && current.range_map_epoch == snap.range_map_epoch
                    {
                        current.opaque_capability_certified = certified;
                    } else if !current.leader {
                        current.opaque_capability_certified = None;
                    }
                }
                let (tx, rx) = mpsc::channel();
                let fetch_targets = targets.clone();
                thread::spawn(move || {
                    let _ = tx.send(fetch_range_stats(&fetch_targets));
                });
                stats_poll = Some(StatsPoll {
                    fetched: rx,
                    snap,
                    targets,
                    range_maps_acked,
                    opaque_capability_ready,
                });
            }
        } else {
            invalidate_balance_poll(balance_poll_authority);
            recovery_hold.reset();
            crate::observe::gauges().pd_rehome_recovery_hold.set(0);
            state.lock().unwrap().opaque_capability_certified = None;
            ledger = PushLedger::default();
            lanes.clear();
            beacon = None;
            stats_poll = None;
        }
        thread::sleep(Duration::from_millis(5));
    }
}

pub(super) const RECOVERY_CLEAN_INTERVAL: Duration = Duration::from_secs(60);

pub(super) const RESTART_RECOVERY_GRACE: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub(crate) struct RecoveryHold {
    pub(super) episode: bool,
    pub(super) held: Duration,
    pub(super) clean_since: Option<std::time::Instant>,
    pub(super) last: Option<(std::time::Instant, bool)>,
    pub(super) restarts: HashMap<NodeId, (std::time::Instant, bool)>,
}

impl RecoveryHold {
    pub(super) fn note_restart(&mut self, node: NodeId, now: std::time::Instant) {
        self.restarts.insert(node, (now, false));
    }

    pub(super) fn restarting(
        &mut self,
        down: &BTreeSet<NodeId>,
        seeding: &BTreeSet<NodeId>,
        grace: Duration,
        now: std::time::Instant,
    ) -> BTreeSet<NodeId> {
        let mut restarting = BTreeSet::new();
        self.restarts.retain(|node, (since, seen_seeding)| {
            if seeding.contains(node) {
                *seen_seeding = true;
                return true;
            }
            if down.contains(node) {
                *since = now;
                return true;
            }
            if *seen_seeding {
                return false;
            }
            if now.saturating_duration_since(*since) >= grace {
                log::info!(
                    "pd: restarted node {node} reported no recovery seed within {grace:?}; \
                     no longer counted as recovering"
                );
                return false;
            }
            restarting.insert(*node);
            true
        });
        restarting
    }

    pub(super) fn holds(
        &mut self,
        recovering: bool,
        bound: Duration,
        now: std::time::Instant,
    ) -> bool {
        if bound.is_zero() {
            *self = RecoveryHold::default();
            return false;
        }
        if let Some((at, true)) = self.last {
            self.held = self.held.saturating_add(now.saturating_duration_since(at));
        }
        if recovering {
            self.episode = true;
            self.clean_since = None;
        } else if self.episode {
            let clean = *self.clean_since.get_or_insert(now);
            if now.saturating_duration_since(clean) >= RECOVERY_CLEAN_INTERVAL {
                self.episode = false;
                self.held = Duration::ZERO;
                self.clean_since = None;
            }
        }
        let hold = recovering && self.held < bound;
        self.last = Some((now, hold));
        hold
    }

    pub(super) fn reset(&mut self) {
        *self = RecoveryHold::default();
    }
}

pub(super) struct RehomeOwnerReport {
    pub(super) last_seen: std::time::Instant,
    pub(super) last_refresh: std::time::Instant,
    pub(super) last_progress: u64,
}

pub(crate) struct RehomeReportState {
    pub(super) first_seen: std::time::Instant,
    pub(super) pending_epoch: u64,
    pub(super) delivery_confirmed: bool,
    pub(super) mirror_failures: HashMap<(NodeId, u32), u64>,
    pub(super) owners: HashMap<(NodeId, u32), RehomeOwnerReport>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RehomeAbortCause {
    MirrorFailure,
    ReportGap(NodeId, u32),
    CopyStall(NodeId, u32),
}

pub(super) fn range_log_id(zone_id: ZoneId, lower: &[u8]) -> u64 {
    let mut bytes = b"vals-pd-range-log-v1".to_vec();
    bytes.extend_from_slice(&zone_id.to_le_bytes());
    bytes.extend_from_slice(&(lower.len() as u64).to_le_bytes());
    bytes.extend_from_slice(lower);
    crate::ring::hash64(&bytes)
}

pub(super) fn rehome_owner_refresh(
    state: &mut RehomeReportState,
    owner: (NodeId, u32),
    is_target: bool,
    complete: bool,
    progress: u64,
    now: std::time::Instant,
) {
    match state.owners.entry(owner) {
        std::collections::hash_map::Entry::Vacant(v) => {
            v.insert(RehomeOwnerReport {
                last_seen: now,
                last_refresh: now,
                last_progress: progress,
            });
        }
        std::collections::hash_map::Entry::Occupied(mut o) => {
            let o = o.get_mut();
            o.last_seen = now;
            if !is_target || complete || progress != o.last_progress {
                o.last_refresh = now;
            }
            o.last_progress = progress;
        }
    }
}

pub(super) fn rehome_delivery_refresh(
    state: &mut RehomeReportState,
    delivered: bool,
    now: std::time::Instant,
) {
    if !delivered {
        state.delivery_confirmed = false;
        return;
    }
    if !state.delivery_confirmed {
        state.delivery_confirmed = true;
        state.first_seen = now;
        for owner in state.owners.values_mut() {
            owner.last_seen = now;
            owner.last_refresh = now;
        }
    }
}

pub(super) fn rehome_abort_cause(
    state: &RehomeReportState,
    required: &std::collections::HashSet<(NodeId, u32)>,
    now: std::time::Instant,
    deadline: Duration,
) -> Option<RehomeAbortCause> {
    if !state.delivery_confirmed {
        return None;
    }
    for &(node, shard) in required {
        match state.owners.get(&(node, shard)) {
            None => {
                if now.duration_since(state.first_seen) >= deadline {
                    return Some(RehomeAbortCause::ReportGap(node, shard));
                }
            }
            Some(o) => {
                if now.duration_since(o.last_seen) >= deadline {
                    return Some(RehomeAbortCause::ReportGap(node, shard));
                }
                if now.duration_since(o.last_refresh) >= deadline {
                    return Some(RehomeAbortCause::CopyStall(node, shard));
                }
            }
        }
    }
    None
}

#[derive(Default)]
pub(crate) struct BalanceExecutorState {
    pub(super) proposal: Option<BalanceProposal>,
    pub(super) active_seen: bool,
    blocked_this_poll: bool,
}

impl BalanceExecutorState {
    pub(super) fn may_prepare(&mut self, snap: &PushState, fresh: bool) -> bool {
        self.blocked_this_poll = false;
        let active = snap.range_maps.values().any(|map| map.active_rehomes() > 0);
        if active {
            self.active_seen = true;
            self.blocked_this_poll = true;
            return false;
        }
        if self.active_seen || self.proposal.is_some() {
            self.blocked_this_poll = true;
            let rejected = self.proposal.as_ref().is_some_and(|proposal| {
                snap.balance_prepare_result == Some((proposal.identity, false))
            });
            let completed = self.active_seen && !active;
            if fresh && (rejected || completed) {
                if let Some(proposal) = self.proposal.take() {
                    let committed = snap.range_maps.get(&proposal.zone_id)
                        .is_some_and(|map| {
                            let index = map.range_index(&proposal.lower);
                            map.range_bounds(index).0.unwrap_or_default() == proposal.lower
                                && map.pending_of(index).is_none()
                                && map.token_of(index) == proposal.token
                        });
                    if committed {
                        crate::observe::counters().pd_balance_exec_commits.inc();
                    } else if self.active_seen {
                        crate::observe::counters().pd_balance_exec_aborts.inc();
                    }
                }
                self.active_seen = false;
                crate::observe::counters().pd_balance_exec_cooldown_polls.inc();
            }
            return false;
        }
        fresh
    }

    pub(super) fn prepared(&mut self, proposal: BalanceProposal) {
        self.proposal = Some(proposal);
        self.blocked_this_poll = true;
    }

    pub(super) fn blocks_other_rehomes(&self) -> bool {
        self.blocked_this_poll
    }

}

pub(super) fn load_merge_ceiling(load_split_ops: u64) -> u64 {
    if load_split_ops > 0 {
        load_split_ops / 2
    } else {
        u64::MAX
    }
}

