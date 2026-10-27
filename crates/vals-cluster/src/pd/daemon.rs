use super::*;

pub(super) fn push_targets(
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    members: &AddressedMembers,
) -> Vec<(NodeId, Vec<SocketAddr>)> {
    let mut out = data_nodes.to_vec();
    for (n, _, addrs) in members {
        if !addrs.is_empty() && !out.iter().any(|(b, _)| b == n) {
            out.push((*n, addrs.clone()));
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
fn publish_push_state(
    state: &Mutex<PushState>,
    changed: bool,
    member_epoch: u64,
    members: &AddressedMembers,
    zone_epoch: u64,
    zones: &Catalog,
    range_map_epoch: u64,
    range_maps: &HashMap<ZoneId, RangeMap>,
    leader: bool,
    withhold_initial_map: bool,
) {
    let mut st = state.lock().unwrap();
    if !changed
        && (withhold_initial_map
            || (st.member_epoch == member_epoch
                && st.zone_epoch == zone_epoch
                && st.range_map_epoch == range_map_epoch
                && st.leader == leader))
    {
        return;
    }
    st.member_epoch = member_epoch;
    st.members = members.to_vec();
    if !withhold_initial_map {
        st.zone_epoch = zone_epoch;
        st.zones = zones.clone();
        st.range_map_epoch = range_map_epoch;
        st.range_maps = range_maps.clone();
    }
    st.leader = leader;
    let mut estimates = st.range_entry_estimates.lock().unwrap();
    if !leader || estimates.as_ref().is_some_and(|estimate| {
        estimate.member_epoch != member_epoch
            || estimate.zone_epoch != zone_epoch
            || estimate.range_map_epoch != range_map_epoch
    }) {
        *estimates = None;
    }
}

pub(super) fn ensure_incarnation<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    minted_through: &mut u64,
) {
    if !pd.is_caught_up_leader() || pd.epoch() < *minted_through || pd.incarnation().is_some() {
        return;
    }
    let id = match fresh_incarnation() {
        Ok(id) => id,
        Err(e) => {
            if incarnation_warn_due() {
                log::error!("pd: cannot draw a random cluster incarnation id (retried): {e}");
            }
            return;
        }
    };
    match pd.mint_incarnation(id) {
        Ok(index) => *minted_through = index,
        Err(e) => {
            crate::observe::counters().pd_propose_errors.inc();
            if incarnation_warn_due() {
                log::warn!(
                    "pd: cluster incarnation proposal failed (retried); no config is pushed \
                     until it commits: {e}"
                );
            }
        }
    }
}

fn incarnation_warn_due() -> bool {
    static LAST: Mutex<Option<std::time::Instant>> = Mutex::new(None);
    let mut last = LAST.lock().unwrap();
    let now = std::time::Instant::now();
    if last.is_some_and(|at| now.duration_since(at) < Duration::from_secs(10)) {
        return false;
    }
    *last = Some(now);
    true
}

fn publish_incarnation(
    state: &Mutex<PushState>,
    incarnation: Option<u128>,
    last: &mut Option<u128>,
) {
    if incarnation == *last {
        return;
    }
    if let Some(id) = incarnation {
        log::info!(
            "pd: cluster incarnation {id:032x}; every control push carries it and data nodes \
             bound to another incarnation refuse it"
        );
    }
    *last = incarnation;
    state.lock().unwrap().incarnation = incarnation;
}

fn spawn_pusher(
    data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
    ring: RingConfig,
    balance_poll_authority: Arc<Mutex<BalancePollAuthority>>,
    cmd: Sender<PdCmd>,
    settings: PdPlacementSettings,
) -> (Arc<Mutex<PushState>>, thread::JoinHandle<()>) {
    let push_state = Arc::new(Mutex::new(PushState::new()));
    {
        let mut st = push_state.lock().unwrap();
        st.auto_split_threshold = settings.range_split_threshold;
        st.auto_load_split_ops = settings.range_load_split_ops;
        st.auto_split_poll_ms = settings.range_split_poll_ms;
        st.auto_splits_per_poll = settings.range_splits_per_poll;
        st.auto_merge_threshold = settings.range_merge_threshold;
        st.auto_merge_covered = settings.range_merge_covered;
        st.single_range_warn_entries = settings.single_range_warn_entries;
        st.range_concentration_warn_pct = settings.range_concentration_warn_pct;
        st.rehome_enabled = settings.rehome_enabled;
        st.max_concurrent_rehomes = settings.max_concurrent_rehomes;
        st.rehome_report_deadline_ms = settings.rehome_report_deadline_ms;
        st.rehome_recovery_defer_ms = settings.rehome_recovery_defer_ms;
        st.opaque_range_maps_enabled = settings.opaque_range_maps;
        st.balance_execution_enabled = settings.balance_execute;
        st.balance_high_pct = settings.balance_high_pct;
        st.balance_low_pct = settings.balance_low_pct;
        st.balance_min_improvement_pct = settings.balance_min_improvement_pct;
        st.balance_token_search_limit = settings.balance_token_search_limit;
    }
    let pusher_state = push_state.clone();
    let push_join = crate::observe::spawn_abort_on_panic("vals-pd-push", move || {
        pusher_loop(
            &data_nodes,
            ring,
            &pusher_state,
            &balance_poll_authority,
            &cmd,
        )
    });
    (push_state, push_join)
}

pub(super) fn control_mutations_settled(
    applied: u64,
    proposed_through: u64,
    pending_membership: bool,
    pending_catalog: bool,
) -> bool {
    applied >= proposed_through && !pending_membership && !pending_catalog
}

fn placement_ring<D: BlockDevice>(pd: &PlacementDriver<D>, cfg: RingConfig) -> Ring {
    Ring::from_weighted(
        cfg.rf as usize,
        cfg.vnodes,
        pd.members_addressed().iter().map(|(node, weight, _)| (*node, *weight)),
    )
}

fn placement_topology(authority: &Mutex<BalancePollAuthority>) -> Option<BTreeMap<NodeId, u32>> {
    authority.lock().unwrap().snapshot.as_ref().map(|snap| snap.topology.clone())
}

pub(super) fn initial_range_map(
    zone_id: ZoneId,
    cfg: &ZoneConfig,
    boundaries: Vec<Vec<u8>>,
    capability_ready: bool,
    ring: &Ring,
    topology: Option<&BTreeMap<NodeId, u32>>,
    search_limit: u64,
) -> RangeMap {
    let legacy = RangeMap::from_splits(boundaries);
    if cfg.ordering != crate::zone::Ordering::Sorted || !capability_ready {
        return legacy;
    }
    let mut tokens = Vec::with_capacity(legacy.num_ranges());
    for index in 0..legacy.num_ranges() {
        let lower = legacy.range_bounds(index).0.unwrap_or_default();
        let Some(token) = balanced_opaque_token(
            ring, zone_id, cfg, &tokens, topology, lower, search_limit,
        ) else {
            return legacy;
        };
        tokens.push(token);
    }
    let map = RangeMap::from_opaque_parts(
        legacy.splits().to_vec(),
        tokens,
        vec![None; legacy.num_ranges()],
        vec![false; legacy.num_ranges()],
    )
    .unwrap();
    map
}

pub(super) fn effective_tokens_except(map: &RangeMap, except: Option<usize>) -> Vec<Vec<u8>> {
    (0..map.num_ranges())
        .filter(|&index| Some(index) != except)
        .map(|index| map.pending_of(index).unwrap_or_else(|| map.token_of(index)).to_vec())
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum InitialMapDecision {
    Ready,
    Wait,
    Discard,
}

pub(super) fn initial_map_decision<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    zone_id: ZoneId,
    kind: &RangeOpKind,
) -> InitialMapDecision {
    let RangeOpKind::InstallInitialMap { cfg, map, creation_index, authorized, .. } = kind else {
        if !matches!(kind, RangeOpKind::PrepareBalance(..))
            && !pd.zone_exists(zone_id)
            && pd.epoch() >= pd.last_index()
        {
            log::warn!("pd: discarding a queued range op on dropped zone {zone_id}");
            return InitialMapDecision::Discard;
        }
        return InitialMapDecision::Ready;
    };
    match pd.zones().get(zone_id) {
        Some(committed) if committed == cfg => {
            if let Some(current) = pd.range_maps().get(&zone_id) {
                if current != map {
                    log::warn!(
                        "pd: discarding initial map for zone {zone_id} ({:?}): a different range map is committed",
                        cfg.name,
                    );
                    return InitialMapDecision::Discard;
                }
            }
            if *authorized {
                InitialMapDecision::Ready
            } else {
                InitialMapDecision::Wait
            }
        }
        Some(committed) => {
            log::warn!(
                "pd: discarding initial map for zone {zone_id} ({:?}): committed zone is {:?}",
                cfg.name, committed.name,
            );
            InitialMapDecision::Discard
        }
        None if pd.epoch() < *creation_index => InitialMapDecision::Wait,
        None => {
            log::warn!(
                "pd: discarding initial map for zone {zone_id} ({:?}): creation not committed or zone dropped",
                cfg.name,
            );
            InitialMapDecision::Discard
        }
    }
}

pub(super) fn initial_map_queue_head<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>,
) -> InitialMapDecision {
    let decision = initial_map_decision(pd, ops[0].0, &ops[0].2);
    if decision == InitialMapDecision::Discard {
        ops.remove(0);
    }
    decision
}

pub(super) fn resolve_initial_map<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    ring_cfg: RingConfig,
    push_state: &Mutex<PushState>,
    authority: &Mutex<BalancePollAuthority>,
    zone_id: ZoneId,
    control_settled: bool,
    kind: &mut RangeOpKind,
) -> bool {
    let RangeOpKind::InstallInitialMap {
        map,
        legacy,
        cfg,
        placement,
        authorized,
        ..
    } = kind else {
        return true;
    };
    if !matches!(placement, InitialMapPlacement::Unresolved) {
        return *authorized && pd.zones().get(zone_id) == Some(cfg);
    }
    let (capability_ready, search_limit, member_epoch, published_members) = {
        let state = push_state.lock().unwrap();
        (
            state.opaque_capability_current(),
            state.balance_token_search_limit,
            state.member_epoch,
            state.members.clone(),
        )
    };
    if !control_settled || published_members != pd.members_addressed() {
        return false;
    }
    if capability_ready {
        let topology = placement_topology(authority);
        *map = initial_range_map(
            zone_id,
            cfg,
            legacy.splits().to_vec(),
            true,
            &placement_ring(pd, ring_cfg),
            topology.as_ref(),
            search_limit,
        );
    }
    *placement = if map.supports_opaque_tokens() {
        InitialMapPlacement::Opaque { member_epoch }
    } else {
        InitialMapPlacement::Legacy
    };
    *authorized && pd.zones().get(zone_id) == Some(cfg)
}

pub(super) fn resolve_canonical_target<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    ring_cfg: RingConfig,
    push_state: &Mutex<PushState>,
    authority: &Mutex<BalancePollAuthority>,
    zone_id: ZoneId,
    lower: &[u8],
    kind: &mut RangeOpKind,
) {
    let (target, except) = match kind {
        RangeOpKind::Split(SplitMode::Canonical, target @ CanonicalTarget::Unresolved) => {
            (target, None)
        }
        RangeOpKind::PrepareRehome(target @ CanonicalTarget::Unresolved) => {
            let map = pd.range_map(zone_id);
            (target, Some(map.range_index(lower)))
        }
        _ => return,
    };
    let map = pd.range_map(zone_id);
    let zones = pd.zones();
    let cfg = zones.get(zone_id);
    let (capability_ready, search_limit) = {
        let state = push_state.lock().unwrap();
        (state.opaque_capability_current(), state.balance_token_search_limit)
    };
    let topology = placement_topology(authority);
    let token = cfg.filter(|_| capability_ready && map.supports_opaque_tokens()).and_then(|cfg| {
        balanced_opaque_token(
            &placement_ring(pd, ring_cfg),
            zone_id,
            cfg,
            &effective_tokens_except(&map, except),
            topology.as_ref(),
            lower,
            search_limit,
        )
    });
    *target = match token {
        Some(token) => CanonicalTarget::Opaque(token),
        None => {
            crate::observe::counters().pd_canonical_placements_fallback.inc();
            CanonicalTarget::Legacy
        }
    };
}

pub(super) fn revalidate_queued_placement(
    push_state: &Mutex<PushState>,
    committed_members: &AddressedMembers,
    control_settled: bool,
    kind: &mut RangeOpKind,
) {
    let (capability_ready, member_epoch, published_members) = {
        let state = push_state.lock().unwrap();
        (
            state.opaque_capability_current(),
            state.member_epoch,
            state.members.clone(),
        )
    };
    match kind {
        RangeOpKind::Split(SplitMode::Canonical, target @ CanonicalTarget::Opaque(_))
        | RangeOpKind::PrepareRehome(target @ CanonicalTarget::Opaque(_))
            if !capability_ready =>
        {
            *target = CanonicalTarget::Legacy;
            crate::observe::counters().pd_canonical_placements_fallback.inc();
        }
        RangeOpKind::InstallInitialMap {
            map,
            legacy,
            placement,
            ..
        } => {
            let InitialMapPlacement::Opaque {
                member_epoch: baked_epoch,
            } = placement else {
                return;
            };
            if map.supports_opaque_tokens()
                && (!control_settled
                    || !capability_ready
                    || published_members != *committed_members
                    || member_epoch != *baked_epoch)
            {
                *map = legacy.clone();
                *placement = InitialMapPlacement::Legacy;
            }
        }
        _ => {}
    }
}

fn record_reflected_placement(map: &RangeMap, at: &[u8], kind: &RangeOpKind) {
    match kind {
        RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Opaque(token))
            if map.token_of(map.range_index(at)) == token =>
        {
            crate::observe::counters().pd_canonical_placements_balanced.inc();
        }
        RangeOpKind::PrepareRehome(CanonicalTarget::Opaque(_)) => {
            crate::observe::counters().pd_canonical_placements_balanced.inc();
        }
        RangeOpKind::InstallInitialMap { map, .. } => {
            let count = map.num_ranges() as u64;
            if map.supports_opaque_tokens() {
                crate::observe::counters().pd_canonical_placements_balanced.add(count);
            } else {
                crate::observe::counters().pd_canonical_placements_fallback.add(count);
            }
        }
        _ => {}
    }
}

pub(super) fn range_op_reflected(map: &RangeMap, at: &[u8], kind: &RangeOpKind) -> bool {
    let has_boundary = map.splits().iter().any(|split| split == at);
    let index = map.range_index(at);
    let reflected = match kind {
        RangeOpKind::Split(_, _) => has_boundary,
        RangeOpKind::InstallInitialMap { map: expected, .. } => map == expected,
        RangeOpKind::Merge => !has_boundary,
        RangeOpKind::MergeAll(boundaries) => {
            map.active_rehomes() > 0
                || boundaries.iter().all(|boundary| map.splits().binary_search(boundary).is_err())
        }
        RangeOpKind::PrepareRehome(target) => {
            let expected = match target {
                CanonicalTarget::Opaque(token) => token.as_slice(),
                CanonicalTarget::Legacy => at,
                CanonicalTarget::Unresolved => return false,
            };
            map.pending_of(index) == Some(expected)
        }
        RangeOpKind::PrepareBalance(proposal, _) => {
            map.pending_of(index) == Some(proposal.token.as_slice())
        }
        RangeOpKind::EnableOpaque(_) => map.supports_opaque_tokens(),
        RangeOpKind::AbortRehome => map.pending_of(index).is_none(),
        RangeOpKind::CommitRehome => map.pending_of(index).is_none(),
    };
    if reflected {
        record_reflected_placement(map, at, kind);
    }
    reflected
}

pub(super) fn populated_canonical_split_parent(
    state: &PushState,
    committed_members: &AddressedMembers,
    committed_zones: &Catalog,
    committed_range_maps: &HashMap<ZoneId, RangeMap>,
    zone_id: ZoneId,
    at: &[u8],
    kind: &RangeOpKind,
    now: std::time::Instant,
) -> Option<(Vec<u8>, u64)> {
    if !matches!(kind, RangeOpKind::Split(SplitMode::Canonical, _)) || at.is_empty() {
        return None;
    }
    let map = committed_range_maps.get(&zone_id).cloned().unwrap_or_else(RangeMap::single);
    if map.splits().iter().any(|split| split == at) {
        return None;
    }
    let retained = state.range_entry_estimates.lock().unwrap();
    let estimates = retained.as_ref()?;
    if !state.leader
        || estimates.member_epoch != state.member_epoch
        || estimates.zone_epoch != state.zone_epoch
        || estimates.range_map_epoch != state.range_map_epoch
        || &state.members != committed_members
        || &state.zones != committed_zones
        || &state.range_maps != committed_range_maps
    {
        return None;
    }
    let index = map.range_index(at);
    let parent = map.range_bounds(index).0.unwrap_or_default().to_vec();
    let estimate = estimates.ranges.get(&(zone_id, parent.clone()))?;
    let max_age = Duration::from_millis(
        state.auto_split_poll_ms.saturating_mul(RANGE_ENTRY_ESTIMATE_TTL_POLLS),
    );
    if estimate.entries == 0
        || now.checked_duration_since(estimate.observed_at).is_none_or(|age| age > max_age)
    {
        return None;
    }
    Some((parent, estimate.entries))
}

fn refuse_populated_canonical_split<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    push_state: &Mutex<PushState>,
    zone_id: ZoneId,
    at: &[u8],
    kind: &RangeOpKind,
) -> bool {
    if !matches!(kind, RangeOpKind::Split(SplitMode::Canonical, _)) {
        return false;
    }
    let members = pd.members_addressed();
    let zones = pd.zones();
    let range_maps = pd.range_maps();
    let populated = populated_canonical_split_parent(
        &push_state.lock().unwrap(),
        &members,
        &zones,
        &range_maps,
        zone_id,
        at,
        kind,
        std::time::Instant::now(),
    );
    let Some((parent, entries)) = populated else { return false };
    log::warn!(
        "pd: refusing canonical split of populated zone {zone_id} parent range {:016x} \
         (~{entries} live entries)",
        range_log_id(zone_id, &parent)
    );
    true
}

pub(super) fn propose_range_op<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    zone_id: ZoneId,
    at: &[u8],
    kind: &RangeOpKind,
) -> Result<bool, PartitionError> {
    match kind {
        RangeOpKind::Split(SplitMode::Inherit, _) => pd.split_zone(zone_id, at),
        RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Opaque(token)) => {
            pd.split_zone_canonical_to(zone_id, at, token.clone())
        }
        RangeOpKind::Split(SplitMode::Canonical, _) => pd.split_zone_canonical(zone_id, at),
        RangeOpKind::InstallInitialMap { map, cfg, authorized, .. } => {
            if !authorized {
                return Err(PartitionError::Invariant(
                    "initial range map creation is not confirmed".into(),
                ));
            }
            pd.set_initial_range_map(zone_id, cfg, map).map(|_| true)
        }
        RangeOpKind::Merge => pd.merge_zone(zone_id, at),
        RangeOpKind::MergeAll(boundaries) => pd.merge_zone_all(zone_id, boundaries),
        RangeOpKind::PrepareRehome(CanonicalTarget::Opaque(token)) => {
            pd.prepare_rehome_to(zone_id, at, token)
        }
        RangeOpKind::PrepareRehome(_) => pd.prepare_rehome(zone_id, at),
        RangeOpKind::PrepareBalance(_, _) => unreachable!(),
        RangeOpKind::EnableOpaque(upgrade) => {
            let mut map = upgrade.map.clone().unwrap_or_else(RangeMap::single);
            map.enable_opaque_tokens();
            pd.set_range_map(zone_id, &map).map(|_| true)
        }
        RangeOpKind::AbortRehome => pd.abort_rehome(zone_id, at),
        RangeOpKind::CommitRehome => pd.commit_rehome(zone_id, at),
    }
}

fn process_admin_mutation<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    mutation: AdminMutation,
    pending_range_ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>,
) -> Result<bool, PartitionError> {
    if !pd.is_leader() {
        return Err(PartitionError::NotLeader);
    }
    if !pd.is_caught_up_leader() {
        return Err(PartitionError::NotReady);
    }
    let proposed = match mutation {
        AdminMutation::AddPdLearner(id, addr) => return pd.add_pd_learner(id, addr),
        AdminMutation::RemovePdLearner(id) => return pd.remove_pd_learner(id),
        AdminMutation::ReplacePdVoter(old, new) => return pd.replace_pd_voter_step(old, new),
        AdminMutation::Add(node, weight, addrs) => {
            pd.add_node_addressed(node, weight, &addrs)
        }
        AdminMutation::Remove(node) => pd.remove_node(node),
        AdminMutation::CreateZone(cfg, boundaries) => {
            let id = pd.create_zone(cfg.clone())?;
            if !boundaries.is_empty() {
                let legacy = RangeMap::from_splits(boundaries);
                pending_range_ops.push((
                    id,
                    Vec::new(),
                    RangeOpKind::InstallInitialMap {
                        map: legacy.clone(),
                        legacy,
                        cfg,
                        creation_index: pd.last_index(),
                        creation_term: pd.partition.term(),
                        authorized: false,
                        placement: InitialMapPlacement::Unresolved,
                    },
                ));
            }
            Ok(())
        }
        AdminMutation::DropZone(id) => pd.drop_zone(id),
        AdminMutation::SplitZone(id, at, mode) => {
            let target = if mode == SplitMode::Canonical {
                CanonicalTarget::Unresolved
            } else {
                CanonicalTarget::Legacy
            };
            pending_range_ops.push((id, at, RangeOpKind::Split(mode, target)));
            Ok(())
        }
        AdminMutation::MergeZone(id, at) => {
            pending_range_ops.push((id, at, RangeOpKind::Merge));
            Ok(())
        }
    };
    proposed.map(|()| true)
}

pub(super) struct PendingAdmin {
    index: u64,
    term: u64,
    map_queued: bool,
    pub(super) deadline: Option<std::time::Instant>,
    reply: Option<Sender<Result<AdminOutcome, PartitionError>>>,
}

pub(super) struct PendingRead {
    context: u64,
    index: Option<u64>,
    term: u64,
    pub(super) deadline: std::time::Instant,
    with_state: bool,
    reply: Sender<PdInspection>,
}

pub(super) fn handle_admin<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    mutation: AdminMutation,
    deadline: std::time::Instant,
    reply: Sender<Result<AdminOutcome, PartitionError>>,
    range_ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>,
    waits: &mut Vec<PendingAdmin>,
    control_through: &mut u64,
) {
    if std::time::Instant::now() >= deadline {
        let _ = reply.send(Err(PartitionError::AdminQueueExpired));
        return;
    }
    let queued = matches!(mutation, AdminMutation::SplitZone(..) | AdminMutation::MergeZone(..));
    let map_queued = matches!(&mutation,
        AdminMutation::CreateZone(_, boundaries) if !boundaries.is_empty());
    match process_admin_mutation(pd, mutation, range_ops) {
        Err(error) => {
            crate::observe::counters().pd_propose_errors.inc();
            let _ = reply.send(Err(error));
        }
        Ok(false) => {
            let _ = reply.send(Ok(AdminOutcome::Unchanged));
        }
        Ok(true) if queued => {
            let _ = reply.send(Ok(AdminOutcome::Queued));
        }
        Ok(true) => {
            let index = pd.last_index();
            *control_through = (*control_through).max(index);
            let term = pd.partition.entry_term(index).expect("newly proposed admin entry");
            pd.partition.watch_commit(index, term);
            waits.push(PendingAdmin {
                index, term, map_queued, deadline: Some(deadline), reply: Some(reply),
            });
        }
    }
}

fn watch_boot_map<D: BlockDevice>(pd: &mut PlacementDriver<D>, waits: &mut Vec<PendingAdmin>) {
    let index = pd.last_index();
    let term = pd.partition.entry_term(index).expect("newly proposed boot zone entry");
    pd.partition.watch_commit(index, term);
    waits.push(PendingAdmin {
        index, term, map_queued: true, deadline: None, reply: None,
    });
}

fn inspection<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    authoritative: Option<u64>,
    with_state: bool,
) -> PdInspection {
    PdInspection {
        status: pd.status(),
        authoritative,
        members: pd.members_addressed(),
        zones: pd.zones(),
        range_maps: pd.range_maps(),
        state: (with_state && authoritative.is_some()).then(|| pd.state_entries()),
    }
}

fn discard_unconfirmed_map(
    ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>,
    index: u64,
    term: u64,
) {
    ops.retain(|(id, _, kind)| {
        let discard = matches!(kind, RangeOpKind::InstallInitialMap {
            creation_index, creation_term, ..
        } if *creation_index == index && *creation_term == term);
        if discard {
            log::warn!("pd: discarding unconfirmed initial map for zone {id} from index={index} term={term}");
        }
        !discard
    });
}

pub(super) fn settle_admin<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    waits: &mut Vec<PendingAdmin>,
    reads: &mut Vec<PendingRead>,
    range_ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>,
) {
    let now = std::time::Instant::now();
    let resolved: BTreeMap<(u64, u64), CommitResolution> = pd.partition
        .take_resolved_commits()
        .into_iter()
        .map(|(index, term, resolution)| ((index, term), resolution))
        .collect();
    waits.retain_mut(|wait| {
        let resolution = resolved.get(&(wait.index, wait.term));
        match resolution {
            Some(CommitResolution::Committed) => {
                let map_expected = wait.map_queued;
                if map_expected {
                    wait.map_queued = false;
                    for (_, _, kind) in range_ops.iter_mut() {
                        if let RangeOpKind::InstallInitialMap {
                            creation_index, creation_term, authorized, ..
                        } = kind {
                            if *creation_index == wait.index && *creation_term == wait.term {
                                *authorized = true;
                                wait.map_queued = true;
                            }
                        }
                    }
                }
                let outcome = if wait.map_queued {
                    AdminOutcome::CommittedMapQueued { index: wait.index, term: wait.term }
                } else if map_expected {
                    AdminOutcome::CommittedMapDropped { index: wait.index, term: wait.term }
                } else {
                    AdminOutcome::Committed { index: wait.index, term: wait.term }
                };
                if let Some(reply) = wait.reply.take() {
                    let _ = reply.send(Ok(outcome));
                }
            }
            Some(CommitResolution::Superseded(committed_term)) => {
                if wait.map_queued {
                    discard_unconfirmed_map(range_ops, wait.index, wait.term);
                }
                if let Some(reply) = wait.reply.take() {
                    let _ = reply.send(Err(PartitionError::Superseded {
                        index: wait.index, term: wait.term, committed_term: *committed_term,
                    }));
                }
            }
            Some(CommitResolution::Unknown) => {
                if wait.map_queued {
                    discard_unconfirmed_map(range_ops, wait.index, wait.term);
                }
                if let Some(reply) = wait.reply.take() {
                    let _ = reply.send(Err(PartitionError::Unknown {
                        index: wait.index, term: wait.term,
                    }));
                }
            }
            None => {
                if wait.map_queued && pd.partition.term() != wait.term {
                    discard_unconfirmed_map(range_ops, wait.index, wait.term);
                    wait.map_queued = false;
                }
                if wait.deadline.is_some_and(|deadline| now >= deadline) {
                    if let Some(reply) = wait.reply.take() {
                        let _ = reply.send(Err(PartitionError::Unknown {
                            index: wait.index, term: wait.term,
                        }));
                    }
                }
                if wait.reply.is_some() || wait.map_queued {
                    return true;
                }
            }
        }
        pd.partition.unwatch_commit(wait.index, wait.term);
        false
    });
    for state in pd.partition.take_read_states() {
        if let Ok(context) = <[u8; 8]>::try_from(state.request_ctx.as_slice()) {
            if let Some(read) = reads.iter_mut()
                .find(|read| read.context == u64::from_le_bytes(context))
            {
                read.index = Some(state.index);
            }
        }
    }
    reads.retain(|read| {
        let status = pd.status();
        let authoritative = read.index.filter(|&index| {
            status.role == "leader" && status.term == read.term && status.applied >= index
                && now < read.deadline
        });
        if authoritative.is_some() || now >= read.deadline
            || status.role != "leader" || status.term != read.term
        {
            let _ = read.reply.send(inspection(pd, authoritative, read.with_state));
            false
        } else {
            true
        }
    });
}

pub(super) fn inspect<D: BlockDevice>(
    pd: &mut PlacementDriver<D>,
    reply: Sender<PdInspection>,
    with_state: bool,
    reads: &mut Vec<PendingRead>,
    next_context: &mut u64,
    timeout: Duration,
) {
    let status = pd.status();
    if status.role != "leader" {
        let _ = reply.send(inspection(pd, None, with_state));
        return;
    }
    *next_context = next_context.wrapping_add(1);
    let context = *next_context;
    if pd.partition.read_index(context.to_le_bytes().to_vec()).is_err() {
        let _ = reply.send(inspection(pd, None, with_state));
    } else {
        reads.push(PendingRead {
            context,
            index: None,
            term: status.term,
            deadline: std::time::Instant::now() + timeout,
            with_state,
            reply,
        });
    }
}

const RAFT_TICK: Duration = Duration::from_millis(100);

fn drive_step<D: BlockDevice>(pd: &mut PlacementDriver<D>, last_tick: &mut std::time::Instant) {
    if last_tick.elapsed() >= RAFT_TICK {
        pd.tick();
        *last_tick = std::time::Instant::now();
    }
    if let Err(e) = pd.drive() {
        crate::observe::counters().pd_drive_errors.inc();
        panic!("pd: raft drive failed; restart required: {e}");
    }
    if let Some((capacity, used, reserved)) = pd.state_capacity() {
        let gauges = crate::observe::gauges();
        gauges.pd_state_capacity_bytes.set(capacity as u64);
        gauges.pd_state_used_bytes.set(used as u64);
        gauges.pd_state_reserved_bytes.set(reserved as u64);
        gauges.pd_state_headroom_bytes.set(
            capacity.saturating_sub(used.saturating_add(reserved)) as u64,
        );
    }
}

impl PdDaemon {
    pub fn spawn_single<D>(
        pd_id: u64,
        engine: Engine<D>,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        let pd = PlacementDriver::new_single(pd_id, engine)?;
        Self::run_single(pd, ring, data_nodes)
    }

    pub fn spawn_single_with_settings<D>(
        pd_id: u64,
        engine: Engine<D>,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        settings.validate().map_err(PartitionError::Invariant)?;
        let pd = PlacementDriver::new_single(pd_id, engine)?;
        Self::run_single_with_settings(pd, ring, data_nodes, settings)
    }

    pub fn spawn_single_durable<D>(
        pd_id: u64,
        engine: Engine<D>,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        let pd = PlacementDriver::open_single(pd_id, engine, dir)?;
        Self::run_single(pd, ring, data_nodes)
    }

    pub fn spawn_single_durable_bounded(
        pd_id: u64,
        capacity: usize,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<PdDaemon, PartitionError> {
        Self::spawn_single_durable_bounded_with_settings(
            pd_id, capacity, ring, data_nodes, dir, PdPlacementSettings::default(),
        )
    }

    pub fn spawn_single_durable_bounded_with_settings(
        pd_id: u64,
        capacity: usize,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        dir: impl AsRef<std::path::Path>,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError> {
        settings.validate().map_err(PartitionError::Invariant)?;
        let pd = PlacementDriver::open_single_bounded(pd_id, capacity, dir)?;
        Self::run_single_with_settings(pd, ring, data_nodes, settings)
    }

    fn run_single<D>(
        pd: PlacementDriver<D>,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        Self::run_single_with_settings(pd, ring, data_nodes, PdPlacementSettings::default())
    }

    fn run_single_with_settings<D>(
        mut pd: PlacementDriver<D>,
        ring: RingConfig,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        settings.validate().map_err(PartitionError::Invariant)?;
        pd.set_global_rf(ring.rf as usize)?;
        let (cmd_tx, cmd_rx) = mpsc::channel::<PdCmd>();
        let admin_timeout_ms = Arc::new(std::sync::atomic::AtomicU64::new(5_000));
        let drive_admin_timeout_ms = admin_timeout_ms.clone();
        let mirrors = PdMirrors::new();
        let drive_mirrors = mirrors.clone();

        let balance_poll_authority = Arc::new(Mutex::new(BalancePollAuthority::default()));
        let (push_state, push_join) = spawn_pusher(
            data_nodes,
            ring,
            balance_poll_authority.clone(),
            cmd_tx.clone(),
            settings,
        );
        let drive_push_state = push_state.clone();
        let drive_balance_poll_authority = balance_poll_authority.clone();

        let drive_join = crate::observe::spawn_abort_on_panic("vals-pd-drive", move || {
            let mut last: AddressedMembers = Vec::new();
            let mut last_zones = Catalog::new();
            let mut last_range_maps: HashMap<ZoneId, RangeMap> = HashMap::new();
            let mut member_epoch: u64 = 0;
            let mut zone_epoch: u64 = 0;
            let mut range_map_epoch: u64 = 0;
            let mut pending_range_ops: Vec<(ZoneId, Vec<u8>, RangeOpKind)> = Vec::new();
            let mut deferred_commands = VecDeque::new();
            let mut admin_waits = Vec::new();
            let mut reads = Vec::new();
            let mut next_context = 0;
            let mut pending_zone_ids: Vec<ZoneId> = Vec::new();
            let mut control_mutation_through: u64 = 0;
            let mut balance_lock_clear_through: u64 = 0;
            let mut head_op_stalls: u32 = 0;
            let mut head_op_stall_key: Option<(ZoneId, Vec<u8>)> = None;
            let mut incarnation_minted_through: u64 = 0;
            let mut last_incarnation: Option<u128> = None;
            let mut last_tick = std::time::Instant::now();
            'drive: loop {
                loop {
                    if pending_range_ops.iter().any(|(_, _, kind)| {
                        matches!(kind, RangeOpKind::PrepareBalance(_, _))
                    }) {
                        while let Ok(command) = cmd_rx.try_recv() {
                            if matches!(&command, PdCmd::Shutdown) {
                                break 'drive;
                            }
                            deferred_commands.push_back(command);
                        }
                        break;
                    }
                    let command = deferred_commands.pop_front()
                        .map_or_else(|| cmd_rx.try_recv(), Ok);
                    match command {
                        Ok(PdCmd::Admin(mutation, deadline, reply)) => handle_admin(
                            &mut pd, mutation, deadline, reply, &mut pending_range_ops,
                            &mut admin_waits, &mut control_mutation_through,
                        ),
                        Ok(PdCmd::Status(reply)) => { let _ = reply.send(pd.status()); }
                        Ok(PdCmd::Inspect(reply, with_state)) => inspect(
                            &mut pd, reply, with_state, &mut reads, &mut next_context,
                            Duration::from_millis(drive_admin_timeout_ms.load(std::sync::atomic::Ordering::Relaxed)),
                        ),
                        Ok(PdCmd::Bootstrap(members)) => {
                            if let Err(e) = pd.bootstrap_members(&members) {
                                crate::observe::counters().pd_propose_errors.inc();
                                if matches!(&e, PartitionError::Invariant(_)) {
                                    log::warn!("pd: boot membership rejected: {e}");
                                } else {
                                    log::debug!("pd: boot membership proposal failed (retried): {e}");
                                    deferred_commands.push_front(PdCmd::Bootstrap(members));
                                    break;
                                }
                            } else {
                                control_mutation_through =
                                    control_mutation_through.max(pd.last_index());
                            }
                        }
                        Ok(PdCmd::Add(n, w, addrs)) => {
                            if let Err(e) = pd.add_node_addressed(n, w, &addrs) {
                                crate::observe::counters().pd_propose_errors.inc();
                                if matches!(&e, PartitionError::Invariant(_)) {
                                    log::warn!("pd: add_node({n}, weight {w}) rejected: {e}");
                                } else {
                                    log::debug!(
                                        "pd: add_node({n}, weight {w}) proposal failed (retried): {e}"
                                    );
                                    deferred_commands.push_front(PdCmd::Add(n, w, addrs));
                                    break;
                                }
                            } else {
                                control_mutation_through = control_mutation_through.max(pd.last_index());
                            }
                        }
                        Ok(PdCmd::CreateZone(cfg, boundaries)) => {
                            match pd.create_zone(cfg.clone()) {
                                Err(e) if matches!(&e, PartitionError::Invariant(_)) => {
                                    log::warn!("pd: create_zone rejected: {e}");
                                    crate::observe::counters().pd_propose_errors.inc();
                                }
                                Err(e) => {
                                    log::debug!("pd: create_zone proposal failed (retried): {e}");
                                    crate::observe::counters().pd_propose_errors.inc();
                                    deferred_commands
                                        .push_front(PdCmd::CreateZone(cfg, boundaries));
                                    break;
                                }
                                Ok(id) => {
                                    control_mutation_through =
                                        control_mutation_through.max(pd.last_index());
                                    pending_zone_ids.push(id);
                                    if !boundaries.is_empty() {
                                        let legacy = RangeMap::from_splits(boundaries);
                                        pending_range_ops.push((
                                            id,
                                            Vec::new(),
                                            RangeOpKind::InstallInitialMap {
                                                map: legacy.clone(),
                                                legacy,
                                                cfg,
                                                creation_index: pd.last_index(),
                                                creation_term: pd.partition.term(),
                                                authorized: false,
                                                placement: InitialMapPlacement::Unresolved,
                                            },
                                        ));
                                        watch_boot_map(&mut pd, &mut admin_waits);
                                    }
                                }
                            }
                        }
                        Ok(PdCmd::Shutdown) => {
                            log::info!("pd: graceful shutdown; drive loop exiting");
                            break 'drive;
                        }
                        Ok(PdCmd::SplitZone(id, at, mode)) => {
                            let target = if mode == SplitMode::Canonical {
                                CanonicalTarget::Unresolved
                            } else {
                                CanonicalTarget::Legacy
                            };
                            pending_range_ops.push((id, at, RangeOpKind::Split(mode, target)))
                        }
                        Ok(PdCmd::MergeZone(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::Merge))
                        }
                        Ok(PdCmd::MergeZones(id, boundaries)) => {
                            let at = boundaries.first().cloned().unwrap_or_default();
                            pending_range_ops.push((id, at, RangeOpKind::MergeAll(boundaries)))
                        }
                        Ok(PdCmd::PrepareRehome(id, at)) => {
                            pending_range_ops.push((
                                id,
                                at,
                                RangeOpKind::PrepareRehome(CanonicalTarget::Unresolved),
                            ))
                        }
                        Ok(PdCmd::PrepareBalance(proposal)) => {
                            pending_range_ops.push((
                                proposal.zone_id,
                                proposal.lower.clone(),
                                RangeOpKind::PrepareBalance(
                                    proposal,
                                    BalancePreparePhase::Acquire,
                                ),
                            ))
                        }
                        Ok(PdCmd::EnableOpaque(upgrade)) => {
                            pending_range_ops.push((
                                upgrade.zone_id,
                                Vec::new(),
                                RangeOpKind::EnableOpaque(upgrade),
                            ))
                        }
                        Ok(PdCmd::AbortRehome(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::AbortRehome))
                        }
                        Ok(PdCmd::CommitRehome(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::CommitRehome))
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break 'drive,
                    }
                }
                let installing_new_zone = pending_range_ops.first().is_some_and(|(_, _, kind)| {
                    matches!(kind, RangeOpKind::InstallInitialMap { .. })
                });
                let loop_settled = control_mutations_settled(
                    pd.epoch(),
                    control_mutation_through.max(pd.last_index()),
                    false,
                    false,
                );
                let control_settled = control_mutations_settled(
                    pd.epoch(),
                    control_mutation_through,
                    false,
                    false,
                );
                if installing_new_zone || loop_settled {
                while !pending_range_ops.is_empty() {
                    let map_decision = initial_map_queue_head(&pd, &mut pending_range_ops);
                    if map_decision == InitialMapDecision::Discard {
                        continue;
                    }
                    {
                        let (id, at, kind) = pending_range_ops.first_mut().unwrap();
                        if !resolve_initial_map(
                            &pd,
                            ring,
                            &drive_push_state,
                            &drive_balance_poll_authority,
                            *id,
                            control_settled,
                            kind,
                        ) {
                            break;
                        }
                        resolve_canonical_target(
                            &pd,
                            ring,
                            &drive_push_state,
                            &drive_balance_poll_authority,
                            *id,
                            at,
                            kind,
                        );
                    }
                    let (id, at, kind) = pending_range_ops.first().cloned().unwrap();
                    let map = pd.range_map(id);
                    let reflected = range_op_reflected(&map, &at, &kind);
                    if reflected {
                        if let RangeOpKind::PrepareBalance(proposal, _) = &kind {
                            set_balance_prepare_result(&drive_push_state, proposal, true);
                        }
                        pending_range_ops.remove(0);
                        continue;
                    }
                    if let RangeOpKind::PrepareBalance(proposal, phase) = &kind {
                        let set_phase = |ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>, next| {
                            if let Some((_, _, RangeOpKind::PrepareBalance(_, phase))) = ops.first_mut() {
                                *phase = next;
                            }
                        };
                        match *phase {
                            BalancePreparePhase::Acquire => {
                                let state = drive_push_state.lock().unwrap().clone();
                                let authority = drive_balance_poll_authority.lock().unwrap();
                                if let Err(reason) = validate_balance_proposal(
                                    &pd, &state, &authority, ring, proposal
                                ) {
                                    record_balance_stale(reason, proposal);
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                                match pd.acquire_balance_intent(proposal.identity) {
                                    Ok(index) => set_phase(
                                        &mut pending_range_ops,
                                        BalancePreparePhase::AcquireSubmitted(index),
                                    ),
                                    Err(e) => {
                                        log::warn!("pd: balance intent proposal failed (retried): {e}");
                                        crate::observe::counters().pd_propose_errors.inc();
                                    }
                                }
                            }
                            BalancePreparePhase::AcquireSubmitted(submitted) => {
                                if pd.balance_intent().ok().flatten() == Some(proposal.identity) {
                                    set_phase(&mut pending_range_ops, BalancePreparePhase::IntentReflected);
                                    continue;
                                }
                                if pd.epoch() >= submitted {
                                    record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                            }
                            BalancePreparePhase::IntentReflected => {
                                let state = drive_push_state.lock().unwrap().clone();
                                let authority = drive_balance_poll_authority.lock().unwrap();
                                if let Err(reason) = validate_balance_proposal(
                                    &pd, &state, &authority, ring, proposal
                                ) {
                                    record_balance_stale(reason, proposal);
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::warn!("pd: stale balance intent clear failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                } else {
                                    match pd.prepare_balance_map(
                                        id, &at, &proposal.token, &proposal.map, proposal.identity
                                    ) {
                                        Ok(Some(index)) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::MapSubmitted(index),
                                        ),
                                        Ok(None) => {
                                            record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                            match pd.clear_balance_intent(proposal.identity) {
                                                Ok(index) => set_phase(
                                                    &mut pending_range_ops,
                                                    BalancePreparePhase::Clearing(index),
                                                ),
                                                Err(e) => {
                                                    log::warn!("pd: rejected balance intent clear failed (retried): {e}");
                                                    crate::observe::counters().pd_propose_errors.inc();
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!("pd: balance map proposal failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                            BalancePreparePhase::MapSubmitted(submitted) => {
                                if pd.epoch() >= submitted {
                                    record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::warn!("pd: failed balance map intent clear failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                            BalancePreparePhase::Clearing(submitted) => {
                                if pd.balance_intent().ok().flatten() != Some(proposal.identity) {
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                                if pd.epoch() >= submitted {
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::warn!("pd: balance intent clear retry failed: {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    }
                    if let RangeOpKind::EnableOpaque(upgrade) = &kind {
                        let state = drive_push_state.lock().unwrap().clone();
                        if let Err(reason) = validate_balance_upgrade(&pd, &state, ring, upgrade) {
                            record_balance_upgrade_stale(reason, upgrade.zone_id);
                            pending_range_ops.remove(0);
                            continue;
                        }
                    }
                    revalidate_queued_placement(
                        &drive_push_state,
                        &pd.members_addressed(),
                        control_settled,
                        &mut pending_range_ops.first_mut().unwrap().2,
                    );
                    let kind = pending_range_ops.first().unwrap().2.clone();
                    if refuse_populated_canonical_split(
                        &pd,
                        &drive_push_state,
                        id,
                        &at,
                        &kind,
                    ) {
                        pending_range_ops.remove(0);
                        head_op_stalls = 0;
                        head_op_stall_key = None;
                        continue;
                    }
                    let head_key = (id, at.to_vec());
                    if head_op_stall_key.as_ref() != Some(&head_key) {
                        head_op_stalls = 0;
                        head_op_stall_key = Some(head_key);
                    }
                    let proposed = propose_range_op(&mut pd, id, &at, &kind);
                    match proposed {
                        Ok(true) => head_op_stalls = 0,
                        Ok(false) => {
                            head_op_stalls += 1;
                            if head_op_stalls >= 400 {
                                log::warn!(
                                    "pd: range-op on zone {id} stalled {} polls without \
                                     proposing or reflecting; dropping (queue {})",
                                    head_op_stalls,
                                    pending_range_ops.len()
                                );
                                crate::observe::counters().pd_range_ops_dropped.inc();
                                pending_range_ops.remove(0);
                                head_op_stalls = 0;
                                head_op_stall_key = None;
                            }
                        }
                        Err(e) => {
                            log::warn!("pd: range-op on zone {id} proposal failed (retried): {e}");
                            crate::observe::counters().pd_propose_errors.inc();
                        }
                    }
                    break;
                }
                }
                drive_step(&mut pd, &mut last_tick);
                settle_admin(&mut pd, &mut admin_waits, &mut reads, &mut pending_range_ops);
                ensure_incarnation(&mut pd, &mut incarnation_minted_through);
                if pending_range_ops.is_empty()
                    && pd.epoch() >= balance_lock_clear_through
                    && pd.range_maps().values().all(|map| map.active_rehomes() == 0)
                {
                    if pd.clear_orphan_balance_lock().unwrap_or(false) {
                        balance_lock_clear_through = pd.last_index();
                    }
                }
                if pending_range_ops.is_empty() && pd.is_leader() && pd.epoch() >= pd.last_index() {
                    let _ = pd.collect_orphan_range_maps();
                }
                let committed_zones = pd.zones();
                pending_zone_ids.retain(|id| !committed_zones.contains(*id));

                let epoch_now = pd.epoch();
                let changed = mirror_if_changed(
                    pd.members_addressed(),
                    &mut last,
                    &mut member_epoch,
                    epoch_now,
                    &drive_mirrors.members,
                ) | mirror_if_changed(
                    pd.zones(),
                    &mut last_zones,
                    &mut zone_epoch,
                    epoch_now,
                    &drive_mirrors.zones,
                ) | mirror_if_changed(
                    pd.range_maps(),
                    &mut last_range_maps,
                    &mut range_map_epoch,
                    epoch_now,
                    &drive_mirrors.range_maps,
                );
                publish_incarnation(&drive_push_state, pd.incarnation(), &mut last_incarnation);
                publish_push_state(
                    &drive_push_state,
                    changed,
                    member_epoch,
                    &last,
                    zone_epoch,
                    &last_zones,
                    range_map_epoch,
                    &last_range_maps,
                    true,
                    pending_range_ops.iter().any(|(_, _, kind)| {
                        matches!(kind, RangeOpKind::InstallInitialMap { .. })
                    }),
                );
                thread::sleep(Duration::from_millis(5));
            }
            drive_push_state.lock().unwrap().stop = true;
        });

        Ok(PdDaemon {
            cmd: cmd_tx,
            admin_timeout_ms,
            mirrors,
            drive_join,
            push_join,
            push_state,
            accept_join: None,
            accept_stop: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            raft_addr: None,
        })
    }

    pub fn bind() -> io::Result<(TcpListener, SocketAddr)> {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        let addr = listener.local_addr()?;
        Ok((listener, addr))
    }

    pub fn spawn<D>(
        pd_id: u64,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        engine: Engine<D>,
        ring: RingConfig,
        campaign: bool,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        let voters: Vec<u64> = pd_peers.iter().map(|&(id, _)| id).collect();
        let mut pd = PlacementDriver::new(pd_id, voters, engine)?;
        if campaign {
            pd.campaign()?;
        }
        Self::run_multi(pd, listener, pd_peers, data_nodes, ring)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_durable<D>(
        pd_id: u64,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        engine: Engine<D>,
        ring: RingConfig,
        campaign: bool,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        let voters: Vec<u64> = pd_peers.iter().map(|&(id, _)| id).collect();
        let mut pd = PlacementDriver::open(pd_id, voters, engine, dir)?;
        if campaign && pd.partition.term() == 0 {
            pd.campaign()?;
        }
        Self::run_multi(pd, listener, pd_peers, data_nodes, ring)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_durable_bounded(
        pd_id: u64,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        capacity: usize,
        ring: RingConfig,
        campaign: bool,
        dir: impl AsRef<std::path::Path>,
    ) -> Result<PdDaemon, PartitionError> {
        Self::spawn_durable_bounded_with_settings(
            pd_id, listener, pd_peers, data_nodes, capacity, ring, campaign, dir,
            PdPlacementSettings::default(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_durable_bounded_with_settings(
        pd_id: u64,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        capacity: usize,
        ring: RingConfig,
        campaign: bool,
        dir: impl AsRef<std::path::Path>,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError> {
        settings.validate().map_err(PartitionError::Invariant)?;
        let voters: Vec<u64> = pd_peers.iter().map(|&(id, _)| id).collect();
        let mut pd = PlacementDriver::open_bounded(pd_id, voters, capacity, dir)?;
        if campaign && pd.partition.term() == 0 {
            pd.campaign()?;
        }
        Self::run_multi_with_settings(pd, listener, pd_peers, data_nodes, ring, settings)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn spawn_joining_bounded_with_settings(
        pd_id: u64,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        capacity: usize,
        ring: RingConfig,
        dir: impl AsRef<std::path::Path>,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError> {
        settings.validate().map_err(PartitionError::Invariant)?;
        let pd = PlacementDriver::open_bounded_joining(pd_id, capacity, dir)?;
        if pd.partition.is_joining() {
            log::info!(
                "pd {pd_id}: empty raft directory joins an existing group; it waits for a leader to \
                 add it as a learner (pd-add-learner) and send it a snapshot"
            );
        }
        Self::run_multi_with_settings(pd, listener, pd_peers, data_nodes, ring, settings)
    }

    fn run_multi<D>(
        pd: PlacementDriver<D>,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        ring: RingConfig,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        Self::run_multi_with_settings(
            pd, listener, pd_peers, data_nodes, ring, PdPlacementSettings::default(),
        )
    }

    fn run_multi_with_settings<D>(
        mut pd: PlacementDriver<D>,
        listener: TcpListener,
        pd_peers: Vec<(u64, SocketAddr)>,
        data_nodes: Vec<(NodeId, Vec<SocketAddr>)>,
        ring: RingConfig,
        settings: PdPlacementSettings,
    ) -> Result<PdDaemon, PartitionError>
    where
        D: BlockDevice + Send + 'static,
    {
        settings.validate().map_err(PartitionError::Invariant)?;
        pd.set_global_rf(ring.rf as usize)?;
        let (cmd_tx, cmd_rx) = mpsc::channel::<PdCmd>();
        let admin_timeout_ms = Arc::new(std::sync::atomic::AtomicU64::new(5_000));
        let drive_admin_timeout_ms = admin_timeout_ms.clone();
        let (inbox_tx, inbox_rx) = mpsc::channel::<(
            Message,
            crate::transport::Credit,
            crate::transport::ByteCredit,
        )>();
        let mirrors = PdMirrors::new();
        let drive_mirrors = mirrors.clone();

        let raft_addr = listener.local_addr().ok();
        let accept_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let accept_stop_rx = accept_stop.clone();
        let accept_join = crate::observe::spawn_abort_on_panic("vals-pd-accept", move || {
            raft_accept_loop(listener, inbox_tx, accept_stop_rx)
        });

        let config_peers: HashMap<u64, SocketAddr> = pd_peers.into_iter().collect();
        let mut peer_addr = config_peers.clone();
        let mut peer_conf_version = None;

        let balance_poll_authority = Arc::new(Mutex::new(BalancePollAuthority::default()));
        let (push_state, push_join) = spawn_pusher(
            data_nodes,
            ring,
            balance_poll_authority.clone(),
            cmd_tx.clone(),
            settings,
        );
        let drive_push_state = push_state.clone();
        let drive_balance_poll_authority = balance_poll_authority.clone();

        let drive_join = crate::observe::spawn_abort_on_panic("vals-pd-drive", move || {
            let mut raft_peers = RaftPeers::default();
            let mut last: AddressedMembers = Vec::new();
            let mut last_zones = Catalog::new();
            let mut last_range_maps: HashMap<ZoneId, RangeMap> = HashMap::new();
            let mut member_epoch: u64 = 0;
            let mut zone_epoch: u64 = 0;
            let mut range_map_epoch: u64 = 0;
            let mut last_leader = false;
            let mut pending_bootstrap: Option<AddressedMembers> = None;
            let mut pending: Vec<(NodeId, u32, Vec<SocketAddr>, bool)> = Vec::new();
            let mut pending_zones: Vec<(Option<ZoneId>, Option<ZoneConfig>, Vec<Vec<u8>>)> =
                Vec::new();
            let mut pending_range_ops: Vec<(ZoneId, Vec<u8>, RangeOpKind)> = Vec::new();
            let mut deferred_commands = VecDeque::new();
            let mut admin_waits = Vec::new();
            let mut reads = Vec::new();
            let mut next_context = 0;
            let mut control_mutation_through: u64 = 0;
            let mut balance_lock_clear_through: u64 = 0;
            let mut head_op_stalls: u32 = 0;
            let mut head_op_stall_key: Option<(ZoneId, Vec<u8>)> = None;
            let mut incarnation_minted_through: u64 = 0;
            let mut last_incarnation: Option<u128> = None;
            let mut last_tick = std::time::Instant::now();
            'drive: loop {
                loop {
                    if pending_range_ops.iter().any(|(_, _, kind)| {
                        matches!(kind, RangeOpKind::PrepareBalance(_, _))
                    }) {
                        while let Ok(command) = cmd_rx.try_recv() {
                            if matches!(&command, PdCmd::Shutdown) {
                                break 'drive;
                            }
                            deferred_commands.push_back(command);
                        }
                        break;
                    }
                    let command = deferred_commands.pop_front()
                        .map_or_else(|| cmd_rx.try_recv(), Ok);
                    match command {
                        Ok(PdCmd::Admin(mutation, deadline, reply)) => handle_admin(
                            &mut pd, mutation, deadline, reply, &mut pending_range_ops,
                            &mut admin_waits, &mut control_mutation_through,
                        ),
                        Ok(PdCmd::Status(reply)) => { let _ = reply.send(pd.status()); }
                        Ok(PdCmd::Inspect(reply, with_state)) => inspect(
                            &mut pd, reply, with_state, &mut reads, &mut next_context,
                            Duration::from_millis(drive_admin_timeout_ms.load(std::sync::atomic::Ordering::Relaxed)),
                        ),
                        Ok(PdCmd::Bootstrap(members)) => pending_bootstrap = Some(members),
                        Ok(PdCmd::Add(n, w, addrs)) => pending.push((n, w, addrs, true)),
                        Ok(PdCmd::CreateZone(cfg, boundaries)) => {
                            pending_zones.push((None, Some(cfg), boundaries));
                        }
                        Ok(PdCmd::Shutdown) => {
                            log::info!("pd: graceful shutdown; drive loop exiting");
                            break 'drive;
                        }
                        Ok(PdCmd::SplitZone(id, at, mode)) => {
                            let target = if mode == SplitMode::Canonical {
                                CanonicalTarget::Unresolved
                            } else {
                                CanonicalTarget::Legacy
                            };
                            pending_range_ops.push((id, at, RangeOpKind::Split(mode, target)))
                        }
                        Ok(PdCmd::MergeZone(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::Merge))
                        }
                        Ok(PdCmd::MergeZones(id, boundaries)) => {
                            let at = boundaries.first().cloned().unwrap_or_default();
                            pending_range_ops.push((id, at, RangeOpKind::MergeAll(boundaries)))
                        }
                        Ok(PdCmd::PrepareRehome(id, at)) => {
                            pending_range_ops.push((
                                id,
                                at,
                                RangeOpKind::PrepareRehome(CanonicalTarget::Unresolved),
                            ))
                        }
                        Ok(PdCmd::PrepareBalance(proposal)) => {
                            pending_range_ops.push((
                                proposal.zone_id,
                                proposal.lower.clone(),
                                RangeOpKind::PrepareBalance(
                                    proposal,
                                    BalancePreparePhase::Acquire,
                                ),
                            ))
                        }
                        Ok(PdCmd::EnableOpaque(upgrade)) => {
                            pending_range_ops.push((
                                upgrade.zone_id,
                                Vec::new(),
                                RangeOpKind::EnableOpaque(upgrade),
                            ))
                        }
                        Ok(PdCmd::AbortRehome(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::AbortRehome))
                        }
                        Ok(PdCmd::CommitRehome(id, at)) => {
                            pending_range_ops.push((id, at, RangeOpKind::CommitRehome))
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break 'drive,
                    }
                }
                while let Ok((msg, _request_credit, _byte_credit)) = inbox_rx.try_recv() {
                    if let Err(e) = pd.step(msg) {
                        log::warn!("pd: raft step on an inbound peer message failed: {e}");
                        crate::observe::counters().pd_drive_errors.inc();
                    }
                }
                drive_step(&mut pd, &mut last_tick);
                settle_admin(&mut pd, &mut admin_waits, &mut reads, &mut pending_range_ops);
                ensure_incarnation(&mut pd, &mut incarnation_minted_through);
                if peer_conf_version != Some(pd.partition.conf_version()) {
                    peer_conf_version = Some(pd.partition.conf_version());
                    peer_addr = resolve_pd_peers(&pd, &config_peers);
                    raft_peers.retain(&peer_addr);
                }
                for msg in pd.take_outgoing() {
                    if let Some(&addr) = peer_addr.get(&msg.to) {
                        raft_peers.send(msg.to, addr, &msg);
                    }
                }

                let now = pd.members_addressed();
                if let Some(members) = pending_bootstrap.as_ref() {
                    let mut expected = members.clone();
                    expected.sort_unstable_by_key(|(node, _, _)| *node);
                    if now == expected {
                        pending_bootstrap = None;
                    } else {
                        match pd.bootstrap_members(members) {
                            Ok(()) => {
                                control_mutation_through =
                                    control_mutation_through.max(pd.last_index());
                            }
                            Err(e) if matches!(&e, PartitionError::Invariant(_)) => {
                                log::warn!("pd: boot membership rejected: {e}");
                                pending_bootstrap = None;
                            }
                            Err(e) => {
                                log::debug!(
                                    "pd: boot membership proposal failed (retried): {e}"
                                );
                                crate::observe::counters().pd_propose_errors.inc();
                            }
                        }
                    }
                }
                let mut live_nodes: std::collections::HashSet<NodeId> =
                    std::collections::HashSet::new();
                if pending_bootstrap.is_none() {
                    pending.retain(|(n, w, addrs, is_add)| {
                    let (n, w) = (*n, *w);
                    if !live_nodes.insert(n) {
                        return true;
                    }
                    let reflected = if *is_add {
                        now.iter()
                            .any(|(m, mw, ma)| *m == n && *mw == w && ma == addrs)
                    } else {
                        !now.iter().any(|(m, _, _)| *m == n)
                    };
                    if reflected {
                        live_nodes.remove(&n);
                        return false;
                    }
                    let proposed = if *is_add {
                        pd.add_node_addressed(n, w, addrs)
                    } else {
                        pd.remove_node(n)
                    };
                    if let Err(e) = proposed {
                        crate::observe::counters().pd_propose_errors.inc();
                        if matches!(&e, PartitionError::Invariant(_)) {
                            log::warn!("pd: rejected membership change for node {n}: {e}");
                            live_nodes.remove(&n);
                            return false;
                        }
                        log::debug!(
                            "pd: membership re-proposal for node {n} failed (retried): {e}"
                        );
                    } else {
                        control_mutation_through = control_mutation_through.max(pd.last_index());
                    }
                        true
                    });
                }

                let epoch_now = pd.epoch();
                let members_changed = mirror_if_changed(
                    now,
                    &mut last,
                    &mut member_epoch,
                    epoch_now,
                    &drive_mirrors.members,
                );

                let now_zones = pd.zones();
                let mut live_zones: std::collections::HashSet<ZoneId> =
                    std::collections::HashSet::new();
                let mut rejected_zones = Vec::new();
                if pending_bootstrap.is_none() && pending.is_empty() {
                    pending_zones.retain_mut(|(id, cfg, boundaries)| {
                    if let Some(zone_id) = *id {
                        if !live_zones.insert(zone_id) {
                            return true;
                        }
                        let reflected = match cfg {
                            Some(c) => now_zones.get(zone_id) == Some(c),
                            None => !now_zones.contains(zone_id),
                        };
                        if reflected {
                            live_zones.remove(&zone_id);
                            return false;
                        }
                        if cfg.is_some() && now_zones.contains(zone_id) {
                            live_zones.remove(&zone_id);
                            *id = None;
                        }
                    }
                    let proposed = match cfg {
                        Some(c) if id.is_none() => pd.create_zone(c.clone()).map(Some),
                        Some(c) => pd.propose_zone(id.unwrap(), c).map(|_| None),
                        None => pd.drop_zone(id.unwrap()).map(|_| None),
                    };
                    match proposed {
                        Ok(Some(zone_id)) => {
                            *id = Some(zone_id);
                            live_zones.insert(zone_id);
                            if !boundaries.is_empty() {
                                let legacy = RangeMap::from_splits(std::mem::take(boundaries));
                                pending_range_ops.push((
                                    zone_id,
                                    Vec::new(),
                                    RangeOpKind::InstallInitialMap {
                                        map: legacy.clone(),
                                        legacy,
                                        cfg: cfg.as_ref().unwrap().clone(),
                                        creation_index: pd.last_index(),
                                        creation_term: pd.partition.term(),
                                        authorized: false,
                                        placement: InitialMapPlacement::Unresolved,
                                    },
                                ));
                                watch_boot_map(&mut pd, &mut admin_waits);
                            }
                            control_mutation_through =
                                control_mutation_through.max(pd.last_index());
                        }
                        Ok(None) => {
                            control_mutation_through =
                                control_mutation_through.max(pd.last_index());
                        }
                        Err(e) => {
                            crate::observe::counters().pd_propose_errors.inc();
                            if matches!(&e, PartitionError::Invariant(_)) {
                                let label = id.map_or_else(
                                    || "new zone".to_string(),
                                    |zone_id| format!("zone {zone_id}"),
                                );
                                log::warn!("pd: rejected zone change for {label}: {e}");
                                if let Some(zone_id) = *id {
                                    rejected_zones.push(zone_id);
                                    live_zones.remove(&zone_id);
                                }
                                return false;
                            }
                            log::debug!("pd: zone proposal failed (retried): {e}");
                        }
                    }
                        true
                    });
                }
                if !rejected_zones.is_empty() {
                    pending_range_ops.retain(|(id, _, kind)| {
                        !rejected_zones.contains(id)
                            || !matches!(kind, RangeOpKind::InstallInitialMap { .. })
                    });
                }
                let zones_changed = mirror_if_changed(
                    now_zones,
                    &mut last_zones,
                    &mut zone_epoch,
                    epoch_now,
                    &drive_mirrors.zones,
                );

                let installing_new_zone = pending_range_ops.first().is_some_and(|(_, _, kind)| {
                    matches!(kind, RangeOpKind::InstallInitialMap { .. })
                });
                let loop_settled = control_mutations_settled(
                    pd.epoch(),
                    control_mutation_through.max(pd.last_index()),
                    pending_bootstrap.is_some() || !pending.is_empty(),
                    !pending_zones.is_empty(),
                );
                let control_settled = control_mutations_settled(
                    pd.epoch(),
                    control_mutation_through,
                    pending_bootstrap.is_some() || !pending.is_empty(),
                    !pending_zones.is_empty(),
                );
                if installing_new_zone || loop_settled {
                while !pending_range_ops.is_empty() {
                    let map_decision = initial_map_queue_head(&pd, &mut pending_range_ops);
                    if map_decision == InitialMapDecision::Discard {
                        continue;
                    }
                    {
                        let (id, at, kind) = pending_range_ops.first_mut().unwrap();
                        if !resolve_initial_map(
                            &pd,
                            ring,
                            &drive_push_state,
                            &drive_balance_poll_authority,
                            *id,
                            control_settled,
                            kind,
                        ) {
                            break;
                        }
                        resolve_canonical_target(
                            &pd,
                            ring,
                            &drive_push_state,
                            &drive_balance_poll_authority,
                            *id,
                            at,
                            kind,
                        );
                    }
                    let (id, at, kind) = pending_range_ops.first().cloned().unwrap();
                    let map = pd.range_map(id);
                    let reflected = range_op_reflected(&map, &at, &kind);
                    if reflected {
                        if let RangeOpKind::PrepareBalance(proposal, _) = &kind {
                            set_balance_prepare_result(&drive_push_state, proposal, true);
                        }
                        pending_range_ops.remove(0);
                        continue;
                    }
                    if let RangeOpKind::PrepareBalance(proposal, phase) = &kind {
                        let set_phase = |ops: &mut Vec<(ZoneId, Vec<u8>, RangeOpKind)>, next| {
                            if let Some((_, _, RangeOpKind::PrepareBalance(_, phase))) = ops.first_mut() {
                                *phase = next;
                            }
                        };
                        match *phase {
                            BalancePreparePhase::Acquire => {
                                let state = drive_push_state.lock().unwrap().clone();
                                let authority = drive_balance_poll_authority.lock().unwrap();
                                if let Err(reason) = validate_balance_proposal(
                                    &pd, &state, &authority, ring, proposal
                                ) {
                                    record_balance_stale(reason, proposal);
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                                match pd.acquire_balance_intent(proposal.identity) {
                                    Ok(index) => set_phase(
                                        &mut pending_range_ops,
                                        BalancePreparePhase::AcquireSubmitted(index),
                                    ),
                                    Err(e) => {
                                        log::debug!("pd: balance intent proposal failed (retried): {e}");
                                        crate::observe::counters().pd_propose_errors.inc();
                                    }
                                }
                            }
                            BalancePreparePhase::AcquireSubmitted(submitted) => {
                                if pd.balance_intent().ok().flatten() == Some(proposal.identity) {
                                    set_phase(&mut pending_range_ops, BalancePreparePhase::IntentReflected);
                                    continue;
                                }
                                if pd.epoch() >= submitted {
                                    record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                            }
                            BalancePreparePhase::IntentReflected => {
                                let state = drive_push_state.lock().unwrap().clone();
                                let authority = drive_balance_poll_authority.lock().unwrap();
                                if let Err(reason) = validate_balance_proposal(
                                    &pd, &state, &authority, ring, proposal
                                ) {
                                    record_balance_stale(reason, proposal);
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::debug!("pd: stale balance intent clear failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                } else {
                                    match pd.prepare_balance_map(
                                        id, &at, &proposal.token, &proposal.map, proposal.identity
                                    ) {
                                        Ok(Some(index)) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::MapSubmitted(index),
                                        ),
                                        Ok(None) => {
                                            record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                            match pd.clear_balance_intent(proposal.identity) {
                                                Ok(index) => set_phase(
                                                    &mut pending_range_ops,
                                                    BalancePreparePhase::Clearing(index),
                                                ),
                                                Err(e) => {
                                                    log::debug!("pd: rejected balance intent clear failed (retried): {e}");
                                                    crate::observe::counters().pd_propose_errors.inc();
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            log::debug!("pd: balance map proposal failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                            BalancePreparePhase::MapSubmitted(submitted) => {
                                if pd.epoch() >= submitted {
                                    record_balance_stale(BalanceStaleReason::RangeMap, proposal);
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::debug!("pd: failed balance map intent clear failed (retried): {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                            BalancePreparePhase::Clearing(submitted) => {
                                if pd.balance_intent().ok().flatten() != Some(proposal.identity) {
                                    set_balance_prepare_result(&drive_push_state, proposal, false);
                                    pending_range_ops.remove(0);
                                    continue;
                                }
                                if pd.epoch() >= submitted {
                                    match pd.clear_balance_intent(proposal.identity) {
                                        Ok(index) => set_phase(
                                            &mut pending_range_ops,
                                            BalancePreparePhase::Clearing(index),
                                        ),
                                        Err(e) => {
                                            log::debug!("pd: balance intent clear retry failed: {e}");
                                            crate::observe::counters().pd_propose_errors.inc();
                                        }
                                    }
                                }
                            }
                        }
                        break;
                    }
                    if let RangeOpKind::EnableOpaque(upgrade) = &kind {
                        let state = drive_push_state.lock().unwrap().clone();
                        if let Err(reason) = validate_balance_upgrade(&pd, &state, ring, upgrade) {
                            record_balance_upgrade_stale(reason, upgrade.zone_id);
                            pending_range_ops.remove(0);
                            continue;
                        }
                    }
                    revalidate_queued_placement(
                        &drive_push_state,
                        &pd.members_addressed(),
                        control_settled,
                        &mut pending_range_ops.first_mut().unwrap().2,
                    );
                    let kind = pending_range_ops.first().unwrap().2.clone();
                    if refuse_populated_canonical_split(
                        &pd,
                        &drive_push_state,
                        id,
                        &at,
                        &kind,
                    ) {
                        pending_range_ops.remove(0);
                        head_op_stalls = 0;
                        head_op_stall_key = None;
                        continue;
                    }
                    let head_key = (id, at.to_vec());
                    if head_op_stall_key.as_ref() != Some(&head_key) {
                        head_op_stalls = 0;
                        head_op_stall_key = Some(head_key);
                    }
                    let proposed = propose_range_op(&mut pd, id, &at, &kind);
                    match proposed {
                        Ok(true) => head_op_stalls = 0,
                        Ok(false) => {
                            head_op_stalls += 1;
                            if head_op_stalls >= 400 {
                                log::warn!(
                                    "pd: range-op on zone {id} stalled {} polls without \
                                     proposing or reflecting; dropping (queue {})",
                                    head_op_stalls,
                                    pending_range_ops.len()
                                );
                                crate::observe::counters().pd_range_ops_dropped.inc();
                                pending_range_ops.remove(0);
                                head_op_stalls = 0;
                                head_op_stall_key = None;
                            }
                        }
                        Err(e) => {
                            log::debug!("pd: range-op on zone {id} re-proposal failed (retried): {e}");
                            crate::observe::counters().pd_propose_errors.inc();
                        }
                    }
                    break;
                }
                }
                if pending_range_ops.is_empty()
                    && pd.is_leader()
                    && pd.epoch() >= balance_lock_clear_through
                    && pd.range_maps().values().all(|map| map.active_rehomes() == 0)
                {
                    if pd.clear_orphan_balance_lock().unwrap_or(false) {
                        balance_lock_clear_through = pd.last_index();
                    }
                }
                if pending_range_ops.is_empty() && pd.is_leader() && pd.epoch() >= pd.last_index() {
                    let _ = pd.collect_orphan_range_maps();
                }
                let now_range_maps = pd.range_maps();
                let range_maps_changed = mirror_if_changed(
                    now_range_maps,
                    &mut last_range_maps,
                    &mut range_map_epoch,
                    epoch_now,
                    &drive_mirrors.range_maps,
                );

                let leader_now = pd.is_leader();
                publish_incarnation(&drive_push_state, pd.incarnation(), &mut last_incarnation);
                publish_push_state(
                    &drive_push_state,
                    members_changed
                        || zones_changed
                        || range_maps_changed
                        || leader_now != last_leader,
                    member_epoch,
                    &last,
                    zone_epoch,
                    &last_zones,
                    range_map_epoch,
                    &last_range_maps,
                    leader_now,
                    pending_range_ops.iter().any(|(_, _, kind)| {
                        matches!(kind, RangeOpKind::InstallInitialMap { .. })
                    }),
                );
                last_leader = leader_now;
                thread::sleep(Duration::from_millis(5));
            }
            drive_push_state.lock().unwrap().stop = true;
        });

        Ok(PdDaemon {
            cmd: cmd_tx,
            admin_timeout_ms,
            mirrors,
            drive_join,
            push_join,
            push_state,
            accept_join: Some(accept_join),
            accept_stop,
            raft_addr,
        })
    }

    pub fn shutdown(self) {
        let _ = self.cmd.send(PdCmd::Shutdown);
        self.accept_stop
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(addr) = self.raft_addr {
            let _ = TcpStream::connect_timeout(&addr, Duration::from_secs(1));
        }
        let _ = self.drive_join.join();
        self.push_state.lock().unwrap().stop = true;
        let _ = self.push_join.join();
        if let Some(j) = self.accept_join {
            let _ = j.join();
        }
        log::info!("pd: daemon shut down");
    }

    pub fn admin_submit(&self, mutation: AdminMutation) -> Result<AdminOutcome, PartitionError> {
        let timeout = Duration::from_millis(
            self.admin_timeout_ms.load(std::sync::atomic::Ordering::Relaxed),
        );
        let deadline = std::time::Instant::now() + timeout;
        let (reply_tx, reply_rx) = mpsc::channel();
        self.cmd.send(PdCmd::Admin(mutation, deadline, reply_tx))
            .map_err(|_| PartitionError::RestartRequired)?;
        reply_rx.recv_timeout(timeout + Duration::from_millis(100))
            .unwrap_or(Err(PartitionError::AdminNoReply))
    }

    fn submit_admin(&self, mutation: AdminMutation) -> Result<(), PartitionError> {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match self.admin_submit(mutation.clone()) {
                Err(PartitionError::NotReady) if std::time::Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(5));
                }
                result => return result.map(|_| ()),
            }
        }
    }

    pub fn status(&self) -> Option<PdStatus> {
        let (tx, rx) = mpsc::channel();
        self.cmd.send(PdCmd::Status(tx)).ok()?;
        rx.recv_timeout(Duration::from_secs(1)).ok()
    }

    pub fn inspect(&self) -> Option<PdInspection> {
        self.inspect_with(false)
    }

    pub fn capture_state(&self) -> Option<PdInspection> {
        self.inspect_with(true)
    }

    fn inspect_with(&self, with_state: bool) -> Option<PdInspection> {
        let (tx, rx) = mpsc::channel();
        self.cmd.send(PdCmd::Inspect(tx, with_state)).ok()?;
        let timeout = Duration::from_millis(
            self.admin_timeout_ms.load(std::sync::atomic::Ordering::Relaxed),
        );
        rx.recv_timeout(timeout + Duration::from_millis(100)).ok()
    }

    #[cfg(test)]
    pub fn set_admin_timeout(&self, timeout: Duration) {
        self.admin_timeout_ms.store(timeout.as_millis().max(1) as u64,
            std::sync::atomic::Ordering::Relaxed);
    }

    fn submit_boot(&self, cmd: PdCmd, what: &str) {
        if self.cmd.send(cmd).is_err() {
            log::error!("pd: daemon drive thread has exited; {what} dropped");
            crate::observe::counters().pd_propose_errors.inc();
        }
    }

    pub fn bootstrap_members(&self, members: AddressedMembers) {
        self.submit_boot(PdCmd::Bootstrap(members), "bootstrap_members");
    }

    pub fn add_boot_node_addressed(&self, node: NodeId, weight: u32, addrs: Vec<SocketAddr>) {
        self.submit_boot(PdCmd::Add(node, weight, addrs), "add_boot_node_addressed");
    }

    pub fn create_boot_zone(&self, cfg: ZoneConfig) {
        self.submit_boot(PdCmd::CreateZone(cfg, Vec::new()), "create_boot_zone");
    }

    pub fn add_node(&self, node: NodeId) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::Add(node, 1, Vec::new()))
    }

    pub fn add_node_weighted(&self, node: NodeId, weight: u32) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::Add(node, weight, Vec::new()))
    }

    pub fn add_node_addressed(
        &self,
        node: NodeId,
        weight: u32,
        addrs: Vec<SocketAddr>,
    ) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::Add(node, weight, addrs))
    }

    pub fn remove_node(&self, node: NodeId) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::Remove(node))
    }

    pub fn create_zone(&self, cfg: ZoneConfig) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::CreateZone(cfg, Vec::new()))
    }

    pub fn create_zone_presplit(
        &self,
        cfg: ZoneConfig,
        boundaries: Vec<Vec<u8>>,
    ) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::CreateZone(cfg, boundaries))
    }

    pub fn drop_zone(&self, id: ZoneId) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::DropZone(id))
    }

    pub fn split_zone(
        &self,
        zone_id: ZoneId,
        at: impl Into<Vec<u8>>,
    ) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::SplitZone(
            zone_id,
            at.into(),
            SplitMode::Inherit,
        ))
    }

    pub fn split_zone_canonical(
        &self,
        zone_id: ZoneId,
        at: impl Into<Vec<u8>>,
    ) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::SplitZone(
            zone_id,
            at.into(),
            SplitMode::Canonical,
        ))
    }

    pub fn merge_zone(
        &self,
        zone_id: ZoneId,
        at: impl Into<Vec<u8>>,
    ) -> Result<(), PartitionError> {
        self.submit_admin(AdminMutation::MergeZone(zone_id, at.into()))
    }

    pub fn set_auto_split(&self, threshold: u64, poll_ms: u64) {
        let mut st = self.push_state.lock().unwrap();
        st.auto_split_threshold = threshold;
        st.auto_split_poll_ms = poll_ms.max(1);
    }

    pub fn set_auto_splits_per_poll(&self, per_poll: usize) {
        self.push_state.lock().unwrap().auto_splits_per_poll = per_poll.max(1);
    }

    pub fn set_auto_load_split(&self, ops_per_sec: u64) {
        self.push_state.lock().unwrap().auto_load_split_ops = ops_per_sec;
    }

    pub fn set_rehome_enabled(&self, enabled: bool) {
        self.push_state.lock().unwrap().rehome_enabled = enabled;
    }

    pub fn set_single_range_warn(&self, entries: u64) {
        self.push_state.lock().unwrap().single_range_warn_entries = entries;
    }

    pub fn set_auto_merge(&self, threshold: u64) {
        self.push_state.lock().unwrap().auto_merge_threshold = threshold;
    }

    pub fn range_map(&self, zone_id: ZoneId) -> RangeMap {
        self.mirrors
            .range_maps
            .lock()
            .unwrap()
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(RangeMap::single)
    }

    pub fn members(&self) -> Vec<NodeId> {
        self.mirrors
            .members
            .lock()
            .unwrap()
            .iter()
            .map(|(n, _, _)| *n)
            .collect()
    }

    pub fn members_weighted(&self) -> Vec<(NodeId, u32)> {
        self.mirrors
            .members
            .lock()
            .unwrap()
            .iter()
            .map(|(n, w, _)| (*n, *w))
            .collect()
    }

    pub fn members_addressed(&self) -> AddressedMembers {
        self.mirrors.members.lock().unwrap().clone()
    }

    pub fn catalog(&self) -> Catalog {
        self.mirrors.zones.lock().unwrap().clone()
    }
}

type InboundRaft = (
    Message,
    crate::transport::Credit,
    crate::transport::ByteCredit,
);

pub(super) fn resolve_pd_peers<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    config_peers: &HashMap<u64, SocketAddr>,
) -> HashMap<u64, SocketAddr> {
    let mut peers = config_peers.clone();
    let (voters, learners) = pd.pd_voters();
    if voters.is_empty() && learners.is_empty() {
        return peers;
    }
    let mut unaddressed = Vec::new();
    for &id in voters.iter().chain(&learners) {
        if id == pd.id() || peers.contains_key(&id) {
            continue;
        }
        match pd.pd_peer_record(id) {
            Some(PdPeerRecord::Member(addr)) => {
                peers.insert(id, addr);
            }
            _ => unaddressed.push(id),
        }
    }
    let mut configured: Vec<u64> = config_peers.keys().copied().collect();
    configured.sort_unstable();
    if configured != voters {
        let message = format!(
            "pd {}: committed PD voters are {voters:?} (learners {learners:?}) but `pd_peer` lists \
             {configured:?}; once a membership change finishes, set every replica's `pd_peer` list \
             to the committed voters",
            pd.id()
        );
        if learners.is_empty() {
            log::warn!("{message}");
        } else {
            log::info!("{message}");
        }
    }
    if !unaddressed.is_empty() {
        log::warn!(
            "pd {}: no address for PD members {unaddressed:?}: neither `pd_peer` nor committed state \
             names one, so raft messages to them are dropped",
            pd.id()
        );
    }
    peers
}

fn raft_accept_loop(
    listener: TcpListener,
    inbox: Sender<InboundRaft>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    for conn in listener.incoming() {
        if stop.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        let Ok(stream) = conn else { continue };
        let Some(connection_credit) = crate::transport::acquire_connection() else {
            crate::observe::counters().transport_connection_rejections.inc();
            continue;
        };
        let inbox = inbox.clone();
        crate::observe::spawn_abort_on_panic("vals-pd-reader", move || {
            raft_reader(stream, inbox, connection_credit)
        });
    }
}

const RAFT_HELLO_MAGIC: &[u8; 4] = b"VPDA";

const RAFT_HELLO_MAX: usize = 4096;

fn read_exact_until(
    stream: &mut TcpStream,
    mut buf: &mut [u8],
    deadline: Option<std::time::Instant>,
) -> io::Result<()> {
    while !buf.is_empty() {
        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "transport deadline"));
            }
            stream.set_read_timeout(Some(remaining))?;
        } else {
            stream.set_read_timeout(None)?;
        }
        match stream.read(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed")),
            Ok(n) => buf = &mut buf[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

fn timed_out(error: &io::Error) -> bool {
    matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock)
}

pub(super) fn raft_frame_fits_budget(frame_len: usize, max_queued_bytes: usize) -> bool {
    frame_len <= max_queued_bytes
}

fn raft_reader(
    mut stream: TcpStream,
    inbox: Sender<InboundRaft>,
    _connection_credit: crate::transport::Credit,
) {
    let limits = crate::transport::limits();
    let initial_deadline = limits.initial_timeout.map(|d| std::time::Instant::now() + d);
    let mut len = [0u8; 4];
    if let Err(e) = read_exact_until(&mut stream, &mut len, initial_deadline) {
        if timed_out(&e) {
            crate::observe::counters().transport_initial_timeouts.inc();
        }
        return;
    }
    let hello_len = u32::from_le_bytes(len) as usize;
    let mut hello = vec![0u8; hello_len.min(RAFT_HELLO_MAX)];
    if hello_len > RAFT_HELLO_MAX {
        return;
    }
    if let Err(e) = read_exact_until(&mut stream, &mut hello, initial_deadline) {
        if timed_out(&e) {
            crate::observe::counters().transport_initial_timeouts.inc();
        }
        return;
    }
    const VER_OFF: usize = RAFT_HELLO_MAGIC.len();
    const TOK_OFF: usize = VER_OFF + 2;
    let version_ok = hello.len() >= TOK_OFF
        && hello[..VER_OFF] == *RAFT_HELLO_MAGIC
        && u16::from_le_bytes(hello[VER_OFF..TOK_OFF].try_into().unwrap())
            >= crate::proto::MIN_PROTO_VERSION;
    if !version_ok || !crate::auth::verify(&hello[TOK_OFF..]) {
        log::warn!(
            "pd: DENIED raft peer connection from {:?} (missing/invalid cluster \
             secret hello or unsupported protocol version; a legitimate peer means \
             a misconfigured cluster secret or version skew)",
            stream.peer_addr().ok()
        );
        crate::observe::counters().control_denied.inc();
        return;
    }
    loop {
        let idle_deadline = limits.idle_timeout.map(|d| std::time::Instant::now() + d);
        let mut len = [0u8; 4];
        if let Err(e) = read_exact_until(&mut stream, &mut len, idle_deadline) {
            if timed_out(&e) {
                crate::observe::counters().transport_idle_timeouts.inc();
            }
            return;
        }
        let frame_len = u32::from_le_bytes(len) as usize;
        if !raft_frame_fits_budget(frame_len, limits.max_queued_bytes) {
            crate::observe::counters().transport_queued_byte_rejections.inc();
            log::warn!(
                "pd: rejecting inbound raft frame of {frame_len} bytes: configured process-wide queued-byte budget is {} bytes",
                limits.max_queued_bytes
            );
            return;
        }
        let Some(request_credit) = crate::transport::acquire_request() else {
            crate::observe::counters().transport_request_rejections.inc();
            return;
        };
        let Some(byte_credit) = crate::transport::acquire_queued_bytes(frame_len) else {
            crate::observe::counters().transport_queued_byte_rejections.inc();
            log::warn!(
                "pd: rejecting inbound raft frame of {frame_len} bytes: process-wide queued-byte budget unavailable"
            );
            return;
        };
        let mut buf = vec![0u8; frame_len];
        let frame_deadline = limits.frame_timeout.map(|d| std::time::Instant::now() + d);
        if let Err(e) = read_exact_until(&mut stream, &mut buf, frame_deadline) {
            if timed_out(&e) {
                crate::observe::counters().transport_frame_timeouts.inc();
            }
            return;
        }
        if let Some(msg) = vals_raft::decode_message(&buf) {
            if inbox.send((msg, request_credit, byte_credit)).is_err() {
                return;
            }
        }
    }
}

const RAFT_SEND_TIMEOUT: Duration = Duration::from_millis(250);

fn raft_conn_open(s: &TcpStream) -> bool {
    if s.set_nonblocking(true).is_err() {
        return false;
    }
    let open = matches!(s.peek(&mut [0u8; 1]), Err(e) if e.kind() == io::ErrorKind::WouldBlock);
    s.set_nonblocking(false).is_ok() && open
}

const RAFT_CONN_USER_TIMEOUT: Duration = Duration::from_millis(1000);
const RAFT_PEER_QUEUE: usize = 1024;

struct RaftPeer {
    addr: SocketAddr,
    tx: mpsc::SyncSender<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct RaftPeers {
    peers: HashMap<u64, RaftPeer>,
}

impl RaftPeers {
    pub(super) fn retain(&mut self, addrs: &HashMap<u64, SocketAddr>) {
        self.peers.retain(|id, peer| addrs.get(id) == Some(&peer.addr));
    }

    pub(super) fn send(&mut self, to: u64, addr: SocketAddr, msg: &Message) -> bool {
        if self.peers.get(&to).is_some_and(|peer| peer.addr != addr) {
            self.peers.remove(&to);
        }
        let peer = match self.peers.entry(to) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => match spawn_raft_peer(to, addr) {
                Some(peer) => e.insert(peer),
                None => {
                    crate::observe::counters().pd_raft_send_drops.inc();
                    return false;
                }
            },
        };
        match peer.tx.try_send(vals_raft::encode_message(msg)) {
            Ok(()) => true,
            Err(mpsc::TrySendError::Full(_)) => {
                crate::observe::counters().pd_raft_send_drops.inc();
                false
            }
            Err(mpsc::TrySendError::Disconnected(_)) => {
                crate::observe::counters().pd_raft_send_drops.inc();
                self.peers.remove(&to);
                false
            }
        }
    }
}

fn spawn_raft_peer(to: u64, addr: SocketAddr) -> Option<RaftPeer> {
    let (tx, rx) = mpsc::sync_channel::<Vec<u8>>(RAFT_PEER_QUEUE);
    let spawned = thread::Builder::new()
        .name(format!("vals-pd-raft-{to}"))
        .spawn(move || {
            let mut conn = None;
            while let Ok(payload) = rx.recv() {
                if !send_raft(&mut conn, addr, &payload) {
                    let mut dropped = 1;
                    while rx.try_recv().is_ok() {
                        dropped += 1;
                    }
                    crate::observe::counters().pd_raft_send_drops.add(dropped);
                }
            }
        });
    match spawned {
        Ok(_) => Some(RaftPeer { addr, tx }),
        Err(e) => {
            log::warn!("pd: could not start the raft sender for peer {to} at {addr}: {e}");
            None
        }
    }
}

fn raft_connect(addr: SocketAddr) -> Option<TcpStream> {
    let mut s = TcpStream::connect_timeout(&addr, RAFT_SEND_TIMEOUT).ok()?;
    let _ = s.set_nodelay(true);
    let _ = s.set_write_timeout(Some(RAFT_SEND_TIMEOUT));
    let _ = socket2::SockRef::from(&s).set_tcp_user_timeout(Some(RAFT_CONN_USER_TIMEOUT));
    let token = crate::auth::token();
    let ver = crate::proto::PROTO_VERSION.to_le_bytes();
    let mut hello = ((RAFT_HELLO_MAGIC.len() + ver.len() + token.len()) as u32)
        .to_le_bytes()
        .to_vec();
    hello.extend_from_slice(RAFT_HELLO_MAGIC);
    hello.extend_from_slice(&ver);
    hello.extend_from_slice(token);
    s.write_all(&hello).ok()?;
    Some(s)
}

pub(super) fn send_raft(conn: &mut Option<TcpStream>, addr: SocketAddr, payload: &[u8]) -> bool {
    if conn.as_ref().is_some_and(|s| !raft_conn_open(s)) {
        *conn = None;
    }
    if conn.is_none() {
        *conn = raft_connect(addr);
    }
    let Some(s) = conn.as_mut() else { return false };
    let ok = s
        .write_all(&(payload.len() as u32).to_le_bytes())
        .and_then(|_| s.write_all(payload))
        .is_ok();
    if !ok {
        *conn = None;
        crate::observe::counters().pd_raft_send_drops.inc();
    }
    true
}

