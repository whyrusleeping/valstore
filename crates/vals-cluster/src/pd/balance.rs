use super::*;

pub(super) type RangeAgg = HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>, u64, u64)>;
type EngineId = (NodeId, u32);

#[derive(Clone)]
pub(super) struct PlannerEndpointStats {
    pub(super) node: NodeId,
    pub(super) addr: SocketAddr,
    pub(super) ranges: Vec<wire::OwnedRangeStat>,
}

pub(super) struct PlannerInputs {
    pub(super) agg: RangeAgg,
    pub(super) fixed: HashMap<(NodeId, u32, ZoneId), u64>,
    pub(super) topology: BTreeMap<NodeId, u32>,
}

#[derive(Clone, PartialEq, Eq)]
pub(super) struct BalancePollSnapshot {
    pub(super) member_epoch: u64,
    pub(super) zone_epoch: u64,
    pub(super) range_map_epoch: u64,
    pub(super) range_maps_acked: bool,
    pub(super) capability_ready: bool,
    pub(super) planner_fingerprint: u64,
    pub(super) topology: BTreeMap<NodeId, u32>,
}

#[derive(Default)]
pub(crate) struct BalancePollAuthority {
    pub(super) generation: u64,
    pub(super) snapshot: Option<BalancePollSnapshot>,
}

pub(super) fn invalidate_balance_poll(authority: &Mutex<BalancePollAuthority>) {
    let mut authority = authority.lock().unwrap();
    authority.generation = authority.generation.saturating_add(1);
    authority.snapshot = None;
}

pub(super) fn exact_planner_endpoint(
    node: NodeId,
    addr: SocketAddr,
    epoch: u64,
    accounting_epoch: u64,
    accounting_ready: bool,
    expected_epoch: u64,
    ranges: &[wire::OwnedRangeStat],
) -> Option<PlannerEndpointStats> {
    (epoch == expected_epoch && accounting_epoch == expected_epoch && accounting_ready).then(|| {
        PlannerEndpointStats {
            node,
            addr,
            ranges: ranges.to_vec(),
        }
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct LoadRatio {
    pub(super) num: u128,
    pub(super) den: u128,
}

impl LoadRatio {
    pub(super) fn zero() -> Self {
        LoadRatio { num: 0, den: 1 }
    }

    pub(super) fn cmp(self, other: Self) -> std::cmp::Ordering {
        self.num.saturating_mul(other.den).cmp(&other.num.saturating_mul(self.den))
    }

    pub(super) fn pct(self) -> u64 {
        if self.num == 0 || self.den == 0 {
            0
        } else {
            self.num.saturating_mul(100).saturating_div(self.den)
                .min(u64::MAX as u128) as u64
        }
    }

    fn above_pct(self, pct: u64) -> bool {
        self.num.saturating_mul(100) > self.den.saturating_mul(pct as u128)
    }

    fn at_or_below_pct(self, pct: u64) -> bool {
        !self.above_pct(pct)
    }

    pub(super) fn improves_by_pct(self, after: Self, pct: u64) -> bool {
        if after.cmp(self) != std::cmp::Ordering::Less {
            return false;
        }
        let delta_num = self.num.saturating_mul(after.den)
            .saturating_sub(after.num.saturating_mul(self.den));
        let delta_den = self.den.saturating_mul(after.den);
        delta_num.saturating_mul(100) >= delta_den.saturating_mul(pct as u128)
    }
}

#[derive(Clone)]
pub(super) struct ProjectedLoads {
    pub(super) nodes: BTreeMap<NodeId, u128>,
    pub(super) engines: BTreeMap<EngineId, u128>,
    pub(super) total: u128,
    pub(super) capacity: u128,
}

pub(super) fn project_current_loads(
    snap: &PushState,
    agg: &RangeAgg,
    local_hash_entries: &HashMap<(NodeId, u32, ZoneId), u64>,
    ring: &Ring,
    topology: Option<&BTreeMap<NodeId, u32>>,
) -> ProjectedLoads {
    use crate::zone::Ordering;

    let mut nodes: BTreeMap<NodeId, u128> = ring.members().map(|node| (node, 0)).collect();
    let mut engines: BTreeMap<EngineId, u128> = BTreeMap::new();
    if let Some(topology) = topology {
        for (&node, &count) in topology {
            for shard in 0..count {
                engines.insert((node, shard), 0);
            }
        }
    }
    for (&(node, shard, zone_id), &entries) in local_hash_entries {
        let hash_zone = zone_id == DEFAULT_ZONE
            || snap.zones.get(zone_id)
                .is_some_and(|cfg| cfg.ordering == Ordering::Unsorted);
        if !hash_zone {
            continue;
        }
        if let Some(load) = nodes.get_mut(&node) {
            *load = load.saturating_add(entries as u128);
        }
        if let Some(load) = engines.get_mut(&(node, shard)) {
            *load = load.saturating_add(entries as u128);
        }
    }
    for (zone_id, cfg) in snap.zones.iter() {
        if cfg.ordering != Ordering::Sorted {
            continue;
        }
        let default_map = RangeMap::single();
        let map = snap.range_maps.get(&zone_id).unwrap_or(&default_map);
        for index in 0..map.num_ranges() {
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            let entries = agg.get(&(zone_id, lower)).map_or(0, |v| v.0) as u128;
            let replicas = if zone_id == DEFAULT_ZONE {
                ring.replicas(map.token_of(index))
            } else {
                map.replicas_of_range(ring, zone_id, index, cfg.rf, cfg.scope.as_ref())
            };
            for node in replicas {
                if let Some(load) = nodes.get_mut(&node) {
                    *load = load.saturating_add(entries);
                }
                if let Some(&count) = topology.and_then(|t| t.get(&node)) {
                    let shard = (crate::ring::hash64(map.token_of(index)) % count as u64) as u32;
                    if let Some(load) = engines.get_mut(&(node, shard)) {
                        *load = load.saturating_add(entries);
                    }
                }
            }
        }
    }
    let total = nodes.values().copied().fold(0, u128::saturating_add);
    let capacity = snap.members.iter()
        .map(|(_, weight, _)| (*weight).max(1) as u128)
        .fold(0, u128::saturating_add);
    ProjectedLoads { nodes, engines, total, capacity }
}

pub(super) fn planner_inputs(
    snap: &PushState,
    endpoints: &[PlannerEndpointStats],
    targets: &[(NodeId, Vec<SocketAddr>)],
    poll_agg: &RangeAgg,
    ring: &Ring,
) -> Option<PlannerInputs> {
    use crate::zone::Ordering;

    let mut by_addr: BTreeMap<SocketAddr, &PlannerEndpointStats> = BTreeMap::new();
    for endpoint in endpoints {
        if by_addr.insert(endpoint.addr, endpoint).is_some() {
            return None;
        }
    }
    let expected: BTreeMap<SocketAddr, NodeId> = targets.iter()
        .flat_map(|(node, addrs)| addrs.iter().map(move |addr| (*addr, *node)))
        .collect();
    if expected.len() != targets.iter().map(|(_, addrs)| addrs.len()).sum::<usize>()
        || by_addr.len() != expected.len()
    {
        return None;
    }

    let mut expected_keys_by_node = BTreeMap::new();
    for (node, _, _) in &snap.members {
        let mut keys = BTreeSet::new();
        keys.insert((DEFAULT_ZONE, Vec::new()));
        for (zone_id, cfg) in snap.zones.iter() {
            let in_scope = cfg.scope.as_ref().is_none_or(|scope| scope.contains(node));
            if !in_scope {
                continue;
            }
            if zone_id == DEFAULT_ZONE || cfg.ordering == Ordering::Unsorted {
                keys.insert((zone_id, Vec::new()));
            } else if cfg.ordering == Ordering::Sorted {
                let default_map = RangeMap::single();
                let map = snap.range_maps.get(&zone_id).unwrap_or(&default_map);
                for index in 0..map.num_ranges() {
                    keys.insert((
                        zone_id,
                        map.range_bounds(index).0.unwrap_or_default().to_vec(),
                    ));
                }
            }
        }
        expected_keys_by_node.insert(*node, keys);
    }

    let mut identities: BTreeMap<SocketAddr, (NodeId, u32, u32)> = BTreeMap::new();
    for (&addr, &node) in &expected {
        let endpoint = *by_addr.get(&addr)?;
        if endpoint.node != node || endpoint.ranges.is_empty() {
            return None;
        }
        let first = &endpoint.ranges[0];
        if first.shard_count == 0 || first.shard_index >= first.shard_count
            || endpoint.ranges.iter().any(|report| {
                report.shard_index != first.shard_index
                    || report.shard_count != first.shard_count
            })
        {
            return None;
        }
        let mut keys = BTreeSet::new();
        if endpoint.ranges.iter().any(|report| {
            !keys.insert((report.zone_id, report.lower_bound.clone()))
        }) {
            return None;
        }
        let mut default_only = BTreeSet::new();
        default_only.insert((DEFAULT_ZONE, Vec::new()));
        if keys != *expected_keys_by_node.get(&node).unwrap_or(&default_only) {
            return None;
        }
        identities.insert(addr, (node, first.shard_index, first.shard_count));
    }

    let mut topology = BTreeMap::new();
    for (node, _, _) in &snap.members {
        let node_identities: Vec<(u32, u32)> = identities.values()
            .filter_map(|(owner, shard, count)| (*owner == *node).then_some((*shard, *count)))
            .collect();
        let counts: BTreeSet<u32> = node_identities.iter().map(|(_, count)| *count).collect();
        if counts.len() != 1 {
            return None;
        }
        let count = *counts.iter().next().unwrap();
        let shards: BTreeSet<u32> = node_identities.iter().map(|(shard, _)| *shard).collect();
        if node_identities.len() != count as usize
            || shards.iter().copied().ne(0..count)
        {
            return None;
        }
        topology.insert(*node, count);
    }

    let mut fixed = HashMap::new();
    for endpoint in endpoints {
        let (_, shard, _) = identities[&endpoint.addr];
        for report in &endpoint.ranges {
            let unsorted = report.zone_id == DEFAULT_ZONE
                || snap.zones.get(report.zone_id)
                    .is_some_and(|cfg| cfg.ordering == Ordering::Unsorted);
            if unsorted {
                fixed.insert((endpoint.node, shard, report.zone_id), report.entries);
            }
        }
    }

    let mut expected_agg_keys = BTreeSet::new();
    for (zone_id, cfg) in snap.zones.iter() {
        if zone_id == DEFAULT_ZONE || cfg.ordering != Ordering::Sorted {
            continue;
        }
        let default_map = RangeMap::single();
        let map = snap.range_maps.get(&zone_id).unwrap_or(&default_map);
        for index in 0..map.num_ranges() {
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            let key = (zone_id, lower.clone());
            expected_agg_keys.insert(key.clone());
            let authoritative = poll_agg.get(&key)?;
            let fresh_max = endpoints.iter()
                .filter(|endpoint| {
                    cfg.scope.as_ref().is_none_or(|scope| scope.contains(&endpoint.node))
                })
                .filter_map(|endpoint| endpoint.ranges.iter().find(|report| {
                    report.zone_id == zone_id && report.lower_bound == lower
                }))
                .map(|report| report.entries)
                .max()?;
            if fresh_max != authoritative.0 {
                return None;
            }
            let owners = map.replicas_of_range(
                ring, zone_id, index, cfg.rf, cfg.scope.as_ref()
            );
            if owners.len() != cfg.rf {
                return None;
            }
            for node in owners {
                let count = *topology.get(&node)?;
                let shard = (crate::ring::hash64(map.token_of(index)) % count as u64) as u32;
                let endpoint = endpoints.iter().find(|endpoint| {
                    identities.get(&endpoint.addr) == Some(&(node, shard, count))
                })?;
                let report = endpoint.ranges.iter().find(|report| {
                    report.zone_id == zone_id && report.lower_bound == lower
                })?;
                if report.entries != authoritative.0 {
                    return None;
                }
            }
        }
    }
    if poll_agg.keys().cloned().collect::<BTreeSet<_>>() != expected_agg_keys {
        return None;
    }
    Some(PlannerInputs { agg: poll_agg.clone(), fixed, topology })
}

fn capacity_of(snap: &PushState, node: NodeId) -> u128 {
    snap.members.iter().find(|(n, _, _)| *n == node)
        .map_or(1, |(_, weight, _)| (*weight).max(1) as u128)
}

pub(super) fn hottest_node_with_id(
    loads: &ProjectedLoads,
    snap: &PushState,
) -> Option<(NodeId, LoadRatio)> {
    loads.nodes.iter().map(|(&node, &load)| (node, LoadRatio {
        num: load.saturating_mul(loads.capacity),
        den: loads.total.saturating_mul(capacity_of(snap, node)).max(1),
    })).max_by(|a, b| a.1.cmp(b.1))
}

fn hottest_node(loads: &ProjectedLoads, snap: &PushState) -> LoadRatio {
    hottest_node_with_id(loads, snap).map_or_else(LoadRatio::zero, |(_, ratio)| ratio)
}

pub(super) fn hottest_engine_with_id(
    loads: &ProjectedLoads,
    snap: &PushState,
    topology: &BTreeMap<NodeId, u32>,
) -> Option<((NodeId, u32), LoadRatio)> {
    loads.engines.iter().map(|(&engine @ (node, _), &load)| (engine, LoadRatio {
        num: load.saturating_mul(loads.capacity)
            .saturating_mul(topology.get(&node).copied().unwrap_or(1) as u128),
        den: loads.total.saturating_mul(capacity_of(snap, node)).max(1),
    })).max_by(|a, b| a.1.cmp(b.1))
}

fn hottest_engine(
    loads: &ProjectedLoads,
    snap: &PushState,
    topology: &BTreeMap<NodeId, u32>,
) -> LoadRatio {
    hottest_engine_with_id(loads, snap, topology)
        .map_or_else(LoadRatio::zero, |(_, ratio)| ratio)
}

fn normalized_deviation(loads: &ProjectedLoads, snap: &PushState) -> u128 {
    loads.nodes.iter().fold(0u128, |sum, (&node, &load)| {
        let weight = capacity_of(snap, node);
        let actual = load.saturating_mul(loads.capacity);
        let target = loads.total.saturating_mul(weight);
        let delta = actual.abs_diff(target);
        sum.saturating_add(delta.saturating_mul(delta).saturating_div(weight))
    })
}

#[derive(Clone)]
pub(super) struct BalancePlan {
    pub(super) zone_id: ZoneId,
    pub(super) range_index: usize,
    pub(super) lower: Vec<u8>,
    pub(super) entries: u64,
    pub(super) copy_entries: u128,
    pub(super) token: Vec<u8>,
    pub(super) nonce: u64,
    pub(super) targets: Vec<EngineId>,
    pub(super) owner_overlap: usize,
    pub(super) after_node: LoadRatio,
    pub(super) after_engine: LoadRatio,
    pub(super) deviation: u128,
}

#[derive(Clone)]
pub(crate) struct BalanceMapUpgrade {
    pub(super) zone_id: ZoneId,
    pub(super) member_epoch: u64,
    pub(super) members: AddressedMembers,
    pub(super) zone_epoch: u64,
    pub(super) zones: Catalog,
    pub(super) range_map_epoch: u64,
    pub(super) map: Option<RangeMap>,
    pub(super) ring: RingConfig,
}

#[derive(Clone)]
pub(crate) struct BalanceProposal {
    pub(super) identity: u64,
    pub(super) poll_generation: u64,
    pub(super) zone_id: ZoneId,
    pub(super) range_index: usize,
    pub(super) lower: Vec<u8>,
    pub(super) token: Vec<u8>,
    pub(super) entries: u64,
    pub(super) copy_entries: u128,
    pub(super) nonce: u64,
    pub(super) targets: Vec<EngineId>,
    pub(super) member_epoch: u64,
    pub(super) members: AddressedMembers,
    pub(super) zone_epoch: u64,
    pub(super) zones: Catalog,
    pub(super) range_map_epoch: u64,
    pub(super) map: RangeMap,
    pub(super) ring: RingConfig,
    pub(super) topology: BTreeMap<NodeId, u32>,
    pub(super) planner_fingerprint: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum BalanceStaleReason {
    Identity,
    Gate,
    Capability,
    Poll,
    Membership,
    Catalog,
    RangeMap,
    Topology,
}

pub(super) fn planner_fingerprint(inputs: &PlannerInputs) -> u64 {
    let mut bytes = b"vals-pd-balance-input-v1".to_vec();
    let agg: BTreeMap<_, _> = inputs.agg.iter().collect();
    for ((zone, lower), (entries, _, failures, ops)) in agg {
        bytes.extend_from_slice(&zone.to_le_bytes());
        bytes.extend_from_slice(&(lower.len() as u64).to_le_bytes());
        bytes.extend_from_slice(lower);
        bytes.extend_from_slice(&entries.to_le_bytes());
        bytes.extend_from_slice(&failures.to_le_bytes());
        bytes.extend_from_slice(&ops.to_le_bytes());
    }
    let fixed: BTreeMap<_, _> = inputs.fixed.iter().collect();
    for ((node, shard, zone), entries) in fixed {
        bytes.extend_from_slice(&node.to_le_bytes());
        bytes.extend_from_slice(&shard.to_le_bytes());
        bytes.extend_from_slice(&zone.to_le_bytes());
        bytes.extend_from_slice(&entries.to_le_bytes());
    }
    for (node, shards) in &inputs.topology {
        bytes.extend_from_slice(&node.to_le_bytes());
        bytes.extend_from_slice(&shards.to_le_bytes());
    }
    crate::ring::hash64(&bytes)
}

pub(super) fn complete_planner_fingerprint(
    inputs: &PlannerInputs,
    endpoints: &[PlannerEndpointStats],
) -> u64 {
    let mut bytes = planner_fingerprint(inputs).to_le_bytes().to_vec();
    let mut ordered: Vec<_> = endpoints.iter().collect();
    ordered.sort_by_key(|endpoint| (endpoint.node, endpoint.addr));
    for endpoint in ordered {
        bytes.extend_from_slice(&endpoint.node.to_le_bytes());
        bytes.extend_from_slice(endpoint.addr.to_string().as_bytes());
        let mut ranges: Vec<_> = endpoint.ranges.iter().collect();
        ranges.sort_by(|a, b| {
            (a.zone_id, &a.lower_bound).cmp(&(b.zone_id, &b.lower_bound))
        });
        for range in ranges {
            bytes.extend_from_slice(&range.zone_id.to_le_bytes());
            bytes.extend_from_slice(&(range.lower_bound.len() as u64).to_le_bytes());
            bytes.extend_from_slice(&range.lower_bound);
            bytes.extend_from_slice(&range.entries.to_le_bytes());
            match &range.midpoint {
                Some(midpoint) => {
                    bytes.push(1);
                    bytes.extend_from_slice(&(midpoint.len() as u64).to_le_bytes());
                    bytes.extend_from_slice(midpoint);
                }
                None => bytes.push(0),
            }
            bytes.extend_from_slice(&range.ops_per_sec.to_le_bytes());
            bytes.extend_from_slice(&range.shard_index.to_le_bytes());
            bytes.extend_from_slice(&range.shard_count.to_le_bytes());
            bytes.extend_from_slice(&range.rehome_mirror_epoch.to_le_bytes());
            bytes.extend_from_slice(&range.rehome_mirror_ok.to_le_bytes());
            bytes.extend_from_slice(&range.rehome_mirror_failures.to_le_bytes());
            bytes.extend_from_slice(&range.rehome_copy_complete_epoch.to_le_bytes());
            bytes.extend_from_slice(&range.rehome_copy_progress.to_le_bytes());
        }
    }
    crate::ring::hash64(&bytes)
}

pub(super) fn balance_proposal_identity(proposal: &BalanceProposal) -> u64 {
    let mut bytes = b"vals-pd-balance-proposal-v1".to_vec();
    bytes.extend_from_slice(&proposal.poll_generation.to_le_bytes());
    bytes.extend_from_slice(&proposal.member_epoch.to_le_bytes());
    for (node, weight, addrs) in &proposal.members {
        bytes.extend_from_slice(&node.to_le_bytes());
        bytes.extend_from_slice(&weight.to_le_bytes());
        for addr in addrs {
            let text = addr.to_string();
            bytes.extend_from_slice(&(text.len() as u64).to_le_bytes());
            bytes.extend_from_slice(text.as_bytes());
        }
    }
    bytes.extend_from_slice(&proposal.zone_epoch.to_le_bytes());
    for (zone, cfg) in sorted_zones(&proposal.zones) {
        bytes.extend_from_slice(&zone.to_le_bytes());
        bytes.extend_from_slice(&encode_zone_config(&cfg));
    }
    bytes.extend_from_slice(&proposal.range_map_epoch.to_le_bytes());
    bytes.extend_from_slice(&proposal.map.encode());
    bytes.extend_from_slice(&proposal.ring.vnodes.to_le_bytes());
    bytes.extend_from_slice(&proposal.ring.rf.to_le_bytes());
    bytes.extend_from_slice(&proposal.zone_id.to_le_bytes());
    bytes.extend_from_slice(&(proposal.range_index as u64).to_le_bytes());
    bytes.extend_from_slice(&(proposal.lower.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&proposal.lower);
    bytes.extend_from_slice(&proposal.token);
    bytes.extend_from_slice(&proposal.entries.to_le_bytes());
    bytes.extend_from_slice(&proposal.copy_entries.to_le_bytes());
    bytes.extend_from_slice(&proposal.nonce.to_le_bytes());
    bytes.extend_from_slice(&proposal.planner_fingerprint.to_le_bytes());
    for (node, shards) in &proposal.topology {
        bytes.extend_from_slice(&node.to_le_bytes());
        bytes.extend_from_slice(&shards.to_le_bytes());
    }
    for (node, shard) in &proposal.targets {
        bytes.extend_from_slice(&node.to_le_bytes());
        bytes.extend_from_slice(&shard.to_le_bytes());
    }
    crate::ring::hash64(&bytes)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn make_balance_proposal(
    snap: &PushState,
    ring_cfg: RingConfig,
    endpoints: &[PlannerEndpointStats],
    targets: &[(NodeId, Vec<SocketAddr>)],
    agg: &RangeAgg,
    ring: &Ring,
    plan: &BalancePlan,
    capability_ready: bool,
    poll_generation: u64,
) -> Option<BalanceProposal> {
    if !snap.balance_execution_enabled || !capability_ready {
        return None;
    }
    let inputs = planner_inputs(snap, endpoints, targets, agg, ring)?;
    let map = snap.range_maps.get(&plan.zone_id)?.clone();
    let index = map.range_index(&plan.lower);
    if index != plan.range_index
        || map.range_bounds(index).0.unwrap_or_default() != plan.lower
        || map.pending_of(index).is_some()
        || map.token_of(index) == plan.token
    {
        return None;
    }
    let planner_fingerprint = complete_planner_fingerprint(&inputs, endpoints);
    let mut proposal = BalanceProposal {
        identity: 0,
        poll_generation,
        zone_id: plan.zone_id,
        range_index: plan.range_index,
        lower: plan.lower.clone(),
        token: plan.token.clone(),
        entries: plan.entries,
        copy_entries: plan.copy_entries,
        nonce: plan.nonce,
        targets: plan.targets.clone(),
        member_epoch: snap.member_epoch,
        members: snap.members.clone(),
        zone_epoch: snap.zone_epoch,
        zones: snap.zones.clone(),
        range_map_epoch: snap.range_map_epoch,
        map,
        ring: ring_cfg,
        topology: inputs.topology,
        planner_fingerprint,
    };
    proposal.identity = balance_proposal_identity(&proposal);
    Some(proposal)
}

pub(super) fn validate_balance_snapshot(
    state: &PushState,
    ring_cfg: RingConfig,
    proposal: &BalanceProposal,
) -> Result<(), BalanceStaleReason> {
    if balance_proposal_identity(proposal) != proposal.identity {
        return Err(BalanceStaleReason::Identity);
    }
    if !state.balance_execution_enabled || !state.opaque_range_maps_enabled {
        return Err(BalanceStaleReason::Gate);
    }
    if ring_cfg != proposal.ring
        || state.member_epoch != proposal.member_epoch
        || state.members != proposal.members
    {
        return Err(BalanceStaleReason::Membership);
    }
    if state.zone_epoch != proposal.zone_epoch || state.zones != proposal.zones {
        return Err(BalanceStaleReason::Catalog);
    }
    let map = state.range_maps.get(&proposal.zone_id)
        .ok_or(BalanceStaleReason::RangeMap)?;
    if state.range_map_epoch != proposal.range_map_epoch || map != &proposal.map {
        return Err(BalanceStaleReason::RangeMap);
    }
    if !state.opaque_capability_current() {
        return Err(BalanceStaleReason::Capability);
    }
    let index = map.range_index(&proposal.lower);
    if index != proposal.range_index
        || map.range_bounds(index).0.unwrap_or_default() != proposal.lower
        || map.pending_of(index).is_some()
        || map.token_of(index) == proposal.token
        || !map.supports_opaque_tokens()
    {
        return Err(BalanceStaleReason::RangeMap);
    }
    let cfg = proposal.zones.get(proposal.zone_id).ok_or(BalanceStaleReason::Catalog)?;
    let ring = Ring::from_weighted(
        ring_cfg.rf as usize,
        ring_cfg.vnodes,
        proposal.members.iter().map(|(node, weight, _)| (*node, *weight)),
    );
    let mut targets: Vec<EngineId> = ring.replicas_in_zone(
        proposal.zone_id, &proposal.token, cfg.rf, cfg.scope.as_ref()
    ).into_iter().map(|node| {
        let count = proposal.topology.get(&node).copied().unwrap_or(0);
        (node, count)
    }).map(|(node, count)| {
        (node, if count == 0 { u32::MAX } else {
            (crate::ring::hash64(&proposal.token) % count as u64) as u32
        })
    }).collect();
    targets.sort_unstable();
    if targets != proposal.targets {
        return Err(BalanceStaleReason::Topology);
    }
    Ok(())
}

pub(super) fn validate_balance_upgrade<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    state: &PushState,
    ring_cfg: RingConfig,
    upgrade: &BalanceMapUpgrade,
) -> Result<(), BalanceStaleReason> {
    if !state.balance_execution_enabled || !state.opaque_range_maps_enabled {
        return Err(BalanceStaleReason::Gate);
    }
    if ring_cfg != upgrade.ring
        || state.member_epoch != upgrade.member_epoch
        || state.members != upgrade.members
        || pd.members_addressed() != upgrade.members
    {
        return Err(BalanceStaleReason::Membership);
    }
    if state.zone_epoch != upgrade.zone_epoch
        || state.zones != upgrade.zones
        || pd.zones() != upgrade.zones
    {
        return Err(BalanceStaleReason::Catalog);
    }
    let actual_maps = pd.range_maps();
    if state.range_map_epoch != upgrade.range_map_epoch
        || state.range_maps.get(&upgrade.zone_id) != upgrade.map.as_ref()
        || actual_maps.get(&upgrade.zone_id) != upgrade.map.as_ref()
        || upgrade.map.as_ref().is_some_and(|map| {
            map.supports_opaque_tokens() || map.active_rehomes() > 0
        })
    {
        return Err(BalanceStaleReason::RangeMap);
    }
    if !state.opaque_capability_current() {
        return Err(BalanceStaleReason::Capability);
    }
    Ok(())
}

pub(super) fn validate_balance_proposal<D: BlockDevice>(
    pd: &PlacementDriver<D>,
    state: &PushState,
    authority: &BalancePollAuthority,
    ring_cfg: RingConfig,
    proposal: &BalanceProposal,
) -> Result<(), BalanceStaleReason> {
    validate_balance_snapshot(state, ring_cfg, proposal)?;
    if !pd.is_leader() {
        return Err(BalanceStaleReason::Poll);
    }
    let expected_poll = BalancePollSnapshot {
        member_epoch: proposal.member_epoch,
        zone_epoch: proposal.zone_epoch,
        range_map_epoch: proposal.range_map_epoch,
        range_maps_acked: true,
        capability_ready: true,
        planner_fingerprint: proposal.planner_fingerprint,
        topology: proposal.topology.clone(),
    };
    if authority.generation != proposal.poll_generation
        || authority.snapshot.as_ref() != Some(&expected_poll)
    {
        return Err(BalanceStaleReason::Poll);
    }
    if pd.members_addressed() != proposal.members {
        return Err(BalanceStaleReason::Membership);
    }
    if pd.zones() != proposal.zones {
        return Err(BalanceStaleReason::Catalog);
    }
    if pd.range_map(proposal.zone_id) != proposal.map {
        return Err(BalanceStaleReason::RangeMap);
    }
    Ok(())
}

pub(super) fn record_balance_upgrade_stale(reason: BalanceStaleReason, zone_id: ZoneId) {
    let counters = crate::observe::counters();
    counters.pd_balance_exec_stale.inc();
    match reason {
        BalanceStaleReason::Identity => counters.pd_balance_exec_stale_identity.inc(),
        BalanceStaleReason::Gate => counters.pd_balance_exec_stale_gate.inc(),
        BalanceStaleReason::Capability => counters.pd_balance_exec_stale_capability.inc(),
        BalanceStaleReason::Poll => counters.pd_balance_exec_stale_poll.inc(),
        BalanceStaleReason::Membership => counters.pd_balance_exec_stale_membership.inc(),
        BalanceStaleReason::Catalog => counters.pd_balance_exec_stale_catalog.inc(),
        BalanceStaleReason::RangeMap => counters.pd_balance_exec_stale_range_map.inc(),
        BalanceStaleReason::Topology => counters.pd_balance_exec_stale_topology.inc(),
    }
    log::warn!("pd: rejected stale opaque-map upgrade for zone {zone_id}: {reason:?}");
}

pub(super) fn set_balance_prepare_result(
    state: &Mutex<PushState>,
    proposal: &BalanceProposal,
    accepted: bool,
) {
    state.lock().unwrap().balance_prepare_result = Some((proposal.identity, accepted));
}

pub(super) fn record_balance_stale(reason: BalanceStaleReason, proposal: &BalanceProposal) {
    let counters = crate::observe::counters();
    counters.pd_balance_exec_stale.inc();
    match reason {
        BalanceStaleReason::Identity => counters.pd_balance_exec_stale_identity.inc(),
        BalanceStaleReason::Gate => counters.pd_balance_exec_stale_gate.inc(),
        BalanceStaleReason::Capability => counters.pd_balance_exec_stale_capability.inc(),
        BalanceStaleReason::Poll => counters.pd_balance_exec_stale_poll.inc(),
        BalanceStaleReason::Membership => counters.pd_balance_exec_stale_membership.inc(),
        BalanceStaleReason::Catalog => counters.pd_balance_exec_stale_catalog.inc(),
        BalanceStaleReason::RangeMap => counters.pd_balance_exec_stale_range_map.inc(),
        BalanceStaleReason::Topology => counters.pd_balance_exec_stale_topology.inc(),
    }
    log::warn!(
        "pd: rejected stale balance proposal {:016x} for zone {}: {:?}",
        proposal.identity,
        proposal.zone_id,
        reason
    );
}

#[derive(Clone, Copy)]
pub(super) enum BalanceNoop {
    Capability,
    BlockedInput,
    ActiveRehome,
    BelowHigh,
    BelowImprovement,
    NoFeasible,
    SplitFirst,
}

pub(super) struct BalanceOutcome {
    pub(super) before_node: u64,
    pub(super) after_node: u64,
    pub(super) before_engine: u64,
    pub(super) after_engine: u64,
    movable: u64,
    pub(super) blocked: u64,
    pub(super) exhausted: u64,
    pub(super) plan: Option<BalancePlan>,
    pub(super) noop: Option<BalanceNoop>,
}

pub(super) fn opaque_planner_seed(member_epoch: u64, zone_id: ZoneId, lower: &[u8], nonce: u64) -> [u8; 16] {
    let mut domain = b"vals-pd-balance-v1".to_vec();
    domain.extend_from_slice(&member_epoch.to_le_bytes());
    domain.extend_from_slice(&zone_id.to_le_bytes());
    domain.extend_from_slice(&(lower.len() as u64).to_le_bytes());
    domain.extend_from_slice(lower);
    let identity = crate::ring::hash64(&domain);
    let mut nonce_domain = domain;
    nonce_domain.extend_from_slice(&nonce.to_le_bytes());
    let mixed = crate::ring::hash64(&nonce_domain);
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&identity.to_le_bytes());
    seed[8..].copy_from_slice(&mixed.to_le_bytes());
    seed
}

fn canonical_placement_seed(zone_id: ZoneId, lower: &[u8], nonce: u64) -> [u8; 16] {
    let mut domain = b"vals-pd-canonical-place-v1".to_vec();
    domain.extend_from_slice(&zone_id.to_le_bytes());
    domain.extend_from_slice(&(lower.len() as u64).to_le_bytes());
    domain.extend_from_slice(lower);
    let identity = crate::ring::hash64(&domain);
    domain.extend_from_slice(&nonce.to_le_bytes());
    let mixed = crate::ring::hash64(&domain);
    let mut seed = [0u8; 16];
    seed[..8].copy_from_slice(&identity.to_le_bytes());
    seed[8..].copy_from_slice(&mixed.to_le_bytes());
    seed
}

pub(super) fn balanced_opaque_token(
    ring: &Ring,
    zone_id: ZoneId,
    cfg: &ZoneConfig,
    other_tokens: &[Vec<u8>],
    topology: Option<&BTreeMap<NodeId, u32>>,
    lower: &[u8],
    search_limit: u64,
) -> Option<Vec<u8>> {
    let eligible: Vec<NodeId> = ring.members()
        .filter(|node| cfg.scope.as_ref().is_none_or(|scope| scope.contains(node)))
        .collect();
    if cfg.rf == 0 || eligible.len() < cfg.rf || search_limit == 0 {
        return None;
    }
    let mut node_slots: BTreeMap<NodeId, usize> =
        eligible.iter().map(|&node| (node, 0)).collect();
    let mut primaries = node_slots.clone();
    let mut engine_slots = BTreeMap::new();
    if let Some(topology) = topology {
        for &node in &eligible {
            for shard in 0..topology.get(&node).copied().unwrap_or(0) {
                engine_slots.insert((node, shard), 0usize);
            }
        }
    }
    for token in other_tokens {
        let replicas = ring.replicas_in_zone(zone_id, token, cfg.rf, cfg.scope.as_ref());
        if let Some(primary) = replicas.first().and_then(|node| primaries.get_mut(node)) {
            *primary += 1;
        }
        for node in replicas {
            if let Some(slots) = node_slots.get_mut(&node) {
                *slots += 1;
            }
            if let Some(shards) = topology.and_then(|topology| topology.get(&node)).copied() {
                if shards > 0 {
                    if let Some(slots) = engine_slots.get_mut(&(
                        node,
                        (crate::ring::hash64(token) % shards as u64) as u32,
                    )) {
                        *slots += 1;
                    }
                }
            }
        }
    }

    let mut best: Option<((usize, usize, usize, u128, u128, u128, u64), Vec<u8>)> = None;
    for nonce in 0..search_limit {
        let token = crate::partition::opaque_placement_token(canonical_placement_seed(
            zone_id, lower, nonce,
        ));
        let replicas = ring.replicas_in_zone(zone_id, &token, cfg.rf, cfg.scope.as_ref());
        if replicas.len() != cfg.rf {
            continue;
        }
        let mut nodes = node_slots.clone();
        let mut candidate_primaries = primaries.clone();
        let mut engines = engine_slots.clone();
        if let Some(primary) = replicas.first().and_then(|node| candidate_primaries.get_mut(node)) {
            *primary += 1;
        }
        for node in replicas {
            *nodes.get_mut(&node)? += 1;
            if let Some(shards) = topology.and_then(|topology| topology.get(&node)).copied() {
                if shards > 0 {
                    *engines.get_mut(&(
                        node,
                        (crate::ring::hash64(&token) % shards as u64) as u32,
                    ))? += 1;
                }
            }
        }
        let node_spread = nodes.values().max().unwrap_or(&0) - nodes.values().min().unwrap_or(&0);
        let primary_spread = candidate_primaries.values().max().unwrap_or(&0)
            - candidate_primaries.values().min().unwrap_or(&0);
        let engine_spread = engines.values().max().unwrap_or(&0)
            - engines.values().min().unwrap_or(&0);
        let score = (
            primary_spread,
            node_spread,
            engine_spread,
            nodes.values().map(|&v| (v as u128).saturating_mul(v as u128)).sum(),
            candidate_primaries.values()
                .map(|&v| (v as u128).saturating_mul(v as u128)).sum(),
            engines.values().map(|&v| (v as u128).saturating_mul(v as u128)).sum(),
            nonce,
        );
        if best.as_ref().is_none_or(|(best_score, _)| score < *best_score) {
            best = Some((score, token));
        }
    }
    best.map(|(_, token)| token)
}

fn combinations(nodes: &[NodeId], want: usize) -> Vec<Vec<NodeId>> {
    fn visit(nodes: &[NodeId], want: usize, at: usize, selected: &mut Vec<NodeId>, out: &mut Vec<Vec<NodeId>>) {
        if selected.len() == want {
            out.push(selected.clone());
            return;
        }
        if nodes.len().saturating_sub(at) < want.saturating_sub(selected.len()) {
            return;
        }
        for i in at..nodes.len() {
            selected.push(nodes[i]);
            visit(nodes, want, i + 1, selected, out);
            selected.pop();
        }
    }
    let mut out = Vec::new();
    visit(nodes, want, 0, &mut Vec::new(), &mut out);
    out
}

fn projected_move(
    current: &ProjectedLoads,
    weight: u128,
    current_engines: &[EngineId],
    target_engines: &[EngineId],
) -> ProjectedLoads {
    let mut out = current.clone();
    let current_nodes: BTreeSet<NodeId> = current_engines.iter().map(|p| p.0).collect();
    let target_nodes: BTreeSet<NodeId> = target_engines.iter().map(|p| p.0).collect();
    for node in current_nodes.difference(&target_nodes) {
        if let Some(load) = out.nodes.get_mut(node) {
            *load = load.saturating_sub(weight);
        }
    }
    for node in target_nodes.difference(&current_nodes) {
        if let Some(load) = out.nodes.get_mut(node) {
            *load = load.saturating_add(weight);
        }
    }
    for engine in current_engines {
        if !target_engines.contains(engine) {
            if let Some(load) = out.engines.get_mut(engine) {
                *load = load.saturating_sub(weight);
            }
        }
    }
    for engine in target_engines {
        if !current_engines.contains(engine) {
            if let Some(load) = out.engines.get_mut(engine) {
                *load = load.saturating_add(weight);
            }
        }
    }
    out
}

pub(super) fn target_plan_better(a: &BalancePlan, b: &BalancePlan) -> bool {
    a.after_engine.cmp(b.after_engine).then_with(|| a.after_node.cmp(b.after_node))
        .then_with(|| a.deviation.cmp(&b.deviation))
        .then_with(|| b.owner_overlap.cmp(&a.owner_overlap))
        .then_with(|| a.targets.cmp(&b.targets))
        .then_with(|| a.nonce.cmp(&b.nonce))
        == std::cmp::Ordering::Less
}

fn range_plan_better(a: &BalancePlan, b: &BalancePlan) -> bool {
    a.after_engine.cmp(b.after_engine).then_with(|| a.after_node.cmp(b.after_node))
        .then_with(|| b.entries.cmp(&a.entries))
        .then_with(|| a.zone_id.cmp(&b.zone_id))
        .then_with(|| a.lower.cmp(&b.lower))
        .then_with(|| a.deviation.cmp(&b.deviation))
        .then_with(|| a.targets.cmp(&b.targets))
        .then_with(|| a.nonce.cmp(&b.nonce))
        == std::cmp::Ordering::Less
}

pub(super) fn load_objective_better(
    engine: LoadRatio,
    node: LoadRatio,
    deviation: u128,
    current_engine: LoadRatio,
    current_node: LoadRatio,
    current_deviation: u128,
) -> bool {
    engine.cmp(current_engine)
        .then_with(|| node.cmp(current_node))
        .then_with(|| deviation.cmp(&current_deviation))
        == std::cmp::Ordering::Less
}

pub(super) fn plan_is_bounded(plan: &BalancePlan, low_pct: u64) -> bool {
    plan.after_engine.at_or_below_pct(low_pct)
        && plan.after_node.at_or_below_pct(low_pct)
}

pub(super) fn select_low_water_plan(
    bounded: Option<BalancePlan>,
    fallback: Option<BalancePlan>,
) -> Option<(BalancePlan, bool)> {
    bounded.map(|plan| (plan, true)).or_else(|| fallback.map(|plan| (plan, false)))
}

pub(super) fn balance_dry_run(
    snap: &PushState,
    endpoints: &[PlannerEndpointStats],
    targets: &[(NodeId, Vec<SocketAddr>)],
    poll_agg: &RangeAgg,
    ring: &Ring,
    capability_ready: bool,
) -> BalanceOutcome {
    use crate::zone::Ordering;

    let sorted_total = snap.zones.iter().filter(|(zone_id, cfg)| {
        *zone_id != DEFAULT_ZONE && cfg.ordering == Ordering::Sorted
    }).map(|(zone_id, _)| snap.range_maps.get(&zone_id).map_or(1, RangeMap::num_ranges))
        .sum::<usize>() as u64;
    let blocked_outcome = |reason| BalanceOutcome {
        before_node: 0, after_node: 0, before_engine: 0, after_engine: 0,
        movable: 0, blocked: sorted_total, exhausted: 0, plan: None, noop: Some(reason),
    };
    if !capability_ready {
        return blocked_outcome(BalanceNoop::Capability);
    }
    if snap.range_maps.values().any(|map| map.active_rehomes() > 0) {
        return blocked_outcome(BalanceNoop::ActiveRehome);
    }
    let Some(inputs) = planner_inputs(snap, endpoints, targets, poll_agg, ring) else {
        return blocked_outcome(BalanceNoop::BlockedInput);
    };
    let topology = inputs.topology;
    let agg = inputs.agg;
    let current = project_current_loads(snap, &agg, &inputs.fixed, ring, Some(&topology));
    let before_node = hottest_node(&current, snap);
    let before_engine = hottest_engine(&current, snap, &topology);
    let before_deviation = normalized_deviation(&current, snap);
    let mut outcome = BalanceOutcome {
        before_node: before_node.pct(), after_node: 0,
        before_engine: before_engine.pct(), after_engine: 0,
        movable: 0, blocked: 0, exhausted: 0, plan: None, noop: None,
    };
    let below_high = !before_node.above_pct(snap.balance_high_pct)
        && !before_engine.above_pct(snap.balance_high_pct);

    let mut ranges: Vec<(ZoneId, usize, Vec<u8>)> = snap.zones.iter()
        .filter(|(zone_id, cfg)| *zone_id != DEFAULT_ZONE && cfg.ordering == Ordering::Sorted)
        .flat_map(|(zone_id, _)| {
            let n = snap.range_maps.get(&zone_id).map_or(1, RangeMap::num_ranges);
            (0..n).map(move |index| {
                let lower = snap.range_maps.get(&zone_id)
                    .and_then(|m| m.range_bounds(index).0).unwrap_or_default().to_vec();
                (zone_id, index, lower)
            })
        }).collect();
    ranges.sort();
    let mut best_bounded: Option<BalancePlan> = None;
    let mut best_fallback: Option<BalancePlan> = None;
    let mut indivisible = false;
    for (zone_id, index, lower) in ranges {
        let Some(cfg) = snap.zones.get(zone_id) else { continue };
        let Some(map) = snap.range_maps.get(&zone_id) else {
            outcome.blocked += 1;
            continue;
        };
        let Some(&(entries, _, _, _)) = agg.get(&(zone_id, lower.clone())) else {
            outcome.blocked += 1;
            continue;
        };
        if entries == 0 || !map.supports_opaque_tokens() || map.pending_of(index).is_some() {
            outcome.blocked += 1;
            continue;
        }
        let mut eligible: Vec<NodeId> = ring.members()
            .filter(|node| cfg.scope.as_ref().is_none_or(|scope| scope.contains(node)))
            .collect();
        eligible.sort_unstable();
        if cfg.rf == 0 || eligible.len() < cfg.rf {
            outcome.blocked += 1;
            continue;
        }
        let current_nodes = map.replicas_of_range(
            ring, zone_id, index, cfg.rf, cfg.scope.as_ref()
        );
        if current_nodes.len() != cfg.rf {
            outcome.blocked += 1;
            continue;
        }
        let mut current_engines: Vec<EngineId> = current_nodes.iter().map(|node| {
            let count = topology[node];
            (*node, (crate::ring::hash64(map.token_of(index)) % count as u64) as u32)
        }).collect();
        current_engines.sort_unstable();
        outcome.movable += 1;
        if below_high {
            continue;
        }

        let feasible_sets = combinations(&eligible, cfg.rf);
        let feasible_lookup: BTreeSet<Vec<NodeId>> = feasible_sets.iter().cloned().collect();
        let mut seen_sets = BTreeSet::new();
        let mut range_fits_low = false;
        let mut range_bounded: Option<BalancePlan> = None;
        let mut range_fallback: Option<BalancePlan> = None;
        for nonce in 0..snap.balance_token_search_limit {
            let token = crate::partition::opaque_placement_token(opaque_planner_seed(
                snap.member_epoch, zone_id, &lower, nonce
            ));
            let mut realized = ring.replicas_in_zone(
                zone_id, &token, cfg.rf, cfg.scope.as_ref()
            );
            realized.sort_unstable();
            if !feasible_lookup.contains(&realized) {
                continue;
            }
            seen_sets.insert(realized.clone());
            let mut target_engines: Vec<EngineId> = realized.iter().map(|node| {
                let count = topology[node];
                (*node, (crate::ring::hash64(&token) % count as u64) as u32)
            }).collect();
            target_engines.sort_unstable();

            let range_alone_fits = target_engines.iter().all(|(node, _)| {
                let node_ratio = LoadRatio {
                    num: (entries as u128).saturating_mul(current.capacity),
                    den: current.total.saturating_mul(capacity_of(snap, *node)).max(1),
                };
                let engine_ratio = LoadRatio {
                    num: (entries as u128).saturating_mul(current.capacity)
                        .saturating_mul(topology[node] as u128),
                    den: current.total.saturating_mul(capacity_of(snap, *node)).max(1),
                };
                node_ratio.at_or_below_pct(snap.balance_low_pct)
                    && engine_ratio.at_or_below_pct(snap.balance_low_pct)
            });
            range_fits_low |= range_alone_fits;
            if target_engines == current_engines {
                continue;
            }
            let projected = projected_move(
                &current, entries as u128, &current_engines, &target_engines
            );
            let after_engine = hottest_engine(&projected, snap, &topology);
            let after_node = hottest_node(&projected, snap);
            let deviation = normalized_deviation(&projected, snap);
            let improves_current = load_objective_better(
                after_engine, after_node, deviation,
                before_engine, before_node, before_deviation,
            );
            if !improves_current {
                continue;
            }
            let current_node_set: BTreeSet<_> = current_nodes.iter().copied().collect();
            let target_node_set: BTreeSet<_> = realized.iter().copied().collect();
            let owner_overlap = current_node_set.intersection(&target_node_set).count();
            let copy_pairs = target_engines.iter()
                .filter(|pair| !current_engines.contains(pair)).count() as u128;
            let candidate = BalancePlan {
                zone_id, range_index: index, lower: lower.clone(), entries,
                copy_entries: (entries as u128).saturating_mul(copy_pairs),
                token, nonce, targets: target_engines,
                owner_overlap, after_node, after_engine, deviation,
            };
            let slot = if plan_is_bounded(&candidate, snap.balance_low_pct) {
                &mut range_bounded
            } else {
                &mut range_fallback
            };
            if slot.as_ref().is_none_or(|old| target_plan_better(&candidate, old)) {
                *slot = Some(candidate);
            }
        }
        let unseen = feasible_sets.len().saturating_sub(seen_sets.len()) as u64;
        outcome.exhausted = outcome.exhausted.saturating_add(unseen);
        if unseen == 0 && !range_fits_low {
            indivisible = true;
        }
        if let Some(candidate) = range_bounded {
            if best_bounded.as_ref().is_none_or(|old| range_plan_better(&candidate, old)) {
                best_bounded = Some(candidate);
            }
        }
        if let Some(candidate) = range_fallback {
            if best_fallback.as_ref().is_none_or(|old| range_plan_better(&candidate, old)) {
                best_fallback = Some(candidate);
            }
        }
    }
    if below_high {
        outcome.noop = Some(if outcome.movable == 0 && outcome.blocked > 0 {
            BalanceNoop::BlockedInput
        } else {
            BalanceNoop::BelowHigh
        });
        return outcome;
    }
    let Some((plan, bounded)) = select_low_water_plan(best_bounded, best_fallback) else {
        outcome.noop = Some(if outcome.movable == 0 && outcome.blocked > 0 {
            BalanceNoop::BlockedInput
        } else if indivisible {
            BalanceNoop::SplitFirst
        } else {
            BalanceNoop::NoFeasible
        });
        return outcome;
    };
    outcome.after_node = plan.after_node.pct();
    outcome.after_engine = plan.after_engine.pct();
    let improves_overload = bounded
        || (before_engine.above_pct(snap.balance_high_pct)
            && before_engine.improves_by_pct(plan.after_engine, snap.balance_min_improvement_pct))
        || (before_node.above_pct(snap.balance_high_pct)
            && before_node.improves_by_pct(plan.after_node, snap.balance_min_improvement_pct));
    if improves_overload {
        outcome.plan = Some(plan);
    } else {
        outcome.noop = Some(if indivisible {
            BalanceNoop::SplitFirst
        } else {
            BalanceNoop::BelowImprovement
        });
    }
    outcome
}

pub(super) fn publish_balance_plan(outcome: &BalanceOutcome) {
    let gauges = crate::observe::gauges();
    gauges.pd_balance_before_node_pct.set(outcome.before_node);
    gauges.pd_balance_after_node_pct.set(outcome.after_node);
    gauges.pd_balance_before_engine_pct.set(outcome.before_engine);
    gauges.pd_balance_after_engine_pct.set(outcome.after_engine);
    gauges.pd_balance_movable_ranges.set(outcome.movable);
    gauges.pd_balance_blocked_ranges.set(outcome.blocked);
    gauges.pd_balance_token_exhaustions.set(outcome.exhausted);
    gauges.pd_balance_proposal.set(u64::from(outcome.plan.is_some()));
    gauges.pd_balance_proposed_entries.set(outcome.plan.as_ref().map_or(0, |p| p.entries));
    gauges.pd_balance_proposed_copy_entries.set(
        outcome.plan.as_ref().map_or(0, |p| p.copy_entries.min(u64::MAX as u128) as u64)
    );
    let counters = crate::observe::counters();
    if outcome.exhausted > 0 {
        counters.pd_balance_token_search_exhausted.add(outcome.exhausted);
    }
    if let Some(reason) = outcome.noop {
        match reason {
            BalanceNoop::Capability => counters.pd_balance_noop_capability.inc(),
            BalanceNoop::BlockedInput => counters.pd_balance_noop_blocked_input.inc(),
            BalanceNoop::ActiveRehome => counters.pd_balance_noop_active_rehome.inc(),
            BalanceNoop::BelowHigh => counters.pd_balance_noop_below_high.inc(),
            BalanceNoop::BelowImprovement => counters.pd_balance_noop_below_improvement.inc(),
            BalanceNoop::NoFeasible => counters.pd_balance_noop_no_feasible.inc(),
            BalanceNoop::SplitFirst => counters.pd_balance_noop_split_first.inc(),
        }
    }
    if let Some(plan) = outcome.plan.as_ref() {
        let mut identity = b"vals-pd-balance-log-v1".to_vec();
        identity.extend_from_slice(&plan.zone_id.to_le_bytes());
        identity.extend_from_slice(&plan.lower);
        let digest = crate::ring::hash64(&identity);
        log::info!(
            "pd: balance dry-run zone {} range #{} id {:016x} -> {:?}; \
             projected node {}% -> {}%, engine {}% -> {}%, entries {}, copy entries {}",
            plan.zone_id, plan.range_index, digest, plan.targets,
            outcome.before_node, outcome.after_node,
            outcome.before_engine, outcome.after_engine,
            plan.entries, plan.copy_entries
        );
        let _ = plan.token;
    }
}

#[allow(clippy::type_complexity)]
pub(super) fn node_owned_entries(
    snap: &PushState,
    agg: &HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>, u64, u64)>,
    local_hash_entries: &HashMap<(NodeId, u32, ZoneId), u64>,
    ring: &Ring,
) -> HashMap<NodeId, u128> {
    project_current_loads(snap, agg, local_hash_entries, ring, None)
        .nodes.into_iter().collect()
}

pub(super) fn node_load_skew_pct(owned: &HashMap<NodeId, u128>) -> u64 {
    let total = owned.values().copied().fold(0u128, u128::saturating_add);
    if owned.is_empty() || total == 0 {
        return 0;
    }
    let max = owned.values().copied().max().unwrap_or(0);
    let pct = max
        .saturating_mul(owned.len() as u128)
        .saturating_mul(100)
        / total;
    pct.min(u64::MAX as u128) as u64
}

#[allow(clippy::type_complexity)]
pub(super) fn publish_range_metrics(
    snap: &PushState,
    agg: &HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>, u64, u64)>,
    local_hash_entries: &HashMap<(NodeId, u32, ZoneId), u64>,
    ring: &Ring,
    last_warn: &mut HashMap<ZoneId, std::time::Instant>,
) {
    use crate::zone::Ordering;

    let g = crate::observe::gauges();
    let warn_on = snap.single_range_warn_entries > 0;
    let concentration_warn_on = warn_on && snap.range_concentration_warn_pct > 0;

    let mut sorted_ranges_total: u64 = 0;
    let mut single_range_zones: u64 = 0;
    let mut oversized_zones: u64 = 0;
    let mut concentrated_zones: u64 = 0;
    let mut max_range_entries: u64 = 0;
    let mut worst_skew_pct: u64 = 0;
    let mut unrehomed_ranges: u64 = 0;
    let mut active_rehomes: u64 = 0;

    let now = std::time::Instant::now();
    for (zone_id, cfg) in snap.zones.iter() {
        if cfg.ordering != Ordering::Sorted {
            continue;
        }
        let map = snap
            .range_maps
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(RangeMap::single);
        let num = map.num_ranges();
        sorted_ranges_total += num as u64;
        unrehomed_ranges += map.unrehomed_ranges() as u64;
        active_rehomes += map.active_rehomes() as u64;

        let mut sum_entries: u64 = 0;
        let mut zone_max: u64 = 0;
        for i in 0..num {
            let lower_bound = map.range_bounds(i).0.unwrap_or_default().to_vec();
            let e = agg
                .get(&(zone_id, lower_bound))
                .map(|(e, _, _, _)| *e)
                .unwrap_or(0);
            sum_entries += e;
            zone_max = zone_max.max(e);
        }
        max_range_entries = max_range_entries.max(zone_max);

        if num == 1 {
            single_range_zones += 1;
            if warn_on && zone_max > snap.single_range_warn_entries {
                oversized_zones += 1;
                let due = last_warn
                    .get(&zone_id)
                    .is_none_or(|t| now.duration_since(*t) >= SINGLE_RANGE_WARN_EVERY);
                if due {
                    log::warn!(
                        "pd: sorted zone {zone_id} ({}) is on a single range (~{zone_max} \
                         entries > warn threshold {}) → one replica set + one intra-node \
                         partition; presplit with real-key boundaries (create-zone ... \
                         presplit=key,... — numeric K only spreads first-byte-uniform keys) \
                         or enable auto-split (VALS_PD_RANGE_SPLIT_THRESHOLD)",
                        cfg.name,
                        snap.single_range_warn_entries,
                    );
                    crate::observe::counters().pd_single_range_warnings.inc();
                    last_warn.insert(zone_id, now);
                }
            }
        } else if sum_entries > 0 {
            let mean = sum_entries as f64 / num as f64;
            if mean > 0.0 {
                let skew = (zone_max as f64 / mean * 100.0) as u64;
                worst_skew_pct = worst_skew_pct.max(skew);
            }
            let concentration_pct =
                ((zone_max as u128 * 100) / sum_entries as u128) as u64;
            if concentration_warn_on
                && zone_max > snap.single_range_warn_entries
                && concentration_pct >= snap.range_concentration_warn_pct
            {
                concentrated_zones += 1;
                let due = last_warn
                    .get(&zone_id)
                    .is_none_or(|t| now.duration_since(*t) >= SINGLE_RANGE_WARN_EVERY);
                if due {
                    log::warn!(
                        "pd: sorted zone {zone_id} ({}) has {zone_max}/{sum_entries} entries \
                         ({concentration_pct}%) concentrated in one of {num} ranges; a populated \
                         range needs the confirmed re-home protocol to move; only still-empty \
                         ranges may be split canonically (split-zone ... canonical), and new zones \
                         should presplit with real-key boundaries (presplit=key,...)",
                        cfg.name,
                    );
                    crate::observe::counters()
                        .pd_range_concentration_warnings
                        .inc();
                    last_warn.insert(zone_id, now);
                }
            }
        }
    }

    last_warn.retain(|zid, _| {
        snap.zones
            .get(*zid)
            .is_some_and(|c| c.ordering == Ordering::Sorted)
    });

    g.pd_sorted_ranges.set(sorted_ranges_total);
    g.pd_single_range_sorted_zones.set(single_range_zones);
    g.pd_max_range_entries.set(max_range_entries);
    g.pd_oversized_single_range_zones.set(oversized_zones);
    g.pd_concentrated_range_zones.set(concentrated_zones);
    g.pd_range_skew_pct.set(worst_skew_pct);
    g.pd_node_load_skew_pct.set(node_load_skew_pct(&node_owned_entries(
        snap,
        agg,
        local_hash_entries,
        ring,
    )));
    g.pd_unrehomed_ranges.set(unrehomed_ranges);
    g.pd_rehomes_active.set(active_rehomes);
}

