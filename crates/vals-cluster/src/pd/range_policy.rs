use super::*;

pub(super) fn rehome_owner_shard(
    targets: &[(NodeId, Vec<SocketAddr>)],
    node: NodeId,
    token: &[u8],
) -> u32 {
    let shard_count = targets.iter()
        .find(|(target, _)| *target == node)
        .map(|(_, addrs)| addrs.len())
        .filter(|count| *count != 0)
        .unwrap_or(1);
    (crate::ring::hash64(token) % shard_count as u64) as u32
}

pub(super) fn rehome_delivery_confirmed(
    targets: &[(NodeId, Vec<SocketAddr>)],
    confirmed_range_maps: &HashMap<SocketAddr, u64>,
    required: &std::collections::HashSet<(NodeId, u32)>,
    epoch: u64,
) -> bool {
    required.iter().all(|(node, shard)| {
        targets.iter().find(|(target, _)| target == node)
            .and_then(|(_, addrs)| addrs.get(*shard as usize))
            .is_some_and(|addr| range_maps_confirmed_at(confirmed_range_maps, addr, epoch))
    })
}

type CoveredReports = std::collections::HashSet<(ZoneId, Vec<u8>, NodeId, u32)>;
type OwnerReports = HashMap<(ZoneId, Vec<u8>, NodeId, u32), u64>;

fn owner_load_agg(
    snap: &PushState,
    owner_reports: &OwnerReports,
    ring: &Ring,
    targets: &[(NodeId, Vec<SocketAddr>)],
) -> RangeAgg {
    let mut agg = RangeAgg::new();
    for (zone_id, cfg) in snap.zones.iter() {
        if cfg.ordering != crate::zone::Ordering::Sorted {
            continue;
        }
        let default_map = RangeMap::single();
        let map = snap.range_maps.get(&zone_id).unwrap_or(&default_map);
        for index in 0..map.num_ranges() {
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            let token = map.token_of(index);
            let owners = if zone_id == DEFAULT_ZONE {
                ring.replicas(token)
            } else {
                map.replicas_of_range(ring, zone_id, index, cfg.rf, cfg.scope.as_ref())
            };
            let entries = owners.into_iter()
                .filter_map(|node| owner_reports.get(&(
                    zone_id,
                    lower.clone(),
                    node,
                    rehome_owner_shard(targets, node, token),
                )).copied())
                .max();
            if let Some(entries) = entries {
                agg.insert((zone_id, lower), (entries, None, 0, 0));
            }
        }
    }
    agg
}

#[allow(clippy::type_complexity)]
pub(super) fn covered_empty_merges(
    snap: &PushState,
    agg: &HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>, u64, u64)>,
    covered: &CoveredReports,
    ring: &Ring,
    targets: &[(NodeId, Vec<SocketAddr>)],
) -> (std::collections::HashSet<(ZoneId, Vec<u8>)>, Vec<(ZoneId, Vec<u8>)>) {
    let mut dead_ranges = std::collections::HashSet::new();
    let mut merges = Vec::new();
    if covered.is_empty() {
        return (dead_ranges, merges);
    }
    let reported: std::collections::HashSet<ZoneId> =
        covered.iter().map(|(zone_id, _, _, _)| *zone_id).collect();
    let mut zones: Vec<ZoneId> = reported.into_iter().collect();
    zones.sort_unstable();
    for zone_id in zones {
        let (Some(cfg), Some(map)) = (snap.zones.get(zone_id), snap.range_maps.get(&zone_id)) else {
            continue;
        };
        let num = map.num_ranges();
        if cfg.ordering != crate::zone::Ordering::Sorted || map.active_rehomes() > 0 {
            continue;
        }
        let dead: Vec<bool> = (0..num).map(|index| {
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            let quiet = agg.get(&(zone_id, lower.clone())).is_some_and(|(_, _, _, ops)| *ops == 0);
            if !quiet || map.pending_of(index).is_some() {
                return false;
            }
            let owners = map.replicas_of_range(ring, zone_id, index, cfg.rf, cfg.scope.as_ref());
            let token = map.token_of(index);
            let all_covered = !owners.is_empty() && owners.iter().all(|node| {
                covered.contains(&(
                    zone_id,
                    lower.clone(),
                    *node,
                    rehome_owner_shard(targets, *node, token),
                ))
            });
            if all_covered {
                dead_ranges.insert((zone_id, lower));
            }
            all_covered
        }).collect();
        if !dead.windows(2).any(|pair| pair[0] && pair[1]) {
            continue;
        }
        let mut token_ranges: HashMap<&[u8], usize> = HashMap::new();
        for index in 0..num {
            *token_ranges.entry(map.token_of(index)).or_insert(0) += 1;
        }
        for right in 1..num {
            let boundary = map.range_bounds(right).0.unwrap_or_default();
            let others_placed_by_boundary = token_ranges.get(boundary).copied().unwrap_or(0)
                - usize::from(map.token_of(right) == boundary);
            if dead[right - 1] && dead[right] && others_placed_by_boundary == 0 {
                merges.push((zone_id, boundary.to_vec()));
            }
        }
    }
    (dead_ranges, merges)
}

pub(crate) struct EndpointRangeStats {
    epoch: u64,
    accounting: Option<(u64, bool)>,
    ranges: Vec<wire::OwnedRangeStat>,
    recovery_seeding: bool,
    covered_empty: Vec<u32>,
}

pub(crate) type RangeStatsFetch = Vec<Vec<Option<EndpointRangeStats>>>;

fn fetch_endpoint_range_stats(addr: SocketAddr) -> Option<EndpointRangeStats> {
    let legacy = |epoch, ranges| EndpointRangeStats {
        epoch,
        accounting: None,
        ranges,
        recovery_seeding: false,
        covered_empty: Vec::new(),
    };
    match blocking_request(addr, &Request::RangeStatsV3) {
        Ok(Response::RangeStatsV3 {
            epoch,
            accounting_epoch,
            accounting_ready,
            ranges,
            recovery_seeding,
            covered_empty,
        }) => Some(EndpointRangeStats {
            epoch,
            accounting: Some((accounting_epoch, accounting_ready)),
            ranges,
            recovery_seeding: recovery_seeding == Some(true),
            covered_empty,
        }),
        Ok(_) | Err(_) => match blocking_request(addr, &Request::RangeStatsV2) {
            Ok(Response::RangeStatsV2 { epoch, ranges }) => Some(legacy(epoch, ranges)),
            Ok(_) | Err(_) => match blocking_request(addr, &Request::RangeStats) {
                Ok(Response::RangeStats { epoch, ranges }) => Some(legacy(epoch, ranges)),
                _ => None,
            },
        },
    }
}

pub(crate) fn fetch_range_stats(targets: &[(NodeId, Vec<SocketAddr>)]) -> RangeStatsFetch {
    thread::scope(|scope| {
        let fetches: Vec<Vec<_>> = targets
            .iter()
            .map(|(_, addrs)| {
                addrs
                    .iter()
                    .map(|&addr| scope.spawn(move || fetch_endpoint_range_stats(addr)))
                    .collect()
            })
            .collect();
        fetches
            .into_iter()
            .map(|node| node.into_iter().map(|fetch| fetch.join().ok().flatten()).collect())
            .collect()
    })
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn range_stats_poll(
    targets: &[(NodeId, Vec<SocketAddr>)],
    snap: &PushState,
    cmd: &Sender<PdCmd>,
    last_warn: &mut HashMap<ZoneId, std::time::Instant>,
    confirmed_range_maps: &HashMap<SocketAddr, u64>,
    range_maps_acked: bool,
    opaque_capability_ready: bool,
    ring_cfg: RingConfig,
    rehome_reports: &mut HashMap<(ZoneId, Vec<u8>), RehomeReportState>,
    rehome_backoff: &mut HashMap<(ZoneId, Vec<u8>), (std::time::Instant, u32)>,
    balance_executor: &mut BalanceExecutorState,
    balance_poll_authority: &Mutex<BalancePollAuthority>,
    recovery_hold: &mut RecoveryHold,
) -> RangeEntryPoll {
    range_stats_apply(
        fetch_range_stats(targets),
        targets,
        snap,
        cmd,
        last_warn,
        confirmed_range_maps,
        range_maps_acked,
        opaque_capability_ready,
        ring_cfg,
        rehome_reports,
        rehome_backoff,
        balance_executor,
        balance_poll_authority,
        recovery_hold,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn range_stats_apply(
    fetched: RangeStatsFetch,
    targets: &[(NodeId, Vec<SocketAddr>)],
    snap: &PushState,
    cmd: &Sender<PdCmd>,
    last_warn: &mut HashMap<ZoneId, std::time::Instant>,
    confirmed_range_maps: &HashMap<SocketAddr, u64>,
    range_maps_acked: bool,
    opaque_capability_ready: bool,
    ring_cfg: RingConfig,
    rehome_reports: &mut HashMap<(ZoneId, Vec<u8>), RehomeReportState>,
    rehome_backoff: &mut HashMap<(ZoneId, Vec<u8>), (std::time::Instant, u32)>,
    balance_executor: &mut BalanceExecutorState,
    balance_poll_authority: &Mutex<BalancePollAuthority>,
    recovery_hold: &mut RecoveryHold,
) -> RangeEntryPoll {
    #[allow(clippy::type_complexity)]
    let mut agg: HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>, u64, u64)> =
        HashMap::new();
    let mut reports: HashMap<(ZoneId, Vec<u8>), Vec<(NodeId, wire::OwnedRangeStat)>> =
        HashMap::new();
    let mut planner_endpoints: Vec<PlannerEndpointStats> = Vec::new();
    let mut local_hash_entries: HashMap<(NodeId, u32, ZoneId), u64> = HashMap::new();
    let mut covered_reports: CoveredReports = std::collections::HashSet::new();
    let mut owner_reports: OwnerReports = HashMap::new();
    let mut down_nodes: BTreeSet<NodeId> = BTreeSet::new();
    let mut seeding_nodes: BTreeSet<NodeId> = BTreeSet::new();
    for ((reporter, addrs), fetched) in targets.iter().zip(fetched) {
        if addrs.is_empty() {
            down_nodes.insert(*reporter);
        }
        for (addr, stats) in addrs.iter().zip(fetched) {
            let Some(EndpointRangeStats {
                epoch,
                accounting,
                ranges,
                recovery_seeding,
                mut covered_empty,
            }) = stats
            else {
                down_nodes.insert(*reporter);
                continue;
            };
            if recovery_seeding {
                seeding_nodes.insert(*reporter);
            }
            let exact = match accounting {
                Some((accounting_epoch, accounting_ready)) => {
                    if let Some(endpoint) = exact_planner_endpoint(
                        *reporter,
                        *addr,
                        epoch,
                        accounting_epoch,
                        accounting_ready,
                        snap.range_map_epoch,
                        &ranges,
                    ) {
                        planner_endpoints.push(endpoint);
                    }
                    accounting_ready && accounting_epoch == epoch
                }
                None => true,
            };
            covered_empty.sort_unstable();
            let current_epoch = epoch == snap.range_map_epoch;
            for (index, r) in ranges.into_iter().enumerate() {
                let unsorted = r.zone_id == DEFAULT_ZONE
                    || snap.zones.get(r.zone_id)
                        .is_some_and(|cfg| cfg.ordering == crate::zone::Ordering::Unsorted);
                if unsorted {
                    if current_epoch {
                        let entries = local_hash_entries
                            .entry((*reporter, r.shard_index, r.zone_id))
                            .or_insert(0);
                        *entries = (*entries).max(r.entries);
                    }
                    continue;
                }
                if !snap.zones.get(r.zone_id)
                    .is_some_and(|cfg| cfg.ordering == crate::zone::Ordering::Sorted)
                {
                    continue;
                }
                if current_epoch {
                    let e = agg
                        .entry((r.zone_id, r.lower_bound.clone()))
                        .or_insert((0, None, 0, 0));
                    if r.entries > e.0 {
                        e.0 = r.entries;
                        e.1 = r.midpoint.clone();
                    }
                    if r.ops_per_sec > e.3 {
                        e.3 = r.ops_per_sec;
                        if r.midpoint.is_some() {
                            e.1 = r.midpoint.clone();
                        }
                    }
                    e.2 += r.rehome_mirror_failures;
                    if exact {
                        let owned = owner_reports
                            .entry((r.zone_id, r.lower_bound.clone(), *reporter, r.shard_index))
                            .or_insert(0);
                        *owned = (*owned).max(r.entries);
                    }
                    if covered_empty.binary_search(&(index as u32)).is_ok() {
                        covered_reports.insert((r.zone_id, r.lower_bound.clone(), *reporter, r.shard_index));
                    }
                }
                reports
                    .entry((r.zone_id, r.lower_bound.clone()))
                    .or_default()
                    .push((*reporter, r));
            }
        }
    }

    let ring = Ring::from_weighted(
        ring_cfg.rf as usize,
        ring_cfg.vnodes,
        snap.members.iter().map(|(node, weight, _)| (*node, *weight)),
    );
    publish_range_metrics(snap, &agg, &local_hash_entries, &ring, last_warn);
    let topology: BTreeMap<NodeId, u32> = targets.iter()
        .map(|(node, addrs)| (*node, addrs.len().max(1) as u32))
        .collect();
    let owner_agg = owner_load_agg(snap, &owner_reports, &ring, targets);
    let measured = snap.zones.iter().all(|(zone_id, cfg)| {
        if cfg.ordering != crate::zone::Ordering::Sorted {
            return true;
        }
        let default_map = RangeMap::single();
        let map = snap.range_maps.get(&zone_id).unwrap_or(&default_map);
        (0..map.num_ranges()).all(|index| {
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            owner_agg.contains_key(&(zone_id, lower))
        })
    });
    let loads = project_current_loads(snap, &owner_agg, &local_hash_entries, &ring, Some(&topology));
    let hot_node = hottest_node_with_id(&loads, snap)
        .filter(|(_, ratio)| ratio.pct() > snap.balance_high_pct)
        .map(|(node, _)| node);
    let hot_engine = hottest_engine_with_id(&loads, snap, &topology);
    if measured {
        crate::observe::gauges().pd_engine_load_skew_pct.set(
            hot_engine.map_or(0, |(_, ratio)| ratio.pct())
        );
    }
    let hot_engine = hot_engine
        .filter(|(_, ratio)| ratio.pct() > snap.balance_high_pct)
        .map(|(engine, _)| engine);
    let poll_inputs = planner_inputs(snap, &planner_endpoints, targets, &agg, &ring);
    let poll_generation = {
        let mut authority = balance_poll_authority.lock().unwrap();
        authority.generation = authority.generation.saturating_add(1);
        authority.snapshot = poll_inputs.as_ref()
            .filter(|_| snap.leader && range_maps_acked && opaque_capability_ready)
            .map(|inputs| BalancePollSnapshot {
                member_epoch: snap.member_epoch,
                zone_epoch: snap.zone_epoch,
                range_map_epoch: snap.range_map_epoch,
                range_maps_acked,
                capability_ready: opaque_capability_ready,
                planner_fingerprint: complete_planner_fingerprint(inputs, &planner_endpoints),
                topology: inputs.topology.clone(),
            });
        authority.generation
    };
    let balance_outcome = balance_dry_run(
        snap,
        &planner_endpoints,
        targets,
        &agg,
        &ring,
        opaque_capability_ready,
    );
    publish_balance_plan(&balance_outcome);

    let now = std::time::Instant::now();
    let restart_grace = RESTART_RECOVERY_GRACE.max(Duration::from_millis(snap.auto_split_poll_ms).saturating_mul(2));
    let restarting_nodes = recovery_hold.restarting(&down_nodes, &seeding_nodes, restart_grace, now);
    let recovery_hold = recovery_hold.holds(
        !down_nodes.is_empty() || !seeding_nodes.is_empty() || !restarting_nodes.is_empty(),
        Duration::from_millis(snap.rehome_recovery_defer_ms),
        now,
    );
    {
        let g = crate::observe::gauges();
        g.pd_recovery_down_nodes.set(down_nodes.len() as u64);
        g.pd_recovery_seeding_nodes.set(seeding_nodes.len() as u64);
        g.pd_recovery_restarting_nodes.set(restarting_nodes.len() as u64);
        g.pd_rehome_recovery_hold.set(u64::from(recovery_hold));
    }
    let active: std::collections::HashSet<(ZoneId, Vec<u8>)> = snap.range_maps.iter()
        .flat_map(|(&zone_id, map)| (0..map.num_ranges()).filter_map(move |index| {
            map.pending_of(index).map(|_| (
                zone_id,
                map.range_bounds(index).0.unwrap_or_default().to_vec(),
            ))
        }))
        .collect();
    rehome_reports.retain(|range, _| active.contains(range));
    let mut delivery_stalled = 0;
    rehome_backoff.retain(|_, (ready_at, _)| {
        now < *ready_at + Duration::from_secs(3600)
    });

    for (zone_id, map) in &snap.range_maps {
        let Some(cfg) = snap.zones.get(*zone_id) else { continue };
        for index in 0..map.num_ranges() {
            let Some(pending) = map.pending_of(index) else { continue };
            let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
            let current = map.replicas_of_range(
                &ring, *zone_id, index, cfg.rf, cfg.scope.as_ref()
            );
            let target = ring.replicas_in_zone(
                *zone_id, pending, cfg.rf, cfg.scope.as_ref()
            );
            let range_key = (*zone_id, lower.clone());
            let range_reports = reports.get(&range_key);
            let report_state = rehome_reports.entry(range_key).or_insert_with(|| {
                RehomeReportState {
                    first_seen: now,
                    pending_epoch: snap.range_map_epoch,
                    delivery_confirmed: false,
                    mirror_failures: HashMap::new(),
                    owners: HashMap::new(),
                }
            });
            let current_shards: HashMap<NodeId, u32> = current.iter().map(|node| {
                (*node, rehome_owner_shard(targets, *node, map.token_of(index)))
            }).collect();
            let target_shards: HashMap<NodeId, u32> = target.iter().map(|node| {
                (*node, rehome_owner_shard(targets, *node, pending))
            }).collect();
            let required: std::collections::HashSet<(NodeId, u32)> = current_shards.iter()
                .chain(target_shards.iter()).map(|(node, shard)| (*node, *shard)).collect();
            let delivered = rehome_delivery_confirmed(
                targets,
                confirmed_range_maps,
                &required,
                report_state.pending_epoch,
            );
            if !delivered {
                delivery_stalled += 1;
            }
            rehome_delivery_refresh(report_state, delivered, now);
            if let Some(reports) = range_reports {
                for (node, report) in reports {
                    let owner = (*node, report.shard_index);
                    if !required.contains(&owner) {
                        continue;
                    }
                    let is_target = target_shards.get(node) == Some(&report.shard_index);
                    let complete =
                        report.rehome_copy_complete_epoch >= report_state.pending_epoch;
                    rehome_owner_refresh(
                        report_state,
                        owner,
                        is_target,
                        complete,
                        report.rehome_copy_progress,
                        now,
                    );
                }
            }
            let deadline = Duration::from_millis(snap.rehome_report_deadline_ms);
            let mut mirror_failure_grew = false;
            if let Some(rs) = range_reports {
                for (node, r) in rs {
                    let owner = (*node, r.shard_index);
                    let tracked = current_shards.get(node) == Some(&r.shard_index)
                        && r.rehome_mirror_epoch >= snap.range_map_epoch;
                    let seen = report_state.mirror_failures.entry(owner).or_insert(0);
                    if tracked {
                        if r.rehome_mirror_failures > *seen {
                            mirror_failure_grew = true;
                        }
                        *seen = r.rehome_mirror_failures;
                    } else {
                        *seen = 0;
                    }
                }
            }
            let failure = mirror_failure_grew;
            let cause = if failure {
                Some(RehomeAbortCause::MirrorFailure)
            } else {
                rehome_abort_cause(report_state, &required, now, deadline)
            };
            let current_ok = current_shards.iter().all(|(node, shard)| {
                range_reports.is_some_and(|rs| rs.iter().any(|(reporter, r)| {
                    reporter == node && r.shard_index == *shard
                        && r.rehome_mirror_epoch >= snap.range_map_epoch
                        && r.rehome_mirror_failures == 0
                }))
            });
            let copied = target_shards.iter().all(|(node, shard)| {
                range_reports.is_some_and(|rs| rs.iter().any(|(reporter, r)| {
                    reporter == node && r.shard_index == *shard
                        && r.rehome_copy_complete_epoch >= report_state.pending_epoch
                }))
            });
            if let Some(cause) = cause {
                if cmd.send(PdCmd::AbortRehome(*zone_id, lower.clone())).is_ok() {
                    let counters = crate::observe::counters();
                    counters.pd_rehomes_aborted.inc();
                    let range_id = range_log_id(*zone_id, &lower);
                    match cause {
                        RehomeAbortCause::MirrorFailure => {
                            counters.pd_rehomes_aborted_mirror_failure.inc();
                            log::warn!(
                                "pd: rehome abort zone {zone_id} range {range_id:016x}: \
                                 mirror failures reported at epoch {}",
                                snap.range_map_epoch
                            );
                        }
                        RehomeAbortCause::ReportGap(node, shard) => {
                            counters.pd_rehomes_aborted_report_gap.inc();
                            log::warn!(
                                "pd: rehome abort zone {zone_id} range {range_id:016x}: \
                                 no report from required owner node {node} shard {shard} \
                                 within {deadline:?}"
                            );
                        }
                        RehomeAbortCause::CopyStall(node, shard) => {
                            counters.pd_rehomes_aborted_copy_stall.inc();
                            log::warn!(
                                "pd: rehome abort zone {zone_id} range {range_id:016x}: \
                                 pending owner node {node} shard {shard} reported no copy \
                                 progress within {deadline:?}"
                            );
                        }
                    }
                    let attempts = rehome_backoff
                        .get(&(*zone_id, lower.clone()))
                        .map(|(_, n)| *n)
                        .unwrap_or(0)
                        .saturating_add(1)
                        .min(7);
                    let delay = Duration::from_secs(60u64 << (attempts - 1).min(6));
                    rehome_backoff.insert((*zone_id, lower.clone()), (now + delay, attempts));
                }
            } else if range_maps_acked && current_ok && copied
                && cmd.send(PdCmd::CommitRehome(*zone_id, lower.clone())).is_ok()
            {
                rehome_backoff.remove(&(*zone_id, lower.clone()));
                crate::observe::counters().pd_rehomes_completed.inc();
                log::info!(
                    "pd: rehome commit zone {zone_id} range {:016x} at epoch {}",
                    range_log_id(*zone_id, &lower),
                    snap.range_map_epoch
                );
            }
        }
    }
    crate::observe::gauges()
        .pd_rehomes_delivery_stalled
        .set(delivery_stalled);

    use crate::zone::Ordering;
    let mut actioned: std::collections::HashSet<ZoneId> = std::collections::HashSet::new();

    let (covered_dead, covered_merges) = if snap.auto_merge_covered {
        covered_empty_merges(snap, &agg, &covered_reports, &ring, targets)
    } else {
        Default::default()
    };
    if snap.auto_split_threshold > 0 || snap.auto_load_split_ops > 0 {
        let mut hot: Vec<_> = agg
            .iter()
            .filter(|(_, (entries, _, _, ops_per_sec))| {
                (snap.auto_split_threshold > 0 && *entries > snap.auto_split_threshold)
                    || (snap.auto_load_split_ops > 0 && *ops_per_sec > snap.auto_load_split_ops)
            })
            .collect();
        hot.sort_unstable_by(|a, b| {
            b.1.0.cmp(&a.1.0).then(b.1.3.cmp(&a.1.3)).then(a.0.cmp(b.0))
        });
        let mut splits_this_poll: HashMap<ZoneId, usize> = HashMap::new();
        for ((zone_id, lower_bound), (entries, midpoint, _, ops_per_sec)) in hot {
            let (zone_id, entries, ops_per_sec) = (*zone_id, *entries, *ops_per_sec);
            if splits_this_poll.get(&zone_id).copied().unwrap_or(0) >= snap.auto_splits_per_poll
                || covered_dead.contains(&(zone_id, lower_bound.clone()))
            {
                continue;
            }
            let Some(mid) = midpoint.clone() else { continue };
            let map = snap
                .range_maps
                .get(&zone_id)
                .cloned()
                .unwrap_or_else(RangeMap::single);
            let idx = map.range_index(lower_bound);
            let (lo, hi) = map.range_bounds(idx);
            if lo.unwrap_or_default() != lower_bound.as_slice() {
                continue;
            }
            let valid = map.active_rehomes() == 0
                && !mid.is_empty()
                && lo.is_none_or(|l| mid.as_slice() > l)
                && hi.is_none_or(|h| mid.as_slice() < h)
                && !map.splits().iter().any(|s| s.as_slice() == mid.as_slice());
            if !valid {
                continue;
            }
            log::info!(
                "pd: auto-split zone {zone_id} range {:016x} ({entries} entries, \
                 {ops_per_sec} ops/s; thresholds {} entries, {} ops/s)",
                range_log_id(zone_id, lower_bound),
                snap.auto_split_threshold,
                snap.auto_load_split_ops
            );
            if cmd
                .send(PdCmd::SplitZone(zone_id, mid, SplitMode::Inherit))
                .is_ok()
            {
                actioned.insert(zone_id);
                *splits_this_poll.entry(zone_id).or_insert(0) += 1;
                crate::observe::counters().pd_range_splits.inc();
            }
        }
    }

    let mut covered_merged: std::collections::HashSet<ZoneId> = std::collections::HashSet::new();
    let mut zone_boundaries: BTreeMap<ZoneId, Vec<Vec<u8>>> = BTreeMap::new();
    for (zone_id, boundary) in covered_merges {
        zone_boundaries.entry(zone_id).or_default().push(boundary);
    }
    for (zone_id, boundaries) in zone_boundaries {
        let count = boundaries.len() as u64;
        log::debug!(
            "pd: covered merge zone {zone_id}: {count} ranges into their left neighbours \
             (all empty and range-deleted on every owner)"
        );
        if cmd.send(PdCmd::MergeZones(zone_id, boundaries)).is_ok() {
            covered_merged.insert(zone_id);
            crate::observe::counters().pd_range_covered_merges.add(count);
        }
    }

    let fresh_balance_poll = snap.leader
        && range_maps_acked
        && opaque_capability_ready
        && poll_inputs.is_some();
    let may_prepare_balance = snap.balance_execution_enabled
        && balance_executor.may_prepare(snap, fresh_balance_poll);
    let mut balance_prepare_sent = false;
    if may_prepare_balance {
        if let Some(zone_id) = snap.zones.iter()
            .filter_map(|(zone_id, cfg)| {
                let map = snap.range_maps.get(&zone_id);
                (zone_id != DEFAULT_ZONE
                    && cfg.ordering == Ordering::Sorted
                    && map.is_none_or(|map| {
                        !map.supports_opaque_tokens() && map.active_rehomes() == 0
                    })
                    && !actioned.contains(&zone_id)
                    && !covered_merged.contains(&zone_id))
                    .then_some(zone_id)
            })
            .min()
        {
            let upgrade = BalanceMapUpgrade {
                zone_id,
                member_epoch: snap.member_epoch,
                members: snap.members.clone(),
                zone_epoch: snap.zone_epoch,
                zones: snap.zones.clone(),
                range_map_epoch: snap.range_map_epoch,
                map: snap.range_maps.get(&zone_id).cloned(),
                ring: ring_cfg,
            };
            if cmd.send(PdCmd::EnableOpaque(upgrade)).is_ok() {
                actioned.insert(zone_id);
                log::info!("pd: balance executor enabling opaque tokens for zone {zone_id}");
            }
        } else if let Some(plan) = balance_outcome.plan.as_ref() {
            if recovery_hold {
                crate::observe::counters().pd_rehome_starts_deferred_recovery.inc();
            } else if !actioned.contains(&plan.zone_id)
                && !covered_merged.contains(&plan.zone_id)
                && snap.range_maps.values().all(|map| map.active_rehomes() == 0)
            {
                if let Some(proposal) = make_balance_proposal(
                    snap,
                    ring_cfg,
                    &planner_endpoints,
                    targets,
                    &agg,
                    &ring,
                    plan,
                    opaque_capability_ready,
                    poll_generation,
                ) {
                    if cmd.send(PdCmd::PrepareBalance(proposal.clone())).is_ok() {
                        crate::observe::counters().pd_balance_exec_prepares.inc();
                        crate::observe::counters().pd_balance_exec_copy_entries.add(
                            proposal.copy_entries.min(u64::MAX as u128) as u64
                        );
                        log::info!(
                            "pd: balance execute proposal {:016x} zone {} range #{}",
                            proposal.identity,
                            proposal.zone_id,
                            proposal.range_index
                        );
                        actioned.insert(proposal.zone_id);
                        balance_executor.prepared(proposal);
                        balance_prepare_sent = true;
                    }
                }
            }
        }
    }

    if snap.rehome_enabled
        && !balance_prepare_sent
        && !(snap.balance_execution_enabled && balance_executor.blocks_other_rehomes())
    {
        let active: usize = snap.range_maps.values().map(RangeMap::active_rehomes).sum();
        let limit = if snap.balance_execution_enabled {
            1
        } else {
            snap.max_concurrent_rehomes
        };
        let mut slots = limit.saturating_sub(active);
        if slots > 0 {
            let mut candidates: Vec<(bool, u64, ZoneId, Vec<u8>)> = Vec::new();
            for (&zone_id, map) in &snap.range_maps {
                if map.active_rehomes() > 0 {
                    continue;
                }
                for index in 0..map.num_ranges() {
                    let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
                    if !map.placement_debt_of(index)
                        || map.pending_of(index).is_some()
                        || covered_dead.contains(&(zone_id, lower.clone()))
                    {
                        continue;
                    }
                    if rehome_backoff
                        .get(&(zone_id, lower.clone()))
                        .is_some_and(|(ready_at, _)| now < *ready_at)
                    {
                        continue;
                    }
                    let entries = owner_agg.get(&(zone_id, lower.clone())).map_or(0, |v| v.0);
                    let cfg = snap.zones.get(zone_id);
                    let replicas = map.replicas_of_range(
                        &ring, zone_id, index, cfg.map_or(1, |cfg| cfg.rf),
                        cfg.and_then(|cfg| cfg.scope.as_ref()),
                    );
                    let hot = replicas.iter().any(|node| {
                        hot_node == Some(*node) || hot_engine == Some((
                            *node, rehome_owner_shard(targets, *node, map.token_of(index))
                        ))
                    });
                    candidates.push((hot, entries, zone_id, lower));
                }
            }
            if recovery_hold {
                if candidates.iter().any(|(_, _, zone_id, _)| !actioned.contains(zone_id)) {
                    crate::observe::counters().pd_rehome_starts_deferred_recovery.inc();
                }
            } else if !measured || !candidates.iter().any(|(hot, _, zone_id, _)| {
                *hot && !actioned.contains(zone_id)
            }) {
                if candidates.iter().any(|(_, _, zone_id, _)| !actioned.contains(zone_id)) {
                    crate::observe::counters().pd_rehome_debt_deferred_balanced.inc();
                }
            } else {
                candidates.retain(|(hot, _, _, _)| *hot);
                candidates.sort_unstable_by_key(|b| std::cmp::Reverse(b.1));
                for (_, entries, zone_id, lower) in candidates {
                    if slots == 0 || actioned.contains(&zone_id) {
                        break;
                    }
                    if cmd.send(PdCmd::PrepareRehome(zone_id, lower.clone())).is_ok() {
                        actioned.insert(zone_id);
                        slots -= 1;
                        crate::observe::counters().pd_rehomes_started.inc();
                        log::info!(
                            "pd: rehome prepare zone {zone_id} range {:016x} (~{entries} entries)",
                            range_log_id(zone_id, &lower)
                        );
                    }
                }
            }
        }
    }

    let split_ceiling = if snap.auto_split_threshold > 0 {
        snap.auto_split_threshold / 2
    } else {
        u64::MAX
    };
    let merge_ceiling = snap.auto_merge_threshold.min(split_ceiling);
    let load_merge_ceiling = load_merge_ceiling(snap.auto_load_split_ops);
    if merge_ceiling > 0 && load_merge_ceiling > 0 {
        for (zone_id, cfg) in snap.zones.iter() {
            if cfg.ordering != Ordering::Sorted
                || actioned.contains(&zone_id)
                || covered_merged.contains(&zone_id)
            {
                continue;
            }
            let map = snap
                .range_maps
                .get(&zone_id)
                .cloned()
                .unwrap_or_else(RangeMap::single);
            let num = map.num_ranges();
            if num < 2 || map.active_rehomes() > 0 {
                continue;
            }
            let entries = |i: usize| -> u64 {
                let lower_bound = map.range_bounds(i).0.unwrap_or_default().to_vec();
                agg.get(&(zone_id, lower_bound))
                    .map(|(e, _, _, _)| *e)
                    .unwrap_or(0)
            };
            let ops = |i: usize| -> u64 {
                let lower_bound = map.range_bounds(i).0.unwrap_or_default().to_vec();
                agg.get(&(zone_id, lower_bound))
                    .map(|(_, _, _, ops)| *ops)
                    .unwrap_or(0)
            };
            let mut best: Option<(usize, u64, u64)> = None;
            for i in 0..num - 1 {
                if map.pending_of(i).is_some() || map.pending_of(i + 1).is_some() {
                    continue;
                }
                let combined = entries(i).saturating_add(entries(i + 1));
                let combined_ops = ops(i).saturating_add(ops(i + 1));
                if combined < merge_ceiling
                    && combined_ops < load_merge_ceiling
                    && best.is_none_or(|(_, b, _)| combined < b)
                {
                    best = Some((i, combined, combined_ops));
                }
            }
            let Some((pair, combined, combined_ops)) = best else { continue };
            let boundary = map.range_bounds(pair + 1).0.unwrap().to_vec();
            log::info!(
                "pd: auto-merge zone {zone_id} range {:016x} into its left neighbour \
                 ({combined} combined entries < merge ceiling {merge_ceiling}; \
                 {combined_ops} ops/s < load ceiling {load_merge_ceiling})",
                range_log_id(zone_id, &boundary)
            );
            if cmd.send(PdCmd::MergeZone(zone_id, boundary)).is_ok() {
                actioned.insert(zone_id);
                crate::observe::counters().pd_range_merges.inc();
            }
        }
    }

    RangeEntryPoll {
        member_epoch: snap.member_epoch,
        zone_epoch: snap.zone_epoch,
        range_map_epoch: snap.range_map_epoch,
        observed_at: std::time::Instant::now(),
        ranges: agg.into_iter().map(|(range, values)| (range, values.0)).collect(),
    }
}

