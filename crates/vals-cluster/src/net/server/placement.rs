use super::*;

pub(crate) fn catalog_to_wire(cat: &Catalog) -> Vec<wire::OwnedZone> {
    let mut v: Vec<wire::OwnedZone> = cat
        .iter()
        .map(|(id, cfg)| wire::OwnedZone {
            id,
            rf: cfg.rf as u32,
            ordering: ordering_to_u8(cfg.ordering),
            scope: cfg.scope.as_ref().map(|s| s.iter().copied().collect()),
            name: cfg.name.clone(),
        })
        .collect();
    v.sort_unstable_by_key(|z| z.id);
    v
}

pub(super) fn members_reply<D: BlockDevice>(shared: &ServerShared<D>) -> Response {
    let ring = shared.ring.borrow();
    let members: Vec<(NodeId, u32)> = ring.members_weighted().collect();
    let addrs = {
        let book = shared.peer_addrs.borrow();
        members
            .iter()
            .filter_map(|&(n, _)| book.get(&n).map(|a| (n, a.clone())))
            .collect()
    };
    Response::Members {
        epoch: shared.ring_epoch(),
        vnodes: ring.vnodes(),
        rf: ring.rf() as u32,
        members,
        addrs,
    }
}

pub(super) fn catalog_reply<D: BlockDevice>(shared: &ServerShared<D>) -> Response {
    Response::Zones {
        epoch: shared.catalog_epoch(),
        zones: catalog_to_wire(&shared.catalog.borrow()),
    }
}

pub(super) fn range_maps_from_wire(
    maps: Vec<wire::OwnedRangeMap>,
) -> Result<FastMap<ZoneId, RangeMap>, &'static str> {
    maps.into_iter()
        .map(|m| {
            let map = if m.opaque_tokens {
                RangeMap::from_opaque_parts(
                    m.splits, m.tokens, m.pending, m.placement_debt
                )
            } else {
                RangeMap::from_splits_tokens_and_pending(m.splits, m.tokens, m.pending)
            }.map_err(|_| "invalid range map tokens")?;
            Ok((m.zone_id, map))
        })
        .collect()
}

fn range_maps_to_wire(maps: &FastMap<ZoneId, RangeMap>) -> Vec<wire::OwnedRangeMap> {
    let mut v: Vec<wire::OwnedRangeMap> = maps
        .iter()
        .map(|(&zone_id, m)| wire::OwnedRangeMap {
            zone_id,
            splits: m.splits().to_vec(),
            tokens: m.tokens().to_vec(),
            pending: m.pending().to_vec(),
            placement_debt: m.placement_debt().to_vec(),
            opaque_tokens: m.supports_opaque_tokens(),
        })
        .collect();
    v.sort_unstable_by_key(|m| m.zone_id);
    v
}

pub(super) fn range_maps_reply<D: BlockDevice>(shared: &ServerShared<D>) -> Response {
    Response::RangeMaps {
        epoch: shared.range_map_epoch(),
        maps: range_maps_to_wire(&shared.range_maps.borrow()),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MirrorOutcome {
    Mirrored,
    Busy,
    Failed,
}

pub(super) fn classify_mirror_acks(acks: &[BatchAck]) -> MirrorOutcome {
    let applied_ack = |ack: &BatchAck| {
        matches!(
            ack,
            BatchAck::Applied(crate::replica::Applied::Won)
                | BatchAck::Applied(crate::replica::Applied::Lost(_))
        )
    };
    if acks.is_empty() {
        return MirrorOutcome::Failed;
    }
    if acks.iter().all(applied_ack) {
        MirrorOutcome::Mirrored
    } else if acks
        .iter()
        .all(|ack| applied_ack(ack) || matches!(ack, BatchAck::Busy | BatchAck::Stale(_)))
    {
        MirrorOutcome::Busy
    } else {
        MirrorOutcome::Failed
    }
}

fn record_mirror_ack_classes(acks: &[BatchAck]) {
    let counters = crate::observe::counters();
    for ack in acks {
        match ack {
            BatchAck::Applied(crate::replica::Applied::Won)
            | BatchAck::Applied(crate::replica::Applied::Lost(_)) => {
                counters.rehome_mirror_applied.inc()
            }
            BatchAck::Applied(crate::replica::Applied::Rejected) => {
                counters.rehome_mirror_rejected.inc()
            }
            BatchAck::Busy => counters.rehome_mirror_busy.inc(),
            BatchAck::Stale(_) => counters.rehome_mirror_stale.inc(),
            BatchAck::Timeout => counters.rehome_mirror_timeouts.inc(),
            BatchAck::Overloaded | BatchAck::Remote => {
                counters.rehome_mirror_remote_errors.inc()
            }
            BatchAck::Unreachable => counters.rehome_mirror_unreachable.inc(),
        }
    }
}

pub(super) async fn apply_mirror_with_timeout_retries<F, Fut>(
    expected_acks: usize,
    mut apply: F,
) -> MirrorOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Vec<BatchAck>>,
{
    const TIMEOUT_RETRIES: usize = 2;
    let mut retries = 0;
    loop {
        let acks = apply().await;
        record_mirror_ack_classes(&acks);
        if acks.iter().any(|ack| matches!(ack, BatchAck::Timeout)) {
            if retries == TIMEOUT_RETRIES {
                crate::observe::counters().rehome_mirror_timeout_retry_exhausted.inc();
                return MirrorOutcome::Failed;
            }
            retries += 1;
            crate::observe::counters().rehome_mirror_timeout_retries.inc();
            continue;
        }
        let outcome = if acks.len() == expected_acks {
            classify_mirror_acks(&acks)
        } else {
            MirrorOutcome::Failed
        };
        if retries == 0 {
            return outcome;
        }
        if outcome == MirrorOutcome::Mirrored {
            crate::observe::counters().rehome_mirror_timeout_retry_successes.inc();
            return outcome;
        }
        crate::observe::counters().rehome_mirror_timeout_retry_unresolved.inc();
        return MirrorOutcome::Failed;
    }
}

pub(super) fn mirrored_response(outcome: MirrorOutcome, success: Response, failure: &str) -> Response {
    match outcome {
        MirrorOutcome::Mirrored => success,
        MirrorOutcome::Busy => Response::Busy,
        MirrorOutcome::Failed => Response::Err(failure.into()),
    }
}

pub(super) fn record_mirror_outcome(
    stat: &mut RehomeMirrorStat,
    outcome: &mut MirrorOutcome,
    target_outcome: MirrorOutcome,
) {
    match target_outcome {
        MirrorOutcome::Mirrored => stat.ok += 1,
        MirrorOutcome::Busy => {
            if *outcome == MirrorOutcome::Mirrored {
                *outcome = MirrorOutcome::Busy;
            }
        }
        MirrorOutcome::Failed => {
            stat.failures += 1;
            *outcome = MirrorOutcome::Failed;
        }
    }
}

fn note_unmirrored_write<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    map: Option<&RangeMap>,
    key: &[u8],
) {
    let mut all = shared.range_write_marks.borrow_mut();
    let Some(zone) = all.get_mut(&zone_id) else {
        return;
    };
    if zone.marks.is_empty() {
        return;
    }
    let index = map.map_or(0, |map| map.range_index(key));
    if map.is_some_and(|map| map.pending_of(index).is_some()) {
        return;
    }
    let lower = map
        .and_then(|map| map.range_bounds(index).0)
        .unwrap_or_default();
    if let Some(slot) = zone.marks.get_mut(lower) {
        zone.counter += 1;
        slot.value = zone.counter;
    }
}

pub(super) fn post_apply_needs_mirror<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    key: &[u8],
) -> bool {
    let maps = shared.range_maps.borrow();
    let map = maps.get(&zone_id);
    if map.is_some_and(|map| map.pending_for_key(key).is_some()) {
        return true;
    }
    note_unmirrored_write(shared, zone_id, map, key);
    false
}

pub(super) const MAX_MIRROR_BATCH_ENTRIES: usize = 1024;

pub(super) struct MirrorBatch<'a> {
    pub(super) lower: Vec<u8>,
    pub(super) target: NodeId,
    pub(super) entries: Vec<BatchEntry<'a>>,
}

pub(super) fn plan_mirror_batches<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    map: &RangeMap,
    cfg: &ZoneConfig,
    ring: &Ring,
    writes: I,
) -> Vec<MirrorBatch<'a>>
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a wire::OwnedWrite>,
{
    let (shard_index, shard_count) = shared.shard();
    let mut groups: FastMap<(usize, NodeId), Vec<&wire::OwnedWrite>> = FastMap::default();
    for write in writes {
        let (key, _, _) = write;
        let index = map.range_index(key);
        let Some(pending) = map.pending_of(index) else { continue };
        let current = map.replicas_of_range(ring, zone_id, index, cfg.rf, cfg.scope.as_ref());
        let current_shard = (crate::ring::hash64(map.token_of(index)) % shard_count as u64) as usize;
        if !current.contains(&shared.node_id) || shard_index != current_shard {
            continue;
        }
        let targets = ring.replicas_in_zone(zone_id, pending, cfg.rf, cfg.scope.as_ref());
        let pending_shard = (crate::ring::hash64(pending) % shard_count as u64) as usize;
        for target in targets.into_iter().filter(|n| {
            !current.contains(n) || current_shard != pending_shard
        }) {
            groups.entry((index, target)).or_default().push(write);
        }
    }

    let mut batches = Vec::new();
    for ((index, target), writes) in groups {
        let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
        for chunk in writes.chunks(MAX_MIRROR_BATCH_ENTRIES) {
            let entries = chunk
                .iter()
                .map(|&&(ref key, ref value, version)| BatchEntry {
                    key,
                    value: value.as_deref(),
                    version,
                })
                .collect();
            batches.push(MirrorBatch {
                lower: lower.clone(),
                target,
                entries,
            });
        }
    }
    batches
}

pub(super) async fn apply_mirror_batches<F, Fut>(
    batch_sizes: &[usize],
    apply: F,
) -> Vec<MirrorOutcome>
where
    F: Fn(usize) -> Fut,
    Fut: Future<Output = Vec<BatchAck>>,
{
    const MAX_CONCURRENT_MIRROR_BATCHES: usize = 4;
    let mut outcomes = Vec::with_capacity(batch_sizes.len());
    for start in (0..batch_sizes.len()).step_by(MAX_CONCURRENT_MIRROR_BATCHES) {
        let end = (start + MAX_CONCURRENT_MIRROR_BATCHES).min(batch_sizes.len());
        let apply = &apply;
        let futures: Vec<_> = (start..end)
            .map(|index| {
                apply_mirror_with_timeout_retries(batch_sizes[index], move || apply(index))
            })
            .collect();
        outcomes.extend(vals_rt::join_all(futures).await);
    }
    outcomes
}

pub(super) fn record_mirror_batch_outcomes<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    epoch: u64,
    batches: &[MirrorBatch<'_>],
    outcomes: Vec<MirrorOutcome>,
) -> MirrorOutcome {
    let mut outcome = MirrorOutcome::Mirrored;
    for (batch, target_outcome) in batches.iter().zip(outcomes) {
        let mut stats = shared.rehome_mirrors.borrow_mut();
        let stat = stats.entry((zone_id, batch.lower.clone())).or_default();
        if stat.epoch != epoch {
            *stat = RehomeMirrorStat {
                epoch,
                ok: 0,
                failures: 0,
                copy_complete_epoch: 0,
            };
        }
        record_mirror_outcome(stat, &mut outcome, target_outcome);
    }
    outcome
}

pub(super) fn bump_range_write_marks<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    map: &RangeMap,
    indices: impl IntoIterator<Item = usize>,
) {
    let mut all = shared.range_write_marks.borrow_mut();
    let Some(zone) = all.get_mut(&zone_id) else {
        return;
    };
    for index in indices {
        if map.pending_of(index).is_some() {
            continue;
        }
        let lower = map.range_bounds(index).0.unwrap_or_default();
        if let Some(slot) = zone.marks.get_mut(lower) {
            zone.counter += 1;
            slot.value = zone.counter;
        }
    }
}

pub(super) fn rehome_transport<D: BlockDevice>(
    shared: &ServerShared<D>,
    ring: &Ring,
    epoch: u64,
) -> Rc<NetTransport> {
    let ring_epoch = shared.ring_epoch();
    let transport = {
        let cached = shared.rehome_transport.borrow();
        cached.as_ref().filter(|cached| {
            cached.ring_epoch == ring_epoch && cached.range_map_epoch == epoch
        }).map(|cached| cached.transport.clone())
    };
    transport.unwrap_or_else(|| {
        let peers: Vec<(NodeId, Vec<SocketAddr>)> = ring
            .members()
            .filter_map(|node| shared.peer_addrs_of(node).map(|addrs| (node, addrs)))
            .collect();
        let transport = Rc::new(
            NetTransport::connect_sharded(peers).with_rpc_deadline(Duration::from_secs(2))
        );
        transport.set_route_epoch(ring_epoch);
        let mut routed = shared.range_maps_clone();
        for map in routed.values_mut() {
            for index in 0..map.num_ranges() {
                if map.pending_of(index).is_some() {
                    let _ = map.commit_pending(index);
                }
            }
        }
        transport.set_sorted_ranges(&routed);
        *shared.rehome_transport.borrow_mut() = Some(RehomeTransport {
            ring_epoch,
            range_map_epoch: epoch,
            transport: transport.clone(),
        });
        transport
    })
}

pub(super) async fn mirror_rehome_writes<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    writes: I,
    sync: bool,
) -> MirrorOutcome
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a wire::OwnedWrite>,
{
    let (ring, epoch, batches) = {
        let maps = shared.range_maps.borrow();
        let single;
        let map = match maps.get(&zone_id) {
            Some(map) => map,
            None => {
                single = RangeMap::single();
                &single
            }
        };
        let tracked = shared
            .range_write_marks
            .borrow()
            .get(&zone_id)
            .is_some_and(|zone| !zone.marks.is_empty());
        let active_rehomes = map.active_rehomes();
        if !tracked && active_rehomes == 0 {
            return MirrorOutcome::Mirrored;
        }
        let writes: Vec<&wire::OwnedWrite> = writes.into_iter().collect();
        if tracked {
            bump_range_write_marks(
                shared,
                zone_id,
                map,
                writes.iter().map(|(key, _, _)| map.range_index(key)),
            );
        }
        if active_rehomes == 0 {
            return MirrorOutcome::Mirrored;
        }
        let Some(cfg) = shared.zone_config(zone_id) else {
            return MirrorOutcome::Failed;
        };
        let ring = shared.ring.borrow().clone();
        let epoch = shared.range_map_epoch();
        let batches = plan_mirror_batches(
            shared,
            zone_id,
            map,
            &cfg,
            &ring,
            writes.iter().copied(),
        );
        (ring, epoch, batches)
    };
    if batches.is_empty() {
        return MirrorOutcome::Mirrored;
    }
    let transport = rehome_transport(shared, &ring, epoch);
    let batch_sizes: Vec<usize> = batches.iter().map(|batch| batch.entries.len()).collect();
    let outcomes = apply_mirror_batches(&batch_sizes, |index| {
        let batch = &batches[index];
        transport.apply_batch(batch.target, zone_id, sync, &batch.entries)
    }).await;
    record_mirror_batch_outcomes(shared, zone_id, epoch, &batches, outcomes)
}

pub(super) async fn mirror_apply_response<D: BlockDevice>(
    shared: Rc<ServerShared<D>>,
    zone_id: ZoneId,
    write: wire::OwnedWrite,
    applied: crate::replica::Applied,
) -> Response {
    let writes = [write];
    #[cfg(test)]
    async_write_mirror_hook();
    let mirror_epoch = shared.range_map_epoch();
    let outcome = mirror_rehome_writes(shared.as_ref(), zone_id, writes.iter(), false).await;
    let response = mirrored_response(
        outcome,
        Response::Apply(applied),
        "re-home mirror failed; retry write",
    );
    fence_mirrored_response(
        shared.as_ref(),
        zone_id,
        writes.iter().map(|(key, _, _)| key.as_slice()),
        mirror_epoch,
        None,
        response,
    )
}

pub(super) fn applied_batch_writes<'a>(
    writes: &'a [wire::OwnedWrite],
    results: &[crate::replica::Applied],
) -> Vec<&'a wire::OwnedWrite> {
    writes.iter().zip(results).filter_map(|(write, applied)| {
        (!matches!(applied, crate::replica::Applied::Rejected)).then_some(write)
    }).collect()
}

pub(super) async fn mirror_batch_response<D: BlockDevice>(
    shared: Rc<ServerShared<D>>,
    zone_id: ZoneId,
    writes: &[wire::OwnedWrite],
    response: Response,
    sync: bool,
    fenced: Option<&FencedWrites>,
) -> Response {
    let Response::Batch(results) = response else {
        return response;
    };
    #[cfg(test)]
    async_write_mirror_hook();
    let mirror_epoch = shared.range_map_epoch();
    let mirrored_writes = applied_batch_writes(writes, &results);
    let outcome = mirror_rehome_writes(
        shared.as_ref(), zone_id, mirrored_writes.iter().copied(), sync,
    ).await;
    let response = mirrored_response(
        outcome,
        Response::Batch(results),
        "re-home mirror failed; retry batch",
    );
    fence_mirrored_response(
        shared.as_ref(),
        zone_id,
        writes.iter().map(|(key, _, _)| key.as_slice()),
        mirror_epoch,
        fenced,
        response,
    )
}

pub(super) fn range_stats_reply<D: BlockDevice>(
    shared: &ServerShared<D>,
    with_load: bool,
    logical: bool,
) -> Response {
    let range_load = if with_load {
        shared.sample_range_load()
    } else {
        FastMap::default()
    };
    let open: Vec<(ZoneId, Rc<RefCell<VersionedStore<D>>>)> = shared
        .zones
        .borrow()
        .iter()
        .map(|(id, s)| (*id, s.clone()))
        .collect();
    let mut ranges: Vec<wire::OwnedRangeStat> = Vec::new();
    let accounting_epoch = shared.range_map_epoch();
    let mut accounting_ready = logical;
    let mut recovery_seeding = false;
    let mut covered_empty: Vec<u32> = Vec::new();
    for (zone_id, store) in open {
        let ordering = if zone_id == DEFAULT_ZONE {
            Some(crate::zone::Ordering::Unsorted)
        } else {
            shared.zone_config(zone_id).map(|c| c.ordering)
        };
        let Some(ordering) = ordering else { continue };
        let store = store.borrow();
        recovery_seeding |= store.recovery_seed_pending();
        let (shard_index, shard_count) = shared.shard();
        if ordering == crate::zone::Ordering::Unsorted {
            let exact_entries = logical.then(|| store.exact_live_keys()).flatten();
            if logical && exact_entries.is_none() {
                accounting_ready = false;
            }
            let entries = exact_entries.unwrap_or_else(|| store.live_keys());
            if entries > 0 || with_load {
                ranges.push(wire::OwnedRangeStat {
                    zone_id,
                    lower_bound: Vec::new(),
                    entries,
                    midpoint: None,
                    ops_per_sec: 0,
                    shard_index: shard_index as u32,
                    shard_count: shard_count as u32,
                    rehome_mirror_epoch: 0,
                    rehome_mirror_ok: 0,
                    rehome_mirror_failures: 0,
                    rehome_copy_complete_epoch: 0,
                    rehome_copy_progress: 0,
                });
            }
            continue;
        }
        let map = shared.range_map(zone_id);
        if logical {
            store.set_logical_range_map(accounting_epoch, map.clone());
        }
        let logical_counts = logical
            .then(|| store.logical_range_counts(accounting_epoch))
            .flatten();
        if logical && logical_counts.is_none() {
            accounting_ready = false;
        }
        let count_bounds = (logical && logical_counts.is_none())
            .then(|| store.logical_range_count_bounds(accounting_epoch))
            .flatten()
            .filter(|bounds| bounds.len() == map.num_ranges());
        let rehome_mirrors = shared.rehome_mirrors.borrow();
        let rehome_copy_progress = shared.rehome_copy_progress.borrow();
        for i in 0..map.num_ranges() {
            let (lo, hi) = map.range_bounds(i);
            let ops_per_sec = range_load
                .get(&zone_id)
                .and_then(|rates| rates.get(i))
                .copied()
                .unwrap_or(0);
            let (entries, midpoint) = match (&logical_counts, &count_bounds) {
                (Some(counts), _) if counts[i] == 0 && ops_per_sec == 0 => (0, None),
                (Some(counts), _) => (counts[i], store.engine().range_midpoint(lo, hi)),
                (None, bounds) => {
                    let (estimated_entries, midpoint) = store.engine().range_estimate(lo, hi);
                    let entries = bounds.as_ref().map_or(estimated_entries, |bounds| {
                        let (lower, upper) = bounds[i];
                        estimated_entries.clamp(lower, upper.max(lower))
                    });
                    (entries, midpoint)
                }
            };
            if !with_load && entries == 0 && ops_per_sec == 0 && map.pending_of(i).is_none() {
                continue;
            }
            let none_uncovered = match (&logical_counts, &count_bounds) {
                (Some(counts), _) => counts[i] == 0,
                (None, Some(bounds)) => bounds[i].0.max(bounds[i].1) == 0,
                (None, None) => false,
            };
            if none_uncovered
                && store.engine().range_tombstones().covers_span(lo.unwrap_or_default(), hi, 0)
            {
                covered_empty.push(ranges.len() as u32);
            }
            let lower_bound = lo.unwrap_or_default().to_vec();
            let stat = if rehome_mirrors.is_empty() {
                RehomeMirrorStat::default()
            } else {
                rehome_mirrors.get(&(zone_id, lower_bound.clone())).copied().unwrap_or_default()
            };
            let progress = if rehome_copy_progress.is_empty() {
                0
            } else {
                rehome_copy_progress.get(&(zone_id, lower_bound.clone())).copied().unwrap_or(0)
            };
            ranges.push(wire::OwnedRangeStat {
                zone_id,
                lower_bound,
                entries,
                midpoint,
                ops_per_sec,
                shard_index: shard_index as u32,
                shard_count: shard_count as u32,
                rehome_mirror_epoch: stat.epoch,
                rehome_mirror_ok: stat.ok,
                rehome_mirror_failures: stat.failures,
                rehome_copy_complete_epoch: stat.copy_complete_epoch,
                rehome_copy_progress: progress,
            });
        }
    }
    if with_load && !ranges.iter().any(|stat| stat.zone_id == DEFAULT_ZONE) {
        let (shard_index, shard_count) = shared.shard();
        ranges.push(wire::OwnedRangeStat {
            zone_id: DEFAULT_ZONE,
            lower_bound: Vec::new(),
            entries: 0,
            midpoint: None,
            ops_per_sec: 0,
            shard_index: shard_index as u32,
            shard_count: shard_count as u32,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        });
    }
    if logical {
        if !accounting_ready {
            crate::observe::counters()
                .logical_accounting_reseed_requests
                .inc();
            shared.kick_logical_accounting();
            shared.kick_sweep();
        }
        Response::RangeStatsV3 {
            epoch: shared.range_map_epoch(),
            accounting_epoch,
            accounting_ready,
            ranges,
            recovery_seeding: Some(recovery_seeding),
            covered_empty,
        }
    } else if with_load {
        Response::RangeStatsV2 {
            epoch: shared.range_map_epoch(),
            ranges,
        }
    } else {
        Response::RangeStats {
            epoch: shared.range_map_epoch(),
            ranges,
        }
    }
}

pub(super) fn owns_range_write<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    cfg: &ZoneConfig,
    map: &RangeMap,
    ring: &Ring,
    index: usize,
) -> bool {
    let (shard_index, shard_count) = shared.shard();
    let token = map.token_of(index);
    let committed = shard_index == (crate::ring::hash64(token) % shard_count as u64) as usize
        && ring
            .replicas_in_zone(zone_id, token, cfg.rf, cfg.scope.as_ref())
            .contains(&shared.node_id);
    committed
        || map.pending_of(index).is_some_and(|pending| {
            shard_index == (crate::ring::hash64(pending) % shard_count as u64) as usize
                && ring
                    .replicas_in_zone(zone_id, pending, cfg.rf, cfg.scope.as_ref())
                    .contains(&shared.node_id)
        })
}

pub(super) fn with_fence_map<D: BlockDevice, R>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    ring: &Ring,
    f: impl FnOnce(&RangeMap) -> R,
) -> Option<R> {
    let maps = shared.range_maps.borrow();
    match maps.get(&zone_id) {
        Some(map) => Some(f(map)),
        None if ring.is_empty() => None,
        None => Some(f(&RangeMap::single())),
    }
}

pub(super) fn stale_range_write<D: BlockDevice>(shared: &ServerShared<D>) -> Response {
    crate::observe::counters().range_fence_rejections.inc();
    Response::StaleEpoch(shared.range_map_epoch().max(shared.ring_epoch()))
}

pub(super) fn rehome_write_fence_one<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    key: &[u8],
) -> Option<Response> {
    let catalog = shared.catalog.borrow();
    let Some(cfg) = catalog.get(zone_id) else {
        return shared.held_uncataloged_sorted(zone_id).then(|| stale_range_write(shared));
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return None;
    }
    if !shared.sorted_placement_ready() {
        return Some(stale_range_write(shared));
    }
    let ring = shared.ring.borrow();
    let owned = with_fence_map(shared, zone_id, &ring, |map| {
        owns_range_write(shared, zone_id, cfg, map, &ring, map.range_index(key))
    })?;
    (!owned).then(|| stale_range_write(shared))
}

fn stale_range_read<D: BlockDevice>(shared: &ServerShared<D>) -> Response {
    crate::observe::counters().range_read_fence_rejections.inc();
    Response::StaleEpoch(shared.range_map_epoch().max(shared.ring_epoch()))
}

pub(super) fn rehome_read_fence<'a, D, I>(
    shared: &ServerShared<D>,
    epoch: u64,
    zone_id: ZoneId,
    keys: I,
) -> Option<Response>
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a [u8]>,
{
    if epoch == wire::INSPECT_EPOCH {
        return None;
    }
    let catalog = shared.catalog.borrow();
    let Some(cfg) = catalog.get(zone_id) else {
        return shared.held_uncataloged_sorted(zone_id).then(|| stale_range_read(shared));
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return None;
    }
    if !shared.sorted_placement_ready() {
        return Some(stale_range_read(shared));
    }
    let ring = shared.ring.borrow();
    let owned = with_fence_map(shared, zone_id, &ring, |map| {
        shared.range_owner_cache(zone_id, map.num_ranges(), |owners| {
            keys.into_iter().all(|key| {
                let index = map.range_index(key);
                *owners[index].get_or_insert_with(|| owns_range_write(shared, zone_id, cfg, map, &ring, index))
            })
        })
    })?;
    (!owned).then(|| stale_range_read(shared))
}

pub(super) fn rehome_scan_fence<D: BlockDevice>(
    shared: &ServerShared<D>,
    epoch: u64,
    zone_id: ZoneId,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> Option<Response> {
    if epoch == wire::INSPECT_EPOCH {
        return None;
    }
    let catalog = shared.catalog.borrow();
    let Some(cfg) = catalog.get(zone_id) else {
        return shared.held_uncataloged_sorted(zone_id).then(|| stale_range_read(shared));
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return None;
    }
    if !shared.sorted_placement_ready() {
        return Some(stale_range_read(shared));
    }
    let ring = shared.ring.borrow();
    let owned = with_fence_map(shared, zone_id, &ring, |map| {
        let start = lo.map_or(0, |l| map.range_index(l));
        let end = hi.map_or(map.num_ranges(), |h| {
            1 + map.splits().partition_point(|s| s.as_slice() < h)
        });
        shared.range_owner_cache(zone_id, map.num_ranges(), |owners| {
            (start..end).all(|index| {
                *owners[index].get_or_insert_with(|| owns_range_write(shared, zone_id, cfg, map, &ring, index))
            })
        })
    })?;
    (!owned).then(|| stale_range_read(shared))
}

pub(super) struct FencedWrites {
    pub(super) stamp: PlacementStamp,
    pub(super) indices: Vec<usize>,
}

pub(super) fn fence_writes<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    keys: I,
) -> Result<Option<FencedWrites>, Response>
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a [u8]>,
{
    let catalog = shared.catalog.borrow();
    let Some(cfg) = catalog.get(zone_id) else {
        return if shared.held_uncataloged_sorted(zone_id) {
            Err(stale_range_write(shared))
        } else {
            Ok(None)
        };
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return Ok(None);
    }
    if !shared.sorted_placement_ready() {
        return Err(stale_range_write(shared));
    }
    let ring = shared.ring.borrow();
    let indices = with_fence_map(shared, zone_id, &ring, |map| {
        shared.range_owner_cache(zone_id, map.num_ranges(), |owners| {
            keys.into_iter()
                .map(|key| {
                    let index = map.range_index(key);
                    let owns = *owners[index]
                        .get_or_insert_with(|| owns_range_write(shared, zone_id, cfg, map, &ring, index));
                    owns.then_some(index).ok_or(())
                })
                .collect::<Result<Vec<usize>, ()>>()
        })
    });
    match indices {
        None => Ok(None),
        Some(Ok(indices)) => Ok(Some(FencedWrites { stamp: shared.placement_stamp(), indices })),
        Some(Err(())) => Err(stale_range_write(shared)),
    }
}

pub(super) fn rehome_write_fence<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    keys: I,
) -> Option<Response>
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a [u8]>,
{
    fence_writes(shared, zone_id, keys).err()
}

pub(super) fn fence_write_response<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    keys: I,
    response: Response,
) -> Response
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a [u8]>,
{
    rehome_write_fence(shared, zone_id, keys).unwrap_or(response)
}

pub(super) fn fence_mirrored_response<'a, D, I>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    keys: I,
    mirror_epoch: u64,
    fenced: Option<&FencedWrites>,
    response: Response,
) -> Response
where
    D: BlockDevice,
    I: IntoIterator<Item = &'a [u8]>,
{
    if shared.range_map_epoch() != mirror_epoch {
        stale_range_write(shared)
    } else if fenced.is_some_and(|fenced| fenced.stamp == shared.placement_stamp())
        && shared.sorted_placement_ready()
    {
        response
    } else {
        fence_write_response(shared, zone_id, keys, response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hlc::Version;
    use crate::replica::ensure_tree_ready_cooperative;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Engine, Ordering};

    fn logical_stats<D: BlockDevice>(shared: &ServerShared<D>) -> (bool, Vec<wire::OwnedRangeStat>) {
        let Response::RangeStatsV3 {
            accounting_ready,
            ranges,
            ..
        } = range_stats_reply(shared, true, true)
        else {
            panic!("expected RangeStatsV3")
        };
        (accounting_ready, ranges)
    }

    #[test]
    fn sorted_routes_never_hash_route_a_sorted_zone_without_a_map() {
        let shared = ServerShared::single_zone_on(
            1,
            VersionedStore::new(Engine::create_prewiped(
                MemBlockDevice::new(256, 512),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
            ), 1).unwrap(),
            Ring::from_members(1, 64, [1]),
        );
        let sorted = |name: &str| crate::zone::ZoneConfig {
            name: name.into(), rf: 1, ordering: crate::zone::Ordering::Sorted, scope: None,
        };
        shared.catalog.borrow_mut().insert(7, sorted("unsplit"));
        shared.catalog.borrow_mut().insert(8, sorted("split"));
        shared.catalog.borrow_mut().insert(9, crate::zone::ZoneConfig {
            name: "hashed".into(), rf: 1, ordering: crate::zone::Ordering::Unsorted, scope: None,
        });
        let split = RangeMap::from_splits([b"m".to_vec()]);
        shared.range_maps.borrow_mut().insert(8, split.clone());
        let routes = shared.sorted_routes();
        assert_eq!(routes.get(&7), Some(&RangeMap::single()));
        assert_eq!(routes.get(&8), Some(&split));
        assert!(!routes.contains_key(&9));
        let shards = 8;
        let single_shard = vals_client::partition::data_shard(Some(&RangeMap::single()), b"", shards);
        let key = (0..64)
            .map(|i| format!("key-{i}").into_bytes())
            .find(|k| (crate::ring::hash64(k) % shards as u64) as usize != single_shard)
            .unwrap();
        assert_eq!(
            vals_client::partition::data_shard(routes.get(&7), &key, shards),
            single_shard,
            "a mapless sorted key routes by its single-range token, never by key hash"
        );
    }

    #[test]
    fn write_fence_owner_cache_and_post_apply_skip_follow_unepoched_placement_changes() {
        let shared = ServerShared::single_zone_on(
            1,
            VersionedStore::new(Engine::create_prewiped(
                MemBlockDevice::new(256, 512),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
            ), 1).unwrap(),
            Ring::from_members(1, 64, [1, 2]),
        );
        let zone = 7;
        shared.catalog.borrow_mut().insert(zone, crate::zone::ZoneConfig {
            name: "fenced".into(), rf: 1, ordering: crate::zone::Ordering::Sorted, scope: None,
        });
        let map = RangeMap::from_splits((0..32).map(|i| format!("k{i:02}").into_bytes()));
        shared.range_maps.borrow_mut().insert(zone, map.clone());
        shared.range_map_epoch.set(10);
        let owner = |token: &[u8]| shared.ring.borrow().replicas_in_zone(zone, token, 1, None)[0];
        let (owned, moved_to) = (1..map.num_ranges())
            .find_map(|i| {
                (owner(map.token_of(i)) == 1)
                    .then(|| (0..i).find(|&j| owner(map.token_of(j)) == 2).map(|j| (i, j)))
                    .flatten()
            })
            .unwrap();
        let foreign = (0..map.num_ranges()).find(|&i| owner(map.token_of(i)) == 2).unwrap();
        let key_in = |index: usize| {
            let mut key = map.range_bounds(index).0.unwrap_or_default().to_vec();
            key.push(b'x');
            key
        };
        let (mine, theirs) = (key_in(owned), key_in(foreign));
        let fence = |key: &[u8]| fence_writes(&shared, zone, [key]);

        let fenced = fence(&mine).ok().flatten().unwrap();
        assert_eq!(fenced.indices, vec![owned]);
        assert!(matches!(fence(&theirs), Err(Response::StaleEpoch(10))));
        assert!(matches!(
            fence_writes(&shared, zone, [mine.as_slice(), theirs.as_slice()]),
            Err(Response::StaleEpoch(10))
        ));
        assert!(matches!(
            fence_mirrored_response(&shared, zone, [mine.as_slice()], 10, Some(&fenced), Response::Denied),
            Response::Denied
        ));
        shared.note_fenced_range_ops(zone, Some(&fenced), [mine.as_slice()]);
        assert_eq!(shared.range_load.borrow().counts[&zone][owned], 1);

        let mut rehomed = map.clone();
        assert!(rehomed.set_pending(owned, map.token_of(moved_to).to_vec()));
        assert!(rehomed.commit_pending(owned));
        assert_eq!(owner(rehomed.token_of(owned)), 2);
        shared.range_maps.borrow_mut().insert(zone, rehomed);
        assert!(
            matches!(
                fence_mirrored_response(&shared, zone, [mine.as_slice()], 10, Some(&fenced), Response::Denied),
                Response::StaleEpoch(10)
            ),
            "a map change since the pre-apply fence re-runs the fence even at the same epoch"
        );
        assert!(matches!(fence(&mine), Err(Response::StaleEpoch(10))), "the owner cache follows the map");

        shared.range_maps.borrow_mut().insert(zone, map.clone());
        let fenced = fence(&mine).ok().flatten().unwrap();
        shared.ring.borrow_mut().set_members_weighted([(2, 1)]);
        assert!(matches!(
            fence_mirrored_response(&shared, zone, [mine.as_slice()], 10, Some(&fenced), Response::Denied),
            Response::StaleEpoch(10)
        ));
        assert!(matches!(fence(&mine), Err(Response::StaleEpoch(10))), "the owner cache follows the ring");

        shared.ring.borrow_mut().set_members_weighted([(1, 1), (2, 1)]);
        let fenced = fence(&mine).ok().flatten().unwrap();
        let mut finer = map.clone();
        let mut at = map.range_bounds(owned).0.unwrap().to_vec();
        at.push(b'm');
        assert!(finer.split(&at));
        shared.range_maps.borrow_mut().insert(zone, finer);
        shared.reset_range_load();
        shared.note_fenced_range_ops(zone, Some(&fenced), [mine.as_slice()]);
        let counts = shared.range_load.borrow().counts[&zone].clone();
        assert_eq!(counts.len(), map.num_ranges() + 1);
        assert_eq!(counts[owned + 1], 1, "stale fence indices are not reused after a split");
        assert_eq!(counts.iter().sum::<u64>(), 1);
    }

    #[test]
    fn v3_lists_only_ranges_wholly_range_deleted_with_nothing_uncovered() {
        fn store(ordering: Ordering) -> VersionedStore<MemBlockDevice> {
            VersionedStore::new(Engine::create_prewiped(
                MemBlockDevice::new(256, 512),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering },
            ), 1).unwrap()
        }
        let v = |hlc| Version { hlc, node: 1 };
        let shared = ServerShared::single_zone_on(
            1, store(Ordering::Unsorted), Ring::from_members(1, 64, [1]),
        );
        let zone = 42;
        let config = crate::zone::ZoneConfig {
            name: "covered-empty".into(), rf: 1,
            ordering: crate::zone::Ordering::Sorted, scope: None,
        };
        shared.catalog.borrow_mut().insert(zone, config.clone());
        let map = RangeMap::from_splits(
            [b"c".to_vec(), b"g".to_vec(), b"m".to_vec(), b"t".to_vec(), b"x".to_vec()],
        );
        shared.range_maps.borrow_mut().insert(zone, map.clone());
        shared.range_map_epoch.set(10);
        let mut sorted = store(Ordering::Sorted);
        for (hlc, key) in [b"a", b"b", b"d", b"h", b"n", b"u"].into_iter().enumerate() {
            sorted.apply(key, Some(b"v"), v(hlc as u64 + 1)).unwrap();
        }
        sorted.apply_range_delete(None, Some(b"c"), v(10), 0).unwrap();
        sorted.apply_range_delete(Some(b"d"), Some(b"e"), v(11), 0).unwrap();
        sorted.apply_range_delete(Some(b"g"), Some(b"m"), v(12), 0).unwrap();
        sorted.apply_range_delete(Some(b"m"), Some(b"t"), v(13), 0).unwrap();
        sorted.apply(b"p", Some(b"v"), v(14)).unwrap();
        let sorted = Rc::new(RefCell::new(sorted));
        shared.zones.borrow_mut().insert(zone, sorted.clone());
        sorted.borrow().prepare_zone_seed(
            (0, 0, 10), &shared.ring.borrow(), zone, Some(config), Some(map),
        );
        vals_rt::block_on(ensure_tree_ready_cooperative(
            &sorted.borrow(), 1, &std::collections::HashSet::from([0]),
        ), || {}).unwrap();
        while sorted.borrow().rt_fold_pending() {
            sorted.borrow_mut().rt_fold_step().unwrap();
        }
        let Response::RangeStatsV3 { accounting_ready, ranges, covered_empty, .. } =
            range_stats_reply(&shared, true, true) else { panic!("expected V3") };
        assert!(accounting_ready);
        let entries: Vec<(Vec<u8>, u64)> = ranges.iter()
            .filter(|r| r.zone_id == zone)
            .map(|r| (r.lower_bound.clone(), r.entries))
            .collect();
        assert_eq!(entries, vec![
            (b"".to_vec(), 0), (b"c".to_vec(), 0), (b"g".to_vec(), 0),
            (b"m".to_vec(), 1), (b"t".to_vec(), 1), (b"x".to_vec(), 0),
        ]);
        let covered: Vec<&[u8]> = covered_empty.iter()
            .map(|&index| {
                let stat = &ranges[index as usize];
                assert_eq!(stat.zone_id, zone);
                stat.lower_bound.as_slice()
            })
            .collect();
        assert_eq!(
            covered,
            vec![b"".as_slice(), b"g".as_slice()],
            "only wholly range-deleted empty ranges: not a partly deleted one, one written \
             after its delete, a live one, or an empty one nothing deleted"
        );
    }

    #[test]
    fn other_zone_epoch_rebase_keeps_v3_ready_but_rejects_old_mirror_receipt() {
        fn store(ordering: Ordering) -> VersionedStore<MemBlockDevice> {
            VersionedStore::new(Engine::create_prewiped(
                MemBlockDevice::new(256, 512),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering },
            ), 1).unwrap()
        }
        let shared = ServerShared::single_zone_on(
            1, store(Ordering::Unsorted), Ring::from_members(2, 64, [1, 2, 3]),
        );
        let zone = 42;
        let config = crate::zone::ZoneConfig {
            name: "epoch-rebase".into(), rf: 2,
            ordering: crate::zone::Ordering::Sorted, scope: None,
        };
        shared.catalog.borrow_mut().insert(zone, config.clone());
        let map = RangeMap::from_splits([b"m".to_vec()]);
        shared.range_maps.borrow_mut().insert(zone, map.clone());
        shared.range_map_epoch.set(10);
        let mut sorted = store(Ordering::Sorted);
        sorted.apply(b"a", Some(b"v"), Version { hlc: 1, node: 1 }).unwrap();
        sorted.apply(b"z", None, Version { hlc: 2, node: 1 }).unwrap();
        let sorted = Rc::new(RefCell::new(sorted));
        shared.zones.borrow_mut().insert(zone, sorted.clone());
        sorted.borrow().prepare_zone_seed(
            (0, 0, 10), &shared.ring.borrow(), zone, Some(config.clone()), Some(map.clone()),
        );
        vals_rt::block_on(ensure_tree_ready_cooperative(
            &sorted.borrow(), 1, &std::collections::HashSet::from([0]),
        ), || {}).unwrap();
        assert!(logical_stats(&shared).0);
        shared.range_maps.borrow_mut().insert(43, RangeMap::from_splits([b"other".to_vec()]));
        shared.range_map_epoch.set(11);
        sorted.borrow().prepare_zone_seed(
            (0, 0, 11), &shared.ring.borrow(), zone, Some(config), Some(map),
        );
        let Response::RangeStatsV3 {
            epoch, accounting_epoch, accounting_ready, ranges, recovery_seeding, ..
        } = range_stats_reply(&shared, true, true) else { panic!("expected V3") };
        assert_eq!((epoch, accounting_epoch), (11, 11));
        assert!(accounting_ready);
        assert_eq!(recovery_seeding, Some(false));
        assert_eq!(ranges.iter().filter(|r| r.zone_id == zone).map(|r| r.entries).collect::<Vec<_>>(), vec![1, 1]);
        assert_eq!(sorted.borrow().logical_range_counts(10), None);
        assert!(matches!(
            fence_mirrored_response(&shared, zone, [b"a".as_slice()], 10, None, Response::Denied),
            Response::StaleEpoch(11)
        ));
        sorted.borrow().invalidate_leaf_seed();
        assert!(!sorted.borrow().is_leaf_seeded());
        let Response::RangeStatsV3 { recovery_seeding, .. } =
            range_stats_reply(&shared, true, true) else { panic!("expected V3") };
        assert_eq!(recovery_seeding, Some(false), "a non-recovery reseed is not reported as recovery");
        assert!(matches!(range_stats_reply(&shared, true, false), Response::RangeStatsV2 { .. }));
    }

    #[test]
    fn only_reopened_unseeded_engines_report_recovery_seeding() {
        let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted };
        let reopened = || {
            let dev = MemBlockDevice::new(256, 1024);
            let mut s = VersionedStore::new(Engine::create_prewiped(dev.share(), cfg), 1).unwrap();
            for i in 0..40u64 {
                s.apply(format!("k{i:02}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                    .unwrap();
            }
            s.flush().unwrap();
            s.sync().unwrap();
            drop(s);
            VersionedStore::open(dev.share(), cfg, 1).unwrap()
        };
        let fresh = VersionedStore::new(Engine::create_prewiped(
            MemBlockDevice::new(256, 512),
            Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        ), 1).unwrap();
        let shared = ServerShared::single_zone_on(1, fresh, Ring::from_members(2, 64, [1, 2, 3]));
        let config = crate::zone::ZoneConfig {
            name: "reopened".into(), rf: 2, ordering: crate::zone::Ordering::Sorted, scope: None,
        };
        let map = RangeMap::from_splits([b"k20".to_vec()]);
        shared.range_map_epoch.set(10);
        let flag = |shared: &ServerShared<MemBlockDevice>| {
            let Response::RangeStatsV3 { recovery_seeding, .. } =
                range_stats_reply(shared, true, true) else { panic!("expected V3") };
            recovery_seeding
        };
        assert_eq!(flag(&shared), Some(false));
        let orphan = Rc::new(RefCell::new(reopened()));
        assert!(orphan.borrow().recovery_seed_pending());
        shared.zones.borrow_mut().insert(43, orphan);
        assert_eq!(flag(&shared), Some(false), "a store without a zone config is skipped");

        let zone = 42;
        shared.catalog.borrow_mut().insert(zone, config.clone());
        shared.range_maps.borrow_mut().insert(zone, map.clone());
        let store = Rc::new(RefCell::new(reopened()));
        shared.zones.borrow_mut().insert(zone, store.clone());
        assert_eq!(flag(&shared), Some(true), "a reopened engine reseeding after a restart is recovery");
        store.borrow().prepare_zone_seed(
            (0, 0, 10), &shared.ring.borrow(), zone, Some(config.clone()), Some(map.clone()),
        );
        assert_eq!(flag(&shared), Some(true));
        vals_rt::block_on(ensure_tree_ready_cooperative(
            &store.borrow(), 1, &std::collections::HashSet::from([0]),
        ), || {}).unwrap();
        assert_eq!(flag(&shared), Some(false), "recovery ends when the seed publishes");

        let mut moved = map.clone();
        assert!(moved.split(b"k30"));
        shared.range_maps.borrow_mut().insert(zone, moved.clone());
        shared.range_map_epoch.set(11);
        let wider = Ring::from_members(2, 64, [1, 2, 3, 4]);
        store.borrow().prepare_zone_seed((1, 0, 11), &wider, zone, Some(config), Some(moved));
        assert!(!store.borrow().is_leaf_seeded(), "a placement change reseeds the engine");
        assert_eq!(flag(&shared), Some(false), "a placement-change reseed is not recovery");
    }

    #[test]
    fn late_attached_and_swapped_stores_inherit_the_driver_and_sync_tree_answers_busy() {
        let _policy = crate::replica::seed_policy::policy_for_test(true, 25, 64 << 20);
        let geom = crate::zonestore::ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 512,
        };
        let provider = Rc::new(crate::zonestore::MemZoneStore::new(1, geom));
        let zone0 = crate::zonestore::ZoneStore::open(
            &*provider, DEFAULT_ZONE, crate::zone::Ordering::Unsorted,
        ).unwrap();
        let zone = 5;
        let mut catalog = Catalog::new();
        catalog.insert(zone, crate::zone::ZoneConfig {
            name: "late".into(), rf: 1, ordering: crate::zone::Ordering::Unsorted, scope: None,
        });
        let shared = ServerShared::zoned(1, zone0, provider, catalog, Ring::from_members(1, 64, [1]));
        shared.attach_seed_driver(true);
        assert!(shared.zone_store(DEFAULT_ZONE).unwrap().borrow().seed_driver_attached());
        let late = shared.zone_store(zone).unwrap();
        assert!(late.borrow().seed_driver_attached(), "a zone opened after the driver attached is driven");
        for i in 0..20u64 {
            late.borrow_mut()
                .apply(format!("k{i:02}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                .unwrap();
        }
        late.borrow().invalidate_leaf_seed();
        let sync_tree = || OwnedRequest::SyncTree {
            zone_id: zone, level: 1, scope_g: 1, scope: vec![0], requester: None,
        };
        let busy_before = crate::observe::counters().ae_seed_not_ready_busy.get();
        assert!(matches!(
            serve_request_mode(&shared, sync_tree(), unconditional_apply_mode()),
            Response::Busy
        ));
        let scope = std::collections::HashSet::from([0]);
        let served = vals_rt::block_on(serve_sync_tree(&late, 1, 1, &scope, None), || {});
        assert!(matches!(served, Response::Busy), "a peer SyncTree never drives the seed");
        assert!(!late.borrow().is_leaf_seeded());
        assert!(crate::observe::counters().ae_seed_not_ready_busy.get() >= busy_before + 2);

        shared.attach_seed_driver(false);
        let served = vals_rt::block_on(serve_sync_tree(&late, 1, 1, &scope, None), || {});
        assert!(matches!(served, Response::Tree(_)), "knockout: without a driver the handler seeds on demand");
        assert!(matches!(
            serve_request_mode(&shared, sync_tree(), unconditional_apply_mode()),
            Response::Tree(_)
        ));

        for attached in [true, false] {
            shared.attach_seed_driver(attached);
            let swapped = shared
                .prepare_default_zone_swap(
                    MemBlockDevice::new(256, 512),
                    Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
                    1,
                )
                .unwrap();
            shared.commit_default_zone_swap(swapped);
            assert_eq!(
                shared.zone_store(DEFAULT_ZONE).unwrap().borrow().seed_driver_attached(),
                attached,
                "a swapped-in default zone inherits the driver attachment"
            );
        }
    }

    #[test]
    fn logical_accounting_poll_does_not_read_run_sidecars() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Arc;

        struct PanicReadDevice {
            inner: MemBlockDevice,
            armed: Arc<AtomicBool>,
        }

        impl BlockDevice for PanicReadDevice {
            fn block_size(&self) -> usize {
                self.inner.block_size()
            }

            fn num_blocks(&self) -> u64 {
                self.inner.num_blocks()
            }

            fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                assert!(
                    !self.armed.load(AtomicOrdering::Acquire),
                    "RangeStatsV3 poll performed synchronous run-sidecar I/O"
                );
                self.inner.read_at(lba, buf)
            }

            fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
                self.inner.write_at(lba, buf)
            }

            fn flush(&self) -> vals_spdk::Result<()> {
                self.inner.flush()
            }

            fn sibling(&self) -> vals_spdk::Result<Self> {
                Ok(PanicReadDevice {
                    inner: self.inner.share(),
                    armed: self.armed.clone(),
                })
            }
        }

        let armed = Arc::new(AtomicBool::new(false));
        let dev = PanicReadDevice {
            inner: MemBlockDevice::new(256, 512),
            armed: armed.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store
            .apply(
                b"run-backed",
                Some(b"value"),
                Version { hlc: 1, node: 1 },
            )
            .unwrap();
        store.flush().unwrap();
        assert!(store.run_count() > 0);
        store
            .append_unknown_for_test(
                b"dirty",
                Some(b"value"),
                Version { hlc: 2, node: 1 },
            )
            .unwrap();
        store.invalidate_leaf_seed();
        let shared = ServerShared::single_zone(store);

        armed.store(true, AtomicOrdering::Release);
        let (ready, _) = logical_stats(&shared);
        assert!(!ready);
        assert!(shared.take_rebalance_pending());
    }

    #[test]
    fn unsorted_unknown_insert_is_unready_until_exact_rebuild() {
        let dev = MemBlockDevice::new(256, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        let shared = ServerShared::single_zone(VersionedStore::new(engine, 1).unwrap());
        assert!(!shared.take_rebalance_pending());
        let (ready, ranges) = logical_stats(&shared);
        assert!(ready);
        assert_eq!(ranges[0].entries, 0);
        assert!(!shared.take_rebalance_pending());

        let store = shared.zones.borrow().get(&DEFAULT_ZONE).unwrap().clone();
        store
            .borrow_mut()
            .append_unknown_for_test(
                b"previously-absent",
                Some(b"value"),
                Version { hlc: 1, node: 1 },
            )
            .unwrap();
        assert!(!shared.take_rebalance_pending());
        let before = crate::observe::snapshot().logical_accounting_reseed_requests;
        let (ready, ranges) = logical_stats(&shared);
        assert!(!ready, "a dirty live-key count must not enter the planner");
        assert_eq!(ranges[0].entries, 0, "the unready count is stale");
        assert!(shared.take_rebalance_pending(), "the unready poll must schedule a reseed");
        assert!(
            crate::observe::snapshot().logical_accounting_reseed_requests > before
        );

        let rebuilt = {
            let store = store.borrow();
            vals_rt::block_on(
                ensure_tree_ready_cooperative(
                    &store,
                    1,
                    &std::collections::HashSet::from([0]),
                ),
                || {},
            )
            .unwrap()
        };
        assert!(rebuilt);
        let (ready, ranges) = logical_stats(&shared);
        assert!(ready, "an exact dirty-bucket rebuild restores readiness");
        assert_eq!(ranges[0].entries, 1);
    }
    #[test]
    fn populated_split_poll_is_unready_without_run_io_until_recount() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
        use std::sync::Arc;

        struct PanicReadDevice {
            inner: MemBlockDevice,
            armed: Arc<AtomicBool>,
        }

        impl BlockDevice for PanicReadDevice {
            fn block_size(&self) -> usize {
                self.inner.block_size()
            }

            fn num_blocks(&self) -> u64 {
                self.inner.num_blocks()
            }

            fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                assert!(
                    !self.armed.load(AtomicOrdering::Acquire),
                    "RangeStatsV3 poll after a split performed synchronous run I/O"
                );
                self.inner.read_at(lba, buf)
            }

            fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
                self.inner.write_at(lba, buf)
            }

            fn flush(&self) -> vals_spdk::Result<()> {
                self.inner.flush()
            }

            fn sibling(&self) -> vals_spdk::Result<Self> {
                Ok(PanicReadDevice { inner: self.inner.share(), armed: self.armed.clone() })
            }
        }

        let armed = Arc::new(AtomicBool::new(false));
        let store = |ordering: Ordering| {
            let dev = PanicReadDevice { inner: MemBlockDevice::new(256, 1 << 14), armed: armed.clone() };
            VersionedStore::new(Engine::create_prewiped(
                dev,
                Config { seg_blocks: 8, reserved_blocks: 0, ordering },
            ), 1).unwrap()
        };
        let shared = ServerShared::single_zone_on(
            1, store(Ordering::Unsorted), Ring::from_members(2, 64, [1, 2, 3]),
        );
        let zone = 42;
        let config = crate::zone::ZoneConfig {
            name: "split-recount".into(), rf: 2,
            ordering: crate::zone::Ordering::Sorted, scope: None,
        };
        shared.catalog.borrow_mut().insert(zone, config.clone());
        let map = RangeMap::from_splits([b"k100".to_vec()]);
        shared.range_maps.borrow_mut().insert(zone, map.clone());
        shared.range_map_epoch.set(10);
        let mut sorted = store(Ordering::Sorted);
        for i in 0..400u64 {
            let key = format!("k{i:03}");
            sorted.apply(key.as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 }).unwrap();
            if i % 100 == 99 {
                sorted.flush().unwrap();
            }
        }
        let sorted = Rc::new(RefCell::new(sorted));
        shared.zones.borrow_mut().insert(zone, sorted.clone());
        sorted.borrow().prepare_zone_seed(
            (0, 0, 10), &shared.ring.borrow(), zone, Some(config.clone()), Some(map.clone()),
        );
        while !sorted.borrow().seed_step().unwrap() {}
        assert!(logical_stats(&shared).0);
        let zone_entries = |ranges: &[wire::OwnedRangeStat]| {
            ranges.iter().filter(|r| r.zone_id == zone).map(|r| r.entries).collect::<Vec<_>>()
        };

        let mut split = map.clone();
        assert!(split.split(b"k300"));
        shared.range_maps.borrow_mut().insert(zone, split.clone());
        shared.range_map_epoch.set(11);
        let _ = shared.take_rebalance_pending();
        armed.store(true, AtomicOrdering::Release);
        let (ready, pending) = logical_stats(&shared);
        armed.store(false, AtomicOrdering::Release);
        assert!(!ready, "a pending split recount must not enter the planner");
        let pending = zone_entries(&pending);
        assert_eq!(pending[0], 100, "a range outside the pending span reports its exact count");
        assert!(pending[1] <= 300 && pending[2] <= 300, "pending children are capped by the span total");
        assert!(shared.take_rebalance_pending(), "the unready poll kicks accounting");
        assert!(sorted.borrow().is_leaf_seeded());
        assert!(sorted.borrow().logical_recount_pending());
        while sorted.borrow().logical_recount_step().unwrap() != crate::replica::RecountStep::Done {}
        let (ready, ranges) = logical_stats(&shared);
        assert!(ready);
        assert_eq!(zone_entries(&ranges), vec![100, 200, 100]);
    }
}

