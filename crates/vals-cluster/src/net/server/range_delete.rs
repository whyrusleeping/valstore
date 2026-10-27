use super::*;
use crate::observe::{note_range_delete_refused, RangeDeleteRefusal};

pub(super) fn fence_range_delete<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    slices: &[wire::OwnedSlice],
) -> Result<(), Response> {
    let catalog = shared.catalog.borrow();
    let Some(cfg) = catalog.get(zone_id) else {
        if shared.held_uncataloged_sorted(zone_id) {
            note_range_delete_refused(RangeDeleteRefusal::Stale);
            return Err(stale_range_write(shared));
        }
        return Err(Response::Err(format!("range delete: unknown zone {zone_id}")));
    };
    if cfg.ordering != crate::zone::Ordering::Sorted {
        return Err(Response::Err(format!("range delete: zone {zone_id} is not sorted")));
    }
    if slices
        .iter()
        .any(|(lo, hi)| matches!((lo, hi), (Some(l), Some(h)) if l >= h))
    {
        return Err(Response::Err("range delete: empty slice (lo >= hi)".into()));
    }
    if !shared.sorted_placement_ready() {
        note_range_delete_refused(RangeDeleteRefusal::Stale);
        return Err(stale_range_write(shared));
    }
    let ring = shared.ring.borrow();
    let owned = with_fence_map(shared, zone_id, &ring, |map| {
        shared.range_owner_cache(zone_id, map.num_ranges(), |owners| {
            slices.iter().all(|(lo, hi)| {
                map.ranges_overlapping(lo.as_deref(), hi.as_deref())
                    .iter()
                    .all(|slice| {
                        *owners[slice.index].get_or_insert_with(|| {
                            owns_range_write(shared, zone_id, cfg, map, &ring, slice.index)
                        })
                    })
            })
        })
    });
    if owned.is_none_or(|owned| owned) {
        Ok(())
    } else {
        note_range_delete_refused(RangeDeleteRefusal::Stale);
        Err(stale_range_write(shared))
    }
}

fn apply_slices<D: BlockDevice>(
    store: &RefCell<VersionedStore<D>>,
    version: crate::hlc::Version,
    slices: &[wire::OwnedSlice],
) -> Result<Vec<Applied>, Response> {
    let wall_ms = crate::hlc::physical_now_ms();
    let mut s = store.borrow_mut();
    let mut results = Vec::with_capacity(slices.len());
    for (lo, hi) in slices {
        match s.apply_range_delete(lo.as_deref(), hi.as_deref(), version, wall_ms) {
            Ok(Applied::Rejected) => {
                note_range_delete_refused(RangeDeleteRefusal::Skew);
                results.push(Applied::Rejected);
            }
            Ok(applied) => {
                crate::observe::counters().range_deletes_applied_total.inc();
                results.push(applied);
            }
            Err(e) if e.is_backpressure() || e.is_device_full() => {
                note_range_delete_refused(RangeDeleteRefusal::Space);
                return Err(engine_space_rejection(s.engine()));
            }
            Err(e) => {
                note_range_delete_refused(match e {
                    vals_store::EngineError::RangeTombstoneCap(_) => RangeDeleteRefusal::Cap,
                    vals_store::EngineError::RangeTombstonesUnsupported => {
                        RangeDeleteRefusal::Unsupported
                    }
                    _ => RangeDeleteRefusal::Error,
                });
                return Err(Response::Err(format!("range delete: {e}")));
            }
        }
    }
    Ok(results)
}

pub(super) async fn serve_range_delete<D: BlockDevice + AsyncRead>(
    shared: Rc<ServerShared<D>>,
    zone_id: ZoneId,
    store: &RefCell<VersionedStore<D>>,
    sync: bool,
    version: crate::hlc::Version,
    slices: &[wire::OwnedSlice],
) -> Response {
    if let Err(resp) = fence_range_delete(&shared, zone_id, slices) {
        return resp;
    }
    let stamp = shared.placement_stamp();
    let mirror_epoch = shared.range_map_epoch();
    let applied = apply_slices(store, version, slices);
    if store.borrow().rt_fold_pending() {
        let folded = crate::replica::rt_fold_turn_with(store, Some(shared.read_gate()), || {}).await;
        if !matches!(folded, Ok(crate::replica::RecountStep::Done)) || store.borrow().rt_fold_pending() {
            shared.kick_logical_accounting();
        }
    }
    let results = match applied {
        Ok(results) => results,
        Err(resp) => return resp,
    };
    if sync {
        match group_commit(&shared, zone_id, store).await {
            SyncResult::Ok => {}
            SyncResult::Space => {
                note_range_delete_refused(RangeDeleteRefusal::Space);
                return space_rejection(store);
            }
            SyncResult::Err(e) => {
                note_range_delete_refused(RangeDeleteRefusal::Error);
                return Response::Err(e);
            }
        }
    }
    let mirrored: Vec<&wire::OwnedSlice> = slices
        .iter()
        .zip(&results)
        .filter_map(|(slice, applied)| (*applied != Applied::Rejected).then_some(slice))
        .collect();
    let outcome = mirror_rehome_range_delete(&shared, zone_id, version, sync, &mirrored).await;
    if outcome == MirrorOutcome::Failed {
        note_range_delete_refused(RangeDeleteRefusal::Mirror);
    }
    let response = mirrored_response(
        outcome,
        Response::Batch(results),
        "re-home mirror failed; retry range delete",
    );
    if shared.range_map_epoch() != mirror_epoch {
        note_range_delete_refused(RangeDeleteRefusal::Stale);
        return stale_range_write(&shared);
    }
    if shared.placement_stamp() != stamp || !shared.sorted_placement_ready() {
        if let Err(resp) = fence_range_delete(&shared, zone_id, slices) {
            return resp;
        }
    }
    response
}

struct RangeDeleteMirror {
    lower: Vec<u8>,
    target: NodeId,
    slices: Vec<wire::OwnedSlice>,
}

async fn mirror_rehome_range_delete<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    version: crate::hlc::Version,
    sync: bool,
    slices: &[&wire::OwnedSlice],
) -> MirrorOutcome {
    let maps = shared.range_maps.borrow();
    let Some(map) = maps.get(&zone_id) else {
        return MirrorOutcome::Mirrored;
    };
    let pieces: Vec<crate::partition::RangeSlice> = slices
        .iter()
        .flat_map(|(lo, hi)| map.ranges_overlapping(lo.as_deref(), hi.as_deref()))
        .collect();
    bump_range_write_marks(shared, zone_id, map, pieces.iter().map(|piece| piece.index));
    if map.active_rehomes() == 0 {
        return MirrorOutcome::Mirrored;
    }
    let Some(cfg) = shared.zone_config(zone_id) else {
        return MirrorOutcome::Failed;
    };
    let ring = shared.ring.borrow().clone();
    let epoch = shared.range_map_epoch();
    let (shard_index, shard_count) = shared.shard();
    let shard_of = |token: &[u8]| (crate::ring::hash64(token) % shard_count as u64) as usize;
    let mut groups: std::collections::BTreeMap<(usize, NodeId), Vec<wire::OwnedSlice>> =
        std::collections::BTreeMap::new();
    for piece in pieces {
        let Some(pending) = map.pending_of(piece.index) else { continue };
        let current = map.replicas_of_range(&ring, zone_id, piece.index, cfg.rf, cfg.scope.as_ref());
        let current_shard = shard_of(map.token_of(piece.index));
        if !current.contains(&shared.node_id) || shard_index != current_shard {
            continue;
        }
        let pending_shard = shard_of(pending);
        for target in ring
            .replicas_in_zone(zone_id, pending, cfg.rf, cfg.scope.as_ref())
            .into_iter()
            .filter(|n| !current.contains(n) || current_shard != pending_shard)
        {
            groups
                .entry((piece.index, target))
                .or_default()
                .push((piece.lo.clone(), piece.hi.clone()));
        }
    }
    let mut batches = Vec::new();
    for ((index, target), slices) in groups {
        let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
        for chunk in slices.chunks(wire::MAX_RANGE_DELETE_SLICES) {
            batches.push(RangeDeleteMirror { lower: lower.clone(), target, slices: chunk.to_vec() });
        }
    }
    drop(maps);
    if batches.is_empty() {
        return MirrorOutcome::Mirrored;
    }
    let transport = rehome_transport(shared, &ring, epoch);
    let sizes: Vec<usize> = batches.iter().map(|batch| batch.slices.len()).collect();
    let outcomes = apply_mirror_batches(&sizes, |index| {
        let batch = &batches[index];
        let transport = &transport;
        async move {
            let slices: Vec<wire::WireSlice> = batch
                .slices
                .iter()
                .map(|(lo, hi)| (lo.as_deref(), hi.as_deref()))
                .collect();
            transport
                .range_delete(batch.target, zone_id, sync, version, &slices)
                .await
        }
    })
    .await;
    let mut outcome = MirrorOutcome::Mirrored;
    let mut stats = shared.rehome_mirrors.borrow_mut();
    for (batch, target_outcome) in batches.iter().zip(outcomes) {
        let stat = stats.entry((zone_id, batch.lower.clone())).or_default();
        if stat.epoch != epoch {
            *stat = RehomeMirrorStat { epoch, ok: 0, failures: 0, copy_complete_epoch: 0 };
        }
        record_mirror_outcome(stat, &mut outcome, target_outcome);
    }
    outcome
}

pub(super) fn serve_sync_range_tombstones<D: BlockDevice>(
    shared: &ServerShared<D>,
    zone_id: ZoneId,
    requester: Option<NodeId>,
    digest: u64,
    after: Option<&[u8]>,
) -> Response {
    let store = match shared.zone_store(zone_id) {
        Ok(store) => store,
        Err(e) => return Response::Err(e),
    };
    let Some(cfg) = shared.zone_config(zone_id) else {
        return Response::Err(format!("range tombstones: unknown zone {zone_id}"));
    };
    let fragments = crate::rt_sync::local_fragments(&store.borrow());
    let page = match requester {
        Some(requester) if cfg.ordering == crate::zone::Ordering::Sorted => {
            let Some(map) = shared.sorted_range_map(zone_id) else {
                return Response::Busy;
            };
            let ring = shared.ring.borrow();
            let scope = crate::rt_sync::RtScope::new(
                &map,
                &ring,
                zone_id,
                cfg.rf,
                cfg.scope.as_ref(),
                shared.node_id,
                Some(requester),
                shared.shard(),
            );
            crate::rt_sync::respond(fragments, Some(&scope), digest, after)
        }
        _ => crate::rt_sync::respond(fragments, None, digest, after),
    };
    Response::RangeTombstones { same: page.same, more: page.more, fragments: page.fragments }
}
