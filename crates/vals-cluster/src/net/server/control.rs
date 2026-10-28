use super::*;

pub(super) fn serve_control_request<D: BlockDevice>(
    shared: &ServerShared<D>,
    req: OwnedRequest,
) -> Response {
    macro_rules! control_auth {
        ($auth:expr, $op:literal) => {
            if !crate::auth::verify($auth) {
                log::warn!(
                    "node {}: DENIED unauthenticated {} (control secret mismatch; \
                     a legitimate push means a misconfigured cluster secret)",
                    shared.node_id,
                    $op
                );
                crate::observe::counters().control_denied.inc();
                return Response::Denied;
            }
        };
    }
    macro_rules! control_incarnation {
        ($incarnation:expr, $op:literal) => {
            if let Err(e) = shared.admit_incarnation($incarnation, $op) {
                return Response::Err(e);
            }
        };
    }
    match req {
        OwnedRequest::SetMembers {
            auth,
            epoch,
            vnodes,
            rf,
            members,
            addrs,
            incarnation,
        } => {
            control_auth!(&auth, "SetMembers");
            control_incarnation!(incarnation, "SetMembers");
            let old_epoch = shared.ring_epoch();
            if epoch >= old_epoch {
                if epoch > old_epoch {
                    log::info!(
                        "node {}: adopting ring config epoch {epoch} (was {old_epoch}): \
                         {} members, vnodes {vnodes}, rf {rf}",
                        shared.node_id,
                        members.len()
                    );
                    crate::observe::counters().ring_adoptions.inc();
                }
                shared.epoch.set(epoch);
                *shared.rehome_transport.borrow_mut() = None;
                shared.membership_adopted.set(true);
                let mut ring = shared.ring.borrow_mut();
                if vnodes != 0 && rf >= 1 {
                    ring.reconfigure(rf as usize, vnodes, members);
                } else {
                    ring.set_members_weighted(members);
                }
                drop(ring);
                shared.merge_peer_addrs(&addrs);
                shared.kick_sweep();
            } else {
                log::debug!(
                    "node {}: ignoring stale SetMembers push (epoch {epoch} < {old_epoch})",
                    shared.node_id
                );
                crate::observe::counters().stale_pushes_ignored.inc();
            }
            members_reply(shared)
        }
        OwnedRequest::Members => members_reply(shared),
        OwnedRequest::SetZones {
            auth,
            epoch,
            zones,
            incarnation,
        } => {
            control_auth!(&auth, "SetZones");
            control_incarnation!(incarnation, "SetZones");
            let old_epoch = shared.catalog_epoch();
            if epoch >= old_epoch {
                if epoch > old_epoch {
                    log::info!(
                        "node {}: adopting zone catalog epoch {epoch} (was {old_epoch}): \
                         {} zones",
                        shared.node_id,
                        zones.len()
                    );
                    crate::observe::counters().catalog_adoptions.inc();
                }
                shared.catalog_epoch.set(epoch);
                let new_cat = catalog_from_wire(zones);
                let stale: Vec<ZoneId> = shared
                    .zones
                    .borrow()
                    .keys()
                    .copied()
                    .filter(|&z| z != DEFAULT_ZONE && !new_cat.contains(z))
                    .collect();
                for z in stale {
                    shared.drop_zone(z);
                }
                *shared.catalog.borrow_mut() = new_cat;
                shared.release_uncataloged_sorted();
                shared.persist_catalog();
                shared.expect_range_maps();
                shared.kick_sweep();
            } else {
                log::debug!(
                    "node {}: ignoring stale SetZones push (epoch {epoch} < {old_epoch})",
                    shared.node_id
                );
                crate::observe::counters().stale_pushes_ignored.inc();
            }
            catalog_reply(shared)
        }
        OwnedRequest::Zones => catalog_reply(shared),
        OwnedRequest::SetRangeMaps {
            auth,
            epoch,
            maps,
            incarnation,
        } => {
            control_auth!(&auth, "SetRangeMaps");
            control_incarnation!(incarnation, "SetRangeMaps");
            let old_epoch = shared.range_map_epoch();
            if epoch >= old_epoch {
                if epoch > old_epoch {
                    log::info!(
                        "node {}: adopting range maps epoch {epoch} (was {old_epoch}): \
                         {} sorted zones",
                        shared.node_id,
                        maps.len()
                    );
                    crate::observe::counters().range_map_adoptions.inc();
                }
                let mut new_maps = match range_maps_from_wire(maps) {
                    Ok(maps) => maps,
                    Err(e) => {
                        log::error!("node {}: rejected SetRangeMaps epoch {epoch}: {e}", shared.node_id);
                        return Response::Err(e.into());
                    }
                };
                shared.range_map_epoch.set(epoch);
                *shared.rehome_transport.borrow_mut() = None;
                {
                    let mut stats = shared.rehome_mirrors.borrow_mut();
                    stats.clear();
                    for (&zone_id, map) in &new_maps {
                        for index in 0..map.num_ranges() {
                            if map.pending_of(index).is_some() {
                                let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
                                stats.insert(
                                    (zone_id, lower),
                                    RehomeMirrorStat {
                                        epoch,
                                        ok: 0,
                                        failures: 0,
                                        copy_complete_epoch: 0,
                                    },
                                );
                            }
                        }
                    }
                }
                let maps_changed = {
                    let old_maps = shared.range_maps.borrow();
                    let mut changed = old_maps.len() != new_maps.len();
                    for (zone_id, map) in new_maps.iter_mut() {
                        match old_maps.get(zone_id) {
                            Some(old) if old == map => *map = old.clone(),
                            _ => changed = true,
                        }
                    }
                    changed
                };
                *shared.range_maps.borrow_mut() = new_maps;
                shared.adopt_range_maps();
                if maps_changed {
                    shared.reset_range_load();
                }
                {
                    shared.prune_range_write_marks(crate::hlc::physical_now_ms());
                    let boundary_kept = |zone_id: &ZoneId, lower: &[u8]| {
                        shared.range_maps.borrow().get(zone_id).is_some_and(|map| {
                            let index = map.range_index(lower);
                            map.range_bounds(index).0.unwrap_or_default() == lower
                        })
                    };
                    shared
                        .rehome_copy_progress
                        .borrow_mut()
                        .retain(|(zone_id, lower), _| boundary_kept(zone_id, lower));
                }
                shared.kick_sweep();
            } else {
                log::debug!(
                    "node {}: ignoring stale SetRangeMaps push (epoch {epoch} < {old_epoch})",
                    shared.node_id
                );
                crate::observe::counters().stale_pushes_ignored.inc();
            }
            range_maps_reply(shared)
        }
        OwnedRequest::RangeMaps => range_maps_reply(shared),
        OwnedRequest::RangeStats => range_stats_reply(shared, false, false),
        OwnedRequest::RangeStatsV2 => range_stats_reply(shared, true, false),
        OwnedRequest::RangeStatsV3 => range_stats_reply(shared, true, true),
        OwnedRequest::RangeWriteMark {
            epoch,
            zone_id,
            lower,
        } => {
            if shared.range_map_epoch() < epoch {
                return Response::StaleEpoch(shared.range_map_epoch());
            }
            let map = shared.range_map(zone_id);
            let index = map.range_index(&lower);
            if map.range_bounds(index).0.unwrap_or_default() != lower.as_slice()
                || map.pending_of(index).is_none()
            {
                return Response::StaleEpoch(shared.range_map_epoch());
            }
            Response::Watermark(shared.range_write_mark_register(
                zone_id,
                lower,
                crate::hlc::physical_now_ms(),
            ))
        }
        OwnedRequest::GetWatermark => Response::Watermark(shared.watermark()),
        OwnedRequest::GcFloor { zone_id } => match shared.zone_store(zone_id) {
            Ok(store) => Response::GcFloor(store.borrow().admission_floor_ms(crate::hlc::physical_now_ms())),
            Err(e) => Response::Err(e),
        },
        OwnedRequest::CleanupStatus => shared.cleanup_status_reply(),
        _ => Response::Err("data request requires the async server".into()),
    }
}

#[cfg(test)]
pub(crate) fn serve_request<D: BlockDevice>(
    shared: &ServerShared<D>,
    req: OwnedRequest,
) -> Response {
    serve_request_mode(shared, req, unconditional_apply_mode())
}

#[cfg(test)]
pub(super) fn serve_request_mode<D: BlockDevice>(
    shared: &ServerShared<D>,
    req: OwnedRequest,
    mode: ApplyMode,
) -> Response {
    if matches!(
        &req,
        OwnedRequest::Apply { .. } | OwnedRequest::BatchApply { .. }
    ) && crate::ram::ram_over_budget()
    {
        crate::observe::counters().ram_busy_rejections.inc();
        return Response::Busy;
    }
    macro_rules! zone {
        ($zone_id:expr) => {
            match shared.zone_store($zone_id) {
                Ok(s) => s,
                Err(e) => return Response::Err(e),
            }
        };
    }
    match req {
        OwnedRequest::Apply {
            zone_id,
            key,
            value,
            version,
            ..
        } => {
            if let Some(resp) =
                rehome_write_fence(shared, zone_id, std::iter::once(key.as_slice()))
            {
                return resp;
            }
            let store = zone!(zone_id);
            if let Some(resp) = admission_shed_rejection(&store) {
                return resp;
            }
            shared.note_range_ops(zone_id, std::iter::once(key.as_slice()));
            let started = std::time::Instant::now();
            let r = store.borrow_mut().apply_unconditional_at(
                &key,
                value.as_deref(),
                version,
                crate::hlc::physical_now_ms(),
                mode,
            );
            match r {
                Ok(a) => {
                    note_single_apply_written(a, value.as_ref().map_or(0, Vec::len));
                    crate::observe::histograms()
                        .apply_latency_us
                        .observe(started.elapsed());
                    Response::Apply(a)
                }
                Err(e) if e.is_backpressure() || e.is_device_full() => {
                    space_rejection(&store)
                }
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::Read { zone_id, key, .. } => {
            let store = zone!(zone_id);
            shared.note_range_ops(zone_id, std::iter::once(key.as_slice()));
            let r = store.borrow().get_versioned(&key);
            match r {
                Ok(o) => Response::Read(o),
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::Digest { zone_id, key, .. } => {
            vals_store::attribution::DIGEST_LOOKUPS.add(1);
            let store = zone!(zone_id);
            shared.note_range_ops(zone_id, std::iter::once(key.as_slice()));
            let r = store.borrow().digest_version(&key);
            match r {
                Ok(version) => Response::Digest(version),
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::Scan {
            zone_id,
            lo,
            hi,
            max,
            ..
        } => {
            let store = zone!(zone_id);
            shared.note_range_ops(zone_id, std::iter::once(lo.as_deref().unwrap_or_default()));
            let cap = if max == 0 { usize::MAX } else { max as usize };
            let started = std::time::Instant::now();
            let r = store
                .borrow()
                .scan_page(lo.as_deref(), hi.as_deref(), cap, SCAN_REPLY_BUDGET);
            crate::observe::histograms()
                .scan_latency_us
                .observe(started.elapsed());
            match r {
                Ok((entries, more)) => {
                    crate::observe::counters()
                        .scan_entries_returned
                        .add(entries.len() as u64);
                    Response::Scan { entries, more, fragments: Vec::new(), resume: None }
                }
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::BatchApply {
            zone_id,
            sync,
            writes,
            ..
        } => {
            if let Some(resp) = rehome_write_fence(
                shared,
                zone_id,
                writes.iter().map(|(key, _, _)| key.as_slice()),
            ) {
                return resp;
            }
            let store = zone!(zone_id);
            if let Some(resp) = admission_shed_rejection(&store) {
                return resp;
            }
            shared.note_range_ops(
                zone_id,
                writes.iter().map(|(key, _, _)| key.as_slice()),
            );
            let started = std::time::Instant::now();
            let value_bytes: u64 = writes
                .iter()
                .map(|(_, v, _)| v.as_ref().map_or(0, |v| v.len() as u64))
                .sum();
            let mut store = store.borrow_mut();
            let mut results = Vec::with_capacity(writes.len());
            let wall_ms = crate::hlc::physical_now_ms();
            for (key, value, version) in &writes {
                match store.apply_unconditional_at(
                    key,
                    value.as_deref(),
                    *version,
                    wall_ms,
                    mode,
                ) {
                    Ok(a) => results.push(a),
                    Err(e) if e.is_backpressure() || e.is_device_full() => {
                        return engine_space_rejection(store.engine());
                    }
                    Err(e) => return Response::Err(format!("{e}")),
                }
            }
            if sync {
                let flush_started = std::time::Instant::now();
                if let Err(e) = store.sync() {
                    if mode == ApplyMode::Blind {
                        crate::observe::counters().blind_sync_errors.inc();
                    }
                    if e.is_backpressure() || e.is_device_full() {
                        return engine_space_rejection(store.engine());
                    }
                    return Response::Err(format!("{e}"));
                }
                crate::observe::histograms()
                    .sync_flush_us
                    .observe(flush_started.elapsed());
                let c = crate::observe::counters();
                c.sync_commits.inc();
                c.synced_writes.add(writes.len() as u64);
                shared.record_synced_writes(writes.len() as u64);
            }
            let c = crate::observe::counters();
            c.records_written.add(writes.len() as u64);
            c.record_bytes_written.add(value_bytes);
            crate::observe::histograms()
                .batch_apply_latency_us
                .observe(started.elapsed());
            Response::Batch(results)
        }
        OwnedRequest::SyncTree {
            zone_id,
            level,
            scope_g,
            scope,
            requester,
        } => {
            let store = zone!(zone_id);
            if store.borrow().seed_not_ready_for_caller() {
                crate::observe::counters().ae_seed_not_ready_busy.inc();
                return Response::Busy;
            }
            if store.borrow().regroup_pending() {
                crate::observe::counters().ae_regroup_not_ready_busy.inc();
                return Response::Busy;
            }
            if store.borrow().rt_fold_pending() {
                crate::observe::counters().rt_fold_not_ready_busy.inc();
                return Response::Busy;
            }
            let scope: std::collections::HashSet<u32> = scope.into_iter().collect();
            let r = store
                .borrow()
                .scoped_leaf_digests(level, scope_g, &scope, requester);
            match r {
                Ok(nodes) => Response::Tree(nodes),
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::SyncBucketDigests {
            zone_id,
            num_buckets,
            ids,
            after,
        } => {
            let store = zone!(zone_id);
            if store.borrow().rt_fold_not_ready_for_caller() {
                crate::observe::counters().rt_fold_not_ready_busy.inc();
                return Response::Busy;
            }
            let r = store
                .borrow()
                .bucket_digest_page(num_buckets, &ids, after.as_deref(), SCAN_REPLY_BUDGET);
            match r {
                Ok((digests, more)) => Response::BucketDigests {
                    digests,
                    more,
                    resume: None,
                },
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::SyncValues { zone_id, keys } => {
            let store = zone!(zone_id);
            let r = store.borrow().values_page(&keys, SCAN_REPLY_BUDGET);
            match r {
                Ok((entries, more)) => Response::Values { entries, more },
                Err(e) => Response::Err(format!("{e}")),
            }
        }
        OwnedRequest::MixedBatchRead { .. } => {
            Response::Err("mixed batch read requires the async server".into())
        }
        control => serve_control_request(shared, control),
    }
}

