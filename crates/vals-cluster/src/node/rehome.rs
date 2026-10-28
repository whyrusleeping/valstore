use super::*;

pub(super) const NODE_PEER_RPC_DEADLINE: Duration = Duration::from_secs(5);

struct RehomeCopyCursor {
    pending: Vec<u8>,
    epoch: u64,
    source: NodeId,
    mark: u64,
    forget_gen: u64,
    at: Option<Vec<u8>>,
    done: bool,
    last_pending_ms: u64,
    held: Option<(Vec<u8>, u64)>,
}

const REHOME_HOLD_RETRY_MS: u64 = 5_000;

pub(super) async fn rehome_copy_apply<D: BlockDevice + AsyncRead>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    gate: &crate::net::ReadGate,
    key: &[u8],
    value: Option<&[u8]>,
    version: crate::hlc::Version,
) -> Result<Option<crate::replica::Applied>, EngineError> {
    let wall_ms = physical_now_ms();
    let dev = store.borrow().device_handle();
    crate::replica::apply_staged_with_retry(
        store,
        dev.as_ref(),
        gate,
        key,
        value,
        version,
        wall_ms,
        crate::replica::ApplyRetryMode::Standard,
        || {
            #[cfg(test)]
            rehome_copy_commit_hook();
        },
    )
    .await
    .map_err(crate::replica::ApplyRetryError::into_engine)
}

#[cfg(test)]
thread_local! {
    static REHOME_COPY_COMMIT_HOOK: std::cell::RefCell<Option<Box<dyn FnMut()>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_rehome_copy_commit_hook(hook: Option<Box<dyn FnMut()>>) {
    REHOME_COPY_COMMIT_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
fn rehome_copy_commit_hook() {
    REHOME_COPY_COMMIT_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f();
        }
    });
}

pub(super) fn rehome_copy_next(page: &crate::coord::ScanPage) -> Option<Option<Vec<u8>>> {
    if !page.more {
        return Some(None);
    }
    if let Some(resume) = &page.resume {
        return Some(Some(resume.clone()));
    }
    let mut next = page.entries.last()?.0.clone();
    next.push(0);
    Some(Some(next))
}

async fn rehome_target_only_leftover<D: BlockDevice + AsyncRead>(
    shared: &ServerShared<D>,
    store: &std::cell::RefCell<VersionedStore<D>>,
    zone_id: ZoneId,
    (lo, hi): (Option<&[u8]>, Option<&[u8]>),
    page: &crate::coord::ScanPage,
    older_than_ms: u64,
) -> Option<Option<Vec<u8>>> {
    let dev = store.borrow().device_handle();
    let mut after = lo.map(<[u8]>::to_vec);
    loop {
        let reply = crate::net::serve_scan_keys(
            store,
            dev.as_ref(),
            shared.read_gate(),
            after.as_deref(),
            hi,
            shared.read_site(zone_id),
        )
        .await;
        let crate::wire::Response::Scan { entries, more, resume, .. } = reply else {
            return None;
        };
        for (key, stored) in &entries {
            if stored.value.is_some()
                && stored.version.physical_ms() < older_than_ms
                && page.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)).is_err()
            {
                return Some(Some(key.clone()));
            }
        }
        if !more {
            return Some(None);
        }
        let next = resume.or_else(|| {
            entries.last().map(|(k, _)| {
                let mut k = k.clone();
                k.push(0);
                k
            })
        })?;
        if after.as_deref().is_some_and(|a| next.as_slice() <= a) {
            return None;
        }
        after = Some(next);
    }
}

pub(super) fn rehome_copy_apply_fragments<D: BlockDevice>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    fragments: &[crate::wire::RtFragment],
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
) -> bool {
    let wall_ms = physical_now_ms();
    for f in fragments {
        let piece_lo = f.lo.as_deref().max(lo);
        let piece_hi = match (f.hi.as_deref(), hi) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        if piece_hi.is_some_and(|h| h <= piece_lo.unwrap_or_default()) {
            continue;
        }
        if !matches!(
            store.borrow_mut().apply_range_delete(piece_lo, piece_hi, f.version, wall_ms),
            Ok(crate::replica::Applied::Won)
        ) {
            return false;
        }
    }
    true
}

pub(super) async fn rehome_copy_loop<D: BlockDevice + AsyncRead + 'static>(
    shared: Rc<ServerShared<D>>,
    peer_shards: Vec<(NodeId, Vec<SocketAddr>)>,
) {
    let transport = NetTransport::connect_sharded(peer_shards)
        .with_rpc_deadline(NODE_PEER_RPC_DEADLINE);
    let page_entries = shared.settings.rehome_copy_page_entries;
    let pages_per_tick = shared.settings.rehome_copy_pages_per_tick;
    let hold_ms = shared.settings.rehome_copy_hold_ms;
    let mut cursors: std::collections::HashMap<(ZoneId, Vec<u8>), RehomeCopyCursor> =
        std::collections::HashMap::new();
    let stop = shared.add_shutdown_notify();
    let timer = match Timer::every(Duration::from_millis(100)) {
        Ok(timer) => timer,
        Err(_) => return,
    };
    let mut maps_epoch = None;
    let mut maps = std::collections::HashMap::new();
    let mut has_pending = false;
    let mut ring_epoch_seen = None;
    let mut ring = shared.ring().borrow().clone();
    loop {
        if let Either::Right(()) = vals_rt::race(timer.tick(), stop.notified()).await {
            return;
        }
        if shared.is_shutdown() {
            return;
        }
        let epoch = shared.range_map_epoch();
        let ring_epoch = shared.ring_epoch();
        let now_ms = physical_now_ms();
        transport.set_route_epoch(ring_epoch);
        if maps_epoch != Some(epoch) {
            maps = shared.range_maps_clone();
            transport.set_sorted_ranges(&maps);
            has_pending = maps.values().any(|map| {
                (0..map.num_ranges()).any(|index| map.pending_of(index).is_some())
            });
            maps_epoch = Some(epoch);
        }
        if ring_epoch_seen != Some(ring_epoch) {
            ring = shared.ring().borrow().clone();
            ring_epoch_seen = Some(ring_epoch);
        }
        if !has_pending && cursors.is_empty() {
            shared.prune_rehome_copy_holds(now_ms);
            continue;
        }
        for (&zone_id, map) in &maps {
            let Some(cfg) = shared.zone_config(zone_id) else { continue };
            for index in 0..map.num_ranges() {
                let Some(pending) = map.pending_of(index) else { continue };
                let targets = ring.replicas_in_zone(
                    zone_id, pending, cfg.rf, cfg.scope.as_ref()
                );
                let (shard_index, shard_count) = shared.shard();
                if !targets.contains(&shared.node_id)
                    || crate::ring::hash64(pending) as usize % shard_count != shard_index
                {
                    continue;
                }
                let current = map.replicas_of_range(
                    &ring, zone_id, index, cfg.rf, cfg.scope.as_ref()
                );
                let current_shard = (
                    crate::ring::hash64(map.token_of(index)) % shard_count as u64
                ) as usize;
                if current.contains(&shared.node_id) && current_shard == shard_index {
                    let lower = map.range_bounds(index).0.unwrap_or_default().to_vec();
                    let complete = shared.rehome_mirrors.borrow()
                        .get(&(zone_id, lower.clone()))
                        .is_some_and(|stat| stat.copy_complete_epoch >= epoch);
                    if !complete {
                        let Ok(store) = shared.zone_store(zone_id) else { continue };
                        if store.borrow_mut().sync().is_err() {
                            continue;
                        }
                        shared.rehome_mirrors.borrow_mut()
                            .entry((zone_id, lower)).or_default()
                            .copy_complete_epoch = epoch;
                    }
                    continue;
                }
                let (lo, hi) = map.range_bounds(index);
                let lower = lo.unwrap_or_default().to_vec();
                let ck = (zone_id, lower.clone());
                let known = cursors.get(&ck).map(|e| {
                    (e.pending.clone(), e.epoch, e.source, e.mark, e.forget_gen)
                });
                let valid = match known {
                    Some((token, entry_epoch, source, mark, forget_gen))
                        if token == pending =>
                    {
                        if entry_epoch == epoch {
                            true
                        } else if !current.contains(&source) || !transport.has_peer(source) {
                            cursors.remove(&ck);
                            crate::observe::counters().rehome_copy_restarts.inc();
                            false
                        } else {
                            match transport
                                .range_write_mark(source, zone_id, epoch, &lower)
                                .await
                            {
                                Ok(m) if m == mark
                                    && shared.zone_forget_gen(zone_id) == forget_gen =>
                                {
                                    if let Some(e) = cursors.get_mut(&ck) {
                                        e.epoch = epoch;
                                    }
                                    crate::observe::counters().rehome_copy_resumes.inc();
                                    true
                                }
                                Ok(m) => {
                                    cursors.insert(ck.clone(), RehomeCopyCursor {
                                        pending: pending.to_vec(),
                                        epoch,
                                        source,
                                        mark: m,
                                        forget_gen: shared.zone_forget_gen(zone_id),
                                        at: None,
                                        done: false,
                                        last_pending_ms: now_ms,
                                        held: None,
                                    });
                                    crate::observe::counters().rehome_copy_restarts.inc();
                                    true
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                    Some(_) => {
                        cursors.remove(&ck);
                        false
                    }
                    None => false,
                };
                if !valid {
                    let Some(source) = current.iter().copied()
                        .find(|n| transport.has_peer(*n))
                    else {
                        continue;
                    };
                    let forget_gen = shared.zone_forget_gen(zone_id);
                    let Ok(mark) = transport
                        .range_write_mark(source, zone_id, epoch, &lower)
                        .await
                    else {
                        continue;
                    };
                    cursors.insert(ck.clone(), RehomeCopyCursor {
                        pending: pending.to_vec(),
                        epoch,
                        source,
                        mark,
                        forget_gen,
                        at: None,
                        done: false,
                        last_pending_ms: now_ms,
                        held: None,
                    });
                }
                let (source, done, mut at) = {
                    let e = &cursors[&ck];
                    if e.held.as_ref().is_some_and(|(_, retry_ms)| now_ms < *retry_ms) {
                        continue;
                    }
                    (e.source, e.done, e.at.clone())
                };
                if done {
                    let Ok(store) = shared.zone_store(zone_id) else { continue };
                    if store.borrow_mut().sync().is_err() {
                        continue;
                    }
                    shared.rehome_mirrors.borrow_mut()
                        .entry((zone_id, lower)).or_default()
                        .copy_complete_epoch = epoch;
                    continue;
                }
                for _ in 0..pages_per_tick {
                    let cursor = at.as_deref().or(lo).map(<[u8]>::to_vec);
                    let cursor = cursor.as_deref();
                    let page_ms = physical_now_ms();
                    let Ok(page) = transport
                        .scan(source, zone_id, cursor, hi, page_entries)
                        .await
                    else {
                        break;
                    };
                    let Some(next) = rehome_copy_next(&page) else {
                        break;
                    };
                    if page.more && crate::ram::ram_over_budget() {
                        break;
                    }
                    let Ok(store) = shared.zone_store(zone_id) else { break };
                    let applied = rehome_copy_apply_fragments(store.as_ref(), &page.fragments, lo, hi);
                    if store.borrow().rt_fold_pending() {
                        shared.kick_logical_accounting();
                    }
                    if !applied {
                        break;
                    }
                    let mut copied = true;
                    for (key, stored) in &page.entries {
                        if !matches!(
                            rehome_copy_apply(
                                store.as_ref(),
                                shared.read_gate(),
                                key,
                                stored.value.as_deref(),
                                stored.version,
                            )
                            .await,
                            Ok(Some(_))
                        ) {
                            copied = false;
                            break;
                        }
                    }
                    if !copied {
                        break;
                    }
                    let grace = store.borrow().gc_grace_ms();
                    if grace != 0 {
                        let window_hi = next.as_deref().or(hi);
                        match rehome_target_only_leftover(
                            &shared,
                            store.as_ref(),
                            zone_id,
                            (cursor, window_hi),
                            &page,
                            page_ms.saturating_sub(grace / 2),
                        )
                        .await
                        {
                            Some(None) => {}
                            Some(Some(key)) => {
                                crate::observe::counters().rehome_copy_target_only_blocks.inc();
                                let Some(e) = cursors.get_mut(&ck) else { break };
                                let repeat = e.held.as_ref().is_some_and(|(held, _)| *held == key);
                                e.held = Some((key.clone(), now_ms.saturating_add(REHOME_HOLD_RETRY_MS)));
                                if repeat {
                                    break;
                                }
                                log::warn!(
                                    "node {}: re-home copy of zone {zone_id} range {:?} is held: this target \
                                     holds key {:?}, older than half the tombstone-GC grace, that source node \
                                     {source} does not; it may be a leftover whose delete the source collected",
                                    shared.node_id,
                                    lower,
                                    key
                                );
                                break;
                            }
                            None => break,
                        }
                    }
                    if let Some(e) = cursors.get_mut(&ck) {
                        e.held = None;
                    }
                    crate::observe::counters().rehome_copy_pages.inc();
                    *shared.rehome_copy_progress.borrow_mut()
                        .entry((zone_id, lower.clone())).or_insert(0)
                        += page.entries.len() as u64;
                    if let Some(next) = next {
                        at = Some(next);
                        if let Some(e) = cursors.get_mut(&ck) {
                            e.at = at.clone();
                        }
                    } else {
                        if store.borrow_mut().sync().is_err() {
                            break;
                        }
                        if let Some(e) = cursors.get_mut(&ck) {
                            e.done = true;
                            e.at = None;
                        }
                        shared.rehome_mirrors.borrow_mut()
                            .entry((zone_id, lower.clone())).or_default()
                            .copy_complete_epoch = epoch;
                        break;
                    }
                }
            }
        }
        cursors.retain(|(zone_id, lower), e| {
            let keep = maps.get(zone_id).is_some_and(|map| {
                let index = map.range_index(lower);
                if map.range_bounds(index).0.unwrap_or_default() != lower.as_slice() {
                    return false;
                }
                if map.pending_of(index).is_some() {
                    e.last_pending_ms = now_ms;
                    true
                } else {
                    !e.done && now_ms < e.last_pending_ms.saturating_add(hold_ms)
                }
            });
            if keep {
                shared.hold_rehome_copy(
                    *zone_id,
                    lower.clone(),
                    e.last_pending_ms.saturating_add(hold_ms),
                );
            }
            keep
        });
        shared.prune_rehome_copy_holds(now_ms);
    }
}

