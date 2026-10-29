use super::*;
use crate::hlc::Version;

const REFUSED_COPIES_PER_ZONE: usize = 65_536;

pub(super) type RefusedCopies = std::collections::BTreeMap<Vec<u8>, Version>;

#[cfg(test)]
pub(crate) static FAIL_FENCE_WRITES: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn fence_write_fails(_key: &[u8]) -> bool {
    #[cfg(test)]
    if FAIL_FENCE_WRITES.load(std::sync::atomic::Ordering::SeqCst) && _key.starts_with(b"fence-fails") {
        return true;
    }
    false
}

#[derive(Clone)]
pub(crate) struct OwnerView {
    pub(crate) ring: Ring,
    pub(crate) cfg: Option<ZoneConfig>,
    pub(crate) map: Option<Arc<RangeMap>>,
}

impl OwnerView {
    pub(crate) fn owns(&self, zone_id: ZoneId, key: &[u8], node: NodeId, shard: (usize, usize)) -> bool {
        self.owns_with(zone_id, key, node, shard, true)
    }

    pub(crate) fn owns_committed(&self, zone_id: ZoneId, key: &[u8], node: NodeId, shard: (usize, usize)) -> bool {
        self.owns_with(zone_id, key, node, shard, false)
    }

    fn owns_with(&self, zone_id: ZoneId, key: &[u8], node: NodeId, shard: (usize, usize), pending: bool) -> bool {
        let (shard_index, shard_count) = shard;
        let shard_of = |place: &[u8]| (crate::ring::hash64(place) % shard_count as u64) as usize;
        let Some(cfg) = &self.cfg else {
            return self.ring.replicas(key).contains(&node) && (shard_count <= 1 || shard_of(key) == shard_index);
        };
        let Some(map) = &self.map else {
            return self.ring.replicas_in_zone(zone_id, key, cfg.rf, cfg.scope.as_ref()).contains(&node)
                && (shard_count <= 1 || shard_of(key) == shard_index);
        };
        let committed = map.replicas_for_key(&self.ring, zone_id, key, cfg.rf, cfg.scope.as_ref()).contains(&node)
            && (shard_count <= 1 || shard_of(map.range_token(key)) == shard_index);
        committed
            || (pending
                && map.pending_for_key(key).is_some_and(|token| {
                    self.ring.replicas_in_zone(zone_id, token, cfg.rf, cfg.scope.as_ref()).contains(&node)
                        && (shard_count <= 1 || shard_of(token) == shard_index)
                }))
    }
}

impl<D: BlockDevice> ServerShared<D> {
    pub(crate) fn owner_view(&self, zone_id: ZoneId) -> Option<OwnerView> {
        let ring = self.ring.borrow().clone();
        if zone_id == DEFAULT_ZONE {
            return Some(OwnerView { ring, cfg: None, map: None });
        }
        let cfg = self.zone_config(zone_id)?;
        let map = match cfg.ordering {
            crate::zone::Ordering::Sorted => Some(Arc::new(self.sorted_range_map(zone_id)?)),
            crate::zone::Ordering::Unsorted => None,
        };
        Some(OwnerView { ring, cfg: Some(cfg), map })
    }

    pub(crate) fn note_refused_copies(&self, zone_id: ZoneId, copies: Vec<crate::antientropy::RefusedCopyNote>) {
        if copies.is_empty() {
            return;
        }
        let mut all = self.refused_copies.borrow_mut();
        let zone = all.entry(zone_id).or_default();
        let mut added = false;
        for (key, version) in copies {
            if zone.len() >= REFUSED_COPIES_PER_ZONE && !zone.contains_key(&key) {
                crate::observe::counters().cleanup_refused_untracked.inc();
                continue;
            }
            if zone.insert(key, version) != Some(version) {
                crate::observe::counters().cleanup_refused_copies.inc();
                added = true;
            }
        }
        drop(all);
        if added {
            self.fence_regained_copies();
        }
    }

    pub(crate) fn is_refused_copy(&self, zone_id: ZoneId, key: &[u8], version: Version) -> bool {
        self.refused_copies
            .borrow()
            .get(&zone_id)
            .and_then(|zone| zone.get(key))
            .is_some_and(|&refused| refused == version)
    }

    pub(crate) fn prune_refused_copies(&self, zone_id: ZoneId) {
        let Ok(store) = self.zone_store(zone_id) else {
            return;
        };
        let entries: Vec<(Vec<u8>, Version)> = match self.refused_copies.borrow().get(&zone_id) {
            Some(zone) => zone.iter().map(|(key, &version)| (key.clone(), version)).collect(),
            None => return,
        };
        let gone: Vec<Vec<u8>> = {
            let s = store.borrow();
            entries
                .into_iter()
                .filter(|(key, version)| match s.stored(key) {
                    Ok(Some(cur)) => cur.version != *version,
                    Ok(None) => true,
                    Err(_) => false,
                })
                .map(|(key, _)| key)
                .collect()
        };
        let mut all = self.refused_copies.borrow_mut();
        if let Some(zone) = all.get_mut(&zone_id) {
            for key in gone {
                zone.remove(&key);
            }
        }
        all.retain(|_, zone| !zone.is_empty());
    }

    pub(crate) fn regain_fence_failed(&self) -> bool {
        self.regain_fence_failed.get()
    }

    pub(crate) fn fence_regained_copies(&self) {
        let zones: Vec<ZoneId> = self.refused_copies.borrow().keys().copied().collect();
        let mut failed = false;
        for zone_id in zones {
            let Some(view) = self.owner_view(zone_id) else {
                continue;
            };
            let regained: Vec<(Vec<u8>, Version)> = self
                .refused_copies
                .borrow()
                .get(&zone_id)
                .map(|zone| {
                    zone.iter()
                        .filter(|(key, _)| view.owns(zone_id, key, self.node_id, self.shard()))
                        .map(|(key, &version)| (key.clone(), version))
                        .collect()
                })
                .unwrap_or_default();
            if regained.is_empty() {
                continue;
            }
            let Ok(store) = self.zone_store(zone_id) else {
                failed = true;
                continue;
            };
            let wall_ms = crate::hlc::physical_now_ms();
            let mut fenced = 0u64;
            let mut sample: Option<Vec<u8>> = None;
            for (key, version) in regained {
                let outcome = {
                    let mut s = store.borrow_mut();
                    match s.stored(&key) {
                        Ok(Some(cur)) if cur.version == version && cur.value.is_some() => {
                            let delete = Version { hlc: version.hlc.saturating_add(1), node: self.node_id.into() };
                            if fence_write_fails(&key) {
                                Err(vals_store::EngineError::Cancelled)
                            } else {
                                s.apply_unconditional_at(&key, None, delete, wall_ms, unconditional_apply_mode(), LiveFloor::Enforce)
                                    .map(|_| true)
                            }
                        }
                        Ok(_) => Ok(false),
                        Err(e) => Err(e),
                    }
                };
                match outcome {
                    Ok(deleted) => {
                        if let Some(zone) = self.refused_copies.borrow_mut().get_mut(&zone_id) {
                            zone.remove(&key);
                        }
                        if deleted {
                            fenced += 1;
                            sample.get_or_insert(key);
                        }
                    }
                    Err(e) => {
                        failed = true;
                        log::warn!(
                            "node {}: zone {zone_id}: could not delete a refused former-owner copy this node owns \
                             again; refusing to serve reads and anti-entropy until it can (retried every sweep \
                             pass): {e}",
                            self.node_id
                        );
                    }
                }
            }
            self.refused_copies.borrow_mut().retain(|_, zone| !zone.is_empty());
            if fenced > 0 {
                self.invalidate_zone_write_marks(zone_id);
                crate::observe::counters().former_owner_copies_fenced.add(fenced);
                log::warn!(
                    "node {}: zone {zone_id}: a placement change made this node an owner of {fenced} keys whose \
                     copy here every owner had refused as older than its tombstone-GC floor; deleted \
                     them locally so a collected delete cannot come back (e.g. {:?})",
                    self.node_id,
                    sample.as_deref().map(String::from_utf8_lossy).unwrap_or_default()
                );
            }
        }
        self.regain_fence_failed.set(failed);
    }
}
