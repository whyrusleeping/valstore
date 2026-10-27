use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

use vals_spdk::{AsyncRead, BlockDevice};
use vals_store::range_tombstone::Fragment;

use crate::antientropy::AePeer;
use vals_client::ae::RtSyncPage;
use crate::coord::TransportError;
use crate::partition::RangeMap;
use crate::replica::{pack_version, unpack_version, Applied, VersionedStore};
use crate::ring::{hash64, NodeId, Ring};
use crate::wire::RtFragment;
use crate::zone::ZoneId;

pub(crate) const RT_SYNC_PAGE_FRAGMENTS: usize = 1024;

pub(crate) const RT_SYNC_PAGE_BYTES: usize = 1 << 20;

pub(crate) struct RtScope<'a> {
    pub map: &'a RangeMap,
    pub ring: &'a Ring,
    pub zone_id: ZoneId,
    pub rf: usize,
    pub scope: Option<&'a BTreeSet<NodeId>>,
    pub local: NodeId,
    pub counterpart: Option<NodeId>,
    pub shard: (usize, usize),
    kept: RefCell<HashMap<usize, bool>>,
}

impl<'a> RtScope<'a> {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        map: &'a RangeMap,
        ring: &'a Ring,
        zone_id: ZoneId,
        rf: usize,
        scope: Option<&'a BTreeSet<NodeId>>,
        local: NodeId,
        counterpart: Option<NodeId>,
        shard: (usize, usize),
    ) -> Self {
        RtScope {
            map,
            ring,
            zone_id,
            rf,
            scope,
            local,
            counterpart,
            shard,
            kept: RefCell::new(HashMap::new()),
        }
    }

    fn keeps_range(&self, index: usize) -> bool {
        if let Some(&kept) = self.kept.borrow().get(&index) {
            return kept;
        }
        let mut owners = self.map.replicas_of_range(self.ring, self.zone_id, index, self.rf, self.scope);
        let pending = self.map.pending_of(index);
        if let Some(token) = pending {
            owners.extend(self.ring.replicas_in_zone(self.zone_id, token, self.rf, self.scope));
        }
        let (shard_index, shard_count) = self.shard;
        let shard_of = |token: &[u8]| (hash64(token) % shard_count.max(1) as u64) as usize;
        let kept = owners.contains(&self.local)
            && self.counterpart.is_none_or(|peer| owners.contains(&peer))
            && (shard_count <= 1
                || shard_of(self.map.token_of(index)) == shard_index
                || pending.is_some_and(|token| shard_of(token) == shard_index));
        self.kept.borrow_mut().insert(index, kept);
        kept
    }

    pub(crate) fn clip(&self, fragments: impl IntoIterator<Item = Fragment>) -> Vec<Fragment> {
        let mut out: Vec<Fragment> = Vec::new();
        for f in fragments {
            let lo = (!f.lo.is_empty()).then_some(f.lo.as_slice());
            for slice in self.map.ranges_overlapping(lo, f.hi.as_deref()) {
                if !self.keeps_range(slice.index) {
                    continue;
                }
                let piece_lo = slice.lo.unwrap_or_default();
                match out.last_mut() {
                    Some(last) if last.version == f.version && last.hi.as_deref() == Some(piece_lo.as_slice()) => {
                        last.hi = slice.hi;
                    }
                    _ => out.push(Fragment { lo: piece_lo, hi: slice.hi, version: f.version }),
                }
            }
        }
        out
    }
}

pub(crate) fn digest(fragments: &[Fragment]) -> u64 {
    let mut h = 0u64;
    let mut buf = Vec::new();
    for f in fragments {
        buf.clear();
        buf.extend_from_slice(&h.to_le_bytes());
        buf.extend_from_slice(&(f.lo.len() as u32).to_le_bytes());
        buf.extend_from_slice(&f.lo);
        match &f.hi {
            Some(hi) => {
                buf.push(1);
                buf.extend_from_slice(&(hi.len() as u32).to_le_bytes());
                buf.extend_from_slice(hi);
            }
            None => buf.push(0),
        }
        buf.extend_from_slice(&f.version.to_le_bytes());
        h = hash64(&buf);
    }
    h
}

pub(crate) fn to_wire(f: Fragment) -> RtFragment {
    RtFragment {
        lo: (!f.lo.is_empty()).then_some(f.lo),
        hi: f.hi,
        version: unpack_version(f.version),
    }
}

pub(crate) fn from_wire(f: RtFragment) -> Fragment {
    Fragment {
        lo: f.lo.unwrap_or_default(),
        hi: f.hi,
        version: pack_version(f.version),
    }
}

pub(crate) fn page(fragments: Vec<Fragment>, after: Option<&[u8]>) -> (Vec<RtFragment>, bool) {
    let start = after.map_or(0, |after| fragments.partition_point(|f| f.lo.as_slice() <= after));
    let total = fragments.len();
    let mut bytes = 0usize;
    let mut out = Vec::new();
    for f in fragments.into_iter().skip(start) {
        let size = 2 * (1 + 4) + 16 + f.lo.len() + f.hi.as_ref().map_or(0, Vec::len);
        if !out.is_empty() && (out.len() >= RT_SYNC_PAGE_FRAGMENTS || bytes + size > RT_SYNC_PAGE_BYTES) {
            break;
        }
        bytes += size;
        out.push(to_wire(f));
    }
    let more = start + out.len() < total;
    (out, more)
}

pub(crate) fn respond(
    fragments: Vec<Fragment>,
    scope: Option<&RtScope<'_>>,
    digest: u64,
    after: Option<&[u8]>,
) -> RtSyncPage {
    let fragments = match scope {
        Some(scope) => scope.clip(fragments),
        None => fragments,
    };
    if self::digest(&fragments) == digest {
        return RtSyncPage { same: true, more: false, fragments: Vec::new() };
    }
    let (fragments, more) = page(fragments, after);
    RtSyncPage { same: false, more, fragments }
}

pub(crate) fn local_fragments<D: BlockDevice>(store: &VersionedStore<D>) -> Vec<Fragment> {
    store.engine().range_tombstones().fragments().collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RtExchange {
    Unsupported,
    Same,
    Pulled { fragments: usize, rejected: usize },
}

#[cfg(test)]
static PAUSED_ZONES: std::sync::Mutex<Vec<ZoneId>> = std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn pause_exchange(zone_id: ZoneId, paused: bool) {
    let mut zones = PAUSED_ZONES.lock().unwrap();
    zones.retain(|z| *z != zone_id);
    if paused {
        zones.push(zone_id);
    }
}

pub(crate) async fn exchange<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    scope: &RtScope<'_>,
) -> Result<RtExchange, TransportError> {
    #[cfg(test)]
    if PAUSED_ZONES.lock().unwrap().contains(&scope.zone_id) {
        return Ok(RtExchange::Unsupported);
    }
    let mine = scope.clip(local_fragments(&local.borrow()));
    let mine = digest(&mine);
    let mut after: Option<Vec<u8>> = None;
    let mut fragments = 0usize;
    let mut rejected = 0usize;
    loop {
        let Some(page) = peer
            .range_tombstones(scope.zone_id, Some(scope.local), mine, after.as_deref())
            .await?
        else {
            return Ok(RtExchange::Unsupported);
        };
        if page.same {
            return Ok(if after.is_none() {
                RtExchange::Same
            } else {
                RtExchange::Pulled { fragments, rejected }
            });
        }
        let next = page.fragments.last().map(|f| f.lo.clone().unwrap_or_default());
        if page.more
            && next
                .as_deref()
                .is_none_or(|next| after.as_deref().is_some_and(|after| next <= after))
        {
            return Err(TransportError::Remote(
                "peer returned a range-tombstone page without progress".into(),
            ));
        }
        let wall_ms = crate::hlc::physical_now_ms();
        for piece in scope.clip(page.fragments.into_iter().map(from_wire)) {
            let lo = (!piece.lo.is_empty()).then_some(piece.lo.as_slice());
            match local
                .borrow_mut()
                .apply_range_delete(lo, piece.hi.as_deref(), unpack_version(piece.version), wall_ms)
                .map_err(|e| TransportError::Engine(e.to_string()))?
            {
                Applied::Rejected => rejected += 1,
                _ => fragments += 1,
            }
        }
        if !page.more {
            return Ok(RtExchange::Pulled { fragments, rejected });
        }
        after = next;
        vals_rt::yield_now().await;
    }
}
