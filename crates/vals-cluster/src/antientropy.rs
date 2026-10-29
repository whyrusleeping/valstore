use std::cell::RefCell;
use std::collections::{BTreeSet, HashSet};

use vals_spdk::{AsyncRead, BlockDevice};

use crate::coord::{Transport, TransportError};
use crate::hlc::Version;
use crate::net::ReadGate;
use crate::partition::RangeMap;
use crate::replica::{Applied, Stored, VersionedStore};
use vals_store::range_tombstone::Fragment;
use crate::ring::{hash64, NodeId, Ring};
use crate::zone::{ZoneId, DEFAULT_ZONE};
use vals_client::types::{MixedReadEntry, MixedReadReply, MixedReadRole};

pub const DEFAULT_BUCKETS: u32 = 4096;

const TREE_FANOUT: u32 = 16;

fn tree_levels(leaf_count: u32) -> Vec<u32> {
    let mut levels = Vec::new();
    let mut level = TREE_FANOUT;
    while level < leaf_count {
        if leaf_count.is_multiple_of(level) {
            levels.push(level);
        }
        level = match level.checked_mul(TREE_FANOUT) {
            Some(l) => l,
            None => break,
        };
    }
    levels.push(leaf_count);
    levels
}

pub const CLEANUP_MAX_KEYS_PER_PASS: usize = 8_192;

pub const CLEANUP_MAX_HANDOFF_RPCS_PER_PASS: usize = 256;

pub const CLEANUP_HANDOFF_WALL_BUDGET: std::time::Duration =
    std::time::Duration::from_millis(250);

const CLEANUP_YIELD_EVERY: usize = 1024;

const CLEANUP_PROBE_MIN_KEYS: usize = 1;

const CLEANUP_PROBE_MAX_KEYS: usize = 1024;

pub const CLEANUP_MAX_PAGE_RESTARTS: usize = 3;

#[cfg(test)]
thread_local! {
    static CLEANUP_PAGE_REBORROW_HOOK: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None);
    static CLEANUP_TEST_ELAPSED: std::cell::Cell<Option<std::time::Duration>> = const { std::cell::Cell::new(None) };
}

fn cleanup_elapsed(started: std::time::Instant) -> std::time::Duration {
    #[cfg(test)]
    if let Some(elapsed) = CLEANUP_TEST_ELAPSED.with(|clock| clock.get()) {
        return elapsed;
    }
    started.elapsed()
}
#[cfg(test)]
fn cleanup_page_reborrow_hook() {
    CLEANUP_PAGE_REBORROW_HOOK.with(|h| {
        if let Some(hook) = h.borrow_mut().as_mut() {
            hook();
        }
    });
}

pub struct CleanupProgress<D: BlockDevice> {
    pub released: usize,
    pub scanned: usize,
    pub handoff_rpcs: usize,
    pub probe_released: usize,
    pub next_cursor: Option<Vec<u8>>,
    pub wrapped: bool,
    pub deferred: bool,
    pub kept: usize,
    pub kept_samples: Vec<Vec<u8>>,
    pub refused: Vec<RefusedCopyNote>,
    pub forget_plan: Option<vals_store::CompactPlan<D>>,
}

pub const CLEANUP_KEPT_SAMPLES: usize = 4;

pub type RefusedCopyNote = (Vec<u8>, Version);

#[derive(Clone, Copy)]
pub struct HandoffPolicy<'a> {
    pub trusted: &'a dyn Fn(&[u8]) -> bool,
    pub refused: &'a dyn Fn(&[u8], Version) -> bool,
}

fn never_trusted(_: &[u8]) -> bool {
    false
}

fn never_refused(_: &[u8], _: Version) -> bool {
    false
}

impl HandoffPolicy<'static> {
    pub const PLAIN: Self = HandoffPolicy { trusted: &never_trusted, refused: &never_refused };
}

impl<D: BlockDevice> CleanupProgress<D> {
    fn note_kept(&mut self, key: &[u8]) {
        self.kept += 1;
        if self.kept_samples.len() < CLEANUP_KEPT_SAMPLES {
            self.kept_samples.push(key.to_vec());
        }
    }
}

impl<D: BlockDevice> Default for CleanupProgress<D> {
    fn default() -> Self {
        Self {
            released: 0,
            scanned: 0,
            handoff_rpcs: 0,
            probe_released: 0,
            next_cursor: None,
            wrapped: false,
            deferred: false,
            kept: 0,
            kept_samples: Vec::new(),
            refused: Vec::new(),
            forget_plan: None,
        }
    }
}

const AE_RECONCILE_MAX_DIGESTS_PER_PASS: usize = 50_000;

const AE_RECONCILE_MAX_VALUE_READS_PER_PASS: usize = 8_192;

const AE_RECONCILE_MAX_DIGEST_PAGES_PER_PASS: usize = 16;

const AE_VALUE_FETCH_CHUNK: usize = 256;

const AE_RECONCILE_YIELD_EVERY: usize = 128;

pub fn bucket_of(key: &[u8], num_buckets: u32) -> u32 {
    (hash64(key) % num_buckets as u64) as u32
}

pub(crate) fn entry_digest(key: &[u8], version: Version) -> u64 {
    let mut buf = Vec::with_capacity(key.len() + 16);
    buf.extend_from_slice(key);
    buf.extend_from_slice(&version.hlc.to_le_bytes());
    buf.extend_from_slice(&version.node.to_le_bytes());
    hash64(&buf)
}

pub fn leaf_digests(entries: &[(Vec<u8>, Stored)], num_buckets: u32) -> Vec<u64> {
    let mut leaves = vec![0u64; num_buckets as usize];
    for (key, stored) in entries {
        leaves[bucket_of(key, num_buckets) as usize] ^= entry_digest(key, stored.version);
    }
    leaves
}

pub fn leaf_digests_versioned<'a>(
    entries: impl Iterator<Item = (&'a [u8], Version)>,
    num_buckets: u32,
) -> Vec<u64> {
    let mut leaves = vec![0u64; num_buckets as usize];
    for (key, version) in entries {
        leaves[bucket_of(key, num_buckets) as usize] ^= entry_digest(key, version);
    }
    leaves
}

fn mix2(a: u64, b: u64) -> u64 {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&a.to_le_bytes());
    buf[8..16].copy_from_slice(&b.to_le_bytes());
    hash64(&buf)
}

pub fn root(leaves: &[u64]) -> u64 {
    if leaves.is_empty() {
        return 0;
    }
    let mut level = leaves.to_vec();
    while level.len() > 1 {
        level = level
            .chunks(2)
            .map(|pair| match pair {
                [a, b] => mix2(*a, *b),
                [a] => *a,
                _ => unreachable!(),
            })
            .collect();
    }
    level[0]
}

pub use vals_client::ae::AePeer;

#[cfg(test)]
pub async fn reconcile<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    num_buckets: u32,
) -> Result<usize, TransportError> {
    reconcile_with(
        local,
        peer,
        DEFAULT_ZONE,
        num_buckets,
        usize::MAX,
        usize::MAX,
        usize::MAX,
        None,
        None,
        None,
        None,
        |_| Some(crate::replica::LiveFloor::Waive),
    )
    .await
    .map(|(applied, _)| applied)
}

#[allow(clippy::too_many_arguments)]
pub async fn reconcile_scoped<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    num_buckets: u32,
    ring: &Ring,
    local_node: NodeId,
    counterpart: NodeId,
    shard: (usize, usize),
    resume: Option<&[u8]>,
) -> Result<(usize, Option<Vec<u8>>), TransportError> {
    reconcile_scoped_gate(
        local,
        peer,
        num_buckets,
        ring,
        local_node,
        counterpart,
        shard,
        (0, 1),
        resume,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_scoped_gate<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    num_buckets: u32,
    ring: &Ring,
    local_node: NodeId,
    counterpart: NodeId,
    shard: (usize, usize),
    peer_shard: (usize, usize),
    resume: Option<&[u8]>,
    gate: Option<&ReadGate>,
) -> Result<(usize, Option<Vec<u8>>), TransportError> {
    let (shard_index, shard_count) = shard;
    reconcile_with(
        local,
        peer,
        DEFAULT_ZONE,
        num_buckets,
        AE_RECONCILE_MAX_DIGESTS_PER_PASS,
        AE_RECONCILE_MAX_DIGEST_PAGES_PER_PASS,
        AE_RECONCILE_MAX_VALUE_READS_PER_PASS,
        resume,
        Some(local_node),
        Some(counterpart),
        gate,
        |key| {
            let owners = ring.replicas(key);
            if !owners.contains(&local_node) || !owners.contains(&counterpart) {
                return None;
            }
            if shard_count > 1 {
                let s = (crate::ring::hash64(key) % shard_count as u64) as usize;
                if s != shard_index {
                    return None;
                }
            }
            Some(owner_floor(true, key, peer_shard))
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn reconcile_scoped_zone<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    num_buckets: u32,
    ring: &Ring,
    local_node: NodeId,
    counterpart: NodeId,
    zone_id: ZoneId,
    rf: usize,
    scope: Option<&BTreeSet<NodeId>>,
    range_map: Option<&RangeMap>,
    shard: (usize, usize),
    resume: Option<&[u8]>,
) -> Result<(usize, Option<Vec<u8>>), TransportError> {
    reconcile_scoped_zone_gate(
        local,
        peer,
        num_buckets,
        ring,
        local_node,
        counterpart,
        zone_id,
        rf,
        scope,
        range_map,
        shard,
        (0, 1),
        resume,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn reconcile_scoped_zone_gate<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    num_buckets: u32,
    ring: &Ring,
    local_node: NodeId,
    counterpart: NodeId,
    zone_id: ZoneId,
    rf: usize,
    scope: Option<&BTreeSet<NodeId>>,
    range_map: Option<&RangeMap>,
    shard: (usize, usize),
    peer_shard: (usize, usize),
    resume: Option<&[u8]>,
    gate: Option<&ReadGate>,
) -> Result<(usize, Option<Vec<u8>>), TransportError> {
    let (shard_index, shard_count) = shard;
    reconcile_with(
        local,
        peer,
        zone_id,
        num_buckets,
        AE_RECONCILE_MAX_DIGESTS_PER_PASS,
        AE_RECONCILE_MAX_DIGEST_PAGES_PER_PASS,
        AE_RECONCILE_MAX_VALUE_READS_PER_PASS,
        resume,
        Some(local_node),
        Some(counterpart),
        gate,
        |key| {
            let place = match range_map {
                Some(m) => m.range_token(key),
                None => key,
            };
            let committed = match range_map {
                Some(m) => m.replicas_of_range(ring, zone_id, m.range_index(key), rf, scope),
                None => ring.replicas_in_zone(zone_id, place, rf, scope),
            };
            let owners = match range_map {
                Some(m) => m.replicas_for_key_while_rehoming(
                    ring, zone_id, key, rf, scope
                ),
                None => committed.clone(),
            };
            if !owners.contains(&local_node) || !owners.contains(&counterpart) {
                return None;
            }
            if shard_count > 1 {
                let current = (crate::ring::hash64(place) % shard_count as u64) as usize;
                let pending = range_map
                    .and_then(|m| m.pending_for_key(key))
                    .map(|token| (crate::ring::hash64(token) % shard_count as u64) as usize);
                if current != shard_index && pending != Some(shard_index) {
                    return None;
                }
            }
            Some(owner_floor(committed.contains(&counterpart), place, peer_shard))
        },
    )
    .await
}

fn owner_floor(committed_owner: bool, place: &[u8], (peer_index, peer_count): (usize, usize)) -> crate::replica::LiveFloor {
    let peer_count = peer_count.max(1);
    if committed_owner && (crate::ring::hash64(place) % peer_count as u64) as usize == peer_index {
        crate::replica::LiveFloor::Waive
    } else {
        crate::replica::LiveFloor::Enforce
    }
}

#[cfg(test)]
thread_local! {
    static LOCAL_VERSION_REBORROW_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
fn set_local_version_reborrow_hook(hook: Option<Box<dyn FnMut()>>) {
    LOCAL_VERSION_REBORROW_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(test)]
fn local_version_reborrow_hook() {
    LOCAL_VERSION_REBORROW_HOOK.with(|slot| {
        if let Some(hook) = slot.borrow_mut().as_mut() {
            hook();
        }
    });
}

async fn local_version<D: BlockDevice + AsyncRead>(
    local: &RefCell<VersionedStore<D>>,
    key: &[u8],
    gate: Option<&ReadGate>,
) -> Result<Option<Version>, TransportError> {
    for _ in 0..8 {
        let _guard = match gate {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let dev = local.borrow().device_handle();
        let (generation, mutable) = {
            let store = local.borrow();
            (
                store.storage_generation(),
                store.mutable_entry(key).map(|entry| entry.version),
            )
        };
        let probe = local.borrow().probe_version(key);
        let resolved = VersionedStore::version_from_probe(probe, dev.as_ref()).await;
        #[cfg(test)]
        local_version_reborrow_hook();
        let store = local.borrow();
        if store.storage_generation() != generation
            || store.mutable_entry(key).map(|entry| entry.version) != mutable
        {
            continue;
        }
        drop(store);
        return resolved.map_err(|e| TransportError::Engine(e.to_string()));
    }
    Err(TransportError::Busy)
}

async fn local_apply<D: BlockDevice + AsyncRead>(
    local: &RefCell<VersionedStore<D>>,
    key: &[u8],
    value: Option<&[u8]>,
    version: Version,
    gate: Option<&ReadGate>,
    floor: crate::replica::LiveFloor,
) -> Result<Applied, TransportError> {
    local_apply_mode(
        local,
        key,
        value,
        version,
        gate,
        crate::replica::unconditional_apply_mode(),
        floor,
    )
    .await
}

async fn local_apply_mode<D: BlockDevice + AsyncRead>(
    local: &RefCell<VersionedStore<D>>,
    key: &[u8],
    value: Option<&[u8]>,
    version: Version,
    gate: Option<&ReadGate>,
    mode: crate::replica::ApplyMode,
    floor: crate::replica::LiveFloor,
) -> Result<Applied, TransportError> {
    let wall_ms = crate::hlc::physical_now_ms();
    if mode == crate::replica::ApplyMode::Blind {
        return local
            .borrow_mut()
            .apply_unconditional_at(key, value, version, wall_ms, mode, floor)
            .map_err(|e| TransportError::Engine(e.to_string()));
    }
    for _ in 0..8 {
        let _guard = match gate {
            Some(gate) => Some(gate.acquire().await),
            None => None,
        };
        let staged = local
            .borrow_mut()
            .apply_unconditional_stage(key, value, version, wall_ms, mode, floor)
            .map_err(|e| TransportError::Engine(e.to_string()))?;
        let (probe, gen) = match staged {
            crate::replica::StagedApply::Done(applied) => return Ok(applied),
            crate::replica::StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
        };
        let dev = local.borrow().device_handle();
        let run_old = VersionedStore::version_from_probe(probe, dev.as_ref())
            .await
            .map_err(|e| TransportError::Engine(e.to_string()))?;
        match local
            .borrow_mut()
            .apply_commit_staged(key, value, version, run_old, gen, wall_ms)
            .map_err(|e| TransportError::Engine(e.to_string()))?
        {
            crate::replica::StagedApplyOutcome::Applied(applied) => return Ok(applied),
            crate::replica::StagedApplyOutcome::Retry => continue,
        }
    }
    Err(TransportError::Busy)
}

fn digest_cursor_page((digests, more): crate::replica::DigestPage) -> crate::replica::DigestCursorPage {
    let next = if more {
        digests.last().map(|(key, _)| key.clone())
    } else {
        None
    };
    (digests, next)
}

fn apply_is_acknowledged(applied: Applied) -> bool {
    matches!(applied, Applied::Won | Applied::Lost(_))
}

async fn pace_local_op(ops: &mut usize) {
    *ops += 1;
    if ops.is_multiple_of(AE_RECONCILE_YIELD_EVERY) {
        vals_rt::yield_now().await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_with<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    peer: &P,
    zone_id: ZoneId,
    num_buckets: u32,
    max_digests: usize,
    max_digest_pages: usize,
    max_value_reads: usize,
    resume: Option<&[u8]>,
    local_node: Option<NodeId>,
    counterpart: Option<NodeId>,
    gate: Option<&ReadGate>,
    keep: impl Fn(&[u8]) -> Option<crate::replica::LiveFloor>,
) -> Result<(usize, Option<Vec<u8>>), TransportError> {
    let counters = crate::observe::counters();

    let mut scope_g = 1u32;
    let mut scope: HashSet<u32> = HashSet::from([0]);
    if !crate::replica::ensure_seeded_cooperative_cell(local)
        .await
        .map_err(|e| TransportError::Engine(e.to_string()))?
    {
        return Err(TransportError::Busy);
    }
    for level in tree_levels(num_buckets) {
        let scope_ids: Vec<u32> = scope.iter().copied().collect();
        let mut attempts = 0;
        let (mine, theirs) = loop {
            if !crate::replica::ensure_tree_ready_cooperative_cell(local, scope_g, &scope)
                .await
                .map_err(|e| TransportError::Engine(e.to_string()))?
            {
                return Err(TransportError::Busy);
            }
            let token = local.borrow().tree_scope_token(scope_g, &scope);
            let mine = local
                .borrow()
                .scoped_leaf_digests(level, scope_g, &scope, counterpart)
                .map_err(|e| TransportError::Engine(e.to_string()))?;
            let theirs = peer
                .tree(zone_id, level, scope_g, &scope_ids, local_node)
                .await?;
            if local.borrow().tree_scope_token_valid(&token) {
                break (mine, theirs);
            }
            attempts += 1;
            if attempts == 3 {
                counters.ae_tree_token_exhaustions.inc();
                return Err(TransportError::Busy);
            }
        };
        counters.ae_tree_nodes_examined.add(mine.len() as u64);
        let theirs: std::collections::HashMap<u32, u64> = theirs.into_iter().collect();
        let differing: HashSet<u32> = mine
            .into_iter()
            .filter(|(id, d)| theirs.get(id).copied() != Some(*d))
            .map(|(id, _)| id)
            .collect();
        if differing.is_empty() {
            return Ok((0, None));
        }
        scope_g = level;
        scope = differing;
    }
    let diff: Vec<u32> = scope.into_iter().collect();
    let mut applied = 0usize;
    let mut examined = 0usize;
    let mut value_reads = 0usize;
    let mut pages = 0usize;
    let mut after: Option<Vec<u8>> = resume.map(<[u8]>::to_vec);
    let mut local_ops = 0usize;
    loop {
        pages += 1;
        let (digests, next) = peer
            .bucket_digests(zone_id, num_buckets, &diff, after.as_deref())
            .await?;

        let fetchable = max_value_reads - value_reads;
        let mut need: Vec<Vec<u8>> = Vec::new();
        let mut seen = 0usize;
        for (key, peer_version) in digests {
            seen += 1;
            if keep(&key).is_none() {
                continue;
            }
            pace_local_op(&mut local_ops).await;
            let behind = match local_version(local, &key, gate).await? {
                None => true,
                Some(cur) => peer_version.wins_over(&cur),
            };
            if behind {
                need.push(key);
                if need.len() >= fetchable {
                    break;
                }
            }
        }
        examined += seen;
        counters.ae_digests_examined.add(seen as u64);
        let probing_cut = need.len() >= fetchable;

        let mut start = 0;
        while start < need.len() {
            let take = (need.len() - start).min(AE_VALUE_FETCH_CHUNK);
            let req = &need[start..start + take];
            let (entries, trunc) = peer.values(zone_id, req).await?;
            value_reads += entries.len();
            counters.ae_value_reads.add(entries.len() as u64);
            if crate::ram::ram_over_budget() {
                return Err(TransportError::Busy);
            }
            let last_resolved = if trunc {
                entries.last().map(|(k, _)| k.clone())
            } else {
                req.last().cloned()
            };
            for (key, stored) in entries {
                let Some(floor) = keep(&key) else {
                    continue;
                };
                pace_local_op(&mut local_ops).await;
                let acknowledged = local_apply(
                    local,
                    &key,
                    stored.value.as_deref(),
                    stored.version,
                    gate,
                    floor,
                )
                .await?;
                if apply_is_acknowledged(acknowledged) {
                    applied += 1;
                    counters.ae_entries_applied.inc();
                }
            }
            start += match (trunc, &last_resolved) {
                (false, _) => take,
                (true, Some(k)) => req.iter().position(|x| x == k).map_or(1, |i| i + 1),
                (true, None) => 1,
            };
            if value_reads >= max_value_reads {
                let resume_at = need.get(start.saturating_sub(1)).cloned().or(next);
                return Ok((applied, resume_at));
            }
        }
        if probing_cut {
            return Ok((applied, need.last().cloned()));
        }

        let Some(next) = next else {
            return Ok((applied, None));
        };
        after = Some(next);
        if examined >= max_digests || pages >= max_digest_pages {
            return Ok((applied, after));
        }
        vals_rt::yield_now().await;
    }
}

pub async fn sweep<D: BlockDevice + AsyncRead, P: AePeer>(
    local: &RefCell<VersionedStore<D>>,
    local_node: NodeId,
    ring: &Ring,
    num_buckets: u32,
    peers: &[(NodeId, P)],
) -> Result<usize, TransportError> {
    let mut applied = 0;
    for (peer_node, peer) in peers {
        if *peer_node == local_node {
            continue;
        }
        if let Ok((n, _)) = reconcile_scoped(
            local,
            peer,
            num_buckets,
            ring,
            local_node,
            *peer_node,
            (0, 1),
            None,
        )
        .await
        {
            applied += n;
        }
    }
    Ok(applied)
}

pub trait ForgetGate {
    type Hold;
    fn acquire(&mut self) -> impl std::future::Future<Output = Option<Self::Hold>>;
}

impl<F: FnMut() -> bool> ForgetGate for F {
    type Hold = ();
    fn acquire(&mut self) -> impl std::future::Future<Output = Option<()>> {
        let ready = self();
        async move { ready.then_some(()) }
    }
}

#[derive(Default)]
struct RtHandoff {
    released: Vec<Fragment>,
    cut: Option<Vec<u8>>,
    rpcs: usize,
    deferred: bool,
    stopped: bool,
}

#[allow(clippy::too_many_arguments)]
async fn handoff_range_tombstones<D: BlockDevice, T: Transport>(
    local: &RefCell<VersionedStore<D>>,
    zone_id: ZoneId,
    transport: &T,
    min_confirms: usize,
    owners: &impl Fn(&[u8]) -> Vec<NodeId>,
    is_mine: &impl Fn(&[u8], &[NodeId]) -> bool,
    map: &RangeMap,
    window: (&[u8], Option<&[u8]>),
    started: std::time::Instant,
    stop: &impl Fn() -> bool,
) -> RtHandoff {
    let mut out = RtHandoff::default();
    let fragments = {
        let store = local.borrow();
        let f = store.engine().range_tombstones();
        if f.is_empty() {
            return out;
        }
        f.clipped(window.0, window.1)
    };
    let mut pieces: Vec<(Fragment, Vec<NodeId>)> = Vec::new();
    let mut calls: BTreeSet<(NodeId, u128)> = BTreeSet::new();
    'fragments: for f in fragments {
        let lo = (!f.lo.is_empty()).then_some(f.lo.as_slice());
        for slice in map.ranges_overlapping(lo, f.hi.as_deref()) {
            let piece = Fragment { lo: slice.lo.unwrap_or_default(), hi: slice.hi, version: f.version };
            let piece_owners = owners(&piece.lo);
            if piece_owners.is_empty() || is_mine(&piece.lo, &piece_owners) {
                continue;
            }
            let new_calls = piece_owners.iter().filter(|&&o| !calls.contains(&(o, piece.version))).count();
            if !pieces.is_empty() && calls.len() + new_calls > CLEANUP_MAX_HANDOFF_RPCS_PER_PASS / 2 {
                out.cut = Some(piece.lo.clone());
                out.deferred = true;
                break 'fragments;
            }
            calls.extend(piece_owners.iter().map(|&o| (o, piece.version)));
            pieces.push((piece, piece_owners));
        }
    }
    let mut confirms = vec![0usize; pieces.len()];
    for (owner, version) in calls {
        if stop() {
            out.stopped = true;
            return out;
        }
        if started.elapsed() >= CLEANUP_HANDOFF_WALL_BUDGET {
            out.deferred = true;
            break;
        }
        let sent: Vec<usize> = pieces
            .iter()
            .enumerate()
            .filter(|(_, (piece, piece_owners))| piece.version == version && piece_owners.contains(&owner))
            .map(|(i, _)| i)
            .collect();
        let slices: Vec<crate::wire::WireSlice<'_>> = sent
            .iter()
            .map(|&i| {
                let piece = &pieces[i].0;
                ((!piece.lo.is_empty()).then_some(piece.lo.as_slice()), piece.hi.as_deref())
            })
            .collect();
        out.rpcs += 1;
        let acks = transport
            .range_delete(owner, zone_id, false, crate::replica::unpack_version(version), &slices)
            .await;
        for (&i, ack) in sent.iter().zip(acks) {
            if matches!(ack, crate::coord::BatchAck::Applied(Applied::Won | Applied::Lost(_))) {
                confirms[i] += 1;
            }
        }
    }
    let store = local.borrow();
    for ((piece, piece_owners), confirmed) in pieces.into_iter().zip(confirms) {
        if confirmed >= min_confirms.min(piece_owners.len())
            && store.rt_release_accounted(&piece.lo, piece.hi.as_deref(), piece.version)
        {
            out.released.push(piece);
        } else {
            out.deferred = true;
        }
    }
    out
}

#[allow(clippy::too_many_arguments)]
async fn cleanup_with<D: BlockDevice + vals_spdk::AsyncRead, T: Transport, G: ForgetGate>(
    local: &RefCell<VersionedStore<D>>,
    zone_id: ZoneId,
    transport: &T,
    min_confirms: usize,
    owners: impl Fn(&[u8]) -> Vec<NodeId>,
    is_mine: impl Fn(&[u8], &[NodeId]) -> bool,
    range_map: Option<&RangeMap>,
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    mut gate: G,
    placement_valid: impl Fn() -> bool,
    stop: impl Fn() -> bool,
    policy: HandoffPolicy<'_>,
) -> Result<CleanupProgress<D>, TransportError> {
    let dev = local.borrow().device_handle();
    let refusal_age_ms = local.borrow().gc_grace_ms() / 2;
    let mut page_restarts = 0usize;
    let mut staged = local
        .borrow()
        .stage_scan_page(resume, None, CLEANUP_MAX_KEYS_PER_PASS, usize::MAX)
        .map_err(|e| TransportError::Engine(e.to_string()))?;
    let (keys, more) = 'finish: loop {
        match staged {
            vals_store::StagedScanPage::Done(done) => {
                break local
                    .borrow()
                    .finish_cleanup_page(done)
                    .map_err(|e| TransportError::Engine(e.to_string()))?;
            }
            vals_store::StagedScanPage::Runs(mut merge) => {
                let generation = merge.aggregate_generation();
                loop {
                    let stepped = merge.step(&dev).await;
                    if local.borrow().aggregate_generation() != generation {
                        page_restarts += 1;
                        if page_restarts >= CLEANUP_MAX_PAGE_RESTARTS {
                            return Ok(CleanupProgress {
                                next_cursor: resume.map(<[u8]>::to_vec),
                                deferred: true,
                                wrapped: false,
                                ..CleanupProgress::default()
                            });
                        }
                        staged = local
                            .borrow()
                            .stage_scan_page(
                                resume,
                                None,
                                CLEANUP_MAX_KEYS_PER_PASS,
                                usize::MAX,
                            )
                            .map_err(|e| TransportError::Engine(e.to_string()))?;
                        break;
                    }
                    match stepped.map_err(|e| TransportError::Engine(e.to_string()))? {
                        vals_store::StagedScanStep::Done => {
                            #[cfg(test)]
                            cleanup_page_reborrow_hook();
                            if local.borrow().aggregate_generation() != generation {
                                page_restarts += 1;
                                if page_restarts >= CLEANUP_MAX_PAGE_RESTARTS {
                                    return Ok(CleanupProgress {
                                        next_cursor: resume.map(<[u8]>::to_vec),
                                        deferred: true,
                                        wrapped: false,
                                        ..CleanupProgress::default()
                                    });
                                }
                                staged = local
                                    .borrow()
                                    .stage_scan_page(
                                        resume,
                                        None,
                                        CLEANUP_MAX_KEYS_PER_PASS,
                                        usize::MAX,
                                    )
                                    .map_err(|e| TransportError::Engine(e.to_string()))?;
                                break;
                            }
                            break 'finish local
                                .borrow()
                                .finish_cleanup_page(merge.finish())
                                .map_err(|e| TransportError::Engine(e.to_string()))?;
                        }
                        vals_store::StagedScanStep::NeedMem { from } => {
                            #[cfg(test)]
                            cleanup_page_reborrow_hook();
                            let store = local.borrow();
                            if store.aggregate_generation() != generation {
                                drop(store);
                                page_restarts += 1;
                                if page_restarts >= CLEANUP_MAX_PAGE_RESTARTS {
                                    return Ok(CleanupProgress {
                                        next_cursor: resume.map(<[u8]>::to_vec),
                                        deferred: true,
                                        wrapped: false,
                                        ..CleanupProgress::default()
                                    });
                                }
                                staged = local
                                    .borrow()
                                    .stage_scan_page(
                                        resume,
                                        None,
                                        CLEANUP_MAX_KEYS_PER_PASS,
                                        usize::MAX,
                                    )
                                    .map_err(|e| TransportError::Engine(e.to_string()))?;
                                break;
                            }
                            store
                                .scan_supply_mem(&mut merge, &from)
                                .map_err(|e| TransportError::Engine(e.to_string()))?;
                        }
                        vals_store::StagedScanStep::NeedBlocks { cursor } => {
                            #[cfg(test)]
                            cleanup_page_reborrow_hook();
                            let store = local.borrow();
                            if store.aggregate_generation() != generation {
                                drop(store);
                                page_restarts += 1;
                                if page_restarts >= CLEANUP_MAX_PAGE_RESTARTS {
                                    return Ok(CleanupProgress {
                                        next_cursor: resume.map(<[u8]>::to_vec),
                                        deferred: true,
                                        wrapped: false,
                                        ..CleanupProgress::default()
                                    });
                                }
                                staged = local
                                    .borrow()
                                    .stage_scan_page(
                                        resume,
                                        None,
                                        CLEANUP_MAX_KEYS_PER_PASS,
                                        usize::MAX,
                                    )
                                    .map_err(|e| TransportError::Engine(e.to_string()))?;
                                break;
                            }
                            store
                                .scan_supply_blocks(&mut merge, cursor)
                                .map_err(|e| TransportError::Engine(e.to_string()))?;
                        }
                    }
                }
            }
        }
    };
    let (mut keys, mut more) = match end {
        Some(end) => {
            let mut keys = keys;
            let cut = keys.iter().position(|(k, _)| k.as_slice() > end);
            let reached = cut.is_some() || keys.last().is_some_and(|(k, _)| k.as_slice() == end);
            if let Some(cut) = cut {
                keys.truncate(cut);
            }
            (keys, more && !reached)
        }
        None => (keys, more),
    };
    let mut page_last = keys.last().map(|(k, _)| k.clone());
    let rt = match range_map {
        Some(map) if !more || page_last.is_some() => {
            let window_hi = if more { page_last.as_deref() } else { end };
            handoff_range_tombstones(
                local,
                zone_id,
                transport,
                min_confirms,
                &owners,
                &is_mine,
                map,
                (resume.unwrap_or_default(), window_hi),
                std::time::Instant::now(),
                &stop,
            )
            .await
        }
        _ => RtHandoff::default(),
    };
    if rt.stopped {
        return Ok(CleanupProgress {
            next_cursor: resume.map(<[u8]>::to_vec),
            deferred: true,
            wrapped: false,
            ..CleanupProgress::default()
        });
    }
    if let Some(cut) = &rt.cut {
        keys.retain(|(k, _)| k < cut);
        more = true;
        page_last = Some(cut.clone());
    }
    let mut progress = CleanupProgress {
        wrapped: !more,
        handoff_rpcs: rt.rpcs,
        deferred: rt.deferred,
        ..CleanupProgress::default()
    };
    let mut release: Vec<(Vec<u8>, Version)> = Vec::new();
    let mut completed_cursor = resume.map(<[u8]>::to_vec);
    let place = |key: &[u8]| {
        let key_owners = owners(key);
        (!key_owners.is_empty() && !is_mine(key, &key_owners)).then_some(key_owners)
    };
    let mut placed: Vec<Option<Vec<NodeId>>> = Vec::with_capacity(keys.len());
    let mut held = vec![false; keys.len()];
    let mut probed_to = 0usize;
    let mut probe_window = CLEANUP_PROBE_MIN_KEYS;
    let mut probing = true;
    let handoff_started = std::time::Instant::now();
    for i in 0..keys.len() {
        if stop() {
            progress.next_cursor = completed_cursor;
            progress.wrapped = false;
            progress.deferred = true;
            return Ok(progress);
        }
        if i != 0 && i % CLEANUP_YIELD_EVERY == 0 {
            vals_rt::yield_now().await;
        }
        if placed.len() == i {
            placed.push(place(&keys[i].0));
        }
        if probing && i >= probed_to && placed[i].is_some() {
            if cleanup_elapsed(handoff_started) >= CLEANUP_HANDOFF_WALL_BUDGET {
                probing = false;
            } else {
                let mut end = i;
                let mut taken = 0usize;
                while end < keys.len() && taken < probe_window {
                    if placed.len() == end {
                        placed.push(place(&keys[end].0));
                    }
                    taken += usize::from(placed[end].is_some());
                    end += 1;
                }
                let window_started = std::time::Instant::now();
                let probe = probe_owner_versions(transport, zone_id, &keys[i..end], &placed[i..end]);
                let cutoff = ProbeCutoff { started: handoff_started, stop: &stop };
                match vals_rt::race(probe, cutoff).await {
                    vals_rt::Either::Left(window_held) => {
                        probing = window_held.contains(&true);
                        held[i..end].copy_from_slice(&window_held);
                    }
                    vals_rt::Either::Right(()) => probing = false,
                }
                probed_to = end;
                probe_window = if cleanup_elapsed(window_started) < CLEANUP_HANDOFF_WALL_BUDGET / 8 {
                    (probe_window * 2).min(CLEANUP_PROBE_MAX_KEYS)
                } else {
                    (probe_window / 2).max(CLEANUP_PROBE_MIN_KEYS)
                };
                if stop() {
                    progress.next_cursor = completed_cursor;
                    progress.wrapped = false;
                    progress.deferred = true;
                    return Ok(progress);
                }
            }
        }
        let (key, listed_version) = (std::mem::take(&mut keys[i].0), keys[i].1);
        progress.scanned += 1;
        let Some(owners) = placed[i].take() else {
            completed_cursor = Some(key);
            continue;
        };
        if held[i] {
            progress.probe_released += 1;
            release.push((key.clone(), listed_version));
            completed_cursor = Some(key);
            continue;
        }
        if progress.handoff_rpcs.saturating_add(owners.len())
            > CLEANUP_MAX_HANDOFF_RPCS_PER_PASS
        {
            progress.scanned -= 1;
            progress.next_cursor = completed_cursor.clone();
            progress.wrapped = false;
            progress.deferred = true;
            break;
        }
        let Some(stored) = local
            .borrow()
            .stored(&key)
            .map_err(|e| TransportError::Engine(e.to_string()))?
        else {
            completed_cursor = Some(key);
            continue;
        };
        if stored.version != listed_version {
            progress.deferred = true;
            progress.note_kept(&key);
            completed_cursor = Some(key);
            continue;
        }
        if (policy.refused)(&key, stored.version) {
            progress.deferred = true;
            progress.note_kept(&key);
            progress.refused.push((key.clone(), stored.version));
            completed_cursor = Some(key);
            continue;
        }
        let trusted = (policy.trusted)(&key);
        let need = min_confirms.min(owners.len());
        let mut confirms: usize = 0;
        let mut refusals: usize = 0;
        let mut key_failed = false;
        for (owner_i, &owner) in owners.iter().enumerate() {
            let remaining = owners.len() - owner_i;
            let only_refused = confirms == 0 && refusals == owner_i;
            if cleanup_elapsed(handoff_started) >= CLEANUP_HANDOFF_WALL_BUDGET
                || (confirms.saturating_add(remaining) < need && !only_refused)
            {
                progress.deferred = true;
                key_failed = true;
                break;
            }
            if stop() {
                progress.next_cursor = completed_cursor;
                progress.wrapped = false;
                progress.deferred = true;
                return Ok(progress);
            }
            progress.handoff_rpcs += 1;
            let value = stored.value.as_deref();
            let reply = if trusted {
                crate::observe::counters().cleanup_trusted_handoffs.inc();
                transport.apply_fill(owner, zone_id, &key, value, stored.version).await
            } else {
                transport.apply(owner, zone_id, &key, value, stored.version).await
            };
            refusals += usize::from(matches!(reply, Ok(Applied::Rejected)));
            let confirmed = matches!(reply, Ok(Applied::Won) | Ok(Applied::Lost(_)));
            if confirmed {
                confirms += 1;
            } else {
                progress.deferred = true;
                key_failed = true;
            }
            if key_failed && confirms >= need {
                break;
            }
            if cleanup_elapsed(handoff_started) >= CLEANUP_HANDOFF_WALL_BUDGET {
                progress.deferred = true;
            }
        }
        if confirms >= need {
            release.push((key.clone(), stored.version));
        } else {
            progress.deferred = true;
            progress.note_kept(&key);
            if confirms == 0
                && refusals == owners.len()
                && refusal_age_ms > 0
                && stored.value.is_some()
                && stored.version.physical_ms().saturating_add(refusal_age_ms) < crate::hlc::physical_now_ms()
                && {
                    progress.handoff_rpcs += owners.len();
                    no_owner_holds(transport, zone_id, &owners, &key, stored.version).await
                }
            {
                progress.refused.push((key.clone(), stored.version));
            }
        }
        completed_cursor = Some(key);
        if key_failed || cleanup_elapsed(handoff_started) >= CLEANUP_HANDOFF_WALL_BUDGET {
            progress.next_cursor = completed_cursor.clone();
            if local.borrow().engine().ordering() == vals_store::Ordering::Sorted {
                if let Some(past) = progress.next_cursor.as_mut() {
                    past.push(0);
                }
            }
            progress.wrapped = false;
            progress.deferred = true;
            break;
        }
    }
    if progress.wrapped || progress.next_cursor.is_none() {
        progress.next_cursor = more.then_some(page_last.unwrap_or_default());
    }
    if release.is_empty() && rt.released.is_empty() && !local.borrow().cleanup_forget_pending() {
        return Ok(progress);
    }
    if stop() {
        progress.deferred = true;
        progress.wrapped = false;
        progress.next_cursor = completed_cursor;
        return Ok(progress);
    }
    if !placement_valid() {
        progress.deferred = true;
        progress.wrapped = false;
        return Ok(progress);
    }
    {
        let mut store = local.borrow_mut();
        store.accumulate_released(&rt.released);
        store.accumulate_forget(&release, progress.wrapped);
        if !store.cleanup_forget_due() {
            progress.deferred |= store.cleanup_forget_pending();
            return Ok(progress);
        }
    }
    let Some(_hold) = gate.acquire().await else {
        progress.deferred = true;
        return Ok(progress);
    };
    if !placement_valid() {
        progress.deferred = true;
        progress.wrapped = false;
        return Ok(progress);
    }
    let mut store = local.borrow_mut();
    let (released, plan) = store
        .cleanup_forget_batch()
        .map_err(|e| TransportError::Engine(e.to_string()))?;
    progress.deferred |= store.cleanup_forget_pending() || (released == 0 && plan.is_none());
    progress.released = released;
    progress.forget_plan = plan;
    Ok(progress)
}

async fn no_owner_holds<T: Transport>(
    transport: &T,
    zone_id: ZoneId,
    owners: &[NodeId],
    key: &[u8],
    version: Version,
) -> bool {
    for &owner in owners {
        match transport.digest(owner, zone_id, key).await {
            Ok(None) => {}
            Ok(Some(held)) if held < version => {}
            _ => return false,
        }
    }
    true
}

struct ProbeCutoff<'a, S: Fn() -> bool> {
    started: std::time::Instant,
    stop: &'a S,
}

impl<S: Fn() -> bool> std::future::Future for ProbeCutoff<'_, S> {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<()> {
        if (self.stop)() || cleanup_elapsed(self.started) >= CLEANUP_HANDOFF_WALL_BUDGET {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    }
}

async fn probe_owner_versions<T: Transport>(
    transport: &T,
    zone_id: ZoneId,
    keys: &[(Vec<u8>, Version)],
    placed: &[Option<Vec<NodeId>>],
) -> Vec<bool> {
    let mut held: Vec<bool> = placed.iter().map(Option::is_some).collect();
    let probed: BTreeSet<NodeId> = placed.iter().flatten().flatten().copied().collect();
    let asks = probed.into_iter().map(|owner| {
        let slots: Vec<usize> = (0..keys.len())
            .filter(|&i| placed[i].as_ref().is_some_and(|o| o.contains(&owner)))
            .collect();
        async move {
            let reads: Vec<MixedReadEntry<'_>> = slots
                .iter()
                .map(|&i| MixedReadEntry { key: &keys[i].0, role: MixedReadRole::Digest })
                .collect();
            let replies = transport.read_mixed(owner, zone_id, &reads).await;
            (slots, replies)
        }
    });
    for (slots, replies) in vals_rt::join_all(asks.collect()).await {
        for (j, &i) in slots.iter().enumerate() {
            let at_least_listed = matches!(
                replies.get(j),
                Some(Ok(MixedReadReply::Digest(Some(version)))) if *version >= keys[i].1
            );
            held[i] &= at_least_listed;
        }
    }
    held
}

#[allow(clippy::too_many_arguments)]
pub async fn cleanup_scoped<D: BlockDevice + vals_spdk::AsyncRead, T: Transport, G: ForgetGate>(
    local: &RefCell<VersionedStore<D>>,
    transport: &T,
    ring: &Ring,
    local_node: NodeId,
    min_confirms: usize,
    shard: (usize, usize),
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    gate: G,
    placement_valid: impl Fn() -> bool,
    stop: impl Fn() -> bool,
) -> Result<CleanupProgress<D>, TransportError> {
    cleanup_scoped_with(
        local,
        transport,
        ring,
        local_node,
        min_confirms,
        shard,
        resume,
        end,
        gate,
        placement_valid,
        stop,
        HandoffPolicy::PLAIN,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn cleanup_scoped_with<D: BlockDevice + vals_spdk::AsyncRead, T: Transport, G: ForgetGate>(
    local: &RefCell<VersionedStore<D>>,
    transport: &T,
    ring: &Ring,
    local_node: NodeId,
    min_confirms: usize,
    shard: (usize, usize),
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    gate: G,
    placement_valid: impl Fn() -> bool,
    stop: impl Fn() -> bool,
    policy: HandoffPolicy<'_>,
) -> Result<CleanupProgress<D>, TransportError> {
    let (shard_index, shard_count) = shard;
    cleanup_with(
        local,
        DEFAULT_ZONE,
        transport,
        min_confirms,
        |key| ring.replicas(key),
        |key, owners| {
            if !owners.contains(&local_node) {
                return false;
            }
            if shard_count > 1 {
                let s = (crate::ring::hash64(key) % shard_count as u64) as usize;
                return s == shard_index;
            }
            true
        },
        None,
        resume,
        end,
        gate,
        placement_valid,
        stop,
        policy,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn cleanup_scoped_zone<
    D: BlockDevice + vals_spdk::AsyncRead,
    T: Transport,
    G: ForgetGate,
>(
    local: &RefCell<VersionedStore<D>>,
    transport: &T,
    ring: &Ring,
    local_node: NodeId,
    zone_id: ZoneId,
    rf: usize,
    scope: Option<&BTreeSet<NodeId>>,
    min_confirms: usize,
    range_map: Option<&RangeMap>,
    shard: (usize, usize),
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    gate: G,
    copy_hold: impl Fn(&[u8]) -> bool,
    placement_valid: impl Fn() -> bool,
    stop: impl Fn() -> bool,
) -> Result<CleanupProgress<D>, TransportError> {
    cleanup_scoped_zone_with(
        local,
        transport,
        ring,
        local_node,
        zone_id,
        rf,
        scope,
        min_confirms,
        range_map,
        shard,
        resume,
        end,
        gate,
        copy_hold,
        placement_valid,
        stop,
        HandoffPolicy::PLAIN,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn cleanup_scoped_zone_with<
    D: BlockDevice + vals_spdk::AsyncRead,
    T: Transport,
    G: ForgetGate,
>(
    local: &RefCell<VersionedStore<D>>,
    transport: &T,
    ring: &Ring,
    local_node: NodeId,
    zone_id: ZoneId,
    rf: usize,
    scope: Option<&BTreeSet<NodeId>>,
    min_confirms: usize,
    range_map: Option<&RangeMap>,
    shard: (usize, usize),
    resume: Option<&[u8]>,
    end: Option<&[u8]>,
    gate: G,
    copy_hold: impl Fn(&[u8]) -> bool,
    placement_valid: impl Fn() -> bool,
    stop: impl Fn() -> bool,
    policy: HandoffPolicy<'_>,
) -> Result<CleanupProgress<D>, TransportError> {
    let (shard_index, shard_count) = shard;
    cleanup_with(
        local,
        zone_id,
        transport,
        min_confirms,
        |key| match range_map {
            Some(m) => m.replicas_for_key_while_rehoming(
                ring, zone_id, key, rf, scope
            ),
            None => ring.replicas_in_zone(zone_id, key, rf, scope),
        },
        |key, owners| {
            if copy_hold(key) {
                return true;
            }
            if !owners.contains(&local_node) {
                return false;
            }
            if shard_count > 1 {
                let place = match range_map {
                    Some(m) => m.range_token(key),
                    None => key,
                };
                let current = (crate::ring::hash64(place) % shard_count as u64) as usize;
                let pending = range_map
                    .and_then(|m| m.pending_for_key(key))
                    .map(|token| (crate::ring::hash64(token) % shard_count as u64) as usize);
                return current == shard_index || pending == Some(shard_index);
            }
            true
        },
        range_map,
        resume,
        end,
        gate,
        placement_valid,
        stop,
        policy,
    )
    .await
}

pub struct LocalPeer<'a, D: BlockDevice>(pub &'a VersionedStore<D>);

impl<D: BlockDevice + AsyncRead> AePeer for LocalPeer<'_, D> {
    async fn tree(
        &self,
        _zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &[u32],
        requester: Option<NodeId>,
    ) -> Result<Vec<(u32, u64)>, TransportError> {
        let scope: HashSet<u32> = scope.iter().copied().collect();
        if !crate::replica::ensure_tree_ready_cooperative(self.0, scope_g, &scope)
            .await
            .map_err(|e| TransportError::Engine(e.to_string()))?
        {
            return Err(TransportError::Busy);
        }
        let token = self.0.tree_scope_token(scope_g, &scope);
        let tree = self
            .0
            .scoped_leaf_digests(level, scope_g, &scope, requester)
            .map_err(|e| TransportError::Engine(e.to_string()))?;
        if !self.0.tree_scope_token_valid(&token) {
            return Err(TransportError::Busy);
        }
        Ok(tree)
    }

    async fn bucket_digests(
        &self,
        _zone_id: ZoneId,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
    ) -> Result<crate::replica::DigestCursorPage, TransportError> {
        self.0
            .bucket_digest_page(num_buckets, ids, after, usize::MAX)
            .map(digest_cursor_page)
            .map_err(|e| TransportError::Engine(e.to_string()))
    }

    async fn values(
        &self,
        _zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> Result<crate::replica::EntryPage, TransportError> {
        self.0
            .values_page(keys, usize::MAX)
            .map_err(|e| TransportError::Engine(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Engine, Ordering};

    fn store(node: u32) -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    }

    fn run<F: std::future::Future>(f: F) -> F::Output {
        vals_rt::block_on(f, || {})
    }

    fn v(hlc: u64, node: u32) -> Version {
        Version { hlc, node: node.into() }
    }

    #[test]
    fn local_ae_apply_modes_ack_and_converge_by_logical_winner() {
        let build = || {
            let mut s = store(1);
            s.apply(b"k", Some(b"winner"), v(20, 1)).unwrap();
            s.flush().unwrap();
            RefCell::new(s)
        };
        let blind = build();
        let legacy = build();
        let blind_ack = run(local_apply_mode(
            &blind,
            b"k",
            Some(b"stale"),
            v(10, 2),
            None,
            crate::replica::ApplyMode::Blind,
            crate::replica::LiveFloor::Waive,
        ))
        .unwrap();
        let legacy_ack = run(local_apply_mode(
            &legacy,
            b"k",
            Some(b"stale"),
            v(10, 2),
            None,
            crate::replica::ApplyMode::Legacy,
            crate::replica::LiveFloor::Waive,
        ))
        .unwrap();
        assert_eq!(blind_ack, Applied::Won);
        assert_eq!(legacy_ack, Applied::Lost(v(20, 1)));
        assert!(apply_is_acknowledged(blind_ack));
        assert!(apply_is_acknowledged(legacy_ack));
        assert_eq!(
            blind.borrow().get_versioned(b"k").unwrap(),
            legacy.borrow().get_versioned(b"k").unwrap()
        );

        let remote = Stored {
            version: v(30, 2),
            value: Some(b"remote".to_vec()),
        };
        let accepted = run(local_apply_mode(
            &blind,
            b"k",
            remote.value.as_deref(),
            remote.version,
            None,
            crate::replica::ApplyMode::Blind,
            crate::replica::LiveFloor::Waive,
        ))
        .unwrap();
        assert!(apply_is_acknowledged(accepted));
        assert_eq!(blind.borrow().get_versioned(b"k").unwrap(), Some(remote));
        assert!(!apply_is_acknowledged(Applied::Rejected));
    }

    #[test]
    fn ae_does_not_copy_a_collected_expired_tombstone_back() {
        let now = crate::hlc::physical_now_ms();
        let grace = 1_000;
        let at = |ms: u64, node: u32| Version { hlc: ms << crate::hlc::COUNTER_BITS, node: node.into() };
        let mut collected = store(1).with_gc_grace_ms(grace);
        collected.apply_at(b"k", None, at(now - 10_000, 2), now - 10_000).unwrap();
        collected.apply_at(b"anchor", Some(b"a"), at(now, 1), now).unwrap();
        collected.flush().unwrap();
        collected.advance_durable_gc_watermark(now);
        assert!(collected.tombstone_gc_due(now));
        let plan = collected.compact_plan_tombstone_gc(now).unwrap().expect("a tombstone GC merge");
        let wdev = std::sync::Arc::new(collected.device_sibling().unwrap());
        let written = run(vals_store::compact_and_write(plan, wdev)).unwrap();
        collected.compact_commit(written).unwrap();
        assert_eq!(collected.get_versioned(b"k").unwrap(), None);

        let mut holding = store(1).with_gc_grace_ms(grace);
        holding.apply_at(b"anchor", Some(b"a"), at(now - 10_000, 1), now - 10_000).unwrap();
        holding.flush().unwrap();
        holding.apply_at(b"k", None, at(now - 10_000, 2), now - 10_000).unwrap();
        holding.flush().unwrap();
        holding.advance_durable_gc_watermark(now);
        assert_eq!(
            holding.apply_at(b"absent", None, at(now - 10_000, 2), now).unwrap(),
            Applied::Won,
            "an expired tombstone for an absent key is acknowledged"
        );
        assert_eq!(holding.get_versioned(b"absent").unwrap(), None, "but not stored");
        assert!(holding.get_versioned(b"k").unwrap().is_some());

        let collected = RefCell::new(collected);
        run(reconcile(&collected, &LocalPeer(&holding), DEFAULT_BUCKETS)).unwrap();
        assert_eq!(
            collected.borrow().get_versioned(b"k").unwrap(),
            None,
            "an expired tombstone for an absent key must not be re-stored"
        );
        let fresh = at(now - 100, 3);
        holding.apply_at(b"fresh", None, fresh, now).unwrap();
        run(reconcile(&collected, &LocalPeer(&holding), DEFAULT_BUCKETS)).unwrap();
        assert_eq!(
            collected.borrow().get_versioned(b"fresh").unwrap(),
            Some(Stored { version: fresh, value: None }),
            "a tombstone inside grace is still repaired"
        );

        assert!(holding.tombstone_gc_due(now));
        let plan = holding.compact_plan_tombstone_gc(now).unwrap().expect("a multi-run tombstone GC merge");
        let wdev = std::sync::Arc::new(holding.device_sibling().unwrap());
        let written = run(vals_store::compact_and_write(plan, wdev)).unwrap();
        holding.compact_commit(written).unwrap();
        assert_eq!(holding.get_versioned(b"k").unwrap(), None);
        assert!(holding.get_versioned(b"fresh").unwrap().is_some());
        assert!(!holding.tombstone_gc_due(now));
    }

    #[test]
    fn attached_seed_driver_is_the_only_seed_driver() {
        for isolation in [true, false] {
            let _policy = crate::replica::seed_policy::policy_for_test(isolation, 25, 64 << 20);
            let stale = RefCell::new(store(1));
            for i in 0..40u64 {
                stale
                    .borrow_mut()
                    .apply(format!("k{i:03}").as_bytes(), Some(b"a"), v(i + 1, 1))
                    .unwrap();
            }
            stale.borrow_mut().flush().unwrap();
            stale.borrow().invalidate_leaf_seed();
            stale.borrow().set_seed_driver(true);
            let mut current = store(2);
            current.apply(b"k000", Some(b"b"), v(1_000, 2)).unwrap();
            let all = HashSet::from([0]);
            let reconciled = run(reconcile(&stale, &LocalPeer(&current), DEFAULT_BUCKETS));
            if !isolation {
                assert_eq!(reconciled.unwrap(), 1, "knockout: reconcile drives the seed itself");
                assert!(stale.borrow().is_leaf_seeded());
                continue;
            }
            assert!(matches!(reconciled, Err(TransportError::Busy)), "{reconciled:?}");
            let ready = run(crate::replica::ensure_tree_ready_cooperative_cell(&stale, 1, &all));
            assert!(!ready.unwrap());
            let tree = {
                let local = stale.borrow();
                run(LocalPeer(&*local).tree(DEFAULT_ZONE, DEFAULT_BUCKETS, 1, &[0], None))
            };
            assert!(matches!(tree, Err(TransportError::Busy)));
            assert!(!run(crate::replica::ensure_seeded_cooperative_cell(&stale)).unwrap());
            assert!(!stale.borrow().is_leaf_seeded());
            assert_eq!(stale.borrow().seed_test_counts().1, 0, "non-drivers fold nothing");
            while !stale.borrow().seed_step_cooperative().unwrap() {}
            assert!(stale.borrow().seed_test_counts().1 >= 40);
            assert_eq!(run(reconcile(&stale, &LocalPeer(&current), DEFAULT_BUCKETS)).unwrap(), 1);
            assert_eq!(run(reconcile(&stale, &LocalPeer(&current), DEFAULT_BUCKETS)).unwrap(), 0);
            stale.borrow().set_seed_driver(false);
            stale.borrow().invalidate_leaf_seed();
            assert!(run(crate::replica::ensure_tree_ready_cooperative_cell(&stale, 1, &all)).unwrap(),
                "a store without a driver task still seeds on demand");
        }
    }

    #[test]
    fn anti_entropy_hashes_and_repairs_the_full_writer_identity() {
        let hlc = 1_000 << crate::hlc::COUNTER_BITS;
        let lower = Version {
            hlc,
            node: 0x8000_0000_0000_0011,
        };
        let higher = Version {
            hlc,
            node: 0x8000_0000_0000_0022,
        };
        assert_ne!(entry_digest(b"k", lower), entry_digest(b"k", higher));

        let stale = RefCell::new(store(1));
        stale.borrow_mut().apply(b"k", Some(b"a"), lower).unwrap();
        let mut current = store(2);
        current.apply(b"k", Some(b"b"), higher).unwrap();

        assert_eq!(run(reconcile(&stale, &LocalPeer(&current), DEFAULT_BUCKETS)).unwrap(), 1);
        let got = stale.borrow().get_versioned(b"k").unwrap().unwrap();
        assert_eq!(got.version, higher);
        assert_eq!(got.value.as_deref(), Some(&b"b"[..]));
        assert_eq!(run(reconcile(&stale, &LocalPeer(&current), DEFAULT_BUCKETS)).unwrap(), 0);
    }

    #[test]
    fn local_version_counts_existence_not_digest() {
        let local = RefCell::new(store(1));
        local.borrow_mut().apply(b"k", Some(b"old"), v(10, 1)).unwrap();
        local.borrow_mut().flush().unwrap();
        let before = vals_store::attribution::lookup_snapshot();
        assert_eq!(run(local_version(&local, b"k", None)).unwrap(), Some(v(10, 1)));
        let after = vals_store::attribution::lookup_snapshot();
        assert_eq!(after[2][0] - before[2][0], 1);
        assert_eq!(after[2][1] - before[2][1], 1);
        assert_eq!(after[2][2] - before[2][2], 1);
        assert_eq!(after[1], before[1]);
    }

    #[test]
    fn local_version_retries_a_same_key_mutable_change() {
        let local = std::rc::Rc::new(RefCell::new(store(1)));
        local
            .borrow_mut()
            .apply(b"k", Some(b"old"), v(10, 1))
            .unwrap();
        local.borrow_mut().flush().unwrap();
        let fired = std::rc::Rc::new(std::cell::Cell::new(false));
        {
            let local = local.clone();
            let fired = fired.clone();
            set_local_version_reborrow_hook(Some(Box::new(move || {
                if !fired.replace(true) {
                    local
                        .borrow_mut()
                        .apply(b"k", Some(b"new"), v(20, 1))
                        .unwrap();
                }
            })));
        }
        let got = run(local_version(&local, b"k", None)).unwrap();
        set_local_version_reborrow_hook(None);
        assert_eq!(got, Some(v(20, 1)));
        assert!(fired.get());
    }

    fn owned_keys(ring: &Ring, node: NodeId, pool: &[Vec<u8>]) -> Vec<Vec<u8>> {
        pool.iter()
            .filter(|k| ring.replicas(k).contains(&node))
            .cloned()
            .collect()
    }

    #[test]
    fn scoped_reconcile_regains_owned_without_adopting_others() {
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();

        let n1 = RefCell::new(store(1));
        let mut n2 = store(2);
        for k in &keys {
            let ver = v(100, 9);
            if ring.replicas(k).contains(&2) {
                n2.apply(k, Some(b"v"), ver).unwrap();
            }
        }

        let (pulled, _) = run(reconcile_scoped(
            &n1,
            &LocalPeer(&n2),
            DEFAULT_BUCKETS,
            &ring,
            1,
            2,
            (0, 1),
            None,
        ))
        .unwrap();

        let shared = owned_keys(&ring, 1, &keys)
            .into_iter()
            .filter(|k| ring.replicas(k).contains(&2))
            .count();
        assert!(shared > 0, "test needs some shared keys");
        assert_eq!(pulled, shared);
        for k in &keys {
            let has = n1.borrow().get(k).unwrap().is_some();
            let should = ring.replicas(k).contains(&1) && ring.replicas(k).contains(&2);
            assert_eq!(has, should, "n1 holds exactly its shared keys");
        }
        for k in &keys {
            if !ring.replicas(k).contains(&1) {
                assert!(
                    n1.borrow().get(k).unwrap().is_none(),
                    "must not over-replicate"
                );
            }
        }
    }

    #[test]
    fn sweep_converges_a_rejoined_node_over_the_cluster() {
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..300).map(|i| format!("k{i:04}").into_bytes()).collect();

        let n1 = RefCell::new(store(1));
        let mut n2 = store(2);
        let mut n3 = store(3);
        for k in &keys {
            let ver = v(100, 9);
            if ring.replicas(k).contains(&2) {
                n2.apply(k, Some(b"v"), ver).unwrap();
            }
            if ring.replicas(k).contains(&3) {
                n3.apply(k, Some(b"v"), ver).unwrap();
            }
        }

        let peers = [(2, LocalPeer(&n2)), (3, LocalPeer(&n3))];
        run(sweep(&n1, 1, &ring, DEFAULT_BUCKETS, &peers)).unwrap();

        for k in &keys {
            assert_eq!(
                n1.borrow().get(k).unwrap().is_some(),
                ring.replicas(k).contains(&1),
                "node 1 ends holding exactly the keys it is a replica for"
            );
        }
    }

    fn wire_placement(s: &VersionedStore<MemBlockDevice>, ring: &Ring) {
        let r = ring.clone();
        let f: crate::replica::PlacementFn = std::sync::Arc::new(move |k: &[u8]| r.replicas(k));
        s.set_placement(f);
        s.invalidate_leaf_seed();
    }

    #[test]
    fn converged_rf_lt_n_pair_examines_zero_digests() {
        use std::collections::HashSet;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..400).map(|i| format!("k{i:04}").into_bytes()).collect();

        let mut s1 = store(1);
        let mut s2 = store(2);
        for k in &keys {
            let ver = v(100, 9);
            if ring.replicas(k).contains(&1) {
                s1.apply(k, Some(b"v"), ver).unwrap();
            }
            if ring.replicas(k).contains(&2) {
                s2.apply(k, Some(b"v"), ver).unwrap();
            }
        }
        wire_placement(&s1, &ring);
        wire_placement(&s2, &ring);

        let full = HashSet::from([0]);
        let s1_unscoped = s1
            .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &full, None)
            .unwrap();
        let s2_unscoped = s2
            .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &full, None)
            .unwrap();
        assert_ne!(
            s1_unscoped, s2_unscoped,
            "unscoped full trees differ on the non-shared keysets (the residual)"
        );
        let s1_scoped = s1
            .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &full, Some(2))
            .unwrap();
        let s2_scoped = s2
            .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &full, Some(1))
            .unwrap();
        assert_eq!(
            s1_scoped, s2_scoped,
            "peer-scoped trees match over the shared keyspace"
        );

        let s1 = RefCell::new(s1);
        let peer = CountingPeer::new(&s2);
        let (applied, cursor) = run(reconcile_scoped(
            &s1,
            &peer,
            DEFAULT_BUCKETS,
            &ring,
            1,
            2,
            (0, 1),
            None,
        ))
        .unwrap();
        assert_eq!(applied, 0, "converged: nothing applied");
        assert_eq!(
            peer.digest_entries.get(),
            0,
            "converged RF<N pair examines 0 phase-1 digests (the ~68k→0 acceptance criterion)"
        );
        assert!(cursor.is_none(), "no resume cursor when nothing differs");
    }

    #[test]
    fn scoped_reconcile_still_heals_shared_divergence() {
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..400).map(|i| format!("k{i:04}").into_bytes()).collect();

        let mut s1 = store(1);
        let mut s2 = store(2);
        for k in &keys {
            if ring.replicas(k).contains(&1) {
                s1.apply(k, Some(b"v"), v(100, 9)).unwrap();
            }
            if ring.replicas(k).contains(&2) {
                s2.apply(k, Some(b"v"), v(100, 9)).unwrap();
            }
        }
        let shared_key = keys
            .iter()
            .find(|k| ring.replicas(k).contains(&1) && ring.replicas(k).contains(&2))
            .cloned()
            .expect("some key is shared by nodes 1 and 2");
        s2.apply(&shared_key, Some(b"new"), v(300, 2)).unwrap();

        wire_placement(&s1, &ring);
        wire_placement(&s2, &ring);

        let s1 = RefCell::new(s1);
        let peer = CountingPeer::new(&s2);
        let (applied, _) = run(reconcile_scoped(
            &s1,
            &peer,
            DEFAULT_BUCKETS,
            &ring,
            1,
            2,
            (0, 1),
            None,
        ))
        .unwrap();
        assert_eq!(applied, 1, "the one shared divergent key healed");
        assert_eq!(
            s1.borrow().get(&shared_key).unwrap().as_deref(),
            Some(&b"new"[..]),
            "node 1 adopted the newer shared value"
        );
        assert!(
            peer.digest_entries.get() <= 32,
            "sparse shared divergence stays localized (examined {} digests)",
            peer.digest_entries.get()
        );
    }

    #[test]
    fn scoped_zone_reconcile_skips_out_of_scope_node() {
        let ring = Ring::from_members(3, 64, [1, 2, 3]);
        let zid = 5;
        let scope: BTreeSet<NodeId> = [1, 2].into_iter().collect();

        let mut peer = store(9);
        let keys: Vec<Vec<u8>> = (0..120).map(|i| format!("k{i:03}").into_bytes()).collect();
        for k in &keys {
            peer.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }

        let n3 = RefCell::new(store(3));
        let (pulled3, _) = run(reconcile_scoped_zone(
            &n3,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            3,
            2,
            zid,
            2,
            Some(&scope),
            None,
            (0, 1),
            None,
        ))
        .unwrap();
        assert_eq!(pulled3, 0, "out-of-scope node adopts nothing");
        assert!(keys.iter().all(|k| n3.borrow().get(k).unwrap().is_none()));

        let n1 = RefCell::new(store(1));
        let (pulled1, _) = run(reconcile_scoped_zone(
            &n1,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            1,
            2,
            zid,
            2,
            Some(&scope),
            None,
            (0, 1),
            None,
        ))
        .unwrap();
        assert!(pulled1 > 0, "in-scope node pulls its share of zone 5");
    }

    #[test]
    fn sorted_zone_reconcile_scopes_by_range_token_not_key() {
        use crate::partition::RangeMap;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let zid = 5;
        let map = RangeMap::from_splits([b"m".to_vec()]);

        let mut peer = store(9);
        let keys: Vec<Vec<u8>> = (0..200)
            .map(|i| format!("{}{i:03}", if i % 2 == 0 { "a" } else { "z" }).into_bytes())
            .collect();
        for k in &keys {
            peer.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }

        let n1 = RefCell::new(store(1));
        run(reconcile_scoped_zone(
            &n1,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            1,
            9,
            zid,
            2,
            None,
            Some(&map),
            (0, 1),
            None,
        ))
        .unwrap();

        let mut token_differs_from_key = false;
        for k in &keys {
            let by_token = ring
                .replicas_in_zone(zid, map.range_token(k), 2, None)
                .contains(&1);
            let by_key = ring.replicas_in_zone(zid, k, 2, None).contains(&1);
            assert_eq!(
                n1.borrow().get(k).unwrap().is_some(),
                by_token,
                "node 1 holds key {k:?} iff its range token places on it"
            );
            if by_token != by_key {
                token_differs_from_key = true;
            }
        }
        assert!(
            token_differs_from_key,
            "range-token scoping must differ from key scoping for some key"
        );
    }

    #[test]
    fn sorted_zone_reconcile_does_not_adopt_sibling_shard_keys() {
        use crate::partition::RangeMap;
        let ring = Ring::from_members(2, 64, [1, 9]);
        let zid = 5;
        const K: usize = 2;
        let splits: Vec<Vec<u8>> = (1..10)
            .map(|r| format!("k{:03}", r * 20).into_bytes())
            .collect();
        let map = RangeMap::from_splits(splits);

        let mut peer = store(9);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:03}").into_bytes()).collect();
        for k in &keys {
            peer.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }

        let n1 = RefCell::new(store(1));
        run(reconcile_scoped_zone(
            &n1,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            1,
            9,
            zid,
            2,
            None,
            Some(&map),
            (0, K),
            None,
        ))
        .unwrap();

        let mut saw_mine = false;
        let mut saw_sibling = false;
        for k in &keys {
            let on_shard0 = (crate::ring::hash64(map.range_token(k)) % K as u64) as usize == 0;
            assert_eq!(
                n1.borrow().get(k).unwrap().is_some(),
                on_shard0,
                "shard 0 holds key {k:?} iff its range token hashes to shard 0"
            );
            saw_mine |= on_shard0;
            saw_sibling |= !on_shard0;
        }
        assert!(
            saw_mine && saw_sibling,
            "the range map must place keys on both shard 0 and its sibling"
        );
    }

    #[test]
    fn default_zone_reconcile_does_not_adopt_sibling_shard_keys() {
        let ring = Ring::from_members(2, 64, [1, 9]);
        const K: usize = 2;

        let mut peer = store(9);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("key-{i:03}").into_bytes()).collect();
        for k in &keys {
            peer.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }

        let n1 = RefCell::new(store(1));
        run(reconcile_scoped(
            &n1,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            1,
            9,
            (1, K),
            None,
        ))
        .unwrap();

        let mut saw_mine = false;
        let mut saw_sibling = false;
        for k in &keys {
            let on_shard1 = (crate::ring::hash64(k) % K as u64) as usize == 1;
            assert_eq!(
                n1.borrow().get(k).unwrap().is_some(),
                on_shard1,
                "shard 1 holds key {k:?} iff it hashes to shard 1"
            );
            saw_mine |= on_shard1;
            saw_sibling |= !on_shard1;
        }
        assert!(
            saw_mine && saw_sibling,
            "the key set must land on both shard 1 and its sibling"
        );
    }

    #[test]
    fn ae_waives_the_gc_floor_only_for_the_peer_shard_that_owns_the_key() {
        let now = crate::hlc::physical_now_ms();
        let at = |ms: u64| Version { hlc: ms << crate::hlc::COUNTER_BITS, node: 9 };
        let zone = 11;
        let ring = Ring::from_members(2, 64, [1, 2]);
        let map = RangeMap::from_splits([b"m".to_vec()]);
        let sorted = |node: u32| {
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(512, 4096),
                Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
            );
            let s = VersionedStore::new(engine, node).unwrap().with_gc_grace_ms(60_000);
            let (ring, map) = (ring.clone(), map.clone());
            s.set_placement(std::sync::Arc::new(move |key| {
                map.replicas_for_key_while_rehoming(&ring, zone, key, 2, None)
            }));
            s
        };
        let (zombie, aged) = (&b"m/zombie"[..], &b"m/aged"[..]);
        let owning_shard = (hash64(map.range_token(zombie)) % 2) as usize;
        let mut local = sorted(1);
        local.apply_at(zombie, Some(b"old"), at(now - 600_000), now).unwrap();
        local.apply_at(zombie, None, at(now - 500_000), now).unwrap();
        local.apply_at(b"anchor", Some(b"a"), at(now), now).unwrap();
        local.advance_durable_gc_watermark(now);
        local.compact(now).unwrap();
        assert_eq!(local.get_versioned(zombie).unwrap(), None, "the delete was collected");
        let mut stale_sibling = sorted(2);
        stale_sibling.apply_at(zombie, Some(b"old"), at(now - 600_000), now).unwrap();
        let mut owner_shard = sorted(2);
        owner_shard.apply_at(aged, Some(b"aged"), at(now - 600_000), now).unwrap();
        let local = RefCell::new(local);
        let pull = |peer: &VersionedStore<MemBlockDevice>, peer_index: usize| {
            run(reconcile_scoped_zone_gate(
                &local,
                &LocalPeer(peer),
                DEFAULT_BUCKETS,
                &ring,
                1,
                2,
                zone,
                2,
                None,
                Some(&map),
                (owning_shard, 2),
                (peer_index, 2),
                None,
                None,
            ))
            .unwrap()
        };
        pull(&stale_sibling, 1 - owning_shard);
        assert_eq!(
            local.borrow().get_versioned(zombie).unwrap(),
            None,
            "a peer shard that does not own the key is not trusted past the floor"
        );
        pull(&owner_shard, owning_shard);
        assert_eq!(local.borrow().get(aged).unwrap().as_deref(), Some(&b"aged"[..]));
    }

    #[test]
    fn reconcile_pulls_only_keys_the_peer_also_owns() {
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let zid = 5;
        let mut peer = store(3);
        let keys: Vec<Vec<u8>> = (0..300).map(|i| format!("k{i:03}").into_bytes()).collect();
        for k in &keys {
            peer.apply(k, Some(b"v"), v(100, 3)).unwrap();
        }
        let shared = |owners: Vec<NodeId>| owners.contains(&1) && owners.contains(&3);
        let default = RefCell::new(store(1));
        run(reconcile_scoped(&default, &LocalPeer(&peer), DEFAULT_BUCKETS, &ring, 1, 3, (0, 1), None)).unwrap();
        let zoned = RefCell::new(store(1));
        run(reconcile_scoped_zone(
            &zoned,
            &LocalPeer(&peer),
            DEFAULT_BUCKETS,
            &ring,
            1,
            3,
            zid,
            2,
            None,
            None,
            (0, 1),
            None,
        ))
        .unwrap();
        let (mut pulled, mut skipped) = (0, 0);
        for k in &keys {
            for (store, owners) in [(&default, ring.replicas(k)), (&zoned, ring.replicas_in_zone(zid, k, 2, None))] {
                let both = shared(owners.clone());
                assert_eq!(
                    store.borrow().get(k).unwrap().is_some(),
                    both,
                    "key {k:?} with owners {owners:?}: only keys both peers own are pulled"
                );
                pulled += usize::from(both);
                skipped += usize::from(owners.contains(&1) && !both);
            }
        }
        assert!(pulled > 0 && skipped > 0, "the key set must include shared keys and keys only node 1 owns");
    }

    #[test]
    fn cleanup_page_restart_cap_defers_under_generation_churn() {
        use crate::coord::LocalCluster;
        use std::cell::Cell;
        use std::rc::Rc;

        let mut s = store(9);
        for i in 0..200 {
            s.apply(format!("k{i:04}").as_bytes(), Some(b"v"), v(100 + i, 9))
                .unwrap();
        }
        s.flush().unwrap();
        let local = Rc::new(RefCell::new(s));
        let hook_store = local.clone();
        let bumps = Rc::new(Cell::new(0usize));
        let hook_bumps = bumps.clone();
        CLEANUP_PAGE_REBORROW_HOOK.with(|h| {
            *h.borrow_mut() = Some(Box::new(move || {
                hook_store.borrow_mut().bump_scan_generation_for_test();
                hook_bumps.set(hook_bumps.get() + 1);
            }));
        });
        let ring = Ring::from_members(1, 64, [1]);
        let mut cluster = LocalCluster::new();
        cluster.insert(1, store(1));
        let incoming = b"k0005".to_vec();
        let p = run(cleanup_scoped(
            &local,
            &cluster,
            &ring,
            9,
            1,
            (0, 1),
            Some(&incoming),
            None,
            || true,
            || true,
            || false,
        ))
        .unwrap();
        CLEANUP_PAGE_REBORROW_HOOK.with(|h| *h.borrow_mut() = None);
        assert_eq!(bumps.get(), CLEANUP_MAX_PAGE_RESTARTS);
        assert!(p.deferred && !p.wrapped);
        assert_eq!(p.next_cursor, Some(incoming));
        assert_eq!(p.scanned, 0);
    }

    #[test]
    fn cleanup_batches_run_forgets_until_cycle_wraps() {
        use crate::coord::LocalCluster;

        let ring = Ring::from_members(3, 64, [1, 2, 3, 9]);
        let keys = (0..900).map(|i| format!("batch-{i:06}").into_bytes()).collect::<Vec<_>>();
        let unowned = keys.iter().filter(|key| !ring.replicas(key).contains(&9)).count();
        assert!(unowned > CLEANUP_MAX_HANDOFF_RPCS_PER_PASS / 3);
        assert!(unowned < keys.len());
        let dev = MemBlockDevice::new(4096, 2048);
        let engine = Engine::create_prewiped(
            dev,
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
        );
        let mut source = VersionedStore::new(engine, 9).unwrap();
        for key in &keys {
            source.apply(key, Some(b"v"), v(100, 9)).unwrap();
        }
        source.flush().unwrap();
        let source = RefCell::new(source);
        let mut cluster = LocalCluster::new();
        for node in [1, 2, 3] {
            let dev = MemBlockDevice::new(4096, 1024);
            let engine = Engine::create_prewiped(
                dev,
                Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
            );
            cluster.insert(node, VersionedStore::new(engine, node).unwrap());
        }
        let mut cursor = None;
        let mut passes = 0;
        let gate_acquisitions = std::cell::Cell::new(0);
        let plan = loop {
            passes += 1;
            assert!(passes < 20, "cleanup must reach wrap");
            let progress = run(cleanup_scoped(
                &source, &cluster, &ring, 9, 3, (0, 1), cursor.as_deref(), None,
                || { gate_acquisitions.set(gate_acquisitions.get() + 1); true },
                || true, || false,
            )).unwrap();
            assert_eq!(progress.released, 0);
            assert!(progress.deferred, "waiting batches keep zone dirty");
            if progress.wrapped {
                assert!(progress.next_cursor.is_none());
                break progress.forget_plan.expect("one plan at cycle wrap");
            }
            assert!(progress.forget_plan.is_none(), "no per-pass rewrites");
            assert_eq!(gate_acquisitions.get(), 0, "accumulating below cap must not quiesce reads");
            cursor = progress.next_cursor;
        };
        assert!(passes > 1);
        assert_eq!(gate_acquisitions.get(), 1, "only the due batch acquires the gate");
        for resume in [None, Some(&b"z"[..])] {
            let progress = run(cleanup_scoped(
                &source, &cluster, &ring, 9, 3, (0, 1), resume, None,
                || { gate_acquisitions.set(gate_acquisitions.get() + 1); true },
                || true, || false,
            )).unwrap();
            assert!(progress.deferred && progress.forget_plan.is_none());
            assert_eq!(gate_acquisitions.get(), 1, "in-flight forget must not quiesce reads");
        }
        let wdev = std::sync::Arc::new(source.borrow().device_sibling().unwrap());
        let written = run(vals_store::compact_and_write(plan, wdev)).unwrap();
        let stats = source.borrow_mut().compact_commit(written).unwrap();
        assert_eq!(stats.current_dropped_tombstones.len(), unowned);
        for key in &keys {
            assert_eq!(source.borrow().get(key).unwrap().is_some(), ring.replicas(key).contains(&9));
        }
        let next = run(cleanup_scoped(
            &source, &cluster, &ring, 9, 3, (0, 1), None, None,
            || true, || true, || false,
        )).unwrap();
        assert!(next.wrapped && !next.deferred, "next cycle completes the zone");
        assert_eq!(next.handoff_rpcs, 0);
        assert!(next.forget_plan.is_none());
    }

    #[test]
    fn cleanup_empty_wrap_triggers_batch_behind_gate_and_placement_fence() {
        use crate::coord::LocalCluster;

        let dev = MemBlockDevice::new(4096, 1024);
        let engine = Engine::create_prewiped(
            dev,
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted },
        );
        let mut source = VersionedStore::new(engine, 9).unwrap();
        let version = v(100, 9);
        source.apply(b"a", Some(b"v"), version).unwrap();
        source.flush().unwrap();
        source.accumulate_forget(&[(b"a".to_vec(), version)], false);
        let source = RefCell::new(source);
        let ring = Ring::from_members(1, 64, [1]);
        let mut cluster = LocalCluster::new();
        cluster.insert(1, store(1));
        for (gate, placement) in [(false, true), (true, false), (true, true)] {
            let valid = std::cell::Cell::new(true);
            let progress = run(cleanup_scoped(
                &source, &cluster, &ring, 9, 1, (0, 1), Some(b"z"), None,
                || { valid.set(placement); gate }, || valid.get(), || false,
            )).unwrap();
            assert_eq!(progress.scanned, 0);
            assert_eq!(progress.handoff_rpcs, 0);
            assert!(progress.deferred);
            if gate && placement {
                let plan = progress.forget_plan.expect("empty wrapped pass submits retained batch");
                let wdev = std::sync::Arc::new(source.borrow().device_sibling().unwrap());
                let written = run(vals_store::compact_and_write(plan, wdev)).unwrap();
                source.borrow_mut().compact_commit(written).unwrap();
            } else {
                assert!(progress.forget_plan.is_none());
                assert!(source.borrow().get(b"a").unwrap().is_some());
            }
        }
        assert!(source.borrow().get(b"a").unwrap().is_none());
    }

    #[test]
    fn cleanup_commits_confirmed_release_before_rpc_cap_resume() {
        use crate::coord::LocalCluster;

        let ring = Ring::from_members(1, 64, [1]);
        let count = CLEANUP_MAX_HANDOFF_RPCS_PER_PASS + 1;
        let mut source = store(9);
        for i in 0..count {
            source
                .apply(format!("capped-{i:06}").as_bytes(), Some(b"v"), v(100, 9))
                .unwrap();
        }
        let source = RefCell::new(source);
        let mut cluster = LocalCluster::new();
        cluster.insert(1, store(1));

        let progress = run(cleanup_scoped(
            &source,
            &cluster,
            &ring,
            9,
            1,
            (0, 1),
            None,
            None,
            || true,
            || true,
            || false,
        ))
        .unwrap();
        assert_eq!(progress.handoff_rpcs, CLEANUP_MAX_HANDOFF_RPCS_PER_PASS);
        assert_eq!(progress.released, CLEANUP_MAX_HANDOFF_RPCS_PER_PASS);
        assert_eq!(source.borrow().tracked_keys(), 1);
        assert!(progress.deferred && !progress.wrapped && progress.next_cursor.is_some());
    }

    #[test]
    fn cleanup_releases_keys_every_owner_already_holds_without_apply_rpcs() {
        use crate::coord::LocalCluster;

        let ring = Ring::from_members(1, 64, [1]);
        let count = CLEANUP_MAX_HANDOFF_RPCS_PER_PASS * 3 / 2;
        let roomy = |node: u32| {
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(4096, 256),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
            );
            VersionedStore::new(engine, node).unwrap()
        };
        let mut source = roomy(9);
        let mut owner = roomy(1);
        for i in 0..count {
            let key = format!("held-{i:06}");
            source.apply(key.as_bytes(), Some(b"v"), v(100, 9)).unwrap();
            owner.apply(key.as_bytes(), Some(b"v"), v(100 + (i % 3) as u64, 9)).unwrap();
        }
        source.apply(b"deleted", Some(b"v"), v(100, 9)).unwrap();
        owner.apply(b"deleted", None, v(300, 1)).unwrap();
        source.apply(b"lagging", Some(b"new"), v(200, 9)).unwrap();
        owner.apply(b"lagging", Some(b"old"), v(150, 9)).unwrap();
        source.apply(b"missing", Some(b"m"), v(200, 9)).unwrap();
        let source = RefCell::new(source);
        let mut cluster = LocalCluster::new();
        cluster.insert(1, owner);

        let progress = run(cleanup_scoped(
            &source, &cluster, &ring, 9, 1, (0, 1), None, None,
            || true, || true, || false,
        ))
        .unwrap();
        assert_eq!(progress.probe_released, count + 1, "owner-held keys need no push");
        assert_eq!(progress.handoff_rpcs, 2, "only the lagging and missing keys are pushed");
        assert!(progress.wrapped && !progress.deferred);
        assert_eq!(source.borrow().tracked_keys(), 0);
        let owner = cluster.store(1).unwrap().borrow();
        assert_eq!(owner.get(b"lagging").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(owner.get(b"missing").unwrap().as_deref(), Some(&b"m"[..]));
        assert_eq!(owner.get(b"deleted").unwrap(), None);
        assert_eq!(owner.get(b"held-000002").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn cleanup_owner_probe_cuts_a_grown_window_when_the_peer_slows_down() {
        use crate::coord::{RingView, Transport, TransportError};
        use crate::zone::Catalog;
        use std::cell::Cell;
        use std::time::Duration;

        struct TestClock;
        impl Drop for TestClock {
            fn drop(&mut self) {
                CLEANUP_TEST_ELAPSED.with(|clock| clock.set(None));
            }
        }
        let _clock = TestClock;

        struct SlowsDown {
            fast: usize,
            digests: Cell<usize>,
            stop_at: usize,
            slow_completed: Cell<usize>,
        }
        impl Transport for SlowsDown {
            async fn apply(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
                _value: Option<&[u8]>,
                _version: Version,
            ) -> Result<Applied, TransportError> {
                Ok(Applied::Won)
            }
            async fn read(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                self.digests.set(self.digests.get() + 1);
                if self.digests.get() > self.fast {
                    if self.stop_at == usize::MAX {
                        CLEANUP_TEST_ELAPSED.with(|clock| clock.set(Some(CLEANUP_HANDOFF_WALL_BUDGET)));
                    }
                    vals_rt::yield_now().await;
                    self.slow_completed.set(self.slow_completed.get() + 1);
                }
                Ok(Some(Stored { version: v(100, 9), value: Some(b"v".to_vec()) }))
            }
            async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
                Err(TransportError::Unavailable)
            }
            async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
                Err(TransportError::Unavailable)
            }
        }

        let ring = Ring::from_members(1, 64, [1]);
        let count = 6000;
        let fast = 3 * CLEANUP_PROBE_MAX_KEYS;
        for stop_at in [usize::MAX, fast + 3] {
            CLEANUP_TEST_ELAPSED.with(|clock| clock.set(Some(Duration::ZERO)));
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(4096, 512),
                Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
            );
            let mut source = VersionedStore::new(engine, 9).unwrap();
            for i in 0..count {
                source.apply(format!("grow-{i:06}").as_bytes(), Some(b"v"), v(100, 9)).unwrap();
            }
            let source = RefCell::new(source);
            let peer = SlowsDown { fast, digests: Cell::new(0), stop_at, slow_completed: Cell::new(0) };
            let progress = run(cleanup_scoped(
                &source, &peer, &ring, 9, 1, (0, 1), None, None,
                || true, || true, || peer.digests.get() >= peer.stop_at,
            ))
            .unwrap();
            assert!(peer.digests.get() > fast, "the probe must grow its window before the peer slows");
            if stop_at == usize::MAX {
                assert_eq!(peer.slow_completed.get(), 0, "the deadline cancels the in-flight window");
            } else {
                assert!(peer.digests.get() <= stop_at + 1, "{} digests after a stop at {stop_at}", peer.digests.get());
            }
            assert!(peer.digests.get() < fast + CLEANUP_PROBE_MAX_KEYS / 2, "{} digests", peer.digests.get());
            assert!(progress.deferred && !progress.wrapped);
        }
    }

    #[test]
    fn cleanup_owner_probe_stays_within_the_pass_budget_on_slow_scalar_digests() {
        use crate::coord::{RingView, Transport, TransportError};
        use crate::zone::Catalog;
        use std::cell::Cell;
        use std::time::{Duration, Instant};

        struct SlowDigests {
            delay: Duration,
            digests: Cell<usize>,
            applies: Cell<usize>,
            stop_after: usize,
        }
        impl Transport for SlowDigests {
            async fn apply(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
                _value: Option<&[u8]>,
                _version: Version,
            ) -> Result<Applied, TransportError> {
                self.applies.set(self.applies.get() + 1);
                Ok(Applied::Won)
            }
            async fn read(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                self.digests.set(self.digests.get() + 1);
                std::thread::sleep(self.delay);
                Ok(Some(Stored { version: v(100, 9), value: Some(b"v".to_vec()) }))
            }
            async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
                Err(TransportError::Unavailable)
            }
            async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
                Err(TransportError::Unavailable)
            }
        }

        let ring = Ring::from_members(1, 64, [1]);
        let count = 2000;
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(4096, 256),
            Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        );
        let mut source = VersionedStore::new(engine, 9).unwrap();
        for i in 0..count {
            source.apply(format!("slow-{i:06}").as_bytes(), Some(b"v"), v(100, 9)).unwrap();
        }
        let source = RefCell::new(source);

        for (delay, stop_after) in [(Duration::from_millis(2), usize::MAX), (Duration::from_millis(60), usize::MAX), (Duration::from_millis(100), 1)] {
            let slow = SlowDigests { delay, digests: Cell::new(0), applies: Cell::new(0), stop_after };
            let started = Instant::now();
            let progress = run(cleanup_scoped(
                &source, &slow, &ring, 9, 1, (0, 1), None, None,
                || true, || true, || slow.digests.get() >= slow.stop_after,
            ))
            .unwrap();
            let took = started.elapsed();
            let bound = CLEANUP_HANDOFF_WALL_BUDGET + (CLEANUP_HANDOFF_WALL_BUDGET / 4).max(delay * 2);
            assert!(took < bound + Duration::from_millis(200), "{delay:?} digests: one pass took {took:?}");
            if stop_after == usize::MAX {
                assert!(slow.digests.get() < count / 2, "{delay:?}: probed {} of {count} keys in one pass", slow.digests.get());
                assert_eq!(progress.probe_released, slow.digests.get());
                assert!(progress.probe_released >= CLEANUP_PROBE_MIN_KEYS);
            } else {
                assert_eq!(slow.digests.get(), stop_after, "a stop during the first window ends the probe");
            }
            assert_eq!(slow.applies.get(), 0);
            assert!(progress.deferred && !progress.wrapped);
            assert_eq!(progress.next_cursor.is_some(), stop_after == usize::MAX);
        }
    }

    #[test]
    fn cleanup_end_bound_wraps_at_the_bound_and_leaves_later_keys() {
        use crate::coord::LocalCluster;

        let ring = Ring::from_members(1, 64, [1]);
        let mut source = store(9);
        for i in 0..10 {
            source.apply(format!("k{i}").as_bytes(), Some(b"v"), v(100, 9)).unwrap();
        }
        let source = RefCell::new(source);
        let mut cluster = LocalCluster::new();
        cluster.insert(1, store(1));
        let progress = run(cleanup_scoped(
            &source, &cluster, &ring, 9, 1, (0, 1), Some(b"k2"), Some(b"k6"),
            || true, || true, || false,
        ))
        .unwrap();
        assert!(progress.wrapped && progress.next_cursor.is_none(), "the bound ends the lap");
        assert_eq!((progress.scanned, progress.handoff_rpcs), (4, 4), "k3..=k6 only: the bound is inclusive");
        let held: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("k{i}").into_bytes())
            .filter(|k| source.borrow().get(k).unwrap().is_some())
            .collect();
        let expect: Vec<Vec<u8>> = [0, 1, 2, 7, 8, 9].iter().map(|i| format!("k{i}").into_bytes()).collect();
        assert_eq!(held, expect);
    }

    #[test]
    fn bounded_cleanup_resumes_and_caps_slow_flaky_handoffs() {
        use crate::coord::{RingView, Transport, TransportError};
        use crate::zone::Catalog;
        use std::cell::Cell;

        let ring = Ring::from_members(1, 64, [1]);
        let count = CLEANUP_MAX_HANDOFF_RPCS_PER_PASS + 257;
        let dev = MemBlockDevice::new(4096, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        let mut source = VersionedStore::new(engine, 9).unwrap();
        for i in 0..count {
            let key = format!("large-{i:06}");
            source.apply(key.as_bytes(), Some(b"v"), v(100, 9)).unwrap();
        }
        let source = RefCell::new(source);

        struct Flaky {
            calls: Cell<usize>,
            fail_every: usize,
            delay: std::time::Duration,
        }
        impl Transport for Flaky {
            async fn apply(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
                _value: Option<&[u8]>,
                _version: Version,
            ) -> Result<Applied, TransportError> {
                let n = self.calls.get() + 1;
                self.calls.set(n);
                if !self.delay.is_zero() {
                    std::thread::sleep(self.delay);
                }
                if n.is_multiple_of(self.fail_every) {
                    Err(TransportError::Unavailable)
                } else {
                    Ok(Applied::Won)
                }
            }
            async fn read(
                &self,
                _node: NodeId,
                _zone: ZoneId,
                _key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                Ok(None)
            }
            async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
                Err(TransportError::Unavailable)
            }
            async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
                Err(TransportError::Unavailable)
            }
        }
        let flaky = Flaky {
            calls: Cell::new(0),
            fail_every: 97,
            delay: std::time::Duration::ZERO,
        };
        let mut cursor: Option<Vec<u8>> = None;
        let mut saw_forward_resume = false;
        let mut passes = 0;
        loop {
            let before = cursor.clone();
            let p = run(cleanup_scoped(
                &source,
                &flaky,
                &ring,
                9,
                1,
                (0, 1),
                cursor.as_deref(),
                None,
                || true,
                || true,
                || false,
            ))
            .unwrap();
            passes += 1;
            assert!(p.scanned <= CLEANUP_MAX_KEYS_PER_PASS);
            assert!(p.handoff_rpcs <= CLEANUP_MAX_HANDOFF_RPCS_PER_PASS);
            if before.is_some() && p.next_cursor > before {
                saw_forward_resume = true;
            }
            cursor = p.next_cursor;
            if p.wrapped {
                break;
            }
            assert!(passes < count * 2, "bounded cursor failed to reach wrap");
        }
        assert!(passes > 1, "large set must span bounded passes");
        assert!(saw_forward_resume, "cursor must advance rather than restart");
        assert!(source.borrow().tracked_keys() > 0, "flaky keys keep their last copy");

        let slow = Flaky {
            calls: Cell::new(0),
            fail_every: usize::MAX,
            delay: std::time::Duration::from_millis(140),
        };
        let before_calls = slow.calls.get();
        let started = std::time::Instant::now();
        let slow_p = run(cleanup_scoped(
            &source,
            &slow,
            &ring,
            9,
            1,
            (0, 1),
            None,
            None,
            || true,
            || true,
            || false,
        ))
        .unwrap();
        let calls = slow.calls.get() - before_calls;
        assert!(calls <= 2, "wall budget must stop after the second slow RPC");
        assert!(started.elapsed() < std::time::Duration::from_millis(500));
        assert!(!slow_p.wrapped && slow_p.next_cursor.is_some());

        let reliable = Flaky {
            calls: Cell::new(0),
            fail_every: usize::MAX,
            delay: std::time::Duration::ZERO,
        };
        cursor = None;
        let mut cycle_dirty = false;
        for _ in 0..100 {
            let p = run(cleanup_scoped(
                &source,
                &reliable,
                &ring,
                9,
                1,
                (0, 1),
                cursor.as_deref(),
                None,
                || true,
                || true,
                || false,
            ))
            .unwrap();
            cycle_dirty |= p.deferred;
            cursor = p.next_cursor;
            if p.wrapped {
                if cycle_dirty {
                    cursor = None;
                    cycle_dirty = false;
                    continue;
                }
                break;
            }
        }
        assert!(source.borrow().keys().unwrap().is_empty());
    }

    #[test]
    fn cleanup_releases_unowned_keys_after_handoff() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();

        let mut f = store(3);
        for k in &keys {
            f.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }
        let f = RefCell::new(f);
        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));

        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();

        let unowned = keys
            .iter()
            .filter(|k| !ring.replicas(k).contains(&3))
            .count();
        assert!(unowned > 0 && unowned < keys.len(), "test needs a mix");
        assert_eq!(released.released, unowned, "released exactly the un-owned keys");

        for k in &keys {
            let owns = ring.replicas(k).contains(&3);
            assert_eq!(f.borrow().get(k).unwrap().is_some(), owns);
            if !owns {
                assert!(
                    f.borrow().get_versioned(k).unwrap().is_none(),
                    "released key must be forgotten entirely, not tombstoned"
                );
                for owner in ring.replicas(k) {
                    assert_eq!(
                        lc.store(owner).unwrap().borrow().get(k).unwrap().as_deref(),
                        Some(&b"v"[..]),
                        "owner {owner} got the handoff"
                    );
                }
            }
        }
    }

    #[test]
    fn cleanup_hands_off_wrong_shard_sorted_keys_and_keeps_right_shard_ones() {
        use crate::coord::{RingView, Transport, TransportError};
        use crate::partition::RangeMap;
        use crate::replica::Applied;
        use crate::zone::{Catalog, ZoneId};
        use std::collections::HashSet;

        let ring = Ring::from_members(1, 64, [1]);
        let zid = 5;
        const K: usize = 2;
        let splits: Vec<Vec<u8>> = (1..10)
            .map(|r| format!("k{:03}", r * 20).into_bytes())
            .collect();
        let map = RangeMap::from_splits(splits);

        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:03}").into_bytes()).collect();
        let mut f = store(1);
        for k in &keys {
            f.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }
        let f = RefCell::new(f);

        struct HandoffRec {
            handed: RefCell<HashSet<Vec<u8>>>,
        }
        impl Transport for HandoffRec {
            async fn apply(
                &self,
                _node: NodeId,
                _zone_id: ZoneId,
                key: &[u8],
                _value: Option<&[u8]>,
                _version: Version,
            ) -> Result<Applied, TransportError> {
                self.handed.borrow_mut().insert(key.to_vec());
                Ok(Applied::Won)
            }
            async fn read(
                &self,
                _node: NodeId,
                _zone_id: ZoneId,
                _key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                Ok(None)
            }
            async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
                Err(TransportError::Unavailable)
            }
            async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
                Err(TransportError::Unavailable)
            }
        }
        let t = HandoffRec {
            handed: RefCell::new(HashSet::new()),
        };

        let released = run(cleanup_scoped_zone(
            &f,
            &t,
            &ring,
            1,
            zid,
            1,
            None,
            1,
            Some(&map),
            (0, K),
            None,
            None,
            || true,
            |_| false,
            || true,
            || false,
        ))
        .unwrap();

        let wrong_shard =
            |k: &[u8]| (crate::ring::hash64(map.range_token(k)) % K as u64) as usize != 0;
        let expect_moved = keys.iter().filter(|k| wrong_shard(k)).count();
        assert!(
            expect_moved > 0 && expect_moved < keys.len(),
            "the split must leave a mix of right- and wrong-shard keys"
        );
        assert_eq!(
            released.released, expect_moved,
            "released exactly the wrong-shard keys"
        );

        let handed = t.handed.borrow();
        for k in &keys {
            if wrong_shard(k) {
                assert!(handed.contains(k), "wrong-shard key {k:?} handed off");
                assert!(
                    f.borrow().get_versioned(k).unwrap().is_none(),
                    "wrong-shard key {k:?} forgotten"
                );
            } else {
                assert!(
                    f.borrow().get(k).unwrap().is_some(),
                    "right-shard key {k:?} kept"
                );
                assert!(!handed.contains(k), "right-shard key {k:?} not handed off");
            }
        }
    }

    #[test]
    fn cleanup_defers_the_forget_while_reads_are_in_flight() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..50).map(|i| format!("k{i:04}").into_bytes()).collect();
        let mut f = store(3);
        for k in &keys {
            f.apply(k, Some(b"v"), v(100, 9)).unwrap();
        }
        let f = RefCell::new(f);
        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));

        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || false, || true, || false)).unwrap();
        assert_eq!(released.released, 0, "forget deferred while reads are in flight");
        for k in &keys {
            assert!(f.borrow().get(k).unwrap().is_some(), "key kept locally");
        }
        let unowned: Vec<_> = keys
            .iter()
            .filter(|k| !ring.replicas(k).contains(&3))
            .collect();
        assert!(!unowned.is_empty(), "test needs un-owned keys");
        for k in &unowned {
            for owner in ring.replicas(k) {
                assert!(lc.store(owner).unwrap().borrow().get(k).unwrap().is_some());
            }
        }

        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(released.released, unowned.len(), "deferred keys released next pass");
        for k in &unowned {
            assert!(f.borrow().get_versioned(k).unwrap().is_none());
        }
    }

    #[test]
    fn cleanup_releases_at_a_quorum_threshold_when_an_owner_cannot_confirm() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let key = keys
            .iter()
            .find(|k| !ring.replicas(k).contains(&3) && ring.replicas(k).len() == 2)
            .unwrap()
            .clone();
        let owners = ring.replicas(&key);

        let build = || {
            let mut f = store(3);
            f.apply(&key, Some(b"v"), v(100, 9)).unwrap();
            let f = RefCell::new(f);
            let mut lc = LocalCluster::new();
            lc.insert(1, store(1));
            lc.insert(2, store(2));
            lc.set_down(owners[0], true);
            (f, lc)
        };

        let (f, lc) = build();
        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(
            released.released, 0,
            "all-owners threshold keeps a key an owner can't ack"
        );
        assert!(f.borrow().get(&key).unwrap().is_some());

        let (f, lc) = build();
        let released = run(cleanup_scoped(&f, &lc, &ring, 3, 1, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(
            released.released, 1,
            "quorum threshold releases on one owner confirm"
        );
        assert!(
            f.borrow().get_versioned(&key).unwrap().is_none(),
            "released key is forgotten entirely"
        );
        assert!(lc
            .store(owners[1])
            .unwrap()
            .borrow()
            .get(&key)
            .unwrap()
            .is_some());
    }

    #[test]
    fn rehome_copy_survives_sweeps_and_cutover() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let zone_id = 9;
        let mut map = RangeMap::single();
        assert!(map.split(b"m"));
        assert!(map.set_pending(1, b"m".to_vec()));
        let key = b"pending-copy";
        let current = map.replicas_for_key(&ring, zone_id, key, 1, None)[0];
        let pending = ring.replicas_in_zone(zone_id, b"m", 1, None)[0];
        assert_ne!(current, pending);
        let shards = 4usize;
        let pending_shard = (crate::ring::hash64(b"m") % shards as u64) as usize;
        let mut copied = store(pending);
        copied.apply(key, Some(b"v"), v(100, current)).unwrap();
        let copied = RefCell::new(copied);
        let mut transport = LocalCluster::new();
        transport.insert(current, store(current));

        let progress = run(cleanup_scoped_zone(
            &copied,
            &transport,
            &ring,
            pending,
            zone_id,
            1,
            None,
            1,
            Some(&map),
            (pending_shard, shards),
            None,
            None,
            || true,
            |_| false,
            || true,
            || false,
        )).unwrap();
        assert_eq!(progress.released, 0);
        assert_eq!(copied.borrow().get(key).unwrap().as_deref(), Some(&b"v"[..]));
        for _ in 0..3 {
            let progress = run(cleanup_scoped_zone(
                &copied,
                &transport,
                &ring,
                pending,
                zone_id,
                1,
                None,
                1,
                Some(&map),
                (pending_shard, shards),
                None,
                None,
                || true,
                |_| false,
                || true,
                || false,
            )).unwrap();
            assert_eq!(progress.released, 0);
        }
        assert!(map.commit_pending(1));
        let progress = run(cleanup_scoped_zone(
            &copied,
            &transport,
            &ring,
            pending,
            zone_id,
            1,
            None,
            1,
            Some(&map),
            (pending_shard, shards),
            None,
            None,
            || true,
            |_| false,
            || true,
            || false,
        )).unwrap();
        assert_eq!(progress.released, 0);
        assert_eq!(copied.borrow().get(key).unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn cleanup_keeps_key_when_an_owner_cannot_confirm() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let key = keys
            .iter()
            .find(|k| !ring.replicas(k).contains(&3))
            .unwrap()
            .clone();
        let owners = ring.replicas(&key);

        let mut f = store(3);
        f.apply(&key, Some(b"v"), v(100, 9)).unwrap();
        let f = RefCell::new(f);
        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));
        lc.set_down(owners[0], true);

        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(released.released, 0, "must not drop when an owner can't confirm");
        assert!(
            f.borrow().get(&key).unwrap().is_some(),
            "the key is retained for a later pass"
        );
    }

    #[test]
    fn sorted_cleanup_moves_past_a_key_no_owner_confirms() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let stuck = keys.iter().find(|k| ring.replicas(k) == vec![1]).unwrap().clone();
        let later = keys.iter().find(|k| **k > stuck && ring.replicas(k) == vec![2]).unwrap().clone();
        let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted };
        let mut f = VersionedStore::new(Engine::create_prewiped(MemBlockDevice::new(256, 256), cfg), 3).unwrap();
        f.apply(&stuck, Some(b"v"), v(100, 9)).unwrap();
        f.apply(&later, Some(b"v"), v(100, 9)).unwrap();
        let f = RefCell::new(f);
        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));
        lc.set_down(1, true);
        let pass = |resume: Option<&[u8]>| {
            run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), resume, None, || true, || true, || false))
                .unwrap()
        };
        let first = pass(None);
        assert_eq!((first.kept, first.kept_samples.clone()), (1, vec![stuck.clone()]));
        let second = pass(first.next_cursor.as_deref());
        assert_eq!(second.kept, 0, "the next pass starts after the key it could not hand off");
        assert_eq!(lc.store(2).unwrap().borrow().get(&later).unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn memtable_cleanup_is_fenced_when_placement_changes_during_handoff() {
        use crate::coord::{LocalCluster, RingView};
        use crate::zone::Catalog;
        use std::cell::Cell;
        use std::rc::Rc;

        let ring = Ring::from_members(1, 64, [1]);
        let key = b"placement-race".to_vec();
        let mut source = store(9);
        source.apply(&key, Some(b"only-copy"), v(100, 9)).unwrap();
        let source = Rc::new(RefCell::new(source));
        let valid = Rc::new(Cell::new(true));

        struct FlipPlacement {
            inner: LocalCluster<MemBlockDevice>,
            valid: Rc<Cell<bool>>,
        }
        impl Transport for FlipPlacement {
            async fn apply(
                &self,
                node: NodeId,
                zone: ZoneId,
                key: &[u8],
                value: Option<&[u8]>,
                version: Version,
            ) -> Result<Applied, TransportError> {
                let result = self.inner.apply(node, zone, key, value, version).await;
                self.valid.set(false);
                result
            }
            async fn read(
                &self,
                node: NodeId,
                zone: ZoneId,
                key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                self.inner.read(node, zone, key).await
            }
            async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
                self.inner.members(node).await
            }
            async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
                self.inner.zones(node).await
            }
        }
        let mut inner = LocalCluster::new();
        inner.insert(1, store(1));
        let transport = FlipPlacement { inner, valid: valid.clone() };
        let progress = run(cleanup_scoped(
            &source,
            &transport,
            &ring,
            9,
            1,
            (0, 1),
            None,
            None,
            || true,
            || valid.get(),
            || false,
        ))
        .unwrap();
        assert_eq!(progress.released, 0);
        assert!(progress.deferred && !progress.wrapped);
        assert_eq!(
            source.borrow().get(&key).unwrap().as_deref(),
            Some(&b"only-copy"[..]),
            "old placement must not forget the newly-rightful/last local copy"
        );
    }

    #[test]
    fn cleanup_keeps_a_key_a_newer_write_lands_on_during_handoff() {
        use crate::coord::{LocalCluster, RingView, Transport, TransportError};
        use crate::replica::Applied;
        use crate::zone::{Catalog, ZoneId};
        use std::cell::Cell;
        use std::rc::Rc;

        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let key = keys
            .iter()
            .find(|k| !ring.replicas(k).contains(&3))
            .unwrap()
            .clone();
        let owners = ring.replicas(&key);

        let mut f = store(3);
        f.apply(&key, Some(b"old"), v(100, 9)).unwrap();
        let local = Rc::new(RefCell::new(f));

        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));

        struct Injecting {
            inner: LocalCluster<MemBlockDevice>,
            local: Rc<RefCell<VersionedStore<MemBlockDevice>>>,
            key: Vec<u8>,
            newer: Version,
            injected: Cell<bool>,
        }
        impl Transport for Injecting {
            async fn apply(
                &self,
                node: NodeId,
                zone_id: ZoneId,
                key: &[u8],
                value: Option<&[u8]>,
                version: Version,
            ) -> Result<Applied, TransportError> {
                if !self.injected.replace(true) {
                    self.local
                        .borrow_mut()
                        .apply(&self.key, Some(b"new"), self.newer)
                        .unwrap();
                }
                self.inner.apply(node, zone_id, key, value, version).await
            }
            async fn read(
                &self,
                node: NodeId,
                zone_id: ZoneId,
                key: &[u8],
            ) -> Result<Option<Stored>, TransportError> {
                self.inner.read(node, zone_id, key).await
            }
            async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
                self.inner.members(node).await
            }
            async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
                self.inner.zones(node).await
            }
        }

        let t = Injecting {
            inner: lc,
            local: local.clone(),
            key: key.clone(),
            newer: v(200, 9),
            injected: Cell::new(false),
        };

        let released = run(cleanup_scoped(&local, &t, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(
            released.released, 0,
            "a key that changed during handoff must be kept"
        );

        assert_eq!(
            local.borrow().get(&key).unwrap().as_deref(),
            Some(&b"new"[..]),
            "newer write must not be dropped by forget"
        );
        for owner in &owners {
            assert_eq!(
                t.inner
                    .store(*owner)
                    .unwrap()
                    .borrow()
                    .get(&key)
                    .unwrap()
                    .as_deref(),
                Some(&b"old"[..]),
                "owner {owner} got the in-flight handoff"
            );
        }
    }

    #[test]
    fn cleanup_hands_off_tombstones_too() {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(2, 64, [1, 2, 3]);
        let keys: Vec<Vec<u8>> = (0..200).map(|i| format!("k{i:04}").into_bytes()).collect();
        let key = keys
            .iter()
            .find(|k| !ring.replicas(k).contains(&3))
            .unwrap()
            .clone();

        let mut f = store(3);
        f.apply(&key, None, v(100, 9)).unwrap();
        let f = RefCell::new(f);
        let mut lc = LocalCluster::new();
        lc.insert(1, store(1));
        lc.insert(2, store(2));

        let released = run(cleanup_scoped(&f, &lc, &ring, 3, usize::MAX, (0, 1), None, None, || true, || true, || false)).unwrap();
        assert_eq!(released.released, 1);
        assert!(f.borrow().get_versioned(&key).unwrap().is_none());
        for owner in ring.replicas(&key) {
            let s = lc.store(owner).unwrap().borrow();
            let st = s
                .get_versioned(&key)
                .unwrap()
                .expect("owner has the tombstone");
            assert!(st.value.is_none(), "owner holds a tombstone, not a value");
        }
    }

    #[test]
    fn version_leaf_digests_match_the_entries_based_leaves() {
        let mut s = store(1);
        for i in 0..120u32 {
            s.apply(
                format!("k{i:03}").as_bytes(),
                Some(b"v"),
                v(100 + i as u64, 1),
            )
            .unwrap();
        }
        s.delete_at(b"k010", 9999).unwrap();
        let from_index = s.version_leaf_digests(DEFAULT_BUCKETS).unwrap();
        let from_entries = leaf_digests(&s.entries().unwrap(), DEFAULT_BUCKETS);
        assert_eq!(from_index, from_entries);
        assert_eq!(root(&from_index), root(&from_entries));
    }

    #[test]
    fn bucket_entries_returns_exactly_the_wanted_buckets() {
        let mut s = store(1);
        for i in 0..200u32 {
            s.apply(format!("k{i:03}").as_bytes(), Some(b"v"), v(100, 1))
                .unwrap();
        }
        let ids = [3u32, 7, 42];
        let mut got = s.bucket_entries(DEFAULT_BUCKETS, &ids).unwrap();
        let want: HashSet<u32> = ids.iter().copied().collect();
        let mut expected: Vec<(Vec<u8>, Stored)> = s
            .entries()
            .unwrap()
            .into_iter()
            .filter(|(k, _)| want.contains(&bucket_of(k, DEFAULT_BUCKETS)))
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        expected.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got, expected);
        assert!(got
            .iter()
            .all(|(k, _)| want.contains(&bucket_of(k, DEFAULT_BUCKETS))));

        for nb in [16u32, 100] {
            let ids = [0u32, 1, 5];
            let want: HashSet<u32> = ids.iter().copied().collect();
            let mut got = s.bucket_entries(nb, &ids).unwrap();
            let mut expected: Vec<(Vec<u8>, Stored)> = s
                .entries()
                .unwrap()
                .into_iter()
                .filter(|(k, _)| want.contains(&bucket_of(k, nb)))
                .collect();
            got.sort_by(|a, b| a.0.cmp(&b.0));
            expected.sort_by(|a, b| a.0.cmp(&b.0));
            assert_eq!(got, expected, "bucket_entries wrong at num_buckets={nb}");
        }
    }

    #[test]
    fn identical_stores_need_no_transfer() {
        let a = RefCell::new(store(1));
        let mut b = store(2);
        for i in 0..50u32 {
            let key = format!("k{i}");
            let ver = v(100 + i as u64, 1);
            a.borrow_mut()
                .apply(key.as_bytes(), Some(b"x"), ver)
                .unwrap();
            b.apply(key.as_bytes(), Some(b"x"), ver).unwrap();
        }
        let ea = a.borrow().entries().unwrap();
        let eb = b.entries().unwrap();
        assert_eq!(
            root(&leaf_digests(&ea, DEFAULT_BUCKETS)),
            root(&leaf_digests(&eb, DEFAULT_BUCKETS))
        );
        let applied = run(reconcile(&a, &LocalPeer(&b), DEFAULT_BUCKETS)).unwrap();
        assert_eq!(applied, 0);
    }

    #[test]
    fn reconcile_defers_local_apply_while_over_ram_budget() {
        let a = RefCell::new(store(1));
        let mut b = store(2);
        b.apply(b"only_b", Some(b"v"), v(150, 2)).unwrap();

        let override_guard = crate::ram::override_for_test(2, 1);
        let result = run(reconcile(&a, &LocalPeer(&b), DEFAULT_BUCKETS));
        assert!(result.is_err());
        assert_eq!(a.borrow().get(b"only_b").unwrap(), None);

        drop(override_guard);
        assert_eq!(
            run(reconcile(&a, &LocalPeer(&b), DEFAULT_BUCKETS)).unwrap(),
            1
        );
        assert_eq!(a.borrow().get(b"only_b").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn reconcile_pulls_missing_and_newer_entries() {
        let a = RefCell::new(store(1));
        let mut b = store(2);

        a.borrow_mut()
            .apply(b"shared", Some(b"old"), v(100, 1))
            .unwrap();
        b.apply(b"shared", Some(b"new"), v(200, 1)).unwrap();
        b.apply(b"only_b", Some(b"v"), v(150, 2)).unwrap();
        a.borrow_mut()
            .apply(b"only_a", Some(b"v"), v(120, 1))
            .unwrap();

        let applied = run(reconcile(&a, &LocalPeer(&b), DEFAULT_BUCKETS)).unwrap();
        assert_eq!(applied, 2, "pulled the newer 'shared' and 'only_b'");

        let a = a.borrow();
        assert_eq!(a.get(b"shared").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(a.get(b"only_b").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(a.get(b"only_a").unwrap().as_deref(), Some(&b"v"[..]));
    }

    struct PrefetchedValues<'a> {
        digests: LocalPeer<'a, MemBlockDevice>,
        values: std::collections::HashMap<Vec<u8>, Stored>,
    }

    impl AePeer for PrefetchedValues<'_> {
        async fn tree(
            &self,
            zone_id: ZoneId,
            level: u32,
            scope_g: u32,
            scope: &[u32],
            requester: Option<NodeId>,
        ) -> Result<Vec<(u32, u64)>, TransportError> {
            self.digests.tree(zone_id, level, scope_g, scope, requester).await
        }
        async fn bucket_digests(
            &self,
            zone_id: ZoneId,
            num_buckets: u32,
            ids: &[u32],
            after: Option<&[u8]>,
        ) -> Result<crate::replica::DigestCursorPage, TransportError> {
            self.digests.bucket_digests(zone_id, num_buckets, ids, after).await
        }
        async fn values(
            &self,
            _zone_id: ZoneId,
            keys: &[Vec<u8>],
        ) -> Result<crate::replica::EntryPage, TransportError> {
            let found = keys.iter().filter_map(|key| Some((key.clone(), self.values.get(key)?.clone())));
            Ok((found.collect(), false))
        }
    }

    fn stale_flushed_copy(keys: usize) -> (RefCell<VersionedStore<MemBlockDevice>>, VersionedStore<MemBlockDevice>) {
        let big = |node| {
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(4096, 2048),
                Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Unsorted },
            );
            VersionedStore::new(engine, node).unwrap()
        };
        let a = RefCell::new(big(1));
        let mut b = big(2);
        for i in 0..keys {
            let key = format!("k{i:05}").into_bytes();
            a.borrow_mut().apply(&key, Some(b"old"), v(100, 1)).unwrap();
            b.apply(&key, Some(b"new"), v(200, 2)).unwrap();
        }
        a.borrow_mut().flush().unwrap();
        (a, b)
    }

    fn newer_values(keys: impl Iterator<Item = usize>) -> std::collections::HashMap<Vec<u8>, Stored> {
        keys.map(|i| (format!("k{i:05}").into_bytes(), Stored { version: v(200, 2), value: Some(b"new".to_vec()) }))
            .collect()
    }

    fn local_probes() -> u64 {
        vals_store::attribution::lookup_snapshot()[2][0]
    }

    #[test]
    fn reconcile_yields_between_local_probes_and_applies_of_one_digest_page() {
        let keys = 4 * AE_RECONCILE_YIELD_EVERY + 7;
        let (a, b) = stale_flushed_copy(keys);
        let peer = PrefetchedValues { digests: LocalPeer(&b), values: newer_values(0..keys) };
        let local_ops = || (local_probes(), a.borrow().mutable_generation());
        let start = local_ops();
        let last = std::cell::Cell::new(start);
        let longest = std::cell::Cell::new(0u64);
        let note = || {
            let now = local_ops();
            let (p, w) = last.replace(now);
            longest.set(longest.get().max((now.0 - p) + (now.1 - w)));
        };
        let ticker = async {
            loop {
                note();
                vals_rt::yield_now().await;
            }
        };
        let vals_rt::Either::Left(applied) = run(vals_rt::race(reconcile(&a, &peer, DEFAULT_BUCKETS), ticker)) else {
            unreachable!("the ticker never finishes")
        };
        note();
        assert_eq!(applied.unwrap(), keys);
        let end = last.get();
        assert_eq!((end.0 - start.0, end.1 - start.1), (keys as u64, keys as u64), "one local probe and one write per key");
        assert!(
            longest.get() <= AE_RECONCILE_YIELD_EVERY as u64,
            "{} local probes and applies ran without yielding to other tasks",
            longest.get()
        );
        assert_eq!(a.borrow().get(b"k00000").unwrap().as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn value_budgeted_reconcile_probes_no_further_than_it_can_fetch() {
        let keys = 200;
        let budget = 16;
        let (a, b) = stale_flushed_copy(keys);
        let peer = PrefetchedValues { digests: LocalPeer(&b), values: newer_values((0..keys).step_by(2)) };
        let (mut resume, mut passes) = (None::<Vec<u8>>, 0);
        loop {
            let before = local_probes();
            let (applied, next) = run(reconcile_with(
                &a,
                &peer,
                DEFAULT_ZONE,
                DEFAULT_BUCKETS,
                usize::MAX,
                usize::MAX,
                budget,
                resume.as_deref(),
                None,
                None,
                None,
                |_| Some(crate::replica::LiveFloor::Waive),
            ))
            .unwrap();
            let probed = local_probes() - before;
            assert!(probed <= budget as u64, "pass {passes} probed {probed} keys for a {budget}-value budget");
            assert!(applied <= budget / 2);
            passes += 1;
            assert!(passes < 100, "budgeted reconcile failed to converge");
            match next {
                Some(c) => resume = Some(c),
                None => break,
            }
        }
        assert!(passes >= keys / budget, "{passes} passes");
        let a = a.borrow();
        for i in 0..keys {
            let want: &[u8] = if i % 2 == 0 { b"new" } else { b"old" };
            assert_eq!(a.get(format!("k{i:05}").as_bytes()).unwrap().as_deref(), Some(want), "k{i:05}");
        }
    }

    struct AwaitMutationPeer<'a> {
        local: &'a RefCell<VersionedStore<MemBlockDevice>>,
        remote: &'a VersionedStore<MemBlockDevice>,
        tree_calls: std::cell::Cell<usize>,
        mutating_calls: usize,
    }

    impl AePeer for AwaitMutationPeer<'_> {
        async fn tree(
            &self,
            _zone_id: ZoneId,
            level: u32,
            scope_g: u32,
            scope: &[u32],
            requester: Option<NodeId>,
        ) -> Result<Vec<(u32, u64)>, TransportError> {
            let calls = self.tree_calls.get();
            self.tree_calls.set(calls + 1);
            if calls < self.mutating_calls {
                vals_rt::yield_now().await;
                self.local
                    .borrow_mut()
                    .append_unknown_for_test(b"await-race", Some(b"new"), v(200 + calls as u64, 1))
                    .unwrap();
            }
            let scope: HashSet<u32> = scope.iter().copied().collect();
            self.remote
                .scoped_leaf_digests(level, scope_g, &scope, requester)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }

        async fn bucket_digests(
            &self,
            _zone_id: ZoneId,
            num_buckets: u32,
            ids: &[u32],
            after: Option<&[u8]>,
        ) -> Result<crate::replica::DigestCursorPage, TransportError> {
            self.remote
                .bucket_digest_page(num_buckets, ids, after, usize::MAX)
                .map(digest_cursor_page)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }

        async fn values(
            &self,
            _zone_id: ZoneId,
            keys: &[Vec<u8>],
        ) -> Result<crate::replica::EntryPage, TransportError> {
            self.remote
                .values_page(keys, usize::MAX)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }
    }

    #[test]
    fn tree_descent_restarts_when_unknown_old_append_lands_during_peer_await() {
        let mut local = store(1);
        let mut remote = store(2);
        local
            .apply(b"await-race", Some(b"old"), v(100, 7))
            .unwrap();
        remote
            .apply(b"await-race", Some(b"old"), v(100, 7))
            .unwrap();
        let local = RefCell::new(local);
        let peer = AwaitMutationPeer {
            local: &local,
            remote: &remote,
            tree_calls: std::cell::Cell::new(0),
            mutating_calls: 1,
        };

        let applied = run(reconcile(&local, &peer, DEFAULT_BUCKETS)).unwrap();
        assert_eq!(applied, 0);
        assert!(
            peer.tree_calls.get() >= 2,
            "tree level must restart instead of comparing the pre-append local digest"
        );
        assert_eq!(
            local.borrow().get(b"await-race").unwrap().as_deref(),
            Some(&b"new"[..])
        );
        assert!(local
            .borrow()
            .version_leaf_digests(DEFAULT_BUCKETS)
            .is_ok());
    }

    #[test]
    fn tree_descent_counts_token_exhaustion_when_every_peer_await_sees_a_write() {
        let mut local = store(1);
        let mut remote = store(2);
        local
            .apply(b"await-race", Some(b"old"), v(100, 7))
            .unwrap();
        remote
            .apply(b"await-race", Some(b"old"), v(100, 7))
            .unwrap();
        let local = RefCell::new(local);
        let peer = AwaitMutationPeer {
            local: &local,
            remote: &remote,
            tree_calls: std::cell::Cell::new(0),
            mutating_calls: usize::MAX,
        };
        let counters = crate::observe::counters();
        let exhaustions = counters.ae_tree_token_exhaustions.get();
        let reconciled = run(reconcile(&local, &peer, DEFAULT_BUCKETS));
        assert!(matches!(reconciled, Err(TransportError::Busy)), "{reconciled:?}");
        assert_eq!(peer.tree_calls.get(), 3);
        assert!(counters.ae_tree_token_exhaustions.get() > exhaustions);
    }

    struct CountingPeer<'a, D: BlockDevice> {
        store: &'a VersionedStore<D>,
        value_calls: std::cell::Cell<usize>,
        value_entries: std::cell::Cell<usize>,
        digest_entries: std::cell::Cell<usize>,
    }

    impl<'a, D: BlockDevice> CountingPeer<'a, D> {
        fn new(store: &'a VersionedStore<D>) -> Self {
            Self {
                store,
                value_calls: std::cell::Cell::new(0),
                value_entries: std::cell::Cell::new(0),
                digest_entries: std::cell::Cell::new(0),
            }
        }
    }

    impl<D: BlockDevice> AePeer for CountingPeer<'_, D> {
        async fn tree(
            &self,
            _z: ZoneId,
            level: u32,
            scope_g: u32,
            scope: &[u32],
            requester: Option<NodeId>,
        ) -> Result<Vec<(u32, u64)>, TransportError> {
            let scope: HashSet<u32> = scope.iter().copied().collect();
            self.store
                .scoped_leaf_digests(level, scope_g, &scope, requester)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }
        async fn bucket_digests(
            &self,
            _z: ZoneId,
            nb: u32,
            ids: &[u32],
            after: Option<&[u8]>,
        ) -> Result<crate::replica::DigestCursorPage, TransportError> {
            let page = self
                .store
                .bucket_digest_page(nb, ids, after, usize::MAX)
                .map_err(|e| TransportError::Engine(e.to_string()))?;
            self.digest_entries
                .set(self.digest_entries.get() + page.0.len());
            Ok(digest_cursor_page(page))
        }
        async fn values(
            &self,
            _z: ZoneId,
            keys: &[Vec<u8>],
        ) -> Result<crate::replica::EntryPage, TransportError> {
            self.value_calls.set(self.value_calls.get() + 1);
            let page = self
                .store
                .values_page(keys, usize::MAX)
                .map_err(|e| TransportError::Engine(e.to_string()))?;
            self.value_entries
                .set(self.value_entries.get() + page.0.len());
            Ok(page)
        }
    }

    #[test]
    fn reconcile_reads_values_only_for_divergent_keys() {
        let mut a = store(1);
        let mut b = store(2);
        for i in 0..100u32 {
            let key = format!("k{i:03}").into_bytes();
            let ver = v(100 + i as u64, 7);
            a.apply(&key, Some(b"v"), ver).unwrap();
            b.apply(&key, Some(b"v"), ver).unwrap();
        }
        b.apply(b"k042", Some(b"new"), v(9_999, 7)).unwrap();

        let a = RefCell::new(a);
        let peer = CountingPeer::new(&b);
        let (applied, cursor) = run(reconcile_with(
            &a,
            &peer,
            DEFAULT_ZONE,
            1,
            usize::MAX,
            usize::MAX,
            usize::MAX,
            None,
            None,
            None,
            None,
            |_| Some(crate::replica::LiveFloor::Waive),
        ))
        .unwrap();
        assert_eq!(applied, 1, "only the one divergent key applied");
        assert!(cursor.is_none(), "caught up in one pass");
        assert_eq!(peer.value_calls.get(), 1, "exactly one phase-2 value fetch");
        assert_eq!(
            peer.value_entries.get(),
            1,
            "read back only the divergent key's value, not the 100 matching ones"
        );
        assert_eq!(
            a.borrow().get(b"k042").unwrap().as_deref(),
            Some(&b"new"[..])
        );

        let (applied2, _) = run(reconcile_with(
            &a,
            &peer,
            DEFAULT_ZONE,
            1,
            usize::MAX,
            usize::MAX,
            usize::MAX,
            None,
            None,
            None,
            None,
            |_| Some(crate::replica::LiveFloor::Waive),
        ))
        .unwrap();
        assert_eq!(applied2, 0);
        assert_eq!(
            peer.value_calls.get(),
            1,
            "a converged keyspace reads no values at all"
        );
    }

    #[test]
    fn tree_levels_is_a_fanout_ladder_ending_at_the_leaf_count() {
        assert_eq!(tree_levels(DEFAULT_BUCKETS), vec![16, 256, 4096]);
        assert_eq!(tree_levels(256), vec![16, 256]);
        assert_eq!(tree_levels(16), vec![16]);
        assert_eq!(tree_levels(8), vec![8]);
        assert_eq!(tree_levels(1), vec![1]);
    }

    #[test]
    fn hierarchical_descent_localizes_a_sparse_diff() {
        const N: u32 = 4_000;
        let big = || {
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(8192, 1024),
                Config {
                    seg_blocks: 64,
                    reserved_blocks: 0,
                    ordering: Ordering::Unsorted,
                },
            );
            VersionedStore::new(engine, 1).unwrap()
        };
        let mut a = big();
        let mut b = big();
        for i in 0..N {
            let key = format!("key{i:06}").into_bytes();
            let ver = v(100 + i as u64, 7);
            a.apply(&key, Some(b"v"), ver).unwrap();
            b.apply(&key, Some(b"v"), ver).unwrap();
        }
        b.apply(b"key004242", Some(b"new"), v(9_999_999, 7))
            .unwrap();

        let a = RefCell::new(a);
        let peer = CountingPeer::new(&b);
        let (applied, cursor) = run(reconcile_with(
            &a,
            &peer,
            DEFAULT_ZONE,
            DEFAULT_BUCKETS,
            usize::MAX,
            usize::MAX,
            usize::MAX,
            None,
            None,
            None,
            None,
            |_| Some(crate::replica::LiveFloor::Waive),
        ))
        .unwrap();
        assert_eq!(applied, 1, "only the one divergent key applied");
        assert!(cursor.is_none(), "caught up in one pass");
        assert_eq!(
            peer.value_entries.get(),
            1,
            "read back only the divergent key"
        );
        assert!(
            peer.digest_entries.get() < 64,
            "sparse diff should examine only the differing leaf's keys, examined {}",
            peer.digest_entries.get()
        );
    }

    struct PagedPeer<'a, D: BlockDevice>(&'a VersionedStore<D>, usize);

    impl<D: BlockDevice> AePeer for PagedPeer<'_, D> {
        async fn tree(
            &self,
            _zone_id: ZoneId,
            level: u32,
            scope_g: u32,
            scope: &[u32],
            requester: Option<NodeId>,
        ) -> Result<Vec<(u32, u64)>, TransportError> {
            let scope: HashSet<u32> = scope.iter().copied().collect();
            self.0
                .scoped_leaf_digests(level, scope_g, &scope, requester)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }

        async fn bucket_digests(
            &self,
            _zone_id: ZoneId,
            num_buckets: u32,
            ids: &[u32],
            after: Option<&[u8]>,
        ) -> Result<crate::replica::DigestCursorPage, TransportError> {
            self.0
                .bucket_digest_page(num_buckets, ids, after, self.1)
                .map(digest_cursor_page)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }

        async fn values(
            &self,
            _zone_id: ZoneId,
            keys: &[Vec<u8>],
        ) -> Result<crate::replica::EntryPage, TransportError> {
            self.0
                .values_page(keys, self.1)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }
    }

    #[test]
    fn reconcile_pages_through_a_budgeted_peer() {
        let a = RefCell::new(store(1));
        let mut b = store(2);
        let mut want = Vec::new();
        for i in 0..40u32 {
            let key = format!("k{i:02}").into_bytes();
            let val = format!("v{i}").into_bytes();
            b.apply(&key, Some(&val), v(100 + i as u64, 2)).unwrap();
            want.push((key, val));
        }
        b.apply(b"dead", None, v(500, 2)).unwrap();

        let applied = run(reconcile(&a, &PagedPeer(&b, 1), DEFAULT_BUCKETS)).unwrap();
        assert_eq!(applied, 41, "every entry (incl. the tombstone) pulled");
        let a = a.borrow();
        for (key, val) in &want {
            assert_eq!(a.get(key).unwrap().as_deref(), Some(&val[..]));
        }
        assert!(a.get(b"dead").unwrap().is_none());
    }

    #[test]
    fn bounded_reconcile_stops_per_pass_and_resumes_to_convergence() {
        let a = RefCell::new(store(1));
        let mut b = store(2);
        let total = 40usize;
        for i in 0..total as u32 {
            let key = format!("k{i:02}").into_bytes();
            b.apply(&key, Some(b"v"), v(100 + i as u64, 2)).unwrap();
        }
        let peer = PagedPeer(&b, 1);
        let budget = 5;

        let mut passes = 0;
        let mut resume: Option<Vec<u8>> = None;
        loop {
            let (_, next) = run(reconcile_with(
                &a,
                &peer,
                DEFAULT_ZONE,
                DEFAULT_BUCKETS,
                budget,
                usize::MAX,
                usize::MAX,
                resume.as_deref(),
                None,
                None,
                None,
                |_| Some(crate::replica::LiveFloor::Waive),
            ))
            .unwrap();
            assert!(
                a.borrow().keys().unwrap().len() <= passes * budget + budget,
                "pass {passes} pulled past the budget"
            );
            passes += 1;
            match next {
                Some(c) => resume = Some(c),
                None => break,
            }
            assert!(passes < 100, "bounded reconcile failed to converge");
        }
        assert!(
            passes > total / budget - 1,
            "expected multiple bounded passes"
        );
        let a = a.borrow();
        for i in 0..total as u32 {
            let key = format!("k{i:02}").into_bytes();
            assert_eq!(a.get(&key).unwrap().as_deref(), Some(&b"v"[..]), "{key:?}");
        }
    }

    struct ResumePeer<'a, D: BlockDevice> {
        store: &'a VersionedStore<D>,
        calls: std::cell::Cell<usize>,
        empty: std::cell::Cell<usize>,
    }

    impl<D: BlockDevice> AePeer for ResumePeer<'_, D> {
        async fn tree(
            &self,
            zone_id: ZoneId,
            level: u32,
            scope_g: u32,
            scope: &[u32],
            requester: Option<NodeId>,
        ) -> Result<Vec<(u32, u64)>, TransportError> {
            PagedPeer(self.store, usize::MAX)
                .tree(zone_id, level, scope_g, scope, requester)
                .await
        }

        async fn bucket_digests(
            &self,
            _zone_id: ZoneId,
            num_buckets: u32,
            ids: &[u32],
            after: Option<&[u8]>,
        ) -> Result<crate::replica::DigestCursorPage, TransportError> {
            let call = self.calls.get();
            self.calls.set(call + 1);
            if call.is_multiple_of(2) {
                let mut resume = after.map_or_else(Vec::new, <[u8]>::to_vec);
                resume.push(0);
                self.empty.set(self.empty.get() + 1);
                return Ok((Vec::new(), Some(resume)));
            }
            self.store
                .bucket_digest_page(num_buckets, ids, after, 64)
                .map(digest_cursor_page)
                .map_err(|e| TransportError::Engine(e.to_string()))
        }

        async fn values(
            &self,
            zone_id: ZoneId,
            keys: &[Vec<u8>],
        ) -> Result<crate::replica::EntryPage, TransportError> {
            PagedPeer(self.store, usize::MAX).values(zone_id, keys).await
        }
    }

    #[test]
    fn reconcile_follows_resume_only_digest_pages_and_caps_pages_per_pass() {
        let mut b = store(2);
        for i in 0..40u32 {
            let key = format!("k{i:02}").into_bytes();
            b.apply(&key, Some(b"v"), v(100 + i as u64, 2)).unwrap();
        }
        let converged = |a: &RefCell<VersionedStore<MemBlockDevice>>| {
            (0..40u32).all(|i| {
                a.borrow().get(format!("k{i:02}").as_bytes()).unwrap().as_deref() == Some(&b"v"[..])
            })
        };

        let a = RefCell::new(store(1));
        let peer = ResumePeer { store: &b, calls: Default::default(), empty: Default::default() };
        assert_eq!(run(reconcile(&a, &peer, DEFAULT_BUCKETS)).unwrap(), 40);
        assert!(peer.empty.get() > 1);
        assert!(converged(&a));

        let a = RefCell::new(store(1));
        let peer = ResumePeer { store: &b, calls: Default::default(), empty: Default::default() };
        let (mut passes, mut resume) = (0usize, None::<Vec<u8>>);
        loop {
            let calls = peer.calls.get();
            let (_, next) = run(reconcile_with(
                &a,
                &peer,
                DEFAULT_ZONE,
                DEFAULT_BUCKETS,
                usize::MAX,
                2,
                usize::MAX,
                resume.as_deref(),
                None,
                None,
                None,
                |_| Some(crate::replica::LiveFloor::Waive),
            ))
            .unwrap();
            assert!(peer.calls.get() - calls <= 2, "pass {passes} exceeded its digest page cap");
            passes += 1;
            assert!(passes < 200, "page-capped reconcile failed to converge");
            match next {
                Some(next) => resume = Some(next),
                None => break,
            }
        }
        assert!(passes > 2);
        assert!(converged(&a));
    }

    #[test]
    fn bidirectional_reconcile_converges_a_diverged_pair() {
        let a = RefCell::new(store(1));
        let b = RefCell::new(store(2));
        a.borrow_mut().apply(b"x", Some(b"ax"), v(100, 1)).unwrap();
        a.borrow_mut()
            .apply(b"shared", Some(b"a"), v(300, 1))
            .unwrap();
        b.borrow_mut().apply(b"y", Some(b"by"), v(110, 2)).unwrap();
        b.borrow_mut()
            .apply(b"shared", Some(b"b"), v(200, 2))
            .unwrap();

        run(reconcile(&a, &LocalPeer(&b.borrow()), DEFAULT_BUCKETS)).unwrap();
        run(reconcile(&b, &LocalPeer(&a.borrow()), DEFAULT_BUCKETS)).unwrap();

        for (s, label) in [(&a, "a"), (&b, "b")] {
            let s = s.borrow();
            assert_eq!(s.get(b"x").unwrap().as_deref(), Some(&b"ax"[..]), "{label}");
            assert_eq!(s.get(b"y").unwrap().as_deref(), Some(&b"by"[..]), "{label}");
            assert_eq!(
                s.get(b"shared").unwrap().as_deref(),
                Some(&b"a"[..]),
                "{label}"
            );
        }
    }
}
