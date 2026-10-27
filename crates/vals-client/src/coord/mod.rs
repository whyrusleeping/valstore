use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::failure::FailureDetector;
use crate::hints::{Hint, HintStore, MemHintStore};
use crate::hlc::{physical_now_ms, Hlc, Version};
use crate::partition::RangeMap;
use crate::ring::{NodeId, Ring};
use crate::types::{Applied, CasResult, MixedReadEntry, MixedReadReply, MixedReadRole, Stored};
use crate::zone::{Catalog, Ordering, ZoneId, DEFAULT_ZONE};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    Unavailable,
    Timeout,
    Engine(String),
    Remote(String),
    StaleEpoch(u64),
    Busy,
    Overloaded,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unavailable => write!(f, "node unavailable"),
            TransportError::Timeout => write!(f, "rpc deadline exceeded"),
            TransportError::Engine(e) => write!(f, "engine: {e}"),
            TransportError::Remote(m) => write!(f, "remote: {m}"),
            TransportError::StaleEpoch(e) => {
                write!(
                    f,
                    "refused: coordinator ring epoch is stale (replica at {e})"
                )
            }
            TransportError::Busy => {
                write!(
                    f,
                    "busy: replica refused retryably (reserve floor, RAM budget, admission gate, \
                     or anti-entropy seed not ready), retry with backoff"
                )
            }
            TransportError::Overloaded => write!(f, "client transport budget exceeded"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaError {
    pub node: NodeId,
    pub error: TransportError,
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node {}: {}", self.node, self.error)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RingView {
    pub epoch: u64,
    pub vnodes: u32,
    pub rf: u32,
    pub members: Vec<(NodeId, u32)>,
    pub addrs: Vec<(NodeId, Vec<std::net::SocketAddr>)>,
}

pub struct BatchEntry<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub version: Version,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchAck {
    Applied(Applied),
    Stale(u64),
    Busy,
    Timeout,
    Overloaded,
    Remote,
    Unreachable,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScanPage {
    pub entries: Vec<(Vec<u8>, Stored)>,
    pub more: bool,
    pub fragments: Vec<crate::wire::RtFragment>,
    pub resume: Option<Vec<u8>>,
}

pub(crate) fn stored_response_bytes(stored: &Stored) -> usize {
    stored.value.as_ref().map_or(0, Vec::capacity)
}

pub(crate) fn mixed_response_bytes(
    responses: &Vec<Result<MixedReadReply, TransportError>>,
) -> usize {
    responses
        .capacity()
        .saturating_mul(std::mem::size_of::<Result<MixedReadReply, TransportError>>())
        .saturating_add(responses.iter().fold(0usize, |bytes, response| match response {
            Ok(MixedReadReply::Value(Some(stored))) => {
                bytes.saturating_add(stored_response_bytes(stored))
            }
            Err(TransportError::Engine(message) | TransportError::Remote(message)) => {
                bytes.saturating_add(message.capacity())
            }
            _ => bytes,
        }))
}

pub(crate) fn scan_response_bytes(page: &ScanPage) -> usize {
    page.entries
        .capacity()
        .saturating_mul(std::mem::size_of::<(Vec<u8>, Stored)>())
        .saturating_add(page.entries.iter().fold(0usize, |bytes, (key, stored)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(stored_response_bytes(stored))
        }))
        .saturating_add(
            page.fragments
                .capacity()
                .saturating_mul(std::mem::size_of::<crate::wire::RtFragment>()),
        )
        .saturating_add(page.fragments.iter().fold(0usize, |bytes, f| {
            bytes
                .saturating_add(f.lo.as_ref().map_or(0, Vec::capacity))
                .saturating_add(f.hi.as_ref().map_or(0, Vec::capacity))
        }))
        .saturating_add(page.resume.as_ref().map_or(0, Vec::capacity))
}

type PrefetchedRound<'a> = Pin<Box<dyn Future<Output = scan::ScanRound> + 'a>>;
type MixedNodeResult = (
    NodeId,
    Vec<(usize, MixedReadRole)>,
    Vec<Result<MixedReadReply, TransportError>>,
    Option<Credit>,
);
type MixedNodeFuture<'a> = Pin<Box<dyn Future<Output = MixedNodeResult> + 'a>>;

pub trait Transport {
    #[doc(hidden)]
    fn client_budgets(&self) -> Option<ClientBudgets> {
        None
    }

    #[doc(hidden)]
    fn retain_response(
        &self,
        _node: NodeId,
        _bytes: usize,
    ) -> Result<Option<Credit>, TransportError> {
        Ok(None)
    }

    fn set_route_epoch(&self, _epoch: u64) {}

    fn set_sorted_ranges(&self, _ranges: &std::collections::HashMap<ZoneId, RangeMap>) {}

    fn add_peer(&self, _node: NodeId, _addrs: &[std::net::SocketAddr]) {}

    fn flush_requests(&self) {}

    fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> impl std::future::Future<Output = Result<Applied, TransportError>>;

    fn apply_send(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
    ) -> impl std::future::Future<Output = Result<(), TransportError>> {
        async move {
            self.apply(node, zone_id, key, value, version)
                .await
                .map(|_| ())
        }
    }

    fn apply_persistent(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        persistence: Persistence,
    ) -> impl std::future::Future<Output = Result<Applied, TransportError>> {
        async move {
            if persistence == Persistence::Buffered {
                return self.apply(node, zone_id, key, value, version).await;
            }
            let entry = [BatchEntry {
                key,
                value,
                version,
            }];
            match self.apply_batch(node, zone_id, true, &entry).await.into_iter().next() {
                Some(BatchAck::Applied(a)) => Ok(a),
                Some(BatchAck::Stale(e)) => Err(TransportError::StaleEpoch(e)),
                Some(BatchAck::Busy) => Err(TransportError::Busy),
                Some(BatchAck::Overloaded) => Err(TransportError::Overloaded),
                _ => Err(TransportError::Unavailable),
            }
        }
    }
    fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Stored>, TransportError>>;

    fn digest(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> impl std::future::Future<Output = Result<Option<Version>, TransportError>> {
        async move {
            self.read(node, zone_id, key)
                .await
                .map(|o| o.map(|s| s.version))
        }
    }

    fn read_values(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> impl std::future::Future<Output = Vec<Result<Option<Stored>, TransportError>>> {
        async move {
            let mut out = Vec::with_capacity(keys.len());
            for k in keys {
                out.push(self.read(node, zone_id, k).await);
            }
            out
        }
    }

    fn read_mixed(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        reads: &[MixedReadEntry<'_>],
    ) -> impl std::future::Future<Output = Vec<Result<MixedReadReply, TransportError>>> {
        async move {
            let mut out = Vec::with_capacity(reads.len());
            for entry in reads {
                let result = match entry.role {
                    MixedReadRole::Value => self
                        .read(node, zone_id, entry.key)
                        .await
                        .map(MixedReadReply::Value),
                    MixedReadRole::Digest => self
                        .digest(node, zone_id, entry.key)
                        .await
                        .map(MixedReadReply::Digest),
                };
                out.push(result);
            }
            out
        }
    }

    fn scan(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _lo: Option<&[u8]>,
        _hi: Option<&[u8]>,
        _max: u32,
    ) -> impl std::future::Future<Output = Result<ScanPage, TransportError>> {
        async { Err(TransportError::Remote("scan unsupported".into())) }
    }

    fn scan_many(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        scans: &[crate::wire::WireScan<'_>],
        keys_only: bool,
    ) -> impl Future<Output = Vec<Result<ScanPage, TransportError>>> {
        async move {
            let mut out = Vec::with_capacity(scans.len());
            for &(lo, hi, max) in scans {
                let mut result = self.scan(node, zone_id, lo, hi, max).await;
                if keys_only {
                    if let Ok(page) = &mut result {
                        for (_, stored) in &mut page.entries {
                            stored.value = stored.value.as_ref().map(|_| Vec::new());
                        }
                    }
                }
                out.push(result);
            }
            out
        }
    }

    fn scan_rev(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _lo: Option<&[u8]>,
        _hi: Option<&[u8]>,
        _max: u32,
        _keys_only: bool,
    ) -> impl Future<Output = Result<ScanPage, TransportError>> {
        async { Err(TransportError::Remote("reverse scan unsupported".into())) }
    }

    fn scan_many_rev(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        scans: &[crate::wire::WireScan<'_>],
        keys_only: bool,
    ) -> impl Future<Output = Vec<Result<ScanPage, TransportError>>> {
        async move {
            let mut out = Vec::with_capacity(scans.len());
            for &(lo, hi, max) in scans {
                out.push(self.scan_rev(node, zone_id, lo, hi, max, keys_only).await);
            }
            out
        }
    }

    fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
        Some(false)
    }

    fn range_delete(
        &self,
        _node: NodeId,
        _zone_id: ZoneId,
        _sync: bool,
        _version: Version,
        slices: &[crate::wire::WireSlice<'_>],
    ) -> impl std::future::Future<Output = Vec<BatchAck>> {
        let n = slices.len();
        async move { vec![BatchAck::Remote; n] }
    }

    fn members(
        &self,
        node: NodeId,
    ) -> impl std::future::Future<Output = Result<RingView, TransportError>>;

    fn zones(
        &self,
        node: NodeId,
    ) -> impl std::future::Future<Output = Result<Catalog, TransportError>>;

    fn range_maps(
        &self,
        _node: NodeId,
    ) -> impl std::future::Future<Output = Result<Vec<(ZoneId, RangeMap)>, TransportError>> {
        async { Ok(Vec::new()) }
    }

    fn apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        _sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> impl std::future::Future<Output = Vec<BatchAck>> {
        async move {
            let mut out = Vec::with_capacity(writes.len());
            for e in writes {
                out.push(
                    match self.apply(node, zone_id, e.key, e.value, e.version).await {
                        Ok(a) => BatchAck::Applied(a),
                        Err(TransportError::StaleEpoch(ep)) => BatchAck::Stale(ep),
                        Err(TransportError::Busy) => BatchAck::Busy,
                        Err(TransportError::Timeout) => BatchAck::Timeout,
                        Err(TransportError::Overloaded) => BatchAck::Overloaded,
                        Err(TransportError::Unavailable) => BatchAck::Unreachable,
                        Err(_) => BatchAck::Remote,
                    },
                );
            }
            out
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConcern {
    Unacknowledged,
    One,
    Majority,
    All,
    Quorum(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Persistence {
    Buffered,
    Device,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    pub concern: WriteConcern,
    pub persistence: Persistence,
}

impl WriteOptions {
    pub const fn buffered(concern: WriteConcern) -> Self {
        WriteOptions {
            concern,
            persistence: Persistence::Buffered,
        }
    }

    pub const fn device_durable(concern: WriteConcern) -> Self {
        WriteOptions {
            concern,
            persistence: Persistence::Device,
        }
    }

    pub fn required_acks(self, replicas: usize) -> Result<usize, CoordError> {
        let w = match self.concern {
            WriteConcern::Unacknowledged => 0,
            WriteConcern::One => 1,
            WriteConcern::Majority => replicas / 2 + 1,
            WriteConcern::All => replicas,
            WriteConcern::Quorum(w) => w,
        };
        let valid = match self.concern {
            WriteConcern::Unacknowledged => self.persistence == Persistence::Buffered,
            _ => w >= 1 && w <= replicas,
        };
        if valid {
            Ok(w)
        } else {
            Err(CoordError::InvalidWriteOptions {
                options: self,
                replicas,
            })
        }
    }
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions::buffered(WriteConcern::All)
    }
}

impl From<WriteConcern> for WriteOptions {
    fn from(concern: WriteConcern) -> Self {
        WriteOptions::buffered(concern)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum CoordError {
    QuorumFailed {
        needed: usize,
        got: usize,
    },
    ScanQuorumFailed {
        zone_id: ZoneId,
        cursor: Option<Vec<u8>>,
        needed: usize,
        got: usize,
        replicas: Vec<ReplicaError>,
    },
    SkewRejected {
        needed: usize,
        got: usize,
        rejected: usize,
    },
    NoReplicas,
    UnknownZone(ZoneId),
    NotSorted(ZoneId),
    Unavailable,
    Overloaded,
    StaleRing {
        epoch: u64,
    },
    Busy {
        needed: usize,
        got: usize,
        busy: usize,
    },
    InvalidRange,
    InvalidWriteOptions {
        options: WriteOptions,
        replicas: usize,
    },
    Unsupported(NodeId),
    RangeDeleteIncomplete {
        version: Version,
        missed: Vec<RangeDeleteMiss>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub struct RangeDeleteMiss {
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
    pub error: CoordError,
}

impl std::fmt::Display for CoordError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CoordError::QuorumFailed { needed, got } => {
                write!(f, "quorum failed: needed {needed}, got {got}")
            }
            CoordError::ScanQuorumFailed {
                zone_id,
                cursor,
                needed,
                got,
                replicas,
            } => {
                write!(f, "scan quorum failed: needed {needed}, got {got} (zone {zone_id}, cursor ")?;
                match cursor {
                    Some(c) => {
                        for b in c {
                            write!(f, "{b:02x}")?;
                        }
                    }
                    None => write!(f, "start")?,
                }
                write!(f, ")")?;
                for (i, r) in replicas.iter().enumerate() {
                    write!(f, "{}{r}", if i == 0 { ": " } else { "; " })?;
                }
                Ok(())
            }
            CoordError::SkewRejected {
                needed,
                got,
                rejected,
            } => write!(
                f,
                "quorum failed on clock skew: needed {needed}, got {got} (committed), \
                 {rejected} replica(s) refused on skew"
            ),
            CoordError::NoReplicas => write!(f, "no replicas for key"),
            CoordError::UnknownZone(z) => write!(f, "unknown zone {z}"),
            CoordError::NotSorted(z) => write!(f, "zone {z} is not sorted (cannot range-scan)"),
            CoordError::Unavailable => write!(f, "node unavailable"),
            CoordError::Overloaded => write!(f, "coordinator overloaded (credit pool exhausted)"),
            CoordError::StaleRing { epoch } => {
                write!(
                    f,
                    "stale ring: replica is at epoch {epoch}, refresh required"
                )
            }
            CoordError::Busy { needed, got, busy } => write!(
                f,
                "backpressure: needed {needed}, got {got} (committed), \
                 {busy} healthy replica(s) refused retryably (reserve floor, RAM budget \
                 or admission gate) — retry with backoff"
            ),
            CoordError::InvalidRange => write!(f, "invalid range: lo must be below hi"),
            CoordError::InvalidWriteOptions { options, replicas } => write!(
                f,
                "invalid write options ({:?}, {:?}) for {replicas} replica(s): an acknowledged \
                 concern needs 1 <= W <= replicas, and Device persistence needs an \
                 acknowledged concern",
                options.concern, options.persistence
            ),
            CoordError::Unsupported(node) => write!(
                f,
                "node {node} does not support range deletes (hello feature RANGE_TOMBSTONES)"
            ),
            CoordError::RangeDeleteIncomplete { version, missed } => {
                write!(
                    f,
                    "range delete at version {}.{} missed its write quorum on {} slice(s)",
                    version.hlc,
                    version.node,
                    missed.len()
                )?;
                for (i, m) in missed.iter().enumerate().take(4) {
                    write!(f, "{}[", if i == 0 { ": " } else { "; " })?;
                    for (j, bound) in [&m.lo, &m.hi].into_iter().enumerate() {
                        if j == 1 {
                            write!(f, ", ")?;
                        }
                        match bound {
                            Some(b) => {
                                for byte in b {
                                    write!(f, "{byte:02x}")?;
                                }
                            }
                            None => write!(f, "{}", if j == 0 { "start" } else { "end" })?,
                        }
                    }
                    write!(f, "): {}", m.error)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for CoordError {}

#[derive(Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    Ok {
        version: Version,
        acks: usize,
    },
    Failed {
        error: CoordError,
        acks: usize,
    },
}

#[derive(Debug)]
pub struct BatchResult {
    pub outcomes: Vec<WriteOutcome>,
}

impl BatchResult {
    pub fn all_ok(&self) -> bool {
        self.outcomes
            .iter()
            .all(|o| matches!(o, WriteOutcome::Ok { .. }))
    }

    pub fn failures(&self) -> impl Iterator<Item = (usize, &CoordError)> {
        self.outcomes
            .iter()
            .enumerate()
            .filter_map(|(i, o)| match o {
                WriteOutcome::Failed { error, .. } => Some((i, error)),
                WriteOutcome::Ok { .. } => None,
            })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SloppyAck {
    pub version: Version,
    pub home_acks: usize,
    pub fallback_acks: usize,
    pub hinted: usize,
}

impl SloppyAck {
    pub fn is_strict(&self) -> bool {
        self.fallback_acks == 0
    }
}

struct Committed {
    version: Version,
    home_acks: usize,
    fallback_acks: usize,
    hinted: usize,
}

impl Committed {
    fn into_sloppy_ack(self) -> SloppyAck {
        SloppyAck {
            version: self.version,
            home_acks: self.home_acks,
            fallback_acks: self.fallback_acks,
            hinted: self.hinted,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFanout {
    Digest,
    All,
    Quorum,
}

pub struct Coordinator<T: Transport> {
    node: NodeId,
    ring: RefCell<Ring>,
    epoch: Cell<u64>,
    #[doc(hidden)]
    pub transport: T,
    hlc: Hlc,
    r: usize,
    read_fanout: ReadFanout,
    hints: RefCell<Box<dyn HintStore>>,
    #[doc(hidden)]
    pub read_rotor: Cell<usize>,
    credits: Rc<RefCell<CreditPool>>,
    catalog: RefCell<Catalog>,
    range_maps: RefCell<HashMap<ZoneId, RangeMap>>,
    fd: Option<RefCell<FailureDetector>>,
    clock: Box<dyn Fn() -> u64>,
    auto_refresh: Cell<bool>,
    scan_prefetch: Cell<bool>,
    digest_hedge: Option<DigestHedge>,
}

struct DigestHedge {
    clock: Rc<HedgeClock>,
    delay_ticks: u64,
    tick_period: Duration,
}


mod credits;
pub use credits::*;
use credits::batch_cost;
mod routing;
mod write;
#[cfg(test)]
use write::{applied_is_ack, classify_apply, AckKind};
mod read;
mod scan;
mod scan_many;
mod scan_rev;
pub use scan_many::ScanQuery;
pub use scan::{scan_merge_bound, scan_merge_lww};
mod range_delete;

mod hedge;
pub use hedge::HedgeClock;

mod service;
pub use service::{BatchWriteHandle, CoordinatorService};

#[cfg(test)]
mod tests;
