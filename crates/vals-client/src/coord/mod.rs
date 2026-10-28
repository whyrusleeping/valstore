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

/// Why one request to one replica failed.
///
/// The coordinator folds these into a [`CoordError`] for the whole operation;
/// they appear directly in [`ReplicaError`] and from the transport's own
/// methods.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The node could not be reached: no connection, a refused or reset
    /// connection, an unknown node, or a node marked down.
    Unavailable,
    /// The connect/hello or RPC deadline expired. The request may still have
    /// been applied.
    Timeout,
    /// The replica's storage engine failed the request.
    Engine(String),
    /// The replica answered with an error, or with a reply this client could
    /// not use.
    Remote(String),
    /// The replica runs a newer ring epoch (carried here) than the request was
    /// routed with, and refused it.
    StaleEpoch(u64),
    /// The replica is healthy but refused retryably (reserve floor, memory
    /// budget, admission gate, or anti-entropy not ready). Retry with backoff.
    Busy,
    /// This client's own request budget is exhausted; nothing was sent.
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

/// One replica's failure, reported in [`CoordError::ScanQuorumFailed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaError {
    /// The replica.
    pub node: NodeId,
    /// What went wrong.
    pub error: TransportError,
}

impl std::fmt::Display for ReplicaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node {}: {}", self.node, self.error)
    }
}

#[doc(hidden)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RingView {
    pub epoch: u64,
    pub vnodes: u32,
    pub rf: u32,
    pub members: Vec<(NodeId, u32)>,
    pub addrs: Vec<(NodeId, Vec<std::net::SocketAddr>)>,
}

#[doc(hidden)]
pub struct BatchEntry<'a> {
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
    pub version: Version,
}

#[doc(hidden)]
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

#[doc(hidden)]
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

/// How a [`Coordinator`] reaches replicas.
///
/// [`NetTransport`](crate::NetTransport) speaks the vals TCP protocol to data
/// nodes, and `vals-cluster`'s `LocalCluster` calls in-process stores. The
/// trait appears in public signatures as the coordinator's type parameter, but
/// its methods are an internal interface between the coordinator and those
/// two transports and change with the wire protocol; implementing it outside
/// this repository is not supported.
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

/// How many of a key's replicas (`W`) must acknowledge a write.
///
/// The coordinator sends the write to every replica of the key and returns
/// once `W` have acknowledged. Requests to slower replicas that are still
/// unsent at that point are dropped, and background repair (read repair,
/// hints, anti-entropy) brings those replicas up to date. `RF` below is the key's
/// replica count (the zone's replication factor, or fewer if the cluster has
/// fewer eligible nodes).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteConcern {
    /// `W = 0`: send and return without waiting for any replica. Success only
    /// means the client could send the request. Not allowed with
    /// [`Persistence::Device`].
    Unacknowledged,
    /// `W = 1`.
    One,
    /// `W = RF / 2 + 1`. Paired with a majority read, `W + R > RF` at every
    /// replication factor.
    Majority,
    /// `W = RF`: every replica must acknowledge, so one unreachable replica
    /// fails the write.
    All,
    /// An explicit `W` with `1 <= W <= RF`; anything else is refused with
    /// [`CoordError::InvalidWriteOptions`] rather than clamped.
    Quorum(usize),
}

/// What a replica's acknowledgement means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Persistence {
    /// The replica acknowledges once the write is installed in its memory,
    /// before any device sync. An acknowledged
    /// write survives as long as one acknowledging replica keeps running; a
    /// power loss or crash of every acknowledging replica before their next
    /// sync can lose it (and a lost delete can let an older value return).
    Buffered,
    /// The replica acknowledges after a device sync that covers the write.
    /// Concurrent writes share syncs, but each write still waits for one.
    Device,
}

/// The write concern and persistence for a write, delete, batch or range
/// delete.
///
/// Every write method takes `impl Into<WriteOptions>`, so a bare
/// [`WriteConcern`] works and means [`Persistence::Buffered`]. The default is
/// `WriteOptions::buffered(WriteConcern::All)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteOptions {
    /// How many replicas must acknowledge.
    pub concern: WriteConcern,
    /// What each acknowledgement means.
    pub persistence: Persistence,
}

impl WriteOptions {
    /// `concern` with [`Persistence::Buffered`].
    pub const fn buffered(concern: WriteConcern) -> Self {
        WriteOptions {
            concern,
            persistence: Persistence::Buffered,
        }
    }

    /// `concern` with [`Persistence::Device`].
    pub const fn device_durable(concern: WriteConcern) -> Self {
        WriteOptions {
            concern,
            persistence: Persistence::Device,
        }
    }

    /// The `W` these options need when the key has `replicas` replicas, or
    /// [`CoordError::InvalidWriteOptions`] if they cannot be met.
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

/// Why a coordinator operation failed.
///
/// For writes, deletes and range deletes, every variant except
/// [`InvalidWriteOptions`](Self::InvalidWriteOptions),
/// [`InvalidRange`](Self::InvalidRange), [`UnknownZone`](Self::UnknownZone),
/// [`NotSorted`](Self::NotSorted), [`NoReplicas`](Self::NoReplicas) and
/// [`Unsupported`](Self::Unsupported) can follow a partial apply: some
/// replicas may hold the write. Treat the outcome as unknown, and if you retry,
/// expect the retry to be a new write with a newer version.
#[derive(Debug, PartialEq, Eq)]
pub enum CoordError {
    /// Fewer than `needed` replicas acknowledged (writes) or answered (reads).
    /// Usually replicas are down or unreachable.
    QuorumFailed {
        /// Acknowledgements or replies required.
        needed: usize,
        /// How many arrived.
        got: usize,
    },
    /// A scan could not read one range from `needed` replicas. `cursor` is the
    /// key the failed page started at (`None` for the start of the range), and
    /// `replicas` lists each replica's failure.
    ScanQuorumFailed {
        /// The zone scanned.
        zone_id: ZoneId,
        /// Where the failed page started.
        cursor: Option<Vec<u8>>,
        /// Replies required.
        needed: usize,
        /// Replies received.
        got: usize,
        /// The replicas that failed and why.
        replicas: Vec<ReplicaError>,
    },
    /// The write missed its quorum because `rejected` replicas refused its
    /// version: too far ahead of their clocks or, with tombstone GC enabled,
    /// older than their GC grace allows. Check clock synchronization.
    SkewRejected {
        /// Acknowledgements required.
        needed: usize,
        /// Acknowledgements received.
        got: usize,
        /// Replicas that refused on clock skew.
        rejected: usize,
    },
    /// The ring has no eligible replica for the key (for example, an empty
    /// ring or a zone scope with no members). Nothing was sent.
    NoReplicas,
    /// The zone is not in the catalog, even after refreshing it from a member.
    UnknownZone(ZoneId),
    /// The operation needs a sorted zone (scans, range deletes) and this zone
    /// is unsorted. Zone 0 is always unsorted.
    NotSorted(ZoneId),
    /// A node needed for the operation (or for a ring or catalog refresh)
    /// could not be reached.
    Unavailable,
    /// This client's request budget ([`ClientBudgets`], or the coordinator's
    /// credits) is exhausted: a single request larger than a budget, a
    /// shedding call such as [`Coordinator::try_write`] under load, or the
    /// transport's response budget.
    Overloaded,
    /// A replica runs a newer ring epoch. With automatic refresh (the default)
    /// this is returned only if refreshing and retrying once did not help.
    StaleRing {
        /// The replica's epoch.
        epoch: u64,
    },
    /// The quorum was missed because `busy` healthy replicas refused
    /// retryably (full reserve, memory budget, admission gate). Retry with
    /// backoff.
    Busy {
        /// Acknowledgements required.
        needed: usize,
        /// Acknowledgements received.
        got: usize,
        /// Replicas that refused retryably.
        busy: usize,
    },
    /// A range delete's `lo` is not below its `hi`. Nothing was sent.
    InvalidRange,
    /// The [`WriteOptions`] cannot be met for a key with `replicas` replicas:
    /// `Quorum(0)`, `Quorum(n)` with `n > replicas`, or `Unacknowledged` with
    /// `Device` persistence. Nothing was sent for that key.
    InvalidWriteOptions {
        /// The options given.
        options: WriteOptions,
        /// The key's replica count.
        replicas: usize,
    },
    /// The node does not support range deletes. Nothing was sent.
    Unsupported(NodeId),
    /// A range delete reached its write quorum on some ranges but not on the
    /// ranges listed in `missed`, which may hold the tombstone on some
    /// replicas. A retry is a new range delete at a newer version.
    RangeDeleteIncomplete {
        /// The version the range delete was written at.
        version: Version,
        /// The ranges that missed their quorum, and why.
        missed: Vec<RangeDeleteMiss>,
    },
}

/// One range a range delete could not complete; see
/// [`CoordError::RangeDeleteIncomplete`].
#[derive(Debug, PartialEq, Eq)]
pub struct RangeDeleteMiss {
    /// Inclusive lower bound of the range (`None` = start of the zone).
    pub lo: Option<Vec<u8>>,
    /// Exclusive upper bound of the range (`None` = end of the zone).
    pub hi: Option<Vec<u8>>,
    /// Why it failed.
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

/// The outcome of one write in a batch.
#[derive(Debug, PartialEq, Eq)]
pub enum WriteOutcome {
    /// The write met its concern.
    Ok {
        /// The version it was written at.
        version: Version,
        /// Replicas that acknowledged by the time the batch returned.
        acks: usize,
    },
    /// The write missed its concern. If `acks > 0` it is on some replicas;
    /// even with `acks == 0` it may be (see [`CoordError`]).
    Failed {
        /// Why.
        error: CoordError,
        /// Replicas that acknowledged.
        acks: usize,
    },
}

/// Per-write outcomes of a batch, in input order.
///
/// A batch is not atomic: each write succeeds or fails on its own.
#[derive(Debug)]
pub struct BatchResult {
    /// One outcome per input write, in the same order.
    pub outcomes: Vec<WriteOutcome>,
}

impl BatchResult {
    /// Whether every write met its concern.
    pub fn all_ok(&self) -> bool {
        self.outcomes
            .iter()
            .all(|o| matches!(o, WriteOutcome::Ok { .. }))
    }

    /// The input index and error of each failed write.
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

/// What a sloppy-quorum write ([`Coordinator::write_sloppy`]) achieved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SloppyAck {
    /// The version written.
    pub version: Version,
    /// Acknowledgements from the key's home replicas.
    pub home_acks: usize,
    /// Acknowledgements from fallback nodes outside the home set.
    pub fallback_acks: usize,
    /// Unreachable home replicas for which this coordinator kept a hint to
    /// deliver later.
    pub hinted: usize,
}

impl SloppyAck {
    /// Whether the concern was met by home replicas alone, so the write has
    /// the same read guarantee as a strict write.
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

/// How a quorum read contacts replicas; see [`Coordinator::with_read_fanout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFanout {
    /// The default. Asks `R` replicas: one returns the value and the others
    /// return only the version they hold, so a strong read costs about one
    /// device read. A spare replica is asked when one fails, and if the value
    /// holder is not the newest, the newest value is fetched in a second step.
    Digest,
    /// Ask every replica for the value and use the first `R` replies.
    All,
    /// Ask exactly `R` replicas for the value, and others only if some fail.
    Quorum,
}

/// The client: routes each operation to its key's replicas and applies the
/// quorum rules.
///
/// Build one with [`Coordinator::new`] over a transport, usually a
/// [`NetTransport`](crate::NetTransport), then refresh its ring and catalog
/// from a live node. See the [crate documentation](crate) for the threading,
/// retry, cancellation and consistency rules that apply to every method.
///
/// Methods without a zone argument use [`DEFAULT_ZONE`]; the `_in` variants
/// take a zone. Reads return only live values: a deleted key reads as `None`.
/// Every write is stamped with a [`Version`] from this coordinator's hybrid
/// logical clock, whose writer id is random per coordinator, so two
/// coordinators never produce the same version.
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
