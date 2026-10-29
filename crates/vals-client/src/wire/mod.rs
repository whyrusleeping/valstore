use crate::hlc::Version;
use crate::partition::RangeMap;
use crate::ring::NodeId;
use crate::types::{Applied, MixedReadReply, MixedReadRole, Stored};
use crate::zone::ZoneId;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

const OP_APPLY: u8 = 1;
const OP_READ: u8 = 2;
const OP_SYNC_TREE: u8 = 3;
const OP_SYNC_BUCKET_DIGESTS: u8 = 4;
const OP_SET_MEMBERS: u8 = 5;
const OP_MEMBERS: u8 = 6;
const OP_BATCH_APPLY: u8 = 7;
const OP_SET_ZONES: u8 = 8;
const OP_ZONES: u8 = 9;
const OP_GET_WATERMARK: u8 = 10;
const OP_SCAN: u8 = 11;
const OP_SET_RANGE_MAPS: u8 = 12;
const OP_RANGE_MAPS: u8 = 13;
const OP_DIGEST: u8 = 14;
const OP_SYNC_VALUES: u8 = 15;
const OP_RANGE_STATS: u8 = 16;
const OP_RANGE_WRITE_MARK: u8 = 17;
const OP_RANGE_STATS_V2: u8 = 18;
const OP_MIXED_BATCH_READ: u8 = 19;
const OP_RANGE_STATS_V3: u8 = 20;
const OP_RANGE_DELETE: u8 = 21;
const OP_SYNC_RANGE_TOMBSTONES: u8 = 22;
const OP_MULTI_SCAN: u8 = 23;
const OP_GC_FLOOR: u8 = 24;
const OP_CLEANUP_STATUS: u8 = 25;
const RESP_MULTI_SCAN: u8 = 21;

#[cfg(test)]
mod multi_scan_tests;

pub const MAX_MULTI_SCAN_ENTRIES: usize = 256;
pub type WireScan<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, u32);
pub type OwnedScan = (Option<Vec<u8>>, Option<Vec<u8>>, u32);
const RANGE_MAP_WIRE_MAGIC: u32 = u32::MAX;
const RANGE_MAP_WIRE_V4: u32 = 4;
const RANGE_MAP_WIRE_V3: u32 = 3;
const RANGE_MAP_WIRE_V2: u32 = 2;

const RESP_APPLY: u8 = 0;
const RESP_READ: u8 = 1;
const RESP_ERR: u8 = 2;
const RESP_TREE: u8 = 3;
const RESP_BUCKET_DIGESTS: u8 = 4;
const RESP_MEMBERS: u8 = 5;
const RESP_BATCH: u8 = 6;
const RESP_ZONES: u8 = 7;
const RESP_STALE_EPOCH: u8 = 8;
const RESP_WATERMARK: u8 = 9;
const RESP_SCAN: u8 = 10;
const RESP_RANGE_MAPS: u8 = 11;
const RESP_DIGEST: u8 = 12;
const RESP_DENIED: u8 = 13;
const RESP_VALUES: u8 = 14;
const RESP_RANGE_STATS: u8 = 15;
const RESP_BUSY: u8 = 16;
const RESP_RANGE_STATS_V2: u8 = 17;
const RESP_MIXED_BATCH_READ: u8 = 18;
const RESP_RANGE_STATS_V3: u8 = 19;
const RESP_RANGE_TOMBSTONES: u8 = 20;
const RESP_GC_FLOOR: u8 = 22;
const RESP_CLEANUP_STATUS: u8 = 23;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsettledZone {
    pub zone_id: ZoneId,
    pub lapped: bool,
    pub pending_rehome: bool,
    pub kept: u64,
    pub samples: Vec<Vec<u8>>,
}

pub const MAX_MIXED_BATCH_READ_ENTRIES: usize = 4096;

pub const MAX_RANGE_DELETE_SLICES: usize = 4096;

const TAG_NONE: u8 = 0;
const TAG_SOME: u8 = 1;

const APPLIED_WON: u8 = 0;
const APPLIED_LOST: u8 = 1;
const APPLIED_REJECTED: u8 = 2;

#[derive(Debug, PartialEq, Eq)]
pub enum WireError {
    Truncated,
    BadTag(u8),
    BadUtf8,
    BadRangeMap,
    TooManyEntries,
    TrailingBytes,
}

pub type WireWrite<'a> = (&'a [u8], Option<&'a [u8]>, Version);

pub type OwnedWrite = (Vec<u8>, Option<Vec<u8>>, Version);

pub type WireSlice<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

pub type OwnedSlice = (Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RtFragment {
    pub lo: Option<Vec<u8>>,
    pub hi: Option<Vec<u8>>,
    pub version: Version,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum MixedReadResult {
    Reply(MixedReadReply),
    Busy,
    Err(String),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WireZone<'a> {
    pub id: u32,
    pub rf: u32,
    pub ordering: u8,
    pub scope: Option<&'a [u32]>,
    pub name: &'a str,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct WireRangeMap<'a> {
    pub zone_id: u32,
    pub splits: &'a [Vec<u8>],
    pub tokens: &'a [Vec<u8>],
    pub pending: &'a [Option<Vec<u8>>],
    pub placement_debt: &'a [bool],
    pub opaque_tokens: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OwnedRangeMap {
    pub zone_id: u32,
    pub splits: Vec<Vec<u8>>,
    pub tokens: Vec<Vec<u8>>,
    pub pending: Vec<Option<Vec<u8>>>,
    pub placement_debt: Vec<bool>,
    pub opaque_tokens: bool,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OwnedRangeStat {
    pub zone_id: u32,
    pub lower_bound: Vec<u8>,
    pub entries: u64,
    pub midpoint: Option<Vec<u8>>,
    pub ops_per_sec: u64,
    pub shard_index: u32,
    pub shard_count: u32,
    pub rehome_mirror_epoch: u64,
    pub rehome_mirror_ok: u64,
    pub rehome_mirror_failures: u64,
    pub rehome_copy_complete_epoch: u64,
    pub rehome_copy_progress: u64,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OwnedZone {
    pub id: u32,
    pub rf: u32,
    pub ordering: u8,
    pub scope: Option<Vec<u32>>,
    pub name: String,
}

pub const INSPECT_EPOCH: u64 = u64::MAX;

#[derive(Debug, PartialEq, Eq)]
pub enum Request<'a> {
    Apply {
        epoch: u64,
        zone_id: ZoneId,
        key: &'a [u8],
        value: Option<&'a [u8]>,
        version: Version,
    },
    Read {
        epoch: u64,
        zone_id: ZoneId,
        key: &'a [u8],
    },
    Digest {
        epoch: u64,
        zone_id: ZoneId,
        key: &'a [u8],
    },
    Scan {
        epoch: u64,
        zone_id: ZoneId,
        lo: Option<&'a [u8]>,
        hi: Option<&'a [u8]>,
        max: u32,
    },
    SyncTree {
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &'a [u32],
        requester: Option<NodeId>,
    },
    SyncBucketDigests {
        zone_id: ZoneId,
        num_buckets: u32,
        ids: &'a [u32],
        after: Option<&'a [u8]>,
    },
    SyncValues {
        zone_id: ZoneId,
        keys: &'a [&'a [u8]],
    },
    SetMembers {
        auth: &'a [u8],
        epoch: u64,
        vnodes: u32,
        rf: u32,
        members: &'a [(u32, u32)],
        addrs: &'a [(u32, Vec<SocketAddr>)],
        incarnation: Option<u128>,
    },
    Members,
    SetZones {
        auth: &'a [u8],
        epoch: u64,
        zones: &'a [WireZone<'a>],
        incarnation: Option<u128>,
    },
    Zones,
    SetRangeMaps {
        auth: &'a [u8],
        epoch: u64,
        maps: &'a [WireRangeMap<'a>],
        incarnation: Option<u128>,
    },
    RangeMaps,
    RangeStats,
    RangeStatsV2,
    RangeStatsV3,
    RangeWriteMark {
        epoch: u64,
        zone_id: ZoneId,
        lower: &'a [u8],
    },
    GetWatermark,
    BatchApply {
        epoch: u64,
        zone_id: ZoneId,
        sync: bool,
        fill: bool,
        writes: &'a [WireWrite<'a>],
    },
    MixedBatchRead {
        epoch: u64,
        zone_id: ZoneId,
        start: u32,
        reads: &'a [(&'a [u8], MixedReadRole)],
    },
    MultiScan {
        epoch: u64,
        zone_id: ZoneId,
        keys_only: bool,
        start: u32,
        scans: &'a [WireScan<'a>],
        reverse: bool,
    },
    RangeDelete {
        epoch: u64,
        zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: &'a [WireSlice<'a>],
    },
    SyncRangeTombstones {
        zone_id: ZoneId,
        requester: Option<NodeId>,
        digest: u64,
        after: Option<&'a [u8]>,
    },
    GcFloor {
        zone_id: ZoneId,
    },
    CleanupStatus,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OwnedRequest {
    Apply {
        epoch: u64,
        zone_id: ZoneId,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
        version: Version,
    },
    Read {
        epoch: u64,
        zone_id: ZoneId,
        key: Vec<u8>,
    },
    Digest {
        epoch: u64,
        zone_id: ZoneId,
        key: Vec<u8>,
    },
    Scan {
        epoch: u64,
        zone_id: ZoneId,
        lo: Option<Vec<u8>>,
        hi: Option<Vec<u8>>,
        max: u32,
    },
    SyncTree {
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: Vec<u32>,
        requester: Option<NodeId>,
    },
    SyncBucketDigests {
        zone_id: ZoneId,
        num_buckets: u32,
        ids: Vec<u32>,
        after: Option<Vec<u8>>,
    },
    SyncValues {
        zone_id: ZoneId,
        keys: Vec<Vec<u8>>,
    },
    SetMembers {
        auth: Vec<u8>,
        epoch: u64,
        vnodes: u32,
        rf: u32,
        members: Vec<(u32, u32)>,
        addrs: Vec<(u32, Vec<SocketAddr>)>,
        incarnation: Option<u128>,
    },
    Members,
    SetZones {
        auth: Vec<u8>,
        epoch: u64,
        zones: Vec<OwnedZone>,
        incarnation: Option<u128>,
    },
    Zones,
    SetRangeMaps {
        auth: Vec<u8>,
        epoch: u64,
        maps: Vec<OwnedRangeMap>,
        incarnation: Option<u128>,
    },
    RangeMaps,
    RangeStats,
    RangeStatsV2,
    RangeStatsV3,
    RangeWriteMark {
        epoch: u64,
        zone_id: ZoneId,
        lower: Vec<u8>,
    },
    GetWatermark,
    BatchApply {
        epoch: u64,
        zone_id: ZoneId,
        sync: bool,
        fill: bool,
        writes: Vec<OwnedWrite>,
    },
    MixedBatchRead {
        epoch: u64,
        zone_id: ZoneId,
        start: u32,
        reads: Vec<(Vec<u8>, MixedReadRole)>,
    },
    MultiScan {
        epoch: u64,
        zone_id: ZoneId,
        keys_only: bool,
        start: u32,
        scans: Vec<OwnedScan>,
        reverse: bool,
    },
    RangeDelete {
        epoch: u64,
        zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: Vec<OwnedSlice>,
    },
    SyncRangeTombstones {
        zone_id: ZoneId,
        requester: Option<NodeId>,
        digest: u64,
        after: Option<Vec<u8>>,
    },
    GcFloor {
        zone_id: ZoneId,
    },
    CleanupStatus,
}

impl OwnedRequest {
    pub fn route_epoch(&self) -> Option<u64> {
        match *self {
            OwnedRequest::Apply { epoch, .. }
            | OwnedRequest::Read { epoch, .. }
            | OwnedRequest::Digest { epoch, .. }
            | OwnedRequest::Scan { epoch, .. }
            | OwnedRequest::MultiScan { epoch, .. }
            | OwnedRequest::BatchApply { epoch, .. }
            | OwnedRequest::MixedBatchRead { epoch, .. }
            | OwnedRequest::RangeDelete { epoch, .. } => Some(epoch),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    MultiScan {
        start: u32,
        more: bool,
        keys_only: bool,
        pages: Vec<crate::coord::ScanPage>,
    },
    Apply(Applied),
    Read(Option<Stored>),
    Digest(Option<Version>),
    Scan {
        entries: Vec<(Vec<u8>, Stored)>,
        more: bool,
        fragments: Vec<RtFragment>,
        resume: Option<Vec<u8>>,
    },
    Tree(Vec<(u32, u64)>),
    BucketDigests {
        digests: Vec<(Vec<u8>, Version)>,
        more: bool,
        resume: Option<Vec<u8>>,
    },
    Values {
        entries: Vec<(Vec<u8>, Stored)>,
        more: bool,
    },
    Members {
        epoch: u64,
        vnodes: u32,
        rf: u32,
        members: Vec<(u32, u32)>,
        addrs: Vec<(u32, Vec<SocketAddr>)>,
    },
    Zones {
        epoch: u64,
        zones: Vec<OwnedZone>,
    },
    RangeMaps {
        epoch: u64,
        maps: Vec<OwnedRangeMap>,
    },
    RangeStats {
        epoch: u64,
        ranges: Vec<OwnedRangeStat>,
    },
    RangeStatsV2 {
        epoch: u64,
        ranges: Vec<OwnedRangeStat>,
    },
    RangeStatsV3 {
        epoch: u64,
        accounting_epoch: u64,
        accounting_ready: bool,
        ranges: Vec<OwnedRangeStat>,
        recovery_seeding: Option<bool>,
        covered_empty: Vec<u32>,
    },
    Batch(Vec<Applied>),
    MixedBatchRead {
        start: u32,
        results: Vec<MixedReadResult>,
        more: bool,
    },
    RangeTombstones {
        same: bool,
        more: bool,
        fragments: Vec<RtFragment>,
    },
    StaleEpoch(u64),
    Watermark(u64),
    GcFloor(u64),
    CleanupStatus {
        ring_epoch: u64,
        gc_grace_ms: u64,
        records: u64,
        unsettled: Vec<UnsettledZone>,
    },
    Denied,
    Busy,
    Err(String),
}


mod decode;
mod encode;

pub use decode::*;
pub use encode::*;

#[cfg(test)]
mod tests;
