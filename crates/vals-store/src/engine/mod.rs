use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;

use vals_spdk::{AsyncRead, BlockDevice, BlockError, WriteHandle};

use crate::alloc::{Allocator, SegmentId};
use crate::index::{new_index, FxBuildHasher, Index, IndexEntry, PackedEntry};
use crate::manifest::{Manifest, RunMeta};
use crate::range_tombstone::{FragmentCapExceeded, FragmentSet, MaxTree, RtRecord};
use crate::run::{self, DeviceRun, RunBuildOpts};
use crate::snapshot::SnapshotEntry;
use crate::snapshot_io::{self, LiveSnapshot, SnapshotError};
use crate::vlog::{
    replay_segment, LogError, Record, RecordKind, ValueLog, ValuePtr, HEADER_LEN, SEG_HEADER_LEN,
};
use crate::Ordering;

const MAX_INFLIGHT_HEADER_WIPES: usize = 256;

struct OpenRun<D: BlockDevice> {
    meta: RunMeta,
    run: DeviceRun<D>,
    id: u64,
    covered: bool,
    rebuild_covered: bool,
    victim_lives: std::cell::RefCell<Option<VictimLives>>,
    rts: Vec<RtRecord>,
    max_tree: Option<MaxTree>,
}

type VictimLives = ((u64, usize), Option<Vec<(SegmentId, u64)>>);

impl<D: BlockDevice> OpenRun<D> {
    fn new(meta: RunMeta, run: DeviceRun<D>, covered: bool) -> Self {
        static NEXT_RUN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        OpenRun {
            meta,
            run,
            id: NEXT_RUN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            covered,
            rebuild_covered: false,
            victim_lives: std::cell::RefCell::new(None),
            rts: Vec::new(),
            max_tree: None,
        }
    }

    fn with_rts(mut self, mut rts: Vec<RtRecord>) -> Self {
        rts.sort_by_key(|r| r.seq);
        self.rts = rts;
        self
    }

    fn with_max_tree(mut self, rt_active: bool) -> Self {
        if rt_active {
            self.build_max_tree();
        }
        self
    }

    fn build_max_tree(&mut self) -> u64 {
        if self.max_tree.is_some() || !self.run.supports_range_tombstones() || self.run.index_len() == 0 {
            return 0;
        }
        let run = &self.run;
        let tree = MaxTree::build(run.index_len(), |i| run.block_max_hlc(i).unwrap_or(u64::MAX));
        let blocks = tree.blocks() as u64;
        self.max_tree = Some(tree);
        blocks
    }

    fn rewrite_bound(&self, out_version: u16) -> usize {
        self.meta.handle.len as usize + run::rewrite_growth_bytes(&self.run, out_version)
    }

    fn rewrite_output_bound<'a>(inputs: impl Iterator<Item = &'a Self> + Clone) -> usize
    where
        D: 'a,
    {
        let out_version = run::merge_write_version(inputs.clone().map(|or| or.run.format_version()));
        inputs.map(|or| or.rewrite_bound(out_version)).sum()
    }
}

pub type KvPair = (Vec<u8>, Vec<u8>);

pub type ScanSpanPage = (Vec<(Vec<u8>, u128)>, crate::vlog::SpanReadPlan, bool);


mod dealloc;
pub use dealloc::DeallocStepStats;

mod diagnostic;
pub use diagnostic::KeyVersion;
mod digest;
mod flush;
mod keyfilter;
pub use keyfilter::CoverScanPlan;
use keyfilter::{CoverScanBackoff, SinkKind};
mod merge;
mod pins;
pub use pins::{PinnedRuns, RunPin, RunPinState};
mod range_delete;
pub use range_delete::{rt_collected_total, rt_covered_dropped_total, rt_dominated_dropped_total, RangeDeleteApplied, RtScanStats};
use range_delete::{RtCompaction, RtState};
mod read;
mod reclaim;
mod recovery;
mod scan;
mod scan_rev;
mod scan_rev_staged;
pub use scan_rev_staged::{StagedScanRevMerge, StagedScanRevPage};
mod space;
mod winner;
mod write;

pub use digest::{DigestCheckpoint, DigestPagePlan, StagedVersion};
pub use flush::{build_and_write_flush, FlushCommit, FlushCommitStep, FlushPlan, FlushPlanStep, WrittenFlush, FLUSH_COMMIT_SLICE_ENTRIES, FLUSH_SLICE_ENTRIES};
pub use merge::{compact_and_write, compact_and_write_cancellable, inner_window_merges_total, CompactPlan, MergeKind, WrittenCompact};
pub use read::{resolve_run_probe, resolve_run_probe_counted, ProbeList, StagedGet};
pub use scan::{
    ScanCoveredBudget, StagedScanDone, StagedScanMerge, SCAN_COVERED_BUDGET, StagedScanPage, StagedScanStep, WinnerCountBlocks, WinnerCountPlan,
};
pub use merge::configure_admit_band_pct;
pub use winner::{
    configure_winner_directory, degrade_winner_directory, winner_directory_operational,
    winner_directory_requested, AuthoritativePut, RecoveredVersions, WinnerProbeStats,
    WinnerReadToken,
};

use flush::*;
use merge::*;
use winner::*;

type VersionDigestPage = (Vec<(Vec<u8>, u128)>, bool);
type ResumableDigestPage = (Vec<(Vec<u8>, u128)>, bool, Option<Vec<u8>>);

type VersionedScanPage = (Vec<(Vec<u8>, Vec<u8>, u128)>, bool);

type WinnersPage = (Vec<(Vec<u8>, IndexEntry)>, bool);

type ResolvedItems = (Vec<(u64, ValuePtr)>, Vec<(usize, u128)>);

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactStats {
    pub segments_freed: usize,
    pub live_copied: usize,
    pub tombstones_retained: usize,
    pub dead_dropped: usize,
    pub gc_dropped: usize,
    pub victims_scanned: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct CompactRunsStats {
    pub runs_merged: usize,
    pub entries_written: usize,
    pub segments_freed: usize,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub current_dropped_tombstones: Vec<(Vec<u8>, u128)>,
    pub covered_dropped: u64,
    pub collected_range_tombstones: Vec<RtRecord>,
    pub released_range_tombstones: usize,
    pub dominated_range_tombstones: usize,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ForgetRunsStats {
    pub dropped: Vec<(Vec<u8>, u128)>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CapacityStats {
    pub mutable_entries: u64,
    pub mutable_logical_bytes: u64,
    pub mutable_allocated_bytes: u64,
    pub flush_protection_bytes: u64,
    pub run_count: u64,
    pub run_entries: u64,
    pub run_disk_bytes: u64,
    pub sidecar_disk_bytes: u64,
    pub live_value_bytes: u64,
    pub run_resident_bytes: u64,
    pub fence_resident_bytes: u64,
    pub fence_payload_bytes: u64,
    pub filter_resident_bytes: u64,
    pub run_filter_resident_bytes: u64,
    pub run_filter_payload_bytes: u64,
    pub manifest_resident_bytes: u64,
    pub winner_directory_bytes: u64,
}

#[derive(Default)]
struct ValueLivenessAcc {
    segs: Vec<SegmentId>,
    seg_bytes: Vec<u64>,
    idx: std::collections::HashMap<SegmentId, usize>,
    bytes: u64,
}

impl ValueLivenessAcc {
    fn add(&mut self, ptr: ValuePtr) {
        self.bytes += ptr.len as u64;
        match self.idx.get(&ptr.segment) {
            Some(&i) => self.seg_bytes[i] += ptr.len as u64,
            None => {
                self.idx.insert(ptr.segment, self.segs.len());
                self.segs.push(ptr.segment);
                self.seg_bytes.push(ptr.len as u64);
            }
        }
    }

    fn into_parts(self) -> (Vec<SegmentId>, Vec<u64>, u64) {
        (self.segs, self.seg_bytes, self.bytes)
    }
}

fn value_liveness(entries: &[SnapshotEntry]) -> (Vec<SegmentId>, Vec<u64>, u64) {
    let mut acc = ValueLivenessAcc::default();
    for e in entries {
        acc.add(e.ptr);
    }
    acc.into_parts()
}

fn tombstone_min_version(entries: &[SnapshotEntry]) -> Option<u128> {
    entries
        .iter()
        .filter(|e| e.tombstone)
        .map(|e| e.version)
        .min()
}

fn note_mutable_tombstone(current: Option<(u128, u64)>, version: u128, seq: u64) -> (u128, u64) {
    current.map_or((version, seq), |(min, max_seq)| (min.min(version), max_seq.max(seq)))
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub seg_blocks: u64,
    pub reserved_blocks: u64,
    pub ordering: Ordering,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            seg_blocks: 256,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        }
    }
}

#[derive(Debug)]
pub enum EngineError {
    Log(LogError),
    Io(BlockError),
    NotSorted,
    GeometryMismatch {
        recorded_block_size: u32,
        open_block_size: usize,
    },
    InsufficientHeadroom {
        needed_segments: u64,
        free_segments: u64,
    },
    Poisoned,
    PartialForget {
        dropped: Vec<(Vec<u8>, u128)>,
        source: Box<EngineError>,
    },
    FlushInFlight,
    MaintenanceInFlight,
    CompactionConflict,
    Cancelled,
    Snapshot(SnapshotError),
    MemtableIntegrity(Box<crate::index::IndexIntegrityError>),
    EncodeIntegrity(Box<crate::run::EncodeIntegrityError>),
    CorruptValueRecord {
        key: Box<[u8]>,
        ptr: ValuePtr,
        reason: &'static str,
    },
    InvalidRange,
    RangeTombstonesUnsupported,
    RangeTombstoneCap(FragmentCapExceeded),
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Log(e) => write!(f, "{e}"),
            EngineError::Io(e) => write!(f, "{e}"),
            EngineError::NotSorted => write!(f, "scan requires a sorted zone"),
            EngineError::GeometryMismatch {
                recorded_block_size,
                open_block_size,
            } => write!(
                f,
                "on-device geometry mismatch: segment header records block_size \
                 {recorded_block_size} but the device is opened at {open_block_size}; \
                 refusing to recover (replay would mis-frame the log)"
            ),
            EngineError::InsufficientHeadroom {
                needed_segments,
                free_segments,
            } => write!(
                f,
                "compaction needs ~{needed_segments} free segments to hold the copied \
                 live data but only {free_segments} are free; refusing to start"
            ),
            EngineError::Poisoned => write!(
                f,
                "engine is poisoned: a failed compaction could not be rolled back \
                 (device refused the invalidation writes); mutations are refused \
                 until the engine is reopened and recovery replays the log"
            ),
            EngineError::PartialForget { dropped, source } => write!(
                f,
                "bounded forget or tombstone GC failed after dropping {} key(s) from the index: {source}",
                dropped.len()
            ),
            EngineError::FlushInFlight => write!(
                f,
                "delete refused: a non-blocking flush is in flight and delete removes \
                 the memtable entry (a snapshotted run could resurrect the key); \
                 retry after the flush commits or aborts"
            ),
            EngineError::MaintenanceInFlight => write!(
                f,
                "truncate refused: an off-core flush or merge is in flight and still owns \
                 reserved segments; retry after it commits or aborts"
            ),
            EngineError::CompactionConflict => write!(
                f,
                "compaction inputs or reserved output changed before commit; retrying"
            ),
            EngineError::Cancelled => write!(f, "compaction cancelled"),
            EngineError::Snapshot(e) => write!(f, "{e}"),
            EngineError::MemtableIntegrity(e) => write!(f, "{e}"),
            EngineError::EncodeIntegrity(e) => write!(f, "{e}"),
            EngineError::InvalidRange => write!(f, "range delete refused: the span is empty (lo >= hi)"),
            EngineError::RangeTombstonesUnsupported => write!(
                f,
                "range delete refused: runs are written below format v6 (VALS_RUN_WRITE_VERSION), \
                 which cannot persist range tombstones"
            ),
            EngineError::RangeTombstoneCap(e) => write!(f, "{e}"),
            EngineError::CorruptValueRecord { key, ptr, reason } => write!(
                f,
                "value-log rewrite refused: the record at segment {} offset {} len {} for a {}-byte key \
                 is corrupt ({reason}); the merge is not published",
                ptr.segment.0,
                ptr.offset,
                ptr.len,
                key.len()
            ),
        }
    }
}

impl std::error::Error for EngineError {}

impl EngineError {
    pub fn is_backpressure(&self) -> bool {
        matches!(self, EngineError::Log(LogError::Backpressure))
    }

    pub fn is_device_full(&self) -> bool {
        matches!(self, EngineError::Log(LogError::DeviceFull))
    }

    pub fn memtable_integrity(&self) -> Option<&crate::index::IndexIntegrityError> {
        match self {
            EngineError::MemtableIntegrity(e) => Some(e),
            _ => None,
        }
    }

    pub fn encode_integrity(&self) -> Option<&crate::run::EncodeIntegrityError> {
        match self {
            EngineError::EncodeIntegrity(e) => Some(e),
            _ => None,
        }
    }

    pub fn corrupt_value_record(&self) -> Option<(&[u8], ValuePtr, &'static str)> {
        match self {
            EngineError::CorruptValueRecord { key, ptr, reason } => Some((key, *ptr, reason)),
            _ => None,
        }
    }
}

impl From<LogError> for EngineError {
    fn from(e: LogError) -> Self {
        EngineError::Log(e)
    }
}
impl From<SnapshotError> for EngineError {
    fn from(e: SnapshotError) -> Self {
        match e {
            SnapshotError::EncodeIntegrity(e) => EngineError::EncodeIntegrity(e),
            e => EngineError::Snapshot(e),
        }
    }
}
impl From<BlockError> for EngineError {
    fn from(e: BlockError) -> Self {
        EngineError::Io(e)
    }
}

struct ShadowSampleEntry {
    key: Vec<u8>,
    version: u128,
    len: u64,
    bloom: run::BloomKey,
    run_shadowed: bool,
}

#[derive(Default)]
struct ShadowSample {
    entries: Vec<ShadowSampleEntry>,
    sampled_bytes: u64,
    value_bytes: u64,
    fpr: f64,
    entries_sampled: u64,
    tombstones: Vec<u128>,
}

#[derive(Default)]
struct ShadowCache {
    gen: u64,
    mutable_gen: u64,
    accounted_seq: u64,
    run_ids: Vec<u64>,
    samples: std::collections::HashMap<u64, ShadowSample>,
    computed: bool,
    gated: bool,
    per_run: Vec<u64>,
    per_run_fold: Vec<u64>,
    total: u64,
}

struct ShadowFoldPending {
    predicted_bytes: u64,
}

#[derive(Default)]
struct ShadowState {
    cache: std::cell::RefCell<ShadowCache>,
    cooldown_until_epoch: u64,
    backoff_shift: u32,
    pending: Option<ShadowFoldPending>,
    folds: u64,
    disarms: u64,
}

pub struct Engine<D: BlockDevice> {
    dev: Arc<D>,
    alloc: Allocator,
    log: ValueLog,
    index: Box<dyn Index>,
    block_size: usize,
    ordering: Ordering,
    poisoned: bool,
    dealloc_error_warned: bool,
    runs: Vec<OpenRun<D>>,
    manifest_gen: u64,
    storage_gen: u64,
    mutable_gen: u64,
    live_manifest: Option<LiveSnapshot>,
    last_flush_epoch: u64,
    last_flush_seq: u64,
    mutable_tombstones: Option<(u128, u64)>,
    gc_fold_cursor: usize,
    flush_in_flight: bool,
    flush_snapshot: Option<FlushSnapshotState>,
    flush_cleanup: Vec<SnapshotEntry>,
    flush_value_segs: std::collections::HashSet<SegmentId>,
    compact_in_flight: bool,
    runs_removed_epoch: u64,
    compact_cursor: SegmentId,
    maint_reserve_enabled: bool,
    shadow: ShadowState,
    merge_policy: MergePolicy,
    pressure_infeasible_streak: u32,
    gc_excluded_segs: std::collections::HashSet<SegmentId>,
    shed_rng: u64,
    winner_directory: Option<WinnerDirectory>,
    key_filter: crate::keyfilter::KeyFilter,
    cover_scan_token: Option<u64>,
    cover_scan_backoff: std::collections::HashMap<u64, CoverScanBackoff>,
    run_pins: std::cell::RefCell<pins::RunPins>,
    rts: RtState,
    #[cfg(test)]
    test_orphan_reclaim: bool,
    #[cfg(test)]
    test_run_pin_pressure: bool,
    #[cfg(test)]
    test_key_filter_knockout: bool,
    #[cfg(test)]
    test_kf_knockouts: keyfilter::KeyFilterKnockouts,
    #[cfg(any(test, feature = "test-fixtures"))]
    test_flush_source_flip: Option<(usize, usize, u8)>,
    #[cfg(any(test, feature = "test-fixtures"))]
    test_fail_forget_append: Option<Vec<u8>>,
}


#[cfg(test)]
pub(crate) mod tests;
