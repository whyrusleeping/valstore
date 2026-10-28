//! `vals-store`: the single-node storage engine under every vals replica.
//!
//! An [`Engine`] stores keys and values on one [`vals_spdk::BlockDevice`]
//! region. Writes append to a checksummed value log (`vlog`), and an index
//! maps each key to its newest record. The index starts in memory and is
//! flushed to sorted on-disk runs, which are merged in the background, so the
//! index does not have to fit in RAM. On open, the engine loads its manifest
//! and runs and replays the log tail. Compaction reclaims space held by
//! overwritten and deleted values.
//!
//! Keys can be unordered (hash index) or ordered (for range scans); see
//! [`Config`]. Every record carries a caller-supplied version, which the
//! cluster layer uses for last-writer-wins replication. Most applications use
//! the engine through `vals-cluster` rather than directly.
//!
//! # Supported API
//!
//! For direct use, the supported surface is [`Engine::create`],
//! [`Engine::open`], [`Engine::put`], [`Engine::get`], [`Engine::version`],
//! [`Engine::scan`], [`Engine::scan_bounded`], [`Engine::sync`] and the
//! low-level [`Engine::delete`], with [`Config`], [`Ordering`] and
//! [`EngineError`], over a `vals_spdk` block device. The [`Engine`] type
//! documents the version, delete, durability and maintenance rules, which
//! differ from the replicated API: the engine is not interchangeable with
//! `vals-cluster`'s `VersionedStore`, which adds replication's versioned
//! deletes and clock checks on top of an engine.
//!
//! Every other public item (flush, compaction and reclaim plans, runs,
//! manifests, snapshots, dump and log-scan helpers, pacing) is the interface
//! `vals-cluster` uses to drive maintenance and recovery. It may change in
//! any revision. The crate is not published and has no API freeze.


#[cfg(all(test, feature = "heap-profile"))]
mod heap_profile;

pub mod alloc;
pub mod attribution;
pub mod crc;
pub mod dump;
pub mod engine;
pub mod index;
pub mod keyfilter;
pub mod manifest;
pub mod pace;
pub mod range_tombstone;
pub mod run;
pub mod snapshot;
pub mod snapshot_io;
pub mod vlog;

pub use dump::{
    read_range_tombstones, scan_vlog_records, scan_vlog_records_paced, scan_vlog_segments_parallel,
    scan_vlog_segments_parallel_paced, ReadPacer, ScanError, VlogScanStats,
};
pub use engine::{
    build_and_write_flush, compact_and_write, compact_and_write_cancellable, inner_window_merges_total,
    configure_admit_band_pct, configure_winner_directory, degrade_winner_directory, resolve_run_probe, resolve_run_probe_counted, winner_directory_operational,
    winner_directory_requested, CompactPlan,
    CapacityStats, CompactRunsStats, CompactStats, Config, CoverScanPlan, DeallocStepStats,
    DigestCheckpoint, DigestPagePlan, Engine, EngineError, RtScanStats, FlushCommit, FlushCommitStep,
    FlushPlan, FlushPlanStep, ForgetRunsStats, ProbeList, RecoveredVersions, ScanSpanPage,
    ScanCoveredBudget, StagedGet, StagedScanDone, StagedScanMerge, StagedScanPage, StagedScanStep, StagedVersion,
    StagedScanRevMerge, StagedScanRevPage, SCAN_COVERED_BUDGET,
    MergeKind, WinnerCountBlocks, WinnerCountPlan, WrittenCompact, WrittenFlush, FLUSH_COMMIT_SLICE_ENTRIES,
    FLUSH_SLICE_ENTRIES,
};
pub use keyfilter::KeyFilterStats;
pub use manifest::{Manifest, RunMeta};
pub use pace::{
    maint_job_queued, maint_job_started, maint_pace_backlog_unpaced_ns, maint_pace_totals,
};
pub use run::{RunBuildOpts, RunHandle, SidecarVersions};
pub use snapshot::SnapshotEntry;
pub use snapshot_io::{load_manifest, write_manifest, LiveSnapshot, SnapshotError};
pub use vlog::{vlog_stall_totals, vlog_sync_totals, ReadPlan, Record, RecordKind, SpanReadPlan};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ordering {
    Sorted,
    Unsorted,
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_spdk::{BlockDevice, MemBlockDevice, DEFAULT_BLOCK_SIZE};

    #[test]
    fn block_layer_is_wired() {
        let dev = MemBlockDevice::new(DEFAULT_BLOCK_SIZE, 8);
        let buf = vec![7u8; DEFAULT_BLOCK_SIZE];
        dev.write_at(0, &buf).unwrap();
        let mut out = vec![0u8; DEFAULT_BLOCK_SIZE];
        dev.read_at(0, &mut out).unwrap();
        assert_eq!(buf, out);
        assert_eq!(Ordering::Sorted, Ordering::Sorted);
    }
}
