use std::sync::OnceLock;

use vals_spdk::{AsyncRead, BlockDevice};
use vals_store::{Engine, EngineError, RecoveredVersions};

use crate::hlc::{physical_now_ms, Hlc, Version};
use crate::partition::RangeMap;
use crate::ring::NodeId;


#[allow(clippy::large_enum_variant)]
pub enum StagedApply {
    Done(Applied),
    NeedsRunRead {
        probe: vals_store::StagedVersion,
        gen: u64,
    },
}

pub enum StagedApplyOutcome {
    Applied(Applied),
    Retry,
}

pub(crate) enum ApplyRetryMode {
    Fill,
    Unconditional(ApplyMode),
}

pub(crate) enum ApplyRetryError {
    Apply(EngineError),
    Probe(EngineError),
}

impl ApplyRetryError {
    pub(crate) fn into_engine(self) -> EngineError {
        match self {
            ApplyRetryError::Apply(error) | ApplyRetryError::Probe(error) => error,
        }
    }
}

pub(crate) async fn apply_staged_with_retry<D, F>(
    store: &std::cell::RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &crate::net::ReadGate,
    key: &[u8],
    value: Option<&[u8]>,
    version: Version,
    wall_ms: u64,
    mode: ApplyRetryMode,
    mut before_commit: F,
) -> Result<Option<Applied>, ApplyRetryError>
where
    D: BlockDevice + AsyncRead,
    F: FnMut(),
{
    const MAX_ATTEMPTS: usize = 8;
    for _ in 0..MAX_ATTEMPTS {
        let _guard = gate.acquire().await;
        let staged = match mode {
            ApplyRetryMode::Fill => store
                .borrow_mut()
                .apply_stage_with(key, value, version, wall_ms, LiveFloor::Waive)
                .map_err(ApplyRetryError::Apply)?,
            ApplyRetryMode::Unconditional(mode) => store
                .borrow_mut()
                .apply_unconditional_stage(key, value, version, wall_ms, mode, LiveFloor::Enforce)
                .map_err(ApplyRetryError::Apply)?,
        };
        let (probe, gen) = match staged {
            StagedApply::Done(applied) => return Ok(Some(applied)),
            StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
        };
        let probe_timer = vals_store::attribution::Timer::new(&vals_store::attribution::APPLY_PROBE_WALL, None);
        let run_old = match mode {
            ApplyRetryMode::Fill => VersionedStore::version_from_probe(probe, dev)
                .await
                .map_err(ApplyRetryError::Probe)?,
            ApplyRetryMode::Unconditional(_) => {
                if let vals_store::StagedVersion::Runs { probes, .. } = &probe {
                    crate::observe::counters()
                        .apply_probe_runs
                        .add(probes.len() as u64);
                }
                let probe_bytes = probe.probe_bytes();
                let (run_old, blocks) = VersionedStore::version_from_probe_counted(probe, dev)
                    .await
                    .map_err(ApplyRetryError::Probe)?;
                crate::observe::counters().apply_probe_blocks.add(blocks);
                crate::observe::counters().apply_probe_bytes.add(probe_bytes);
                run_old
            }
        };
        drop(probe_timer);
        before_commit();
        match store
            .borrow_mut()
            .apply_commit_staged(key, value, version, run_old, gen, wall_ms)
            .map_err(ApplyRetryError::Apply)?
        {
            StagedApplyOutcome::Applied(applied) => return Ok(Some(applied)),
            StagedApplyOutcome::Retry => continue,
        }
    }
    Ok(None)
}

struct DirtySnapshot {
    buckets: Vec<(u32, u64)>,
    aggregate: (u64, u64),
    placement: u64,
    accounted: u64,
}

pub(crate) struct TreeScopeToken {
    buckets: Vec<(u32, u64)>,
    placement: u64,
}

#[derive(PartialEq, Eq)]
struct SeedPlacementInputs {
    ring_epoch: u64,
    catalog_epoch: u64,
    zone_id: crate::zone::ZoneId,
    config: Option<crate::zone::ZoneConfig>,
    range_map: Option<RangeMap>,
    rf: usize,
    vnodes: u32,
    members: Vec<(NodeId, u32)>,
}

struct SeedPlacement {
    range_epoch: u64,
    inputs: SeedPlacementInputs,
}

#[derive(Clone)]
struct RegroupSpan {
    lo: Option<Box<[u8]>>,
    hi: Option<Box<[u8]>>,
    after: Option<Box<[u8]>>,
}

struct Regroup {
    old: PlacementFn,
    spans: Vec<RegroupSpan>,
    depth: u32,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RegroupKnockout {
    None,
    Misroute,
    NoCompose,
    ComposeNewSpansOnly,
}

#[derive(Clone)]
enum RecountChild {
    Derived,
    Scan { after: Option<Box<[u8]>>, covered: u64, done: bool },
}

#[derive(Clone)]
struct SplitRecount {
    lo: Option<Box<[u8]>>,
    hi: Option<Box<[u8]>>,
    cuts: Vec<Box<[u8]>>,
    children: Vec<RecountChild>,
    total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecountStep {
    Done,
    More,
    Paused,
    Abandoned,
}

struct LogicalRangeCounts {
    epoch: u64,
    map: RangeMap,
    counts: Option<Vec<u64>>,
    generation: u64,
    recounts: Vec<SplitRecount>,
}

enum SeedCursor<D: BlockDevice> {
    Stream(vals_store::run::RunVersionStream<D>),
    Mem { entries: Vec<(Vec<u8>, u128)>, pos: usize, bytes: u64 },
}

impl<D: BlockDevice> SeedCursor<D> {
    fn resident_bound_bytes(&self) -> u64 {
        match self {
            Self::Stream(cursor) => cursor.resident_bound_bytes(),
            Self::Mem { bytes, .. } => *bytes,
        }
    }

    fn next_step_reserving(
        &mut self,
        reserve: impl FnMut(u64) -> bool,
    ) -> Result<Option<vals_store::run::SidecarCursorStep>, vals_store::SnapshotError> {
        match self {
            Self::Stream(cursor) => cursor.next_step_reserving(reserve),
            Self::Mem { entries, pos, bytes } => {
                if *pos == entries.len() {
                    *entries = Vec::new();
                    *bytes = 0;
                    return Ok(Some(vals_store::run::SidecarCursorStep::Done));
                }
                let head = std::mem::take(&mut entries[*pos]);
                *pos += 1;
                *bytes -= head.0.capacity() as u64;
                Ok(Some(vals_store::run::SidecarCursorStep::Entry(head)))
            }
        }
    }
}

struct StreamSeed<D: BlockDevice> {
    cursors: Vec<SeedCursor<D>>,
    heads: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize, u128)>>,
    refill: Vec<usize>,
    finished: Vec<bool>,
    pending: Vec<vals_store::run::RunStreamSource<D>>,
    pin: Option<vals_store::engine::RunPin>,
    ticket: Option<seed_policy::PinTicket>,
    mem_pos: usize,
    runs_epoch: u64,
    windows_bytes: u64,
    head_bytes: u64,
    publish_ranges: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SeedEnd {
    Restart,
    PlacementReset,
    GenerationReset,
    RamShed,
    CorruptSidecar,
    Error,
    Dropped,
}

/// One replica's store: a `vals-store` engine plus the versioned
/// last-writer-wins rules of the cluster.
///
/// Writes and deletes carry a client [`Version`]; the greater
/// version wins, deletes are tombstones that hide older values, and a
/// replica refuses versions too far ahead of its clock. A data node owns its
/// stores; hand one to a [`Node`](crate::Node) constructor rather than
/// writing to it yourself while it is served.
pub struct VersionedStore<D: BlockDevice> {
    engine: Engine<D>,
    hlc: Hlc,
    gc_grace_ms: u64,
    durable_gc_watermark_ms: std::cell::Cell<u64>,
    live_admission_cutoff_ms: std::cell::Cell<u64>,
    last_tombstone_gc_ms: Option<u64>,
    applied_high_ms: std::cell::Cell<u64>,
    versions: VersionIndex,
    seed_progress: std::cell::RefCell<Option<SeedProgress>>,
    stream_seed: std::cell::RefCell<Option<StreamSeed<D>>>,
    seed_restart_count: std::cell::Cell<u32>,
    seed_started: std::cell::Cell<Option<std::time::Instant>>,
    seed_deferred: std::cell::Cell<bool>,
    seed_driver: std::cell::Cell<bool>,
    seed_recovery: std::cell::Cell<bool>,
    pinned_reported: std::cell::Cell<u64>,
    seed_pin_ticket: std::cell::Cell<u64>,
    #[cfg(test)]
    seed_generation_resets: std::cell::Cell<u64>,
    #[cfg(test)]
    seed_run_causes: std::cell::Cell<[u64; 4]>,
    #[cfg(test)]
    seed_folds: std::cell::Cell<u64>,
    #[cfg(test)]
    seed_spills: std::cell::Cell<u64>,
    node: NodeId,
    placement: std::cell::RefCell<Option<PlacementFn>>,
    placement_ranges: std::cell::RefCell<Option<std::sync::Arc<RangeMap>>>,
    placement_generation: std::cell::Cell<u64>,
    seed_placement: std::cell::RefCell<Option<SeedPlacement>>,
    regroup: std::cell::RefCell<Option<Regroup>>,
    rt_accounting: std::cell::RefCell<rt_accounting::RtAccounting>,
    #[cfg(test)]
    regroup_knockout: std::cell::Cell<RegroupKnockout>,
    logical_range_counts: std::cell::RefCell<Option<LogicalRangeCounts>>,
    #[cfg(test)]
    split_recount_disabled: std::cell::Cell<bool>,
    #[cfg(test)]
    split_recount_visited: std::cell::Cell<u64>,
    #[cfg(test)]
    split_recount_fallbacks: std::cell::Cell<u64>,
    pending_forget: std::collections::HashMap<Vec<u8>, Version>,
    accumulating_forget: std::collections::HashMap<Vec<u8>, Version>,
    pending_released: vals_store::range_tombstone::FragmentSet,
    accumulating_released: vals_store::range_tombstone::FragmentSet,
    cleanup_placement: CleanupPlacement,
    accumulating_forget_placement: Option<CleanupPlacement>,
    pending_forget_placement: Option<CleanupPlacement>,
    accumulating_forget_bytes: usize,
    accumulating_forget_wrapped: bool,
    #[cfg(test)]
    forget_batch_cap: usize,
}

impl<D: BlockDevice> Drop for VersionedStore<D> {
    fn drop(&mut self) {
        crate::ram_broker::clear_waiter(crate::ram_broker::Class::Seed, self as *const Self as usize);
        seed_policy::adjust_pinned_node(self.pinned_reported.get(), 0);
        if self.stream_seed.get_mut().is_some() {
            self.note_seed_end(SeedEnd::Dropped);
        }
    }
}

mod codec;
mod lifecycle;
mod maintenance;
mod read;
mod rt_accounting;
mod seed_permit;
pub(crate) mod seed_policy;
mod version_index;
mod write;

use codec::*;
use version_index::*;

pub use codec::{
    blind_append_enabled, configure_blind_append, execute_read_plan, split_stored_blob, Applied,
    CasResult, DigestCursorPage, DigestPage, EntryPage, PlacementFn, PlanPage, SpanPage, Stored,
    ValuePlan,
};
pub use version_index::LeafDigestError;
pub(crate) use codec::{
    adopt_blob, decode_read, execute_read_plan_checked, pack_version, unconditional_apply_mode,
    unpack_version, ApplyMode, LiveFloor,
};
pub use lifecycle::{
    ensure_seeded_cooperative_cell, ensure_tree_ready_cooperative,
    ensure_tree_ready_cooperative_cell,
};
pub(crate) use lifecycle::{logical_recount_turn_with, regroup_turn_with};
pub(crate) use rt_accounting::rt_fold_turn_with;
pub(crate) use maintenance::CleanupPlacement;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod split_recount_tests;
#[cfg(test)]
mod seed_pin_tests;
#[cfg(test)]
mod regroup_tests;
#[cfg(test)]
mod range_delete_tests;
