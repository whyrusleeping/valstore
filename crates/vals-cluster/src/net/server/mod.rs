use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};

type FastMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<vals_store::index::FxHasher>>;

static ZONE_FREE_MINIMA: OnceLock<Mutex<HashMap<usize, u64>>> = OnceLock::new();
static ZONE_DROP_PENDING_AGES: OnceLock<Mutex<HashMap<usize, u64>>> = OnceLock::new();
static SWEEP_PASS_AGES: OnceLock<Mutex<HashMap<usize, u64>>> = OnceLock::new();
static MAX_RUN_BYTES: OnceLock<Mutex<HashMap<usize, u64>>> = OnceLock::new();

static KEY_FILTER_MAXIMA: OnceLock<Mutex<HashMap<usize, [u64; 5]>>> = OnceLock::new();

fn publish_key_filter_maxima(id: usize, values: Option<[u64; 5]>) {
    let mut shards = KEY_FILTER_MAXIMA.get_or_init(|| Mutex::new(HashMap::new()))
        .lock().unwrap_or_else(|e| e.into_inner());
    match values {
        Some(values) => { shards.insert(id, values); }
        None => { shards.remove(&id); }
    }
    let mut maxima = [0u64; 5];
    for values in shards.values() {
        for (max, value) in maxima.iter_mut().zip(values) {
            *max = (*max).max(*value);
        }
    }
    let g = crate::observe::gauges();
    g.key_filter_max_sub_filters.set(maxima[0]);
    g.key_filter_max_est_fpr_ppm.set(maxima[1]);
    g.key_filter_max_uncovered_runs.set(maxima[2]);
    g.key_filter_rebuild_wait_seconds_max.set(maxima[3]);
    g.key_filter_rebuild_open_seconds_max.set(maxima[4]);
    g.key_filter_rebuild_wait_seconds_total.set(vals_store::keyfilter::rebuild_wait_seconds_total());
    g.key_filter_rebuild_urgent_grants.set(vals_store::keyfilter::rebuild_urgent_grants());
}

#[cfg(test)]
mod key_filter_metrics_tests {
    use super::*;

    #[test]
    fn maxima_span_shards_and_remove_teardown_values() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        publish_key_filter_maxima(1, Some([4, 20_000, 3, 12, 45]));
        publish_key_filter_maxima(2, Some([7, 10_000, 0, 30, 25]));
        let values = || {
            let g = crate::observe::gauges();
            [g.key_filter_max_sub_filters.get(), g.key_filter_max_est_fpr_ppm.get(),
                g.key_filter_max_uncovered_runs.get(), g.key_filter_rebuild_wait_seconds_max.get(),
                g.key_filter_rebuild_open_seconds_max.get()]
        };
        assert_eq!(values(), [7, 20_000, 3, 30, 45]);
        publish_key_filter_maxima(1, Some([2, 5_000, 0, 0, 0]));
        assert_eq!(values(), [7, 10_000, 0, 30, 25]);
        publish_key_filter_maxima(2, None);
        assert_eq!(values(), [2, 5_000, 0, 0, 0]);
        publish_key_filter_maxima(1, None);
        assert_eq!(values(), [0; 5]);
    }
}

fn publish_max_run_bytes(id: usize, bytes: Option<u64>) -> u64 {
    let maxima = MAX_RUN_BYTES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut maxima = maxima.lock().unwrap_or_else(|e| e.into_inner());
    match bytes {
        Some(bytes) => {
            maxima.insert(id, bytes);
        }
        None => {
            maxima.remove(&id);
        }
    }
    maxima.values().copied().max().unwrap_or(0)
}

fn publish_sweep_pass_age(id: usize, age: Option<u64>) -> u64 {
    let ages = SWEEP_PASS_AGES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut ages = ages.lock().unwrap_or_else(|e| e.into_inner());
    match age {
        Some(age) => {
            ages.insert(id, age);
        }
        None => {
            ages.remove(&id);
        }
    }
    ages.values().copied().max().unwrap_or(0)
}

fn publish_zone_free_min(id: usize, free: Option<u64>) -> u64 {
    let minima = ZONE_FREE_MINIMA.get_or_init(|| Mutex::new(HashMap::new()));
    let mut minima = minima.lock().unwrap_or_else(|e| e.into_inner());
    match free {
        Some(free) => {
            minima.insert(id, free);
        }
        None => {
            minima.remove(&id);
        }
    }
    minima.values().copied().min().unwrap_or(0)
}

fn publish_zone_drop_pending_age(id: usize, age: Option<u64>) -> u64 {
    let ages = ZONE_DROP_PENDING_AGES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut ages = ages.lock().unwrap_or_else(|e| e.into_inner());
    match age {
        Some(age) => {
            ages.insert(id, age);
        }
        None => {
            ages.remove(&id);
        }
    }
    ages.values().copied().max().unwrap_or(0)
}

use std::future::Future;
use std::io::{self, ErrorKind};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::RawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use vals_rt::LocalExecutor;
use vals_spdk::{AsyncRead, BlockDevice};

use crate::coord::{BatchAck, BatchEntry, Transport};
use crate::net::NetTransport;
use crate::partition::RangeMap;
use crate::reactor::{self, Interest};
use crate::replica::{unconditional_apply_mode, ApplyMode, LiveFloor, StagedApply, VersionedStore};
use crate::ring::{NodeId, Ring};
use vals_client::types::{Applied, MixedReadReply, MixedReadRole, Stored};
use crate::wire::{self, MixedReadResult, OwnedRequest, Response};
use crate::zone::{ordering_to_u8, Catalog, ZoneConfig, ZoneId, DEFAULT_ZONE};
use crate::zonestore::ZoneStore;

use super::{
    catalog_from_wire, write_framed, FrameErr, FrameRead, MAX_CONN_INFLIGHT_READS,
    MAX_CONN_SEND_BUF, MAX_FRAME, SCAN_REPLY_BUDGET,
};

struct Liveness {
    grace_ms: u64,
    log: Box<dyn crate::liveness::LivenessLog>,
}

pub(crate) struct ServerShared<D: BlockDevice> {
    pub(crate) settings: crate::node_settings::NodeSettings,
    pub(crate) node_id: NodeId,
    zones: RefCell<FastMap<ZoneId, Rc<RefCell<VersionedStore<D>>>>>,
    devices: RefCell<FastMap<ZoneId, Arc<D>>>,
    provider: Option<Rc<dyn ZoneStore<D>>>,
    catalog: Tracked<Catalog>,
    range_maps: Tracked<FastMap<ZoneId, RangeMap>>,
    range_owners: RefCell<RangeOwners>,
    range_load: RefCell<RangeLoadTracker>,
    range_map_epoch: Cell<u64>,
    range_maps_expected: Cell<bool>,
    range_maps_adopted: Cell<bool>,
    uncataloged_sorted: RefCell<Vec<ZoneId>>,
    ring: Tracked<Ring>,
    peer_addrs: RefCell<FastMap<NodeId, Vec<SocketAddr>>>,
    pub(crate) rehome_mirrors: RefCell<FastMap<(ZoneId, Vec<u8>), RehomeMirrorStat>>,
    pub(crate) rehome_copy_progress: RefCell<FastMap<(ZoneId, Vec<u8>), u64>>,
    range_write_marks: RefCell<FastMap<ZoneId, ZoneWriteMarks>>,
    rehome_copy_holds: RefCell<FastMap<(ZoneId, Vec<u8>), u64>>,
    cleanup_status: RefCell<FastMap<ZoneId, CleanupZoneStatus>>,
    cleanup_recheck: Cell<(u64, Option<std::time::Instant>)>,
    zone_forget_gens: RefCell<FastMap<ZoneId, u64>>,
    rehome_transport: RefCell<Option<RehomeTransport>>,
    read_gate: ReadGate,
    scan_gate: ReadGate,
    winner_probe_sample_cursor: Cell<u16>,
    batch_apply_gate: ReadGate,
    sync_groups: RefCell<FastMap<ZoneId, SyncGroup>>,
    device_waits: Cell<usize>,
    domain_flushes: RefCell<Vec<DomainFlush>>,
    flush_interrupt: Arc<reactor::Notify>,
    sweep_notify: reactor::Notify,
    logical_accounting_notify: reactor::Notify,
    seed_driver: Cell<bool>,
    foreground_ops: Cell<u64>,
    rebalance_pending: Cell<bool>,
    epoch: Cell<u64>,
    catalog_epoch: Cell<u64>,
    liveness: Option<Liveness>,
    catalog_store: Option<Box<dyn crate::catalog_store::CatalogStore + Send>>,
    incarnation: Cell<Option<u128>>,
    fenced: Cell<bool>,
    resync_pending: Cell<bool>,
    wipe_pending: Cell<Option<std::time::Instant>>,
    last_contact: Cell<u64>,
    membership_adopted: Cell<bool>,
    had_persisted_watermark: Cell<bool>,
    watermark_floor: Cell<u64>,
    durable_gc_watermark: Cell<u64>,
    shutdown: Cell<bool>,
    force_close: Cell<bool>,
    shutdown_notifies: RefCell<Vec<Rc<reactor::Notify>>>,
    conn_outs: RefCell<Vec<std::rc::Weak<ServerOut>>>,
    pending_drops: RefCell<Vec<PendingDrop<D>>>,
    maint_jobs: RefCell<FastMap<ZoneId, u64>>,
    maint_reservations: RefCell<FastMap<ZoneId, Vec<crate::ram_broker::Reservation>>>,
    flush_builds: RefCell<FastMap<ZoneId, u64>>,
    published_live_keys: Cell<u64>,
    published_used_segments: Cell<u64>,
    published_free_segments: Cell<u64>,
    published_resident_runs: Cell<u64>,
    published_frozen_runs: Cell<u64>,
    published_stranded_runs: Cell<u64>,
    published_shadowed_bytes: Cell<u64>,
    published_shadow_folds: Cell<u64>,
    published_shadow_fold_disarms: Cell<u64>,
    published_memtable_bytes: Cell<u64>,
    published_run_metadata_bytes: Cell<u64>,
    published_winner_directory_keys: Cell<u64>,
    published_winner_directory_bytes: Cell<u64>,
    published_key_filter: Cell<[u64; KEY_FILTER_GAUGES]>,
    published_run_meta: Cell<[u64; RUN_META_GAUGES]>,
    registered_engines: Cell<u64>,
    shard: Cell<(usize, usize)>,
    shard_write_counters: std::sync::Arc<crate::observe::ShardWriteCounters>,
    sweep_every: Cell<Duration>,
    sweep_pass_at: Cell<std::time::Instant>,
    sweep_pass_count: Cell<u64>,
    sweep_held_count: Cell<u64>,
    transport_deadline_poll: Cell<std::time::Instant>,
    pub(crate) integrity_watch: RefCell<crate::integrity::IntegrityWatch>,
    #[cfg(test)]
    inject_accept_errors: Cell<u32>,
}

pub(crate) struct Tracked<T> {
    value: RefCell<T>,
    generation: Cell<u64>,
}

impl<T> Tracked<T> {
    fn new(value: T) -> Self {
        Tracked { value: RefCell::new(value), generation: Cell::new(0) }
    }

    pub(crate) fn borrow(&self) -> std::cell::Ref<'_, T> {
        self.value.borrow()
    }

    pub(crate) fn borrow_mut(&self) -> std::cell::RefMut<'_, T> {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.value.borrow_mut()
    }

    fn generation(&self) -> u64 {
        self.generation.get()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct PlacementStamp {
    ring: u64,
    catalog: u64,
    range_maps: u64,
    shard: (usize, usize),
}

#[derive(Default)]
struct RangeOwners {
    stamp: Option<PlacementStamp>,
    zones: FastMap<ZoneId, Vec<Option<bool>>>,
}

struct RehomeTransport {
    ring_epoch: u64,
    range_map_epoch: u64,
    transport: Rc<NetTransport>,
}

struct RangeLoadTracker {
    counts: FastMap<ZoneId, Vec<u64>>,
    sampled_at: std::time::Instant,
}

impl RangeLoadTracker {
    fn new() -> Self {
        RangeLoadTracker {
            counts: FastMap::default(),
            sampled_at: std::time::Instant::now(),
        }
    }
}

#[derive(Default)]
struct CleanupZoneStatus {
    clean: Option<u64>,
    lapped: Option<u64>,
    stamp: (u64, u64, u64),
    kept: u64,
    samples: Vec<Vec<u8>>,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct RehomeMirrorStat {
    pub(crate) epoch: u64,
    pub(crate) ok: u64,
    pub(crate) failures: u64,
    pub(crate) copy_complete_epoch: u64,
}

const REHOME_MARK_TTL_MS: u64 = 600_000;

#[derive(Default)]
struct ZoneWriteMarks {
    counter: u64,
    marks: FastMap<Vec<u8>, RangeWriteMarkSlot>,
}

struct RangeWriteMarkSlot {
    value: u64,
    last_pending_ms: u64,
}

struct PendingDrop<D: BlockDevice> {
    zone_id: ZoneId,
    store: Rc<RefCell<VersionedStore<D>>>,
    dev: Arc<D>,
    since: std::time::Instant,
    last_warned: Option<std::time::Instant>,
}

#[derive(Default)]
struct SyncGroup {
    running: bool,
    commits: u64,
    waiters: Vec<Rc<SyncWaiter>>,
}

#[derive(Clone)]
enum SyncResult {
    Ok,
    Space,
    Err(String),
}

struct SyncWaiter {
    result: RefCell<Option<SyncResult>>,
    waker: RefCell<Option<Waker>>,
}

impl SyncWaiter {
    fn new() -> Self {
        SyncWaiter {
            result: RefCell::new(None),
            waker: RefCell::new(None),
        }
    }
}

/// Cancelling a leader returns its snapshot to the group queue and hands the
/// lead over: a waiter only ever receives the result of a round whose
/// `sync_store` ran start to finish after the waiter's writes were applied,
/// so an abandoned round's flush never acknowledges anything.
struct SyncLeaderGuard<'a, D: BlockDevice> {
    shared: &'a ServerShared<D>,
    zone_id: ZoneId,
    armed: bool,
    batch: Vec<Rc<SyncWaiter>>,
}

impl<D: BlockDevice> Drop for SyncLeaderGuard<'_, D> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let wakers: Vec<Waker> = {
            let mut groups = self.shared.sync_groups.borrow_mut();
            let Some(group) = groups.get_mut(&self.zone_id) else {
                return;
            };
            group.running = false;
            let mut queued = std::mem::take(&mut self.batch);
            queued.append(&mut group.waiters);
            group.waiters = queued;
            group
                .waiters
                .iter()
                .filter_map(|pending| pending.waker.borrow_mut().take())
                .collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }
}


mod accept;
mod apply;
mod connection;
mod control;
mod placement;
mod range_delete;
mod read;
mod read_gate;
mod scan;
mod shared;

use super::{Listener, Sock};

use apply::*;
use control::*;
use placement::*;
use range_delete::*;
use read::*;
use scan::*;
use shared::{KEY_FILTER_GAUGES, RUN_META_GAUGES};

pub use accept::{ReplicaServer, ServerHandle, ShardedServer};
pub(crate) use accept::{accept_loop, server_poll, DeadlineKind, ServerOut};
pub(crate) use connection::ConnDriver;
#[cfg(test)]
pub(crate) use placement::catalog_to_wire;
pub(crate) use read_gate::{InflightGuard, ReadGate, ReadPause};
#[cfg(test)]
pub(crate) use control::serve_request;
pub(crate) use scan::serve_scan_keys;
#[cfg(test)]
pub(crate) use scan::{serve_scan, serve_scan_counted, set_zone_scan_reply_budget};

#[cfg(test)]
mod staged_scan;
#[cfg(test)]
mod staged_apply;
#[cfg(test)]
mod corrupt_read;
#[cfg(test)]
mod digest_serve;
#[cfg(test)]
mod range_delete_tests;
