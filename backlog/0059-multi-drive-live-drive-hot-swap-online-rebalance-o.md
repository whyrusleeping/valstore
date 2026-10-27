---
id: "0059"
title: 'Multi-drive: live drive hot-swap (online rebalance on a serving node)'
status: done
priority: 5
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Split from grooming umbrella 0025 (BACKLOG.md §Deferred follow-ons, "Multi-drive" bullet).

Drive rebalance on the multi-drive (JBOD) path is **offline** today: stop the node → `replan` + `relocate` → restart. `relocate` is already crash-safe + streaming (task 0013: per-move crash journal, copy-then-commit, resumes after a crash with no loss even at RF=1). This task is the online variant: add/remove a drive from a running node without a restart.

Scope (judgement allowed on the exact seam):
- Admin op to attach a new drive (or retire one) on a live node; the node replans partition placement and relocates affected partitions in the background using the existing journaled relocate machinery, while continuing to serve.
- Serving correctness during migration: reads/writes to a partition mid-move must not lose data (e.g. brief per-partition pause on the commit step, or forward to the new location).
- Crash mid-hot-swap resumes or rolls back cleanly on reboot (reuse the relocate journal).
- Not in scope: SPDK hot-plug PCI-level events; operator issues the admin op.

## Acceptance criteria
- Off-hardware (MemDriveSet) test: add a drive to a live node under concurrent load; partitions rebalance; zero lost/duplicated keys; kill mid-move recovers.
- Drive retire path equivalent test.
- BACKLOG.md bullet updated.

## Acceptance criteria

## Plan

Goal: online drive add/retire on a serving multi-drive node — the node replans and relocates affected partitions in the background while continuing to serve, reusing the existing journaled relocate machinery (crash-safe), scoped to the non-zoned `serve_multidrive` path with MemDriveSet tests.

Design (seam):

1. drive.rs — refactor `relocate` into a shared core `relocate_with(ds, old, new, mover)` that keeps ALL of the existing machinery (move list construction, crash journal load/persist/resume, copy-then-commit ordering via `dst_is_pending_source`, cycle staging via spare slot, final fingerprint re-persist + journal clear) but delegates the actual data copy of one move to a `mover` callback:
   `mover(src: &Partition, dst: &Partition, staged_from_scratch: bool) -> Result<(), LayoutError>`
   - The existing offline `relocate` becomes `relocate_with(..., offline mover = stream_partition)` — zero behavior change; all existing tests must keep passing.
   - Note the src passed to the mover is either the old region or the scratch slot (as today).

2. node.rs — live rebalance orchestration, public API on `NodeHandle` (or a free fn), generic over DS:
   `pub fn rebalance_drives<DS>(&self, ds: &DS, new_caps: &[u64], region_blocks: u64, geom: PartitionGeom, node_id: NodeId) -> Result<(usize, u32), LayoutError>` (moved count + new ring weight) — exact signature at implementer's judgement; it must be callable from the operator/test thread while the node serves.
   - Old layout: decode from the drive set's persisted layout fingerprint (`read_layout_fingerprint` + `Layout::decode`); error loudly if absent/undecodable (live rebalance requires the fingerprint seam). Sanity-check partition count == shard count.
   - New layout: `replan(old, new_caps, cores)`.
   - Run `relocate_with` with a LIVE mover: for each move of partition p, the mover
     a. opens the destination region device on the caller thread (`ds.open_region(dst, create=true)`; `DS::Dev: Send`),
     b. sends a new `NodeCtl` variant to shard p's ctl channel carrying a type-erased job — e.g. `NodeCtl::SwapStore { job: Box<dyn FnOnce(&dyn Any) -> Result<(), String> + Send>, reply: mpsc::Sender<...> }` — where the closure downcasts the `&dyn Any` to `ServerShared<D>` (D = DS::Dev, known at the orchestration site) and runs ON THE SERVING CORE,
     c. waits for the reply before advancing the journal status (so `Done` is only recorded after the data is durably at dst).
   - The per-shard job on the serving core (this is the per-partition pause):
     * refuse if shutdown began;
     * pause the read gate + drain in-flight device reads (reuse the `drain_reads` helper pattern; bounded wait, retry/fail cleanly on timeout);
     * wait until no off-core flush/compaction is in flight for zone 0 (`flush_in_flight()` / `compact_in_flight()` short borrows; bounded wait — do NOT let a maint job for the old engine commit into the new one);
     * synchronously (no awaits from here to swap): create the destination engine over the shipped device (`VersionedStore::new(Engine::create(dev, geom.config()), node)`), stream every key from the LIVE store (`keys()`/`stored()`/`apply()`, preserving versions+tombstones), `sync()` the destination, then swap the engine in place so every holder (sweep, handlers) sees the new one — replace the contents of the existing `Rc<RefCell<VersionedStore<D>>>` for DEFAULT_ZONE (`*store.borrow_mut() = new_store` via a small pub(crate) ServerShared helper) and update the `devices` map entry so the poll loop polls the new device;
     * unpause the gate (guard drop), reply Ok.
   - IMPORTANT correctness notes for the implementer: because the copy source is always the shard's live engine, the mover ignores the journal's `Staged` scratch source distinction for reading (live engine is authoritative) — but staging must still stream live→scratch AND swap the shard onto the scratch engine, so the released old region can be safely reformatted by the next move in the cycle. I.e. the live mover treats every move uniformly: "stream live engine → given dst region, swap shard to dst". Verify this preserves the journal resume semantics (a crash then resumes OFFLINE from regions, where `Staged`'s scratch slot holds the data — consistent because the swap happened after the scratch copy synced).
   - After `relocate_with` completes, return `node_weight(count of drives with cap > 0)`.
   - Drive retire = same call with the retired drive's cap set to 0 in `new_caps` (replan places nothing there); drive add = new_caps including the new drive.

3. ServerShared (net/server.rs): add a small pub(crate) `swap_default_zone(store: VersionedStore<D>)` (or similar) that replaces the zone-0 engine contents + devices-map device handle; keep it tight and documented. Expose whatever minimal accessors the ctl job needs (read_gate, flush/compact in-flight checks already exist via zone_store()).

4. NodeCtl: new variant is plain `Send` data (boxed closure + reply sender), keeping NodeHandle untyped. ctl_loop runs it, passing `&*shared` as `&dyn Any`. A downcast failure (caller's DS::Dev ≠ served D) replies with a clear error.

5. Tests (off-hardware, MemDriveSet — likely in node.rs tests or a new tests file):
   a. Drive add under load: serve_multidrive on a MemDriveSet-like drive set with a spare drive at cap 0 (mirroring the existing offline test trick), write N keys (sync'd) through the normal client path, then start a writer thread producing ongoing load while calling rebalance_drives with the 4th drive's real cap; assert moved>0, new layout hosts partitions on the new drive, and afterwards EVERY key (pre-existing + concurrently written, tracked by the writer) reads back with its correct value; zero lost/duplicated keys.
      - MemDriveSet is not Sync; the serve path consumes DS by value. Solution at implementer's judgement — e.g. make MemDriveSet cheaply shareable (wrap internals in Arc<Mutex<..>> replacing RefCell, keeping the same API), or add an `impl DriveSet for &T`/Arc wrapper. Prefer making MemDriveSet Send+Sync-shareable (Mutex instead of RefCell) since tests need one instance both serving and rebalancing.
   b. Kill mid-move recovers: wrap the drive set in a fault-injecting DriveSet (like the existing FaultDriveSet) that errors on the k-th journal write during the LIVE rebalance → rebalance returns Err mid-way, journal persists; shut the node down; run the OFFLINE `relocate(old,new)` resume to completion; reopen via build_stores/verify_layout; every synced key present. (Sweeping a couple of fault points is enough; full sweep already covered offline.)
   c. Drive retire equivalent of (a): retire a drive (cap → 0 in new_caps) under load; no partitions remain on it; zero lost keys. Retired drive's regions must be fully drained before the call returns.
   d. Existing offline relocate/crash-sweep tests all still pass unchanged.

6. BACKLOG.md: update the multi-drive deferred bullet (live hot-swap now implemented for the non-zoned path; zoned multidrive live-swap + SPDK on-box validation remain follow-ons).

Out of scope: zoned multidrive (`serve_multidrive_zoned`) live swap, SPDK hot-plug, PD/ring weight re-advertisement automation (return the new weight; the operator pushes it). Verify with `cargo test -p vals-cluster` + `cargo build` and clippy.

### Starting points
- crates/vals-cluster/src/drive.rs — relocate/stream_partition/persist_journal/find_spare_slot, MemDriveSet, FaultDriveSet test helper, crash_sweep test
- crates/vals-cluster/src/node.rs — NodeCtl, ctl_loop, spawn_shard_at, serve_multidrive, drain_reads, maint_loop/snapshot_loop (flush_in_flight/compact_in_flight gates)
- crates/vals-cluster/src/net/server.rs — ServerShared { zones, devices, read_gate, pending_drops }, zone_store(), single_zone_on
- offline flow docs: LayoutError::RelocateInProgress, verify_layout in drive.rs

## Work log
- 2026-07-05 plan: Goal: online drive add/retire on a serving multi-drive node — the node replans and relocates affected partitions in the background while continuing to serve, reusing the existing journaled relocate 
…[truncated]
- 2026-07-05 context hints: 4 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/drive.rs — relocate/stream_partition/persist_journal/find_spare_slot/MoveStatus; MemDriveSet; FaultDriveSet + crash_sweep in its tests; crates/vals-cluster/src/node.rs — No
…[truncated]
- 2026-07-05 implementer report: Implemented Task 0059: online drive hot-swap (live rebalance) for the non-zoned `serve_multidrive` path, reusing the crash-safe journaled relocate machinery.  ## What changed  **`drive.rs`** - Refacto
…[truncated]
- 2026-07-05 review tier: high-powered — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change implements online drive hot-swap for the non-zoned serve_multidrive path exactly as planned: relocate is refactored into a mover-parameterized relocate_with (offline path unchanged), a live
…[truncated]
- 2026-07-05 decision: accept — commit: cluster: live drive hot-swap — online rebalance on a serving multidrive node (task 0059)  Online drive add/retire for the non-zoned serve_multidrive path, reusing the crash-safe journaled relocate m
…[truncated]
- 2026-07-05 usage: 176,293 tok (in 422, out 175,871, cache_r 35,959,397, cache_w 634,989) · cost n/a (unpriced)
  implementer: 144,430 tok (in 322, out 144,108, cache_r 32,335,299, cache_w 293,942) · cost n/a (unpriced)
  coordinator: 17,300 tok (in 44, out 17,256, cache_r 2,697,127, cache_w 285,384) · cost n/a (unpriced)
  reviewer:Claude: 14,563 tok (in 56, out 14,507, cache_r 926,971, cache_w 55,663) · cost n/a (unpriced)
