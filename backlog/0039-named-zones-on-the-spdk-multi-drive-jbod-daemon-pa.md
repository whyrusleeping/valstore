---
id: "0039"
title: Named zones on the SPDK multi-drive (JBOD) daemon path
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0006"
spec_refs: []
---

## Description
Task 0006 closed SPDK liveness + config weight, but `backend = spdk` with multiple drives / multiple `listen` addresses still serves the **default zone only** (`serve_multidrive` → `ServerShared::single_zone_on` per partition). The single-drive path serves the full catalog via `SpdkZoneStore` + `serve_zoned`.

This is real design work, not a wiring gap: each partition would need a zone-aware store (zone × partition engine multiplier — see BACKLOG.md §Deferred "Multi-drive"), and sorted-zone range scans interact with hash-based shard routing (overlaps task 0029's range-token sharding design). Options include a per-partition region-sub-allocating zone store (an `SpdkZoneStore` scoped to each partition's LBA region, with its own in-region superblock) or restricting named zones to unsorted on the sharded path first.

## Acceptance criteria
- A multi-drive `backend = spdk` node serves catalog-pushed and boot-reopened named zones (at minimum unsorted zones; state explicitly how sorted zones/scans are handled or deferred with rationale).
- Crash-safe per-partition zone metadata (a reopen recovers every zone with the right ordering).
- Off-hardware coverage via `MemDriveSet`/mem block devices; on-hardware verification noted for the box.

## Acceptance criteria

## Plan

Goal: `backend = spdk` multi-drive (JBOD) nodes serve the PD-pushed zone catalog (named zones), not just the default zone — implemented generically so it's fully testable off-hardware via `MemDriveSet`.

Design decisions (made after code study):
1. **Per-partition region-sub-allocating zone store.** Each partition's LBA region (opened once at build time via `DriveSet::open_region`, one device/qpair per partition owned by its shard thread) is subdivided exactly like the single-drive `SpdkZoneStore` subdivides a namespace: a small reserved superblock area at the front (double-buffered, crc'd — reuse `SuperblockStore`), then fixed-size zone sub-regions handed out by the existing `RegionTable`. Zone engines run on a new generic offset-view device `SubDev<D>` (an `Arc<D>` parent + base/len), so **no runtime cross-thread SPDK qpair creation** is needed (the existing lazily-materializing single-drive path stays as-is).
2. **Sorted zones are DEFERRED on the multi-drive path, with rationale.** The client routes a sorted zone's data ops/scans by `hash(range_token) % shards`, but the PD can *split* a range map at runtime (`PdDaemon::split_zone` exists); a split moves keys to a different shard while per-shard anti-entropy only reconciles shard s ↔ peer shard s — stranded, unscannable data. Solving that is exactly task 0029's range-token sharding design. So the sharded path refuses to materialize sorted zones with a clear error (boot reopen + lazy materialization + hence AE), unsorted named zones are fully served. Documented in code + docs.
3. **Format change, fail-loud legacy handling.** The new layout reserves the per-partition superblock, so it is not byte-compatible with existing default-zone-only multidrive nodes. On reopen (`create == false`) a partition with no valid superblock errors out loudly ("pre-zoned layout or blank; reformat with format=true") instead of silently reading engine data as a fresh namespace and clobbering it.

Implementation steps:
1. `crates/vals-cluster/src/zonestore.rs`:
   - `SubDev<D: BlockDevice>` — offset view over `Arc<D>`: forwards ALL `BlockDevice` methods (incl. `read_batch` with per-op lba offset, `submit_write`, `poll_writes`, `poll_device`, `take_scratch`/`release_scratch`, `sibling()` → wrap parent sibling) plus `AsyncRead::read_async` (lba + base). Bounds-check lba+len ≤ region len.
   - `RegionZoneStore<D>` — per-partition provider implementing `ZoneStore<SubDev<D>>`: `open(parent: Arc<D> or D, cfg { super_blocks, zone_region_blocks, seg_blocks, reserved_blocks }, create)`; create=true initializes a fresh superblock (gen 0, empty), create=false loads it and **errors if absent/invalid** (legacy/blank guard). Zone open/drop mirror `SpdkZoneStore` exactly: existing region reopens with the *persisted* ordering; new zone = alloc → `wipe_region` durably → persist superblock → `Engine::create`. `zone_ordering` from the region table (boot-before-serve with an empty boot catalog).
2. `crates/vals-cluster/src/net/server.rs`: a consuming builder on `ServerShared` (e.g. `refuse_sorted_zones()`): boot reopen skips sorted zones with an error log; `zone_store()` materialization returns a clear error for `Ordering::Sorted` (cites task-0029 rationale). Everything else (SetZones adoption, unsorted zones, teardown/pending-drops, liveness stamping) reuses the existing zoned machinery untouched.
3. `crates/vals-cluster/src/node.rs`: `Node::serve_multidrive_zoned` + `serve_multidrive_zoned_with_liveness` — plan/verify layout and compute weight exactly like `serve_multidrive`, but open each partition's parent region device up front and `spawn_shard_at` with a build closure constructing `RegionZoneStore` + zone-0 open (Unsorted) + `ServerShared::zoned(...).refuse_sorted_zones()` (+ `.with_liveness(mk_log(s))` on the liveness variant). Empty boot catalog; no catalog store (region table carries ordering, PD re-push supplies names/scope) — same reasoning as the single-drive SPDK zoned path.
4. `crates/vals-server`: config knob `zone_region_mib` (per-zone sub-region within a partition; sensible default derived from `region_mib`, e.g. region_mib/8 clamped ≥ 1 MiB and ≥ a few segments; validate superblock + ≥1 zone region fits the partition region). Switch the spdk multidrive branch of `run_node.rs` to the zoned serve fns; update the stale "default zone only" comments and the boot log line.
5. Docs: `docs/multi-drive.md` (and the zones doc if it mentions the gap): named zones on JBOD, per-partition superblock format change (reformat required), default-zone capacity now `zone_region_mib`, sorted zones deferred to task 0029 with the range-split/AE rationale.
6. Off-hardware tests:
   - `RegionZoneStore` unit tests over a `MemBlockDevice` parent: zone isolation, persistence across a fresh store over the same device, ordering recovered from the region table, drop + reuse does not resurrect (wipe-on-reuse), exhaustion error, legacy/blank reopen refused, `SubDev` offset/bounds correctness.
   - Node-level tests (mirror existing `serve_multidrive*` tests over `MemDriveSet`): multidrive-zoned node cluster serves default-zone ops across shards; a pushed catalog with an unsorted named zone serves writes/reads across shards; restart (create=false) reopens the named zone *before serving*; a sorted zone in the catalog is refused with a clear error while other zones keep serving; liveness variant unfences + persists per-shard watermarks.
7. Verify: `cargo build --workspace`, `cargo test --workspace` (no spdk feature), `cargo clippy` clean under the pinned toolchain. On-hardware verification is explicitly out of scope (noted for the box — follow-up task exists for box validation, 0041-style).

### Starting points
- crates/vals-cluster/src/zonestore.rs — RegionTable, SuperblockStore, wipe_region, SpdkZoneStore (the model to mirror); test MemShard shows the offset-view pattern
- crates/vals-cluster/src/drive.rs — DriveSet/MemDriveSet/plan/verify_layout/build_stores; MemDriveSet keys devices by (drive,base) and retains them, so a partition's parent device persists across reopen
- crates/vals-cluster/src/node.rs — serve_multidrive{,_with_liveness}, serve_sharded_with_liveness, spawn_shard_at; existing multidrive tests near line 3064
- crates/vals-cluster/src/net/server.rs — ServerShared::zoned (boot reopen), zone_store() lazy materialization (~line 860), with_liveness, drop_zone/pending_drops
- crates/vals-spdk/src/lib.rs — BlockDevice trait (Send+Sync, read_batch/submit_write/poll_device/take_scratch/sibling) + AsyncRead (impl-trait future, not object-safe) + forward_block_device macro
- crates/vals-server/src/run_node.rs — serve_spdk multidrive branch to switch; crates/vals-server/src/config.rs for the zone_region_mib knob
- crates/vals-client/src/net/client.rs data_shard/scan_shard — client already routes sorted zones by range token; crates/vals-cluster/src/pd.rs split_zone — why sorted zones are deferred (range split strands data across shards under shard-s AE)

## Work log
- 2026-07-04 plan: Goal: `backend = spdk` multi-drive (JBOD) nodes serve the PD-pushed zone catalog (named zones), not just the default zone — implemented generically so it's fully testable off-hardware via `MemDriveS
…[truncated]
- 2026-07-04 context hints: 7 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/zonestore.rs — RegionTable, SuperblockStore, wipe_region, SpdkZoneStore (the model to mirror); the test-only MemShard struct shows the offset-view device pattern; crates/vals
…[truncated]
- 2026-07-04 implementer report: Implemented named (unsorted) zones on the SPDK multi-drive (JBOD) daemon path, generically and fully off-hardware testable. All acceptance criteria met.  ## What changed  **`crates/vals-cluster/src/zo
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — Complete, correct, high-quality implementation of named (unsorted) zones on the SPDK multi-drive JBOD path. It generalizes the single-drive SpdkZoneStore into a backend-agnostic RegionZoneStore + SubD
…[truncated]
- 2026-07-04 decision: accept — commit: feat: named (unsorted) zones on the SPDK multi-drive (JBOD) path — RegionZoneStore + SubDev per partition, serve_multidrive_zoned, zone_region_mib; sorted zones deferred to range-token sharding (tas
…[truncated]
- 2026-07-04 usage: 115,504 tok (in 432, out 115,072, cache_r 40,956,603, cache_w 513,784) · cost n/a (unpriced)
  implementer: 100,343 tok (in 360, out 99,983, cache_r 37,995,364, cache_w 284,160) · cost n/a (unpriced)
  coordinator: 7,747 tok (in 30, out 7,717, cache_r 2,359,975, cache_w 180,287) · cost n/a (unpriced)
  reviewer:Claude: 7,414 tok (in 42, out 7,372, cache_r 601,264, cache_w 49,337) · cost n/a (unpriced)
