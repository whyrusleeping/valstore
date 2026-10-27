---
id: "0040"
title: Persist the liveness watermark on the SPDK superblock (drop spdk data_dir requirement)
status: done
priority: 4
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0006"
spec_refs: []
---

## Description
Task 0006 wired the liveness lease on `backend = spdk` using `FileLivenessLog` under a now-required `data_dir` (`liveness` / `liveness-shard{s}` files). The BACKLOG.md §Deferred follow-on remains: persist the progress watermark **on-device** so a pure-NVMe node needs no filesystem state.

Sketch: extend the zoned path's superblock record (`crates/vals-cluster/src/zonestore.rs`, `SUPER_VERSION` bump v1→v2 accepting v1 as watermark-absent) or carve dedicated double-buffered watermark blocks; expose an `SpdkLivenessLog` sharing the `SuperblockStore` with the provider (both live on the shard thread; only `Send` is needed to cross into it). The multi-drive path has **no reserved blocks at all** today — giving it any needs a layout change, which is the same seam as the deferred durable drive-layout fingerprint (BACKLOG.md §Deferred "Drive layout fingerprint"); consider solving both with one reserved region.

## Acceptance criteria
- `backend = spdk` + `gc_grace > 0` works without `data_dir` (config validation relaxed accordingly), watermark durable across restart/power loss (double-buffered or equivalent).
- v1 superblocks (no watermark) still open; a fresh/absent watermark reads as `None` (conservative).
- Off-hardware tests via the mem block-device superblock plumbing; `record_wipe` durability story stated (on-device or documented as best-effort).

## Acceptance criteria

## Plan

Persist the liveness progress watermark on the zoned SPDK superblock so `backend = spdk` + `gc_grace > 0` no longer needs `data_dir`.

Context: both SPDK serving paths in valsd are zoned and already carry a double-buffered, crc-checked superblock — single-drive via `SpdkZoneStore` (namespace-front superblock) and multi-drive via one `RegionZoneStore` per partition (`zone_super_blocks = 16` at the partition front). So the watermark can live in the existing superblock record on BOTH paths; no new reserved region / drive-layout change is needed (the "multi-drive has no reserved blocks" note in the task predates the zoned JBOD path). The non-zoned multi-drive path is not used by valsd and stays as-is.

1. Superblock record v2 (crates/vals-cluster/src/zonestore.rs):
   - `SUPER_VERSION` 1 → 2. New layout: `[magic u32][version u16][generation u64][wm_present u8][wm u64][count u32][region entries…][crc32c]` (watermark fields in the fixed header, before count).
   - `encode_superblock(generation, regions, watermark: Option<u64>)` always writes v2.
   - `decode_superblock`: version 1 → parse the old layout, watermark = None (v1 accepted as watermark-absent); version 2 → new layout; any other version → fail-loud `Err(Corrupt)` as today.
   - `SuperblockStore::persist`/`load` gain the watermark (load returns the higher-generation slot's record incl. watermark). Update all call sites and existing tests (incl. the future-version fail-loud test near line 1227 and the SpdkZoneStore-mirror helper near 1445).

2. Shared super state + on-device liveness log:
   - Refactor the provider state (`RegionTable` + generation, currently `RefCell<…State>`) of `RegionZoneStore` and `SpdkZoneStore` into a shared, generic `SuperState<SD: BlockDevice> { store: SuperblockStore<SD>, table: RegionTable, generation: u64, watermark: Option<u64> }` held as `Arc<Mutex<…>>` (Mutex, not RefCell, so the handle is Send to cross into the shard thread; both users then live on that one thread, so it is uncontended).
   - New `pub struct SuperblockLivenessLog<SD: BlockDevice>(Arc<Mutex<SuperState<SD>>>)` implementing `LivenessLog`:
     - `load()` → the watermark recovered at open (None on fresh/v1 → conservative, matches the trait docs).
     - `store(wm)` → lock, generation += 1, persist(generation, current regions, Some(wm)) — same double-buffered slot alternation, so a torn write never destroys the previous good copy; map `ZoneStoreError` → `io::Error`.
     - `record_wipe` → documented best-effort (process log only, e.g. `log::warn!`); state this explicitly in the doc comment (the acceptance criteria allow "documented as best-effort").
   - Providers expose `pub fn liveness_log(&self) -> SuperblockLivenessLog<…>` (clones the Arc). Zone alloc/drop persists keep the current watermark; watermark stores keep the current regions.

3. Wiring:
   - `Node::serve_multidrive_zoned_with_liveness` (node.rs): change `mk_log` to `Fn(usize, &RegionZoneStore<DS::Dev>) -> L` so the caller can derive the log from each partition's provider (update the in-crate test near node.rs:4018 that passes MemLivenessLog — it just ignores the provider arg).
   - `run_node.rs`: SPDK single-drive path uses `provider.liveness_log()`; multi-drive path uses `|_, p| p.liveness_log()`. Drop the `data_dir` usage + stale "tracked follow-on" comments on both paths. File/mem backends unchanged.
4. Config (crates/vals-server/src/config.rs): remove the `backend = spdk && gc_grace > 0 requires data_dir` validation; update the `data_dir` doc comment and the config tests (~lines 758–773) so spdk + gc_grace without data_dir is now accepted.

5. Off-hardware tests (MemBlockDevice, per acceptance criteria):
   - Codec: v2 roundtrip with watermark None/Some; a hand-built v1 record (old layout + crc) decodes as watermark-absent with regions intact; future version still fail-loud.
   - `SuperblockStore`: persist wm + regions, reopen loads them; higher generation wins across slots.
   - `RegionZoneStore` end-to-end: create → `liveness_log().load() == None`; store a watermark; allocate a zone (regions persist keeps the wm); store a new wm (keeps the regions); reopen over the same MemBlockDevice → watermark and regions both recovered. Also `SpdkLivenessLog`-equivalent restart-durability via the store.
   - Config test update as in (4).

6. Docs sweep: fix references to the spdk data_dir requirement (run_node comments, config docs, BACKLOG.md deferred entry, docs/deploy.md or docs/SPDK.md if they mention it).

Verification: `cargo build`, `cargo test -p vals-cluster -p vals-server` (workspace tests if cheap), `cargo clippy` on touched crates. `--features spdk` compiles only with the SPDK toolchain; keep the cfg'd code consistent by inspection (SpdkZoneStore mirrors RegionZoneStore).

### Starting points
- crates/vals-cluster/src/zonestore.rs — SUPER_VERSION, encode/decode_superblock, SuperblockStore, RegionZoneStore (RefCell state), SpdkZoneStore (cfg feature=spdk)
- crates/vals-cluster/src/liveness.rs — LivenessLog trait (load/store/record_wipe), FileLivenessLog for reference
- crates/vals-cluster/src/node.rs:630 serve_multidrive_zoned_with_liveness (mk_log: Fn(usize) -> L) and test near :4018
- crates/vals-server/src/run_node.rs — spdk single-drive (~:320) and multi-drive (~:210) liveness wiring using FileLivenessLog + data_dir
- crates/vals-server/src/config.rs:466-475 — the spdk+gc_grace data_dir validation to relax; tests ~:758
- SpdkBlockDevice/SpdkController are unsafe impl Send+Sync (vals-spdk/src/spdk.rs:384) — Arc<Mutex<...>> keeps the log Send

## Work log
- 2026-07-04 plan: Persist the liveness progress watermark on the zoned SPDK superblock so `backend = spdk` + `gc_grace > 0` no longer needs `data_dir`.  Context: both SPDK serving paths in valsd are zoned and already c
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/zonestore.rs — SUPER_VERSION (l.340), encode/decode_superblock (l.362-437), SuperblockStore (l.634), RegionZoneStore state RefCell (l.856-995), SpdkZoneStore (l.1019-1150, cf
…[truncated]
- 2026-07-04 implementer report: Persisted the liveness progress watermark on the zoned SPDK superblock, dropping the `data_dir` requirement for `backend = spdk` + `gc_grace > 0`.  ## What changed  **crates/vals-cluster/src/zonestore
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly and completely persists the liveness watermark on the zoned SPDK superblock, dropping the spdk data_dir requirement. Superblock record bumped to v2 with the watermark in the fixed
…[truncated]
- 2026-07-04 decision: accept — commit: feat(liveness): persist the progress watermark on the zoned SPDK superblock — superblock record v2 (v1 opens watermark-absent, future versions fail loud), SuperblockLivenessLog sharing the provider'
…[truncated]
- 2026-07-04 usage: 71,324 tok (in 286, out 71,038, cache_r 11,024,951, cache_w 334,718) · cost n/a (unpriced)
  implementer: 47,650 tok (in 194, out 47,456, cache_r 8,738,755, cache_w 126,524) · cost n/a (unpriced)
  coordinator: 17,741 tok (in 52, out 17,689, cache_r 1,770,330, cache_w 168,737) · cost n/a (unpriced)
  reviewer:Claude: 5,933 tok (in 40, out 5,893, cache_r 515,866, cache_w 39,457) · cost n/a (unpriced)
