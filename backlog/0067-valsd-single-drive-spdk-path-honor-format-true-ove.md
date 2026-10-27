---
id: "0067"
title: 'valsd single-drive SPDK path: honor format=true over stale superblock; no O(region) init spin'
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
Discovered during task 0027's box A/B (oldbox, 2026-07-06; see `bench/b1-box-ab-notes.md` "Box-recipe fixes"). The valsd single-drive SPDK path (`serve_spdk_zoned`) has two pre-existing gaps that make the documented cluster recipe (`rsweep.sh`, node confs with `backend = spdk`, `format = true`) fail on every run after the first:

1. **`format = true` is ignored**: with a non-blank superblock on the drive (e.g. left by a prior valsd or a distsorted bench run), startup errors with "corrupt zone metadata: unsupported superblock format version" instead of reformatting.
2. **O(region) init spin**: with a large `region_mib` (the recipe's 1048576 = 1 TiB), the reactor spins at ~99% CPU doing an O(region) init and never opens its listen port.

Task 0027 worked around this with a kernel-side drive wipe between runs (setup.sh reset → dd-zero 4 MiB of superblock → setup.sh vfio rebind) and `region_mib = 8192` — that's a harness workaround, not a fix.

## Acceptance criteria
- `format = true` on the single-drive SPDK daemon path reformats over any pre-existing/unknown superblock (matching the multi-drive path's behavior, or an explicit documented equivalent).
- Startup with a large configured region either completes in reasonable time (no O(region) blank-scan spin) or fails fast with a clear error; the listen port is never silently withheld while the reactor spins.
- Verified on the box: back-to-back `rsweep.sh` runs succeed with no kernel-side dd wipe between runs (can fold into another box session, e.g. task 0041).
- Unit/integration coverage for "start with format=true over a stale superblock" at whatever layer is testable off-box.

## Plan

Fix the valsd single-drive SPDK path's two pre-existing gaps (documented in bench/b1-box-ab-notes.md "Box-recipe fixes"): `format = true` ignored over a stale/unknown superblock, and the O(region) synchronous init wipe that spins the reactor and withholds the listen port.

1. **Honor `format = true` (reformat semantics).**
   - Add a `create: bool` parameter to `SpdkZoneStore::open` (crates/vals-cluster/src/zonestore.rs), mirroring `RegionZoneStore::open`.
   - Semantics for BOTH `SpdkZoneStore::open` and `RegionZoneStore::open` (shared helper, factored so it is unit-testable over `MemBlockDevice`):
     - `create == true` → **reformat unconditionally**: do NOT `load()` (or tolerate any load error/content); persist a fresh empty superblock (gen 0, no regions, no watermark) and start with an empty `RegionTable`. Any pre-existing superblock — valid, unknown-version, or garbage — is overwritten. This matches the documented multi-drive semantics ("reformat (`create == true`)", node.rs ~803) and the config doc ("Format the drives on this boot").
     - `create == false` → `load()`: `Some` → recover; decode error (e.g. "unsupported superblock format version") → propagate with a clear message telling the operator to set `format = true`; `None` (blank/garbage) → refuse loudly like `RegionZoneStore` does today ("no zone superblock … reformat with format = true"). Note this changes the single-drive path's current silent treat-blank-as-fresh; that silent path is the unsafe one the task exists to fix.
   - Plumb `cfg.format` through `serve_spdk_zoned` (crates/vals-server/src/run_node.rs ~283). Update the two `SpdkZoneStore::open` call sites in crates/vals-bench/src/main.rs (~2697, ~2929): pass `true` (the bench always formats a scratch cluster; the manual superblock dd-wipe at ~2923 becomes redundant and may be removed). `dump.rs` does not call `SpdkZoneStore::open` (read-only superblock access) — leave it, but confirm.

2. **Kill the O(region) init spin: deep-queue the region wipe.**
   - `wipe_region` (zonestore.rs ~674) currently issues one *synchronous* `write_at` per segment header — with the recipe's `region_mib = 1048576` / `seg_blocks = 256` that is ~1M serial sync writes before the port opens.
   - Rewrite it to use the existing async write seam: `BlockDevice::submit_write` with a bounded in-flight window (e.g. 128–256 handles), draining via `poll_writes` + `WriteHandle::is_done`, checking every handle's `into_result`, then `flush()` at the end (the durable-wipe-before-superblock-record ordering is preserved — all writes complete before flush, flush before the caller persists the table). Sync backends (mem/file) degrade to the sequential default `submit_write` — correctness identical.
   - Add an info-level log around a large wipe (e.g. segment count) so a multi-second format is visible, never silent.

3. **Tests (off-box, MemBlockDevice, in zonestore.rs).**
   - `create=true` over an unknown-version superblock (write SUPER_MAGIC + version 99 into slot 0) → opens fresh, `existing()` empty; a subsequent reopen with `create=false` sees an empty-but-valid zoned store (no error).
   - `create=true` over a *valid* previous superblock holding zones → reformats: `existing()` empty, prior zone data not replayed.
   - `create=false` over blank → `Corrupt` error mentioning `format = true`.
   - `create=false` over unknown-version superblock → clear error (not a panic, not silent format).
   - Wipe still blanks every segment header (a zone created over a dirty region recovers empty) and goes through `submit_write` (small counting wrapper device is fine).
   - Run the full workspace test suite; fix any test relying on the old create=true-recovers or blank-treats-as-fresh behavior (expected: reformat is what callers mean by format).
4. Update the `format` config doc comment (config.rs ~316) to state the reformat-over-anything semantics and the fail-loud reopen.

5. **Box verification (coordinator-driven, after review):** rsync the fixed tree to oldbox:~/vals, build valsd with the spdk feature, and run `rsweep.sh` back-to-back twice with `format = true` and NO kernel-side dd wipe between runs; additionally verify a node with `region_mib = 1048576` (1 TiB) opens its listen port in reasonable time (seconds, not minutes). Record results in the task work log / bench notes.

### Starting points
- crates/vals-cluster/src/zonestore.rs: SpdkZoneStore::open ~1209, RegionZoneStore::open ~1036, wipe_region ~674, decode_superblock ~430 (SUPER_MAGIC 0x564c_5a53, versions 1|2)
- crates/vals-server/src/run_node.rs: serve_spdk_zoned ~268 (cfg.format currently unused on this path)
- crates/vals-spdk/src/lib.rs: BlockDevice::submit_write ~206, poll_writes ~212, WriteHandle ~52
- crates/vals-bench/src/main.rs ~2697 and ~2929: SpdkZoneStore::open call sites (second one hand-wipes the superblock first)
- crates/vals-server/src/config.rs ~316: `format` doc comment
- bench/b1-box-ab-notes.md lines 12-30: the observed failure modes

## Work log
- 2026-07-05 plan: Fix the valsd single-drive SPDK path's two pre-existing gaps (documented in bench/b1-box-ab-notes.md "Box-recipe fixes"): `format = true` ignored over a stale/unknown superblock, and the O(region) syn
…[truncated]
- 2026-07-05 context hints: 6 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/zonestore.rs: SpdkZoneStore::open ~1209, RegionZoneStore::open ~1036, wipe_region ~674, decode_superblock ~430 (SUPER_MAGIC 0x564c_5a53, versions 1|2); crates/vals-server/src/r
…[truncated]
- 2026-07-05 implementer report: Implemented Task 0067: valsd single-drive SPDK path now honors `format = true` over a stale superblock and no longer does an O(region) synchronous init spin.  ## Changes  **crates/vals-cluster/src/zon
…[truncated]
- 2026-07-05 revision: Addressed the reviewer's correctness issue in the reformat (`create == true`) path.  ## Problem fixed `load_or_init_superblock`'s create path called `persist(0, &[], None)`, which writes only slot 0 (
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change correctly and completely satisfies the task. `format = true` now reformats over any prior superblock (valid, unknown-version, or garbage) via a shared, unit-testable `load_or_init_superbloc
…[truncated]
- 2026-07-05 decision: accept — commit: cluster: honor format=true over any stale superblock + deep-queue region wipe on the SPDK zoned paths (task 0067)  - `SpdkZoneStore::open` gains `create`; both it and `RegionZoneStore::open` share   `
…[truncated]
- 2026-07-05 usage: 61,092 tok (in 254, out 60,838, cache_r 8,090,272, cache_w 258,816) · cost n/a (unpriced)
  implementer: 33,345 tok (in 156, out 33,189, cache_r 4,880,073, cache_w 98,263) · cost n/a (unpriced)
  coordinator: 20,546 tok (in 54, out 20,492, cache_r 2,752,156, cache_w 126,220) · cost n/a (unpriced)
  reviewer:Claude: 7,201 tok (in 44, out 7,157, cache_r 458,043, cache_w 34,333) · cost n/a (unpriced)
