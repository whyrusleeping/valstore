---
id: "0032"
title: Wipe (truncate) can resurrect a stale pre-wipe manifest at reboot — bricked boot or data resurrection
status: done
priority: 1
created: "2026-07-03"
updated: "2026-07-04"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Storage engine (single node)
---

## Description
## Bug (confirmed by repro)

`Engine::truncate()` resets `manifest_gen = 0`, but **freed** manifest-chain segments are never wiped: `snapshot_io::write_chain` frees the previous generation's segments (`alloc.free`) with their `ChainHeader`s left intact on device, and `truncate()` zeroes only `used_list()` (the freed chains are not in it). After a wipe, new manifests restart at generation 1 — **lower** than the stale freed chains still on device (any generation ≤ N−1 whose segments weren't coincidentally reused). At the next reopen, `load_latest_chain` picks the *highest complete generation*, i.e. the stale pre-wipe manifest.

Repro: `crates/vals-store/tests/stale_manifest_repro.rs` (currently `#[ignore]`d — un-ignore when fixed; kill-the-fix test). Sequence: 3 flushes (so gen 2's chain is freed intact at a high segment id) → `truncate()` → 1 put + flush (new gen 1) → `Engine::open` fails with `Snapshot(Corrupt("footer crc mismatch"))` — **deterministically, on every boot** (crash-loop / bricked node). Depending on how the stale manifest's named segments were reused, the failure can instead be the `mark_allocated` "segment marked twice" panic, or — worst case — a stale run that still parses and **resurrects pre-wipe keys**, which is exactly the resurrection hazard the liveness wipe (`VersionedStore::clear`, wipe-before-rejoin) exists to prevent.

Blast radius: any wiped-and-resynced node (liveness lease `WipeAndResync`) that had flushed ≥ 3 times before the wipe, plus any other `truncate()` caller. Not SPDK-zone-creation (that path runs `wipe_region`, which zeroes every segment-start block).

## Fix directions (either alone closes the repro; both are good hygiene)

1. **Never reset the generation.** `truncate()` should keep `manifest_gen` monotonic (drop the `self.manifest_gen = 0` reset; keep counting from the last value). Generations are cheap u64s; monotonicity across the engine's whole device lifetime is what the "highest complete generation wins" recovery rule assumes.
2. **Wipe chain headers on free.** In `write_chain`, zero each freed prev-generation segment's first block (a single block write per segment) before/at `alloc.free`, and have `truncate()` also blank stale chain headers (or wipe every segment-start block like `zonestore::wipe_region` does).

## Acceptance criteria

- The repro test un-ignored and passing.
- A crash-interleaving check: crash between truncate's device wipe and the first post-wipe flush must still boot empty (not load a stale chain).


## Acceptance criteria

## Plan

Root cause: `Engine::truncate()` zeroes only `used_list()` segment headers and resets `manifest_gen = 0`, while manifest-chain segments freed earlier by `snapshot_io::write_chain` keep intact `ChainHeader`s on device. Post-wipe manifests restart at generation 1, so `load_latest_chain` ("highest complete generation wins") can pick a stale pre-wipe chain at reboot.

Implement BOTH fix directions from the task (belt and braces), plus a crash-safe wipe ordering:

1. **Monotonic generations** — in `Engine::truncate()` (crates/vals-store/src/engine.rs ~2849), drop the `self.manifest_gen = 0` reset; the counter keeps advancing across the wipe. Update the surrounding comment.

2. **Wipe chain headers on free** — in `snapshot_io::write_chain` (crates/vals-store/src/snapshot_io.rs ~241), after the new head is durable and before/at `alloc.free`, zero each freed prev-generation segment's first block (one block write per segment; no extra flush needed — an unwiped lower-gen complete chain loses to the durable higher-gen head anyway; this is hygiene that keeps stale headers from accumulating).

3. **Full-sweep, crash-ordered wipe in `truncate()`** — the key fix for the crash criterion. Stale chains can also linger from crash leftovers (e.g. crash after a new head commits but before prev is freed/wiped), so truncate must not trust `used_list()` alone:
   - Phase A: zero the first block of every segment NOT in `used_list()` (the potentially-stale freed chains), then `dev.flush()`. A crash during/after phase A leaves the live (highest-gen) manifest intact → node boots with full pre-wipe state, which is fine (the wipe never committed; caller re-runs it).
   - Phase B (existing code): zero the used segments' first blocks, flush, free them, reset in-memory state. A crash during/after phase B can only lose chains — never expose a stale one — because every free-segment header was already durably zeroed; boot finds either the intact pre-wipe manifest (crash before its head was zeroed) or nothing (boots empty).
   This gives `wipe_region`-equivalent coverage (every segment-start block zeroed) with a crash-safe ordering.

4. **Tests** (crates/vals-store/tests/stale_manifest_repro.rs):
   - Un-ignore `truncate_then_flush_then_reopen_must_not_resurrect_pre_wipe_manifest` (kill-the-fix test) and drop the TEMP-repro framing from the file docs.
   - Add a crash-interleaving test: same 3-flush setup → `truncate()` → drop the engine WITHOUT any post-wipe put/flush (simulating a crash between the wipe and the first post-wipe flush) → `Engine::open` must succeed and be empty (no stale chain loaded, pre-wipe keys gone).
   - Consider a unit test in snapshot_io for wipe-on-free (freed prev segments' header blocks are zeroed) if straightforward.

5. **Verify**: `cargo test -p vals-store` (all tests incl. the existing `truncate_*` tests at engine.rs ~5355+), plus `cargo build --workspace` and clippy/fmt if the repo is clean on those. Note: backlog task 0031 mentions a pre-existing ordering-dependent SIGABRT in the suite under rustc 1.96 — don't chase unrelated failures, but confirm the touched tests pass.

### Starting points
- crates/vals-store/src/engine.rs: Engine::truncate() ~line 2813 (the `manifest_gen = 0` reset at ~2849)
- crates/vals-store/src/snapshot_io.rs: write_chain ~170 (prev freed at ~241 after head flush), load_latest_chain ~289 (scans ALL segments' first blocks, highest complete gen wins)
- crates/vals-store/tests/stale_manifest_repro.rs: existing #[ignore]d repro to un-ignore
- crates/vals-cluster/src/zonestore.rs:601 wipe_region — precedent for zeroing every segment-start block
- Allocator: alloc.num_segments(), alloc.used_list(), alloc.segment_range(SegmentId) in crates/vals-store/src/alloc.rs
- engine truncate tests at crates/vals-store/src/engine.rs ~5355 (truncate_empties_the_store_and_survives_reopen etc.)

## Work log
- 2026-07-04 plan: Root cause: `Engine::truncate()` zeroes only `used_list()` segment headers and resets `manifest_gen = 0`, while manifest-chain segments freed earlier by `snapshot_io::write_chain` keep intact `ChainHe
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs: Engine::truncate() ~line 2813 (the `manifest_gen = 0` reset at ~2849); crates/vals-store/src/snapshot_io.rs: write_chain ~170 (prev freed at ~241 after head flush), lo
…[truncated]
- 2026-07-04 implementer report: Fixed task 0032 — `truncate()` (liveness wipe) could resurrect a stale higher-generation manifest chain at reboot. Implemented all three fix directions plus tests.  ## Changes  **crates/vals-store/s
…[truncated]
- 2026-07-04 revision: Addressed both reviewer points.  **crates/vals-store/src/snapshot_io.rs — `write_chain` error-path fix:** Made the header wipe-on-free strictly best-effort. The previous version propagated a wipe-wr
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change fully implements the task: truncate() no longer resets manifest_gen (keeping generations monotonic), write_chain scrubs freed prev-generation chain headers best-effort, and truncate() now d
…[truncated]
- 2026-07-04 decision: accept — commit: Fix wipe (truncate) resurrecting stale pre-wipe manifest at reboot (task 0032)  - Engine::truncate keeps manifest_gen monotonic (no reset to 0) and wipes   every segment-start block in two crash-order
…[truncated]
- 2026-07-04 usage: 32,139 tok (in 120, out 32,019, cache_r 1,784,109, cache_w 117,203) · cost n/a (unpriced)
  coordinator: 13,927 tok (in 36, out 13,891, cache_r 674,804, cache_w 48,783) · cost n/a (unpriced)
  implementer: 13,667 tok (in 54, out 13,613, cache_r 910,010, cache_w 46,482) · cost n/a (unpriced)
  reviewer:Claude: 4,545 tok (in 30, out 4,515, cache_r 199,295, cache_w 21,938) · cost n/a (unpriced)
