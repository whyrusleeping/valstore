---
id: "0096"
title: 'BatchApply read-before-write: extend 0093''s staged async LWW resolve to the batch path (serving-core QD1 run reads dominate fresh-key ingest)'
status: done
priority: 2
created: "2026-07-09"
updated: "2026-07-09"
depends_on: []
spec_refs:
    - ARCHITECTURE.md
---

## Description
Found on the 2026-07-09 bootes-on-vals 1B live test (bench/bootes-live-1b/notes.md, task 0095).

Under sustained fresh-key batch ingest (bootes backfill: ~all keys brand-new), the serving core burns >50% of its cycles resolving LWW old versions on the BatchApply path: `VersionedStore::apply_at → Engine::resolve → DeviceRun::lookup` was 18.2% flat + ~33% inline qpair completion polling (the synchronous QD1 run-block read spin) in a 120s DWARF profile of node2's vals-shard at t≈360M records. Task 0093 staged/async'd this read-before-write for single `Apply` only; BatchApply still does it synchronously per record on the serving core.

Amplifier: the resident run count grows when merges fall behind (see 0069), so aggregate bloom false positives make even brand-new keys hit the device. Combined with the June-26 baseline comparison this is a large share of the ~4.6x per-record batch apply regression (5.0 -> 23.1 µs/record at matched progress).

Fix directions (evaluate):
- extend 0093's staged async apply to BatchApply (stage all entries, spawn the cold reads off-borrow, commit staged);
- and/or a fresh-insert fast path: skip resolve when every run bloom is negative (already?) — measure the bloom FP rate at run counts 8-18 and consider sizing blooms for the observed keyspace;
- and/or bound resolve cost per batch (batch the run reads, overlap them at depth instead of QD1-serial).

Acceptance: on a backfill-shaped box workload (fresh-key BatchApply at >=8 resident runs), serving-core share of DeviceRun::lookup+completion-poll drops to <10%, batch apply per-record cost back to ~June levels (<=10 µs/record at 100M+ records/node), no LWW correctness regression (equivalence tests).

## Acceptance criteria

## Plan

Extend task 0093's staged async LWW apply to the BatchApply path so a fresh-key batch ingest no longer burns the serving core on synchronous QD1 run reads.

**1. New async `serve_batch_apply` in crates/vals-cluster/src/net/server.rs** (sibling of `serve_apply`/`serve_scan`), driving the existing per-key staged primitives batch-wide:
- Bounded attempt loop (~8, mirroring `serve_apply`). Per attempt: acquire the ReadGate (guard held across the whole attempt — pins run/vlog segments against compaction frees, same rationale as `serve_scan`); then, with NO `.await` between admission and the borrow, take one brief `borrow_mut()` and `apply_stage` every (remaining) write in batch order:
  - `StagedApply::Done(a)` → record the result inline (hot key: memtable hit or all-blooms-negative, zero device I/O — the fresh-insert fast path already exists at stage level);
  - `StagedApply::NeedsRunRead { probe, gen }` → collect `(idx, probe)`; `gen` is shared (no await inside the stage pass, so the manifest generation cannot move mid-stage);
  - `Err(e) if e.is_backpressure()` → whole batch answers `Response::Busy` (+ busy_rejections), matching the synchronous arm's semantics (earlier entries already applied; LWW retry is idempotent); other `Err` → `Response::Err`.
- Drop the borrow (gate still held); resolve the cold entries' old versions **overlapped** via `VersionedStore::version_from_probe` in bounded windows with `vals_rt::join_all` (reuse `PAGE_READ_CONCURRENCY` or a dedicated small const — note the O(N·W) rationale comment at PAGE_READ_CONCURRENCY). Any device/decode error → `Response::Err` (a read error is a non-ack, obligation #4).
- Commit under a brief re-borrow: `apply_commit_staged(key, value, version, run_old, gen, wall_ms)` per cold entry in batch order (its memtable re-probe makes duplicate keys within a batch resolve correctly — later commit sees the earlier commit's memtable version). `Retry` (generation moved during the await) → carry the still-uncommitted cold entries into the next attempt (re-stage them). Backpressure → Busy; Err → Err.
- After the attempt budget, fall back to synchronous `store.apply()` for the stragglers (guaranteed progress, one brief QD1 stall — same as `serve_apply`'s tail).
- Tail identical to the synchronous arm: if `sync`, `store.sync()` under a brief borrow with `sync_flush_us`/`sync_commits`/`synced_writes`; then `records_written`, `record_bytes_written`, `batch_apply_latency_us` (end-to-end, spanning the awaited reads); reply `Response::Batch(results)` with results in the original writes order.
- `wall_ms` captured once per batch via `crate::hlc::physical_now_ms()` (as the single-Apply arm does per request).

**2. Dispatch**: add an `OwnedRequest::BatchApply` arm in `dispatch_frame`, guarded by a kill switch `staged_batch_apply_enabled()` (env `VALS_STAGED_BATCH_APPLY`, default ON, read per call — mirror `staged_scan_enabled`); when disabled the request falls through to the synchronous `serve_request` arm (which stays unchanged as reference/in-process path). The staged arm spawns a task counted against `reads_inflight` (like Read/Scan/Apply), with the same out-of-order-reply justification comment as single Apply (LWW arbitrates; coordinator fan-out already tolerates it). Update the now-stale scope comments ("BatchApply … keep the synchronous path") at the single-Apply arm (~line 1859) and the serve_request BatchApply arm.

**3. Observability**: new counter `batch_apply_run_reads` in vals-cluster observe (following existing counter patterns/export), incremented by the batch's cold-entry count — kept separate from `apply_run_reads` so the single-Apply cold rate stays cleanly attributed.

**4. Tests** (follow the 0093 patterns in replica.rs `staged_apply_*` tests and server.rs `staged_scan_tests`):
- Equivalence: a `drive_staged_batch` helper (block_on) vs the synchronous per-record `apply` reference over a flushed store — identical results vector, final store state (get/current_version), and Merkle leaf digests. Cases: memtable-hot keys, flushed bloom-positive keys, brand-new keys, duplicate keys within one batch (later entry must see the earlier entry's version — Won then Lost), tombstones, losing (older-version) writes, skew-rejected writes.
- Race/kill-the-fix: a flush (and/or truncate) between stage and commit bumps the manifest generation → commit answers Retry, the loop re-stages and converges; assert the final state is correct (and that skipping revalidation would have been wrong).
- Busy propagation: the staged batch path answers `Response::Busy` on a reserve-floor wedged store (the existing net/tests.rs BatchApply Busy test covers the sync arm; add the staged analogue).
- Kill switch: `VALS_STAGED_BATCH_APPLY=0` routes the synchronous arm (follow whatever env-var hygiene the staged-scan tests use).

**5. Verify**: `cargo build` + full workspace `cargo test`.

**Box A/B (the acceptance numbers — <10% serving-core lookup share, ≤10 µs/record)**: deferred to the task-0097 replay-benchmark run on oldbox, which can A/B this change directly via `VALS_STAGED_BATCH_APPLY=0/1` — that is precisely why the kill switch exists. Note this in the work log and in 0097.

### Starting points
- crates/vals-cluster/src/net/server.rs:1843-1933 — single-Apply staged dispatch arm (the pattern to extend); :2384-2439 serve_apply (stage/read/commit loop + sync fallback); :3362-3419 synchronous BatchApply arm (semantics to preserve); :2508-2609 PAGE_READ_CONCURRENCY + windowed join_all pattern; :2664-2672 staged_scan_enabled kill-switch precedent
- crates/vals-cluster/src/replica.rs:2472-2566 — apply_stage / apply_commit_staged (per-key primitives, reusable as-is); :2788-2818 drive_staged test helper; tests at :3196+ staged_apply_matches_sync_across_cases etc.
- crates/vals-cluster/src/net/tests.rs:2436 — existing BatchApply reserve-floor Busy test
- vals_rt::join_all re-polls all pending children per wake — keep windows bounded (see PAGE_READ_CONCURRENCY doc comment)
- apply_commit_staged's memtable re-probe is what makes duplicate keys within one batch LWW-correct when both staged before either commits

## Work log
- 2026-07-09 plan: Extend task 0093's staged async LWW apply to the BatchApply path so a fresh-key batch ingest no longer burns the serving core on synchronous QD1 run reads.  **1. New async `serve_batch_apply` in crate
…[truncated]
- 2026-07-09 context hints: 5 recorded with plan
- 2026-07-09 context hints: crates/vals-cluster/src/net/server.rs:1843-1933 — single-Apply staged dispatch arm (the pattern to extend); :2384-2439 serve_apply (stage/read/commit loop + sync fallback); :3362-3419 synchronous Ba
…[truncated]
- 2026-07-09 implemented: Added `serve_batch_apply` (crates/vals-cluster/src/net/server.rs) — the batch analogue of `serve_apply`: a bounded (8) attempt loop that acquires the ReadGate once per attempt, `apply_stage`s every pending write under one brief borrow (hot keys resolve inline; cold flushed keys collected with a shared `gen`), resolves the cold old-versions overlapped via `version_from_probe` in `BATCH_READ_CONCURRENCY`(=64)-wide `join_all` windows off the borrow, then `apply_commit_staged`s in batch order under a re-borrow (per-commit memtable re-probe → within-batch duplicate correctness); `Retry` carries stragglers to the next attempt, a spent budget falls back to synchronous `apply`, and the sync/counters tail matches the synchronous arm. Dispatch: guarded `OwnedRequest::BatchApply if staged_batch_apply_enabled()` arm spawns the task against `reads_inflight`; kill switch `VALS_STAGED_BATCH_APPLY=0` (default ON) falls through to the unchanged synchronous `serve_request` arm. New counter `batch_apply_run_reads` (crates/vals-client/src/observe.rs), auto-exported. Updated the stale scope comments on both the single-Apply arm and the synchronous BatchApply arm. Tests (server.rs `staged_batch_apply_tests`): equivalence vs sync per-record `apply` over a flushed store (results vector + final value/version + Merkle leaf digests; hot/cold/absent/tombstone/losing/dup-cold/dup-absent/skew cases), a commit-hook race that flushes between reads and commit → Retry + converge, reserve-floor Busy, and kill-switch parity. `cargo build --workspace` + `cargo test --workspace` green; clippy clean on the new code (two remaining warnings pre-existing, unrelated).
- 2026-07-09 box A/B deferred: the acceptance numbers (<10% serving-core DeviceRun::lookup+poll share, ≤10 µs/record at 100M+ recs/node) ride on the task-0097 replay benchmark on oldbox, A/B'd directly via `VALS_STAGED_BATCH_APPLY=0/1`. No hardware benchmarking done here (out of scope).
- 2026-07-09 implementer report: Extended task 0093's staged async LWW apply to the BatchApply path (task 0096).  ## What changed **`crates/vals-cluster/src/net/server.rs`** - New `async fn serve_batch_apply` (sibling of `serve_apply
…[truncated]
- 2026-07-09 review tier: single-opus — reviewers: Claude
- 2026-07-09 review (Claude): accept — The change correctly extends task 0093's staged async LWW apply to the BatchApply path. The new `serve_batch_apply` faithfully mirrors the proven `serve_apply`/`serve_scan` invariants (gate held per a
…[truncated]
- 2026-07-09 decision: accept — commit: cluster: staged async LWW resolve for BatchApply (task 0096) — serve_batch_apply drives apply_stage/apply_commit_staged batch-wide: hot keys resolve inline under one brief borrow, cold flushed keys'
…[truncated]
- 2026-07-09 usage: 68,131 tok (in 266, out 67,865, cache_r 8,883,419, cache_w 265,849) · $9.3901
  implementer: 47,244 tok (in 178, out 47,066, cache_r 6,664,819, cache_w 119,077) · $5.2542
  reviewer:Claude: 10,973 tok (in 50, out 10,923, cache_r 742,481, cache_w 50,176) · $0.9582
  coordinator: 9,914 tok (in 38, out 9,876, cache_r 1,476,119, cache_w 96,596) · $3.1777
