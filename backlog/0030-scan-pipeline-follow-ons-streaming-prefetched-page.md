---
id: "0030"
title: 'Scan pipeline follow-ons: streaming/prefetched pages; async run-block cursors'
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
Two follow-ons from the 2026-07 scan-performance pass (`docs/scan-performance.md`):

1. **Streaming scan / page prefetch.** `Coordinator::scan_in` is strictly request-response per page: the server idles while the coordinator merges/drains a page and the coordinator idles while the server builds the next. Options: (a) a `ScanStream` wire op (server pushes successive budgeted pages on one request, client acks via flow control — additive opcode under a PROTO feature bit); (b) client-side prefetch: issue the next page's RPC as soon as the current reply's resume bound is known, overlapping merge with server work. (b) is contained but only hides the client-side gap; (a) also removes the per-page RTT + request decode.

2. **Async run-block cursors for scan page selection.** `scan_winners_page` consumes each overlapping run through `RunScan`, whose block loads are *synchronous QD1 device reads under the engine borrow* (~56 µs each on NAND). A page spanning R runs and B blocks pays R×B sync reads serialized on the serving core before the (now-coalesced, overlapped) value phase even starts. Options: prefetch the next block per cursor asynchronously; or a two-stage plan like `probe_get` (locate blocks under the borrow, read them off-borrow, merge in RAM).

Acceptance: box-measured scan keys/s improvement on the firehose corpus attributable to each (perf-profiled), 0 errors, value-verified.

## Acceptance criteria

## Plan

## Scope for this session

Task 0030 lists two follow-ons. This session implements **item 2 — async run-block cursors for scan page selection** (a two-stage, borrow-free scan page plan, the `probe_get`/`DigestPagePlan` pattern applied to `scan_winners_page`). Item 1 (streaming/prefetched pages) is split into a new backlog task; box measurement of both is split into a hardware-validation task (same precedent as 0028 → 0042).

## Problem

`serve_scan` → `VersionedStore::plan_scan` → `Engine::scan_winners_page` selects a scan page under the store borrow, and each overlapping run is consumed through `RunScan`, whose block loads are **synchronous QD1 device reads under the borrow** (~56 µs each on NAND). A page spanning R runs and B blocks serializes R×B sync reads on the single serving core before the (already overlapped) span value phase starts, starving other requests.

## Design: staged scan page selection

Stage 1 (brief borrow, RAM-only — no device reads):
- New `Engine::stage_scan_page(lo, hi, max_entries, max_bytes) -> StagedScanPage` (name flexible):
  - captures a memtable chunk via `Index::scan_page` (refill size 4096 min max_entries, exactly like today) + `mem_more`;
  - for each run overlapping `[lo, hi)`: the ordered list of in-range data-block `RangeLocator`s (built from the in-RAM block index — new `DeviceRun::locate_blocks(&alloc, lo, hi) -> Vec<RangeLocator>`, mirroring `block_scan`'s start-block selection), runs kept newest→oldest-distinguishable so the merge tie-break matches;
  - owned `lo`/`hi`, budget state, and the engine's `manifest_gen` (expose an accessor) as the plan's **generation guard**.
- Memtable-only fast path (`runs.is_empty()`): return the RAM page directly (no staging).

Stage 2 (off the borrow, async):
- Drive the same k-way merge as `scan_winners_page` — identical min-key selection, `argmax(version)` with memtable-wins-tie then newest-run-wins-tie, identical entry/byte budgeting and `more` semantics — but block loads go through `RangeLocator::read(dev).await` + block decode (expose the decode used by `RunScan`). Prime every cursor's first block concurrently (`vals_rt::join_all`); subsequent per-cursor loads are on-demand awaits (per-cursor lookahead prefetch is out of scope — note as a refinement).
- When the captured memtable chunk is exhausted with `mem_more`, the merge yields control (`NeedMemRefill { from }`-style resumable step); the caller re-borrows briefly, **checks the generation**, and refills from `Index::scan_page(from, hi, ...)`.

Generation guard (correctness):
- `manifest_gen` bumps on every run-set change (flush, staged install, compact, co-compact). If it changed at ANY re-borrow (mem refill or the final span-plan build), **restart the entire page selection from the original `lo`** (discard partial output — flush/compact don't change logical content, so a restart is safe and cheap; run-set changes are rare per page). Cap restarts (~3), then fall back to the existing sync `plan_scan`. This makes the staged path's pointer freshness exactly equal to the sync path's.
- Extent/segment safety off-borrow comes from the existing `ReadGate` (held across all of `serve_scan`, same as today): frees are quiesce-gated, the same invariant the staged digest page and `probe_get` already rely on.

Wiring:
- `VersionedStore` grows thin wrappers (stage-1 builder, mem-refill chunk accessor, generation accessor, and a "build `SpanReadPlan` + (key, version) list from finished winners" helper equivalent to the back half of today's `plan_scan`).
- `serve_scan` (net/server.rs) drives the loop: gate acquire → stage 1 under brief borrow → async merge with refill/restart handling → brief re-borrow (gen check) to build the span plan → existing `execute_span_page`. Sync `plan_scan` stays as the fallback and for other callers.
- Kill switch env `VALS_STAGED_SCAN=0` → use the old sync `plan_scan` path (house style, cf. `VALS_VLOG_CO_COMPACT`).

## Testing / acceptance for this increment

- Equivalence: staged page output (entries, versions, `more`) == sync `scan_bounded`/`plan_scan` across corpora with multiple runs + memtable, overlapping keys with version ties (memtable vs run, run vs run), tombstones, budget-boundary pages, and full paged walks of the keyspace.
- Refill path: a tiny refill chunk forces multiple mem refills mid-page; results still identical.
- Restart path: flush (or compact) the engine between stages via a hand-driven plan; the staged scan detects the gen change, restarts, and returns the correct page; restart cap falls back to sync.
- Existing vals-cluster net/AE scan tests pass unchanged (AE `Scan` pull and client scans both route through `serve_scan`).
- `cargo build --workspace && cargo test -p vals-store -p vals-cluster` (plus full workspace test pass) and clippy clean under the pinned toolchain.

Box-measured keys/s attribution (the task's original acceptance) moves to the split-off hardware task.

### Starting points
- crates/vals-store/src/engine.rs: scan_winners_page (~1800), scan_plan (~3602), probe_get/StagedGet (~3635), plan_version_digest_page/DigestPagePlan (~3787) — the staging idiom to mirror
- crates/vals-store/src/run.rs: RangeLocator (~1124, read at 1139), block_scan/RunScan (~1394, 1529), locate_block/locate_sidecar
- crates/vals-store/src/engine.rs: manifest_gen bumps at 2070/2278/2530/2831 — the generation guard
- crates/vals-cluster/src/net/server.rs: serve_scan (~2083), execute_span_page (~2003), ReadGate (~2152)
- crates/vals-cluster/src/replica.rs: plan_scan (~1567), bucket_digest_page_from_plan (~1529)
- kill-switch precedent: rg VALS_VLOG_CO_COMPACT

## Work log
- 2026-07-04 plan: ## Scope for this session  Task 0030 lists two follow-ons. This session implements **item 2 — async run-block cursors for scan page selection** (a two-stage, borrow-free scan page plan, the `probe_g
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-store/src/engine.rs: scan_winners_page (~1800) is the merge to stage; scan_plan (~3602) keeps working as sync fallback; probe_get/StagedGet (~3635) and plan_version_digest_page/version_dig
…[truncated]
- 2026-07-04 implementer report: Implemented Task 0030 **item 2 — staged (borrow-free, async) run-block cursors for scan page selection**. (Item 1 streaming/prefetch and box measurement were explicitly out of scope.)  ## What chang
…[truncated]
- 2026-07-04 revision: Addressed both reviewer findings.  ## 1. (Must fix) `locate_blocks` was O(run) per page → now windowed (O(page))  Replaced `DeviceRun::locate_blocks(alloc, lo, hi)` (materialized a `RangeLocator` fo
…[truncated]
- 2026-07-04 review tier: high-powered — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change cleanly implements item 2 of task 0030 — a staged, borrow-free, async scan page-selection path mirroring the probe_get/DigestPagePlan idiom. Stage 1 (`Engine::stage_scan_page`) captures a
…[truncated]
- 2026-07-04 revision: Closed the review gap: the `serve_scan_staged` restart loop and the restart-cap → sync `plan_scan` fallback are now directly exercised by tests that drive the *real* production restart logic.  ## Ap
…[truncated]
- 2026-07-04 review (Claude): accept — The revision cleanly closes the one minor gap from the prior round: the implementer added a test-only, production-no-op re-borrow hook and two new server-level tests that deterministically exercise th
…[truncated]
- 2026-07-04 decision: accept — commit: perf: staged (borrow-free, async) scan page selection — run blocks read off-borrow via windowed RangeLocators (O(page), not O(run)), generation-guarded restarts with sync fallback, VALS_STAGED_SCAN 
…[truncated]
- 2026-07-04 usage: 193,453 tok (in 454, out 192,999, cache_r 26,521,523, cache_w 869,403) · cost n/a (unpriced)
  implementer: 138,332 tok (in 286, out 138,046, cache_r 20,718,400, cache_w 248,939) · cost n/a (unpriced)
  coordinator: 38,377 tok (in 84, out 38,293, cache_r 3,819,942, cache_w 475,640) · cost n/a (unpriced)
  reviewer:Claude: 16,744 tok (in 84, out 16,660, cache_r 1,983,181, cache_w 144,824) · cost n/a (unpriced)
