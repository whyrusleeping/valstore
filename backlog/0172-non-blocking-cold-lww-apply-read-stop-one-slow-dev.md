---
id: "0172"
title: 'Non-blocking cold LWW apply read: stop one slow device read from wedging the whole shard'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - bench/lab-bench-20260810-0169/notes.md
---

## Description
0169's attribution (bench/lab-bench-20260810-0169/notes.md) showed the sustained-overwrite crawl is drive-internal read-latency degradation (~100 µs → ~4 ms for 5–45 s stretches on the 9100 PRO under overwrite write pressure) amplified by the serving path: the per-put cold LWW run-block read is a **synchronous** `read_at` (`vals_spdk_read` busy-poll) on the shard thread, so one slow read stalls every connection on that engine (~250 applies/s at 100% CPU), and client concurrency slots then absorb into the slow engine until the whole cluster crawls at 0–20k ops/s just under the RPC deadline.

Fix direction: make the apply path's LWW resolve read non-blocking for the shard — use the async read path (interleave other conns/applies while the read is in flight, as the read phase already does at 700k ops/s during the same passes) and/or move it off the serving qpair. A 4 ms device read should cost that *op* 4 ms, not the *engine* 4 ms.

Note the interaction with task 0167 (blind-append LWW), which removes the cold read altogether — if 0167 lands first this task may reduce to verifying no other sync serving-path reads remain (e.g. point-read fallbacks, staged-scan restarts).

## Acceptance criteria
- The overwrite apply path no longer performs a synchronous device read on the shard thread (verified by code inspection + a crawl-window perf profile showing no `vals_spdk_read` busy-poll on the serving thread).
- F-protocol re-run (bench/lab-bench-20260810-0169 harness): deep crawl windows (<100k cluster ops/s with zero maintenance activity) eliminated or reduced to per-op latency blips; pass-over-pass write means no worse than baseline.
- Correctness suites green (vals-store, vals-cluster debug).

## Plan

1. Reconcile task 0172 with the already-staged async Apply implementation from task 0093 by tracing every production `OwnedRequest::Apply` route and every possible cold-LWW probe to identify the remaining path that can reach synchronous `BlockDevice::read_at`/`vals_spdk_read` (including retry fallbacks, feature-disabled branches, wrappers, and server-side background work that can be misattributed in perf).
2. Make the production overwrite Apply path fail closed or remain asynchronous under generation races and all routing/configuration modes: no retry/fallback may call synchronous `VersionedStore::apply` for a flushed key on the shard thread. Keep LWW correctness, re-home fencing/mirroring, admission, counters, and response semantics intact; limit changes to BatchApply only where the same synchronous escape exists and is needed to uphold the invariant.
3. Add deterministic regression coverage with a device that distinguishes/panics on synchronous reads while permitting async reads. Exercise flushed-key Apply through the real connection driver, plus generation-race/retry exhaustion and relevant BatchApply paths, and assert correctness and nonblocking progress of an unrelated operation while a cold read is pending.
4. Run targeted tests, then the full debug `vals-store` and `vals-cluster` suites (no cargo fmt; hand-format touched regions).
5. Rebuild and deploy before hardware use, then run the 0169 F-protocol harness on lab with crawl-window metrics/perf capture. Confirm serving-thread profiles no longer contain `vals_spdk_read`, deep zero-maintenance crawl windows below 100k ops/s are eliminated or reduced to per-op latency blips, and pass means are no worse than the recorded baseline. Record scripts/config, results, profiles, and conclusions under a new task-0172 benchmark artifact and update the task work log/docs as appropriate.

### Starting points
- crates/vals-cluster/src/net/server.rs: ConnDriver::dispatch_frame Apply arm, serve_apply retry fallback, serve_request synchronous helper
- crates/vals-cluster/src/replica.rs: apply_stage/apply_commit_staged/current_version
- crates/vals-store/src/engine.rs: resolve_run_probe_counted/version_from_probe_counted
- crates/vals-spdk/src/spdk.rs and csrc/spdk_shim.c: AsyncRead vs vals_spdk_read
- bench/lab-bench-20260810-0169/notes.md and run_arm.sh

## Work log
- 2026-08-10 plan: 1. Reconcile task 0172 with the already-staged async Apply implementation from task 0093 by tracing every production `OwnedRequest::Apply` route and every possible cold-LWW probe to identify the remai
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: Find the concrete path before changing it; task 0093 already implemented staged async single Apply, so do not duplicate that design.; crates/vals-cluster/src/net/server.rs: synchronous `store.apply` r
…[truncated]
- 2026-08-10 preload: 11 file(s), ~64 KiB seeded into implementer context
- 2026-08-10 implemented: Reconciled every production request route and the crawl profile. Single Apply was already async, but its retry exhaustion fell back to synchronous `VersionedStore::apply`; BatchApply had the same fallback and a kill switch that restored the fully synchronous route. Both exhausted retry budgets now fail closed with `Busy`, and production BatchApply is unconditionally staged. Deterministic `AsyncOnlyReadDevice` coverage panics on synchronous reads, exercises flushed Apply through `ConnDriver`, proves an unrelated hot Apply progresses while the cold read is held, and exhausts single/batch generation retries without a sync escape.
- 2026-08-10 hardware reconciliation: The first all-fresh-binary F rerun still crawled, and its serving-thread profile identified the remaining synchronous stack as background `reconcile_scoped` → `Engine::resolve`/`VersionedStore::{version,apply}` → `vals_spdk_read`. Anti-entropy local digest comparison and fetched-value apply now use staged async reads under the server ReadGate. Valid arm F172C had stable 489–553k overwrite means, no 5-second windows below 300k (minimum 734–951k server ops/s), zero read errors/misses, and 1000/1000 correctness every pass. No lone-hot-shard trigger fired; a separate serving-thread overwrite profile contains no `vals_spdk_read`. Artifacts and invalid diagnostic-arm caveats are in `bench/lab-bench-20260810-0172/notes.md`.
- 2026-08-10 verification: `cargo test -p vals-store --no-fail-fast` and `cargo test -p vals-cluster --no-fail-fast` passed in debug. Targeted staged Apply/BatchApply and anti-entropy tests passed. Clippy remains blocked by five pre-existing `vals-store` warnings unrelated to this task.
- 2026-08-10 review follow-up: Audited the re-home copy target path and replaced its per-entry synchronous `VersionedStore::apply` with bounded staged async probe/read/commit under the shard ReadGate. Retry exhaustion defers the page without advancing its cursor. Added a pre-flushed AsyncOnly target regression plus deterministic generation-race exhaustion coverage; existing copy resume/restart tests remain green. Normalized trailing whitespace in captured perf reports so cached diff checks pass.
- 2026-08-10 implementer report: Implemented task 0172 and marked it done.  Code changes: - Kept single Apply on the existing staged async probe path, removed its synchronous `VersionedStore::apply` retry-exhaustion fallback, and now
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): revise — The staged network Apply/BatchApply changes, anti-entropy conversion, regression coverage, and F172C hardware results are strong and address the paths observed in the benchmark. However, the requested
…[truncated]
- 2026-08-10 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-10 revision: Addressed the review findings.  Changes: - Audited `rehome_copy_loop` and replaced its production per-entry synchronous `VersionedStore::apply` with `rehome_copy_apply`, which:   - acquires the shard
…[truncated]
- 2026-08-10 review (sol): accept — The revision addresses the prior production-path gap. `rehome_copy_loop` now applies copied entries through a bounded staged async probe/read/commit helper under the shard `ReadGate`, defers the page
…[truncated]
- 2026-08-10 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-10 decision: accept — commit: 0172: make cold apply reads non-blocking
- 2026-08-10 usage: 35,309,016 tok (in 3,637,217, out 61,687, cache_r 31,610,112, cache_w 0) · cost n/a (unpriced)
  implementer: 28,558,538 tok (in 1,738,359, out 47,443, cache_r 26,772,736, cache_w 0) · cost n/a (unpriced)
  coordinator: 4,269,474 tok (in 1,293,084, out 8,070, cache_r 2,968,320, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,481,004 tok (in 605,774, out 6,174, cache_r 1,869,056, cache_w 0) · cost n/a (unpriced)
