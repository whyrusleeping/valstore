---
id: "0175"
title: Enable feature-gated blind Apply/BatchApply and validate overwrite throughput
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-11"
depends_on:
    - "0173"
    - "0174"
spec_refs:
    - docs/blind-append-lww.md#Reply and API contract
    - docs/blind-append-lww.md#Implementation and rollout sequence
    - docs/blind-append-lww.md#Validation
---

## Description
After the version-argmax/GC and dirty-Merkle prerequisites land, implement phases 4–5 and validate the accepted blind-append behavior in `docs/blind-append-lww.md`.

Acceptance criteria:
- `VALS_BLIND_APPEND_LWW=1` removes cold run-version probes from unconditional single and batch Apply while retaining admission, skew, gc-floor, append, requested-sync, mirror/re-home, per-entry, and partial-failure boundaries.
- The existing wire `Applied::Won` means optimistic accepted on the blind path; CAS remains authoritative and legacy/mixed replicas remain compatible.
- Flag-off behavior remains available; corrected readers/recovery are not rolled back. In-tree consumers and client docs no longer assume blind `Won` proves the LWW winner.
- Unit/integration/crash/soak tests cover stale/new puts and tombstones, retries, repeated batch keys, read repair, AE, handoff, mixed replicas, sync, and restart.
- Re-running the 0145 benchmark shows unconditional flushed overwrites approach zero apply probe blocks and removes the attributable insert/overwrite gap without unacceptable read-p99, AE, or space regression; artifacts record distributions and metrics.

## Acceptance criteria

## Plan

Implement and validate rollout phases 4–5 of docs/blind-append-lww.md:

1. Add a process-stable `VALS_BLIND_APPEND_LWW` feature switch using the repository’s existing environment/config conventions. Keep legacy resolving behavior when off; never gate or roll back the version-argmax readers/recovery or dirty-Merkle prerequisites.
2. Add a narrow blind unconditional-apply path in `VersionedStore` that preserves skew rejection, monotone live-value GC-floor admission, HLC/data-progress observation, engine append errors/backpressure, and mutable max-Version retention. On an accepted blind candidate append using unknown old-winner Merkle semantics (or a proven mutable-dominated no-op optimization only when sound), return wire `Applied::Won` as optimistic acceptance. Leave direct local put/delete and every CAS path authoritative/resolving.
3. Route network single Apply and BatchApply through the blind path when enabled. For enabled unconditional writes, do not construct or execute run probes; preserve range/re-home fencing, RAM/space admission, mirror/re-home ordering and sync-before-ack, per-entry ordered results, repeated-key semantics, partial-append behavior, group commit, requested-sync error handling, counters, and response encoding. Flag-off and mixed-replica behavior must remain compatible.
4. Audit in-tree consumers and public/client/architecture documentation so `Won`/`Lost` is treated only as an ack for unconditional replicated writes and no client-facing text claims blind `Won` proves the LWW winner. Update the blind-append design status/rollout wording to accurately describe the feature-gated implementation and its rollback constraint.
5. Add focused observability for blind accepted/appended candidates and rejection/no-op categories where the code can distinguish them, while retaining legacy probe metrics. Add targeted unit/integration tests for flag off/on (using testable config seams rather than process-global environment races), stale/new puts and tombstones over flushed runs, exact retries, repeated batch keys, requested sync/failure boundaries, restart, read repair/AE/handoff/mirror paths, mixed legacy/blind replicas, zero apply probes on the blind path, and CAS remaining authoritative.
6. Run targeted tests and full debug vals-store/vals-cluster/workspace suites without cargo fmt. Then execute the existing 0145 insert-versus-flushed-overwrite benchmark procedure with multiple passes/arm and capture artifacts: enabled/disabled throughput distributions plus apply probe/read metrics, read p99, AE/dirty backlog, runs/space indicators. Confirm enabled unconditional overwrite probes approach zero and assess regressions; if hardware execution is unavailable, leave a precise reproducible artifact/runbook and do not overstate performance acceptance.

### Starting points
- docs/blind-append-lww.md §§Reply and API contract, Implementation and rollout sequence, Validation
- crates/vals-cluster/src/replica.rs: apply_at, apply_stage, decide_and_store, note_unknown_old_winner
- crates/vals-cluster/src/net/server.rs: request dispatch, serve_apply, serve_batch_apply, group_commit, mirror paths
- crates/vals-cluster/src/antientropy.rs: in-tree Applied::Won/Lost assumptions
- crates/vals-client/src/observe.rs and ARCHITECTURE.md: metrics and client-facing semantics
- plans/overwrite-cliff-local-repro.md and task 0145 artifacts for A/B validation

## Work log
- 2026-08-11 plan: Implement and validate rollout phases 4–5 of docs/blind-append-lww.md:  1. Add a process-stable `VALS_BLIND_APPEND_LWW` feature switch using the repository’s existing environment/config convention
…[truncated]
- 2026-08-11 context hints: 6 recorded with plan
- 2026-08-11 context hints: Do not cargo fmt; existing tree is not rustfmt-clean. Hand-format touched regions.; Keep code comments only for SAFETY/doctests/Prometheus HELP; design prose belongs in docs.; docs/blind-append-lww.md
…[truncated]
- 2026-08-11 preload: 8 file(s), ~60 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0175’s feature-gated blind writer.  Changes: - Added a process-stable, default-off `VALS_BLIND_APPEND_LWW=1` switch. - Added `VersionedStore::apply_blind_at`, preserving skew and mo
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, claude
- 2026-08-11 review (sol): revise — The core blind-append implementation appears directionally correct: it is default-off and process-stable, appends without constructing run probes, preserves skew/GC-floor and sync/error boundaries, re
…[truncated]
- 2026-08-11 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-11 revision: Addressed the review comprehensively.  Changes made: - Added explicit `ApplyMode::{Legacy, Blind}` routing seams while retaining the process-stable/default-off `OnceLock` environment switch.   - `Conn
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior findings. It adds an injectable `ApplyMode` seam while retaining the process-stable environment switch, exercises both legacy and blind routing through the single-Appl
…[truncated]
- 2026-08-11 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-11 revision: Addressed the final artifact/validation gaps narrowly.  Changes: - Added optional end-to-end strong-read latency sampling to `vals-bench x netload` and `netread` via `VALS_NETREAD_LAT_SAMPLE_EVERY=N`.
…[truncated]
- 2026-08-11 revision: Finalized the task-0175 hardware artifact.  Changes: - Created `bench/lab-bench-20260811-0175/notes.md` with:   - exact three-node/four-shard, RF=2, fresh-TRIMmed 9100 PRO methodology;   - same-bin
…[truncated]
- 2026-08-11 review (sol): accept — The latest revision completes the remaining performance-validation work without undermining the previously accepted implementation. It adds optional, default-off end-to-end strong-read latency samplin
…[truncated]
- 2026-08-11 review (claude): accept — The revision fully implements task 0175: a process-stable VALS_BLIND_APPEND_LWW switch with a clean ApplyMode test seam, a blind unconditional apply path in VersionedStore that preserves skew/GC-floor
…[truncated]
- 2026-08-11 decision: accept — commit: 0175: enable and validate feature-gated blind append
- 2026-08-11 usage: 51,804,607 tok (in 5,358,360, out 114,599, cache_r 47,778,823, cache_w 91,410) · cost n/a (unpriced)
  implementer: 40,633,836 tok (in 2,527,174, out 63,014, cache_r 38,043,648, cache_w 0) · cost n/a (unpriced)
  coordinator: 6,151,653 tok (in 1,330,605, out 23,608, cache_r 4,797,440, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 5,005,422 tok (in 1,500,537, out 14,325, cache_r 3,490,560, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 13,696 tok (in 44, out 13,652, cache_r 1,447,175, cache_w 91,410) · cost n/a (unpriced)
