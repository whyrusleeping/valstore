---
id: "0156"
title: 'Post-cutover cleanup forget-quiesce vs continuous reads: bimodal read stalls (0.1–2 s) and starved release of obsolete copies'
status: done
priority: 2
created: "2026-08-08"
updated: "2026-08-08"
depends_on: []
spec_refs: []
---

## Description
Found during 0151's box validation (bench/lab-rehome-20260808/FINDINGS.md). After re-home cutovers left ~44M obsolete post-cutover copies on the former owners (20M-key zone, RF=2, 45 cutovers), the confirmed-handoff cleanup's forget quiesce (task 0062's ReadGate pause in `QuiesceGate::acquire`, crates/vals-cluster/src/node.rs:1851-1884) interacted pathologically with continuous read load:

- Each forget attempt pauses the ReadGate (blocks NEW reads) and waits an escalating `quiesce_wait` (100 ms → 2 s cap) for in-flight reads to drain; under pipelined read load a read is always in flight, so the attempt times out, defers, and retries next tick — observed `cleanup_quiesce_timeouts` 66/73 on the two loaded nodes and steady `cleanup forget deferred — 1 in-flight read(s) did not drain within {100..2000}ms` WARN spam.
- Net effect on hardware: point reads went bimodal (p50 2-4 ms, stall mode 0.7-2.2 s — the quiesce windows), aggregate read throughput collapsed 3.3k → 431 reads/s over ~40 min while nodes sat ~7% CPU, and release progress was glacial (live_keys shrank ~10k/18 min; at that pace the 44M-copy backlog takes weeks).

Both sides lose: reads stall behind the pause, and the forget starves behind the reads. Investigate:
1. Why a single in-flight read fails to drain within up to 2 s (is it queued behind the pause itself, or stuck behind the concurrent merge backlog?) — possible livelock.
2. Scope the pause: quiesce per-zone/per-range rather than gating the whole shard's reads, or forget only what is provably not being read (key-range aware pause).
3. Back off the retry cadence when deferred (don't re-pause every tick under load), and/or make the release path yield to foreground latency SLOs.

Acceptance criteria:
- A kill-the-fix style test: continuous pipelined reads + a large release backlog → reads keep a bounded tail (no multi-hundred-ms quiesce stalls) AND the backlog drains at a nonzero guaranteed pace.
- Re-home release of a 1M-entry range under read load completes in bounded time on the in-process harness.
- Hardware re-check noted for the next lab session.

## Acceptance criteria

## Plan

## Diagnosis (from source + bench/lab-rehome-20260808/FINDINGS.md)

The confirmed-handoff cleanup's forget quiesce (QuiesceGate, node.rs:1851) pauses the shard-wide ReadGate, then waits an escalating 100ms→2s (`quiesce_wait`) for `inflight==0`. Two structural problems interact:

1. **Long-lived gate holders make the drain unwinnable.** The ReadGate is held for the ENTIRE duration of streaming/staged serves: `serve_bucket_digests` (whole AE digest page across many 256KiB device windows — wall time unbounded in run count), `serve_scan` (whole staged page + span reads), `serve_sync_values`, `serve_batch_apply`, and point reads slowed by a huge run backlog. Under continuous AE healing + pipelined reads there is nearly always one holder that outlives even the 2s cap, so `drain_reads` times out (66/73 observed), the forget defers, and release starves.
2. **Every failed attempt still stalls reads for the full wait.** The pause blocks all new admissions for the whole escalated window, and the sweep loop re-attempts next tick — so the shard oscillates paused/unpaused: bimodal point reads (stall mode 0.7–2.2s = the quiesce windows), aggregate reads collapsing 3.3k→431/s, zero forget progress. Both sides lose (the observed livelock).

The escalation design (larger wait ⇒ eventually wins) is exactly backwards under this workload: it converts failure into ever-longer client stalls without ever winning.

## Fix design

**A. Cooperative pause checkpoints for long gate holders (makes drains fast).**
Add a checkpoint primitive to `ReadGate`/`InflightGuard` (net/server.rs:2366): at a checkpoint, if no pause is pending, it's a no-op (fast path, zero overhead); if a pause is pending, the holder deregisters (waking `drained` if it was last), parks until unpaused, re-acquires, and the caller learns it yielded so it can revalidate staged state. Wire checkpoints at natural window boundaries of the unbounded holders:
- `serve_bucket_digests` — between ranged read windows (where 0154 already yields);
- `serve_scan` staged path — at page/window boundaries (the staged-scan machinery already has `scan_generation` guards + restart);
- optionally `serve_sync_values`/`serve_batch_apply` if cheap (they are bounded; lower priority).

**Safety analysis required (not optional):** after a holder yields and resumes, a forget (memtable path: index removes + Delete appends; no synchronous segment reclaim) or a compact-plan install may have happened. Prove or enforce that a resumed holder cannot read reclaimed/inconsistent state: use the existing generation guards (`scan_generation`) where present; for the digest plan, either show the plan's sources are immutable across a memtable-path forget (runs untouched, memtable portion materialized at plan time) and that compact installs bump a generation it can check, or abort the page with a retryable error on generation change (AE retries next pass; rolling CRC is a backstop, not the mechanism). Add a test that exercises yield→forget→resume.

**B. Bound the pause cost + back off on failure (bounded read tail even in pathological cases).**
- Give the cleanup quiesce its own small wait cap (~250ms; keep the doubling but cap there) — with A in place drains should complete in ≈ one point-read/window time, so a big cap is no longer the win condition. Leave the compaction loop's constants alone (it benefits from A automatically; don't widen the blast radius).
- When a cleanup quiesce attempt still times out, back off instead of re-pausing every tick: enforce a minimum spacing between attempts that bounds the pause duty cycle (e.g. skip attempts for ~8× the failed wait). Keep the existing `cleanup_quiesce_timeouts` counter; consider a companion counter for backoff skips if trivial.
- Keep the existing deferred/`cleanup_ok=false` retry semantics so progress is still driven.

**C. Tests (kill-the-fix, per acceptance criteria).**
1. In-process node test: continuous pipelined point reads PLUS a long streaming holder (real digest/scan serve over a store with runs) PLUS a release backlog (keys owned by a peer, confirmed-handoff path) → assert (a) the backlog drains to zero within bounded ticks, and (b) max read-admission stall stays bounded (assert well under the old multi-hundred-ms/2s stalls; measure acquire-wait). Verify the test fails without the fix (starved forget or multi-second stall).
2. Re-home-style release harness: a large populated range (100k+ keys; use 1M if runtime stays reasonable on MemBlockDevice, else 100k default + #[ignore] 1M variant) released under continuous read load completes in bounded time.
3. ReadGate checkpoint unit tests: no-pause fast path; pause-pending → holder yields, drained fires, forget runs, holder resumes after unpause; nested/limit interactions sane.
4. Existing tests must stay green (`cleanup_quiesces_inflight_reads_instead_of_starving`, compaction quiesce tests, gate tests).

**D. Docs + notes.** Update the paragraph describing post-cutover release cost / forget-quiesce behavior (check `rg -n "quiesce" docs/`) to the new semantics. Note in the task work log that a hardware re-check on lab is owed next box session.

Style constraints: no explanatory comments in code (SAFETY notes allowed); hand-format new regions only — never `cargo fmt`. Verify with `cargo test -p vals-cluster` (+ any touched vals-store tests) and `cargo check --workspace`.

### Starting points
- crates/vals-cluster/src/node.rs:1784-1884 — drain_reads + QuiesceGate (ForgetGate impl)
- crates/vals-cluster/src/node.rs:1255-1264 — COMPACT_QUIESCE_WAIT/_MAX + quiesce_wait escalation
- crates/vals-cluster/src/node.rs:2331-2420 — sweep_loop cleanup call sites, deferred/cleanup_ok semantics, CleanupCompact submit
- crates/vals-cluster/src/net/server.rs:2366-2482 — ReadGate/AcquireRead/InflightGuard/ReadPause
- crates/vals-cluster/src/net/server.rs:2110-2144 serve_bucket_digests (holds gate across whole streamed digest page); 2207 serve_scan; 2348 serve_sync_values; 1929 serve_batch_apply
- crates/vals-cluster/src/antientropy.rs:433-719 — cleanup_with (gate.acquire before still_ours check + forget/forget_plan)
- crates/vals-store/src/engine.rs:2798 forget_keys (memtable path: Delete appends + index removes, no sync segment reclaim); 2367 forget_plan
- crates/vals-cluster/src/node.rs:3433 cleanup_quiesces_inflight_reads_instead_of_starving (existing test pattern to extend)
- bench/lab-rehome-20260808/FINDINGS.md section 4 — hardware symptoms
- 0154 landed streaming digest serve with yields between 256KiB windows — checkpoint goes at that same boundary

## Work log
- 2026-08-08 plan: ## Diagnosis (from source + bench/lab-rehome-20260808/FINDINGS.md)  The confirmed-handoff cleanup's forget quiesce (QuiesceGate, node.rs:1851) pauses the shard-wide ReadGate, then waits an escalati
…[truncated]
- 2026-08-08 context hints: 10 recorded with plan
- 2026-08-08 context hints: crates/vals-cluster/src/node.rs:1784-1884 drain_reads + QuiesceGate; :1255-1264 quiesce_wait; :2331-2420 sweep_loop cleanup; :3433 existing starvation test; crates/vals-cluster/src/net/server.rs:2366-
…[truncated]
- 2026-08-08 preload: 3 file(s), ~11 KiB seeded into implementer context
- 2026-08-08 implementer report: Implemented task 0156.  Changes: - Added cooperative `ReadGate` checkpoints: active holders deregister under a pending pause, signal drain, park, then re-acquire with gate-limit/nested-pause behavior 
…[truncated]
- 2026-08-08 revision: Addressed both review revisions.  Changes: - Fixed the vlog-compaction safety gap in `execute_span_page`: any actual `ReadGate` checkpoint yield now invalidates the span page immediately and returns `
…[truncated]
- 2026-08-08 review tier: single-opus — reviewers: sol
- 2026-08-08 review (sol): accept — The change addresses the cleanup livelock with cooperative ReadGate checkpoints, generation-safe restart/invalidation behavior for streamed digests and scans, a cleanup-specific 250 ms cap plus retry 
…[truncated]
- 2026-08-08 decision: accept — commit: 0156: post-cutover release no longer livelocks with continuous reads — cooperative ReadGate checkpoints let long streamed serves (AE bucket digests incl. legacy sidecar chunks, staged scan/span wind
…[truncated]
- 2026-08-08 usage: 12,008,617 tok (in 2,272,767, out 56,234, cache_r 12,488,828, cache_w 803,270) · cost n/a (unpriced)
  implementer: 10,149,990 tok (in 1,410,147, out 28,419, cache_r 8,711,424, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,841,067 tok (in 862,560, out 10,315, cache_r 968,192, cache_w 0) · cost n/a (unpriced)
  coordinator: 17,560 tok (in 60, out 17,500, cache_r 2,809,212, cache_w 803,270) · cost n/a (unpriced)
