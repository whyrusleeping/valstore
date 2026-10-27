---
id: "0201"
title: Hardware A/B linear victim-live aggregation under mature batch ingest
status: done
priority: 2
created: "2026-08-13"
updated: "2026-08-14"
depends_on: []
spec_refs:
    - bench/task-0200-atproto-backfill/FINDINGS.md
    - ARCHITECTURE.md
---

## Description
Validate task 0200's measured maintenance-planning optimization on a controlled fresh or safely restartable SPDK corpus. The current implementation changes `victim_lives_ascending` and `smallest_victim` from one full run-segment scan per victim to one aggregation pass; perf attributed 22.99% of core cycles (38.55% atom cycles) to the old helper during a mature 1,000-entry persistent batch arm.

Acceptance criteria:
- Interleaved baseline/candidate hardware arms use the same mature corpus, profile, sorted zones, 1,000-entry persistent batches, and clean 1/2/4/8 caller sweep.
- Retain source/binary/config identity, throughput/errors/tails, per-thread CPU profiles, BatchApply gate counters, run/merge/space state, and correctness checks.
- Demonstrate a repeatable material CPU or clean-throughput improvement without changed victim selection, space reclamation, errors, or tails; otherwise revert the candidate.
- Run full vals-store/vals-cluster checks and update task-0200 findings with the final decision.

## Acceptance criteria

## Plan

1. Preserve and preflight the stopped lab variable-layout corpus left by task 0202: prove all three nodes are inactive, retain the current capacity-profile/format=false configs, catalog/sorted-range state, corpus/run/merge/space baselines, and read/scan correctness hashes before any writer starts. Refuse any format=true or destructive step.
2. Make the candidate behaviorally identical to the old helpers for all accepted metadata, including deterministic tie ordering and the old parallel-vector zip semantics. Add a deterministic differential regression that compares candidate victim/live aggregation, smallest-victim selection, and sorted victim order against an in-test reference implementation over varied runs, shared/zero-live segments, ties, absent metadata, and uneven vectors.
3. Build two release SPDK server binaries from the same current source/config on every node: candidate and a baseline differing only by reversal of the linear-aggregation patch. Retain source manifests/diffs, build logs, executable hashes/sizes, node configs, process command lines/environments, kernel/host provenance, and client identity. Restart only with format=false and verify the same persisted extents/catalog/scans after every restart.
4. Run a rotated, interleaved baseline/candidate sequence on that same mature capacity-profile corpus. In every repetition run a clean 1/2/4/8-caller sweep of device-persistent 1,000-entry batches against the same sorted zones and key/value shape, with fixed warmup and measurement windows. Use disjoint arm prefixes or monotonic versions to preserve correctness while matching offered work, and reject Busy/partial/transport errors.
5. For every arm retain client throughput, error counts and latency samples/tails; server metrics before/after (BatchApply gate/run-read/probe/sync/latency counters, records, flushes/merges, merge bytes, used/free/min-free segments, runs, compaction/space/terminal errors); process/thread CPU counters; and per-thread perf profiles of the hottest shard during the mature 8-caller phase. Perform post-arm sampled reads/scans and catalog/extent checks.
6. Analyze matched/interleaved deltas, profiles, selection-equivalence tests, reclamation/run/merge state, errors, and tails. Retain the optimization only for a repeatable material CPU or clean-throughput improvement with no correctness/selection/reclamation regression; otherwise restore the baseline implementation. Update bench/task-0200-atproto-backfill/FINDINGS.md with the method, evidence, and final decision, and add a task-0201 result artifact with raw-data provenance.
7. Run the full debug vals-store and vals-cluster suites (plus focused candidate differential tests and an optimized vals-store/server build), inspect the complete diff, and prepare the accepted result for review.

### Starting points
- crates/vals-store/src/engine.rs: Engine::{smallest_victim,victim_lives_ascending} and staged candidate diff
- bench/task-0200-atproto-backfill/FINDINGS.md §Profile finding and candidate / §Required next validation
- bench/task-0202-zone-geometry/FINDINGS.md: stopped format=false capacity-profile variable-layout corpus and retained raw harness evidence
- plans/bootes-soak-runbook.md and plans/backfill-replay-runbook.md for lab/SPDK lifecycle and correctness checks
- bench/task-0186-blind-batchapply/ harness patterns for provenance, fixed-duration sweeps, latency, metrics, PMU and fail-closed validation

## Work log
- 2026-08-14 plan: 1. Preserve and preflight the stopped lab variable-layout corpus left by task 0202: prove all three nodes are inactive, retain the current capacity-profile/format=false configs, catalog/sorted-rang
…[truncated]
- 2026-08-14 context hints: 5 recorded with plan
- 2026-08-14 context hints: The lab clock currently reports 2026-08-14; systemd vals-node is inactive on all 3 and ~/vals/run/node.conf is profile=capacity, format=false, with 1/80/80-ish persisted variable layout from task 0
…[truncated]
- 2026-08-14 preload: 6 file(s), ~34 KiB seeded into implementer context
- 2026-08-14 result: Completed the preserved-corpus three-repetition hardware matrix with 24/24 clean primary arms and retained per-arm provenance, metrics, PMU/thread profiles, tails, scans, and zone state. The matrix did not reproduce the hotspot or establish a repeatable candidate benefit; same-prefix insert/overwrite work and 2:1 order confound observed CPU/tail differences, and no primary interval crossed a merge. Reverted the production helpers exactly, including legacy equal-live ties, and retained only a test-local linear semantic differential. See `bench/task-0201-victim-live/FINDINGS.md`.
- 2026-08-14 review revision: Removed the production tie change; made the test-local candidate cover exact sorted vectors, legacy tied-minimum membership, empty/absent/uneven/saturating semantics; corrected architecture, binary sizes, and confound wording; hardened analysis around required real error and device-sync counters; normalized text artifacts; and reran focused/full tests, optimized builds, analysis, patch/shell checks, and full staged diff checking. Status remains `in_review`.
- 2026-08-14 implementer report: Completed task 0201 and applied the fail-closed result.  - Preserved/reopened the task-0202 lab corpus only with `profile=capacity` and `format=false`; retained configs, catalog, scans, extent/zone
…[truncated]
- 2026-08-14 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-14 review (sol): revise — The implementation makes the correct fail-closed production decision and retains unusually complete hardware provenance, but two required consistency/validation pieces are still missing: the durable a
…[truncated]
- 2026-08-14 review (sol#2/sol): accept — The production candidate was correctly reverted after the hardware matrix failed to show a repeatable benefit, and the retained deterministic tie-break is small, sensible, and covered across the relev
…[truncated]
- 2026-08-14 revision: Addressed the review in one consolidated revision:  - Fully restored production victim helpers to the exact pre-candidate implementation, including legacy unspecified equal-live `smallest_victim` ties
…[truncated]
- 2026-08-14 review (sol): accept — The revision resolves the blocking issues. Production now restores the exact pre-candidate nonlinear helpers, the test-only differential actually exercises the linear aggregation across the required e
…[truncated]
- 2026-08-14 review (sol#2/sol): accept — The revision resolves the prior correctness and cleanliness findings. Production now exactly restores the pre-candidate nonlinear helpers; the test-only linear implementation is differentially checked
…[truncated]
- 2026-08-14 decision: accept — commit: bench(0201): revert unproven victim aggregation
- 2026-08-14 usage: 37,705,764 tok (in 4,921,502, out 101,254, cache_r 32,683,008, cache_w 0) · cost n/a (unpriced)
  implementer: 30,337,837 tok (in 2,243,732, out 72,345, cache_r 28,021,760, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,853,242 tok (in 1,108,031, out 10,043, cache_r 1,735,168, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,471,454 tok (in 802,428, out 11,170, cache_r 1,657,856, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,043,231 tok (in 767,311, out 7,696, cache_r 1,268,224, cache_w 0) · cost n/a (unpriced)
