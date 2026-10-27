---
id: "0166"
title: 'Smooth the sustained-overwrite merge cadence: pace tiered merges against ingest instead of pass-sized bursts'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
Follow-on to 0145. With merge-when-selectable scheduling, sustained overwrite is no longer degrading but oscillates in a ~2-pass tiered cycle: fast passes (419-454k ops/s, runs grow 4→12/node) alternate with merge-burden passes (201-282k ops/s) as each shard-engine crosses min fan-in (4) and merges its full admissible suffix at once (evidence: `bench/lab-bench-20260810-0145/notes.md`). The mean (~318k) is fine; the amplitude (±40%) is the gap.

Ideas to evaluate (measure first):
- Pace merge input: cap per-merge input (below `VALS_MERGE_INPUT_CAP_BYTES`) so merges run smaller and more often, overlapping ingest instead of monopolizing a pass.
- Consider lowering effective min fan-in under sustained cold-resolve load (e.g. when `apply_probe_runs`/`apply_run_reads` ratio is high) — trades write amp for latency stability.
- Check whether merge (off-core) device bandwidth contention with the serving path's cold LWW block reads is the dominant slow-pass term (SPDK qpair contention), vs commit-time serving-core slices.

## Acceptance criteria
- Repeat the `bench/lab-bench-20260810-0145` F protocol: overwrite passes within ±15% of their mean, mean not lower than the current ~318k.
- No regression in write amp beyond ~10% (run_merge_input_bytes per ingested byte).

## Plan

Goal: cut the ±40% pass-over-pass amplitude of sustained overwrite (2-pass tiered merge cycle) to ±15% around a mean ≥ ~318k, with write-amp regression ≤10% (bench/lab-bench-20260810-0145 F protocol).

Phase 1 — measure first (hardware attribution):
1. rsync + rebuild HEAD on lab-1/2/3 (SPDK), fresh format=true cluster (existing node.conf, 4 shards/node, RF=2; no PD needed for `vals-bench x netload`).
2. Re-baseline the F protocol (8M×256B, 16 coords×64, pass1 insert + 5 overwrite passes) at current HEAD, with a 5s per-node metrics sampler (server ops rate, run_merges, run_merge_ms/input/output bytes, resident_runs, apply_probe_*, used_segments, flush/compact commit hists) + node logs ("merged N runs off-core … ms" lines per shard) to attribute the slow-pass term: synchronized multi-shard merge bursts (each of the 4 shards has its own compact worker; min fan-in 4 makes them all cross selectability in the same pass) vs serving-core commit slices vs probe amplification.
3. Save to bench/lab-bench-20260816-0166/.

Phase 2 — lever (based on attribution; candidates, smallest-risk first):
- Desynchronize/pace shard merges: per-node cap on concurrent recurring merges (env-tunable, e.g. VALS_MERGE_NODE_CONCURRENCY, gate ONLY the recurring path — pressure/floor/lone-GC merges never gated, preserving 0085/0089 liveness) so merge burden spreads across passes instead of bursting on all 4 shards at once; and/or per-shard phase jitter on merge-due.
- Fallback: smaller folds (cap fan-in nearer min_k) if attribution shows single-merge size, not synchronization, dominates — watching the ≤10% WA budget.
Implementation delegated to the implementer if the backend is healthy (memory notes it 400'd on 08-14/15; if still down, implement directly per recorded precedent). Unit tests for the scheduling change + local repro (plans/overwrite-cliff-local-repro.md) as regression sanity; debug suites for vals-store/vals-cluster.

Phase 3 — validate: rebuild boxes, repeat the F protocol on a fresh cluster with the lever on; acceptance = overwrite passes within ±15% of mean, mean ≥ baseline re-run's mean (~318k reference), run_merge_input_bytes per ingested byte within +10% of the same-session baseline. If the knob needs tuning, A/B via env without rebuild. Write up notes.md; note the default chosen.

### Starting points
- crates/vals-store/src/engine.rs:520 merge_input_cap_bytes / :645 select_recurring_merge / :2262 recurring_merge_selectable / :2488 compact_plan_inner (PlanKind::Recurring vs Pressure)
- crates/vals-cluster/src/node.rs:1223 zone_maint_due / :1687 snapshot_loop scheduling / :806 per-shard maint::spawn (one compact worker per shard)
- crates/vals-cluster/src/maint.rs worker_loop
- bench/lab-bench-20260810-0145/notes.md — the oscillation evidence + protocol
- plans/bootes-soak-runbook.md §0.5 rsync+build commands
- plans/overwrite-cliff-local-repro.md — local file-backend sanity repro (flat ~150k; does NOT reproduce the amplitude)

## Work log
- 2026-08-10 plan: Goal: cut the ±40% pass-over-pass amplitude of sustained overwrite (2-pass tiered merge cycle) to ±15% around a mean ≥ ~318k, with write-amp regression ≤10% (bench/lab-bench-20260810-0145 F p
…[truncated]
- 2026-08-10 context hints: 6 recorded with plan
- 2026-08-10 context hints: crates/vals-store/src/engine.rs:128 build_and_write_flush, :296 compact_and_write (chunk loop ~379-418), :2488 compact_plan_inner (PlanKind), :645 select_recurring_merge (forced branch at n>=force_run
…[truncated]
- 2026-08-10 preload: 4 file(s), ~28 KiB seeded into implementer context
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The task's literal acceptance target (±15% pass amplitude at mean ≥318k) was not met — but the change does exactly what a measure-first task should: a rigorous 6-arm hardware attribution shows th
…[truncated]
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The revision addresses the material finding from the previous round: permit gating is now decoupled from pacing via a dedicated permit_gated flag set only for unforced Recurring/Forget plans, so lone-
…[truncated]
- 2026-08-10 decision: accept — commit: 0166: sustained-overwrite cadence — attribution refutes the merge-scheduling premise; ship maintenance-I/O duty-cycle pacing instead. Six-arm hardware attribution (bench/lab-bench-20260810-0166):
…[truncated]
- 2026-08-10 usage: 159,136 tok (in 392, out 158,744, cache_r 29,036,330, cache_w 986,019) · cost n/a (unpriced)
  coordinator: 142,622 tok (in 340, out 142,282, cache_r 27,304,828, cache_w 898,271) · cost n/a (unpriced)
  reviewer:claude: 16,514 tok (in 52, out 16,462, cache_r 1,731,502, cache_w 87,748) · cost n/a (unpriced)
