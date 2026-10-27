---
id: "0161"
title: Pre-floor reclaim of cross-run LWW-shadowed vlog garbage (the ~60-min backlinks soak wall)
status: done
priority: 1
created: "2026-08-09"
updated: "2026-08-09"
depends_on: []
spec_refs:
    - docs/backpressure.md#Zone-region capacity and maintenance reserve
    - docs/out-of-core-index.md
---

## Description
The 0130 soak arms of 2026-08-09 (`bench/bootes-soak-lab/run-20260809T*/FINDINGS.md`, four arms) established that the backlinks ~60-min wall is caused by cross-run LWW-shadowed vlog garbage that **no current signal can see**: an old value version stays "live" in the run that references it until a merge folds that run with its shadower, so `lsm_dead_fraction` reads < 0.10 while the region is actually ~87% dead, the pressure gate never opens pre-floor, and the region meets the reserve floor mostly dead — where the aged-vlog GC (landed this session) is budget-starved (~few segs net per 1.6 GB index rewrite) and the producer stalls.

Two naive designs are now empirically ruled out:
- **v3 hard low-free pressure gate (free ≤ 0.35) + one-shot bottommost fold** (arm 4, `run-20260809T134528Z-fold-gc`): the always-on trigger has no net-gain discipline → perpetual ~3 s zero-gain pressure merges (~500 MB/s storm, 143 GiB churned in minutes), plus reserve inflation from pricing the FULL gainful victim prefix in `minimal_pressure_needed_segs` → false `region_exhausted` terminals (209×) on reclaimable states.
- **Floor-only GC** (arm 3, `run-20260809T122519Z-floor-gc`): engages but nets ~0/pass at floor budget; unusable as recovery.

Design requirements for the real fix:
1. A garbage signal that sees cross-run shadowing WITHOUT full merges — candidates: per-run shadow estimation via bloom/range overlap between runs; sampling key lookups; accounting deltas at flush time (bytes superseded per flush window); or folding incrementally (tiered merges already dedupe — attribute their drop counts to a running shadowed-bytes estimator).
2. Net-gain discipline at the TRIGGER level (0159's lesson, re-learned in arm 4): a reclaim cycle must be planned only when its projected net gain is real, must disarm itself when wrong, and must never become a permanent per-sweep loop.
3. Reserve pricing must use the SMALLEST gainful victim prefix (flexible shapes mean minimal cost is small), never the full prefix — arm 4's inflation.
4. Resolve the open puzzle first, locally: single-run GC passes on the real workload freed ≈ index-only despite an apparently ample budget (arm 4 findings §3) — build a file-device repro with a realistic per-seg dead distribution (high-cardinality keyspace, hot-repo overwrite clustering) and verify victim selection/prefix efficacy before another hardware arm.

Acceptance criteria:
- Local repro reproducing the invisible-garbage fill and the floor-arrival-mostly-dead state at soak-like shape.
- Pre-floor reclaim keeps a full-rate bootes-shaped overwrite workload's region occupancy bounded (never meets the floor mostly-dead) without zero-gain churn (device-write amplification bounded and measured).
- Kill-the-fix tests for the trigger discipline (no perpetual pressure loops, no false terminals, reserve not inflated).
- Hardware validation: a fresh bootes soak arm passes the ~60-min wall with the producer never stalling on the backlinks pair (this unblocks 0130's multi-hour bar).

## Acceptance criteria

## Plan

Goal: make cross-run LWW-shadowed vlog garbage visible pre-floor and reclaim it with disciplined triggers, locally proven on a bootes-shaped repro; hardware arm last.

Context (from the four 2026-08-09 arms + code reading):
- Per-run accounting (`value_seg_bytes`) counts a run's own references as live by definition; a key's old version in run R is garbage only relative to NEWER runs/memtable. So `lsm_dead_fraction` and single-run victim selection are both structurally blind to cross-run shadow garbage — this is very likely the arm-4 "single-run GC freed ≈ index-only" puzzle (its victims only carry merge-history dead, not shadow dead). Phase A must confirm this empirically.
- Merges (merge_cursors) are pure LWW dedupe — dropping an older shadowed version is always legal; folds are the only place shadow garbage becomes visible. Tiered fold cadence (ratio/cap-gated) is far too slow vs. garbage accumulation on an overwrite-heavy workload → region meets the floor mostly dead.
- Node pressure gate (node.rs zone_maint_due): free ≤ 0.50 AND dead ≥ 0.10 — the gate shape is fine; it's fed a blind dead signal. Arm 4's mistake was removing the dead condition instead of fixing the signal.
- Run bloom filters (RunFilter) are RAM-resident → cross-run membership probes are nearly free.
- Reserve pricing already uses the smallest gainful victim prefix (one_run_victim_needed_segs → gainful_victim_prefix_len); requirement is to PIN it with tests, not change it.

Phase A — file-device repro + puzzle resolution (first; informs B):
Engine-level driver (heavy #[ignore]-able test or test-util harness in vals-store) on a file/mem block device, few-hundred-MB region: high-cardinality keyspace (~1M+ keys), 256B–1KB values, clustered hot-key overwrites (~20% of keys take ~80% of writes, keys clustered so shadowing lands in aged runs). Drive puts + a sweep loop replicating zone_maint_due gating (flush thresholds, run-threshold recurring merges, pressure planning + aged-vlog GC). Track ground-truth live bytes (driver-side key→len map) vs visible lsm_dead_fraction, free segments, per-run victim availability/gainful prefix. Must reproduce: floor arrival mostly-dead with visible dead < 0.10, and single-run GC netting ≈ index-only; record why (expected: per-run victims have almost no visible dead).

Phase B — fix (leading design; adapt details to Phase A findings):
1. Engine shadow estimator: sample K (~128–256) entries per run from a few random in-RAM block refs (bounded device reads), probe all newer runs' RAM blooms + memtable for key membership; value-length-weighted shadowed fraction × run.value_bytes → per-run shadowed_bytes_est; cache keyed on run-set generation, refresh rate-limited and only when free_fraction ≤ soft threshold (steady-state cost ~0). Bloom-FP overestimate (~1%/run) noted and tolerated (gate thresholds ≫ FP noise; disarm feedback backstops). Kill switch VALS_SHADOW_EST=0.
2. Corrected signal: engine `lsm_dead_fraction_est()` = visible dead + estimated shadowed bytes; node MaintSignals feeds THIS to the existing pressure gate (free ≤ 0.50 && dead ≥ 0.10). Raw gauge stays.
3. Pre-floor shadow fold: in the pressure planner, when pre-floor (never at floor) and aged-vlog GC has no candidate and the capped suffix excludes the run(s) carrying the bulk of estimated shadow garbage, plan a fold whose suffix starts at that run — allowed past VALS_MERGE_INPUT_CAP_BYTES only under trigger discipline: (a) projected net gain (est recoverable segs) ≥ max(abs min, ratio × fold index-churn segs); (b) one-shot: after commit compare realized discovered-dead vs predicted, on mismatch (<~50%) exponential backoff/disarm until estimate materially regrows + new intake; (c) unconditional minimum spacing between shadow folds (intake-bytes based). After the fold the existing (already net-gain-disciplined) aged-vlog GC reclaims the now-visible dead. Kill switch VALS_SHADOW_FOLD=0.
4. Reserve/exhaustion untouched: maint_reserve_segs and region_exhausted must be provably unchanged by the new machinery.
5. Metrics (observe.rs, with HELP): shadowed-bytes-est gauge, shadow_folds, shadow_fold_disarms.
6. docs/backpressure.md: replace the "remains open work" sentence with the estimator+fold design.

Phase C — kill-the-fix tests:
- Repro-as-test: fix ON → occupancy bounded (never floor-mostly-dead), measured device-write amplification under a stated bound; VALS_SHADOW_FOLD=0 → reproduces the wall (pins the repro).
- Storm-free (arm-4 lesson): disjoint-key fill at low free → zero shadow folds, no perpetual pressure merges.
- Disarm: wrong estimate → exactly one fold then backoff.
- No false terminals / no reserve inflation: region_exhausted and maint_reserve identical with the machinery on/off across mixed states.
- Tests fabricating fullness pin VALS_ORPHAN_RECLAIM=0.

Phase D — hardware validation (coordinator-run, after local acceptance): fresh bootes soak arm per plans/bootes-soak-runbook.md; pass the ~60-min backlinks wall with the producer never stalling; FINDINGS.md under bench/bootes-soak-lab/. If the session runs out of room this phase is split to a follow-on task.

Constraints: no cargo fmt (tree not rustfmt-clean); no explanatory comments (only SAFETY/Prometheus HELP/doctests; design prose in docs/); verify with cargo test -p vals-store, -p vals-cluster, cargo check --workspace.

### Starting points
- crates/vals-store/src/engine.rs: compact_plan_inner ~2395 (PlanKind::Pressure path), aged_vlog_gc_* ~2674-2800, select_victims/victim_lives_ascending/gainful_victim_prefix_len/affordable_victim_prefix ~2803-2918, minimal_pressure_needed_segs ~3329, region_exhausted ~3370, maint_reserve_segs ~3489, lsm_dead_fraction ~3536, merge_all_runs/merge_cursors LWW dedupe ~2250
- crates/vals-store/src/run.rs: RunFilter::contains (RAM blooms) ~410, DeviceRun block refs/block_scan ~1922, for_each_bucket_digest ~1997
- crates/vals-cluster/src/node.rs: PRESSURE_FREE_FRACTION=0.50 / PRESSURE_DEAD_MIN_FRACTION=0.10 ~1183, MaintSignals::sample ~1199, zone_maint_due ~1221, sweep planning ~1691-1716
- docs/backpressure.md#Zone-region capacity and maintenance reserve (lines ~160-230): current design prose incl. the 'remains open work' sentence to replace
- Existing test scaffolding for multi-run fills: engine.rs tests aged_dead_vlog_gc_reclaims_multi_run_state_pre_floor ~11583, floor_multi_run_state_reclaims_via_aged_vlog_gc ~11501; env-guard patterns co_compact_env/orphan_reclaim_off_env ~4950
- bench/bootes-soak-lab/run-20260809T134528Z-fold-gc/FINDINGS.md: arm-4 storm + false-terminal evidence; the puzzle in §3
- Env knob style: fn vlog_gc_enabled()/pressure_net_gain_enabled() in engine.rs ~520-545

## Work log
- 2026-08-09 plan: Goal: make cross-run LWW-shadowed vlog garbage visible pre-floor and reclaim it with disciplined triggers, locally proven on a bootes-shaped repro; hardware arm last.  Context (from the four 2026-08-0
…[truncated]
- 2026-08-09 context hints: 7 recorded with plan
- 2026-08-09 context hints: engine.rs key line refs: compact_plan_inner ~2395 (Pressure path at 2409-2417 does orphan reclaim + aged-vlog GC first), aged_vlog_gc_shape ~2696 (shared-seg exclusion + affordable prefix + net-gain m
…[truncated]
- 2026-08-09 preload: 5 file(s), ~35 KiB seeded into implementer context
- 2026-08-09 review tier: high-powered — reviewers: sol, claude
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — Faithful and disciplined implementation of the 0161 plan: a sampled cross-run shadow estimator (bloom/memtable probes, generation-keyed cache, free-fraction soft gate), a corrected pressure-gate signa
…[truncated]
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — The revision cleanly resolves the observe-only-arm nit: lsm_dead_fraction_gate now refreshes the shadow estimator whenever VALS_SHADOW_EST=1 (gauge stays live, including during cooldown) while VALS_SH
…[truncated]
- 2026-08-09 decision: accept — commit: 0161 (local phases): pre-floor reclaim of cross-run LWW-shadowed vlog garbage — sampled shadow estimator + disciplined pre-floor shadow fold; plus a 0160 orphan-sweep corruption fix found by the rep
…[truncated]
- 2026-08-09 decision: accept — commit: 0161 Phase D hardware arm + re-diagnosis: shadow machinery validated SAFE and ACCURATE at production scale; the ~60-min backlinks wall is a genuine CAPACITY wall, not reclaimable garbage. Fresh lab
…[truncated]
- 2026-08-09 usage: 225,875 tok (in 496, out 225,379, cache_r 49,816,148, cache_w 3,004,676) · cost n/a (unpriced)
  coordinator: 200,070 tok (in 430, out 199,640, cache_r 47,631,577, cache_w 2,900,946) · cost n/a (unpriced)
  reviewer:claude: 25,805 tok (in 66, out 25,739, cache_r 2,184,571, cache_w 103,730) · cost n/a (unpriced)
