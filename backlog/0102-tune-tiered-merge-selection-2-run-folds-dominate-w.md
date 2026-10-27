---
id: "0102"
title: 'Tune tiered merge selection: 2-run folds dominate (worst-case WA); reach k≈4–8 fan-in in steady state'
status: done
priority: 3
created: "2026-07-09"
updated: "2026-07-09"
depends_on:
    - "0069"
spec_refs: []
---

## Description
Found in the task-0101 short A/B (bench/t0101/notes.md): 328 of 360 recurring merges in the decoupled arm folded only 2 runs — the worst-WA increment (~63% per 0088; WA ∝ 1/(k−1), k=4–8 would cost 5–15%). The `select_merge_suffix` gate (VALS_MERGE_TIER_SIZE_RATIO=1.5 default, VALS_MERGE_TIER_K=6) stops the newest-suffix too eagerly under a steady flush cadence, so cumulative decoupled merge traffic ran ~2.8× ingested data where k≈6 would give ~1.3–1.5×.

Acceptance criteria:
- Reproduce with a unit-level trace of select_merge_suffix on a steady same-size-flush sequence; adjust the selection defaults/logic so typical steady-state folds reach k≈4–8 (e.g. wait for k similar-sized runs before merging rather than folding each new pair).
- Show reduced cumulative merge input per ingested byte on a driven engine test (or re-run the t0101 arm) with no run-count blowup (bounded resident runs).
- Store + cluster suites green.

## Acceptance criteria

## Plan

## Root cause (confirmed in code, engine.rs)

`compact_plan_inner` (engine.rs ~3497) computes the tiered newest-suffix selection via `select_merge_suffix` (line 877), then applies `s.min(n - min_input_runs)` — for `PlanKind::Recurring` (min 2) this FORCE-FOLDS the next-older run whenever the gate correctly stopped at 1 run ("wait for more similar-sized runs"). On a steady flush cadence the next-older run is the accumulated mid/base run, so every new flush produces a 2-run fold that re-copies the base (+1 flush stride each time) — exactly the t0101 pathology (328/360 merges, WA ≈ 63%/increment).

## Fix

1. **Skip instead of force-fold.** In `compact_plan_inner`, for `PlanKind::Recurring` (non-legacy): drop the `s.min(n - min_input_runs)` clamp. Let `sel = n - start` (fan-in the gate chose). If `sel < min_fanin`, return `Ok(None)` — no merge this tick; runs accumulate until the ratio cascade produces a ≥ min_fanin fold. LoneGc/Pressure/legacy paths unchanged (bottommost, min 1).
2. **Minimum fan-in knob.** New const `MERGE_TIER_MIN_K: usize = 4` + env override `VALS_MERGE_TIER_MIN_K` (getter alongside `merge_tier_k()`, clamp to ≥2 and ≤ merge_tier_k). Rationale: classic STCS min_threshold; with ratio 1.5 / k 6 this yields steady-state fan-ins ~4–6 (hand-simulated: folds go 4,4,4,6… on same-size flushes with an accreting base, and the base graduates bottommost when cum(newer) ≥ base/1.5).
3. **Safety valve against run pileup** (pathological size sequences — e.g. strictly-shrinking flushes — could otherwise skip forever while run_count grows): if the gate says skip but `n >= merge_force_runs()` (new const `MERGE_FORCE_RUNS = 2 × MERGE_TIER_K = 12`, env `VALS_MERGE_FORCE_RUNS`), re-run `select_merge_suffix` with `size_ratio = f64::INFINITY` (k and input-size cap still apply) and proceed with that selection (≥2 runs). This bounds resident runs / read amp without reintroducing the base-refold as a steady-state behavior.
4. Update the doc comments on `select_merge_suffix` + `compact_plan_inner` to describe skip semantics, min fan-in, and the valve.

## Tests

5. **Unit-level steady-cadence trace (acceptance #1):** new test simulating the selection policy loop on a same-size flush sequence — maintain a `sizes` vec, each step push one flush-size run, apply the same decision logic (skip / fold / valve), replace folded suffix by its sum. Assert over ~100 steps: (a) every fired fold has fan-in ≥ 4 (except explicit valve cases — none should occur here), (b) resident run count stays bounded (≤ ~10), (c) cumulative merge input bytes / cumulative ingested bytes is quasi-linear — well under the ~2.8× measured pre-fix (expect ≲1.5×). If the decision logic is extracted into a small pure helper (e.g. `select_recurring_merge(sizes, k, min_k, ratio, cap, force_runs) -> Option<usize>`) call that helper from both `compact_plan_inner` and the test — preferred, keeps policy unit-testable like `select_merge_suffix` already is.
6. **Driven engine test (acceptance #2):** extend/add next to `recurring_merge_bounds_run_count_and_reads_transparently` (engine.rs ~11029): drive flush+off-core-merge cycles on MemBlockDevice, accumulate per-plan input bytes (Σ merged_inputs handle.len) and ingested bytes; assert bounded max run count, reduced cumulative merge input per ingested byte, and read transparency (newest value wins). IMPORTANT: that test's `while e.run_count() >= 4 { if let Some(plan) ... }` loop will now spin forever on a None plan — restructure to break when compact_plan returns None.
7. **Fix existing tests that assume a 2–3-run recurring plan yields Some.** Engine tests around lines 6801, 6895, 8412, 8533, 8598, 10731, 11175, 11238, 11295, 11337 call `e.compact_plan(...).unwrap().unwrap()` — any that build < 4 similar runs will now get None. Either build ≥4 runs or set `VALS_MERGE_TIER_MIN_K=2` under the existing `CO_COMPACT_ENV_LOCK` env-mutex pattern (engine.rs ~6184; also add `remove_var("VALS_MERGE_TIER_MIN_K")`/`VALS_MERGE_FORCE_RUNS` to `co_compact_env`/`cluster_reclaim_env` so leaks can't cross tests). Same check for vals-cluster maint.rs/node.rs/replica.rs tests and vals-bench main.rs:5779 (make sure nothing loops on "plan until None" semantics).

## Verification

- `cargo test -p vals-store` and `cargo test -p vals-cluster` green (plus a full `cargo test --workspace` if time permits).
- The two new tests demonstrate both acceptance criteria at unit/engine level; the hardware re-run of the t0101 arm stays with task 0097/follow-up (not in scope here).

### Starting points
- crates/vals-store/src/engine.rs:877 select_merge_suffix (pure selection fn + its unit tests at ~10926)
- crates/vals-store/src/engine.rs:3497-3531 compact_plan_inner — the s.min(n - min_input_runs) clamp is the defect
- crates/vals-store/src/engine.rs:805-855 MERGE_TIER_K/MERGE_TIER_SIZE_RATIO/MERGE_INPUT_CAP_BYTES consts + env getters — add MIN_K and FORCE_RUNS alongside
- crates/vals-store/src/engine.rs:6184 CO_COMPACT_ENV_LOCK env-mutex pattern for tests that set process-global env
- crates/vals-store/src/engine.rs:11029 recurring_merge_bounds_run_count_and_reads_transparently — while run_count()>=4 loop would spin on None; extend for cumulative-input assertion
- crates/vals-cluster/src/node.rs:1698 RUN_COMPACT_THRESHOLD=8 — the node-level trigger; Ok(None) from compact_plan is already handled silently (node.rs ~2551)
- bench/t0101/notes.md — the measured pathology + root-cause hypothesis this task fixes

## Work log
- 2026-07-09 plan: ## Root cause (confirmed in code, engine.rs)  `compact_plan_inner` (engine.rs ~3497) computes the tiered newest-suffix selection via `select_merge_suffix` (line 877), then applies `s.min(n - min_input
…[truncated]
- 2026-07-09 context hints: 7 recorded with plan
- 2026-07-09 context hints: crates/vals-store/src/engine.rs:877 select_merge_suffix (pure selection fn + its unit tests at ~10926); crates/vals-store/src/engine.rs:3497-3531 compact_plan_inner — the s.min(n - min_input_runs) c
…[truncated]
- 2026-07-09 implementer report: ## Summary  Fixed the tiered-merge selection defect (task 0102) where recurring merges force-folded just the newest pair every flush — re-copying the accreting base (t0101: 328/360 merges folded onl
…[truncated]
- 2026-07-09 review tier: single-opus — reviewers: Claude
- 2026-07-09 review (Claude): accept — The change correctly fixes the tiered-merge 2-run-fold pathology: it replaces the `s.min(n - min_input_runs)` force-fold clamp in `compact_plan_inner` with a new `select_recurring_merge` policy that s
…[truncated]
- 2026-07-09 decision: accept — commit: engine: fix tiered merge selection base-refold (task 0102) — recurring merges skip below min fan-in (VALS_MERGE_TIER_MIN_K=4) instead of force-folding the newest pair; run-pileup valve VALS_MERGE_FO
…[truncated]
- 2026-07-09 usage: 84,254 tok (in 200, out 84,054, cache_r 5,319,859, cache_w 242,343) · $7.7401
  implementer: 55,633 tok (in 134, out 55,499, cache_r 4,296,354, cache_w 106,778) · $4.2037
  coordinator: 21,106 tok (in 30, out 21,076, cache_r 620,311, cache_w 100,153) · $2.9263
  reviewer:Claude: 7,515 tok (in 36, out 7,479, cache_r 403,194, cache_w 35,412) · $0.6101
