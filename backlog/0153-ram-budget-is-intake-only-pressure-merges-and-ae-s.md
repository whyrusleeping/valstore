---
id: "0153"
title: 'RAM budget is intake-only: non-intake maintenance/AE working sets grow past the budget to OOM (0152 run falsified 0134 on hardware)'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-07"
depends_on:
    - "0134"
spec_refs: []
---

## Description
The 0152 box run (bench/replay-lab/run-20260807T195353Z-rambudget40g/FINDINGS.md) showed the 0134 RAM budget engaging correctly (RSS sampling, Busy shedding, headroom→0) while two nodes were OOM-killed anyway at ~47 GiB **with intake fully shed**. The killing growth is non-intake working set, which the budget deliberately admits. The run establishes the failure but NOT the owning allocator — `vals_pressure_merges` = 0 and `vals_ae_seed_placement_coalesces` = 0 throughout, so the obvious suspects are unconfirmed hypotheses. Step 1 is attribution; do not fix blind.

**Established facts (from the run):**
- Both dying nodes sat at the device reserve floor for their final 15–25 min (`no free segments for snapshot` compaction-plan failures ~1/s) and could not dig out.
- On both dying nodes `run_merges` froze while RSS grew — no merge committed during the growth windows (node 1: 15→48 GiB burst in ~3 min). Whether a merge job was actively running and owned the bytes is not directly observable (no in-flight-merge gauge); the large-merge explanation is a hypothesis for step 1.
- Node 3 gained +6.6 GiB in 5 min while the only significant activity was AE cooperative seeding (38.7k steps/5 min, `ae_seed_journal_ops` ~frozen — grinding, not finalizing), sweeps, and failed compaction plans, under continuous PD auto-split churn (137 splits).
- The failed-plan retry loop alone does not leak (survivor node 2 ran the same spam and returned memory).
- Both dead boxes consumed their full 8 GiB swap; the statm-RSS-only budget check is blind to swapped-out anon pages (true footprint at kill ≈ 55 GiB vs 40 GiB budget).

**Work items:**
1. **Attribute the growth.** Instrument or reproduce (a file-device or small-corpus repro may suffice) to find which allocations own the burst and the creep. Candidates to check, in rough order: in-flight merge working set in `compact_and_write` (`RunStreamBuilder` index state, prefetch windows, co-compact value buffering, dropped-tombstone accumulation — which part scales with input size?), AE cooperative-seed staged accumulators (O(engine keys), held across placement churn), sweep/cleanup paths, glibc arena retention under this allocation pattern.
2. **Bound the responsible working set(s)** so a node over budget stops growing: cap/chunk the allocator found in (1), and/or shed or defer the owning activity under RAM pressure (the AE-heal-apply deferral pattern from 0134 extends naturally). Hardening regardless of attribution: `Engine::compact_plan_inner` with `PlanKind::Pressure`/`LoneGc` sets `start = 0` (merge ALL runs), bypassing `merge_input_cap_bytes` — that path did not fire in this run but has the same unbounded shape one tier worse; cap it (repeated bounded merges still make progress).
3. **Make the budget check swap-aware.** Add statm/status `VmSwap` to the resident measurement (or, on cgroup v2, use `memory.current` + `memory.swap.current` — `memory.current` alone does NOT include swap), so swapped-out anon pages cannot fake headroom.

Acceptance criteria:
- Allocation attribution for both observed growth modes is documented (measurement, not inference from counters alone).
- The responsible working set(s) are bounded or shed under RAM pressure, with unit coverage; pressure/lone-GC plan inputs are capped.
- Budget check accounts for swapped-out anonymous memory, with unit coverage.
- Task 0152's replay re-run (the reproduction criterion, 0152 depends on this task) shows: zero OOM kills, RSS plateau at/near the budget, maintenance counters still advancing, reads served throughout, and restart-free recovery after ingest stops.

## Plan

Scope for this session: the three code-side work items (attribution instrumentation + local measurement, bounding/shedding, swap-aware budget). The hardware reproduction criterion is task 0152's re-run, which depends on this task.

Code reading has identified the two concrete unbounded working sets matching the run's growth modes; the implementation both MEASURES them locally (repro, not inference) and instruments them for hardware attribution on the 0152 re-run:

A. Attribution
A1. New gauges (with Prometheus HELP docs in vals-client observe.rs):
   - merge in-flight gauge(s): count of in-flight off-core merges and sum of their input bytes (set when a Compact job is dispatched in maint.rs/node.rs, cleared on commit/abort). Fills the "no in-flight-merge gauge" observability hole from the 0152 run.
   - AE cooperative-seed resident estimate gauge: bytes staged in SeedProgress (winners key bytes + per-slot overhead + winner_slots + staging/stale_staging groups), updated in seed_step; zeroed on reset.
   - process_swap_bytes gauge (see C).
A2. Local measurement (ignored measurement tests, pattern of replica.rs cooperative_seed_step_budget_measurement):
   - Merge working set: file-device engine, tombstone-heavy corpus with gc_collect_before, run compact_plan_pressure→compact_and_write at 2–3 input scales; print peak RSS delta plus per-component sizes (BlockedBloom for n_upper input entries allocated up-front, RunStreamBuilder index, MergeStream dropped_tombstones Vec, read_run_parts output). Establish which terms scale O(input entries).
   - Seed working set: cooperative seed over N keys at 2 scales; print SeedProgress footprint vs N; then bump placement_generation repeatedly mid-projection to demonstrate the grinding mode (restart_projection keeps O(keys) winners + moves staging to stale_staging, held indefinitely under split churn — matches node 3's creep).
   Document measured scaling + attribution in docs/backpressure.md ("Replica RAM budget" section) — which allocations own the burst (merge-all working set: bloom + dropped tombstones + index, all O(input)) and the creep (seed staged winners O(engine keys) held across placement churn).

B. Bounding/shedding
B1. Cap Pressure/LoneGc plan inputs (engine.rs compact_plan_inner): instead of start=0 (merge ALL), pick the deepest suffix start with cumulative input bytes <= merge_input_cap_bytes, always including at least the newest run. Pressure proceeds capped (non-bottommost already nulls gc_collect_before; victim selection still applies to the capped inputs; repeated bounded merges make progress). LoneGc whose capped suffix does not reach run 0 returns None (a non-bottom lone-GC merge cannot collect). forget_plan: implementer must inspect forget semantics — if a capped suffix could silently miss forget keys, leave forget_plan uncapped (forget corpora are small) and note the decision. Unit tests: pure suffix-selection tests + engine test that an over-cap pressure plan merges only a suffix and repeated plans converge.
B2. Shed the seed under RAM pressure: in seed_step, when ram_over_budget() and an in-flight seed is not yet published, drop SeedProgress (freeing the O(keys) staged state) and return not-done so seeding retries when headroom returns; count these sheds (new counter, e.g. ae_seed_ram_sheds). Post-publish drain phase (which frees memory) keeps running. Unit test with ram::override_for_test: over budget → staged progress dropped and memory-freeing path taken; back under budget → seed completes.

C. Swap-aware budget check
   - New vals-client observe fn process_swap_bytes(): parse VmSwap from /proc/self/status (kB → bytes; 0 when absent). ram.rs budget check uses rss + swap; process_resident_bytes gauge keeps RSS semantics, new process_swap_bytes gauge exported. Unit tests for the parser and for over_budget with swap included. Update docs/backpressure.md.

Conventions: no explanatory comments (only SAFETY / Prometheus HELP / doctests); hand-format new regions, never cargo fmt. Verify with cargo build and targeted cargo test -p vals-store -p vals-cluster filters.

### Starting points
- crates/vals-cluster/src/ram.rs — budget check (check(), sampled_rss, override_for_test)
- crates/vals-client/src/observe.rs:565 process_resident_bytes() (statm); gauges struct ~line 329; Prometheus HELP text near line 388
- crates/vals-store/src/engine.rs:2339 compact_plan_inner (PlanKind::LoneGc/Pressure set start=0, bypassing merge_input_cap_bytes); select_merge_suffix/select_recurring_merge ~552-598 are the pure selection helpers to mirror
- crates/vals-store/src/engine.rs:288 compact_and_write — working set: BlockedBloom::with_capacity(n_upper) in RunStreamBuilder::new (run.rs:1036), MergeStream.dropped (engine.rs:707, into_dropped:770), read_run_parts (run.rs:1406)
- crates/vals-cluster/src/replica.rs:469 seed_step; SeedProgress struct ~105 (winners, winner_slots, staging, stale_staging); restart_projection:146 keeps winners across placement churn; reset_seed_progress:463; ignored measurement-test pattern at ~4474
- crates/vals-cluster/src/maint.rs:86,104 off-core compact_and_write dispatch; node.rs:1692-1696 plan-kind selection; node.rs:1525,1557 compact_commit — hook merge in-flight gauges here
- crates/vals-cluster/src/antientropy.rs:347 existing ram_over_budget shed pattern (returns Busy)
- docs/backpressure.md section 'Replica RAM budget' (line ~126) — extend with attribution + swap + caps
- memory.md conventions: no explanatory comments; tree not rustfmt-clean — never cargo fmt

## Work log
- 2026-08-07 plan: Scope for this session: the three code-side work items (attribution instrumentation + local measurement, bounding/shedding, swap-aware budget). The hardware reproduction criterion is task 0152's re-ru
…[truncated]
- 2026-08-07 context hints: 9 recorded with plan
- 2026-08-07 context hints: crates/vals-cluster/src/ram.rs — budget check (check(), sampled_rss, override_for_test); crates/vals-client/src/observe.rs:565 process_resident_bytes() (statm); Gauges struct ~line 329; Prometheus H
…[truncated]
- 2026-08-07 preload: 5 file(s), ~36 KiB seeded into implementer context
- 2026-08-07 implementer report: Implemented Task 0153’s code-side RAM hardening and attribution work.  Changes: - Made RAM admission swap-aware: `/proc/self/status` `VmSwap` is parsed and sampled with RSS; budget/headroom now use 
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): revise — The swap-aware accounting, AE seed shedding, and attribution metrics are generally well integrated, but the pressure/lone-GC cap is not complete and its suffix strategy can stop making compaction prog
…[truncated]
- 2026-08-07 revision: Addressed both pressure/lone-GC cap review findings.  Changes: - Removed the legacy safety bypass. `PlanKind::Pressure` and `PlanKind::LoneGc` now always use `select_capped_suffix`; only legacy recurr
…[truncated]
- 2026-08-07 review (sol): accept — The revision addresses both prior major findings. Pressure and lone-GC plans now honor the input cap even under legacy policy, while uncapped behavior is limited to the explicitly retained legacy recu
…[truncated]
- 2026-08-07 decision: accept — commit: 0153: bound non-intake RAM growth — swap-aware budget (RSS+VmSwap, process_swap_bytes), pressure/lone-GC merge inputs capped by VALS_MERGE_INPUT_CAP_BYTES (lone-GC defers unless cap reaches bottommo
…[truncated]
- 2026-08-07 usage: 18,152,752 tok (in 1,485,578, out 69,158, cache_r 18,105,272, cache_w 203,124) · cost n/a (unpriced)
  implementer: 16,728,948 tok (in 1,112,043, out 41,353, cache_r 15,575,552, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,406,853 tok (in 373,497, out 10,892, cache_r 1,022,464, cache_w 0) · cost n/a (unpriced)
  coordinator: 16,951 tok (in 38, out 16,913, cache_r 1,507,256, cache_w 203,124) · cost n/a (unpriced)
