---
id: "0120"
title: Balance-aware / bounded-load placement for small RF clusters
status: done
priority: 4
created: "2026-08-01"
updated: "2026-08-11"
depends_on:
    - "0113"
spec_refs: []
---

## Description
Speculative follow-up from 0113. 0113 reconciled the vnodes default (64→128, ~30% excess-skew cut) but deliberately deferred any change to the pure-hash placement scheme. This task is to evaluate whether a balance-aware placement is worth its cost, and is only worth pursuing if a real small-cluster deployment hits the hottest-node capacity ceiling.

## Questions to work through
- Evaluate **consistent-hashing-with-bounded-loads** (each node capped at (1+ε)×fair share; overflow keys cascade to the next node) vs an **explicit range→replica-set assignment** that equalizes owned entries. Both trade away pure-hash determinism / minimal-disruption for assignment state + migration machinery.
- Quantify the achievable skew improvement over the vnodes=128 baseline (0113's table) for N∈{3,4,5}, RF=2.
- **Interaction with auto-split (0108/0118):** when a hot range splits, do the children get placed to *relieve* the hot node, or do they inherit near-parent ring positions and keep the skew? Determines whether splitting can be made balance-aware instead of a separate placement redesign.
- Migration/compat: how to roll out without a full keyspace reshuffle; epoch/agreement implications (vnodes/rf are PD-authoritative + epoch-checked today).

## Outcome
A decision doc + (if pursued) a design and phased plan. Depends on 0113's observability follow-up landing first so the win is measurable.

## Decision (2026-08-10, user)

Promoted on the condition of **zero added per-write cost** — treat that as an
acceptance criterion. Placement changes happen at assignment time (PD-side);
the client hot path stays a routing lookup (ring or range map, as today), with
any bounded-load cascade logic memoized into the assignment, never evaluated
per-write. Context since filing: the 0130 ceiling run met this task's trigger
condition literally (n1 at 1.79× by token luck; split children inherit parent
placement; one engine floored while 11/12 pairs had space), and re-home (0155)
— the migration machinery this needs — has landed (hardware validation: 0168).

## Acceptance criteria

## Plan

Produce a docs-only placement decision and phased implementation design, grounded in the now-observed capacity trigger and the zero-added-per-write-cost constraint.

1. Quantify the opportunity. Re-run a deterministic Monte-Carlo using the production `hash64`/ring walk at vnodes=128, RF=2, equal member weights, for N=3,4,5. Report baseline hottest/mean (the capacity metric) and hottest/coldest distributions, then compare with PD-side assignment of a sufficiently split sorted zone's range weights across replica pairs. State the workload assumptions, trial/range counts, granularity lower bound, and avoid presenting equal-range idealization as a guarantee for one oversized range.
2. Compare the two candidate families against the acceptance constraint. Reject request-time consistent-hashing-with-bounded-load cascades because they add client/write-path work and need per-key assignment state. Retain the pure ring for unsorted/default zones. Choose PD-side, assignment-time balancing for sorted ranges, reusing their cached range-map lookup and confirmed re-home protocol so the request hot path remains the same range lookup + ring routing it performs today.
3. Design the selected mechanism. Treat a range placement token as an opaque PD-selected seed (not necessarily its lower bound), generated deterministically until it maps to a desired RF-distinct target set. Define a capacity-weighted objective from PD's per-range entry estimates, bounded-load target/hysteresis, deterministic tie-breaks, one-range/one-zone scheduling, split-child behavior, failure/rollback semantics, membership/epoch agreement, and compatibility gating. Explicitly cover indivisible hot ranges and why split first, then re-home, is necessary.
4. Record a phased rollout. Gate implementation behind the unresolved healthy-node mirror-failure blocker/default-off re-home status; then add opaque-token format/capability support and tests; add dry-run planner metrics; enable opt-in one-at-a-time assignment and hardware A/B validation; only consider default-on after safety and migration gates. Keep vnodes/ring behavior unchanged for hash zones and mixed-version clusters.
5. Update the canonical placement documentation (primarily `docs/algorithms.md`, with focused cross-links/wording in `docs/zones.md` if needed) so the old 'revisit if trigger occurs' decision is replaced by this decision and its scope. If implementation work is clearly separable, create accepted follow-on backlog task(s), depending on task 0176 where re-home robustness gates them.
6. Verify documentation against current RangeMap/re-home/PD code and run lightweight doc hygiene checks (links/search/diff); no production behavior change and therefore no claim that balancing already ships.

### Starting points
- docs/algorithms.md:83-156 — current vnodes=128 skew decision and sorted range-token routing
- docs/zones.md:77-221,234-310 — token semantics, split/re-home protocol, default-off blocker, auto-split scheduling
- crates/vals-client/src/partition.rs:RangeMap / valid_token — tokens currently restricted to prior boundaries but encoded explicitly in V2/V3
- crates/vals-cluster/src/pd.rs:700-995 — RangeStats aggregation, load metric, current canonical re-home candidates
- backlog/0113, backlog/0130, backlog/0168 — original baseline, capacity trigger, hardware/default-off evidence
- Acceptance condition: zero added per-write cost; client routing remains the existing cached lookup, never runtime bounded-load cascade

## Work log
- 2026-08-11 plan: Produce a docs-only placement decision and phased implementation design, grounded in the now-observed capacity trigger and the zero-added-per-write-cost constraint.  1. Quantify the opportunity. Re-ru
…[truncated]
- 2026-08-11 context hints: 6 recorded with plan
- 2026-08-11 context hints: docs/algorithms.md:83-156 — current vnodes=128 skew decision and sorted range-token routing; docs/zones.md:77-221,234-310 — token semantics, split/re-home protocol, default-off blocker, auto-split
…[truncated]
- 2026-08-11 preload: 7 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0120 as a docs-only placement decision.  - Reworked `docs/algorithms.md` with the deterministic vnodes=128/RF=2 Monte Carlo for N=3/4/5, reporting hottest/mean and hottest/coldest dis
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The docs-only change satisfies task 0120: it quantifies vnodes=128 RF=2 skew for N=3/4/5, makes the zero-per-write-cost decision explicit, selects a credible PD-side opaque-token design for sorted ran
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The docs-only change correctly and cleanly satisfies task 0120. It quantifies the vnodes=128 RF=2 baseline and the idealized assignment opportunity for N=3/4/5 with appropriate granularity caveats; re
…[truncated]
- 2026-08-11 revision: Applied the two documentation revisions:  - Updated `docs/zones.md` Scalability and Build order passages to state that confirmed populated-range re-home is built, while its automatic executor remains
…[truncated]
- 2026-08-11 review (sol): accept — The revision fully addresses the prior documentation findings. `docs/zones.md` now accurately distinguishes the built confirmed re-home protocol from its default-off automatic executor and future bala
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision remains correct and improves documentation quality. The Monte Carlo is now fully reproducible: the specified Python version, single seeded RNG stream, `sample`/`randbytes` call order, rin
…[truncated]
- 2026-08-11 decision: accept — commit: docs: design balanced sorted-range placement
- 2026-08-11 usage: 13,731,771 tok (in 2,813,249, out 64,634, cache_r 10,853,888, cache_w 0) · cost n/a (unpriced)
  implementer: 9,424,969 tok (in 1,175,316, out 38,965, cache_r 8,210,688, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,530,867 tok (in 816,198, out 8,173, cache_r 1,706,496, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,233,140 tok (in 478,387, out 9,793, cache_r 744,960, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 542,795 tok (in 343,348, out 7,703, cache_r 191,744, cache_w 0) · cost n/a (unpriced)
