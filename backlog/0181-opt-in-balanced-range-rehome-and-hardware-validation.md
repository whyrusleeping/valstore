---
id: "0181"
title: Opt-in balanced sorted-range re-home executor
status: done
priority: 4
created: "2026-08-11"
updated: "2026-08-11"
depends_on:
    - "0176"
    - "0179"
    - "0180"
spec_refs:
    - docs/algorithms.md#compatibility-and-rollout
    - docs/zones.md
---

## Description

Execute task 0120's proven dry-run plans through confirmed re-home, opt-in and one move at a time, and verify the executor locally. Populated auto-splits remain metadata-only: the executor must split/observe/re-home as separate operations and must never change a populated child's placement as part of split. Follow-on task 0182 owns the destructive hardware A/B, benchmark findings, and any later default-on reconsideration.

## Acceptance criteria

- A default-off PD feature gate may prepare only the dry-run planner's deterministic proposal, with at most one active move globally and one per zone; it waits for commit/abort plus a fresh statistics poll before replanning.
- Membership, weight, RF, scope, capability, ring epoch, or relevant range-map changes invalidate a proposal before prepare. Split/merge and balance re-home cannot action the same zone in one poll.
- Prepare, mirror, copy, cutover, fencing, abort, resume, and cleanup reuse the confirmed re-home protocol without weakening ack-implies-mirrored. Abort leaves the old token authoritative and is reason-attributed; no separate assignment database is introduced.
- Tests cover deterministic execution, stale-plan rejection, split-first handling of an oversized range, pre-visibility assignment of new-zone presplits, failure/rollback, PD restart, node churn, and convergence without oscillation under high/low hysteresis.
- The executor remains default-off, and canonical docs explicitly defer the destructive hardware A/B, benchmark findings, and any default-on reconsideration to task 0182.
- Automatic balancing remains default-off unless opaque-token compatibility, prediction accuracy, task 0176's re-home safety gate, and hardware churn/correctness gates all pass. Hash/default zones and `vnodes` behavior remain unchanged.
- Canonical docs record the locally verified executor semantics and the explicit default-off decision.

## Plan

Ship the opt-in executor as a narrow mutation layer over task 0180's deterministic planner, then prove it locally and on hardware without changing request-time routing or the confirmed re-home protocol.

1. Add a separate default-off balance-execution gate (independent of the opaque-capability and ordinary canonical re-home gates). Preserve the planner's exact chosen opaque token and immutable proposal identity. When enabled, allow only one global active balance move and one per zone, and require the prior move to reach commit/abort and then one complete fresh stats poll before replanning/preparing.
2. Make prepare fail closed against proposal staleness at both poll selection and authoritative command application. Bind the proposal to membership/weights and member/ring epoch, catalog/zone RF/scope and zone epoch, opaque capability readiness, range-map epoch plus the selected range's bounds/current token/debt/pending state, and fresh endpoint topology/statistics. Carry the planner-selected token into a dedicated prepare command/API; never substitute the canonical lower bound. Reject and attribute any mismatch, node churn, capability loss, structural map change, or no-longer-current proposal without mutating the map.
3. Integrate action arbitration so split/merge and balance execution cannot act on the same zone in one poll. Oversized indivisible ranges remain split-first: issue only the inherited metadata split, wait for map visibility and fresh child estimates, then plan a later child re-home. Preserve pre-visibility balanced assignment only for transactional presplits on a newly created, not-yet-writable zone, with no copy/re-home; do not change placement during any populated split.
4. Reuse the existing pending-token push, mirror, copy, cutover, fencing, abort, restart/resume, and cleanup flow unchanged after prepare. Keep the old token authoritative on abort, retain explicit placement debt as specified, and add reason-attributed execution metrics/logging for prepares, stale rejections, commit/abort outcomes, cooldown/fresh-poll waits, copy volume, and abort causes without exposing key material or adding a separate assignment database.
5. Add deterministic focused tests for exact token execution, one-global/one-zone serialization, fresh-poll cooldown and convergence/no oscillation under high/low hysteresis, stale proposal rejection for every relevant snapshot input, split-first sequencing, new-zone presplit assignment, failure/rollback and old-token authority, PD restart/resume, node churn/capability loss, and unchanged hash/default zones and vnodes. Re-run existing confirmed re-home, fencing, mirror, split/merge, planner, codec, and cluster suites in debug mode.
6. Execute a reproducible lab hardware A/B on an uneven RF=2 sorted workload using a fresh rebuild/deploy. Keep control and opt-in arms otherwise identical and record source/config/commands plus `pd_node_load_skew_pct`, planner prediction versus actual per-node/per-engine owned entries, capacity headroom, write throughput and tails, copy volume, attributed aborts, and exact consistency across a node outage and PD restart. Use repeated samples/passes where device variance matters and compare distributions rather than a single pass.
7. Check in a self-contained `bench/lab-balance-*/FINDINGS.md` (and configs/scripts/artifact index as appropriate), then update `docs/algorithms.md`, `docs/zones.md`, metrics/config docs, and `ARCHITECTURE.md` with shipped semantics and an explicit evidence-based default-on/default-off decision. Keep automatic balancing default-off unless every compatibility, prediction, re-home-safety, churn, correctness, and no-per-write-regression gate passes; leave hash/default zones and vnodes behavior unchanged. Do not run cargo fmt; preserve unrelated workflow changes.

### Starting points
- crates/vals-cluster/src/pd.rs: range_stats_poll, BalancePlan/balance_dry_run, PdCmd/RangeOpKind, Pd::prepare_rehome
- crates/vals-client/src/partition.rs: V4 pending-token/debt transitions and set_pending_opaque
- crates/vals-client/src/observe.rs: PD balance/re-home metrics and Prometheus HELP
- docs/algorithms.md#Planned assignment algorithm and #Compatibility and rollout
- docs/zones.md: dry-run config/metrics and confirmed re-home protocol
- plans/bootes-soak-runbook.md and bench/lab-rehome-20260810/FINDINGS.md: hardware methodology/provenance

## Work log
- 2026-08-11 plan: Ship the opt-in executor as a narrow mutation layer over task 0180's deterministic planner, then prove it locally and on hardware without changing request-time routing or the confirmed re-home protoco
…[truncated]
- 2026-08-11 context hints: 6 recorded with plan
- 2026-08-11 context hints: crates/vals-cluster/src/pd.rs: range_stats_poll, BalancePlan/balance_dry_run, PdCmd/RangeOpKind, Pd::prepare_rehome; crates/vals-client/src/partition.rs: V4 pending-token/debt transitions and set_pend
…[truncated]
- 2026-08-11 preload: 10 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 implementer report: BLOCKED — The implementation, focused tests, metrics, and canonical docs are complete and verified, but task 0181 also requires a destructive fresh hardware A/B and checked-in findings. The lab runbook’s n
…[truncated]
- 2026-08-11 revision: Implemented and staged task 0181 as the default-off local executor; no hardware state was touched.  Changes: - Added independent `VALS_PD_BALANCE_EXECUTE` gate, default off and separate from opaque-ma
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): revise — The default-off gate, exact opaque-token path, action arbitration, atomic presplit map, metrics, and documentation are substantial, but the executor does not yet satisfy the fail-closed/current-propos
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The default-off gate, exact-token prepare path, structural arbitration, and documentation are substantial, but the executor is not yet fail-closed or globally serialized across realistic Raft delays.
…[truncated]
- 2026-08-11 revision: Addressed all reviewer findings and updated the staged diff.  Fail-closed serialization changes: - Added a shared `BalancePollAuthority` guarded by `Arc<Mutex<_>>`. Each eligible planner poll publishe
…[truncated]
- 2026-08-11 review (sol): revise — The revision fixes key-material logging and now waits for an exact reflected prepare while using a replicated global lock/CAS to serialize delayed proposals. However, the production poll-authority log
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The revision fixes the key-material logging and adds explicit commit tracking, conditional locking, and control-mutation gating, but the new generic Raft CAS mechanism is not a safe replicated-state-m
…[truncated]
- 2026-08-11 usage: 106,074,842 tok (in 7,788,514, out 157,688, cache_r 98,128,640, cache_w 0) · cost n/a (unpriced)
  implementer: 98,431,458 tok (in 4,585,943, out 117,771, cache_r 93,727,744, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 3,599,348 tok (in 1,506,983, out 15,181, cache_r 2,077,184, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 3,166,975 tok (in 1,224,699, out 17,156, cache_r 1,925,120, cache_w 0) · cost n/a (unpriced)
  coordinator: 877,061 tok (in 470,889, out 7,580, cache_r 398,592, cache_w 0) · cost n/a (unpriced)
