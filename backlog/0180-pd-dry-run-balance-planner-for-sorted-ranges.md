---
id: "0180"
title: PD dry-run capacity-weighted balance planner for sorted ranges
status: done
priority: 4
created: "2026-08-11"
updated: "2026-08-11"
depends_on:
    - "0179"
spec_refs:
    - docs/algorithms.md#planned-assignment-algorithm
---

## Description

Implement the read-only planning and observability phase of task 0120. Using the same range-stat aggregation and all-replica load projection as `pd_node_load_skew_pct`, deterministically choose candidate sorted-range moves that minimize capacity-normalized hottest-engine and hottest-node load. Discover a fixed-size opaque token for each feasible node set and shard placement using a bounded deterministic seed search, but do not prepare or commit a re-home.

## Acceptance criteria

- The planner accounts for fixed hash/default-zone load, every sorted range replica and routed shard, per-zone RF/scope, advertised node capacity weights, reported shard counts, current-owner overlap, and an indivisible range's estimated entry weight.
- Candidate and target selection follows the documented deterministic objective and tie-breaks. The bounded seed search evaluates both the zone-salted replica set and `hash64(token) % shard_count`, so balancing nodes cannot accidentally concentrate an engine. Focused tests cover N=3/4/5 RF=2, unequal capacities, unequal shard counts, scopes, uneven/oversized ranges, infeasible target sets/shards, stale/missing estimates, and repeatability across map iteration and PD restart.
- Configurable high/low bounded-load thresholds and a minimum-improvement threshold prevent estimate noise from producing proposals; no-feasible-plan and split-first cases are attributed.
- A bounded, domain-separated token search tests candidates through the production zone-salted ring walk. Exhaustion skips a target safely and is observable; it never weakens RF, distinct-node, or scope constraints.
- Dry-run metrics expose projected before/after hottest-node/mean and hottest-engine/target, proposed range/copy entries, movable and blocked ranges, and reason-attributed no-op counts. Logs identify the deterministic proposed range and target without exposing key data.
- The planner runs only when the opaque-token capability gate is satisfied and remains read-only. It sends no `PrepareRehome`, changes no range-map epoch, and adds zero work to client/write paths.
- Documentation records configuration and metric semantics and compares dry-run predictions with observed `pd_node_load_skew_pct` on a representative workload.

## Plan

Implement the read-only sorted-range balance planner as a poll-time PD subsystem, preserving current routing and re-home behavior:

1. Add deterministic planner inputs/configuration to PD state and the range-stats poll. Derive authoritative node capacities from current addressed members, sorted-range estimates from the same fresh aggregation used by `pd_node_load_skew_pct`, fixed default/unsorted-zone engine loads from local shard reports, and per-node shard counts only from consistent fresh reports. Require the existing operator opaque-map gate plus fresh all-endpoint opaque capability acknowledgement before planning; treat missing/stale/inconsistent estimates, active re-homes, unsupported maps/capability, insufficient RF/scope, and absent shard topology as attributed no-ops rather than guessing.
2. Implement a pure planner (prefer a focused module or isolated helpers) with integer/rational comparison so results do not depend on floating point or map iteration. Build node and `(node, shard)` loads including every current sorted replica plus fixed hash load; compute capacity-weighted node targets and equally divided per-engine targets. Enumerate eligible ranges and every RF-distinct in-scope target node set, project owner and shard changes for each indivisible range, and rank plans exactly by the documented engine maximum, node maximum, squared normalized deviation, current-owner overlap/copy cost, node/shard IDs, then range improvement/weight/zone/lower bytes. Reject no-op placements and candidates below configurable high/low hysteresis or minimum-improvement thresholds; distinguish bounded no-plan, split-first/indivisible, and blocked-input cases.
3. Implement bounded, domain/version-separated opaque seed discovery from the committed snapshot identity (planner version, member/ring epoch, zone ID, and range lower bound). Search nonces in increasing order, construct the shipped fixed-size V1 opaque token, and evaluate each token through production `Ring::replicas_in_zone` plus production `hash64(token) % reported_shard_count`. Keep RF/distinct-node/scope constraints exact, choose the best realizable engine placement under the same deterministic objective with nonce last, skip exhausted target sets safely, and expose exhaustion. Do not install the token or send any PD command.
4. Integrate one global dry-run proposal per fresh poll after normal stats collection, with no `PrepareRehome`, map mutation, epoch change, or client/write-path work. Log only non-key identifiers (zone/range ordinal or stable digest, target nodes/shards, projected ratios/copy estimate), never raw boundaries/key data. Keep automatic canonical re-home behavior unchanged for this phase.
5. Add Prometheus observability for projected before/after hottest-node-to-target and hottest-engine-to-target ratios, proposed range/copy entry volume, movable and blocked ranges, proposal presence, token-search exhaustion, and cumulative reason-attributed no-op outcomes. Define clear zero/reset and ratio scaling semantics, and ensure the existing `pd_node_load_skew_pct` is computed from the same load projection rather than drifting to a duplicate model.
6. Add focused deterministic tests covering N=3/4/5 and RF=2, unequal capacity weights, unequal shard counts, zone scopes, fixed hash load, current-owner overlap, uneven and oversized indivisible ranges, infeasible target sets/shards, bounded seed exhaustion, missing/stale/inconsistent estimates, high/low/min-improvement suppression, capability gating, repeatability under reordered maps/reports and simulated PD restart, and a hard assertion that dry-run emits no command or range-map mutation. Include a representative deterministic workload comparison where planner "before" node skew agrees with `pd_node_load_skew_pct` and document the observed/predicted values.
7. Update `docs/algorithms.md` and `docs/zones.md` (and configuration/metrics docs if separate) with shipped dry-run-only behavior, exact environment variables/defaults, thresholds and ratio units, no-op reasons, capability prerequisites, privacy-safe logs, and the representative prediction-vs-observed comparison. Run targeted vals-client/vals-cluster tests plus appropriate workspace checks; do not run cargo fmt because the existing tree is not rustfmt-clean, and preserve unrelated working-tree changes.

### Starting points
- crates/vals-cluster/src/pd.rs: range_stats_poll, node_owned_entries, publish_range_metrics, PushState and opaque feature preflight
- crates/vals-client/src/observe.rs: PD gauges/counters and Prometheus HELP text
- crates/vals-client/src/ring.rs: production replicas_in_zone ring walk and hash64
- crates/vals-client/src/partition.rs: opaque_placement_token and fixed V1 seed/token format
- docs/algorithms.md#Planned assignment algorithm
- docs/zones.md: current PD range configuration/metric documentation

## Work log
- 2026-08-11 plan: Implement the read-only sorted-range balance planner as a poll-time PD subsystem, preserving current routing and re-home behavior:  1. Add deterministic planner inputs/configuration to PD state and th
…[truncated]
- 2026-08-11 context hints: 6 recorded with plan
- 2026-08-11 context hints: Existing task-file modifications in backlog/0179 and backlog/0180 are workflow state; preserve them.; Use member_epoch as the committed ring/membership identity if there is no separate ring epoch in P
…[truncated]
- 2026-08-11 preload: 9 file(s), ~64 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0180’s read-only PD balance planner.  Changes: - Added poll-time, capacity-weighted sorted-range planning using the same all-replica/fixed-hash load projection as `pd_node_load_skew
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): revise — The planner is read-only, uses production routing/token primitives, exports the requested observability, and the current vals-cluster suite passes (509 passed, 3 ignored). However, it can plan from in
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The planner is read-only and has a solid deterministic skeleton, shared load projection, capability gating, opaque-token search, metrics, and documentation. However, it can currently emit proposals fr
…[truncated]
- 2026-08-11 revision: Addressed all reviewer findings for task 0180.  Revisions: - Made planner polls fail closed unless every expected endpoint returns successful `RangeStatsV2` at exactly the committed range-map epoch. F
…[truncated]
- 2026-08-11 review (sol): revise — The revision addresses the prior partial-poll, objective-ordering, low-water, and focused-test concerns, and the full vals-cluster test binary passes (514 passed, 3 ignored). One production integratio
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The revision substantially improves the implementation: planner inputs now fail closed on missing shards/ranges and mixed epochs, the token search avoids repeated ring walks, exact-ratio threshold hel
…[truncated]
- 2026-08-11 revision: Completed the remaining production integration corrections.  Changes: - Made V2 completeness validation scope-aware per member: every endpoint/shard and default marker remain mandatory, while scoped-z
…[truncated]
- 2026-08-11 review (sol): accept — The revision makes endpoint expectations scope-aware while preserving complete fresh endpoint/shard validation and fail-closed aggregation checks. The planner now handles out-of-scope nodes correctly,
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision addresses the remaining correctness issues. The planner now validates its complete V2 endpoint snapshot against the same authoritative max aggregation used by `pd_node_load_skew_pct`, req
…[truncated]
- 2026-08-11 decision: accept — commit: cluster: add dry-run range balance planner
- 2026-08-11 usage: 39,210,723 tok (in 6,160,274, out 123,473, cache_r 32,926,976, cache_w 0) · cost n/a (unpriced)
  implementer: 31,145,064 tok (in 2,606,217, out 77,023, cache_r 28,461,824, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 3,525,059 tok (in 1,356,654, out 17,493, cache_r 2,150,912, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,452,465 tok (in 1,288,136, out 8,745, cache_r 1,155,584, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 2,088,135 tok (in 909,267, out 20,212, cache_r 1,158,656, cache_w 0) · cost n/a (unpriced)
