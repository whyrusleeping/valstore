---
id: "0149"
title: 'PD guardrail: refuse a canonical split of a range with known live entries'
status: done
priority: 3
created: "2026-08-07"
updated: "2026-09-08"
depends_on: []
spec_refs: []
---

## Description
Add a best-effort PD guardrail against explicitly canonical-splitting a populated range. Both legacy and V4/opaque canonical operations directly change the new upper range's placement and mark it debt-free; automatic debt rehome does not necessarily rescue misplaced existing keys. Opaque placement tokens need not equal key boundaries.

Retain the latest applicable parent-range estimates from range_stats_poll and refuse a canonical split when that parent has a known non-zero live-entry estimate, or explicitly downgrade to inherited placement with a warning. Lookup must use the parent containing the requested boundary, not the not-yet-existing child boundary. Define epoch/freshness invalidation and recheck at queued-command execution.

Absent/stale stats remain permissive for explicitly requested young-zone setup. This is an operator footgun guard, not proof of emptiness or a replacement for confirmed rehome.

## Acceptance criteria
- Cover legacy and opaque-token canonical operations; known populated parents are refused or downgraded with a warning naming zone/range.
- Unknown/stale estimates and inherited splits preserve the explicitly documented behavior.
- Tests cover populated, empty, unknown/stale, topology/epoch changes, queued-command recheck, inherited, and opaque-token cases.
- Do not weaken confirmed-rehome fencing or claim that canonical children automatically acquire rehome debt.

## Plan

Retain bounded-lived, epoch/topology-scoped range entry estimates from PD range_stats_poll. At canonical split execution (including queued operations), resolve the existing parent by requested key boundary and refuse known-positive applicable estimates with a zone/range diagnostic, for both legacy and opaque targets. Keep unknown/stale estimates permissive and inherited splits unchanged; do not alter confirmed-rehome fencing. Add targeted regression tests for populated/empty/unknown/stale, parent lookup, epoch/topology invalidation and queued execution, plus document the best-effort limitation. Run focused debug tests and obtain an independent review.

### Starting points
- crates/vals-cluster/src/pd/range_policy.rs::range_stats_poll
- crates/vals-cluster/src/pd/daemon.rs::apply_range_op and both daemon command loops
- crates/vals-cluster/src/pd/{state,driver,tests}.rs
- docs/zones.md canonical split guidance

## Outcome

Implemented and independently accepted: current parent live-entry estimates guard legacy and opaque canonical splits at queued execution, with epoch/topology invalidation and a three-poll freshness window. Unknown/stale estimates remain permissive; inherited splits and confirmed-rehome fencing are unchanged. Documented in docs/zones.md. Validation: cargo check -p vals-cluster, 76 debug PD tests, and diff checks passed. Scoped commit preserves preexisting unaccepted task 0278/0279 changes.

## Work log
- Owner approved promotion with refreshed legacy/V4 scope; original token/key explanation and automatic-rescue implication were stale.
- 2026-09-08 implementer report: Implemented Task 0149’s PD canonical-split guardrail.  - `range_stats_poll` now returns current-epoch sorted-range live-entry estimates; the pusher retains them in shared state without adding them t
…[truncated]
