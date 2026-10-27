---
id: "0118"
title: 'Load-based (hot-range) splits: per-range ops/s telemetry + load trigger'
status: done
priority: 4
created: "2026-08-01"
updated: "2026-08-11"
depends_on: []
spec_refs: []
---

## Description
Split off from task 0111 (the load-based half of 0108's direction #3). Split a *hot* range — high read/write rate concentrated on one range even when it isn't large — so ordered-scan/point load spreads across replica sets.

Requires:
- **Per-range load telemetry (ops/s).** Track read+write op counts per range in the node serving path and expose a load estimate (ops/s over a recent window) on the `RangeStats` reply — a new field on `wire::OwnedRangeStat` (wire-format change → version/compat handling, matching how `entries`/`midpoint` are carried today). The estimate must be cheap (no device I/O on the serving core).
- **Load trigger in the PD poll.** Alongside the entry-count split trigger in `range_stats_poll`, add a load threshold (`VALS_PD_RANGE_LOAD_SPLIT_OPS` or similar, 0 = off) that splits a range whose ops/s exceeds it at the reported midpoint, even when its entry count is below the size threshold.

## Acceptance
A range that is hot but not large is split to distribute load. Remains anti-thrash-safe (respects the same one-op-per-zone-per-poll discipline and hysteresis as size-split + the 0111 merge policy — a range that just split for load must not immediately merge back), and interacts correctly with scope-bounded zones + weight-aware placement (task 0071). Wire change stays backward-compatible with older peers/clients (or bumps the version cleanly).

## Acceptance criteria

## Plan

1. Extend range-load accounting in the data-node serving path with in-memory per-range operation counters and recent-interval ops/s sampling. Count point reads/writes and the constituent operations of batches (and range scans at their routed start) without device I/O, keep accounting aligned with the current sorted range map, and prune stale range keys after map changes.
2. Add a versioned RangeStats wire exchange that carries `ops_per_sec` on `OwnedRangeStat` while retaining the existing request/response encoding for older peers. Make the PD use the new exchange and keep legacy clients able to request/decode the old stats shape.
3. Add `VALS_PD_RANGE_LOAD_SPLIT_OPS` (`0` disables) to PD state/config and polling. Aggregate replica reports conservatively, trigger at a valid reported midpoint under the existing one-action-per-zone-per-poll discipline, and extend merge hysteresis so a load-split pair cannot merge while its combined load remains at or above half the configured load split threshold.
4. Add focused wire, server-accounting, PD-trigger/hysteresis, and integration coverage proving a hot-but-small range splits while legacy RangeStats remains compatible. Update operator/protocol documentation for the new setting and wire version.
5. Run the relevant vals-client wire tests and vals-cluster unit/integration suites in debug mode, then address any regressions.

### Starting points
- crates/vals-client/src/wire.rs: RangeStats request/response encoding and OwnedRangeStat
- crates/vals-cluster/src/net/server.rs: ServerShared serving paths and range_stats_reply
- crates/vals-cluster/src/pd.rs: PushState, range_stats_poll, split/merge hysteresis, env config
- crates/vals-cluster/src/net/tests.rs: range stats and PD integration tests
- docs/zones.md and docs/formats.md

## Work log
- 2026-08-11 plan: 1. Extend range-load accounting in the data-node serving path with in-memory per-range operation counters and recent-interval ops/s sampling. Count point reads/writes and the constituent operations of
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: Preserve legacy RangeStats by assigning new request/response tags for the ops/s-capable exchange rather than appending into the unframed per-stat list shape.; Count logical operations (batch writes in
…[truncated]
- 2026-08-11 preload: 6 file(s), ~60 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented Task 0118.  Changes: - Added cheap in-memory per-sorted-range operation accounting on data nodes. Counts accepted point reads/digests/writes, each batch write, and scans at their routed st
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change correctly adds cheap in-memory per-range operation accounting, a backward-compatible V2 RangeStats exchange, conservative PD-side load aggregation and splitting, and load-aware merge hyster
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The change correctly adds cheap per-range serving-path counters, backward-compatible V2 stats encoding with legacy fallback, conservative PD load aggregation and triggering, and load-aware merge hyste
…[truncated]
- 2026-08-11 revision: Added deterministic coverage for the actual `range_stats_poll` adjacent-range merge path.  - The new test serves controlled `RangeStatsV2` replies for a sorted two-range map through a local protocol-c
…[truncated]
- 2026-08-11 review (sol): accept — The revision preserves the previously correct implementation and adds deterministic end-to-end unit coverage of the actual `range_stats_poll` merge decision: an adjacent pair at exactly half the confi
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision addresses the prior coverage concern with a deterministic poll-level test that drives actual RangeStatsV2 responses through range_stats_poll, proves a pair at exactly half the load thresh
…[truncated]
- 2026-08-11 decision: accept — commit: cluster: split hot ranges by load
- 2026-08-11 usage: 16,213,161 tok (in 2,640,348, out 50,637, cache_r 13,522,176, cache_w 0) · cost n/a (unpriced)
  implementer: 12,657,258 tok (in 1,274,994, out 30,712, cache_r 11,351,552, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,360,398 tok (in 488,853, out 7,801, cache_r 863,744, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,256,574 tok (in 450,297, out 5,765, cache_r 800,512, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 938,931 tok (in 426,204, out 6,359, cache_r 506,368, cache_w 0) · cost n/a (unpriced)
