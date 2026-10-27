---
id: "0119"
title: PD per-node load-skew gauge (pd_node_load_skew_pct)
status: done
priority: 4
created: "2026-08-01"
updated: "2026-08-11"
depends_on:
    - "0113"
spec_refs: []
---

## Description
Follow-up from 0113 (small-cluster load skew). Add a PD-side observability surface for per-node load imbalance, so operators can see the hottest node's over-share directly rather than inferring it from per-node RAM/segment metrics.

## Context
0113 established that pure consistent hashing has a fixed per-node owned-entries skew (hottest/coldest ~1.16 mean, ~1.43 worst at vnodes=64, N=3, RF=2), and that this is distinct from `pd_range_skew_pct` (which measures split churn within a zone, not node load). There is currently no per-node load metric.

The PD already aggregates per-(zone, range) entry estimates from node reports (see `crates/vals-cluster/src/pd.rs` ~L940 publish path — the `agg: HashMap<(ZoneId, Vec<u8>), (entries, ...)>` and `RangeMap`). It also holds the authoritative ring. So it can map each range token through its replica set (via the ring, honoring per-zone rf/scope and the zone-0 unsalted path) and sum entries per node → per-node owned entries.

## Acceptance
- [ ] A `pd_node_load_skew_pct` gauge (max/mean of per-node owned entries ×100) published on the PD's metrics endpoint, computed across all zones (hash + sorted, including zone 0).
- [ ] Correctly attributes each range's entries to ALL rf replicas (not just primary), using the same placement function nodes use.
- [ ] Cheap enough to run on the existing publish cadence (no per-key work; per-range × rf only).
- [ ] Test covering a skewed vs balanced synthetic membership.
- [ ] Optional: a throttled warn log above an operator-set skew threshold.

## Acceptance criteria

## Plan

1. Extend the shared metrics registry/export list with `pd_node_load_skew_pct` and clear HELP text defining max/mean ×100 and the empty/no-load behavior.
2. Extend the existing PD range-stat publish path to compute owned-entry totals for every authoritative member on each publish cadence. For sorted zones, walk each committed `RangeMap` range once, obtain its aggregated entry estimate, and add it to every node returned by the exact production placement path (`RangeMap::replicas_of_range` / `Ring::replicas_in_zone`) with the zone's rf and scope. Preserve the default zone's legacy unsalted placement semantics.
3. Include hash/unsorted zones, including zone 0, in the load total. Since they have no stable range token from which aggregate counts can be reassigned, surface their local per-node live-entry totals through the existing range-stats poll without changing the wire shape, combine shard reports without double-counting aliases, and add those already-replicated local totals directly; keep existing sorted-range aggregation and automation semantics unchanged.
4. Calculate max/mean ×100 over all current ring members with saturating/wide arithmetic where appropriate, publish 0 for an empty membership or zero total, and keep complexity to reports plus range × rf (no key scans).
5. Add focused tests for balanced and skewed synthetic memberships/placements, all-rf attribution, zone rf/scope, zone-0 unsalted/hash inclusion, and metric export visibility. Run targeted cluster/client tests and the relevant crate test suites; do not run repository-wide rustfmt.

### Starting points
- crates/vals-cluster/src/pd.rs:703 range_stats_poll and 1049 publish_range_metrics
- crates/vals-cluster/src/net/server.rs:3577 range_stats_reply currently skips unsorted zones
- crates/vals-client/src/observe.rs Gauges and gauges_named
- crates/vals-client/src/coord/mod.rs:1036 shows zone 0 uses Ring::replicas (unsalted), unlike named zones
- crates/vals-client/src/partition.rs RangeMap::replicas_of_range

## Work log
- 2026-08-11 plan: 1. Extend the shared metrics registry/export list with `pd_node_load_skew_pct` and clear HELP text defining max/mean ×100 and the empty/no-load behavior. 2. Extend the existing PD range-stat publish
…[truncated]
- 2026-08-11 context hints: 5 recorded with plan
- 2026-08-11 context hints: crates/vals-cluster/src/pd.rs:703 range_stats_poll and 1049 publish_range_metrics; crates/vals-cluster/src/net/server.rs:3577 range_stats_reply currently skips unsorted zones; crates/vals-client/src/o
…[truncated]
- 2026-08-11 preload: 5 file(s), ~41 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented Task 0119.  Changes: - Added/exported `pd_node_load_skew_pct` with HELP text defining max/mean ×100 and zero behavior. - Made PD range-stat polling run on its normal cadence even when spl
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): accept — The change satisfies Task 0119: it exports the new gauge with clear semantics, polls and reports local hash-zone totals (including zone 0), attributes sorted-range estimates to every production replic
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The change cleanly implements the PD node-load-skew gauge and its export, attributes sorted-range estimates through the production replica-placement paths (including all replicas and scoped RF), incor
…[truncated]
- 2026-08-11 decision: accept — commit: cluster: expose PD node load skew
- 2026-08-11 usage: 4,892,291 tok (in 1,545,702, out 30,109, cache_r 3,316,480, cache_w 0) · cost n/a (unpriced)
  implementer: 1,976,742 tok (in 490,285, out 13,689, cache_r 1,472,768, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,154,569 tok (in 399,616, out 5,385, cache_r 749,568, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,005,631 tok (in 312,205, out 5,810, cache_r 687,616, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 755,349 tok (in 343,596, out 5,225, cache_r 406,528, cache_w 0) · cost n/a (unpriced)
