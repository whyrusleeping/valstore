---
id: "0108"
title: Automatic range splitting + safe defaults/guardrails for sorted zones (kill the single-range footgun)
status: done
priority: 3
created: "2026-07-31"
updated: "2026-07-31"
depends_on: []
spec_refs:
    - docs/zones.md#Zones
---

## Description
## Motivation
Sorted zones today require **manual, static** pre-splits (`PlacementDriver::split_zone`); the PD owns only *where* the boundaries are and never adds one for you (`docs/zones.md` "Growth = range splits"). Consequences:
- A fresh sorted zone = **one range**, token `""` → pinned to one RF-scoped replica pair and one intra-node partition (O(1) placement regardless of cluster size). Works in dev, silently concentrates at scale.
- The operator must (a) know to split and (b) split *well* for their real key distribution. Getting (b) wrong is easy — see the 2026-07-31 soak: `records` was split, `backlinks` wasn't, and the single-range backlinks zone filled one 32 GiB region and (via the fatal space path, sibling task) killed two nodes.
- This bites exactly the users who chose vals for its differentiator (ordered range scans); hash/default zones auto-distribute and are immune.

The split *mechanism* is already cheap, raft-replicated, and minimal-disruption. What's missing is a **policy** to drive it — every mature ordered store (Bigtable/HBase, CockroachDB, TiKV, Spanner) does automatic size/load-triggered splitting + rebalance.

## Direction (for discussion — this is a proposal, not accepted scope)
1. **Auto-split policy in the PD.** Size- and/or load-triggered range splits via the existing `split_zone`. New sorted zones start pre-split or split quickly. Needs:
   - per-range telemetry to the PD (bytes/`used_segments`/`live_keys`/entries per range),
   - a split-decision loop with hysteresis/anti-thrash,
   - split-point selection **without a global sort** — a per-range midpoint/median reported by the range's owning node.
2. **Cheap guardrails to ship first** (buy safety before the autopilot exists):
   - `create-zone --presplit K` — seed K evenly-distributed boundaries at creation.
   - Refuse/warn a single-range sorted zone once it crosses N bytes/keys ("this zone is on one replica set; split it").
   - Per-range / per-partition **fill + skew metrics** and a pre-wall alert.
3. **Optional later:** range **merge** for shrink; load-based (not just size-based) splits for hot ranges.

## Open questions / tradeoffs
- How the PD gets per-range size telemetry (extend the members/heartbeat push, or a pull).
- Splitting changes a range's token, which also re-shards it **intra-node** (`hash64(token) % shards`) → data may move partitions on split; quantify that rebalance cost.
- Avoiding split storms and oscillation; interaction with scope-bounded zones and weight-aware placement (task 0071).
- Default posture: do we auto-split *all* sorted zones, or opt-in per zone?

## Acceptance (design-level)
A sorted zone under sustained (even skewed) write load distributes across nodes/partitions **without operator intervention**; the single-range concentration default is gone; and the guardrails make the remaining manual cases loud rather than silent.

## Acceptance criteria

## Plan

Context: the auto-split *policy* already exists in the PD (`auto_split_poll` in `crates/vals-cluster/src/pd.rs`: polls each node's `RangeStats`, splits a range past `auto_split_threshold` at a node-reported midpoint, with epoch/anti-thrash guards). It is opt-in and OFF by default. This task ships the missing guardrails from the task's section 2 and picks a safe default.

Decision on defaults (documented, env-reversible): enable auto-split by DEFAULT with a conservative threshold, and make the loud single-range warning + per-range metrics DEFAULT-ON as the backstop when an operator disables auto-resharding. This satisfies the design acceptance ("distributes without operator intervention; the single-range concentration default is gone"). The in-code "off by default" comment and docs/zones.md are updated to reflect the flip and its rationale; a memory note records that 0108 intentionally reversed the prior default-off posture per user instruction.

1. Presplit at creation (`create-zone ... presplit=<K>`):
   - Add a pure helper `even_boundaries(k: u32) -> Vec<Vec<u8>>` in `crates/vals-client/src/partition.rs` near `RangeMap`: for k>=2 emit k-1 single-byte boundaries at `floor(i*256/k)` for i in 1..k, deduped, strictly increasing, dropping 0x00; k<=1 → empty; cap k at 256 (caller errors above that). Unit tests: count = k-1, strictly increasing, all non-empty, distinct.
   - Plumb boundaries through PD: change `PdCmd::CreateZone(ZoneConfig)` → `CreateZone(ZoneConfig, Vec<Vec<u8>>)`. In BOTH drive-loop `CreateZone` handlers (single-node ~line 1009 and multi-replica ~line 1259), after assigning `id`, push each boundary as `(id, boundary, RangeOpKind::Split)` into `pending_range_ops` (reuses the existing sequential split driver; range-map keys are independent of the catalog entry so ordering vs the zone commit doesn't matter). Keep `PdDaemon::create_zone(cfg)` working (pass empty boundaries) and add `create_zone_presplit(cfg, boundaries)`.
   - Admin CLI (`crates/vals-server/src/admin.rs`): accept an optional trailing `presplit=<K>` token on `create-zone <name> <rf> <sorted|unsorted>`. Only valid for sorted zones with K in 2..=256 (error otherwise, and error if presplit given for unsorted). Compute boundaries via `even_boundaries(K)`; call `create_zone_presplit`. Update the module doc + unknown-command help text.

2. Loud warn + per-range metrics (default-on backstop):
   - Decouple the poll from auto-split: in `pusher_loop`, run the RangeStats poll when EITHER `auto_split_threshold > 0` OR the warn guardrail is enabled (`single_range_warn_entries > 0`), at the existing cadence. Refactor `auto_split_poll` into a poll that (a) always aggregates per-(zone,token) max entries + midpoint, (b) computes and publishes the metrics below, (c) logs the throttled warning, and (d) enqueues splits only when `auto_split_threshold > 0` and entries exceed it (unchanged split logic).
   - Warn: for every SORTED zone whose committed range map is still a single range (`num_ranges()==1`) and whose reported entries exceed `single_range_warn_entries`, log a loud `log::warn!` ("sorted zone N is on a single range (~E entries) → one replica set/partition; presplit or enable auto-split") throttled to at most once per zone per 60s (track last-warn Instant per zone in pusher-loop-local state), and bump a `pd_single_range_warnings` counter.
   - Config: add `single_range_warn_entries` to `PushState` (default `DEFAULT_SINGLE_RANGE_WARN_ENTRIES = 1_000_000`), read from env `VALS_PD_SINGLE_RANGE_WARN_ENTRIES` at spawn (0 disables). Change the auto-split default: `VALS_PD_RANGE_SPLIT_THRESHOLD` now defaults to `DEFAULT_RANGE_SPLIT_THRESHOLD = 1_000_000` instead of 0 (0 still disables). Extend `set_auto_split` or add a setter so tests can set the warn threshold race-free if needed.

3. Metrics (new gauges + counter in `crates/vals-client/src/observe.rs`, set by the PD each poll; rendered by the existing metrics endpoint which PD already supports):
   - Gauges (add to `Gauges` struct, `new()`, and `gauges_named()`): `pd_sorted_ranges` (Σ num_ranges over sorted zones), `pd_single_range_sorted_zones` (count of sorted zones with exactly one range), `pd_max_range_entries` (hottest single range's estimate across sorted zones), `pd_oversized_single_range_zones` (count of single-range sorted zones over the warn threshold — the footgun alert; >0 means act), `pd_range_skew_pct` (for the multi-range zone maximizing max_range/mean_range, that ratio ×100 as an integer; 0 if no multi-range zone). Set them at the end of each poll pass.
   - Counter (add to the `counters!` macro list): `pd_single_range_warnings`.

4. Docs + comment: update the `pusher_loop` "Off by default" comment to describe the new default-on posture and env overrides; update `docs/zones.md` (Scalability / "Growth = range splits") to state auto-split is now on by default with the guardrails (presplit, single-range warning + metrics) and list the env knobs.

5. Tests: unit test `even_boundaries`; a PD test asserting a single-range sorted zone over the warn threshold sets `pd_oversized_single_range_zones`/logs a warning (can extend the existing `pd_auto_splits_a_hot_range` harness or add a sibling test); a test that `create-zone ... presplit=K` yields K ranges in the committed map. Keep changes green: `cargo build` + `cargo test` for vals-cluster, vals-client, vals-server.

Verification: `cargo test -p vals-client -p vals-cluster -p vals-server` (plus `cargo clippy`), and confirm the full suite still passes with auto-split now default-on (no test writes >1M entries, so no unexpected splits).

### Starting points
- crates/vals-cluster/src/pd.rs: auto_split_poll (~747), pusher_loop (~672), PushState + spawn_pusher env read (~626,876), CreateZone handlers (~1009 single, ~1259 multi), pending_range_ops sequential driver (~1046, ~1395)
- crates/vals-server/src/admin.rs: create-zone handler (~171) + module doc/help
- crates/vals-cluster/src/net/server.rs: range_stats_reply (~3362) already yields per-range entries+midpoint
- crates/vals-client/src/observe.rs: counters! macro (~168/210), Gauges struct + gauges_named (~366,401)
- crates/vals-client/src/partition.rs: RangeMap::split/splits (~104,179) — put even_boundaries helper here
- existing test: crates/vals-cluster/src/node.rs pd_auto_splits_a_hot_range (~6601) uses pd.set_auto_split

## Work log
- 2026-07-31 plan: Context: the auto-split *policy* already exists in the PD (`auto_split_poll` in `crates/vals-cluster/src/pd.rs`: polls each node's `RangeStats`, splits a range past `auto_split_threshold` at a node-re
…[truncated]
- 2026-07-31 context hints: 6 recorded with plan
- 2026-07-31 implementer report: Implemented task 0108: shipped the sorted-zone guardrails and flipped auto-split to default-on.  ## Changes  **1. Presplit at creation** - `crates/vals-client/src/partition.rs`: new pure `even_boundar
…[truncated]
- 2026-07-31 review tier: high-powered — reviewers: claude
- 2026-07-31 review (claude): accept — The change fully implements task 0108's shippable scope: presplit-at-creation (`even_boundaries` + `create-zone ... presplit=K` with validation), the default-on single-range warning + per-range fill/s
…[truncated]
- 2026-07-31 decision: accept — commit: feat(0108): sorted-zone split guardrails + default-on auto-split  Ship the "cheap guardrails" for sorted zones and flip auto-split to default-on, killing the single-range concentration footgun:  - cre
…[truncated]
- 2026-07-31 usage: 85,462 tok (in 334, out 85,128, cache_r 11,526,431, cache_w 340,387) · $10.0205
  implementer: 48,268 tok (in 220, out 48,048, cache_r 8,341,527, cache_w 116,891) · $6.1036
  coordinator: 27,144 tok (in 62, out 27,082, cache_r 2,446,016, cache_w 177,973) · $3.0127
  reviewer:claude: 10,050 tok (in 52, out 9,998, cache_r 738,888, cache_w 45,523) · $0.9042
