---
id: "0111"
title: Sorted-zone range merge (shrink) + load-based (hot-range) splits
status: done
priority: 4
created: "2026-07-31"
updated: "2026-08-01"
depends_on:
    - "0108"
spec_refs: []
---

## Description
The "optional later" items from task 0108's direction #3, deferred out of the guardrails slice:

- **Range merge for shrink.** An auto-policy that merges adjacent under-full ranges (the inverse of auto-split) so a zone that shrank doesn't keep a needlessly-fragmented range map. The `merge_zone` mechanism already exists (raft-replicated, shard-aware hand-off); this is the missing *policy* + anti-thrash vs auto-split (hysteresis so a range near the threshold doesn't split↔merge oscillate).
- **Load-based (not just size-based) splits.** Split a *hot* range — high read/write rate concentrated on one range even when it isn't large — so ordered-scan/point load spreads across replica sets. Needs per-range load telemetry (ops/s) added to the `RangeStats` reply and a load trigger alongside the entry-count trigger.

## Acceptance
A sorted zone that shrinks reclaims range-map fragmentation automatically without oscillating against auto-split; and a range that is hot but not large is split to distribute load. Both remain anti-thrash-safe and interact correctly with scope-bounded zones + weight-aware placement (task 0071).

## Acceptance criteria

## Plan

SCOPE: This session implements the **range-merge (shrink) policy with hysteresis** — the first half of 0111. The load-based (hot-range) split half is split off into task 0118 (needs new per-range ops/s telemetry + a RangeStats wire-format change; separate, riskier slice). 0111's acceptance for the merge half: a sorted zone that shrinks reclaims range-map fragmentation automatically (when enabled) without oscillating against auto-split, and it stays anti-thrash-safe.

The merge *mechanism* is already complete: `PdCmd::MergeZone`, the drive-loop range-op discipline (pd.rs ~1195-1214), `PlacementDriver::merge_zone`, and the admin CLI merge command. Only the *policy* is missing. All changes are in `crates/vals-cluster/src/pd.rs` plus one counter and a docs note.

1. **Config field (PushState).** Add `auto_merge_threshold: u64`. Default `DEFAULT_RANGE_MERGE_THRESHOLD = 0` (OFF). Rationale to document: auto-merge collapses adjacent under-full ranges, which includes deliberately presplit-empty ranges (`create-zone … presplit=K`) and scope/weight-tuned layouts; merge moves data, so opt-in is the safe, reversible default (mirrors nothing forcibly — the operator turns it on for shrink-reclaim). Read env `VALS_PD_RANGE_MERGE_THRESHOLD` at spawn in `spawn_pusher` (like the split threshold). Add `PdDaemon::set_auto_merge(&self, threshold: u64)` writing the field race-free (mirror `set_single_range_warn`).

2. **Poll gate.** In `pusher_loop`, extend `poll_enabled` to also fire when `auto_merge_threshold > 0`.

3. **Merge policy in `range_stats_poll`.** After the existing split loop, add a merge pass. Reuse the same `agg: HashMap<(ZoneId, Vec<u8>), (u64, Option<Vec<u8>>)>`:
   - Refactor so the split loop borrows `&agg` (or share an `actioned: HashSet<ZoneId>`), because a zone must take at most ONE range op (split OR merge) per poll. Have the split loop insert into `actioned` on enqueue.
   - Hysteresis clamp: `let split_ceiling = if auto_split_threshold > 0 { auto_split_threshold / 2 } else { u64::MAX }; let merge_ceiling = auto_merge_threshold.min(split_ceiling);`. This guarantees a merged pair (combined < merge_ceiling ≤ split/2) cannot be an immediate split candidate — the merged range must at least ~double before it re-splits. Prevents split↔merge oscillation regardless of how the operator sets the two thresholds.
   - For each sorted zone (from `snap.zones`, `Ordering::Sorted`) NOT already in `actioned`, with a committed map of `num_ranges() >= 2`: scan adjacent pairs `(i, i+1)`, compute `combined = entries(i) + entries(i+1)` from `agg` (0 for unreported/empty ranges). Pick the pair with the smallest `combined` that is `< merge_ceiling`. If found, `log::info!` (mirror the split log line) and `cmd.send(PdCmd::MergeZone(zone_id, map.token_of(pair+1).to_vec()))`; on success insert the zone into `actioned` and bump the merge counter. One merge per zone per poll.
   - Skip when `merge_ceiling == 0` (feature off). Stale-epoch replies are already filtered upstream. `merge_at` idempotence + the sequential drive queue make a duplicate enqueue harmless (same as split).

4. **Metric.** Add a `pd_range_merges` counter in `crates/vals-client/src/observe.rs` (near `pd_single_range_warnings`), incremented on each enqueued merge. (Optionally add `pd_range_splits` too for symmetry — nice-to-have.)

5. **Docs.** Add `VALS_PD_RANGE_MERGE_THRESHOLD` (default 0 = off, opt-in shrink-reclaim, hysteresis note) to `docs/zones.md` alongside the existing `VALS_PD_RANGE_SPLIT_*` docs.

6. **Tests (in pd.rs / node.rs mod tests, mem-only, mirroring `pd_auto_splits_a_hot_range` in node.rs).**
   - `pd_auto_merges_shrunk_ranges`: presplit or split a sorted zone into ≥3 ranges holding only a little data, auto-split OFF, `set_auto_merge` high enough to cover the combined per-pair; poll until `num_ranges()` shrinks; assert data intact (coordinator read + full ordered scan complete) and it settles (doesn't over-merge past 1).
   - `pd_merge_does_not_oscillate_against_split` (anti-thrash/hysteresis): enable BOTH `set_auto_split(70,100)` and `set_auto_merge(1000,…)` on a single range of N=100; assert it splits to 2 ranges (~50 each) and STAYS at 2 over several polls (merge_ceiling = min(1000, 35) = 35 < 100 combined, so no merge-back).

Verify: `cargo build -p vals-cluster` and `cargo test -p vals-cluster` (plus `-p vals-client` for the observe change). Hand-format new regions only (do NOT run crate-wide `cargo fmt` — rustfmt version skew reformats unrelated code, per memory).

### Starting points
- crates/vals-cluster/src/pd.rs: range_stats_poll (~773), split loop (~808-853), PushState.auto_split_threshold (~638), spawn_pusher env read (~1014-1029), set_auto_split/set_single_range_warn (~1741-1755), DEFAULT_RANGE_SPLIT_THRESHOLD (~1951), drive-loop range-op discipline (~1195-1214)
- merge mechanism already exists: PdCmd::MergeZone, PlacementDriver::merge_zone (~388), RangeMap::merge_at/token_of/range_bounds (crates/vals-client/src/partition.rs ~132-202)
- test model: node.rs pd_auto_splits_a_hot_range (~6605) and pd_warns_on_an_oversized_single_range_sorted_zone (~6776)
- counter list: crates/vals-client/src/observe.rs pd_single_range_warnings (~312)
- docs/zones.md — VALS_PD_RANGE_SPLIT_* section

## Work log
- 2026-08-01 plan: SCOPE: This session implements the **range-merge (shrink) policy with hysteresis** — the first half of 0111. The load-based (hot-range) split half is split off into task 0118 (needs new per-range op
…[truncated]
- 2026-08-01 context hints: 5 recorded with plan
- 2026-08-01 context hints: Primary file: crates/vals-cluster/src/pd.rs — range_stats_poll ~773, split loop ~808-853 (uses split_zones HashSet), PushState ~638, PushState::new ~657, spawn_pusher env read ~1014, set_single_rang
…[truncated]
- 2026-08-01 implementer report: Implemented the **range-merge (shrink-reclaim) policy with hysteresis** — the merge half of task 0111 (load-based/hot-range split half correctly left to task 0118, not touched). The merge *mechanism
…[truncated]
- 2026-08-01 review tier: single-opus — reviewers: claude
- 2026-08-01 review (claude): accept — The change correctly and completely implements the scoped merge (shrink-reclaim) half of task 0111 exactly as the plan specifies: an opt-in (default-0) auto-merge policy in range_stats_poll that colla
…[truncated]
- 2026-08-01 usage: 55,897 tok (in 164, out 55,733, cache_r 4,179,200, cache_w 223,674) · $4.8817
  implementer: 24,315 tok (in 68, out 24,247, cache_r 1,394,605, cache_w 61,085) · $1.6856
  coordinator: 18,822 tok (in 46, out 18,776, cache_r 2,079,738, cache_w 114,558) · $2.2255
  reviewer:claude: 12,760 tok (in 50, out 12,710, cache_r 704,857, cache_w 48,031) · $0.9706
