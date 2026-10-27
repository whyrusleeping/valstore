---
id: "0165"
title: rehome_copy_loop clones the full RangeMap set twice per 100 ms tick per shard (top CPU item in 0130 hot-shard profile)
status: done
priority: 4
created: "2026-08-10"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
The 0130 sustained-ceiling arm's perf profile of n1's hottest shard (`bench/bootes-soak-lab/run-20260809T221529Z-0130-ceiling/perf-n1-hotshard-5h30m.txt`) shows `RangeMap::clone` under `rehome_copy_loop` as the single largest CPU item (~13% of samples, plus ~2% more under `range_map`): `crates/vals-cluster/src/node.rs:1956-1957` calls `shared.range_maps_clone()` TWICE per 100 ms tick (once for `transport.set_sorted_ranges`, once for iteration), and also clones the ring — on every shard thread, even when re-home is disabled (`VALS_PD_REHOME_ENABLED` unset) and no range has a pending target. With 358 ranges × 4 shards × 10 Hz this is constant allocation churn on the hot serving threads.

Fix shape (any reasonable subset):
- Cache per loop-iteration: clone once, reuse for both call sites.
- Epoch-gate: only re-clone/re-set when `range_map_epoch()` changed since last tick; skip the whole body when nothing changed.
- Skip early when re-home is disabled.

Acceptance criteria:
- No full RangeMap clone happens on ticks where the range-map epoch is unchanged (or when re-home is disabled), verified by test or by construction.
- Behavior under active re-home is unchanged (existing rehome tests pass).
- `cargo test -p vals-cluster` green.

## Acceptance criteria

## Plan

Context: 0155 already collapsed the double clone to a single `shared.range_maps_clone()` per tick (node.rs ~1999). Remaining problem: `rehome_copy_loop` still clones the full RangeMap set AND the ring on every 100 ms tick on every shard thread, and walks every range, even when the range-map epoch hasn't changed and no range has a pending target (the common case — rehome default OFF, PD never sets pending).

Change (all in `rehome_copy_loop`, crates/vals-cluster/src/node.rs):
1. Cache across loop iterations: last-seen `range_map_epoch`, last-seen `ring_epoch`, the cloned `maps: HashMap<ZoneId, RangeMap>`, the cloned `ring`, and a precomputed `has_pending: bool` (any zone map has `pending_of(index).is_some()` for any index).
2. Per tick: read `epoch = shared.range_map_epoch()` and `ring_epoch = shared.ring_epoch()`.
   - If the range-map epoch changed (or first tick): re-clone maps, call `transport.set_sorted_ranges(&maps)`, recompute `has_pending`, store new epoch. Otherwise reuse the cached maps and DO NOT clone or call set_sorted_ranges.
   - If the ring epoch changed (or first tick): re-clone the ring. Otherwise reuse the cached ring.
   - Keep `transport.set_route_epoch(shared.ring_epoch())` each tick (cheap cell set) unless inspection shows it's heavier — then gate it on ring-epoch change too.
3. Early skip: after the cache refresh check, if `!has_pending && cursors.is_empty()`, call `shared.prune_rehome_copy_holds(now_ms)` and `continue` — skipping the per-zone/per-range walk entirely. (With no pending ranges and no live cursors, the walk, the mirror fast-path, and `cursors.retain` are all no-ops; holds pruning is the only side effect that must keep running.)
4. Body otherwise unchanged, operating on the cached maps/ring.

Safety notes: range maps are only replaced via SetRangeMaps which sets `range_map_epoch` (equal-epoch re-push carries identical content by protocol), so epoch-gating is sound. `pending_of` is a property of the map itself, so `has_pending` is stable between epoch changes.

Verification: `cargo test -p vals-cluster` in debug (the rehome integration tests in net/tests.rs — copy/mirror/cutover, resume-across-abort, restart-on-gap-write — exercise the active-rehome path). "No clone on unchanged-epoch ticks" holds by construction; if a cheap unit test of a refresh-decision helper falls out naturally, add it, but don't contort the code for it.

Style: no explanatory comments; hand-format only touched regions (tree is not rustfmt-clean).

### Starting points
- crates/vals-cluster/src/node.rs:1972-2201 rehome_copy_loop (the whole loop body)
- crates/vals-cluster/src/net/server.rs:255-273 range_map_epoch()/range_maps_clone()
- crates/vals-cluster/src/net/server.rs:3364-3410 SetRangeMaps adoption (epoch set + maps replace)
- rehome tests: crates/vals-cluster/src/net/tests.rs:1895,2068,2235,2329,2517,2553
- run: cargo test -p vals-cluster (debug, not --release)

## Work log
- 2026-08-10 plan: Context: 0155 already collapsed the double clone to a single `shared.range_maps_clone()` per tick (node.rs ~1999). Remaining problem: `rehome_copy_loop` still clones the full RangeMap set AND the ring
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: crates/vals-cluster/src/node.rs:1972-2201 — rehome_copy_loop, the whole loop; crates/vals-cluster/src/net/server.rs:255-273 — range_map_epoch(), range_maps_clone(); crates/vals-cluster/src/net/ser
…[truncated]
- 2026-08-10 preload: 1 file(s), ~13 KiB seeded into implementer context
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The change implements exactly the planned fix in rehome_copy_loop (node.rs:1988-2019): epoch-gated caching of the RangeMap set (clone + set_sorted_ranges only on range_map_epoch change), ring-epoch-ga
…[truncated]
- 2026-08-10 decision: accept — commit: 0165: stop rehome_copy_loop's per-tick RangeMap/ring clones — epoch-gated caching + idle skip. The 100 ms loop on every shard thread cloned the full RangeMap set (twice, counting set_sorted_ranges' 
…[truncated]
- 2026-08-10 usage: 26,018 tok (in 86, out 25,932, cache_r 1,503,564, cache_w 83,385) · cost n/a (unpriced)
  coordinator: 15,972 tok (in 44, out 15,928, cache_r 920,706, cache_w 38,115) · cost n/a (unpriced)
  reviewer:claude: 10,046 tok (in 42, out 10,004, cache_r 582,858, cache_w 45,270) · cost n/a (unpriced)
