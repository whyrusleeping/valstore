---
id: "0469"
title: 'Remaining range-count write cost after 0442: per-sweep and per-range-stats whole-map RangeMap clones and walks'
status: done
priority: 4
created: "2026-10-15"
updated: "2026-10-23"
depends_on: []
spec_refs: []
---

## Description

Split from 0442 (bench/0442-range-index/FINDINGS.md §4). After the range_index search index, the lab-1 3-node RF 2 A/B still shows +0.309 µs/write (+12.0%) at 17,000 vs 336 ranges. The largest known remaining range-count item is background whole-map work on the shard: `sweep_loop` (`sorted_routes()` clones every zone map each sweep), `serve_control_request` (range stats replies clone each zone's map via `ServerShared::range_map`, pass another clone to `set_logical_range_map`, and walk every range), and `set_sorted_ranges`. In FIX-LBR-17K these rows total ≈ 320 inclusive cycles per write, including `RangeMap::clone` (152); 0435 §5 attributed ≈ +355 of this work to the range count.

Candidates: hold range maps as shared immutable `Rc<RangeMap>` (or `Arc`) so readers borrow instead of clone; avoid the second clone into the accounting state when the map is unchanged; check whether the PD range-stats poll cadence or reply shape can avoid a per-poll O(ranges) walk.

Acceptance: A/B the top item with bench/0435-range-residual/ab.py on lab-1 (base = 0442's FIX), or record it as not worth fixing.

Owner, 2026-10-20: accepted; vals must handle massive datasets. Range count grows with data (about one range per 0.5–1M entries, so ~25k–50k at the 24B ATProto target, more beyond that), and every shard currently pays for whole-cluster maps. Aim for background range work that scales with what changed, not with the total range count. Measure at 17k and at least one higher count (for example 50k), so the residual's slope is shown and not just one point.

## Outcome

RangeMap clones are O(1) (Arc copy-on-write, shared split index, pointer-equality fast path); SetRangeMaps preserves the map object for unchanged zones; range-stats V3 skips midpoints for 0-entry/0-ops ranges and re-home lookups while those maps are empty. Preregistered lab-1 A/B (bench/0469-range-map-share, 14 arms, ab.py gained --split-par and --template): 50K ranges 3.197 → 2.945 µs/write (−7.8%), 17K 2.892 → 2.782 (−3.8%), 336 unchanged; residual slope 9.2 → 4.9 ns/write per 1,000 ranges; sweep, clone and RT-exchange clone rows gone in lbr. Remaining range-count items proposed as 0502 (PD per-split map rebuild) and 0503 (range-stats reply walk).

Commit: perf(0469): copy-on-write RangeMap (O(1) clones), keep unchanged pushed maps, skip empty-range midpoints; lab-1 −7.8% shard µs/write at 50K ranges, −3.8% at 17K
