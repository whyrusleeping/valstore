---
id: "0442"
title: 'Remaining range-count write cost after 0435: note_winner range_index search and per-sweep/control RangeMap clones'
status: done
priority: 4
created: "2026-10-14"
updated: "2026-10-15"
depends_on: []
spec_refs: []
---

## Description

Split from 0435 (bench/0435-range-residual/FINDINGS.md §5). After the fence fix, the lab-1 3-node RF 2 A/B still shows +0.415 µs/write (+15.5%) at 17,000 vs 336 ranges (lbr +1,043 cycles/write). The callers are logical range accounting (`note_logical_winner_apply` → `note_winner` → `RangeMap::range_index`, ≈ +560), background whole-map clones and walks (`sweep_loop`, `serve_control_request`, `set_sorted_ranges`, ≈ +355), and the one remaining fence search (≈ +250).

Candidates: a faster `range_index`, e.g. contiguous split storage (−23% per search in a microbenchmark, ≈ 37% of its range-count part); reusing the fence's range index in `note_winner` when the accounting map is the same; and avoiding the per-sweep and per-control-request `RangeMap` clones.

Acceptance: A/B the top item with bench/0435-range-residual/ab.py on lab-1, or record it as not worth fixing.

## Outcome

RangeMap::range_index (vals-client partition.rs) now answers maps with >16 splits from a lazily built OnceLock 16-way search tree of 8-byte big-endian key heads (per-block prefix from the next block's fence, full-compare fallback only on equal heads, prefix check only in a level's last block), dropped on split/split_canonical/split_canonical_with_token/merge_at and excluded from PartialEq/Debug; semantics and wire format unchanged, differential test vs partition_point (incl. invalidation and last-block prefix cases, each kill-tested). Preregistered lab-1 A/B with 0435's ab.py (bench/0442-range-index/FINDINGS.md): 17K ranges BASE 3.093/3.085 → FIX 2.873/2.873 µs/write (−7.0%, gap 26× spread), 336 control −3.2%, all arms clean; lbr note_logical_winner_apply 775→215 and fence_writes 581→241 cycles/write. Residual now +0.309 µs (+12.0%); remaining background whole-map clones/walks proposed as 0469. Artifacts in /data/dataset/task0442-results/.

Commit: perf(0442): RangeMap::range_index uses a lazy 16-way index of 8-byte key heads (dropped on split/merge); lab-1 A/B −7.0% shard µs/write at 17K ranges, −3.2% at 336; residual +0.309 µs; whole-map clones split to proposed 0469
