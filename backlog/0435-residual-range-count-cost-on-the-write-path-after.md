---
id: "0435"
title: Residual range-count cost on the write path after 0420 (+12% shard µs/write at 17k vs 336 ranges)
status: done
priority: 3
created: "2026-10-13"
updated: "2026-10-14"
depends_on: []
spec_refs: []
---

## Description

Split from 0420 (bench/0420-rangemap-borrow/FINDINGS.md §5). With the RangeMap copy removed, the local 3-node RF 2 A/B still runs +0.53 µs/write (+12%) at R3's 17,000 ranges compared with 336 ranges (FIX 4.63/4.96 vs 4.33/4.20). Candidates: `RangeMap::active_rehomes()`, which scans every range's `pending` slot on every batch response in `mirror_rehome_writes` (could be a count maintained with the map); `range_index` searches; per-range `RangeStats` work. Attribute it first (perf on lab, or counters), then A/B the top item with `ab.py`.

Acceptance: the residual is attributed to callers, and the top one is fixed with an A/B or recorded as not worth fixing.

## Outcome

Attributed the 17k-vs-336-range write residual with perf on lab-1: +0.67 µs/write (+23.5%), lbr +2,666 cycles/write. By caller: write fences ≈1,000 (top), note_winner ≈540, note_range_ops ≈170, background map clones and walks ≈380, active_rehomes ≈100. Fix: ServerShared's ring, catalog and range_maps are now generation-tracked (Tracked<T>). fence_writes caches owns_range_write per range under the placement stamp and returns the batch's range indices. note_fenced_range_ops reuses those indices. fence_mirrored_response skips the re-fence when the stamp and readiness are unchanged; the epoch check is kept. Kill-the-fix unit test included. Preregistered lab-1 A/B: 17k BASE 3.527/3.539 vs FIX 3.075/3.094 µs/write (−12.7%); 336 2.839 → 2.670. lbr shows no fence under mirror_batch_response. The remaining residual (+0.415 µs) is proposed as 0442. Findings are in bench/0435-range-residual/FINDINGS.md; docs/zones.md is updated.

Commit: perf(0435): write fence caches per-range ownership under a placement stamp, reuses its range indices for load sampling, and skips the post-apply re-fence when ring/catalog/range maps are unchanged; −12.7% shard µs/write at 17k ranges
