---
id: "0454"
title: 'Range deletes 0444e: covered-entry filtering in flush/merge/fold/GC, RT record collection, shadow estimator'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0452"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0444 (docs/range-deletes.md §7 engine half). vals-store only.

## Outcome

Compaction honours range tombstones. S_A is set by advance_range_tombstone_accounted_seq (0 = no filtering). Flushes drop entries covered by records with seq <= min(S_A, covered_seq), and an emptied attested group must keep a zero residual. Merges, folds and vlog GC drop entries covered by persisted records with seq <= S_A, reported as covered_dropped, never as current drops. rt_seq_floor is written. Collection needs all three conditions, is re-checked at commit, recomputes F and returns collected_range_tombstones. Replay skips covered tail entries. The shadow estimator counts covered values. Kill-the-fix tests cover partial-merge collection plus reopen and space reclaim. cargo test -p vals-store passes (537 lib tests).

Commit: feat(0454): range-tombstone compaction filtering with the accounted seq, rt_seq_floor, RT collection with commit re-check, covered-entry replay skip, shadow estimate
