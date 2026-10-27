---
id: "0445"
title: 'Range deletes (0439) slice 3: replica semantics — apply_range_delete, cover checks on every apply path, synthetic tombstones, accounted set + RT fold'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0444"
    - "0456"
    - "0457"
    - "0458"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements docs/range-deletes.md §3 (replica side), §4 "Logical state excludes covered keys" and "The accounted set and the RT fold", and §6 (vals-cluster replica layer).

## Outcome

Replica range-delete semantics complete via 0456 (apply_range_delete, cover test on every apply path, cover-aware local stamps, synthetic tombstones on Read/Digest/MixedBatchRead/SyncValues), 0457 (accounted set A/S_A, cover-filtered seeds/rebuilds/regroups/recounts/digest pages, acct(k), S_A to compaction, collected RTs) and 0458 (RT fold with immutable T, bounded A-skipping turns, covered memtable removal, Busy/not-ready while pending). Acceptance re-checked at HEAD in replica/range_delete_tests.rs (older hidden/newer wins, stragglers on all apply paths incl. never-seen key with no append, overlapping-RT fold equals fresh seed and fails if targeting F, crash/reopen leaves); spot kill-the-fix (fold→F, cursor, blind cover, staged re-test) each caught; cargo test -p vals-cluster --lib 891 passed. Follow-ups: RangeDelete handler kicks accounting (0446), vals_rt_fold_pending gauge (0449).

Commit: chore(0445): close range-delete replica semantics slice after end-to-end acceptance re-check (sub-slices 0456-0458)
