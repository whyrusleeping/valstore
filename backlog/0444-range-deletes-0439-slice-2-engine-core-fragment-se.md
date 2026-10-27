---
id: "0444"
title: 'Range deletes (0439) slice 2: engine core — fragment set, record kind 2, run v6 (max_hlc, RT block, rt_seq_floor), cover-aware reads, flush/merge filtering, RT collection'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0443"
    - "0450"
    - "0451"
    - "0452"
    - "0453"
    - "0454"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements docs/range-deletes.md §1 and the engine half of §7 (vals-store only).

## Outcome

Range-delete engine core (vals-store) complete via 0450 (fail-loud newer formats), 0451 (run v6), 0455 (FragmentSet), 0452 (apply_range_delete, kind 2, RT records in runs, v6 fence), 0453 (cover-aware reads, max trees, skip scan), 0454 (accounted-seq compaction filtering, rt_seq_floor, three-condition RT collection, shadow estimate). Acceptance re-checked against engine/tests/range_delete.rs: reopen/unsynced loss, skip boundaries, collection + partial-merge reopen, space reclaim, v6 fence across merges/forget; no pre-existing engine assertion changed; cargo test -p vals-store green, workspace check clean. Metric export and Persistence::Device request path are in 0446; S_A wiring in 0445.

Commit: chore(0444): close range-delete engine core slice after end-to-end acceptance re-check (sub-slices 0450-0455)
