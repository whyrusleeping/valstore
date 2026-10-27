---
id: "0453"
title: 'Range deletes 0444d: cover-aware engine reads/probes, per-run max tree, block-skipping engine scan'
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

Part of 0444 (docs/range-deletes.md §1 "Block version bound", §2 replica-side skip rule (engine half), §3 engine reads). vals-store only.

## Outcome

Engine point reads, staged probes and winner-directory lookups fold cover(k) in (covered key = tombstone at cover, no value read; newer writes win); batch winner tokens defer to per-key probes while F is non-empty. MaxTree (internal nodes only) on v6 runs while F is non-empty, built at flush/merge install, open, and Engine::build_range_tombstone_max_trees(); zero-RT engines build none. Engine::scan_winners_page_counted filters covered winners and skips whole-block-inside blocks via rt_next_block, with RtScanStats. Skip-boundary, random-model, covered-read and tree-lifecycle tests added; three kill-the-fix mutations fail both skip tests. Accounting scans and the staged replica scan stay physical (handoffs in 0445/0446). vals-store 529 tests and vals-cluster lib tests green; comprehensive review accepted (nits: no hi clamp in the skip and block_reads counted only when F is active, both left for 0446).

Commit: feat(0453): cover-aware engine reads — synthetic tombstones on point reads/probes, per-run max tree, block-skipping engine scan with RtScanStats
