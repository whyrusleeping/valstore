---
id: "0452"
title: 'Range deletes 0444c: fragment set, record kind 2 append/replay, RT records persisted in v6 runs, v6-run downgrade invariant'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on:
    - "0450"
    - "0451"
    - "0455"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0444 (docs/range-deletes.md §1 "In-RAM fragment set", "Persistence", "Downgrade fence"). vals-store only.

## Outcome

Sorted engines hold F (FragmentSet) plus unflushed RT tail records; apply_range_delete plans the overlay (no-op/cap refusal appends nothing), refuses below run v6, commits an empty v6 marker run when no v6 run exists, appends kind 2 above every committed covered_seq. Replay overlays tail records (seq > covered_seq) and each run's RT block (cached on OpenRun). Flushes persist tail records (RT-only run when the memtable is empty, v6 forced); streamed/full merges, forget and vlog-GC carry inputs' records. Unflushed RT segments protected like memtable values (co-compaction loss found by test); flush headroom counts RT bytes; legacy compaction errors on kind 2; valsd dump skips kind 2 until 0448. Tests cover reopen, fence across rewrites, RT-only runs; review fixed seq-0 loss and RT-only reserve. Reads stay cover-unaware (0453).

Commit: feat(0452): range-tombstone engine core — Engine::apply_range_delete, record kind 2 append/replay, RT records moved by flushes and every run rewrite, empty v6 fence run before the first RT, unflushed RT segments protected
