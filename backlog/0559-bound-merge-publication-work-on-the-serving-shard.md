---
id: "0559"
title: Bound merge publication work on the serving shard instead of rescanning all engine segments per commit
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: node/maintenance.rs invokes compact_commit inline under store borrow in maint_loop. engine/merge.rs:2549 builds protected_segments over all engine segments;2803 calls reclaim_orphan_segments, rebuilding the same protection set and scanning alloc.used_list (alloc.rs:162-170 walks full configured segment capacity). Co-compacted commits build another whole-survivor keep set at2767-2783. Tombstone validation at2608-2642 also does dropped×non-input-run probes with synchronous read on filter hit. This contradicts bounded changed-work behavior even for a small merge, and blocks serving for the synchronous work.

Existing R8 evidence independently recalculated from bench/0544-r5/captures/SOAK5B-V5-R8/perf-node1-{3500,5000,7000,9000,10200}M-lbr.data.metrics1: interval mean compact_commit =30.89,78.67,61.37,80.57,110.12 ms as node used segments grows863529→2523646. Last interval145 commits. These are historical whole-commit timings including I/O, not new causal attribution to any one set build or a measured fix speedup.

Suggested acceptance: first phase attribution and deterministic work/scale benchmark at fixed merge size, then make publication/reclaim bounded or incremental while preserving ownership/conflict/crash safety; avoid whole-engine HashSet rebuilds and full allocator sweeps on each small commit; regression workload large unaffected engine state + small merge. Consider incremental ownership accounting and bounded orphan-reclaim cursor, but choose smallest evidenced fix, not an unmeasured blanket refcount redesign. Validate pins, concurrent flushes, aborted plans, GC, cleanup and crash-point tests; measure serving latency and ingest.

## Outcome

compact_commit no longer builds whole-engine HashSets. A per-plan ticket replaces the start-of-commit protected set (stale merges are refused with no side effects; debug builds still assert). Reclaim and the co-compaction dead check use a SegmentSet bitmap and Allocator::used_outside, walking the memtable only for orphan candidates; the reference set is unchanged. Bench small_merge_commit_cost_vs_unaffected_state, small-merge commit: 61→6.8 ms at 217K refs and 1M memtable; 127→15 ms at 3M memtable; 22→2.9 ms at empty memtable. Remaining cost: manifest publication (proposed 0566) and the memtable-bounded walk. vals-store: 610 pass, new tests fail when the fix is removed. vals-cluster passes except the known net flake. Clippy is clean. No N2 run.

Commit: engine: merge commit drops whole-engine reference HashSets (0559): plan ticket, bitmap orphan reclaim, memtable walked only for candidates; small-merge commit 61->6.8 ms at 217K refs
