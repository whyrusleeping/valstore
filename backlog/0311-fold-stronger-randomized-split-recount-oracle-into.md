---
id: "0311"
title: Fold stronger randomized split-recount oracle into split_recount_tests.rs
status: done
priority: 3
created: "2026-09-26"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

From the 0310 re-review by agent_6. The in-tree randomized test is weak: 8 seeds × 60 ops, with no forget or GC compaction and few completed spans. agent_6 privately ran a stronger version (96 seeds × 200 ops) that includes:
- multi-cut splits;
- split and merge in the same epoch;
- forgets, including the live cursor key;
- `advance_durable_gc_watermark` + `compact_runs` tombstone drops;
- up to 10 steps per op;
- per-op bracket and exact-when-ready checks;
- a final exact check of counts and leaf digests.

That run found 0 mismatches over 2689 boundary changes on pending spans. Fold that oracle into `crates/vals-cluster/src/replica/split_recount_tests.rs`, keeping the runtime reasonable in debug. Also make `assert_bounds_bracket_exact` assert `lo == hi` for finished children inside spans.

Acceptance: the new test passes in debug, and a deliberate mutation of `note` (for example `<` instead of `<=`) makes it fail.

## Outcome

random_split_merge_sequences_stay_exact_without_reseed now runs 32 seeds × 200 ops covering multi-cut splits, split+merge in one epoch, merges, mutations, forgets including the live cursor key, GC watermark + compact_runs tombstone drops, flushes and 0–10 recount steps, with per-op bracket/no-reseed/exact-when-ready checks and final exact counts + leaf digests (≥900 map changes on pending spans asserted). assert_bounds_bracket_exact also requires lo==hi for finished children inside spans. Debug: all 18 split_recount_tests pass (~14.5 s). Mutations of SplitRecount::note (`<=`→`<`, dropping `*done ||`) make the test fail.

Commit: cluster(0311): stronger randomized split-recount oracle (32 seeds x 200 ops: multi-cut, split+merge per epoch, cursor-key forgets, GC tombstone drops); finished span children must bracket exactly
