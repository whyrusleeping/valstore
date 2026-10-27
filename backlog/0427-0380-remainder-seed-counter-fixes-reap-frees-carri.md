---
id: "0427"
title: '0380 remainder: seed counter fixes (reap frees, carries, uncounted resets), cluster pin tests, no-runs compact drop journaling'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-12"
depends_on:
    - "0380"
    - "0372"
    - "0377"
spec_refs: []
---

## Description

Split from 0380 (2026-10-11). These items need vals-cluster files (`replica/lifecycle.rs`, `replica/maintenance.rs`, `replica/tests.rs`, `replica/seed_pin_tests.rs`, `node/sweep.rs`, `node/maintenance.rs`, `observe.rs`) that hold 0377's and 0372's uncommitted in-review hunks. `commit` sweeps whole files, so start this only once 0372 and 0377 are committed.

Already done in 0380 (vals-store only): pinned v5 index RAM charged (item 2), commit-time pin limit compares held bytes, real admission-band pressure revocation test, the trivially-true `e.compact()` assert, `compact_commit` winner-directory refresh consistency and rate-limited forget-append logging (items 8b and 8c).

## Outcome

All items done. (1) New `ae_seed_pin_reaped_segments` counts reap frees. `ae_seed_merge_carries` now adds the removal-epoch delta. Each seed end is counted once through `reset_seed_progress(SeedEnd)`, with new `ae_seed_error_resets` (sweep warns at most once a minute per zone) and `ae_seed_dropped` (clear, invalidate, store drop). The sync fallback no longer double-counts its restart. The identity is in docs/backpressure.md. (2) New tests: exact reconciliation, the reap count, carries through cleanup forget and lone GC, spill plus carry, sweep and snapshot-tick reap wiring, and seniority after a revocation. The stale test was renamed. (3) The no-runs compact and compact_step now journal GC drops after a zeroing or flush failure, but not on the rollback path. 12 code knockouts each failed their test. vals-store 478/478 pass. vals-cluster lib 830/831; the failure is the known pd range_metrics gauge flake, which passes alone.

Commit: fix(0427): seed counter fixes (reap frees, epoch-delta carries, error/dropped ends, fallback double count), no-runs compaction GC drops journaled after zeroing failure, cluster pin/reap/spill/seniority tests
