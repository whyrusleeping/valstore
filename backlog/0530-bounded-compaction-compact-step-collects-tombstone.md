---
id: "0530"
title: Bounded compaction (compact_step) collects tombstones with the volatile GC cutoff instead of the persisted one
status: done
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found in the 2026-10-29 project review (code read, not yet reproduced). `VersionedStore::compact()` (crates/vals-cluster/src/replica/maintenance.rs ~533) drops tombstones below `durable_gc_cutoff_ms()` (persisted watermark − gc_grace, replica/lifecycle.rs:2073), but `compact_step()` (~569), which the daemon's maintenance loop calls (node/maintenance.rs:238), drops tombstones with `min(wall, high_water) − gc_grace`, i.e. the volatile high water. So bounded compaction can collect a tombstone above the persisted admission floor; after a restart that reloads the older persisted watermark, the node's admission floor is lower than the collected tombstone and it can admit the pre-delete value (resurrection). The existing test `replica/tests.rs` "never uses an unpersisted GC cutoff" shows the invariant is intended (task 0173).

Acceptance:
- Every tombstone-collecting compaction path uses the durable cutoff (one shared helper).
- The "never uses an unpersisted GC cutoff" regression covers both `compact` and `compact_step`, plus a crash/reopen case showing an old value is refused after restart; the test fails with the fix reverted.

## Outcome

compact and compact_step share tombstone_gc_filter (cutoff = persisted liveness watermark - gc_grace); compact_step no longer uses min(wall, high_water) - gc_grace, so a restart that reloads the persisted watermark can't admit a value whose tombstone was collected. Run compaction paths already used the durable cutoff. compact_never_uses_an_unpersisted_gc_cutoff covers both compactions; new gc_compaction_never_lets_an_old_value_back_in_after_a_restart reopens the store after the high-water keys are forgotten and re-applies the pre-delete value. Both fail with the old compact_step (reopened store returned the deleted value). vals-cluster tests and vals-server backup tests pass; reviewer sol accepted.

Commit: 0530: bounded compaction (compact_step) collects tombstones with the durable GC cutoff, shared with compact via one filter
