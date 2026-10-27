---
id: "0126"
title: Bound cooperative flush snapshot destruction on terminal error paths
status: done
priority: 5
created: "2026-08-04"
updated: "2026-09-08"
depends_on:
    - "0094"
spec_refs: []
---

## Description

Follow-up hardening from task 0094 review: uncommon `flush_plan_step` sync/allocation failures and `flush_commit_start` manifest failures can still drop a full snapshot `Vec<SnapshotEntry>` in one serving-core call. Make cleanup incremental or move destruction off-core without weakening flush-gate cleanup or manifest failure semantics.

Acceptance criteria:
- Plan-finalization and commit-start failure paths do not synchronously destroy O(memtable) snapshot entries on the serving core.
- Cleanup remains safe before and after the manifest commit point, with `flush_in_flight` reliably cleared where retry is valid.
- Focused failure-injection tests cover bounded cleanup and existing store/cluster suites remain green.

## Outcome

Completed bounded deferred snapshot destruction for cooperative flush tail-sync, allocation, manifest-write, and mid-plan abort failures (implementation 6c42fe93 plus reviewed allocation-release correction). Retry gates and manifest/segment semantics preserved; cluster schedules pending cleanup; exhausted cleanup releases its backing allocation. Full debug vals-store (312 unit tests plus integration) and vals-cluster (608 unit tests plus integration/doc) suites passed, focused failure tests passed after revision, diff check clean, independent reviewer accepted.

Commit: fix(store): release drained flush cleanup allocation and finalize bounded cleanup
