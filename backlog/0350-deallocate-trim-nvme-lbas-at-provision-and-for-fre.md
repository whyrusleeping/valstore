---
id: "0350"
title: 'Deallocate (TRIM) NVMe LBAs at provision and for freed segments: fix FTL-state write-latency collapse'
status: done
priority: 2
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

Split from 0348 (bench/0348-scale/FINDINGS.md §"Drive deallocation discriminator"; raw /data/dataset/task0348-results/DRIVE-DEALLOCATION/).

## Outcome

Software scope done: - Provision deallocates each claimed namespace in full, after the serial checks and before the layout is written. - Freed segments are deallocated only after a later successful manifest write; reusing a segment cancels its deallocation. - `Engine::dealloc_step` runs one bounded synchronous DSM batch per shard snapshot tick. Errors are advisory, and an unsupported device turns it off. - The `device_dealloc_segments_per_tick` node setting defaults to 256; 0 disables provision and runtime deallocation. Metrics are `vals_device_dealloc_*`. - Crash-safety tests cover manifest failure then reopen, a free before the barrier, free then reuse, and the positive merge/co-compaction path. - Docs updated. Comprehensive review: 2/2 accept. The hardware A/B and the SPDK C build are split into 0353, which should come before 0352.

Commit: feat(0350): NVMe DSM deallocation at provision (whole namespace) and for freed segments after a durable manifest barrier; vals_device_dealloc_* metrics; device_dealloc_segments_per_tick kill switch; crash-safety tests; docs
