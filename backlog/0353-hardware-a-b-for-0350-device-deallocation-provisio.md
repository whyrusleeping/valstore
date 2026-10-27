---
id: "0353"
title: Hardware A/B for 0350 device deallocation (provision + runtime TRIM) on lab-1+3
status: done
priority: 2
created: "2026-09-30"
updated: "2026-09-30"
depends_on:
    - "0350"
spec_refs: []
---

## Description

Split from 0350 (work item 4 and the hardware acceptance criterion). 0350 delivers the software: provision-time whole-namespace DSM deallocate, runtime deallocation of freed segments after a durable manifest barrier, `vals_device_dealloc_*` counters, and a node-settings kill switch. This task measures it on hardware.

## Outcome

ABBA on lab-1+3 (57234ef6; DSM shim builds on-box), all arms from 100% NUSE fills. Dealloc ON vs OFF: −0.62% / −8.10% ACK in 30–350M / 150–400M, so no gain; ON arms ended at 93–94% NUSE. Runtime DSM ~4.5–5.2 ms/command, ~0.17% of shard CPU. The long arm FAILED correctness: +3 backlinks in the census, 2 index-CRC compaction errors on node3, DSM errors 2/1; rate fell 874k→233k. That criterion is NOT met here and moved to 0357 (rerun after 0355). Follow-ups: 0355 (p1, root cause; corpus retained), 0356 (p1, default off), 0354 (runtime DSM cost). Evidence: bench/0353-dealloc/FINDINGS.md.

Commit: bench(0353): hardware ABBA for 0350 device deallocation on lab-1+3 (no throughput gain) and failed runtime-dealloc long arm; harness, FINDINGS; follow-ups 0354-0357
