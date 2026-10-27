---
id: "0211"
title: Isolate mixed-read payload allocation from DMA-retention locality
status: done
priority: 1
created: "2026-08-15"
updated: "2026-08-15"
depends_on:
    - "0206"
spec_refs:
    - bench/lab-read-dma-20260817-0206/FINDINGS.md
    - bench/lab-read-ceiling-20260816-0205/FINDINGS.md
---

## Description

Run the three-arm hardware experiment proposed after task 0206 to determine whether a compact contiguous response arena removes per-value allocation cost without the cache/TLB penalty of retaining one 4 KiB DMA scratch buffer per value.

Acceptance criteria:
- Implement two exact experimental patches over the same control source: (A) current per-value `Vec` baseline, (B) one preallocated contiguous batch value arena that copies each validated value while hot and releases DMA scratch immediately, and (C) task-0206 retained-DMA direct encoding. Preserve validation, winner revalidation, result order, paging/bounds, wire bytes, fallback, tombstones, errors, and read repair in both spikes.
- Instrument/test each arm sufficiently to prove its intended payload lifecycle: per-value allocations and two copies in A; no value-sized per-value allocation, immediate scratch release, compact arena retention, and two copies in B; no temporary value copy/allocation and page-lifetime scratch retention in C. Record actual page scratch count/bytes and value allocation/copy counts.
- At the task-0205 mature RF=2/R=2 width-256, 12-coordinator × 2-in-flight knee, run matched warmup plus at least three 30-second generations per arm with identical source/config/corpus/client provenance. Retain throughput, task-clock/key, p50/p99/max, correctness, allocator/memmove profiles, top-down memory/backend bound data, device reads/blocks, NIC bytes/key, RSS/swap, and corpus/run/directory state.
- Analyze whether B beats both A and C beyond run noise, whether C's larger retained DMA footprint explains its neutral result, and whether remaining structural batch allocations dominate. Use a clear advance/reject threshold and preserve contradictory evidence.
- Do not retain either production experiment unless it demonstrates a repeatable material CPU/key and throughput or latency gain without correctness/resource regression; otherwise remove it. Restore all lab nodes inactive in their supported `format = false` state and retain exact commands, patches, raw evidence, findings, and operational guidance.

## Outcome

Measured matched per-value Vec, compact page arena, and retained-DMA mixed-read arms on the mature lab knee. Both candidates were neutral and removed; retained evidence shows structural batch work, not retained-DMA locality, dominates the isolated payload change. Fleet restored inactive on the supported control.

Commit: bench: isolate mixed-read arena costs
