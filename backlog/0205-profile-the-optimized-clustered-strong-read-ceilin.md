---
id: "0205"
title: Profile the optimized clustered strong-read ceiling
status: done
priority: 1
created: "2026-08-14"
updated: "2026-08-14"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/SPDK.md
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description
Establish the first limiting resource after task 0204 at the winning mature RF=2/R=2 width-256, 12-coordinator × 2-in-flight shape.

Acceptance criteria:
- Reproduce repeated exact-current-source throughput, p50/p99/max, errors, misses, mismatches, corpus/run state, and winner-directory coverage with exact binary/config provenance.
- Capture high-sample hot-shard call graphs plus task-clock, cycles, instructions, LLC/cache misses, stalled cycles or top-down backend-bound metrics, branch misses, context switches, kernel/softirq CPU, allocator/memcpy symbols, SPDK submission/completion/scratch costs, connection write/drain/queue metrics, NIC packets/bytes, and device reads/blocks.
- Run a short dual-client or otherwise independent load-source control and a populated-LBA SPDK qpair-depth control sufficient to distinguish serving CPU, client/NIC, and device ceilings.
- Identify the next dominant cost with retained evidence and a bounded A/B plan; do not infer it from the pre-0204 profile.
- Preserve unrelated work, restore the fleet, and publish raw captures, findings, and operational guidance.

## Acceptance criteria

## Plan

1. Inventory and preserve the current workspace and lab fleet before changing anything: record Git/source status (including unrelated edits), exact daemon/client hashes, configs, service/process state, mature corpus/run counts, RF=2/R=2 topology, winner-directory coverage, NIC/device identity, and current metrics. Rebuild and deploy the exact current code only if needed, using the established no-`--delete`, `third_party`/`target`-preserving procedure; do not reformat or mutate the corpus.
2. Add a dedicated `bench/lab-read-ceiling-20260816-0205/` evidence bundle with a repeatable capture harness and a running README. At the supported width-256, 12-coordinator × 2-in-flight shape, collect at least three exact-current-source 30-second generations after warmup, retaining client throughput and p50/p99/max, errors/misses/mismatches/value checks, before/after server metrics, run/corpus/directory state, binary/config provenance, process and per-thread CPU, NIC and block-device counters, and connection write/drain/queue telemetry.
3. On representative accepted generations, identify hot serving shard TIDs and collect high-sample call graphs plus synchronized `perf stat` evidence. Cover task-clock, cycles, instructions, cache/LLC misses, stalled-cycle or supported top-down backend metrics, branch misses, context switches, kernel/softirq CPU, allocator and memcpy symbols, winner-directory work, SPDK submission/completion/poll/DMA scratch costs, and socket response construction/write/drain/queue costs. Document unsupported PMU events or measurement caveats rather than silently omitting them.
4. Run two bounded controls without weakening RF=2/R=2/R=2 semantics: (a) split the same aggregate offered load across two independent client processes/hosts where feasible, comparing throughput, latency, client CPU, NIC utilization, and server work to the single-client baseline; and (b) while the store is safely stopped, run read-only SPDK random reads over populated LBAs across a useful qpair-depth sweep (including the serving path's effective depth), retaining IOPS/latency/CPU evidence. Never format or write the devices, then restart and verify exact corpus/directory/run health.
5. Analyze normalized per-key deltas and profiles to distinguish client/NIC, serving CPU/memory, and NVMe ceilings. Name the first dominant post-0204 cost only when supported by retained evidence, and publish a bounded, falsifiable A/B plan for the corresponding follow-up; preserve neutral/contradictory evidence.
6. Restore the fleet to its original supported service/config state, verify strong reads and directory coverage after restoration, and publish `FINDINGS.md` plus operational guidance and exact commands/raw paths. Review the resulting evidence against every acceptance criterion; no production code changes are expected unless a small capture-only fix is indispensable.

### Starting points
- bench/lab-read-breakthrough-20260814-0204/FINDINGS.md and capture-final.sh are the immediate baseline and harness starting point.
- plans/box-provisioning-runbook.md and memory.md warn that lab deploys must not use rsync --delete and must preserve remote third_party/target artifacts.
- Current HEAD is 8ea7fba; code-bearing 0204 commit is 33d2503. Preserve existing unrelated edits in backlog/0201 and memory.md.

## Work log
- 2026-08-14 plan: 1. Inventory and preserve the current workspace and lab fleet before changing anything: record Git/source status (including unrelated edits), exact daemon/client hashes, configs, service/process st
…[truncated]
- 2026-08-14 context hints: 3 recorded with plan
- 2026-08-14 context hints: Use the existing 0204 benchmark bundle as the immediate known-good baseline; retain exact commands and raw captures, not summaries alone.; Deploy to lab without rsync --delete; exclude/preserve thi
…[truncated]
- 2026-08-14 preload: 5 file(s), ~39 KiB seeded into implementer context
- 2026-08-14 completed: exact-current-source 30-second reproductions, hot-shard call graphs/PMU evidence, independent-client and read-only populated-LBA SPDK depth controls, fleet restoration, and findings retained under `bench/lab-read-ceiling-20260816-0205/`. The first actionable wall is saturated per-shard mixed-batch CPU/memory work; the corrected production-geometry device control establishes the QD128–256 depth knee but does not name NVMe as the next wall before the bounded response-workspace A/B.
- 2026-08-14 implementer report: Implemented task 0205 end to end.  - Added `bench/lab-read-ceiling-20260816-0205/` with reproducible generation, synchronized hot-TID profile, dual-client, and stopped-store SPDK control harnesses;
…[truncated]
- 2026-08-14 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-14 review (sol): revise — The throughput reproductions, profiling, dual-client control, raw evidence, and bounded follow-up plan are otherwise comprehensive, but two capture-path defects mean explicit acceptance criteria are n
…[truncated]
- 2026-08-14 review (sol#2/sol): revise — The repeated throughput, server PMU/call-graph evidence, saturation analysis, dual-client control, restoration captures, and bounded follow-up are generally strong. However, two capture-path correctne
…[truncated]
- 2026-08-14 revision: fixed explicit 262,144 MiB production-region raw-read geometry and post-read transport counter deltas, reran both hardware controls, rejected the superseded captures in place, revised conclusions, and restored all nodes inactive with `format = false`.
- 2026-08-14 revision: Addressed both review defects and reran the affected hardware controls.  - Fixed `vals-bench spdk rawread` to require an explicit production region stride, validate nonzero/aligned/in-device geometry,
…[truncated]
- 2026-08-14 review (sol): accept — The revision resolves both major review findings. The SPDK control now uses and records the actual 262,144 MiB production-region stride, has been rerun over the correct populated prefixes, and the old
…[truncated]
- 2026-08-14 review (sol#2/sol): accept — The revision fixes both prior correctness defects and reruns the affected hardware controls. The raw-read command now uses and records the actual 256 GiB production-region stride, the corrected latenc
…[truncated]
- 2026-08-14 revision: Applied the two accepted-review hardening fixes without rerunning or modifying hardware captures.  - Replaced both MiB shifts with `mib_to_bytes_checked`, which performs checked `usize -> u64` convers
…[truncated]
- 2026-08-14 review (sol): accept — The hardening revision correctly replaces the lossy MiB shifts with checked `usize`→`u64` conversion and multiplication for both region and populated-prefix sizes, and adds a focused overflow test. 
…[truncated]
- 2026-08-14 review (sol#2/sol): accept — The follow-up cleanly addresses both hardening findings. MiB conversion now uses checked integer conversion and multiplication for both region and populated-prefix sizes, with a focused regression tes
…[truncated]
- 2026-08-14 decision: accept — commit: bench: profile optimized clustered read ceiling
- 2026-08-14 usage: 30,382,058 tok (in 4,640,093, out 81,549, cache_r 25,660,416, cache_w 0) · cost n/a (unpriced)
  implementer: 20,135,736 tok (in 1,686,956, out 49,804, cache_r 18,398,976, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 4,729,023 tok (in 1,226,245, out 11,194, cache_r 3,491,584, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 3,766,505 tok (in 1,030,465, out 13,224, cache_r 2,722,816, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,750,794 tok (in 696,427, out 7,327, cache_r 1,047,040, cache_w 0) · cost n/a (unpriced)
