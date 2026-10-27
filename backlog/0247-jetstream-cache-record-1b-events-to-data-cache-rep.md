---
id: "0247"
title: Cache capture and replay investigation superseded by full-cache runs
status: done
priority: 3
created: "2026-08-24"
updated: "2026-09-29"
depends_on:
    - "0248"
spec_refs: []
---

## Description

## Disposition — closed as superseded, not a clean integrity pass

Owner-directed backlog triage (2026-09-29): cache capture was already complete and replayable. Later 0321/0326 runs ingested the full sealed cache (2,090,212,819 persisted KV) and recorded throughput, memory, maintenance and restart behavior. See `bench/0321-long-soak/FINDINGS.md` and `bench/0326-shutdown/FINDINGS.md`. Those runs supersede this historical replay investigation and its obsolete 360M-key/80-GiB-slab assumptions; do not perform another replay to satisfy the old plan.

The original failed runs remain failed. The later logical census does not clear replica integrity, 0248, 0250, 0336, or the production sizing gate 0203. Their unresolved work stays in those tasks. No evidence is deleted and no retained-drive wipe is authorized by closure.

### Historical scope
Two-phase run using the new bootes jetstream cache feature (plus a new `--jetstream-cache-max-events` cap added for this):

**Phase A — record:** fresh lab cluster (wipes retained 0232 corpus, user-approved), bootes `--jetstream-cache-record /data/jetstream-cache-<date> --jetstream-cache-max-events 1000000000`. Cache seals complete at 1B commit events (early-complete path). Resumable; expect 1.5-3 days at quota pacing.

**Phase B — replay:** fresh cluster again, `--jetstream-cache-replay` with no network dependency; watch overall performance and find bottlenecks. User explicitly wants the capacity wall data (replay is expected to turn capacity-terminal ~1/3 through: ~360M keys/node RAM wall, 80 GiB slab floors).

Acceptance:
- Complete manifest.json at ~1B events in /data cache dir; replayable.
- Replay run with metrics captured (cluster + bootes side), bottleneck analysis written up under bench/.


## Acceptance criteria

## Historical plan — superseded

1. Audit the sealed cache manifest and prior record/replay evidence; identify which acceptance criteria are already satisfied without redoing completed work.
2. If prior replay evidence is insufficient for the requested capacity-wall analysis, run a coordinated fresh-cluster cache replay using the existing hardware runbooks and the owner's test-data wipe authorization. Verify fleet idle, preserve sealed cache/cores/binaries, source-pin rebuilt binaries, collect cluster and driver metrics, and stop safely at a defensible capacity terminal or safety failure.
3. Write a concise bench findings report with cache completion/replayability evidence, measured throughput and capacity/bottleneck analysis, limitations, and final fleet state. Review evidence and commit the accepted result; do not claim a completed gate if safety faults preclude it.

### Starting points
- plans/bootes-soak-runbook.md §5.1 cache capture/replay
- bench/metrics-0248-driver.promz and /home/user/code/bootes/soak/bootes-cache-replay.20260829T001546Z.* contain prior replay evidence
- bench/0233-batched-hardware-20260907 records latest fleet state: stopped, format=false, retained corpus crash recovery needed
- Memory notes: AER storms continue; 0248 paused, cores/cache must be preserved; no fleet reservation remains

## Work log

- 2026-09-06: Owner paused the 0248 validation and released lab for other hardware gates, explicitly allowing cluster test-data wipes. Its shared phase-B bootes replay stopped at 04:30:57 UTC after its 300-second drain deadline, with final cursor withheld (unclean drain); the sealed `/data/dataset/jetstream-cache-20260824` cache and existing logs/metrics are retained. This is not a completed clean perf gate. Resume only as a coordinated workload, not an independently restarted fleet reservation. Stop receipts: `bench/0248-pause-20260906/FINDINGS.md`.
- 2026-09-07: Audited the sealed cache and prior replay, then ran a source-pinned fresh replay. Cache phase A is complete and replayable, but the capacity-wall run is blocked again by task 0248: lab-3 SIGSEGVed after 7m12s of replay at only ~17M events, with a new 4.13 GB core showing impossible allocator chunk size in `seed_step`. No capacity rejection had occurred. The run stopped immediately; cache/core/binaries were preserved and the fleet is inactive with `format=false`. Findings: `bench/0247-cache-replay-20260907/FINDINGS.md`.
- 2026-09-07 review: Independent evidence review accepted the blocked outcome. Coordinator reran the analyzer and confirmed the 420-second healthy window, 40,546 events/s, and zero capacity rejection. No further replay is authorized by this session; resume after 0248 is resolved or the owner decides how to proceed.
