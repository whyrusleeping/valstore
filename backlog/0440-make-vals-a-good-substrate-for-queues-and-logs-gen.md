---
id: "0440"
title: 'Make vals a good substrate for queues and logs: general-purpose primitives'
status: done
priority: 4
created: "2026-10-14"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
## Why
Discussion 2026-10-14 (owner): could vals serve as a large queue/log, a Redpanda or Redis-queue replacement (for example, wheeler's stage-to-stage topics)? Appends fit well: queue entries are write-once keys, the blind-append path vals is optimized for. But a queue builder hits several gaps, and none is queue-specific. This task gathers the general-purpose primitives that would close them. It is a discussion and decision task: accepted items are split into their own tasks.

The list was first written from code and docs. It was updated on 2026-10-23 against what has since shipped and the 0471 measurements (bench/0471-queue-hw/FINDINGS.md).

## Where vals falls over as a queue (status 2026-10-23)
1. **Consumed history slows scans: solved for range deletes.** Range deletes (0439) keep head scans at 0.4 ms p50 regardless of deleted history (540M messages deleted in 0471 CAL-2-FIX). Follow-ups made long-lived queues and rings flat over 2 h at 500k msgs/s:
   - 0473: RT fold cost;
   - 0475: dominated RT records;
   - 0477: covered-range merges;
   - 0478/0480: re-home cleanup;
   - 0485: debt re-homes.

   Per-key deletes still return point tombstones on every replica page. With per-key deletes, a head scan took 30 s after 5 min in 0471, and `gc_grace` defaults to 0, so these tombstones are never collected. The recommendation is to use range deletes for queues. Nothing further is planned here.
2. **No safe read point: open.** The one large item brought to the owner is **proposed 0504**. 0471 sidestepped it with one producer per partition, dense sequence numbers, and consumers that take only the contiguous prefix. Multi-writer streams have no safe point to read up to.
3. **Tail hotspot: measured, deferred.** A single partition with one producer and one consumer reached 32k msgs/s in 0471, against 170k for Redis R24. It is bound by produce round-trip latency (1.9 ms per batch, one batch in flight) and by every write, scan and delete landing on one shard per node. Load splits exist but are off by default (`range_load_split_ops = 0`) and cut at the midpoint. Spreading a stream over P partitions (0471 ran 64) is the supported answer. Hash-bucketed keys (candidate 5) remain a client-side pattern with no server work planned.
4. **No push: open, deferred.** Polling consumers reached 1.6–1.8 ms e2e p50 at 200k–600k msgs/s in 0471, so polling latency is not what binds today. The cost is idle load at low rates, which has not been measured.
5. **No atomic claims or ownership: open, deferred.** Linearizable zones (0026) stay parked. PD leases and fencing are not proposed.
6. **Retention: solved.** Range deletes give ring-buffer retention (0471 Q2 ring, then 0475/0477/0485: RSS, disk and live keys flat for 2 h with exact verification). A zone per time bucket is not needed.
7. **Smaller gaps:**
   - reverse scans: shipped (0484);
   - keys-only scans: shipped (0482, MultiScan with index-only replies), so counts, lag and existence checks no longer ship values;
   - live-only replica replies: replicas skip range-covered entries (0446b), and point tombstones are still returned (item 1);
   - value compression: still client-side only.

0471 also found two limits that no primitive here addresses:
- The client fan-out of 3× per write caps one client at about 930k msgs/s on 10G. Server-side forwarding was rejected twice (0302, 0306).
- p50 latency is about 3× Redis's.

## Candidate primitives (status 2026-10-23)
1. **Range deletes:** shipped (0439 and follow-ups above).
2. **Closed timestamp plus client-stamped append keys:** proposed as **0504**, which starts with a design note. The note must cover:
   - semantics and the exact guarantee;
   - partial writes copied by anti-entropy or hints;
   - floor hand-off across re-home, split, merge and restart;
   - `gc_grace`, write concern, the read fence, and scan-variant interactions;
   - opt-in and versioning;
   - hot-path overhead.
3. **Long-poll scan / watch:** `scan_wait(zone, lo, hi, after, timeout)`. Deferred: 0471 showed polling latency is not the binding limit, and with 0504, `after` could become a read point. Must add essentially nothing to the apply path when no watcher is registered.
4. **Scan options:**
   - reverse scans: shipped (0484);
   - keys/versions-only scans: shipped (0482);
   - live-only replies: partly shipped (range-covered entries skipped);
   - point tombstones: remain (item 1).
5. **Hash-bucketed sorted keys:** client-side pattern, not pursued.
6. **Linearizable coordination for low-rate control traffic** (PD leases with TTL, fencing epochs, CAS, counters; or 0026): deferred. Putting this on the PD raft log loads the control plane the data path depends on, and no queue workload has needed it yet.
7. **Per-key TTL:** not pursued. Range deletes cover queue heads and rings.
8. **Server-side value compression:** not pursued until a capacity win justifies the shard CPU.

## Benchmark
Run as 0471 on lab SPDK instead of locally: Q1 work queue, hot tail, Q3 burst and Q2 ring, against Redis Streams. It ran with `gc_grace = 0` only. Tombstone GC under `gc_grace > 0` was fixed and validated separately (0499, 0194).

## Acceptance
The owner decides which primitives to pursue. Each accepted one is split into its own task with semantics, interactions (anti-entropy, `gc_grace`, range lifecycle, wire/format versioning) and validation. The benchmark is run or explicitly deferred.


## Acceptance criteria

## Work log

- 2026-10-29: owner decision recorded (backlog triage): 0504 (safe read point) rejected for now and closed; the deferred items (long-poll/watch, PD leases and fencing, per-key TTL, server-side compression, hash bucketing) stay deferred. All acceptance criteria are met (decision recorded, benchmark run as 0471), so this task closes.

- 2026-10-20: owner accepted as a short decision pass. Much of the list has since shipped or been measured: range deletes (0439), the 0471 queue benchmark, reverse scans (0484), index-only scan replies (0482), and the re-home and cleanup fixes (0475/0477/0478/0485). Update the list to match, then bring the owner at most one of the remaining large items: coordinator-stamped append plus a closed timestamp, long-poll/watch scans, or PD leases and fencing.
- 2026-10-23 (unattended session): the list above is updated to match shipped work and the 0471 numbers. The benchmark criterion is met by 0471. The item brought to the owner is the safe read point, filed as **proposed 0504** (design note first). Why this one rather than the other two:
  - It is a correctness gap: without it, multi-writer streams cannot be read without risking silently missed writes.
  - Long-poll/watch is a latency and idle-load optimization. 0471 measured polling at about 1.7 ms e2e p50, which is not binding.
  - PD leases and fencing would load the control plane, and no measured workload needs them.

  Waiting on the owner: accept, reshape or reject 0504, and confirm that the deferred items (watch, leases/fencing, TTL, compression, hash bucketing) stay deferred. 0440 can close once that decision is recorded.
