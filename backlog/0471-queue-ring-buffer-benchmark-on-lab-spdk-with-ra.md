---
id: "0471"
title: Queue / ring-buffer benchmark on lab SPDK with range deletes, vs Redis Streams
status: done
priority: 2
created: "2026-10-15"
updated: "2026-10-16"
depends_on: []
spec_refs:
    - docs/range-deletes.md
---

## Description
## Why
Owner 2026-10-15: performance of range deletes in use (as opposed to 0449's zero-RT neutrality check) on real hardware, specifically vals as a queue (Redis-queue style) and as a big ring buffer. This is the "first step" benchmark that proposed 0440 describes, run on lab instead of locally. The owner chose to discard corpus K8 and provision fresh SPDK layouts sized for queues.

## Scope (owner-confirmed defaults)
- 3 nodes (lab-1/2/3) on SPDK, fresh layouts, HEAD build; client on devbox over 10G. Sorted queue zones with RF 3 and Quorum(2) writes, 512 B messages, 64 partitions (plus a 1-partition hot-tail arm), producer batches of 64.
- New client: `vals-bench x netqueue` (producers/consumers/trimmer over real nodes; backends vals and Redis Streams through one harness so latency accounting is identical).
- Scenarios:
  - Q1 work queue: consumers scan their partition head (contiguous-prefix rule) and range-delete what they consumed. Measure produce rate and latency, e2e produce→consume latency, scan and delete latency, and backlog stability.
  - Q2 ring buffer: fixed capacity per partition, a trimmer deletes older than N, for hours. Disk use must level off, throughput must not decay, head scans must stay constant-cost.
  - Q3 burst: consumers paused until a large backlog builds (~100M), then drain. Measure drain rate and the cost of the big delete.
  - Baselines: append-only (no deletes) and per-key deletes.
- Redis Streams comparison on the same boxes (XADD / XREADGROUP+XACK / XTRIM MINID, AOF everysec), at equal message sizes and rates.

## Acceptance
- K8 discarded through the registry (dealloc discard path, bench/CORPORA.md updated); drives provisioned with the offline provisioner and verified serials.
- bench/&lt;id&gt;-queue-hw/FINDINGS.md with setup, preregistered metrics, results per scenario, the Redis comparison, and the limits found (filed as tasks or proposals).
- Correctness: consumers see no gaps or duplicates; the end-of-run verification shows the live entries per partition are exactly [cursor, head).

## Acceptance criteria

## Work log
