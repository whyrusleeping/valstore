---
id: "0208"
title: Break the four-shard maintenance-qpair wall and re-gate eight read shards
status: done
priority: 2
created: "2026-08-14"
updated: "2026-08-15"
depends_on:
    - "0205"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/SPDK.md
    - bench/lab-shard-scaling-20260811-0184/FINDINGS.md
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description

Redesign per-controller maintenance I/O ownership so eight serving shards can retain reliable flush/compaction capacity within the Samsung 9100 PRO controller's 15-live-qpair limit.

Acceptance criteria:
- Design and document a controller-owned or otherwise bounded shared maintenance-qpair model; do not share an SPDK qpair concurrently across threads without an explicit ownership/handoff protocol.
- Eight serving shards plus all required maintenance/control activity stay below the controller qpair ceiling, with observable allocation/ownership and no permanently unflushable shard.
- Preserve thread-per-core serving, flush/compaction progress, shutdown, recovery, zone isolation, RAM admission, and device-error fault isolation.
- Add software scheduling/liveness tests and an SPDK hardware stress gate covering continuous reads plus flushes, merges, and compaction on every shard.
- Repeat a fresh comparable mature 4-vs-8-shard RF=2 read sweep with enough independent client NIC capacity; record CPU, PMU, qpair use, populated-LBA device IOPS/bandwidth, network, RAM, run state, maintenance, errors, and tails.
- Keep four shards as guidance unless eight shards is fully mature, correctness-clean, and materially faster; restore fleet configuration after the gate.

## Outcome

Replaced per-shard maintenance workers with a fair node-scoped two-lane pool and sole-thread per-controller SPDK maintenance qpairs, retaining owner/zone routing and bounded 8+2 qpair use. Added liveness/fault tests, ownership telemetry, documentation, reusable gates, analyzers, and retained lab evidence. Fresh RF=2 gates proved every-owner flush, run-merge, and incremental compaction progress under reads; eight shards improved median mature reads from 4.403M to 5.177M keys/s (+17.6%) with clean correctness/tails, so eight is promoted for Samsung 9100 PRO latency/read throughput while four remains the CPU/RAM-efficiency option. All hardware arms restored the original inactive fleet and config hashes.

Commit: Share maintenance qpairs and gate eight shards
