---
id: "0204"
title: 'Clustered strong-read performance breakthrough: profile, optimize, and hardware-gate'
status: done
priority: 1
created: "2026-08-14"
updated: "2026-08-14"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/SPDK.md
    - docs/formats.md#1. Wire protocol (`vals-cluster`)
---

## Description
Measure the current mature-corpus RF=2 strong-read path on the lab hardware cluster and ship a step-change improvement. Start from the supported latency profile and the task-0185 batched digest protocol; find the first real client, protocol, serving-core, NVMe, or NIC limit before choosing the optimization.

Acceptance criteria:
- Preserve unrelated working-tree changes and record exact source/binary/config/corpus provenance.
- Establish repeated current baselines across useful batch widths and load shapes, with value verification, errors, p50/p99, per-client/server CPU and PMU, RPC/syscall/network cost, device reads/blocks, shard utilization, RAM, and run state.
- Retain profiling evidence for the dominant cost and implement the highest-leverage correctness-preserving optimization; do not weaken RF=2/R=2 version argmax, tombstone, stale-epoch, fallback, read-repair, compatibility, or response-bound behavior.
- Add kill-the-fix tests and focused/broad debug-suite coverage.
- Demonstrate a substantial repeatable hardware gain over the latest batched-digest baseline with no correctness, p99, space, write, or maintenance regression; document neutral experiments rather than shipping them.
- Update architecture/performance documentation and operational guidance with exact evidence and restore the fleet responsibly.

## Acceptance criteria

## Work log
