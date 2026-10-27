---
id: "0195"
title: 'Hardware-cluster performance breakthrough: measure, profile, optimize, and gate'
status: done
priority: 1
created: "2026-08-12"
updated: "2026-08-12"
depends_on: []
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/SPDK.md
---

## Description
Find and ship a step-change performance improvement on the lab hardware cluster. Start from the latest supported latency profile and mature corpus, measure before designing, identify the first actual CPU/I/O/network/client bottleneck, implement the highest-leverage fix, and validate it with short matched A/Bs before a correctness soak.

Acceptance criteria:
- Preserve all unrelated in-progress task 0194 working-tree changes and establish exact benchmark/source/config provenance.
- Reproduce a current mature-corpus baseline with enough client concurrency and record throughput, p50/p99, errors, per-thread/server/client CPU and PMU, network, NVMe/device I/O, run state, and RAM.
- Profile the limiting path and retain evidence for the dominant cost; do not optimize from intuition alone.
- Implement a correctness-preserving optimization with kill-the-fix tests and no unsupported on-disk/wire compatibility break.
- Demonstrate a substantial repeatable gain in at least one primary production workload (writes are the top priority), with no material correctness, p99, space, or maintenance regression; stop/document if experiments are neutral.
- Run focused and broad debug suites, then hardware A/B and a responsible correctness/churn validation; update architecture/performance documentation with exact evidence and operational guidance.

## Acceptance criteria

## Work log
