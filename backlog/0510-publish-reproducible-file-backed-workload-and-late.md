---
id: "0510"
title: Publish reproducible file-backed workload and latency benchmarks for ordinary hardware
status: done
priority: 2
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/SPDK.md#Measured throughput
---

## Description

Accepted follow-up to adopter review 0507, finding 2 and priority 3. Answer what an adopter can expect from the deployment they actually start with, without weakening the existing SPDK evidence.

Acceptance criteria:
- Ship a copyable benchmark recipe using a file-backed valsd deployment with default capacity profile and anti-entropy enabled; record hardware, revision, dataset size/age, concurrency, RF/W/R, and persistence settings.
- Measure buffered and device-durable writes plus reads and a representative mixed workload, reporting throughput, errors/value validation, p50/p95/p99 and resource usage at moderate load and near saturation.
- Include a modest single-machine setup; distinguish same-host traffic from independent host/network results and warm-cache from device-backed reads.
- Make the recipe reusable on the reader's machine, using existing harnesses where possible. No comparative winner claims without matched measurements.
- Publish concise user-facing results and link them beside headline performance. Keep bulky local measurements in ignored bench storage.

## Outcome

Published docs/benchmarks.md: file-backed valsd (3 nodes plus PD, default settings, anti-entropy on, RF 3, majority writes and reads, 1 KiB values, 4M aged keys), measured on one 24-core host with a 990 PRO and on three such hosts with an external client. It covers buffered and device-durable writes, warm and cold reads and mixed workloads, saturated and at half load, with p50/p95/p99, retries, value checks, CPU, RSS and disk I/O. Every run had zero errors. The recipe is scripts/file-bench.py, built on vals-bench netprogress (new load and reuse modes, p95, Busy backoff). ARCHITECTURE.md links the page. README, docs/README and SPDK.md link lines are in the working tree for the 0505 stack. Filed proposed tasks 0523 (fsync blocks the serving thread) and 0524 (anti-entropy stalls). Evidence: bench/0510-file-bench.

Commit: 0510: publish file-backed valsd benchmarks for ordinary hardware (docs/benchmarks.md, scripts/file-bench.py recipe; netprogress load/reuse, p95, Busy backoff, window/CPU reporting)
