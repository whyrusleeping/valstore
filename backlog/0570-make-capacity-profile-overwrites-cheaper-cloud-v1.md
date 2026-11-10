---
id: "0570"
title: 'Make capacity-profile overwrites cheaper (cloud v1: overwrites 239k/s vs fresh inserts 803k/s, bound by run probes)'
status: todo
priority: 2
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description
Owner request (2026-11-10): make overwrites faster.

## Evidence (task 0567 run bench/cloud/runs/v1-20261110, i4i.2xlarge, 1 drive, 4 SPDK partitions, RF 1, capacity profile, run format v7, client colocated)
- Batch-32 writes of 100 B values at the same width/concurrency: **803k/s for fresh keys** (prefill) vs **239k/s for overwrites** (timed step wrapping onto existing keys); single-key overwrites 224-235k/s.
- Node counters per second: overwrite steps do ~56k `vals_apply_probe_runs` / `vals_*apply_run_reads` (one run-block read per probe); fresh inserts ~0 (blooms say absent). The probe rate sits near the strong point-read ceiling of that box (72k/s), so overwrite throughput is bounded by the same run-block read + zstd decompression cost tracked in 0568.
- docs/deploy.md: capacity "resolves the current maximum version before acknowledging; a flushed positive may read a run block". The latency profile avoids this with blind-append + an in-RAM winner directory (62.9 B/physical key, rebuild on open), which is why capacity stays the default.

## Goal
Raise capacity-profile overwrite throughput without the latency profile's RAM/startup cost and without weakening LWW/CAS semantics (docs/blind-append-lww.md).

## Approach (measure first)
1. Reproduce locally (L1: file backend or rss_bench-style overwrite workload) and attribute per-overwrite cost: probe count, device bytes, decompression, version compare, allocation.
2. Candidate directions, to be measured not assumed: make the probe cheaper (0568's fixes: smaller decompress unit / sub-index / decompressed-block cache benefit writes too); avoid the probe when the incoming version provably wins (e.g. a compact per-run or per-range max-version bound, or an HLC/sequence fence so a newer write needn't find the old winner); batch/sort probes within a BatchApply so one block serves many keys.
3. Keep/reject on numbers: overwrite throughput and p99, fresh-insert throughput (no regression), RSS, restart time; hardware confirmation on the cloud v1 profile (~$0.50) or the lab.

## Acceptance
- Attribution of the overwrite cost with numbers.
- A kept change shows a measured overwrite throughput gain with no regression on inserts/reads/RSS, correctness tests (incl. kill-the-fix regressions) and doc updates; or a documented rejection.

## Acceptance criteria

## Work log
