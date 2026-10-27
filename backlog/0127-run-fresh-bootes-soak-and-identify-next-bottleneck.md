---
id: "0127"
title: Run fresh bootes soak and identify next bottleneck
status: done
priority: 2
created: "2026-08-04"
updated: "2026-08-04"
depends_on: []
spec_refs: []
---

## Description
Run a fresh bootes firehose+backfill soak against the 3-node lab SPDK cluster using current valstore HEAD and current valsgo/bootes, capture cluster/client throughput, CPU, memory, device, errors, splits/backpressure, and profile the limiting resource. Record the measurements and the next actionable bottleneck.

## Acceptance criteria
- Fresh current-HEAD 3-node RF=2 soak completes with value-path preflight passing.
- Time-series measurements cover bootes throughput/errors and per-node CPU, memory, device/engine health.
- At least one targeted profile or A/B distinguishes the dominant bottleneck.
- Results and follow-up recommendation are written up under `bench/bootes-soak-lab/`.

## Work log
