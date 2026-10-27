---
id: "0129"
title: Investigate bootes→vals sink throughput and backpressure
status: done
priority: 2
created: "2026-08-04"
updated: "2026-08-04"
depends_on:
    - "0127"
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
Follow up the 2026-08-04 bootes soak: at 300 parallel backfills, bootes' 50k-entry buffer filled and thousands of producer callbacks remained live, driving ~86 GiB RSS, while vals servers were ~96–98% box-idle with no Busy/errors. Determine why the bootes→vals sink only drains roughly 45k logical records+backlinks/s despite server headroom, identify the limiting stage, and validate a targeted improvement with a fresh A/B.

## Acceptance criteria
- Measure stage-level sink throughput/latency and distinguish bootes serialization, valsgo encoding/network, server BatchApply, and NVMe sync costs.
- Identify the dominant throughput limiter with profile/metrics evidence.
- Implement or specify a targeted fix and validate it against the current serial-flush baseline.
- Record results under `bench/bootes-soak-lab/` and correct prior bottleneck wording as needed.

## Work log
