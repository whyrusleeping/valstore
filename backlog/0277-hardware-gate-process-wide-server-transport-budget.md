---
id: "0277"
title: Hardware-gate process-wide server transport budgets and deadlines
status: done
priority: 2
created: "2026-09-08"
updated: "2026-09-08"
depends_on:
    - "0263"
    - "0271"
spec_refs:
    - spec.md#Constraints / conventions
---

## Description

Accepted hardware-validation scope split from 0263; source implementation alone does not authorize production adoption. After the coordinated protocol-v2 gate 0271, compare parent baseline and 0263 candidate on lab SPDK fleet with matched workload/config, record exact binaries and effective transport settings. Measure whole-arm write/read throughput, hot-path CPU profiles, and p95/p99 latency; value-verify with kvchurn. Include normal-load rejection/timeout counter checks and a targeted abusive-client arm demonstrating bounded credits and healthy-client progress. Preserve existing data unless the applicable runbook and owner authorization allow replacement. Record evidence and adoption verdict; investigate material regressions before recommending the finite-default candidate. Do not call local tests or unbounded-mode performance a pass for bounded-mode adoption.

## Acceptance criteria

- Specifically profile the new response temporary allocation/copy, output buffer release/reallocation, and shared atomic accounting against the parent baseline; measure their write-throughput and tail-latency impact before adoption.

## Outcome

Accepted finite-default server transport adoption on reachable RF=2 SPDK hardware: three matched fresh-layout pairs, exact parent/candidate builds and fixed v2 client, nine write/read samples per side, twelve CPU profiles covering response/output/accounting paths, six full-value kvchurn passes, zero normal transport rejection/timeouts, and abusive-load rejections with healthy progress. Write/read median throughput changed -1.08%/+1.61%; pooled write p95/p99 +4.31%/+3.39%, with noisy-pair and AER limitations explicit. Corrected attempt/success counts and probe limitations; independent review accepted twice. Analyzer deterministic, Python compile and authored-file whitespace checks passed; preserved raw perf whitespace exempt. Lab-1 unavailable; lab-2/3 cleanly stopped, final v2 parent corpus and cache/cores/binaries preserved.

Commit: Record reviewed finite server transport-budget SPDK gate
