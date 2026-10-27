---
id: "0255"
title: Hardware A/B gate for PD range-map delivery liveness (0252) under rehome ingest
status: done
priority: 2
created: "2026-09-05"
updated: "2026-09-07"
depends_on:
    - "0252"
spec_refs: []
---

## Description

Split hardware acceptance scope from 0252. Run the same-workload hardware A/B for PD delivery liveness only after allocator-control and crashed-node confounds from 0248/0233 are cleared. Do not disrupt ongoing fleet experiments to obtain this gate.

Acceptance: record builds/configs and actual driver-rate slopes for both arms; compare per-endpoint preflight/push attempts and failures, confirmation ages, delivery stalls versus ReportGap/CopyStall aborts and completed rehomes. Demonstrate healthy endpoint delivery and slow endpoint recovery without weakened wire capability or rehome epoch fences. If confounds remain, document them and leave the gate open; no hardware-pass claim from local tests. Coordinate with 0233 and evidence in bench/0248-takeover-20260905.

## Outcome

Passed scoped hardware A/B on matched fresh three-node SPDK arms differing only in 0252. Slow-window ingest was 71,952 vs 72,387 events/s. Control globally starved map delivery and ReportGap-aborted; candidate delivered to 11 healthy endpoints, attributed 31 blocked preflights, gated deadlines on required delivery, recovered and committed 4.367s after successful preflight with zero aborts. V4/epoch fences preserved. Recorded reproducible evidence in bench/0255-hardware-20260907; independent standard review accepted, analyzer reproduced, Python syntax and helper check passed, implementer reported 64 PD tests passing. Fleet stopped, format=false, fault removed, cache/cores preserved. All post-evidence node drains required 300s SIGKILL; retained data needs crash recovery. Does not pass or resolve 0233/0260 populated mirror-latency gate.

Commit: bench: pass PD delivery-liveness hardware A/B gate
