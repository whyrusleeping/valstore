---
id: "0279"
title: Prepare finite Rust client budget hardware-gate benchmark and probes
status: done
priority: 2
created: "2026-09-08"
updated: "2026-09-09"
depends_on:
    - "0264"
    - "0271"
    - "0278"
spec_refs: []
---

## Description

Software prerequisite split from the original hardware-gate scope after the benchmark/probe work proved substantial enough for independent review. `bench/0264-client-budgets-20260908/FINDINGS.md` remains preliminary localhost evidence only. This task prepares trustworthy finite-versus-unbounded benchmark selection, receipts, latency/fanout instrumentation, and executable fault/scan probes without touching lab or claiming production adoption.

The original hardware acceptance requirements are transferred unchanged to dependent task 0287: matched alternating three-node RF2 SPDK arms after 0271; whole-arm read/scan/write throughput, CPU/perf and latency tails; concurrent/batched logical and transport admission; kvchurn full-value verification; stalled-destination release with disjoint healthy progress, cancellation and response pressure; complete provenance/artifacts/adoption decision; forensic retention and documented safe postflight. Completing 0279 is only readiness to execute that gate.

Acceptance:
- `vals-bench netload` consistently selects production finite defaults or a true explicit-unbounded mode for every worker and verification coordinator, and records actual mode, effective limits, raw latency samples, fanout, logical/transport terminal accounting, connection peaks and reconnects.
- The artifact-local Rust probe provides explicit finite-default/unbounded clean scan selection with nonvacuous exact verification, read fanout/hedge/scalar fallback coverage, and finite-only destination admission, partial/wholly-unsent cancellation, timeout-derived logical failure, real incoming response-byte pressure with same-pool healthy progress, and sloppy fallback/heal checks.
- Fault probes assert expected local classifications, deadline evidence, terminal logical/transport operations and waiters, retained-byte baseline, reconnects, and forbidden hint/FD state. Probe configuration defaults remain the actual production defaults unless explicitly overridden.
- Focused library/probe checks and controlled local TCP or file-backed executions exercise critical new paths. Retain concise receipts and clearly distinguish local correctness evidence from task 0287's hardware-only adoption evidence. No hardware mutation is permitted.

## Outcome

Completed reviewed software prerequisite: true finite/unbounded netload and clean scan selection, fanout/raw latency/credit/reconnect receipts, executable admission/cancellation/response-pressure/fallback probes, and corrected batched All fanout with RF3/R2 regression. Independent review accepted after closing false-assurance paths; coordinator reran 5 probe and 9 netload tests plus diff check; implementer passed 149 client tests and local TCP/file-backed checks. All original SPDK measurements/faults/adoption requirements preserved in accepted dependent task 0287. No hardware mutations or hardware adoption claim.

Commit: feat(bench): prepare finite-client budget gate tooling
