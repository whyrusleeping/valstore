---
id: "0287"
title: Review finite-client no-go evidence and reconcile adoption status
status: done
priority: 2
created: "2026-09-09"
updated: "2026-09-29"
depends_on:
    - "0279"
spec_refs: []
---

## Description

### Current scope — evidence disposition, not another hardware run

The gate already ran and returned **no-go** in `bench/0287-hardware-20260909/FINDINGS.md`. Review the retained evidence/probe changes, record the completed safety checks and unmet hedge-loser criterion, and reconcile the performance/adoption result with today's finite-default client and documentation. Do not silently relabel it a pass, remove resource bounds, or rerun the historical matrix. Any new optimization/hardware gate needs a concrete current question and the current fleet/retention prerequisites.

Acceptance: an explicit reviewed disposition (including no-go/incomplete where applicable), accurate current supported-default claims, and narrowly identified residual work only if still relevant. The old session/staging statements below are historical, not a description of the current index or a live tooling blocker.

## Outcome

0287 no-go stands. SPDK RF2 gate validated finite-client faults, credit/cancel release, response-pressure isolation, sloppy fallback/heal, full-value correctness; not performance-adopted (width-32 Quorum -6.78%, Digest -4.59% median, all pairs negative); hardware hedge-loser criterion unmet (local tests cover hedge and cancellation only separately). Finite NetTransport defaults remain the Rust/daemon-peer default as resource bounds; unbounded is an explicit benchmark opt-out; Go clients unaffected. Updated FINDINGS.md disposition, ARCHITECTURE.md, 0279 README, and marked plans/client-budget-hardware-gate.md historical. The stale pointer in docs/backpressure.md (dirty with owner's 0338 work) is tracked as a follow-up task. No hardware rerun scheduled.

Commit: docs(0287): record reviewed no-go disposition of finite-client SPDK gate — finite defaults kept as resource bounds, not perf-adopted (Quorum -6.78%, Digest -4.59%); hedge-loser unproven
