---
id: "0290"
title: Hardware-gate logical range balance accounting
status: done
priority: 2
created: "2026-09-10"
updated: "2026-09-10"
depends_on:
    - "0284"
spec_refs:
    - docs/zones.md
---

## Description

Task 0284 replaces layout-dependent sorted-range balance weights with volatile epoch-scoped logical-winner counts and touches the replicated apply and tombstone-retirement paths. Project SPDK adoption policy requires a separate hardware gate; task 0284 intentionally made no fleet changes and its local release A/B is not adoption evidence.

Required adoption validation split from accepted task 0284 (not speculative follow-on scope).

Acceptance: run matched baseline/candidate write-throughput and latency arms on the three-node SPDK cluster with CPU profiles and full value/version/tombstone verification. Exercise fresh load, overwrite churn, compaction, clean and crash reopen, split, and confirmed re-home while checking V3 readiness/epoch receipts, exact current-owner agreement, and fail-closed planner behavior during every reseed window. Reproduce physically different replica layouts without logical divergence and show the old estimates disagree while logical counts converge. Keep `VALS_PD_BALANCE_EXECUTE` off until the accounting gate passes; do not combine this gate with unrelated task 0287 acceptance.

## Outcome

Passed source-pinned fd1932a6/af028dfc SPDK accounting adoption gate: three primary write pairs median -1.93%, matched scalar-apply latency +0.33%, CPU profiles, V2 divergence in all ten ranges with exact V3 convergence, clean/crash reopen, confirmed rehome and populated split with fail-closed readiness/epoch receipts. Full-value integrity scans and independent kvchurn version/CAS/tombstone checks passed; strengthened evidence checker and both reviewers accept. No product-code changes. Fleet cleanly stopped on candidate protocol-v2 corpus with balance execution off; cache/forensics preserved. Scope only backlog/0290 and bench/0290-logical-balance-hardware-20260910; preserve and exclude all pre-existing staged/unstaged 0287 work. Task 0262 is now dependency-ready.

Commit: Validate logical range accounting on three-node SPDK hardware
