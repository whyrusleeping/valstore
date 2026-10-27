---
id: "0292"
title: Bound rehome copy impact on foreground write throughput
status: done
priority: 3
created: "2026-09-10"
updated: "2026-09-11"
depends_on:
    - "0262"
spec_refs:
    - docs/algorithms.md#compatibility-and-rollout
---

## Description

Task 0262's final source-pinned asynchronous RF2 SPDK candidate passed readiness, correctness, outage abort, persisted-PD resume, exact data, prediction/actual, capacity, and convergence checks, but a clean non-fault 30-second sample during a real 15,000-winner move sustained only 65.6% of matched control write throughput. This independently reproduced the rejected candidate's earlier 67.2% result, so the measured movement regression and this remediation task remain valid. The executor remains default-off.

## Acceptance criteria

- Attribute the clean moving-arm foreground loss between mirror traffic, copy reads/writes, shard scheduling, and maintenance using bounded counters/profiles rather than the deliberately faulted PD-restart sample.
- Implement the smallest explicit copy/mirror admission or scheduling bound that preserves rehome forward progress without starving foreground work; retain reason-attributed abort and restart-resume semantics.
- Rerun a source-pinned matched RF2 SPDK control/moving pair with nonvacuous copy and mirror counters, write throughput and server/client tails, exact values, capacity headroom, and safe teardown.
- Do not enable the balance executor by default unless the moving pair removes the material foreground regression and all task 0262 correctness/convergence results remain valid.

## Outcome

Confirmed implementation 4918b522 and both prior comprehensive acceptances. Post-result non-Rejected-only mirroring, epoch fencing, and four-destination bound accepted. Corrected RF2 SPDK evidence assertions pass: 212,734 moving writes/s (75.24% control), 640,374 mirrors/19,200 copy entries, exact 600k values, outage abort, persisted-PD resume, convergence, capacity and safe teardown. Executor remains default-off for residual loss. Fresh tests: 63/63 server tests pass serially; parallel probe-counter failure is tracked by 0286 and passes isolated. Evidence: bench/0292-rehome-admission-hardware-20260911/{FINDINGS.md,corrected/}. Unrelated 0279/0287 diffs hash-verified unchanged; no hardware rerun. Stale prepared finalization journal had no commit/SHA; preserved at .git/ycc/task-finalizations/0292.prepared-original-baseline.backup.json before normal helper retry.

Commit: Finalize accepted bounded rehome scheduling gate
