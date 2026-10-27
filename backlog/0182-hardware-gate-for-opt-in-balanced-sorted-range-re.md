---
id: "0182"
title: Hardware gate for opt-in balanced sorted-range re-home
status: done
priority: 4
created: "2026-08-11"
updated: "2026-09-07"
depends_on:
    - "0181"
spec_refs:
    - docs/algorithms.md#compatibility-and-rollout
    - docs/zones.md
---

## Description

Run the destructive hardware-validation phase split from task 0181 once the lab fleet is explicitly reserved. Use task 0181's default-off executor; do not reconsider the default before this task is complete.

## Acceptance criteria

- A reproducible fresh-deploy A/B on lab (or a documented equivalent hardware fleet) uses an uneven RF=2 sorted workload, an otherwise-identical control arm, and an opt-in balanced-execution arm.
- Artifacts record source/build/config/commands and time series or samples for `pd_node_load_skew_pct`, planner predicted versus actual per-node and per-engine owned entries, engine capacity headroom, write throughput/tails, copy volume, and reason-attributed aborts.
- The run checks exact consistency through a genuine node outage and PD restart, including resumed/aborted move behavior and convergence without oscillation under high/low hysteresis.
- Results demonstrate material skew/capacity improvement without a per-write regression, or clearly identify the blocker and retain default-off.
- A self-contained `bench/lab-balance-*/FINDINGS.md` and canonical docs record the evidence and explicit default-on/default-off decision. Hash/default zones and `vnodes` behavior remain unchanged.

## Outcome

Reviewed and accepted source-pinned fresh lab control/opt-in hardware attempt (artifacts commit 773ce76a). Planner blocked all movement on persistent V2 current-owner estimate disagreement; default-off retained, no balance pass claimed. Baseline exhaustive 2.4M-key consistency checks passed through node outage and PD restart; blocked-executor matched write samples within 2.4%. Analysis independently reproduced; reviewer accepted; diff checks clean. Proposed 0261 tracks diagnosis and dependent 0262 preserves unrun movement/recovery/convergence gate. Fleet stopped with format=false and forensic evidence preserved.

Commit: Record reviewed balance gate blocker and deferred validation
