---
id: "0262"
title: Rerun balanced rehome movement hardware gate after estimate-coherence resolution
status: done
priority: 4
created: "2026-09-07"
updated: "2026-09-11"
depends_on:
    - "0182"
    - "0261"
    - "0284"
    - "0290"
    - "0291"
spec_refs:
    - docs/zones.md
    - docs/algorithms.md#compatibility-and-rollout
---

## Description

Residual unrun validation split from 0182, whose reproducible hardware attempt identified a fail-closed V2 estimate-coherence blocker and retained default-off. Baseline outage/restart consistency passed but no pending move existed. Starting evidence/procedures: bench/lab-balance-20260907-0182/FINDINGS.md, run.py and deploy.sh. Do not claim these baseline checks validate movement.

Acceptance: after resolving the blocker, confirm fleet reservation/idleness and preserve forensic evidence/cache; rebuild source-pinned matched fresh uneven RF=2 control/opt-in arms; compare planner predictions with actual node/engine ownership and capacity headroom, write throughput/tails, copy and reason-attributed aborts. Exercise genuine node outage and persisted-state PD restart during accepted moves, exact consistency, abort old-token authority and restart resume/cleanup, and convergence without high/low hysteresis oscillation. Document material skew/capacity improvement without per-write regression or a new concrete blocker, retaining default-off unless all gates pass. Publish self-contained findings and update canonical docs. Hash/default zones and vnodes remain unchanged; record fleet-health confounds and safe teardown.

## Outcome

Reviewed asynchronous accounting recovery keeps V3 polls free of seed I/O and progresses independently of an occupied sweep without synchronous fallback. Final source-pinned three-node RF2 SPDK gate exercised real outage/attributed abort, old-token authority, online recovery, pending-PD restart/resume/commit, exact 600k values, prediction/actual ownership, capacity, and stable hysteresis. Node/engine skew improved 120%/230% to 100.63%/162.5%. Gate decision: retain default-off; active-move writes were 65.6% of control versus 99.0% steady, with explicit accepted remediation task 0292. Evidence: bench/0262-balanced-rehome-hardware-20260910-final-async. Independent debug vals-cluster suite passed (619 unit tests, 5 ignored, integration/docs passed); final reviewer accepted. Fleet cleanly stopped, forensics/cache preserved. Unrelated 0279/0287 index/worktree state hash-verified unchanged and excluded.

Commit: Repair online logical accounting and complete SPDK movement gate
