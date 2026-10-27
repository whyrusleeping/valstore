---
id: "0291"
title: Fix logical accounting forward progress after rehome abort under writes
status: done
priority: 2
created: "2026-09-10"
updated: "2026-09-10"
depends_on:
    - "0284"
    - "0290"
spec_refs:
    - docs/zones.md
---

## Description

Task 0262's source-pinned three-node SPDK movement gate reached a coherent V3 planner input and an accepted balance re-home, then killed the entering pending owner while writes continued. The move correctly aborted for mirror failure and retained old-token authority, but one healthy endpoint (`n3s3`) remained `accounting_ready=false` at the current map/accounting epoch for the full 1,200-second wait. The PD correctly failed closed with all 12 ranges blocked. A full all-node reopen restored readiness and exact data, so this is online accounting forward-progress rather than the old V2 estimate disagreement or demonstrated data loss. Evidence: `bench/0262-balanced-rehome-hardware-20260910/FINDINGS.md` and `execute/state/post-abort-stuck-summary.txt`.

Reproduce the pending-map abort plus concurrent unknown-old/write transition deterministically without hardware, including the interaction between map invalidation, a published or in-cleanup cooperative seed, and subsequent scheduling. Add observability sufficient to distinguish seed cleanup, projection, journal replay, RAM shed and dirty-accounting states; identify the exact missing forward-progress edge rather than adding a timeout or weakening V3 readiness. Implement a bounded fix and regressions proving that every endpoint reaches exact current-epoch logical counts after prepare/abort under writes and that planner fences remain closed until it does. Re-run task 0262's residual successful movement, persisted-PD restart, throughput/tails and hysteresis hardware criteria afterward; do not turn the executor default on from this software fix alone.

## Outcome

Completed software prerequisite split from 0262: unready V3 polls now coalesce an AE sweep request without weakening readiness/epoch fences or performing a serving scan. Added bounded seed-projection/reseed observability and deterministic abort/cleanup/write/exact-count scheduling regressions. Both independent reviewers accepted; coordinator independently passed full debug vals-cluster suite (615 unit tests, 5 ignored, integration/doc suites passed), focused tests, 0262 evidence checker and diff checks. Retained source-pinned 0262 hardware evidence documents real copy/mirror, mirror-failure abort, old-token authority and exact 600k values plus pre-fix readiness stall. 0262 remains todo for successful movement, pending-PD restart, moving performance and hysteresis on repaired source; executor default-off, fleet stopped, cache/forensics preserved. Scope excludes all pre-existing 0279/0287 staged and unstaged changes.

Commit: Reschedule exact logical accounting after unready V3 polls
