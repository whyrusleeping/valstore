---
id: "0284"
title: Decide and implement layout-independent current-owner balance accounting
status: done
priority: 3
created: "2026-09-08"
updated: "2026-09-10"
depends_on:
    - "0261"
spec_refs:
    - docs/zones.md
---

## Description

Task 0261 reproduced equal logical corpora with different V2 estimates solely from physical flush layout; its disjoint-run extrema fix does not resolve the current-owner coherence blocker for movement gate 0262. See bench/0261-estimate-diagnosis-20260908/FINDINGS.md. Decide whether balance weights mean physical resident entries or logical winners before changing safety semantics. A concrete proposal is map-epoch-scoped exact winner counts, cooperatively seeded on open and maintained at exact winner transitions, with explicit split/rehome invalidation or transfer. Evaluate whether persistence is necessary rather than assuming it.

Acceptance: record the chosen semantics and lifecycle/migration design; implement bounded coherent accounting and receipts without unbounded serving scans; preserve complete-poll/current-owner/topology/epoch validation and default-off execution; test equal-data replicas with different flush, overwrite and compaction histories, tombstones, reopen, splits and rehomes; measure any added write-path cost per project policy. Demonstrate that the 0261 reproduction no longer blocks quiescent equal current owners before running 0262. Do not merely add estimate tolerance or substitute a maximum.

## Outcome

Implemented volatile, cooperatively seeded exact logical winner accounting with V3 epoch/readiness receipts; retained tombstones count until retirement, unknown-old/dirty accounting fails closed, and all existing planner fences/default-off execution remain. Layout/history, tombstone, reopen, split/rehome, receipt/fallback and wire regressions pass; task-0261 equal-owner reproduction now reaches planner inputs. Full workspace build/tests passed; both independent reviewers accepted after stale unsorted readiness correction. Local write A/B +30 ns/+1.35%; mandatory SPDK adoption scope split into accepted todo 0290 and added to 0262 prerequisites. Preserve and exclude unrelated pre-existing 0287 changes from this scoped commit.

Commit: Use epoch-scoped logical winner counts for balance planning
