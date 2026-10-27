---
id: "0238"
title: Scale and bound PD state storage for production range counts
status: done
priority: 1
created: "2026-08-23"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R3 — The daemon PD state machine is a fixed 64 MiB append-only engine, while range maps are rewritten whole (P0 for the stated scale target)
---

## Description

Replace whole-range-map rewrites with incremental/per-range state or another O(1)-update representation; remove the fixed unmaintained 64 MiB PD state-engine ceiling; expose capacity metrics and fail safely before exhaustion. Validate with a thousands-of-splits test sized from the 24B-record target.

## Outcome

Replaced daemon fixed 64 MiB append-only state with configurable bounded live-key materialization backed by durable Raft snapshots/log; atomic conditional per-range updates, legacy migration, conservative proposal reservations, and capacity metrics. Verified 24,000-range state, 2,000 durable sequential splits with <=256-byte/two-write steady-state commands, snapshot+tail recovery, capacity rejection, legacy migration, and follower apply. vals-raft, vals-cluster, vals-server suites and workspace check passed; two comprehensive reviewers accepted initial and revised changes. Coordinated PD upgrade/no downgrade documented. Optional performance measurement captured as proposed 0273; no hardware changed.

Commit: Scale PD state with bounded materialization and incremental range records
