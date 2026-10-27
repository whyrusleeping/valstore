---
id: "0441"
title: 'Backup slice 1: `valsd backup create` (authoritative PD-state capture + replica-merged logical export of every zone) and `valsd backup verify`'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R11 — A supported backup is described, but no supported restore ships (P1)
---

## Description

First bounded slice of 0268 (restore/DR drill), split out so the backup format and capture can be reviewed before restore is built on them. 0268 keeps the restore half (PD seeding with a new incarnation + lineage, data load at original versions, compare-against-cluster, end-to-end drill).

Contract decided in 0268's work log (2026-10-14): restore replaces the incarnation; this slice only captures.

## Acceptance criteria

- A test cluster with sorted and unsorted zones, overwrites, deletes and split ranges backs up to files whose merged records exactly equal the expected logical state (keys, versions, values, tombstones), with each key appearing once even though it is replicated.
- `export-state` refuses non-authoritative capture.
- Placement changes during the backup make it fail.
- Verify catches a flipped byte, a truncated file, a missing file and a manifest/count mismatch.
- The workspace builds; the vals-cluster and vals-server suites pass.

## Outcome

Shipped `valsd backup create` and `valsd backup verify`. - `create`: captures PD state with the new admin `export-state`, which only a confirmed leader answers. It then streams every replica shard of zone 0 and every catalog zone, keeps the LWW winner per key (tombstones included), and fails if replicas disagree at an identical version. - Output is a versioned directory: MANIFEST (written last), pd-state, and zone-<id>.vrec files with CRCs. A placement change during the run removes it. - `verify`: checks every file offline. - Docs: new docs/backup-restore.md, plus formats.md §3.7. - Tests: a 3-node integration test exactly matches the logical state (kill-the-fix checked). Unit tests cover merge conflicts, format strictness, verify damage cases and capture refusal. The vals-cluster and vals-server suites pass. - Not run on SPDK hardware. Restore stays in 0268.

Commit: feat(0441): valsd backup create|verify — confirmed-leader PD-state capture (admin export-state) + replica-merged versioned logical export of every zone, discarded on placement change
