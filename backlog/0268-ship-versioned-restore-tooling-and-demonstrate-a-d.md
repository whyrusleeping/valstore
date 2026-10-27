---
id: "0268"
title: Ship versioned restore tooling and demonstrate a disaster-recovery drill
status: done
priority: 3
created: "2026-09-07"
updated: "2026-10-14"
depends_on:
    - "0283"
spec_refs:
    - docs/production-readiness-review-2026-08.md#R11 — A supported backup is described, but no supported restore ships (P1)
---

## Description

Accepted operational recovery work (2026-09-29), separate from release packaging. Blocked on one explicit recovery/authority contract shared with 0283: preserve versus replace cluster incarnation and how obsolete authority is fenced. Decide that once before either implementation; do not add a circular implementation dependency. After the decision, implement logical dump/verify, PD-state capture and a fenced-target restore drill as successive bounded slices, not independent incompatible designs. Define a supported backup/restore contract and versioned tool independent of unversioned sibling-repo paths. Address catalogs/range maps, zones, versions, tombstones, replica duplicates, consistency points, and validation. Prove end-to-end restore into a separate target with exact logical-data verification and documented limitations; include PD-state loss/recovery decisions from 0112. A task acceptance does not authorize destructive fleet operations, publication, or a production-readiness claim. Coordinate formats with 0236/0238 before freezing the restore format.

## Outcome

Restore half shipped on 0441's backup. `backup restore-pd` seeds an empty PD data_dir with one Raft snapshot at the backup's index/term (ids/weights/ring/boot zones must match; target addresses; balance lock and pending re-homes cleared; new incarnation; lineage key L; body digest; multi-voter needs a shared --incarnation). `backup load` checks lineage, waits for nodes to adopt, writes every record at its original version to every replica (≤256 rec/8 MiB batches, sync), fails on newer data, idempotent. `backup compare` re-exports and merge-joins exactly (collected tombstones allowed). Unit tests plus a 3-node drill: backup, stop, restore to new dirs/ports, load ×2, compare, client reads, restarted old PD refused, tamper detected. Docs updated (backup-restore runbook, deploy case 4, formats, pd-admin). L0 only: no SPDK/three-PD/hardware drill; throughput unmeasured.

Commit: feat(0268): restore a backup into a new fenced cluster — valsd backup restore-pd / load / compare, lineage key L, new incarnation; restore drill test incl. old-PD fencing
