---
id: "0511"
title: Qualify backup and restore on a three-PD SPDK deployment
status: done
priority: 1
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs:
    - docs/backup-restore.md#Limits and evidence
    - docs/deploy.md#PD availability and recovery
---

## Description

Accepted follow-up to adopter review 0507, finding 5 and priority 4. Establish demonstrated recoverability, separately from throughput/ingest qualification.

Acceptance criteria:
- Run a bounded representative backup/verify/restore-pd/load/compare drill using three PDs on independent hosts and SPDK data nodes, with separately fenced source and fresh targets (or safe sequential reuse after verified backup and retained source evidence).
- Include overwrites, point deletes, range deletes, multiple zones and source-authority fencing; validate every restored logical record and deletion, then verify restart behavior.
- Record exact revision/configuration, dataset size, backup/load/compare throughput, serving latency impact, peak space/RAM, wall-clock recovery time, observed RPO and capacity/spare-storage prerequisites.
- Exercise interrupted load/rerun and failure reporting with bounded local checks first; ensure differing multi-PD restore seeds cannot silently be accepted by the documented procedure.
- Preserve bench/RETENTION.md holds and coordinate ownership before destructive hardware work. Prefer the shortest drill that qualifies the behavior, not another multi-day ingest soak.
- Update public evidence boundaries precisely; do not conflate valsd ingest soak with backup qualification. Known stale-former-owner tombstone-GC exposure is tracked separately and must be disclosed.

## Outcome

Qualified backup/restore once on lab-1/2/3 (3 voting PDs, 3 SPDK nodes, gc_grace 0, drives reused in sequence). - Data: 4.85M records (2.27 GB). - create took 310 s; load 94.6 s, SIGKILLed once and rerun; compare 203.5 s; recovery 341 s. - Every key verified, the old PD group fenced, and a restart re-verified. - Read p99 under the probe rose from 4.2 to 6.5 ms. Code: - Restored PD voters confirm one restore-seed digest through Raft (keys W/A, `restore-seed` file). load and compare refuse divergent seeds; a 3-PD integration test covers this. - Backup and compare retry a replica's Err reply 3 times; lab-2's CRC refusals are proposed as 0519. Docs: - backup-restore.md holds the evidence, capacity figures and the gc_grace>0 limit (0517). - formats.md documents the new keys and file.

Commit: 0511: qualify backup/restore on 3 PDs + 3 SPDK nodes; restored PD voters confirm one restore seed; backup reads retry replica errors
