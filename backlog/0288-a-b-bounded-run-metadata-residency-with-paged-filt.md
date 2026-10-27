---
id: "0288"
title: A/B bounded run-metadata residency with paged filters and hierarchical fences
status: done
priority: 4
created: "2026-09-09"
updated: "2026-10-14"
depends_on:
    - "0285"
spec_refs: []
---

## Description
Task 0285's local baseline (bench/0285-capacity-local/report.md) finds current resident filters+flat fences require ~93.7 GiB/node at 100B/four-node RF1; filters alone exceed 32 GiB. Retained in the 2026-09-29 triage as a bounded capacity experiment, blocked until the owner selects this deployment target as a current requirement; not an adopted storage rewrite: compare current run lookup with jointly paged/partitioned filters and hierarchical sparse fences under a fixed metadata cache. Preserve ordered scans and exact version/tombstone behavior; keep sidecar semantics and unrelated value layout unchanged. No fleet operation or format migration is authorized.

Acceptance on promotion: baseline/candidate within inclusive 8-core/32GiB and 4-core/16GiB budgets; measure residency/allocation, cold positive and in-range/out-of-range negative probes, fresh-insert absence checks, and scans with continuation/version/tombstone verification. Report offered/completed/harness-dropped/server-rejected load, arrival and service tails, CPU and physical I/O. Include recurring flush/merge history and stable debt rather than only a two-run quiescent corpus; account global reservations and duplicated maintenance metadata. Treat local tests as mechanism evidence, not full-scale/enterprise-NVMe proof. Decide measured win/loss and next gate before adopting any protocol/persistence change; bounded exact AE seeding remains an independent prerequisite for low-RAM restart.

## Acceptance criteria

## Work log

- 2026-10-14: declined by owner. The 100B-key/4-node budget in 8-core/32 GiB and 4-core/16 GiB boxes is not a current target; the priority is performance within the RAM we already manage. Closed without work; reopen only if a small-RAM deployment target is selected.
