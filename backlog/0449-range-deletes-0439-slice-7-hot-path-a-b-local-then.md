---
id: "0449"
title: 'Range deletes (0439) slice 7: hot-path A/B (local then box), docs, Go client follow-up task'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-15"
depends_on:
    - "0448"
spec_refs:
    - docs/range-deletes.md
---

## Description

Implements the docs/range-deletes.md validation plan's performance gates and the doc/rollout follow-through.

## Outcome

Zero-RT hot-path neutrality confirmed against pre-feature df5303e6 (bench/0449-rt-hot-path/FINDINGS.md, preregistered rule): lab-1 17k-range write ABBA +0.23% shard µs/write (idle-subtracted 0.0%), devbox point reads +0.75%, scans −1.1%, local writes inside shared-box noise; vals-bench capacity shows fence +8 B per block exactly and no max tree; HEAD arms report vals_rt_max_tree_built_blocks_total 0. Head-of-queue tallies recorded (counters test now prints them). Docs: range-deletes.md marked built (as-built metrics: no fragments gauge, no fold_pending gauge), algorithms.md §5 accounted set/fold and §6 replica skip, zones.md API bullet, ARCHITECTURE ✅, deploy.md Range deletes section (rollout, fail-closed downgrade, v5 rollback window refuses RTs, backups/dumps, fragment cap), backup-restore.md RT handling + VREC v2 compatibility, getting-started.md API + cumulative-delete advice. Harnesses save final /metrics per node. Go/valsdump follow-up created as 0468.

Commit: docs+bench(0449): range-delete zero-RT hot-path A/B passes (box write +0.23%, reads +0.75%, scans −1.1%, fence +8 B/block); head-of-queue numbers; range deletes marked built in docs; Go follow-up 0468
