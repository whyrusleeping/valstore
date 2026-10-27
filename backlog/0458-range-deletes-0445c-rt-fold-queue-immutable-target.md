---
id: "0458"
title: 'Range deletes 0445c: RT fold — queue, immutable target T, bounded accounting turns, covered memtable removal, acct(k) with cursor, Busy/accounting-not-ready while pending, max trees on first fold'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0457"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0445 (docs/range-deletes.md §4 "The accounted set and the RT fold", §6). vals-cluster replica layer + accounting task.

## Outcome

RT fold runs on the shard accounting task: queued RTs fold into an immutable T over its delta spans in bounded, A-skipping pages (sync or prefetched), retiring T-covered winners from leaves and counts, removing covered memtable entries and building max trees; completion sets A := T, S_A := S_T. acct(k) = T below the cursor. Seeds absorb folds; collection recomputes T. While pending, tree/digest requests are Busy, dirty rebuilds never publish, RangeStatsV3 is not ready. Tests: overlapping RTs with random writes equal a fresh seed (fails if targeting F), crash/reopen, Busy, skip, collection, rebuild/digest races; ~35 kill-the-fix mutations caught. vals-cluster 891 and vals-store 537 tests green; two reviewers accepted. Follow-ups noted in 0446, 0449.

Commit: feat(0458): range-tombstone fold — queued RTs, immutable target T, bounded A-skipping fold pages on the accounting task, acct(k) by cursor, covered memtable removal, max trees at fold start, Busy/not-ready while pending
