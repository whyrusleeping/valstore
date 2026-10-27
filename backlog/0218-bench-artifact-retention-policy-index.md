---
id: "0218"
title: Bench artifact retention policy + index
status: done
priority: 4
created: "2026-08-16"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

bench/ holds 427 MiB tracked (50 dirs) + ~12 GiB untracked local logs (two byte-identical 3.9 GiB bootes logs, one 4.1 GiB). Proposal from survey: committed benchmark index (task | dir | status | summary | commit | hardware | raw URI | SHA-256 | retention); keep FINDINGS/summaries/commands/patches, externally archive raw metrics/perf/logs; largest tracked pruning targets: bootes-soak-lab 168 MiB, task-0186 54 MiB, 0208/0210/0184/0201/replay-lab 17–21 MiB each; merge task-0175-blind-append README into lab-bench-20260811-0175; delete local duplicate untracked logs after external retention confirmed.

## Outcome

Completed benchmark retention policy and index covering all 77 collections, with reproducible historical archive-stream SHA-256s, explicit provenance/unknown archive locations, and fail-closed archive/restore/deletion rules. Consolidated 0175 reproduction notes with compatibility link. Independent review verified coverage, all checksums, links and raw preservation; follow-up review accepted superseded 0232 gate labeling and incident hold. No raw evidence removed; external archival/pruning split to 0258. Hardware gates 0255–0257 blocked pending authorized coordinated hardware window after read-only fleet check found node2 inactive and diagnostic arms still active. Live metrics capture excluded from commit.

Commit: docs: finalize reviewed benchmark retention and incident holds
