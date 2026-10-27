---
id: "0489"
title: '0484 serving foundation: descending scan pages with generation guards and reverse reply coverage'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on:
    - "0488"
spec_refs: []
---

## Description

Necessary implementation slice of accepted 0484. Add an internal reverse serving entry point backed by staged reverse engine pages, storage-generation checked async reads/refills/restarts, bounded retry synchronous fallback, concurrent mutable winner/RT finalization, keys-only mode, and descending RT fragment clipping/reply-budget resumes. Keep existing ascending/write call paths unchanged. Acceptance: serving tests compare reverse pages with ascending winners under mixed runs/memtable, limits and tiny reply/covered budgets (including empty covered pages); force storage changes at refill/finalization and fallback; concurrent overwrite/tombstone/RT tests; reverse budget boundary regression with kill-the-fix. Run relevant cluster/store tests and independent review. Document internal API/coverage contract. No reverse RPC/client exposure in this slice: parent 0484 retains negotiated wire/client/coordinator integration, multi-replica split/re-home/min-coverage tests, natural timestamp benchmark, performance gates and end-to-end docs.

## Outcome

Completed the necessary internal-serving slice of 0484: staged reverse selection with generation-guarded async reads/refills, stale-error discard, sync fallback, mutable-winner finalization, keys-only replies, reverse RT clipping and budget resumes. Ten serving regressions; full cluster suite 964 passed/7 ignored, store suite 559 passed/25 ignored plus 11 integrations, Clippy succeeds with warnings. Two mutations fail as intended. Independent review found and verified the fix for empty fragments at prefix cuts, then accepted. Ascending/write method bodies and unrelated dirty work preserved. 0484 stays in_progress: no reverse RPC/client yet; negotiated wire/client/coordinator, multi-replica correctness and benchmark/performance gates remain.

Commit: feat(cluster): add internal descending scan serving with reverse coverage
