---
id: "0488"
title: '0484 staged engine foundation: asynchronous descending winner pages and covered-work resumes'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on:
    - "0487"
spec_refs: []
---

## Description

Necessary accepted implementation slice of 0484 following committed 0487. Add borrow-free staged descending winner selection using bounded reverse run-block locator windows, reverse memtable refills, async reads and existing LWW/RT visibility. Reuse StagedScanDone/value span planning and StagedScanStep, expose captured storage/mutable generations for serving callers. Covered-work budgets must produce strictly progressing exclusive hi=resume, including empty covered pages; preserve conservative RT block skipping and read attribution. Existing ascending/write method bodies stay unchanged.

Acceptance: staged pages equal synchronous reverse/reversed ascending winners across mixed runs/memtable, bounds and entry/byte budgets; force block-window and memtable refills; validate RT filtered-work and block budgets, empty progress pages, full-cover skip and same-HLC survivors; generation capture and corruption propagation; value span order/materialization and scan meter checks. Existing vals-store suite passes, substantive pagination regression kill-the-fix, independent review. Document engine API and caller generation/resume contract. This does not complete parent 0484: serving restarts/fallback, wire/client/coordinator, all cluster tests, natural timestamp benchmark/performance validation and end-to-end docs remain in that task.

## Outcome

Completed necessary engine-only slice of accepted 0484: bounded reverse locator windows, borrow-free asynchronous descending LWW/RT merge, memtable refills, exclusive covered-work resume keys, captured generations, value-span compatibility and opt-in read attribution. Existing ascending/write method bodies and unrelated work preserved. Seven new regressions plus extended synchronous property matrix; 559 unit and 11 integration tests pass (25 ignored). Resume key+0 mutation fails as intended. Clippy succeeds with existing warnings and an acknowledged inline enum-size advisory. Independent reviewer accepted and independently reran tests, Clippy and mutation check. Parent 0484 remains in_progress; serving/wire/client/coordinator integration, cluster tests and benchmark/performance gates remain explicitly outstanding.

Commit: feat(store): stage asynchronous descending winner scans
