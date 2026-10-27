---
id: "0465"
title: 'Range deletes 0447d: measure the AE Busy fraction under continuous small range deletes; inline-small-fold mitigation if material'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0464"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0447 (docs/range-deletes.md "Risks to measure": AE availability under continuous deletes).

## Outcome

Measured with `vals-bench rtqueue` (3 local nodes, RF 3 sorted zone; bench/0465-rt-ae-busy). Before: a delete per append at 200/s lost ~39% of AE reconcile slots to pending folds (0% without deletes), so it was material. Causes: fold_target cloned and sorted every RT record per fold (323 µs → 7 µs with `range_tombstone_records_between`), and AE requests ran before the accounting task woke. Mitigation: serve_range_delete runs one fold turn before replying and kicks accounting only if still pending. Fold Busy is now 0 in paced arms and ~2% unpaced. The remaining loss is write-churn scope-token exhaustion, which predates range deletes: proposed 0467. A kill-the-fix server test was added, and the docs and the HELP text are updated. vals-store, vals-cluster and vals-server tests pass; the known flake passed on rerun.

Commit: feat(0465): AE Busy under continuous range deletes measured (vals-bench rtqueue); RangeDelete runs one inline fold turn, fold targets read only (S_A, S_T] records — fold Busy 39% → 0% at 200 deletes/s
