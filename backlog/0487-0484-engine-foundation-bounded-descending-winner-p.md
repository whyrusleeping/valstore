---
id: "0487"
title: '0484 engine foundation: bounded descending winner pages over memtable and runs'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

Necessary implementation slice of accepted task 0484, not a replacement for its end-to-end acceptance. Add a synchronous Engine::scan_winners_page_rev (and counted twin) over the same [lo, hi) window as ascending winner scans, descending keys, same LWW/tombstone/range-tombstone visibility and page-budget conventions. Resume with exclusive hi = last returned key. Add bounded reverse front-coded memtable pages, reverse run block cursors seeded from the resident fence index, and reverse covered-block skipping. Leave existing ascending/write call paths unchanged.

Acceptance: property-style comparisons to reversed ascending winners across mixed memtable/runs, overwrites, point/range tombstones, byte/entry budgets, bounds, empty/unbounded windows and memtable refills; explicit block-boundary/covered-block skip regressions; existing vals-store tests pass. No format or wire changes in this slice. Document engine API scope and remaining work in parent 0484. Async staged serving, wire/client/coordinator, cluster pagination tests, user-facing scan docs, and 0481 natural-timestamp vs inverted benchmark remain required by 0484.

## Outcome

Completed the synchronous engine slice split from accepted task 0484: reverse front-coded memtable/run cursors, paged descending LWW winners, and conservative reverse RT max-tree skipping. Ascending/write method bodies and formats/wire unchanged. Five focused regressions, same-HLC kill-the-fix, 552 unit plus 11 integration tests pass (25 ignored); Clippy has only existing warnings. Independent review accepted the final snapshot and independently reran the full suite. Parent 0484 remains in_progress with staged serving, negotiated protocol/coordinator, cluster tests, natural-timestamp benchmark, performance validation and end-to-end docs explicitly outstanding. Unrelated dirty work excluded.

Commit: feat(store): add bounded descending winner scans
