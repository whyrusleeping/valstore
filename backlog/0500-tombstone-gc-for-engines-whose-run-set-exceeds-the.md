---
id: "0500"
title: Tombstone GC for engines whose run set exceeds the merge input cap
status: done
priority: 3
created: "2026-10-22"
updated: "2026-10-22"
depends_on: []
spec_refs: []
---

## Description
Split from 0499. 0499 made tombstone GC a whole-engine merge (kind `lonegc`), rate-limited to one per min(gc_grace, 1 h). `Engine::compact_plan_lone_gc` plans nothing when the run set is over `VALS_MERGE_INPUT_CAP_BYTES` (2 GiB of run index) and has more than one run, because `select_capped_suffix` must start at 0. Such engines, which are normal at scale once frozen runs exist, still never collect expired point tombstones, except through region-pressure merges and shadow folds. A reviewer reproduced this by running `replica::tests::tombstone_gc_flushes_the_tail_and_collects_across_runs` with `VALS_MERGE_INPUT_CAP_BYTES=1`.

Acceptance criteria:
- Bounded-progress GC for over-cap run sets. Each GC rewrite reads and writes at most about the cap, plus a probe of the other runs. One candidate is a capped shadow-fold-style window whose shadow sources prove that the dropped tombstone's key has no older candidate. Expired tombstones then converge to collected without relying on unrelated writes or region pressure, and deletes inside grace never resurrect.
- A regression test with a reduced cap reaches collection.
- The cost per engine stays bounded by the cap and the GC interval, not by total data (owner scaling principle).

## Acceptance criteria

## Work log
- 2026-10-22 implemented inside 0499 after review: over-cap GC folds rotating capped windows with shadow-fold probe sources; regression `tombstone_gc_over_the_input_cap_folds_windows_until_collected`. Nothing left here.
