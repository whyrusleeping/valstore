---
id: "0484"
title: Descending (reverse) range scans for sorted zones
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why
Sorted-zone scans only run in ascending key order. "Latest N" therefore needs keys built with an inverted timestamp (`u64::MAX - ts`). With that encoding, oldest-first and newest-first can't both be cheap without writing every entry twice, and "last key under a prefix" (queue producer tail recovery, 0440 item 4) has no direct form. Graph uses (0481): newest-N edges of a vertex, newest followers, and paging backwards through an adjacency list.

## Semantics
- `scan_in_rev(zone, lo, hi, r, limit)`, or a direction parameter: the same half-open `[lo, hi)` window and the same LWW, tombstone and range-tombstone visibility as `scan_in`, but keys are returned in descending order starting from the largest key `< hi`.
- Strong under `W + R > RF` exactly like ascending scans.

## Sketch (to be checked against code)
- **Engine:** a reverse twin of `scan_winners_page`.
  - *Run cursor:* binary-search the resident fence index for the last block whose first key is `< hi`. Blocks are front-coded forwards (each key stores its shared-prefix length relative to the previous key), so decode the whole block (≤ ~32 KiB), then iterate it backwards and step to the previous block. That costs at most one extra block decode per run per page.
  - *Memtable:* same idea over its front-coded blocks (reverse block-map range, decode, iterate backwards), with a refillable reverse `scan_page` cursor so paging stays O(page).
  - *Merge:* k-way by max key instead of min.
  - Range-tombstone cover tests are per-key and direction-independent. The covered-block skip (`rt_next_block` / `next_picking`) needs a reverse twin.
  - Value-log span coalescing is unchanged: it sorts by LBA anyway.
- **Wire:** a direction flag behind a hello feature bit, as a new opcode or flag. Coordinate with the batched multi-scan task so both share one encoding.
  - A truncated descending reply covers `[last_key, hi)`; resume with `hi = last_key`. Exclusive upper bound, so no `++0x00` is needed.
  - A range-tombstone resume key covers `[resume, hi)`.
- **Coordinator:** visit the overlapping range slices in reverse order and mirror the min-coverage rule (algorithms.md §6): bound = **max** over the pages' coverage lower bounds; merge only keys `≥ bound`. Limited scans stay serial (newest slice first).
  - Peers without the feature get an explicit `Err`, not a silent fallback; emulating reverse scans with ascending scans is not viable for large ranges.
- Go client parity (valsgo) is a follow-on.

## Acceptance
- Engine property test: the reverse scan equals the reversed ascending scan over memtable plus mixed runs, overwrites, tombstones and range tombstones, at every page budget.
- Cluster tests:
  - paged multi-replica reverse merges across splits and pending re-homes;
  - `limit` scans;
  - forced small reply budgets;
  - a kill-the-fix test for the mirrored min-coverage bound.
- 0481 latest-N query using natural timestamps runs on reverse scans, reported against the inverted-timestamp baseline.
- No cost to ascending scans or the write path.
- Docs: formats.md, algorithms.md §6, zones.md range-scan section, ARCHITECTURE.md.

## Acceptance criteria

## Outcome

Completed reverse-scan acceptance across committed foundations 0487–0491 and final benchmark/docs: natural-timestamp latest-N mode with exact checks; six local and three SPDK arms pass, 319,804 paired edges and zero mismatches/integrity events. Reverse latest-N costs 1.89x local / 1.67x SPDK p50 versus inverted ascending; matched ascending/write arms show no material regression at measured resolution. Original cluster restored, ring12 retained. Clean tests: 171 client, 967 cluster, 559 store, 42 bench, four runner tests; Clippy succeeds with warnings. Independent reviewer accepted and reran reverse tests plus both graph modes. Unrelated covered-merge/PD work excluded; saved zones/memory hunks require unchanged restoration.

Commit: bench(0484): validate descending graph scans and document reverse API
