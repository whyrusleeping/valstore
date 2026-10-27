---
id: "0475"
title: 'Range deletes: collect dominated RT records without gc_grace, so queue-style cumulative deletes don''t grow records without bound'
status: done
priority: 2
created: "2026-10-15"
updated: "2026-10-17"
depends_on: []
spec_refs:
    - docs/range-deletes.md
---

## Description

## Description
## Why
0471/0473: a queue consumer that issues cumulative `[prefix, cursor)` range deletes leaves one RT record per delete. With `gc_grace = 0`, records are never collected (docs/range-deletes.md §7). With `gc_grace > 0` they stay for the whole grace period. A queue doing ~500 deletes/s per engine piles up millions of records per engine per hour, held in RAM, written by every flush and copied by every merge, even though F collapses them into one fragment. 0473 removes the per-delete O(records) walk, but the growth itself stays unbounded.

## Idea
A record r is *dominated* when another record r' has r'.version > r.version and a span containing r's span. Dropping r doesn't change F. Collect dominated records in run rewrites (and possibly when the tail is flushed) without waiting for `gc_grace`, under conditions to be designed:
- r' must be durable whenever r's removal commits: in a committed run, or synced, never only in the unsynced tail;
- both records must be at or below `S_A`, so that A and the fold target are unchanged;
- interaction with cleanup clipping, AE RT exchange (peers may hold r without r'), split/rehome, backups/dumps.

## Acceptance (if accepted)
A design addendum to docs/range-deletes.md, then implementation with kill-the-fix tests. Then a 0471 ring-buffer soak showing per-engine record count and RSS stay bounded over hours.

## Acceptance criteria

## Agreed approach (owner, 2026-10-16)
Narrow, provably safe version first. When a flush or run rewrite builds its output, drop record r if another record r' meets all of these:
- r' goes into the same output run, so the same manifest commit makes both durable;
- r' has a higher version and a span that contains r's span;
- both records are at or below `S_A`.
F and A are then unchanged. No pruning of the unflushed tail and none across runs outside the rewrite. Write a §7 design addendum before the code.

Kill-the-fix tests: crash/reopen, gc_grace collection, cleanup clipping, and AE convergence between differently compacted replicas. Hardware check: a ring arm with RSS and record count flat. This can run in parallel with 0478.

## Outcome

Dominated RT records are dropped at flush and in every run rewrite under the owner's narrow rule (same output, containing span, higher version, both ≤ S_A), designed in docs/range-deletes.md §7. Kill-the-fix tests cover a model check, crash/reopen, gc_grace collection of the dominator, cleanup-clip parity and 3-replica AE convergence. On a 2 h lab ring arm (V-Q2-RING-500000-R7-0475, tree d0958b55) RSS went from 1.3 to 2.3 GB per node, against 16.5 GB before. Each node applied 84M slices, 99.9% were dropped, and 67k–106k records were held. Verify was exact with 0 probe violations. The remaining slow growth is run entries and 0477's range debris (bench/0475-dominated-rt/FINDINGS.md). Two reviewers (claude, sol) accepted.

Commit: fix(0475): flushes and run rewrites drop RT records dominated by a newer accounted record in the same output, bounding queue/ring record growth (2 h lab ring arm: RSS 2.3 GB vs 16.5 GB, verify exact)
