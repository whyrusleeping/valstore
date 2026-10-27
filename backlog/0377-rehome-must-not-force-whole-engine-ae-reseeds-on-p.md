---
id: "0377"
title: 'Rehome must not force whole-engine AE reseeds on peers: invalidate only the changed spans (0374 item C)'
status: done
priority: 3
created: "2026-10-04"
updated: "2026-10-12"
depends_on:
    - "0374"
spec_refs: []
---

## Description
From 0374 (bug c). A rehome commit changes placement and makes the current owners invalidate their whole engine's AE leaves (`prepare_zone_seed`, `crates/vals-cluster/src/replica/lifecycle.rs` ~975-1002 / ~1125-1152), so peers reseed ~88M keys for a ~1M-key range.
- **Effect:** RECOV01 showed a ~5.5 min peer reseed under ingest; RECOV02 saw peers reseed during the settle after the PD hold released.
- **Not observed in FULL11:** 0 healthy-node reseeds across 3 rehome commits (why not is unexplained).

**Design** (in the 0374 work log, round-2 entry): invalidate/regroup only the leaves of spans whose placement changed. This needs:
- a bounded page scan that returns versions;
- write-path routing between old and new placement during the regroup;
- gating of tree serving and dirty rebuilds.

Estimated 500+ lines.

**Acceptance:**
- property test: leaves and logical counts after a rehome equal a from-scratch seed;
- peers do no full-engine reseed on a single-range rehome (metric);
- a hardware datapoint (a rehome under ingest with no peer-wide seeding dip).

## Validation amendment (0407, owner-approved 2026-10-08)
Tiers L0 and L1; see docs/validation.md. This replaces the dedicated hardware datapoint with:
- a property test;
- a 3-process file-backed cluster test with the PD, asserting zero `ae_seed` starts on healthy peers across a rehome under load.

The hardware datapoint is only a piggyback on the next full replay's rehome windows.

## Acceptance criteria

## Work log

### 2026-10-10: implemented and reviewed; integration held for 0416
- **Code:** worktree `.wt/0377` (branch 0377-span-reseed, base 2ec32878), uncommitted.
  - A rehome now regroups only the changed spans. Keys in a span's unscanned part route to the old placement; pages move digests from the old group to the new one in a single synchronous step. Prefetched pages are validated by storage generation and fall back to a sync page on read error.
  - Back-to-back changes compose a frozen mixed routing, up to depth 8, then fall back with a count.
  - Tree serving, SyncTree and dirty rebuilds answer not-ready while a regroup runs.
  - Kill switch `ae_span_regroup` / `VALS_AE_SPAN_REGROUP`.
  - New `ae_regroup_*` counters; docs updated in zones.md, algorithms.md and configuration.md.
- **FULL11 explained:** its 3 rehomes kept the same sorted replica node set (the `placement_equivalent` path), so there was no invalidation. RECOV01/02's rehomes changed the node set.
- **L0:** property test, 24 seeds × 240 ops: 451 regroups, 1197 compositions, async flush/compaction, staged applies, prefetch discards; 0 mismatches and 0 unexpected reseeds. Knockouts: misroute gives 572 mismatches; no compose gives 95 reseeds; switch off gives 44 reseeds. Plus dirty-bucket, depth-cap and prefetch-error tests.
- **L1:** `vals-server/tests/rehome_regroup.rs`, 3 processes plus the PD, a single-range rehome under write load:
  - ON: seed starts [0,0,0], invalidations [0,0,0], regroups = completions [2,2,2], 0 fallbacks.
  - After settle: AE applied, digests, sync errors and reconcile skips are all 0, and trees were compared.
  - Knockout: seed starts [2,2,2].
  - The coordinator fixed a setup flake (StaleRing on the initial load) and added the sync-error and skip asserts: 10/10 passes.
- **Review:** opus (agent_10), two rounds. No exactness break was found; the back-to-back fallback major was fixed; approved.
- **Integration held:** the main tree's uncommitted `crates/vals-store/src/engine/scan.rs` is 0416's preregistered A/B candidate (a BinaryHeap k-way merge in `winner_count_page_prefetched`), and 0377 rewrites the same loop to carry versions (`winner_page_prefetched` with a sink).
  - After 0416's keep/remove verdict, port 0377's version-carrying onto whichever loop survives.
  - Confirm the count/recount path, 0416's hot caller, is unchanged in cost: the sink is a no-op closure for counts. Rerun the `prefetched_winner*` tests and the property and e2e tests, then integrate.
- **Hardware piggyback** on the next replay's rehome windows: regroups and completions rise while `ae_seed_placement_invalidations` and `ae_seed_starts` stay flat, with `ae_regroup_fallbacks` and `ae_regroup_depth_fallbacks` at 0. If no window changes the node set, record it as inconclusive.
- 2026-10-10: integration note: the main tree now carries 0366, 0371 and 0372. 0366 split a test in `replica/seed_pin_tests.rs`, which 0377 also edits. Rebase onto the main tree when integrating, along with the 0416 `scan.rs` port.
- 2026-10-11: **ported onto 0416's heap merge (d1715c80) and integrated into the main tree, uncommitted.**
  - `winner_page_prefetched` keeps 0416's BinaryHeap of (key, cursor). Run-head versions are held in a side array indexed by cursor, and duplicate keys fold to the max version; a memtable entry wins ties, as before.
  - The first port carried the u128 inside the heap entries. That cost the count/recount path +2–7% merge ns/key in 0416's `bench_0416_winner_count`.
  - With the side array, the interleaved release A/B (ABABAB, 3 rounds, pinned core; A = main without 0377) is neutral within ±1% at 4/12/24/40 runs.
  - After the port: `prefetched_winner*` 3/3, staged_scan 10/10, regroup 12/12, replica:: 173/0, antientropy 40/0, `rehome_regroup` 3×2/2.

## Outcome

A rehome now reseeds only the spans that changed. A property test and a 3-process cluster test show no peer reseeds during a rehome under load, and back-to-back rehomes avoid a full reseed. Ported onto 0416's committed heap merge; the first port slowed the recount 2–7%, so version carrying was reworked and the interleaved benchmark is within ±1%. Integrated in s_8666773a (2026-10-11); committed 2026-10-12.

Commit: fix(0377): rehome reseeds only changed spans, not whole engines — property + 3-process cluster tests show no peer reseeds under load; heap-merge recount within ±1%
