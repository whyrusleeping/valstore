---
id: "0310"
title: Range-bounded logical recount on populated inherited splits (remove O(store) refold per auto-split)
status: done
priority: 1
created: "2026-09-25"
updated: "2026-09-26"
depends_on:
    - "0309"
spec_refs: []
---

## Description
## Evidence (task 0309 LONG01, 2026-09-26)

On the 2-node RF2/W2 device-sync fixture, the B prefix (94.85M KV) with auto-splits reaches only 417–469k ACK KV/s whole-write. Per quartile it runs 1.1M → 0.24M. With splits disabled as a diagnostic it reaches 794k.

Late shard-thread perf (cpu_core, leaf symbols, cycles per KV) went from 31k early to 203k late. Of that, about 100k per KV is cooperative seeding: `SeedProgress::fold_key`, `seed_step_inner`, `entry_digest`, `Ring::walk`/`replicas_in_zone`, `logical_range_index` and sip hashing.

Per-node counters over about 45 splits:
- `ae_seed_fold_ops` went 0 → 1.1 B against a 95M-key store.
- `logical_range_map_invalidations` rose by 360, which is 45 splits × 8 shard stores.
- `ae_seed_placement_reuses` rose by 109, while `ae_seed_placement_invalidations` stayed flat.
- `ae_seed_staged_bytes` reached 5–8.5 GB.
- Node stop exceeded 300 s (SIGKILL) in both split arms.

Cause: in master, `set_logical_range_map`, on any boundary change, sets counts to None and calls `invalidate_leaf_seed()`. That clears every AE leaf and forces a full O(store) cooperative seed on every shard store of the zone. AE leaves themselves are still valid, because placement-equivalent reuse holds.

## Design

The read-only design review (agent_2, 2026-09-26) chose a range-bounded recount:
- `set_logical_range_map` handles an inherited single-cut split without touching leaves. It inserts placeholder children and records a `SplitRecount { lo, cut, hi, total, scan side, cursor, covered }` span.
- A bounded cooperative recount of the smaller child runs through the new `Engine::winner_count_page`, which wraps `scan_winners_page`, with the same collapse and tombstones. The other child is derived as `total − covered`.
- Concurrent apply/retire hooks adjust `total`/`covered` by the cursor.
- A merge becomes an O(1) sum.
- Every other shape falls back to today's full invalidation.
- Readiness stays false until all spans finish, so the planner remains fail-closed.

Full design text: agent_2 report in the session log; summarized in `bench/0297-ingest-1m/long01/FINDINGS.md` when written.

## Constraints

- Implement on a branch from master in a separate worktree.
- Do not touch the uncommitted 0294-phase14 overlay in the main working tree. The new design subsumes it, and the overlay stays unadopted.
- Follow the exact-count contracts of 0284/0290.
- No explanatory comments in Rust code; hand-format new code only, and never run `cargo fmt`.

## Acceptance criteria

- [ ] Unit tests pass, covering:
  - a populated split with concurrent applies, deletes, retires, flush, compaction and forget;
  - counts equal a brute-force count and a golden full reseed;
  - leaves untouched and no seed started;
  - bounded visit count;
  - every fallback shape;
  - merge sum;
  - RAM pause;
  - `clear()`;
  - rehome rebase while a recount is pending;
  - engine `winner_count_page`.
- [ ] The full vals-store and vals-cluster suites pass in debug.
- [ ] Fixed-name transition counters and a recount turn-latency histogram are added. `docs/algorithms.md` is updated.
- [ ] Hardware A/B on LONG01 B (splits on) against master:
  - `ae_seed_fold_ops` stays flat after boot;
  - recount keys ≈ Σ smaller-child sizes;
  - whole-write and late-quartile ACK/s improve;
  - exact counts, samples and cursor match;
  - node stop is clean;
  - accept or reject against a preregistered band.

## Work log

## Work log

### 2026-09-26: implemented (agent_3) and reviewed (agent_5); first hardware pair +71%

- **Branch.** `0310-split-recount` in worktree `.wt/0310`. Commits:
  - `8289b354` — store `winner_count_page`
  - `3cea5cc8` — cluster transition, hooks, recount, counters, tests
  - `012895e5` — sweep drive
  - `2660f6b1` — docs
- **Tests.** Debug suites for store, cluster, client and server all pass.
- **Review.** No blockers. Items to fix before merge:
  - **MAJOR:** PD can auto-split a still-pending child, because the unready V3 report uses inflated `range_estimate`. That falls back to a full reseed. Fix with a PD gate on unready ranges, deferral, or N-way spans.
  - **minor:** the fallback counter over-counts (it includes boot installs).
  - **minor:** the recount page does synchronous run I/O (measured mean 7 ms per turn, max bucket 25 ms at 4096 winners). Use pages of 1024 or smaller, or a time cap.
  - **minor:** under RAM pressure the loop breaks and depends on an external kick to resume.
  - **minor:** abandon returns `Ok(true)`.
- **Hardware.** LONG01-B8 (branch) vs B9 (master), on the same trimmed drives, with 52–54 splits:
  - whole-write 793k vs 463k;
  - `ae_seed_fold_ops` 0 vs 1.16 B;
  - `recount_keys` 12.5–13.0M per node, 0 mismatches, clean stops.

  This is a single pair. A preregistered ABBA is still required before merge.

### 2026-09-26: review fixes, ABBA accepted, merged

- **Review fixes (agent_3, round 2):**
  - `904963c5`, `35f325dc`, `9c09bb8d`: group re-partition, so nested splits, multi-cuts and edge merges stay exact without a reseed.
  - Turns are bounded at 1024 winners or 1 ms.
  - Fallback counting is gated, and the RAM pause now resumes itself.
  - V3 reporting while a recount is pending is partially exact, with clamped estimates for pending children.
- **Re-review (agent_6):** no blockers and no majors. Its stronger private randomized oracle found 0 mismatches.
- **Preregistered ABBA (B11-M / B12-R / B13-R / B14-M):**
  - R/M = 815.8k / 464.9k = **1.755×**.
  - All PASS with clean stops.
  - Seed folds 0, fallbacks 0, mismatches 0.
  - Turn p99 ≤ 2.5 ms.
- **Result:** ACCEPTED. Merged to master as `d8202b6c`.
- **Overlay:** the 0294 phase14 overlay was retired at the owner's choice and archived in `bench/0294-ingest-optimization/retired-phase14-overlay-20260926/`.
- **Follow-up (minor):** fold agent_6's stronger randomized oracle into `split_recount_tests.rs`.
