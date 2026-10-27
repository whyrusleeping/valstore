---
id: "0318"
title: Split recount without synchronous run-block I/O on the shard thread (async prefetch, sync merge)
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-27"
depends_on:
    - "0317"
spec_refs: []
---

## Description
## Evidence

- **LONG01-D4-L** (5 s PD poll, 120 splits during write): recount turns took 326 s per node for 104M keys. That is about 3.1 µs per key and about 0.8 shard-cores per node.
- **LONG01-D9-REHOMEORDER-POLL1000** (1 s poll, rehome-safe build 0317): splits roughly doubled to about 0.9/s. Recount turn time grew by about 1.35 s every second per node, meaning about 1.35 shard-cores were spent on recount. ACK fell to about 800k against L's 990k, and the maximum range size still grew to more than 3M, so split demand is still not met.
- **Why each key is so expensive.** Every `logical_recount_step` turn calls `Engine::winner_count_page`. That call re-seeks every overlapping run and reads each run block synchronously via `RunScan::next` → `read_run_range`, a blocking SPDK read that spin-polls on the shard thread. It then stops at the 1 ms deadline with about 400 winners. With about 7 runs per engine, that is about 10–16 serial block reads per page, about 80 µs each, so the turn is almost entirely I/O wait. Shard `nvme/spdk_qpair_process_completions` rose to 16.7 % of cpu_core cycles at t300 on D9.

## Candidate (one mechanism)

Keep the page merge synchronous and atomic, exactly as it is now: no awaits between reading the memtable and runs and updating `covered`/`after`, so the 0310 invariants I1/I2 and the apply/retire hooks are unchanged. Move the run-block reads before the merge, as asynchronous concurrent reads:

1. **Plan.** From the recount's `page_lo`, take a plan for each overlapping run: `straddling_start_block` + `locate_blocks_window(W)`, using RangeLocators with async read. Record `storage_generation`.
2. **Read.** Await all the reads concurrently while holding the shard read gate (`shared.read_gate().acquire()`). Never hold a RefCell borrow across an await.
3. **Merge.** Re-borrow. If `storage_generation` changed, discard the blocks and re-plan. After N consecutive discards, fall back to today's sync page so progress is guaranteed. Otherwise, run the winner merge using only the prefetched decoded blocks plus the memtable, bounded by a horizon: the minimum over runs that have unfetched in-range blocks of that next block's first key (exclusive). With W ≥ 2, every page yields at least one winner.

Page size and turn budget otherwise stay as they are. Counters: prefetch pages, discards and sync fallbacks.

## Acceptance

- **Tests.** Unit tests show that prefetched pages give exactly the same counts as the sync path over randomized run/memtable layouts with tombstones and overlapping versions. They also cover writes interleaved between prefetch and merge, storage-generation change → discard/fallback, and the horizon edge cases. The full vals-store and vals-cluster suites pass.
- **Fleet.** A preregistered D-prefix A/B at the 1 s poll against D9. The candidate must cut recount turn seconds per key by at least 3×, raise ACK/s, and keep 0 mismatches/fallbacks and all correctness gates.

## Acceptance criteria

## Work log

### 2026-09-27: implemented; D10 positive; ABBA running

- **Implementation.** Commit `3530cb2f` on branch `0318-recount-prefetch`, on master `f29ea91c`. It adds:
  - `Engine::winner_count_prefetch_plan` and `winner_count_page_prefetched`, which use a horizon bound;
  - `logical_recount_turn`, which holds the read gate, reads concurrently, and runs the merge synchronously;
  - W = 16, with a sync fallback after 4 discards;
  - four counters (prefetch pages, blocks, discards, sync fallbacks).
- **Tests.** vals-store passes (335) and vals-cluster passes (706). This includes randomized sync equivalence, horizon edge cases, writes in flight, and a generation-change discard followed by the fallback.
- **D10 against D9** (1 s poll, D prefix):
  - ACK/s: 1,010,109 against 825,921 (1.223×).
  - Recount time per key: 0.165 µs against 2.53 µs.
  - Quartiles: 1082/1008/994/965k. Sliding-10 s minimum: 860k.
  - 0 mismatches, 0 fallbacks, 0 sync fallbacks, 102 discards.
  - Settle took 171 s. The run PASSed and every sustain gate passed.
- **Pending.** The D11 (R) / D12 (M) ABBA and an independent review.

### 2026-09-27: ABBA accepted and merged (`0320be0e`)

- **ABBA at a 1 s split poll, D prefix:** D9-M 825,921, D10-R 1,010,109, D11-R 911,073, D12-M 817,280 ACK KV/s.
  - R/M ratio is **1.169×**.
  - Recount time per key is 0.07× of M.
  - Every run had 0 mismatches, fallbacks, sync fallbacks and seed folds.
  - Every run PASSed, and every settle converged within 133–171 s.
- **Review.** An independent read-only review found no correctness bug. Its two hardening notes are filed as proposed task 0320.
- **Evidence.** `bench/0297-ingest-1m/long01/FINDINGS.md`, `SUMMARY-RECOUNT-0318.json`.
