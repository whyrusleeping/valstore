---
id: "0544"
title: 'Bootes soak throughput pass: full 5B replay (R5) on HEAD vs R4, attribute and fix the top limiter'
status: done
priority: 2
created: "2026-10-29"
updated: "2026-10-30"
depends_on: []
spec_refs: []
---

## Description
## Why
Owner 2026-10-29: take another pass at bootes soak performance, see how far it goes and speed it up. The last full replay, SOAK5B-V5-R4 (0386, valsd e9e8f7fe, 2026-10-12), ran the 5B-event cache at 1.59M ACK/s for the first 1B, decaying to 0.40-0.48M from 8B (3.67 h active ingest). Since then, write-path perf work landed but no full replay has measured it: 0420 (RangeMap borrow in mirror_rehome_writes, 43-49% of R3 8B shard cycles), 0421/0422 (post-restart cover scans/seed), 0435/0442/0469 (range-count write cost). Correctness work landed too (range deletes, 0499/0500 GC, 0515/0518 cleanup, 0529-0535 recovery/bounds/SPDK ownership checks), so a regression is also possible.

## Work
1. Run R5: a fresh full-cache replay on current HEAD with R4's layout, bootes settings (D32 / gate 64) and lifecycle via bench/0363-compact-values/run_full1.py, with on-box call-graph captures at early/mid/late ACK states. Monitor live against R4's per-1B table; stop early on a clear regression.
2. Attribute the top limiter of the late regime (and the early one if it regressed) from the captures and counters.
3. Fix and A/B the top limiter where feasible; file follow-ups for the rest.

## Acceptance
- R5 per-1B ACK/s table vs R4, with EOF correctness (loss-free drain, cursor, exact counts, samplecheck).
- Caller attribution of shard CPU at the late state.
- At least one A/B-tested improvement, or a measured explanation of why the top limiter can't be cut now, with follow-ups filed.

## Acceptance criteria

## Work log

## Work log

2026-10-29 (session 1). Details and receipts are in `bench/0544-r5/README.md`; raw data is in `/data/dataset/task0363-results/SOAK5B-V5-R{5,6,7}`.
- **Harness brought up to HEAD:** builds include `vendor/`; `real-client14` rebuilt and repinned (the old binary was pruned); the pruned 0295 trace is tolerated; R5-family arms expect run format v6 and record `vals_cleanup_errors`; boxes needed `core_pattern` re-set after a reboot.
- **R5 (HEAD ae8c1a1d, fresh discard), per 1B ACK/s:**
  - 2.11M / 2.03M / 1.68M against R4's 1.59M / 1.43M / 1.21M (+33–42%);
  - after the drive step (~1.2 TB/node, ~2.9B), 1.08M at 3–4B, falling to 0.78M by 4.9B;
  - operator-stopped at 5.12B with a loss-free drain.
- **Post-step attribution (node1 lbr + 1 Hz metrics):** shards saturated, 10.6 µs/write right after the step and 14.1 µs/write at 5B. About 25% of shard time spins on the value-log device (tail write 0.2 → 2.4 ms, drain 0.05 → 1.1 ms at the step). Recount (`logical_accounting_loop`) is 8% at 5B.
- **A/B of a tail-pad candidate (never rewrite a vlog block):**
  - R7, matched against R5 → neutral post-step (1.08 / 1.06M both), so rejected; branch `wip-0544-vlog-pad`.
  - Every vlog write is slow after the step, not just tail rewrites, so the limiter is device write service at ~1 outstanding commit per shard.
  - The 400M ABBA design was invalid: an idle drive recovers to its pre-step state.
- **Integrity on alphac-2:** an R6 AE-reconcile CRC (×8) was cleared by a clean reopen: in-memory event, record-and-continue. R7 node1 had a 0248-class glibc heap abort; it recovered.
- **Next:**
  - SPDK perf queue-depth probe on a post-step drive;
  - if bandwidth scales with depth, pipelined group commits;
  - then the 5B+ recount growth.
  - R7 runs on to EOF for the late regime.

2026-10-30 (session 1, continued).
- **R7 (HEAD + measured-neutral pad) to EOF: all correctness PASS** (cursor, exact counts, whole-cache samplecheck, owned aediag). It had 3 recovered crashes.
- **Per 1B ACK/s, R7 vs R4 (`bench/0544-r5/README.md`):**
  - pre-step faster: 1.94 / 1.91 / 1.61M against 1.59 / 1.43 / 1.21M;
  - post-step slower: 1.07 / 0.84 / 0.77 / 0.63 / 0.56M at 3–8B against 1.21 / 1.04 / 0.72 / 0.83 / 0.81M;
  - total active time about equal (12.3 ks to 10.2B against 13.2 ks to 10.41B).
- **Post-step device throughput:** about 335 MB/s per node against R4's implied ~460. Shards are saturated on synchronous device waits (31 µs/write at 10.2B).
- **R8 control (R4 binary on today's drives):** stopped at ~2B by 4 alphac-2 RAM fail-fasts (xor 0x04) that exhausted the unit's restart limit. Regression vs drive aging is still undecided.
- **Next, in order:**
  - repeat the R8 control (or run it on alphac-1/3 with the 2-node profile) to separate a post-step code regression from drive aging; if it is a regression, bisect the SPDK/vlog changes since R4 (0535 qpair owner lock and affinity checks first);
  - SPDK perf queue-depth probe on a post-step drive;
  - pipelined group commit, so shards don't spin on device completions.

2026-10-30 (owner approved steps 1-3).
- **Step 1, R4-binary control (R8):** not obtainable on this fleet. On the R4 binary, alphac-2's bad bit (xor 0x04) trips the memtable fail-fast every few minutes: 8 crashes in 33 min, against 0–3 per whole run on HEAD. Recoveries confound the rate. The harness now gives an exit-75 restart-limit the same bounded reset as a start-limit hit.
- **Code comparison:** the vlog write path is byte-identical between R4's source and HEAD. DRIVE02 shows post-step drives held ~1.05 GB/s per node while vals reached 0.53–0.60 (HEAD ~0.34), so the drive is not the binding limit.
- **Step 2, fresh drive probe:** 4 KiB sequential writes scale 0.43 → 3.2 GB/s from QD1 to QD32; 64 KiB writes reach 6.9 GB/s. The post-step probe runs after R9.
- **Step 3 candidate PIPE0544 (padded async tail plus a yielding wait for the commit's writes): rejected.**
  - R9 per 1B: 2.17 / 1.45 / 0.94 / 1.13 / 0.81M against R5's 2.11 / 2.03 / 1.68 / 1.07 / 0.85M.
  - The yielding leader rechecks only after every other ready task runs, so the pre-step commit wait rises from 0.05 ms to 1.3–1.9 ms, and at fixed client depth throughput falls.
  - Branch `wip-0544-pipe`.
- **Next direction:** more device concurrency without adding commit latency.
- **Also:** proposed 0545 (pre-existing gap: an async vlog write failure reaped by another caller is never reported to the covering sync); fixed the `dirty_oldest_age` global-gauge test flake (isolated_env_test).

2026-10-30 (owner: implement and run coalescing).
- **COAL0544 (65606830, branch `wip-0544-coalesce`, now in the worktree):** the value log coalesces full blocks into ≤16 KiB writes (the size of one shim DMA pool slot); a commit writes its staged blocks plus the partial tail in one synchronous write and keeps both on failure.
- **R8 (reused name) full replay: EOF PASS** with 0 crashes. Active ingest to 10.2B was 9,630 s against R7's 12,299 s (−21.7%).
- **Per 1B ACK/s:** 2.18 / 1.96 / 1.82 / 1.17 / 1.06 / 0.95 / 0.88 / 0.90 / 0.80 / 0.71M.
- **Mechanism:** device ops −73% at equal MB/s; inflight-cap stalls 0.
- **Strict keep rule:** missed by small margins (3–4B +9.3% against +10%; 1–2B −3.4% against −3%, inside R5/R7 spread). The owner decides whether to keep it.
- **Two ordering bugs in drafts were caught by existing tests:** roll must write the synchronous tail before the async staged submit; a sync must keep its staged blocks on failure.
- **Next lever:** 64 KiB coalescing (shim DMA slot size).

## Outcome

- **Root cause:** after the drive's steady-state step, ingest was bound by the value log's write issue pattern, not by the drive.
  - A ~200 B record stream became one 4 KiB device write per filled block: ~95k ops/s per node at low depth, ~0.34 GB/s.
  - A raw probe of the post-step drive still gives ≥1.2 GB/s at depth, and 16 KiB writes reach 2.8× the 4 KiB rate at QD1.
- **Fix:** coalesce full value-log blocks into contiguous writes of up to 16 KiB (one shim DMA slot). A commit writes its staged blocks plus the partial tail in one synchronous write and keeps both on failure; reads drain staged blocks first.
- **Full 5B replay (R8):** all correctness PASS with 0 crashes. Ingest to 10.2B took 9,630 s against HEAD's 12,299 s (−21.7%); +25–60% per 1B from 4B on. The preregistered rule missed narrowly (3–4B +9.3% against +10%; 1–2B −3.4% against −3%, inside run-to-run spread). The owner accepted.
- **Rejected:** the padded tail (neutral) and the yielding commit wait (adds scheduler latency; −30–50% pre-step).
- **Baselines:** HEAD (R5/R7) is +33–42% over R4 pre-step but slower post-step.
- **Follow-ups:** 64 KiB coalescing (shim DMA slots); proposed 0545.

2026-10-31 follow-up.
- **64 KiB coalescing (COAL64, branch `wip-0544-coalesce64`): rejected.**
  - Full replay R9 stopped at 4.66B; per 1B −10…−17% against 16 KiB at 2–5B.
  - Fewer, larger writes (node1 ~3.5k ops/s) left more bytes for each commit's synchronous staged+tail write (3.1–4.6 ms against 2.1–2.5 ms), so device throughput fell. 16 KiB (86983dc) stays.
- **The R9 build hit a rustc SIGSEGV on alphac-2** (RAM class); the retry built identical binaries.
- **Owner is considering ECC SO-DIMMs for alphac-2.** The MS-02 Ultra (WM880) supports them. Kernel 7.0 has no EDAC driver for host bridge 8086:7d1c, so corrected errors would not be reported.
