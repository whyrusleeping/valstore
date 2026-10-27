---
id: "0374"
title: 'Crash recovery collapses ingest (FULL10-3N: 1.58M → 0.13–0.25M ACK/s for 46 min after one node restart): AE seeding starves the apply path'
status: done
priority: 1
created: "2026-10-04"
updated: "2026-10-04"
depends_on: []
spec_refs: []
---

## Description
## Why
Crash recovery collapses ingest throughput for tens of minutes, which blocks the multi-hour 4–5B-event soak on the bit-flipping lab fleet: node crashes are expected every few hours.
- **FULL10-3N** (`bench/0363-compact-values/FULL10-FINDINGS.md`, 3 nodes, master b8f20e16): 1.58M ACK KV/s until node2 crashed at ~1.6B ACK. node2 served again after 17.6 s and ACKs resumed after 26 s. For the remaining 46 min the cluster ran at **0.13–0.25M ACK/s** and never recovered before EOF.
- **During that phase:**
  - AE tree seeding was active on all three nodes;
  - BatchApply latency on seeding nodes was 100–285 ms (vs 15 ms);
  - devices were nearly idle;
  - bootes Busy wait was 0.39 s/s;
  - node3 applied 117,751 AE repair entries.
- **Earlier occurrences of the same pattern:**
  - 0348 M1: 715 s to reach 80% after a restart;
  - FULL6-LAZY: a 20 min crawl at 50–200k after a node3 crash;
  - FULL3 restart check: AE sync errors at boot.

## Hypothesis (to confirm)
Post-restart AE seeding (VersionIndex leaf seeding from run streams, 0231/0323 cooperative `seed_step`; repair pulls) shares shard threads with the apply path and starves it. Bootes' strictly ordered admission with RF2/W2 then makes the whole cluster as slow as the slowest replica set.

## Work
1. **Attribute from FULL10 raw data** (`/data/dataset/task0363-results/FULL10-3N/`): per node over time, seed steps/turn costs, seed permits, repair/AE apply counts, shard CPU split, BatchApply queueing, and which zone/shard/ranges are slow. Also whether the restarted node or the healthy peers are the bottleneck.
2. **Fix:** make recovery work yield to foreground apply. Options:
   - adaptive seed budget per turn tied to apply queue depth or BatchApply latency;
   - defer seeding/repair while the apply backlog is high;
   - move more seed work off-core;
   - make writes not depend on seeded trees (dirty-bucket marking until seeded).
   Correctness of AE convergence after recovery must be preserved and measured.
3. **Hardware A/B:** a 3-node mid-ingest kill/restart of one node at a fixed ACK point, baseline vs fix. Preregister:
   - time from restart to ≥ 80% of the pre-crash rate (target ≤ 120 s);
   - mean rate over the 10 min after restart ≥ 0.7× pre-crash;
   - AE converges after drain (sweep liveness, ownership-aware tree compare);
   - correctness (cursor, samplecheck, counts).

## Related
- aediag must become ownership-aware for 3+ nodes: compare only ranges both nodes own (FULL10 found one shard per pair "fully different" only because of range ownership).
- Bench harness item.

## Acceptance criteria

## Work log

### 2026-10-04: attribution (agent_5, read-only, FULL10 raw data)
- **Bottleneck: node2 (restarted).** AE leaf seeding pinned 4 of 8 shard threads at 100% CPU for ~20–25 min (`ae_seed_concurrency` = 4, `node_settings.rs:88`). ACK is exactly closed-loop on chunk latency (depth 32), set by the slowest shard of each fan-out chunk. Peers stayed idle for the first ~20 min.
- **Not the cause:** RAM, devices, sync fallbacks, repair (bounded: node3 117,751 once), key-order waits. The 0364 cover-scan only mattered in the first ~4 min.
- **Mechanism (with code locations):**
  - `logical_accounting_loop` calls `seed_step_cooperative()` + `yield_now()` in a loop (`sweep.rs:106-129`), yielding only to scans, never to applies. Steps fold ≤4096 keys at ~1.4 µs per fold, i.e. up to ~6 ms per step (`lifecycle.rs:678`, `751`, `919-928`).
  - **Extra drivers:** `ensure_tree_ready_inner` (`lifecycle.rs:1546-1554`) and `ensure_seeded_cooperative_cell` (`1482-1494`), reached from `reconcile_with` (`antientropy.rs:462/472`) and from peers' SyncTree requests via a detached spawn (`connection.rs:458-481` → `scan.rs:10`). These keep spinning after the peer's 5 s deadline (inferred).
- **Bugs:**
  - (a) **Wasted seeding.** Every range split bumps the logical-range generation (`lifecycle.rs:1061`, `1069-1082`), and an in-flight seed resets from scratch (`603-611`). node2 folded 4.04B keys for 1.41B live (2.9×), with 266 projection retries; seeds finished only in split-free gaps.
  - (b) Seed-driving AE handlers outlive the peer's deadline.
  - (c) **Full reseeds on healthy peers.** A rehome commit invalidates a whole engine's leaves (`prepare_zone_seed`, `lifecycle.rs:975-1002`), so peers reseed ~88M keys for a ~1M-key range. 7 rehomes ran during and after the outage, and their commits line up with the peers' collapses.
  - (d) The seed journal is not RAM-accounted.
- **Plan** (A, B and G together, behind a kill switch; D separately):
  - **B:** an in-flight seed survives a generation bump (publish counts for the seed's map, then remap via the split-recount path, `lifecycle.rs:1035-1062`).
  - **A:** a single budgeted seed driver per shard (e.g. ≤25% duty while BatchApply or the gate is busy, full when idle); AE handlers return Busy instead of driving seeds.
  - **G:** account and cap the seed journal.
  - **D:** the PD defers rehome debt while a node is down or seeding.
  - **C** (a rehome dirties only changed spans) is a larger follow-up.
- **Hardware A/B:** FULL10 config; SIGKILL node2 at 1.0B ACK with a 10 s systemd restart; 30 min of ingest, then drain, then a 600 s AE window. ABBA order, arm chosen by env flag.
  - t80 ≤ 120 s (180 s allowed for the cover-scan phase);
  - mean ACK over the 600 s after serving ≥ 0.7× pre-kill;
  - seeding completes within 60 min under ingest;
  - correctness checks pass and AE converges.

### 2026-10-04: B+A+G implemented (agent_3), reviewed (agent_5: OK as A/B candidate), RECOV01 hardware A/B, committed
- **Implementation** (kill switch `ae_seed_isolation`, default 1; `ae_seed_duty_pct` 25; `ae_seed_journal_max_bytes` 64 MiB):
  - B: seeds carry across range-generation bumps (Keep/Remap via the 0310 path);
  - A: a single budgeted seed driver per shard; AE handlers return Busy/not-ready;
  - G: the journal is RAM-charged and capped, spilling into the seed staging.
  - Tests include knockouts for each.
- **RECOV01** (`bench/0363-compact-values/RECOV01-FINDINGS.md`): 3 nodes, SIGKILL node2 at 1.0B ACK.

  | | ON | OFF-R2 |
  |---|---:|---:|
  | Reference ACK/s | 1.641M | 1.659M |
  | **t80** | **70 s** | 1,209 s |
  | **R600** | **0.639** (gate 0.7, missed) | 0.088 |
  | Post-kill mean | 943k | 610k |
  | Whole run | 1.186M | 876k |
  | Generation resets / carries | 0 / 17 | 207 / 0 |
  | Folds | 0.90B | 2.27B |

  - Correctness PASS in both, including ownership-aware aediag 72/72.
- **Why R600 < 0.7:**
  - (1) The duty budget did not hold: seed time was 84% of busy+pause. `seed_foreground_busy()` mostly sees scans rather than BatchApply load, and restart doubling compounded it (37 seed restarts on node2).
  - (2) A PD rehome at ~480–540 s made both peers reseed for ~5.5 min (bug c).
- **Amendment 1 (accepted by the coordinator):** a 0375 `site=readback` pre-publication catch is record-and-watch.
- **Next round** (the review MAJORs plus the hardware findings):
  - a real foreground signal (BatchApply staged/in-flight on the shard);
  - cap or decay duty escalation;
  - seed permits = shard count under isolation (or release during pauses);
  - check the journal gauge vs the per-seed cap (373 MB peak);
  - D: the PD defers rehomes while any node is seeding or down;
  - C: a rehome dirties only the changed spans;
  - review minors (recount duty, shutdown re-enable, spill debug_assert, test gaps).
  - Then RECOV02 (ON only, same preregistration).


### 2026-10-04: second round (agent, local only; uncommitted)
- **1. Duty budget**
  - The foreground signal is now any request `note_range_ops` saw in the last 200 ms (Apply, BatchApply, reads, MixedBatchRead, Scan), or gate work in flight.
  - The duty is a wall-clock budget (`DutyBudget`, `seed_policy.rs`). It earns `duty_pct` of elapsed time, holds at most 20 ms of credit, and pauses until any debt is repaid (at most 1 s per pause). Many short steps cannot evade it.
  - Escalation is capped at max(duty, 50%). Split-recount turns share the same budget.
  - Tests:
    - deterministic shard simulation: applies at 60% load never in flight at step boundaries give a 0.25 seed share; the gate-only knockout gives ≥ 0.35;
    - executor tests (debug): ON 0.252, gate-only knockout 0.950; 37 restarts 0.502; idle 0.998.
- **2. Permits**
  - Under isolation, shard-driver seeds take an uncapped permit. They still honour the shed cooldown and still count in `ae_seed_active`, giving at most one seed per shard.
  - RAM stays bounded: staging, memtable snapshots and cursor windows are broker-reserved, and the journal cap is node-wide.
  - Releasing permits during pauses was rejected: the seed's RAM stays held through a pause, so releasing the permit would not bound memory.
  - Test: cap 1 lets three driver seeds start; the knockout serializes them.
- **3. Journal**
  - The cap was per seed and checked after `Vec::push` doubling, so a seed could reach about 2× the cap. Four seeds and the node-wide gauge explain 373 MB.
  - Now `ae_seed_journal_max_bytes` caps the node total (default 256 MiB). Growth is reserved exactly up to the remaining room, and a write that does not fit spills.
  - Tests: one seed peak ≤ cap; three seeds share one cap. Knockout (isolation off): more than 3× the cap.
- **4. D (PD rehome hold)**
  - `RangeStatsV3` gains an optional trailing `seed_pending` byte. Old decoders ignore it, and replies without it never hold.
  - `accounting_ready` was unusable as the signal: it is false whenever a split recount is pending (PD `blocked_input` was 199 of 221 noops in RECOV01-ON). It also stays true after a leaf invalidation, as a test shows.
  - The PD holds new debt re-homes, balance proposals and copy-complete commits while any target endpoint is unreachable or reports `seed_pending`. Aborts are unchanged.
  - The hold lasts at most `rehome_recovery_defer_ms` (default 30 min, 0 disables) per recovery episode, and an expired hold stays expired until recovery clears.
  - Metrics: `pd_recovery_down_nodes`, `pd_recovery_seeding_nodes`, `pd_rehome_recovery_hold`, `pd_rehome_starts_deferred_recovery`, `pd_rehome_commits_deferred_recovery`.
  - Test: seeding and down hold starts and commits, which resume afterwards; the bound expires the hold; knockout `rehome_recovery_defer_ms=0`.
- **6. Minors**
  - A driver exiting at shutdown no longer detaches its stores.
  - `debug_assert` in `spill_seed_op`.
  - Tests: carried seed + spill + unknown-old (exact against a from-scratch seed); late-attached and default-zone-swap stores; `SyncTree` Busy on the control and `serve_sync_tree` paths, plus a no-driver knockout.
- **5. C: not implemented. Design for a follow-up:**
  - **Why it is cheap in principle.** Leaves are per-(replica group, bucket) XORs. Bucket counts, live and range counts do not depend on the group. So a placement change only needs each winner (k, v) in a changed span moved: `leaves[G_old][b] ^= d; leaves[G_new][b] ^= d`.
    - `changed_placement_spans` already yields the exact spans when only the range map changed.
  - **(a) Regroup task.** Replace `invalidate_leaf_seed` in `prepare_zone_seed` with a cooperative `Regroup { old: PlacementFn, spans: [(lo, hi, after, done)] }`. The shard driver runs it under the duty budget.
    - It scans winners with versions page by page. This needs a version-returning bounded page (`winner_count_page` returns only counts).
    - Pages are validated by storage generation, like the recount prefetch.
  - **(b) Write routing.** Leaf updates must use the old placement for keys in the not-yet-scanned part of an unfinished span (`> after`), and the new placement otherwise.
    - A write before the cursor passes k updates G_old, and the scan later moves k's current winner. It is exact because pages are read under one borrow.
  - **(c) Gating.** While a regroup is in flight:
    - dirty-bucket rebuilds and tree serving answer not-ready (a rebuild uses the new placement for every key);
    - an in-flight full seed keeps today's behaviour (reset);
    - a second placement change falls back to full invalidation (prepare and commit are about 60 s apart; a 1M-key regroup at 25% duty takes about 6 s).
  - **(d) Exactness property test.** Random prepare/commit/abort interleaved with writes, forgets, flushes, splits and regroup steps. The drained leaves, counts and live must equal a from-scratch seed under the final placement. The knockout routes unscanned keys with the new placement.
  - **Estimate:** 500+ lines including an engine page API, which is too large for this round.
  - D prevents the RECOV01 case during recovery. C remains the fix for healthy-cluster rehome cost.

### 2026-10-04: review fixes after RECOV02 (agent; uncommitted)
- **M1: the PD hold could hold itself in a healthy cluster.**
  - (a) Copy-complete commits are never held. The commit-hold branch and its `pd_rehome_commits_deferred_recovery` counter are removed. Splits are never frozen by the hold.
  - (b) The trailing `RangeStatsV3` byte is now `recovery_seeding`. A store reopened with runs (`VersionedStore::lazy` with `run_count > 0`) sets `seed_recovery`, and the first published seed clears it. Placement-change, split-recount and unknown-old invalidations never set it.
    - The value set is still 0/1. Round-2 nodes send 1 for any unseeded engine, which a new PD reads conservatively as recovery. Round-2 binaries are unreleased (RECOV02 builds only).
    - Stores without a zone config are excluded (m6).
  - (c) `RecoveryHold` is cumulative per episode. Hold time accrues only between polls that held. An episode ends only after 60 s of consecutive clean polls, and the total hold is capped at `rehome_recovery_defer_ms`.
  - (d) A PD that is not leader resets the hold.
  - Tests:
    - a healthy cluster where the re-home's own placement reseed (flag 0) does not hold its commit, the following split or the next start; the knockout (the round-2 "any unseeded" report) holds the start;
    - recovery seeding or a down node holds starts but never commits, even in a polling round where the hold is active;
    - flapping stays within 13 polls of 5 s against a 60 s bound, with a fresh episode after 60 s clean; a zero bound disables the hold;
    - leadership loss resets the hold; the knockout carries the old term's exhausted budget;
    - node-side: a reopened engine reports recovery until its seed publishes; a placement-change reseed reports 0; a store without a zone config is skipped.
- **m2: journal cap.** The cap is now enforced on reservations, not the lagging gauge.
  - A process-wide `AtomicU64` is reserved by `fetch_update` CAS in 64 KiB chunks before a journal grows its capacity or key bytes, and released at publish or reset.
  - The forced (re-entrant) path overdraws and is accounted.
  - Tests: an 8-thread CAS test never exceeds the cap; three seeds share the cap exactly (journal bytes ≤ reservations ≤ cap).
- **m5:** `DutyBudget` pauses only once the debt reaches 1 ms (smaller debt carries over). At 25% each pause is at least 4 ms. The test asserts the share stays within 0.20–0.27 and the pause count is bounded.
- **m4:** recount cost is charged on every outcome, not only `More`. Remaining edge: the sync-fallback recount still charges its read-gate wait, which over-throttles slightly (the safe direction).
- **m7:** docs updated: `zones.md` (exact recovery definition, episode semantics), `backpressure.md` (exact journal guarantee, debt threshold), `configuration.md`, `formats.md`, `ARCHITECTURE.md`.

### 2026-10-04: round 2 hardware-validated (RECOV02 ×2, FULL11-3N), re-review mergeable, committed
- **RECOV02-ON / ON-R2** (`bench/0363-compact-values/RECOV02-FINDINGS.md`): all gates passed twice.

  | | ON | ON-R2 | RECOV01-ON |
  |---|---:|---:|---:|
  | t80 | 76 s | 72 s | 70 s |
  | R600 | 0.808 | 0.821 | 0.639 |
  | Post-kill mean | 1.28M | 1.27M | 943k |
  | Seeding done after drain end | 175 s | 170 s | |

  - The PD hold deferred 24 starts and 0 commits; peers had 0 placement resets.
  - Pre-final build: the journal peaked 4% over the cap and there were 133k duty pauses, both fixed afterwards.
- **Re-review fixes (M1 PD hold narrowed):**
  - only new rehome starts are held, for down nodes or restart-caused (`recovery_seeding`) seeding;
  - cumulative per-episode bound with 60 s clean hysteresis; leadership reset;
  - the journal cap is CAS-exact; duty pauses only at ≥1 ms debt;
  - plus m4/m6/m7, and the final MINOR (`clear()` resets `seed_recovery`).
  - agent_5 re-review: mergeable.
- **FULL11-3N** (healthy run, final round-2 code without the one-line `clear()` fix; `FULL11-FINDINGS.md`):
  - 0–1.0B 1.661M ACK/s (FULL10 healthy phase 1.599M, 1.04×); whole run 1.403M.
  - lab-2 had two natural events at 1.037B: an **exit 75** (0370 fail-fast; memtable single-bit `xor=0x04`, 3 attempts), then 20 s later a SIGSEGV in glibc `_int_malloc` from `RangeMap::clone` (heap corruption; core kept). Recovery: t80 79 s, R600 0.788.
  - Journal peak 0.999× the cap.
  - Correctness passed (owned aediag 72/72).
  - G1/G3 formal misses: the natural crash, and a ~12 s PD release lag after seeding finished.
- **Follow-up (small):** for ~7 s after a restarted node serves, it runs recovery seeds but the PD sees it as neither down nor seeding, so a start could slip through. Report `recovery_seeding` from reopen (before the first seed starts), or have the PD treat a beacon-detected restart as recovering until its first stats. Filed into 0369.

