---
id: "0381"
title: 'Forced merges ignore the input cap (.min(n-2)) → huge runs rewritten on every flush; write amplification grows with data (SOAK5B-V3-R2: merge output/write 195 → 1,026 B)'
status: done
priority: 1
created: "2026-10-05"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
## Evidence (SOAK5B-V3-R2, running; node1 observer, master a6c28570)
After the drive step, device write bandwidth is roughly flat (0.41–0.52 GB/s per node). But device bytes per synced write grew from 598 to 1,467, and **merge output per write from 195 to 1,026 B**, so the write rate fell 1.10M → 281k per node over 6 → 91 min.

- `vals_shadow_folds` = 0 and `vals_pressure_merges` = 0: all merges are ordinary recurring merges. 1,565 merges, input ≈ output (1.36 / 1.34 TB): creates-only, no garbage.
- `vals_merge_in_flight_input_bytes` reached **15.7 GB for 2 merges**, far above `VALS_MERGE_INPUT_CAP_BYTES` (2 GiB).
- From ~79 min, the RAM-reserve pool also started deferring merges (`vals_ram_reserve_deferrals` +1,669 in 6 min; reserved merge bytes 12–15 GB).

## Suspected bug
`select_recurring_merge` (`crates/vals-store/src/engine/merge.rs` ~587-608):
- When `n >= force_runs` (default 12 = 2×K) and the capped suffix has < min_k runs, it forces `select_merge_suffix(sizes, k, ∞, size_cap).min(n - 2)`.
- The `.min(n - 2)` **forces at least the two newest runs even when they exceed the cap**.
- Frozen runs accumulate at ~1–2 GiB each, so at multi-billion-key scale every engine sits at ≥12 runs. After each flush, the new small run is merged into the large output of the previous forced merge.
- That run is rewritten on every flush and keeps growing: ~quadratic write amplification.
- Forced merges are also not paced (`paced: !selection.forced_backlog`, `merge.rs` ~1428), so they compete with ingest at full rate.
- The same rule probably contributed to the late-run decline in the earlier full-cache runs (FULL1 reached ~21 runs per engine at 2.09B on 2 nodes).

## Fix
- Never force a merge whose input exceeds the cap. If no ≥2-run window fits under the cap, either:
  - merge the cheapest adjacent window that fits (e.g. the smallest adjacent pair or tier under the cap), or
  - skip, and let the run count grow.

  Write cost is now cheap per run: the 0364 key filter gives 99%+ negatives, and 0378 makes fence/filter RAM proportional to entries, not runs.
- Revisit `force_runs` (12) and whether forced merges should be paced.
- Consider making the policy explicitly size-tiered with a frozen top tier: runs at or above the cap are never rewritten by recurring merges; only shadow/GC-driven folds (space reclaim for overwrite/delete workloads) may rewrite them.
- Keep the costs of more runs bounded and measured:
  - point reads / overwrites (key-filter positives) probe each overlapping run's bloom;
  - scans, AE seeds, cover-scans and recounts are k-way over all runs;
  - recovery opens every run.

## Acceptance (preregister)
- **Unit/property tests:**
  - no recurring/forced merge has input > cap;
  - write amplification under a creates-only flush sequence stays bounded (O(log_T(cap/flush)) rewrites per byte, not growing with data);
  - run count grows ~linearly with data ÷ cap;
  - knockout: the old `.min(n - 2)` rule.
- **Hardware (5B-cache replay, same setup as SOAK5B-V3-R2):**
  - merge output per synced write stays ≤ ~300 B after the drive step (R2: rose to 1,026);
  - device bytes per write ≤ ~700;
  - post-step ACK ≥ 1.5× R2 at matched progress;
  - runs per engine and point-read latency reported;
  - correctness passes.
- Optional quick config probe first: `VALS_MERGE_FORCE_RUNS=64`.

## Acceptance criteria

## Work log

### 2026-10-06: implementation (uncommitted, local only)
**Policy** (`engine/merge.rs`: `MergePolicy`, `select_recurring_merge`, `select_forced_window`):
- **Cap.** No recurring or forced merge reads more than `VALS_MERGE_INPUT_CAP_BYTES`.
- **Frozen runs.** A run is frozen when it is ≥ `VALS_MERGE_FREEZE_BYTES` (new; default = the cap), or when size × (1 + 1/r) > cap, i.e. too large for any balanced window under the cap (1.2 GiB at the defaults). Frozen runs are never recurring or forced inputs.
- **Tiering.** Tiered suffix selection works only on the active tail, the runs newer than the newest frozen run.
- **Valve.** It now counts active-tail runs, not total runs (`VALS_MERGE_FORCE_RUNS` default 12 → 6 = K).
  - It takes the cheapest adjacent window of 2..K tail runs under the cap: fewest input bytes per eliminated run, newest then widest on ties.
  - It tries balanced windows first (largest ≤ r × rest), then any window under the cap. If nothing fits, there is no merge.
- **Adjacency.** It is required: `entry_shadowed` and shadow folds assume later run = newer, and `compact_commit` puts the output at the first input's position.
- **Tombstone GC and cluster-all co-compaction** now need a window that covers every run (`start == 0 && end == n`).
- **Pacing.** Every recurring merge is paced, forced ones included. Pressure, shadow folds and vlog GC stay unpaced.
- **Space-reclaim paths are unchanged** and still rewrite frozen runs:
  - pressure: capped suffix; at the floor it may cross the cap with the minimal two-run suffix;
  - shadow fold: may cross the cap, gated on gain ≥ max(8 segments, churn/4);
  - aged vlog GC: one run;
  - lone-run GC: only when the whole set fits under the cap, or a single run;
  - forget: all runs, uncapped.

**Metrics:**
- `vals_run_merges_by_kind_total{kind}` and `vals_run_merge_input_bytes_by_kind_total{kind}`, with kind = recurring/forced/pressure/shadow/vlog_gc/lonegc/forget;
- `vals_frozen_runs`;
- `vals_max_run_bytes`.

**Simulation.** Micro-simulation (`engine/tests/policy.rs::merge_policy_micro_simulation`; ~200 MiB flushes, 2 GiB cap). Merge input per flushed byte:

| data/engine | pre-0381 | 0381 | runs (0381) | max merge input (pre-0381 / 0381) |
| --- | --- | --- | --- | --- |
| 10 GB | 1.45 | 1.45 | 8 | 1.71 / 1.71 GiB |
| 40 GB | 33.3 | 1.50 | 26 | 21.8 / 1.97 GiB |
| 100 GB | 166 | 1.53 | 63 | 77.2 / 1.98 GiB |
| 160 GB | 307 | 1.53 | 101 | 133.8 / 1.98 GiB |

**Valve variants.** Scratch comparison, now removed; columns are merge input per flushed byte and runs at the end:

| valve | uniform 157 GB | log-uniform 1 MiB..512 MiB flushes, 328 GB | geometric ×4 cadence, 286 GB |
| --- | --- | --- | --- |
| counts all runs, ≥12, balanced | 2.25 / 96 | 1.90 / 211 | 1.99 / 204 |
| no valve | 1.53 / 103 | 1.47 / 264 | 1.00 / 405 |
| tail-12 | 1.53 / 103 | 1.48 / 263 | 1.40 / 285 |
| tail-6 (chosen) | 1.52 / 104 | 1.66 / 209 | 1.92 / 138 |
| counts all runs, unbalanced cheapest-capped | 5.00 / 80 | 9.97 / 170 | — |

A total-run trigger is permanently on once the frozen runs alone exceed it. At that point it buys about 7% fewer runs for about 47% more merge input. Tail-6 matches no-valve on uniform flushes and has the fewest runs on the pathological shapes.

**Tests:**
- new `engine/tests/policy.rs`: a property test over 96 random cases (cap, freeze, K, ratio, flush shapes, injected oversized runs) checking every merge stays under the cap, takes no frozen input and has fan-in ≤ K; WA ≤ log_{1+1/r}(cap/min_flush)+1 and flat in data; run count linear in data; a knockout of the pre-0381 rule (merge input > 10× cap, WA grows 1.8× per 2× data); a valve pathology test; and window-choice unit cases;
- `merge.rs`: a non-suffix forced window merges in place with LWW reads correct and no tombstone GC; a frozen base is never an input and forced merges are paced and kinded;
- `reclaim.rs`: `frozen_runs_keep_overwrite_and_delete_garbage_bounded` (1 MiB cap, a frozen base present at 100/100 samples, 200k overwrites and deletes). Results: no rejections, true dead ≤ 0.274, 18 shadow folds, 18 vlog GCs, 12 pressure merges; recurring merges stay ≤ cap and paced.

Replacing production code with the old `.min(n-2)` rule fails 6 of the new tests.

`floor_multi_run_state_reclaims_via_aged_vlog_gc` and `aged_dead_vlog_gc_reclaims_multi_run_state_pre_floor` used the cap-crossing forced fold as setup. They now apply their 32 KiB cap after the setup fold.

**Suites (debug):** vals-store, vals-cluster (lib + integration) and vals-server all pass. The exception is the known-flaky `daemon::three_node_cluster_with_pd_serves_zones_and_survives_restart`: `server_ops` was 99 against a threshold of ≥100, and it passed 2 of 3 isolated reruns. `cargo check --workspace --all-targets` and `cargo check -p vals-spdk --features spdk` are clean.

**Open:** the hardware acceptance (5B-cache replay) is still to run.

### 2026-10-06: review follow-ups (uncommitted)
- **RAM broker** (`node/maintenance.rs::reserve_merge`):
  - Only reclaim merges overdraw: pressure, shadow fold, value-log GC, lone-run GC and forget.
  - Recurring and forced merges now use `try_reserve`, so they defer when the broker says no, and the deferred plan frees its segments. Before this, every merge overdrew once there were ≥ `run_compact_threshold` runs, which is always true with a frozen tier.
  - The run-count term in `zone_maint_due` still makes a plan attempt every tick; when nothing is selectable that attempt is a no-op.
  - Tests: `recurring_and_forced_merges_reserve_normally_while_reclaim_overdraws`, `many_run_engine_defers_recurring_merges_under_ram_pressure_until_capacity_returns`.
- **Log arguments.** The swapped arguments in the merge log line are fixed.
- **Stress tests** (build verify on; 2 compaction lanes plus a flush lane):
  - **Engine level, `engine/tests/stress.rs`:** 4–8 engines on regions of one shared disk. A mock device completes serving writes late and out of order. The run mixes flushes, forced/pressure/shadow/vlog-GC merges, AE-style run pins and reaps, and TRIM `dealloc_step`.
    - Scale: 8 runs × 20k ops.
    - Checks: segment-ownership invariants at every plan and commit (counting in-flight outputs); full reopen and CRC block scans of every published run; reads and scans against a reference model with overwrites and tombstones.
    - Harness sensitivity: freeing or scribbling an in-flight output is caught by a commit conflict or a CRC read error.
  - **Node level, `node/merge_stress_tests.rs`:** real `snapshot_loop`/`maint_loop`, 6 zones, 3 × 20k applies.
- **Real bug found (it predates 0381).** `Engine::truncate` (liveness `wipe_all_zones`) ignored in-flight off-core flushes and merges. It freed their reserved outputs and inputs while the merge kept running. Two effects:
  - the merge failed with a `crc mismatch` reading its wiped inputs;
  - the merge's output writes could land on segments that new post-wipe data had reused.
  - Knockout `truncate_waits_for_an_in_flight_off_core_merge` reproduces the corrupted post-truncate value.
  - Fix: `truncate` returns `EngineError::MaintenanceInFlight`, and `wipe_all_zones` refuses before recording the wipe, then retries on the next sweep (`liveness_wipe_defers_while_an_off_core_merge_is_in_flight`).
  - It reaches the V4 incident only if lab-2 performed a liveness wipe (`vals_wipes`).
- **New counter:** `vals_run_merge_inner_windows_total`.

### 2026-10-06: review (agent_2: OK after broker fix), SOAK5B-V4 partial hardware run, fixes, stress tests; committed
- **Review:** correctness verified. Newest-wins is by version on every path, middle-window placement keeps time order, and no tombstone is dropped above an older value (scratch model test). MAJOR: the RAM-broker bypass at ≥ 8 runs → fixed: only pressure/shadow/vlog-GC/lone-GC/forget merges overdraw; recurring/forced merges reserve and defer. Follow-ups filed as 0382 (O(runs²) shadow estimate; `probe_get` ignores the key filter) and 0383 (frozen-run garbage reclaim for overwrite/delete workloads).
- **SOAK5B-V4** (pin 2786c97f, valsd f8a8ce68…; `bench/0363-compact-values/SOAK5B-V4-FINDINGS.md`), to 2.665B ACK:
  - **G1:** merge output/device bytes per synced write 172–235 / 572–628 (V3-R2 266/691 by 2.5B, rising to 329–388 / 760–820 after);
  - **G3:** max merge = cap + 1 MiB segment rounding, vs V3-R2's 47.6 GB;
  - 75–85 frozen runs per node;
  - whole-run 1.331M ACK/s (V3-R2 1.343M at matched ACK).
  - **Stop:** two build-verify failures on lab-2 within 0.56 s (zones 2 and 1, unpublished). They followed a heap-corruption SIGABRT and two single-bit exit-75s on lab-2 in the 30 min before; lab-1/3 had zero issues under more forced merges. G1 (post-step), G2 and G4 were not evaluated.
- **lab-2 memtest:** 2 h, 8 × 4400M, 0 failures (`/data/dataset/task0363-results/MEMTEST-lab-2/`). Rules out an lab-3-level rate but not lab-1-level; the hugepage/DMA pool is untested. The V4 stop is unresolved, leaning hardware.
- **Stress tests (no 0381 race found):**
  - engine `engine/tests/stress.rs`: 8 seeds × 20k ops, 4–8 engines on one disk, out-of-order mock completions, 2 lanes, TRIM, pins, build verify; 400 forced + other merges, 188 cross-engine overlaps, ~1,000 run CRC scans; 0 failures;
  - node `node/merge_stress_tests.rs`: real loops, 6 zones, 3 seeds × 20k; ~1,000 merges each, 22–24 inner windows; 0 failures.
- **Pre-existing bug found and fixed:** `Engine::truncate` (liveness wipe) ignored in-flight off-core flushes/merges, so a merge could read wiped inputs or write over reused segments. It now returns `MaintenanceInFlight` and the wipe retries on the next sweep (knockout test). Not the V4 cause: `vals_wipes` = 0 on lab-2.
- Merge log argument order fixed; new `vals_run_merge_inner_windows_total`.

### 2026-10-06: in_review (backlog cleanup, owner-approved)
The code is committed and reviewed. The pre-step part of G1/G3 passed in V4. What's left is the post-drive-step hardware acceptance (G1 after the step, G2 ≥ 1.5× R2, G4). That is now part of the full-replay and write-throughput decay tasks. lab-2's RAM is known bad and will not be replaced, so judge it under that run's stop rules.

### 2026-10-06: second independent review (agent_1, opus): CLOSE WITH FOLLOW-UPS; closed
- **Verified sound:**
  - Inner windows: every read path picks winners by version. `compact_commit` keeps time order. Tombstone GC and cluster-all co-compaction only run on a whole window, rechecked at commit. Pins and key-filter coverage work by handle or per run. Headroom shrink only moves `start`.
  - Frozen classification is not persisted.
  - Broker deferral frees everything it allocated, with no livelock.
  - `Replica::clear` returns before resetting state on `MaintenanceInFlight`.
  - Policy property and knockout tests pin the claims.
- Debug suites pass on 36e57156: vals-store lib + integration; vals-cluster lib + integration.
- **New MAJOR (liveness, not safety):** under sustained ingest the liveness wipe can starve, because some zone always has a flush or merge in flight. Filed as a separate task.
- **Minors, moved to 0382:** `vals_run_merge_inner_windows_total` counts at plan time; non-frozen runs older than the newest frozen run are stranded (bounded at ~K−1 per frozen run).
- **Acceptance, stated plainly:**
  - G3 passed in V4 (max merge = cap + segment rounding).
  - G1 held pre-step (172–235 B).
  - **G2 (≥ 1.5× post-step ACK) and G4 were not passed; they are waived to 0386.** If 0386 shows no post-step gain, revisit under 0385.

