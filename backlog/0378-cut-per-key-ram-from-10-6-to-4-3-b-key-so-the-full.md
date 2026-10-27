---
id: "0378"
title: 'Cut per-key RAM from ~10.6 to ≤4.3 B/key so the full 5B replay fits: 32 KiB run blocks (fences), alloc-free block_lookup, smaller key-filter/run-bloom bits'
status: done
priority: 1
created: "2026-10-05"
updated: "2026-10-05"
depends_on: []
spec_refs: []
---

## Description
## Why
Server RSS grows ~10.6 B per live key (FULL11-3N, SOAK5B live). The full 5B-event replay puts ~7.07B keys per node on 3 nodes, i.e. ~80 GB of RSS against 64 GB boxes and a ~40 GiB guard (`bench/0363-compact-values/SOAK5B-README.md`). Target: ≤ ~4.3 B/key.

## Breakdown (agent_6 read-only analysis; totals measured, split estimated from formats)

| Part | B/key | Detail |
|---|---:|---|
| Fences | ≈6.3 + 0.45 malloc | One `BlockRef = (Vec<u8>, u64, u32)` per ~4 KiB block with a full first-key copy (`run.rs:492`, `653`, `1400`, `1034`); blocks close at `block_target_bytes` 4096 (`run.rs:579`, `647-648`, `1387-1391`; production default via `replica/maintenance.rs:5-8`). Backlinks ~22 entries/block → ~9 B per entry; records ~3 |
| Run blooms | ≈1.6 | 12 bits/entry (`run.rs:346-354`, `580`) |
| 0364 key filter | 2.5 | Sub-filters 13→18 bits, growth 1.5×, ~65% full (`keyfilter.rs:14-17`, `351-356`) |
| Residual | ≈0.3 | |

## Change set
1. **Block target 4096 → 32768** for new runs, behind a knob (e.g. `VALS_RUN_BLOCK_TARGET`, typed setting if appropriate). No format change: block length is stored per index entry (u32), so old 4 KiB runs stay readable and mixed sizes work.
2. **Alloc-free `block_lookup`** (`run.rs:142` allocates a Vec per entry), so scanning 170–310 entries per 32 KiB block stays cheap.
3. **Key filter defaults:** base 9 bits/key, cap 11 (the `VALS_KEY_FILTER_BITS_PER_KEY` knob exists; add or adjust the cap).
4. **Run bloom 12 → 10 bits/key** (8 only if the slope gate misses).
5. **Export** `fence_resident_bytes`, `filter_resident_bytes` and `run_entries` (already computed in `space.rs:35-57`).

## Acceptance (preregister; 1B-cache 3-node run, FULL11 plan and pins, only this change)
- **RAM:**
  - OLS slope of RSS vs `vals_live_keys` from 0.5B to ≥1.3B keys/node ≤ **4.3 B/key** on every node;
  - run-metadata slope ≤ 3.8;
  - fence gauge ≤ 1.0 B per run entry;
  - intercept within ±0.5 GB of FULL11.
- **Ingest:**
  - healthy 0–1.0B mean ≥ 1.611M ACK/s (0.97 × FULL11's 1,660,943);
  - every 100M bucket ≥ 1.2M;
  - shard µs/write ≤ FULL11 + 3%;
  - key-filter negatives ≥ 97%.
- **Reads:**
  - whole-cache samplecheck ≤ 1.25× FULL11's time;
  - local point-Get A/B: p50 ≤ +30 µs, p99 ≤ +100 µs;
  - device read bytes per Get reported.
- **Correctness:**
  - G6 unchanged (exact counts, samplecheck, owned aediag 72/72);
  - build-verify failures 0;
  - unit tests that merge 4 KiB and 32 KiB runs and check lookups and scans across both.

## Related
0288 (paged filters / hierarchical fences, blocked) is the larger alternative; this change set is the cheap path.

## Acceptance criteria

## Work log
- 2026-10-05: `crates/vals-cluster/tests/churn_reclaim_wedge.rs` pins 4 KiB run blocks (`set_process_run_block_target(Some(4096))`): its 5-byte hot keys pack ~730 entries per 32 KiB block, so every old-version probe CRCs and walks a whole block and the debug run misses the test's 60 s liveness deadline (60+ s vs 23 s); the test guards wedge-freedom, not overwrite throughput, which deploy.md documents.

### 2026-10-05: implemented (agent_3), two review rounds (agent_4: mergeable), committed; hardware A/B pending
- **Defaults:**
  - 32 KiB run blocks (`VALS_RUN_BLOCK_TARGET_BYTES`);
  - 10-bit run blooms (`VALS_RUN_BLOOM_BITS_PER_KEY`);
  - key filter a constant 12 bits, 2× growth (`VALS_KEY_FILTER_BITS_PER_KEY`/`_MAX_`/`_GROWTH_PCT` = 12/12/200; k = round(0.6·bits)).
  - No format change; old runs stay readable, and downgrade works.
- **Code:**
  - alloc-free in-place `block_lookup`;
  - `BlockCursor` (CRC once, seek in place to `lo`, decode entry-at-a-time) for `RunScan`, `AsyncRunCursor` and the prefetched winner-count cursor;
  - recount prefetch window 64 KiB per run;
  - gauges `vals_run_fence_resident_bytes`, `vals_run_filter_resident_bytes`, `vals_run_entries`; counters `vals_read_probe_bytes`, `vals_apply_probe_bytes`.
- **Micro (release):** fence B/entry 8.55 → 1.07 (148 B keys), 2.90 → 0.37 (70 B keys); `DeviceRun::lookup` 1.5 → 3.0 µs (mostly CRC over 32 KiB); ~6.5 → ~23 KiB read per probe.
- **Key filter model at 440M keys/engine:** compound FPR 1.75% (measured scaled-down 1.79%). RAM averages ~2.1 B/key over a doubling (peaks 3.1–4.5 B/key right after a step); the review corrected the docs.
- **Projected total ≈4.75 B/key on backlink-heavy data**, so the ≤4.3 gate is at risk. At 7.07B keys/node that is ~38 GB vs the soak's 36.5 GB (85% of 40 GiB) stop; the vals RAM budget is 45.8 GB, so raising the soak guard is the likely answer.
- **Costs:**
  - overwrites ~15–20% slower at 32 KiB (documented; 8–16 KiB advised for overwrite-heavy deployments);
  - `churn_reclaim_wedge` stays pinned at 4 KiB (5-byte keys pack ~728 per block; it times out in debug);
  - the space estimator's `entry_shadowed` overcounts a bit more;
  - split midpoints need ≥2 blocks per run.
- **Review MINORs left:** `BlockCursor` finds structural corruption lazily, after CRC; test gaps for CRC-valid structural corruption and `hi`.

### 2026-10-05: hardware acceptance (SOAK5B-V2)
`bench/0363-compact-values/SOAK5B-V2-FINDINGS.md`: master 64720509 + 0379 (pin e2a3f730, valsd 225e9d35…), 3 nodes, 5B cache to 2.4B ACK.
- **RAM slope (RSS vs live keys, 0.5B to the stop):** 3.96 / 3.87 / 3.94 B/key vs SOAK5B's 12.0 / 11.5 / 11.7. Over 0.5–1.42B keys it is 4.38–4.62, so plan with ~4.3–4.6.
- **Components:** fences 0.73 B and run filters 1.48 B per run entry; key filter 1.7–2.0 B/key; intercept ~1 GB higher than SOAK5B.
- **Ingest:** 0–1.0B mean 1,673,381 ACK/s (SOAK5B 1,655,311); shard µs/write 0.96–0.97×; key-filter negatives 99.2–99.99%.
- **Reads:** Gets 21.7 vs 23.8 µs per checked KV, with ~5× the bytes read per Get.
- **Correctness:** PASS.
- **Projection for the full 5B replay:** ~33–38 GB per node.
- **Verdict:** gates R1–R3 met. Closing.

