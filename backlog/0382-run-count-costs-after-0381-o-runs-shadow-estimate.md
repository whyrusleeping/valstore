---
id: "0382"
title: 'Run-count costs after 0381: O(runs²) shadow estimate per flush, probe_get ignores the key filter (≈1 extra block read per GET at 100 runs)'
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-06"
depends_on:
    - "0381"
spec_refs: []
---

## Description

From the 0381 review (2026-10-06). With the 0381 merge policy, run count grows with data (~100 runs per engine at ~160 GB). Two costs grow superlinearly with it:

1. **Shadow estimate is O(runs²) on the shard thread.** `refresh_shadow_estimates` (`crates/vals-store/src/engine/space.rs` ~442-471) recomputes every run on every manifest-generation bump (every flush and commit) once free ≤ 60%. Per run it does 2 synchronous block reads, then probes every later run's bloom for each sampled entry. At 100 runs that is ~200 sync reads plus ~800k bloom probes per flush per engine, inside `MaintSignals::sample`.
   - Fix: cache per-run estimates and recompute only runs whose newer-run set changed; or sample a bounded subset per tick.
   - Measure shard time before and after at 100 runs.
2. **`probe_get` ignores the key filter** (`read.rs` ~248-292). It prefetches and probes every run's bloom (10 bits/key, ~1% FPR), so at 100 runs that is ~1 extra block read per GET (vs ~0.15 at 15 runs).
   - Consider: consult the 0364 key filter first, so a negative skips all covered runs. It is a superset filter, so a negative is definitive for covered runs.
   - Add a fence/range pre-check before the bloom probe.
   - Measure point-read p50/p99 at 100 runs.
3. **Minor:**
   - `cover_scan_merge_soon_start` (`keyfilter.rs` ~197-205) ignores forced windows;
   - `frozen_run_count` calls `MergePolicy::from_env()` per publish.

**Acceptance:**
- a microbench at 100 runs per engine: shard µs per flush for shadow estimation, and GET latency;
- tests;
- hardware numbers from the next 5B replay (runs per engine, Get p50/p99, shard time).

**Added 2026-10-06 (second 0381 review, agent_1):**
- `vals_run_merge_inner_windows_total` is counted at plan time (merge.rs ~1593-1595), so broker-deferred or failed plans count too (once per tick under RAM pressure). Count at commit, e.g. with an `inner` flag on `WrittenCompact`.
- Non-frozen runs older than the newest frozen run are never recurring inputs again (merge.rs ~666-672). Example: tail `[0.1, 1.0, 0.3, 1.1, 1.0, 0.9]` GiB; the valve merges `(1.0, 0.9)`, the 1.9 GiB output freezes, and the four older tail runs are stranded. Bounded at ~K−1 per frozen run, so still linear. Measure the stranded count per engine with the other run-count costs.

## Outcome

Shadow estimation is no longer O(runs²) per flush: cached per-run samples (2 sync reads only for new runs) with incremental bloom probes, and a bloom-only re-evaluation on newer-run removal, tested equal to a from-scratch reference. In release at 100 runs this cut refresh time ~43× (9.8 ms to 0.23 ms per flush) and bloom probes from 454k to 7.7k. The sample cache is charged to run-metadata resident bytes. Staged GET skips covered runs on key-filter negatives: misses went from 1.10 to 0.019 block reads per GET and p50 from 3.6 to 0.2 µs. Hits are unchanged (proposed follow-up 0394). Minor fixes: MergePolicy cached at open, cover scans avoid the exact recurring/forced window, inner-window merges counted at commit, and new vals_stranded_runs/vals_engine_stranded_runs gauges. Hardware numbers moved to 0393 (needs the 0386 replay). The standard reviewer accepted, and the store/cluster/server suites pass in debug.

Commit: perf(0382): incremental shadow estimate, key-filter-aware staged GET, cached MergePolicy, cover-scan forced windows, commit-time inner-window count, stranded-run gauges; 100-run microbench
