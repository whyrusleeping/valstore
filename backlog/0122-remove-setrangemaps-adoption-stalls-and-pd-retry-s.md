---
id: "0122"
title: Remove auto-split migration sweep stalls and PD retry storms
status: done
priority: 2
created: "2026-08-02"
updated: "2026-08-03"
depends_on:
    - "0121"
spec_refs:
    - bench/bootes-soak-lab/run-20260802T184023Z/FINDINGS.md
    - crates/vals-cluster/src/pd.rs#pusher_loop
    - crates/vals-cluster/src/net/server.rs#SetRangeMaps
---

## Description
The fresh 2026-08-02 bootes soak (`bench/bootes-soak-lab/run-20260802T184023Z/`) exposed intermittent serving stalls while auto-split grew the maps from 14 to 110 ranges (96 automatic splits in ~32 minutes). The causal contribution of map adoption is not yet proven: timeout bursts also occurred without a logged PD push failure, so slow control pushes may be a symptom of an already-stalled serving core, an amplifier through eager sweeps, or both.

Every epoch is pushed independently to all 12 shards. Each shard's synchronous `SetRangeMaps` handler replaces maps and calls `kick_sweep`; while serving is stressed, some one-shot PD control connections fail with EAGAIN for 1–66 seconds. The in-load failure windows at epochs 37, 73, and 106 overlap or closely follow valsgo's 5-second data RPC timeout bursts. Bootes ultimately logged 89 batch retries, 9 dropped 5k batches, 138 failed direct deletes, and 10 cursor-save failures despite 0 server errors, 0 Busy, bounded memory, and healthy merge progress.

Counter accounting exposes a separate retry-cadence problem: `vals_pd_push_failures` was already 25,016 before the soak because the 5 ms pusher loop retried immediately-refused startup pushes while nodes were formatting for ~16 seconds; it rose by another 109 to 25,125 during the soak's slower EAGAIN windows. Thus the 25k total is primarily a startup spin, not 25k auto-split failures.

First correlate shard-level stalls, split/adoption epochs, eager sweep work, and the independent flush-plan/flush-commit monolith metrics (task 0094) to establish causality. Then fix the synchronous range-map adoption/push contribution, if any, and bound/back off the retry loop so startup connection refusals cannot hot-loop the PD counter. Likely areas include making adoption cheap/coalesced, decoupling eager sweep work from control replies, coalescing superseded epochs, and per-target retry backoff rather than unconditional 5 ms retries.

## Acceptance criteria
- Instrumentation or a controlled A/B establishes whether `SetRangeMaps` adoption/eager sweep contributes to the 5-second data RPC timeout episodes (distinguished from task 0094 flush monoliths).
- Under a comparable 3-node, 4-shard/node, RF2 sorted-zone bootes load with frequent automatic splitting, the fixed control path does not cause or amplify 5-second data RPC timeout episodes or dropped bootes batches/deletes.
- PD does not hot-loop thousands of `pd_push_failures` while freshly formatting nodes refuse startup connections, nor retry slow range-map targets every 5 ms; retries are bounded/backed off while remaining retry-until-confirmed.
- All shards converge on the latest map epoch and eager migration/healing semantics remain correct.
- Regression tests cover slow/unresponsive range-map adoption and superseded map epochs without blocking raft liveness or losing eventual delivery.

## Work log

- 2026-08-03 attribution: correlated timeout starts (completion timestamp minus valsgo's fixed 5s deadline) against per-shard map adoption, audited the sweep path, resumed the preserved 177.8M-write SPDK dataset, profiled a live 14-minute workload, and cold-profiled node2's startup placement sweep. The dominant multi-second mechanism is the migration cleanup prefix: every changed `range_map_epoch` leads `cleanup_with` to call `VersionedStore::keys()`, which synchronously executes full-shard `Engine::recovered_versions()` and builds/deduplicates the entire run-backed key set before the first 4096-key cooperative yield. Cold perf captured `recovered_versions` (2.88% of all process cycles over 150s), `HashMap::insert` (1.57%), `VersionedStore::keys`, and `sweep_loop`; shard control acknowledgements spread over 16s. Details: [`ROOT-CAUSE-0122.md`](../bench/bootes-soak-lab/run-20260802T184023Z/ROOT-CAUSE-0122.md).
- 2026-08-03 exclusions: ordinary `SetRangeMaps` assignment/adoption completes in a median ~2–3ms; qpair, Busy, server/snapshot/compaction errors stayed zero; sync flush averaged ~0.75ms. Flush plan/commit monoliths remain real (~0.18–0.50s) but did not exceed 1s and are insufficient to explain 5s data timeouts plus 47–66s shard control unresponsiveness. The primary remediation is task 0074's paged/budgeted cleanup with resume cursor; task 0094 remains the sub-second tail follow-up. PD per-target retry backoff remains a separate part of 0122.
- 2026-08-03 completion: added per-kind/per-target exponential push backoff (50ms–2s), immediate bypass for superseding epochs, and regression tests for no hot loop, eventual delivery, and superseded delivery. Fresh 35m37s 3×4-shard SPDK bootes soak after tasks 0074/0125 crossed ~39 map epochs and wrote 82.6M replica records: 0 batch retries/drops, 1 direct-delete timeout, 0 cursor failures, 0 server/Busy/cleanup/compaction errors, and all 12 shards converged. Startup push failures fell from ~25k to 88; load added 11 attempts. Profiles show cleanup/seed no longer dominant; bottleneck shifted to front-coded index/run lookup and task 0094's 128–151ms flush-plan snapshots plus 354–467ms flush commits. Artifacts: [`FINDINGS.md`](../bench/bootes-soak-lab/run-20260803T224721Z-task0122-final/FINDINGS.md).
