---
id: "0124"
title: Box-validate task 0074 with a fresh latest-code bootes soak
status: done
priority: 2
created: "2026-08-03"
updated: "2026-08-03"
depends_on:
    - "0074"
spec_refs:
    - bench/bootes-soak-lab/run-20260802T184023Z/ROOT-CAUSE-0122.md
    - plans/bootes-soak-runbook.md
---

## Description
Re-run the fresh 3-node lab SPDK bootes soak from current HEAD after task 0074. Reproduce the same topology/workload as task 0121 and determine whether budgeted/resumable cleanup eliminated the auto-split migration serving stalls.

## Acceptance criteria
- Sync and rebuild current source (`6e9523f` or newer) on all three nodes before launch; fresh PD and formatted devices, RF2 sorted zones with runbook pre-splits, parity preflight passes.
- Run bootes with fresh backfill state and 300 parallel backfills for at least the prior ~32-minute observation window, while auto-split drives frequent range-map epochs.
- Capture start/end metrics, service/RAM/throughput monitoring, node+PD logs, bootes vals error counts, cleanup progress/error counters, flush/merge health, and map convergence.
- Compare directly to task 0121: 89 retries, 9 dropped batches, 138 failed deletes, 10 cursor failures, and 47–66s shard control unresponsiveness. Conclude whether 0074 fixed the stalls and identify any residual bottleneck.
- Restore `format=true`, stop workload and cluster, and preserve findings/artifacts.

## Work log

- 2026-08-03 setup: synced commit `6e9523f` (source manifest `1f8c33…`) to all three lab boxes, rebuilt SPDK valsd successfully and identically on each, wiped PD raft state, formatted the three scratch devices with `format=true`, created the RF2 sorted zones with 14 runbook pre-split ranges, and passed the 100-key split-range valsgo preflight.
- 2026-08-03 soak: ran 300 fresh parallel backfills for a 45m50s live observation. Auto-split added 45 ranges (14→59, epoch 18→63); all 12 shards converged. All 70 service samples stayed active, RAM peaked at 3.91/6.34/8.56 GiB, and cleanup/server/Busy/qpair/snapshot/compaction errors remained zero. The cluster recorded 81,136,507 replicated writes, 17 run merges, and 13,102 freed segments.
- 2026-08-03 result: task 0074 removed the monolithic cleanup path from profiles (`recovered_versions`, `VersionedStore::keys`, and cleanup `merge_all_runs` absent; `cleanup_with` only 0.18–0.30%), but did **not** eliminate five-second serving stalls. In the matched first 32m29s bootes logged 53 retries, 0 drops, 96 failed deletes, and 0 cursor failures versus task 0121's 89/9/138/10; over 45m50s it logged 85/0/161/0. Worst shard map-adoption spread improved from 47–66s to 11.171s, but still crossed the 5s client deadline.
- 2026-08-03 residual: every range-map epoch invalidates the full placement-grouped Merkle leaf seed. Epochs arrived faster than cooperative full-dataset seeds completed, producing 891/1,072/1,225 seed steps and 291/417/582 seconds of seed-generation wall time; `SeedProgress::fold_key`, `VersionIndex::apply_leaf`, key decode, and paged index scans dominate profiles after the old cleanup path disappeared. Proposed follow-up task 0125 captures coalescing/incremental reseeding. Full evidence: [`bench/bootes-soak-lab/run-20260803T174813Z-post0074/FINDINGS.md`](../bench/bootes-soak-lab/run-20260803T174813Z-post0074/FINDINGS.md).
- 2026-08-03 teardown: bootes again exceeded 120s after SIGTERM and required SIGKILL (task 0123); all nodes and the PD were stopped and every node config was left at `format=true`.
