---
id: "0476"
title: Range-deleted queue entries stay visible to small-limit head scans for >1 h in the ring-buffer arm (split/rehome churn)
status: done
priority: 1
created: "2026-10-15"
updated: "2026-10-16"
depends_on: []
spec_refs:
    - docs/range-deletes.md
---

## Description
## Found by
0471 arm V-Q2-RING-500000. lab: 3 SPDK nodes × 8 shards, HEAD c4913078 (0472 and 0473 fixes in). Zone `ring` (id 11?), sorted, RF 3, presplit into 64 partitions `p0000/`..`p0063/`. Producers append `p{part:04}/{seq:016}` at 500k msgs/s with W=2. A trimmer range-deletes `[p{part}/, key(part, head − 1,000,000))` every 100 ms per partition, using the 0472-fixed client. Auto-split took the zone from 64 to 4,246 ranges in about an hour, and re-home is default-on.

## Symptom
The head probe scans `[p{part}/, p{part}0)` with limit 10 and R=2 (`Coordinator::scan_in`) once per second on partitions 0, 16, 32 and 48.
- Partition 48: from t≈2,800 s, every probe returned seq 20,770,368 first, while the deleted floor was 20.77M and later 55.3M. It stayed until the end of the run (t=7,210 s).
- Partition 0: from t≈6,400 s, every probe returned seq 48,896,640 first, with the floor at 55.3M.
- In total 5,167 probe violations: a range-deleted key visible to a scan.
- The end-of-run verify scanned every partition from its prefix with limit 4096 pages (R=2) and found `covered_visible: 0`. So the covered key was either visible only to limit-10 scans or had healed by the time of the verify.
- Produce retries (Busy) happened throughout, with no trim errors.

Evidence: /data/dataset/task0471-results/V-Q2-RING-500000 (stdout NQ_SAMPLE deltas, stderr "head probe … returned covered seq"), node logs on the boxes under /home/user/vals0471/node.{log,stderr}, PD log pd.stderr on lab-1.

## Suspects
- A scan merge or cover bug with truncated pages: small limit, covered-work budget, resume keys, fragments trailer windows.
- A re-home or split copy that leaves an uncovered entry on a replica (or on a non-owner shard that still answers).
- The entry being rewritten at a newer version, e.g. by read repair or handoff.

## Acceptance
Root cause identified with per-replica evidence, then a fix with a kill-the-fix test. The netqueue ring arm rerun must show 0 probe violations.

## Acceptance criteria

## Work log
