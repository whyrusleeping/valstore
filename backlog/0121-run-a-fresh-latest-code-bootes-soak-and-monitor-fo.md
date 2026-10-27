---
id: "0121"
title: Run a fresh latest-code bootes soak and monitor for bottlenecks
status: done
priority: 2
created: "2026-08-02"
updated: "2026-08-02"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
Stand up a clean 3-node lab SPDK cluster from the current valstore source, launch a fresh bootes firehose+backfill soak, and actively monitor throughput, errors, backpressure, memory, merge/flush progress, range splitting, qpair allocation, and other emerging bottlenecks. Preserve quantitative artifacts/findings and stop or intervene if cluster health degrades.

## Acceptance criteria
- All three boxes are synced and successfully rebuild current source with the SPDK feature before launch.
- PD raft state and all SPDK data devices are freshly initialized; records/backlinks zones are created with the runbook pre-splits and RF2.
- Client parity preflight passes on a split range before bootes starts.
- Bootes starts from fresh backfill state and runs long enough to expose sustained-load behavior.
- Monitoring captures load/throughput, vals/client errors, Busy rejections, node memory, flush/merge progress, and notable log warnings; findings identify any bottleneck or support a healthy conclusion.

## Work log

- 2026-08-02 validation: synced a source manifest hash of `6e550d…` to all three boxes, rebuilt SPDK valsd successfully on each, wiped PD raft state, formatted all three Samsung 9100 PRO scratch devices, created RF2 sorted zones with the runbook pre-splits, and passed the valsgo 100-key split-range parity preflight.
- 2026-08-02 soak: ran bootes for a 32m29s live capture (300 fresh parallel backfills), recording 154,974,491 replicated write observations after the start snapshot. All 60 monitor samples kept all nodes active; RAM peaked at 4.20/6.19/6.18 GiB. Merges and auto-splits progressed with zero server, Busy, qpair, snapshot, and compaction errors.
- 2026-08-02 finding: repeated five-second shard RPC stalls caused 89 batch retries, 9 dropped 5k batches, 138 failed direct deletes, and 10 cursor failures. Frequent range-map adoption and task-0094 flush monoliths are leading suspects; task 0122 requires an instrumented causal A/B. Bootes also failed to exit within 120s of SIGTERM and required SIGKILL (task 0123). Full evidence: [`bench/bootes-soak-lab/run-20260802T184023Z/FINDINGS.md`](../bench/bootes-soak-lab/run-20260802T184023Z/FINDINGS.md).
