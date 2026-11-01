---
id: "0546"
title: Two-node (lab-1+3) replay profile and a single-box commit-path microbenchmark while lab-2 awaits ECC RAM
status: in_progress
priority: 2
created: "2026-10-31"
updated: "2026-10-31"
depends_on: []
spec_refs: []
---

## Description
Owner 2026-10-31: lab-2 is out until its RAM is replaced with ECC, so benchmark on lab-1 and lab-3 only.

## Work
1. **2-node replay profile:** SOAK5B-V5 lifecycle on lab-1+3 (RF2, each node holds everything) on the 5B cache, with a 2-node layout and sizing, RAM/disk projections and stop rules, and an operator stop at about 4.5–5B ACK. One baseline run on current master (86983dc).
2. **Single-box commit-path microbenchmark:** a vals-bench workload that repeatedly appends a batch and commits it (sync_prepare + device flush) with several concurrent writers on the SPDK engine; run on lab-1 after preconditioning the drive into its post-step state. Report commits/s, commit latency, device MB/s and write ops/s.
3. Use (2) to rank commit-path candidates (coalescing size, more in-flight writes per commit), then confirm the best with (1).

## Acceptance
- A 2-node baseline per-0.5B table with EOF-or-stop correctness (cursor, counts, samplecheck).
- The microbenchmark reproduces the post-step commit-latency regime seen in R8 (about 2–2.5 ms per commit write) and ranks at least two candidates.
- A confirmed keep or reject on 2 nodes for the top candidate.

## Acceptance criteria

## Work log

2026-10-31 (session 1).
- **N2 profile:** `SOAK5B-V5-N2[-Rn]` in run_full1.py and launch-v5.sh (local bench harness).
- **N2 baseline, master 86983dc: PASS**, 0 crashes, 4,290 s to 4.5B. Per 0.5B 1.74 → 1.45M pre-step and ~0.83M post-step (step at ~2.0B). Node1 (lab-1, the weaker drive) carries a flush backlog.
- **`vals-bench commitwrite` added:**
  - It reproduces the post-step regime after ~500 GB of writes (335 MB/s, prepare 3.8 ms).
  - Steady-state ABBA: commit size 1,000 / 2,000 / 4,000 records give 0.915 / 1.13 / 1.63M records/s; the flush wait is half of each commit.
  - 0544's short fio probe was not steady state.
- **Next:** test a pipelined commit (overlap the next commit's writes with the in-flight flush) in the microbenchmark, then confirm on N2.
