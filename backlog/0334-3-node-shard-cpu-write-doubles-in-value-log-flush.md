---
id: "0334"
title: '3-node shard CPU/write doubles in value-log flush + SPDK completion path (12 µs vs 6 µs on 2-node): isolate and fix'
status: done
priority: 2
created: "2026-09-28"
updated: "2026-09-28"
depends_on:
    - "0327"
spec_refs: []
---

## Description

Split from 0327 (bench/0327-3node-depth/FINDINGS.md; raw /data/dataset/task0327-results/).

## Outcome

Mechanism measured: 3-node shard µs/write is device write-completion waiting in the sync path (~0.9 ms sync drain+tail per sync, ~300 µs append queue stalls vs ~35 µs on 2N, ~15% busy-poll during domain FLUSH), not added per-write work. The single fix (cooperative sync-prepare with ordered deferral) lost 5–27% ACK KV/s in both matched windows of both D16/400M ABBA pairs (BASE 835.6/822.8 and 807.6/816.0 vs COOP 608.5/670.7 and 766.3/760.5 k/s; shard ~11–13 µs/write unchanged), so it was rejected. The documented reason no cheap fix exists: the limiter is device write latency before FLUSH, not shard CPU. Master keeps only the neutral phase-1 counters plus the harness and findings in bench/0334-sync-spin. The fix is archived on branch 0334-coop (870901ab/7f0ecac3). Follow-up 0335 is proposed.

Commit: bench/obs(0334): sync-path wait counters + 3-node diagnosis; cooperative sync-prepare fix rejected by matched ABBA (archived on 0334-coop)
