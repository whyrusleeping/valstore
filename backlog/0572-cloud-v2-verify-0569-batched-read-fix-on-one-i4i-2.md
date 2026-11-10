---
id: "0572"
title: 'Cloud v2: verify 0569 batched-read fix on one i4i.2xlarge (~$1)'
status: done
priority: 2
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

Owner request 2026-11-10: rerun the single-node cloud trial on the 0569 commit, spend ≤ ~$1-2. Reduced v1 profile (prefill, strong/batched reads, verify-prefill, m1-closed, m2-open50). Check: netprogress multiread quorum_failed gone (v1: 40 / 22), vals_value_read_restarts / vals_value_read_busy counts during the m1/m2 stall windows, no bad reads; record point-read rate vs v1 (72k/s; 0570 frames since). Tear down and sweep afterwards.

## Outcome

Run v2-20261110 (~$0.2 instance + ~$0.05 builder, torn down, sweep clean): m1-closed 1.98M multireads with 0 quorum_failed (v1 40) while vals_value_read_restarts=132 and vals_value_read_busy=0, all reads verified; m2 had no race (0 restarts) and 0 errors. Strong reads 100k/s (v1 72k), m1 110k ops/s (v1 76k); verify-prefill 65k vs 79k keys/s unexplained (different data layout). Profile scripts/cloud/profiles/v2-1node-i4i2xl-reads.json added; findings in bench/cloud/runs/v2-20261110/FINDINGS.md.

Commit: 0572: cloud v2 single i4i.2xlarge rerun on 98da583: m1 0 multiread quorum_failed (v1 40), 132 value-read restarts all re-read, 0 Busy; strong reads 100k/s; v2 read profile added
