---
id: "0572"
title: 'Cloud v2: verify 0569 batched-read fix on one i4i.2xlarge (~$1)'
status: in_progress
priority: 2
created: "2026-11-10"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description
Owner request 2026-11-10: rerun the single-node cloud trial on the 0569 commit, spend ≤ ~$1-2. Reduced v1 profile (prefill, strong/batched reads, verify-prefill, m1-closed, m2-open50). Check: netprogress multiread quorum_failed gone (v1: 40 / 22), vals_value_read_restarts / vals_value_read_busy counts during the m1/m2 stall windows, no bad reads; record point-read rate vs v1 (72k/s; 0570 frames since). Tear down and sweep afterwards.

## Acceptance criteria

## Work log
