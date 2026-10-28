---
id: "0480"
title: 'Cleanup reclaim still lags re-home inflow under ring load: target moved spans instead of the store-wide lap'
status: done
priority: 3
created: "2026-10-16"
updated: "2026-10-20"
depends_on: []
spec_refs: []
---

## Description
## Found by
0478 hardware arm V-Q2-RING-500000-R4 (lab, 3 SPDK nodes x 8 shards, ring 64 x 1M at 500k msgs/s, 1 h).

## Observed
With the 0478 batched owner digest probe, former owners do forget re-homed copies during the arm (`vals_cleanup_forget_keys` 1.5M–2.4M per node in 1 h, 0 before), but reclaim lags the re-home inflow: 44 re-homes copied ~8.8k pages x 4096 entries per node, cleanup released ~2.8M keys per node during load and kept draining afterwards (~3k keys/s per node idle; stray former-owner listings 247 -> 213 in 30 min idle). Cleanup pace is now set by the lap: one 8,192-key page per zone per sweep pass under load, the lap walks every winner including covered ones, and a forget needs a lap wrap or 32 MiB of accumulated keys.

## Idea (to be measured first)
Target the moved spans: when a shard's placement changes (CleanupPlacement already computes the changed spans), scan those spans first instead of waiting for the store-wide lap, and/or allow several pages per pass while every page is fully probe-confirmed. Measure whether the remaining space/RSS growth in ring arms comes from these strays or from 0475/0477 before building.

## Acceptance criteria

## Work log

- 2026-10-20: closed without implementation (owner accepted). 0485 made the inflow go away: on its R15 default-config 2 h ring arm there were zero debt re-homes, `vals_live_keys` stayed flat at 64.04M and RSS and disk stayed flat (bench/0485-rehome-decisions/FINDINGS.md). Reopen as a new task only if a workload that re-homes a lot shows stray former-owner copies building up again.
