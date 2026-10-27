---
id: "0300"
title: Re-evaluate range-map reseed reuse candidate once shard utilization rises
status: done
priority: 4
created: "2026-09-22"
updated: "2026-09-22"
depends_on:
    - "0298"
spec_refs: []
---

## Description
Branch `0297-rangemap-reuse` (commit `277dac39`) keeps logical range counts and the Merkle leaf seed across placement-equivalent range-map epochs (inherited auto-split: 3 full reseeds per split → 1; counters `logical_range_boundary_rebases`, `ae_seed_placement_reuses`; unit tests added; vals-cluster/vals-client suites pass). RANGEMAP01 CAND-01 proved it removes the work (invalidations 24→12, seed starts 24→16, folds −37%) with +0.3% throughput because shard threads were ~35% busy on the one-lane client — measured-neutral, so not adopted per cleanup doctrine.

Revisit only if, after the stage-3 concurrency work (0298) raises shard utilization, reseed CPU/wall (`ae_seed_wall_us`, fold ops per ACK) becomes a measured limiter or seed staged bytes become a debt problem. At 1M KV/s with a 1M-entry split threshold the current full-store refold per epoch is O(N) per split (quadratic overall), so a range-local recount would eventually be required; that design is not authorized by this note.

## Acceptance criteria

## Work log

### 2026-09-22 — resolved: re-evaluated under a depth-4 pipelined client (PRESSURE01), +8.3%/+14.9% in two matched pairs, adopted as `59d6b7bb`. Range-local recount for the remaining O(store) refold per boundary change stays a future design question, not authorized here.
