---
id: "0323"
title: Bound concurrent full AE seeds per node; stop shed/restart thrash above the RAM budget
status: done
priority: 2
created: "2026-09-27"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (SOAK0321-F2 settle phase)

- **Concurrent seeds pushed RSS past the budget.** About 16+ concurrent full AE seeds per node, one per zone store, at 1.4B keys/node took RSS from 20 GB to 36–45 GB. That is above the 36 GiB `VALS_RAM_BUDGET_BYTES`. `ae_seed_ram_sheds` rose to 171–186.
- **Shed seeds restart at once, and RSS does not come back down.** `ae_seed_starts` climbed about 1/s (116 → 324 on node3). RSS stayed above budget, suggesting allocator retention and/or other seeds growing while one shed.
- **Allocations are retained after seeding.** The early 3-node refold also left RSS 6–7 GB higher, and it stayed there for the rest of the run.

## Candidate guardrails (to design and measure; 0322 removes the main trigger, so this is defence in depth)

- A node-wide cap on concurrent full seeds, using a permit held by `SeedProgress`/`StreamSeed` and released on drop. When no permit is available, skip the store until the next sweep instead of spinning.
- Backoff before restarting a seed after a RAM shed.
- Check whether glibc retention (`malloc_trim` after a shed or completion) explains RSS staying above budget.

## Acceptance

Unit tests for permit release on every path. Then a local or hardware reproduction of mass reseeding under a RAM budget: RSS stays at or below budget plus a small margin, with no restart thrash.

## Acceptance criteria

## Outcome

Node-wide RAII seed permits (VALS_AE_SEED_CONCURRENCY, default 4) are released on every reset/drop path; synchronous seeding bypasses them. Adds a 10 s cooldown after a RAM shed, and seed drivers now defer to Busy instead of spinning. Sweep re-kicks accounting for unseeded stores. New metrics: ae_seed_active and ae_seed_permit_deferrals. Adds a rate-limited off-thread malloc_trim. Local seedmass A/B (16 stores, 180 MiB budget): unlimited seeded 0/16 with 48 starts, 48 sheds and a 205.5 MB peak; cap 4 seeded 16/16 with 16 starts, 0 sheds and a 118.1 MB peak. Trim cut retained RSS from 123.6 to 65.9 MB. vals-cluster and vals-server tests pass, and the standard reviewer accepted. Hardware validation is proposed as 0329.

Commit: replica(0323): node-wide AE seed permits, shed cooldown, non-spinning seed drivers, off-thread malloc_trim; seedmass local A/B
