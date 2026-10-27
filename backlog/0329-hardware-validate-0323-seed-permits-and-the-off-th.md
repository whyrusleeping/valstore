---
id: "0329"
title: Hardware-validate 0323 seed permits and the off-thread malloc_trim at multi-GB heap scale
status: done
priority: 3
created: "2026-09-28"
updated: "2026-10-06"
depends_on:
    - "0323"
spec_refs: []
---

## Description
0323 added the following, each validated only in a small local file-backed repro (100k keys/store, 180 MiB budget):
- node-wide AE seed permits (`VALS_AE_SEED_CONCURRENCY`, default 4);
- a 10 s RAM-shed cooldown;
- a rate-limited off-thread glibc `malloc_trim(0)`.

Risk: at the 1.4B keys/node, 36 GiB-budget scale of SOAK0321, `malloc_trim` walks and locks each arena, which could stall shard-thread allocations. A cap of 4 could also lengthen the reseed of all stores after a placement change.

Acceptance, on the next 3-node hardware run that triggers a mass reseed (e.g. a refold or settle):
- report peak RSS against `VALS_RAM_BUDGET_BYTES`;
- report ae_seed_starts/ram_sheds/permit_deferrals and ae_seed_active;
- report the wall time to seed every store;
- report the serving p99/p999 around trim events.

Use that evidence to decide whether to keep the default cap and trim, or tune them.

## Acceptance criteria

## Work log

- 2026-09-29: accepted P3 gate, blocked on fleet health/qualification and explicit reservation/retention approval (0248/0250/0336). Coordinate with 0331/0332 on one suitable existing-harness run, retaining separate criteria and a mass-reseed phase; do not invent a standalone soak. The shared Limiter tests from 0338 do not substitute for hardware trim/latency evidence.
- 2026-10-06: closed as **superseded** (backlog cleanup, owner-approved). 0374 replaced the seeding model: driver seeds under `ae_seed_isolation=1` bypass `ae_seed_concurrency`, so the permit cap no longer governs default seeding. 0379 pins seed inputs. Mass reseeds at scale were observed in RECOV01/02, SOAK5B and SOAK5B-V2 (8 recovery seeds completed in ~4 min under ingest), with RSS reported against the guard. The one thing not measured, malloc_trim's effect on serving tail latency, is not worth a dedicated run; if trim shows up in a profile, the decay task can pick it up.
