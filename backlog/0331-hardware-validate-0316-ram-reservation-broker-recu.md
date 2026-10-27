---
id: "0331"
title: 'Hardware-validate 0316 RAM reservation broker: recurring flush/merge deferral vs write throughput at 1.4B keys/node'
status: done
priority: 3
created: "2026-09-28"
updated: "2026-10-06"
depends_on:
    - "0316"
spec_refs: []
---

## Description
0316 added a process-wide reservation broker, `crates/vals-cluster/src/ram_broker.rs`. Recurring (paced) flushes and merges, and the cooperative seed, now reserve before they start. They defer when either of these is true:
- the maintenance pool is full. The pool is `VALS_RAM_MAINT_RESERVE_BYTES`, default 19% of the RAM budget.
- RSS is over budget.

Before 0316, local flush and merge stayed admitted over budget. Liveness paths overdraw into debt instead of deferring. These are pressure flushes, flushes at 2× the per-engine budget, pressure/forced/GC merges, and the sync seed fallback.

The merge estimate is 40 B per input entry plus 2 MiB. It is unvalidated at scale. The local gate never bound the 32/16 GiB arms, which peaked at about 0.7 GB RSS.

Risk: at SOAK0321 scale (about 1.4B keys per node, 36 GiB budget), deferred recurring merges could grow run count and cost write throughput. Throughput is the top perf priority.

Acceptance: on the next authorized 3-node hardware run (it can share 0329's run), report:
- `ram_reserve_deferrals`, `ram_reserve_debt_bytes` and `ram_reserve_oldest_wait_ms`;
- `ram_reserved_*` against RSS;
- run counts;
- ACK throughput against the 0321-F4 baseline.

Retune the pool default or the merge estimate if deferral costs throughput.

This task needs owner authorization and box availability.

## Acceptance criteria

## Work log

- 2026-09-29: accepted P3 gate, blocked on fleet health/qualification and explicit reservation/retention approval (0248/0250/0336). Share one suitable current-harness run with 0329/0332, with enough pressure to bind reservations and separate acceptance reporting. Do not count a nonbinding low-RSS arm as validation or build a new harness for this ticket.
- 2026-10-06: closed as **superseded** (backlog cleanup, owner-approved). The broker has been exercised at scale:
  - SOAK5B: merge reservations grew to 4–8 GB against the 8.71 GB pool; node2 at 3.9–4.0B keys had 977 deferrals, and merges per 100M fell 30 → 13.
  - V3-R2: late deferrals (+1,669 in 6 min, 12–15 GB reserved).
  - 0381 changed the policy: recurring/forced merges use `try_reserve` and defer; only reclaim merges overdraw.
  - Whether broker deferral contributes to throughput decay is a candidate in the write-throughput decay task.
