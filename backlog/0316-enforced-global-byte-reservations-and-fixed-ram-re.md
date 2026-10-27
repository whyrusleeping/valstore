---
id: "0316"
title: Enforced global byte reservations and fixed-RAM rebuild gate for streaming AE seed
status: done
priority: 2
created: "2026-09-27"
updated: "2026-09-28"
depends_on:
    - "0289"
spec_refs: []
---

## Description

Split from 0289. 0289 prototyped an opt-in streaming exact AE seed (`VALS_AE_SEED_MODE=stream`); see bench/0289-ae-seed-ab/report.md. Locally it removed the O(keys) seed winner directory: +1.8 MB seed RSS vs +291 MB baseline at 1M keys. Two parts of the 0289 acceptance were deferred to this task: enforced global reservations/debt, and rebuild completion inside a fixed inclusive RAM envelope.

Scope: a process-wide byte-reservation/debt broker shared across shards, covering serving copies, memtables, AE seed cursors and snapshots, flush/merge builders and duplicated metadata, metadata cache, and SPDK/DMA scratch. Maintenance and AE must reserve before allocating, and must defer or shed rather than overshoot.

Acceptance:
- Run cgroup-capped 8c/32 GiB and 4c/16 GiB local arms (file backend) with the streaming seed.
- Include degraded writes, a reseed/rehome, and multiple flush/merge cycles.
- Report restarts/fallbacks, debt/queue age, useful throughput, and rebuild completion within the envelope.
- Keep measured RSS separate from reserved bytes.
- No hardware or fleet runs without an availability/authorization check. Make no 100B claim.
- Decide whether stream seed becomes the default and whether the baseline seed path can be removed.

## Outcome

Broker ram_broker.rs: charged memtable/run-metadata/DMA bytes; seed/flush/merge reserve before allocating within VALS_RAM_MAINT_RESERVE_BYTES (19% default) and defer; liveness paths overdraw into debt; reserved vs RSS metrics. Stream AE seed is default (baseline opt-out). Local cgroup gate (bench/0316-reserve-gate): exact AE-digest rebuild 266.5 s (8c/32G), 277.5 s (4c/16G), 154.3 s (tight 512M stream), 0 OOM/debt/sheds; tight baseline incomplete at 393 s. Baseline removal deferred (proposed 0330); hardware broker validation proposed 0331. Big arms did not bind; populated rehome unverified.

Commit: ram(0316): process-wide RAM reservation/debt broker, streaming AE seed default, local cgroup gate with exact AE-digest rebuild probe
