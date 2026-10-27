---
id: "0425"
title: '0369 remainder: AE-window liveness gate in harness, runtime create-zone hold, cleanup cursor/speed, AE sync error logging, PD restart-gap hold, docs, sweep test robustness'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-11"
depends_on:
    - "0369"
spec_refs: []
---

## Description

Split from 0369 (2026-10-11). 0369 shipped the items on files no other session had in flight: item 1 (pending-I/O underflow counter + debug_assert), item 4 (PD restart detection), item 5 (PD error log only), item 6 (StaleEpoch(0) → StaleRing), item 8 (explicit epoch-0 ack), and item 9's pusher_loop epoch-0 gate test. These items remain. Most touch files that were dirty with other sessions' uncommitted work on 2026-10-11 (`node/sweep.rs`, `replica/*`, `maint*`, `net/server/{control,shared}.rs`, `observe.rs`, `docs/{deploy,configuration,zones,algorithms}.md`, `bench/0363-compact-values/run_full1.py`). Before editing, check `git status --short -- <file>`.

Item numbers refer to 0369's description and work log.

- **3. Harness AE windows.** In `bench/0363-compact-values/run_full1.py` and its analyzers, an AE window passes only if `vals_sweep_passes` advanced, or `vals_sweep_pass_age_seconds` stayed bounded, during the window. It must not pass vacuously.
- **5 (docs).** Add a `docs/deploy.md` line: with V4 opaque range maps committed, the PD must run with `VALS_PD_OPAQUE_RANGE_MAPS=1`. Otherwise every range-map push is withheld and restarted nodes stay held. The PD now logs this at error level once.
- **7. Runtime create-zone gap.** A runtime create-zone with `presplit=` leaves a few-ms gap between SetZones and SetRangeMaps. During it, the sweep uses single-range placement on a never-held shard. Hold newly learned sorted zones per zone until their first map push (node side, `shared.rs`/`sweep.rs`), or document why the stray-only worst case is acceptable.
- **9 (rest). Test robustness:**
  - Use per-shard counts instead of the process-wide `sweep_passes` / `sweep_sorted_zones_held` in assertions.
  - Make the hold-only knockout observable (not healed within 300 ms).
  - Drive the planted-stray cleanup test on a polled mock device, so it exercises 0368.
- **10. Over-replication cleanup (L1).** Persist the cleanup cursor, or start it at a random or saved offset. Also run faster when the shard is idle, and/or target known-suspect keys. Measure keys/s and the persisted cursor with file-backed strays at depth N.
- **11. AE sync errors at boot.** Log the reason at info level, rate-limited, and confirm the errors are benign retries. The hardware count is observed in 0418 step 2.
- **12. Restart gap.** For ~7 s after a restarted node is serving, the PD sees it as neither down nor seeding. Fix one of two ways:
  - The node reports `recovery_seeding` from store reopen. First find why `recovery_seed_pending()` reads false during the gap: zone stores not yet open, or a sorted zone skipped while held.
  - Or the PD treats a beacon-detected restart (ring- or catalog-epoch regression) as recovering until it has seen seeding start and finish, or until a bounded timeout.
- **13.** Document the ~2-poll (~12 s) recovery-hold release lag.
- **2.** The L2a idle-restart check stays in 0418 step 2.

Acceptance: each item is implemented with a regression test (L0/L1) or explicitly closed with a reason. Item 2 and the item 11/12 hardware observations remain recorded through 0418.

## Outcome

Item 12 (PD side): beacon_repush returns restarted nodes (ring- or catalog-epoch regression); RecoveryHold keeps per-node restart watches that make range_stats_poll hold starts until a poll has seen the node's recovery seed and then a poll without it, or until max(30 s, 2 polls) pass with none; down polls refresh the grace; leadership loss clears watches. Tests with knockout; 0380 item 7 flaky asserts fixed. Full vals-cluster lib 806 passed. Items 3, 5 docs, 7, 9 rest, 10, 11, 13 and item 12 docs/gauge moved to 0426 (files carry 0371/0372/0377/0403 uncommitted hunks); item 2 and 11/12 hardware observations stay in 0418 step 2.

Commit: fix(0425): PD holds rehome starts for a beacon-detected restart until its recovery seed is seen to finish or a grace passes; de-flake pd hold tests (0380 item 7); rest split to 0426
