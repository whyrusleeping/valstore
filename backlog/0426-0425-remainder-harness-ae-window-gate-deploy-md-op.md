---
id: "0426"
title: '0425 remainder: harness AE-window gate, deploy.md opaque-map line, runtime create-zone hold, cleanup cursor/speed, AE sync error logging, sweep test robustness, hold docs + restarting-nodes gauge'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-12"
depends_on:
    - "0425"
    - "0371"
    - "0372"
    - "0377"
    - "0403"
spec_refs: []
---

## Description

Split from 0425 (2026-10-11). 0425 shipped item 12 on the PD side only (pd/*.rs were the only clean files): a beacon-detected restart (ring- or catalog-epoch regression) now counts as recovering until a poll has seen its recovery seed start and then finish, or until a grace of max(30 s, 2 range-stats polls) passes with no seed reported. It also replaced the flaky global-counter `!deferred` asserts in the pd hold tests (0380 item 7).

Everything below touches files that carry other in-review tasks' uncommitted hunks (`commit` sweeps whole files): `node/sweep.rs`, `observe.rs`, `net/server/control.rs`, `docs/zones.md`, `docs/configuration.md` (0377); `net/server/shared.rs` (0372); `docs/deploy.md` (0371); `bench/0363-compact-values/run_full1.py` (0403). This task depends on those tasks so it starts only once they are committed. Before editing, still run `git status --short -- <file>`.

Item numbers are 0369's (see the 0425 description for full wording):
- **3. Harness AE windows** (`run_full1.py` + analyzers): a window passes only if `vals_sweep_passes` advanced or `vals_sweep_pass_age_seconds` stayed bounded during it; never vacuously.
- **5 (docs).** `docs/deploy.md`: with V4 opaque range maps committed the PD must run with `VALS_PD_OPAQUE_RANGE_MAPS=1`, otherwise every range-map push is withheld and restarted nodes stay held; the PD logs this once at error level.
- **7. Runtime create-zone gap** (`shared.rs`/`sweep.rs`): hold newly learned sorted zones per zone until their first map push, or document why the stray-only worst case is acceptable.
- **9 (rest). Test robustness** (`node/tests.rs` + a per-shard held counter in `sweep.rs`): per-shard counts instead of process-wide `sweep_passes`/`sweep_sorted_zones_held` (ServerShared already has a cfg(test) `sweep_pass_count`); make the hold-only knockout observable (not healed within 300 ms); drive the planted-stray cleanup test on a polled mock device (0368).
- **10. Over-replication cleanup (L1)** (`sweep.rs` cleanup_cursors): persist the cursor or start at a random/saved offset; run faster when idle and/or target suspect keys; measure keys/s and the persisted cursor with file-backed strays at depth N.
- **11. AE sync errors at boot** (`sweep.rs` ~683, currently debug): log the reason at info, rate-limited; confirm benign retries. Hardware count observed in 0418 step 2.
- **12 (rest).** Docs: `docs/zones.md` "Each range-stats poll counts a node as recovering in either of two cases" needs the third case (beacon-detected restart, grace) and `docs/configuration.md` `rehome_recovery_defer_ms` row likewise. Add a `pd_recovery_restarting_nodes` gauge (`observe.rs`) set from `RecoveryHold::restarting` in `range_policy.rs`, so analyzers can attribute a hold with down=0 and seeding=0. The node-side cause of the ~7 s unreported gap (FULL11-FINDINGS: m6 skips stores without a zone config; or V3 reply timing out and falling back to V2) is still unknown; 0418 step 2 should check `pd_rehome_recovery_hold` stays 1 across the restart gap.
- **13.** Document in `docs/zones.md` the ~2-poll (~12 s) recovery-hold release lag after seeding completes.

Acceptance: each item implemented with an L0/L1 regression test, or closed with a reason. Item 2 and the item 11/12 hardware observations stay recorded through 0418.

## Outcome

All items closed. 3: non-vacuous AE-window liveness (passes advanced or age<=60 s, >=2 clean samples) gates post-drain as verify_incomplete; receipt <label>-liveness.json. 5: deploy.md opaque-map line. 7: closed with reason in ARCHITECTURE.md (stray-only worst case). 9: per-shard sweep counts, in-gap stray check, planted stray on poll-gated drives (fails with 0368 poll knocked out). 10: cleanup laps from a sampled origin; idle burst cleanup_idle_burst_ms shared per pass; new counters; file-backed L1 at depth 1M: 2.0 s vs 123.6 s. 11: AE errors at info, 1/60 s/shard with reason. 12/13: docs + pd_recovery_restarting_nodes gauge. Suites: cluster 837/837, store 479/479, bench unittests OK. Hardware AE-error reasons to be read on the next box run.

Commit: fix(0426): AE-window liveness gate; cleanup laps from a sampled origin with idle burst; rate-limited AE error logs; sweep test robustness; restarting-nodes gauge; hold/opaque-map/create-zone docs
