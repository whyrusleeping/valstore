---
id: "0151"
title: 'Box-validate confirmed re-home on lab: auto-split + auto-re-home spread a growing sorted zone; flip defaults only after validation'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-08"
depends_on:
    - "0142"
spec_refs: []
---

## Description
Task 0142 landed the confirmed re-home protocol (prepare → server-side dual-write mirroring → range-scoped copy → shard-aware confirm → cutover → release) behind `VALS_PD_REHOME_ENABLED` (default OFF, `VALS_PD_MAX_CONCURRENT_REHOMES` default 1, `VALS_PD_REHOME_REPORT_DEADLINE_MS` default 60s). This task is the box run that earns the default flip — the deliberate opt-in-until-box-validated posture (learned from 0108/0139).

Procedure sketch (see plans/bootes-soak-runbook.md for run mechanics; REBUILD ~/vals before the run):
1. Fresh sorted zone on lab (RF=2), sustained sorted ingest (e.g. valsmix-style prefixed keyspace) with auto-split ON and `VALS_PD_REHOME_ENABLED=1`.
2. Watch `pd_unrehomed_ranges` burn down and `pd_rehomes_started/completed/aborted`, `pd_rehomes_active`; confirm `pd_range_skew_pct` converges and load spreads across replica sets AND intra-node shards with no operator intervention (this is 0142's acceptance criterion 2, deferred to hardware).
3. Concurrent read/verify pass across cutovers: zero not-found for acked keys (mind the stale-coordinator caveat in docs/zones.md — coordinators must refresh maps; see task 0150).
4. Kill a pending owner mid-copy and a PD leader mid-re-home: confirm abort-or-resume with no loss (0142's criterion 3 on real hardware).
5. Reassess the 1M entries/range auto-split threshold + 5 s poll cadence now that a split schedules a data move (open question inherited from 0110/0142).

Acceptance criteria:
- A grown sorted zone converges to spread placement with re-home enabled; throughput/latency during moves recorded (compare against the 0148 canonical-split arms, bench/lab-canonical-split-20260807/).
- Interruption scenarios show no data loss.
- Decision recorded (and implemented or filed) on flipping `VALS_PD_REHOME_ENABLED` default and on split threshold/cadence defaults. Consider whether 0150 (data-path epoch fencing) must land first for a default-on decision.

## Acceptance criteria

## Plan

Ops task driven directly via Bash (same pattern as 0152), plus one small client-instrument change in the sibling vals-examples repo.

## Instrument prerequisite
docs/zones.md states cutover consistency assumes coordinators refresh range maps; valsmix today refreshes once at connect. Add `--refresh SECS` to valsmix (default 0 = off): each per-thread coordinator periodically calls `refresh_catalog` (folds in `refresh_range_maps`) between waves in load_slice / run_writer / run_foreground. Rebuild valsmix + valsq against current valstore HEAD (path dep via ~/code/vals symlink) — mandatory anyway because 0142 introduced range-map v3 encoding whenever a pending token is set.

## Phase R1 — convergence (0142 acceptance criterion 2 on hardware)
1. Rebuild ~/vals on all three lab boxes from HEAD (runbook §0.5), verify DONE_RC=0.
2. Fresh cluster (runbook §1–2): wipe PD raft, format=true nodes, VALS_RAM_BUDGET_BYTES=40GiB. PD launched with VALS_PD_REHOME_ENABLED=1 (defaults otherwise: max concurrent 1, report deadline 60s, auto-split threshold 1M, poll 5s).
3. `create-zone smix 2 sorted` with NO presplit — deliberate worst case: single range, token "", all inherited auto-splits create re-home debt.
4. valsmix load 20M sorted keys (vsize 256, seed 42, 8 writers, pipeline 64, tier=majority, r=2, --refresh 5). Background sampler logs PD :9465 (vals_pd_unrehomed_ranges, pd_rehomes_started/completed/aborted/active, pd_range_skew_pct, pd_sorted_ranges, pd_range_splits) + node :9464 (records_written, live_keys, used/resident, busy/errors) every 10 s to bench/lab-rehome-20260808/.
5. After load, let the burndown run: expect pd_unrehomed_ranges → 0, skew converges, live_keys spreads across all three nodes (compare 0148 Arm A 20M/20M/0 vs Arm B/C spread), intra-node shard spread via per-shard metrics. Record load throughput + move-window throughput/latency vs bench/lab-canonical-split-20260807 arms.
6. Correctness under churn across cutovers: valsmix 120 s read workload with writers on and --refresh 5 → 0 consistency errors / 0 missing; valsq windows + full count = 20,000,000.

## Phase R2 — interruption (0142 acceptance criterion 3 on hardware)
During remaining burndown (or a second loaded band to create fresh debt):
a. While vals_pd_rehomes_active=1 (poll ~0.5 s), hard-kill a non-PD data node holding part of the move (kill -9 via systemctl kill -s KILL) mid-copy → expect abort (mirror failure or 60 s report-gap deadline), pd_rehomes_aborted increments, no cutover. Restart the node; rehome retries and completes.
b. While active=1, restart vals-pd mid-rehome → raft-persisted pending state must resume or safely abort; no wedged pending, burndown continues.
After both: full count still exactly 20M, 120 s read-verify 0 consistency errors → no loss.

## Decision phase (acceptance criterion 3)
- VALS_PD_REHOME_ENABLED default: decide from data + two structural facts: (i) stale external coordinators are silently acked post-cutover until 0150 lands; (ii) any active rehome switches map encoding to v3, which valsgo cannot decode until 0144 — so default-on today would break valsgo/bootes clients mid-move. Likely verdict: validated but keep default OFF until 0150 (+0144) land; record in docs/zones.md + task, and note in 0150/0144.
- Split threshold/cadence: from measured copy duration and impact at ~1M-entry ranges, decide whether 1M/5s defaults stand; record (or file follow-up).

## Wrap-up
FINDINGS.md + raw samples under bench/lab-rehome-20260808/, runbook/docs updates if guidance changes, boxes left stopped/clean, memory groomed, task done + commit (hand-stage carefully: tree has an unrelated 0152 usage-bookkeeping hunk to include or leave consistent).

### Starting points
- crates/vals-cluster/src/pd.rs:640 range_stats_poll — abort/commit predicates :740-770, rehome trigger :819-848 (competes with auto-split via `actioned`)
- docs/zones.md:117-165 confirmed re-home protocol + stale-coordinator caveat
- ~/code/vals-examples/valsmix/src/main.rs — adopt_config :481, load_slice :530, run_writer :585, run_foreground :759
- plans/bootes-soak-runbook.md — fleet launch mechanics, LD_LIBRARY_PATH, systemd-run one-liners
- bench/lab-canonical-split-20260807/notes.md — 0148 comparison arms

## Work log
- 2026-08-08 plan: Ops task driven directly via Bash (same pattern as 0152), plus one small client-instrument change in the sibling vals-examples repo.  ## Instrument prerequisite docs/zones.md states cutover consistenc
…[truncated]
- 2026-08-08 context hints: 5 recorded with plan
- 2026-08-08 review tier: simple (coordinator self-review)
- 2026-08-08 decision: accept — commit: 0151: confirmed re-home box-validated on lab — a 20M-key sorted zone grown from a single range converged 46 inherited ranges → 0 unrehomed with no operator intervention (replica-set AND intra-n
…[truncated]
- 2026-08-08 usage: 69,948 tok (in 182, out 69,766, cache_r 9,716,407, cache_w 648,631) · cost n/a (unpriced)
