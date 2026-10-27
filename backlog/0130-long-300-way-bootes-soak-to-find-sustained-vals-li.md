---
id: "0130"
title: Long 300-way bootes soak to find sustained vals limits
status: done
priority: 1
created: "2026-08-04"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description
Run a substantially longer fresh bootes backfill/firehose soak against the 3-node lab SPDK cluster, using 300 active backfills and enough producer concurrency to expose vals limits rather than optimizing bootes memory. Instrument bootes, valsgo/server throughput, Busy/errors/timeouts, per-node placement/load, maintenance progress, split behavior, RSS/device usage, and profiles around the first sustained bottleneck. Fix actionable vals bottlenecks found during the run and continue validation until the limiting stage is upstream PDS download rather than vals persistence.

Acceptance criteria:
- Fresh cluster and corpus, current bootes/valsgo/valsd builds, split-placement preflight passes.
- Multi-hour soak reaches sustained compaction/split/reclaim conditions rather than only startup behavior.
- Time-series samples distinguish upstream production, bootes sink, network/client, serving cores, maintenance, NVMe, and capacity/backpressure limits.
- Any vals correctness, availability, or material performance bottleneck encountered is root-caused and fixed, then revalidated.
- Findings and operationally useful artifacts are recorded under bench/bootes-soak-lab and the runbook is updated if defaults/guidance change.

## Acceptance criteria

## Plan

6h+ sustained-ceiling soak arm (coordinator-driven ops per plans/bootes-soak-runbook.md). The 0164 arm eliminated the ~60-min backlinks wall (3h44m at full rate, zero backpressure); this arm answers 0130's remaining question — the SUSTAINED ceiling — by running long enough to see which arrives first: (a) placement-skew floor on the hottest node's backlinks engine (projected ~5.5–6h; n1 was 1.8× n2's applies), or (b) RAM shedding at the 40 GiB budget (~111 B/key measured ⇒ ram_busy projected ~8–9h). Secondary goals: reach the gate-benefit regime (batch_apply_gate_waits > 0 vs baseline run-20260804T220428Z-batch1000's 261 timeouts at 4h45), and observe the 0163 mostly-live floor signature at scale if the floor bites (evidence for 0163/0118/0120/0155, not fixed here).

Execution:
1. Preflight §0: ssh sanity, hugepages=4096, panic_on_oops=1, 9100 PRO on vfio-pci, node.conf format=true + zone_region_mib=81920.
2. §0.5: rsync HEAD (db85501; code unchanged since 9827811 so builds should be ~no-op) to all 3 boxes, parallel SPDK release builds, DONE_RC=0, fresh binary mtime.
3. Producer: rebuild bootes via scripts/build-with-indigo-fix.sh (valsgo ≥6c7aa2a); rebuild /tmp/interop.
4. §1–3: fresh PD raft wipe + start; 3 nodes VALS_RAM_BUDGET_BYTES=40GiB, format=true; zones: records 2 sorted + 5 canonical presplits, backlinks 2 sorted with the 47-boundary distribution-aligned presplit (expect 54 sorted ranges).
5. §4: interop parity preflight on records + both backlinks ref families — stop if it fails.
6. §5: launch bootes 300/64, fresh backfill.db, GOMEMLIMIT=64GiB.
7. Collector: reuse run-20260809T181924Z-0164-presplit/collect.sh into bench/bootes-soak-lab/run-<TS>-0130-ceiling/, budget extended to ~8h, 60 s samples, background job. Checkpoints every ~45–60 min: min_zone_free per node + decline rate (floor projection), RSS vs budget, busy/ram_busy/errors/plan_failures, gate_waits, splits, bootes progress/retries.
8. Target ≥6h of full-rate ingest. Near the projected ceiling capture snapshots + perf profile of the hottest node. If the skew floor hits: record the full 0163 signature (1/min WARN loop, Busy spin, region_exhausted NOT firing, shadow est ~0) then treat as end of useful life. If RAM shed hits first: verify graceful shedding (ram_busy rising, reads/maintenance live, RSS bounded).
9. Clean bounded stop §7; final logs/metrics; FINDINGS.md comparing vs the 0164 arm and baseline; runbook updates if guidance changes; work log; commit.

Contingency: any new wedge/OOM/corruption ⇒ capture evidence, autopsy, root-cause; fixes per recorded precedent (coordinator implements directly if implementer backend still down). 0137 direct-delete failures are expected noise. Space-amp numbers feed 0158.

### Starting points
- plans/bootes-soak-runbook.md — full procedure
- bench/bootes-soak-lab/run-20260809T181924Z-0164-presplit/{FINDINGS.md,collect.sh} — prior arm + collector to reuse
- ceiling projections: skew floor ~5.5-6h (n1), RAM shed ~8-9h at 40GiB

## Work log

- 2026-08-05 4h45 checkpoint: the batch1000 arm reached 46.3M backfill records / 194.5M physical applies with all nodes healthy, maintenance progressing, bounded RSS, zero Busy, and zero dropped batches. As run lookup pressure grew it accumulated 261 retryable batch timeouts and 445 direct (unretried) firehose delete failures, overwhelmingly on hot shard 4. Mean BatchApply stayed 3.7–5.1 ms but completed tails exceeded 1 s and client-abandoned work continued server-side. A live perf profile was backend-bound in run lookup/front-coded decode/SPDK completion. Root cause: staged BatchApply used only the generic per-connection 1,024 read cap, with no shard-wide work bound. Prepared an eight-handler shard-wide cancellation-safe gate plus `batch_apply_gate_waits`; focused tests, full vals-cluster tests, workspace check, and diff check pass. Findings: `bench/bootes-soak-lab/run-20260804T220428Z-batch1000/CHECKPOINT-4H45.md`. Fresh hardware A/B remains. Also identified a bootes correctness bug: direct deletes are neither retried nor ordered with buffered creates, so timeout loses a tombstone and queue reordering can resurrect a key; route `Value=nil` through the records batch queue in the sibling bootes repo.
- 2026-08-05 terminal/corpus capture: node 2 stopped accepting all four data ports and SSH during 04:58–05:02 UTC; metrics stayed reachable but `records_written` froze at 72,078,111. Bootes was not automatically stopped and continued until noticed at 15:08 UTC, dropping batches under RF=2. Its bounded 5-minute shutdown expired with 362,455 records + 410,784 backlinks dropped, 50k buffered, and final cursor withheld. Nodes 1/3 stayed live. Captured every records/backlinks zone on all four shards from nodes 1/3 using live admin dump, yielding 16 replayable raw `.vdump` files: ~155.15M records / 82 GiB. With RF=2 over three nodes every replica pair intersects {1,3}, so the union covers normally placed data (not a certified backup under divergence). Files: `/home/user/code/bootes/soak/dump-20260805T1515Z`; procedure/summaries/replay command: `bench/bootes-soak-lab/run-20260804T220428Z-batch1000/dump-20260805T1515Z/README.md`.
- Node-2 failure autopsy while still wedged: this was not a clean vals shutdown (`graceful_shutdowns=0`), process abort/OOM, or ordinary listener accept error (`accept_transient_errors=0`). The vals metrics thread remains fast on :9464 but every metric is frozen; all four data ports silently drop SYNs rather than refuse, while SSH completes TCP handshake but never sends its banner and an earlier remote `systemctl show` hung indefinitely. RSS before failure was 18–21 GiB on a 60 GiB box, not near its known OOM limit. Stopping nodes 1/3 and PD did not recover node 2. This proves a host/userland-wide wedge rather than only one vals shard; leading hypothesis is node-2 OS/root-device or kernel/systemd I/O stall (resident metrics can answer from RAM, while sshd startup and vals threads/logging block), but exact cause requires console/BMC access and `journalctl -k`/NVMe/systemd inspection before reboot. Do not label it root-caused yet.
- 2026-08-06 STATUS: no longer running (cluster idle since the 2026-08-05 terminal capture), so this returns to `todo`. Remaining scope is narrowed to one thing: **land the shard-wide staged-BatchApply admission gate** (`MAX_STAGED_BATCH_APPLIES` + `batch_apply_gate_waits`, currently uncommitted working-tree changes in `crates/vals-cluster/src/net/server.rs` and `crates/vals-client/src/observe.rs`; unit tests, `cargo test -p vals-cluster` 424 passed and `cargo check --workspace` all pass) and **A/B it on a fresh cluster arm** against the pre-gate baseline (`bench/bootes-soak-lab/run-20260804T220428Z-batch1000`), watching batch timeouts, completed-tail latency and `batch_apply_gate_waits`. The node-2 wedge went to task 0131 (closed: kernel MGLRU Oops), the RAM findings to 0133/0134, and the bootes delete-ordering bug to task 0137.
- 2026-08-08 plan: Remaining scope (per 2026-08-06 work-log narrowing): the shard-wide staged-BatchApply gate (MAX_STAGED_BATCH_APPLIES=8 + batch_apply_gate_waits) is already committed (bad3860); what's left is the fres
…[truncated]
- 2026-08-08 context hints: 4 recorded with plan
- 2026-08-09 gate A/B arm executed (`bench/bootes-soak-lab/run-20260808T223525Z-gate-ab/FINDINGS.md`): fresh cluster on HEAD fb43e88, 300/64 bootes soak. Ingested 37.3M records / 156.3M applies in 65 min (~4× baseline rate) with `batch_apply_gate_waits` 0/0/0 — the gate's cap of 8 never throttled a healthy write storm (no-regression validated); 299 client 5 s batch retries all recovered, 0 drops, 0 server_errors, bounded RSS, mean BatchApply 5.7/8.9/15.9 ms. The gate's *benefit* regime (staged-work pileup behind a hot shard, hours in) was not reached: at 65 min the backlinks zone-2 vlog allocator hit free=0 on BOTH replicas of the hot pair → permanent Busy quorum failure + reclaim deadlock (compaction snapshot needs a segment, retried 1/s for 2 h, segments_freed frozen) → total producer stall. Stopped after 2 h of confirmed zero progress; graceful node shutdowns; evidence preserved on-box. New p1 task 0157 filed (root-cause free=0, reserve-floor reclaim guarantee, per-zone segment gauges, operator recovery path); 461 more direct delete failures re-confirm 0137. REMAINING SCOPE: re-run the multi-hour soak (gate-benefit + upstream-PDS-limited bar) after 0157 lands — 0157 is now this task's effective prerequisite.
- 2026-08-09 review tier: simple (coordinator self-review)
- 2026-08-09 decision: accept — commit: 0130 gate A/B soak arm: shard-wide BatchApply gate no-regression validated at 4× baseline ingest (gate_waits 0/0/0, 37.3M rec / 156.3M applies in 65 min, 0 drops/errors, bounded RSS) — but the arm 
…[truncated]
- 2026-08-09 usage: 68,195 tok (in 166, out 68,029, cache_r 7,719,545, cache_w 323,448) · cost n/a (unpriced)
- 2026-08-09 plan: Post-0157 multi-hour soak arm (coordinator-driven ops per plans/bootes-soak-runbook.md). The 2026-08-08 gate-A/B arm validated no-regression but died at 65 min on the backlinks free=0 wall; 0157 (ccb8
…[truncated]
- 2026-08-09 context hints: 4 recorded with plan
- 2026-08-09 context hints: crates/vals-store/src/engine.rs: compact_plan_inner ~2381 (PlanKind::Pressure paths 2411-2600), minimal_pressure_needed_segs ~3035, region_exhausted ~3064, maint_reserve_segs ~3156, lsm_dead_fraction 
…[truncated]
- 2026-08-09 preload: 3 file(s), ~31 KiB seeded into implementer context
- 2026-08-09 context hints: crates/vals-store/src/engine.rs: compact_plan_inner ~2381 (PlanKind::Pressure paths 2411-2600), minimal_pressure_needed_segs ~3035, region_exhausted ~3064, maint_reserve_segs ~3156, lsm_dead_fraction 
…[truncated]
- 2026-08-09 preload: 3 file(s), ~31 KiB seeded into implementer context
- 2026-08-09 context hints: crates/vals-store/src/engine.rs: compact_plan_inner ~2381 (PlanKind::Pressure paths), minimal_pressure_needed_segs ~3035, region_exhausted ~3064, maint_reserve_segs ~3156, lsm_dead_fraction ~3197; tes
…[truncated]
- 2026-08-09 preload: 3 file(s), ~31 KiB seeded into implementer context
- 2026-08-09 context hints: crates/vals-store/src/engine.rs: compact_plan_inner ~2381 (PlanKind::Pressure paths), minimal_pressure_needed_segs ~3035, region_exhausted ~3064, maint_reserve_segs ~3156, lsm_dead_fraction ~3197; tes
…[truncated]
- 2026-08-09 preload: 3 file(s), ~31 KiB seeded into implementer context
- 2026-08-09 review tier: simple (coordinator self-review)
- 2026-08-09 decision: accept — commit: 0130 post-0157 soak arm: 0157 reserve-floor mechanics validated on hardware (no DeviceFull spam, no reclaim deadlock, min_zone_free_segments gives ~50 min of warning) at ~4x-baseline ingest (35.7M rec
…[truncated]
- 2026-08-09 usage: 54,241 tok (in 100, out 54,141, cache_r 3,913,965, cache_w 282,039) · cost n/a (unpriced)
- 2026-08-09 plan: Post-floor-stack multi-hour soak arm (coordinator-driven ops per plans/bootes-soak-runbook.md). Prior arm (run-20260809T063307Z-post0157) died at ~57 min: backlinks floor → zero-net-gain livelock (0
…[truncated]
- 2026-08-09 preload: 4 file(s), ~40 KiB seeded into implementer context
- 2026-08-09 review tier: high-powered — reviewers: sol, claude
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — A solid incremental step on task 0130: the soak arm was executed per plan, produced complete and internally consistent artifacts (FINDINGS.md, 72-sample timeseries, snapshots, final logs/metrics), val
…[truncated]
- 2026-08-09 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-09 review (claude): accept — The revision fully addresses the prior findings and materially advances the task: the runbook wall gotcha is rewritten for the current (quiet, retryable, slow) floor behavior with the VALS_VLOG_GC kno
…[truncated]
- 2026-08-09 decision: accept — commit: 0130 four-arm soak session: floor stack validated on hardware, wall root-caused to invisible cross-run shadowed vlog garbage; plan-aligned reserve + aged-vlog GC landed, pre-floor reclaim split to 016
…[truncated]
- 2026-08-09 usage: 212,872 tok (in 404, out 212,468, cache_r 32,396,127, cache_w 2,501,113) · cost n/a (unpriced)
  coordinator: 191,615 tok (in 348, out 191,267, cache_r 30,605,147, cache_w 2,317,133) · cost n/a (unpriced)
  reviewer:claude: 21,257 tok (in 56, out 21,201, cache_r 1,790,980, cache_w 183,980) · cost n/a (unpriced)
- 2026-08-09 re-scope (from 0164 session): the ~60-min backlinks capacity wall is ELIMINATED — run-20260809T181924Z-0164-presplit sustained full rate 3 h 44 min with zero busy/ram_busy/errors/plan-failures (distribution-aligned 48-range backlinks presplit + zone_region_mib=81920; runbook §3 updated). 0130's multi-hour bar is unblocked; its remaining question is the SUSTAINED ceiling: (a) placement-skew floor on the hottest node (projected ~5.5–6 h at full rate; levers 0118/0120/0155 if it bites) and (b) RAM at ~111 B/key measured resident (~8–9 h to ram_busy shedding at the 40 GiB budget). Next 0130 arm should target 6 h+ and characterize which ceiling arrives first.
- 2026-08-09 plan: 6h+ sustained-ceiling soak arm (coordinator-driven ops per plans/bootes-soak-runbook.md). The 0164 arm eliminated the ~60-min backlinks wall (3h44m at full rate, zero backpressure); this arm answers 0
…[truncated]
- 2026-08-09 context hints: 3 recorded with plan
- 2026-08-10 review tier: simple (coordinator self-review)
- 2026-08-10 decision: accept — commit: 0130 sustained-ceiling soak arm (CLOSES the task): the ceiling race is answered — placement-skew capacity floor first, at 6h25m, in the RECORDS zone; RAM never bites. Fresh 3-node SPDK cluster on db
…[truncated]
- 2026-08-10 usage: 47,249 tok (in 118, out 47,131, cache_r 4,428,512, cache_w 861,896) · cost n/a (unpriced)
