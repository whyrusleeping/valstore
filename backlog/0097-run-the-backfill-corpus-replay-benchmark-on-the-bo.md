---
id: "0097"
title: Run the backfill-corpus replay benchmark on the box (baseline for 0069/0094/0096 A/Bs)
status: done
priority: 3
created: "2026-07-09"
updated: "2026-08-06"
depends_on:
    - "0069"
    - "0104"
    - "0105"
spec_refs: []
---

## Description
Execute the replay benchmark per plans/backfill-replay-runbook.md against the 2026-07-09 1B corpus (~/run/backfill/dump on oldbox, 3.254B records / 1.37 TiB).

- Fresh 4-node SPDK target cluster with sorted zones named zone-1/zone-2 (matching the .vdump headers) + the ingest pre-splits.
- `valsdump replay --tier one` (ingest-faithful) and ideally a `--tier nosync` (ceiling) pass; collectors + milestone-style profiles.
- Record throughput-vs-progress + batch_apply percentiles in a bench/ notes file; 0 replay errs / server_errors; spot valsq validations.
- This is also the intended A/B harness for 0069 (incremental merge), 0094 (flush monoliths), and 0096 (BatchApply staged resolve): baseline now, re-run after each lands.
- NOTE (2026-07-09): 0096's staged BatchApply resolve has landed, default ON. A/B it in-place via `VALS_STAGED_BATCH_APPLY=0` (baseline, synchronous QD1 per-record LWW reads) vs unset/`1` (staged, overlapped); watch `vals_batch_apply_run_reads` for the cold-read rate. 0096's acceptance numbers (<10% serving-core DeviceRun::lookup+poll share, ≤10 µs/record at 100M+ recs/node) should be measured in this run.

Consider `--in dump/node1.` (~690M) as the short variant if box time is tight.

## Acceptance criteria

## Plan

Execute plans/backfill-replay-runbook.md on oldbox, coordinator-driven (operational task; the long replay runs detached on the box), with the analysis/notes write-up delegated to the implementer at the end.

1. Box prep: verify no valsd running, corpus intact (~/run/backfill/dump, 8 .vdump files), disk/hugepage state clean. Sync vals repo to HEAD (829340f, includes 0096 staged BatchApply) and rebuild valsd with `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk`; rebuild vals-examples tools (valsdump, valsq).
2. Ground truth (cheap, optional but valuable): boot the existing source cluster once with FORMAT=false, record a few `valsq count` ranges + one known-deleted key, stop cleanly.
3. Fresh target cluster: `sudo ./cluster.sh setup` (TRIM+vfio), `start` as root, chmod 666 pd.sock; drop records/backlinks, create sorted zone-1/zone-2, apply the runbook's pre-splits, verify with `admin zones` and that dropped regions were freed.
4. Observability: start collect.sh + sys.sh detached; profiles early (~10 min), mid-corpus, late.
5. Replay, primary run: full corpus `--in dump/node --batch 1000 --tier one --r 1 --rf 1` detached (budget 5–10 h). Monitor via a local background job; take profiles at milestones. On completion capture per-thread final lines (errs must be 0), vals_records_written delta, server_errors/busy_rejections.
6. Validation: spot `valsq count` ranges vs ground truth, `valsq get` of a deleted key → absent.
7. If box time permits after the tier-one run: a `--tier nosync` pass (full or node1. subset — decide by remaining time; record which). Optional best-effort: short VALS_STAGED_BATCH_APPLY=0 comparison window; otherwise note as follow-up.
8. Delegate to implementer: analyze obs/*.jsonl + replay logs + profiles (via ssh) and author bench/t0097/notes.md — throughput-vs-progress, batch_apply p50/p99/p999 per node, 0096 acceptance measurements (DeviceRun::lookup+poll share <10%, ≤10 µs/record at 100M+ recs/node), validation results, exact corpus/tier provenance for future A/Bs.
9. Teardown per runbook (keep dump/), task done, commit.

### Starting points
- plans/backfill-replay-runbook.md
- plans/backfill-dump-runbook.md §Caveats (root start, hugepage leaks, sudo env LD_LIBRARY_PATH, pd.sock perms)
- bench/bootes-live-1b/notes.md (ingest-run decline curve to compare)
- memory.md: valsd needs VALS_SPDK_LINK=1 …
- box paths: ~/vals, ~/vals-examples/backfill, ~/run/backfill

## Work log
- 2026-07-09 plan: Execute plans/backfill-replay-runbook.md on oldbox, coordinator-driven (operational task; the long replay runs detached on the box), with the analysis/notes write-up delegated to the impleme
…[truncated]
- 2026-07-09 context hints: 5 recorded with plan
- 2026-07-09 aborted/reset: a tier-one full-corpus replay was started on oldbox but killed ~2h in (user call: do obvious perf work first — 0069 — then baseline). Box cleaned: replay + watcher + collect.sh/sys.sh killed, cluster stopped via `sudo ./cluster.sh stop`, hugepages + /var/run/dpdk cleared, no valsd left. dump/ corpus intact (1.7T). NOTE: the 2h run produced no progress lines in replay.tierone.1815.log beyond the header — worth checking valsdump replay's progress reporting before the next attempt. Task set back to todo, now depends on 0069.
- 2026-07-10 aborted/reset (2nd attempt, post-merge-improvements @ b16b988): tier-one full-corpus replay ran ~90 min to ~540M records (avg ~110k rec/s, windows 70–220k; vs ingest-era 62–65k/s and falling at the same wall point — merge fix confirmed: 6–17 small off-core merges/node, ~94s total). Profiling at 500M found the new ceiling: serving cores 73–92% CPU with 32% of cycles in DeviceRun::locate_block — ~60% of corpus records are re-puts triggering cold LWW resolves that probe ALL runs (k≈7-miss blooms) and read EVERY bloom-positive block sequentially (resolve_run_probe argmax). Filed 0104 (early-exit newest-first resolve, p2) + 0105 (blocked blooms, p3); user killed the run to fix those first. Box cleaned same as before (valsdump/watchers killed by PID, cluster.sh stop, dpdk+hugepages cleared, no valsd); partial-run data kept at ~/run/backfill/obs-t0097-partial-20260710/ + prof/20260710T14*-replay_early + 20260710T155714Z-replay_500m; ground truth t0097/source-truth.txt intact; dump/ corpus intact. Next attempt should baseline AFTER 0104/0105 land.
- 2026-07-15 blocked (no hardware): oldbox is no longer accessible; new machines are being set up. Everything box-resident is presumed lost — the 1.7T dump/ corpus, the t0097 partial-run obs/profiles (obs-t0097-partial-20260710, prof/*replay_*), and ground truth source-truth.txt. Unblocking this task on new hardware requires (a) re-running the bootes ingest + dump per plans/backfill-dump-runbook.md to re-capture a corpus, then (b) this runbook — hostnames/paths in both plans must be re-pointed (banners added 2026-07-15). Note: 0104/0105 have since landed, so the next baseline captures them from the start.
- 2026-08-06 CLOSED as superseded. The blocker was permanent: oldbox is gone and its 1.37 TiB / 3.25B-record corpus with it, so this exact run can never be executed. The lab-era successor is task 0132 (replay the 2026-08-05 bootes corpus, `plans/backfill-replay-runbook.md`), which already produced the numbers this task existed to get: post-0069 merges are no longer the bottleneck, 0104/0105 landed before the run, and the staged BatchApply cold-read rate is measured directly as `batch_apply_run_reads` per apply (0.3) in `bench/replay-lab/run-20260805T231341Z-tier-one/FINDINGS.md`. Task 0099 re-pointed to depend on 0132.
