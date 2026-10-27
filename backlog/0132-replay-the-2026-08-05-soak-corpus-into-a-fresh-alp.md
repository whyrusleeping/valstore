---
id: "0132"
title: Replay the 2026-08-05 soak corpus into a fresh lab cluster (tier one + nosync)
status: done
priority: 2
created: "2026-08-05"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - plans/backfill-replay-runbook.md
    - plans/bootes-soak-runbook.md
---

## Description
Replay the task-0130 soak's captured corpus (`/home/user/code/bootes/soak/dump-20260805T1515Z`, 16 raw `.vdump` files, ~155.15M puts/tombstones, 82 GiB) into a fresh 3-node lab SPDK cluster as a repeatable write-path benchmark, using valsd built from the current working tree (includes the uncommitted task-0130 shard-wide BatchApply gate + `batch_apply_gate_waits`).

Two arms, sequentially, each against a freshly formatted cluster:
1. `--tier one` (durable group commit; ingest-faithful, comparable to the soak).
2. `--tier nosync` (write-path ceiling).

Zones: `records` 2 sorted with hot-collection pre-splits, `backlinks` 2 sorted `presplit=8`, auto-split default-on.

Acceptance criteria:
- Both arms complete with 0 replay write errors and 0 `server_errors`.
- Node RSS stays bounded; no node wedge/OOM; cluster `records_written` delta consistent with records replayed (RF=2 ⇒ ~2× physical applies).
- Throughput curve, batch-apply latency, gate-wait counts, busy_rejections and split activity recorded as a time series per arm.
- Findings written under `bench/replay-lab/<run>/` and the replay runbook updated with the lab-era procedure.

## Acceptance criteria

## Plan

Execute the two-arm replay benchmark directly (coordinator-run ops task, per 0130/0164 precedent; no engine code changes expected).

**Scope decision — full corpus, not the node1. subset.** The work log's 2026-08-06 "subset only" revision rested on two beliefs that later work overturned: (a) RAM: 0153/0154 fixed the runaway working set — the 0152 run-3 validation replayed the FULL 155.16M-record corpus with peak RSS 14.7 GiB vs the 40 GiB budget, zero OOM; measured resident cost is ~106–111 B/key (not the 240–400 B/key assumed on 08-06). (b) Device: the reserve-floor write errors in that run came from 32 GiB zone slabs + numeric backlinks presplit=8 concentration — 0164 shipped 80 GiB slabs (already in the box node.confs) + the 48-range real-key backlinks presplit; the 0130 ceiling soak sustained 484M physical applies before any floor, vs ~310M for this corpus at RF=2. Full corpus is the ingest-faithful benchmark the task originally wanted. Fallback: if arm 1's min_zone_free_segments declines linearly toward the floor (≈50 min warning per the runbook), abort and rerun both arms on the `--in dump/node1.` subset.

**Procedure (per plans/bootes-soak-runbook.md §§0–3,7 + plans/backfill-replay-runbook.md, adapted):**
1. Rsync current HEAD (fb5bed6) to the three boxes, rebuild SPDK valsd in parallel, verify DONE_RC=0 and fresh binary mtime.
2. Arm 1 (`--tier one`): fresh PD raft wipe + format=true node start (VALS_RAM_BUDGET_BYTES=40 GiB, sudo systemd-run one-liners). Create zones: `records 2 sorted` + canonical hot-collection presplits, `backlinks 2 sorted presplit=<48 real-key boundaries>` (runbook §3). Verify 54 pd_sorted_ranges. Interop preflight (batch/check-many on records + both backlinks families).
3. Start a metrics collector (30 s cadence → timeseries CSV per node: records_written, server_errors, busy/ram_busy/region_exhausted rejections, batch_apply latency hist, batch_apply_gate_waits, batch_apply_run_reads, min_zone_free_segments, plan_failures, resident_runs, RSS, pd sorted_ranges). Run `valsdump replay --nodes … --in dump/node --batch 1000 --tier one --r 1 --rf 2` from devbox in background; monitor.
4. On completion: final per-thread stats (errs must be 0), cluster records_written delta ≈ 2× logical replayed, spot valsq count/get validation, bounded stop.
5. Arm 2 (`--tier nosync`): repeat 2–4 on a freshly formatted cluster.
6. Write FINDINGS.md + timeseries under `bench/replay-lab/run-<ts>-tier-one/` and `…-tier-nosync/`; update plans/backfill-replay-runbook.md with the lab-era procedure (replacing the dead oldbox §1–3 guidance with the lab flow, zone names records/backlinks, 0164 levers, full-corpus capacity math).
7. Review: simple tier (ops + docs), then update memory if lessons, mark done, commit.

### Starting points
- plans/bootes-soak-runbook.md §0.5 rsync+build, §1–3 cluster+zones, §7 stop
- bench/replay-lab/run-20260808T010817Z-rambudget40g-0154/{FINDINGS.md,monitor.sh} — prior full-corpus replay + collector
- corpus: /home/user/code/bootes/soak/dump-20260805T1515Z (16 .vdump, 155,158,024 records)
- replay tool: ~/code/vals-examples/target/release/valsdump

## Work log

- 2026-08-05/06 arm 1 (`--tier one`, full corpus) — **did not complete: nodes 2 and 3 OOM-killed at 47.1 GiB peak on 60 GiB boxes ~50 min in**; node 1 survived by filling a zone region first (`no free segments for snapshot` ×708) and falling into retryable `Busy` (251,709) with 0 `server_errors`. The replayer read all 155,158,024 records (154.72M puts, 437,802 tombstones) in 3190 s but 64.1M writes failed after the nodes died. Peak throughput 180k physical applies/s (~90k logical rec/s) decaying to ~15k/s as run count and cold LWW resolves grew (0.3 `batch_apply_run_reads` per apply); placement skew 1.65× (node 3 took 71.3M applies vs 43.5M/42.4M). Findings: `bench/replay-lab/run-20260805T231341Z-tier-one/FINDINGS.md`. First hypothesis (per-engine memtable cap × 8 engines) filed as task 0133.
- 2026-08-06 arm 1 re-run with `VALS_FLUSH_MAX_BYTES=512 MiB` — **falsified that hypothesis**: identical RAM trajectory and identical outcome (n3 OOM 01:09:48, n2 01:12:34, both 47.1 GiB), env var verified in `/proc/<valsd>/environ`. Measured the real terms: fully-resident per-key index at 240–400 B/key plus an un-budgeted maintenance working set; at equal `live_keys` (32.8M vs 33.6M) the device-throttled node held 7.4 GiB and the un-throttled one 17.7 GiB. Corpus arithmetic (~116M distinct keys, RF=2, 1.65× skew ⇒ ~100M key-replicas on the hot node ⇒ 30–40 GiB of index alone) shows the full corpus **cannot** fit three 60 GiB boxes today. Filed task 0134 (RAM backpressure: shed load instead of OOM) and corrected 0133's scope. Findings: `bench/replay-lab/run-20260806T001507Z-tier-one-flushcap512m/FINDINGS.md`. Next: run the arms on a fitting subset (`--in <dump>/node1.`, 8 files, ~68.8M records).
- 2026-08-06 STATUS: not actively running — the cluster has been idle since the 2026-08-06 01:15 postmortem, so this moves back to `todo` rather than `in_progress`. Scope is now revised by the two failed full-corpus attempts: the full 155M-record corpus provably does not fit three 60 GiB boxes at RF=2 (see task 0134), so both arms should run against the fitting subset `--in <dump>/node1.` (8 files, ~68.8M records, ~30M key-replicas on the hot node ⇒ ~12–20 GiB/node). Prerequisite check: the corpus at `/home/user/code/bootes/soak/dump-20260805T1515Z` must still exist on devbox.
- 2026-08-10 plan: Execute the two-arm replay benchmark directly (coordinator-run ops task, per 0130/0164 precedent; no engine code changes expected).  **Scope decision — full corpus, not the node1. subset.** The work
…[truncated]
- 2026-08-10 context hints: 4 recorded with plan
- 2026-08-10 review tier: simple (coordinator self-review)
- 2026-08-10 decision: accept — commit: 0132: full-corpus replay benchmark CLEAN on lab — both arms (tier one + nosync) replay all 155,158,024 records (154.72M puts + 437,802 tombstones, 82 GiB raw, RF=2 ⇒ 309,441,044 physical applie
…[truncated]
- 2026-08-10 usage: 53,869 tok (in 200, out 53,669, cache_r 8,564,066, cache_w 628,337) · cost n/a (unpriced)
