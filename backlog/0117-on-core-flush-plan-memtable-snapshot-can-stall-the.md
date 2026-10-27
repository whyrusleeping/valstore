---
id: "0117"
title: 'On-core flush_plan memtable snapshot can stall the serving core >1s (node2: 65% of flushes) — verify after 0114 fix'
status: done
priority: 3
created: "2026-08-01"
updated: "2026-08-02"
depends_on: []
spec_refs: []
---

## Description
## Context

Found during the 2026-08-01 soak (see `bench/bootes-soak-lab/run-20260801-baseline/FINDINGS.md`). `flush_plan` — the memtable-flush prep — runs **on the serving core** under a `store.borrow_mut()` (crates/vals-cluster/src/node.rs:2588): it snapshots (clones, +sorts in on-core-prep mode) the memtable and reserves segments, then hands the CPU-heavy `build_run` + the QD1 write to the off-core worker (task 0091 split). The clone/reserve is deliberately left on-core as "cheap prep".

On **node2** this prep was NOT cheap: **65% of flush_plans took >1 s** (5,923 / 9,048), vs **0%** on node1 and node3. Because the borrow is held on the serving core, each >1 s prep stalls that shard's serving — visible as ~3,267 `apply`s >1 s on node2 (its p99 is otherwise ~2.5 ms).

## Why node2 specifically (likely downstream of 0114)

node2 was the node whose **off-core flush writes were qpair-starved** (8,544 `off-core flush write failed: failed to allocate NVMe qpair`, task 0114) AND under constant region pressure (`pressure_flushes` 20,879 vs 1 on node1). Starved flush writes ⇒ the frozen memtable can't drain ⇒ the live memtable balloons past its byte cap ⇒ the on-core clone of a giant memtable takes >1 s ⇒ serving stalls ⇒ more pressure. So the immediate trigger is 0114.

## The latent issue (why this is its own task, not just 0114)

Even with 0114 fixed, `flush_plan` doing an **unbounded on-core memtable snapshot** means *any* condition that lets the memtable grow past its cap (a slow/backpressured flush drain, a pressure-flush storm, a large byte cap) re-introduces multi-second serving stalls. The prep cost should be bounded and/or off the serving core.

## Work
- First: **re-run the soak on the 0114-fixed binary and re-measure** `flush_plan_us` / `apply_latency_us` tails. If node2's >1 s flush_plans vanish, this may be fully absorbed by 0114 — downgrade/close.
- If tails persist (or to harden regardless): bound the on-core prep — move the snapshot clone (and sort) off the serving core, or cap/curb the memtable so a single flush_plan can't clone an oversized memtable, so a slow flush drain can never translate into a multi-second serving stall.
- Confirm whether on-core-prep mode (`VALS_FLUSH_ONCORE_PREP`) was in effect and whether the sort is the dominant term.

## Acceptance
- [x] Post-0114-fix rerun measurement of `flush_plan_us` / serving-stall tail recorded (A/B vs the baseline snapshot)
- [x] Decision: **absorbed by 0114; close without a code change**
- [x] If fixed: not applicable — this task selected the accepted "absorbed by 0114" path rather than implementing new hardening

## Outcome

A fresh 30m47s three-node SPDK bootes run from current source (including
`30ea431`) measured 409 post-start flush plans. No `flush_plan`, snapshot, or
sync observation exceeded one second on any node. Node2 changed from 5,923 /
9,048 (65.46%) pre-0114 to **0 / 136 (0%)** post-0114; its >1s apply fraction
fell about 19x. All nodes reported `maint_qpair_errors=0`, and complete logs had
zero qpair-allocation or off-core flush/compaction-write failures. This confirms
the proposed chain: task-0114's qpair starvation prevented flush drain and
inflated node2's snapshots.

`VALS_FLUSH_ONCORE_PREP` was absent from the deployed environment, so its code
default (`false`) applied: sorting was off-core. Snapshot cloning still accounted
for essentially all measured plan time, but every plan stayed at or below the
500ms histogram bucket once maintenance drained normally.

Decision: close as absorbed by 0114, the no-new-code branch explicitly allowed
by this task. Full A/B tables, setup, causal counters, caveats, and raw artifacts
are in
[`bench/bootes-soak-lab/run-20260802-post0114/FINDINGS.md`](../bench/bootes-soak-lab/run-20260802-post0114/FINDINGS.md).
Existing maintenance regressions were also checked with `cargo test -p
vals-cluster maint --lib` (10 passed).

## Acceptance criteria

## Plan

1. Verify current HEAD contains task 0114, inspect the current box/bootes state, and use the saved bootes soak runbook rather than relying on stale binaries.
2. Rsync current source to all three lab nodes, rebuild the SPDK vals-server binaries, then start a fresh PD + three-node cluster with the same RF=2 sorted-zone topology and workload settings as the baseline. Confirm whether VALS_FLUSH_ONCORE_PREP is explicitly configured; otherwise record its default from code.
3. Run a targeted sustained soak long enough to exercise repeated flushes on every node while monitoring qpair errors, maintenance progress, RAM, and service health. Capture node/PD/bootes metrics and relevant logs into a new timestamped directory under bench/bootes-soak-lab.
4. Parse pre/post histogram buckets for flush_plan_us, flush_plan_snapshot_us, flush_plan_sync_us, and apply_latency_us; compare counts and >1s fractions with the 2026-08-01 baseline. Also quantify maintenance-qpair allocation failures and pressure flushes to test the proposed causal link.
5. Make the acceptance decision from evidence: if post-0114 >1s snapshot/serving tails vanish under a representative run, document that 0117 is absorbed by 0114 and close it; if they persist, keep the task scoped to implementing a bound/off-core snapshot hardening with a regression test, then run code verification.
6. Review the resulting evidence/documentation (and any code change if needed) against every acceptance criterion, preserve unrelated dirty backlog files, and commit only task-0117-owned changes.

### Starting points
- plans/bootes-soak-runbook.md — authoritative lab deployment/launch procedure
- bench/bootes-soak-lab/run-20260801-baseline/FINDINGS.md — pre-0114 comparison
- crates/vals-cluster/src/node.rs:2588 — flush-plan timing attribution
- Commit 30ea431 — task 0114 qpair-demand fix

## Work log
- 2026-08-02 plan: 1. Verify current HEAD contains task 0114, inspect the current box/bootes state, and use the saved bootes soak runbook rather than relying on stale binaries. 2. Rsync current source to all three alpha
…[truncated]
- 2026-08-02 context hints: 4 recorded with plan
- 2026-08-02 context hints: plans/bootes-soak-runbook.md; bench/bootes-soak-lab/run-20260801-baseline/FINDINGS.md; crates/vals-cluster/src/node.rs:2588; Task 0114 commit 30ea431 is present in history; boxes are reachable as a
…[truncated]
- 2026-08-02 implementer report: Completed task 0117 and committed it as `ec2506e` (`0117: validate flush-plan tails after qpair fix`).  - Rsynced current HEAD (including 0114 commit `30ea431`) to lab-1/2/3, rebuilt all SPDK vals-
…[truncated]
- 2026-08-02 review tier: single-opus — reviewers: sol
- 2026-08-02 review (sol): accept — The task is complete on its explicitly permitted no-code path. The committed findings document a fresh post-0114 three-node SPDK rerun, A/B histogram calculations for flush-plan/snapshot/sync and appl
…[truncated]
- 2026-08-02 usage: 2,288,666 tok (in 540,033, out 22,425, cache_r 1,726,208, cache_w 0) · cost n/a (unpriced)
  implementer: 1,843,199 tok (in 302,999, out 15,720, cache_r 1,524,480, cache_w 0) · cost n/a (unpriced)
  coordinator: 314,177 tok (in 157,736, out 2,841, cache_r 153,600, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 131,290 tok (in 79,298, out 3,864, cache_r 48,128, cache_w 0) · cost n/a (unpriced)
