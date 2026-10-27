---
id: "0412"
title: 'Close R3 owned-aediag 72/72: compare the never-compared n1n2/n2n3 zone2/shard0 after node2 seeds finish (0411 remainder)'
status: done
priority: 3
created: "2026-10-10"
updated: "2026-10-12"
depends_on:
    - "0411"
spec_refs: []
---

## Description
Split from 0411. That task's digest-path acceptance is met: resumable digest pages, 0 serves over 5 s on R3, cap Busy 0/23/0 and heap gap ≤0.77 GB. Its owned-aediag 72/72 criterion is unmet.

## Evidence (0411 arm, `bench/0363-compact-values/R3-DIGEST-0411.md`; receipts `/data/dataset/task0363-results/R3-DIGEST-0411/arm-20261010T064348Z/`)
- **Coverage:** owned aediag 68/72, with 0 differing owned keys and agreeing maps. All digest merges completed.
- **Missing comparisons:** n1n2 and n2n3, zones 0 and 2, shard 0. Each failed at its first `tree` RPC to node2 with **seed-not-ready Busy** (`ae_seed_not_ready_busy` rose 9,324→10,436).
  - The recovery gate tolerates one active seed per node. Node2's last seed finished 940 s into the aediag window.
  - aediag's 120×500 ms retry budget (60 s) had already been spent on those shards.
- **Coverage across arms:** z0/s0 for both pairs was equal in 0401's 66/72 arm. **n1n2 z2/s0 and n2n3 z2/s0 have never been compared in any arm.** The R3 verdict therefore stays UNKNOWN; no difference has been found.

## Acceptance
- Get owner approval for **one** more bounded, application-read-only L2a reopen of retained R3, using the 0411 server binary (or the committed tree) and the resume-aware aediag. The 0411 rule is that one failed or inconclusive arm is a report, not permission to retry.
- Pick the smallest fix that lets the comparisons complete, for example:
  - gate the aediag start on node2's (or every node's) seed completion, within the recovery allowance; or
  - make aediag retry seed-not-ready `tree` Busy for longer, per shard, without letting one shard consume every other shard's budget.
- Run a targeted value-byte check alongside, e.g. a bounded samplecheck or sampled Get subset against the sealed cache using the 0406 checkpoints, if that fits the budget. 0411's arm ran none: aediag compares keys and versions only.
- Run only the four missing comparisons, or all 72 if cheaper. They must complete with no differing keys and agreeing maps. Record the R3 corpus verdict.
- Bounds: 0411's (≤3 h total, ≤90 min recovery, 3,600 s aediag). Reuse the 0401/0411 receipts (cursor, samplecheck, counts, heap).
- Not a dependency of 0386 unless the owner decides it should be. The 0386 L3 run carries its own end-of-run aediag.

## Acceptance criteria

## Work log
- 2026-10-12: closed. R3 is consumed, so no further arm is possible. The supplementary R3 union is 72/72 with 0 differing, and the R4 full replay (0386) then passed its preregistered owned aediag at 72/72 with 0 differing owned keys. The R3 preregistered verdict stays "inconclusive" as recorded; the question it covered is answered by R4.
- 2026-10-10: blocked until the owner approves one more bounded R3 reopen. 0411's acceptance forbids an automatic retry arm, and the owner's validation preference requires approval for extra hardware arms. To unblock, approve the arm and the gate/tool choice above, then set the task to todo.
- 2026-10-10: **owner approved** the bounded read-only R3 arm. It runs as step 3 of the consolidated R3 session 0418, not as a separate reopen. Remaining work here: choose and land the aediag gate/retry fix before 0418 launches. Then close from 0418's findings. 0411's no-retry rule still holds: an inconclusive result is reported, not retried. Status: blocked → todo.
- 2026-10-11: **tool fix chosen and landed as 0423** (split out so it could be committed before 0418): owned-mode aediag defers a failing (zone, shard), attempts every other one first and re-attempts the deferred ones in rounds until `--defer-s` (default 3000 s) from start; only the final failure is an `error` row. Staged binary for 0418 step 3: `/data/dataset/task0363-results/AEDIAG/aediag-0412-defer-499ead20` (sha256 `499ead20…2a87`). Not done (seed-completion gate) because it needs new controller logic in the uncommitted run_full1.py. Remaining here: run in 0418 step 3 with the parallel value-byte Gets, then record the 72/72 verdict. Budget note: in 0411 the z2 processes did ~2,400 s of work for 7 shards; adding shard 0's never-run merge must still fit 0418's aediag bound.
- 2026-10-11: todo → blocked (work-loop session s_f6472827). No software work remains; the only open item is 0418 step 3 on hardware. 0418 is gated on 0404/0409, which wait on in_review 0403, whose reviewed hunks need the owner's integration commit along with 0366/0371/0372/0377/0387/0406. Consecutive work-loop sessions selected this task and ended without changes. **Unblock:** set back to todo when 0418 is ready to launch (0404 and 0409 done).
- 2026-10-12: 0418 step 3 (bench/0363-compact-values/R3-SESSION-0418.md).
  - The preregistered pass reached 60/72 with 0 differing. Node2's last recovery seed was still running, and its seed-not-ready `tree` Busy outlasted the 0423 defer window. The sampled value-byte Gets ran in parallel and passed 10,172,125/10,172,125.
  - A seed-gated rerun in the same reopen, before any write (`step3b/`), completed the 12 missing comparisons. **n1n2 z2/s0 and n2n3 z2/s0 are equal**, with 0 differing and agreeing maps.
  - **Preregistered verdict: inconclusive.** The rerun is a retry beyond this task's no-retry rule and its 3,600 s aediag allowance, and nothing approved it, so it does not by itself meet the acceptance criteria.
  - Supplementary result: the union of both passes is 72/72, with 0 differing and agreeing maps (it is 0412's seed-completion-gate option).
  - Owner decision needed: accept the supplement and close this task, or leave the R3 verdict at UNKNOWN. R3 has since been consumed to EOF, so no further pre-ingest arm is possible.
