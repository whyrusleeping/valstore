---
id: "0171"
title: 'Soak re-validation of bootes buffered delete path (0137): dropped tombstones zero, no resurrections'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on:
    - "0137"
spec_refs: []
---

## Description
Split from task 0137 (its hardware acceptance criterion). The code fix is committed in the bootes checkout (`/home/user/code/bootes`, commit "ingest: route vals deletes through the ordered, retrying records queue"): firehose deletes now flow as nil-value KVs through the buffered, ordered, retrying records batch queue; unit tests cover ordering (same-flush and cross-flush), retry, and unparseable URIs.

Remaining validation on hardware:

## Acceptance criteria
- Run a bootes soak per `plans/bootes-soak-runbook.md` with the fixed bootes build (rebuild via `scripts/build-with-indigo-fix.sh`; remember `~/vals` is rsync'd — rebuild before every run).
- Confirm zero dropped tombstones: no `vals: dropping batch after retries` for delete-bearing batches, and the old `vals: delete failed` error no longer appears (the path is gone).
- Spot-check no resurrected keys: a sample of deleted URIs stays absent after the soak.
- Note the outcome in `bench/bootes-soak-lab/notes.md`.

## Plan

1. Verify the lab hosts, SPDK prerequisites, box configs, current valstore/bootes source state, and that bootes contains commit 6f5b9de with the buffered ordered delete queue.
2. Follow plans/bootes-soak-runbook.md end to end: sync current valstore source to all three boxes, rebuild the SPDK server binaries, initialize a fresh PD/node cluster, create and pre-split records/backlinks, and run valsgo parity preflights.
3. Rebuild bootes through scripts/build-with-indigo-fix.sh, start a fresh fixed-build soak, and collect a bounded validation window long enough to observe live delete traffic while monitoring cluster/producer health.
4. Prove the delete-path criteria from retained evidence: count forbidden drop/legacy-delete errors, identify a sample of URIs deleted during the validation window, and query them after subsequent traffic/flushes to confirm they remain absent. Stop bootes cleanly and retain the run artifacts.
5. Append the exact run, duration, revisions, counters/log checks, absent-key sample, and result to bench/bootes-soak-lab/notes.md; run focused consistency checks on the documentation diff.

## Work log
- 2026-08-10 plan: 1. Verify the lab hosts, SPDK prerequisites, box configs, current valstore/bootes source state, and that bootes contains commit 6f5b9de with the buffered ordered delete queue. 2. Follow plans/boote
…[truncated]
- 2026-08-10 context hints: The bootes checkout is /home/user/code/bootes; fixed commit is 6f5b9de. Its untracked .indigo-stream-fix/ and soak/ directories are expected artifacts.; Use timeout around SSH; avoid pgrep/pkill -f pat
…[truncated]
- 2026-08-10 preload: 2 file(s), ~43 KiB seeded into implementer context
- 2026-08-10 implementer report: Completed task 0171 hardware validation and documented a PASS.  - Ran a fresh 3-node lab SPDK soak per the runbook after verifying hugepages/vfio/configs, rsyncing valstore HEAD `44738861`, rebuild
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): accept — The change satisfies task 0171: it documents a fresh runbook-based hardware soak using the fixed bootes commit, verifies the forbidden drop/legacy-delete paths stayed at zero, captures live create/upd
…[truncated]
- 2026-08-10 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-10 revision: Addressed the auditability finding without rerunning the soak.  - Added and staged `run-20260810T210642Z-0171-delete-validation/CHECKS.md` containing:   - the literal valsq wrapper and AT-URI→record
…[truncated]
- 2026-08-10 review (sol): accept — The revision fully addresses the prior auditability finding. CHECKS.md now retains the literal URI-to-record-key query wrapper, both query-pass result tables, final node metrics, forbidden-signature c
…[truncated]
- 2026-08-10 review (claude): unknown — reviewer error: turn 1: API returned non-200 status code 429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later.
…[truncated]
- 2026-08-10 decision: accept — commit: 0171: validate buffered deletes on hardware
- 2026-08-10 usage: 7,430,857 tok (in 2,126,867, out 39,094, cache_r 5,264,896, cache_w 0) · cost n/a (unpriced)
  implementer: 3,835,331 tok (in 654,358, out 26,285, cache_r 3,154,688, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,766,791 tok (in 1,153,294, out 7,865, cache_r 1,605,632, cache_w 0) · cost n/a (unpriced)
  coordinator: 828,735 tok (in 319,215, out 4,944, cache_r 504,576, cache_w 0) · cost n/a (unpriced)
