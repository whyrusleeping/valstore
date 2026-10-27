---
id: "0330"
title: Remove baseline (winner-directory) AE seed path after exact value census + populated-rehome oracle
status: done
priority: 2
created: "2026-09-28"
updated: "2026-09-29"
depends_on:
    - "0316"
spec_refs: []
---

## Description

0316 made the streaming AE seed the default. `VALS_AE_SEED_MODE=baseline` remains as the opt-out. 0316's local cgroup gate (bench/0316-reserve-gate/report.md) found:
- **Tight 512 MiB arm, baseline:** never reached exact digest equality within 393 s, with 6 seed sheds and 16k reservation deferrals.
- **Stream:** equality at 154 s.

0316 deferred removing the baseline path because two prerequisites were missing.

Prerequisites:
- **Exact byte-for-byte value census** of a rebuilt replica. The 0316 probe compares AE Merkle digests only.
- **Verified populated-range rehome** under the stream seed. In the 0316 harness, netload wrote zone 0, so the zone-1 split never exercised a populated rehome.

Acceptance (narrowed 2026-09-29):
- Both oracles pass under the stream seed.
- Run the vals-cluster debug suite and investigate failures rather than retrying until green.

Scope split 2026-09-29: the deletion half (baseline winner-directory accumulator, `seed_progress.pending` / `projection_pos` / published cleanup, `baseline_for_test` seam, the six baseline-mechanics tests from bench/0289-ae-seed-ab/report.md, the `VALS_AE_SEED_MODE` opt-out and docs/backpressure.md update, preserving legacy sidecar compatibility and the latency-profile winner directory) moved to task 0346. Reason: those edits land in files holding the owner's uncommitted 0338 changes (`replica/mod.rs`, `docs/backpressure.md`, ...), which cannot be committed until 0338 is committed.

## Outcome

Both 0330 prerequisite oracles pass under the stream AE seed (local, file-backed, 3-node RF2, in-process): (1) a wiped node-3 rebuilt by AE matches the model and surviving peers exactly (key/version/value bytes/tombstones, both directions) over run-backed data with overwrites, deletes and a merge; (2) a populated sorted range rehomed to a previously empty non-owner (copy observed in progress then complete, writes during/after) matches the model and retained source exactly, with AE convergence. Stream mode forced in isolated child processes and asserted via seed fold ops > 0 and zero baseline winner cleanup. New tests stable 15/15 locally, pass at HEAD alone; vals-cluster debug lib suite 708 passed. Deletion of the baseline mechanics/opt-out/docs moved to 0346 (todo) because it edits files holding the owner's uncommitted 0338 changes. Not SPDK/hardware validation.

Commit: test(0330): stream-seed correctness oracles — exact byte-for-byte rebuilt-replica census and populated sorted-zone rehome census (node/seed_oracle_tests.rs); baseline deletion split to 0346
