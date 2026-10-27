---
id: "0259"
title: Diagnose retained-catalog node shutdowns exceeding the five-minute drain deadline
status: done
priority: 2
created: "2026-09-06"
updated: "2026-09-08"
depends_on: []
spec_refs: []
---

## Description

During the 0256 retained-catalog recovery gate, the initial node-1 stop and final node-1/node-2 teardown exceeded systemd's 300s stop deadline and were SIGKILLed; a later node-1 stop completed cleanly in 48s. Evidence: bench/0256-hardware-20260906/FINDINGS.md, stop-node1-journal.txt, stop-node1-second.txt, teardown-node-journals.txt and phase logs. Recovery-seeding involvement is a hypothesis, not established root cause. This did not invalidate the independently successful affinity recovery cycle, but leaves crash recovery necessary after operational shutdown.

If accepted: reproduce on an authorized fleet or representative local fixture, capture live shutdown task/thread state before timeout, identify the blocking drain/sync stage, and implement only an evidence-supported fix. Verify graceful persistence/readback and bounded shutdown under the failing conditions; do not merely increase systemd timeouts or disable affinity enforcement. Preserve cores, matching binaries, retained test corpus unless separately authorized, and other fleet gates.

## Outcome

Fixed a locally reproduced unbounded executor-task wait after connection drain. Shards and handle-bearing replica servers now run shutdown as the foreground completion condition, preserve per-pass I/O polling, cancel unfinished tasks, then perform unchanged persistence and joins. Retained-run stalled-read regression completes in ~2s and verifies reopen/readback; reverting the fix fails its 6s watchdog. Runtime cadence kill-the-fix also demonstrated. Coordinator verified 33 runtime tests and 594 cluster library tests passed (4 ignored); implementer verified 11 shutdown-filtered tests and ordinary clippy with existing warnings. Both independent reviewers accepted. Historical 0256 logs show drain requested without shutdown completion, but do not establish whether executor tasks or later stages stalled; SPDK drops, syncs, and joins remain potentially blocking. No fleet services/data/forensics touched. Optional non-destructive hardware confirmation recorded as proposed 0276.

Commit: fix(cluster): cancel unfinished shard tasks after shutdown drain
