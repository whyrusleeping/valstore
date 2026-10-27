---
id: "0237"
title: Make Raft persistence and state-machine failures fail-stop
status: done
priority: 1
created: "2026-08-23"
updated: "2026-09-07"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R2 — Raft messages escape before durable state, and persistence failure is non-fatal (P0 control-plane safety)
---

## Description

Make durable Raft storage and state-machine apply failures fatal/restart-required rather than logging and continuing after an incompletely handled Ready. Verify the full Ready/LightReady lifecycle, outbox handling, and restart recovery under injected failures.

Scope correction from owner-approved backlog triage: raft-rs 0.7 distinguishes messages permitted before persistence (`take_messages`) from persistence-dependent messages (`take_persisted_messages`). Current drive code uses that distinction. Previously committed entries may also be applied before persisting the current Ready. The original review's unsafe-vote/message-ordering diagnosis is not established and must not motivate an unnecessary ordering rewrite. The confirmed problem is continuing after a failed Ready without completing the required lifecycle.

## Acceptance criteria

- Inject log append, hard-state, snapshot, and state-machine apply failures, including LightReady handling where applicable.
- Failed persistence/apply stops service and requires recovery; the daemon cannot continue serving a partially applied state or driving an abandoned Ready.
- Outbox behavior on failure is explicit and no persistence-dependent message escapes before its prerequisite durability.
- Restart from durable state restores consistent committed state; tests cover failure partway through applying a batch.
- Preserve raft-rs's legal pre-persistence message behavior; pin the demonstrated failure with kill-the-fix tests.

## Outcome

Partition drive failures now terminally refuse further driving/service and discard all unsent outbox traffic; PD daemon aborts under existing supervision. Preserved raft-rs Ready/LightReady ordering. Repaired restart recovery after snapshot durability precedes hard-state persistence. Fault-injection regressions cover append, hard state, LightReady commit, incoming/local snapshots, and mid-batch Ready/LightReady apply recovery. Both independent safety reviewers accepted. Verified vals-raft 23 passed, vals-cluster 582 passed/4 ignored and all targets compile, diff check clean.

Commit: Make Raft persistence and apply failures fail-stop
