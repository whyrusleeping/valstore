---
id: "0551"
title: Guarantee bounded I/O progress in LocalExecutor::run_until under continuously runnable work
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: vals-rt/src/lib.rs:293-307 only invokes poll_io when ready queue is empty and foreground wake flag clear. A cooperative background loop calling yield_now continually leaves ready work queued; I/O readiness/transport flush hooks never run until it finishes, potentially forever if completion depends on I/O. Used by CoordinatorService::run at vals-client/src/coord/service.rs:118-122. Read-only reviewer ran a standalone probe: first I/O hook after Some(1000) of 1000 background yields. run and run_until_and_cancel do not have this condition. Acceptance if accepted: deterministic regression with runnable yielding work and an I/O-driven foreground completion; bounded nonblocking I/O progress while runnable work exists, reserving blocking reactor waits for idle periods. Do not simply add a potentially 1ms blocking poll_io after every task poll (throughput risk).

## Outcome

run_until now calls its hook after every bounded drain round (like run/run_until_and_cancel), so a continuously yielding task can't starve I/O-driven completions. New LocalExecutor::run_until_with splits hooks: nonblocking while tasks/foreground are runnable, blocking park only when idle; CoordinatorService::run and submit_write_batch_in use it with reactor::poll_io_nonblocking/poll_io. Regression tests (fail on old gating, verified) plus park-only-when-idle test; vals-rt, vals-client, vals-cluster tests and clippy pass; reviewer accepted.

Commit: 0551: LocalExecutor::run_until polls I/O after every ready round (no starvation under self-yielding tasks); new run_until_with(fut, poll_io_nonblocking, poll_io) parks only when idle; CoordinatorService uses it
