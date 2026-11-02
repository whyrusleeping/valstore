---
id: "0552"
title: Coalesce repeated task wakeups in the runtime ready queue
status: done
priority: 3
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548: vals-rt/src/lib.rs:47-52,74-78 enqueue one ReadyTask for every wake, guarded only by task active state (not already queued). drain_ready polls every duplicate for pending tasks. A read-only reviewer executed a probe issuing 10,000 wake_by_ref calls to one task: it was polled 10,001 times including initial poll. Child I/O completions in join_all can share one parent waker, so this is not restricted to a malicious external waker. Ready memory and poll work scale with notifications rather than runnable tasks; pass budget equals full queue size, also delaying foreground/I/O. Acceptance if accepted: per-task scheduled/queued bit with correct cross-thread lost-wake handling, cleared at the right boundary before polling; deterministic repeated/concurrent-wake regressions and relevant runtime tests. Measure actual ingest/latency before claiming production speedup.

## Outcome

TaskNotify gained a `queued` AtomicBool (set at spawn, swapped true on wake so only the first wake pushes a ReadyTask, swapped false with AcqRel in drain_ready just before polling so a wake during poll requeues and coalesced cross-thread writes are visible). New tests: repeated_wakes_before_a_poll_queue_the_task_once (10,000 wake_by_ref -> ready len 1, 2 polls), spawned_task_woken_from_another_thread_during_poll_is_not_lost, concurrent_wakes_coalesce_without_losing_the_last_one (4 threads x 5,000 wakes, ready len <= 1); the first and third fail with coalescing disabled. vals-rt 39 tests + clippy, Miri on the wake tests, and vals-client/cluster/server/store/bench test suites pass. No production ingest/latency speedup measured or claimed. Reviewer (sol) accepted; its comment-convention nit was fixed by moving the rationale into the field doc comment.

Commit: vals-rt: coalesce repeated task wakeups with a per-task queued bit cleared just before polling (0552); 10,000 wakes before a poll now queue/poll once, wakes during a poll still requeue
