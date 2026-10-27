---
id: "0239"
title: Make vals-rt RawWakers sound for cross-thread wakeups
status: done
priority: 1
created: "2026-08-23"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R4 — `vals-rt` constructs `Send + Sync` `Waker`s over `Rc`/`Cell` (P0 safe-Rust unsoundness)
---

## Description

Replace RawWakers backed by Rc/Cell/RefCell with a genuinely thread-safe wake state and executor notification path. Safe futures may send Waker to another thread, so the current single-thread convention is insufficient. Add a cross-thread wake regression and sanitizer/Miri coverage where practical.

## Outcome

Replaced Rc/Cell RawWaker vtables with safe Arc-backed std::task::Wake implementations, synchronized task notifications and atomic foreground flags while preserving local non-Send futures and bounded ready draining. Weak notification identities reject stale slot-reuse wakes; retained wakers remain safe after executor destruction. Added cross-thread clone/wake/wake_by_ref/drop, wake-during-poll and lifetime regressions. Verified 30 vals-rt tests, clippy with warnings denied, dependent-crate checks, store tests (308 passed, 7 ignored), cluster tests and diff checks; two independent reviewers accepted. Miri attempted but unavailable for installed pinned toolchain. poll_io retains its existing nonblocking-poll expectation.

Commit: fix(rt): make executor wakers safe across threads
