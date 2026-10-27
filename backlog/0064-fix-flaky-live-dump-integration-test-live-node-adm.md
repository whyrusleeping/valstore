---
id: "0064"
title: Fix flaky live_dump integration test (live_node_admin_dump_round_trips_while_serving)
status: done
priority: 4
created: "2026-07-05"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
`cargo test -p vals-server --test live_dump` fails intermittently (~1 in 5–8 runs, observed 2026-07-05 during task 0062, and reproduced at HEAD `323cdb3` — pre-existing, unrelated to the cleanup-quiesce change). Failing runs take ~21s vs ~1.3s for passing runs, suggesting a timeout/deadline is being hit inside the test.

## Acceptance criteria
- Root cause identified (capture the failure output; the ~21s runtime hints at a stalled wait hitting a deadline).
- Test made deterministic or the underlying race fixed.
- `cargo test -p vals-server --test live_dump` passes 20× consecutively.

## Acceptance criteria

## Plan

Root cause (reproduced on 1st debug run; confirmed by 13 leftover /tmp/valsd-live-dump-* failure dirs): the test's "node kept serving" check writes key `post-dump` into the same zone but never adds it to `expected`. The subsequent parallel-dump `eventually` loop asserts `replay(dump) == expected`, so it only passes if the first dump attempt beats the ~100ms background flush of the `post-dump` write. When the flush wins (debug builds / loaded machine), `post-dump` appears in every dump and the loop can never succeed → 20s `eventually` deadline → ~21s failing run.

Fix (test-only, ~2 lines): after the serving check, insert `post-dump -> alive` into `expected` so the parallel-dump comparison converges deterministically once the flush syncs the tail (the eventually loop already tolerates the pre-flush state).

Verification: run the live_dump test binary (debug) 20× consecutively — must pass all; also run once via `cargo test -p vals-server --test live_dump`. Clean up stale /tmp/valsd-live-dump-* dirs.

Given the change is a tiny, test-only 2-line fix, I will make the edit directly as coordinator and self-review (review tier 'simple').

### Starting points
- crates/vals-server/tests/live_dump.rs:287-311 — serving check writes b"post-dump" but expected is not updated before the parallel-dump eventually loop

## Work log
- 2026-07-05 plan: Root cause (reproduced on 1st debug run; confirmed by 13 leftover /tmp/valsd-live-dump-* failure dirs): the test's "node kept serving" check writes key `post-dump` into the same zone but never adds it
…[truncated]
- 2026-07-05 context hints: 1 recorded with plan
- 2026-07-05 review tier: simple (coordinator self-review)
- 2026-07-05 decision: accept — commit: test: fix flaky live_dump — fold post-dump liveness write into expected state (task 0064)  The serving-liveness check writes key `post-dump` into the dumped zone but never added it to `expected`, so
…[truncated]
- 2026-07-05 usage: 7,420 tok (in 34, out 7,386, cache_r 382,279, cache_w 24,473) · cost n/a (unpriced)
