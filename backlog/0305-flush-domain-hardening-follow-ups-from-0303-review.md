---
id: "0305"
title: Cover flush-domain cancellation failure propagation and shutdown timeout
status: done
priority: 2
created: "2026-09-24"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description
Add the missing correctness coverage identified in the independent review of shipped 0303 behavior (`bench/0303-flush-domain-ab/FINDINGS.md`). First check whether later tests already cover each case; extend existing fixtures rather than create a parallel implementation.

## Acceptance criteria

- Cancel/requeue with multiple waiters and a new arrival during hand-off: exactly one result per waiter, no lost wake or stranded request.
- A domain flush failure reaches another shard's waiters, not only the initiating shard.
- Exercise the shutdown drain timeout path and its documented failure behavior with a bounded test.
- Tests exercise the actual implementation and fail when the relevant protection is removed. Run the focused and debug cluster suites.
- No speculative caching of `FlushDomain::get`, no new virtual-clock framework absent demonstrated need, and no change to the one-flush-in-flight performance property merely to satisfy a stylistic preference.

## Work log

- 2026-09-29: owner accepted the test-only slice at P2. The earlier lookup-lock optimization and virtual-clock suggestions are not implementation scope. An orphan flush can overlap its successor without a known correctness defect; in-flight SPDK I/O at a drain timeout/panic can still abort in `vals_spdk_close`. Do not weaken that safety behavior to make a test pass.
- 2026-09-30 BLOCKED (coordinator): the tests are done and committed on branch `0305-flush-domain-tests` (6435b4b7, a fast-forward from master ef9bb766). They are not on master because the fixtures live in `crates/vals-cluster/src/net/server/staged_apply.rs`, and that file already has the owner's uncommitted 0338 hunks (`isolated_env_test`). The commit tool cannot separate the two changes, and the owner chose to keep 0338 uncommitted. This is the same decision 0267 is blocked on. The branch contains: (1) `cancelled_leader_mid_flush_hands_off_without_failing_waiters`, extended to a cancelled leader with two waiters plus a new arrival during hand-off. It asserts that both waiters are woken, each future gets exactly one Batch result with no stranded waiter, every ack is covered by the oracle, and the group is not left running. (2) New `domain_flush_failure_reaches_other_shard_waiter`: shard B waits on shard A's failed domain flush and gets "device flush failed" with no blocking fallback and no own submit. (3) New `finalize_shutdown_times_out_with_unfinished_domain_flush_tracked`, using a cfg(test) thread-local override of SHUTDOWN_FLUSH_DRAIN in shared.rs (production still uses 5 s). It checks that finalize returns within the bound, the flush stays tracked, the domain stays inflight, and completed/failed are not advanced. Production change: `FlushDomain::failed` is now pub(super). Mutation checks in debug: removing the hand-off wake in SyncLeaderGuard::drop fails (1), and so does dropping the requeued batch. Removing the `failed >= need` checks at both apply.rs sites fails (2); removing only the first site livelocks in the claim loop. Removing the drain-timeout bail makes (3) hang past 120 s. Results: `cargo test -p vals-cluster` in debug passed on the dirty tree (701 lib tests plus integration tests), and the lib suite passed on the branch alone (690 passed). To unblock, the owner resolves 0338. Then either run `git stash; git merge --ff-only 0305-flush-domain-tests; git stash pop`, or cherry-pick 6435b4b7 on top of the committed 0338. After that, close this task.
- 2026-09-30 (done): 0338 committed (5af3c40c); 6435b4b7 cherry-picked cleanly onto master as b7f223c3 (auto-merged staged_apply.rs). The full debug workspace suite passed at b7f223c3 on a clean checkout via `scripts/ci.sh` (1,366 passed, 0 failed).
