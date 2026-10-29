---
id: "0542"
title: PD inspection issued before a new leader's first commit in its term is dropped by raft-rs and times out after 5 s
status: proposed
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found while getting the tests to run on macOS (0525). raft-rs 0.7 `step_leader` silently drops a `MsgReadIndex` when the leader has not yet committed an entry in its current term (`if !self.commit_to_current_term() { return Ok(()) }`, raft.rs ~2105). `pd/daemon.rs::inspect` issues `partition.read_index(ctx)` as soon as `status.role == "leader"`, so an admin inspection (export-state, backup create/load/compare, status with authority) that lands in the window between election and the no-op commit never gets a read state and replies non-authoritative after the 5 s admin timeout: "pd N could not confirm itself as the current leader (role leader, leader N)".

Evidence: on an Apple M1, `vals-server --test backup a_three_pd_restore_refuses_voters_seeded_differently_and_loads_identical_seeds` failed 2 of 5 runs this way (leader elected at 57.194, cluster stopped 5.2 s later with the read never confirmed); the test calls `backup load` once, right after `leader_sock()` sees role=leader. The window exists on Linux too, just narrower.

Likely fix: in `vals_raft::Partition::read_index`, defer the ctx while the node is leader and `!raft.commit_to_current_term()`, then issue deferred reads from `drive()` once the term has a commit (drop them on losing leadership; the daemon already answers pending reads on role/term change). Regression test: a read index requested right after campaign, before the no-op commits, still yields a read state.

Acceptance: an inspection issued immediately after election is authoritative once the term's first entry commits; regression test; the backup test above passes repeatedly on macOS.

## Acceptance criteria

## Work log
