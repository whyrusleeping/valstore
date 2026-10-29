---
id: "0526"
title: Timing-assertion tests fail when the CI machine's CPUs are saturated
status: in_review
priority: 4
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found during 0506 (2026-10-29). `scripts/ci.sh` passes on devbox unloaded and beside `stress -c 48` (load 54–100), but beside `stress -c 160` (load ~167 on 192 cores) all three runs failed on wall-clock assertions: vals-cluster `maint::tests::cover_scans_progress_while_merges_are_continuously_queued` (3/3), `antientropy::tests::cleanup_owner_probe_cuts_a_grown_window_when_the_peer_slows_down` ("one pass took 784ms"), `net::tests::a_connection_silent_for_a_deadline_is_replaced_and_the_next_request_succeeds` (QuorumFailed), and vals-server `tests/rehome_regroup.rs` `knockout_whole_engine_invalidation_reseeds_peers_on_a_rehome` (timed out). List in plans/testing-notes.md ("CPU saturation").

Proposal: make these assertions robust to starvation (logical progress counts instead of wall-clock budgets, longer deadlines, or isolation), so the public CI lane stays green on oversubscribed runners. Acceptance: ci.sh passes 3 runs in a row beside `stress -c 160` on devbox, with each test still failing when its guarded behaviour is removed.

## Acceptance criteria

## Work log

- 2026-10-29 (uncommitted, in the main worktree; accepted by the owner as part of "fix the identified test issues"): fixed the four tests above plus vals-server `daemon.rs` `three_node_cluster_with_pd_serves_zones_and_survives_restart` (node 1 `ops >= 100` now polled while reads are sent to node 1). Root causes and changes are in plans/testing-notes.md ("CPU saturation"); notably the rehome_regroup timeout was a port collision (node 2 and PD metrics), fixed by holding all port reservations while allocating. Kill-the-fix: each test fails with its guarded behaviour removed (scan-lane reservation 0; cleanup cutoff disabled / stop ignored; reconnect reset no-op; span regrouping instead of invalidation; server op counter no-op). Each target passed 5/5 beside `stress -c 160`; vals-cluster lib passed unloaded (1008 passed). Not yet done: the acceptance's three full `scripts/ci.sh` runs beside `stress -c 160` (deliberately not run on the shared devbox while other sessions build).
