---
id: "0272"
title: Verify and close the daemon membership assertion race
status: done
priority: 3
created: "2026-09-07"
updated: "2026-09-29"
depends_on: []
spec_refs: []
---

## Description

Current scope (2026-09-29): verify-and-close, not a new implementation. The current test already polls the `pd-admin.sock` membership reply with `eventually("PD boot membership committed", ...)`; the original description below misidentifies that view as node 1's admin view. Verify the actual queried authority, preserve member/weight/restart assertions, and repeat the existing test before recording closure. Only change code if a concrete residual race remains.

### Original report
Comprehensive review of 0236 reproduced a pre-existing timing race in crates/vals-server/tests/daemon.rs, three_node_cluster_with_pd_serves_zones_and_survives_restart: coordinator refresh_ring can see another node adopt membership before node 1's admin members view updates. Baseline reproduced 1/30 isolated runs; revised handshake tree 4/48 on review host. Consider waiting boundedly for node 1's own expected membership rather than immediately asserting its line count.

Acceptance if approved: preserve the original cluster/restart assertions, wait on the actual queried node's observable adoption with a useful timeout diagnostic, and demonstrate repeated isolated/parallel runs without this race. Do not mask real missing-member failures by weakening expected membership.

## Outcome

Verified closed without a code change. The test queries the PD admin socket, which serves the PD's committed-membership mirror. Boot membership is committed in one atomic ConditionalBatch, so the existing eventually(4 lines) wait, added in 7e5f1877, waits on the correct authority with exact membership. The weight, data, zone and restart assertions are all preserved. Results: isolated 30/30; parallel 64/64 at 8-way, 92/96 at 16-way and 185/192 at 48-way; whole binary 20/20. None of the parallel failures was the membership race; all trace to free_addr port collisions between concurrent test processes. A temporary immediate-assert negative control failed 37/96 on the race.

Commit: backlog(0272): verify-and-close daemon PD membership assertion race — existing eventually() wait on PD committed members closes it (0 membership failures across 400+ isolated/parallel runs; immediate-assert negative control fails 37/96)
