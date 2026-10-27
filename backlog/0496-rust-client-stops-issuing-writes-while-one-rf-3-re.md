---
id: "0496"
title: Rust client stops issuing writes while one RF-3 replica host is black-holed
status: done
priority: 3
created: "2026-10-21"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description

Found by the 0280 host-loss drill (bench/0280-three-pd/FINDINGS.md, 2026-10-21). While lab-1 rebooted (54 s unreachable, no RST), `vals-bench vals netqueue` with `VALS_BENCH_BOUNDED_CLIENT=1` (32 producers, inflight 4, RF 3, W 2, sorted zone) acknowledged ~63 writes per 5 s with p99 = the 5 s RPC deadline, and the two healthy nodes received ~13 applies/s instead of 2,000. So the client, not the servers, stalled: each request waited for the dead replica. A data-node process restart (connection refused at once) cost only 1–2 s. Earlier (0471 notes) the unbounded client hung 8 of 32 producers in ep_poll after a box reboot.

Acceptance: find where the coordinator/transport/netqueue path blocks on the unreachable replica (connect, write buffer, or waiting for all RF replies); with one replica host black-holed, W=2 writes continue at near full rate on hardware or a reproducible local blackhole test; record the before/after.

## Outcome

Cause: batch_once and range_delete_round waited for every replica's reply (RPC deadline on the black-holed node), and after a reset each range delete re-probed the dead node's features. Fix: vals_rt::join_until; batches resolve once every entry reaches W; range deletes use a rolling 8-request window (≤4 per node), skip met requests, resolve at quorum; range-tombstone support falls back to last-negotiated features. Local blackhole test (6 rounds of Majority batch + range delete, 3 s deadline): before 36.0 s hung / 51.0 s reconnecting, after 38–66 ms. Unit test covers >8-request deletes with early/late dead node. Hardware 0280 drill not re-run.

Commit: fix(0496): W<RF batch writes and range deletes resolve at quorum instead of waiting for a black-holed replica; range-tombstone support uses last-negotiated features across reconnects
