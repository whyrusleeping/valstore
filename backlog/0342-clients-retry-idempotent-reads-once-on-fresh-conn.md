---
id: "0342"
title: 'Clients: retry idempotent reads once on fresh conn after transport error; per-replica causes in Rust QuorumFailed'
status: done
priority: 4
created: "2026-09-29"
updated: "2026-10-13"
depends_on: []
spec_refs: []
---

## Description

Proposed from 0336 (bench/0336-replica-integrity/FINDINGS.md §5, §7).

- In census0326, one node1/shard5 page failed with `connection reset by peer`. The likely cause is a race between the server's 120 s idle close (node1 counted 164 `vals_transport_idle_timeouts`) and a client request on the reused multiplexed connection.
- Proposal: in valsgo and the Rust vals-client, retry an idempotent read or scan request once on a freshly dialed connection when the failure is a transport error on a connection that had been idle. Alternatively, redial before reuse once the connection has been idle longer than the server's idle timeout.
- Separately, the Rust `CoordError::QuorumFailed {needed, got}` (`crates/vals-client/src/coord/scan.rs`) throws away the per-replica causes. Mirror valsgo `793f055` (ReplicaError / ScanQuorumError).

Acceptance: fake-server tests for an idle-closed connection and for replica-attributed errors.

## Outcome

Rust vals-client: MuxRpc marks requests sent on a reused connection resumed after ≥1 s quiet (until its first reply); rpc_with_wait resends idempotent reads (Read/Digest/Scan/MixedBatchRead/Sync*/metadata) once on Unavailable, counted in idle_conn_retries; writes and timeouts are never resent. Scans now fail with CoordError::ScanQuorumFailed {zone_id, cursor, needed, got, replicas: Vec<ReplicaError{node, error}>} (TransportError gains PartialEq/Eq). valsgo (vals-examples 160e2f1): requestIdempotent for Get/Scan/Members/Zones/RangeMaps with the same rule. Fake-server tests: Rust idle-closed read retried / write not / busy-conn read not; vals-cluster scan replica attribution (Remote corrupt + Busy, Display text); Go idle_retry_test.go (go test -race x3). vals-client and vals-cluster lib suites pass; delayed_hello test flake is pre-existing (5/16 on d40c34e1), filed as proposed 0437.

Commit: feat(0342): clients resend idempotent reads once on a fresh conn after an idle reused conn fails; Rust scan quorum failures name each failed replica and cause (CoordError::ScanQuorumFailed)
