---
id: "0504"
title: 'Safe read point for sorted zones: closed-timestamp floor plus client-stamped append keys (design note first)'
status: proposed
priority: 4
created: "2026-10-23"
updated: "2026-10-23"
depends_on: []
spec_refs: []
---

## Description
## Why
Brought from the 0440 decision pass as the one remaining large queue/log primitive, recommended over long-poll/watch scans and PD leases/fencing (reasons in 0440's work log, 2026-10-23).

A consumer scanning a sorted zone in key order can't tell whether an earlier key will still arrive. Causes include a slow producer, a retry, or a quorum write that is acknowledged after a later one. 0471's queue benchmark avoided the problem by giving each partition one producer with dense sequence numbers and having consumers take only the contiguous prefix. That works only with one writer per partition. Anything with many writers per stream has no safe point to read up to: change feeds, event logs, time-series ingest, derived-data maintenance, or several producers on one topic. Today the only workaround is reading behind "now" by a guessed lag, which can silently miss a write.

## Proposal (to be settled in the design note, not implemented yet)
- **Append keys, stamped in the client library.** `append(zone, prefix, value)` writes the key `prefix ‖ version.hlc ‖ version.node`. The Rust client is the coordinator and already stamps one `Version` per write, so every replica gets the same key. Key order is HLC order within a prefix, and the writer identity in `node` keeps keys unique. This part may need no wire change.
- **Replica floor.** For zones that opt in, a replica rejects a live write whose version is more than `lag` below its own HLC, without storing it. Precedents: the HLC skew check, and the read-free `gc_grace` live-value floor in docs/blind-append-lww.md. The floor is monotone because the HLC is monotone.
- **Read point.** Scan replies carry each replica's closed timestamp, `closed = hlc_now − lag`, taken on the shard together with the scan snapshot. A reader that merged R replies takes `T = min(closed)`. If W + R > RF, no write stamped below T can still be acknowledged later: a write that missed a replica's snapshot is rejected by that replica from then on, and every W-set includes one of the R replicas read. This is quorum overlap, not consensus.
- **Costs.** A write that arrives late is rejected, and the producer retries with a new stamp, which gives a new key. Producers therefore need their own idempotency IDs, and consumers deduplicate. `lag` must cover the skew bound (`max_skew_ms`, 500 ms by default) plus the delivery and retry time.

## Questions the design note must answer
- **Partial writes.** A write that got fewer than W acks can sit on one replica and be copied by anti-entropy or hinted handoff after T has passed it. It is not an acknowledged write, but a later reader starting below T would see it. Decide between exempting such copies from the floor and documenting it, or applying the floor to copy paths too.
- **Range lifecycle.** A new owner after a re-home cutover, split or merge must start with a floor at or above the closed timestamps the old owners reported. Otherwise it could accept a write that a reader using the old map has already passed. The same applies to restart, wipe and rejoin: the floor must survive recovery, as the `gc_grace` floor does.
- **Interactions.** `gc_grace` and tombstones (deletes stay exempt, as for the GC floor), range deletes, blind apply in both profiles, write concern (`Quorum(n)`, `One`), the read fence (0476), and MultiScan, reverse and keys-only scans.
- **Opt-in and versioning.** A per-zone option, hello negotiation for the closed timestamp in scan replies, and the Go client.
- **Overhead.** The floor check must add essentially nothing to the write path. Write throughput is the top priority, so measure it.

## Acceptance
- A design note (docs/, reviewed before implementation, like docs/range-deletes.md for 0439) answers the questions above and states the guarantee exactly.
- Implementation and validation get their own slices once the note is accepted. Validation needs at least: a kill-the-fix test that drops a late write below T under every write concern, a netsim/fault test across re-home and restart, and a hot-path A/B showing no measurable cost when the option is off.


## Acceptance criteria

## Work log
