---
id: "0482"
title: Batched multi-prefix scan RPC with a keys-only option
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on:
    - "0481"
spec_refs: []
---

## Description

## Description
## Why
Graph traversals (0481), and queue/log consumers too (0440 item 4), need many small scans: "first N edges of each of these 500 vertices". Today each one is a separate `Coordinator::scan_in` with its own `Scan` RPC per replica. Scans with a `limit` also run one range slice at a time. One traversal hop therefore costs one round trip per frontier vertex. Point reads already have the batched answer, `MixedBatchRead` (≤ 4096 keys per RPC, position-stable, paged), and batching took point reads from ~1.19M to ~5M keys/s. Scans have no equivalent.

Separately, every scan reply carries values. Runs are index-only, so each returned entry costs a value-log read unless 0028 co-compaction has made the values sequential. The engine's `IndexEntry` already holds the key, the LWW version and the tombstone bit. A **keys-and-versions-only** scan could therefore be served from the memtable and run blocks with no value-log I/O. Uses: degree counts, existence checks, adjacency lists whose data lives entirely in the key, and queue lag/depth.

## Sketch (to be checked against code)
- **Wire:** a new opcode (e.g. `MultiScan`) behind a new hello feature bit (next free is bit 4):
  - Request: `[u64 epoch][u32 zone][u8 flags][u32 start][u32 count]`, then `[opt lo][opt hi][u32 max] × count`.
  - `flags` bit 0 = keys-only (key, version, tombstone; no value bytes).
  - Reply: position-stable per sub-scan, each with its own `more` / resume key and range-tombstone fragment trailer under the same rules as `Scan`. Paged under the 8 MiB reply budget like `MixedBatchRead` (a contiguous prefix of sub-scans, resumed with `start`).
  - Keys-only could also be a flag on plain `Scan`. Check whether its decoder tolerates trailing bytes, or use the new opcode for both.
- **Server:**
  - Fence each sub-scan's span exactly like `Scan` (0476).
  - Serve sub-scans in one task, sharing the read gate.
  - Overlap their run-block and value reads at queue depth as `serve_scan` does, and coalesce value spans across the whole request.
- **Coordinator:** `scan_many_in(zone, &[(lo, hi, limit)], r, keys_only)`.
  - Split each sub-scan by range slice and group by (replica, shard).
  - Apply the min-coverage paged merge (algorithms.md §6) **per sub-scan**; a sub-scan that crosses ranges continues into the next slice.
  - Return results in input order.
  - Fall back to per-sub-scan `Scan` against peers without the feature bit.
- Go client (valsgo) parity is a follow-on, not part of this task.

## Acceptance
- Unit and integration tests:
  - batched results equal N independent `scan_in` calls, including sub-scans that span splits, re-home pending ranges, range tombstones and paging under a forced small reply budget (`set_zone_scan_reply_budget`);
  - keys-only results equal the full scan's (key, version) set;
  - mixed-version fallback works.
- A kill-the-fix test for the per-sub-scan min-coverage rule.
- 0481 A/B, out-neighbours and 2-hop: report RPCs/query, p50/p99 and throughput against the per-vertex `scan_in` baseline. The keys-only degree count must show zero value-log reads.
- Write path unchanged: no apply-path cost.
- Docs: formats.md §1.3 / §1.5, algorithms.md §6, ARCHITECTURE.md.

## Acceptance criteria

## Outcome

MultiScan (bit 4/opcode 23) adds shard batching/paging, per-query quorum/min-coverage and keys-only scans, with scalar fallback and value-free cold negotiation. Clean workspace check, scan/codec/fuzz tests and min-coverage mutation pass; both independent reviewers accept. Corrected graph ABBA: two-hop RPC/query 28.94→8.108, mean p50/p99 −31%/−29%, mixed throughput +8.1%; isolated full scans still favor scalar scan_in. Local and SPDK degree checks return 66,836 keys with zero value-record reads; every arm ends with 319,804 edges each direction and zero mismatches. No apply-path or disk-format changes. Findings/receipts in bench/0482-multi-scan; lab restored. Unrelated wire hunks excluded and preserved in /tmp/vals0482-before/preexisting.patch for immediate restoration.

Commit: feat(0482): batch sorted scans with negotiated index-only replies
