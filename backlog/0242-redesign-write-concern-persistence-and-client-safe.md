---
id: "0242"
title: Decide write-concern and persistence contracts before API freeze
status: done
priority: 4
created: "2026-08-23"
updated: "2026-10-15"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R8 — “Durability” controls replica acknowledgements while default writes are not device-durable (P1)
    - docs/production-readiness-review-2026-08.md#R9 — Client liveness and memory protection are opt-in (P1)
---

## Description

Decide and then implement validated write options and explicit write-concern/persistence semantics in place of misleading method combinations. Record the simple write/delete default and consumer migration before implementation; preserve an explicitly named high-throughput buffered path and reject impossible/zero quorums rather than silently clamping them.

Owner-approved triage separated finite client deadlines and operation/byte/per-node budgets into accepted task 0264. This API contract redesign is retained as accepted conditional work, but blocked pending the owner's explicit default-semantics/consumer-migration decision. Backlog acceptance on 2026-09-29 does not choose those semantics. Coordinate identity/format changes with 0236 and supported API work with 0269; do not silently freeze a new API or change persistence defaults as part of resource-limit work.

## Outcome

Rust client writes take `impl Into<WriteOptions>`: bare `WriteConcern` and `write(k,v)` (All) are buffered; `WriteOptions::device_durable(c)` waits for device sync. `Durability` → `WriteConcern::{Unacknowledged, One, Majority, All, Quorum(n)}`; write_tier[_in]/try_write_tier → write_with/write_in/try_write; all *_with_persistence removed. Quorum(0), W > replicas and Unacknowledged+Device fail with `InvalidWriteOptions` before stamping/sending (single, sloppy, batch per entry, range delete incl. stale retry). All workspace callers migrated. Kill-the-fix-verified tests in vals-client and range_delete_tests. Docs: getting-started contract + migration table + unchanged Go client contract (Put buffered, PutBatch synced), batch-writes, ARCHITECTURE, algorithms, range-deletes, readiness R8. Workspace tests green except known parallel flakes (pass alone); spdk check clean; 2/2 reviewers accept.

Commit: feat(0242): validated WriteOptions { WriteConcern, Persistence } replace Durability and *_with_persistence; buffered default, explicit device durability, impossible W rejected instead of clamped; docs + migration notes
