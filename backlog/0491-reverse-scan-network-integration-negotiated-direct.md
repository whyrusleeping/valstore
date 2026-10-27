---
id: "0491"
title: 'Reverse scan network integration: negotiated direction, TCP dispatch and transport'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

Necessary implementation slice of accepted 0484, following 0487–0490. Expose the existing reverse serving and coordinator paths over the negotiated wire protocol, using a shared direction encoding for scalar and batched scans. NetTransport must implement reverse hooks, reject unsupported peers explicitly (never emulate via ascending), and preserve ascending compatibility. Add TCP tests for pagination/limits/keys-only, split routing, RT visibility and feature negotiation; wire malformed/roundtrip tests. Document the wire contract. Preserve pre-existing unrelated working-tree changes. Parent 0484 retains natural-timestamp 0481 latest-N benchmark, ascending/write performance gates, and any remaining public docs.

## Outcome

Implemented shared reverse MultiScan direction encoding (hello bit 5 + bit 4), scalar/batched NetTransport hooks, gated TCP dispatch, and old-peer errors without ascending fallback. Wire/TCP tests cover budgets, quorum RT visibility, limits, keys-only, chunking, shard routing, re-home fences and reconnect negotiation. Isolated candidate: 171 client + 967 cluster lib tests pass (7 ignored), 4 wire-fuzz tests pass, workspace check and Clippy succeed with warnings; direction-off mutation fails as intended. Independent review accepted, with known global-counter flakes passing isolated reruns. Parent 0484 remains in_progress for benchmark/performance and zones docs. Unrelated covered_empty hunks are excluded from this commit and saved for unchanged restoration.

Commit: feat(net): expose negotiated descending scans over MultiScan
