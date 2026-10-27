---
id: "0490"
title: 'Reverse coordinator foundation: descending quorum pagination and batched scans'
status: done
priority: 3
created: "2026-10-17"
updated: "2026-10-17"
depends_on: []
spec_refs: []
---

## Description

Necessary implementation slice of accepted 0484, following committed 0487–0489. Add reverse-only Transport hooks (unsupported by default; no ascending emulation), Coordinator::scan_in_rev and scan_many_in_rev using descending range-slice traversal and max coverage lower-bound LWW/RT merging. Preserve existing ascending/write paths and unrelated dirty wire files. Verify scalar/batched limits, per-query progress, prefix/empty keys, empty RT pages and small server reply budgets, multi-replica splits and committed-token authority during pending re-homes, failure/unsupported behavior, and a kill-the-fix regression for the mirrored bound. Use the existing internal reverse serving through an in-process test transport; no negotiated RPC is claimed. Document API/coverage in algorithms/scan-performance. Parent 0484 retains negotiated Scan/MultiScan direction integration, network cluster tests, latest-N natural-timestamp benchmark vs inverted baseline, performance gates and remaining public docs.

## Outcome

Added reverse-only Transport hooks and scalar/batched Coordinator APIs with descending committed-token traversal, max lower-coverage merging, LWW/RT filtering and exclusive pagination. Eight targeted tests cover budgets, limits, failures, splits, pending re-homes and quorum intersection; max→min mutation fails. Client/cluster suites pass (168+968, 7 ignored), workspace check and clippy succeed with warnings. Two independent reviewers accepted. Parent 0484 remains in_progress for negotiated network integration and performance/benchmark gates; unrelated dirty work preserved.

Commit: feat(client): add descending quorum scan coordinator foundation
