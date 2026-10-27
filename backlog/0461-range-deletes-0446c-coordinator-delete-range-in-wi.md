---
id: "0461"
title: 'Range deletes 0446c: coordinator — delete_range_in(_with_persistence), Transport::range_delete, fragment-aware scan merge with resume keys, end-to-end + lagging-replica + head-of-queue counter tests'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0460"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0446 (docs/range-deletes.md "Semantics", §2 coordinator side, validation plan). vals-client coord + vals-cluster LocalCluster/tests.

## Outcome

Coordinator::delete_range_in(_with_persistence): sorted zones only; InvalidRange / Unsupported(node) checked before stamping; one version, per-range slices grouped per replica (≤4096/request, 8 concurrent), W per slice, RangeDeleteIncomplete { version, missed }, StaleEpoch refresh + retry of missed spans with the same version. Transport gained range_delete and supports_range_tombstones (NetTransport: negotiated hello bits, Members probe when unknown; LocalCluster applies slices and returns clipped fragments). ScanPage carries fragments + resume; resume = coverage; all responders' fragments (single-responder too) hide covered winners. Tests: LocalCluster + NetTransport e2e, stragglers, R=2/3 lagging-replica multi-page scans, refusal/incomplete/stale cases, head-of-queue counters (1 and 3 nodes, N=10k vs 200k, before/after fold, compaction, cold reopen) failing with block skip or max tree disabled. vals-client + vals-cluster suites green; docs updated.

Commit: feat(0461): range-delete coordinator — delete_range_in, Transport::range_delete, fragment-aware scan merge with resume keys; e2e, lagging-replica and head-of-queue counter tests
