---
id: "0260"
title: Batch rehome write mirrors to eliminate donor BatchApply deadline overruns
status: done
priority: 2
created: "2026-09-07"
updated: "2026-09-07"
depends_on: []
spec_refs: []
---

## Description

Required prerequisite split from accepted task 0233 after its fresh hardware gate failed. Diagnosis: bench/0233-fresh-hardware-20260906/DIAGNOSIS.md. mirror_rehome_writes in crates/vals-cluster/src/net/server/placement.rs serially awaits one durable RPC per entry per future target before connection.rs can respond to the donor BatchApply. The captured cross-shard move generated 14,743 mirror acknowledgements versus 14,800 predicted across four retries, with zero inner timeouts while each external call exceeded 5 seconds. Existing batch apply histogram ends before mirror wait.

Implement bounded mirror batching by pending range/future target (respect transport shard routing), preserving original LWW versions, sync/durability, timeout-uncertain idempotent retry, fail-closed acknowledgements, per-range failure attribution, and epoch/write-mark/copy-completion/response fences. Do not relax safety fences or simply raise client deadlines. Add focused large cross-shard tests demonstrating RPC count scales with bounded destination groups rather than individual entries, full success requires all required acknowledgements, and partial/timeout failures remain correctly attributed and fail-closed. Add an outer BatchApply response-readiness latency measurement covering local apply and mirror waits, with clear metric semantics. Review as correctness-sensitive write-path work. Hardware verification remains in 0233, which should rerun the fresh 500–600k child gate after this prerequisite; this task need not consume another fleet run.

## Outcome

Implemented 1,024-entry bounded range/target mirror batches with original versions/sync, complete-ack fail-closed validation, unchanged timeout retry and response/write-mark/epoch fences, and per-range failure attribution. Added response-readiness histogram covering handler dispatch, local apply and mirrors, excluding pre-dispatch rejections/transmission. Per-range RehomeMirrorStat ok/failures now count batch/target outcomes; global rehome_mirror_applied still counts entry acknowledgements. Focused 4,113-entry RF=2 cross-shard test demonstrates 10 calls instead of 8,226; partial ack/timeout tests and all 9 mirror-filter tests pass. Implementer verified workspace check, vals-client 123 unit tests + 2 doctests, cluster suite 575 passed/4 ignored with one unrelated timing-sensitive test passing isolated. Both correctness reviewers accepted; final coordinator touch-up clarified metric HELP boundary. Hardware not run; 0233 is now dependency-ready for the fresh populated-child gate.

Commit: Batch rehome write mirrors by range and destination
