---
id: "0256"
title: 'Hardware gate: format=false SPDK recovery with qpair affinity enforcement enabled (0249)'
status: done
priority: 2
created: "2026-09-05"
updated: "2026-09-06"
depends_on:
    - "0249"
spec_refs: []
---

## Description

Split remaining hardware acceptance from 0249's reviewed recovery-binding fix. The active 0248 allocator/corruption diagnostic fleet must not be restarted or rebuilt merely to run this gate; schedule it on an available gate cluster or alongside an already-approved diagnostic restart.

Run plans/bootes-soak-runbook.md §4.1 on a freshly rebuilt candidate. Preserve all drives/PD state; never use format=true on the retained corpus. Record binary hash, configuration/environment, catalog size, shard readiness, logs and retained-key evidence.

Acceptance:
- A four-shard node restarts with format=false and VALS_SPDK_AFFINITY_CHECK=1 over a non-trivial persisted multi-zone catalog; all persisted zones reopen and checked data remain readable through the restarted node.
- No affinity/ownership-steal panic or SPDK fatal occurs.
- Run the SPDK-linked sealed_owner_binding_rejects_cross_thread_use_and_rebind test successfully, proving strict cross-thread use/rebind rejection.
- Preserve current diagnostic allocator arms and rate evidence if coordinated with 0248; distinguish restart confounds from hardware/allocator conclusions.

0249 local evidence: persisted multi-zone recovery regression and full debug cluster suite pass; workspace SPDK-feature all-target check passes. Live hardware restart and SPDK-linked runtime test were not performed.

## Outcome

0256 passed on rebuilt 7d799c43 candidate: clean four-shard lab-1 format=false restart with affinity=1 reopened both persisted named zones; 300 keys/zone verified directly on restarted node and via cluster, parity checks passed, no affinity/SPDK panic, SPDK-linked strict owner-binding test passed. Independent standard review accepted receipts in bench/0256-hardware-20260906. Fleet stopped and evidence/corpus preserved; final node1/node2 teardown timeout kills require crash recovery next use. Proposed 0259 captures shutdown diagnosis. Preserved pre-existing staged fleet-release/backlog/metrics changes in final tree.

Commit: bench: pass retained-catalog SPDK affinity recovery gate
