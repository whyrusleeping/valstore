---
id: "0263"
title: Bound server transport connections, requests, queued bytes, and idle lifetimes
status: done
priority: 2
created: "2026-09-07"
updated: "2026-09-08"
depends_on: []
spec_refs:
    - docs/production-readiness-review-2026-08.md#R5 — The network trust model is not suitable for an external production claim (P0 security)
---

## Description

Accepted bounded slice split from 0241 during owner-approved triage. Add validated process-wide connection, in-flight request, and queued-byte budgets plus finite idle/initial-protocol deadlines. Inventory data/control/PD listeners and explicitly document coverage; per-connection limits alone are insufficient. Authentication/encryption/authz remain separate in 0241.

Acceptance:
- Specify finite defaults and explicit benchmark/development opt-outs; enforce bounded accounting and release credits on completion, failure, and cancellation.
- Slow-client, oversized/partial-frame, connection-flood, and stalled-reader tests demonstrate bounded resource use and continued progress for healthy clients.
- Preserve protocol compatibility and validate hot-path CPU/throughput/tail impact before adoption under the project's hardware gate policy.
- Document effective budgets and rejection/timeout metrics. Do not describe these bounds as transport authentication.

## Outcome

Implemented validated process-wide connection/request/retained-byte budgets, finite initial/frame/idle/write deadlines, explicit unbounded development opt-out, rejection/timeout metrics, listener coverage docs and adversarial regression tests. Fixed failed-credit underflow, pre-allocation/capacity accounting, admin queue credit lifetime and absolute deadlines, and preserved large PD snapshot framing. Both comprehensive reviewers accepted. Workspace build and finite-default sequential workspace tests passed; unbounded parallel workspace tests passed; coordinator reran targeted transport/admin/input-buffer tests and diff checks successfully. Accepted hardware adoption scope split into task 0277 (depends on 0263 and 0271); production adoption remains pending matched SPDK CPU/throughput/tail/value verification. No hardware operations performed.

Commit: Bound server transport resources across process-wide listeners
