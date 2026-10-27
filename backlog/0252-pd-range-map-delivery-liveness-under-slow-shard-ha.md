---
id: "0252"
title: 'PD range-map delivery liveness under slow shard handshakes: expose preflight stalls and gate rehome deadlines on delivery'
status: done
priority: 1
created: "2026-09-05"
updated: "2026-09-05"
depends_on: []
spec_refs: []
---

## Description

Found during 0248/0233 takeover, 2026-09-05. At c4c28276, node1 malloc-check overload makes PD range-map delivery intermittent (6–13 minute gaps) while rehomes abort after60s. All-node V4 preflight in pd/push.rs:332–351 returns silently if any shard's1s handshake fails; preflight probes all endpoints on every attempt (369–392). Beacon timeout also removes cached features. Zero push-failure delta does NOT imply a push was attempted. PD starts CopyStall/ReportGap timers before owners receive the pending map; 0 completed rehomes in first5h, all ReportGap aborts in inspected interval name node1.

Investigate/fix without weakening wire compatibility or rehome fences. First add preflight-block/attempt and last-confirmed-age telemetry, per-endpoint failure attribution. Then design deadline behavior for undelivered pending maps and capability handling with explicit restart/downgrade semantics. Merely preserving feature-cache entries does NOT fix the unconditional probe/ready=false path. Do not treat permanently cached features as safe across arbitrary binary restarts. Do not relax RangeWriteMark epoch fences based on this evidence.

Acceptance: deterministic slow-one-endpoint tests demonstrate observable blocked delivery; healthy endpoints and recovery are not starved indefinitely; deadlines distinguish delivery stalls from actual copy stalls; old/V3 endpoints and restarts still fail closed. Hardware A/B on the same workload after allocator-control and crashed-node confounds are cleared. Evidence: bench/0248-takeover-20260905 and task0233.

## Outcome

Completed software scope: endpoint-local V4 preflight/push retries with endpoint-attributed attempts, blocks, failures and confirmation ages; report/copy deadlines gated on actual required-owner map delivery using authoritative shard topology. Sending-connection V4 checks and mirror/commit/epoch fences preserved. Added deterministic slow/recovering endpoint, legacy/downgrade, multi-shard no-report and deadline tests; documented restart semantics and indefinite delivery-wait recovery. Both comprehensive reviewers accepted after correcting multi-shard owner selection. Workspace check, full debug cluster suite before narrow revision, 64 PD tests after revision, metrics/client observation checks, and coordinator's 15 rehome tests passed. Hardware A/B explicitly split into open dependent task0255; no deployment or hardware-pass claim. Existing task0253 acceptance and live metrics capture changes preserved.

Commit: fix(pd): isolate range-map delivery retries and gate rehome deadlines
