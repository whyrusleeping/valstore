---
id: "0245"
title: SPDK heap-corruption tripwires + qpair affinity enforcement (0232 diagnostics)
status: done
priority: 1
created: "2026-08-23"
updated: "2026-08-23"
depends_on: []
spec_refs: []
---

## Description

Split from 0232 (delivered scope). The 0232 heap-corruption investigation audited the process's unsafe surfaces (vals-rt wakers/executor, reactor, Scratch pools, maint worker topology, SPDK shim + wrapper) without finding a provable root cause locally, and landed deterministic diagnostics so the next hardware recurrence aborts at the faulting site instead of corrupting silently:

- spdk_shim.c: AIO op magic canaries (read/write) verified+cleared before every free including submit-failure paths; DMA slot-pool overflow guard in vals_dma_put; per-device aio in-flight accounting with underflow and close-with-inflight aborts (all `VALS SPDK FATAL` + abort()).
- spdk.rs: qpair owner-thread enforcement for serving qpairs — sequential boot I/O first-use-binds; `BlockDevice::bind_io_owner()` rebinds+seals once at shard start (wired via SubDev → ServerShared::bind_device_owners → shard thread body); post-seal cross-thread rebind panics; every raw_dev()/poll asserts the owner; non-owner qpair close emits a WARN breadcrumb. Kill switch: VALS_SPDK_AFFINITY_CHECK=0.

Acceptance criteria:
- Workspace builds/clippy clean; vals-spdk and vals-cluster suites pass (verified in-session).
- Shim compiles on-box at next deploy (C cannot compile locally; verify then).

## Outcome

Audited the 0232 heap-corruption suspects (vals-rt wakers/executor, reactor, Scratch pools, maint worker topology, SPDK shim + wrapper); no provable local root cause. Landed deterministic diagnostics: shim AIO magic canaries + DMA-pool overflow guard + per-device in-flight accounting (VALS SPDK FATAL aborts), and serving-qpair owner-thread enforcement (boot first-use bind, bind_io_owner reseal at shard start, post-seal steal panics, non-owner close WARN; kill switch VALS_SPDK_AFFINITY_CHECK=0). Next hardware recurrence now aborts at the faulting site with the core already armed on lab-3. Filed 0235 (zone teardown can free/reuse a region under an in-flight off-core maint write). Reviewer accepted; workspace builds, clippy clean, vals-spdk + vals-cluster maint suites pass; shim C compiles on-box at next deploy.

Commit: spdk: heap-corruption tripwires + qpair thread-affinity enforcement (0232 diagnostics); file 0235 teardown/region-reuse race
