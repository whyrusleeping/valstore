---
id: "0539"
title: Describe 0523's off-thread file sync and gate-permit release in ARCHITECTURE.md, batch-writes.md and backpressure.md
status: todo
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on:
    - "0538"
spec_refs: []
---

## Description
Follow-up from 0523 (commit 3d667b1). 0523 updated docs/benchmarks.md, configuration.md and embedding.md, but ARCHITECTURE.md, docs/batch-writes.md and docs/backpressure.md held 0538's uncommitted docs-pass hunks, so 0523 left them alone.

To do once 0538 lands:
- docs/batch-writes.md, after "Asynchronous group commit and the node-wide flush domain (0303)": on the file backend `FileBlockDevice::submit_flush` runs `fdatasync` on a `vals-fsync` helper thread shared by the file's handles. Flushes queued while one runs share the next. The leader awaits a waker, and an eventfd wakes the shard out of epoll, so the shard serves other work while it waits. The ack ordering is unchanged.
- docs/backpressure.md, "Staged BatchApply gate": a device-durable request on a backend whose flushes complete off-thread (`BlockDevice::flush_completes_off_thread`, the file backend) gives its permit back once its writes are applied, before it waits for the group commit. SPDK keeps the permit until the ack.
- ARCHITECTURE.md performance summary (~line 1206): it still says "~1.4–5k device-durable writes/s ... limited by fsync". Point it at the post-0523 single-host numbers in docs/benchmarks.md (11k write-device at 256 in flight).

Acceptance: the three docs match the code; no other content changes.

## Acceptance criteria

## Work log
