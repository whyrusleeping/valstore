---
id: "0206"
title: Allocation-free mixed value replies from DMA scratch to ServerOut
status: done
priority: 1
created: "2026-08-14"
updated: "2026-08-15"
depends_on:
    - "0205"
spec_refs:
    - ARCHITECTURE.md#Status
    - docs/formats.md#1. Wire protocol (`vals-cluster`)
    - bench/lab-read-breakthrough-20260814-0204/FINDINGS.md
---

## Description
Remove the per-value heap allocation and extra copy in the batched strong-read server path, subject to task 0205 confirming response construction/allocator/copy cost remains material.

Acceptance criteria:
- Introduce a server-internal value result that can retain checked value bytes until final mixed-response encoding without changing the public `Stored` API or existing wire format.
- Encode each successful value directly into the framed `ServerOut` buffer with no temporary value `Vec`; provide an inline-value spike first if it is the fastest clean mechanism test.
- Ensure DMA scratch is released through RAII on success, corruption, Busy/retry, connection loss, future cancellation, oversized response, and paging/resumption paths.
- Preserve full key/version validation, winner-token revalidation, tombstones, per-key errors, result order, reply bounds, compatibility fallback, and read repair behavior.
- Add kill-the-fix allocation/copy tests or instrumentation and cancellation/error-path tests.
- Demonstrate a repeatable hardware CPU/key and throughput or latency gain at the task-0205 knee, with zero correctness errors and no large-value, RSS, write, or maintenance regression; remove the experiment if neutral.

## Acceptance criteria

## Plan

1. Preserve and inventory the dirty workspace and the stopped lab fleet before touching code. Treat the existing modifications to backlog/0201, backlog/0203, backlog/0205, and memory.md as unrelated. Record exact Git/binary/config/corpus provenance for any hardware work, rebuild before runs, never deploy with `rsync --delete`, preserve remote `third_party`/`target`, and verify `format = false` before every restart.
2. Trace the mixed-batch value path from `ReadPlan` scratch acquisition through winner/full-key/version validation and `serve_mixed_batch_read` response framing. Introduce the smallest cancellation-safe RAII scratch lease needed to let a checked decoded value borrow the completed read buffer until encoding; refactor the relevant read APIs so scratch is returned exactly once on normal completion, I/O/corruption errors, dropped futures, and dropped retained results. Keep `Stored` and all public client-facing response types unchanged.
3. Add a server-internal mixed-read result representation that can own the retained checked scratch lease (plus decoded version/value range or tombstone state) alongside digest, Busy, and bounded error results. Preserve storage-generation/mutable-entry or winner-token revalidation after I/O and before accepting a result. Keep staging lookahead and reply-page admission bounded so unselected lookahead never acquires DMA buffers and only the current page can retain scratch.
4. Change the mixed-batch dispatch to encode the completed page straight into the existing framed `ServerOut` buffer, writing successful value bytes from retained scratch without creating a temporary value `Vec` or rebuilding a public `Response::MixedBatchRead` value tree. Use a dedicated borrowed/internal encoder or an inline-value spike if that is the cleanest first proof. Preserve byte-for-byte wire format, result order, start/more paging semantics, MAX_FRAME/SCAN_REPLY_BUDGET behavior, tombstones, per-key errors, compatibility fallback, connection draining, and read-repair-visible client behavior; release every retained lease immediately after framing (including oversized-frame rollback).
5. Add focused kill-the-fix coverage: scratch take/release accounting and future-drop tests for success, I/O error, corrupt key/version/record, Busy/revalidation, cancellation, and result drop; mixed server tests for exact wire bytes, value/tombstone/digest/error ordering, paging/resumption, oversized and single-large-value handling, connection/output loss, and unchanged client fallback/read-repair behavior. Add test-only allocation/copy instrumentation that fails if the server materializes successful value bytes into a temporary heap `Vec`, while allowing the final `ServerOut` growth/copy.
6. Run targeted vals-spdk/vals-store/vals-cluster tests and the relevant debug workspace suites without running repository-wide rustfmt (hand-format touched regions). Inspect the final diff for public API/wire drift, unsafe lifetime assumptions, leaked scratch, and unrelated changes.
7. Build a retained task-0206 benchmark bundle and run a bounded matched lab A/B at task 0205's mature RF=2/R=2, width-256, 12-coordinator × 2-in-flight knee: at least three 30-second generations per arm after warmup, exact provenance, correctness/value sampling, task-clock and CPU/key, throughput and p50/p99/max, allocator/memmove profiles, device reads/blocks, NIC bytes/key, scratch counters, RSS, run/corpus/directory state, and short large-value/write/maintenance regression checks. Advance only for a repeatable material CPU/copy/allocation reduction and throughput or latency gain (using 0205's >5.3M/s and >=10% task-clock/key targets as the strong gate); if neutral, remove the production experiment while retaining truthful evidence. Restore every node to its original inactive/supported state and document exact commands, raw captures, findings, and operational guidance.

## Work log
- 2026-08-14 plan: 1. Preserve and inventory the dirty workspace and the stopped lab fleet before touching code. Treat the existing modifications to backlog/0201, backlog/0203, backlog/0205, and memory.md as unrelate
…[truncated]
- 2026-08-14 context hints: Primary mixed path: crates/vals-cluster/src/net/server.rs:1619 ServerOut, dispatch at ~2011, MixedValueStage/execute_mixed_value/serve_mixed_batch_read at ~3261.; Current scratch lifecycle is manual i
…[truncated]
- 2026-08-14 preload: 8 file(s), ~38 KiB seeded into implementer context
- 2026-08-14 implementer report: Implemented task 0206’s production path while preserving the pre-existing staged backlog/memory changes.  Changes: - Added `RetainedReadValue` plus an internal RAII scratch guard in `vals-store`. Ch
…[truncated]
- 2026-08-14 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-14 review (sol): revise — The direct borrowed encoder and retained-result shape are directionally correct and preserve the apparent wire representation, but the change is not safe on the production SPDK cancellation path and t
…[truncated]
- 2026-08-14 review (sol#2/sol): revise — The production code is generally clean and the core approach looks correct: retained scratch is RAII-managed, winner revalidation remains after I/O, and successful values are encoded into ServerOut th
…[truncated]
- 2026-08-15 revision: Addressed the review findings and completed the hardware gate.  Code: - Fixed accepted SPDK `ReadAt` and `WriteAt` lifetime safety in `crates/vals-spdk/src/spdk.rs`. Accepted operations now own a `Sub
…[truncated]
- 2026-08-15 review (sol): accept — The revision addresses the prior blockers. The neutral retained-DMA experiment has been removed from production, while a complete matched three-generation A/B bundle documents the rejection, correctne
…[truncated]
- 2026-08-15 review (sol#2/sol): accept — The revision resolves the prior findings. The rejected mixed-read experiment is no longer present in production source, while the retained benchmark bundle contains reproducible three-generation A/B e
…[truncated]
- 2026-08-15 decision: accept — commit: bench: reject retained-DMA mixed replies
- 2026-08-15 usage: 25,541,928 tok (in 3,283,017, out 89,823, cache_r 22,169,088, cache_w 0) · cost n/a (unpriced)
  implementer: 17,880,815 tok (in 1,205,327, out 59,808, cache_r 16,615,680, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 3,149,738 tok (in 686,087, out 11,683, cache_r 2,451,968, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,275,454 tok (in 752,713, out 7,989, cache_r 1,514,752, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,235,921 tok (in 638,890, out 10,343, cache_r 1,586,688, cache_w 0) · cost n/a (unpriced)
