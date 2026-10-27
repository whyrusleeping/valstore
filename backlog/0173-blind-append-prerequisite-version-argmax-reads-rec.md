---
id: "0173"
title: 'Blind-append prerequisite: version-argmax reads/recovery and tombstone-GC safety'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-11"
depends_on:
    - "0167"
spec_refs:
    - docs/blind-append-lww.md#Storage and read invariants
    - docs/blind-append-lww.md#Read-free `gc_grace` resurrection floor
    - docs/blind-append-lww.md#Implementation and rollout sequence
---

## Description
Implement phases 1–2 of `docs/blind-append-lww.md` while retaining the legacy resolving writer.

Acceptance criteria:
- Mutable insertion/replay retains the greatest `Version`; point values/digests, scans/pages, and recovery choose the same argmax across mutable state and all live runs, with generation-safe retries around async reads.
- Partial merges cannot GC a winning tombstone while an uncovered older candidate can resurface.
- With `gc_grace > 0`, every stale unconditional live candidate is rejected by a read-free monotone cutoff; cutoff advancement is crash-durable before the corresponding tombstone-GC commit.
- Permutation/property/crash tests cover old/new puts and tombstones across tail, flush, runs, partial merge, restart, and GC. Existing store/cluster suites pass.

## Acceptance criteria

## Plan

Implement phases 1–2 of docs/blind-append-lww.md without enabling blind writes:

1. Establish one argmax-by-Version rule in vals-store. Make mutable insertion/replay retain the highest Version (while still advancing physical seq/epoch watermarks), resolve synchronous point reads/digests across both mutable and every candidate run, and make async run probes inspect all candidate blocks rather than stopping at the first match. Preserve equal-version retry semantics.
2. Introduce an in-memory source-set generation that advances for mutable-index and run-manifest changes. Carry mutable candidates plus all run locators in staged point get/version probes, and update network/AE/local staged consumers to revalidate after each await and retry rather than mixing snapshots. Apply the same source generation to staged scans/pages so mutable churn is guarded as well as manifest churn.
3. Keep scans and digest pages max-merging duplicate keys, fix any paging/refill edge cases exposed by loser copies, and ensure recovery seeds/replays tail collisions by Version while max-merging tail and runs. Flush must remove only the exact captured index entry, not a same-key replacement.
4. Harden tombstone GC. Only allow collection with complete run overlap coverage and when no uncovered mutable/tail candidate can resurface; partial/suffix merges defer GC. Retain correct current-winner retirement reporting.
5. Add a monotone live-value admission cutoff for gc_grace and apply it read-free to every unconditional live candidate before legacy winner resolution; tombstones remain exempt. Reuse the durable liveness watermark metadata: load its derived floor into stores, synchronously persist any advance before planning/committing tombstone GC, fail closed on persistence failure, and propagate the floor to lazily opened/replaced zones. Keep standalone/direct test paths conservative (no collection at an unpersisted higher cutoff).
6. Add focused permutation, paging, generation-race, replay/reopen, partial-merge/tombstone, admission-floor, and persistence-failure/crash-boundary tests in vals-store/vals-cluster. Run targeted suites first, then the existing vals-store and vals-cluster suites in debug mode. Do not run cargo fmt; hand-format touched regions only.

### Starting points
- docs/blind-append-lww.md#Storage and read invariants
- crates/vals-store/src/engine.rs: replay_winners, put_kind, resolve, probe_get/probe_version, resolve_run_probe_counted, flush_commit_step, compact_plan_inner/compact_commit
- crates/vals-cluster/src/replica.rs: gc_collect_before, apply_stage/apply_commit_staged, staged read wrappers
- crates/vals-cluster/src/net/server.rs: build_get_plan, serve_digest, liveness watermark loading/persistence and lazy zone open
- crates/vals-cluster/src/node.rs: maintenance compaction planning loop
- crates/vals-cluster/src/liveness.rs: durable watermark envelope

## Work log
- 2026-08-10 plan: Implement phases 1–2 of docs/blind-append-lww.md without enabling blind writes:  1. Establish one argmax-by-Version rule in vals-store. Make mutable insertion/replay retain the highest Version (whil
…[truncated]
- 2026-08-10 context hints: 6 recorded with plan
- 2026-08-10 context hints: The task is large but intentionally covers phases 1–2 as one accepted backlog item; keep the writer resolving and do not implement dirty Merkle or blind Apply.; crates/vals-store/src/engine.rs symbo
…[truncated]
- 2026-08-10 preload: 8 file(s), ~64 KiB seeded into implementer context
- 2026-08-10 implementer report: Implemented Task 0173 phases 1–2 while retaining the resolving writer.  Changes: - Made mutable insertion and value-log replay retain the maximum Version (with deterministic seq handling for equal-v
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): revise — The argmax read/replay work and source-generation plumbing are broadly aligned with the design, but two GC safety holes remain: failed liveness persistence can still be treated as durable, and an asyn
…[truncated]
- 2026-08-10 review (claude): revise — The change faithfully implements phases 1–2 of docs/blind-append-lww.md: argmax-by-Version is enforced across mutable insertion, replay, point reads/digests, scans, and merges; flush removes only ex
…[truncated]
- 2026-08-11 revision: Addressed all reviewer findings while preserving the Version-argmax implementation.  Revision changes: - Split ordinary in-memory liveness watermark state from `durable_gc_watermark`. Only successfull
…[truncated]
- 2026-08-11 review (sol): revise — The revision fixes both previously reported GC defects: only successfully persisted watermarks are propagated as durable, and compaction commit now rejects newly uncovered lower candidates. However, t
…[truncated]
- 2026-08-11 review (claude): accept — The revision fully addresses both reviewers' findings while preserving the phase 1–2 argmax implementation. The over-broad invalidation is fixed by splitting storage_gen (run set + value relocation)
…[truncated]
- 2026-08-11 review (sol): accept — The revision now addresses the remaining generation-safety gap: aggregate scan, digest-page, SyncValues, and cleanup paths validate both storage and mutable generations after async work, while point r
…[truncated]
- 2026-08-11 review (claude): accept — The third revision cleanly addresses the remaining second-round findings while preserving everything previously fixed. AE/scan surfaces (bucket digest pages, span/entry execution, staged scans, sync-v
…[truncated]
- 2026-08-11 decision: accept — commit: 0173: enforce version-argmax reads and safe tombstone GC
- 2026-08-11 usage: 106,196,525 tok (in 7,512,521, out 172,900, cache_r 108,990,017, cache_w 533,848) · cost n/a (unpriced)
  implementer: 95,101,519 tok (in 4,439,355, out 80,660, cache_r 90,581,504, cache_w 0) · cost n/a (unpriced)
  coordinator: 5,677,083 tok (in 1,682,117, out 12,118, cache_r 3,982,848, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 5,353,220 tok (in 1,390,859, out 15,609, cache_r 3,946,752, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 64,703 tok (in 190, out 64,513, cache_r 10,478,913, cache_w 533,848) · cost n/a (unpriced)
