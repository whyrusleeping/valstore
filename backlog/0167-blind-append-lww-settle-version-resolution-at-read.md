---
id: "0167"
title: 'Blind-append LWW: settle version resolution at read/merge time to remove the per-put cold run-block read'
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
The remaining insert-vs-overwrite gap after 0145 (941k vs 201-454k ops/s) is fully attributed: every overwrite of a flushed key pays exactly ~1.00 cold run-index block read to resolve the LWW old version before apply (`apply_probe_blocks`/`apply_run_reads` ≈ 1.0 in `bench/lab-bench-20260810-0145/metrics.pass*.txt`); the early-exit newest-first probe is already optimal. Only removing the pre-write resolve closes the gap.

Direction (task 0145 description, "Avoid the pre-write resolve"): a put already carries an HLC version; LWW could be settled at read/merge/flush time (blind append + index insert). Hard parts:
- `Applied::Won/Lost` semantics: the wire reply currently reports the LWW outcome; blind append can only report it lazily or optimistically.
- `VersionIndex::apply_leaf` needs the old version to XOR the AE Merkle leaf — deferring that means leaf digests must be reconciled at flush/merge when the superseded version surfaces.
- gc_grace resurrection checks read the old version too.

Exploratory/design task — needs a spec-level decision on relaxing the Applied reply before implementation.

## Decision (2026-08-10, user)

Promoted: relying on LWW convergence instead of the pre-write resolve is correct
given the perf goals. Implied semantics: plain puts no longer report an
authoritative `Applied::Won/Lost` (optimistic or lazy reply — confirm flavor at
design time); CAS keeps its authoritative read. Design must still cover the two
non-reply consumers of the old version: AE Merkle leaf XOR reconciliation at
flush/merge (`VersionIndex::apply_leaf`) and gc_grace resurrection checks.

## Acceptance criteria

## Plan

Complete this explicitly exploratory/design task with a spec-level blind-append LWW design, not a rushed write-path implementation.

1. Trace the unconditional Apply/BatchApply, local put/delete, CAS, read, scan/digest, anti-entropy VersionIndex, flush, merge/compaction, recovery, and gc_grace paths. Record which paths currently rely on resolving the old on-disk winner and distinguish cheap in-memory conflicts from cold-run conflicts.
2. Add a focused design document under docs/ and link/summarize the contract in ARCHITECTURE.md. Settle the user-authorized reply semantics precisely: unconditional replicated writes may acknowledge optimistically after admission/skew/gc-floor checks and local append; they no longer authoritatively report Won/Lost, while CAS remains authoritative. Preserve wire compatibility where practical and define deletes, retries, batches, read-repair/AE, and failure behavior.
3. Specify storage/read correctness for multiple physical versions: in-memory winner handling, point reads and scans selecting the maximum LWW Version rather than newest physical run, flush/merge/recovery treatment of losers, and tombstone retention. State invariants that prevent stale values or deletes from resurfacing.
4. Fully resolve the two non-reply consumers of the old version. Define a correct anti-entropy Merkle strategy for unresolved cold winners (including the interval before flush/merge and crash recovery, so dirty state cannot produce false equality), and retain a read-free monotone gc_grace resurrection floor for stale live values while tombstones remain admissible.
5. Give an implementation sequence, metrics/feature flag and rollback plan, compatibility implications, and concrete unit/integration/property/performance validation. Explicitly call out any prerequisite or remaining implementation work rather than silently widening this design task into a risky code rewrite.
6. Run documentation/link/style checks available in the repo and inspect the final diff against the task decision and all hard parts in its description.

### Starting points
- crates/vals-cluster/src/replica.rs: VersionIndex::apply_leaf, apply_stage/decide_and_store, reads/scans/CAS
- crates/vals-cluster/src/net/server.rs: serve_apply and serve_batch_apply staged cold-run reads
- crates/vals-store/src/engine.rs: probe_get/probe_version, run lookup, flush and recurring merge selection
- crates/vals-client/src/types.rs: Applied and CasResult; crates/vals-client/src/wire.rs response compatibility
- ARCHITECTURE.md tombstone GC/LWW contract; docs/out-of-core-index.md anti-entropy and gc_grace obligations

## Work log
- 2026-08-10 plan: Complete this explicitly exploratory/design task with a spec-level blind-append LWW design, not a rushed write-path implementation.  1. Trace the unconditional Apply/BatchApply, local put/delete, CAS,
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: crates/vals-cluster/src/replica.rs: VersionIndex::apply_leaf, apply_stage/decide_and_store, reads/scans/CAS; crates/vals-cluster/src/net/server.rs: serve_apply and serve_batch_apply staged cold-run re
…[truncated]
- 2026-08-10 preload: 6 file(s), ~30 KiB seeded into implementer context
- 2026-08-10 implementer report: Completed task 0167 as an explicit design deliverable, without changing the write path.  Changed: - Added `docs/blind-append-lww.md`, defining the accepted optimistic `Applied::Won`/accepted semantics
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): accept — The change satisfies task 0167 as a design deliverable without prematurely changing the write path. The new design document accurately inventories the affected current paths, settles optimistic wire-c
…[truncated]
- 2026-08-10 review (claude): accept — A thorough, technically accurate spec-level design for blind-append LWW that fulfills every step of the task plan without widening into a code rewrite. The path inventory, reply-semantics contract, st
…[truncated]
- 2026-08-10 decision: accept — commit: 0167: specify blind-append LWW semantics and safety
- 2026-08-10 usage: 3,443,362 tok (in 1,272,487, out 37,883, cache_r 2,840,779, cache_w 43,485) · cost n/a (unpriced)
  implementer: 2,060,179 tok (in 729,394, out 17,249, cache_r 1,313,536, cache_w 0) · cost n/a (unpriced)
  coordinator: 851,977 tok (in 278,569, out 6,112, cache_r 567,296, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 522,445 tok (in 264,480, out 5,805, cache_r 252,160, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 8,761 tok (in 44, out 8,717, cache_r 707,787, cache_w 43,485) · cost n/a (unpriced)
