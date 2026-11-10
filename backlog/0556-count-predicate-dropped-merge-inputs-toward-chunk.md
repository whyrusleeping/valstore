---
id: "0556"
title: Count predicate-dropped merge inputs toward chunk and cancellation boundaries
status: done
priority: 3
created: "2026-11-02"
updated: "2026-11-10"
depends_on: []
spec_refs: []
---

## Description

0548 source review: vals-store/src/engine/merge.rs:1190-1233 MergeStream::next loops internally past every predicate-dropped winner, pushing dropped identities then continuing at 1229-1231. The async worker's CO_COMPACT_CHUNK budget/cancel check at 560-570 sees only returned survivors; an all-tombstone-GC or all-forget stretch can consume an entire selected run in one next call before yielding/pacing/cancellation at 637. Predicates are installed at 504-512. This is off-core worker monopolization/cancellation latency, not a demonstrated direct serving-shard stall. Suggested acceptance: budgeted step/checkpoint for consumed inputs including dropped entries, deterministic all-drop input spanning multiple chunks with observable early cancel/checkpoint, preserve dropped accounting and output correctness. Benchmark wall-time impact locally before hardware; no measured production latency claim.

## Outcome

MergeStream::next_within charges each dropped winner's consumed inputs against the chunk budget and returns Paused at 0; compact_and_write_inner uses it so tombstone-GC/forget stretches yield, pace and check cancel every 8192 inputs. Tests: budget/pause unit test and an 18000-input all-drop merge that now yields >=4 times (3 without the fix) with correct dropped accounting and commit; both fail with the fix killed. vals-store lib 618 pass, clippy clean. Local release wall-time: all-drop merge 0.119-0.127 s -> 0.116-0.118 s, survivor merges noise-level unchanged.

Commit: engine merge (0556): predicate-dropped merge inputs charge the CO_COMPACT_CHUNK budget (MergeStream::next_within pauses), so all-drop stretches reach the worker's yield/pace/cancel checkpoint per chunk
