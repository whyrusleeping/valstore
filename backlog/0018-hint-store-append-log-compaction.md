---
id: "0018"
title: 'Hint store: append-log + compaction'
status: done
priority: 4
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Efficiency/healing: `FileHintStore` rewrites the whole file per change — fine for small backlogs, not large ones. Move to an append-log with periodic compaction (keep the fsync-durable rename discipline it already has).

## Acceptance criteria
- Hint mutations are O(append); log compacts when garbage ratio passes a threshold.
- Power-loss durability preserved (fsync data + dir); recovery test from a torn tail.

## Acceptance criteria

## Plan

Convert `FileHintStore` (crates/vals-cluster/src/hints.rs) from whole-file-rewrite-per-change to an append-log with compaction, preserving the fsync-durable discipline.

**File format v2 (append log)**
- Header: `[HINT_MAGIC][u16 version=2]`, then a sequence of self-checksummed records.
- Each record: `[u8 op][u32 payload_len][payload][crc32(op‖len‖payload)]` (LE, reuse `vals_store::crc::crc32`).
- Ops: `SNAPSHOT` (full queues, reusing the existing per-node hint encoding), `RECORD` (node + one hint), `DRAIN` (node), `RESTORE` (node + hints).

**Runtime behavior**
- `record`/`drain`/`restore` mutate the in-memory `MemHintStore` as today, then append one record to an open `File` handle and `sync_all()` — O(append) per mutation. Keep the existing best-effort error discipline (warn + `hint_persist_errors` counter; prior bytes stay intact on failure).
- Skip the append when the mutation was a no-op (empty drain, empty restore) — matches current behavior.
- Cap-eviction on `record` needs no log entry: replay applies ops through a `MemHintStore` with the same cap, which reproduces eviction deterministically.

**Open / recovery**
- On open: read the file. If v1 (legacy whole-snapshot format), decode it with the existing v1 decoder for backward compat. If v2, replay records in order, applying each to the mem store; stop at the first torn/invalid record (short read or crc mismatch) and keep the valid prefix — a torn tail loses at most the last in-flight mutation (hints are best-effort; anti-entropy is the backstop). Unknown magic/version ⇒ empty backlog, as today.
- After replay, immediately rewrite the file as a fresh compacted v2 log (header + one SNAPSHOT record) via the existing durable temp-write → fsync → rename → fsync-dir sequence (`persist_durable`), then open the append handle on the fresh file. This sidesteps torn-tail truncation edge cases, migrates v1 files, and bounds replay cost at the next startup. Also fsync the parent dir when the file is first created.

**Compaction**
- Track appended-op count vs. live hint count. When garbage passes a threshold (e.g. ops ≥ 64 and ops > 4 × live hints — implementer may tune), rewrite as a compacted snapshot log using the same durable temp+rename sequence and swap the append handle. Compaction failure is best-effort (warn + counter, keep appending to the old log).

**Tests** (extend the existing module tests)
1. Reopen/replay round-trip incl. tombstone hints (adapt existing `file_store_survives_reopen`).
2. Torn-tail recovery: truncate mid-record / append garbage bytes, reopen ⇒ valid prefix recovered, store remains usable and durable afterwards.
3. Corrupt record crc ⇒ replay stops at the bad record, prefix kept.
4. Compaction: many record+drain cycles ⇒ file size shrinks back (compaction fired), state stays correct across reopen.
5. O(append): with a large backlog, one more `record` grows the file by a small constant amount (not proportional to backlog size).
6. v1 legacy file still loads (encode with old format, open, backlog recovered).
7. Keep/adapt `persist_leaves_no_temp_file_and_replaces_atomically` for the compaction path.

Verify: `cargo test -p vals-cluster hints`, plus the `coord/tests.rs` FileHintStore integration test (~line 934), then `cargo build` + full `cargo test -p vals-cluster` and clippy for the touched crate. Update the module/struct doc comments (they currently describe the whole-file rewrite) and BACKLOG.md §Efficiency/healing if it lists this item.

### Starting points
- crates/vals-cluster/src/hints.rs — FileHintStore, encode/decode, persist_durable (all to rework)
- crates/vals-client/src/hints.rs — HintStore trait, MemHintStore (unchanged; queues() accessor for snapshots)
- crates/vals-cluster/src/coord/tests.rs:~934 — existing FileHintStore integration test
- crate::observe::counters().hint_persist_errors — existing best-effort error counter
- vals_store::crc::crc32 — checksum helper already in use
- BACKLOG.md §Efficiency/healing — mentions this task; update the entry

## Work log
- 2026-07-04 plan: Convert `FileHintStore` (crates/vals-cluster/src/hints.rs) from whole-file-rewrite-per-change to an append-log with compaction, preserving the fsync-durable discipline.  **File format v2 (append log)*
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-cluster/src/hints.rs — FileHintStore, encode/decode, persist_durable (all to rework); crates/vals-client/src/hints.rs — HintStore trait, MemHintStore (unchanged; queues() accessor for 
…[truncated]
- 2026-07-04 implementer report: Converted `FileHintStore` (crates/vals-cluster/src/hints.rs) from whole-file-rewrite-per-change to an fsync-durable append log with compaction.  ## What changed - **File format v2 append log**: `[HINT
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change converts FileHintStore from whole-file-rewrite-per-change to an fsync-durable v2 append log with periodic snapshot compaction, fully meeting the acceptance criteria. Mutations are O(append)
…[truncated]
- 2026-07-04 decision: accept — commit: feat(hints): FileHintStore append-log + compaction — O(append) mutations via self-checksummed records, garbage-ratio snapshot compaction, fsync-durable (data + dir), torn-tail recovery keeping valid
…[truncated]
- 2026-07-04 usage: 44,683 tok (in 80, out 44,603, cache_r 1,191,759, cache_w 124,724) · cost n/a (unpriced)
  implementer: 33,663 tok (in 40, out 33,623, cache_r 688,916, cache_w 46,859) · cost n/a (unpriced)
  reviewer:Claude: 5,911 tok (in 18, out 5,893, cache_r 151,321, cache_w 34,341) · cost n/a (unpriced)
  coordinator: 5,109 tok (in 22, out 5,087, cache_r 351,522, cache_w 43,524) · cost n/a (unpriced)
