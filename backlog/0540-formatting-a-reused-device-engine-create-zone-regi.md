---
id: "0540"
title: Formatting a reused device (Engine::create, zone region reuse) resurrects the previous store's records
status: todo
priority: 1
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found while building 0532's crash harness (2026-10-29).

Since 0528, value-log replay searches forward past a damaged region for the next CRC-valid record of the segment's epoch and keeps it. A format only wipes segment header blocks (docs/formats.md §2.2 "pre-format resurrection fence") and the new log starts again at epoch 0 (`Engine::create_prewiped`: `ValueLog::new(.., 0, 0)`), so a segment that the previous store wrote at epoch 0 still holds CRC-valid epoch-0 records past the new data's end. Replay walks past the new tail's zero padding into them.

Reproduction (scratch test, removed after the run): `Engine::create` on a 512-byte-block `MemBlockDevice` (seg_blocks 16), put `old-00..old-39` (100-byte values), sync, drop; `Engine::create` again on the same device, put one key, sync, drop; `Engine::open`. 36 of the 40 old keys read back.

The same applies to any path that restarts epochs on a device holding old bodies: `wipe_region` + `create_prewiped` for a reused zone region (vals-cluster zonestore.rs), and recovery of a truncated store whose crash left no visible header (recovery then starts from `RECOVERY_EPOCH_GAP`, see 0532).

The record-epoch binding (formats.md §2.1) assumes a segment header epoch is never reused for a segment that may still hold that epoch's records. 0532 restores that for recovery after a crash: `next_epoch` = highest visible epoch + 2^32, and the first log segment opened after a recovery has its header written and flushed before any record goes into it, so the next recovery sees that epoch. This task covers formats. One recovery case is also left open: value-rewrite epochs that a merge reserves (`ValueLog::reserve_epochs`, merge.rs) before the first log segment after a recovery are not fenced, so a crash before that merge's device flush lets the next incarnation reuse them (only reachable through a replay that walks rewrite segments, or a log segment landing on a rewrite segment's stale body).

Possible directions: start every format (create, region reuse, and recovery with no visible header and no manifest) at a random 64-bit epoch base (keeping epochs monotone after it); or limit 0528's forward search so it cannot cross into a prior incarnation; or both. Check `valsd dump` and segment reclaim, which use the same replay.

Acceptance: a regression test that formats a used device (and a reused zone region) and shows no old record replays; the 0532 crash harness extended to start from a device holding a previous store's data, and it stays green.

## Acceptance criteria

## Work log
