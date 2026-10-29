---
id: "0528"
title: Value-log replay treats any bad record as a torn tail and silently drops the rest of the segment's synced records
status: todo
priority: 2
created: "2026-10-29"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
Found in the 2026-10-29 project review. `replay_segment` (crates/vals-store/src/vlog.rs ~1330–1356) stops at the first record whose magic/CRC fails (`Err(_) => break`) without checking whether intact records of the same epoch follow. An append-only log only tears at its tail, so a bad record followed by valid same-epoch records is mid-log corruption of acknowledged data. Review repro (scratch test, not committed): write 40 keys, `sync()`, flip one bit in the first record's key, reopen → `Engine::open` succeeds, 0/40 synced keys present, nothing logged or counted.

Acceptance:
- Replay distinguishes a torn tail from mid-segment corruption (e.g. scan forward for a later intact record of the segment's epoch).
- Mid-segment corruption is never silent: it is counted (metric) and logged, and either the bad record is skipped while later intact records are recovered, or open fails with a corruption error — pick one and document it in docs/formats.md / the recovery docs.
- A genuine torn tail still recovers exactly as today.
- Regression test (the bit-flip repro above) that fails with the fix reverted.

## Acceptance criteria

## Work log
