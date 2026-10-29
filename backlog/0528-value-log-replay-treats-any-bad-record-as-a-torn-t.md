---
id: "0528"
title: Value-log replay treats any bad record as a torn tail and silently drops the rest of the segment's synced records
status: done
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

## Outcome

replay_segment searches forward for the next CRC-valid same-epoch record at any bad position: none means torn tail (unchanged, silent); otherwise the damaged bytes are skipped, later records kept, a warning logged and the region counted in vals_vlog_replay_skips_total / vals_vlog_replay_skipped_bytes_total. Skip-and-recover (not fail-open) because async out-of-order block writes make crash gaps among unsynced records legitimate; documented in docs/formats.md §2.2. Regression tests/vlog_replay_corruption.rs (bit-flip repro 0/40 → 39/40 + counted) and new vlog unit test fail with the forward search disabled; fuzz property now checks subsequence-of-baseline.

Commit: 0528: value-log replay skips mid-segment damage and recovers later intact records of the segment's epoch; damaged regions are logged and counted (vals_vlog_replay_skips_total, _skipped_bytes_total); torn tails unchanged
