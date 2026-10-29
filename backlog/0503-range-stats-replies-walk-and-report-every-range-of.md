---
id: "0503"
title: Range-stats replies walk and report every range of the zone on every node each poll
status: done
priority: 4
created: "2026-10-23"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description
After 0469 the range-stats V3 reply is the only background shard row that still grows clearly with the total range count (lab-1 lbr, FIX: 96 cycles/write at 17,000 ranges, 204 at 50,000; mostly engine midpoints for held ranges, plus one reply entry per range of the zone every 5 s poll, including ranges the shard does not hold). See bench/0469-range-map-share/FINDINGS.md §3–§4. In a large cluster (nodes ≫ RF) most entries are for ranges the node does not hold, so the reply size and walk follow total cluster ranges, not the shard's share.

Candidates: report only held ranges or ranges with non-zero entries/ops (needs the PD's `measured`/owner aggregation to treat a missing report from a non-owner as zero); cache midpoints per range and recompute only for ranges written since the last poll.

Acceptance: semantics of the PD's split/merge/balance inputs preserved (tests), and an lab-1 A/B with bench/0435-range-residual/ab.py at 17,000 and 50,000 ranges, or recorded as not worth fixing.

## Acceptance criteria

## Work log

- 2026-10-29: closed, not pursued (owner, backlog triage). The revisit trigger (17,000+ ranges per node in practice) has not occurred. The analysis stays in bench/0469-range-map-share/FINDINGS.md §3–§4; re-file from this description if range counts reach that scale.

- 2026-10-26: triage: kept proposed (parked). The fix changes the PD's split/merge/balance aggregation for a small per-write gain; revisit once range counts per node approach 17,000+ in practice or 0502 shows such counts are a real target.
