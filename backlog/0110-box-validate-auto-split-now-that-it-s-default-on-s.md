---
id: "0110"
title: Box-validate auto-split now that it's default-on (soak a skewed-write sorted zone, confirm no split storms)
status: done
priority: 3
created: "2026-07-31"
updated: "2026-08-07"
depends_on:
    - "0108"
spec_refs: []
---

## Description
Task 0108 flipped auto-split **on by default** (`DEFAULT_RANGE_SPLIT_THRESHOLD = 1_000_000` entries/range) — deliberately reversing the repo's prior default-off "opt-in until box-validated" posture per user instruction. That validation is now owed.

## Acceptance
- On the lab fleet, run a sorted-zone soak with sustained (and skewed) write load and confirm: (a) a fresh single-range sorted zone auto-splits and spreads across nodes/partitions without operator intervention; (b) no split storms / oscillation (the one-split-per-zone-per-poll + midpoint validity guards hold); (c) the `pd_*` guardrail metrics (`pd_sorted_ranges`, `pd_max_range_entries`, `pd_range_skew_pct`, `pd_oversized_single_range_zones`) read sensibly; (d) the RangeStats poll's per-poll cost on the PD pusher thread is negligible at fleet scale.
- Reassess whether 1M entries is the right default threshold given observed value sizes / region fill, and whether the 5s poll cadence is appropriate.
- If any pathology appears, either tune the defaults or reconsider the default-on posture.

Notes: the poll now runs on every PD by default (auto-split threshold or warn threshold nonzero). See memory.md (2026-07-31) and `docs/zones.md`.

## Acceptance criteria

## Work log

- 2026-08-07 CLOSED (superseded): task 0139 makes splits metadata-only, so auto-split no longer moves data and the "confirm it spreads across nodes/partitions without intervention" criterion is void by design. Correctness validation of auto-split under load is task 0143; the load-spreading validation (and the threshold/cadence reassessment this task also asked for) moves to task 0142's acceptance.
