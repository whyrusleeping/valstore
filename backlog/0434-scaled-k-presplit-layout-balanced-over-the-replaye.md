---
id: "0434"
title: 'SCALED-k: presplit layout balanced over the replayed prefix, so worst-shard fill transfers (0405 calibration follow-up)'
status: done
priority: 4
created: "2026-10-12"
updated: "2026-10-12"
depends_on:
    - "0405"
spec_refs: []
---

## Description
0405's SCALED-k8 calibration was **not a valid proxy** (bench/0405-scaled-profile/CALIBRATION-K8.md).

**What transferred:** mean-shard fill, run counts, RSS per key and shadow-fold size.

**What failed:**
- **Records worst-shard fill.** It ran 52–88 % against R3's 39–63 % at f = 0.5–0.9.
- **Early disk stop.** As a result, the hot shard (lab-3/7/1) hit the 0402 disk stop at f = 0.974.

**Cause.** Per-shard skew follows the absolute replay position. Both arms show records max/mean of ≈1.5 at ~1.1–1.2 B ACK. R3's presplit layout is byte-balanced over the whole 5B cache, and R3 grows out of the skew later (1.05 at its end). A 1/k prefix replay ends inside it.

**Idea (needs owner approval for another L2b arm):**
- Derive a presplit layout byte-balanced over the replayed 1/k prefix (`byte-presplit-count-search` on the prefix) and use it for SCALED-k.
- Re-run k=8 once against the same preregistered bands.

**Trade-off:** the layout would no longer be R3's, and R3's mean trajectory already transfers without it. Alternative: no new arm. Judge SCALED-k on mean-shard fill only and keep worst-shard and wall-time questions on L2a/L3.

Also unexplained: forced-merge count was 254 vs 1,489, possibly because time-based flush/compaction cadence does not scale with k.

## Acceptance criteria

## Work log
- 2026-10-12: declined; took the task's own alternative. No new arm: SCALED-k is judged on mean-shard fill only, and worst-shard fill and wall-time questions stay on L2a/L3.
