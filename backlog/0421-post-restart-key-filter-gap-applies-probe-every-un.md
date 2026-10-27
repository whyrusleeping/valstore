---
id: "0421"
title: 'Post-restart key-filter gap: applies probe every uncovered run''s bloom for ~45 min after a restart (x2.5-2.7 shard us/write at +5 min)'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-13"
depends_on:
    - "0416"
spec_refs: []
---

## Description

Split from 0416 (bench/0416-late-shard-cpu/FINDINGS.md, sections 3 and "Restart control"). After a clean restart at the 8B-equivalent state of the L1 model, the key filter (in memory only) covers none of the 33 runs (`vals_key_filter_uncovered_runs`), so each apply probes ~19.5 run blooms (`vals_lookup_bloom_probes{caller="apply"}`) instead of ~0.19 and reads a run block for ~0.36 bloom positives per write. Against a no-restart arm at matched writes this is +13.4k cycles/write at R+5 min (49% of a ×2.68 excess) and +7.0k at R+20 (35% of ×2.13); uncovered runs reach 0 at ~45 min. R3's crash restarts show the same 1.5–2.0× shape (DECAY-R3-0391.md).

Candidates to measure: persist/reload the key filter with the manifest, or rebuild it before (or with priority over) serving applies after a restart, or a restart-time rebuild permit boost (0400 permits). Coordinate with proposed 0414 item 1 (KeyFilter::excludes cost).

Acceptance: preregistered L1 A/B with restart arms (bench/0416-late-shard-cpu/driver.py --restart-at), showing the R+5/R+20 run-probe excess reduced, or a recorded reason it is not worth it; RAM cost of any persisted/rebuilt structure accounted.

## Outcome

Kept by preregistered L1 restart A/B (bench/0421-cover-restart/FINDINGS.md). After a restart, cover scans of active-uncovered runs are unpaced, go in a new CLASS_COVER ahead of ordinary merges (pressure merges still first; shadow-rebuild scans keep pacing and the 0366 single-lane duty share), and maint_loop relays finished chunks between flush-commit steps. Coverage at R+9.6 vs R+39.8 min (FIX1 without relay: R+26), apply run probes/write 18.14→8.72 (R+5) and 7.52→0.105 (R+20), shard µs/write R+0→R+60 4.816→4.245 (−11.9%; pre-restart spread 0.072). No errors or rejections, merges caught up by R+12, no new RAM (filter 1.26→1.81 GB in all arms). Remaining excess: the 0422 seed; coverage still takes ~2.3× its scan work (1 s planning tick).

Commit: perf(0421): post-restart cover scans unpaced, ahead of ordinary merges, resubmitted between flush-commit steps; L1 restart A/B: all runs covered at R+9.6 vs R+39.8 min, first-hour shard µs/write −11.9%
