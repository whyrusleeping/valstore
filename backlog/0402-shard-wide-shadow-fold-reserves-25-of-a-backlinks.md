---
id: "0402"
title: Shard-wide shadow fold reserves ~25% of a backlinks region transiently and trips the 10% disk planned stop near EOF (SOAK5B-V5-R3, 0383-related)
status: done
priority: 2
created: "2026-10-08"
updated: "2026-10-09"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (SOAK5B-V5-R3, 0386)
At 18:24Z, at 10.17B of ~10.45B ACK, lab-3 zone 2 shard 0 went from 67.6% to 92.1% used within 30 s. The cause was a **shadow** merge of 49 runs: 56,062,351,610 B in, 52,332,331,008 B out, 538.7 s. Its whole output is reserved up front.
- The harness's disk planned stop (data-zone shard <10% free) fired, ending the run 2.7% short of EOF.
- The merge completed at 18:33:10Z and freed 53,493 segments, leaving the shard at 64.9% used with 4 runs. The physical zone never filled.
- The V5 disk projection (SOAK5B-V5-README.md, 83.7% worst EOF fill) reserved only 2 GiB × 2 lanes for merges, because no exempt-kind merge appeared in the shorter V4/V4-R2/V4-R3 logs.
- Raw: `/data/dataset/task0363-results/SOAK5B-V5-R3/watch.jsonl` and live-logs/lab-3-node3.stderr.

## Acceptance
- From the R3 logs (all nodes), list every shadow/forced/pressure merge: size, transient bytes as a fraction of the region, and duration. Project the worst-case simultaneous transient at EOF per shard, including the existing pin reservation.
- Choose and justify one of the following, then preregister the choice for 0386's next attempt:
  1. a disk-stop amendment that distinguishes transient merge reservations from persistent use, with an exhaustion-safety argument (e.g. sustained-window or in-flight-output-aware stop, plus a hard low-free guard; cite the existing zone-exhaustion backpressure test);
  2. or a code change that bounds or streams the shadow fold's transient space. This relates to 0383, so coordinate with it.
- Show that the chosen rule would not have stopped R3 at 18:24, and that it still stops before real exhaustion.

## Acceptance criteria

## Outcome

Option 1 (harness-only disk-stop amendment; no engine/pin change). Inventory of R3: 2 shadow merges (lab-3 o0 z2 52.3 GB out = 23.5% region, 538.7 s; lab-2 o6 z2 49.8 GB = 22.3%, 547.3 s), 1,494 forced (≤0.79% region, ≤88.8 s), 0 pressure, 6,540 recurring; full inventory exported outside repo, compact SUMMARY-0402.json committed. EOF projection without double-counting pins: admitted per-engine transient bounded by free minus commit headroom (not the maintenance reserve), peak ≈100% possible. Preregistered rule: <10% free sustained 1200 s, or <3% free, or rising vals_busy_rejections−vals_admit_shed_rejections → planned stop. Replay: old rule stops R3 at 18:24:29Z, new rule never stops R3; synthetic creep/fast-fill/unrecovered transients stop. 11 unittest cases pass; cited Rust backpressure tests pass. Runner wiring is 0409.

Commit: bench(0402): R3 merge inventory + EOF transient projection; preregister sustained-window disk stop (10% free for 1200 s, hard 3%, floor-only Busy rise) in disk_stop.py for 0386; wiring split to 0409
