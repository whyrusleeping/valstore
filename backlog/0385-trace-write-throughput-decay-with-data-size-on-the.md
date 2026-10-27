---
id: "0385"
title: Trace write-throughput decay with data size on the 5B replay (1.5M → <0.5M ACK/s); attribute per-ACK cost growth before and after the drive step
status: done
priority: 1
created: "2026-10-06"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why
Write throughput is the top priority, and it falls steadily as data grows. The owner accepts 1M+ as demonstrated (0352), but wants this decay found and fixed.

## What we know (3 nodes, 5B-event cache, bootes depth 32 / gate 64)
| run | per-bucket ACK/s | notes |
|---|---|---|
| SOAK5B (pre-0378/0379/0381) | per 500M: 1.653M, 1.657M, 1.577M, 1.262M, 1.154M, 1.024M, 0.939M, 0.785M | from 1.6B ACK, confounded by stuck AE seeds (0379, −23%) |
| V3-R2 (0378+0379, pre-0381) | per 1B: 1.50M, 1.31M, 1.13M, 954k, 752k, 432k, 303k, 246k | late decline mostly forced-merge write amp (0381), plus merge-RAM deferrals |
| V4 (0381) | whole run 1.331M to 2.665B vs V3-R2 1.343M at matched ACK | 0381 kept merge bytes per write flat (172–235 B), **but ACK matched V3-R2 up to 2.5B**: the 1–2.5B decline is NOT forced-merge write amp |
| V4-R3 (0384) | 0–1B 1.694M; 1.0–1.5B 1.620M | stopped at 1.55B (lab-2) |

Facts already established:
- **Drive step.** The drive changes state at ~1.04–1.19 TB written per node (~2.44B ACK on 3 nodes). After the step, device write GB/s is roughly flat (0.41–0.57 GB/s/node; DRIVE01/02). Flush cost per command rises 0.55 → 2.1 ms.
- **Post-step model (FULL9).** ACK/s ≈ device write GB/s ÷ device bytes per ACK. Flush coalescing alone does not lift it (FULL8/FULL9 rejected).
- **No run since 0381 has been observed past the drive step;** all three V4 runs stopped earlier on lab-2.

## Candidate terms (none measured as a share of the decline yet)
1. **Device bytes per ACK:** vlog vs run-block vs index/filter/footer vs merge output. Does it rise before the step?
2. **Shard CPU per write growing with run count.** About 75–100 runs per engine after 0381.
   - The O(runs²) shadow estimate runs on every manifest bump once free ≤ 60% (0382 item 1). Disk free crosses 60% partway through the run, so this is a step-shaped hypothesis to check against the free-fraction timeline.
   - Key-filter positives and run-bloom probes on overwrite and existence checks.
3. **Slowest-shard gating:** busiest/median `vals-shard` ratio by bucket (noise floor ~1.15).
4. **RAM broker deferrals** shrinking merge throughput at multiple billions of keys per node, so run count and probe cost grow (SOAK5B node2: merges per 100M 30 → 13).
5. **Merge lanes CPU-bound** (`BlockedBloom::insert` is the top symbol), so merge debt accumulates.
6. **Growth of the in-RAM index/fences:** search depth and cache misses per write. malloc/free is ~29% of shard cycles.
7. **Drive state:** read p99 159 → 2,200 µs after the step (DRIVE01) slows merge input reads and build verify.

## Plan (measure before design)
1. **Read-only decomposition** of existing raw data (`/data/dataset/task0363-results/` SOAK5B-V2, V3-R2, V4, V4-R3). Per 100M ACK bucket, per node:
   - ACK/s;
   - device write GB/s and device bytes per ACK, split by source;
   - shard busy µs per write (max and median thread);
   - vlog stall µs and domain flush device/publish times;
   - runs and frozen runs per engine; disk free fraction;
   - shadow-estimate refreshes, if measurable;
   - broker deferrals, merges per 100M, merge-debt bytes;
   - AE seed activity; Get/probe counts.

   Find which terms grow in step with the 0.5–2.5B decline (pre-step) and which with the post-step decline. List any counters that are missing; add them (no behaviour change) before the next run.
2. **Profiles on the next full-replay run:** perf at matched early, mid and late points (cpu_core section, leaf symbols; no frame pointers).
3. **Short targeted A/Bs per surviving hypothesis.** Preregister each. Prefer short runs that start from a retained large corpus over full replays; compare whole-arm distributions (~13% run-to-run variance).

## Acceptance
- A written attribution (`bench/…/DECAY-FINDINGS.md`) that accounts for most of the growth in per-ACK cost (shard µs per write, or device bytes per ACK after the step) from 0.5B to the latest observed point, with evidence for each term.
- A ranked list of fixes. Fixes are filed as tasks or merged here with A/B evidence; 0382 is the likely first.
- Measured-neutral or negative paths are removed per the cleanup doctrine.

## Notes
- lab-2's RAM is known bad and will not be replaced or retested (owner, 2026-10-06). Its integrity events are expected; judge them under the full-replay stop rules.

## Acceptance criteria

## Outcome

DECAY-FINDINGS.md (bench/0363-compact-values) attributes the decay using the existing SOAK5B-V2/V3-R2/V4/V4-R3 data and perf captures. Endpoint cost goes 0.582 → 4.907 µs/ACK; 90% of that growth is post-step. Post-step: merge output from giant merges accounts for 101% of the byte growth; symmetric accounting splits the post-step cost growth 64% bytes and 36% lower attained device GB/s, the latter causally unresolved. Pre-step: shard CPU per write rises +48%, ~56% of it in allocator/copy/compare/poll leaf symbols, callers unknown. The 0382 shadow refresh has zero pre-step share (first 60%-free crossing at 3.63B). Fixes are ranked; follow-ups are filed as 0389 (counters), 0390 (pre-step caller attribution + A/B) and 0391 (post-0381 late regime incl. the 0382 A/B). The 3.8 MB ANALYSIS-decay.json is kept untracked in /data/dataset/task0363-results/. Standard review accepted.

Commit: bench(0385): write-throughput decay decomposition from existing 5B-replay data (pre-step shard CPU growth; post-step merge bytes + attained-bandwidth fall); follow-ups 0389/0390/0391
