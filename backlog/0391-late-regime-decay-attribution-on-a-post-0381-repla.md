---
id: "0391"
title: 'Late-regime decay attribution on a post-0381 replay past the drive step: bounded merge bytes/ACK, attained-bandwidth residual, broker debt, merge-lane work, shadow refresh'
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-10"
depends_on:
    - "0385"
spec_refs: []
---

## Description

Split from 0385 (DECAY-FINDINGS.md fixes #1, #3 and #4). In pre-0381 V3-R2, the post-step cost grows 1.00 → 4.91 µs/ACK. Symmetric accounting splits that growth 64% bytes, nearly all of it merge output from giant merges, and 36% falling attained device GB/s (0.517 → 0.299/node), which is causally unresolved. No post-0381 run has been observed past the drive step (~2.5–2.9B ACK).

Do it on the 0386 full replay (or a continuation of a retained large post-0381 corpus). Use the new counters if they have landed.
1. Measure merge B/ACK and device B/ACK past the step under 0381. Check whether it holds near ~240 merge B/ACK, which is a 2.9× byte-budget ceiling against V3-R2's late point.
2. Matched perf captures at early, mid and late points (cpu_core, leaf symbols). Record the actual capture start/end on the box and the counter endpoints that match them.
3. Attribute the attained-bandwidth residual: drive read/verify latency, broker deferrals (bytes/wait), merge-lane useful CPU vs polling, slowest-shard gating.
4. 0382 item 1 (bounded/incremental shadow refresh): at the real trigger (≤60% free per engine), run a preregistered short A/B on the retained corpus.

Acceptance: an updated decay attribution covering the post-0381 late regime, with each surviving hypothesis given an A/B verdict or filed as a task. Remove measured-neutral or negative paths.

## Outcome

L0 attribution from R3 in bench/0363-compact-values/DECAY-R3-0391.md (analyze_0391.py, SUMMARY-0391.json). - 0381 holds the byte budget to 10B: device ~410 B/ACK per node, merge 141–159 (V3-R2 at 8B: 1,468). - The decline is per-write shard CPU at roughly constant utilization: ACK/s 1.43M → 0.61M, shard µs/write 5.25 → 16.41. Node1 alone (never restarted) shows ×3.18. - The drive step adds a one-time 1.1–1.5 µs/write; post-restart nodes add about 10% late. - Rejected by measurement: RAM broker, merge debt and lanes, shard skew, drive bandwidth. - Leaf growth: allocator 45%, memmove 13%, select_victims 11% (fixed in bd0d50c2). - Shadow refresh: L0 ceiling ≤0.6% of shard CPU. - Filed 0416 (L1 call stacks + post-restart A/B) and 0417 (L2a on-box captures + shadow-refresh A/B, needs 0404). The L2a items remain unconfirmed. The standard review accepted on round 2.

Commit: bench(0391): post-0381 late-regime decay attribution from R3 (L0) — bytes/ACK flat ~410 to 10B, decline is per-write shard CPU x3.1; follow-ups 0416 (late callers + post-restart, L1) and 0417 (L2a captures + shadow-refresh A/B)
