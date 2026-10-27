---
id: "0223"
title: Sorted-zone scans under write churn regressed ~7x with deadline-miss storm (found in 0222)
status: done
priority: 2
created: "2026-08-19"
updated: "2026-08-19"
depends_on: []
spec_refs: []
---

## Description

Found during the 0222 sorted-vs-unsorted session (bench/lab-sorted-vs-unsorted-20260819-0222/FINDINGS.md).

On HEAD db31462 (latency profile, 4 shards/node, lab, 20M×256B sorted zone smix with 24 canonical ranges), subrange scans (width 512, 24 readers × pipeline 64) under background write churn collapse:

- 2026-08-06 code at ~54k bg writes/s: 2,621 scans/s (1.34M rows/s), 0 transport errors.
- HEAD at ~62k bg writes/s (writers=2): 353 scans/s (181k rows/s), 25,960 scan deadline misses in 60 s (~55% of attempts), scan latency body 100–260 ms, p99.9 ~1 s. (raw/P4-sorted-scan-writers2.log)
- HEAD at 217k bg writes/s (writers=8): 207 scans/s, 20,915 misses, p99 ~1 s. (V-valsmix-session.log flow 3)
- HEAD quiescent (writers=0): 3,615 scans/s (1.85M rows/s), p99 16 ms, clean — so base scan capacity is fine; the regression is specifically scan-vs-write contention. Point reads under the same churn are fine (225k/s sorted).

Zero consistency errors in all arms — this is a liveness/latency regression, not correctness.

Suspects to bisect/instrument:
- staged-scan cache introduced in d181a83a (cluster: scan/apply dedup, staged-scan cache).
- ReadGate staged span pages / restart-after-yield behavior under write pressure (0156 family; docs/backpressure.md).
- Latency-profile (winner directory / blind append) interaction with scan paths — old numbers were capacity-profile-era defaults.

Acceptance: identify the mechanism; restore scans under ~60k bg writes/s churn to the same order as 2026-08-06 (≥ ~2k scans/s clean, no deadline-miss storm) or document why the new floor is acceptable. Local repro likely possible with the single-box valsmix recipe (vals-examples run-local-sorted.sh) before going to hardware.

## Outcome

Root cause: 0173 made scans restart on aggregate_generation whose mutable_gen bumps every put — under churn a yielding scan never stabilizes, exhausts staged restarts, falls into sync plan_scan, then Busy (the 0222 miss storm). Fix: scans invalidate on storage_gen only; 0173's argmax invariant kept per-key via a finalize pass re-resolving only keys whose winner advanced (quiescent fast path skips it). Also fixed latent hole: orphan sweep now bumps storage_gen when freeing segments. Added 4 scan mechanism counters. Proof: bench/local-scan-churn-20260819-0223/FINDINGS.md — forced-yield harness reproduces the storm on baseline (15 scans/s, 9,122 misses) and shows it eliminated (743 scans/s, 0 misses, 0 restarts, 0 consistency errors). Kill-the-fix tests added; suites green in DEBUG. lab hardware re-validation filed as 0224. Review: sol accept.

Commit: 0223: fix sorted-scan collapse under write churn — per-key finalize replaces global mutable-generation scan invalidation; orphan sweep now fences storage_gen
