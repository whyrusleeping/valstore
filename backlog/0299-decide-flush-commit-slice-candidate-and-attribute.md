---
id: "0299"
title: Decide flush-commit slice candidate and attribute the residual multi-store flush burst
status: done
priority: 2
created: "2026-09-22"
updated: "2026-09-22"
depends_on:
    - "0297"
spec_refs: []
---

## Description
## Evidence

`bench/0297-ingest-1m/rangemap01/FINDINGS.md` Result 2/3: a store flush commits its memtable in on-core steps of 4096 index removals (~5.5 ms each); each step delays every in-flight batch apply on that shard and the client waits for the max over 8 shard sub-batches. Single-store flushes cost a 1 s dip to 70–140k every ~8 s; simultaneous flushes of all records stores (they fill in lockstep) cost 3–4 s at ~25% rate. Candidate `1768d750` (commit slice 4096→512, branch `0297-flush-commit-slice`) measured +5.0% and +8.7% whole-write in two matched pairs (flush-second ACK +24–31%, stalls 11–16 s → 5–7 s, clean seconds unchanged, commit overhead ≤ +11%), below the preregistered +8%/+40% accept band → inconclusive-positive, not adopted.

## What

Decide the flush-commit slice candidate with one more matched pair (or a longer/bigger-corpus run that has more flushes), and characterize the residual multi-store burst: attribute the remaining 59–104k seconds between plan slices (0.5 ms × ~700), the BASE-style prepare-epoch reseed and off-core run build (device write bandwidth / maint thread contention). If the residual is plan slices, extend the same bounded-step treatment to `flush_plan_step`; if it is the lockstep multi-store flush, evaluate staggering per-store flush thresholds (only if the max-of-fanout analysis shows it helps).

## Acceptance

Adopt only with n≥3 matched pairs showing ≥ +5% whole-write and ≥ +30% flush-second ACK, clean seconds unchanged, commit/plan total time ≤ +15%, correctness/lifecycle/debt gates PASS. Reject/park otherwise. Not a 1M claim; this recovers part of the ~20% late loss on the one-lane path and reduces tail latency that grows with client in-flight depth (0298).

## Acceptance criteria

## Work log

### 2026-09-22 — decided by owner: merged as `35680a9a` on the n=2 evidence (+5.0%/+8.7%). Residual multi-store burst attribution remains open but is now secondary to the staged-apply gate limit found in PRESSURE01 (see 0298); reopen only if flush-second tails become the measured limiter at higher client depth.
