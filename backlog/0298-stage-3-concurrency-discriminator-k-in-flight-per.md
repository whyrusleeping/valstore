---
id: "0298"
title: 'Stage-3 concurrency discriminator: K in-flight per-zone PutBatches with per-key ordering guard on the realistic fixture'
status: done
priority: 1
created: "2026-09-22"
updated: "2026-09-22"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why

Task 0297 RANGEMAP01 (`bench/0297-ingest-1m/rangemap01/FINDINGS.md`) measured the one-lane realistic ceiling at ~140–150k ACK KV/s with clean seconds at 200–219k = exactly 2 in-flight 1000-KV PutBatches / ~9.5 ms RTT (Little's law). Shard threads were ~35% busy and device sync ~1 ms, so server headroom exists but a one-lane ordered client cannot use it. This is the stage-3 lever toward the 1M target.

## What

Prototype (bench client only, not production bootes) bounded parallelism at independently orderable boundaries: keep up to K PutBatches per zone in flight; a chunk whose keys collide with any in-flight chunk waits (per-key ordering preserved); bounded K and memory; unchanged RF2/W2 device-sync, retries and acknowledgement semantics; same harness/fixture/guards as RANGEMAP01 (`run_ab.py`), BASE server `4f6620be`.

Arms: K=2 (equivalent to accepted sink), K=8; one matched pair, repeat once if within ±5%. Measure whole-write and clean-second ACK/s, PutBatch p50/p99, actual in-flight KV, Busy/fence retries, server RSS/staged debt/resident runs, late-phase regression (post-first-split vs pre-split ratio).

## Acceptance / rejection (preregistered)

- Accept direction: K=8 clean-second ACK/s ≥ +60% over K=2, all correctness/lifecycle gates PASS, PutBatch p99 < 3× K=2, no Busy/fence retry growth, post-split ratio not worse than K=2.
- Reject: < +20%, or retries/debt/late regression like earlier lanes-2 experiments.
- Then: stack FLUSH commit-slice (`0297-flush-commit-slice`) as the next A/B only if flush-second tails become the measured limiter at K=8.

Not a production recommendation; not a 1M claim.

## Acceptance criteria

## Outcome

DEPTH01 (bench/0298-depth-window/): 8 matched 2-node RF2/W2 device-sync runs, gate-32 master-equivalent server, 800k sink window, only depth varied; all PASS, zero gate sheds/Busy. Per-second p50 ACK/s: K4 704k, K8 840k (+19.3%), K16 913k (+29.7%); whole-write +23.7%/+36.5%. Preregistered verdict: not accepted (+29.7% < +30%, RSS 1.30x > 1.25x), not rejected (> +20%). K4 is in-flight-bound; K16 hits the client 10GbE egress wall. Gate-aware Busy retry unneeded at K<=16. 0304 guidance: bounded configurable depth. Next lever remains 0302. Fleet stopped clean, no new cores.

Commit: bench(0298): DEPTH01 depth discriminator at 800k sink window — K16 +29.7% p50 over K4, link-bound
