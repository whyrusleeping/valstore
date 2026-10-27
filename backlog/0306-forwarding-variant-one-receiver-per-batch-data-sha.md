---
id: "0306"
title: 'Forwarding variant: one receiver per (batch, data shard) to keep one BatchApply per node shard; matched dual-link A/B'
status: done
priority: 3
created: "2026-09-24"
updated: "2026-09-24"
depends_on:
    - "0302"
spec_refs: []
---

## Description

## Description
## Why
0302 FWD01 (`bench/0302-replica-forward-ab/FINDINGS.md`) REJECTED per-range receiver forwarding: client bytes/ACK 0.507x but ACK/s 0.626x of master (816k vs 1.30M). Mechanism: choosing the receiver per key range splits each node shard's share of a batch into two half-size requests (one from the client, one forwarded), so ~1.8x server BatchApply calls (28.2-28.6k vs 16.0k per node), 2.3-2.8x device group commits, gate-32 sheds, plus a serial forward hop of 18-31 ms.

## What
Rebase branch `0302-replica-forward` (server, unmerged) and the opt-in valsgo `ForwardReplicas` path (vals-examples f45d16a/15ebcb2; harness real-client10) so the client picks ONE receiver per (PutBatch, data shard) destination group, for example alternating by batch sequence and shard index. The goal is that each node shard again receives exactly one request per batch. Keep all 0302 semantics (refusal before forward, per-node per-entry outcomes, W2/device sync, peers' own fences).

## Acceptance (preregister before runs)
- Same dual-link fixture and ABBA-style order as FWD01 (B = master, F = variant + `--forward`), with the same bands. ACCEPT requires: F bytes/ACK ≤ 0.55x B, F mean ACK/s ≥ 0.97x B, zero gate busy/Busy, and all gates PASS. REJECT if F mean ACK/s < 0.93x B.
- Report server BatchApply calls per node, device commits, and forward-leg latency against FWD01.
- If rejected, keep the code off master.

## Acceptance criteria

## Outcome

Rebased the server branch 0302-replica-forward onto master as 5b178d75, with crates/docs identical to 1b9ed23d. Built the diagnostic client real-client11: it picks one receiver per (PutBatch call, replica set, data shard), sorted[(seq+shard)%n], and has a unit test. Registered arms MASTER0306/FWD0306. The preregistered dual-link ABBA FWD02 (B,F,F,B,B,F; valsd byte-identical to FWD01) passed every gate but is REJECTED: F 979,625 vs B 1,300,970 ACK/s (0.753x), bytes/ACK 0.503x. Zero gate busy, Busy and target failures; receivers split 50/50; BatchApply per node back to 16.0k (FWD01 28.4k). Device commits are still ~1.8x master, the forward hop is 23-36 ms, and PutBatch is +41%. Nothing merged; production valsgo and bootes unchanged. Evidence is in bench/0306-forward-per-shard-ab.

Commit: bench(0306): per-(batch, data shard) forwarding receiver FWD02 dual-link A/B — REJECTED (0.753x ACK/s, 0.503x client bytes/ACK); real-client11 harness; server stays on branch 0302-replica-forward (rebased 5b178d75)
