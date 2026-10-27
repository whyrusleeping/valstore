---
id: "0302"
title: Server-side replica forwarding for BatchApply to halve client egress per ACK (next 0297 lever)
status: done
priority: 1
created: "2026-09-22"
updated: "2026-09-24"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why

Task 0297 SCALE01 reached 941k ACK KV/s whole-write (per-second up to 1.24M) on the realistic two-node RF2/W2 device-sync fixture; the client host's 10 GbE egress is saturated (9.0–9.4 Gb/s, line cap 9.42 Gb/s measured) because the client fans every entry out to both replicas (~1.06 KB on the wire per ACK). Nodes run 18–23% of 24 cores; node inbound ~4.5 Gb/s each. 1M sustained needs ~90% of client line rate with the current wire shape. Evidence: `bench/0297-ingest-1m/scale01/FINDINGS.md`.

## What (design + prototype + A/B)

Client sends each (node,shard)-grouped sub-batch once to one replica; that replica applies it and forwards to the other replica(s), acking only when W replicas have applied with the requested persistence (device sync). Must preserve per-key ordering across forwarded batches, W2/device-sync ack semantics, stale-epoch/range-fence behavior, hinted-failure semantics (healthy refusal not hinted), Busy propagation, and per-entry outcome alignment. Consider load balance of forwarding role across nodes.

## Acceptance (preregistered)

- On SCALE01's best config (`run_scale.py --presplit did --shards 8 --depth 16 --batch 4000 --buffer-cap 800000 --prepare-workers 8 --gogc 300`): whole-write ≥ 1.0M ACK KV/s with client tx ≤ 60% of line rate, all correctness/lifecycle gates PASS, no Busy/debt growth.
- Reject if node CPU or node inbound links saturate below 1M, or if ordering/ack semantics need weakening.
- Then: preregistered sustained validation on a longer trace reaching auto-split/maintenance pressure.

Alternatives needing an owner decision instead: second client host/NIC; wire compression of BatchApply frames.

## Acceptance criteria

## Outcome

Built opt-in ForwardBatchApply (op 21 / resp 20): the receiver runs all BatchApply pre-checks, applies locally, forwards to targets concurrently, and returns per-node per-entry outcomes (W2/device-sync/hint semantics unchanged). Software passed comprehensive review. Preregistered dual-link ABBA (6 runs, all gates PASS) REJECTED it: client bytes/ACK 0.507x but ACK/s 0.626x master (816k vs 1,304k). Cause: per-range receiver choice gives each node shard two half-size requests per chunk (1.77x BatchApply, 2.3-2.8x device commits, gate sheds) plus a serial 18-31 ms forward hop. Server code stays on unmerged branch 0302-replica-forward. The opt-in client side stays committed, off by default (owner): valsgo f45d16a+15ebcb2, bootes 662914f. Variant filed as proposed 0306.

Commit: bench(0302): server-side replica forwarding FWD01 dual-link A/B — REJECTED (0.507x client bytes/ACK, 0.626x ACK/s); real-client10 --forward harness; server stays on branch 0302-replica-forward
