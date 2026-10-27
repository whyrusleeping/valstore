---
id: "0327"
title: '3-node write efficiency: 725k ACK KV/s vs 1.15M on 2 nodes (fan-out/sync barriers per KV, window underfill)'
status: done
priority: 2
created: "2026-09-28"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (read-only attribution, SOAK0321-F3 vs LONG01-D13)

- **Matched progress, 30–350M KV.** 3-node RF2 ran at 0.79M ACK KV/s, against 1.143M on 2-node. That is about 484k applies/s per node, against 1.15M.
- **The client window was not full.** It averaged 25.4 of 32 PutBatches active, and 616k entries were buffered. Mean successful PutBatch was 130 ms.
- **Client NIC and CPU are not the limit.** eno1np0 carried 5.1 Gb/s. Client CPU was 9.6 cores, the same as on 2-node.
- **The server does more work per KV.**
  - Server frames per logical KV: 5.6 against 3.8.
  - Device syncs per KV: 2.6 against 1.8.
  - BatchApply: 26–29 ms against 21–22. Sync flush: about 10 ms against about 6.
- **Decay over the full cache.** Rate fell from 773k to 660k, and BatchApply rose from 31 to 38 ms. Over the same span live keys went from 660M to 1.31B and runs from 183 to 267.

## Next discriminator (one)

A short matched-progress A/B of bootes `--vals-depth` 16 against 24 on the 3-node fixture.
- If the window being underfilled is what limits throughput, expect up to about +20 %.
- If throughput barely moves while server latency rises, that confirms per-destination fan-out and sync barriers as the cost. The candidates then become coalescing per-destination sub-batches across chunks, or larger batches.

## Acceptance

A measured attribution, and at most one fix with a matched A/B.

## Acceptance criteria

## Outcome

Hardware A/B (MASTER0327, 3-node RF2, ABBA to 400M ACK): D24 is 28–45% slower than D16, because 24×2 zones exceeds the per-shard staged-apply gate of 32 (about 8k gate Busy per arm). D16 runs at about 814k ACK KV/s. Root cause of the 3-node deficit: shard CPU per applied write is about 12 µs on 3 nodes against about 6.2 µs on 2 nodes, independent of load. perf puts about 90% of the added cycles in ValueLog drain_and_flush_tail/submit_block and SPDK completion polling; index work is unchanged. No fix here; follow-up 0334 (todo). Harness and FINDINGS are in bench/0327-3node-depth. Reprovisioning destroyed the SOAK0321-F4 drive data; its results on devbox are kept.

Commit: bench(0327): 3-node depth A/B harness + findings — D24 collapses on the staged-apply gate (32); 3-node deficit is 2x shard CPU/write in vlog flush + SPDK completion path; follow-up 0334
