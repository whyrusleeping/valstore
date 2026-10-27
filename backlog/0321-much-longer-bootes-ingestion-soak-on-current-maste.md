---
id: "0321"
title: Much longer bootes ingestion soak on current master (post-0319), beyond the 400M-KV LONG01-D fixture
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description

## Description
## Owner request (2026-09-27)

After 0297/0309 closed (1.15M ACK KV/s sustained over 400.7M KV, about 350 s of writes, on 2-node RF2), the owner wants to see how the system fares on a much, much longer bootes soak.

## Facts to plan around (measured 2026-09-27, LONG01-D13)

- **Source.** The sealed cache `/data/dataset/jetstream-cache-20260824` holds about 1.0B events, which is about 2.1B logical KV at about 2.1 KV per event. At 1.15M/s that is only about 30–35 min of writes. Anything longer means either pacing the replay, adding updates/deletes (for example replaying again over existing keys), or live Jetstream.
- **RAM.** Server RSS is about 16 B/key/replica: 6.5 GB at 400M keys/node. On 2 nodes with RF2, the full cache is about 34 GB/node, right at the 36 GiB `VALS_RAM_BUDGET_BYTES`. On 3 nodes with RF2 it is about 1.4B keys/node, about 22 GB.
- **Device.** 319,705 used segments at 400M keys/node, about 0.8 KB/KV. The full cache on 2 nodes is about 1.7 TB/node. That fits the 4 TB 9100 PRO, but the zone regions (records 128 GiB and backlinks 120 GiB per partition, 8 partitions) would need resizing.
- **Known risks from the long soaks in 0247/0248:**
  - recurring bit-42 heap corruption on lab-2, which recurred once in LONG01-B25-T;
  - AER storms (0250);
  - earlier replay runs were starved by silent restarts.
  - A soak therefore needs driver-rate verification (not just unit liveness), core capture, and stop/preserve-on-first-failure.

## Open questions (owner)

1. Shape: a full-rate replay of the entire cache, or a multi-hour/day soak at a paced rate or with overwrites?
2. Topology: 2 nodes (cap at a RAM-feasible prefix) or add lab-3 for 3-node RF2?
3. Client: the real-client13 fixture harness, or the production bootes binary with its own cache replay and cursor persistence?

## Acceptance (to be finalised and preregistered once the questions are answered)

- Written soak plan with duration/progress bracket, RAM/device/region projections, and rejection gates.
- Soak run on current master with production defaults. Reported: per-10 s and per-progress rates, low tails, resource/debt timelines, and correctness receipts (counts, samples, cursor).
- Any failure is attributed. The fleet is stopped and preserved per the runbooks.
- Findings are recorded under `bench/`.

## Acceptance criteria

## Outcome

Full sealed-cache bootes soak done. - F1 (2 nodes) crashed at 387M KV on the lab-2 bit-42 hardware signature (0248). - F2–F4 (3-node RF2) each ingested 2,090,212,819 KV at about 725k ACK KV/s, with 0 crashes and the cursor saved. - Settle RAM blowups were fixed by 0322 plus the streaming seed (F4 RSS 19.3 GB). - Re-verify on retained F4 data, after 0325 scan admission: exact census PASS (1,090,186,118 backlinks and 1,000,000,008 records, digests match), cursor match, 4,489/4,489 samples match. - Findings are in bench/0321-long-soak/FINDINGS.md. - Follow-ups: 0316, 0323, 0324, 0326, 0327, and proposed 0328.

Commit: backlog(0321): close long bootes soak — full 2.09B-KV cache ingested 3x on 3-node RF2, exact census/cursor/samples PASS on retained F4 data
