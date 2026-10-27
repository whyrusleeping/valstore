---
id: "0420"
title: 'R3 late shard-CPU growth outside the one-node L1 model: attribute allocator/memmove/Vec::retain/RangeMap::clone/flush_dirty growth from call-stack captures on the next hardware replay'
status: done
priority: 1
created: "2026-10-11"
updated: "2026-10-13"
depends_on:
    - "0416"
spec_refs: []
---

## Description

Split from 0416 (bench/0416-late-shard-cpu/FINDINGS.md). The 0390/0416 one-node, one-shard, RF 1, file-backed L1 model reproduces only about a quarter of R3's relative late growth (+38% shard µs/write from the 300M- to the 8B-equivalent state, against ×2.53 on R3 node1). R3's growing leaves that have no equivalent in the model: allocator (+13.2k cycles/write 300M → 8B; `unlink_chunk`/`_int_free_merge_chunk` 9.6% of shard cycles at 8B vs 5.3% in the model), memmove (+3.9k), `Vec::retain` closures, `RangeMap::clone`, `vals_client::net::client::flush_dirty`, `plan_bucket_digest_page`, SPDK `poll_devices`.

Work: on the next hardware replay (0386, or a 0404 continuation), take the piggyback captures registered in 0416's FINDINGS ("Hardware piggyback signal"): `--call-graph lbr` and fp, 10 s, one node's vals-shard at 2B/4B/8B, on-box endpoints and a metrics scrape at both ends (bench/0416-late-shard-cpu/capture.sh). Partition by owner with `analyze.py` (extend its owner list for replication/AE/SPDK paths), name the callers of the allocator/copy growth, and file or A/B the top one.

Acceptance: caller attribution of most of R3-equivalent late growth from real multi-node call stacks; the 0416 recount-fix piggyback check (winner_count cycles per recounted key) recorded.

## Outcome

mirror_rehome_writes no longer clones the zone's whole RangeMap (~8.5k ranges) per mirrored response; it borrows shared.range_maps for the synchronous part only. 0418's real 3-node RF 2 SPDK stacks at R3 8B: the copy + drop is 43–49% of vals-shard cycles (15–20k cycles/write), 51–69% of R3's 300M→8B growth (71–89% with the fixed select_victims and device-poll families). 0416 recount check at 8B: 325–523 merge cycles/key; no 2B trend was captured. Local A/B (ab.py, 3 nodes RF 2, 17k ranges, bootes): shard µs/write BASE 9.65/9.70 vs FIX 4.63/4.96 (−50%, client ×2), positive. 336-range control is within noise. vals-cluster lib: 837 passed. Residual is proposed as 0435.

Commit: perf(0420): borrow the zone RangeMap in mirror_rehome_writes instead of copying it per batch response (R3 8B: 43–49% of shard cycles); local RF 2 A/B at 17k ranges −50% shard µs/write
