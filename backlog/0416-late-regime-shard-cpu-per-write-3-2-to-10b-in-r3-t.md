---
id: "0416"
title: 'Late-regime shard CPU per write (×3.2 to 10B in R3, the whole post-0381 decline): call stacks at the 8B-equivalent state + post-restart elevation, then A/B the top caller'
status: done
priority: 2
created: "2026-10-10"
updated: "2026-10-11"
depends_on:
    - "0391"
spec_refs: []
---

## Description

Split from 0391 (bench/0363-compact-values/DECAY-R3-0391.md). In SOAK5B-V5-R3 (post-0381), device and merge bytes per ACK are flat to 10B (≈410 / ≈147 B/ACK per node). ACK/s falls 1.43M → 0.61M because mean shard CPU per synced write rises 5.25 → 16.41 µs while shard utilization stays roughly constant (cluster ACK/s × mean µs/write has CV 7.6% over 83 clean 100M buckets). Broker deferrals, merge debt, slowest-shard skew and shadow refresh are measured non-causes.

What remains unexplained is which callers grow. Node1 leaf growth from 300M to 8B is +29.2k cycles/write: allocator 45%, memmove 13%, memcmp 6%, select_victims family 11% (already fixed in bd0d50c2, after R3), device-poll family 9%, KeyFilter::excludes 2.5%, RangeMap clone 1.4%, mallinfo2 1.2%. The callers of the allocator, memmove and memcmp growth are unknown, because R3's captures are leaf-only.

Second item: after each restart, a node's shard µs/write is 1.5–2.0× node1's. The excess decays over 30–45 min, but about 1.13–1.16× persists. It adds roughly 10% to the cluster mean late.

## Outcome

L1 model run to the R3 8B-equivalent (667M writes) with on-box-timed fp+lbr captures. Growth 25M→667M (+38% µs/write; model reproduces ~1/4 of R3's relative growth, reported) by owner: split recount (time-averaged 44–64%, merge linear in runs), KeyFilter::excludes 13–17%, run probes ~15%. Clean restart: ×2.68/×2.13/×1.51 at +5/+20/+45 min vs matched no-restart arm, almost all key-filter rebuild gap and logical-accounting reseed. Preregistered A/B kept the heap merge: 333–667M 3.288 vs 3.370/3.361 µs/write, 18/20 buckets below both baselines, lbr merge cycles/key 1,874→692, microbench −59% at 24 runs; ~half the −2.3% is a client-rate confound (fix ~1–2%). Piggyback signal registered; follow-ups proposed 0420 (R3 multi-node callers), 0421 (post-restart key-filter gap), 0422 (post-restart seed); KeyFilter stays in 0414, RangeStats in 0415.

Commit: perf(0416): heap k-way merge in winner_count_page_prefetched (split recount was linear in runs); L1 8B-equivalent call-stack attribution + post-restart excess vs matched control; follow-ups 0420-0422
