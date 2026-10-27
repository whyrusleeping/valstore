---
id: "0315"
title: Parallel compaction lanes per node so run debt stays bounded above 1M KV/s ingest
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-27"
depends_on:
    - "0314"
spec_refs: []
---

## Description
## Evidence (LONG01-D1/D2, 400M KV)

Current master runs at 888k–910k ACK KV/s. Quartiles fall from about 1.25M to 750–830k.

- On both nodes, the single `vals-maint-comp` lane is 100 % busy from about 40 s until the end.
- Resident runs grow from 40 to about 200 per node and are still around 110–160 after 15 min of settle.
- Shard CPU per KV doubles. The growth is led by SPDK completion polling, run probes and false-positive (FP) reads.
- Nodes have 24 cores; shards and maintenance use about 10.

See bench/0297-ingest-1m/long01/FINDINGS.md, section "LONG01-D".

## Candidate (one mechanism)

Run N ≥ 2 compaction lanes per node. Keep at most one merge in flight per engine: that limit already exists through `compact_in_flight`. Keep lane-owned SPDK qpair affinity, the maintenance reserve and headroom accounting, and the backlog-aware pacing from 0314. Default N is chosen from the measured lane CPU and device headroom.

## Acceptance (to be preregistered before any fleet run)

Preregistered D-prefix ABBA against master:
- mean whole-write ACK/s ≥ 1.05×;
- all correctness gates PASS;
- resident runs at end of write lower, and settle converges or reaches lower runs;
- device write bytes/KV ≤ 1.3× master;
- max RSS no worse than master.


## Acceptance criteria

## Work log

### 2026-09-27: LANES accepted and merged

- **Change (`1486c50d`).** `VALS_MAINT_COMPACT_LANES` compaction workers per node (default 2, cap 4 because the 9100 PRO allows 15 qpairs and 8 serving + 1 flush + N lanes must fit). The lanes share one owner-fair queue. Each lane opens its own thread-owned maintenance qpair. At most one merge per engine stays in flight (`compact_in_flight`). Flush keeps one lane. The reserve/headroom accounting and the 0314 pacer are unchanged.
  - New tests: a rendezvous test proves two merges are in flight at once on 2 lanes and serialized on 1 lane; plus a drain/shutdown test and an env-parse test. The full `vals-cluster` suite passes. An independent review found no correctness or concurrency bugs.
  - Docs: `docs/backpressure.md` (lanes; pacer duty is per lane, not process-wide) and `docs/SPDK.md` (qpair budget `S + 1 + N`).
- **Preregistered D-prefix ABBA** (README.md; D3-M, D4-L, D5-L, D6-M; 400.7M KV; settle 900). All four runs PASS. **Every acceptance criterion passes:**
  - ACK/s **1.100×** (990k vs 901k); D4-L is the first D run with whole-write ≥ 1M and every quartile ≥ 0.9M.
  - End-of-write runs 113 vs 178 per node. Maintenance is idle after settle on both arms; runs at the end of settle are 93 vs 116.
  - Device bytes/KV **1.031×**.
  - Max RSS 6.77 vs 6.87 GB (lower).
  - Peak merges in flight 2; 3 maintenance qpairs; 0 qpair errors. Memtable peaks and flush stalls are unchanged.
- **Merged** as `910d60ad`. Evidence: FINDINGS.md section "Parallel compaction lanes (task 0315)", `SUMMARY-LANES-0315*.json`, `compare_0315.py`. The fleet is stopped.
- **Not yet sustained 1M at 400M.** D5-L reached 975k. Debt convergence still fails on every arm because the PD keeps splitting after drain (about 220 → 540 ranges). With lanes, the pacer sleeps 67–79 s per node (vs 2–25 s on M) whenever the queue empties. Next levers: the PD split rate, then pacer sleep under lanes.
