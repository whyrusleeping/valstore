---
id: "0233"
title: 'Hardware validation: rehome-under-ingest with bounded seeding (0231) — no seeding-attributable timeouts/mirror aborts'
status: done
priority: 2
created: "2026-08-23"
updated: "2026-09-07"
depends_on:
    - "0231"
    - "0260"
spec_refs: []
---

## Description

0231 bounded cooperative Merkle-leaf seeding per-turn work (4096-record fold pages; separate sidecar-load turn) and proved the donor-executor stall fix locally: single-600k-key-run A/B at production budget showed max apply stall 105.6ms → 14.5ms and apply p99 during seeding bounded near one fold page. The end-to-end acceptance items of 0231 that require a multi-node cluster remain to be confirmed on hardware, on the next soak (already needed for the 0232 crash investigation).

Validate on the lab cluster under live Jetstream ingest (bootes producer, 0230-style soak or the 0232 rerun):

Acceptance criteria:
- A debt-rehome of a populated split child (~500–600k entries) completes under sustained write load without mirror-failure aborts attributable to donor-side seeding stalls (watch `mirror failures reported` / rehome re-prepares).
- No 5 s RPC timeouts attributable to seeding on the donor shard (correlate timeout windows with ae_seed_* activity; ae_seed_fold_ops now exposes fold progress).
- Donor apply latency during an active rehome recorded and compared against the 0230 soak evidence (bench/bootes-soak-jetstream-20260822-0230/): prepare→commit spans stay well under RPC deadlines; no 60–100 s donor stall windows.
- Note: perf profile of the donor shard during rehome no longer shows seed_step/fold_key monopolizing the executor.

## Outcome

Fresh source-pinned three-node SPDK gate passed after 0260: 12.05M replay events (~44.5k/s), five rehome commits, zero aborts/client 5s timeouts/mirror failures, loss-free producer drain. Exact 660,455-entry donor profile captured wholly within rehome; slightly larger-than-requested child accepted with disclosed sizing deviation, 624,591-entry child also committed. Donor outer response-readiness p99 <=50ms during profile, no observations >1s; no seed/fold monopoly. Analyzer rerun reproduced evidence and independent reviewer accepted. No production source changed. Fleet stopped format=false; known AER storms and 300s forced node drains remain separate issues, retained corpus requires crash recovery. Evidence in bench/0233-batched-hardware-20260907/.

Commit: Validate rehome-under-ingest hardware gate after mirror batching
