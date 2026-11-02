---
id: "0562"
title: N2 run of the async commit tail (one write round trip per value-log commit, flush kept)
status: proposed
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description
From 0546. `drain_and_flush_tail` waits for the in-flight value-log writes and then writes the staged blocks and partial tail synchronously, so each commit pays two device round trips before the cache flush. Candidate T submits staged+tail as one async write first, then waits for every in-flight write once (keeping both for retry if any write failed). It is implemented on branch `wip-0546-fua` (d4daa3e4, `submit_tail_and_drain`, gated by `VALS_VLOG_ASYNC_TAIL=1` when FUA is off).

Bench evidence (lab-1, `vals-bench spdk commitwrite`, 8 writers): post-step with no background I/O +10% (batch 2000) / +16% (batch 1000); pre-step with 350/150 MB/s background +7..+16%. Caution: 0544's PAD0544 (async padded tail before coalescing) was neutral on 3 nodes, and 0546 showed the isolated bench can over-predict.

Acceptance: an N2 arm (`SOAK5B-V5-N2-R3`, BEFORE discard, STOP at 4.5B, full checks) against the N2 baseline (4,290 s to 4.5B), with a keep rule fixed before launch (e.g. ≤ −5% active ingest, no 0.5B range > 5% below); keep → land T on master (as the default, not an env knob) with tests; reject → record.

## Acceptance criteria

## Work log
