---
id: "0401"
title: node1 heap grows ~15 GB uncharged under post-drain read load (allocator in_use 24→39.5 GB, swap, samplecheck >4 h) — SOAK5B-V5-R3
status: done
priority: 2
created: "2026-10-08"
updated: "2026-10-10"
depends_on: []
spec_refs: []
---

## Description

## Description
## Evidence (SOAK5B-V5-R3, 0386, binary c57e90ed… at pin 8736b2b1)
After R3 drained (18:24Z, 10.17B ACK), only post-drain checks ran: a cursor probe, then samplecheck (derive ~75 min, then sampled Gets at R=2 of 2).

**node1 (lab-1, which also hosts the PD):**
- `vals_allocator_in_use_bytes` grew 24.39 → 39.49 GB, and RSS 28.26 → 43.10 GB (peak 43.71 GB, about 0.1 GB under the RSS planned stop).
- `vals_ram_charged_bytes` stayed flat at 24.39 → 24.75 GB, and the run-metadata/key-filter gauges stayed flat.
- RAM admission backpressure engaged from 21:10Z, and the box swapped 3.5–5.9 GB (8 GiB /swap.img).
- Growth was ~1 GB/15 min from 18:35, with a +5.7 GB jump around 20:20–20:35.

**node2/node3** served the same read probes (`vals_read_probe_bytes` ≈ 47 GB, ≈ 2.35M probe runs each) with flat heaps.

**Effect:** samplecheck exceeded its 14,400 s bound with no output, and zone counts/aediag never ran. It normally takes ~60–75 min at this size.

**Logs** were quiet apart from 120 range-map epoch adoptions (PD rehome activity after the drain) and 2 "compaction step skipped — in-flight read(s) did not drain".

Raw data:
- `/data/dataset/task0363-results/SOAK5B-V5-R3/`: soak5b-samples.jsonl, live-logs/lab-1-node1.stderr, samplecheck-*.
- The corpus is retained on the drives, so the problem can be reproduced by restarting the R3 fleet without formatting.

## Acceptance
- Root-cause the node1-only uncharged heap growth. Use the retained R3 corpus or a local repro. Possible causes to discriminate: read/Get path, rehome or AE activity, PD co-location, connection or request state.
- Fix it, or bound and charge it, so that a post-drain samplecheck-sized read workload keeps every node's allocator in-use within ~1 GB of its charged bytes.
- Re-run the R3 post-drain checks: cursor, samplecheck, zone counts, owned aediag. They must finish within the harness bounds with no RAM backpressure or swap. Record whether the R3 corpus passes.
  - **Split (coordinator, 2026-10-10):** cursor, samplecheck and zone counts PASS on the fixed binary. Owned aediag reached 66/72, then 44/72, because of digest-cap Busy caused by abandoned periodic-AE digest scans. That criterion and the overall R3 corpus verdict moved to **0411**, which depends on this task. 0386 resumes after 0411.
- Short report under bench/; 0386 resumes after this.

## Acceptance criteria

## Outcome

Root cause: AE bucket-digest serve working sets were neither bounded nor charged. In-node AE gives up on RPCs after 5 s, but the server scans keep running; node1's digest-serve bound reached ~30 GB. Fix: a dedicated digest-serve byte cap (max_queued_bytes/4) that leaves shared transport credit alone, plus a RAM charge, retryable Busy, a rejection counter and an L0 test. Fixed binary on the R3 corpus (read-only): heap within 0.73 GB of charged bytes, no RAM Busy, no swap; cursor, samplecheck and zone counts PASS. Owned aediag reached only 66/72 under cap contention, so it and the R3 verdict moved to 0411. An aediag backoff variant did worse (44/72) and was reverted.

Commit: fix(0401): bound and RAM-charge AE bucket-digest serving (dedicated cap, Busy on overflow); R3 node1 heap root cause + fixed-binary confirmation; owned aediag split to 0411
