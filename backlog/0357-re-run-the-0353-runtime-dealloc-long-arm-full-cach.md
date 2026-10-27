---
id: "0357"
title: Re-run the 0353 runtime-dealloc long arm (full-cache, correctness-gated) after the 0355 fix
status: done
priority: 2
created: "2026-09-30"
updated: "2026-10-12"
depends_on:
    - "0355"
    - "0361"
    - "0362"
spec_refs: []
---

## Description
Split from 0353: this carries 0353's unmet acceptance criterion, "the runtime-dealloc long arm shows no degradation and no correctness regression". 0353 already delivered the matched ABBA and its receipts in bench/0353-dealloc/FINDINGS.md. Its only long arm (DEALLOC0353-LONG, server 57234ef6) FAILED:
- an exact backlinks census found 3 extra keys;
- node3 logged 2 index-CRC compaction failures;
- the DSM error counters ended at 2 / 1;
- rate fell from 874k to 233k KV/s across the span;
- post-run NUSE was 82% / 75%, versus 43.6% allocated segments.

## Work
- Prerequisites: 0355 has an evidence-backed explanation and any fix is merged, and the owner or 0355 has released the retained LONG corpus for reprovisioning. Use lab-1+3 only; never lab-2.
- Reuse bench/0353-dealloc (`drive_state.py prepare`, `run_dealloc.py run --attempt <new LONG label>`, `drive_state.py readback`). The harness now fails fast on snapshot, compaction or DSM error counters.
- Set `device_dealloc_segments_per_tick` explicitly to a nonzero value, because 0356 changes the default to off.
- Full sealed-cache replay from a full-drive precondition. Report per-100M rate, vlog µs/stall, DSM counters and latency, used segments vs NUSE, and a complete verify (cursor, samples, exact census).
- If the provision-probe finding still matters (NUSE settles only after about 180 s idle), consider recording or trying a post-provision idle wait. Label it as a separate variant.

## Acceptance
- Clean drain; no crashes, restarts or new cores; zero CRC, snapshot or DSM errors; exact census matches.
- Rate and per-stall waits are compared to a matched runtime-dealloc-OFF long control if the ON arm degrades. Without that control, degradation cannot be attributed to deallocation.
- Results are recorded in a FINDINGS file under bench/.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier L3-class: the precondition depends on drive physics; see docs/validation.md. Re-run only if runtime dealloc is re-enabled by default. In that case, piggyback the counters and NUSE on the next full replay. No dedicated long arm.

## Acceptance criteria

## Work log

2026-10-12: closed as moot. Under the 0407 amendment the arm reruns only if runtime dealloc becomes default-on again (0356 made it off), and its RAM prerequisite 0362 is closed won't-fix. If dealloc is re-enabled, piggyback its counters on the next full replay as the amendment says.

2026-10-01 (0361 coordinator): memtester proved uncorrected bit-12 RAM errors on lab-1/3 (bench/0361-attribution/FINDINGS.md). This rerun now also depends on 0362 (RAM remediation + clean memtester re-qualification).
