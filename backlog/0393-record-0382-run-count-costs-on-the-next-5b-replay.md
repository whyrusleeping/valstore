---
id: "0393"
title: 'Record 0382 run-count costs from retained R3 artifacts: runs/engine, stranded runs, Get p50/p99, shadow-refresh shard time'
status: done
priority: 3
created: "2026-10-06"
updated: "2026-10-11"
depends_on:
    - "0382"
spec_refs: []
---

## Description
Split from 0382's hardware-number follow-up. The code changes and microbench landed in 0382; retained R3 now supplies the first record, without waiting for a new 0386 replay.

Tier **L0**. From retained SOAK5B-V5-R3 artifacts, record and compare against the 0382 microbench. No new replay, census or read arm is required:

- runs per engine over time (and frozen + stranded run counts per engine — 0382 gauges);
- Get p50/p99 (client and server side), bloom probes and run blocks read per GET (attribution counters);
- shadow-refresh shard time (0389 counters `SHADOW_WALL`/`SHADOW_CPU`, hits vs generation recomputes) per flush;
- inner-window merge count (now counted at commit).

**Acceptance:** a short bench/ note with the available numbers, whether the 0382 microbench predictions held, and an explicit list of missing or unusable metrics. Inspect the full observer scrapes as well as the filtered samples before declaring a gauge absent. Refresh missing signals only as non-blocking piggybacks on an already-planned run; no dedicated hardware arm is required for this record. File follow-ups if observed costs grow superlinearly.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier **L0**; see docs/validation.md. Record from the SOAK5B-V5-R3 artifacts:
- runs per engine and frozen/stranded gauges;
- Get p50/p99 from samplecheck;
- SHADOW_* counters;
- inner-window merges.

Refresh from a later replay only for gauges absent at pin 8736b2b1. The dependency on 0386 is no longer needed for the first record.

## Acceptance criteria

## Work log

- 2026-10-10: consolidated (owner-approved). Do this as one retained-data pass with 0332, 0380 item 3 and the 0328 census baseline: one agent parses the SOAK5B/V2/V3-R2/V4/V5-R3 prom/watch files once and writes a separate note for each task. First check which 0382/0389 gauges R3 carries (validation.md open caveat).
- 2026-10-10: **L0 record from R3** (bench/0418-retained-metrics/0393-run-count-costs.md; R3 gauge inventory in GAUGES.md).
  - **Runs per engine:** 4.4 → 27.7 mean (max 39) at 10B, linear at ≈2.5 per 1B ACK. Frozen runs reach 21.8 mean (79% of resident), stranded ≤2.9 mean (max 10). The 100-run microbench regime never occurred.
  - **Shadow refresh:** about 2 ms per recompute after the 60%-free gate (≈5.0B), with ~1k bloom probes per recompute, confirming the incremental path. But there are ~32 recomputes per flush, so 60–87 ms per flush, which is 0.37–0.48% of shard wall. That is consistent with 0391's ≤0.6% ceiling. Cost grew sublinearly (×1.19 for ×1.39 runs), so no follow-up is filed.
  - **Inner-window merges:** 31 of 8,019 commits.
  - **GET hits:** 27.1 bloom probes and 1.29 run blocks per GET, matching 0382's "hits unchanged" and 0394.
  - **Missing or unusable:** Get p50/p99 is unobservable at this pin. The R3 samplecheck timed out with empty output and has no latency schema, and the server has no GET histogram. `vals_get_ops` is absent, and the miss path is unobserved.
  - Acceptance met: available numbers recorded, predictions checked, and missing metrics listed.
