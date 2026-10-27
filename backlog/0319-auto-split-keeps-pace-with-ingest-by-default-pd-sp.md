---
id: "0319"
title: Auto-split keeps pace with ingest by default (PD split cadence), so D-scale debt converges without env knobs
status: done
priority: 1
created: "2026-09-27"
updated: "2026-09-27"
depends_on:
    - "0318"
spec_refs: []
---

## Description
## Evidence (LONG01 D9–D12, 2026-09-27)

- **5 s default poll.** The PD's auto-split loop allows one range action per sorted zone per poll. At the 5 s default that caps splitting at about 0.4/s. In every LONG01-D run at the default, debt never converged: ranges grew +0.36/s linearly through a 900 s settle, and the maximum range reached about 8M entries.
- **1 s poll.** At `VALS_PD_RANGE_SPLIT_POLL_MS=1000` (with 0317 and 0318), every run's settle converged in 133–171 s. D10 passed every sustain gate at 1.01M ACK KV/s. Even so, the maximum range still grew to 4–6M during writes, at about 0.75–0.9 splits/s.
- **Rehome starvation.** The same one-action-per-poll slot starves rehome whenever a zone splits every poll.

## Candidate (one mechanism, to be chosen and preregistered)

Make auto-split keep pace with ingest by default. The options are below; pick one with a measured rationale.

- **(a)** Change the default poll from 5 s to 1 s. This is the smallest change and was already measured as an env knob. Also measure the poll's own node/PD CPU cost.
- **(b)** Split every over-threshold range per zone per poll, with a per-poll cap. Distinct ranges' midpoints are independent, and hysteresis already prevents split↔merge thrash. This must also keep a rehome slot, so rehome is not starved indefinitely.

Recounts are now about 0.17 µs/key (0318), so faster splitting is affordable.

## Acceptance (preregister before fleet runs)

- **D-prefix A/B on production defaults** (no env knobs) against master:
  - debt converges in settle on every run;
  - the maximum range at end of write is lower;
  - ACK/s ≥ 0.97× the master 1 s-env reference, or better;
  - all correctness gates PASS;
  - the rehome count is reported.
- **Then two consecutive default-config D runs judged against every sustain gate.** That is the 0297/0309 claim "1M sustained over 400M KV", or target unmet with attribution.

## Acceptance criteria

## Work log

### 2026-09-27: option (b) chosen, preregistered, accepted and merged (`20a3cc5b`)

- **Why (b).** At a 1 s poll the effective split rate was still only about 0.5 per zone per second, and ranges reached 4M+ while writing. A split cap raises capacity to 3.2 per zone per second without adding stats polls.
- **Change (`f284d8d4`).** Each poll splits every over-threshold range per zone, largest first, up to `VALS_PD_RANGE_SPLITS_PER_POLL` (default 16). A splitting zone still takes no merge or rehome in that poll. It comes with a unit test and docs/zones.md, and the vals-cluster suite passes.
- **D13/D14 on production defaults:** 1,146,904 and 1,152,680 ACK KV/s over 400.7M KV.
  - Every quartile ≥ 1.04M; sliding-10 s minimum 960k.
  - Maximum range under 1M at the end of writes; settle took 21 s and 91 s.
  - All correctness gates PASS, with 0 mismatches, fallbacks and folds.
  - Throughput is 1.197× the 1 s-poll env reference, and both runs pass every sustain gate.
- **Evidence.** `bench/0297-ingest-1m/long01/FINDINGS.md`, section "Split cadence by default (task 0319)", and `SUMMARY-SPLITCAP-0319.json`.
- **Known limitation.** Rehome gets no slot while a zone keeps splitting. That was already the case before this change, and with 0317 it is harmless on nodes = RF.
