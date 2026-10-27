---
id: "0332"
title: Confirm 0324 skew-gated debt rehome from retained metrics (settle stability and skew convergence)
status: done
priority: 3
created: "2026-09-28"
updated: "2026-10-11"
depends_on:
    - "0324"
spec_refs: []
---

## Description
Originally the hardware half of 0324's acceptance, this task now uses retained hardware metrics rather than a new run. 0324 shipped the software policy and an offline diagnosis of the SOAK0321-F4 evidence. The policy: a debt rehome starts only when the measured node or engine load skew is above `balance_high_pct`, and then only for debt ranges touching the hottest over-threshold node or engine.

## Validation
Tier **L0**. Analyze retained PD metrics from SOAK5B, V2, V3-R2, V4 and V5-R3. No new hardware arm, destructive provision or full correctness suite is required. Missing observations may be refreshed as non-blocking piggybacks on a later planned run, not by launching a dedicated replay.

## Report
- `pd_node_load_skew_pct` and `pd_engine_load_skew_pct` through write and settle.
- `pd_rehome_debt_deferred_balanced`, `pd_rehomes_started`/`completed`/`aborted` (by cause), and `pd_unrehomed_ranges`, reported as structure only.
- Rehome mirror Busy and ACK rates from the same retained windows, if available, as context only; no F4 throughput adoption gate.

## Acceptance
- Retained evidence shows settle does not reshuffle ranges while skew is at or below the threshold.
- Observed skew above the threshold converges below it, or the reason it cannot is attributed.
- A short evidence note identifies the runs/windows and missing signals. Missing telemetry is recorded, not a reason to launch a new replay or full correctness pass.

## Validation amendment (0407, owner-approved 2026-10-08)
Tier **L0**; see docs/validation.md. This replaces the Run section. Answer from the retained PD metrics of SOAK5B, V2, V3-R2, V4 and V5-R3 (`prom-post-settle-pd.prom`, `watch.jsonl`):
- skew gauges through write and settle;
- `pd_rehome_debt_deferred_balanced`;
- `pd_rehomes_*`.

No hardware run is required. The F4 throughput criterion is dropped.

## Acceptance criteria

## Work log

- 2026-09-29 backlog triage: retained at P3; coordinate one suitable validation run with 0329/0331 rather than three unrelated replays. In addition to the retention blocker below, obtain a trustworthy hardware/health disposition (0248/0250 or qualified alternative hardware). Completion of 0336 alone is not permission to erase its evidence; require explicit release or alternative drives.

- 2026-09-29 (coordinator, unattended): BLOCKED on an owner retention decision. This run needs a fresh destructive provision of the lab scratch 9100 PRO drives (vfio-bound: lab-1 81:00.0, lab-2/3 01:00.0). Those drives currently hold the retained SHUT0326-F2-R2 data, which is the only evidence for 0336 (node1 ValuePtr.len 0x800 flip). 0336 says to decide retention before the next provision. Any one of these unblocks it: (a) the owner authorizes erasing R2, optionally after capturing node1's evidence; (b) 0336's on-drive investigation is accepted and completed first; (c) other scratch drives are provided. Then set this task back to todo.
- 2026-09-29: the owner **released the retained SHUT0326-F2-R2 drives** after 0336's read-only node1 probe; receipts are archived on devbox. The retention blocker is cleared, so destructive reprovisioning of the lab scratch drives is allowed for this run. The platform-health gate from triage is still open: the 0248/0250 disposition, now informed by 0336 FINDINGS §9 (offline Memtest86+ per box, or validated ECC SO-DIMMs on the ECC-capable 285HX SKU).
- 2026-10-06: unblocked to todo (backlog cleanup, owner-approved).
  - Drive retention is no longer a blocker: all boxes and drives were released 2026-10-01.
  - The RAM is known bad and will not be replaced (owner: no budget). Proceed on the current RAM and classify integrity events as possible hardware flips.
  - Re-scope: first try to answer this read-only from the existing raw metrics of SOAK5B / V2 / V3-R2 / V4 (`/data/dataset/task0363-results/`; rehomes at 0.10B, ~1.6B and ~3.0B): skew gauges, debt-deferred counters, and whether settle reshuffled. Only piggyback on the next full-replay run if that data isn't enough. The throughput criterion (F4 725k) is far exceeded at current rates.
- 2026-10-10: consolidated (owner-approved). Do this L0 analysis as one retained-data pass with 0393, 0380 item 3 and the 0328 census baseline: one agent parses the SOAK5B/V2/V3-R2/V4/V5-R3 prom/watch files once and writes a separate note for each task. First check which 0382/0389 gauges R3 carries (validation.md open caveat).
- 2026-10-10: **L0 answered from retained time series** (bench/0418-retained-metrics/0332-skew-rehome.md; scripts there; series under /data/dataset/task0418-results/). The note parses all 28,378 observer samples of SOAK5B, V2, V3-R2, V4 and V5-R3. The threshold is `DEFAULT_BALANCE_HIGH_PCT` = 120 (strict >) at every pin.
  - **(a) Settle does not reshuffle at or below the threshold.** In every run that had a settle, engine skew was 107–120 with 0 starts, and `debt_deferred_balanced` advanced on every poll. All 15 rehome starts across the runs had engine skew ≥121 at the gating poll, with 0 aborts.
  - **(b) Above-threshold skew converges.** Every episode above the threshold returned to ≤120, and R3 decays to 108. Convergence follows range growth, not the 3 moves per run. The long stretches above 120 with no start are attributed in code: auto-split marks the zone `actioned`, so the debt path skips it with no counter. The 30-min recovery hold after crashes also contributes. V4's 122 at abort is the recovery hold.
  - **Missing signals:** there is no counter for the split-first skip and no per-engine skew distribution. Both are recorded only; neither is a reason for a new run.
  - Acceptance met.
