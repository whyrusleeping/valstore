---
id: "0409"
title: Wire the 0402 preregistered disk-stop rule (disk_stop.DiskStop) into run_full1.py in place of capacity_incidents for 0386's next attempt
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-12"
depends_on:
    - "0402"
    - "0403"
spec_refs: []
---

## Description

0402 preregistered a new disk planned-stop rule for 0386's next attempt: bench/0363-compact-values/disk_stop.py, specified in SOAK5B-V5-R3-DISKSTOP-0402.md under "Preregistration". The rule:
- stop if a shard stays below 10% free for 1200 s;
- stop immediately below 3% free;
- stop immediately when the floor-only Busy counter rises. That counter is `vals_busy_rejections - vals_admit_shed_rejections`, read from the same scrape.

run_full1.py was deliberately left untouched because it carries uncommitted 0403/0406 work. Wire the rule in once that work lands.

## Outcome

SOAK5B runners keep one DiskStop across watch ticks. Every tick and host feeds update(host, t, rows|None, floor_busy), with floor_busy = busy - admit_shed from the same scrape. A stop calls request_stop("disk_full") with the reason, incidents and parameters. Non-SOAK5B arms are unchanged. Parameters are recorded in soak-params.json (watch + policy text), disk-stop.json (initial preflight) and the plan. On resume the window is rebuilt from the saved watch.jsonl zone-stats. The Busy baseline is not carried and the downtime gap resets the windows. New tests: recovery before W gives no stop; the sustained, 3% and Busy cases stop; rebuild; R3 replay. The suites are green. R3 replay: 2859 snapshots, 0 stops. R3's continuation amendment must also cover the policy change (documented). First live use: 0418.

Commit: feat(0409): wire the 0402 preregistered disk stop (disk_stop.DiskStop) into run_full1 SOAK5B watch in place of capacity_incidents; params/policy/preflight receipts; resume rebuilds the window from saved zone-stats without carried continuity

## Hardware confirmation (0418, 2026-10-12)

- First live use in 0418 (bench/0363-compact-values/R3-SESSION-0418.md). `disk-stop.json` carries the rebuild receipt (2,868 ticks, 0 replayed stops). The live decider ran on all 1,253 ticks. `floor_busy` was 0 on every host and tick, and the lowest per-shard free fraction was 28.7%. No stop fired. **Confirmed**; the rule never came near its thresholds.
