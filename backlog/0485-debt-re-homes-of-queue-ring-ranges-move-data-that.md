---
id: "0485"
title: Debt re-homes of queue/ring ranges move data that is range-deleted within minutes; their stray copies are the remaining ring-arm RSS growth
status: done
priority: 2
created: "2026-10-17"
updated: "2026-10-19"
depends_on:
    - "0477"
spec_refs: []
---

## Description

## Description
## Found by
0477 hardware arms on lab: the 0471 Q2 ring workload (500k msgs/s, 64 partitions × 1M live, RF 3, 2 h). Data is in bench/0477-covered-merges/FINDINGS.md.

## Observed
- **R9, default config:** about 61 debt re-homes in 2 h, each moving a 0.6M–1M-entry ring range whose data is trimmed within about 2 min. `vals_live_keys` per node grew from 72M to 104M against 64M real. That is stray former-owner copies that cleanup has not yet forgotten (0480): it forgot only 2–4M keys per node. RSS grew from 1.3 to 2.3 GB, about 9 MB/min, and run entries from 99M to 170M.
- **R12, same code with `VALS_PD_REHOME_ENABLED=0`:** `vals_live_keys` stayed at exactly 64M. Run entries fell to 84M and RSS stayed flat at 1.1–1.25 GB. Throughput (500k), verify (exact) and latency were as good or better: trim p99 4.2 ms against 8.1 ms, probe p99 4.1 ms against 24 ms.
- The strays also inflate the PD's max-over-shards range counts, which drive size splits and load projections. 0477 had to add a split hysteresis to covered merges for this reason.

## Options (to decide)
- (a) Exclude a range from debt re-homes when its data is being deleted behind a head. For example: its left neighbour is covered-empty, or the zone has had covered merges recently. This may be too broad for long-retention logs.
- (b) Make debt re-homes of inherited children wait until the range is older than some age or has stopped shrinking.
- (c) Make cleanup forget re-homed copies fast enough that strays don't accumulate (0480).
- (d) A per-zone queue profile with no debt re-homes (0477's reserve option).

## Accepted scope (owner, 2026-10-18)
Diagnose the re-home decisions before choosing a policy, using existing runs and 0477's current-tree default-config baseline. Determine whether genuine owner load stays imbalanced long enough to justify moves, whether transient counts or former-owner copies inflate the imbalance, and how soon moved data is deleted. Capture missing decision inputs in the baseline. Implement only the smallest supported fix: distinguish logical owner load from physical cleanup debt if counts are misleading, or test persistence if imbalance is transient; a retention-aware rule requires further evidence. Do not implement all candidate rules together or broadly exempt recently trimmed zones. Global re-home disablement remains an experimental control, not the default solution. Faster cleanup (0480) remains proposed and separate.

## Acceptance criteria
- A 2 h ring arm with default PD settings keeps `vals_live_keys` within a few percent of the real 64M live count; RSS and disk usage plateau, with exact final verification and zero probe violations.
- Throughput and latency do not regress; use comparable builds and paired measurements where needed to attribute changes.
- A deliberately imbalanced long-retention or append-only control still balances successfully, with focused tests for any changed policy.

## Acceptance criteria

- A 2 h ring arm with default PD settings keeps `vals_live_keys` within a few percent of the real 64M live count; RSS and disk usage plateau, with exact final verification and zero probe violations.
- Throughput and latency do not regress; use comparable builds and paired measurements where needed to attribute changes.
- A deliberately imbalanced long-retention or append-only control still balances successfully, with focused tests for any changed policy.

## Outcome

Debt re-home admission uses logical owner load (owner-shard, accounting-ready reports only). Diagnosis and R15/C1 hardware results in bench/0485-rehome-decisions/FINDINGS.md: R15 default-config 2 h ring arm had zero re-homes, live keys 64.04M flat, RSS ~1.0–1.1 GB flat, disk flat, exact verify, 0 probe violations, trim p99 4.03 ms and probe p99 4.46 ms vs R14 5.31/9.70; the C1 append-only control balanced from 800% to 119.7% with 35 re-homes and no aborts. Focused test debt_rehome_load_counts_only_exact_owner_reports.

Commit: fix(pd): admit debt re-homes on exact owner load, not unready estimates or former-owner copies (0485)
