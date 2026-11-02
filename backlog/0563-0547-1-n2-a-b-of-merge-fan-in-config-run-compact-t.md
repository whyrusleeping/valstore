---
id: "0563"
title: '0547.1: N2 A/B of merge fan-in config (run_compact_threshold is a no-op; tiered K/min-K/valve) for fewer merge-rewrite bytes'
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

Child of 0547 (item 1, merge threshold, config only).

Code reading at 0c92a7e: `run_compact_threshold` only decides whether a maintenance tick asks for a plan; a recurring plan is chosen solely by `select_recurring_merge` (tier K=6, min K=4, valve 6, ratio 1.5, 2 GiB input/freeze cap). The N2 baseline holds 28–50 resident runs per engine, above 8 and 16, so 8 → 12/16 changes nothing. The effective config lever for merge-rewrite bytes is the tier fan-in (`VALS_MERGE_TIER_K`, `VALS_MERGE_TIER_MIN_K`, `VALS_MERGE_FORCE_RUNS`).

N2 baseline node1 at 4.52B (SOAK5B-V5-N2): 522 device B/write = vlog 298 + flush runs 98 + merge runs 125 + catalog/other 5; merge output 1.28x flush output; ~124 MB per flush run.

Acceptance:
- Record why run_compact_threshold is not a lever (code + baseline run counts).
- One N2 arm (config only, same build as the baseline) with a higher tier fan-in; measured device B/write by source, active ingest seconds to 4.5B vs 4,290 s, apply/read probe counts per write, key-filter and run-metadata RAM, resident runs; keep (change the default, docs, tests) or reject with numbers.

## Outcome

run_compact_threshold only gates the plan call (recurring selection is select_recurring_merge; the baseline holds 28–50 runs per engine), so it is not a lever. Config-only N2 arm with tier K / min K / valve = 16: R3 hit a hardware-consistent heap-corruption crash with a PCIe AER storm on alphac-1 at 3.6B (−6.5% to 3.5B, PASS). Replicate R4: PASS, 0 crashes, 4,067 s vs 4,290 s to 4.5B (−5.2%), no 0.5B range below −0.3%. Device B/write 522→470 (node1) and 543→504 (node3); merge-run B/write −42%/−27%; merge CPU −41%/−14%; apply probes −24%/−38%. Resident runs and key-filter RAM not higher; node1 peak RSS +2.5 GB. Adopted as the default with docs; mechanism tests pin the old small fan-in. ci.sh green.

Commit: 0563: merge fan-in default K = min K = valve = 16 (was 6/4/6); N2 replay to 4.5B 4,067 s vs 4,290 s (-5.2%), device B/write -10.1%/-7.0%, merge-run B/write -42%/-27%; run_compact_threshold shown not a lever; 0547 split into 0563/0564/0565
