---
id: "0138"
title: 'Lab benchmark session: raw unsorted read/write + valsmix mixed/sorted flows'
status: done
priority: 2
created: "2026-08-06"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
Stand up a fresh 3-node SPDK cluster on the lab fleet (per plans/bootes-soak-runbook.md) and run a benchmark session:

- **A. raw unsorted** — `vals-bench netload` (write pass + read pass) then `scripts/netread-fleet.sh` for the sustained multi-process read ceiling.
- **B. mixed + sorted** — `valsmix` (vals-examples) five-flow sequence: reads under write churn and mixed read/write on the unsorted default zone; subrange scans under churn, mixed read/scan/write, and point reads on a presplit sorted zone.

Sizing: RF=2, ~20M keys/zone × 256 B (nodes cap at ~47 GiB usable; lab-3 ~30 GiB).

## Acceptance criteria
- Cluster comes up clean from current HEAD (rsync + rebuild on each box).
- Both arms run to completion; valsmix reports 0 consistency errors.
- Results + per-node metrics deltas written up under `bench/`.


## Work log

- 2026-08-07 CLOSED: the session ran and its artifacts are committed under `bench/lab-bench-20260806/` (A1 netload, A2/A3 netread-fleet, B/B2/D valsmix arms, loss probe, per-node metrics deltas, perf profiles). The "0 consistency errors" criterion was **not** met on flow 3 — that failure was the point of the run and is fully captured as follow-on tasks: 0139 (auto-split strands data — fixed), 0140 (`presplit=K` no-op for prefixed keyspaces), 0141 (durability asymmetry between single and batch writes, proposed). Re-validation of the sorted flows is task 0143.
