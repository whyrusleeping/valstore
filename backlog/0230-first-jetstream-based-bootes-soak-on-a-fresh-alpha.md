---
id: "0230"
title: First Jetstream-based bootes soak on a fresh lab cluster
status: done
priority: 2
created: "2026-08-22"
updated: "2026-08-22"
depends_on: []
spec_refs:
    - plans/bootes-soak-runbook.md
---

## Description

Stand up a fresh 3-node SPDK cluster on the lab fleet and drive the rewritten bootes (Jetstream v2 SDK, bootes commit `1c6f56e`) into it: full-network archive replay from seq 0 with live cutover, through the valsgo `--vals` sink.

Context: bootes was rewritten 2026-08-21 to replace the indigo firehose+backfill machinery with one Jetstream stream (`plans/bootes-soak-runbook.md`, updated). Stored values/backlinks are byte-identical to prior soaks; the cursor is now the Jetstream seq under `cursor/jetstream`. Archive replay is byte-metered against the API key (env `JETSTREAM_API_KEY`, us-west host default); the SDK honors 429/Retry-After and Range-resumes, so quota exhaustion pauses rather than breaks the replay. Smoke-tested against production (live tail + authenticated bounded snapshot) on 2026-08-21.

Plan: follow the runbook end to end — sync/rebuild valsd from current HEAD (§0.5), fresh PD + format=true nodes (§1–2), create/presplit `records` + `backlinks` zones (§3; consider VALS_PD_OPAQUE_RANGE_MAPS=1 balanced placement per 0226/0227), valsgo parity preflight (§4), then launch bootes per the new §5 shape.

Open questions this run should answer:
- Does the SDK's default segment-download fanout saturate the vals sink (one ordered PutBatch lane per zone, knee ~8×1000-entry calls)? Sweep `--download-concurrency` if `residual_gap` stalls while the cluster is idle, or vice versa.
- What ingest rate does the replay sustain vs the old 300/64 CAR-backfill reference (~40k physical writes/s cluster-wide), and what does the API-key byte quota do to pacing over hours?
- Producer RSS profile (expect low single-digit GiB now that CAR spill machinery is gone).

Acceptance criteria:
- Preflight parity check passes; soak runs with 0 `write quorum failed` / `dropping batch`, 0 vals server_errors, node RAM bounded per the runbook baselines.
- `jetstream progress` logs show `residual_gap` monotonically shrinking (modulo quota pauses); if the run reaches cutover, live tail continues cleanly on the same loop.
- A bounded stop produces a clean shutdown summary (`deadline_expired=false`, `cursor saved on shutdown`), and a restart resumes from `cursor/jetstream` without a gap.
- Findings (rate curves, download-concurrency sweep if done, quota behavior, RSS) written up under `bench/` with the run logs, and the runbook amended with any corrections.

## Outcome

6h+ Jetstream replay soak on a fresh 3-node SPDK cluster, balanced placement 54/54 after post-certification zone recreate. Producer stack validated: parity preflight pass, bootes <1% CPU/3.5GB RSS, quota-paced ingest (~100k phys writes/s fresh), clean bounded stop (needs --shutdown-timeout 300s), cursor saved + gap-free resume verified. Two bootes fixes landed (oversized >1MB record filter; retry budget 6→12; bootes commit edd77ca). Cluster failures filed: 0231 (debt-rehome seeding stalls donor shards 60–100s on-core, perf captured) and 0232 (valsd double-free SIGABRT on lab-3; core capture armed). Runbook amended; findings+logs in bench/bootes-soak-jetstream-20260822-0230/. Cluster stopped, format=false, ~230M-entry corpus retained.

Commit: 0230: first Jetstream bootes soak on fresh lab; file 0231/0232; runbook + findings (sweeps in uncommitted 0229 changes)
