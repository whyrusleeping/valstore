---
id: "0346"
title: Delete baseline (winner-directory) AE seed mechanics after 0330 oracles pass (after 0338 lands)
status: done
priority: 2
created: "2026-09-29"
updated: "2026-09-30"
depends_on:
    - "0330"
spec_refs: []
---

## Description

Split from 0330 (owner-accepted scope). 0330 now delivers only the two stream-seed correctness oracles (exact byte-for-byte rebuilt-replica value census; populated sorted-zone rehome), because the removal edits files that hold the owner's uncommitted 0338 changes (`crates/vals-cluster/src/replica/mod.rs`, `docs/backpressure.md`, others) and cannot be committed until 0338 is committed.

Acceptance (unchanged from 0330):
- 0330's oracles pass under the stream seed (prerequisite, see 0330 outcome).
- Delete the baseline seed mechanics: the winner directory accumulator, `seed_progress.pending` / `projection_pos` / published cleanup, and the `baseline_for_test` seam.
- Delete or port the six baseline-mechanics tests listed in bench/0289-ae-seed-ab/report.md.
- Remove the `VALS_AE_SEED_MODE` env opt-out and update docs/backpressure.md.
- Preserve required legacy sidecar compatibility and the separate latency-profile winner directory; delete only the baseline seed accumulator/mechanics.
- Keep 0330's oracle tests green; run the vals-cluster debug suite and investigate failures rather than retrying until green.
- Start only once the 0338 working-tree changes are committed (or otherwise resolved) so the edits can be committed.

## Outcome

Baseline AE seed deleted (winner accumulator, pending/projection_pos/published cleanup, baseline_for_test, VALS_AE_SEED_MODE, stream_legacy, ae_seed_cleanup_* metrics). All seeding uses the streaming merge; legacy v<3 sidecar runs are materialized, sorted and reservation-charged as memory cursors, with new mixed v2/v3 exactness tests. Latency-profile winner directory untouched. Six baseline tests deleted, others ported to stream vs recovered-version references; 0330 oracles pass. Docs updated. vals-cluster debug suite (706 lib + integrations), vals-store and vals-server green (ulimit -n 65536). Standard review accepted.

Commit: refactor(0346): delete baseline winner-directory AE seed; stream seed only, legacy v2 sidecars as sorted memory cursors; drop VALS_AE_SEED_MODE, cleanup metrics and baseline tests
