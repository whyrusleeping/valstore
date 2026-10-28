---
id: "0521"
title: A file-backed zone at its reserve floor stays Busy after add-node hands off half its keys
status: proposed
priority: 4
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs: []
---

## Description
Found while verifying the growth path (0509), local smoke test, file backend, gc_grace = 0s.

A one-node RF 1 cluster with `region_mib = 16` was filled with 120 KB values in the default zone: after 65 writes (~7.8 MB) every write and delete to the zone got retryable Busy (`vals_free_segments 5`, maintenance logging `compaction needs ~7 free segments to hold the copied live data but only 5 are free; refusing to start`). Adding a second node with `add-node` made cleanup hand off and forget 34 of the 65 keys (`vals_live_keys` 65 -> 31), but over the next ~3 minutes node 1's free segments stayed at 5, `compaction_plan_failures` kept rising, no merge ran, and writes routed to node 1 kept failing. Reads worked throughout. Backup plus restore into a 64 MiB region recovered it.

Open questions: is this only a tiny-region artifact (reserve capped at half the region), or can a realistically sized zone also stay wedged after its live data shrinks? Should forgetting handed-off keys let the planner pick a cheaper plan (the copied live data is now about half)?

Acceptance: reproduce at a realistic region size; either fix reclaim so a zone whose live data shrinks leaves the floor, or document why not. Update docs/growing.md ("When a zone fills").

## Acceptance criteria

## Work log
