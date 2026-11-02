---
id: "0555"
title: Try feasible aged value-log GC candidates instead of stopping at the highest dead-byte estimate
status: done
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description

0548 source review, not yet a new executable regression: vals-store/src/engine/merge.rs:2232-2258 selects one candidate by gross estimated dead bytes before excluding shared segments or checking victim rewrite headroom/net gain. aged_vlog_gc_shape:2273-2304 evaluates only it and returns None on any failure. Thus an infeasible highest-scored run can repeatedly hide a feasible second candidate. aged_vlog_gc_feasible uses the same path and can misreport available reclaim. Default-on VALS_VLOG_GC; most consequential under pressure when suffix/fold fallback cannot help. Suggested acceptance: deterministic two-candidate regression (highest-scored has only shared victims or unaffordable output; lower-ranked candidate has a gainful affordable rewrite), choose feasible work without unbounded on-core sorting/all-pairs overhead, check feasibility/terminal detection and bounded planner costs. No claim that a production stall was reproduced.

## Outcome

Aged value-log GC now picks the highest-scored run with a gainful, affordable, unshared victim rewrite (single pass, no sort, shared refs built once), so an infeasible top run no longer hides a feasible one in planning or terminal detection; deterministic two-run floor regression (fails on old behavior), vals-store lib+integration tests and clippy -D warnings pass; docs/backpressure.md updated. No production stall was reproduced.

Commit: vlog GC: try feasible lower-scored aged runs instead of stopping at the top dead-byte estimate (0555)
