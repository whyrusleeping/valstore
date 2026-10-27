---
id: "0286"
title: Isolate process-global probe-counter assertions in staged Apply tests
status: done
priority: 4
created: "2026-09-08"
updated: "2026-09-11"
depends_on: []
spec_refs: []
---

## Description

Fresh read-only review of 0278 observed pre-existing `net::server::staged_apply::cold_overwrites_count_probe_runs_and_blocks` fail 2/4 default-parallel net-suite runs, pass 3/3 isolated and pass the full cluster suite with --test-threads 4. The test asserts process-global observe::snapshot() apply_probe_runs/apply_probe_blocks deltas that concurrent tests can increment. staged_apply.rs is untouched by 0278; this is not a client-budget acceptance blocker.

Proposed scope: reproduce and isolate these assertions using a test-local/per-store seam or a genuinely shared serialization guard, without weakening probe regression coverage or changing production metrics semantics. Repeated default-parallel cluster/net suites should pass and the test should still fail when the probe behavior under test is broken. Investigate before assuming the review's attribution is the complete root cause.

## Outcome

Reproduced the global snapshot race and replaced test deltas with a cfg(test) thread-local hook over the batch's own probe counts; production metrics and all assertions unchanged. Independent review accepted. Verification: 10/10 parallel net suites, 3/3 full cluster-library suites (624 passed, 5 ignored each), isolated test, negative mutation, cargo check, diff check, and coordinator 63/63 server rerun passed. Unrelated 0279/0287 changes preserved.

Commit: Isolate staged Apply probe regression counters in parallel tests
