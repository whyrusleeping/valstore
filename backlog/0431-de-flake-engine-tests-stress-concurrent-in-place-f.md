---
id: "0431"
title: De-flake engine::tests::stress::concurrent_in_place_forced_windows_never_corrupt_merge_outputs (pressure merge-coverage assertion, ~1 in 6 debug runs)
status: done
priority: 4
created: "2026-10-11"
updated: "2026-10-14"
depends_on: []
spec_refs: []
---

## Description

Source: memory note m-dm2ckr7fezmx ("vals-store engine::tests::stress::concurrent_in_place_forced_windows_never_corrupt_merge_outputs flakes (~1 in 6, debug) on its `pressure` merge-coverage assertion in the 2026-10-11 dirty tree, with or without 0387's change; rerun before blaming a diff") and backlog 0387 ("2/13 runs with this change, 2/12 with the early-stop check knocked out. Any merge failure panics that harness, so the new branch never runs there").

The test is at crates/vals-store/src/engine/tests/stress.rs:488. It is the only record of this flake; no other task tracks it.

Acceptance:
- Find out why the `pressure` merge-coverage assertion sometimes fails (timing-dependent coverage, or a real merge-scheduling gap), and state which it is.
- If it is a test-only timing issue: make the assertion deterministic without weakening the corruption checks. If it is an engine defect: fix it, with a test that reproduces it.
- 30 consecutive debug runs pass on the integrated tree, after the in_review hunks that touch engine merge code (0366/0371/0372/0387) are committed.
- Retire memory note m-dm2ckr7fezmx (or its merged successor clause) once the test is fixed.

## Outcome

Cause: test-only timing-dependent coverage, not a merge-scheduling gap. Only seed 1 reaches the pressure gate; it yielded 0–5 Pressure plans depending on when off-core results were committed (wall-clock try_recv), which shifted flush counts and free/dead state at planning time (2/24 baseline runs had 0). All runs had failed_puts 0 and pressure-path vlog_gc/shadow merges, so nothing stalled. Also StressDev::sibling advanced the deferred-write RNG from worker threads. Fix (stress.rs only): each job gets a seeded commit op (flush +5..205, merge +20..820, matching measured lags); main loop blocks for it there; sibling uses a fixed seed. Workers stay concurrent; corruption checks unchanged. Totals identical every run (pressure 2). 30/30 consecutive debug runs pass; full vals-store lib suite passes; rogue detection test still catches both faults. Memory m-dm2ckr7fezmx/m-dm2smaz9p2cb superseded by m-dm444fkoix4o.

Commit: test(0431): stress harness commits flush/merge lane results at seeded ops, so concurrent_in_place_forced_windows_never_corrupt_merge_outputs is deterministic per seed (pressure flake was timing-dependent coverage, not an engine gap)
