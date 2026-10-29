---
id: "0506"
title: Stop process-global-counter tests from flaking in parallel CI runs
status: done
priority: 2
created: "2026-10-27"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

With the repo now public, `scripts/ci.sh` runs on every push and PR. On 2026-10-27 the lane failed only because `vals-cluster` `coord::range_delete_scan_cost_tests::head_of_queue_scan_cost_does_not_depend_on_the_deleted_span_three_nodes_r2` (range_delete_scan_cost_tests.rs:209, "max_tree_built_blocks_total moved during every measured scan") read a process-global counter that other tests bump in parallel; it passes alone. Other known global-gauge flakes are listed in plans/testing-notes.md.

Acceptance: these tests measure per-test state (scoped counters or serialized execution), and `scripts/ci.sh` passes repeatedly on a loaded machine.

## Outcome

The four known global-counter flakes (ram swap_counts..., pd range_metrics_warn..., corrupt_read merge_rewrite..., both range_delete_scan_cost head_of_queue tests) now rerun alone in a child process via the crate's existing isolated_env_test seam. scripts/ci.sh passed once unloaded and 3/3 beside stress -c 48 (load 54–100); at full saturation (stress -c 160) only wall-clock timing tests failed, recorded in plans/testing-notes.md and proposed as 0526.

Commit: 0506: run the four process-global-counter tests in isolated child processes (testutil::isolated_env_test); update testing notes with the rule and CPU-saturation evidence
