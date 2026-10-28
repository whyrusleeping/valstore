---
id: "0506"
title: Stop process-global-counter tests from flaking in parallel CI runs
status: proposed
priority: 2
created: "2026-10-27"
updated: "2026-10-27"
depends_on: []
spec_refs: []
---

## Description
With the repo now public, `scripts/ci.sh` runs on every push and PR. On 2026-10-27 the lane failed only because `vals-cluster` `coord::range_delete_scan_cost_tests::head_of_queue_scan_cost_does_not_depend_on_the_deleted_span_three_nodes_r2` (range_delete_scan_cost_tests.rs:209, "max_tree_built_blocks_total moved during every measured scan") read a process-global counter that other tests bump in parallel; it passes alone. Other known global-gauge flakes are listed in plans/testing-notes.md.

Acceptance: these tests measure per-test state (scoped counters or serialized execution), and `scripts/ci.sh` passes repeatedly on a loaded machine.

## Acceptance criteria

## Work log
