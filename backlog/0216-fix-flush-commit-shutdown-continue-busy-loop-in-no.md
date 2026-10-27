---
id: "0216"
title: Fix flush-commit shutdown continue busy-loop in node.rs maintenance loop
status: done
priority: 3
created: "2026-08-16"
updated: "2026-08-17"
depends_on: []
spec_refs: []
---

## Description

Survey finding: at node.rs:1721 (pre-split numbering) the flush-commit loop does `if shared.is_shutdown() { continue; }` targeting the inner commit loop, so shutdown during a perpetually Pending commit busy-loops instead of exiting. Verify intended cancellation semantics; likely change to break/return while ensuring reserved flush state is released. Behavioral fix — needs its own test.

## Outcome

Fixed the no-op `continue` in maint_loop's flush-commit Pending arm (node/maintenance.rs) to `break`, so shutdown during a paged flush commit exits promptly instead of draining every slice; safe because the manifest is already durably installed by flush_commit_start and the remaining slices are only in-memory index cleanup, and `break` still reaches finish_flush_build gauge cleanup. Added maint_loop_stops_flush_commit_cleanup_on_shutdown (node/tests.rs) driving a 12,288-key off-core flush through a real MaintHandle and asserting maint_loop exits with flush_in_flight still true; test verified to fail against the old code. node::tests (56) and maint (15) suites pass. Reviewer (sol) accepted.

Commit: node: exit flush-commit cleanup promptly on shutdown (0216)
