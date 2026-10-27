---
id: "0219"
title: 'Cleanup Tier 3 (cluster): scan dedup, env consolidation, staged-apply retry dedup, ConnDriver RAII, shared constructors'
status: done
priority: 3
created: "2026-08-17"
updated: "2026-08-17"
depends_on:
    - "0215"
spec_refs: []
---

## Description

Split from 0215 (vals-cluster chunk). Behavior-preserving refactors:

- Scan materialization/planning dedup between `net/server/scan.rs` and `net/server/staged_scan.rs`.
- Env parsing consolidation in vals-cluster (repeated `std::env::var(..).parse()` patterns, e.g. pd/daemon.rs, node/maintenance.rs), including caching the per-request `VALS_STAGED_SCAN` env read in the net server instead of reading it on every request.
- Staged-apply retry-loop dedup: node.rs rehome path (`node/rehome.rs`) vs server Apply path (`net/server/staged_apply.rs`) share the retry/backoff shape — extract one helper covering both.
- ConnDriver inflight accounting via RAII guard instead of manual increment/decrement.
- Shared constructors for `ServerShared` (net/server/shared.rs) and `VersionedStore` (replica/mod.rs) where duplicate struct literals exist.

## Acceptance criteria

- Behavior preserved; `cargo test -p vals-cluster` (DEBUG) passes; flaky fencing/timing tests re-run in isolation before judging failure.
- No cargo fmt; no explanatory comments.

## Outcome

Landed the vals-cluster Tier-3 chunk, behavior-preserving: shared execute_generation_checked for both scan materialization paths; VALS_STAGED_SCAN cached via OnceLock in non-test builds (fresh read under cfg(test) for the runtime kill-switch test); crate-private env_parse helper replacing plain parse-with-default env sites (filtered/flag sites left bespoke); shared replica::apply_staged_with_retry unifying serve_apply and rehome_copy_apply retry loops (probe counters and error mapping preserved; device_handle() verified stable so hoisting the dev fetch is equivalent); InflightReadGuard RAII replacing ~12 manual reads_inflight pairs incl. the batch-gate Busy return; ServerShared::from_parts consolidating the two ~45-field literals. VersionedStore had a single literal — no dedup needed. cargo test -p vals-cluster all green (552+), workspace builds; reviewer accepted; backlog.md updated.

Commit: cluster: scan/apply dedup, env_parse helper, staged-scan cache, ConnDriver inflight RAII, ServerShared::from_parts
