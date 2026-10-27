---
id: "0220"
title: 'Cleanup Tier 3 (client/bench/server): fd_* dedup, wire validation, config helpers, bench net-mode unification'
status: done
priority: 3
created: "2026-08-17"
updated: "2026-08-17"
depends_on:
    - "0215"
spec_refs: []
---

## Description

Split from 0215 (client/bench/server chunk). Behavior-preserving refactors:

- vals-client: `fd_*` wrapper dedup in `coord/write.rs` (fd_apply/fd_read/fd_digest/fd_read_mixed/fd_scan share admit/record shape) and shard-grouping dedup across coord paths.
- vals-client `wire/`: unify list-size validation for decoded collections (single bounds-check helper).
- vals-server `config.rs`: shared MiB/config parse helpers (`region_mib`, `zone_region_mib`, overrides parsing share `<zone-id>:<MiB>` shape).
- vals-bench: unify net-mode setup across bench binaries/modes and generate usage text from the dispatch table instead of a hand-maintained string.

## Acceptance criteria

- Behavior preserved; `cargo test -p vals-client -p vals-server -p vals-bench` (DEBUG) passes; bench usage output matches the dispatch table.
- No cargo fmt; no explanatory comments.

## Outcome

Landed the client/bench/server Tier-3 chunk, behavior-preserving: vals-client fd_call (future-factory, admits before constructing the transport future) + fd_call_vec unify all six fd_* wrappers with their exact ok-predicates (Busy-counts-as-ok for apply, any-success for vec paths, empty-input record skip preserved); group_by_data_shard dedups the index-grouping in apply_batch/read_mixed; wire list() now delegates to bounded_list(usize::MAX) with no bounds change. vals-server take_int helper collapses 9 take_one+parse_int-with-default sites (derived zone_region_mib default preserved). vals-bench: net_mode_args + require_spdk helpers, and a workloads! macro that emits both the dispatch match and the cfg-aware usage list from one arm list — unknown workload now prints all 21 enabled modes (was 7, stale) and exits 2. One review round: revised per findings (factory ordering, vec variant, macro single-source), then accepted. All three crates' tests green; workspace builds.

Commit: client, server, bench: fd_call helpers, shard-grouping dedup, wire list unification, take_int config helper, macro-driven bench dispatch/usage
