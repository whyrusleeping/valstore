---
id: "0136"
title: Pin cross-client shard-routing parity in the golden vectors (valsq vectors → valsgo test)
status: done
priority: 3
created: "2026-08-06"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - docs/README.md
---

## Description
Last open acceptance item from task 0106 (valsgo↔Rust placement divergence on split sorted-zone ranges), which is otherwise fixed and validated live.

The 0106 root cause was valsgo keying connections by node only and always dialing the bootstrap (shard-0) address, so sorted-zone keys landed on shard 0 while the Rust client read the canonical owner shard (`hash64(range_token) % shards`). The fix shipped in vals-examples (`valsgo: intra-node shard routing for multi-shard nodes`, 35e7951) and was validated end-to-end on the lab cluster, but nothing in CI would catch a re-divergence: `valsgo/testdata/vectors.json` only carries `hash64`, `ring` and `range_tokens` sections — there is no intra-node shard-routing vector.

Work:
- Extend `valsq vectors` (Rust, the generator of record) with a `shards` section: for a set of (zone kind, shard count, key / range token) cases, emit the expected `data_shard` / `scan_shard` index.
- Regenerate `valsgo/testdata/vectors.json` and add `TestShardRoutingMatchesRust` in `valsgo/vectors_test.go` covering multi-shard nodes, sorted zones with splits (non-empty range tokens) and unsorted zones.
- Verify the test fails against the pre-fix routing (kill-the-fix check).

Acceptance criteria:
- Golden vectors include shard routing; Go test asserts it and is red if either side's routing changes.
- README note on regenerating the vectors stays accurate.

## Acceptance criteria

## Plan

Goal: pin intra-node shard routing (docs/algorithms.md §3) in the cross-language golden vectors so a re-divergence like 0106 goes red in `go test`.

Two repos are involved: this workspace (valstore, aka `~/code/vals` symlink) provides the Rust routing of record; `~/code/vals-examples` holds valsq (generator) and valsgo (Go client + vectors test). NOTE: Edit/Write tools are confined to the valstore workspace — all file changes under ~/code/vals-examples must be made via Bash (heredoc `cat > file` or `patch`).

1. valstore — expose the real routing as pure functions so the generator uses the actual Rust implementation, not a restated formula:
   - In `crates/vals-client/src/partition.rs` add:
     - `pub fn data_shard(map: Option<&RangeMap>, key: &[u8], shards: usize) -> usize` — shards<=1 → 0; Some(map) → hash64(map.range_token(key)) % shards (sorted zone); None → hash64(key) % shards (hash/default zone).
     - `pub fn scan_shard(map: Option<&RangeMap>, lo: Option<&[u8]>, shards: usize) -> usize` — shards<=1 → 0; Some(map) → hash64(map.range_token(lo.unwrap_or(b""))) % shards; None → 0.
     Doc comments referencing docs/algorithms.md §3 (project rule: no explanatory inline comments; doc comments/doctests OK).
   - Refactor `crates/vals-client/src/net/client.rs` `data_shard`/`scan_shard` (lines ~478-514) to delegate to these (behavior identical; `shard_of` may fold away if now unused).
   - Re-export: add both fns to `pub use partition::{...}` in `crates/vals-client/src/lib.rs` AND `crates/vals-cluster/src/lib.rs` so valsq (which deps on vals-cluster) can call them.
   - Verify: `cargo build -p vals-cluster && cargo test -p vals-client` (debug), plus a quick `cargo test -p vals-cluster` sanity if time allows.

2. vals-examples/valsq — extend `print_vectors()` (valsq/src/main.rs) with a `"shards"` section computed by calling the new `vals_cluster::partition::{data_shard, scan_shard}` (import via whatever path the re-export lands at). Coverage required:
   - shard counts including 1 (degenerate → 0) and at least two multi-shard counts (e.g. 4 and 5, so % differs);
   - hash/unsorted zone routing (map=None) over the existing key set;
   - sorted zone routing using the existing 3-split map (splits f,m,s) with probes hitting the empty-token range AND non-empty tokens (f/m/s ranges);
   - scan_shard for lo probes (including lo="" / first range).
   Keep the hand-rolled JSON printing style consistent with the rest of the file.

3. vals-examples/valsgo:
   - Regenerate `valsgo/testdata/vectors.json` via `cargo run -p valsq -- vectors > valsgo/testdata/vectors.json` (run in ~/code/vals-examples); confirm only the new section is added (diff).
   - Add `TestShardRoutingMatchesRust` to `valsgo/vectors_test.go` (package valsgo, so private API is reachable): build a `Client` literal with `shardAddrs` faking a node with N per-shard addresses per vector count, then assert `dataShard` (with a sorted `zoneInfo` + `RangeMap` for sorted cases, hash zoneInfo for unsorted) and `scanShard` (token from `NewRangeMap(splits).RangeToken(lo)`, mirroring the real scan path) match the vectors. Check zoneInfo's actual field/ordering encoding in client.go before writing.
   - `go test ./...` in valsgo must pass.
   - Kill-the-fix check: temporarily revert routing to the pre-0106 behavior (make dataShard always return 0) and confirm the new test fails; then restore. Record the observed failure in the report.

4. README (vals-examples): confirm the vectors-regeneration note (~line 97) still accurate; extend the pinned-algorithms list to mention shard routing if it enumerates them.

Commits (coordinator will handle): valstore via commit tool; vals-examples committed manually with git (tree is currently clean — leave changes staged-ready).

### Starting points
- crates/vals-client/src/net/client.rs:478-514 (data_shard/scan_shard/shard_of of record)
- crates/vals-client/src/partition.rs (RangeMap home; add pure fns here)
- crates/vals-client/src/lib.rs:70 + crates/vals-cluster/src/lib.rs:88 (partition re-export lists)
- ~/code/vals-examples/valsq/src/main.rs:241-377 (print_vectors, hand-rolled JSON)
- ~/code/vals-examples/valsgo/vectors_test.go + client.go:150-180 (dataShard/scanShard, shardAddrs)
- docs/algorithms.md §3 'Per-node shard routing'
- REMINDER: Edit/Write refuse paths outside valstore — use Bash heredocs for vals-examples files
- Do NOT cargo fmt; tree is not rustfmt-clean

## Work log
- 2026-08-10 plan: Goal: pin intra-node shard routing (docs/algorithms.md §3) in the cross-language golden vectors so a re-divergence like 0106 goes red in `go test`.  Two repos are involved: this workspace (valstore, 
…[truncated]
- 2026-08-10 context hints: 8 recorded with plan
- 2026-08-10 context hints: crates/vals-client/src/net/client.rs:478-514 (data_shard/scan_shard/shard_of of record); crates/vals-client/src/partition.rs (RangeMap home; add pure fns here; use crate::ring::hash64); crates/vals-cl
…[truncated]
- 2026-08-10 preload: 4 file(s), ~15 KiB seeded into implementer context
- 2026-08-10 review tier: single-opus — reviewers: sol
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The change correctly pins intra-node shard routing in the cross-language golden vectors. The Rust pure functions in partition.rs are exact behavior-preserving extractions of the NetTransport routing (
…[truncated]
- 2026-08-10 decision: accept — commit: 0136: pin cross-client shard routing in golden vectors — extract pure partition::{data_shard, scan_shard} (docs/algorithms.md §3), delegate net client routing to them, re-export via vals-client/val
…[truncated]
- 2026-08-10 usage: 44,314 tok (in 136, out 44,178, cache_r 3,052,744, cache_w 104,299) · cost n/a (unpriced)
  coordinator: 29,305 tok (in 86, out 29,219, cache_r 2,415,998, cache_w 67,373) · cost n/a (unpriced)
  reviewer:claude: 15,009 tok (in 50, out 14,959, cache_r 636,746, cache_w 36,926) · cost n/a (unpriced)
