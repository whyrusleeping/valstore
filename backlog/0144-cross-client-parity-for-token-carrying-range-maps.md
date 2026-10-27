---
id: "0144"
title: Cross-client parity for token-carrying range maps (valsgo / valsq)
status: done
priority: 3
created: "2026-08-07"
updated: "2026-08-08"
depends_on:
    - "0139"
spec_refs: []
---

## Description
Task 0139 changes the PD<->node wire representation of a sorted zone's range map: it now carries an explicit placement **token** per range (not just the list of split boundaries), and `RangeStats` identifies a range by its **lower bound** instead of by its token. Any out-of-tree client or tool that decodes range maps or replicates the sorted-zone routing rule (`shard = hash64(range_token(key)) % shards`) must be updated, or it will mis-route keys in a zone that has had an inheriting split.

Known candidates: the Go client (`valsgo`, used by bootes) and `valsq`. Neither is in this repo, so 0139 could not verify them.

## Acceptance criteria
- Establish which out-of-tree clients decode range maps / route sorted zones.
- Update them for the token-carrying map, or confirm they never route sorted zones and record that.
- Extend the golden routing vectors (see task 0136) with a non-canonical (inherited-token) range map so a regression is caught cross-client.

## Plan

Goal: restore cross-client parity for token-carrying range maps so valsgo (and everything on it: bootes, the interop preflight) works against valstore HEAD, and pin the non-canonical (inherited-token) behavior in the golden vectors.

Findings (already established):
- The break: post-0139, the `RangeMaps` RPC frames EVERY map as `u32 zone_id, u32 magic=0xFFFFFFFF, u32 version (2|3), list(splits), list(tokens)[, list(opt pending) if v3]` (crates/vals-client/src/wire.rs `put_range_map`/`range_map`). valsgo still parses `zone_id, split_count, splits…`, reads the magic as a count → "truncated reply" on any refresh with a sorted zone.
- valsgo's `RangeMap` (ring.go) carries only splits and routes by the range's lower bound (canonical assumption); after an inheriting split or a committed re-home this mis-places keys. Its `Overlapping` also emits the lower bound as the slice Token used for placement + scanShard.
- Rust client semantics to mirror (coord/mod.rs `replicas_for` + partition.rs): route by the COMMITTED token `tokens[range_index(key)]`; `pending` is parsed/validated but NOT used for client placement. Validation per `from_splits_tokens_and_pending`: tokens.len == splits.len+1, splits non-empty strictly ascending, each token <= its range's lower bound and (empty or exactly some split); pending target likewise valid and != current token.
- Client inventory (acceptance criterion 1): valsgo = custom Go decoder → needs the fix; valsq/valsmix/valsdump = Rust, link vals-client/vals-cluster by path → already token-aware, just verify they build against HEAD; bootes uses valsgo via go.mod replace → inherits the fix on rebuild.
- vals-examples/run-interop.sh step [4] already performs a LIVE inheriting split (`split-zone sevents k00050`, non-canonical) with Go writes on both sides — it is the exact end-to-end regression test. Reproduce the failure first, then fix, then it must pass.

Implementation (sibling repo ~/code/vals-examples — per project memory, edit via Bash if Write/Edit refuse paths outside the workspace; repo is currently clean at be2c7d2):
1. valsgo/ring.go: `RangeMap` gains `tokens [][]byte` (len = splits+1). `NewRangeMap(splits)` stays the canonical constructor (tokens = ["", splits...]). Add a wire constructor `newRangeMapWithTokens(splits, tokens, pending) (*RangeMap, error)` mirroring the Rust validation above. `RangeToken(key)` returns `tokens[rangeIndex(key)]`. `Overlapping` slice Token becomes the range's actual token (possibly inherited), Lo/Hi clipping unchanged. Update doc comments (Go file's existing comment style; valstore's no-comment rule is for Rust code).
2. valsgo wire/client (wire.go + client.go refreshFrom): decode the new per-map framing (magic 0xFFFFFFFF → version must be 2 or 3, splits list, tokens list, v3 also list of optBytes pending; trailing validation via the constructor; unknown version or invalid map → loud refresh error). Keep the legacy fallback: if the u32 after zone_id != 0xFFFFFFFF, treat it as the v1 split count (canonical). All call sites (`ranges[zid] = NewRangeMap(splits)`, dataShard, placement, Scan) flow through RangeToken/Overlapping and need no semantic change beyond the map itself.
3. Golden vectors: extend valsq `print_vectors` (vals-examples/valsq/src/main.rs) with a non-canonical section, e.g. built with the REAL Rust RangeMap: start from splits, do an inheriting `split()` and a committed re-home (`set_pending`+`commit_pending`) so tokens are inherited AND non-monotonic (like partition.rs test `middle_rehome_commit_v2_roundtrips_with_nonmonotonic_tokens`); emit splits, tokens, pending, and key→token probes covering every range. Preferably ALSO emit the exact wire bytes (hex) of a full `Response::RangeMaps` payload via the public `encode_response_payload` (if exported through the crates valsq can reach) so the Go decoder is pinned byte-exactly; if not reachable via public API, emit fields only and synthesize the framing bytes inside a Go unit test. Regenerate `valsgo/testdata/vectors.json` (`cargo run --release -p valsq -- vectors > valsgo/testdata/vectors.json`) and extend valsgo/vectors_test.go: non-canonical map RangeToken agreement (+ wire-decode agreement if hex vectors are present). Add focused Go unit tests for both framings (v1 fallback, v2, v3-with-pending) and for validation rejects.
4. Verification: (a) valsgo `go build ./... && go test ./...`; (b) `cargo build --release` in vals-examples workspace (valsq/valsmix/valsdump compile against HEAD vals-client); (c) `./run-interop.sh` — confirm it fails before the fix (truncated reply) and fully passes after, including the live-split leg; (d) valstore `cargo test -p vals-client` untouched-but-confirm.
5. valstore repo: update plans/bootes-soak-runbook.md step-4 ⚠️ block — replace "valsgo-driven soaks are blocked / use valsq" with the restored-parity guidance (valsgo at/past the fix commit; rebuild /tmp/interop). Grep docs/zones.md for the 0144 gate wording and update only statements that claim valsgo parity is missing (the re-home default-off gate list may keep referencing 0144 as satisfied-by this task; keep edits minimal and factual).
6. Commits: vals-examples gets its own git commit via Bash (message referencing 0144). valstore changes (runbook/docs + backlog state) committed via the coordinator commit tool at the end.

Out of scope (existing separate tasks): dataShard/shard-routing golden vectors (0136), epoch fencing (0150), re-home default-on decision.

### Starting points
- crates/vals-client/src/wire.rs:382-392 put_range_map (RPC framing), :910-939 range_map decode, :24-26 RANGE_MAP_WIRE_* consts
- crates/vals-client/src/partition.rs — RangeMap::from_splits_tokens_and_pending validation, valid_token, split()/set_pending/commit_pending, test middle_rehome_commit_v2_roundtrips_with_nonmonotonic_tokens
- crates/vals-client/src/coord/mod.rs:999-1028 replicas_for/range_token — clients route by committed token; pending unused for placement
- ~/code/vals-examples/valsgo/ring.go — RangeMap/RangeToken/Overlapping
- ~/code/vals-examples/valsgo/client.go:288-323 refreshFrom RangeMaps parsing; :157-180 dataShard/scanShard
- ~/code/vals-examples/valsgo/wire.go — decoder helpers (bytes/optBytes)
- ~/code/vals-examples/valsq/src/main.rs:241-321 print_vectors
- ~/code/vals-examples/valsgo/vectors_test.go + testdata/vectors.json
- ~/code/vals-examples/run-interop.sh — step [4] live inheriting split = the regression test
- Sibling repo edits: use Bash (heredoc/patch) if Write/Edit refuse ~/code/vals-examples paths
- plans/bootes-soak-runbook.md step 4 ⚠️ block to update after fix

## Work log

- 2026-08-07 severity note (PM, from reading the landed 0139 code): the compatibility break is now concrete. `RangeMap::encode` (crates/vals-client/src/partition.rs:196) emits the **v1 byte format only while the map is canonical**; the first inheriting split makes it non-canonical and it switches to a magic+version framing. An out-of-tree decoder that only knows v1 reads the magic as a split count and **fails to decode** — loud, not silent (good), but it means any valsgo/valsq client that decodes range maps will start erroring against a sorted zone as soon as it auto-splits. Sequencing consequence: this must be settled **before** the next bootes soak / corpus replay (tasks 0130, 0132), both of which drive sorted zones (`records`, `backlinks`) through valsgo with auto-split default-on.
- 2026-08-08 plan: Goal: restore cross-client parity for token-carrying range maps so valsgo (and everything on it: bootes, the interop preflight) works against valstore HEAD, and pin the non-canonical (inherited-token)
…[truncated]
- 2026-08-08 context hints: 11 recorded with plan
- 2026-08-08 context hints: crates/vals-client/src/wire.rs:382-392 put_range_map framing; :910-939 range_map decode; :24-26 RANGE_MAP_WIRE_* consts; crates/vals-client/src/partition.rs from_splits_tokens_and_pending + valid_toke
…[truncated]
- 2026-08-08 preload: 5 file(s), ~15 KiB seeded into implementer context
- 2026-08-08 implementer report: Implemented task 0144.  Changes: - valsgo now decodes legacy v1 and current magic-framed map-v2/map-v3 RangeMaps replies, rejects unknown versions/invalid option tags/map shapes, and validates splits,
…[truncated]
- 2026-08-08 review tier: single-opus — reviewers: sol
- 2026-08-08 review (sol): accept — The change satisfies task 0144. valsgo now decodes legacy, map-v2, and map-v3 range-map framings; mirrors Rust validation; routes point operations, shard selection, and overlapping scans by committed 
…[truncated]
- 2026-08-08 decision: accept — commit: 0144: cross-client parity for token-carrying range maps — valsgo (vals-examples 6c7aa2a) now decodes legacy v1 and magic-framed map-v2/v3 RangeMaps replies with Rust-equivalent validation, carries c
…[truncated]
- 2026-08-08 usage: 3,204,390 tok (in 1,553,758, out 46,280, cache_r 4,023,645, cache_w 177,461) · cost n/a (unpriced)
  implementer: 1,666,251 tok (in 487,774, out 17,517, cache_r 1,160,960, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,515,816 tok (in 1,065,924, out 6,500, cache_r 443,392, cache_w 0) · cost n/a (unpriced)
  coordinator: 22,323 tok (in 60, out 22,263, cache_r 2,419,293, cache_w 177,461) · cost n/a (unpriced)
