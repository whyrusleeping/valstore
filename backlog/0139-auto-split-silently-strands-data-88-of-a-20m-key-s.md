---
id: "0139"
title: 'Auto-split silently strands data: ~88% of a 20M-key sorted zone unreadable after 30 auto-splits during load'
status: done
priority: 1
created: "2026-08-06"
updated: "2026-08-07"
depends_on: []
spec_refs:
    - docs/zones.md#Growth = range splits
---

## Description
## Symptom

On a fresh 3-node lab SPDK cluster (RF=2, 4 shards/node), a sorted zone `smix` created with `presplit=8` was bulk-loaded with 20M keys × 256 B via `write_batch_in` at `tier=majority`. The load reported **0 write failures**, nodes reported **0 `server_errors` / 0 `busy_rejections`**, and cluster `live_keys` matched the expected ~40M for the zone (data is on the drives).

Yet afterwards only **~11% of the keys are readable**:

```
window 000005000000..000005001000: count=117    (expected 1000)
window 000009000000..000009001000: count=104
window 000015000000..000015001000: count=121
window 000019000000..000019001000: count=108
```

Both point `get` and range `scan` miss the same keys (checked with `valsq --r 2` and with `valsmix --workload scan`, which logged 98,620 `0 rows / partial rows, expected 512` consistency violations in 120 s). Reads return **not-found silently** — no transport errors, no server errors.

During the load, PD auto-split fired **30 times** (`vals_pd_range_splits 30`, default-on threshold 1M entries/range).

## Diagnosis (hypothesis to confirm)

Per `docs/zones.md`, splitting `[a, c)` at `m` keeps `[a, m)` in place but **newly places `[m, c)`** — the upper half moves to a different replica set / intra-node partition. The doc claims the anti-entropy sweep "understands the split and hands off split-orphaned keys ... so no data is stranded". Observed behaviour is that the handoff is effectively not happening at any useful rate:

- Only node 3 shows AE progress (`ae_entries_applied` 163k, `live_keys` +82k in 5 min ≈ 273 keys/s); nodes 1 and 2 show `ae_entries_applied 0`.
- At that rate a 17M-key backlog needs ~17 h, during which the moved half of every split range reads as absent.

So repeated auto-splits during a sustained write load progressively strand the moved halves, and the reader (which routes by the new range map) sees a zone that has silently lost most of its data.

## Why it matters

Auto-split is **default-on**. Any sorted zone that grows past 1M entries/range under load hits this. The failure is silent (no error counters move), which makes it far worse than a loud one — a client sees not-found for data the cluster still holds.

## Acceptance criteria
- Confirm the mechanism with the control A/B: same load with `VALS_PD_RANGE_SPLIT_THRESHOLD=0` (auto-split off) and enough `presplit=K` that no range crosses the threshold → expect 0 consistency violations. **Confirmed:** the identical no-split load passed all five flows with 0 consistency errors (`bench/lab-bench-20260806/B2-valsmix-nosplit.log`).
- Determine why the split handoff does not move the orphaned keys promptly: is the sweep's over-replication cleanup/handoff path reached at all for split-orphaned ranges, and is it strangled by the per-pass AE budgets (`AE_RECONCILE_MAX_*_PER_PASS`)?
- Fix so that a split never makes committed data unreadable: e.g. serve reads/scans for a moved sub-range from the previous owner until the handoff is confirmed (read-side fallback / fence), and/or make the post-split handoff a prioritised, unbudgeted transfer.
- Regression coverage: a test that splits a populated range and asserts every key stays readable throughout, plus a box run that repeats the 20M-key sorted load with auto-split on and ends with 0 missing keys.

## Artifacts
`bench/lab-bench-20260806/` — `B-valsmix.log` (flow 3 failure), `C-loss-probe-t0.txt`, `metrics.after-armB.*.txt`.

## Plan

## Root cause (confirmed by reading the code)

Two independent facts make a split's moved sub-range effectively unreachable:

1. **AE pull is structurally blind to it.** `VersionedStore::scoped_leaf_digests` (crates/vals-cluster/src/replica.rs:1206) scopes the Merkle exchange with `keep_group = group.contains(own) && group.contains(peer)` — only keys *both* peers currently own. After a split, the moved sub-range is owned by the new replica set but held only by the old one, so it is in no digest exchange and the new owner can never pull it. (Matches `ae_entries_applied = 0` on nodes 1 and 2.) The predicate must stay symmetric for the XOR-folded tree to compare, so this cannot simply be widened.
2. **The only remaining path — the cleanup push — is throttled to a trickle.** `cleanup_with` (antientropy.rs:428) walks the **whole store** with a cursor at `CLEANUP_MAX_KEYS_PER_PASS = 8_192` per pass, hands off at most `CLEANUP_MAX_HANDOFF_RPCS_PER_PASS = 256` keys per pass under a 250 ms wall budget, one pass per 5 s sweep (default `sweep = 5s`). That is ~51 keys/s/shard, ~200-270 keys/s/node — exactly the observed 273 keys/s. Moving 17M keys takes ~90 h, and the scan does not even know *which* keys moved.

So the doc's claim that "the sweep understands the split and hands off split-orphaned keys" is true only in principle; in practice a split during load strands the moved half for hours, silently.

## Direction (user's call: option C — zero-gap, over several sessions)

The end state is: a range's placement token is **explicit** and changing it is an explicit, confirmed, resumable *re-home* (dual-write during migration, cutover only after the copy is confirmed) — so committed data is never unreadable. That is tracked as task 0142.

**This task is step 1: make splits metadata-only, which alone stops the stranding.**

A `RangeMap` stops being "a list of boundaries whose token is implicitly the lower bound" and becomes "a list of ranges, each with an explicit placement token". `RangeMap::split(at)` gives the new upper range **its parent's token**, so *no key changes replica set or intra-node shard when a range splits* → no orphans, nothing to hand off, nothing stranded, and reads stay correct throughout. Presplit-at-creation (`from_splits`, empty zone) stays canonical (token = lower bound), so it still spreads. Auto-split stays default-on and becomes a safe, purely structural operation that prepares the boundaries the re-home protocol (0142) will later move.

## Work

1. **`crates/vals-client/src/partition.rs`** — explicit tokens.
   - Repr: boundaries plus a `tokens` vector of length `num_ranges` (`tokens[0]` for the first range, default `b""`).
   - `from_splits` stays canonical (`token[i] == lower bound of range i`); add a constructor from (splits, tokens) that **validates** the invariant and rejects otherwise: `tokens.len() == splits.len() + 1`, non-decreasing, and each token is either `b""` or an existing boundary and `<= that range's lower bound`.
   - `token_of` / `range_token` return the explicit token; `ranges_overlapping` fills the explicit token.
   - `split(at)`: the new upper range **inherits** the token of the range being split (the left half keeps its own). `merge_at(at)`: the merged range keeps the **left** range's token (unchanged from today's effective behaviour).
   - `encode`/`decode`: keep emitting the current v1 byte format when the map is canonical (so nothing changes on the wire for zones that never had an inheriting split); add a distinguishable versioned form when it is not. `decode` accepts both, stays total on junk (no panic/OOM — `crates/vals-cluster/tests/wire_fuzz.rs`), and keeps the exact-consumption check.
   - Add `is_canonical()` and a count of ranges whose token != their lower bound.
   - Tests: split inheritance leaves `replicas_for_key` **identical for every key** before/after; round-trip both encodings; v1 bytes decode canonical; invalid token vectors are rejected.

2. **`crates/vals-client/src/wire.rs`** — the map on the wire.
   - `WireRangeMap` / `OwnedRangeMap` carry tokens alongside splits (`SetRangeMaps` push and the `RangeMaps` reply). A version mismatch must fail **loudly** (decode error) — never silently drop tokens, since that would silently mis-place data.
   - `OwnedRangeStat` identifies a range by its **lower bound** (`b""` for the first range), not by its token: with inherited tokens several ranges share a token and the PD's token-keyed aggregation would collide and drive wrong split decisions. Rename the field accordingly.

3. **`crates/vals-cluster/src/net/server.rs`** — `range_stats_reply` fills the range's lower bound (it already iterates `range_bounds(i)`); `range_maps_to_wire` / the `SetRangeMaps` handler carry and validate tokens.

4. **`crates/vals-cluster/src/pd.rs`** — `range_stats_poll` and `publish_range_metrics` key ranges by lower bound; the auto-split candidate check (`map.range_index(token)` / `token_of(idx) != token`) becomes a lower-bound lookup; `PdCmd::MergeZone` keeps taking the boundary key. Add a gauge `pd_unrehomed_ranges` (ranges whose token != their lower bound) — the re-home debt 0142 will burn down. Verify auto-split still converges: each split halves a range's entry estimate, so boundary count stays ~data/threshold.

5. **Regression test** (in-process, `crates/vals-cluster/src/coord/tests.rs` or `net/tests.rs` alongside the existing sorted-zone cluster harness): populate a sorted zone, split a populated range, and assert **every key is still readable immediately after the split** (point get and scan), with no dependence on a sweep having run. This test fails on the old inheriting-free behaviour.

6. **`docs/zones.md`** — rewrite the Growth = range splits section: splits are metadata-only (token inheritance) and therefore safe by construction; explain why the old "the sweep hands off split-orphaned keys" claim did not hold (the two throttles + the AE blind spot above); state that spreading a *grown* zone now needs the re-home protocol (task 0142) and that presplit-at-creation still spreads immediately.

## Out of scope (already split out)
- 0142: the re-home protocol (dual-write + confirmed cutover) that restores auto-split's load spreading.
- 0143: the 20M-key box validation.
- 0144 (proposed): out-of-tree client parity for token-carrying maps.

## Verification
`cargo build --workspace` + `cargo test -p vals-client -p vals-cluster` (plus the wire fuzz test). Do **not** run `cargo fmt` crate-wide (rustfmt version skew — hand-format new regions only).

### Starting points
- crates/vals-client/src/partition.rs — RangeMap: splits-only repr, token_of/range_token/split/merge_at/encode/decode/even_boundaries
- crates/vals-cluster/src/replica.rs:1206 scoped_leaf_digests — keep_group = group.contains(own) && group.contains(peer) (the AE blind spot; symmetric, do not widen here)
- crates/vals-cluster/src/antientropy.rs:33-42 CLEANUP_MAX_KEYS_PER_PASS / CLEANUP_MAX_HANDOFF_RPCS_PER_PASS / CLEANUP_HANDOFF_WALL_BUDGET (the throttles)
- crates/vals-cluster/src/net/server.rs:2352 range_stats_reply — already iterates range_bounds(i), fills token_of(i)
- crates/vals-cluster/src/pd.rs:529 range_stats_poll and :649 publish_range_metrics — both key the aggregate by (zone_id, token)
- crates/vals-client/src/wire.rs:70 WireRangeMap/OwnedRangeMap/OwnedRangeStat and put_range_map/r.range_maps()
- crates/vals-cluster/src/node.rs:2084-2115 sweep placement fn uses range_map.range_token(key); cleanup_scoped_zone/reconcile_scoped_zone shard by hash64(range_token(key))

## Work log
- 2026-08-07 box validation (task 0143): Rebuilt all three lab SPDK servers and `valsmix`/`valsq` from HEAD `6cc4300`, then completed two fresh 20M-key auto-split-on arms. Arm A (exact `presplit=8` repro) ended with 46 splits, a full `valsq` count of 20,000,000, 21/21 sampled windows at 1000/1000, and 0 scan/read consistency violations. Arm B (plus seven explicit key-pattern boundaries) ended with 36 splits, count 20,000,000, 19/19 complete windows, and 0 consistency violations. This eliminates the pre-fix 104–121/1000 windows and 98,620 scan violations. Loud quorum/deadline errors still occurred under sustained churn (A read: 244; B scan/read: 17/811), but no silent missing data. Secondary finding: 0139 also made creation-time `presplit=K` inherit one placement token, so neither presplits nor later `split-zone` operations currently spread a sorted zone; the 20M/0/20M node `live_keys` and ~52.5k-key/s loads confirm the single-placement shape. This regression is tracked as task 0147. Details and raw evidence are in `bench/lab-split-fix-20260807/notes.md`. Cluster stopped after collection.
- 2026-08-07 plan: ## Root cause (confirmed by reading the code)  Two independent facts make a split's moved sub-range effectively unreachable:  1. **AE pull is structurally blind to it.** `VersionedStore::scoped_leaf_d
…[truncated]
- 2026-08-07 context hints: 7 recorded with plan
- 2026-08-07 context hints: crates/vals-client/src/partition.rs — RangeMap (splits-only repr, token_of/range_token/split/merge_at/encode/decode/even_boundaries + its tests); crates/vals-client/src/wire.rs:70 WireRangeMap/Owned
…[truncated]
- 2026-08-07 implementer report: Implemented task 0139’s metadata-only split fix.  Changes: - Added explicit per-range placement tokens to `RangeMap`, with canonical construction, validated `(splits, tokens)` construction, token in
…[truncated]
- 2026-08-07 review tier: high-powered — reviewers: sol
- 2026-08-07 review (sol): accept — The implementation satisfies task 0139's planned metadata-only split fix. RangeMap now carries validated explicit placement tokens, split inheritance preserves placement, canonical and versioned persi
…[truncated]
- 2026-08-07 revision: Addressed all four review findings.  Changes: - Updated `ARCHITECTURE.md`, `docs/algorithms.md`, `docs/multi-drive.md`, and `docs/formats.md` to describe explicit placement tokens, metadata-only split
…[truncated]
- 2026-08-07 usage: 14,665,404 tok (in 3,010,232, out 83,716, cache_r 16,152,649, cache_w 785,259) · cost n/a (unpriced)
  implementer: 9,757,467 tok (in 1,605,753, out 36,258, cache_r 8,115,456, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 4,887,692 tok (in 1,404,407, out 27,285, cache_r 3,456,000, cache_w 0) · cost n/a (unpriced)
  coordinator: 20,245 tok (in 72, out 20,173, cache_r 4,581,193, cache_w 785,259) · cost n/a (unpriced)
