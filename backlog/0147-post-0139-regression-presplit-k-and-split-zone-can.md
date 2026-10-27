---
id: "0147"
title: 'Post-0139 regression: presplit=K and split-zone cannot spread a sorted zone at all (all splits inherit the parent token)'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-07"
depends_on: []
spec_refs: []
---

## Description
## Symptom (measured on the box, task 0143, 2026-08-07)

On a fresh 3-node lab cluster with `create-zone smix 2 sorted presplit=8`, PD reported **8 sorted ranges but 7 unrehomed ranges immediately after creation, before a single key was written**, and the subsequent 20M-key load put **all** of the data on one replica pair:

```
vals_pd_sorted_ranges 8      vals_pd_unrehomed_ranges 7     (pre-load, arm A)
live_keys: node1 20,000,000  node2 0  node3 20,000,000      (RF=2 → one replica pair)
bulk load: 52,810 keys/s     (vs 213,996 keys/s for the same load on a spread zone, 2026-08-06 arm D)
```

Adding seven explicit `admin split-zone` boundaries on the real key pattern (arm B) changed nothing: 50 unrehomed ranges, same 20M/0/20M placement, same ~52k keys/s.

## Cause

`RangeMap::split()` now gives the new upper range its **parent's** placement token (task 0139's metadata-only split — correct and necessary). But *every* boundary-creating path goes through `split()`:

- `PdCmd::CreateZone(cfg, boundaries)` (pd.rs:898 and :1096) pushes each presplit boundary as a `RangeOpKind::Split` op, so `create-zone … presplit=K` is K sequential inheriting splits, **not** a canonical `RangeMap::from_splits`.
- `admin split-zone` is a `PdCmd::SplitZone` → `PlacementDriver::split_zone` (pd.rs:248) → `map.split(at)` — also inheriting, even when the range is **empty**.

So after 0139 there is currently **no way at all** to spread a sorted zone: the whole zone permanently shares token `b""`, i.e. one replica set and one intra-node shard, regardless of key distribution. This is broader than task 0140 (which is about `even_boundaries` emitting single-byte boundaries and therefore not matching prefixed keyspaces) — it now applies to *uniform* keyspaces too.

## Why it matters

- Throughput on any sorted zone is capped at one shard (~4× loss measured: 52k vs 214k keys/s load; reads/scans similarly).
- It re-opens the pre-0107 hazard the `backlinks presplit=8` guardrail exists to prevent (one replica pair's device region fills).
- `plans/bootes-soak-runbook.md` §3 (presplit `backlinks`, pre-split `records` at collection boundaries) is now ineffective, which affects tasks 0130 and 0132.
- The docs assert the opposite and must be corrected: `docs/zones.md` §"Presplit at creation" ("these tokens are canonical (token = lower bound) because they are installed while the zone is empty" and "pass explicit boundaries with `admin split-zone` … currently the *only* way to spread a sorted zone"), `docs/zones.md` §"Sorted-zone growth = range splits" ("only presplit-at-creation spreads today"), `docs/algorithms.md` §2 ("presplits created while a zone is empty remain canonical and spread"), and `docs/multi-drive.md`.

## Direction

Splitting an **empty** range has no data to move, so it can safely take a canonical token — the 0142 re-home protocol is only needed for a populated range. Options:

1. Build the presplit map in one shot with `RangeMap::from_splits` (canonical) at zone creation instead of K sequential split ops.
2. Give `split_zone` an explicit "canonical/re-home" variant (or auto-canonicalise when the range's reported entry estimate is 0) so an operator can spread a young zone.
3. Fall out of 0142 entirely (re-home makes any range's token settable) — but the empty-range case should not wait for the full protocol.

## Acceptance criteria
- `create-zone <n> <rf> sorted presplit=K` on an empty zone yields a **canonical** map (`pd_unrehomed_ranges == 0`) and demonstrably spreads keys across replica sets.
- Splitting an empty range via `admin split-zone` also yields a canonical token (or a documented, explicit way to do so exists).
- A populated range's split remains metadata-only (0139 must not regress): a test that splits a populated range and asserts every key stays readable.
- Docs corrected: `docs/zones.md`, `docs/algorithms.md`, `docs/multi-drive.md`, and `plans/bootes-soak-runbook.md` §3 state what actually spreads.
- Regression test asserting `pd_unrehomed_ranges == 0` after a presplit-at-creation.

## Evidence
`bench/lab-split-fix-20260807/` — `A-during.txt` (unrehomed 7 of 8 pre-load), `A-after-summary.txt` / `B-after-summary.txt` (live_keys 20M/0/20M), `notes.md`.

## Plan

Fix the post-0139 regression by making *empty-range* boundary creation canonical, while leaving populated-range splits metadata-only (0139 intact).

1. `crates/vals-client/src/partition.rs` — add `RangeMap::split_canonical(&mut self, at: &[u8]) -> bool`:
   - inserts boundary `at` and gives the NEW upper range token `at` (canonical), leaving the left range's token untouched;
   - returns `false` and leaves the map untouched when `at` is empty, when the boundary already exists, or when the result would violate the wire invariant "tokens are non-decreasing" (`docs/formats.md` §3.3) — i.e. when the range to the right carries an inherited token `< at`. Never emit a map that `from_splits_and_tokens` would reject (decode is validated on every client).
   - On a fully canonical map this is exactly `from_splits(splits ∪ {at})`.
   - Keep `split()` (inheriting) unchanged.

2. `crates/vals-cluster/src/pd.rs`:
   - Introduce an explicit split mode (e.g. `enum SplitMode { Inherit, Canonical }`) threaded through `PdCmd::SplitZone` and `RangeOpKind::Split`, plus `PlacementDriver::split_zone_canonical(zone, at)` (keep `split_zone` = inherit, same signature/semantics).
   - Canonical propose path: if the boundary is genuinely new but `split_canonical` refuses, log a `warn!` explaining why and fall back to an inheriting `split()` so the boundary still lands and the pending-op retry loop always makes progress (never spin).
   - `PdCmd::CreateZone(cfg, boundaries)`: presplit boundaries must be queued as **canonical** splits (both drive loops: ~pd.rs:898 single-node and ~pd.rs:1096 raft). On a brand-new (empty, single-range) zone a sequence of canonical splits yields exactly the canonical `from_splits` map, so `unrehomed_ranges == 0`. Reflected/idempotence checks stay as they are.
   - `PdDaemon::split_zone` keeps its current inherit behaviour; add a canonical entry point (e.g. `split_zone_canonical`) for admin.
   - Auto-split (`range_stats_poll`) keeps using the INHERITING path — populated ranges must stay metadata-only.

3. `crates/vals-server/src/admin.rs` — accept `split-zone <zone> <key> canonical` (bare literal third arg; anything else = clear `err:` message) routing to the canonical entry point. `split-zone <zone> <key>` is unchanged (inherit).

4. Tests (all must pass; run at least `cargo test -p vals-client -p vals-cluster -p vals-server`):
   - partition.rs: canonical split yields token == lower bound and `unrehomed_ranges() == 0`; refusal case (map unchanged) when it would break token ordering; encode/decode round-trip of the resulting canonical map (stays v1 bytes); existing inheriting-split tests untouched.
   - pd.rs: extend `create_zone_presplit_yields_k_ranges` (or add a sibling) asserting the committed map `is_canonical()` / `unrehomed_ranges() == 0` AND that with a multi-node ring the K ranges resolve to more than one distinct replica set (i.e. it demonstrably spreads).
   - pd.rs: `split_zone_canonical` yields a canonical token; plain `split_zone` still inherits (0139 regression guard).
   - node.rs: the existing populated-split test (metadata-only split, every key still readable) must keep passing — confirm, and if it is not already explicit, assert that a PD-driven split of a populated range leaves the map non-canonical and all keys readable.
   - admin.rs: parse tests for `split-zone z k canonical` and for a bogus third arg.

5. Docs — state what actually spreads (and that a canonical split is only safe on an EMPTY range; a populated range needs the 0142 re-home protocol):
   - `docs/zones.md` §"Presplit at creation" (presplit=K is canonical again — via canonical splits at creation; keep the task-0140 caveat that `even_boundaries` is single-byte and misses prefixed keyspaces; replace the "admin split-zone … currently the only way to spread" claim with `admin split-zone <zone> <key> canonical` on an empty range) and §"Sorted-zone growth = range splits" / the metadata-only-split paragraph.
   - `docs/algorithms.md` §2 (token rules: canonical presplit + canonical empty-range split vs inheriting runtime split).
   - `docs/multi-drive.md` (~line 172).
   - `docs/deploy.md` admin command table (~line 200) — document the `canonical` argument.
   - `plans/bootes-soak-runbook.md` §3 — use `split-zone … canonical` for the pre-split `records` boundaries (zone is empty at that point) and note presplit=8 for `backlinks` is canonical.
   - `ARCHITECTURE.md` ~line 636 if its wording is now wrong.

Notes: no explanatory comments in Rust code (SAFETY/doctests/observe.rs HELP only); tree is NOT rustfmt-clean, hand-format new regions only, do not run `cargo fmt`. Do not implement re-homing of populated ranges (task 0142) and do not change `even_boundaries` (task 0140).

### Starting points
- crates/vals-client/src/partition.rs: RangeMap::split (l.135), from_splits (l.34), from_splits_and_tokens validation (l.43), unrehomed_ranges (l.78)
- crates/vals-cluster/src/pd.rs: PlacementDriver::split_zone (l.248), PdCmd/RangeOpKind (l.405-419), CreateZone handling in both drive loops (l.898 and l.1096), pending_range_ops reflected/propose loops (l.931, l.1210), PdDaemon::split_zone (l.1325), existing presplit test (l.2813)
- crates/vals-server/src/admin.rs: split-zone arm (l.154), create-zone presplit arm (l.125), tests from l.219
- docs/formats.md l.428 documents the wire invariant: token_count == split_count+1, tokens non-decreasing, each token is b"" or an existing split
- crates/vals-cluster/src/node.rs l.4700-4800: existing metadata-only split + canonical-map spread test

## Work log
- 2026-08-07 plan: Fix the post-0139 regression by making *empty-range* boundary creation canonical, while leaving populated-range splits metadata-only (0139 intact).  1. `crates/vals-client/src/partition.rs` — add `R
…[truncated]
- 2026-08-07 context hints: 5 recorded with plan
- 2026-08-07 context hints: crates/vals-client/src/partition.rs: RangeMap::split (l.135), from_splits (l.34), from_splits_and_tokens validation (l.43), unrehomed_ranges (l.78); crates/vals-cluster/src/pd.rs: PlacementDriver::spl
…[truncated]
- 2026-08-07 implementer report: Implemented task 0147.  Changes: - Added `RangeMap::split_canonical`, preserving wire token-order invariants and leaving maps unchanged on invalid/duplicate/empty boundaries. - Threaded explicit inher
…[truncated]
- 2026-08-07 review tier: single-opus — reviewers: sol
- 2026-08-07 review (sol): accept — The change satisfies task 0147. It adds invariant-preserving canonical splits while retaining inherited metadata-only splits, threads the mode through both PD drive paths, keeps auto-split inherited, 
…[truncated]
- 2026-08-07 decision: accept — commit: 0147: canonical splits for empty ranges — presplit=K and `split-zone … canonical` spread sorted zones again
- 2026-08-07 usage: 3,501,849 tok (in 984,229, out 35,956, cache_r 3,674,400, cache_w 185,150) · cost n/a (unpriced)
  implementer: 2,515,356 tok (in 567,342, out 14,190, cache_r 1,933,824, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 977,958 tok (in 416,855, out 13,263, cache_r 547,840, cache_w 0) · cost n/a (unpriced)
  coordinator: 8,535 tok (in 32, out 8,503, cache_r 1,192,736, cache_w 185,150) · cost n/a (unpriced)
