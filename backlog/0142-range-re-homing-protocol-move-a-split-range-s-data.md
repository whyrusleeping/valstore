---
id: "0142"
title: 'Range re-homing protocol: move a split range''s data with dual-write + confirmed cutover (step 2 of the zero-gap split fix)'
status: done
priority: 1
created: "2026-08-07"
updated: "2026-08-07"
depends_on:
    - "0139"
spec_refs: []
---

## Description
Follow-on to 0139, which makes splits **metadata-only** (a new range inherits its parent's placement token, so no data moves and nothing is stranded). That fixes the correctness bug but leaves auto-split with no load-spreading effect: a grown zone ends up with many ranges all sharing one token, i.e. still one replica set + one intra-node partition.

This task restores the spreading by making a token change (re-home) an explicit, safe, resumable move.

## Design sketch (agreed direction: option C)

Per range, the map carries `token` (current placement) and, while re-homing, `pending_token` (the target).

1. **Prepare.** PD sets `pending_token = <range lower bound>` on a range whose `token` is inherited, and pushes the map (retry-until-acked, existing epoch machinery).
2. **Dual-write.** While `pending_token` is set, coordinators write to the union of the current and pending replica sets (both must satisfy the write tier, or the write tier is evaluated on the current set with best-effort mirroring — decide and document). Reads continue to be served **only** from the current set, which holds everything → zero read gap, no read amplification.
3. **Bulk copy.** The new owners pull the range's key span (`Scan` of `[lo, hi)`) from a current owner — directed, range-scoped and prioritised, *not* the store-wide budgeted cleanup scan. Note the two throttles found in 0139 that made the old path useless: the store-wide paged cleanup scan (`CLEANUP_MAX_KEYS_PER_PASS`) and the 256-handoff/pass, 250 ms wall budget (`CLEANUP_MAX_HANDOFF_RPCS_PER_PASS`) at one pass per 5 s sweep.
4. **Cutover.** Once every new owner reports the copy complete (and only after every coordinator/node has acked the *prepare* epoch), PD commits `token = pending_token`, clears `pending_token`, and pushes. Coordinators cut over; the old owners' copies become over-replicated and are released by the existing confirmed-handoff cleanup.
5. **Rate control.** PD re-homes a bounded number of ranges at a time so a grown zone converges without a stampede.

Also fix or explicitly document the AE blind spot found in 0139: `VersionedStore::scoped_leaf_digests` scopes the Merkle exchange to keys **both** peers currently own, so a key held by a node that no longer owns it is invisible to every AE pull — AE can never repair a mis-placed key, only the push-side cleanup can.

## Acceptance criteria
- A range can be re-homed with zero reads returning not-found for committed data, under a concurrent write load (in-process test).
- Auto-split + auto-re-home together spread a growing sorted zone across replica sets and intra-node shards again (`pd_range_skew_pct`, `pd_unrehomed_ranges` converge).
- Interrupted re-home (node restart / PD leader change mid-copy) resumes or safely aborts; no data loss either way.
- Docs: `docs/zones.md` Growth section describes the full protocol.

## Plan

## Key design finding: the mirroring must be SERVER-side, not client-side

The task's original sketch says "coordinators write to the union of the current and pending replica sets". That is a trap in this codebase: placement lives in the **client** (`vals-client` `RangeMap::replicas_for_key`), and external clients (valsgo/bootes, valsq, valsmix) route to nodes directly. Client-side dual-write would mean every out-of-tree client has to implement the migration protocol correctly, or it silently fails to mirror — exactly the cross-client hazard task 0144 already flags for 0139's map format.

**Therefore: a write is mirrored by the node that receives it.** A current owner, on applying a write whose range carries a `pending_token`, forwards a copy to the pending owner(s). Unmodified and older clients keep working unchanged for the whole migration; only the final cutover changes routing, and that rides the existing `SetRangeMaps` push. This also puts the abort signal where PD already listens (the `RangeStats` reply), instead of in clients PD never talks to.

## Dual-write semantics (owner decision, 2026-08-07): abort-on-mirror-failure

- A write is acked to the client when it satisfies its write **tier on the current set** — unchanged availability and latency semantics.
- The receiving current owner must additionally forward it to **every** pending owner. Reads are served **only** from the current set for the whole migration, so a lost mirror is never a correctness problem *provided cutover cannot happen after one*.
- Any forward failure (timeout, error, unreachable peer) poisons that re-home: the node records it and reports it to PD, which **aborts** the re-home (clears `pending_token`, pushes) and may retry later. Abort is cheap and safe: the partial copy on the would-be new owners is simply over-replicated data that the existing confirmed-handoff cleanup releases.
- Cutover therefore requires three positives, not an absence of negatives: (1) every node has acked the *prepare* epoch (existing retry-until-acked machinery), (2) every pending owner reports copy-complete at an epoch >= the prepare epoch, (3) every current owner reports **zero** mirror failures for that range since the prepare epoch. A missing report is not a pass — it is treated as (silently) not-yet, and a report gap past a deadline aborts.

## Phase 1 — pending-token state (PD + wire)

- `RangeMap` (crates/vals-client/src/partition.rs) gains a per-range `pending: Option<Vec<u8>>` alongside the existing `tokens`. Accessors: `pending_of(index)`, `pending_for_key(key)`, `set_pending(index, token)`, `clear_pending(index)`, `commit_pending(index)` (token = pending, pending = None), `active_rehomes()`.
- Encoding (`encode`/`decode`): keep emitting **v1** bytes while the map is canonical and has no pending (today's behaviour), the existing **v2** magic+version framing when non-canonical with no pending, and add **v3** when any pending is set. `decode` accepts v1/v2/v3, rejects unknown versions loudly, stays total on arbitrary bytes and keeps the exact-consumption check (`crates/vals-cluster/tests/wire_fuzz.rs` must stay green). Validation: a pending token must be `b""` or an existing boundary, and `<= that range's lower bound`, same rule as `tokens`.
- PD durability comes for free: range maps are already raft-replicated under the `R` prefix as encoded bytes, so pending state survives a leader change and the protocol resumes.
- **Interaction guards:** PD must refuse to auto-split or merge a range that has a pending re-home, and refuse to start a re-home on a range it is splitting in the same poll. Assert this in `range_stats_poll`'s candidate selection and unit-test it — a split during migration would silently change which keys the copy covers.
- Metrics: `pd_rehomes_active` (gauge), `pd_rehomes_started` / `pd_rehomes_completed` / `pd_rehomes_aborted` (counters), plus the existing `pd_unrehomed_ranges` as the debt burn-down.

## Phase 2 — server-side forwarding + failure reporting

- Node write path (`crates/vals-cluster/src/net/server.rs` apply / batch-apply / CAS / delete handlers): after the local apply, if the key's range has a `pending_token`, forward the versioned record to each pending owner that is not already a current owner. Reuse the existing handoff/replicated-apply RPC so versions/timestamps are preserved and the apply stays LWW-idempotent; batch forwards to keep the serving core off a per-key round trip, and honour the shard-wide staged-apply gate landed for task 0130.
- Per-(zone, range) node-side counters: `rehome_mirror_ok`, `rehome_mirror_failures`, `rehome_mirror_epoch` (the range-map epoch the counters refer to; reset on epoch change so a stale zero can never be read as a pass).
- Extend the `RangeStats` reply (`wire::OwnedRangeStat`, already keyed by lower bound after 0139) with those fields; `range_stats_poll` in `crates/vals-cluster/src/pd.rs` aggregates them and aborts on any nonzero failure count.
- Reads, scans and AE are untouched in this phase — pending owners are not owners yet, so nothing routes to them.

## Phase 3 — copy worker, cutover, rate control

- **Copy worker** on the pending owner: a dedicated, range-scoped task (NOT the sweep). It pulls `[lo, hi)` from one current owner via the existing `Scan`, applies preserving versions, and checkpoints a resume cursor. Explicitly avoid the two throttles that made the old path useless (documented in 0139): the store-wide `CLEANUP_MAX_KEYS_PER_PASS` cursor and the 256-handoff / 250 ms-wall budget at one pass per 5 s. Bound it instead by its own byte/ops budget so it cannot starve serving.
- **Copy-complete report:** the worker reports completion (with the epoch it started at) on the `RangeStats` reply. Because forwarding started at the prepare epoch and the scan covers everything committed before it, complete + zero mirror failures = the pending set holds everything.
- **Cutover:** PD commits `token = pending_token`, clears pending, pushes (retry-until-acked). Old owners become over-replicated and are released by the existing confirmed-handoff cleanup — release-only, so its slowness is a space-reclaim latency, not a correctness issue.
- **Rate control / kill switch:** `VALS_PD_MAX_CONCURRENT_REHOMES` (default 1) and `VALS_PD_REHOME_ENABLED`. **Ship default-OFF**; task 0143's box run flips it on and validates, and only then does the default change. This deliberately restores the repo's "opt-in until box-validated" posture that task 0108 reversed and got burned by (task 0139).
- **Trigger:** once enabled, PD schedules a re-home for the range with the largest `entries` whose `token != lower bound` (the 0139 debt), one at a time per zone.

## Tests

1. In-process cluster test (alongside the sorted-zone harness in `crates/vals-cluster/src/net/tests.rs`): populate a sorted zone, split, run a re-home to completion under a concurrent writer, and assert **every committed key is readable at every point** (point get + scan), including across cutover, and that post-cutover reads are served by the new owners.
2. Fault injection: a transport that fails forwards → assert PD aborts, no cutover happens, and every key is still readable and correct.
3. Interruption: drop the pending owner mid-copy and change the PD leader mid-copy → assert it resumes or aborts cleanly, never loses data.
4. `RangeMap` units: pending encode/decode round-trip, v1/v2/v3 acceptance, invalid pending rejected, split/merge refused while pending.
5. `wire_fuzz` stays total.

## Docs

`docs/zones.md` "Growth = metadata-only range splits": add the full re-home protocol (prepare → forward → copy → confirm → cutover → release), state the abort-on-mirror-failure rule and why reads never gap, and record the AE blind spot found in 0139 (`VersionedStore::scoped_leaf_digests` scopes the Merkle exchange to keys both peers *currently* own, so AE can never repair a mis-placed key — only the push side can). Update the "only presplit spreads today" caveat once this lands.

## Suggested split

Phase 1+2 (state, wire, forwarding, reporting — no behaviour change while re-home is never triggered) is a self-contained, independently testable session; phase 3 (worker, cutover, rate control) is the second. Recommend filing them as 0142a/0142b if the first session looks too large to land cleanly.

## Verification

`cargo build --workspace` + `cargo test -p vals-client -p vals-cluster` incl. `wire_fuzz`. Do **not** run `cargo fmt` crate-wide (rustfmt version skew — hand-format new regions only).

### Starting points
- crates/vals-client/src/partition.rs — RangeMap: splits/tokens repr, from_splits_and_tokens validation, is_canonical/unrehomed_ranges, encode/decode (v1 canonical vs RANGE_MAP_V2_MAGIC framing at :196-235)
- crates/vals-cluster/src/pd.rs:529 range_stats_poll — aggregates per (zone_id, lower_bound), publishes metrics, enqueues split at reported midpoint; :546 skips replies with epoch < range_map_epoch
- crates/vals-cluster/src/pd.rs:880 pending_range_ops / RangeOpKind — the existing raft-committed range-op queue to extend with re-home ops
- crates/vals-cluster/src/antientropy.rs:33-42 CLEANUP_MAX_KEYS_PER_PASS / CLEANUP_MAX_HANDOFF_RPCS_PER_PASS / CLEANUP_HANDOFF_WALL_BUDGET — the throttles the copy worker must NOT inherit
- crates/vals-cluster/src/replica.rs:1206 scoped_leaf_digests — keep_group = group.contains(own) && group.contains(peer): AE is blind to keys the peer no longer owns; document, do not widen
- crates/vals-cluster/src/node.rs:2084-2115 sweep placement fn + cleanup_scoped_zone/reconcile_scoped_zone shard by hash64(range_token(key))
- crates/vals-cluster/src/net/tests.rs — existing sorted-zone cluster harness incl. push_range_map/coord.set_range_map helpers (node.rs:4611 push_range_map)
- docs/zones.md:96-165 — Growth = metadata-only range splits, auto-split, guardrails, presplit caveat

## Work log

- 2026-08-07 (PM): confirmed as the **next task** (owner, 2026-08-07) — the split line is the focus and re-homing is the real fix, ahead of 0140. Two additions to scope inherited from closing task 0110: (a) the load-spreading validation 0110 owed (a fresh sorted zone grows and spreads across replica sets / intra-node shards with no operator intervention) belongs to this task's acceptance, and (b) 0110's open question — whether 1M entries/range and the 5 s poll cadence are the right defaults — should be reassessed once re-homing sets the real cost of a split (a split now schedules a data move, so the threshold governs move frequency, not just map size).
- 2026-08-07 plan: ## Key design finding: the mirroring must be SERVER-side, not client-side  The task's original sketch says "coordinators write to the union of the current and pending replica sets". That is a trap in 
…[truncated]
- 2026-08-07 context hints: 8 recorded with plan
- 2026-08-07 context hints: crates/vals-client/src/partition.rs — RangeMap repr, from_splits_and_tokens validation, encode/decode (v1 vs RANGE_MAP_V2_MAGIC at ~:196-235); crates/vals-cluster/src/pd.rs:529 range_stats_poll; :88
…[truncated]
- 2026-08-07 implementer report: Implemented task 0142’s range re-home machinery across client map/wire, node, PD, metrics, and docs.  Changed: - Added per-range pending tokens to `RangeMap`, including validation/accessors/prepare-
…[truncated]
- 2026-08-07 revision: Addressed the review findings.  Changes: - Fixed pending-copy erosion: cleanup owner calculation now uses the union of current and pending replica sets, and intra-node ownership accepts both current-t
…[truncated]
- 2026-08-07 revision: Added the required real in-process, socket-level re-home acceptance tests and fixed the wiring issue they exposed.  New tests in `crates/vals-cluster/src/net/tests.rs`: - `rehome_real_transport_copy_m
…[truncated]
- 2026-08-07 review tier: high-powered — reviewers: sol
- 2026-08-07 review (sol): revise — The change adds most of the intended state, forwarding, copy, PD, metrics, tests, and documentation, but the cutover protocol is not yet safe for real coordinators and has several completion/recovery 
…[truncated]
- 2026-08-07 revision: Addressed all four review findings.  Changes: - Removed the invalid global token monotonicity requirement from `RangeMap`; validation is now strictly per range (`""` or an existing boundary, and `<=` 
…[truncated]
- 2026-08-07 review (sol): revise — The revision fixes non-monotonic token validation, adds durable copy completion, preserves batch sync for mirrors, and implements a report-gap timeout. However, the protocol remains unsafe for the req
…[truncated]
- 2026-08-07 revision: Fixed the shard-blind re-home protocol and added the required cross-shard end-to-end coverage.  Changes: - Extended `OwnedRangeStat` wire data with `shard_index` and `shard_count`; updated response en
…[truncated]
- 2026-08-07 review (sol): revise — The shard-aware revision appears to fix the intra-node copy/mirror/reporting defect and adds useful cross-shard transport coverage. The core cutover correctness issue remains, however: stale external 
…[truncated]
- 2026-08-07 decision: accept — commit: 0142: confirmed range re-home — pending tokens (map v3), server-side dual-write with ack-implies-mirrored, shard-aware copy/confirm/cutover, PD abort on mirror failure or report gap; default-off (VA
…[truncated]
- 2026-08-07 usage: 67,898,295 tok (in 3,963,657, out 176,814, cache_r 66,377,968, cache_w 587,718) · cost n/a (unpriced)
  implementer: 63,041,538 tok (in 3,206,717, out 75,461, cache_r 59,759,360, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 4,817,216 tok (in 756,870, out 61,882, cache_r 3,998,464, cache_w 0) · cost n/a (unpriced)
  coordinator: 39,541 tok (in 70, out 39,471, cache_r 2,620,144, cache_w 587,718) · cost n/a (unpriced)
