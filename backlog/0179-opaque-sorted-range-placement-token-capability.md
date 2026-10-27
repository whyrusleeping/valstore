---
id: "0179"
title: Opaque sorted-range placement-token capability and compatibility gate
status: done
priority: 4
created: "2026-08-11"
updated: "2026-08-11"
depends_on:
    - "0176"
spec_refs:
    - docs/algorithms.md#compatibility-and-rollout
---

## Description

Implement phase 1 of task 0120's selected balance-aware sorted-range design, after re-home's healthy-node abort blocker is closed. Range-map V2 already encodes explicit current tokens and V3 also encodes pending tokens, but `RangeMap::valid_token` and existing valsgo decoders accept only the empty token or a prior split boundary. Add a versioned capability under which a sorted range token may instead be an opaque, fixed-size PD-selected placement seed.

This task changes representation support only. It must not select balanced placements, rewrite existing maps, enable re-home, or add work to point and batch writes. Routing remains the existing cached range lookup followed by `replicas_in_zone` and sorted-zone shard hashing.

## Acceptance criteria

- A versioned range-map/catalog capability distinguishes maps that may contain opaque tokens from canonical/inherited-only maps; existing V2/V3 maps retain their current validation and byte behavior.
- The new format carries explicit per-range placement debt so an intentionally assigned opaque token is not mistaken for an inherited split child. Legacy decode derives debt from `token != lower`; intentional commit clears it, abort preserves it, and split preserves the left state while marking the new right child. `pd_unrehomed_ranges`, candidate selection, and map encoding use this state instead of token shape.
- Rust clients, data nodes, admin tooling, and valsgo decode, validate, retain, and route opaque current and pending tokens identically, including fencing, scans, split/merge refusal during re-home, and shard selection. Routing does not inspect placement debt.
- Old software fails closed on the new capability. The PD cannot emit opaque tokens until every data node has acknowledged support and an operator-controlled gate asserts that stateless coordinators have been upgraded; mixed-version clusters continue using only canonical/inherited tokens.
- Opaque tokens are fixed-size and domain/version identified. No request-time bounded-load cascade, extra lookup, planner branch, or RPC is added to any write path.
- Agreement vectors and focused Rust/Go tests cover codec round trips, invalid capability/token combinations, replica and shard parity, pending-token prepare/abort/commit, and stale-epoch fencing.
- Documentation in `docs/formats.md`, `docs/algorithms.md`, and `docs/zones.md` is updated to match the shipped capability without claiming that the planner is enabled.

## Plan

Implement phase-1 representation and rollout support without enabling balancing or adding write-path work:

1. Introduce a fail-closed RangeMap V4 capability in `vals-client::partition`. Preserve legacy/V2/V3 decode, validation, and byte-for-byte encoding choices for maps that have not opted into the capability. V4 carries explicit placement-debt state for every range plus current/pending tokens. Define one fixed-size, self-identifying opaque token domain/version (a constant header/version plus fixed seed bytes); V4 accepts either the existing canonical/inherited token forms or exactly that opaque form and rejects opaque tokens in legacy/V2/V3 and malformed/unknown opaque forms. Add APIs sufficient for a future planner to capability-upgrade a map and install an opaque pending token, while routing remains the existing token lookup/hash path.
2. Move inheritance semantics to explicit debt: legacy decodes derive debt from `token != lower`; intentional pending commit clears the selected range's debt, abort/clear preserves it, inheriting split preserves left debt and marks the new right child indebted, and canonical presplit state remains debt-free. Make `unrehomed_ranges`, canonical checks/candidate selection, and encoding use debt. Ensure split/merge continue refusing active re-homes; define conservative debt behavior on merge and cover it.
3. Carry V4 unchanged through Rust wire request/response codecs, coordinator cache, data-node adoption/admin flows, scans, re-home fencing, and shard routing. Add protocol agreement/golden tests for codec round trips, invalid version/token/debt combinations, current/pending routing and replica/shard parity, prepare/abort/commit, split/merge refusal, and stale-epoch fencing. Confirm routing never consults debt.
4. Add a handshake feature bit for opaque-range-map support and make PD control pushes observe negotiated data-node features. Add an operator-controlled gate (default off, documented env/config surface) and a PD readiness check requiring both that gate and acknowledgement from every current data-node endpoint before any opaque-capable map can be emitted/installed. Mixed/old nodes must keep receiving only legacy canonical/inherited formats; unknown V4 remains fail-closed. Do not enable a planner, auto-upgrade existing maps, or change existing re-home target selection.
5. Update the sibling `vals-examples/valsgo` decoder/model/tests and shared agreement vectors so Go retains V4 capability, debt, opaque current/pending tokens, routes by committed token, and rejects invalid combinations identically. Commit that sibling repository separately, as established for prior cross-client tasks. Verify Go tests/build and Rust agreement parity.
6. Update `docs/formats.md`, `docs/algorithms.md`, and `docs/zones.md` with the exact V4 layout, opaque-token identity/size, debt transitions, handshake feature and two-part rollout gate. State clearly that planning and opaque placement emission remain disabled in this phase.
7. Run targeted vals-client/vals-cluster/vals-server tests plus workspace build/tests appropriate to the changed surfaces; do not run cargo fmt because the tree is not rustfmt-clean. Keep unrelated existing working-tree changes intact.

### Starting points
- crates/vals-client/src/partition.rs: RangeMap validation, V1/V2/V3 codec, debt currently inferred in unrehomed_ranges
- crates/vals-client/src/wire.rs: SetRangeMaps/RangeMaps framing and focused codec tests
- crates/vals-cluster/src/proto.rs and net handshake: negotiated feature bits are currently zero
- crates/vals-cluster/src/pd.rs: confirmed_range_maps and range_stats_poll gate re-home/candidate selection
- crates/vals-server/src/admin.rs and request handling: node map adoption/fencing
- docs/algorithms.md#Compatibility and rollout is the selected design
- /home/user/code/vals-examples/valsgo: out-of-tree decoder/routing implementation from task 0144

## Work log
- 2026-08-11 plan: Implement phase-1 representation and rollout support without enabling balancing or adding write-path work:  1. Introduce a fail-closed RangeMap V4 capability in `vals-client::partition`. Preserve lega
…[truncated]
- 2026-08-11 context hints: 7 recorded with plan
- 2026-08-11 context hints: Existing unrelated modification: backlog/0178 usage accounting; preserve it and do not edit task 0178.; Sibling vals-examples edits may require shell operations because workspace file tools are confin
…[truncated]
- 2026-08-11 preload: 8 file(s), ~63 KiB seeded into implementer context
- 2026-08-11 implementer report: Implemented task 0179 representation and rollout support.  Main workspace changes: - Added fail-closed RangeMap V4 persistence/wire codecs with explicit per-range placement debt, current/pending token
…[truncated]
- 2026-08-11 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-11 review (sol): revise — The V4 representation, wire propagation, feature negotiation/gate, documentation, and sibling Go decoder are broadly integrated, and the Go suite passes. However, RangeMap mutation still has two corre
…[truncated]
- 2026-08-11 review (sol#2/sol): revise — The V4 representation and propagation are broadly integrated and the Go suite passes, but there are correctness holes in legacy-map persistence and in the compatibility gate, plus the required cross-l
…[truncated]
- 2026-08-11 revision: Addressed all review findings.  Changes in this revision: - Restored legacy persistence compatibility: non-V4 commits now recompute debt from token shape. A valid legacy commit to an earlier boundary
…[truncated]
- 2026-08-11 review (sol): accept — The revision addresses the prior correctness issues: legacy non-lower commits retain V2 round-trip behavior, all structural map changes are refused while any re-home is active (including PD auto-split
…[truncated]
- 2026-08-11 review (sol#2/sol): accept — The revision addresses the prior correctness issues: legacy prior-boundary commits retain V2 routing across persistence, structural changes are blocked while any re-home is active, V4 fanout performs
…[truncated]
- 2026-08-11 decision: accept — commit: cluster: add opaque range map capability
- 2026-08-11 usage: 31,958,515 tok (in 5,578,612, out 89,215, cache_r 26,290,688, cache_w 0) · cost n/a (unpriced)
  implementer: 24,635,277 tok (in 2,459,075, out 59,594, cache_r 22,116,608, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,856,958 tok (in 1,121,125, out 10,393, cache_r 1,725,440, cache_w 0) · cost n/a (unpriced)
  coordinator: 2,569,963 tok (in 1,086,241, out 7,882, cache_r 1,475,840, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,896,317 tok (in 912,171, out 11,346, cache_r 972,800, cache_w 0) · cost n/a (unpriced)
