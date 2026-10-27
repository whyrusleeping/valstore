---
id: "0081"
title: Unsharded node's cleanup handoff to a more-sharded peer lands on shard 0 (mis-sharded apply)
status: done
priority: 4
created: "2026-07-06"
updated: "2026-07-06"
depends_on:
    - "0080"
spec_refs:
    - docs/multi-drive.md
---

## Description
Follow-up found during task 0080 (asymmetric shard counts). The **unsharded** serve path (`Node::serve*`, single listener) builds its cleanup/handoff transport from a single address per peer (`peers` → `vec![a]`), so a 1-shard node's over-replication cleanup *handoff* (`apply`) to a **more-sharded** (JBOD) peer always lands on the peer's shard 0 — mis-sharding the handed-off key onto the wrong sibling shard. AE from the 1-shard node now routes congruently (task 0080), so healing is correct; only the handoff push is coarse. Documented as a known gap in docs/multi-drive.md.

## Acceptance criteria
- The unsharded path's handoff/cleanup transport routes each `apply` by the destination peer's real shard count (build it from the full per-peer shard address lists — `peer_shards` / address book — via `connect_sharded`, as the sharded path already does).
- Off-hardware test: a 1-shard node hands off an unowned key to a 2-shard peer and it lands on the shard `hash64(key) % 2` selects (and is servable there), not unconditionally shard 0.
- The docs/multi-drive.md "known gap" note is updated/removed accordingly.
- Existing cleanup/handoff and multidrive tests stay green.

## Plan

## Root cause

`sweep_loop` (crates/vals-cluster/src/node.rs) keeps two transports:
- reconcile (`transport`): built from `peer_shards` congruently on the sharded path, but from `peers → vec![a]` (one addr per peer) on the unsharded path.
- handoff (`handoff`): built from full `peer_shards` when `shard_count > 1`, else `peers → vec![a]`.

Both per-pass member-install loops (~2802 for reconcile, ~2850 for handoff) skip members already present (`has_peer`), so a statically-seeded single-address entry is NEVER upgraded to the peer's full shard-address list — even after a `SetMembers` push fills the shared address book (`shared.peer_addrs_of`). Consequence: an unsharded node's cleanup handoff `apply` to a 2-shard peer always routes to `data_shard` over a 1-entry list = the peer's shard 0 (mis-sharded).

`NetTransport::add_peer` is already **change-detecting** (re-installs only when the advertised address set actually changed; no connection churn otherwise) — its doc comment describes exactly this "bootstrapped with a single seed address, must pick up the full shard set" upgrade.

## Changes (all in crates/vals-cluster/src/node.rs `sweep_loop`, plus docs + test)

1. **Handoff refresh loop (~2850):** drop the `!handoff.has_peer(n)` guard. Each pass, for every ring member `n != node_id`, resolve the peer's full shard-address list from the address book (`shared.peer_addrs_of(n)`), falling back to the static `peer_shards` entry; if known, call `handoff.add_peer(n, &addrs)` unconditionally (change-detected no-op when unchanged). If no full list is known, leave any existing (static single-address) entry alone — never remove a route. Now a 1-shard node's handoff `apply` routes by `hash64(shard_key) % k'` over the peer's real shard list once membership is known.

2. **Reconcile install loop (~2802):** same treatment — drop the `has_peer` skip; compute the congruent subset (`congruent_shard_addrs(full, shard_index, shard_count)`) from the book-or-static full list and `transport.add_peer(n, &sub)` each pass. This makes the docs claim "a 1-shard node reconciles against all of a peer's shards" true for statically-seeded unsharded nodes too (k=1 ⇒ congruent subset = full list), and lets any node adopt a peer's changed shard set. Keep the "no known address yet" debug branch. Note in a comment: the AE resume cursors are keyed `(zone, peer, entry-index)`; if an entry grows/changes, a stale cursor only means a resume-position reset — correctness is enforced by the puller's `keep` filter, not cursors.

3. **Comments:** update the two comment blocks (2795–2856) to describe the per-pass book-driven refresh (install *and* upgrade), and fix the `serve_sharded`/`serve_multidrive*` comment at ~739–743 if it still says the book is "only" a new-member source.

4. **Docs:** replace the "Known gap (follow-up)" paragraph in docs/multi-drive.md (~lines 75–81) with a short description of the fix: both sweep transports are refreshed from the membership-borne address book every pass, so an unsharded node's handoff routes each `apply` by the destination peer's real shard count; note the residual: a purely static, no-membership-push deployment that only ever supplies one address per peer still has only that one route (the config plumbing already carries full lists to sharded serves; PD deployments always push full lists).

5. **Off-hardware regression test** (node.rs tests, model on existing liveness/membership-push tests):
   - Node 2: `Node::serve_sharded` with 2 shards (listeners a2_0, a2_1), ring over [1, 2].
   - Node 1: unsharded `Node::serve`, static peers knowing only `(2, a2_0)` (the coarse pre-fix wiring).
   - Choose a key with: ring owner (rf=1) = node 2, and `hash64(key) % 2 == 1` (must land on peer shard 1). Seed it into node 1's store pre-serve.
   - Push `SetMembers` (epoch 1, rf 1, members [1,2]) with full addrs `[(1,[a1]),(2,[a2_0,a2_1])]` to a1, a2_0, and a2_1 (fills node 1's address book, advances epochs so the over-replication cleanup runs, and lets node 2 accept epoch-stamped applies).
   - Wait until a direct read at **a2_1** (shard 1) returns the value; assert a direct read at **a2_0** returns None (not mis-sharded onto shard 0); assert node 1 eventually forgets the key (handed off + released).
   - Pre-fix, this test fails: the apply lands on shard 0.

## Verification
- `cargo test -p vals-cluster` green (existing cleanup/handoff, congruence (0079/0080 regressions), liveness, multidrive tests).
- New test fails without the sweep_loop change (implementer should confirm by reverting mentally/logically), passes with it.
- `cargo build` workspace-clean; clippy on touched crate if quick.

### Starting points
- crates/vals-cluster/src/node.rs:2660-2700 — sweep_loop transport construction (reconcile + handoff; unsharded path = peers → vec![a])
- crates/vals-cluster/src/node.rs:2795-2856 — per-pass member install loops for both transports (the has_peer guards to drop)
- crates/vals-cluster/src/node.rs:2601-2639 — gcd + congruent_shard_addrs
- crates/vals-client/src/net/client.rs:683 — NetTransport::add_peer is change-detecting (doc comment describes this exact upgrade)
- crates/vals-cluster/src/net/server.rs:299-312 — peer_addrs_of / merge_peer_addrs (address book, filled by SetMembers)
- crates/vals-cluster/src/node.rs:3419 — test helper set_members_config_with_addrs; ~3820 a full membership-push test to model on; ~5351 sharded two-node test setup with peer_shards
- docs/multi-drive.md:75-81 — the Known gap paragraph to replace
- cleanup handoff routing: apply routes by data_shard = hash64(shard_key) % entry-len; cleanup uses the `handoff` transport (node.rs ~3161)

## Work log
- 2026-07-06 plan: ## Root cause  `sweep_loop` (crates/vals-cluster/src/node.rs) keeps two transports: - reconcile (`transport`): built from `peer_shards` congruently on the sharded path, but from `peers → vec![a]` (o
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: crates/vals-cluster/src/node.rs:2660-2700 — sweep_loop transport construction (reconcile + handoff; unsharded path = peers → vec![a]); crates/vals-cluster/src/node.rs:2795-2856 — per-pass member
…[truncated]
- 2026-07-06 implementer report: Implemented task 0081: fixed the unsharded node's cleanup/handoff transport to route each `apply` by the destination peer's real shard count.  ## Changes (crates/vals-cluster/src/node.rs `sweep_loop`)
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — The change correctly fixes the unsharded node's cleanup/handoff mis-sharding. Both the reconcile and handoff transport-refresh loops in sweep_loop now drop the has_peer skip and, each pass, resolve ev
…[truncated]
- 2026-07-06 decision: accept — commit: cluster: unsharded node's cleanup handoff routes by the peer's real shard count — sweep_loop refreshes both transports from the membership address book every pass (change-detecting add_peer upgrades
…[truncated]
- 2026-07-06 usage: 30,196 tok (in 126, out 30,070, cache_r 2,640,054, cache_w 183,009) · cost n/a (unpriced)
  implementer: 19,636 tok (in 70, out 19,566, cache_r 1,405,588, cache_w 60,253) · cost n/a (unpriced)
  coordinator: 5,501 tok (in 24, out 5,477, cache_r 954,417, cache_w 95,737) · cost n/a (unpriced)
  reviewer:Claude: 5,059 tok (in 32, out 5,027, cache_r 280,049, cache_w 27,019) · cost n/a (unpriced)
