---
id: "0106"
title: valsgo↔Rust client placement divergence on split sorted-zone ranges (cross-client reads miss data)
status: done
priority: 2
created: "2026-07-31"
updated: "2026-08-06"
depends_on: []
spec_refs:
    - docs/README.md
---

## Description
## Symptom
On the live 3-node multi-machine SPDK cluster (lab fleet, RF 2), bootes (valsgo/Go client) ingested ~46M records with zero server_errors, but the Rust client (`valsq`) could see only ~1856 records in the `records` sorted zone and 0 in `backlinks`. The default zone (cursor) is consistent across clients; the divergence is specific to **sorted zones that have splits**.

## Minimal repro (isolated from bootes)
Cluster: `records` sorted zone (RF 2) pre-split at `app.bsky.feed.like/ app.bsky.feed.post/ app.bsky.feed.repost/ app.bsky.graph.follow/ app.bsky.graph0`. Nodes `1@10.0.0.114:7001,2@10.0.0.18:7001,3@10.0.0.200:7001`.

- `valsgo` interop `batch-many records "app.bsky.feed.post/pv" 100` → valsgo `scan-check` reads back all 100 ✅, but `valsq --zone records count 'app.bsky.feed.post/pv' 'app.bsky.feed.post/pv~'` → **0**.
- Control into the **unsplit tail** range: `batch-many records "zzz.pv" 100` → `valsq count 'zzz.pv' 'zzz.pv~'` → **100** ✅ (clients agree).
- A `valsq` single-Put into `app.bsky.feed.post/...` is self-consistent (valsq put→get→count=1), so each client is internally consistent; they just place the same split-range key on **different replica sets**.

## Where it is
Both clients derive the same range token (Go `RangeMap.RangeToken` == Rust `RangeMap::range_token`: empty token for the initial range, else the split lower-bound), then call `ReplicasInZone(zone_id, token, rf, scope)` (Go `client.go:256 placement` → `ring.ReplicasInZone`; Rust `partition.rs` → `ring.replicas_in_zone`). Divergence is token→replica for some tokens (`app.bsky.feed.post/` diverges, `app.bsky.graph0` agrees) — a subtle hash/salt/scope parity gap in the zone-scoped ring placement (`hash.go`/`ring.go` vs the Rust ring), OR valsgo adopting only a subset of the pushed rangemap splits. Nail down which: dump both clients' parsed rangemap + the (zone_id, token)→ring-position for the diverging vs agreeing tokens.

## Acceptance
- Root-caused (hash parity vs rangemap-subset) with a failing kill-the-fix test in `valsgo/vectors_test.go` (or the Rust parity vectors) covering split-range placement for multiple split tokens.
- Go and Rust clients place every split-range key on the identical replica set; the interop repro above passes cross-client.
- If it is a rangemap-adoption bug, valsgo adopts the full split set on Refresh.

## Notes
Found 2026-07-31 while standing up a bootes soak against the real cluster (see the bootes-soak bench notes). Interop tool: `vals-examples/valsgo/cmd/interop`. Only bites sorted zones WITH splits; unsplit sorted zones and the default zone are consistent.

## Acceptance criteria

## Work log
- 2026-08-06 CLOSED. The fix shipped in vals-examples master (`valsgo: intra-node shard routing for multi-shard nodes`, 35e7951: connections keyed by (node, shard), `dataShard`/`scanShard` mirroring the Rust client, PutBatch regrouped by (node, shard)) and was validated live twice on the lab cluster — interop batch-many/put-many into split ranges read back identically by valsgo and valsq, then a full reformatted 300-way soak with both zones readable by the Rust client and exact narrow sub-range counts. `go vet`, gofmt and the valsgo vector tests are green. The only unmet acceptance detail — a golden shard-routing vector so the parity is pinned in CI rather than by live interop — is carried forward as task 0136.
