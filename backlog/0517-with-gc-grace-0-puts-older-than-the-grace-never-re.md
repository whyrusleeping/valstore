---
id: "0517"
title: With gc_grace > 0, puts older than the grace never reach a new replica (AE fill, rehome copy, cleanup handoff, backup load)
status: done
priority: 1
created: "2026-10-28"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found while working on 0515. The live-value admission floor from 0038 (`replica/write.rs`: a put whose `physical_ms` is below `min(wall, high_water) - gc_grace` gets `Applied::Rejected`) applies to every apply path, whether or not the receiver ever held the key. It blocks resurrection, but it also blocks legitimate copies of old data:

- **AE fill (checked).** A two-node RF 2 test (`serve_with_liveness`, grace 60 s) where node 2 holds a recent key and a key written 600 s ago: node 1 pulls the recent key but never the old one. With grace 0 it pulls both. So a node that is new to a key (add-node, remove-node replacement, an emptied disk) never gets data older than `gc_grace`, and the key keeps one replica fewer for good.
- **Rehome copy (from reading the code).** `node/rehome.rs` counts `Ok(Some(_))` as copied, and that includes `Applied::Rejected`. A confirmed re-home can therefore cut over even though the target lacks every value older than the grace. If the new set does not overlap the old one, reads lose those values. They stay physically on the former owner, which can never release them because the owners refuse the handoff.
- **Cleanup handoff of a disjoint move (from reading the code).** For example RF 1, or two quick membership changes: the new owners refuse the old values, so they stay unreadable on the former owner.
- **`valsd backup load` into a target with `gc_grace > 0` (from reading the code).** Once the target's high water has moved, records older than the grace come back `Rejected`. The load then fails with a misleading "clock skew" message. This affects 0511, whose hardware nodes use `gc_grace = 300s`.

Suggested direction, needs design review: puts that a current (co-)owner sends to fill another replica (AE pulls from a co-owner, a rehome copy from the current owner, a fresh restore target) should bypass the floor. Writes from coordinators, hint replays, and handoffs from non-owners would keep it. Safety would then rest on the classic tombstone-GC invariant. Also make the rehome copy fail closed on `Rejected`. 0515 already restricts AE pulls to keys both peers own, which this needs.

Acceptance: regression tests that fail today for AE fill of a new owner, for rehome copy plus cutover of old data, and for backup load into a `gc_grace > 0` target. The resurrection tests from 0038, 0515 and liveness keep passing. Measure on hardware if the client apply path changes.

## Outcome

LiveFloor {Enforce, Waive}: the admission floor is waived for AE pulls from the owning shard of a committed owner (not pending targets or sibling shards), re-home copies (only Won/Lost count as copied, so a refusal blocks cutover) and backup load (BatchApply flags bit 1 `fill`; old nodes keep the floor and load fails closed). Client writes, read repair, hints and cleanup handoffs keep it; the disjoint-move handoff limit is noted in 0518. New regressions (fail without the fix): AE fill of a co-owner, re-home copy + cutover of aged data, valsd backup load into gc_grace=60s targets, plus a sibling-shard AE test. Full vals-cluster/client/server suites pass. Docs updated incl. formats.md flags byte. No hardware run (one enum compare on the apply path).

Commit: 0517: copies from owners skip the tombstone-GC admission floor (AE from the owning peer shard, re-home copy, backup load via BatchApply fill flag); re-home copy fails closed on Rejected
