---
id: "0517"
title: With gc_grace > 0, puts older than the grace never reach a new replica (AE fill, rehome copy, cleanup handoff, backup load)
status: proposed
priority: 1
created: "2026-10-28"
updated: "2026-10-28"
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

## Acceptance criteria

## Work log
