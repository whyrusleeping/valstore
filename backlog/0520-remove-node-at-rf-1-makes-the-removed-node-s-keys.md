---
id: "0520"
title: remove-node at RF 1 makes the removed node's keys unreachable (no drain before removal)
status: done
priority: 3
created: "2026-10-28"
updated: "2026-10-29"
depends_on: []
spec_refs: []
---

## Description

Found while verifying the growth path (0509), file backend, gc_grace = 0s, local smoke test.

With a two-node RF 1 cluster holding 200 keys in the default zone, `valsd admin remove-node 2` returned `ok committed` and right after it 105 of 200 keys read as not found through the `kv` client, even with node 2 still running; they stayed missing until node 2 was added back with `add-node` (gc_grace = 0s), after which all 200 read back. The PD allows the removal because membership stays >= RF, but at RF 1 (or any zone whose remaining replicas do not hold a key) nothing copies the departing node's keys to their new owners first. deploy.md says remove-node data "re-replicates through anti-entropy", which only holds when other replicas exist; 0509 documented the RF 1 caveat.

Possible directions (decide before implementing): refuse remove-node when some zone's RF leaves keys with no surviving replica unless `--force`, or add a drain step that hands the departing node's keys to the new owners before the membership change commits.

Acceptance (narrowed 2026-10-29): `remove-node` refuses, unless `--force` is given, when the removal would leave keys with no surviving replica (an RF 1 ring or RF 1 zone). A regression test shows the refusal and that `--force` still removes the node. docs/growing.md and deploy.md are updated. A drain step that hands the departing node's keys to the new owners is out of scope.

## Outcome

`valsd admin remove-node N` now refuses with `err: membership change refused, nothing applied: removing node N would leave keys with no surviving replica in ...` (naming the default zone at ring rf 1 and/or each rf 1 zone whose scope includes N) before the cleanup gate; `--force` skips both gates and removes the node. PdInspection gained `ring_rf` (the PD's validated global rf). Test admin::tests::remove_node_refuses_to_leave_keys_without_a_replica_unless_forced covers ring rf 1, an rf 1 zone on an rf 2 ring, rf 2 fall-through to the cleanup gate, and forced removal. Docs: growing.md (table + bullet), deploy.md (command row + gate paragraph), evaluation.md. Verified in a task-only worktree: clippy -D warnings, vals-server bin tests, vals-cluster pd tests, backup tests; sol review accepted.

Commit: 0520: remove-node refuses, unless --force, when the ring rf or a zone that may place keys on the node is rf 1 (no surviving replica); PdInspection carries ring_rf; regression test, growing.md/deploy.md/evaluation.md
