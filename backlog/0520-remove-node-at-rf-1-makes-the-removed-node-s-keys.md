---
id: "0520"
title: remove-node at RF 1 makes the removed node's keys unreachable (no drain before removal)
status: proposed
priority: 3
created: "2026-10-28"
updated: "2026-10-28"
depends_on: []
spec_refs: []
---

## Description
Found while verifying the growth path (0509), file backend, gc_grace = 0s, local smoke test.

With a two-node RF 1 cluster holding 200 keys in the default zone, `valsd admin remove-node 2` returned `ok committed` and right after it 105 of 200 keys read as not found through the `kv` client, even with node 2 still running; they stayed missing until node 2 was added back with `add-node` (gc_grace = 0s), after which all 200 read back. The PD allows the removal because membership stays >= RF, but at RF 1 (or any zone whose remaining replicas do not hold a key) nothing copies the departing node's keys to their new owners first. deploy.md says remove-node data "re-replicates through anti-entropy", which only holds when other replicas exist; 0509 documented the RF 1 caveat.

Possible directions (decide before implementing): refuse remove-node when some zone's RF leaves keys with no surviving replica unless `--force`, or add a drain step that hands the departing node's keys to the new owners before the membership change commits.

Acceptance: a regression test showing RF 1 removal either refuses or keeps every key readable; docs/growing.md and deploy.md updated.

## Acceptance criteria

## Work log
