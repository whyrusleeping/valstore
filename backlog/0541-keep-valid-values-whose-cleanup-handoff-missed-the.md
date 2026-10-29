---
id: "0541"
title: 'Keep valid values whose cleanup handoff missed the trust window: owners answer whether they collected a delete for the key'
status: proposed
priority: 3
created: "2026-10-29"
updated: "2026-10-29"
depends_on:
    - "0518"
spec_refs: []
---

## Description
Follow-up proposed by 0518. With gc_grace > 0 a former owner's cleanup handoff skips the receiver's admission floor only within half the grace after the placement change. A valid value handed off later (likely on a large store, whose cleanup lap can take longer than gc_grace/2) is refused by every owner, recorded as refused, and deleted locally if a forced membership change makes its holder an owner again; without --force it stays unreadable on the former owner and the PD gate refuses the next change. The holder cannot tell such a value from a copy whose delete was collected.

Possible direction: owners keep a compact, persisted summary of the keys whose tombstones they collected (for example a per-zone filter of key hashes with the collection cutoff), so a refused handoff can be answered as "collected a newer delete" or "never had a delete newer than this", and only the first is treated as deleted; the second can be accepted.

Acceptance: a regression in which a valid aged value's RF 1 handoff happens after the trust window reaches the new owner (or survives a forced change back), while 0518's stale-copy regressions keep passing.

## Acceptance criteria

## Work log
