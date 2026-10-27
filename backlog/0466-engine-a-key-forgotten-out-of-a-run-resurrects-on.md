---
id: "0466"
title: 'Engine: a key forgotten out of a run resurrects on reopen when its value-log record is in the replayed covered-epoch segment'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-21"
depends_on: []
spec_refs: []
---

## Description

Found while building 0464 (cleanup handoff of range tombstones).

## Outcome

Forgotten run keys stay absent after reopen. Invariant: every Put record with seq > covered_seq is live in the memtable or beaten by a later tail record. Recovery skips memtable insertion of winners with seq <= covered_seq (covered_seq 0 filters nothing); paged flushes capture only entries <= their watermark (Index::flush_page_attested seq ceiling); puts below the memtable winner are not logged; both forget APIs append Delete records for dropped run entries with seq > last_flush_seq or while it is 0. Regression tests for the repro, paged/loser/seq-0 cases and the cluster cleanup reopen assertion; kill-the-fix per mechanism; store/cluster/server suites green. Accepted by claude + sol.

Commit: fix(store): forgotten run keys no longer resurrect from the replayed log tail (0466)
