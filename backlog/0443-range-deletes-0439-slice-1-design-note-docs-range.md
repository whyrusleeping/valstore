---
id: "0443"
title: 'Range deletes (0439) slice 1: design note docs/range-deletes.md, reviewed before implementation'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on: []
spec_refs:
    - docs/range-deletes.md
---

## Description

Slice 1 of 0439 (range deletes for sorted zones). 0439's first acceptance item: "A design note (docs/range-deletes.md, linked from ARCHITECTURE.md), reviewed before implementation, covering the questions above" (engine representation, scans, point reads/digests/read-repair, anti-entropy, range lifecycle, accounting, GC/compaction, formats).

## Outcome

docs/range-deletes.md settles 0439's eight design questions, grounded in the current code, and is linked from ARCHITECTURE.md and docs/README.md. It was accepted by two reviewers after three rounds, which fixed the skip boundary, fold targets, atomic collection of range-tombstone records and the cost of building the max tree. Implementation is split into ordered todo tasks 0444–0449, and umbrella task 0439 depends on 0443–0449.

Commit: docs(0443): range-delete design note docs/range-deletes.md (0439 slice 1); implementation split into 0444-0449; carries the owner's 0442 proposed->todo flip
