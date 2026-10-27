---
id: "0343"
title: Update docs/backpressure.md 0287 pointer to the recorded no-go disposition (after 0338 lands)
status: done
priority: 4
created: "2026-09-29"
updated: "2026-10-01"
depends_on:
    - "0287"
spec_refs: []
---

## Description

Split from 0287. docs/backpressure.md "Defaults and migration" (~line 230) still says "production SPDK adoption remains conditional on the task 0287 hardware gate in plans/client-budget-hardware-gate.md". 0287 is now dispositioned as an executed no-go (see bench/0287-hardware-20260909/FINDINGS.md "Disposition"): finite defaults stay as resource bounds, not performance-adopted (width-32 Quorum -6.78%, Digest -4.59%), hardware hedge-loser cancellation unproven.

The file was not edited in 0287 because it carries the owner's uncommitted 0338 work. Do this once that work is committed/resolved (the file is clean in git).

Acceptance: the sentence points to the 0287 no-go disposition with the same facts as ARCHITECTURE.md's client-backpressure bullet; no other doc changes.

## Outcome

docs/backpressure.md "Defaults and migration" now states the 0287 SPDK RF2 gate is an executed no-go for performance adoption (links bench/0287-hardware-20260909/FINDINGS.md Disposition; 4.59%/6.78% width-32 Digest/Quorum cost, hedge-loser cancellation unproven; finite defaults kept as resource bounds), matching ARCHITECTURE.md. No other doc changes.

Commit: docs(0343): backpressure.md points to the executed 0287 no-go disposition; block 0352/0354 on owner (held drives + 0362 RAM)
