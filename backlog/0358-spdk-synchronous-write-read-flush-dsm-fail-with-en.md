---
id: "0358"
title: SPDK synchronous write/read/flush/DSM fail with ENOMEM on qpair request exhaustion instead of retrying (split from 0355)
status: done
priority: 1
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

Split from 0355 (coordinator triage 2026-10-01).

## Outcome

The synchronous shim paths no longer surface transient request-pool exhaustion as `nvme write: Cannot allocate memory (os error 12)`. That error was the pre-0350 cause of the 0353 node1 flush-commit failure and the bootes value-log batch failures, and of 158 occurrences across the 0321/0326/0348/0353 runs. DSM retries while aio requests are in flight; with nothing outstanding it returns -ENOMEM (the persistent-allocation case). The first DSM error per engine is now logged with its cause. The shim passes `cc -fsyntax-only -Wall -Wextra` against real SPDK headers on lab-1, a mock probe exercises the DSM retry semantics, and the vals-spdk and vals-store tests pass. A hardware exhaustion test was not run because the 0353 drives are held.

Commit: fix(0358): SPDK sync read/write/flush retry -ENOMEM on qpair request exhaustion (like aio paths); DSM retries while aio in flight, else returns; dealloc_step logs first DSM error per engine
