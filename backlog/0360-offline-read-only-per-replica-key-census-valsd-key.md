---
id: "0360"
title: Offline read-only per-replica key census (valsd keyscan) + expected-id diff + runbook for the held 0353 corpus
status: done
priority: 1
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

Split from 0355. 0355 must identify the 3 extra backlinks keys (census 1,090,186,121 vs 1,090,186,118; digest delta 0x1ccdf183e889d1ebdd0d89ccb = sum of three 96-bit ids) on the held lab-1/3 corpus WITHOUT starting daemons (maintenance/AE/PD would mutate evidence). `valsd dump` is a raw vlog scan (all history incl. merge rewrites and freed segments), not the live set, so it cannot identify them. This task builds the missing tooling and the reviewed collection runbook; running it on hardware stays in 0355.

## Outcome

Tooling to identify the 3 extra backlinks keys on the held 0353 corpus without starting daemons. `valsd keyscan` opens only existing zones via Engine::open, writing census ids per partition (pass 1) and, for chosen ids, JSONL with every memtable/run version and its vlog record read back (pass 2). A MemDriveSet regression test (memtable + runs, overwrite, tombstone, missing zone, partition errors) proves zero write/flush/deallocate calls and unchanged bytes. Hand-written SHA-256 passes standard vectors. real-client14 gains census-ids (self-checked vs fixture) and census-diff. plans/0355-readonly-keyscan.md is the hardware procedure; execution stays in 0355. Local cargo and Go tests pass; the SPDK-linked build is only verifiable on-box.

Commit: feat(0360): offline read-only valsd keyscan (census ids + per-version detail), real-client14 census-ids/census-diff, runbook plans/0355-readonly-keyscan.md; 0355 depends on 0360
