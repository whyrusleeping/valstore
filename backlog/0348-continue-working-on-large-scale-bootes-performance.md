---
id: "0348"
title: Continue working on large scale bootes performance
status: done
priority: 3
created: "2026-09-30"
updated: "2026-09-30"
depends_on: []
spec_refs: []
---

## Description

We need to be able to hit >=1M kv writes per second on bootes ingest, basically indefinitely as long as we have the disk for it. Ideally we can show a run up to 5B records without seeing degradation, and also handle any node crashes along the way

## Outcome

Investigation milestone; the remaining goal is split into follow-up tasks. - SCALE0348-M1 (2 nodes, master, full cache) failed at 1.105B ACK KV, averaging 386k. - The injected SIGKILL recovered: writes resumed in 27 s. - A needrestart restart is now excluded on all boxes. - lab-2 hit bit-42 corruption (logged to 0248; drive held). - node1 hit a ram_broker assertion (0349). - The OLD/NEW ABBA found no server regression (about 665k each). - A blkdiscard (NUSE 70% → 0) gave +20–24% with 8× lower stalls. Drive FTL state is the dominant slowdown, and vals never TRIMs (fix: 0350). - 0352 carries the ≥1M demonstration. - 0351 (5B-record corpus and disk) is blocked on an owner decision.

Commit: bench(0348): 2-node full-cache crash-tolerant scale run, server-version ABBA and drive-deallocation discriminator; needrestart excludes vals units; follow-ups 0349-0352
