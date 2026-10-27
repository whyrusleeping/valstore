---
id: "0213"
title: 'Cleanup Tier 2: split oversized source files into modules'
status: done
priority: 2
created: "2026-08-16"
updated: "2026-08-16"
depends_on: []
spec_refs: []
---

## Description
Split per the survey layouts (tests out first, then production modules): vals-store engine.rs (15.7k → engine/ with winner/recovery/write/read/flush/merge/reclaim/space/digest/scan + tests/); vals-cluster node.rs, net/server.rs, pd.rs, replica.rs; vals-client coord/mod.rs, wire.rs, net/client.rs; vals-bench main.rs. Pure code motion — no behavior change, no reformatting of moved code, keep public paths stable via re-exports. Commit per crate.

## Acceptance criteria

## Work log
