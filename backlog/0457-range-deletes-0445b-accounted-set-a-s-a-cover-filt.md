---
id: "0457"
title: 'Range deletes 0445b: accounted set A / S_A — cover-filtered seeds, dirty rebuilds, regroups, split recounts; acct(k) write maintenance; S_A to compaction; collected RTs recompute A'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0456"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0445 (docs/range-deletes.md §4 "Logical state excludes covered keys", "The accounted set and the RT fold" minus the fold, §6, §7 compaction filter). vals-cluster replica layer.

## Outcome

Replica keeps A/S_A (replica/rt_accounting.rs). A full seed filters with F as of its start and publishes A := F_S, S_A := S plus the engine accounted seq. Writes, blind directory writes and retires map the physical old winner through acct(k). Recounts/regroups filter with A; digest pages (rebuilds, served, planned, sync) skip covered winners in the engine merge without charging the budget. Collected RTs recompute A and restart an in-flight seed; clear() resets A; a seed that changes A remaps counts. 13 kill-the-fix tests; vals-cluster lib 883 and vals-store 537 pass; review accepted. The RT fold stays in 0458.

Commit: feat(0457): range-tombstone accounted set A/S_A — cover-filtered seeds publish A and the engine accounted seq, write/retire maintenance through acct(k), A-filtered dirty rebuilds/regroups/split recounts/digest pages, collected RTs shrink A
