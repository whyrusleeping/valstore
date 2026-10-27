---
id: "0463"
title: 'Range deletes 0447b: re-home copy applies Scan page fragments before entries and follows resume keys; re-home (copy + mirror, mirror failure poisons), inherited split inside a fragment and auto-merge across an RT boundary tests'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0462"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0447 (docs/range-deletes.md §5 "Confirmed re-home", "Inherited splits"). vals-cluster node/rehome.rs.

## Outcome

Re-home copy applies each Scan page's fragments (clipped to the moving range; a skew reject or error fails the page, which is retried) before its entries. It advances to the resume key, else last key ++ 0, and is done when !more. Tests: copy + mirror over the wire, using a cfg(test) zone reply budget and an RT-exchange pause; it fails if resume or the fragment apply is removed. Also: a range delete to a dead pending owner is not acked and bumps rehome_mirror_failures; a store-level split inside fragments plus merges across fragment edges (leaves, counts, reads); a node-level PD inherited split inside an RT plus auto-merge (reads, scans, RangeStatsV3 counts). Docs: range-deletes.md, zones.md. cargo test -p vals-cluster green; review accepted.

Commit: feat(0463): re-home copy applies Scan page fragments before entries and follows resume keys; copy/mirror, mirror-failure, inherited-split and auto-merge range-delete lifecycle tests
