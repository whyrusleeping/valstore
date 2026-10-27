---
id: "0451"
title: 'Range deletes 0444b: run format v6 — per-block max_hlc, RT block, footer rt_seq_floor, RT-only runs (format only)'
status: done
priority: 3
created: "2026-10-14"
updated: "2026-10-14"
depends_on:
    - "0443"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0444 (docs/range-deletes.md §1 "Persistence"/"Block version bound", §8 Engine). vals-store only; no range-delete semantics yet.

## Outcome

Run format v6 is the default: per-block max_hlc in the index (8 B/block resident, EncodeCheck cross-checked), index trailer [rt_seq_floor][rt_off][rt_len], optional CRC'd RT block (decode-compared before write, byte-compared on build verify); footer layout unchanged so pre-0451 readers reject v6 loudly. New range_tombstone module (RtRecord, codec); builders take RunRts; DeviceRun exposes block_max_hlc, rt_seq_floor, read_range_tombstones; RT-only runs build/open. VALS_RUN_WRITE_VERSION=5 rollback window; merges write max(configured, newest non-sidecar input); v5→v6 growth charged by output version in planning and allocation (review fix + regressions). Kill-the-fix verified. Docs updated. RT-only engine audit moved to 0452.

Commit: feat(0451): run format v6 — per-block max_hlc, index trailer (rt_seq_floor, RT-block locator), CRC'd range-tombstone block, RT-only runs; VALS_RUN_WRITE_VERSION 4|5|6; v5→v6 rewrite growth in merge planning
