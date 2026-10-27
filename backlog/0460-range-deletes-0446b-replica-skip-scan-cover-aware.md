---
id: "0460"
title: 'Range deletes 0446b: replica skip-scan — cover-aware staged scan (rt_next_block windows, CoverCursor filter), covered-work budget + resume key, Scan fragments trailer for bit-3 peers, Err to non-bit-3 scans over F, vals_scan_rt_* counters'
status: done
priority: 3
created: "2026-10-15"
updated: "2026-10-15"
depends_on:
    - "0459"
spec_refs:
    - docs/range-deletes.md
---

## Description

Part of 0446 (docs/range-deletes.md §2 replica side, §8 metrics). vals-store staged scan + vals-cluster serve_scan.

## Outcome

Replica side of range-delete scans done. `Engine::stage_scan_page_covered` (used by `serve_scan`; unchanged path when F is empty; cleanup keeps the plain scan) picks blocks with `rt_next_block`, filters covered winners incl. memtable, and cuts pages at `SCAN_COVERED_BUDGET` (65,536 filtered or 64 read blocks holding a filtered winner, per page) with a resume key, possibly empty. Bit-3 peers get F clipped to `[lo, coverage)` + resume, with a key-ordered walk keeping entries+fragments within SCAN_REPLY_BUDGET; non-bit-3 peers get Err only when the window meets F. vals_scan_rt_* counters added, rt engine statics exported. Staged path tested against sync + model on all skip-boundary cases, pre-fold and mixed budget cases, trailer/Err rules; kill checks fail. Review accepted round 2. 0461/0447 work logs note resume/fragment handling still needed in coordinator and re-home copy.

Commit: feat(0460): range-delete replica skip-scan — cover-aware staged scan, covered-work budget + resume keys, Scan fragments trailer for bit-3 peers, old-client Err over F, vals_scan_rt_* counters
