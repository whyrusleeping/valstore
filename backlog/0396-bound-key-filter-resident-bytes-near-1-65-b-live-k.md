---
id: "0396"
title: Bound key-filter resident bytes near ~1.65 B/live key (growth-chain slack, stale inserts) without raising absent-Apply reads (0395 split)
status: done
priority: 2
created: "2026-10-07"
updated: "2026-10-07"
depends_on:
    - "0395"
spec_refs: []
---

## Description

Split from 0395 (2026-10-07), which must land first: it exports the key-filter capacity and retired-generation gauges.

## Outcome

Key filter now grows in 20%-of-chain steps and, at 4 sub-filters (or stale), rebuilds a shadow generation sized ~1.1x estimated keys via dual-lane flush/merge sinks plus rebuild scans; probes use only the active generation until every run is shadow-covered, then swap atomically (no false negatives; RAM-only, restart starts clean). VALS_KEY_FILTER_REBUILD=0 restores legacy. Local 100M-key file-backed A/B: EOF 1.618 vs 2.384 B/live key (−32%), rebuild high water 335.6 MB reported, max checkpoint FPR 0.78% vs 1.42%, pooled absent per-run probes −49%/block reads −46% (pointwise small increases at 20M/30M), scan-excluded ingest +1.6–3.4%, scan-inclusive (serialized harness) −9 to −10%. Review fix: merge lanes reserve Σ all input entries (prevented a 60%-FPR tail overfill) and empty runs get scanned. Two reviewers accept. Hardware ACK/overlap/concurrent-merge slack confirmation deferred to 0398 (noted in its task).

Commit: feat(0396): bound key-filter RAM via 20% growth steps and incremental shadow rebuild into an exactly sized generation; rebuild gauges; 100M local A/B 1.618 vs 2.384 B/live key
