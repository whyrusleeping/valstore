---
id: "0222"
title: Fresh sorted-vs-unsorted read/write comparison on lab at current HEAD
status: done
priority: 2
created: "2026-08-19"
updated: "2026-08-19"
depends_on: []
spec_refs: []
---

## Description
The last clean spread sorted-zone benchmark (2026-08-06, task 0138 arm D) predates every 08/2026 perf change (winner directory 0183, write-path fix 0195, batched winner probing 0204, node-scoped maintenance 0208). Re-run the sorted-vs-unsorted comparison on current HEAD (db31462) on a freshly formatted lab cluster.

Plan:
- Deploy HEAD to lab-1/2/3, rebuild SPDK valsd; latency profile, 4 shards/node, RF=2, 20M keys × 256 B per zone.
- Format #1 (unsorted peaks, vals-bench): netload 20M 16×64 write+read, netread-fleet 8×12 30s sustained, netmultiread batch-256 modern batched reads. 
- Format #2 (matched valsmix five-flow session, DURATION=120 WRITERS=8 READERS=24 PIPELINE=64 TIER=majority): unsorted reads-under-churn + mixed 85/15; sorted (zone smix, 23 real-key presplits at valsmix/2a/… while empty, auto-split off) scans-under-churn, mixed 70/20/10, reads-under-churn, plus both bulk-load rates.
- Zero consistency errors required; archive under bench/, leave cluster stopped with format=false.

Note: this reformats the drives, destroying the preserved 0205 mature 16M read corpus (regenerable via bench/lab-read-ceiling-20260816-0205/capture-generation.sh).

## Acceptance criteria

## Work log
