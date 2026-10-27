---
id: "0203"
title: Production-scale capacity-profile ATProto slice gate
status: blocked
priority: 4
created: "2026-08-13"
updated: "2026-09-29"
depends_on:
    - "0202"
    - "0238"
    - "0248"
    - "0250"
spec_refs:
    - bench/task-0200-atproto-backfill/FINDINGS.md
    - plans/bootes-soak-runbook.md
    - docs/deploy.md#Data-node deployment profiles
---

## Description

2026-09-29 disposition: retained as conditional capacity work, blocked on selecting the intended production geometry/scale, the recorded fleet-health prerequisites, and explicit corpus-retention/provisioning approval. Later full-cache runs supersede the old 17M-event checkpoint in the historical log, but do not establish this production sizing gate. Do not extrapolate one-drive test boxes into an unselected fleet design.

Run the go/no-go backfill validation required by task 0200 on the proposed multi-drive production geometry: a fresh current-tree capacity-profile cluster, sorted records/backlinks zones, final value encoding, distribution-derived real-key presplits, and a statistically useful real corpus slice (target at least 0.5–1% of 24B if hardware permits).

Acceptance criteria:
- Capture source/binary/config/drive identity and proposed raw/usable geometry.
- Sweep 1/2/4/8 persistent 1,000-entry producer lanes with lossless Busy retry and same-key ordering.
- Run long enough to cross multiple flush/merge generations; retain per-zone/per-shard used/free segments, run index/value bytes, merge write bytes, RSS/swap/RAM headroom, run reads/probes, range ownership/skew, throughput/tails, and correctness.
- Extrapolate from the worst shard (not cluster average) to 12B records + 12B backlinks at RF=2 with maintenance and failure headroom.
- Produce an explicit fleet-size and expected-runtime recommendation, or identify the blocker and stop the full backfill.

## Acceptance criteria

## Work log
- Owner-approved triage: remains proposed, not an authorized fleet run. Add PD scale (0238), corruption (0248), and fleet-health (0250) prerequisites. Latest fresh replay crashed at about 17M events before capacity rejection; it cannot establish a sizing pass. A separately selected production multi-drive geometry is also required. If different validated hardware removes the 0250 confound, explicitly revise that prerequisite rather than silently ignoring it. Do not extrapolate the current one-drive fleet as the proposed production geometry. Balance execution is not mandatory if sizing retains measured worst-shard skew margins; any assumption of active balancing requires 0261/0262 first.
