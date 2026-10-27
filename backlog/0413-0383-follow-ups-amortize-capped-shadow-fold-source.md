---
id: "0413"
title: '0383 follow-ups: amortize capped shadow-fold source reads (multi-window folds), drop empty fold outputs, hardware confirmation via 0405'
status: done
priority: 3
created: "2026-10-10"
updated: "2026-10-12"
depends_on:
    - "0383"
spec_refs: []
---

## Description
From 0383 (capped shadow folds). On the L1 gate the capped fold streams 15–32 GiB of source index-block/sidecar bytes per 1.5M overwrite/delete ops, against ~1 GiB of merge writes. That is read amplification of roughly the engine's index size per fold.

Candidate work:
1. **Multi-window folds:** one source pass that rewrites several capped windows, each in place and each with its own output, to amortize source reads. Needs a multi-output commit. A/B it on `frozen_garbage_gate_l1` (read bytes, space, writes).
2. **Empty outputs:** a fold whose window is all garbage commits an empty run that stays in the run list. Remove empty outputs at commit instead.
3. **Hot-skew space:** on the hot-skew arm, max space reached 2.98× vs 2.55× on the old uncapped path. Partially dead value segments under the 50% victim threshold accumulate. Check whether purge-time relocation of the window's exclusively owned segments pays for itself.
4. **Hardware confirmation:** piggyback on 0405's scaled profile per docs/validation.md (no standalone arm without approval).

## Acceptance criteria

## Work log
- 2026-10-12: declined. R4 (0386) ran to EOF with ≥25% free in the worst shard, no disk stop and capped folds behaving, so fold read amplification is not what limits throughput. Write-path effort goes to 0420–0422 instead. Refile item 2 (empty fold outputs) if run counts ever show those empty runs piling up.
