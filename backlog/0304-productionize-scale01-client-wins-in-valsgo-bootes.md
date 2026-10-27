---
id: "0304"
title: Productionize SCALE01 client wins in valsgo/bootes sink (batched routing, presized frames, parallel ordered encoding, in-flight window)
status: done
priority: 2
created: "2026-09-22"
updated: "2026-09-25"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why

Task 0297 SCALE01 diagnostic clients (`bench/0297-ingest-1m/scale01/real-client06..09`) showed the accepted client/sink, not the server, capped realistic ingestion: RWMutex reader atomics 13% and per-key double routing 11.5% of client CPU; `encodeBatchApply` growslice/memmove 41%; single admission goroutine ~90% busy on json.Marshal; sink keeps ≤50k KV in flight (one flush at a time, 50k buffer cap) ≈ 700k/s ceiling. Fixes took 465k -> 941k with server gate 32.

## What

Upstream to vals-examples/valsgo and bootes (sibling repos, owner-approved changes only): `batchRouter` (one config snapshot + per-range memo, `TestBatchRouterMatchesPlacement`), `Clock.nowN`, presized BatchApply frames; bounded parallel value encoding with strictly ordered admission (`PrepareRecord`/`PrepareBacklinks`/`PushPrepared`, parity test); a larger bounded in-flight window (buffer cap or overlapping flushes with the per-key ordering guard and unchanged cursor/barrier semantics); gate-aware depth defaults. Records presplit guidance for hot collections (config).

## Acceptance

Production bootes (not the replay harness) reaches the SCALE01 direct-replay rate within the 0297 comparability rules, with cursor/drain/loss-free semantics and all existing sink tests passing.

## Acceptance criteria

- [x] Preregistered ABBA gate (bench/0304-client-productionize/README.md): mean FULL (production bootes from sealed cache, production defaults) ≥ 0.90 × mean REPLAY (real-client09 SCALE01/DUAL01 flags) on MASTER0306; all runs pass exact ACK/per-zone counts, zero drop/reject/oversize, full-value samples, cursor readback, clean stop, no cores. Round 1 0.876 (not accepted, source-bound); round 2 after cross-block cache pipeline 0.965 (accepted).
- [x] Existing and new bootes/valsgo tests (incl. -race ingest) pass; committed valsgo 1a9cf1c, bootes ee09476 (pre-existing cache feature) + 7684920 (0304).

## Acceptance criteria

- [x] Preregistered ABBA gate (bench/0304-client-productionize/README.md): mean FULL (production bootes from sealed cache, production defaults) ≥ 0.90 × mean REPLAY (real-client09 SCALE01/DUAL01 flags) on MASTER0306; all runs pass exact ACK/per-zone counts, zero drop/reject/oversize, full-value samples, cursor readback, clean stop, no cores. Round 1 0.876 (not accepted, source-bound); round 2 after cross-block cache pipeline 0.965 (accepted).
- [x] Existing and new bootes/valsgo tests (incl. -race ingest) pass; committed valsgo 1a9cf1c, bootes ee09476 (pre-existing cache feature) + 7684920 (0304).

## Outcome

SCALE01 client wins committed. - valsgo 1a9cf1c: routing snapshot, nowN, presized frames. - bootes ee09476: the pre-existing cache feature, committed separately. - bootes 7684920: sink window depth 16 / batch 4000 / cap 800k, ordered 16-worker parallel transform, cache read-ahead and cross-block pipeline. Preregistered ABBA gate: - round 1: 0.876 (source-bound, not accepted); - round 2: FULL 1.279M vs REPLAY 1.325M ACK KV/s = 0.965, accepted. All 8 runs passed every gate; fleet stopped clean. Follow-ups: 0307, 0308 (proposed).

Commit: bench(0304): production bootes client gate — FULL 1.28M vs direct replay 1.33M ACK KV/s (0.965x, accepted); real-client12 harness, run_gate.py, evidence for 8 attempts; valsgo 1a9cf1c, bootes ee09476+7684920
