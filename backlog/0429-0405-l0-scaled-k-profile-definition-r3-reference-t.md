---
id: "0429"
title: '0405 L0: SCALED-k profile definition, R3 reference trajectories and preregistered comparison bands'
status: done
priority: 3
created: "2026-10-11"
updated: "2026-10-11"
depends_on: []
spec_refs:
    - docs/validation.md
---

## Description

Split from 0405 (L0/L1 part; runner wiring into run_full1.py and the L2b calibration arm stay in 0405). run_full1.py carries 0403/0409's uncommitted in-review work and the calibration arm needs R3's drives after 0418, so the L0 part lands separately.

Scope (new files under bench/0405-scaled-profile/ only):
1. `scaled_profile.py`: derive SCALED-k from R3's recorded configuration at pin 8736b2b1. Every wall-setting cap scaled, with the reason; knobs deliberately left unscaled listed with reasons; fixed overheads measured, not scaled; stop thresholds in relative terms; `runner_overrides(k)` in the shape run_full1 needs (sizing, settings, env, stops) so the later hookup is small. k=1 must reproduce R3's config exactly.
2. `r3_reference.py`: extract normalised trajectories from an arm directory (R3 now, the calibration arm later): shard fill per zone vs ACK·k, runs per engine, frozen/resident/stranded runs, shadow/forced folds (count, size, transient fraction), RSS per live key excluding fixed overhead, live keys per ACK (prefix-shape check). Compare a candidate against the reference with preregistered bands.
3. `R3-REFERENCE.json` (reference signatures) and README.md: knob table, chosen k recommendation, preregistered bands, data-shape caveats, validity limits, launch prerequisites, remaining 0405 work.
4. Unit tests (k=1 identity, scaling, unscaled knobs, band pass/fail, extraction on a synthetic arm).

Acceptance: tests pass; reference extracted from retained R3 data; README preregisters bands before any calibration launch. Tier L0.

## Outcome

bench/0405-scaled-profile/: scaled_profile.py (k=1 reproduces R3 config exactly; k=8 → zones 32511/26594 MiB, 256 MiB merge cap, 512 MiB flush budget, 8 GiB pin cap, 32 MiB journal, 2Mi key-filter min keys, RAM budget F+(B−F)/k=9.97 GB with F=4.85 GB measured, 150 s 0402 window, ACK limit 1.306B), r3_reference.py (normalised fill/runs/frozen/stranded/RSS/fold signatures + compare with preregistered bands; forced-fold transients report-only), R3-REFERENCE.json (87/87 self-checks), README, 13 unit tests. Independent reviewer re-extracted the reference exactly. 0405 remains todo for run_full1 SCALED-k wiring (after 0403/0409 commit) and the L2b calibration arm (after 0418).

Commit: bench(0405): SCALED-k profile from R3 with knob rationale, R3 reference wall signatures and preregistered calibration bands; 0405 keeps runner wiring and the L2b arm
