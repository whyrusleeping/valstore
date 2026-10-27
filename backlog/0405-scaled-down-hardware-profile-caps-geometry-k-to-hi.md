---
id: "0405"
title: Scaled-down hardware profile (caps/geometry ÷k) to hit disk, merge-policy and RAM walls at ~1/k of the data — calibrated against R3
status: done
priority: 3
created: "2026-10-09"
updated: "2026-10-12"
depends_on: []
spec_refs: []
---

## Description

## Description
## Why
Several walls only appear 8–10B ACK into a full replay:
- the disk planned stop: V3-R2 at 8.07B, and R3's shadow-fold transient at 10.17B;
- the frozen-run tier and tombstone-GC stalls (0383);
- the RAM wall: SOAK5B at 4.19B, before the per-key RAM cuts.

These walls are driven by bytes per shard, keys per node and runs per engine relative to configured caps, not by wall-clock time or drive physics. Shrinking the caps by a factor k should make the same structural regime appear at about 1/k of the data. With k≈8 that means ~1.3B ACK and about 1 h, instead of 8–10 h.

**Use it only where it makes sense.** In scope: disk/space-amplification walls, merge-policy regimes (frozen tier, forced/shadow folds, cap behaviour) and per-key RAM walls.

**Not valid for:**
- absolute throughput, late-regime write decay or the drive write step (0390/0391; 9100 PRO behaviour after ~1 TB written);
- SLC cache effects;
- anything that depends on fixed per-node overheads, which do not scale with k.

These stay on full-scale or retained-corpus runs (0404).

## Scope
1. **Define a scaled profile with factor k.** Identify every cap that sets a wall and scale it, documenting each one:
   - region/partition geometry: records/backlinks MiB per zone and the presplit range count, keeping per-shard balance;
   - merge input cap and frozen threshold;
   - seed pin cap (`ae_seed_pin_max_bytes`);
   - the RAM budget/guards, or a cgroup memory cap. Fixed overheads are measured, not scaled.
   - Also list the knobs deliberately left unscaled: block size, bloom bits, key-filter FPR targets, and so on.
2. **Calibrate against R3** (SOAK5B-V5-R3, raw data in `/data/dataset/task0363-results/SOAK5B-V5-R3/`). Run the scaled profile at the R3 pin to about 10.45B/k ACK. Compare the normalised trajectories:
   - shard fill % against ACK/k;
   - runs per engine, frozen-run count and stranded runs;
   - occurrence, size and transient fraction of shadow/forced folds;
   - RSS and allocator in-use per live key, excluding measured fixed overhead.

   Preregister the agreement bands before the run, e.g. fill trajectory within ±5 points and fold transient fraction within ±25%. If the profile does not reproduce R3's late shadow fold and fill trajectory, it is not a valid proxy. Record which signatures do and do not transfer.
3. **Make it reusable.** Add a `SCALED-k<k>` arm family to `run_full1.py`, or a sibling runner, with its own disk/RAM stop thresholds expressed in the same relative terms (10% free, RSS fraction). Add runner unit tests.
4. **Optional, if cheap:** a local file-backed version of the same profile at a much larger k for the pure merge-policy/space questions (0383/0402 logic), as an L1 step before hardware.

## Acceptance
- Calibration findings under `bench/` state which wall signatures the scaled profile reproduces. Use the preregistered bands.
- Tier **L0/L1** for profile calculations, runner wiring and comparison with retained R3 data; **L2b** for the calibration itself, which is this task's hardware goal. Budget one profile/arm, approximately 10.45B/k ACK (choose k before launch), and at most 2 h total preparation/ingest/checks. Use targeted value/resurrection checks and the registered disk/RAM stops; no full 5B post-run suite by default. If signatures do not transfer or the regime is not reached, record that limitation and seek approval before another factor/arm.
- Apply 0402's already-validated disk rule to the same calibration trace offline as the first use, or piggyback a chosen 0383 design on that arm. No separate hardware arm to revalidate 0402, and no dependency on finishing a future 0383 fix. Do not deliberately exhaust a shared corpus: real-exhaustion stop cases can be fault-injected locally.
- docs/validation.md lists the scaled profile as the L2b tier, with its explicit validity limits.

## Acceptance criteria

## Outcome

SCALED-k<k> runs in run_full1.py: R3's pin, layout and lifecycle with runner_overrides. Scaled config and env are checked per host. Stops: the ACK limit, the 0402 150 s disk window and the scaled RSS stop. Post-drain checks are targeted and capped at 2 h. k=1 reproduces R3. The single L2b arm, SCALED-k8, took 24 min and ended at the disk stop at f=0.974. All correctness checks passed: cursor, exact counts and 1,239,105 value Gets. Verdict: **not a valid proxy** (CALIBRATION-K8.md). Mean fill, run counts, RSS/key and shadow-fold size transfer. Records worst-shard fill does not (prefix placement skew ~1.5x), nor do merge counts; f=0.98 was not reached. Offline, R3's 1200 s 0402 rule would not have stopped. docs/validation.md is updated. Corpus K8 is registered and 0434 is proposed. No more arms without owner approval.

Commit: bench(0405): SCALED-k arm in run_full1 and the k=8 L2b calibration vs R3 (not a valid proxy: prefix placement skew, merge counts); 0402 rule applied offline; corpus K8
