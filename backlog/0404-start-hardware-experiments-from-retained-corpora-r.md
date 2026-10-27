---
id: "0404"
title: Start hardware experiments from retained corpora (registry + --from-corpus arm mode) instead of replaying from zero
status: done
priority: 2
created: "2026-10-09"
updated: "2026-10-12"
depends_on:
    - "0403"
spec_refs: []
---

## Description

## Description
## Why
Most open hardware items ask about late-regime behaviour:
- 0390: shard CPU growth by 2.5B;
- 0391: decay past the drive step;
- 0401: post-drain heap growth;
- 0402 and 0383: disk transients and frozen-run garbage near EOF;
- 0393: run-count costs.

Today every experiment replays from zero (BEFORE discard) just to reach that state, which costs 3–8 h before the interesting part starts. The R3 corpus (10.17B ACK, drained, retained) already shows the alternative: 0401 can be reproduced by restarting the fleet on it.

This task makes "start from an existing corpus" a supported, recorded harness mode. It reuses the fleet-restart machinery from 0403 (resume).

## Scope
1. **Corpus registry.** Keep a small machine-readable file, e.g. `/data/dataset/corpora.json`, mirrored to a committed `bench/CORPORA.md`. Each entry records:
   - the source attempt, server pin/binary SHA and on-disk format versions (run format, superblock/topology V3);
   - the layout/geometry sha and the bootes cursor;
   - ACK and live keys per node, and drive bytes written since the last discard (relevant to the 9100 PRO post-discard slowdown);
   - whether it was drained, and its correctness status (verified / unverified / known-bad);
   - its state: `pristine`, `consumed-by:<attempt>` or `discarded`.

   A BEFORE discard must update the registry. The harness refuses to discard a corpus marked `reserved`.
2. **`--from-corpus <id>` arm mode.** Start the fleet on the corpus without discard or format, and verify the format/topology and geometry match. A different server binary is allowed and expected, because that is the experiment; the harness verifies it can open the format. Then run one of:
   - (a) read-only workloads: samplecheck, read probes, scans, aediag;
   - (b) a continued ingest from the saved cursor to EOF or to a planned ACK;
   - (c) a fixed-duration synthetic overwrite/read load (vals-bench) on top of the corpus.

   The run records the corpus id and its state at start.
3. **Single-use rule.** Read-only arms leave the corpus pristine (best-effort; AE/merges may still run, so record it). Write arms mark it `consumed-by`. A/B arms need either two corpora at the same point, or an A/B design that tolerates order (A then B on the same corpus, with a crossover on the next corpus). Write that rule down.
4. **Imaging is not a prerequisite.** Document the consumable-corpus limitation and any already-known imaging option in a short note. Do not implement imaging, copy a multi-TiB corpus, or run a hardware feasibility study in this task; propose separate work if a concrete write A/B needs it.

## Caveats to record in the runbook
- Format-changing builds cannot use old corpora. For those, use a full replay or a scaled-down arm (0405).
- Throughput measured from a reused corpus is not comparable with fresh-discard arms, because drive state differs. Compare only arms started from equivalent corpus states.
- A corpus built by binary X and continued by binary Y is a mixed-history state. Use it for diagnosing and confirming, not as release-gate evidence. The full 0386 replay remains the release gate.

## Acceptance
- Registry and `--from-corpus` are implemented, with unit tests:
  - refuse discard of a reserved corpus;
  - refuse a format/topology mismatch;
  - the state transitions are recorded.
  - Existing runner tests stay green.
- Tier **L0/L1**: test the registry, refusal/state transitions and `--from-corpus` lifecycle with local/mock integration, including changed-binary history and a no-discard/no-format command trace. Import the retained R3/0401 identity and lifecycle receipts as registry fixtures; they are evidence for the reopen procedure, not proof that the new command ran.
- First hardware use piggybacks on the next planned retained-corpus experiment, with one reopen and a short cursor/value probe; no dedicated samplecheck-sized load or reproduction of the already-fixed 0401 bug. Budget that probe within its parent experiment and record pending confirmation separately from software closure. Do not start new ingest or consume R3 solely to validate the registry.
- A runbook section under plans/. The 0386 README, and any new task that needs late-regime state, cites the corpus id it starts from.

## Acceptance criteria

## Outcome

Software closure done; hardware confirmation is pending as 0418 step 1. corpora.py is the registry (flock, atomic writes, mirror bench/CORPORA.md): states pristine/consumed-by/discarded, a reserved flag, every event logged, each open's binary recorded, mixed-history flag. dealloc.py refuses a discard of a reserved corpus or with no registry, and marks the corpus discarded just before the blkdiscard. run_full1 adds corpus-register (entry built from receipts), corpus-open read-only/synthetic (reopen and hold, no discard or format, changed binary recorded, cursor and format checks) and resume --from-corpus (identity gate before fleet contact, pre-ingest hold via INGEST/STOP, consumed when bootes starts). R3 is registered, reserved for 0418, unverified, with 5 imported 0401/0411 reopens (3 binaries, mixed). Runbook: plans/corpus-registry-runbook.md. test_corpora.py adds 11 tests (real-R3 fixture, command trace with no discard or format); all 199 runner tests pass; 2 reviewers accepted.

Commit: feat(0404): corpus registry with reserved-discard guard; run_full1 corpus-register, corpus-open and resume --from-corpus; R3 registered reserved for 0418

## Hardware confirmation (0418, 2026-10-12)

- First hardware use in 0418 (bench/0363-compact-values/R3-SESSION-0418.md): `resume --from-corpus R3` passed the corpus gate before any fleet contact. The cursor was ready in 79 s and the format check matched (v5). There was no discard, format or provision. The pre-ingest hold lasted 7,949 s and INGEST released it; R3 was then consumed. **Confirmed.** Note: `final.json`'s top-level `server_sha256`/`server_commit` still show the attempt's build pin on a mixed-binary resume. The binary that actually ran is in `server-install.json` and `resume.runtime`.
