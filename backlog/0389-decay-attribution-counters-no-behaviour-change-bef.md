---
id: "0389"
title: 'Decay-attribution counters (no behaviour change) before the next full replay: shadow refresh, write sources, per-shard writes, lookup/probe callers, broker debt, merge phase time'
status: done
priority: 1
created: "2026-10-06"
updated: "2026-10-06"
depends_on:
    - "0385"
spec_refs: []
---

## Description

Split from 0385 (bench/0363-compact-values/DECAY-FINDINGS.md, "Missing counters to file before the next run"). The existing counters could not isolate several decay terms. Add Prometheus counters/gauges only — no behaviour change — so the next full replay (0386) can attribute them.

Counters (see the findings doc for rationale):
- Shadow-estimate refresh (`refresh_shadow_estimates`): refresh count, gated vs ungated engine count, wall/CPU time, sampled blocks/read bytes, newer-run bloom probes.
- Physical write bytes by source: vlog normal / tail / maintenance rewrite, flush run output, merge output, catalog/metadata incl. alignment; run bytes split data / index / filter / footer.
- Per-shard synced writes/ops, so busiest/median can be computed from service counts rather than a CPU proxy.
- GET / Digest / existence lookup counts; bloom probes/positives by caller; covered vs uncovered key-filter positives; apply run-probe time.
- RAM broker: deferred bytes and unique wait duration by class; pending merge-debt bytes (not only in-flight scheduled input).
- Merge: per-kind output bytes; useful merge CPU / input read / build-verify time per merge.
- Per-engine resident/frozen runs and free fraction, exported so the harness can sample them at CPU cadence.

Acceptance:
- The counters above exist (or a short note says why one is infeasible/too costly), have HELP text, and are scraped by the 0363 harness samples (run_full1.py sampled gauges) and observer.
- Unit/integration tests where a counter's semantics are non-obvious; a short A/B or micro-check shows no measurable write-path cost (owner prefers measured-neutral).
- analyze_decay.py reads the new counters when present (null when absent).

## Outcome

All 0389 counter families are exported with HELP/TYPE. AE and other generic probes count as caller=exists; server Digest probes count as digest. Write-source bytes reconcile exactly to device bytes in a test, and the broker, shadow-cache, caller and per-shard semantics are tested. The run_full1.py samples keep the labels, observer.jsonl already stores full bodies, and analyze_decay.py fills fields when present and leaves them null when absent. COUNTERS-0389.md documents the approximations: the merge-debt proxy only counts plans already denied RAM, and input-read time includes decode. Local release A/B, 48 samples per arm: append +0.28% (95% CI [-1.49%, +1.96%]), p50 380/380 ns. That is neutral at about 1–2% resolution, not proof for SPDK or a full replay.

Commit: feat(0389): decay-attribution counters (observability only): shadow refresh, write bytes by source, per-shard writes, lookup callers, broker debt, merge phase time, per-engine gauges; harness + analyze_decay consume them
