---
id: "0276"
title: Confirm retained-catalog SPDK shutdown behavior after executor cancellation fix
status: done
priority: 2
created: "2026-09-08"
updated: "2026-09-09"
depends_on:
    - "0259"
    - "0271"
spec_refs: []
---

## Description

Task 0259 reproduced and fixed an unbounded executor-task wait using a reopened retained-run fixture. It does not prove the historical five-minute lab shutdown stalls had that mechanism: old logs show drain started but lack task stacks or intermediate stage markers. The unchanged SPDK SubmittedIo drop, dump/maintenance joins, and engine sync may still block.

If accepted, run a coordinated non-destructive retained-catalog SPDK stop/restart gate under active recovery/seed work. Use rebuilt matching protocol-v2 binaries/clients and obey upgrade constraints. Preserve corpus, PD state, cores, matching archived binaries, and sealed cache; no provisioning/wipes. Capture shutdown stage timestamps and live stacks before any deadline if progress stops. Verify graceful process exit and direct-node persisted readback, and record serving/polling latency while cooperative work is active. Distinguish an observed pass from proven attribution of historical incidents; implement further fixes only with stage-specific evidence.

## Outcome

Accepted after independent review: non-destructive RF2 retained-catalog gate with source-pinned protocol-v2 daemon byte-identical to retained writer. Conclusively active-seed shutdown completed gracefully in 28.8/33.4s; additional stops 3.5–3.7s, all below 300s. Live stacks isolate bounded maintenance join, not proof of historical executor stalls. Restart and counterpart-down 100k-key full-value reads passed on both replicas. Corpus/PD/cache/cores preserved, fleet stopped; active-read contention and ongoing AER recorded. Evidence in bench/0276-hardware-20260909. Task 0194 marked blocked on separately authorized redundant-PD deployment. Pre-existing backlog changes preserved.

Commit: Validate retained-catalog SPDK shutdown and restart on RF2 hardware
