---
id: "0294"
title: Remove sustained ingestion throughput collapse with measured fixes and matched SPDK validation
status: done
priority: 1
created: "2026-09-11"
updated: "2026-09-28"
depends_on: []
spec_refs: []
---

## Description

Owner explicitly accepted continuing until ingestion is fast: identify dominant bottleneck, implement targeted fix, and demonstrate repeatable sustained-ingest improvement in matched hardware runs without weakening durability, per-key ordering, or correctness. This follows baseline0293, not a claim0293 proved causality or cleared0248. Use short matched experiments and independent review before final sustained gate. No numerical improvement tolerance or target has been owner-specified; report absolute acknowledged throughput and tails alongside source-event rate, compare equivalent corpus progress and whole arms, and do not declare success from a brief empty-store burst.

Starting evidence bench/0293-sustained-ingest-20260911/{FINDINGS.md,CORRECTIONS.md,analysis-corrected.txt}. Baseline54.8M source events/3611s~15.2k/s average, late~6k/s,112M accepted records+backlinks, source5c27de53. Device-sync execution mean~0.98→0.91ms while local apply~3.50→9.15ms and response-ready~5.24→17.21ms. Four serving threads/node; peak perTID94%, CPU/event increases, two Go ordered zone lanes may amplify slow calls. Rehome correlation is confounded with growth; no client wait timings or reliable targeted server stacks. Automatic balance executor OFF does not disable separate default-ON placement-debt rehome.

Plan: first add/test low-overhead bounded client/sink timing and progress telemetry, plus bounded full-byte sampled oracle at serialization/ordered dispatch if feasible; record exact dirty sibling source baseline and preserve unrelated work. Review post-run0293 harness repairs before any hardware. Run matched short RF2 W2 device-sync capacity-profile replay arms from equivalent fresh/source-cursor fixtures, rehome default vs VALS_PD_REHOME_ENABLED=0 diagnostic, all else fixed. Capture completed sink ops, client/per-destination wait/latencies,1s hot-TID CPU, valid target-thread perf, goroutine waits, server sync/apply/response hist deltas, maintenance debt, memory/headroom. Diagnostic suppression is not a production optimization or acceptable end-state. Choose smallest fix based on causal evidence; ordered key-partitioned client concurrency is only a hypothesis. Test and independently review changes; rebuild/source-pin SPDK on every deployment. Require repeat matched gain with necessary maintenance active, full ack/ordering/value checks, and final sustained run sufficient to expose baseline collapse and show maintenance debt behavior. Never improve apparent throughput by dropping/skipping data, buffered acks, weakening RF, disabling required maintenance, or accumulating unbounded debt.

Owner authorizes benchmark replacement of test corpus/PD state under existing guarded offline provisioning; use same3hosts RF2 single explicitly non-HA test PD, not0194/0280 gate. Preserve sealed replay cache, useful unresolved-corruption cores+matching binaries; no OS disk mutation, no format=true. Preflight each run, finite wall and safety limits, adaptive core disk reserve, no auto restart on crash, safe stop clients/nodes/PD and verify actual source-named exe/MainPIDs. Stop and report0248 recurrence rather than rerunning blindly. Existing staged0279/0287 and untracked0293 plus dirty bootes cache work are unrelated and must stay intact; no stash/reset/blanket format/commit. Record accepted work phases and residual risks honestly.

## Outcome

Objective met via successor tasks 0295→0296→0297/0309 (owner-accepted 2026-09-27): LONG01 D13/D14 on production defaults sustained 1,146,904/1,152,680 ACK KV/s over 400.7M KV with every quartile ≥1.04M, debt converged after drain, exact correctness; 0321 ingested the full 2.09B-KV cache on 3-node RF2 with exact census. The 0294 baseline collapse (~100k→22k ACK KV/s) no longer occurs. Own candidates: e06 epoch reuse retained; lanes2/hash/P-core not adopted; phase14 overlay retired by owner (0310). Remaining ingest work tracked in 0316/0323/0324/0326/0327 and 0248.

Commit: backlog(0294): close — sustained ingestion collapse removed by owner-accepted 0297/0309 program (LONG01 D13/D14 1.15M ACK KV/s over 400.7M KV) and 0321 full-cache soak
