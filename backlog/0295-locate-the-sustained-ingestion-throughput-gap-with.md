---
id: "0295"
title: Locate the sustained-ingestion throughput gap with matched direct-write, mutation-trace, and bootes benchmarks
status: done
priority: 1
created: "2026-09-16"
updated: "2026-09-18"
depends_on: []
spec_refs: []
---

## Description

## Description
## Owner request and purpose

Owner requested a handoff task for the performance-gap investigation proposed after task0294's failed sustained candidate comparison. Historical million-write/s results versus current realistic ingestion at ~41k acknowledged KV/s and ~20k source events/s are concerning. Establish where throughput is lost before implementing another optimization. This task is accepted measurement/diagnosis work, not permission to adopt a speculative fix or claim historical workloads are equivalent.

Two distinct gaps must be explained:
1. Fresh-store realistic ingestion starts around100k acknowledged KV/s, already far below historical synthetic results.
2. The committed baseline degrades to~22k acknowledged KV/s as corpus and maintenance work grow.

Deliver a reproducible matched comparison across direct synthetic writes, direct realistic KV mutations, and full bootes ingestion; collect profiles in the slow state; recommend ONE evidence-backed next optimization. Do not run a broad tuning matrix or rewrite the benchmark framework.

### Closeout scope split

The goals, controls and constraints below remain the original historical requirement. Task0295 completed the source/workload audit, comparability analysis, accepted-transform fixtures and validation, maintenance reanalysis, reviewed staged matrix and safety tooling, one consumed cleanup transaction, and the failed build-only SMOKE handoff. Its first released attempt stopped at the mandatory all-host binary-equality gate before provisioning, daemon/client start, admission or any performance measurement. Task0295 therefore does **not** claim that the matched matrix ran, that the throughput gap was located, or that basic-path/client/growth attribution is complete.

Coordinator split all remaining hardware execution to task0296, which depends on task0295. Task0296 begins with an independently reviewed canonical-target BUILD-ONLY proof, then requires a newly released correctness smoke before any synthetic/direct-real/full ladder or slow-state profiling. The used task0295 runtime source and manifest `c87f89df…4380d` are frozen evidence with known noncanonical per-host build targets and an `os.syncfs` cleanup portability bug; they are not execution-ready and must not be fixed, repinned or retried in place.

## Mandatory starting evidence

- `bench/0294-ingest-optimization/phase17/20260914T171300Z-long/RESULTS.md`: completed BASE3600, failed CAND3600, identities, raw metrics/profiles and preservation obligations.
- Same directory `ANALYSIS-baseline.json`, `FAILURE-SUMMARY.json`, `CORE-TRIAGE.md`.
- `bench/0294-ingest-optimization/phase17/20260914T163300Z-smoke/RESULTS.md`: successful short bounded-oracle smoke; NOT sustained adoption.
- `bench/0294-ingest-optimization/phase15/20260914T125600Z-reverse/RESULTS.md`: two short pairs, pooled~+3.75% ACK and~27% less bookkeeping perACK; does not establish candidate sustained gain.
- `bench/0248-ram-check-20260915/RESULTS.md`:32GiB locked online test,13 patterns OK, stopped at45min during Bit Flip, ZERO complete passes. No detected error is not a RAM clearance.
- Task0294 worklog; task0248 (unresolved corruption); `docs/SPDK.md`, `ARCHITECTURE.md`, original benchmark artifacts for historical throughput.
- `spec.md`, existing deployment/provisioning runbooks and current `bench/0294-ingest-optimization/` runner/source guards before any implementation or execution.

### Observations to reproduce or explain, not assume causal

Completed baseline actual scrape-window rates: whole40,919 ACK KV/s and19,980 source events/s; first300s99,899 ACK/s; last300s22,060 ACK/s. Early→late successful records/backlinks PutBatch means~17/18ms→59/86ms; device-sync means stay~1ms. Projection ops/ACK~20→403, fold ops/ACK~20→323. Unrehomed debt0→112; substantial staged seed memory. Late hottest shard near one full CPU. Existing stack captures at t120/t600 do NOT directly profile the worst late-hour state.

Historical documented multi-million clustered results include8drives/64serving cores/loopback RF3; current ingestion uses3drives/12shards/real NIC/RF2W2, realistic sorted keys/values and active maintenance. Audit the exact historical result before selecting its closest reproducible reference. Do not equate source events, acknowledged logical KV mutations, RPCs, and physical replica writes.

## Scope and execution order

### A. Reconcile identities and plan the minimal matched matrix (local/read-only first)

1. Inventory current server/client commits AND dirty source, exact compiled inputs, workload controls and existing accepted benchmark entrypoints. Git HEAD alone is not source identity. Current live Rust tree contains an uncommitted inherited-split candidate; do not silently benchmark or adopt it. Use an isolated export of committed accepted server source as the reference, leaving the live tree and unrelated index untouched.
2. Verify historical throughput provenance: payload/key shape, sorted versus hash namespace, batch/concurrency, RF/W, durability, drives, shards, network, duration and corpus size. Present a compact comparability table, including unavailable facts.
3. Propose the smallest staged experiment matrix and explicit stage gates. Software/fixture work may proceed without touching the fleet. Obtain independent review of final source/runner changes and exact runtime inputs before hardware action. Reuse existing lifecycle/build/provision/safety machinery where practical; do not relax guards or create a new general experiment framework.

### B. Establish the current direct-write reference

4. Short direct-client synthetic benchmark on the accepted committed server using current lab hardware and SAME RF2/W2/device-sync settings as realistic ingestion. Use the closest supported historical small-payload shape, explicitly documenting unavoidable differences. This answers whether the basic clustered write path remains fast.
5. Run direct writes with actual ingestion-shaped keys/values and correct sorted namespaces, recording batch-size distributions, client concurrency, key/value sizes, insert/overwrite/delete mix, routing and shard distribution. Change only the workload dimension under examination. More concurrency is a separately labeled diagnostic, not an unannounced control change or production recommendation.

### C. Isolate bootes and ordered-client overhead

6. Prepare a bounded, source-pinned mutation trace from the sealed replay using the actual accepted bootes record/backlink transformation. Preserve mutation order, duplicates, deletes and namespace semantics. Hash/version the trace, record the exact source cursor interval and transformation/client provenance; independently verify trace counts and transformation parity. Keep huge corpora/cache immutable; avoid unnecessary disk copies, and do not invent synthetic keys while labeling them real.
7. Replay that trace directly through the SAME supported client batching/ordered-dispatch path, excluding source decoding and transformation but preserving per-key ordering, acknowledgement/durability semantics and comparable batching. Compare with full bootes processing the SAME source interval from equivalent initial state. Explicitly distinguish source admission, transformation, queue wait, batch fill, dispatch, per-destination wait, retries, acknowledged KV and final drain.
8. Identify whether gap resides in bootes transformation/admission, ordered client dispatch, server foreground path, or growth-dependent server work. Do not treat summed overlapping latencies or sampled hot-thread percentages as an additive wall-time budget.

### D. Measure the slow state and growth cost

9. Use a source/cursor-defined, reproducible grown-corpus fixture or freshly generated equivalent state. Reopening itself can trigger maintenance and affect performance; record stabilization/recovery and compare equivalent states. NEVER reopen the current crash corpus implicitly or use an old binary on candidate-written data.
10. Capture bounded profiles at the ACTUAL slow point, not only empty-store/t120/t600. Include relevant active shards (not just one convenient thread), perTID CPU, client waits/goroutines, local apply/response/sync latency, index/run lookup work, seed/fold/projection/cleanup activity, allocation/copying and memory headroom. Record profile sampling coverage and hybrid-PMU limitations.
11. Measure maintenance work both per unit time and per acknowledged KV, corpus size/range count, reasons for seed/reconciliation invalidation, repeated versus changed work, move completion/backlog and staged bytes. Denominator effects alone are not proof of increased absolute work. Leading hypothesis: range-local metadata changes induce unnecessarily broad rebuilding and compete with foreground writes; distinguish this from index growth/lookup cost and client serialization before designing a fix.
12. Compare whole matched arms AND common-progress brackets, early/late distributions and low-throughput tails. Never divide post-drain totals by nominal admission duration. Persisted cursor/ack drain and bounded full-value/version/tombstone checks remain required; sampling is not full-corpus truth.

## Constraints and current fleet safety gate

- Fleet was stopped after CAND lab3 crashed25m42 into the Sep14 long arm. Core-only triage supports a key-pointer bit12 clear detected during seed cleanup; root cause and relation to historical bit42 cases remain unknown. Do not attribute to candidate, hardware or software without evidence.
- Preserve lab3 `/home/user/vals/run/core.vals-shard.2308180` (12,469,178,368bytes; SHA256301dd5cdf1b7b388995b1c42ea786f3fa46d11b20fbec21b62de07776d073141) and matching `/home/user/vals0294-phase17-20260914T171300Z/20260914T171300Z-CAND/valsd-inherited-candidate` (SHAdaf4d2a9be5048244e2f9fcd6c4361cb67a6d4cf29937e4b121af6676aab9e12), old useful cores/binaries and sealed replay cache.
- Latest online RAM diagnostic Sep15 left workloads stopped, hugepages4096/free3983 unchanged, root about17GB free. This does NOT satisfy prior independent >20GiB root/≥4000 free-hugepage preflight. Verify current state; no automatic cleanup, hugepage changes or guard relaxation. Local sudo is unavailable; remote sudo-n was available. Record privilege/visibility limitations without workarounds.
- This task authorizes local software/fixture work and diagnostic planning, NOT an immediate fleet restart. Hardware stage requires explicit coordinator/operator scheduling and permission after fresh resource/core/process/source preflight and an agreed corruption-risk decision. Existing owner permission to replace TEST corpus/private PD under guarded offline provisioning does not authorize OS-disk operations, forensic deletion, BIOS changes, reboot, DIMM work or memory-test escalation. No second RAM test is implicit.
- Before every hardware arm: fresh source-checked SPDK builds on all participating hosts, frozen matching hashes, exact inputs/controls, no foreign workloads, finite admission/resource/progress/core guards, owned process identity and bounded cleanup. Stop at first real crash/corruption/safety/data/source/build failure; preserve evidence, stop fleet and report; no automatic retry or next arm.
- Preserve RF/W/device-sync, ordering and legitimate maintenance. No apparent win from dropped/skipped data, weaker acknowledgements, disabling AE/rehome/compaction, or accumulating unbounded maintenance debt. If a suppression diagnostic is separately approved, label it non-production and do not count as optimization/adoption.
- Prior lanes2 experiments did not establish sustained benefit and worsened some late/debt measures; do not adopt by assumption. Inherited-split candidate remains unpromoted. FrontCodedIndex reserve_exact preserves measured RAM savings and current admission does not charge Vec slack; geometric growth is not a free optimization. Study historical task0066 before changing it.
- No unrelated edits/commits/stash/reset/blanket formatting. Preserve staged0279/0287 work, sibling client/cache overlays, historical manifests and artifacts. Use new evidence directories; do not repin old receipts. This task does not authorize a performance fix or production adoption; propose a separate narrowly scoped implementation after diagnosis.

## Acceptance criteria

### Completed in task0295

- [x] Exact reference source/client/workload identities and historical comparability table recorded; logical/source/physical metrics clearly separated.
- [x] Minimal staged matrix and hardware safety/authorization gates independently reviewed; no production algorithm change or unsupported source substitution.
- [x] Bounded realistic fixtures independently validated against the accepted bootes transformation, ordering and source intervals, with durable hashes and repeatable generation/validation/replay commands.
- [x] First released attempt and blocker reported honestly: cleanup consumed, all three accepted-source builds succeeded but differed, and the mandatory hash gate stopped before provision/start/admission. Raw small receipts, build diagnosis and stopped final state are retained.
- [x] Existing BASE3600 maintenance accounting reanalyzed in absolute work/time and per-ACK terms. It supports sharply rising absolute seed/fold/projection work, without claiming client/server causality from overlapping metrics or early profiles.
- [x] Findings state basic-path and ingestion-client cost remain unmeasured, growth-dependent server work is supported but not fully attributed, correctness/resource/debt limits remain, and the exact next action is task0296's independently reviewed canonical-target BUILD-ONLY proof.
- [x] Protected artifacts, sealed cache, sibling overlays and the unrelated 222-path staged baseline are preserved; no TEST-device provision, daemon/client start, replay admission or performance arm occurred; fleet final state is stopped.

### Original execution criteria moved to task0296

- [ ] Direct synthetic, direct realistic mutation and full bootes/accepted-transform paths measured under matched controls from equivalent state.
- [ ] Actual-window/common-progress rates, source/logical/RPC/physical denominators, client phase/batch/queue/wait/retry distributions, correctness/drain/cursor, resource/debt/error and stop receipts retained.
- [ ] Reproducible grown-state run and actual slow-point profiles across relevant shards collected, with growth/maintenance/index/client costs distinguished and no additive interpretation of overlapping latency/work metrics.
- [ ] Dominant bottleneck attribution and one evidence-backed next measurement or optimization selected with acceptance/rejection criteria. No promise of historical 1M/s workload equivalence.

## Handoff output

Return a short executive summary plus `FINDINGS.md` and a reproduction/provenance index in a fresh bench directory. State separately: basic-path capability, ingestion-client cost, growth-dependent server cost, correctness/resource/debt limitations, and the exact next proposed action. Avoid another multi-hour soak until the shorter diagnostic stages justify it.

## Acceptance criteria

### Completed in task0295

- [x] Exact reference source/client/workload identities and historical comparability table recorded; logical/source/physical metrics clearly separated.
- [x] Minimal staged matrix and hardware safety/authorization gates independently reviewed; no production algorithm change or unsupported source substitution.
- [x] Bounded realistic fixtures independently validated against the accepted bootes transformation, ordering and source intervals, with durable hashes and repeatable generation/validation/replay commands.
- [x] First released attempt and blocker reported honestly: cleanup consumed, all three accepted-source builds succeeded but differed, and the mandatory hash gate stopped before provision/start/admission. Raw small receipts, build diagnosis and stopped final state are retained.
- [x] Existing BASE3600 maintenance accounting reanalyzed in absolute work/time and per-ACK terms. It supports sharply rising absolute seed/fold/projection work, without claiming client/server causality from overlapping metrics or early profiles.
- [x] Findings state basic-path and ingestion-client cost remain unmeasured, growth-dependent server work is supported but not fully attributed, correctness/resource/debt limits remain, and the exact next action is task0296's independently reviewed canonical-target BUILD-ONLY proof.
- [x] Protected artifacts, sealed cache, sibling overlays and the unrelated 222-path staged baseline are preserved; no TEST-device provision, daemon/client start, replay admission or performance arm occurred; fleet final state is stopped.

### Original execution criteria moved to task0296

- [ ] Direct synthetic, direct realistic mutation and full bootes/accepted-transform paths measured under matched controls from equivalent state.
- [ ] Actual-window/common-progress rates, source/logical/RPC/physical denominators, client phase/batch/queue/wait/retry distributions, correctness/drain/cursor, resource/debt/error and stop receipts retained.
- [ ] Reproducible grown-state run and actual slow-point profiles across relevant shards collected, with growth/maintenance/index/client costs distinguished and no additive interpretation of overlapping latency/work metrics.
- [ ] Dominant bottleneck attribution and one evidence-backed next measurement or optimization selected with acceptance/rejection criteria. No promise of historical 1M/s workload equivalence.

## Outcome

Completed scoped source/historical audit, independently validated real mutation fixtures and reviewed diagnostic tooling, absolute maintenance reanalysis, and preserved authorized cleanup plus first build-gated smoke failure. No provisioning, server/client start, throughput arm, or bottleneck/adoption claim. Host-specific generated raft-proto build paths explain binary mismatch. Remaining canonical-build proof, matched hardware ladder and actual slow-state attribution explicitly split to accepted task0296. Independent closeout review accepted; 13 runtime tests and Go tests pass; unrelated staged222-path hash unchanged, protected artifacts/cache retained, fleet stopped.

Commit: bench: preserve throughput-gap fixtures, provenance, and failed build handoff
