---
id: "0297"
title: Drive realistic ingestion toward sustained 1M acknowledged KV/s with measured bottleneck fixes
status: done
priority: 1
created: "2026-09-18"
updated: "2026-09-27"
depends_on:
    - "0296"
spec_refs: []
---

## Description
## Owner request and objective

Owner accepted the plan discussed after task0295: establish the current realistic write ceiling, eliminate growth-dependent maintenance cost, increase useful ordered write parallelism where justified, and optimize bootes transformation only if measured. Capture this as a staged performance program, not a promise that current hardware can deliver millions of realistic writes/s.

First engineering target: **1,000,000 acknowledged logical KV mutations/s sustained**, with RF2/W2, device sync, required ordering and normal legitimate maintenance. This is NOT 1M source events/s, RPCs/s, or physical replica writes/s. The observed ingestion mix produces roughly 2.1 logical KV mutations per source event; record the actual ratio for each arm. Higher multi-million targets follow only after the first target's resource/correctness envelope is demonstrated.

Task0296 owns the prerequisite canonical-build correction, correctness smoke, matched direct-synthetic/direct-real/full-bootes diagnostic ladder and actual slow-state attribution. Consume its evidence; do not duplicate completed experiments. If0296 ends with an execution blocker rather than valid measurements, that blocker remains a gate for this task despite backlog dependency completion.

## Starting evidence and uncertainties

- `bench/0295-throughput-gap/FINDINGS.md`, `INDEX.md`, `COMPARABILITY.md`, `maintenance-absolute.json`, and task0296 results.
- BASE3600 fresh ACK rate about100k KV/s, late about22k; fold/projection/cleanup absolute work per second rose roughly3.6–4.4x. This supports a growth-dependent maintenance cost, not yet a proven initiating mechanism.
- Device-sync mean stayed near1ms while client/response latencies increased. This does not establish the saturation ceiling or rule out storage tail effects.
- Historical million-write/s workloads differ in key/value shape, concurrency, topology, durability and active maintenance; they are capability evidence, not a realistic target guarantee.
- The inherited-split candidate is unpromoted. Previous lanes2 experiments did not establish sustained benefit. Neither is approved for adoption by this task.

## Staged plan and decisions

### 1. Establish today's realistic clustered-write ceiling

Use0296's short ladder: current small synthetic writes (capability only), preloaded actual mutations through the accepted ordered batching client, then the exact same source interval through actual bootes processing from equivalent initial state. Keep trace decoding/validation/preload outside measured admission; report preparation separately. Label the bench transform/sink wrapper versus full production bootes orchestration and close that external-validity gap before attributing total ingestion overhead.

Separate key/value/namespace shape from concurrency. The high-concurrency synthetic arm versus one-lane real ingestion is confounded; do not attribute their entire difference to server shape. If necessary, run ONE targeted comparable-dispatch/concurrency discriminator rather than a tuning matrix. Establish whether direct-real writes can approach1M ACK KV/s on current hardware and identify the next limiting resource if not.

### 2. Remove growth-dependent maintenance cost

At matched corpus size/progress, profile the actual slow state and distinguish seed/fold/projection/cleanup from index/run lookup growth, foreground apply, allocation/copying and client serialization. Measure absolute work/time as well as work/ACK, invalidation reasons, repeated versus changed work, staged bytes and move completion/backlog.

Leading hypothesis to test: range-local metadata changes trigger unnecessarily broad/repeated rebuilds. IF supported, prototype the smallest range-local/incremental correction; if refuted, follow the measured dominant cost instead. Do not redesign maintenance on hypothesis alone.

Accept a candidate only when matched A/Bs show lower absolute CPU/work at equivalent progress, useful sustained ACK improvement, preserved correctness and bounded/converging maintenance debt. Reject gains caused by deferred reconciliation, disabled AE/rehome/compaction, or accumulating staged memory/debt. Recovering22k toward100k is worthwhile but is not a1M result.

### 3. Increase useful ordered-client parallelism, if server headroom exists

Measure queue backpressure, actual in-flight KV/RPC counts, batch fill/width, dispatch, per-destination waits, retries and acknowledgement latency. Identify where independent keys/destinations/streams are unnecessarily serialized.

Use Little's law as a capacity check, not a throughput prediction: at1M KV/s and20ms completion latency, roughly20k KV must be genuinely in flight (about20 full1000-entry batches). Buffered entries are not in-flight entries; loaded latency may change with concurrency.

Prototype bounded parallelism only at independently orderable boundaries, preserving per-key ordering, acknowledgement/durability semantics, bounded memory and retries. More input buffering is not a fix. A separately labeled concurrency diagnostic is not a production recommendation. Require improvement at grown state, not only fresh store; reject late/debt regressions like those seen in previous lane experiments.

### 4. Optimize transformation only if implicated

If preloaded direct-real admission is materially faster than matched full bootes after controlling dispatch and initial state, profile source decoding, CBOR/JSON construction, allocations/copies, hashing and orchestration. Optimize the measured hot cost, retaining actual transformation parity, operation order, duplicates/deletes, namespace semantics and loss-free cursor/drain behavior.

If direct-real and full ingestion are similarly limited, reject transformation work as the primary lever and focus elsewhere. Do not turn this into an unrelated serialization rewrite.

### 5. Check physical budgets before promising millions

Measure actual key/value/wire bytes, batch/RPC overhead, physical fanout/retries, device write amplification, CPU per acknowledged KV, per-shard skew, NIC utilization and memory headroom. Separate aggregate cluster traffic from the bottleneck of an individual client/node link.

Illustration only (not a measured mean):600B/logical mutation at1M KV/s is600MB/s of logical payload, roughly1.2GB/s of RF2 replica payload across the cluster before amplification. At2.1KV/source event,1M events/s would be1.26GB/s of logical producer payload—already around one10GbE link's theoretical limit before overhead. Replace these estimates with measured workload sizes.

Scale producers/NICs/shards/nodes only after a demonstrated saturation budget warrants it; report the required topology/cost and per-node efficiency. Do not buy apparent throughput with weaker semantics or workload simplification. Hardware expansion requires an explicit owner decision, not an implicit procurement/deployment mandate.

## Execution/change discipline

Work one evidence-backed candidate at a time. Create narrowly scoped implementation/validation child tasks for concrete fixes once justified, rather than combining several algorithms or a broad tuning matrix in one commit. Keep measured-neutral/negative paths out of production. This task authorizes the staged performance work, not adoption of an unmeasured candidate.

Inherit0295/0296 fleet safety and source-isolation controls: fresh source-checked matching SPDK builds with canonical target paths and pinned toolchain/runtime libraries; explicit reviewed runtime manifests; equivalent fixtures and RF2/W2/device-sync settings; finite resource/progress/core/drain guards across the entire live lifecycle; owned process cleanup; stop/preserve/report at first real corruption/crash/source/build/data/safety failure with no automatic retry. Respect protected artifacts and unrelated staged/dirty/sibling work. Never reopen candidate-written crash data with an incompatible reference binary. User fleet/TEST-wipe/cleanup authority does not remove these correctness gates.

## Acceptance criteria

- [ ] Valid0296 prerequisite measurements consumed, with capability, realistic server, client/orchestration and growth costs separated; any unresolved execution blocker addressed before optimization claims.
- [ ] Measured throughput/resource budget identifies the dominant constraints and feasible concurrency/topology envelope; logical KV/source/RPC/physical units and workload shape are explicit.
- [ ] Each attempted fix has a stated mechanism, narrow changeset, matched A/B evidence and explicit accept/reject decision; no speculative optimization adopted. Correctness, ordering, persistence, memory and maintenance-debt gates pass.
- [ ] Full bootes is compared against the demonstrated direct-real ceiling from equivalent state, with phase timing and actual-window/common-progress distributions, including low-throughput tails. No nominal-duration division of post-drain totals or additive overlapping latency budgets.
- [ ] Before sustained validation, preregister the duration/cursor/corpus-growth bracket and tail/debt/resource rejection gates based on earlier evidence. The test must reach representative growth/maintenance pressure, not just empty-store startup. No multi-hour soak before shorter diagnostics justify it.
- [ ] Deliver either a reproducible >=1M sustained acknowledged logical KV/s result under the preserved contract, OR an explicit measured ceiling and remaining bottleneck/scaling proposal. A below-target outcome must be labeled target unmet, not success by altered workload/semantics or extrapolation.
- [ ] Retain exact sources/builds/commands, raw metrics/profiles with coverage limits, mutation/cursor/drain/full-value/version/tombstone check receipts, maintenance/resource/error/stop results, concise findings and reproduction index. Sampling is not full-corpus truth.
- [ ] Recommend exactly one next evidence-backed action with acceptance/rejection criteria; final fleet state documented and stopped after runs.

## Non-goals

No guarantee of1M source events/s or multi-million realistic KV/s; no weakening RF/W/device sync; no dropped/skipped data for throughput; no maintenance suppression counted as a production win; no blanket concurrency increase, unrelated benchmark framework rewrite or speculative broad redesign.

## Work log

### 2026-09-21 — consume completed task0296 diagnostic; no0297 execution yet

Task0296 completed the owner-approved two-node diagnostic with accepted SYNTH02 capability, matched REAL04/FULL04 and actual early/slow-state PROFILE01. Entry point: `bench/0296-throughput-ladder/FINAL-FINDINGS.md`; detailed clock-safe attribution and exactly one proposed discriminator in `two-node-profile01/ATTRIBUTION.md`.

Matched realistic rates143,235.62direct /129,539.19transform-sink ACKKV/s; the9.562%single-pair difference is not transformation-primary proof. Late~4MACK state has overlapping seed/map reconstruction and foreground run-index/flush growth; primary causal split remains open. No1Mtarget result oroptimization adopted. Next proposed measurement is dynamicmaps versus identical finalboundaries installed beforewrites, preservingRF2/W2device-sync/normalmaintenance and comparingactualprogress/debt withaccept/reject/inconclusivecriteria. This handoff is not a released experiment.

Nodes1/2/privatePD cleanly stopped with PROFILE01v2testcorpus;lab3excluded. Preserve rawprofiles/matchingbinaries/cache/protectedcores per0296RETENTION. Do not replay old releases or apply inherited-split candidate without new review and validation.

### 2026-09-22 — stage 2 executed: RANGEMAP01 two-node A/B (reseed fix rejected as lever; flush-slice candidate positive but not adopted)

Entry point `bench/0297-ingest-1m/rangemap01/FINDINGS.md` (preregistration in `README.md`, per-arm numbers `SUMMARY.json`, compact evidence + receipts `evidence/`, raw `/data/dataset/task0297-results/`). Per-second re-analysis of 0296 PROFILE01 (`bench/0297-ingest-1m/analysis/`) showed every write-phase range-map epoch adoption coinciding with a 1–3 s ACK drop to ~50k, so instead of the presplit discriminator (final boundaries are not exported by the retained PD) the narrowest fix was built and A/B'd: `277dac39` keeps logical counts and the leaf seed across placement-equivalent range-map epochs (3 reseeds per split → 1).

Five valid arms (BASE reproduced the 0296 binary `513d9e95` byte-for-byte; all gates PASS): BASE 145,388 / 133,384; CAND 145,859; FLUSH 152,717 / 144,964 ACK KV/s. CAND removed the reseeds exactly as designed (invalidations 24→12, seed starts 24→16, folds −37%) with **+0.3%** throughput → range-map reseeding refuted as the late-throughput driver; commit left unadopted on branch `0297-rangemap-reuse`. The deep stalls are synchronized multi-store memtable flushes whose on-core commit steps (4096 index removals ≈5.5 ms) inflate batch-apply latency 2.8→8.8 ms and client PutBatch 9.5→47 ms; they merely coincide with split epochs. Second candidate `1768d750` (commit slice 4096→512) gave +5.0% and +8.7% in two matched pairs (flush-second ACK +24–31%, stalls 11–16 s → 5–7 s, clean seconds unchanged) — below the preregistered +8%/+40% accept band, so held on branch `0297-flush-commit-slice`, not adopted.

Measured ceiling: clean seconds run at 200–219k = exactly 2 in-flight 1000-KV PutBatches / ~9.5 ms RTT (Little's law); shard threads ~35% busy; device sync ~1 ms. One-lane realistic ingestion cannot approach 1M on this path. Recommended next action: stage-3 concurrency discriminator (K independently orderable PutBatches per zone with per-key ordering guard, K∈{2,8}, bands in FINDINGS). Fleet: both nodes/private PD stopped cleanly after every arm, no new cores, lab-3 untouched; corpora are throwaway TEST provisions.

### 2026-09-22 (later) — owner direction: flush-slice merged; reseed fix re-tested under pressure and adopted

Merged `0297-flush-commit-slice` (`35680a9a`). Built `real-client05` (`--depth K` pipelined chunks per zone with per-key ordering guard) to push the server harder. K=8 collapsed to 39.6k ACK/s: the per-shard staged-apply gate (`MAX_STAGED_BATCH_APPLIES=8`) sheds Busy under 16 in-flight PutBatches and the sink's 1 s Busy backoff amplifies it (recorded, not tuned around). K=4 fits the gate: **265k ACK/s (+75% over the serial client), zero Busy, all gates PASS**, with a steep late drop (337k→216k) — the server-limited regime. In that regime master+range-map reuse gave **287.0k vs 265.0k (+8.3%) and 230.9k vs 200.9k (+14.9%)** in two matched pairs (post-first-split +18%/+23%, stalls 4→1 and 18→12, seed starts/folds −35…−60%, no restarts/sheds, staged bytes drain within ~4 s, exact counts/values/cursor). Meets the preregistered band → merged as `59d6b7bb` (+docs/algorithms.md note, +`3f22b2c5` test robustness). Main-tree overlay reconciled 3-way; its untracked `inherited_split_tests.rs` knockout assertion updated to the new master semantics (placement reuse no longer depends on the overlay). Full vals-cluster/vals-client/vals-store suites pass with the overlay applied. Evidence: `bench/0297-ingest-1m/rangemap01/FINDINGS.md` Result 4, `SUMMARY.json`, `evidence/PRESSURE01-*`. Best measured realistic rate now 287k ACK KV/s; target unmet; next limiter is the staged-apply gate + client Busy backoff (0298 retargeted). Fleet stopped clean after all ten arms.

### 2026-09-22 (evening) — SCALE01 capacity ladder: 287k → 941k ACK KV/s; client egress now the wall; 1M sustained unmet

Entry point `bench/0297-ingest-1m/scale01/FINDINGS.md` (prereg/knobs `README.md`, all numbers `SUMMARY.json`, compact evidence `evidence/`, raw `/data/dataset/task0297-results/SCALE01-*`). 34 two-node RF2/W2 device-sync attempts on the frozen trace, 33 PASS all gates (A7 = harness client-RSS guard trip, clean stop). Harness fix: server binary now installed as `valsd` so RSS/`pgrep -x valsd` guards are real (they were vacuous in RANGEMAP01, unit proofs held).

Ladder (whole-write ACK/s): records-zone DID-prefix presplit (config; one `like/` range held 67% of records → one shard ~45% of node work) 251k→333k; 2000/4000-entry chunks + 8 shards →465k; diagnostic client fixes (batched routing/stamping, presized frames, GC target, parallel value encoding with strictly ordered admission) →573–600k; staged-apply gate 8→32 with depth 8 →647k; sink in-flight window (buffer cap 50k→800k) with depth 16 →920k/**941k** (per-second up to 1.24M). Master + gate-32 alone reaches 930k. Client NIC tx 9.0–9.4 Gb/s = the measured 9.42 Gb/s line cap (~1.06 KB wire per ACK with client-side RF2 fan-out); nodes 18–23% busy. Rejected/neutral as throughput levers with evidence: async group-commit flush (+3.3%, inconclusive), node-wide flush domain (fewer flushes, lower latency/CPU, same throughput at the link bound), TCP rmem, GC off, NIC IRQ core. **Target 1M sustained unmet**; runs are 9–14 s with no auto-splits under the new presplit (capability, not sustained growth).

Branches (not merged): `0297-staged-gate-32` (`8f2b4649`, → task 0301 todo), `0297-async-group-commit` / `0297-node-flush` (→ 0303 proposed), `0297-diag-latency` (diagnostic only). Client work → 0304 proposed. One next action: server-side replica forwarding to halve client egress (0302 proposed, preregistered ≥1.0M with client tx ≤60%). Fleet: both nodes and private PDs stopped after every attempt, hugepages 4087/4096, no new cores, tcp_rmem restored, lab-3 untouched.

### 2026-09-23 — DUAL01: second client 10 GbE link moves the egress wall (1.15 M; NODEFLUSH diagnostic 1.41 M)

Owner added a second 10GBASE-T port on devbox. Per-destination routing (lab-1 via `eno1np0`, lab-2 via `eno1np1` 10.0.0.139, runtime-only) gives 18.86 Gb/s raw TCP aggregate. Harness now samples both NICs and records routes. Entry point `bench/0297-ingest-1m/dual01/FINDINGS.md` (prereg `README.md`, `SUMMARY.json`, `evidence/`). Same fixture as DEPTH01-K16, 3 attempts, all PASS. Master+gate32 reached 1,168,532 / 1,127,961 ACK/s whole-write (mean +26.4 % over DEPTH01-K16 908k), meeting the preregistered "wall moved" band. Link no longer binding (~4.3 Gb/s/port); the co-limiter is now server response latency under the gate-32 window (response-ready 15–17 → 19–29 ms, sync still ~2 ms). A single diagnostic NODEFLUSH run (0303 build) reached **1,414,490** (p50 1.38 M, +23 % over GATEONLY dual), with its peak seconds approaching the dual-link cap. Capability readings only (≈6–7 s writes, no auto-splits): sustained 1 M is still unmet. Recommended next: matched GATEONLY-vs-NODEFLUSH A/B on the dual link (0303), then a preregistered longer/looped sustained run. Fleet stopped clean, no new cores, lab-3 untouched.

### 2026-09-24 — 0303 flush domain merged (+14.2 % in the server-bound dual-link regime)

AB01 (`bench/0303-flush-domain-ab/`): NODEFLUSH 1,352,952 vs master+gate32 1,185,058 ACK KV/s (+14.2 %, min N > max G). Review fixes were confirmed on hardware (FIX02: fixed build 0.970× old build, within the band; the day's run-to-run drift was visible on both builds). Merged as `b74f5e30`. Master is now at roughly 1.25–1.35 M ACK KV/s on the dual-link capability fixture (6–7 s runs, no auto-splits). Sustained 1 M is still unmet: next is a preregistered longer/looped sustained run.

### 2026-09-24 — 0302 server-side replica forwarding: FWD01 rejected

Dual-link ABBA (`bench/0302-replica-forward-ab/FINDINGS.md`, 6 runs, all gates PASS): forwarding halves client bytes/ACK (1,207.8 → 612.0), but ACK/s drops to 0.626× master (816k vs 1,304k). The cause is per-range receiver choice: each node shard gets two half-size requests per chunk (1.77× server BatchApply, 2.3–2.8× device commits, gate-32 sheds), plus a serial 18–31 ms forward hop. The server stays unmerged on branch `0302-replica-forward`. The opt-in client side stays committed in valsgo/bootes, off by default. The per-(batch, shard) variant is proposed as 0306. Master stays at ~1.26–1.34 M on this fixture, and the sustained run is still this task's next step.

### 2026-09-27 — LONG01 D7–D12: rehome refold (0317) and split recount prefetch (0318) merged; first 400M-KV run passing every sustain gate

Entry point: `bench/0297-ingest-1m/long01/FINDINGS.md`, section "Split cadence, rehome refolds and the recount prefetch". Child task 0309 logs the detail.

- **Split cadence.** The default PD split cadence (one action per zone per 5 s) caps splitting at about 0.4/s. It also starved rehome. With faster polling:
  - **0317 fixes** order-only rehome commits refolding every zone store.
  - **0318** cuts split-recount shard time 15× (ABBA 1.169×).
- **D10.** 1.01M ACK KV/s over 400.7M KV on 2-node RF2/W2 device-sync with the production bootes client. Every progress quartile was ≥ 0.9M, the minimum 10 s window was 860k, 476 splits ran during the write, and maintenance debt converged 171 s after drain. This is the first run to pass every preregistered sustain gate.
- **Still unmet.** The repeat, D11, reached 911k. The 1 s cadence is an env knob.
- **Next.** One evidence-backed action: 0319, auto-split keeps pace by default. It needs a preregistered defaults A/B, then two default-config D runs against the sustain gates.
- **Fleet.** Stopped after every attempt, no new cores; lab-3 untouched.

### 2026-09-27 (evening): 0319 merged. First target met on the LONG01-D fixture, reproduced twice with production defaults

- **Change.** 0319 (`20a3cc5b`) makes the PD split every over-threshold range per zone per poll, capped at 16.
- **Result.** On current master with production defaults, D13 and D14 reached **1,146,904 and 1,152,680 acknowledged logical KV/s**. The setup:
  - 400.7M KV of real Jetstream-derived creates, about 2.1 KV per source event;
  - 2-node RF2/W2 device-sync, production bootes client, dual 10 GbE client links;
  - normal maintenance: auto-split, AE, compaction lanes and rehome enabled.
- **Gates.**
  - Every progress quartile is ≥ 1.04M; the minimum 10 s window is 960k.
  - About 495 auto-splits each; maintenance debt converged 21 s and 91 s after drain.
  - Every correctness gate passes: exact counts, samples, cursor, clean stops, no new cores.
- **Scope, as measured.**
  - About 350 s of writes on a store growing from 0 to 400M keys; not a multi-hour soak.
  - Nodes = RF, so every node holds every key.
  - All creates, no updates or deletes, so the next shapes to test are updates/deletes and more than 2 nodes.
  - Client links run at about 5.5 Gb/s each, about 1.2 KB of wire per ACK.
- **Decision needed.** Closing 0297/0309 on this evidence, or extending, is the owner's call. The candidate extensions are a larger E prefix, a longer run, 3+ nodes, and mixed updates/deletes.

### 2026-09-27: closed by the owner

- The owner accepted the D13/D14 result, 1.15M ACK KV/s sustained over 400.7M KV with production defaults, as meeting the first target. They also asked for a much longer bootes soak as follow-up work, filed as a separate task.
- Open related tasks:
  - 0320 (proposed): recount prefetch hardening.
  - 0307/0308 (todo): bootes client items.
