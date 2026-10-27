---
id: "0146"
title: 'Intra-node shard scaling is sublinear: 4 shards/node deliver ~2.2× a 1-shard node (75k vs 137k reads/s/core)'
status: done
priority: 3
created: "2026-08-07"
updated: "2026-08-10"
depends_on: []
spec_refs: []
---

## Description
## The observation

Comparing the 2026-07-30 single-shard profile (`bench/lab-read-prof/notes.md`) with the 2026-08-06 4-shard runs (task 0138), both `vals-bench netread`, RF=2, 100-256 B values:

| config | serving cores (cluster) | sustained reads/s | per core |
|---|---|---|---|
| 3 nodes × 1 shard (2026-07-30, 100 B) | 3 | ~410,000 | ~137,000 |
| 3 nodes × 4 shards (2026-08-06, 100 B, 4M keys) | 12 | ~897,000 | ~75,000 |

4× the serving cores yields ~2.2× the throughput — per-core efficiency roughly halves. Established by the follow-up sweeps that this is *not* explained by:

- **working set / caching** — reads are flat at ~585k over 2M / 10M / 40M-key windows of the same LSM;
- **client limits** — 1 process × 16 coords (661k) ≈ 4 processes × 12 coords (639k), and the client box has 192 cores;
- **value size** — a 10× larger value costs only 1.5× (per-op bound, not per-byte);
- **the NIC link** — even the 1 KB arm only moves ~191 MB/s per node.

So something shared per *node* limits the aggregate: candidates are softirq/NIC IRQ handling on the single Realtek r8169 (one RX/TX queue set shared by four shard threads), LLC/memory-bandwidth contention between the four shard threads, or per-shard duplication of work (each shard holding its own index/bloom working set).

This matters for capacity planning: the standing advice from the July profile was "shard the node for more serving cores", and that lever is now delivering about half its nominal value.

## Directions
- Per-shard throughput breakdown: run 1, 2, 4 shards/node on the same dataset and plot reads/s per shard, with `perf stat` (LLC-misses, memory bandwidth) and softirq/IRQ distribution per arm.
- Check NIC queue affinity: are RX queues spread across the cores running the shard threads, or piling onto one? (RSS/RPS/XPS configuration, `/proc/interrupts`.)
- Re-test `iommu=pt` in the sharded configuration — it was ~14% of a single serving core in July but only ~1-2% of self time now, so it should be re-measured before spending a reboot on it.

## Acceptance criteria
- A documented per-core scaling curve (1/2/4 shards) with the limiting resource identified.
- Either a config fix (IRQ/queue affinity, `iommu=pt`) or a documented, understood ceiling that capacity planning can rely on.

## Artifacts
`bench/lab-bench-20260806/notes.md`, `G-vsize-sweep.txt`, `prof/*.perfstat`; `bench/lab-read-prof/notes.md` for the July baseline.

## Plan

1. Reconcile the task's quoted July/August baselines with the existing benchmark artifacts, especially the later distributed-client evidence, so the scaling curve compares equivalent sustained RF=2 reads rather than burst or client-limited runs.
2. Preflight lab-1/2/3 and devbox, preserve any active work, and only if the fleet is idle sync/rebuild current HEAD before measuring. Use the same key/value shape and a fresh dataset for each 1/2/4-shard arm, with identical RF/read consistency and enough external client processes to reach a stable ceiling.
3. For each arm capture repeated sustained aggregate throughput, per-node/per-shard serving CPU or operation deltas, host CPU/softirq distribution, NIC queue/IRQ/RPS/XPS state, and perf-stat cache/memory indicators. Keep raw commands and outputs under a task-specific bench directory.
4. Analyze whether the apparent per-core loss is a real node-shared server limit, placement/load imbalance, client ceiling, or an invalid comparison. Re-test or rely on directly applicable existing iommu=pt evidence only when the configuration is verified equivalent; apply a reversible affinity fix only if measurements identify one.
5. Document the 1/2/4 scaling curve, limiting resource, and actionable capacity-planning ceiling/config guidance; update related benchmark notes where needed. Leave all boxes clean and restore the standard 4-shard configuration.
6. Verify the report's arithmetic and provenance against raw artifacts and inspect the final diff for scope.

### Starting points
- bench/lab-read-prof/notes.md (July single/4-shard and distributed-client evidence)
- bench/lab-bench-20260806/notes.md (August 4-shard sustained and perf evidence)
- bench/sorted-shard-scaling-box-notes.md (earlier 1/2/4 methodology and client-limit diagnosis)
- scripts/netread-fleet.sh and scripts/profile-node.sh
- plans/bootes-soak-runbook.md for current lab sync/build/service hygiene

## Work log
- 2026-08-10 plan: 1. Reconcile the task's quoted July/August baselines with the existing benchmark artifacts, especially the later distributed-client evidence, so the scaling curve compares equivalent sustained RF=2 re
…[truncated]
- 2026-08-10 context hints: 5 recorded with plan
- 2026-08-10 context hints: Use timeout-wrapped ssh; do not disturb non-idle hosts or unrelated sessions.; The task's 410k and 897k figures may not be equivalent ceilings; inspect later July distributed-client measurements befor
…[truncated]
- 2026-08-10 preload: 7 file(s), ~61 KiB seeded into implementer context
- 2026-08-10 completed after methodology correction: The original 16×64 generator underdrove four shards. A complete devbox coordinator/depth sweep plus caveated colocated multi-source controls selected topology-tuned external generators and produced a corrected fresh RF=2 16M-key × 100 B curve of 443k / 745k / 1.191M reads/s (1.00× / 1.68× / 2.69×; 100% / 84% / 67% linear efficiency). Matched profiles at the actual ceilings show ~95% per-shard on-CPU over captures with a two-second pre-load lead (effectively saturated during the loaded overlap) and increasing per-read task-clock/instructions; this establishes serving-thread software CPU inflation but does not causally attribute it to a specific path. Complete inspectable client, perf, metrics, `/proc`, NIC/IRQ, config, script, and command artifacts are under `bench/lab-shard-scaling-20260810/corrective-raw/`. Fleet restored to inactive standard four-shard configs.
- 2026-08-10 implementer report: Completed task 0146. Rebuilt current HEAD (`34695b4`) locally and on the idle lab fleet, then ran fresh RF=2/R=2 16M-key × 100 B 1/2/4-shard arms with repeated sustained reads and synchronized per
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): revise — The change provides a useful same-build 1/2/4-shard dataset and good IRQ, CPU, cache, affinity, and corpus-state observations, but it does not establish that the measured four-shard rate is a server-s
…[truncated]
- 2026-08-10 review (claude): accept — The change fully satisfies task 0146's acceptance criteria: it delivers a same-build, same-corpus 1/2/4 shard scaling curve (394k/692k/684k sustained RF=2 reads; 100%/88%/43% per-thread efficiency), i
…[truncated]
- 2026-08-10 revision: Addressed the methodology review with a full corrective benchmark run.  - Re-ran the exact fresh 16M-key × 100 B RF=2/R=2 four-shard arm and swept devbox coordinator count, per-coordinator in-fli
…[truncated]
- 2026-08-10 review (sol): accept — The revision resolves the prior methodology and provenance issues. It re-runs the exact 16M-key workload with topology-tuned in-flight-depth sweeps, demonstrates saturated serving threads at the selec
…[truncated]
- 2026-08-10 review (claude): accept — The revision is a full methodological correction that supersedes the first pass: the original 16\u00d764 generator was shown to underdrive the four-shard arm (same corpus: 657k at q64 \u2192 1.18M at
…[truncated]
- 2026-08-10 revision: Completed the final verification cleanup.  - Normalized trailing spaces/tabs and excess blank lines at EOF in 173 committed raw text artifacts; substantive captured values/content were unchanged. Also
…[truncated]
- 2026-08-10 review (sol): accept — The latest revision is a non-substantive verification cleanup: it normalizes whitespace and EOF formatting in the captured artifacts without changing the benchmark values or conclusions. The analysis
…[truncated]
- 2026-08-10 review (claude): accept — All previous findings are resolved. The corrected report stands: the original low-depth generator result was properly superseded, the topology-tuned 1/2/4 curve (443k/745k/1.191M = 1.00×/1.68×/2.69�
…[truncated]
- 2026-08-10 decision: accept — commit: 0146: measure corrected 1/2/4 shard read scaling
- 2026-08-10 decision: accept — commit: 0146: record read benchmark methodology gotcha
- 2026-08-10 usage: 24,990,037 tok (in 3,543,984, out 114,341, cache_r 22,129,167, cache_w 108,473) · cost n/a (unpriced)
  implementer: 19,073,257 tok (in 1,863,822, out 80,475, cache_r 17,128,960, cache_w 0) · cost n/a (unpriced)
  coordinator: 3,623,864 tok (in 917,916, out 9,244, cache_r 2,696,704, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,281,574 tok (in 762,218, out 13,308, cache_r 1,506,048, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 11,342 tok (in 28, out 11,314, cache_r 797,455, cache_w 108,473) · cost n/a (unpriced)
