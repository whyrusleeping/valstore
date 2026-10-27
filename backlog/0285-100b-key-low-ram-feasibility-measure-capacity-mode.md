---
id: "0285"
title: '100B-key low-RAM feasibility: measure capacity-mode lifecycle memory and cold-I/O budgets'
status: done
priority: 2
created: "2026-09-08"
updated: "2026-09-09"
depends_on: []
spec_refs:
    - docs/out-of-core-index.md
    - docs/blind-append-lww.md
---

## Description

User wants vals closer to a friend's rack-scale target: ~4 servers, 100B logical entries, ~10–30TB logical data, 5–10M application r/w ops/s, replication/HA and predictable latency, 4–8 CPU cores and 16–32GB RAM **per server** (user-confirmed). Hold both budgets fixed across A/B arms; include serving, maintenance, networking and control-plane CPU plus heap, metadata/cache, DMA/hugepage allocations and lifecycle memory peaks. No uncounted helper cores or off-budget memory. Begin feasibility at 8 cores/32GB per server, then assess 4 cores/16GB; report achievable throughput rather than relaxing the resource envelope. This proposed first step is measurement and a design decision, NOT acceptance of a storage rewrite or hardware wipe.

Confirmed workload constraints: ordered scans are required and must not be traded away for a hash-only layout; writes are largely inserts and overwrites are rare; batching is acceptable. Target throughput need not hold through a rebuild, but rebuild must complete correctly within the same CPU/RAM envelope. Super-overwrite performance is a possible future zone-level configuration, not a priority or accepted implementation in this effort. User prefers exploiting enterprise NVMe power-loss protection to avoid explicit drive flushes where safely supported; this is a desired optimization, not an established durability guarantee. Validate exact device/cache completion-to-persistence semantics, drain host-side buffers and await successful write completions before durable acknowledgements, and preserve crash-consistent metadata ordering; PLP does not establish distributed consistency or protect host RAM. Keep required flush/FUA paths unless a qualified device contract justifies elision.

Establish remaining workload details: RF/read/write quorums, acknowledgement durability and consistency contract, exact drive/cache configuration, key/value distribution, insert key-order locality (monotonic vs random), point-read locality/miss rate, scan frequency/length and pagination/consistency expectations, batch sizes, numeric tail latency, and reduced rebuild/failure service expectations. Measure current capacity-mode fully flushed corpora at increasing cardinalities and realistic history. Account filters, per-block fence keys, segment/range metadata, active/sealed/flush state, AE/rehome seed winner tables and complete sidecars, duplicated compaction metadata/build buffers, runtime/DMA memory. Reconcile allocated capacities and RSS with lifecycle peaks during reopen, seed, churn and maintenance. Measure cold reads/probes, CPU/op including maintenance, physical bytes/key and write amplification.

Compare bounded-memory candidate designs only after baseline: paged/partitioned filters and hierarchical fence indexes with bounded root/cache residency; streaming or bounded-partition exact AE seeding; bounded maintenance construction and global reservations. Subsequent hypotheses (not implementation scope), now prioritized for insert-heavy ordered workloads: inline tiny values vs separated vlog (point reads and ordered scans versus compaction write amplification), lower run overlap, and efficient explicit batching. Price blind unconditional writes without a full winner directory if paged filters make fresh-key absence checks I/O-bound; do not prioritize a specialized overwrite path. Bounded hot caches remain workload-dependent. Include scan key ordering, continuation, version/tombstone resolution, and maintenance interference in all relevant gates. Preserve exact key/version/tombstone/AE correctness and avoid trading foreground gains for unbounded background debt.

Acceptance: reproducible baseline artifacts and explicit memory/I/O/CPU budget; identify which structures fail target extrapolation; select smallest next A/B or document infeasibility under fixed constraints. Use open-loop latency with offered/completed/rejected load and ensure steady maintenance debt. Scaled tests establish mechanisms, not proof of full 100B capacity. No hardware runs without availability/authorization checks; unresolved 0248 correctness investigation remains a release blocker, not grounds to stall read-only sizing.

## Acceptance criteria

- [x] Reproducible increasing-cardinality, fully-flushed local capacity baseline with lifecycle RSS and source-accounted run structures.
- [x] Low-overwrite/tombstone history and exact ordered pagination, value, version and tombstone checks.
- [x] Cold file probes report offered/completed/rejected load, latency, CPU and actual read bytes without claiming NVMe behavior.
- [x] Physical allocation/write amplification and stable maintenance-debt qualification are explicit.
- [x] Inclusive 8-core/32 GiB and 4-core/16 GiB ledgers, workload assumption matrix and RF sensitivity are recorded.
- [x] Current-design infeasibility and the smallest next bounded-residency A/B are documented.

## Outcome

Added reproducible local capacity benchmark, source-resident accounting, and four verified 100k/250k/500k lifecycle arms with exact scans/writer/version/tombstone checks, arrival/service latency, harness drops, RSS/I/O/CPU provenance and fixed-budget report. Current 100B/four-node representation fails even RF1: blooms ~35.6GiB/node, run metadata ~93.7GiB/node; AE seed independently O(keys). Independent review accepted after measurement corrections. vals-bench 32 tests and vals-store 318 tests passed (7 ignored); script/checksums/diff checks passed. No fleet operations. 0283 blocked pending explicit authority/restore contract; proposed 0288/0289 capture optional bounded-metadata/AE experiments.

Commit: Measure capacity lifecycle costs and document low-RAM infeasibility
