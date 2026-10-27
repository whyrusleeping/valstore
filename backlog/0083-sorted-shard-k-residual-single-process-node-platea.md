---
id: "0083"
title: 'Sorted-shard k× residual: single-process node plateaus at ~0.65·k on point ops (845k w / 513k r at k=4 vs 1.16M/770k process-split) — find the shared-process bottleneck'
status: done
priority: 4
created: "2026-07-07"
updated: "2026-07-07"
depends_on: []
spec_refs: []
---

## Description
Filed from task 0050's box validation (oldbox, 2026-07-07; full data in `bench/sorted-shard-scaling-box-notes.md`).

The in-process `distsorted` harness (RF=1 R=1, one node × k SPDK shards, 256 ranges, n=500k vsize=64) shows sorted-zone point ops plateauing **below k×**: k=4 WRITE ~845-847k ops/s (2.74×) and READ ~513k ops/s (2.54×) vs the 1-shard baseline (309k / 202k), with the plateau flat from ncoord=24→32 (not a client-thread-count limit). A **process-split control** — four concurrent single-drive 1-shard distsorted processes on the same four drives — aggregates 1.16M writes / 770k reads / 11.56M scan keys/s = **3.77-3.92×**, so the per-shard serving path and the drives deliver ~k×; the cap is *process-shared* contention (one process holding the bench client threads + all 4 shard serving threads + 4 SPDK pollers). Suspects: global observe atomics (per-op counter increments ping-ponging cache lines across cores), allocator contention, memory bandwidth. Related: RANGE-SCAN throughput actively degrades with more client threads (k=4: 8.8M keys/s @nc16 → 2.7M @nc32) — likely the same class, client-side.

Note this is NOT automatically bench-only: a production valsd JBOD node also runs all its shards (and pollers) in one process — only the client is out-of-process.

## Acceptance criteria
- Measure a real `valsd` JBOD node (4 drives / 4 shards, e.g. RF=1 or the t0050 harness topology scaled up) driven by **out-of-process** clients at saturation: does per-node point-op throughput reach ~k× the single-shard rate, or does the ~0.6-0.7×k in-process cap reproduce?
- If it reproduces server-side: profile (perf) the shard serving threads and identify the shared bottleneck (observe atomics / allocator / other); file or implement the fix (e.g. per-shard sharded counters folded on scrape).
- If it does not reproduce: record that the cap is a bench-harness artifact in `bench/sorted-shard-scaling-box-notes.md` and (optionally) split distsorted's client into a separate process for honest k× numbers.

## Plan

Goal: determine whether the ~0.65·k in-process point-op cap (task 0050's distsorted, k=4 sorted shards in one process) reproduces on a REAL `valsd` JBOD node driven by OUT-OF-PROCESS clients at saturation — and if it does, profile the server and identify the shared bottleneck; if not, record it as a bench-harness artifact.

Hardware: oldbox (reachable via `ssh oldbox`, passwordless sudo; 96 threads, idle, verified clean: no valsd/vals-bench, no SPDK residue, data drives 0000:05:00.0/07:00.0/c1:00.0/c3:00.0 vfio-bound; OS RAID 09/0a off-limits).

Design — three topologies, same client rig, sorted zone rf=1 r=1, all on the box:
  A) Baseline: 1 valsd process, 1 drive / 1 shard.
  B) JBOD: 1 valsd process, 4 listens × 4 drives = 4 shards in one process (pdc `data_node = 1*4 @ ...` syntax, as t0050 used `1*2`).
  C) Process-split control: 4 valsd processes (nodes 1..4), 1 drive each, one cluster, rf=1 — the "everything split by process" analogue of 0050's control.
Client: `valsmix` from ~/vals-examples — a real out-of-process load generator over TCP loopback (sorted zone supported, self-verifying). Measure point WRITE (write-only foreground: --workload mixed --mix-write 100, or writers-only if cleaner) and point READ (--workload read) at saturation, sweeping client threads until the plateau; prove the client is not the cap by splitting the same total client load across 2+ valsmix processes and comparing the sum (per 0050's process-split lesson). vsize=64, count ~500k–2M to match the 0050 numbers.

Key trap — range spread: a fresh valsd sorted zone has ONE range, so all keys land on one shard/node (shard = hash64(range token) % k). Pre-split the zone into ~64–256 evenly spaced ranges via `valsd admin split-zone` (scriptable; valsmix key format is known) BEFORE measuring, and verify near-even per-shard key shares via the metrics endpoints / vals_live_keys before trusting any number. Also verify the client discovered all shard addresses (0050: clients upgrade a single-seed route from the membership address book).

Verdict logic:
  - If B ≈ C ≈ ~4× A → the in-process cap was a bench-harness artifact (client+server sharing one process); record that in bench/sorted-shard-scaling-box-notes.md per the acceptance criteria, and optionally add a cheap distsorted client/server process-split mode (env-gated) for honest k× numbers — only if it is genuinely small; otherwise file it as a follow-up.
  - If B ≈ 0.6–0.7 × C → server-side shared-process contention, production-relevant. Profile the JBOD valsd under saturated load (`perf record -g` / `perf stat`, `perf c2c` if available) targeting the shard serving threads; identify the bottleneck (global observe atomics / allocator / memory bandwidth). Implement the fix only if small and safe (e.g. per-shard counters folded on scrape); otherwise file a well-specified follow-up task with the profile data.

Steps:
1. Preflight on box (idle, vfio bindings, no residue) — already verified once; re-verify at run time.
2. rsync local tree (HEAD fa1cc01) → box ~/vals (recipe in plans/distnode-box-bench.md); `VALS_SPDK_LINK=1 cargo build --release -p vals-server -p vals-bench --features spdk`; rebuild ~/vals-examples (valsmix, valsq) against the synced tree (stale-client lesson from 0047).
3. Build a t0083 harness dir on the box (copy/adapt ~/run/cluster/t0050/{start.sh,startnode.sh,adm.sh,hygiene.sh,*.conf}): fresh format=true boots per topology; liveness can be off (gc_grace=0) — this is a throughput A/B, not a healing test; size zone_region_mib for the keyspace.
4. Run A, B, C; for each: load, pre-split ranges, verify spread, sweep client threads to plateau, record WRITE and READ plateaus (+ scan number if cheap). 0 consistency errors required for a run to count.
5. Branch on the verdict logic above (profile vs record-artifact).
6. Write a new "task 0083" section in bench/sorted-shard-scaling-box-notes.md with the full recipe, numbers, and verdict; leave the box clean (graceful teardown, hygiene.sh, drives vfio-bound); update the local repo only (bench notes + any code change).

Out of scope (file follow-ups instead): any large counter-infrastructure refactor; multi-machine client rigs (0014/0022 territory).

### Starting points
- bench/sorted-shard-scaling-box-notes.md — 0050 numbers, recipes, process-split control lesson
- box: ~/run/cluster/t0050/{start.sh,node1.conf,pdc.conf,adm.sh,hygiene.sh} — JBOD valsd conf (multiple listen+pci lines), pdc 'data_node = 1*2 @ a,b' shard syntax, 'zone = <name> <rf> <ordering>' boot zones, admin socket
- ~/code/vals-examples/valsmix/src/main.rs + RUNBOOK-valsmix.md + run-valsmix-spdk.sh — client flags (--nodes, --zone, --ordering sorted, --rf/--r/--tier, --writers/--readers/--pipeline, --workload read|mixed --mix-write, --skip-load)
- plans/distnode-box-bench.md — box sync/build recipe (rsync, VALS_SPDK_LINK=1 ... --features spdk), hygiene/recovery, preflight checks
- shard routing: within a JBOD node shard = hash64(range token) % k (see 0050 notes §B) — fresh sorted zone has 1 range; pre-split via 'valsd admin split-zone <zone> <key>' or point-op load lands on ONE shard
- bench/b1-box-ab-notes.md — drive reset recipe between topologies (setup.sh reset / dd-zero superblocks) if format=true proves insufficient
- distsorted reference: crates/vals-bench/src/main.rs workload_distsorted (VALS_DS_RF/VALS_DS_R/VALS_DS_NRANGES) — the in-process harness being compared against

## Work log
- 2026-07-07 plan: Goal: determine whether the ~0.65·k in-process point-op cap (task 0050's distsorted, k=4 sorted shards in one process) reproduces on a REAL `valsd` JBOD node driven by OUT-OF-PROCESS clients at satur
…[truncated]
- 2026-07-07 context hints: 7 recorded with plan
- 2026-07-07 context hints: bench/sorted-shard-scaling-box-notes.md — 0050 numbers to beat/compare: 1-shard 309k W / 202k R; k=4 in-process 847k/513k; 4-proc control 1.16M/770k; box: ~/run/cluster/t0050/{start.sh,node1.con
…[truncated]
- 2026-07-07 implementer report: ## Task 0083 — resolved: the sub-k× point-op cap is NOT a server-side JBOD bottleneck  ### What I did Measured a real `valsd` JBOD node driven by **out-of-process** `valsmix` clients on oldbox
…[truncated]
- 2026-07-07 review tier: single-opus — reviewers: Claude
- 2026-07-07 review (Claude): accept — Task 0083 is a measurement/investigation task; the deliverable is a recorded verdict, and the staged change is exactly that: an updated backlog file and a thorough new "Task 0083" section in bench/sor
…[truncated]
- 2026-07-07 decision: accept — commit: bench: 0.65·k in-process point-op cap is NOT a server-side JBOD bottleneck — real valsd JBOD node (4 drives/4 shards, one process) driven by out-of-process valsmix ≈ 4-process control (1.47M ≈ 
…[truncated]
- 2026-07-07 usage: 85,507 tok (in 156, out 85,351, cache_r 5,586,452, cache_w 238,978) · cost n/a (unpriced)
  implementer: 72,856 tok (in 128, out 72,728, cache_r 5,011,628, cache_w 146,347) · cost n/a (unpriced)
  coordinator: 9,792 tok (in 18, out 9,774, cache_r 539,346, cache_w 76,416) · cost n/a (unpriced)
  reviewer:Claude: 2,859 tok (in 10, out 2,849, cache_r 35,478, cache_w 16,215) · cost n/a (unpriced)
