---
id: "0087"
title: 'Write-burst throughput gyration at low fill: attribute the 2–4 ms per-op quantum (NVMe FLUSH group commit? run fan-out?)'
status: done
priority: 3
created: "2026-07-07"
updated: "2026-07-08"
depends_on: []
spec_refs: []
---

## Description
Found during task 0084's box validation (bench/t0084/notes.md §Box validation item 5) and visible in task 0083's own burst logs — pre-existing, NOT caused/fixed by the 0084 reclamation work:

10 s 100%-write bursts (valsmix, tier=one, 16×64 pipeline) on an SPDK sorted zone at only ~12% region fill gyrate wildly burst-to-burst: 151k → 25k → 96k → **8.6k** → 69k writes/s. Signature of the slow bursts:
- Nearly FLAT latency distribution: p50 2048 µs, p90 4096 µs, max 4308 µs — every op appears to queue behind a shared ~2–4 ms quantum.
- Effective concurrency ≈ 17 (≈ the 16 client threads) despite a 1024-deep pipeline.
- Serving thread ~65% idle-polling the qpair (`nvme_pcie_qpair_process_completions` self time) — no CPU hotspot, no region pressure, no errors.

Prime suspect: **group commit serializing behind NVMe FLUSH latency** — tier=one waits on vlog sync → device FLUSH; if FLUSH degrades to 2–4 ms while the drive's FTL digests churn, throughput collapses to group_size/flush_latency independent of pipeline depth, and variance tracks drive state (fresh bursts fast). Secondary suspect to check while in there: LWW version-check fan-out over run blooms/block indexes (device reads per write once keys are flushed) — measure write cost vs `run_count` on hardware; an old comment cites ~1.6k/s at 16+ runs but there is no curve.

## Acceptance criteria
- Reproduce the burst gyration and attribute the 2–4 ms quantum: A/B `tier=nosync` (removes the sync barrier), direct NVMe FLUSH latency measurement under churn (spdk perf/fio), and group-commit size instrumentation.
- Measure write throughput/latency vs `run_count` (0..8+) on hardware to quantify the version-check fan-out contribution.
- Outcome: either a fix/tuning (e.g. flush coalescing shape, admission of larger groups) with A/B numbers, or a clear writeup that this is device-FTL-bound with the sustainable-rate envelope documented (bench note).

## Plan

Goal: attribute the 2–4 ms per-op quantum behind the write-burst gyration (task 0083/0084 item 5), measure the run_count fan-out contribution, and either land a low-risk fix/tuning with A/B numbers or write up the device-bound envelope (bench/t0087/notes.md).

Working hypothesis (from code reading): `BatchApply(sync=true)` (net/server.rs ~3183) executes `store.sync()` → `ValueLog::sync` → `dev.flush()` — a synchronous, blocking NVMe FLUSH on the single serving core (vals-spdk `vals_spdk_flush` busy-polls to completion). Every concurrent write group serializes behind that barrier, so throughput ≈ (avg group size)/(flush latency), independent of pipeline depth — matching the flat 2–4 ms latency, effective concurrency ≈ #client threads, and 65% idle qpair polling. FLUSH latency plausibly degrades to 2–4 ms while the drive FTL digests churn (variance tracks drive state).

Steps:
1. Instrumentation (small, local): add (a) a `sync_flush_us` histogram timing the `store.sync()` (device FLUSH barrier) inside the BatchApply sync arm, (b) a group-size measure (writes per synced BatchApply — histogram or counters sync_commits/synced_writes so avg group size is derivable), and optionally (c) a counter distinguishing flush-barrier time from apply time. Keep names/style consistent with observe.rs. Unit-smoke locally (mem/file backend, cargo test + a local bench run).
2. Box repro (oldbox, t0088-style rig, single SPDK valsd on 0000:c1:00.0, sorted zone smix rf=1 tier=one, valsmix over loopback): reproduce the 10 s write-burst gyration at low fill; capture the new metrics — confirm or refute that slow bursts show sync_flush_us ~2–4 ms and fast bursts ~fast, and that ops/flush × flushes/s ≈ throughput.
3. A/B tier=nosync: same burst sequence with --tier nosync (no sync barrier). If gyration disappears / throughput decouples from drive state, the FLUSH barrier is the quantum.
4. Direct FLUSH latency on the raw device under churn: check identify (volatile write cache present?), then measure FLUSH latency directly (spdk perf/examples or a tiny probe) fresh vs immediately after heavy churn, to show the 2–4 ms is device-side FTL state.
5. run_count fan-out curve: on hardware, measure write throughput/latency vs run_count 0,1,2,4,8+ (drive run creation via flush cadence, e.g. VALS_FLUSH_TAIL_EPOCHS; read run_count from metrics) at tier=nosync (to isolate version-check read cost from the flush barrier). Quantify device reads per write vs runs.
6. Verdict + outcome: if attribution confirms the FLUSH barrier, evaluate a low-risk tuning (e.g. coalescing shape) only if it's genuinely small; otherwise document the sustainable-rate envelope + group-commit math in bench/t0087/notes.md and file a follow-on task for an async/coalesced group-commit FLUSH (issue FLUSH off the serving core and coalesce concurrent syncs) — that redesign is out of scope here.
7. Leave the box clean (no valsd/vals-bench, no dpdk/hugepage residue, drives vfio-bound); record everything in bench/t0087/notes.md.

Constraints: don't touch the OS RAID nvme4/nvme5; only the four vfio data drives. Build on box: VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk (and vals-bench if used). valsmix rebuild only needed if the wire changes (it shouldn't — metrics-only).

## Work log
- 2026-07-08 plan: Goal: attribute the 2–4 ms per-op quantum behind the write-burst gyration (task 0083/0084 item 5), measure the run_count fan-out contribution, and either land a low-risk fix/tuning with A/B numbers 
…[truncated]
- 2026-07-08 implementer report: Attributed the write-burst throughput gyration and refuted the task's prime suspect (NVMe FLUSH group-commit serialization).  ## Code changes (commit: instrumentation + bench note) - `crates/vals-clie
…[truncated]
- 2026-07-08 review tier: single-opus — reviewers: Claude
- 2026-07-08 review (Claude): accept — The change delivers exactly what this attribution task asks for: minimal, well-documented server-side instrumentation (sync_flush_us histogram, sync_commits/synced_writes counters, and apply_latency_u
…[truncated]
- 2026-07-08 decision: accept — commit: bench+observe: attribute write-burst gyration (task 0087) — NVMe FLUSH group-commit refuted (~48 µs constant); cause = on-core maintenance inflating apply service time (18→120 µs) + reserve-floo
…[truncated]
- 2026-07-08 usage: 84,366 tok (in 304, out 84,062, cache_r 10,624,867, cache_w 300,962) · $11.1196
  implementer: 64,851 tok (in 224, out 64,627, cache_r 9,235,527, cache_w 156,575) · $7.2132
  coordinator: 16,650 tok (in 62, out 16,588, cache_r 1,279,799, cache_w 122,903) · $3.6461
  reviewer:Claude: 2,865 tok (in 18, out 2,847, cache_r 109,541, cache_w 21,484) · $0.2603
- 2026-07-08 decision: accept — commit: backlog: promote follow-on tasks 0091 + 0092 to todo (accepted); 0087 usage log
