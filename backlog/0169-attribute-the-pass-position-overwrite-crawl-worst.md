---
id: "0169"
title: Attribute the pass-position overwrite crawl (worst-of-run pass ~5) — drive-internal GC suspicion + NVMe Deallocate (TRIM) on segment free
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on: []
spec_refs:
    - bench/lab-bench-20260810-0166/notes.md
    - docs/backpressure.md#Off-core maintenance I/O pacing
---

## Description
Found during 0166 (bench/lab-bench-20260810-0166/notes.md). In all six F-protocol arms — paced and unpaced alike — the 5th overwrite pass is the worst (89–177k vs 200–400k), and the deep crawl windows show a damning signature: **zero maintenance activity** (no merges, no flush builds, no pace charges, snapshots idle), zero server errors, resident runs parked at 3/engine, node CPU near-idle, yet applies crawl at 6–170k server ops/s for up to 45 s with completed-apply latency only 0.5–5 ms (most in-flight ops stuck — survivorship bias). The stall class is external to LSM maintenance scheduling.

Leading hypothesis: drive-internal garbage collection / SLC-cache exhaustion on the consumer 9100 PRO under cumulative overwrite churn. Supporting fact: the engine never issues NVMe Deallocate — every LBA ever written (freed segments included) stays live to the SSD's FTL, so sustained overwrite forces worst-case internal GC.

## Acceptance criteria

- Reproduce the crawl and attribute it: at minimum discriminate drive-internal (e.g. idle-dwell A/B: long settle between passes restores pass-5 throughput; or smartctl/NVMe telemetry deltas) vs software (e.g. staged-read restarts, vlog inflight-queue stalls — vals_vlog_stalls, apply-path blocking).
- If drive-side: prototype Deallocate-on-segment-free (SPDK dataset-management command) behind an env flag and A/B the F protocol with it; decide default.
- If software-side: file/fix the specific mechanism.
- Write up in the 0166 bench dir or a new one.

## Plan

Goal: attribute the pass-position overwrite crawl (worst at OW pass ~5 in every 0166 arm; deep 5s windows at 6–170k server ops/s with ZERO maintenance activity) to drive-internal state (SSD GC / SLC exhaustion — the engine never issues NVMe Deallocate, so every LBA ever written stays live to the FTL) vs software, then if drive-side prototype Deallocate-on-free and A/B it.

Phase 0 — sync + rebuild HEAD valsd (SPDK) on lab-1/2/3; build local release vals-bench on devbox. No PD needed for `vals-bench x netload` (0166 precedent); fresh cluster = restart vals-node units with format=true node.confs.

Phase 1 — Arm R (repro; dirty drive, back-to-back): fresh cluster, 5s sampler (sample2.sh metric set + vals_vlog_stalls/vals_vlog_stall_us_total + server_errors), F protocol: 1× insert + 6× overwrite of `vals-bench x netload 8000000 256 "$PEERS" 64 2 16`. Confirm the positional crawl (count deep windows <100k ops/s/node with zero maint deltas); check software counters (vlog stalls, errors, pace) inside crawl windows.

Phase 2 — Arm W (idle-dwell discriminator): identical, but a 15-min idle dwell between OW pass 4 and pass 5 (drives idle; cluster up). If pass 5's crawl vanishes/deep-window count collapses vs Arm R ⇒ drive-internal GC recovers during idle ⇒ drive-side confirmed. If unchanged ⇒ software-side; pivot to software counters/attribution and file the fix.

Phase 3 — prototype (if drive-side): behind env flag VALS_NVME_DEALLOC:
- spdk_shim.c: vals_spdk_deallocate(dev, lba, nblocks) via spdk_nvme_ns_cmd_dataset_management + SPDK_NVME_DSM_ATTR_DEALLOCATE (DMA'd spdk_nvme_dsm_range array, ≤256 ranges/cmd), sync submit+poll mirroring vals_spdk_flush; guard on namespace deallocate support.
- BlockDevice::discard(lba, blocks) default no-op in vals-spdk/src/lib.rs; SpdkShardDevice impl must apply the same partition base offset as read_at/write_at.
- Engine/zonestore: (a) on reformat (format=true) discard the whole region span before writing headers — resets the FTL's months-old live set; (b) discard freed segments at post-durable-commit free sites (flush/compact/merge free paths, e.g. free_flush_segs/free_compact_segs), batched. CRITICAL ordering: only discard segments whose dereferencing manifest commit is already durable (deallocated blocks read back indeterminate/zeros).
- Off-hardware tests: default no-op for mem/file devices; a counting/mock-device unit proving frees produce discards of the right ranges and that the flag gates it. Debug suites: vals-store, vals-cluster.

Phase 4 — Arm P (dirty drive, VALS_NVME_DEALLOC=1): rebuild boxes, same F protocol. Format-time discard gives the FTL-fresh start; per-free discards keep it sparse. Compare R vs P pass-by-pass + deep-window counts. Decide the default from the data (write-up records the recommendation; conservative default OFF if there is any doubt about drive compatibility).

Phase 5 — write up bench/lab-bench-<date>-0169/notes.md (arms, samples, verdict, mechanism), file follow-ons (e.g. remaining free paths, file-backend PUNCH_HOLE) as separate tasks if warranted.

Notes: implementer backend has been 400ing since 08-14 — try spawn_implementer for Phase 3 once; if down, implement directly per recorded precedent and review via claude tier. Never touch the OS 990 PRO: all discards go through valsd's own opened namespace (the config'd 9100 PRO); no kernel rebind/blkdiscard is used. Box runs: rebuild before every run; one-line sudo systemd-run; wrap ssh in timeout.

### Starting points
- crates/vals-spdk/csrc/spdk_shim.c — vals_spdk_flush is the sync submit+poll template; no DSM support yet
- crates/vals-spdk/src/lib.rs:110 trait BlockDevice (default methods; add discard default no-op; forward_block_device! macro must forward it)
- crates/vals-spdk/src/spdk.rs — SpdkShardDevice partition base-offset mapping in read_at/write_at
- crates/vals-store/src/engine.rs:2297 free_flush_segs / :2306 free_compact_segs + many alloc.free sites
- crates/vals-cluster/src/zonestore.rs:571 reformat
- bench/lab-bench-20260810-0166/notes.md + sample2.sh — F protocol, PEERS, sampler
- plans/bootes-soak-runbook.md §0.5 — box rsync+rebuild recipe; §2 node systemd-run launch

## Work log
- 2026-08-10 plan: Goal: attribute the pass-position overwrite crawl (worst at OW pass ~5 in every 0166 arm; deep 5s windows at 6–170k server ops/s with ZERO maintenance activity) to drive-internal state (SSD GC / SLC
…[truncated]
- 2026-08-10 context hints: 7 recorded with plan
- 2026-08-10 context hints: crates/vals-spdk/csrc/spdk_shim.c — vals_spdk_flush (sync submit+poll pattern) at bottom; add vals_spdk_deallocate here; crates/vals-spdk/src/spdk.rs — ffi block, SpdkBlockDevice::dev_sector(lba) 
…[truncated]
- 2026-08-10 preload: 3 file(s), ~16 KiB seeded into implementer context
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The task's acceptance criteria are fully met. Attribution is thorough and convincing: six F-protocol arms including the idle-dwell discriminator (W: 15-min dwell did not restore pass 5) and the TRIM A
…[truncated]
- 2026-08-10 review (sol): unknown — reviewer error: turn 1: API returned non-200 status code 400: {"detail":"Unsupported parameter: max_output_tokens"}
- 2026-08-10 review (claude): accept — The revision cleanly addresses the previous findings: vals_nvme_dealloc_blocks HELP now says "device blocks" instead of hardcoding 4 KiB, and the drain_discards failure path uses log::warn! instead of
…[truncated]
- 2026-08-10 decision: accept — commit: 0169: pass-position overwrite crawl attributed — drive read-latency spikes amplified ~1000× by the synchronous cold LWW apply read; NVMe Deallocate prototyped (default OFF). Six fresh-cluster F-pro
…[truncated]
- 2026-08-10 usage: 138,867 tok (in 374, out 138,493, cache_r 29,184,913, cache_w 1,191,472) · cost n/a (unpriced)
  coordinator: 120,102 tok (in 302, out 119,800, cache_r 26,174,493, cache_w 1,077,523) · cost n/a (unpriced)
  reviewer:claude: 18,765 tok (in 72, out 18,693, cache_r 3,010,420, cache_w 113,949) · cost n/a (unpriced)
