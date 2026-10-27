---
id: "0373"
title: 'Post-drive-step group commit: attribute vals'' extra ~2.5 ms per domain flush; configurable batch-apply gate; test fewer, larger flushes (FULL7)'
status: done
priority: 2
created: "2026-10-04"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
## Why
After the 9100 PRO's state change (~1.04 TB written since discard), whole-cache ingest drops to ~862k ACK/s (FULL5-V5). It does not recover with fewer read bytes (FULL6-NOVERIFY) or fewer merge bytes (FULL6-LAZY).

The node-wide flush domain (0303) is strictly serial: one NVMe Flush per controller. agent_3's analysis of FULL5:
- each group-commit round goes 7.5 → 12.7 ms after the step, ~82% of the increase being the domain wait;
- domain Flush submit→publish goes 2.5 → 4.8 ms, at ~165 flushes/s, busy ~80%.

DRIVE02 (raw fio, `bench/0363-compact-values/DRIVE02-FINDINGS.md`):
- **Flush cost:** one Flush in flight at vals' cadence costs 0.55 ms before the step and ~2.1 ms after. The cost is per command, not per byte.
- **Flush duty:** at ~35% duty, writes keep up. Back-to-back flushing stalls writes (p50 9 µs → 2 ms) and drops below 0.55 GB/s.
- **Rejected:** concurrent flushes (pipelining) and FUA writes both make it much worse.
- **vals overhead:** vals' flush takes ~2.5 ms longer than the raw drive's in both regimes. The candidates are queueing behind the deep vlog write queue (up to 64 in flight per shard) and publish delay on the owning shard.

## Plan
1. **Instrument (no behaviour change):**
   - split `vals_domain_flush_latency_us` into submit→device completion callback vs completion→publish;
   - record vlog writes in flight at Flush submit;
   - add a flush-domain duty gauge (busy fraction) and writes/bytes per domain flush;
   - export them so a run can attribute the extra ~2.5 ms.
2. **Make the per-shard staged-apply gate configurable.** `MAX_STAGED_BATCH_APPLIES` is a constant 32 in `crates/vals-cluster/src/net/server/apply.rs`; add a typed node setting with default 32. Then a deeper client window (bootes `--vals-depth 32` with gate 64) can test "fewer, larger group commits" post-step. With per-command flush cost, writes per flush set the post-step ceiling; raw writes held 1.05 GB/s with 165 flushes/s vs vals' 0.53–0.60 GB/s.
3. **FULL7 whole-cache A/B vs FULL5-V5** (lab-1+3), preregistered:
   - post-step mean ACK ≥ 1.08× FULL5's 862,629;
   - pre-step ≥ 0.97× FULL5's 1,150,109;
   - RSS within FULL5 + 2 GB;
   - full correctness (cursor, samplecheck, counts, AE with sweep liveness).
   - Report the new flush split.
   - If the split shows the extra time is queueing behind vlog writes, the follow-up is a vals change to bound the write queue depth at flush submit (or prioritize the Flush), not a deeper window.

## Not to do
Pipelining domain flushes and FUA writes are refuted by DRIVE02.

## Acceptance criteria

## Work log

### 2026-10-04: steps 1–2 implemented (sol agent), coordinator-reviewed, committed
- **Metrics:**
  - `vals_domain_flush_device_us`: submit → SPDK completion callback; includes shim retry/poll delay.
  - `vals_domain_flush_publish_us`: callback → publication.
  - `vals_domain_flush_vlog_writes_inflight`: summary, unfinished vlog writes on the submitting shard's zones in the domain at Flush submit.
  - `vals_domain_flush_busy_us_total`: duty = rate/1e6.
  - `vals_vlog_write_bytes_total`.
  - Histogram `_sum` is in seconds, as for the existing ones.
- **Setting:** `batch_apply_gate` (default 32, 1..=256, `VALS_BATCH_APPLY_GATE`), with check-config support; docs require bootes `--vals-server-gate` to match.
- **Review:**
  - `publish` is one-shot, so busy time includes abandoned flushes by design.
  - The completion timestamp handoff can rarely miss a sample (Relaxed `done`), which is harmless.
  - Depth sampling scans local zones once per Flush.
- **Tests:** vals-cluster 731 + integration, vals-store, vals-server (check-config, metrics) and vals-spdk all pass in debug; `cargo check --features spdk` passes. Hardware overhead is unmeasured.
- **Next:** FULL7.

### 2026-10-04: FULL7 (BASE, D32) — deeper window supported; flush attribution
Evidence: `bench/0363-compact-values/FULL7-FINDINGS.md`, `SUMMARY-FULL7.json`; master aea964d2, valsd dc597436…, lab-1+3, whole cache.

**Throughput (ACK/s):**

| | Pre-step | Post-step | Whole |
|---|---:|---:|---:|
| FULL5 | 1,150,109 | 862,629 | 1,073,906 |
| BASE (repro) | 1,161,831 | 865,093 | 1,083,700 |
| **D32** (bootes depth 32 / gate 64, `batch_apply_gate=64`) | **1,277,469** | **1,004,301** | **1,209,579** |

- **D32:** every 100M bucket ≥ 1.064× BASE. Gate busy 0, Busy 0, RSS ≤ +0.63 GB. Client chunk latency rose (93 → 161 ms pre-step, 129 → 212 ms post-step).
- **Correctness (both arms):** cursor, samplecheck 2,090,402, exact counts, AE with sweeps advancing, aediag 24/24 equal.

**Flush attribution (post-step BASE, means):**
- 4.9 ms per flush = device 3.45 ms + publish 1.45 ms. The raw drive mean is 2.30 ms.
- vlog writes in flight at Flush < 0.5, so the "bound the vlog queue at flush" idea has nothing to bound.
- Publication holds the serial slot: 29% of the post-step slot (40% in D32), and grows with batch size (0.21 → 0.32 µs per synced write).

**Next:**
- (a) make gate 64 + bootes depth 32 the defaults (coordinator decision; reversible);
- (b) retargeted code step: start the next domain flush at Flush completion instead of after publication, and/or cut callback → publication delay. Preregister vs FULL7-D32.

### 2026-10-04: step 4 (prompt domain-flush publication) — rejected on hardware and reverted
- **The change:** the Flush completion callback releases the slot and advances `completed`/`failed` itself. Review (agent_4) found it correct, with no blockers. The patch is archived at `bench/0363-compact-values/FULL8-s4-rejected.patch`; build pin 89c8d373, unreferenced.
- **FULL8-PUB vs FULL7-D32** (same bootes depth 32 / gate 64; `FULL8-FINDINGS.md`):

  | | FULL8-PUB | FULL7-D32 | Ratio |
  |---|---:|---:|---:|
  | Pre-step | 1,281,249 | 1,277,469 | 1.003× |
  | **Post-step** | **929,604** | 1,004,301 | **0.926×** |
  | Whole | 1,186,987 | 1,209,579 | 0.981× |

  - **Mechanism worked:** publish ≈ 0, total flush 6.3 → 3.9 ms post-step.
  - **Side effect:** writes per flush fell 36% and flushes/s rose 45%, so the device had a Flush outstanding 0.70 of the time (vs 0.47). vlog stall rose to 258 vs 156 µs/stall.
  - The old publish delay acted as a coalescing interval. This is consistent with DRIVE02: flush duty stalls writes.
  - Correctness passed; 0 events.
- **Conclusion:** post-step throughput is set by writes per flush (and flush duty), not slot turnaround.
- **Next test (config only):** a deeper window, bootes depth 64 / gate 128 (FULL9-D64).

### 2026-10-04: FULL9-D64 (bootes depth 64 / gate 128, buffer 1.6M) — rejected; D32 stays the default
`FULL9-FINDINGS.md`. The whole cache was ingested; bootes exited 0 with the cursor saved. The run was then stopped by the persistent-CRC rule before the correctness checks, so it has no formal verdict.

| | Pre-step | Post-step | Whole |
|---|---:|---:|---:|
| FULL7-D32 | 1,277,469 | 1,004,301 | 1,209,579 |
| FULL9-D64 | 1,392,034 (1.090×) | 776,209 (0.773×) | 1,194,508 |

- **Flush side moved as predicted:** writes/flush 9–10.5k, flushes/s 74–86, flush occupancy 0.32–0.38.
- **Post-step rate still fell.** Device writes were 0.56–0.57 GB/s, but bytes written per ACK rose 610 → 733–743 as the faster pre-step phase deferred more merge work (runs 171 vs 121 at 1.0B).
- **Costs:** client chunk latency 294–532 ms; bootes RSS 4.4 GB; server RSS +2.4 GB.
- **Lever:** post-step throughput ≈ device write GB/s ÷ bytes written per ACK; flush coalescing alone does not lift it.
- **Incident:** node3 (lab-3) `run index crc mismatch site=readback verdict=persistent` on a 2.1 GB backlinks run index, 1 s after the drain. Build-verify was clean; there were no other signals.

### 2026-10-06: closed (backlog cleanup, owner-approved)
- Done: instrumentation (steps 1–2), FULL7 A/B, and next-step (a): `batch_apply_gate` now defaults to 64 (`node_settings.rs`), and bootes runs at depth 32.
- Rejected: step 4 (prompt publication, FULL8) and D64 (FULL9).
- Conclusion: post-step throughput ≈ device write GB/s ÷ bytes written per ACK. That question continues in the write-throughput decay task.

