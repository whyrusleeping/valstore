---
id: "0363"
title: 'bootes: compact vals value format (raw CBOR records, key-only backlinks, varint timestamps) to fit 4–5B events'
status: done
priority: 2
created: "2026-10-01"
updated: "2026-10-06"
depends_on: []
spec_refs: []
---

## Description
## Why
The 5B-event soak (0351/0352) does not fit the 3-node fleet on the current bootes value encoding: about 836 B/KV on disk and about 3.5 KB of drive per event at RF2. Most of the stored bytes repeat the key or are encoding overhead.

## Measured on the 20260824 cache (2026-10-01)
Throwaway measurement, 3M events each at seq 0 and seq 600M. No deletes were present in either sample.

| | seq 0 | seq 600M |
|---|---:|---:|
| KV/event | 1.98 | 2.08 |
| record key / value now, B | 66 / 686 | 66 / 572 |
| ↳ record JSON raw / escaped / source CBOR, B | 367 / 398 / 337 | 257 / 281 / 237 |
| backlink key / value now, B | 150 / 319 | 148 / 317 |
| logical B/event now | 1,214 | 1,142 |
| slim (CBOR + 14 B header; backlink value 6 B), B/event | 571 (0.47×) | 483 (0.42×) |
| slim with unescaped JSON instead of CBOR, B/event | 600 | 504 |

- **Record value today** is JSON of {uri, did, collection, rkey, record, created_at, indexed_at, seq}:
  - uri, did, collection and rkey repeat the key;
  - `record` is a Go string, so the inner JSON is escaped (+9%);
  - `indexed_at` is `time.Now()`, which is nondeterministic on replay.
- **Backlink value today** is a field-named JSON copy of {Ref, RefType, Collection, Path, SourceURI, SourceDID, CreatedAt}. Everything except CreatedAt is in the key `ref\0source_uri\0path` or derivable from it.

## Proposed format (vals sink only; ClickHouse path unchanged)
- **Record value:** `[ver=1][uvarint seq][zigzag-varint created_at µs][raw DAG-CBOR record]`. The JSON is derived at read time. This also skips JSON building on the ingest hot path; the CBOR scan still extracts backlinks and createdAt.
- **Backlink:** the key is unchanged. A per-referencer key is required: blind puts at 1M/s can't maintain a list value, and a prefix scan on `ref\0` lists the referencers. The value is `[ver=1][zigzag-varint created_at µs]`, or empty if the time isn't wanted.
- **Out of scope:** key compaction (did:plc binary, dropping `at://`), because it changes presplit boundaries. It is a possible follow-up.

## Acceptance
- bootes writes the new format, with unit tests for the exact bytes plus a decode helper.
- The valstore bench verifier (derive/samples) is updated to the new format.
- A short 2-node A/B (old vs new format) on the 1B cache measures on-disk B/KV (used segments per live key) and ACK KV/s. Capacity for 4B/5B events on 3 nodes is re-projected from the measured value.
- Readers: only bench verifiers read vals values today (checked ~/code). Confirm with the owner before switching the default.

## Acceptance criteria

## Work log

### 2026-10-01: owner decisions; bootes change committed; verifier built

- **Owner decisions:**
  - backlinks as proposed (key unchanged, value is created_at only);
  - records store the raw DAG-CBOR rather than JSON;
  - keep indexed_at.
- **bootes `8d5e2de`.**
  - `ingest/valsvalue.go` adds the codec and the exported `DecodeRecordValue` / `DecodeBacklinkValue`.
  - `Record.CBOR` carries the source bytes.
  - README Schema section updated.
  - Tests check exact bytes, round trips (including zero times), and that legacy JSON and truncated values are rejected; store-level tests cover both zones. `go test -race ./...` passes.
- **Real-cache measurement with the shipped encoders** (3M events each at seq 0 / 300M / 600M / 900M of the 20260824 cache):

  | | seq 0 | 300M | 600M | 900M |
  |---|---:|---:|---:|---:|
  | old B/event | 1,214 | 1,166 | 1,142 | 1,157 |
  | new B/event | 580 | 514 | 495 | 504 |
  | ratio | 0.478 | 0.441 | 0.433 | 0.436 |

  Record values went from 572–687 B to 259–358 B; backlink values from 318 B to 9 B. The keys are unchanged (66 B / 148–150 B).
- **Verifier real-client15** (`bench/0363-compact-values/`, README there). It vendors bootes 8d5e2de + valsgo 793f055. Binary sha256 `2fb067edfd041d96b8e4ef690410b1096a3871b4a85d262240ce3e0a7a115383`; the coordinator rebuilt it to the same sha.
  - **Records** require exact seq and CBOR. indexed_at must fall in the bootes window, and created_at is exact with the old fallback tolerance. A decoded value must re-encode to the stored bytes exactly.
  - **Backlinks** require exact created_at, with the same fallback rule.
  - **Fixture and trace formats:** fixture schema `task0363-fixture-v3`, canonical trace `task0363-mutation-trace-v2` (hashes the CBOR), frozen sha `3f7bca05…`, reproduced against the 1B cache. Fixtures made by older clients are rejected.
  - **0295 JSON trace:** `replay` of it is refused, because it has no CBOR.
- **Next.**
  - A 2-node old-vs-new A/B on the 1B cache, measuring on-disk B/KV (used segments per live key) and ACK KV/s.
  - Then re-project 4B/5B on 3 nodes.

### 2026-10-01 (later): AB01 fleet A/B stopped at a corruption on lab-2; format not yet accepted

Evidence: `bench/0363-compact-values/AB01-FINDINGS.md`, `SUMMARY-AB01.json`; raw data in `/data/dataset/task0363-results/`.

- **Setup.** lab-1+2, dual links. Master `88c25c2c` (valsd `b1b5858a…`). bootes OLD `9cbb296` vs NEW `8d5e2de`. Drives deallocated before every arm. Each arm stops at 400M ACK.
- **Clean pair OLDa vs NEWa:**

  | | OLDa | NEWa | NEW/OLD |
  |---|---:|---:|---:|
  | ACK KV/s, 30–350M | 1,129,717 | 1,174,868 | +4.0% |
  | ACK KV/s, 150–400M | 1,099,595 | 1,166,145 | +6.1% |
  | B/KV, used segments after drain | ~845 | ~531 | 0.63× |
  | client tx B/ACK | 1222 | 563 | 0.46× |
  | bootes CPU µs/ACK | 10.1 | 7.9 | 0.78× |

  NEWa's sampled check passed: 401,436 keys, exact seq and CBOR, re-encoding byte-identical, cursor matched.
- **NEWb incident.**
  - node2 (lab-2), zone 2 (backlinks): 344 `front-coded index integrity mismatch` flush aborts, covering 2 blocks, each retried every tick. There was also one `run index crc mismatch site=readback`, verdict persistent, with 0 diff bytes.
  - ACK fell to 173k/s. The run stopped with a clean drain and no core. The state is preserved and the drives were not deallocated afterwards.
  - Cause unproven: lab-2's non-ECC RAM history vs software. The guard logs no diff, so a bit flip cannot be told apart from a bug.
  - OLDb was not run, so preregistered criterion (c) fails.
- **Capacity, extrapolated from 400M at 531 B/KV and RF2:** 4B events ≈ 8.9 TB, 5B ≈ 11.1 TB, against about 11.6 TB configured on 3 nodes. 4B fits; 5B has no headroom.
- **Housekeeping.** The coordinator killed 174 orphaned debug `valsd` processes (`/tmp/valsd-e2e-*`, ppid 1, 2 days old) on devbox, which had been holding the bench client host at load ~25.

### 2026-10-02: FULL1, the whole 1B cache on the compact format (lab-1+3, shared eno1np0)

Evidence: `bench/0363-compact-values/FULL1-FINDINGS.md`, `SUMMARY-FULL1.json`; raw data in `/data/dataset/task0363-results/FULL1/`.

- **Completeness.** All 2,090,212,820 KV persisted, cursor 1,008,858,566. 0 drops/rejects/oversized, no integrity or CRC events.
- **Correctness**, checked after a clean restart because a harness host-set error blocked the in-run check: cursor PASS, and samplecheck matched 2,090,402 keys with 0 mismatches.
- **Throughput degraded: 752k whole-run mean.** 100M buckets 1230 → 1007 (by 800M) → 834 → 648 (step at 1.05–1.10B) → 496.
- **Phase 1 (0–1B, −25%).**
  - Resident runs 47→161, shard µs/write 4.4→7.2, BatchApply 14→31 ms, with r ≈ −0.99; compaction pinned at 2 merges.
  - Device waits stayed flat.
  - `probe_version` (`engine/read.rs`) prefetches and probes every resident run's filter on every write, so per-write CPU is O(runs).
- **Phase 2 (step at ~1.04 TB written per freshly deallocated drive).**
  - µs/stall 29→220 and sync drain 0.08→0.85 ms on both nodes at once; device write BW 1.1→~0.63 GB/s per node.
  - Consistent with a post-SLC-cache drive write bandwidth limit. Write amplification is about 1.8× over the 531 B/KV stored bytes: 0.63 GB/s ÷ (531 × 1.8) ≈ 660k, matching the 648k bucket.
- **After drain:** maintenance debt did not converge in 30 min (runs 316→189, merge inputs 27–38 GB, RSS up to 28 GB). B/KV was 532 after drain and 556 after settle; NUSE footprint was 1,195 B/KV (no TRIM).
- **Per-entry overhead dominates now.** Backlinks cost 527 B/key on disk against ~157 B logical, so the key and entry overhead is about 370 B. This is the next storage and device-bandwidth lever.

### 2026-10-02: DRIVE01 (raw fio, no vals) and FULL2 launched

Evidence: `bench/0363-compact-values/DRIVE01-FINDINGS.md`.

- **No raw write cliff.** lab-1 held 1.05 GB/s for 2.4 TB, with write p50 41→46 µs.
- **The drive does change state, keyed by volume, not rate.** The change came at ~1.19 TB written (NUSE 1.03–1.04 TB) at both 1.05 and 0.55 GB/s.
  - Read p99 rose 159→2,200 µs, and temperature reached 74 °C (lab-1 hit the 80 °C guard when unpaced).
  - Unpaced ceiling after the change: 1.8 GB/s. After 30 min idle the fast state recovers.
  - 16 KiB writes behave the same as 128 KiB.
- **So the drive alone doesn't explain FULL1's drop to 0.63 GB/s.** The untested differences are concurrent merge reads (~0.45 GB/s) and the SPDK queue depth.
- **Coordinator notes.**
  - Compaction concurrency is the typed key `maint_compact_lanes` (default 2, max 4; one pool per node, ≤1 merge per engine).
  - Pacing is not the limiter in FULL1: slept 124 s vs charged 3,754 s.
  - Runtime TRIM exists as `device_dealloc_segments_per_tick` (default 0 since 0356). The 0353 correctness failure behind that default was traced to a RAM bit-flip (0355/0361).
- **FULL2 launched**, two config-only full-cache arms vs FULL1 on lab-1+3, each preregistered:
  - LANES4 (`maint_compact_lanes=4`);
  - TRIM (`device_dealloc_segments_per_tick=256`).

### 2026-10-02: FULL2-LANES4 rejected; crash on lab-1; TRIM relaunched crash-tolerant

Evidence: `bench/0363-compact-values/FULL2-FINDINGS.md`.

- **LANES4 (`maint_compact_lanes=4`).**
  - Over 0–1B: merges committed 352 vs 350 and merge input 396 vs 394 GB, the same as FULL1, so the merge policy (not the lane cap) decides what merges. Runs were 154/153 at 1B (FULL1 161/156) and shard µs/write 7.3 (FULL1 7.2).
  - After the drive step (1.068B ACK, 1.035 TB written, the same volume as FULL1) it merged more but wrote more (0.78 vs 0.62 GB/s), with ACK 602k vs FULL1's 657k (−8%).
  - Do not pursue more lanes.
- **Crash at 1.675B ACK on node1 (lab-1).** glibc `corrupted size vs. prev_size` in a shard thread's request decode. A free chunk's size field is consistent only if bit 11 (0x800) is cleared, the same bit-11 signature 0336 found on lab-1. No cause is claimed. The core is kept on lab-1. The drain was clean (persisted 1,676,718,677, 0 drops).
- **FULL2-TRIM relaunched** (`device_dealloc_segments_per_tick=256`, preregistered criteria unchanged) under a crash-tolerant amendment:
  - systemd auto-restart, with a budget of 2 crashes;
  - stop on persistent integrity signals or a failed restart;
  - outage buckets flagged;
  - correctness (cursor + samplecheck, plus a census if any crash) is the arbiter.
- **Write-path research in parallel** (read-only): why capacity-profile writes probe every run, merge-policy knobs, and probe-cost reduction options.

### 2026-10-06: closed (backlog cleanup, owner-approved)
- The compact value format shipped long ago. This task then turned into the run log for every FULL/RECOV/SOAK5B arm; those commits are tagged `bench(0363)`, and the evidence lives in `bench/0363-compact-values/`.
- New runs are logged under the full-replay and write-throughput decay tasks. Keep reusing the `bench/0363-compact-values/` harness and directory.

