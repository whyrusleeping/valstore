---
id: "0352"
title: Demonstrate sustained >=1M ACK KV/s full-cache bootes ingest with crash recovery (after 0349/0350)
status: done
priority: 3
created: "2026-09-30"
updated: "2026-10-06"
depends_on:
    - "0349"
    - "0350"
spec_refs: []
---

## Description
Carries the owner acceptance goal of 0348: bootes ingest at 1M KV writes/s or more, sustained for as long as the disk allows, with node crashes handled along the way.

## Starting point
The 0348 evidence is in bench/0348-scale/FINDINGS.md.
- **SCALE0348-M1** (lab-1/2, master 2005fe17) reached 1.105B ACK KV at a mean of 386k, and only 709k over its first 400M. It then failed for two reasons:
  - lab-2 bit-42 memory corruption, plus persisted corruption on its drive (0248);
  - a node1 ram_broker assertion (0349).
- An unattended-upgrades/needrestart restart of the vals unit is now excluded on all boxes.
- **Drive FTL state was the dominant slowdown** (0350):
  - after deallocation, 2-node lab-1+3 ran at 806–865k;
  - before deallocation it ran at 642–669k;
  - on a shared client link (eno1np0) it stays below 1M;
  - dual-link F1 on lab-1/2 reached 1.016M.
- One planned SIGKILL was handled: writes resumed after 27 s.

## Work
1. After 0349 and 0350 land, run the full cache (2.09B KV) with the bench/0348-scale harness in its crash-tolerant configuration, with one injected SIGKILL.
2. Use a platform pair allowed at that time. lab-2 is held for 0248 evidence unless the owner releases it. Use dual client links where possible, and verify `ip route get` for each node.
3. Attribute and fix, or split out, the top limiter if the rate falls below 1M. Candidates include the client link, per-node device waits, compaction debt growth, and rehome disturbance.

## Acceptance
- Full-cache replay at a mean of 1M ACK KV/s or more, with no sustained collapse (report the fraction of time below 1M).
- At least one injected node crash, recovered automatically.
- Cursor match plus the sampled verify pass, and the exact census where feasible.
- Findings are recorded.

Going beyond the cache (5B records) is 0351.

## Acceptance criteria

## Work log

2026-10-01 (coordinator, work-loop): BLOCKED on owner. All three lab boxes are unavailable for a correctness-gated full-cache run: lab-2's drive is held as 0248 evidence; lab-1+3 hold the retained 0353 LONG corpus until the owner releases it; and 0361 memtester proved uncorrected bit-12 RAM errors on lab-1/3 (0362, blocked on physical/BIOS access), so cursor/verify/census results there are untrustworthy. Unblock: owner remediates RAM and 0362 passes a clean 6 h memtester, plus releases a box pair's drives (or provides another platform pair).

2026-10-01 (owner): all lab boxes and scratch drives are released, including lab-2's 0248 hold and the 0353 corpus. RAM remediation (0362) will not happen soon for budget reasons, so this run proceeds on the current RAM and must treat correctness failures as possible hardware flips. The corpus is the 5B-event capture under 0351. On current value sizes (about 836 B/KV on disk, RF2), 3 nodes hold roughly 2.5–3B events.

2026-10-05: **SOAK5B** — 3-node replay of the sealed 5B-event cache (`/data/dataset/jetstream-cache-5b-20261001`, 5,000,000,002 events, sealed 2026-10-05T00:32Z). Master 92e25363 (valsd 5dec80e6…), bootes 3d5b2e9 (depth 32 / gate 64). Raw data: `/data/dataset/task0363-results/SOAK5B/`; launcher `bench/0363-compact-values/soak5b/launch.sh`; preregistration `SOAK5B-README.md`.
- **Stop:** the planned RAM stop at **4.186B ACK** (~2.0B events). node2 RSS was ≥ 85% of the 40 GiB guard for 60 s; RAM grows ~10.6 B/key, fix in 0378.
- **Result:** PASS — cursor, samplecheck, exact counts, AE window, owned aediag.
- **Throughput:** ingest 3,630 s, **mean 1.153M ACK KV/s**; pre-drive-step (0–2.44B) 1.443M; post-step 900k.
- **Per 500M:** 1.653M, 1.657M, 1.577M, 1.262M, 1.154M, 1.024M, 0.939M, 0.785M. The decline starts at ~1.5B, before the drive step (2.44–2.51B ACK at ~1.06 TB per node). Attribution in progress.
- **Crash:** one, exit 75 on lab-3 at 3.90B ACK, single-bit `xor=0x10` in `entry.seq`. It matches lab-3's memtester bit-12 signature, so classified ram_fault_consistent. Auto-restart; recovered.
- **Drives** were left holding the corpus for attribution.

2026-10-06: **SOAK5B-V3 / V3-R2** — full 5B replay attempts on master a6c28570 content (pin b9b4fa47; 0378 + 0379 + 0380). Raw: `/data/dataset/task0363-results/SOAK5B-V3*`; findings `bench/0363-compact-values/SOAK5B-V3-FINDINGS.md`.
- **V3:** stopped at 1.246B ACK on a key-filter cover-scan CRC on node2, 32 s after an exit-75. The read-only `valsd runverify` (tool commit faf3b48d) re-read every live zone-1/2 run on all 3 nodes (~230 GB): clean. It also caught a transient single-bit read flip on lab-3 itself. Classified `ram_fault_consistent`; the stop rule was amended (a single cover-scan CRC is now record-and-watch).
- **V3-R2:** **PASS**, planned **disk_full** stop at **8.07B ACK** (77% of the cache). lab-2 zone-2 shard 6 fell below 10% free (409M keys, 57 runs).
  - Ingest 15,364 s, mean **525k ACK/s**. Per 1B: 1.50M, 1.31M, 1.13M, 954k, 752k, 432k, 303k, 246k.
  - One crash: exit-75 RAM flip `xor=0x04` on lab-2.
  - Max RSS 40.2 GB at ~5.3–5.5B keys/node, so 0378 held.
  - Drives wrote 6.9–8.9 TB each for ~2 TB live per node.
  - **Decline cause:** merge write amplification grows with data. Merge output per synced write rose 195 → 1,026 B because forced merges ignore the input cap (0381), and the merge RAM pool deferred merges late. Disk was tight as projected (worst backlinks shard ~0.9).
  - Drives keep the corpus.
- **Next:** fix 0381, then a hardware check, then a full replay with larger backlinks partitions if the projection still needs them.

### 2026-10-06: closed — goal met by SOAK5B (owner)
- The owner accepts SOAK5B as meeting the ≥1M ACK KV/s goal: 4.186B ACK at a mean of 1.153M ACK KV/s on 3 nodes, one node crash recovered automatically (exit 75 → restart), and full correctness PASS. RECOV01/02 cover injected kills.
- **Still open, as separate tasks:**
  - getting all the way through the 5B cache;
  - the write-throughput decay with data size (V3-R2 per 1B: 1.50M → 246k).

