---
id: "0386"
title: Complete a full 5B-event replay end to end on 3 nodes (disk/RAM projections, lab-2-tolerant stop rules, full correctness)
status: done
priority: 2
created: "2026-10-06"
updated: "2026-10-12"
depends_on:
    - "0395"
    - "0398"
    - "0400"
    - "0401"
    - "0402"
    - "0409"
    - "0411"
spec_refs: []
---

## Description

## Description
## Goal
Replay the whole sealed 5B-event cache (`/data/dataset/jetstream-cache-5b-20261001`, 5,000,000,002 events, ~10.4B logical KV) on the 3 lab nodes, through to a clean drain and a full correctness pass. No run has finished it:
- SOAK5B hit the planned RAM stop at 4.19B;
- V3-R2 hit disk_full at 8.07B (77%);
- V4, V4-R2 and V4-R3 were stopped early by lab-2 integrity events.

## Known walls, each to be projected before launch
1. **Disk.** V3-R2 hit disk_full at 8.07B ACK: lab-2 zone-2 shard 6 fell below 10% free (409M keys, 57 runs), with ~2 TB live per node at that point and ~3.6 TB of regions per node.
   - The full cache needs ~30% more. Project end-of-file free space per shard, including transient merge space and up to 64 GiB of seed pins.
   - Fix the imbalance or the bytes. Options: larger or more backlinks partitions or presplit (V3 already flagged this), per-key overhead cuts, or a confirmation that 0381's frozen tier doesn't change space amplification.
2. **RAM.** V3-R2 peaked at 40.2 GB RSS at ~5.3–5.5B keys per node, against an RSS stop of 43.8 GB on 64 GiB boxes. At the end of the cache a node holds ≈ 6.9B keys (RF2 over 3 nodes).
   - Project with the 0378 slope (~4 B/key). Then either raise the guard with a safety argument (MemAvailable stays above its floor) or cut RAM first.
3. **Throughput decay.** At V3-R2's tail rates (250–300k) the last 2B ACK takes about 2 h or more. Not a blocker, but 0385 fixes shorten the run. This run is also 0385's first post-0381 data past the drive step: collect 0385's counters and profiles.
4. **lab-2 has known-bad RAM** (owner: no retest, no DIMM budget). 3 nodes are needed for capacity, so lab-2 stays in. Before launch, amend and preregister the stop rules:
   - **Record and continue** for faults caught before publication: exit-75 fail-fast restarts; build-verify, encode-integrity (0384) and merge-input CRC retries; a single cover-scan CRC. V4 findings item 2 has a starting point: record one, stop if the same zone repeats or ≥ 3 occur within 600 s. Re-examine that against V4-R3, where the stop came from an over-broad same-zone rule that counted input-read CRCs.
   - **Stop only on:** evidence of persisted or published corruption (a read-time CRC on a published run that `runverify` confirms on the device), a correctness check failure, a failed restart, or the crash budget.
   - Keep the cores and binaries of any heap-corruption crash for 0248.

## Also report (piggyback)
These measurements are non-blocking: report them when the required windows occur, otherwise mark them unobserved. Do not extend, force extra rehomes or launch another replay to collect them. Integrity-event classification remains mandatory.
- 0379 P1–P3 across any rehome window;
- 0381 post-step G1/G2/G4;
- 0384 `vals_encode_integrity_failures_total` events vs persisted bad runs;
- 0332 rehome/skew data.

## Acceptance
- All 5,000,000,002 events are admitted and persisted, and bootes drains loss-free.
- Cursor match, samplecheck, exact counts and owned aediag pass.
- Every integrity event on every node is classified. Any persisted corruption is investigated before calling the run a pass.
- A findings file in `bench/0363-compact-values/`: per-1B rate, crashes and recoveries, disk/RAM vs projection, and the piggyback reports above.

## Acceptance criteria

## Current validation contract

Tier **L3**: a large end-to-end run is this task's explicit goal, so the full sealed cache and full endpoint correctness remain mandatory. Budget one registered EOF attempt (fresh or policy-eligible resumed). Before launch, derive its total elapsed bound from retained measured ingest/recovery rates plus preparation, endpoint checks and shutdown; the tier's 6–14 h estimate is not an automatic allowance per phase or a correctness-failure threshold. A near-EOF resume should be much shorter than a fresh replay. Further attempts require an explicit disposition and budget, not an automatic relaunch loop. Missing piggyback windows do not fail the gate; real correctness failures do.

0401 is shipped. Its owned-aediag/corpus qualification criterion moved to **0411**; the new disk rule still needs **0409** wiring. These dependencies make the already-recorded blockers explicit, not new validation gates. Preserve R3 while its read-only users are pending. A resumed mixed-binary corpus is confirmation evidence, not automatically a release pass. Record intervening binaries and qualify the history before making any 0386 claim. Do not silently fall back to the old pinned binary or discard workflow below.

## Plan (historical V5 launch; current contract above takes precedence)

Resume of Phase 1 after 0395–0400 and 0399 landed (all deps done; boxes idle, no valsd, route eno1np1 on 2026-10-08).

Coordinator decisions (2026-10-08, unattended; reversible):
D1 RAM gate. Credit 0399 from its local measurement: fences .748→.637 B/key, which is 0.111 GB per 1B keys, ×~7.07B keys/node ≈ 0.79 GB (0.73 GiB). That puts KF2's EOF projection at ≈10.2 GiB MemAvailable in the central case and ≈7.9 GiB in the burst case. The burst case adds non-simultaneous maxima and still stays above the runtime protections: the MemAvailable <5 GiB for 30 s planned stop and the 4 GiB guard. The 8 GiB figure was a projection margin target, not a runtime floor, and the task allows raising the guard with that safety argument. Launch is approved, and V5 is the hardware measurement of 0399. Do NOT adopt 0398's 36 GiB RSS cap. Keep the RAMAB-proven stops unchanged:
- RSS stop at 85% of the 48 GiB guard for 60 s;
- MemAvailable <5 GiB for 30 s;
- the 4 GiB, root and disk 10% guards.
A RAM or disk planned stop is a clean reportable outcome.
D2 Read gate. The 0400 primary (0.043 extra reads, 32 KiB units) is accepted. The +2 sensitivity (0.053) is recorded, not blocking. The 0400 watch rule records a read-gate breach (not a stop) when a covered node (max_uncovered_runs=0) has vals_key_filter_max_est_fpr_ppm >37700 for >30 min.
D3 Arm. Attempt SOAK5B-V5 (relaunch -R2..). Everything is RAMAB-KF2 settings with these exceptions:
- server pin = master HEAD 8736b2b1 (includes 0395–0400 + 0399);
- no --stop-at-ack: run to EOF of all 5,000,000,002 events, then clean bootes drain;
- full end-of-run correctness: loss-free drain, cursor match, samplecheck, exact per-zone counts, owned aediag.
The RAMAB-KF2 settings kept as-is:
- 96/72 byte-presplit layout (sha 3dd02e6e…) and geometry 476930/2048/260084/212749 MiB, fallback 2048;
- 32 KiB blocks and 10-bit run blooms;
- env VALS_MALLOC_TRIM_SECS=0, VALS_KEY_FILTER_REBUILD_URGENT_FPR_PPM=25000, VALS_KEY_FILTER_REBUILD_URGENT_EXTRA=1;
- bootes e8250709 at D32 / gate 64;
- RAMAB policy: publication_watch classifier; crash budget 6 total or 3 per host within 1800 s; classified exit-75 exempt with a sanity cap of 12; heap-corruption cores and binaries kept for 0248.
BEFORE discard replaces the retained KF2-R2 corpus, whose 0400 analysis is committed. No AFTER discard.

Phase 1 (implementer):
1. Wire SOAK5B-V5(-R[2-9]) in run_full1.py as a RAMAB-KF2 clone with the D3 differences. Use the existing SOAK5B EOF correctness path (full samplecheck, cursor, counts, owned aediag), as a to-EOF V3 arm did. Add runner-wiring unit tests covering pin, env, layout sha/sizing, policy, no stop-at-ack and the EOF correctness steps. Keep the existing tests green (test_soak5b, test_full1, test_soak5bv5_projection).
2. Save `plan --attempt SOAK5B-V5` json and its diff against RAMAB-KF2's plan under /data/dataset/task0363-results/SOAK5B-V5-prep/. The only differences should be the D3 ones.
3. Update the top status and preregistration in SOAK5B-V5-README.md with D1–D3 and the launch receipts. Keep the historical sections.
4. Write launcher soak5b/launch-v5.sh, cloned from launch-ramab-kf.sh, with steps: route, clean, prefix, plan (no stop-at-ack), BEFORE discard, run, receipts, crashes. The fleet SPDK build at pin 8736b2b1 must happen in a separate worktree, never on the main tree's branch, with one SHA on all 3 nodes + PD and ldd receipts. Use the same build path RAMAB-KF2 used.
5. Launch detached (setsid nohup). Confirm rising ACK and no policy issue for 10–15 min. Report launch.log/run paths, SHA and ETA.
Phase 2 (coordinator): background watcher until the launcher logs done.
Phase 3 (fresh implementer): classify every integrity event; runverify any suspected persisted corruption. Write bench/0363-compact-values/SOAK5B-V5-FINDINGS.md with:
- per-1B rate;
- crashes and recoveries;
- disk and RAM vs projection, including the measured 0399 credit vs D1;
- read-gate watch;
- piggyback 0379 P1–P3, 0381 G1/G2/G4, 0384 encode events, 0332 rehome/skew, 0385/0389 decay counters.
If a stop occurs before EOF, diagnose and resume the retained attempt under 0403 when its policy permits. Crash-ended runs without a durable cursor require 0408. Never resume after a correctness failure or confirmed corruption. If resume is impossible, record why and budget a fresh attempt explicitly; do not automatically discard and restart from zero.

### Starting points
- bench/0363-compact-values/run_full1.py: RAMAB_* constants l.143-172, KF_PINS, ARM_ENV, ARM_BOOTES; soak5b_policy(ramab=True) l.3027; soak5b setup ~l.3000; plan output ~l.3100 (launcher/layout fields); build_kf ~l.3520
- bench/0363-compact-values/soak5b/launch-ramab-kf.sh (launcher template)
- /data/dataset/task0400-results/prep-kf/RAMAB-KF2/plan.json (reference plan to diff against)
- bench/0363-compact-values/SOAK5B-V5-README.md resume checklist l.597-659
- bench/0400-kf-chain/README.md (KF2 preregistration)

## Outcome

SOAK5B-V5-R4 replayed all 5,000,000,002 events (10.407B KV) to cache EOF on one binary (674fa37d, valsd e9e8f7fe), 5 h 20 min from launch to verdict. - **EOF checks pass:** loss-free drain, cursor probe at 5,065,790,169, exact per-segment counts, whole-cache samplecheck 10,407,283/10,407,283 and owned aediag 72/72 equal. - **Mid-run stop:** segment 1 stopped at 8.90B on a boot-open CRC on lab-2. A read-only device check found 0 faults in 1,188 runs, and the run resumed under the preregistered `R4-boot-crc` amendment. - **Crashes:** 8, all recovered, 4 counted of 6; the 3 heap-corruption aborts have cores kept for 0248. - **Integrity:** every event classified; no persisted corruption. - **Harness:** R4 pin, seed-gated aediag and the boot-CRC rule; 210 tests pass. - **Findings:** `SOAK5B-V5-R4-FINDINGS.md`. 0379 P1–P3 not computed; 0408 crash-resume not exercised on hardware.

Commit: bench(0386): SOAK5B-V5-R4 full 5B replay to EOF PASS — R4 pin 674fa37d, seed-gated owned aediag, R4-boot-crc amendment (runverify-cleared boot CRC, per-segment classifier replay), findings + analyze_r4.py
