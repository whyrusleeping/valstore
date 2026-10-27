---
id: "0170"
title: Hardware A/B of graduated reserve-floor admission shaping (0092) vs the t0087 tier=nosync baseline
status: done
priority: 3
created: "2026-08-10"
updated: "2026-08-10"
depends_on:
    - "0092"
spec_refs:
    - docs/backpressure.md#Zone-region capacity and maintenance reserve
---

## Description
Task 0092 shipped graduated admission shaping default-ON (`VALS_ADMIT_SHAPE=0` kill switch, `VALS_ADMIT_BAND_PCT` band width, `admit_shed_rejections` counter): armed zone engines shed a quadratically-ramping fraction of wire record intake across a pre-floor free-segment band instead of admitting at full rate until the hard reserve floor. The remaining 0092 acceptance criterion is the hardware validation.

Rig: `ssh oldbox`, the `~/run/cluster/t0087` copy of the t0088 SPDK sorted-zone rig (see `bench/t0087/notes.md` "Box rig" — 1024-segment region, 1 KiB values, valsmix bursts). Reproduce the §2 `tier=nosync` burst protocol per arm.

Arms: A = `VALS_ADMIT_SHAPE=0` (baseline, expect the b2/b5-style all-Busy collapses with used_segments overshooting to ~1008–1018/1024); B = shaping on (default band). Optionally sweep `VALS_ADMIT_BAND_PCT` (e.g. 6 vs 12) if B under-delivers.

Note: 0092 shipped the band as `max(region_segments * VALS_ADMIT_BAND_PCT / 100, 1)` — deliberately NOT `max(reserve, pct)` as first planned, because on small regions (floor capped at half the region) `top = 2×floor` covered the whole region and shed on fresh zones. If the box floor turns out larger than 6% of the 1024-segment region, widen `VALS_ADMIT_BAND_PCT` in arm B rather than reintroducing the floor term blindly.

## Acceptance criteria
- Gyration amplitude reduced vs baseline; no all-Busy collapse bursts (no burst with landed ≈ 0 while busy is large).
- used_segments stays clear of the ~1018/1024 overshoot; `admit_shed_rejections` shows the shaping engaged.
- Comparable or better sustained throughput across the six-burst run.
- Findings in a bench/ notes file; flip/keep the default (and band width) accordingly and update docs/backpressure.md if the default changes.

## Plan

1. Re-read the t0087 rig scripts and prior §2 protocol, verify the oldbox is reachable/idle, and inspect current metric names and launcher conventions before touching the device.
2. Sync the current tree to the box and rebuild the SPDK vals-server binary. Preserve the existing rig, outputs, and old notes; create a task-0170 result area/scripts as needed.
3. Run two fresh-format six-burst `tier=nosync` arms with the same 100k preload and 16-thread × 64-pipeline, 10-second, 100%-write protocol: A with `VALS_ADMIT_SHAPE=0`; B with shaping enabled at the default 6% band. Capture per-burst client throughput/latency plus deltas for successful applies, Busy, shaping rejects, and used segments. Keep all non-arm inputs identical. If the default shaped arm clearly under-delivers or fails to engage because the box reserve exceeds its band, run one justified 12% sensitivity arm.
4. Compare collapse incidence, used-segment peak/gyration, shaping engagement, and six-burst aggregate sustained throughput. Make the default/band decision from the full-arm results rather than a single burst.
5. Add a durable bench notes file with exact build/run protocol, raw/derived per-burst tables, acceptance-criterion verdict, decision, and box hygiene. Update docs/backpressure.md only if the default or band changes; make the corresponding minimal code/config change only if the data warrants it.
6. Verify the notes against saved raw outputs, ensure no vals processes or SPDK residue remain and the expected drive is still vfio-bound, then inspect the final diff.

### Starting points
- bench/t0087/notes.md §Box rig and §2 burst protocol
- backlog/0092-smooth-reserve-floor-backpressure-graceful-admissi.md
- docs/backpressure.md#Zone-region capacity and maintenance reserve
- Remote rig: oldbox:~/run/cluster/t0087; rebuild after rsync; format=true reformats the test drive

## Work log
- 2026-08-10 plan: 1. Re-read the t0087 rig scripts and prior §2 protocol, verify the oldbox is reachable/idle, and inspect current metric names and launcher conventions before touching the device. 2. Sync the curre
…[truncated]
- 2026-08-10 context hints: 4 recorded with plan
- 2026-08-10 context hints: bench/t0087/notes.md §Box rig and §2 burst protocol; backlog/0092-smooth-reserve-floor-backpressure-graceful-admissi.md; Remote rig: oldbox:~/run/cluster/t0087; rebuild after rsync; fo
…[truncated]
- 2026-08-10 preload: 4 file(s), ~27 KiB seeded into implementer context
- 2026-08-10 implementer report: Implemented task 0170’s hardware validation and added `bench/t0170/` with the exact one-node SPDK configs/harness, complete shaped-arm raw outputs, two unshaped-control failure captures, and durable
…[truncated]
- 2026-08-10 review tier: high-powered — reviewers: sol, claude
- 2026-08-10 review (sol): revise — The artifacts and shaped-arm calculations are generally well documented and internally consistent, and the reproducible unshaped crash is a useful result. However, the executed harness did not reprodu
…[truncated]
- 2026-08-10 review (claude): accept — A well-executed and honestly reported hardware A/B that satisfies all four acceptance criteria. The shaped arm (B) completed six bursts with 1.44–2.08M landed applies each (no all-Busy collapse), pe
…[truncated]
- 2026-08-10 revision: Addressed all review findings and replaced the invalid benchmark evidence.  Changes: - Corrected `bench/t0170/box/run_arm.sh` preload to use `--writers 0`. The harness now hard-fails unless preload lo
…[truncated]
- 2026-08-10 review (sol): accept — The revision fixes the protocol defect and replaces the invalid evidence with two complete, corrected fresh-format arms. The raw preload checks confirm exactly 100,000 bulk-loaded records and zero pos
…[truncated]
- 2026-08-10 review (claude): revise — The rerun itself is excellent and now fully satisfies the A/B acceptance criteria: both fresh-format arms completed six clean bursts with hard-enforced load-only preload checks (verified in preload.lo
…[truncated]
- 2026-08-10 revision: Closed the remaining review findings without rerunning hardware; the preserved corrected snapshots were sufficient.  Changes: - Inspected the async Apply paths and corrected the metric semantics: `val
…[truncated]
- 2026-08-10 review (sol): accept — The revision fully addresses the remaining review findings. It correctly distinguishes Apply observations from successful writes, derives conservative per-burst success bounds from the preserved snaps
…[truncated]
- 2026-08-10 review (claude): accept — Accept. The revision fully addresses both prior findings and strengthens the work: task 0177 is filed covering the twice-reproduced compact_commit AlreadyFree abort plus the newly surfaced unshaped-ar
…[truncated]
- 2026-08-10 decision: accept — commit: 0170: validate reserve-floor admission shaping on hardware
- 2026-08-10 usage: 15,181,433 tok (in 2,140,738, out 95,543, cache_r 14,578,228, cache_w 258,425) · cost n/a (unpriced)
  implementer: 11,688,389 tok (in 1,073,097, out 45,820, cache_r 10,569,472, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 2,326,607 tok (in 741,824, out 14,735, cache_r 1,570,048, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,139,182 tok (in 325,767, out 7,783, cache_r 805,632, cache_w 0) · cost n/a (unpriced)
  reviewer:claude: 27,255 tok (in 50, out 27,205, cache_r 1,633,076, cache_w 258,425) · cost n/a (unpriced)
