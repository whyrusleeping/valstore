---
id: "0574"
title: 'Backlog groom: decisions needed'
status: proposed
priority: 3
created: "2026-11-11"
updated: "2026-11-11"
depends_on: []
spec_refs: []
---

## Description
Flags from the 2026-11-11 backlog groom that evidence could not settle. Each needs an owner decision. The groom changed no status or meaning for these.

## 0013: the SPDK relocate journal and layout fingerprint are not durable, and no task tracks them
- **Problem:** task 0013 (done) made multi-drive `relocate` crash-safe with a per-move journal persisted through the `DriveSet` seam. Only `MemDriveSet` and test wrappers persist it. `SpdkDriveSet` (`crates/vals-cluster/src/drive.rs`) keeps the seam's no-op defaults for `write_relocate_journal` and `write_layout_fingerprint`, so the RF=1 crash guarantee does not hold on SPDK. 0013's Plan said this was "already tracked with task 0006's ride-alongs", but 0006 filed only 0039/0040/0041, and 0040 put the liveness watermark in the zoned superblock without touching the drive-layout seam.
- **Evidence:** `drive.rs` (trait defaults near line 227; `impl DriveSet for SpdkDriveSet` near line 664 overrides only drive_count, drive_blocks and open_region); `docs/multi-drive.md` says the SPDK on-device fingerprint and durable relocate journal "are a follow-on; the seam is inert there for now". Exposure is limited: no `valsd` path calls `relocate` or `NodeHandle::rebalance_drives`. The only callers are tests and `crates/vals-bench/src/cluster_passive.rs`.
- **Decision needed:** file a task to persist the journal and fingerprint on SPDK (before any valsd drive-rebalance command exists), or accept the documented limitation and leave it untracked.

## 0042: criterion 1 was closed under a reinterpretation with no owner source
- **Problem:** the criterion was "post-compaction scan rate ≥5× the pre-compaction rate". The box run measured 1.61× wall time (91.5k → 147k keys/s) and 64× fewer device reads. The task closed under "interpretation (a)", which counts the device-read reduction as clearing ≥5×. A key-ordered-write control scanned at the same 147k keys/s, which puts the gap at the per-core scan-CPU ceiling.
- **Evidence:** the 0042 work log has an implementer BLOCKED report, then a revision noting "decision accepted: interpretation (a)". `bench/cocompact-box-notes.md` §Verdict interpretation calls it the "Accepted reading" without naming who accepted it. The pre-release commit fb00e315 says "Criterion interpretation (a) recorded". No session log covers 2026-07-06 (the logs start 2026-07-15). Only the implementer, reviewer and coordinator agents appear, so this looks like the agents' own decision. Scan wall-time work later went to 0049 and 0050 (both done).
- **Decision needed:** confirm interpretation (a) as the closure of 0042, or record that the wall-time ≥5× target is still open and say where it should be tracked.

## Acceptance criteria

## Work log
