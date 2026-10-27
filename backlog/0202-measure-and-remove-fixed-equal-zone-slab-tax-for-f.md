---
id: "0202"
title: Measure and remove fixed equal-zone slab tax for full-network capacity
status: done
priority: 1
created: "2026-08-13"
updated: "2026-08-14"
depends_on: []
spec_refs:
    - docs/multi-drive.md#Named zones on the sharded (JBOD) path
    - plans/bootes-soak-runbook.md
    - bench/task-0200-atproto-backfill/FINDINGS.md
---

## Description
The 24B ATProto sizing pass found that each partition currently reserves equal fixed `zone_region_mib` slabs for default, records, and backlinks. A nearly empty default zone therefore consumes one third of raw partition space and unequal records/backlinks cannot borrow capacity, pushing a roughly 40–43 TiB live RF=2 dataset toward 30+ TB raw/node on a three-node shape.

Design and hardware-measure a safe capacity geometry improvement: per-zone region sizing, elastic zone extents, omission/lazy growth of unused default, or an equivalently bounded approach. Preserve allocator/manifest crash safety and maintenance headroom.

Acceptance criteria:
- Measure per-zone/per-shard allocation on a statistically useful real records+backlinks slice and derive worst-shard production sizing.
- Specify format/config/recovery/upgrade semantics and prevent overlapping or silently undersized regions.
- Demonstrate materially better usable/raw capacity than equal fixed slabs without write-throughput, recovery, or correctness regression.
- Add kill-the-fix geometry/reopen/exhaustion tests and document deployment sizing/rollback.

## Acceptance criteria

## Plan

1. Confirm the current JBOD allocator/config/boot geometry and the task-0200 real-corpus evidence, then choose a backward-compatible bounded design: retain `zone_region_mib` as the fallback, add explicit per-zone-id MiB overrides for new allocations, and make the persisted region table support non-overlapping variable contiguous extents. Existing on-device region records keep their exact persisted base/length; config changes never silently resize them.
2. Implement variable-extent allocation and recovery validation in `zonestore.rs`: segment-aligned, in-bounds extents; reject duplicate IDs, overlap, malformed/undersized lengths, and arithmetic overflow; first-fit freed gaps plus tail allocation; preserve wipe-before-publish, double-buffered table durability, drop/reuse semantics, and loud exhaustion. Thread immutable overrides through the JBOD node/server configuration while preserving old configs and single-drive behavior.
3. Add config validation and clear operator-facing syntax for per-zone overrides (including duplicate IDs, zero/too-small/misaligned or individually impossible sizes, and fallback behavior). Document that IDs are stable catalog IDs, existing extents are not resized on `format=false`, a fresh format is required to apply geometry changes to existing zones, and rollback is removal of overrides plus reformat/replay—not opening a variable layout with code that cannot validate it.
4. Add kill-the-fix tests for mixed-size allocation, gap reuse/coalescing behavior, overlap/out-of-range/undersized recovery rejection, exhaustion, default fallback, durable reopen with changed config preserving persisted lengths, and dropped-zone no-resurrection. Run focused and full debug Rust suites without cargo fmt.
5. Build and deploy a current binary to the lab/bootes hardware using the runbook safeguards. On a fresh capacity-profile records+backlinks slice large enough to cross multiple flush/merge generations, capture per-zone/per-shard used/free segments and throughput/recovery/correctness for equal-slab baseline versus a variable geometry with a small default and records/backlinks sized from measured ratio. Reopen `format=false`, verify data, and exercise bounded exhaustion/isolation. Retain raw configs/logs/metrics/provenance.
6. Derive worst-shard full-network production sizing from the real slice (including RF=2, observed physical amplification/skew, and 20–30% maintenance headroom), quantify usable/raw improvement and throughput comparison, and update findings plus deployment/multi-drive/format/runbook documentation with sizing and rollback guidance.

### Starting points
- crates/vals-cluster/src/zonestore.rs: ZoneRegion, RegionTable, RegionZoneConfig/Store, variable `len` is already persisted in v2 records
- crates/vals-server/src/config.rs: `zone_region_mib` parse/validation; preserve it as fallback
- crates/vals-server/src/run_node.rs and crates/vals-cluster/src/node.rs: JBOD RegionZoneConfig plumbing
- docs/multi-drive.md#Named zones on the sharded (JBOD) path
- bench/task-0200-atproto-backfill/FINDINGS.md capacity arithmetic and required validation
- plans/bootes-soak-runbook.md box safeguards and corpus setup

## Work log
- 2026-08-14 plan: 1. Confirm the current JBOD allocator/config/boot geometry and the task-0200 real-corpus evidence, then choose a backward-compatible bounded design: retain `zone_region_mib` as the fallback, add expli
…[truncated]
- 2026-08-14 context hints: 6 recorded with plan
- 2026-08-14 context hints: crates/vals-cluster/src/zonestore.rs: ZoneRegion, RegionTable, RegionZoneConfig/Store; variable `len` is already persisted in v2 records; Preserve unrelated dirty work already present in the shared tr
…[truncated]
- 2026-08-14 preload: 8 file(s), ~64 KiB seeded into implementer context
- 2026-08-14 implementer report: BLOCKED — The code/config/tests/docs and retained-corpus worst-shard sizing are implemented, but the required variable-vs-equal hardware A/B cannot be run safely without an owner decision: all three lab raw
…[truncated]
- 2026-08-14 revision: Implemented task 0202 end-to-end.  Code/config: - Added repeated JBOD-only `zone_region_mib_override = <stable-zone-id>:<MiB>` parsing, duplicate/zero/undersized/misaligned/overflow/out-of-bounds vali
…[truncated]
- 2026-08-14 review tier: high-powered — reviewers: sol, sol#2 (sol)
- 2026-08-14 review (sol): accept — The change satisfies task 0202. It implements bounded per-zone JBOD extent overrides while preserving fallback behavior and persisted base/length authority, validates configured and recovered geometry
…[truncated]
- 2026-08-14 review (sol#2/sol): accept — The variable per-zone geometry is implemented cleanly and correctly across allocator recovery, JBOD configuration/plumbing, and operator tooling. Persisted extents remain authoritative on reopen; allo
…[truncated]
- 2026-08-14 decision: accept — commit: feat(0202): support variable per-zone JBOD extents
- 2026-08-14 usage: 42,724,747 tok (in 4,580,616, out 88,707, cache_r 38,055,424, cache_w 0) · cost n/a (unpriced)
  implementer: 37,838,846 tok (in 2,507,778, out 69,116, cache_r 35,261,952, cache_w 0) · cost n/a (unpriced)
  reviewer:sol#2: 1,760,651 tok (in 600,154, out 6,193, cache_r 1,154,304, cache_w 0) · cost n/a (unpriced)
  coordinator: 1,640,229 tok (in 893,180, out 6,953, cache_r 740,096, cache_w 0) · cost n/a (unpriced)
  reviewer:sol: 1,485,021 tok (in 579,504, out 6,445, cache_r 899,072, cache_w 0) · cost n/a (unpriced)
