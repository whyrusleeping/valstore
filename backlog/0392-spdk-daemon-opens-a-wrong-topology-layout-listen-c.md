---
id: "0392"
title: SPDK daemon opens a wrong-topology layout (listen-count change / single↔multi-partition) and lost acknowledged data on hardware
status: done
priority: 1
created: "2026-10-06"
updated: "2026-10-06"
depends_on:
    - "0274"
spec_refs: []
---

## Description

Found by the 0274 hardware validation (`bench/0274-provision-hardware/FINDINGS.md`, raw evidence in `raw/`, lab-3 at 2026-10-06).

## Outcome

Zone superblock V3 now stores a topology descriptor in every superblock: layout id, node id, single or multi, partition count and index, extent, and segment geometry. - SpdkZoneStore, multi-partition startup, keyscan and dump check it before any write. - Multi-partition opens check all partitions first and require one shared layout id. - Legacy V1/V2 layouts are refused and must be reprovisioned. - Tests: 10 mem-backed topology tests, each refusal test confirmed by a knockout. Debug suites pass. - Hardware (lab-3): single-over-8, 4-over-8, dump, 8-over-single (stale headers), 4-over-single and legacy layouts were all refused with SMART write Δ 0. 400k keys verified with 0 misses after each batch (bench/0392-topology-hardware).

Commit: fix(0392): persist a topology descriptor in every zone superblock (V3) and refuse wrong-topology/legacy SPDK opens before any write; hardware-verified N2c/N4c refusals with SMART Δ0 and intact data
