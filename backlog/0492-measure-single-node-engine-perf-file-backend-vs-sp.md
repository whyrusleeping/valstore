---
id: "0492"
title: 'Measure single-node engine perf: file backend vs SPDK backend on the same lab 9100 PRO'
status: done
priority: 3
created: "2026-10-19"
updated: "2026-10-19"
depends_on: []
spec_refs: []
---

## Description
User request (2026-10-19): what is the perf difference between the file backend and the SPDK backend on the same drive? Single-node engine test on one lab box.

Plan: lab-1 (stable RAM, has perf). Stop only the idle vals0471 node unit there (PD stays; the cluster has no zones). Build vals-bench --features spdk from HEAD in a separate tree. Run `vals-bench <spdk|file> write|read|batchread|mixed` on the scratch 9100 PRO (serial SERIAL-REDACTED, 0000:81:00.0): SPDK arm via vfio; file arm via a nodiscard XFS on the same drive. Same CPU (cgroup AllowedCPUs), repeats per arm. For file reads report both warm page cache and a cgroup-capped page cache (device-bound). Afterwards return the drive to vfio, re-provision node 1 (`valsd provision` with recorded serial) and restart the vals0471 node unit.

Deliverable: bench/<id>-file-vs-spdk/FINDINGS.md with the table + harness.

## Acceptance criteria

## Work log
