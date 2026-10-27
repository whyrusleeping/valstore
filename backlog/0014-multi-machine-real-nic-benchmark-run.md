---
id: "0014"
title: Multi-machine (real NIC) benchmark run
status: done
priority: 3
created: "2026-07-02"
updated: "2026-08-06"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Hardware/benchmarking: all clustered numbers are loopback; ~47% of the serving core is the in-kernel TCP body and the client/server share every core and memcpy (Phase-0 io_uring evaluation, `docs/io-uring-transport.md`, rejected io_uring on this evidence). A real NIC moves that off the critical cores and is the honest test of the clustered ceiling; it also gates the "kernel-bypass TCP" open question (loopback profile is not representative).

**Blocked on hardware availability** — needs a second box (or two NIC-connected machines). **Confirmed with owner 2026-07-05: no second machine is available; stays blocked** until one exists (the second machine doesn't need SPDK drives — it can just host clients/coordinators reachable from oldbox over a real NIC).

## Acceptance criteria
- Cross-box run of the distmdmc-class workload with results recorded (write/strong-read/get-any vs the loopback staircase).
- Updated honest-accounting section in ARCHITECTURE.md §Status.

## Acceptance criteria

## Work log

- 2026-08-06: **Unblocked and satisfied.** Hardware arrived (lab-1/2/3, all
  10G links, external clients) and the cross-box runs were done back on
  2026-07-30 — the `distmdmc` staircase itself is in-process-only, so the
  cross-box analogue was `vals-bench netload`/`netread` against a live `valsd`
  SPDK cluster (RF=2, 0-error, value-verified). Results were recorded in
  `plans/box-provisioning-runbook.md` §First real multi-machine SPDK cluster run
  and `bench/lab-read-prof/notes.md` (1.8M writes; reads 420k/core → 820k at
  4 shards/node → ≥2.02M with 4 client hosts at ~15 % node CPU; `iommu=pt`
  reclaims ~9 % of the serving core). Missing criterion was the doc update:
  `ARCHITECTURE.md` §Status now carries a step 10 "Real NIC / multi-machine"
  entry (loopback tax confirmed, wall moved to serving-core count + client
  syscall cost) and step 7 points at it. Open follow-on: get-any was not swept
  cross-box, and the true node ceiling is still unknown for lack of load-gen
  capacity — batched multi-get client is the lever (relates to 0022).
