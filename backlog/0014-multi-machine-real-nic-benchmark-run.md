---
id: "0014"
title: Multi-machine (real NIC) benchmark run
status: done
priority: 3
created: "2026-07-02"
updated: "2026-11-11"
depends_on: []
spec_refs: []
---

## Description
**Outcome (done 2026-08-06):** the blocker below no longer holds. The lab-1/2/3 cluster arrived and the cross-box runs were done on 2026-07-30; see the 2026-08-06 work-log entry.

BACKLOG.md (the free-form backlog that preceded `backlog/`; its open items became tasks on 2026-07-02 and it was deleted 2026-08-01, so it is not in the public history) §Hardware/benchmarking: all clustered numbers are loopback; ~47% of the serving core is the in-kernel TCP body and the client/server share every core and memcpy (Phase-0 io_uring evaluation, `docs/io-uring-transport.md`, rejected io_uring on this evidence). A real NIC moves that off the critical cores and is the honest test of the clustered ceiling; it also gates the "kernel-bypass TCP" open question (loopback profile is not representative).

**Blocked on hardware availability** — needs a second box (or two NIC-connected machines). **Confirmed with owner 2026-07-05 (no session log covers it, as the logs start 2026-07-15; its only source is the agent-written pre-release commit `ea0e9875` "record owner decisions" in the private pre-release history): no second machine is available; stays blocked** until one exists (the second machine doesn't need SPDK drives — it can just host clients/coordinators reachable from oldbox over a real NIC).

## Acceptance criteria
- Cross-box run of the distmdmc-class workload with results recorded (write/strong-read/get-any vs the loopback staircase). *(Writes and reads recorded; get-any was not swept cross-box, per the 2026-08-06 entry.)*
- Updated honest-accounting section in ARCHITECTURE.md §Status.

## Work log

- 2026-08-06: **Unblocked and satisfied.** Hardware arrived (lab-1/2/3, all
  10G links, external clients) and the cross-box runs were done back on
  2026-07-30 — the `distmdmc` staircase itself is in-process-only, so the
  cross-box analogue was `vals-bench netload`/`netread` against a live `valsd`
  SPDK cluster (RF=2, 0-error, value-verified). Results were recorded in
  `plans/box-provisioning-runbook.md` §First real multi-machine SPDK cluster run
  and `bench/lab-read-prof/notes.md` (local-only bench file; on the devbox the directory carries the real lab host name instead of `lab`) (1.8M writes; reads 420k/core → 820k at
  4 shards/node → ≥2.02M with 4 client hosts at ~15 % node CPU; `iommu=pt`
  reclaims ~9 % of the serving core). Missing criterion was the doc update:
  `ARCHITECTURE.md` §Status now carries a step 10 "Real NIC / multi-machine"
  entry (loopback tax confirmed, wall moved to serving-core count + client
  syscall cost) and step 7 points at it. Open follow-on: get-any was not swept
  cross-box, and the true node ceiling is still unknown for lack of load-gen
  capacity — batched multi-get client is the lever (relates to task 0022, the serving-core lever decision, closed 2026-08-11).

- 2026-11-11 groom: added an outcome line so the Description no longer reads as blocked; noted the 2026-07-05 owner confirmation has no session log and cited its only source, pre-release commit ea0e9875; annotated the get-any criterion as not swept cross-box; noted the bench notes path is local-only with a scrubbed directory name; glossed 0022 and BACKLOG.md; removed a duplicate empty Acceptance criteria heading.
