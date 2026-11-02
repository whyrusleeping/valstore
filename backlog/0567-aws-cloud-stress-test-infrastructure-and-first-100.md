---
id: "0567"
title: AWS cloud stress-test infrastructure and first (~$100-150) instrumented campaign
status: in_progress
priority: 2
created: "2026-11-02"
updated: "2026-11-02"
depends_on: []
spec_refs: []
---

## Description
Owner (2026-11-02): set up an AWS account (pending) and asked for test infrastructure to stress test vals in the cloud: multi-machine, high bandwidth, many raw NVMe per node. First run budget is roughly the low $100s; instrument everything so each dollar teaches as much as possible.

## Scope
- `scripts/cloud/`: stdlib-Python + aws CLI tooling to plan (cost/quota), launch, inventory, and tear down a tagged fleet in one cluster placement group, with a self-terminating deadline on every instance (user-data `shutdown -h` + terminate-on-shutdown) so a dead session cannot leave a bill running.
- Build once per commit on a builder matching the data-node CPU family; ship a tarball (valsd, vals-bench, DPDK libs, SPDK scripts) to the fleet.
- Host prep on bare metal: packages, sysctls, core dumps, hugepages, drive inventory (serials recorded via the kernel driver before SPDK takes them), vfio/uio bind, `valsd provision`.
- Instrumentation on every host for the whole session: valsd/PD /metrics, per-CPU and per-thread CPU, memory, network (incl. ENA allowance counters), TCP stats, interrupts/softnet, NVMe pre/post logs, kernel log, PD leader/term; phase-boundary event log; periodic perf profiles; client per-second samples (netprogress NP_SAMPLE).
- `campaign.py`: phased first campaign (raw ceilings, network ceilings, 3-node valsd throughput sweeps, steady mixed load, small chaos drills, full verification), live fail-fast checks, artifacts under bench/cloud/<run>/.
- A local dry-run mode (file backend, localhost hosts) so orchestration is tested without AWS.

## Acceptance
- Python unit tests (scripts/test_cloud_*.py) for config generation, cost/quota math, metric parsing and live-check logic pass in scripts/ci.sh.
- Local dry run of the campaign against a localhost file-backed fleet completes and produces the artifact tree.
- Runbook in plans/ describing account prerequisites, credentials via `ycc token set`, and the first campaign with its cost estimate.
- The first AWS campaign itself runs only after the owner confirms the account, quota and spend.

## Acceptance criteria

## Work log
