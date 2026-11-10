---
id: "0567"
title: AWS cloud stress-test infrastructure and first (~$100-150) instrumented campaign
status: blocked
priority: 2
created: "2026-11-02"
updated: "2026-11-10"
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
- 2026-11-10 status: tooling, unit tests, local dry runs (3-node and 1-node) and runbook are committed (aa3b318..54c927c). Within the 8-vCPU default quota the first AWS run was the single-instance profile `v1-1node-i4i2xl` (run v1-20261110, ~$0.42, all steps ok; bench/cloud/runs/v1-20261110/FINDINGS.md; spawned 0568, 0569, 0570). `fleet.py sweep`: nothing running.
- 2026-11-10 BLOCKED: the 3-node `c1-3node-i4i` campaign (3-node sweeps, chaos, converge) needs on-demand standard vCPU quota ≥ 544 in us-east-2. The increase request `a01c32b7…` is now CASE_CLOSED and the applied quota is still 8 vCPUs (checked with `aws service-quotas`), so it cannot launch. Unblock: owner gets the `L-1216C47A` quota raised (or picks a smaller multi-node shape that fits the granted quota) and confirms the spend; then follow plans/cloud-aws-runbook.md "Running a campaign". The runbook's "Account state" line still says PENDING.
