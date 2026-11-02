# AWS cloud campaigns — runbook (task 0567)

Stress tests of vals on rented bare metal: many raw NVMe per node, 75–200 Gbps
networking, ECC memory. Tooling lives in `scripts/cloud/`; results land in
`bench/cloud/` (gitignored). Everything is stdlib Python plus the `aws` CLI.

| Piece | What it does |
|---|---|
| `fleet.py` | `plan` (cost + vCPU quota check), `build` (short-lived builder → `bench/cloud/artifacts/vals-<rev12>.tar.gz`), `up`/`status`/`extend`/`down`/`sweep`, and `local` (a dry-run fleet on this machine) |
| `campaign.py` | runs a profile's steps on a fleet: `run`, `collect`, `report`, `health`, `stop` |
| `profiles/*.json` | fleet shape, cluster geometry and the step list (`c1-3node-i4i`, `local-dry-run`) |
| `remote/hostmon.py` | per-host sampler for the whole session (CPU per core and per thread, net incl. ENA allowance counters, TCP, disk, memory, valsd/PD `/metrics`, NUMA, interrupts, clock); `summarize` cuts it per step |
| `remote/hostprep.sh` | packages, sysctls (core dumps, perf), governor, inventory (`out/inventory.json`: CPU/NUMA, NICs, every NVMe with serial, PCI, NUMA node and role) |
| `remote/spdk-bind.sh` | binds the instance-store drives only (never the EBS root) to vfio-pci, or uio_pci_generic when no IOMMU is exposed; hugepages |
| `remote/rawdisk.sh`, `remote/netbench.sh` | fio (kernel driver) and iperf3/netperf/ping baselines |
| `remote/profile.sh` | perf stat + dwarf call graphs of `valsd node` (folded stacks, per-thread CPU, NUMA, IRQ deltas) |
| `remote/build.sh` | SPDK v24.09 + `valsd`/`vals-bench --features spdk` build on the builder |

## Safety rails

- Every instance is tagged `vals-run=<run>`, launched with shutdown behaviour
  `terminate`, and its user-data arms a systemd timer that powers it off at the
  profile's `hours` (so it terminates even if this machine or session dies).
  `fleet.py extend` re-arms it; `fleet.py down` terminates by tag and waits;
  `fleet.py sweep` exits non-zero if anything tagged is still running — run it
  at the end of every session.
- SSH ingress is this machine's public IP only; all other traffic is allowed
  between members of the `vals-cloud` security group. vals has no transport
  auth, so never open its ports wider.
- `up` asks for the run name as confirmation (or `--yes`) and prints the
  worst-case cost (rate × deadline) first. Get the owner's go-ahead for each
  spend; the first campaign was scoped at roughly $100–150.
- Instance-store drives are disposable by construction, so the campaign runs
  `valsd provision` itself. The serials it passes come from
  `out/inventory.json`, read through the kernel nvme driver before SPDK takes
  the drives; `spdk-bind.sh` refuses anything mounted or classified as EBS.

## Account prerequisites (owner)

1. AWS account with a Budget alert.
2. vCPU quota increases in the chosen region (new accounts default to 5 vCPUs):
   *Running On-Demand Standard (A, C, D, H, I, M, R, T, Z) instances*
   (`L-1216C47A`) ≥ 544 for c1 (3×128 + 2×64 + builder 32); spot (`L-34B43A08`)
   only if a profile uses spot. `fleet.py plan` shows need vs quota.
3. An IAM user/role limited to EC2 (instances, key pairs, security groups,
   placement groups, tags, describe*), SSM `GetParameters` (Ubuntu AMI lookup)
   and Service Quotas read.
4. Credentials into this environment with `ycc token set AWS_ACCESS_KEY_ID`,
   `ycc token set AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` (e.g.
   us-east-1; prices in `cloudlib.PRICES` are us-east-1). The local aws CLI is
   v1.22 — fine for these calls; upgrade if a newer instance type is rejected.

## Running a campaign

```sh
scripts/cloud/fleet.py plan  --profile c1-3node-i4i
scripts/cloud/fleet.py build --rev <commit>               # ~30-45 min on c6i.8xlarge, ~$1
scripts/cloud/fleet.py up    --profile c1-3node-i4i --run c1-<date> [--az us-east-1b]
scripts/cloud/campaign.py run --run c1-<date> --artifact bench/cloud/artifacts/vals-<rev12>.tar.gz
scripts/cloud/fleet.py down  --run c1-<date> && scripts/cloud/fleet.py sweep
```

Build on the data nodes' CPU generation (c6i = Ice Lake for i4i; c7i for
i7ie): SPDK/DPDK compile with `-march=native`, and `campaign.py` refuses a
host missing any CPU flag the builder had. Build from a commit (`git archive`),
never from the dirty tree.

`campaign.py run` resumes with `--from <step>` or runs `--only a,b`. It always
collects (`hosts/<host>/` = each host's `out/`) and writes `report.md` at the
end, also after an abort; `collect` can be run any time mid-campaign. Watch it
live with `campaign.py health --run ...` and `tail -f bench/cloud/runs/<run>/events.jsonl`.

Before any cloud run, dry-run the machinery locally (3 file-backed nodes + a
client on localhost, ~10 min):

```sh
cargo build --release -p vals-server -p vals-bench
scripts/cloud/fleet.py local --run dry1
scripts/cloud/campaign.py run --run dry1
```

## First campaign: `c1-3node-i4i` (~$100–120)

Fleet: 3× i4i.metal ($10.98/h; 8× 3.75 TB instance store, 75 Gbps, 2 NUMA,
ECC) + 2× c6in.16xlarge clients ($3.63/h, 100 Gbps) = **$40.21/h**. Each node
runs one voting PD (three-PD reference layout) and `valsd` with 16 shards
(2 partitions × 8 drives), RF 3, default `capacity` profile, AE on, GC off.
Expected wall time ~2–2.5 h including ~15 min boot → ~$90–110; the self-destruct
deadline is 3 h (worst case $121).

| Steps | Question it answers |
|---|---|
| `net` | per-flow and multi-flow TCP ceilings client→node, node mesh, fan-in; RTT (`TCP_RR`, ping) |
| `rawdisk` | AWS Nitro SSD ceilings through the kernel (fio 4k/128k, random/sequential, mixed), latency histograms |
| `spdkbench` | the vals engine on the 8 drives over SPDK (`mdwrite` 100 B and 4 KiB, `mdread`) vs the raw ceiling |
| `w-*`, `wb-*` | replicated quorum write throughput vs client concurrency, 100 B vs 4 KiB, batched, device-durable |
| `prefill`, `r-*`, `verify-prefill` | strong point reads and batched reads at 96 M keys; every prefilled key read back and value-checked |
| `m1`–`m3` | verified mixed load (netprogress) closed-loop, then open-loop at 50% (latency), then device-durable |
| `chaos` | under 30% load: kill -9 a node, black-hole a node, kill the PD leader, SIGSTOP a node, graceful restart — client error/latency timeline, restart-to-serving and AE-settle times |
| `settle`, `converge`, `restart`, `verify-after-restart` | AE quiescence, live-key equality across the three full replicas, whole-cluster cold restart time, and a full read-back after it |

Profiled steps also capture perf on node1 and client1. Live checks stop the
campaign on any integrity counter (`vals_*integrity*`, corrupt reads, run-build
verify failures, durable-write errors), an unexpected process exit, or a bad
netprogress read; ENA allowance-exceeded and busy rejections are recorded as
incidents. On ECC hardware any integrity event is new evidence (see
`docs/evaluation.md#what-the-evidence-does-not-show`): keep the core
(`out/cores/`), the logs and the event timeline.

## Known gaps / next profiles

- IOMMU on i4i.metal is expected (Ubuntu's linux-aws has `INTEL_IOMMU_DEFAULT_ON`)
  but unverified; `spdk-bind.sh` falls back to `uio_pci_generic` and records why.
- `nvme amzn stats` for instance store needs nvme-cli ≥ 2.16 (24.04 ships 2.8),
  so only smart-log is captured for now.
- Not yet covered: fill-and-age soak (cap `default_zone_fraction` to reach the
  full-disk/merge walls in hours), 6/12-node scaling, node add/remove under load,
  a multi-hour ECC soak, latency profile, sorted zones/scans.
