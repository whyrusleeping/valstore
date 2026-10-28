# Box-validation node provisioning — runbook (lab fleet)

The box-validation rig that replaces the retired `oldbox` (spec.md
§Constraints; memory.md). Three machines — **lab-1 / lab-2 / lab-3** —
each a fresh Ubuntu box with a dedicated scratch NVMe, provisioned identically
and repeatably by [`scripts/provision-node.sh`](../scripts/provision-node.sh).

## Fleet inventory

| Host | IP | Role | CPU | RAM | OS drive | Scratch NVMe (SPDK target) |
|---|---|---|---|---|---|---|
| lab-1 | 10.0.0.114 | node | Ultra 9 285HX, 24c/1NUMA | 60 GiB | Samsung 990 PRO 4TB (`nvme0n1`, PCI `0000:01:00.0`) | Samsung 9100 PRO 4TB (`nvme1n1`, PCI `0000:81:00.0`, raw) |
| lab-2 | 10.0.0.18 | node | Ultra 9 285HX, 24c/1NUMA | 60 GiB | Samsung 990 PRO 4TB (`nvme1n1`, PCI `0000:01:00.0`) | Samsung 9100 PRO 4TB (`nvme0n1`, PCI `0000:81:00.0`, raw) |
| lab-3 | 10.0.0.200 † | node | Ultra 9 285HX, 24c/1NUMA | **30 GiB** (bad DIMM — replacement pending) | Samsung 990 PRO **2TB** (`nvme1n1`, PCI `0000:81:00.0`) | Samsung 9100 PRO 4TB (`nvme0n1`, PCI `0000:01:00.0`, raw) |

† **lab-3 networking:** cabled on the **10G SFP+ port `eno1np0`** (DAC), not
the RJ45 `eno3` the others use (`eno3` is **10GBASE-T — also 10G**, so all three
nodes are on 10G links). The installer only enabled DHCP on `eno3`, so the
DAC port came up IPv6-only until we added `dhcp4/dhcp6` to `eno1np0` in
`/etc/netplan/00-installer-config.yaml` (backup: `*.yaml.bak`); it now leases
`10.0.0.200` on the same subnet. That's a **DHCP lease** (not reserved) — it
can change on reboot; a workstation `~/.ssh/config` alias pins `lab-3` →
`.200`. Consider a DHCP reservation (or static) before relying on it long-term.

> **Historical inventory, not authorization to overwrite a drive.** Recheck the
> live hardware before use; device names, PCI addresses, installed RAM, and retained
> data can change. A model match identifies candidates, not the intended controller.
> Record the intended controller's serial independently, verify its live PCI mapping,
> and follow [offline provisioning](../docs/deploy.md#offline-spdk-provisioning).
> Stop all daemons and resolve data-retention holds before provisioning. In particular,
> the retained SHUT0326-F2-R2 corpus is incident evidence for task 0336; a generic
> benchmark recipe is not permission to erase it.

## What provisioning does

`scripts/provision-node.sh` is idempotent, non-destructive (never touches the
scratch drive's data), and staged. Run the whole thing or one stage:

```sh
# On the node (most stages need root):
sudo ./scripts/provision-node.sh                # all stages
sudo ./scripts/provision-node.sh --only net-fix # just one

# Or drive it over ssh from your workstation:
ssh lab-1 'sudo bash -s' < scripts/provision-node.sh
```

Stages:

| Stage | Root? | What |
|---|---|---|
| `packages` | yes | build-essential, clang, pkg-config, libssl-dev; `nvme-cli`, `numactl`, `fio`, `jq`, `linux-tools` (perf); SPDK build deps (`meson`, `ninja-build`, `nasm`, pip `pyelftools`/`jinja2`/`tabulate`) |
| `needrestart` | yes | exclude `^vals` units from automatic needrestart restarts (`/etc/needrestart/conf.d/90-vals.conf`); leave unattended-upgrades enabled |
| `rust` | no (drops to `$SUDO_USER`) | rustup + pinned toolchain **1.96.0** (rustfmt, clippy — keep in sync with `rust-toolchain.toml`); adds `~/.cargo/bin` to `~/.profile` (non-interactive ssh needs it — memory.md) |
| `net-fix` | yes | the boot-hang fix (see below) |
| `governor` | yes | CPU governor → `performance`, persisted via `cpu-performance.service` |
| `spdk-prep` | yes | confirm IOMMU, autoload `vfio-pci`. Hugepages are reserved **per run** by `third_party/spdk/scripts/setup.sh HUGEMEM=<MiB> PCI_ALLOWED=...`, not at boot — so **every reboot drops them** (`HugePages_Total` back to 0, scratch drive back on the `nvme` driver) and this must be re-run before the next soak. `HUGEMEM=8192` ⇒ 4096 hugepages |
| `verify` | no | prints an as-provisioned summary |

The scratch NVMe is left **raw** — the SPDK backend claims it directly (vfio), so
we do not partition or mount it. Host setup does not discard or format it.
`--blkdiscard` is retired: a device-name confirmation is not an identity or retention
check. Fresh VALS layouts use only the [offline provisioner](../docs/deploy.md#offline-spdk-provisioning).
TRIM is not a routine prerequisite; an experiment that specifically needs device
preconditioning requires its own reviewed identity, retention, and stop checks.

After a full run, **reboot** to pick up `net-fix` and `governor` (both are
boot-time), then re-run `--only verify`.

## The boot-hang fix (net-fix)

**Symptom (all three machines will show it):** boot stalls ~2 minutes; on the
console `systemd` sits on cloud-init / "waiting for network". Unplugging and
replugging the cable *appears* to unstick it.

**Root cause:** these boxes have several NICs (lab-1: 4 wired `eno1np0`,
`eno2np1`, `eno3`, `eno4` + wifi `wlo5`) but only one is cabled. The
netplan/networkd generator makes `systemd-networkd-wait-online` wait for **every
managed interface** to reach `degraded`:

```
ExecStart=/lib/systemd/systemd-networkd-wait-online -i eno4:degraded -i eno1np0:degraded -i eno2np1:degraded -i eno3:degraded
```

The un-cabled NICs never come up, so it burns the **full 120 s timeout every
boot**, then fails; cloud-init and the rest of userspace are queued behind it.
The unplug/replug is a red herring — the timeout expires at ~120 s regardless
(the replug just re-acquired DHCP a few seconds later).

**Fix:** a drop-in that makes wait-online succeed when **any** one interface is
online, with a short timeout. It must sort **after** netplan's generator drop-in
(`/run/systemd/generator.late/.../10-netplan.conf`, regenerated every boot) or
netplan's `ExecStart=` silently overrides ours — so the filename is `99-any.conf`:

```ini
# /etc/systemd/system/systemd-networkd-wait-online.service.d/99-any.conf
[Service]
ExecStart=
ExecStart=/lib/systemd/systemd-networkd-wait-online --any --timeout=20
```

Effective on the next boot. **Verified on all three boxes:** userspace boot
dropped from ~2 min → **6–9 s** (total ~33 s incl. ~18–21 s firmware POST),
wait-online `active/success`, zero failed units. (Alternative considered: mark
the spare NICs `optional: true` in netplan — rejected because it's per-host,
per-NIC-name and would need editing on each machine; `--any` is fleet-generic.)

## PCIe AER storm decision (task 0135)

**Decision (2026-08-11): keep Secure Boot and PCIe error reporting enabled, and
BIOS-disable the unused Thunderbolt/USB4 controller on each box during the next
attended maintenance window. This is pending and has not been applied on any
box. Until that window, we explicitly accept the journal noise.** Do not add
`pci=noaer` or `pcie_ports=compat`, and do not disable Secure Boot as a workaround.

The recorded evidence and topology are in the
[lab-2 wedge autopsy](../bench/bootes-soak-lab/run-20260804T220428Z-batch1000/node2-wedge-autopsy/FINDINGS.md#separate-chronic-issue-pcie-aer-storm).
The storm repeats at about 24 lines/s across the fleet and is rooted at the
Thunderbolt/USB4 port recorded as `0000:80:1b.4`; the NVMe devices were verified
on other root ports. Secure Boot puts the kernel in lockdown `integrity` mode,
which correctly rejects the otherwise-targeted `setpci` write. The alternatives
were rejected as follows:

- `pci=noaer` would hide errors from the scratch and OS NVMe drives, an
  unacceptable loss of storage diagnostics.
- `pcie_ports=compat` is broader still: it disables native PCIe port services,
  including AER, hotplug, and PME.
- Disabling Secure Boot solely to mask `ACSViol` weakens boot integrity when an
  attended firmware setting can remove the unused source instead.

Disabling only Thunderbolt/USB4 in firmware leaves Linux's native AER service and
the NVMe endpoints' correctable, non-fatal, and fatal error reporting unchanged.
It is deliberately not part of `provision-node.sh`: it is a physical, per-host
firmware operation. Because the fix is not yet applied, no normal journal rate or
post-change persistence is claimed here.

### Attended application and verification

Apply to **one host at a time**, keeping a known-good host available. Do not do
this during a soak. Gracefully stop `valsd`, return the SPDK device to the kernel
as described in the teardown procedure below, and record the exact firmware
setting and firmware version. Run these read-only checks on the host before the
change to preserve a baseline (journal access may require `sudo`):

```sh
mokutil --sb-state
cat /sys/kernel/security/lockdown
cat /proc/cmdline
lspci -DPPnn | awk 'tolower($0) ~ /non-volatile memory|thunderbolt|usb4|usb controller|pci bridge/'
sudo journalctl -k -b --since '-5 minutes' --no-pager -o cat |
  awk '/AER:|PCIe Bus Error|ACSViol|UnsupReq|HeaderOF/ { n++ }
       END { printf "AER-pattern lines in 5 min: %d (%.2f/s)\n", n, n/300 }'
```

Then perform the only configuration change in this procedure:

1. Reboot into firmware setup using the machine's local console.
2. Disable the **Thunderbolt/USB4 controller** (the exact label may vary), not a
   global `AER`, `PCIe error reporting`, or PCIe port-services option.
3. Save the setting and boot Linux. Record the exact setting label in the ledger
   below.

After boot, leave the host idle for at least ten minutes, then run the following
read-only checks:

```sh
# Secure Boot/lockdown must remain enabled, and neither global suppression flag
# may have appeared on the kernel command line.
mokutil --sb-state
cat /sys/kernel/security/lockdown
if cmdline=$(cat /proc/cmdline); then
  case " $cmdline " in
    *" pci=noaer "*|*" pcie_ports=compat "*)
      echo 'FAIL: global PCIe error suppression is active'
      ;;
    *)
      echo 'PASS: no global PCIe error suppression flag'
      ;;
  esac
else
  echo 'FAIL: could not read /proc/cmdline'
fi

# Compare this topology with the baseline. The disabled controller may vanish;
# do not assume PCI bus numbers remain unchanged after a firmware topology edit.
lspci -DPPnn | awk 'tolower($0) ~ /non-volatile memory|thunderbolt|usb4|usb controller|pci bridge/'

# The repeating storm signatures should be zero or near zero. Investigate and
# classify any residual lines rather than hiding them with a broader switch.
sudo journalctl -k -b --since '-10 minutes' --no-pager -o cat |
  awk '{ total++ }
       /AER:|PCIe Bus Error|ACSViol|UnsupReq|HeaderOF/ { aer++ }
       END { printf "kernel lines: %d; AER-pattern lines: %d (%.3f/s)\n",
                    total, aer, aer/600 }'

# A boot may have no matching enable line; show one when present, but rely on
# the endpoint checks below rather than treating absence as failure.
sudo journalctl -k -b --no-pager -o cat | grep -E 'AER: enabled with IRQ' ||
  echo 'INFO: no AER enable line in this boot log'

# Discover NVMe endpoints by PCI class rather than unstable BDF. Both the
# DevCtl error controls and AER capability are required on each of the two
# endpoints; print FAIL and return a failing status if either is missing.
nvme_count=0
nvme_failed=0
for dev in /sys/bus/pci/devices/*; do
  [ "$(cat "$dev/class")" = 0x010802 ] || continue
  bdf=${dev##*/}
  nvme_count=$((nvme_count + 1))
  echo "NVMe $bdf"
  if details=$(sudo lspci -Dvv -s "$bdf"); then
    printf '%s\n' "$details" |
      grep -E 'DevCtl:.*(CorrErr|NonFatalErr|FatalErr|UnsupReq)' || {
        echo "FAIL: NVMe $bdf has no visible PCIe error controls"
        nvme_failed=1
      }
    printf '%s\n' "$details" | grep -E 'Advanced Error Reporting' || {
      echo "FAIL: NVMe $bdf has no visible AER capability"
      nvme_failed=1
    }
  else
    echo "FAIL: could not inspect NVMe $bdf"
    nvme_failed=1
  fi
done
if [ "$nvme_count" -eq 2 ] && [ "$nvme_failed" -eq 0 ]; then
  echo 'PASS: both NVMe endpoints retain PCIe error controls and AER capability'
else
  echo "FAIL: NVMe AER validation failed (checked $nvme_count endpoints; expected 2)"
  false
fi
```

A host passes when the Thunderbolt/USB4 storm no longer repeats, Secure Boot is
still enabled with `[integrity]` selected in the lockdown file, neither global
suppression flag is present, and both NVMe endpoints still expose their AER
capability/error controls. Do not inject an AER fault to test this. Reboot the
host once more and repeat the post-boot checks to prove the BIOS setting and
quiet journal persist across reboots before proceeding to the next host.

Record completion here; all rows intentionally remain pending until attended
work and both post-change boots have actually been verified:

| Host | Firmware setting | Applied UTC | First/second boot | 10-min AER lines/rate | Secure Boot + lockdown | NVMe AER | State |
|---|---|---|---|---|---|---|---|
| lab-1 | — | — | — | — | — | — | pending |
| lab-2 | — | — | — | — | — | — | pending |
| lab-3 | — | — | — | — | — | — | pending |

If firmware exposes no controller-specific disable control, or disabling it does
not stop the storm, leave that host unchanged and capture its firmware version,
available setting names, topology, and ten-minute journal count. The fallback is
the targeted `ACSViol` mask after disabling Secure Boot, but that weakens boot
integrity and therefore requires a separate explicit owner decision and reviewed
boot-time unit. It is not authorized by this decision. Fleet-wide `pci=noaer`
and `pcie_ports=compat` remain rejected.

## Verify a good boot after the fix

```sh
ssh lab-1 systemd-analyze          # userspace should drop from ~2min to a few s
ssh lab-1 systemctl --failed       # expect 0 failed units
ssh lab-1 'systemctl status systemd-networkd-wait-online.service | grep Active'
```

## Building on the box

The repo pins Rust 1.96.0 (`rust-toolchain.toml`); the `rust` stage installs it.
For SPDK, clone the pinned tag and build per docs/SPDK.md (§Building SPDK):

```sh
export PATH=$HOME/.cargo/bin:$PATH    # non-interactive ssh (memory.md)
cargo build --release -p vals-server                              # non-SPDK
VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk   # SPDK node
```

`~/vals` is an rsynced hardware tree, not a Git checkout. Never mirror the whole
workspace with deletion enabled: each node has its own built SPDK/DPDK tree and
runtime evidence. `--delete` is appropriate only for the explicitly scoped source
inputs in [the soak build procedure](bootes-soak-runbook.md), with `third_party/`,
`target/`, `.git/`, and `run/` protected. Build a
non-interactive remote command with an explicit manifest path, for example
`cargo build --manifest-path /home/user/vals/Cargo.toml ...`.

## First real multi-machine SPDK cluster run (2026-07-30)

The distributed path had only ever run over loopback on one box; this is the
first run across **three networked machines** (docs/SPDK.md §Remaining). RF=2,
3 SPDK nodes + 1 PD, driven by the new multi-threaded `vals-bench netload` client
from the workstation over a real NIC. **All links are 10G** (`eno3` on lab-1/2
is 10GBASE-T; lab-3 on 10G DAC; client 10G; <0.25 ms RTT) — the network is not
the bottleneck. **Result (RF=2, all runs 0 errors / 0 misses / correctness clean;
live keys summed to exactly RF×n):**

| client | write (RF=2) | strong-read |
|---|---|---|
| 1 coord × 128 conc | 172k ops/s | 158k ops/s |
| 16 coords × 64 conc | **1.23M ops/s** | **823k ops/s** |
| 32 coords × 64 conc | **1.80M ops/s** | 341k ⚠️ |

A single coordinator is CPU-bound on TCP+quorum work far below the cluster; scale
`ncoord`. Writes scale to ~1.8M ops/s; **strong-reads peak ~16 coords then drop at
32** — a server-side read-path contention signal at high fan-in worth chasing.
Repeatable procedure:

```sh
# 1. Stop all daemons. Resolve retention holds and independently verify the
# controller serial and its current PCI address per docs/deploy.md.
# Do not select a drive for destruction by model alone; do not blkdiscard it.
: "${pci:?set the independently verified PCI address}"
# HUGEMEM is in MiB; size it for the actual host, not the historical inventory.
cd ~/vals && sudo env PCI_ALLOWED="$pci" HUGEMEM=8192 third_party/spdk/scripts/setup.sh

# 2. Shared secret + per-node config (~/vals/run/{secret,node.conf}); PD on node 1.
#    node.conf: node_id, listen=0.0.0.0:7001, backend=spdk, pci=<scratch>,
#      rf=2, peer=<others>, secret_file, admin_socket. Do not add format=true.
#    pd.conf:   pd_id=1, data_dir, data_node=<id> @ <ip>:7001 (x3), rf=2, secret_file.

# 3. If and only if a fresh layout is explicitly authorized, follow
#    docs/deploy.md#offline-spdk-provisioning. Existing layouts need no provision.
#    Use independently recorded serials, not values read from the selected device
#    and immediately passed back as the expected identity.
LIB=~/vals/third_party/spdk/dpdk/build/lib

# 4. Launch. SPDK valsd MUST run as root (vfio) with the DPDK lib path, and be
#    fully detached or ssh hangs on the channel — use setsid + stdin from /dev/null:
sudo setsid env LD_LIBRARY_PATH=$LIB VALS_LOG=info \
  ~/vals/target/release/valsd node -c ~/vals/run/node.conf </dev/null >~/vals/run/node.log 2>&1 &
# PD runs as `why` (no device), same detach. Verify: valsd admin -s run/pd.sock members.

# 5. Drive load from any machine on the subnet (client needs no SPDK):
#    args: n vsize "id@ip:7001,..." <per-coord-conc> <rf> <ncoord-threads>
vals-bench x netload <n> <vsize> "1@ip1:7001,2@ip2:7001,3@ip3:7001" 64 2 16
```

Teardown: stop the exact units or recorded PIDs for this run, not processes selected
by a command-line substring. Wait for graceful drain/sync and confirm the units have
exited and `pgrep -x valsd` is empty before returning drives to the kernel with
`sudo third_party/spdk/scripts/reset.sh`. If shutdown fails, preserve diagnostics and
resolve it before resetting devices; see [SPDK ownership](../docs/SPDK.md).

## Operational notes (moved from memory.md, 2026-10-11)

- **RAM** is 64 GiB per box as of 2026-10 (the inventory above is historical; check
  with `free -g`). The non-ECC RAM flips bits on all three boxes (0361).
- **Scratch identity and no-write proof.** Scratch BDFs/serials and the SPDK no-write
  smart-log bracket are in [0355-readonly-keyscan.md](0355-readonly-keyscan.md).
- **Clean stop.** From 0424 on, valsd leaves no `spdk*map_*` files on any exit path (0337).
  For older binaries, wait until no `valsd` is running (`pgrep -x valsd`), then run
  `sudo rm -f /dev/hugepages/spdk*map_*`.
- **9100 PRO write cliff.** Write latency degrades after ~1 TB written since the last
  discard/dealloc (FULL1). Record NUSE around long arms.
- **lab-3 panic reboots.** Bit flips can panic-reboot lab-3 mid-soak (evidence
  in `/var/lib/systemd/pstore`). The reboot may upgrade the kernel and reset
  `core_pattern`. Restore both, plus hugepages and vfio, before relaunching
  ([soak-resume-runbook.md](soak-resume-runbook.md) automates the sysctls).
- **Routing.** The devbox→lab-2 route is runtime-only. Run
  `ip route get 10.0.0.18` before benches. devbox reaches lab-1 and lab-3 via
  `eno1np0` and lab-2 via `eno1np1`, so client link use must sum both. The Rust client
  sends every write to all RF replicas (no server forwarding): RF 3 costs 3× payload egress,
  and about 1M msgs/s of 512 B saturates `eno1np0`.
- **9100 PRO features** (fw 0B2QNXH7, `nvme id-ctrl` on lab-3, 2026-10-13): ONCS 0x5d =
  Compare, DSM/deallocate, Write Zeroes (DEAC ok, deallocated blocks read 0) and Timestamp. No
  Copy, Verify, fused ops, directives/streams, FDP, ZNS, PI/metadata (512 B LBA format only) or
  SGL. VWC enabled, AWUPF 0, MDTS 512 KiB. To query a vfio-bound drive, run `setup.sh reset`
  with `PCI_ALLOWED`, then nvme-cli, then rerun `setup.sh` with `HUGEMEM=8192`.
- **Harness recipes** (A/B arms, the 0471 queue cluster, the lab-2 reboot watchdog, returning
  a drive to vfio after an XFS arm): [ab-harnesses.md](ab-harnesses.md).
- **Profiling.** devbox has no perf (`perf_event_paranoid=4`, no sudo). Profile
  shard CPU on lab-1 as in `bench/0390-shard-cpu/FINDINGS.md`. Use the `cpu_core`
  PMU and read leaf symbols. Prometheus histogram `_sum` values are seconds. perf data
  recorded from a root valsd needs `perf report -f`.
- **After a reopen,** gate aediag on `vals_ae_seed_active` = 0 on all nodes. Before that,
  its Busy message is misleading (0433).
- **run_full1 resumes:** the real binary of a resumed attempt is in `server-install.json`, not
  `final.json`'s `server_sha256`.
- **Credentials.** `JETSTREAM_API_KEY` is in `~/code/bootes/soak/jetstream.env`.

## Open items

- ✅ All three boxes provisioned + boot-fixed (2026-07-30). Passwordless sudo for
  `why` is enabled fleet-wide.
- ✅ SPDK + vals built on all three via `scripts/build-spdk.sh` (clones pinned
  `v24.09`, builds DPDK/SPDK, then `VALS_SPDK_LINK=1 cargo build --features spdk`).
  Ubuntu 26.04 / gcc 15 needed extra apt deps now baked into `provision-node.sh`:
  `autoconf automake libtool libtool-bin` (ISA-L autogen) and
  `libnuma-dev libaio-dev uuid-dev` (DPDK/SPDK). SPDK's `pkgdep.sh` aborts early
  on a PEP-668 pip error, so we don't rely on it for the apt deps.
- ✅ **Single-node SPDK smoke validated on lab-1** (2026-07-30, scratch 9100
  PRO @ `0000:81:00.0`): `vals-bench spdk write` 4.18M ops/s correctness-OK;
  `asyncread` QD128 1-core **1.45M IOPS / 5.66 GiB/s**; `shardread` 8-core 1.65M
  IOPS — past the retired PM9A3 box's single-drive ceiling. Bench run as root
  with `LD_LIBRARY_PATH=third_party/spdk/dpdk/build/lib` (memlock-limited as user).
- ✅ **First multi-machine RF=2 cluster run** — see the section above.
- ✅ **Read-path investigated + live-node profiling stood up** (2026-07-30, see
  `bench/lab-read-prof/notes.md`). The apparent read "regression at 32 coords"
  was a single-pass measurement artifact; under sustained load (`netread` +
  `VALS_NETREAD_SECS`) 16 and 32 coords both hit **~420k strong-reads/s/node**,
  which is **single-serving-core bound** (one `vals-shard` thread, task-clock 1.0
  CPU). ~40% of that core is kernel networking, incl. **~14% per-packet IOMMU DMA
  mapping** on the Realtek r8169 NIC (side effect of IOMMU-for-vfio). Profile with
  `scripts/profile-node.sh`. **Next levers:** (a) shard nodes across serving cores
  (the real read-scaling fix); (b) `iommu=pt` kernel cmdline to reclaim the NIC
  IOMMU cost while keeping vfio — untested, needs a reboot.
- ✅ **Sharding fix applied — ~1.9× read throughput** (2026-07-30): reconfigured
  each node to serve **4 SPDK partitions on its one drive** (4 `listen` addrs →
  `serve_multidrive_zoned`) and taught `netload` to drive sharded nodes
  (`connect_sharded`, peer format `id@a1,a2,..;id@..`). Peak strong-read
  ~420k → **~820k ops/s**, node CPU 1.0 → 2.7 cores, 0 server errors, correctness
  clean. Not a full 4×: the single load client co-bottlenecks past ~16 coords.
  Next: multi-client load to find the true 4-shard ceiling; then A/B `iommu=pt`.
  Details in `bench/lab-read-prof/notes.md`.
- ✅ **`iommu=pt` applied fleet-wide — reclaims the NIC IOMMU DMA cost**
  (2026-07-30). A/B on the 4-shard config: `clflush_cache_range` 2.0%→**0%**,
  `iommu_map` 3.6%→**0%**, r8169 tx_map 3.9%→**0.1%** — **~9% of serving-core CPU
  reclaimed**, +4% read throughput (masked by the single-client bottleneck; more
  on a saturated node). vfio still assigns the scratch NVMe fine. Now baked into
  `provision-node.sh` (spdk-prep, needs a reboot) and set on all three boxes
  (`/etc/default/grub`, backup `.bak`). Details in `bench/lab-read-prof/notes.md`.
- ✅ **Multi-client load — read ceiling is client-bound, ≥2M reads/s** (2026-07-30):
  distributing `netread` across hosts (`scripts/netread-fleet.sh`) scaled RF=2
  strong reads 0.9M (1 workstation client) → 1.35M (3 boxes) → **2.02M** (3 boxes
  + workstation), with node boxes only **~15% CPU** — the 4-shard cluster has huge
  headroom; the wall is client load-gen, not the nodes. Co-locating heavy clients
  on nodes saturates them (client vs. server CPU) with no gain. True node ceiling
  (>2M) needs dedicated load machines or a lower-overhead client (batch/multi-get;
  the profile showed ~14% in syscall entry/exit + `fdget`).
- **All links are 10G** (earlier "1G copper" note was wrong): `eno3` on
  lab-1/2 is 10GBASE-T, lab-3 on 10G DAC. For a cleaner throughput sweep,
  run the client from a box too (or several) and sweep `ncoord`/value-size/RF.
- ⚠️ **PCIe AER storm decision recorded, application pending:** retain Secure
  Boot and NVMe AER visibility; BIOS-disable unused Thunderbolt/USB4 during the
  next attended maintenance. Until all three ledger rows above pass two boots,
  the noise is explicitly accepted and no fix is claimed.
- lab-3: replace the bad DIMM (running on 30 GiB until then); reserve its
  `.200` DHCP lease (or go static) so the IP is stable.
- Re-point the old-box runbooks (`plans/distnode-box-bench.md`,
  `plans/backfill-*-runbook.md`, `bench/*-box-notes.md`) at lab hostnames and
  the **per-box** scratch PCI address (resolve live — it is not stable across
  boxes), then re-capture the corpus / rebaseline t0097.
