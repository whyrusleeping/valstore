#!/usr/bin/env bash
#
# provision-node.sh — repeatable box-validation node provisioning for the vals
# rig (the lab-N fleet that replaces the retired oldbox).
#
# Target: a fresh Ubuntu 24.04/26.04 box with a dedicated scratch NVMe, used as
# an SPDK box-validation node (see docs/SPDK.md, docs/deploy.md).
#
# Design goals:
#   - IDEMPOTENT: safe to re-run; each step checks before it acts.
#   - NON-DESTRUCTIVE: it never touches the scratch drive's data.
#     Create layouts only with the offline, serial-checked `valsd provision`
#     workflow in docs/deploy.md after an explicit data-retention decision.
#   - STAGED: run all stages, or one at a time (`STAGE=... ` / `--only <stage>`).
#
# Usage (run ON the node, or `ssh <host> 'bash -s' < scripts/provision-node.sh`):
#   sudo ./scripts/provision-node.sh                 # all stages
#   sudo ./scripts/provision-node.sh --only net-fix  # a single stage
#
# Most stages need root (apt, sysctl, systemd, cpupower). The `rust` stage runs
# as the invoking (non-root) user and installs rustup into ~/.cargo — run it
# without sudo, or the script re-drops privileges via $SUDO_USER.
#
# Stages: needrestart  packages  rust  net-fix  governor  spdk-prep  verify
set -euo pipefail

# ---- pinned versions / knobs ------------------------------------------------
RUST_TOOLCHAIN="1.96.0"          # keep in sync with rust-toolchain.toml
WAIT_ONLINE_TIMEOUT="20"         # seconds; --any so any one online NIC suffices
SPDK_TAG="v24.09"                # pinned known-good (docs/SPDK.md)
PD_RAFT_PORT="7100"              # one voting PD per host listens here (scripts/pd_topology.py)

log()  { printf '\033[1;32m[provision]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[provision]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[provision] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

need_root() { [ "$(id -u)" -eq 0 ] || die "stage '$1' needs root (re-run with sudo)"; }

# The unprivileged user we install rustup for (when invoked via sudo).
TARGET_USER="${SUDO_USER:-$(id -un)}"
TARGET_HOME="$(getent passwd "$TARGET_USER" | cut -d: -f6)"

# ---- stages -----------------------------------------------------------------

stage_packages() {
  need_root packages
  log "installing build + SPDK + ops packages"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev clang git curl ca-certificates \
    autoconf automake libtool libtool-bin \
    nvme-cli numactl libnuma-dev libaio-dev uuid-dev fio jq \
    linux-tools-common "linux-tools-$(uname -r)" \
    meson ninja-build nasm python3 python3-pip \
    || die "apt install failed"
  # SPDK genrpc / DPDK build helpers (docs/SPDK.md; not covered by pkgdep.sh).
  python3 -m pip install --user --break-system-packages -q \
    pyelftools jinja2 tabulate 2>/dev/null \
    || warn "pip deps (pyelftools/jinja2/tabulate) not installed — only needed to BUILD SPDK"
  log "packages done"
}

stage_needrestart() {
  need_root needrestart
  install -d /etc/needrestart/conf.d
  cat > /etc/needrestart/conf.d/90-vals.conf <<'EOF'
$nrconf{override_rc}{qr(^vals)} = 0;
EOF
  log "needrestart excludes vals units; unattended-upgrades remains enabled"
}

stage_rust() {
  # Install rustup + the pinned toolchain for the unprivileged user. rustup
  # lives in ~/.cargo (no root needed); when we're invoked via sudo we run the
  # install *as* $SUDO_USER rather than re-executing the whole script (which is
  # fragile when the script arrives on stdin, i.e. `ssh host sudo bash -s`).
  local runner=()
  if [ "$(id -u)" -eq 0 ] && [ "$TARGET_USER" != "root" ]; then
    log "rust: installing for $TARGET_USER"
    runner=(sudo -u "$TARGET_USER" env "HOME=$TARGET_HOME")
  fi
  "${runner[@]}" bash -euo pipefail -c '
    RUST_TOOLCHAIN="$1"
    cargo_bin="$HOME/.cargo/bin"
    export PATH="$cargo_bin:$PATH"
    if ! command -v rustup >/dev/null 2>&1; then
      echo "[provision] installing rustup (no default toolchain)"
      curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs \
        | sh -s -- -y --no-modify-path --default-toolchain none
    fi
    echo "[provision] installing pinned toolchain $RUST_TOOLCHAIN (rustfmt, clippy)"
    "$cargo_bin/rustup" toolchain install "$RUST_TOOLCHAIN" \
      --component rustfmt --component clippy --profile minimal
    "$cargo_bin/rustup" default "$RUST_TOOLCHAIN"
    "$cargo_bin/rustc" --version
    # Make ~/.cargo/bin available in non-interactive ssh (memory.md lesson).
    prof="$HOME/.profile"
    if ! grep -q ".cargo/bin" "$prof" 2>/dev/null; then
      echo "export PATH=\"\$HOME/.cargo/bin:\$PATH\"" >> "$prof"
      echo "[provision] added ~/.cargo/bin to PATH in $prof"
    fi
  ' _ "$RUST_TOOLCHAIN"
  log "rust done"
}

stage_net_fix() {
  # Fix the 2-minute boot stall: systemd-networkd-wait-online waits for ALL
  # managed NICs (this box has 4 wired + wifi; only one is cabled), so it burns
  # the full timeout every boot. Make it succeed when ANY interface is online.
  need_root net-fix
  local dir=/etc/systemd/system/systemd-networkd-wait-online.service.d
  install -d "$dir"
  # NOTE: drop-ins apply in lexicographic filename order across ALL dirs, and
  # netplan's generator writes /run/.../10-netplan.conf (which it regenerates
  # every boot). Ours must sort AFTER it so our `ExecStart=` reset wins — hence
  # 99-. A lower number (10-) is silently overridden by netplan's ExecStart.
  rm -f "$dir/10-any.conf"   # clean up an earlier (losing) name if present
  cat > "$dir/99-any.conf" <<EOF
# Managed by scripts/provision-node.sh — see plans/box-provisioning-runbook.md.
# Default waits for every managed NIC to be online; on a multi-NIC box with one
# cable that never happens and boot stalls for the full timeout. --any = one is
# enough; the short timeout bounds the worst case if the cable is out at boot.
[Service]
ExecStart=
ExecStart=/lib/systemd/systemd-networkd-wait-online --any --timeout=${WAIT_ONLINE_TIMEOUT}
EOF
  systemctl daemon-reload
  log "net-fix installed (wait-online --any --timeout=${WAIT_ONLINE_TIMEOUT}); effective next boot"
}

stage_governor() {
  # Stable benchmark clocks: performance governor, persisted across reboots.
  need_root governor
  command -v cpupower >/dev/null 2>&1 || { warn "cpupower missing (run 'packages' first)"; return 0; }
  cpupower frequency-set -g performance >/dev/null 2>&1 || warn "could not set governor now"
  cat > /etc/systemd/system/cpu-performance.service <<'EOF'
[Unit]
Description=Set CPU governor to performance (vals bench rig)
After=multi-user.target
[Service]
Type=oneshot
ExecStart=/usr/bin/cpupower frequency-set -g performance
[Install]
WantedBy=multi-user.target
EOF
  systemctl daemon-reload
  systemctl enable cpu-performance.service >/dev/null 2>&1 || true
  log "governor set to performance (persisted via cpu-performance.service)"
}

stage_spdk_prep() {
  # SPDK needs vfio + hugepages, reserved per-boot by third_party/spdk/scripts/
  # setup.sh with HUGEMEM/PCI_ALLOWED (docs/SPDK.md). IOMMU is already on here
  # (iommu_groups present). We only ensure the vfio-pci module autoloads; the
  # cluster start (as root) does the actual bind + hugepage reservation.
  need_root spdk-prep
  if [ "$(ls /sys/kernel/iommu_groups 2>/dev/null | wc -l)" -eq 0 ]; then
    warn "no IOMMU groups — SPDK vfio will fail; enable VT-d/IOMMU in firmware"
  else
    log "IOMMU active ($(ls /sys/kernel/iommu_groups | wc -l) groups)"
  fi
  echo vfio-pci > /etc/modules-load.d/vfio-pci.conf
  modprobe vfio-pci 2>/dev/null || warn "vfio-pci not loadable yet (loads on next boot / via setup.sh)"
  # iommu=pt: host NIC/driver DMA is identity-mapped (no per-packet IOMMU
  # map/clflush — ~9% of a serving core reclaimed on the NIC-DMA-heavy read path;
  # measured in a lab read-profiling run), while vfio can still assign the scratch
  # NVMe. Idempotent GRUB edit; effective next boot.
  if ! grep -q 'iommu=pt' /proc/cmdline; then
    if ! grep -q 'iommu=pt' /etc/default/grub; then
      cp -n /etc/default/grub /etc/default/grub.bak 2>/dev/null || true
      sed -i 's|^GRUB_CMDLINE_LINUX_DEFAULT="\(.*\)"|GRUB_CMDLINE_LINUX_DEFAULT="\1 iommu=pt intel_iommu=on"|' /etc/default/grub
      sed -i 's|"  *iommu=pt|" iommu=pt|' /etc/default/grub  # tidy leading space when empty
      update-grub 2>/dev/null || warn "update-grub failed"
    fi
    warn "iommu=pt set in GRUB — reboot to activate (reclaims NIC IOMMU DMA cost)"
  else
    log "iommu=pt active"
  fi
  log "spdk-prep done (hugepages reserved per-run by third_party/spdk/scripts/setup.sh HUGEMEM=...)"
}

stage_verify() {
  log "=== verification ==="
  echo "-- os:        $(. /etc/os-release; echo "$PRETTY_NAME") / $(uname -r)"
  echo "-- cpu:       $(nproc) cores, governor=$(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null)"
  echo "-- rust:      $("$TARGET_HOME/.cargo/bin/rustc" "+$RUST_TOOLCHAIN" --version 2>/dev/null || echo MISSING)"
  echo "-- nvme:      $(command -v nvme >/dev/null && echo present || echo MISSING)"
  echo "-- fio:       $(command -v fio >/dev/null && echo present || echo MISSING)"
  echo "-- iommu:     $(ls /sys/kernel/iommu_groups 2>/dev/null | wc -l) groups"
  echo "-- wait-online override: $([ -f /etc/systemd/system/systemd-networkd-wait-online.service.d/99-any.conf ] && echo present || echo MISSING)"
  echo "-- clock:     NTPSynchronized=$(timedatectl show -p NTPSynchronized --value 2>/dev/null || echo unknown)"
  echo "-- pd raft:   port ${PD_RAFT_PORT} $(ss -Hltn "sport = :${PD_RAFT_PORT}" 2>/dev/null | grep -q . && echo 'in use' || echo free) (three-PD reference: docs/deploy.md#three-pd-reference-deployment)"
  echo "-- firewall:  $(command -v ufw >/dev/null && ufw status 2>/dev/null | head -1 || echo 'ufw absent') (PD hosts must reach each other's raft port)"
  echo "-- scratch NVMe (untouched):"; lsblk -d -o NAME,SIZE,MODEL 2>/dev/null | grep nvme || true
}

# ---- driver -----------------------------------------------------------------
ALL_STAGES=(needrestart packages rust net-fix governor spdk-prep verify)

main() {
  local only=""
  while [ $# -gt 0 ]; do
    case "$1" in
      --only) only="$2"; shift 2;;
      --blkdiscard) die "--blkdiscard retired: use the identity-checked offline provisioning workflow in docs/deploy.md; no data was changed";;
      -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
      *) die "unknown arg: $1";;
    esac
  done
  if [ -n "$only" ]; then
    "stage_${only//-/_}"
  else
    for s in "${ALL_STAGES[@]}"; do "stage_${s//-/_}"; done
    log "ALL STAGES COMPLETE. Reboot to pick up the boot-time fixes (net-fix, governor)."
  fi
}
main "$@"
