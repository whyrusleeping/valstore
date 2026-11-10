#!/usr/bin/env bash
#
# spdk-bind.sh — bind/unbind the instance-store NVMe drives to SPDK's
# userspace driver with SPDK's own setup.sh, driven by out/inventory.json.
#
# Usage (root):
#   sudo -E env VALS_CLOUD_ROOT=/home/ubuntu/vals bash $ROOT/cloud/remote/spdk-bind.sh bind [--hugemem MiB] [--hugenode N[,N]]
#   ... spdk-bind.sh reset
#   ... spdk-bind.sh status
#   --root DIR    override $VALS_CLOUD_ROOT (default $HOME/vals)
#
#   bind    reads instance_store_pcis from $ROOT/out/inventory.json (refuses if
#           empty, if any listed drive is mounted / held / the root device —
#           re-checked live, not just from the inventory — or if it is an EBS
#           controller), picks vfio-pci when IOMMU groups exist or
#           uio_pci_generic (DRIVER_OVERRIDE) otherwise, runs
#           PCI_ALLOWED="<pcis>" HUGEMEM=<MiB, default 65536> spdk/scripts/setup.sh
#           and verifies the bind + HugePages_Total>0.
#           Writes $ROOT/out/bind/bind-<UTC>.json and setup-<UTC>.log.
#   reset   PCI_ALLOWED=... setup.sh reset, then verifies the kernel nvme driver
#           is back (and /dev/nvme* reappear). Writes reset-<UTC>.json.
#   status  prints the current driver per instance-store PCI and hugepages.
#
# The EBS / root controller is never passed to setup.sh (PCI_ALLOWED is an
# explicit allow-list). setup.sh itself also skips devices with active
# mountpoints/holders, but we refuse earlier and louder.
#
# Prints one machine-readable line at the end:  RESULT {...json...}
set -euo pipefail

log()  { printf '[spdk-bind] %s\n' "$*"; }
warn() { printf '[spdk-bind] WARN: %s\n' "$*" >&2; }
die()  { [ -z "${OUT:-}" ] || fix_owner "$OUT"; printf '[spdk-bind] ERROR: %s\n' "$*" >&2; echo "RESULT {\"ok\":false,\"error\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$*")}"; exit 1; }

default_root() {
  if [ -n "${VALS_CLOUD_ROOT:-}" ]; then echo "$VALS_CLOUD_ROOT"; return; fi
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
    local h; h="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    [ -n "$h" ] && { echo "$h/vals"; return; }
  fi
  echo "$HOME/vals"
}
fix_owner() {
  [ "$(id -u)" -eq 0 ] || return 0
  [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ] || return 0
  local p
  for p in "$@"; do [ -e "$p" ] && chown -R "$SUDO_USER:" "$p" 2>/dev/null || true; done
}
# json_obj key=string key:=rawjson ...  -> compact JSON object on stdout
json_obj() {
  python3 - "$@" <<'PY'
import json, sys
o = {}
for a in sys.argv[1:]:
    k, _, v = a.partition('=')
    if k.endswith(':'):
        k = k[:-1]
        try:
            v = json.loads(v)
        except ValueError:
            pass
    o[k] = v
print(json.dumps(o, separators=(',', ':')))
PY
}

MODE=""
HUGEMEM=65536
HUGENODE=""
ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    bind|reset|status) MODE="$1"; shift;;
    --hugemem) HUGEMEM="$2"; shift 2;;
    --hugenode) HUGENODE="$2"; shift 2;;
    --root) ROOT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "unknown argument: $1";;
  esac
done
[ -n "$MODE" ] || die "usage: spdk-bind.sh bind|reset|status [--hugemem MiB] [--hugenode N,N] [--root DIR]"
[ -n "$ROOT" ] || ROOT="$(default_root)"
OUT="$ROOT/out"
INVENTORY="$OUT/inventory.json"
SETUP="$ROOT/spdk/scripts/setup.sh"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUT/bind"

[ -s "$INVENTORY" ] || die "no inventory at $INVENTORY (run hostprep.sh first)"
python3 -c 'import json,sys; json.load(open(sys.argv[1]))' "$INVENTORY" 2>/dev/null || die "inventory $INVENTORY is not valid JSON"

# ---- helpers over sysfs ---------------------------------------------------------
pci_driver() { # -> driver name or "none"
  local d="/sys/bus/pci/devices/$1/driver"
  [ -e "$d" ] && basename "$(readlink -f "$d")" || echo none
}
hugepages_total() { awk '/^HugePages_Total/ {print $2}' /proc/meminfo; }
hugepages_free()  { awk '/^HugePages_Free/ {print $2}' /proc/meminfo; }
iommu_groups()    { find /sys/kernel/iommu_groups -mindepth 1 -maxdepth 1 2>/dev/null | wc -l; }

# Instance-store PCIs from the inventory, one per line; also the EBS/root PCIs
# so we can double check nothing overlaps.
inv_pcis() {
  python3 - "$INVENTORY" "$1" <<'PY'
import json, sys
inv = json.load(open(sys.argv[1]))
what = sys.argv[2]
if what == 'instance_store':
    print('\n'.join(inv.get('instance_store_pcis', [])))
else:
    print('\n'.join(sorted(e['pci'] for e in inv.get('nvme', [])
                           if e.get('pci') and (e.get('role') == 'ebs' or e.get('is_root') or e.get('mounted')))))
PY
}

# Live check: is any block device behind this PCI mounted, swapped, held by
# md/dm, or backing the root filesystem?  Prints reasons (empty = free).
live_in_use() {
  python3 - "$1" <<'PY'
import glob, os, stat, sys
bdf = sys.argv[1]

def read(p):
    try:
        return open(p).read().strip()
    except OSError:
        return None

def disks_of_devno(devno, seen=None):
    seen = seen if seen is not None else set()
    if devno in seen:
        return set()
    seen.add(devno)
    p = os.path.realpath('/sys/dev/block/' + devno)
    if not os.path.isdir(p):
        return set()
    if os.path.exists(p + '/partition'):
        p = os.path.dirname(p)
    out = set()
    try:
        slaves = os.listdir(p + '/slaves')
    except OSError:
        slaves = []
    if slaves:
        for s in slaves:
            d = read('%s/slaves/%s/dev' % (p, s))
            if d:
                out |= disks_of_devno(d, seen)
    else:
        out.add(os.path.basename(p))
    return out

def disks_of_path(path):
    try:
        st = os.stat(path)
    except OSError:
        return set()
    if not stat.S_ISBLK(st.st_mode):
        return set()
    return disks_of_devno('%d:%d' % (os.major(st.st_rdev), os.minor(st.st_rdev)))

used = {}
for line in (read('/proc/mounts') or '').splitlines():
    f = line.split()
    if f and f[0].startswith('/dev/'):
        for d in disks_of_path(f[0]):
            used.setdefault(d, []).append('mounted at ' + f[1])
for line in (read('/proc/swaps') or '').splitlines()[1:]:
    f = line.split()
    if f and f[0].startswith('/dev/'):
        for d in disks_of_path(f[0]):
            used.setdefault(d, []).append('swap')
try:
    st = os.stat('/')
    for d in disks_of_devno('%d:%d' % (os.major(st.st_dev), os.minor(st.st_dev))):
        used.setdefault(d, []).append('root filesystem')
except OSError:
    pass

reasons = []
for ctrl in glob.glob('/sys/bus/pci/devices/%s/nvme/nvme*' % bdf):
    blocks = [os.path.basename(b) for b in glob.glob(ctrl + '/nvme*n*')]
    for sub in glob.glob('/sys/class/nvme-subsystem/nvme-subsys*'):
        if os.path.exists(sub + '/' + os.path.basename(ctrl)):
            blocks += [os.path.basename(b) for b in glob.glob(sub + '/nvme*n*')]
    for b in blocks:
        for r in used.get(b, []):
            reasons.append('%s %s' % (b, r))
        for h in glob.glob('/sys/class/block/%s/holders/*' % b) + glob.glob('/sys/class/block/%s/%s*/holders/*' % (b, b)):
            reasons.append('%s held by %s' % (b, os.path.basename(h)))
print('; '.join(sorted(set(reasons))))
PY
}

mapfile -t PCIS < <(inv_pcis instance_store | sed '/^$/d')
mapfile -t DENIED < <(inv_pcis denied | sed '/^$/d')

print_status() {
  local p
  printf '%-14s %-16s %s\n' PCI DRIVER NOTE
  for p in "${PCIS[@]}"; do
    printf '%-14s %-16s %s\n' "$p" "$(pci_driver "$p")" "$(live_in_use "$p")"
  done
  printf 'hugepages: total=%s free=%s (%s)\n' "$(hugepages_total)" "$(hugepages_free)" "$(awk '/^Hugepagesize/ {print $2 $3}' /proc/meminfo)"
  printf 'iommu groups: %s\n' "$(iommu_groups)"
}

drivers_json() {
  local p; local -a kv=()
  for p in "${PCIS[@]}"; do kv+=("$p=$(pci_driver "$p")"); done
  json_obj "${kv[@]}"
}

case "$MODE" in
  status)
    [ ${#PCIS[@]} -gt 0 ] || warn "inventory lists no instance-store drives"
    print_status
    echo "RESULT $(json_obj ok:=true mode=status drivers:="$(drivers_json)" hugepages_total:="$(hugepages_total)" hugepages_free:="$(hugepages_free)" iommu_groups:="$(iommu_groups)")"
    exit 0;;
esac

[ "$(id -u)" -eq 0 ] || die "$MODE needs root"
[ ${#PCIS[@]} -gt 0 ] || die "inventory has no instance_store_pcis; refusing to touch any drive"
[ -f "$SETUP" ] || die "SPDK setup.sh not found at $SETUP"
[ -f "$ROOT/spdk/scripts/common.sh" ] || die "setup.sh needs $ROOT/spdk/scripts/common.sh alongside it"
[ -f "$ROOT/spdk/include/spdk/pci_ids.h" ] || die "setup.sh needs $ROOT/spdk/include/spdk/pci_ids.h"

# Sanity: the allow-list must never contain a denied (EBS/root/mounted) PCI.
for p in "${PCIS[@]}"; do
  [ -e "/sys/bus/pci/devices/$p" ] || die "$p from inventory does not exist on this host"
  for d in "${DENIED[@]}"; do [ "$p" = "$d" ] && die "$p is EBS/root/mounted per inventory; refusing"; done
done
ALLOWED="${PCIS[*]}"
LOG="$OUT/bind/$MODE-$TS.log"

if [ "$MODE" = bind ]; then
  for p in "${PCIS[@]}"; do
    reason="$(live_in_use "$p")"
    [ -z "$reason" ] || die "$p is in use ($reason); refusing to bind"
  done
  groups="$(iommu_groups)"
  driver_path="vfio-pci"; why=""; override=""
  if [ "$groups" -gt 0 ]; then
    modprobe vfio-pci 2>/dev/null || die "modprobe vfio-pci failed"
    modprobe vfio_iommu_type1 2>/dev/null || true
    why="IOMMU groups present ($groups): vfio-pci"
  else
    # No IOMMU (intel_iommu=on absent or not supported by the platform):
    # vfio-pci cannot be used without an IOMMU (noiommu mode is "unsafe" and
    # needs a module parameter), so use uio_pci_generic. DPDK then runs in
    # IOVA=PA mode, which is why valsd must run as root. setup.sh would pick
    # the same driver itself; we make it explicit via DRIVER_OVERRIDE.
    modprobe uio_pci_generic 2>/dev/null || die "no IOMMU groups and modprobe uio_pci_generic failed (install linux-modules-extra-$(uname -r))"
    driver_path="uio_pci_generic"; override="uio_pci_generic"
    why="no IOMMU groups: uio_pci_generic via DRIVER_OVERRIDE (IOVA=PA; run SPDK apps as root)"
  fi
  log "$why"
  log "binding ${#PCIS[@]} drives: $ALLOWED  (HUGEMEM=${HUGEMEM} MiB${HUGENODE:+ HUGENODE=$HUGENODE})"
  before_total="$(hugepages_total)"
  rc=0
  envs=(PCI_ALLOWED="$ALLOWED" HUGEMEM="$HUGEMEM")
  [ -z "$HUGENODE" ] || envs+=(HUGENODE="$HUGENODE")
  [ -z "$override" ] || envs+=(DRIVER_OVERRIDE="$override")
  env "${envs[@]}" bash "$SETUP" config > "$LOG" 2>&1 || rc=$?
  sed 's/^/[setup.sh] /' "$LOG"
  [ $rc -eq 0 ] || warn "setup.sh exited $rc (see $LOG)"
  bad=()
  for p in "${PCIS[@]}"; do
    d="$(pci_driver "$p")"
    [ "$d" = "$driver_path" ] || bad+=("$p=$d")
  done
  if [ ${#bad[@]} -gt 0 ] && [ "$driver_path" = uio_pci_generic ]; then
    # Some virtual NVMe functions refuse uio_pci_generic (no INTx). DPDK also
    # supports vfio-pci in no-IOMMU mode (IOVA=PA, root only, no DMA isolation):
    # acceptable on a disposable benchmark host whose only users are vals.
    warn "uio_pci_generic did not bind (${bad[*]}); retrying with vfio-pci in no-IOMMU mode"
    modprobe vfio enable_unsafe_noiommu_mode=1 2>/dev/null || true
    [ -w /sys/module/vfio/parameters/enable_unsafe_noiommu_mode ] && echo Y > /sys/module/vfio/parameters/enable_unsafe_noiommu_mode
    modprobe vfio-pci 2>/dev/null || die "modprobe vfio-pci failed"
    env PCI_ALLOWED="$ALLOWED" bash "$SETUP" reset >> "$LOG" 2>&1 || true
    driver_path="vfio-pci"; override="vfio-pci"
    why="$why; uio_pci_generic failed for ${bad[*]}, fell back to vfio-pci no-IOMMU mode"
    rc=0
    env PCI_ALLOWED="$ALLOWED" HUGEMEM="$HUGEMEM" DRIVER_OVERRIDE=vfio-pci bash "$SETUP" config >> "$LOG" 2>&1 || rc=$?
    sed 's/^/[setup.sh] /' "$LOG" | tail -20
    bad=()
    for p in "${PCIS[@]}"; do
      d="$(pci_driver "$p")"
      [ "$d" = "$driver_path" ] || bad+=("$p=$d")
    done
  fi
  total="$(hugepages_total)"
  ok=true
  [ ${#bad[@]} -eq 0 ] || { ok=false; warn "not bound to $driver_path: ${bad[*]}"; }
  [ "${total:-0}" -gt 0 ] || { ok=false; warn "HugePages_Total is 0 after setup.sh"; }
  out_json="$OUT/bind/bind-$TS.json"
  json_obj ok:="$ok" mode=bind time="$TS" driver="$driver_path" driver_reason="$why" \
    pcis:="$(printf '%s\n' "${PCIS[@]}" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read().split()))')" \
    drivers:="$(drivers_json)" hugemem_mib:="$HUGEMEM" hugenode="$HUGENODE" \
    hugepages_total_before:="${before_total:-0}" hugepages_total:="${total:-0}" hugepages_free:="$(hugepages_free)" \
    hugepage_size_kb:="$(awk '/^Hugepagesize/ {print $2}' /proc/meminfo)" iommu_groups:="$groups" \
    setup_rc:="$rc" setup_log="$LOG" | python3 -m json.tool > "$out_json"
  fix_owner "$OUT/bind"
  print_status
  echo "RESULT $(json_obj ok:="$ok" mode=bind driver="$driver_path" drivers:="$(drivers_json)" hugepages_total:="${total:-0}" hugepages_free:="$(hugepages_free)" setup_rc:="$rc" json="$out_json" log="$LOG")"
  [ "$ok" = true ] || exit 1
  exit 0
fi

# ---- reset -------------------------------------------------------------------------
log "resetting ${#PCIS[@]} drives to the kernel nvme driver: $ALLOWED"
rc=0
env PCI_ALLOWED="$ALLOWED" bash "$SETUP" reset > "$LOG" 2>&1 || rc=$?
sed 's/^/[setup.sh] /' "$LOG"
[ $rc -eq 0 ] || warn "setup.sh reset exited $rc (see $LOG)"
# The kernel re-probes asynchronously; give /dev/nvme* a moment to come back.
for _ in 1 2 3 4 5 6 7 8 9 10; do
  bad=()
  for p in "${PCIS[@]}"; do
    if [ "$(pci_driver "$p")" != nvme ] || ! compgen -G "/sys/bus/pci/devices/$p/nvme/nvme*/nvme*n*" >/dev/null; then
      bad+=("$p=$(pci_driver "$p")")
    fi
  done
  [ ${#bad[@]} -eq 0 ] && break
  sleep 1
done
ok=true
[ ${#bad[@]} -eq 0 ] || { ok=false; warn "not back on the nvme driver with namespaces: ${bad[*]}"; }
out_json="$OUT/bind/reset-$TS.json"
json_obj ok:="$ok" mode=reset time="$TS" drivers:="$(drivers_json)" hugepages_total:="$(hugepages_total)" setup_rc:="$rc" setup_log="$LOG" \
  | python3 -m json.tool > "$out_json"
fix_owner "$OUT/bind"
print_status
echo "RESULT $(json_obj ok:="$ok" mode=reset drivers:="$(drivers_json)" hugepages_total:="$(hugepages_total)" setup_rc:="$rc" json="$out_json" log="$LOG")"
[ "$ok" = true ] || exit 1
