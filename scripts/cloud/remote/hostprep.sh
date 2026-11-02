#!/usr/bin/env bash
#
# hostprep.sh — prepare a cloud stress-test host (EC2 i4i.metal node or
# c6in.16xlarge client) and write its hardware/software inventory.
#
# Usage (on the host, as root, invoked by the orchestrator over ssh):
#   sudo -E env VALS_CLOUD_ROOT=/home/ubuntu/vals bash $ROOT/cloud/remote/hostprep.sh
#   hostprep.sh [--root DIR] [--inventory-only] [--classify-model "<model>"]
#
#   --root DIR            override $VALS_CLOUD_ROOT (default $HOME/vals, or the
#                         invoking sudo user's $HOME/vals when run via sudo)
#   --inventory-only      skip cloud-init wait, packages, sysctls, governor and
#                         FlameGraph; only build the inventory from whatever is
#                         readable (works unprivileged, used by the unit test)
#   --classify-model STR  print the drive role for an NVMe model string
#                         (instance_store | ebs | other) and exit
#
# Full run (root):
#   1. wait for cloud-init (bounded), install missing packages (noninteractive)
#   2. sysctls for core dumps + perf (previous values recorded first); stock
#      network/memory tuning is left alone and only recorded (sysctl -a)
#   3. cpufreq governor=performance (recorded: driver, turbo, C-states)
#   4. clone FlameGraph into $ROOT/FlameGraph (optional)
#   5. inventory -> $ROOT/out/inventory.json + raw text under $ROOT/out/inventory/
#
# Idempotent: re-running merges with the previous inventory so that drives
# which are now bound to vfio-pci/uio (SPDK owns them, no /sys/class/nvme entry)
# keep their recorded model/serial — `valsd provision --device PCI=SERIAL`
# needs those serials later.
#
# Prints one machine-readable line at the end:  RESULT {...json...}
set -euo pipefail

log()  { printf '[hostprep] %s\n' "$*"; }
warn() { printf '[hostprep] WARN: %s\n' "$*" >&2; WARNINGS=$((WARNINGS + 1)); }
die()  { printf '[hostprep] ERROR: %s\n' "$*" >&2; exit 1; }
WARNINGS=0

# ---- root dir / ownership -----------------------------------------------------
default_root() {
  if [ -n "${VALS_CLOUD_ROOT:-}" ]; then echo "$VALS_CLOUD_ROOT"; return; fi
  if [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ]; then
    local h; h="$(getent passwd "$SUDO_USER" | cut -d: -f6)"
    [ -n "$h" ] && { echo "$h/vals"; return; }
  fi
  echo "$HOME/vals"
}

# chown artifacts back to the invoking user so rsync as that user can read them.
fix_owner() {
  [ "$(id -u)" -eq 0 ] || return 0
  [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != root ] || return 0
  local p
  for p in "$@"; do
    [ -e "$p" ] && chown -R "$SUDO_USER:" "$p" 2>/dev/null || true
  done
}

# ---- argument parsing ---------------------------------------------------------
INVENTORY_ONLY=0
CLASSIFY=""; CLASSIFY_SET=0
ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --root) ROOT="$2"; shift 2;;
    --inventory-only) INVENTORY_ONLY=1; shift;;
    --classify-model) CLASSIFY="$2"; CLASSIFY_SET=1; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "unknown argument: $1";;
  esac
done
[ -n "$ROOT" ] || ROOT="$(default_root)"
OUT="$ROOT/out"
INV="$OUT/inventory"

# ---- the inventory builder (python; also hosts the model classifier) ---------
inventory_py() {
  python3 - "$@" <<'PY'
import glob
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import time
import urllib.request

BDF_RE = re.compile(r'^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$')
NS_RE = re.compile(r'^nvme\d+n\d+$')


def classify(model):
    """Drive role from the NVMe model string (what EC2 reports)."""
    m = (model or '').lower()
    if 'instance storage' in m:
        return 'instance_store'
    if 'elastic block store' in m:
        return 'ebs'
    return 'other'


mode = sys.argv[1]
if mode == 'classify':
    print(classify(sys.argv[2]))
    sys.exit(0)

root = sys.argv[2]
out_dir = os.path.join(root, 'out')
inv_dir = os.path.join(out_dir, 'inventory')
os.makedirs(inv_dir, exist_ok=True)
warnings = []


def warn(msg):
    warnings.append(msg)
    print('[hostprep] WARN: ' + msg, file=sys.stderr)


def read(path, default=None):
    try:
        with open(path, errors='replace') as f:
            return f.read().strip()
    except OSError:
        return default


def read_int(path, default=None):
    v = read(path)
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def run(cmd, timeout=60):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout, p.stderr
    except (OSError, subprocess.TimeoutExpired) as e:
        return 127, '', str(e)


def have(cmd):
    return shutil.which(cmd) is not None


def save(rel, text):
    path = os.path.join(inv_dir, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        f.write(text if text.endswith('\n') or not text else text + '\n')


def save_cmd(rel, cmd, timeout=60):
    """Run cmd, save stdout to inv_dir/rel; stderr+rc to rel.err on failure."""
    if not have(cmd[0]):
        warn('%s not installed; skipping %s' % (cmd[0], rel))
        return None
    rc, out, err = run(cmd, timeout)
    if rc != 0:
        save(rel + '.err', 'rc=%d\n%s' % (rc, err))
        if not out:
            return None
    save(rel, out)
    return out


# ---- IMDSv2 ----------------------------------------------------------------
def imds():
    if os.environ.get('VALS_CLOUD_NO_IMDS'):
        return {'available': False, 'reason': 'VALS_CLOUD_NO_IMDS set'}
    base = 'http://169.254.169.254/latest/'
    try:
        req = urllib.request.Request(base + 'api/token', method='PUT',
                                     headers={'X-aws-ec2-metadata-token-ttl-seconds': '120'})
        tok = urllib.request.urlopen(req, timeout=2).read().decode()
    except Exception as e:  # noqa: BLE001 - any failure means "not EC2"
        return {'available': False, 'reason': str(e)[:120]}
    o = {'available': True}
    fields = [('instance_type', 'meta-data/instance-type'),
              ('instance_id', 'meta-data/instance-id'),
              ('availability_zone', 'meta-data/placement/availability-zone'),
              ('placement_group', 'meta-data/placement/group-name'),
              ('ami_id', 'meta-data/ami-id'),
              ('local_ipv4', 'meta-data/local-ipv4'),
              ('public_ipv4', 'meta-data/public-ipv4'),
              ('mac', 'meta-data/mac')]
    for key, path in fields:
        try:
            req = urllib.request.Request(base + path, headers={'X-aws-ec2-metadata-token': tok})
            o[key] = urllib.request.urlopen(req, timeout=2).read().decode()
        except Exception:  # noqa: BLE001 - e.g. 404 when no placement group / public ip
            o[key] = None
    return o


# ---- CPU / memory / IOMMU --------------------------------------------------
def cpu_info():
    rc, out, _ = run(['lscpu'])
    kv = {}
    for line in out.splitlines():
        k, _, v = line.partition(':')
        kv[k.strip()] = v.strip()

    def num(key):
        try:
            return int(kv.get(key, ''))
        except ValueError:
            return None

    nodes = []
    for p in sorted(glob.glob('/sys/devices/system/node/node[0-9]*'),
                    key=lambda s: int(s.rsplit('node', 1)[1])):
        nodes.append({'node': int(p.rsplit('node', 1)[1]), 'cpus': read(p + '/cpulist', '')})
    return {
        'model': kv.get('Model name'),
        'vendor': kv.get('Vendor ID'),
        'sockets': num('Socket(s)'),
        'cores_per_socket': num('Core(s) per socket'),
        'threads_per_core': num('Thread(s) per core'),
        'threads': num('CPU(s)') or os.cpu_count(),
        'online': read('/sys/devices/system/cpu/online'),
        'numa_nodes': nodes,
        'flags_subset': sorted(f for f in kv.get('Flags', '').split()
                               if f in ('sse4_2', 'avx2', 'avx512f', 'avx512bw', 'pclmulqdq', 'rdtscp', 'constant_tsc', 'nonstop_tsc')),
    }


def meminfo():
    kv = {}
    for line in (read('/proc/meminfo', '') or '').splitlines():
        k, _, v = line.partition(':')
        kv[k.strip()] = v.strip()

    def kb(key):
        try:
            return int(kv[key].split()[0]) * 1024
        except (KeyError, ValueError, IndexError):
            return None
    huge = {'total': None, 'free': None, 'size_bytes': kb('Hugepagesize'), 'per_node': {}}
    try:
        huge['total'] = int(kv['HugePages_Total'])
        huge['free'] = int(kv['HugePages_Free'])
    except (KeyError, ValueError):
        pass
    for p in glob.glob('/sys/devices/system/node/node*/hugepages/hugepages-*kB/nr_hugepages'):
        node = p.split('/node/node')[1].split('/')[0]
        size = p.split('hugepages-')[1].split('kB')[0]
        huge['per_node']['node%s/%skB' % (node, size)] = read_int(p)
    return kb('MemTotal'), huge


def iommu_info():
    try:
        groups = len(os.listdir('/sys/kernel/iommu_groups'))
    except OSError:
        groups = 0
    cmdline = read('/proc/cmdline', '') or ''
    flags = [t for t in cmdline.split() if 'iommu' in t]
    return {'groups': groups, 'cmdline_flags': flags,
            'vfio_noiommu': read('/sys/module/vfio/parameters/enable_unsafe_noiommu_mode')}


def cpufreq_info():
    base = '/sys/devices/system/cpu/cpu0/cpufreq'
    o = {'present': os.path.isdir(base)}
    if o['present']:
        o['driver'] = read(base + '/scaling_driver')
        o['governor'] = read(base + '/scaling_governor')
        o['max_khz'] = read_int(base + '/scaling_max_freq')
    o['intel_pstate_no_turbo'] = read('/sys/devices/system/cpu/intel_pstate/no_turbo')
    o['cpufreq_boost'] = read('/sys/devices/system/cpu/cpufreq/boost')
    states = []
    for p in sorted(glob.glob('/sys/devices/system/cpu/cpu0/cpuidle/state*')):
        states.append({'name': read(p + '/name'), 'disable': read(p + '/disable'),
                       'latency_us': read_int(p + '/latency')})
    o['cstates'] = states
    o['cpuidle_driver'] = read('/sys/devices/system/cpu/cpuidle/current_driver')
    return o


# ---- block device usage (mounted / root / held) ----------------------------
def disks_of_devno(devno, seen=None):
    """Base disk names behind a MAJ:MIN (walks partitions and dm/md slaves)."""
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
    slaves = []
    try:
        slaves = os.listdir(p + '/slaves')
    except OSError:
        pass
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


def in_use_disks():
    used, root_disks = set(), set()
    for line in (read('/proc/mounts', '') or '').splitlines():
        src = line.split()[0] if line.split() else ''
        if src.startswith('/dev/'):
            used |= disks_of_path(src)
    for line in (read('/proc/swaps', '') or '').splitlines()[1:]:
        src = line.split()[0] if line.split() else ''
        if src.startswith('/dev/'):
            used |= disks_of_path(src)
    try:
        st = os.stat('/')
        root_disks = disks_of_devno('%d:%d' % (os.major(st.st_dev), os.minor(st.st_dev)))
    except OSError:
        pass
    # findmnt as a second opinion (handles /dev/root style sources).
    if have('findmnt'):
        rc, out, _ = run(['findmnt', '-n', '-o', 'SOURCE', '/'])
        src = out.strip().split('[')[0]
        if rc == 0 and src.startswith('/dev/'):
            root_disks |= disks_of_path(src)
    return used | root_disks, root_disks


def held(block):
    """True if block or any of its partitions has a holder (LVM, md, ...)."""
    for h in glob.glob('/sys/class/block/%s/holders/*' % block) + \
            glob.glob('/sys/class/block/%s/%s*/holders/*' % (block, block)):
        if os.path.exists(h):
            return True
    return False


# ---- NICs ------------------------------------------------------------------
def pci_of(devpath):
    p = os.path.realpath(devpath)
    while p and p != '/':
        if BDF_RE.match(os.path.basename(p)):
            return os.path.basename(p)
        p = os.path.dirname(p)
    return None


def nics():
    out = []
    for iface in sorted(os.listdir('/sys/class/net') if os.path.isdir('/sys/class/net') else []):
        base = '/sys/class/net/' + iface
        if not os.path.exists(base + '/device'):
            continue
        drv = os.path.basename(os.path.realpath(base + '/device/driver')) \
            if os.path.exists(base + '/device/driver') else None
        pci = pci_of(base + '/device')
        numa = read_int(base + '/device/numa_node')
        speed = read_int(base + '/speed')
        combined = None
        if have('ethtool'):
            ei = save_cmd('nic/%s.ethtool-i.txt' % iface, ['ethtool', '-i', iface])
            if ei and not drv:
                m = re.search(r'^driver:\s*(\S+)', ei, re.M)
                drv = m.group(1) if m else drv
            el = save_cmd('nic/%s.ethtool-l.txt' % iface, ['ethtool', '-l', iface])
            if el and 'Current hardware settings' in el:
                cur = el.split('Current hardware settings')[1]
                m = re.search(r'^Combined:\s*(\d+)', cur, re.M)
                combined = int(m.group(1)) if m else None
            save_cmd('nic/%s.ethtool-g.txt' % iface, ['ethtool', '-g', iface])
            save_cmd('nic/%s.ethtool-k.txt' % iface, ['ethtool', '-k', iface])
        if combined is None:
            combined = len(glob.glob(base + '/queues/rx-*')) or None
        try:
            irqs = len(os.listdir(base + '/device/msi_irqs'))
        except OSError:
            irqs = 0
        out.append({'iface': iface, 'driver': drv, 'pci': pci,
                    'numa_node': None if numa is None or numa < 0 else numa,
                    'mtu': read_int(base + '/mtu'),
                    'speed': None if speed is None or speed < 0 else speed,
                    'queues_combined': combined, 'irq_count': irqs,
                    'operstate': read(base + '/operstate'),
                    'mac': read(base + '/address')})
    return out


# ---- NVMe ------------------------------------------------------------------
def pci_driver(bdf):
    p = '/sys/bus/pci/devices/%s/driver' % bdf
    return os.path.basename(os.path.realpath(p)) if os.path.exists(p) else None


def amzn_stats_supported():
    if not have('nvme'):
        return False
    rc, out, err = run(['nvme', 'amzn', 'help'])
    return 'stats' in (out + err)


def namespaces_of(ctrl_path, name):
    ns = [os.path.basename(p) for p in glob.glob(ctrl_path + '/nvme*n*')
          if NS_RE.match(os.path.basename(p))]
    if not ns:
        # native multipath: namespaces live under the subsystem
        for sub in glob.glob('/sys/class/nvme-subsystem/nvme-subsys*'):
            if os.path.exists(sub + '/' + name):
                ns = [os.path.basename(p) for p in glob.glob(sub + '/nvme*n*')
                      if NS_RE.match(os.path.basename(p))]
                break
    out = []
    for n in sorted(ns):
        base = '/sys/class/block/' + n
        sectors = read_int(base + '/size')
        out.append({'name': n,
                    'size_bytes': None if sectors is None else sectors * 512,
                    'lba_bytes': read_int(base + '/queue/logical_block_size')})
    return out


def nvme_inventory(prev):
    used, root_disks = in_use_disks()
    prev_by_pci = {e.get('pci'): e for e in prev.get('nvme', []) if e.get('pci')}
    amzn = amzn_stats_supported()
    have_nvme = have('nvme')
    nvme_denied = False
    out = []
    seen_pci = set()
    for ctrl in sorted(glob.glob('/sys/class/nvme/nvme[0-9]*'),
                       key=lambda s: int(re.sub(r'\D', '', os.path.basename(s)) or 0)):
        name = os.path.basename(ctrl)
        dev = os.path.realpath(ctrl + '/device')
        pci = os.path.basename(dev) if BDF_RE.match(os.path.basename(dev)) else pci_of(dev)
        model = read(ctrl + '/model')
        serial = read(ctrl + '/serial')
        pprev = prev_by_pci.get(pci, {})
        if not serial and pprev.get('serial'):
            serial = pprev['serial']
            warn('%s: serial unreadable, kept previous %s' % (name, serial))
        if not model and pprev.get('model'):
            model = pprev['model']
        nss = namespaces_of(ctrl, name)
        mounted = any(n['name'] in used or held(n['name']) for n in nss)
        numa = read_int(dev + '/numa_node') if pci else None
        entry = {'ctrl': name, 'model': model, 'serial': serial,
                 'firmware': read(ctrl + '/firmware_rev'), 'pci': pci,
                 'numa_node': None if numa is None or numa < 0 else numa,
                 'driver': pci_driver(pci) if pci else 'nvme',
                 'transport': read(ctrl + '/transport'),
                 'vendor_id': read(dev + '/vendor') if pci else None,
                 'device_id': read(dev + '/device') if pci else None,
                 'role': classify(model), 'namespaces': nss, 'mounted': mounted,
                 'is_root': any(n['name'] in root_disks for n in nss)}
        if have_nvme and os.path.exists('/dev/' + name):
            for sub, cmd in (('id-ctrl', ['nvme', 'id-ctrl', '/dev/' + name, '-o', 'json']),
                             ('smart-log', ['nvme', 'smart-log', '/dev/' + name, '-o', 'json'])):
                rc, o, e = run(cmd)
                if rc == 0:
                    save('nvme/%s.%s.json' % (name, sub), o)
                else:
                    save('nvme/%s.%s.err' % (name, sub), 'rc=%d\n%s' % (rc, e))
                    nvme_denied = True
            if amzn:
                rc, o, e = run(['nvme', 'amzn', 'stats', '/dev/' + name])
                if rc == 0 and o.strip():
                    save('nvme/%s.amzn-stats.txt' % name, o)
                    entry['amzn_stats'] = True
                else:
                    entry['amzn_stats'] = False
        if pci:
            seen_pci.add(pci)
        out.append(entry)
    if nvme_denied:
        warn('nvme id-ctrl/smart-log failed for some controllers (not root?); see out/inventory/nvme/*.err')
    # NVMe-class PCI functions not owned by the kernel nvme driver (vfio/uio/none).
    for d in sorted(glob.glob('/sys/bus/pci/devices/*')):
        bdf = os.path.basename(d)
        if bdf in seen_pci or read(d + '/class') != '0x010802':
            continue
        drv = pci_driver(bdf)
        numa = read_int(d + '/numa_node')
        pprev = prev_by_pci.get(bdf)
        if pprev:
            e = dict(pprev)
            e['driver'] = drv
            e['mounted'] = False
            e['is_root'] = False
            e['carried_from_previous'] = True
            e['ctrl'] = pprev.get('ctrl')
            out.append(e)
        else:
            out.append({'ctrl': None, 'model': None, 'serial': None, 'firmware': None,
                        'pci': bdf, 'numa_node': None if numa is None or numa < 0 else numa,
                        'driver': drv, 'transport': 'pcie',
                        'vendor_id': read(d + '/vendor'), 'device_id': read(d + '/device'),
                        'role': 'other', 'namespaces': [], 'mounted': False, 'is_root': False,
                        'note': 'NVMe-class device bound to %s with no previous inventory: identity unknown' % drv})
            warn('%s is an NVMe device bound to %s with no recorded identity' % (bdf, drv))
    out.sort(key=lambda e: e.get('pci') or '')
    return out, sorted(root_disks)


# ---- main --------------------------------------------------------------------
prev = {}
inv_path = os.path.join(out_dir, 'inventory.json')
if os.path.exists(inv_path):
    try:
        with open(inv_path) as f:
            prev = json.load(f)
    except (OSError, ValueError) as e:
        warn('previous inventory unreadable (%s); not merging' % e)

im = imds()
save('imds.txt', '\n'.join('%s=%s' % kv for kv in sorted(im.items())))
cpu = cpu_info()
mem_bytes, huge = meminfo()
nvme, root_disks = nvme_inventory(prev)
osr = {}
for line in (read('/etc/os-release', '') or '').splitlines():
    k, _, v = line.partition('=')
    if k:
        osr[k] = v.strip('"')
inv = {
    'generated_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
    'generated_by': 'hostprep.sh',
    'root': root,
    'hostname': socket.gethostname(),
    'imds': im,
    'kernel': run(['uname', '-a'])[1].strip() or read('/proc/version', ''),
    'cmdline': read('/proc/cmdline', ''),
    'os_release': {k: osr.get(k) for k in ('PRETTY_NAME', 'VERSION_ID', 'ID')},
    'cpu': cpu,
    'mem_bytes': mem_bytes,
    'hugepages': huge,
    'iommu': iommu_info(),
    'cpufreq': cpufreq_info(),
    'nics': nics(),
    'nvme': nvme,
    'instance_store_pcis': sorted(e['pci'] for e in nvme if e.get('role') == 'instance_store' and e.get('pci')),
    'root_disks': root_disks,
    'warnings': warnings,
}
tmp = inv_path + '.tmp'
with open(tmp, 'w') as f:
    json.dump(inv, f, indent=2, sort_keys=False)
    f.write('\n')
os.replace(tmp, inv_path)
print(json.dumps({'inventory': inv_path, 'hostname': inv['hostname'],
                  'instance_type': im.get('instance_type'),
                  'nvme': len(nvme), 'instance_store': len(inv['instance_store_pcis']),
                  'nics': len(inv['nics']), 'warnings': len(warnings)}))
PY
}

if [ "$CLASSIFY_SET" -eq 1 ]; then
  inventory_py classify "$CLASSIFY"
  exit 0
fi

mkdir -p "$INV" "$OUT/cores" "$OUT/raw" "$OUT/prof" "$OUT/bind" "$OUT/net" "$ROOT/run"

# ---- step 1: cloud-init + packages ------------------------------------------
step_packages() {
  export DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a NEEDRESTART_SUSPEND=1
  if command -v cloud-init >/dev/null 2>&1; then
    log "waiting for cloud-init (max 15 min)"
    timeout 900 cloud-init status --wait >/dev/null 2>&1 \
      || warn "cloud-init status --wait did not finish cleanly (rc=$?); continuing"
  fi
  if [ -d /etc/needrestart ]; then
    install -d /etc/needrestart/conf.d
    # shellcheck disable=SC2016  # perl config: $nrconf is literal
    printf '%s\n' '$nrconf{restart} = '"'"'a'"'"';' '$nrconf{kernelhints} = 0;' \
      > /etc/needrestart/conf.d/90-vals-cloud.conf
  fi
  local kver; kver="$(uname -r)"
  # "a|b" = alternatives in preference order; "?pkg" = optional (may not exist).
  local wanted=(fio iperf3 netperf nvme-cli numactl jq ethtool sysstat zstd rsync chrony
                pciutils hwloc-nox linux-tools-common "linux-tools-$kver|linux-tools-aws"
                libnuma1 "libaio1t64|libaio1" libuuid1 python3 "?linux-modules-extra-$kver")
  local installed=() missing=() failed=()
  pkg_installed() { [ "$(dpkg-query -W -f='${db:Status-Status}' "$1" 2>/dev/null)" = installed ]; }
  local spec optional alts a ok
  for spec in "${wanted[@]}"; do
    optional=0; [ "${spec#\?}" != "$spec" ] && { optional=1; spec="${spec#\?}"; }
    IFS='|' read -r -a alts <<< "$spec"
    ok=0
    for a in "${alts[@]}"; do pkg_installed "$a" && { ok=1; installed+=("$a"); break; }; done
    if [ $ok -eq 0 ]; then
      if [ $optional -eq 1 ]; then missing+=("$spec?"); else missing+=("$spec"); fi
    fi
  done
  if [ ${#missing[@]} -gt 0 ]; then
    log "installing missing packages: ${missing[*]}"
    apt-get update -qq >/dev/null 2>&1 || warn "apt-get update failed; trying installs anyway"
    for spec in "${missing[@]}"; do
      optional=0; [ "${spec%\?}" != "$spec" ] && { optional=1; spec="${spec%\?}"; }
      IFS='|' read -r -a alts <<< "$spec"
      ok=0
      for a in "${alts[@]}"; do
        if apt-get install -y -qq --no-install-recommends "$a" >/dev/null 2>&1; then
          ok=1; installed+=("$a"); break
        fi
      done
      if [ $ok -eq 0 ]; then
        failed+=("$spec")
        if [ $optional -eq 1 ]; then log "optional package $spec not available"; else warn "could not install $spec"; fi
      fi
    done
  else
    log "all packages present"
  fi
  PKG_INSTALLED="${installed[*]}"; PKG_FAILED="${failed[*]}"
  # uio_pci_generic availability (for the no-IOMMU SPDK path) is worth knowing now.
  if modinfo uio_pci_generic >/dev/null 2>&1; then log "uio_pci_generic module available"; else warn "uio_pci_generic module not available (needed only if there is no IOMMU)"; fi
}

# ---- step 2: sysctls ----------------------------------------------------------
step_sysctls() {
  local keys=(kernel.core_pattern fs.suid_dumpable kernel.perf_event_paranoid kernel.kptr_restrict kernel.nmi_watchdog)
  if [ ! -s "$INV/sysctl-before.txt" ]; then
    local k
    for k in "${keys[@]}"; do printf '%s = %s\n' "$k" "$(sysctl -n "$k" 2>/dev/null || echo '?')"; done > "$INV/sysctl-before.txt"
  fi
  mkdir -p "$OUT/cores"; chmod 1777 "$OUT/cores"
  sysctl -q -w "kernel.core_pattern=$OUT/cores/core.%e.%p.%t" || warn "could not set kernel.core_pattern"
  sysctl -q -w fs.suid_dumpable=2 || warn "could not set fs.suid_dumpable"
  sysctl -q -w kernel.perf_event_paranoid=-1 || warn "could not set kernel.perf_event_paranoid"
  sysctl -q -w kernel.kptr_restrict=0 || warn "could not set kernel.kptr_restrict"
  sysctl -q -w kernel.nmi_watchdog=0 || warn "could not set kernel.nmi_watchdog"
  # apport rewrites core_pattern when it (re)starts; keep it out of the way.
  if systemctl list-unit-files apport.service >/dev/null 2>&1 && systemctl is-enabled apport >/dev/null 2>&1; then
    systemctl stop apport >/dev/null 2>&1 || true
    systemctl disable apport >/dev/null 2>&1 || warn "could not disable apport"
    log "apport disabled (it would reset kernel.core_pattern)"
  fi
  # Core dumps also need a core rlimit; cover ssh sessions of the test user.
  printf '%s\n' '* soft core unlimited' '* hard core unlimited' > /etc/security/limits.d/90-vals-cloud-core.conf
  sysctl -a > "$INV/sysctl-all.txt" 2>/dev/null || warn "sysctl -a failed"
  log "sysctls set (core_pattern -> $OUT/cores, perf unrestricted, nmi_watchdog=0)"
}

# ---- step 3: cpufreq ----------------------------------------------------------
step_cpufreq() {
  local base=/sys/devices/system/cpu/cpu0/cpufreq
  if [ -d "$base" ]; then
    log "cpufreq driver=$(cat $base/scaling_driver 2>/dev/null) governor(before)=$(cat $base/scaling_governor 2>/dev/null)"
    local g n=0
    for g in /sys/devices/system/cpu/cpu[0-9]*/cpufreq/scaling_governor; do
      echo performance > "$g" 2>/dev/null && n=$((n + 1)) || true
    done
    log "governor=performance on $n cpus"
  else
    log "no cpufreq interface (governor not applicable)"
  fi
}

# ---- step 4: FlameGraph -------------------------------------------------------
step_flamegraph() {
  if [ -x "$ROOT/FlameGraph/flamegraph.pl" ]; then log "FlameGraph present"; return 0; fi
  command -v git >/dev/null 2>&1 || { warn "git missing; FlameGraph not cloned"; return 0; }
  if timeout 180 git clone -q --depth 1 https://github.com/brendangregg/FlameGraph "$ROOT/FlameGraph" >/dev/null 2>&1; then
    log "FlameGraph cloned"
  else
    warn "FlameGraph clone failed (flamegraphs will be skipped)"
  fi
  fix_owner "$ROOT/FlameGraph"
}

# ---- step 5: raw inventory text files -----------------------------------------
dump() { # dump <file> <cmd...>
  local f="$INV/$1"; shift
  command -v "$1" >/dev/null 2>&1 || { warn "$1 not installed ($(basename "$f") skipped)"; return 0; }
  "$@" > "$f" 2>/dev/null || warn "$* failed (partial $(basename "$f"))"
}

step_inventory_raw() {
  hostname > "$INV/hostname.txt" 2>/dev/null || true
  uname -a > "$INV/uname.txt"
  cat /proc/cmdline > "$INV/cmdline.txt"
  cat /etc/os-release > "$INV/os-release.txt" 2>/dev/null || true
  dump lscpu.json lscpu -J
  dump lscpu.txt lscpu
  dump numactl.txt numactl -H
  dump free.txt free -b
  cat /proc/meminfo > "$INV/meminfo.txt"
  { grep -i huge /proc/meminfo; for f in /sys/kernel/mm/hugepages/*/nr_hugepages /sys/devices/system/node/node*/hugepages/*/nr_hugepages; do
      [ -e "$f" ] && printf '%s = %s\n' "$f" "$(cat "$f")"; done; } > "$INV/hugepages.txt" 2>/dev/null || true
  { printf 'iommu_groups=%s\n' "$(find /sys/kernel/iommu_groups -mindepth 1 -maxdepth 1 2>/dev/null | wc -l)"
    printf 'cmdline_iommu=%s\n' "$(tr ' ' '\n' < /proc/cmdline | grep -i iommu | tr '\n' ' ')"
    dmesg 2>/dev/null | grep -i -E 'DMAR|IOMMU' | head -40; } > "$INV/iommu.txt" 2>/dev/null || true
  dump lstopo.xml lstopo-no-graphics --of xml
  dump lstopo.txt lstopo-no-graphics -p
  dump chrony.txt chronyc tracking
  ulimit -a > "$INV/ulimit.txt" 2>/dev/null || true
  dump lsblk.txt lsblk -o NAME,TYPE,SIZE,MODEL,SERIAL,MOUNTPOINT,PKNAME
  dump lsblk.json lsblk -J -O
  cat /proc/mounts > "$INV/mounts.txt"
  cat /proc/interrupts > "$INV/interrupts.txt" 2>/dev/null || true
  { for c in /sys/devices/system/cpu/cpu0/cpufreq/scaling_driver /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor \
             /sys/devices/system/cpu/intel_pstate/no_turbo /sys/devices/system/cpu/cpufreq/boost \
             /sys/devices/system/cpu/cpuidle/current_driver; do
      [ -r "$c" ] && printf '%s = %s\n' "$c" "$(cat "$c")"; done
    for s in /sys/devices/system/cpu/cpu0/cpuidle/state*; do
      [ -d "$s" ] && printf 'cpu0 %s: name=%s disable=%s latency_us=%s\n' "$(basename "$s")" "$(cat "$s/name")" "$(cat "$s/disable" 2>/dev/null)" "$(cat "$s/latency")"; done; } \
    > "$INV/cpufreq.txt" 2>/dev/null || true
  [ -s "$INV/sysctl-all.txt" ] || sysctl -a > "$INV/sysctl-all.txt" 2>/dev/null || true
  dump lspci.txt lspci -nn
  dump nvme-list.json nvme list -o json
}

# ---- driver ---------------------------------------------------------------------
PKG_INSTALLED=""; PKG_FAILED=""
if [ "$INVENTORY_ONLY" -eq 0 ]; then
  [ "$(id -u)" -eq 0 ] || die "full hostprep needs root (use --inventory-only for an unprivileged inventory)"
  step_packages
  step_sysctls
  step_cpufreq
  step_flamegraph
fi
step_inventory_raw
log "building inventory -> $OUT/inventory.json"
SUMMARY="$(inventory_py build "$ROOT")"
fix_owner "$OUT" "$ROOT/run"
# shellcheck disable=SC2016
RESULT="$(python3 -c 'import json,sys; s=json.loads(sys.argv[1]); s.update(ok=True, inventory_only=sys.argv[2]=="1", packages_installed=sys.argv[3].split(), packages_failed=sys.argv[4].split(), script_warnings=int(sys.argv[5])); print(json.dumps(s,separators=(",",":")))' \
  "$SUMMARY" "$INVENTORY_ONLY" "$PKG_INSTALLED" "$PKG_FAILED" "$WARNINGS")"
echo "RESULT $RESULT"
