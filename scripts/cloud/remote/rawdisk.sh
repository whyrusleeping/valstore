#!/usr/bin/env bash
#
# rawdisk.sh — kernel-driver fio baseline of the instance-store NVMe drives
# (all drives concurrently, one job section per drive, so the JSON has both
# per-drive and whole-node numbers).
#
# Usage (root):
#   sudo -E env VALS_CLOUD_ROOT=/home/ubuntu/vals bash $ROOT/cloud/remote/rawdisk.sh \
#       [--secs 30] [--size 64G]
#       [--tests [precondition,]randread4k,randwrite4k,seqwrite128k,seqread128k,randrw70_4k] [--root DIR]
#
# Tests (direct=1, time_based, ramp_time=5, group_reporting per drive):
#   randread4k    4k randread   iodepth=64 numjobs=4
#   randwrite4k   4k randwrite  iodepth=64 numjobs=4
#   seqwrite128k  128k write    iodepth=32 numjobs=1
#   seqread128k   128k read     iodepth=32 numjobs=1
#   randrw70_4k   4k randrw 70% read iodepth=64 numjobs=4
#
# Engine: io_uring, falling back to libaio. Each drive's jobs are pinned to the
# drive's NUMA node (numa_cpu_nodes / numa_mem_policy from the inventory) when
# fio supports it; whether that happened is recorded in summary.json.
#
# Refuses to run if any target drive is mounted/held/root/EBS, or is not bound
# to the kernel nvme driver (i.e. SPDK owns it: run spdk-bind.sh reset first).
#
# Outputs under $ROOT/out/raw/:
#   fio-<test>.json         fio --output-format=json+ (with latency histograms)
#   jobs/<test>.fio         the job file actually run
#   nvme-pre/ nvme-post/    smart-log (+ amzn stats when supported) snapshots
#   summary.json/.txt       per-test aggregate IOPS / MB/s / p50 p99 p99.9
#
# Caveat: random reads of never-written LBAs on instance store return zeros
# without touching NAND. For NAND-realistic numbers bound every job to a region
# with --size (e.g. 64G) and run `precondition` first: it writes that whole
# region once (1 MiB sequential, not time-based), so later reads hit data.
#
# Prints one machine-readable line at the end:  RESULT {...json...}
set -euo pipefail

log()  { printf '[rawdisk] %s\n' "$*"; }
warn() { printf '[rawdisk] WARN: %s\n' "$*" >&2; }
die()  { [ -z "${OUT:-}" ] || fix_owner "$OUT"; printf '[rawdisk] ERROR: %s\n' "$*" >&2; echo "RESULT {\"ok\":false,\"error\":$(python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$*")}"; exit 1; }

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

SECS=30
TESTS="randread4k,randwrite4k,seqwrite128k,seqread128k,randrw70_4k"
SIZE=""
ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --secs) SECS="$2"; shift 2;;
    --tests) TESTS="$2"; shift 2;;
    --size) SIZE="$2"; shift 2;;
    --root) ROOT="$2"; shift 2;;
    -h|--help) grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) die "unknown argument: $1";;
  esac
done
[ -n "$ROOT" ] || ROOT="$(default_root)"
OUT="$ROOT/out"
RAW="$OUT/raw"
INVENTORY="$OUT/inventory.json"
mkdir -p "$RAW/jobs" "$RAW/nvme-pre" "$RAW/nvme-post"

[ "$(id -u)" -eq 0 ] || die "rawdisk.sh needs root (raw block device access)"
command -v fio >/dev/null 2>&1 || die "fio not installed"
[ -s "$INVENTORY" ] || die "no inventory at $INVENTORY (run hostprep.sh first)"

# ---- targets: instance-store PCIs -> block devices (live checks) ---------------
# Prints one line per drive: PCI BLOCK NUMA  — or "REFUSE <reason>" lines.
resolve_targets() {
  python3 - "$INVENTORY" <<'PY'
import glob, json, os, stat, sys
inv = json.load(open(sys.argv[1]))
pcis = inv.get('instance_store_pcis', [])
by_pci = {e.get('pci'): e for e in inv.get('nvme', [])}
if not pcis:
    print('REFUSE inventory has no instance_store_pcis')
    sys.exit(0)

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

used = set()
for line in (read('/proc/mounts') or '').splitlines() + (read('/proc/swaps') or '').splitlines()[1:]:
    f = line.split()
    if f and f[0].startswith('/dev/'):
        used |= disks_of_path(f[0])
try:
    st = os.stat('/')
    root_disks = disks_of_devno('%d:%d' % (os.major(st.st_dev), os.minor(st.st_dev)))
except OSError:
    root_disks = set()

for bdf in pcis:
    e = by_pci.get(bdf, {})
    if e.get('role') == 'ebs' or e.get('is_root'):
        print('REFUSE %s is EBS/root per inventory' % bdf)
        continue
    drv = os.path.basename(os.path.realpath('/sys/bus/pci/devices/%s/driver' % bdf)) \
        if os.path.exists('/sys/bus/pci/devices/%s/driver' % bdf) else 'none'
    if drv != 'nvme':
        print('REFUSE %s is bound to %s, not the kernel nvme driver (spdk-bind.sh reset?)' % (bdf, drv))
        continue
    ctrls = glob.glob('/sys/bus/pci/devices/%s/nvme/nvme*' % bdf)
    blocks = []
    for c in ctrls:
        blocks += [os.path.basename(b) for b in glob.glob(c + '/nvme*n*')]
        for sub in glob.glob('/sys/class/nvme-subsystem/nvme-subsys*'):
            if os.path.exists(sub + '/' + os.path.basename(c)):
                blocks += [os.path.basename(b) for b in glob.glob(sub + '/nvme*n*')]
    blocks = sorted(set(b for b in blocks if os.path.exists('/dev/' + b)))
    if not blocks:
        print('REFUSE %s has no namespace block device' % bdf)
        continue
    model = read('%s/model' % ctrls[0]) if ctrls else None
    if model and 'elastic block store' in model.lower():
        print('REFUSE %s is an EBS controller (%s)' % (bdf, model))
        continue
    for b in blocks:
        if b in used or b in root_disks:
            print('REFUSE %s (%s) is mounted / swap / root' % (bdf, b))
            continue
        if glob.glob('/sys/class/block/%s/holders/*' % b) or glob.glob('/sys/class/block/%s/%s*/holders/*' % (b, b)):
            print('REFUSE %s (%s) is held by md/dm' % (bdf, b))
            continue
        numa = read('/sys/bus/pci/devices/%s/numa_node' % bdf)
        numa = numa if numa not in (None, '', '-1') else ''
        print('%s %s %s' % (bdf, b, numa))
PY
}

mapfile -t TARGETS < <(resolve_targets)
REFUSALS=()
DRIVES=()   # "pci block numa"
for t in "${TARGETS[@]}"; do
  case "$t" in
    REFUSE\ *) REFUSALS+=("${t#REFUSE }");;
    "") ;;
    *) DRIVES+=("$t");;
  esac
done
if [ ${#REFUSALS[@]} -gt 0 ]; then
  for r in "${REFUSALS[@]}"; do warn "$r"; done
  die "refusing to run: ${#REFUSALS[@]} target(s) unsafe"
fi
[ ${#DRIVES[@]} -gt 0 ] || die "no usable instance-store drives"
log "${#DRIVES[@]} drives:"
for d in "${DRIVES[@]}"; do log "  $d"; done

# ---- engine / numa capability probes -------------------------------------------
ENGINE=io_uring
if ! fio --enghelp 2>/dev/null | grep -qw io_uring; then ENGINE=libaio; fi
probe="$(mktemp)"
cat > "$probe" <<EOF
[global]
ioengine=$ENGINE
direct=1
[p]
filename=/dev/null
rw=read
size=4k
numa_cpu_nodes=0
numa_mem_policy=local
EOF
NUMA_PIN=true
if ! fio --parse-only "$probe" >/dev/null 2>&1; then
  sed -i '/^numa_/d' "$probe"
  if fio --parse-only "$probe" >/dev/null 2>&1; then
    NUMA_PIN=false; warn "fio lacks libnuma support: jobs will not be pinned to the drive's NUMA node"
  elif [ "$ENGINE" = io_uring ]; then
    ENGINE=libaio
    sed -i 's/^ioengine=.*/ioengine=libaio/' "$probe"
    fio --parse-only "$probe" >/dev/null 2>&1 || NUMA_PIN=false
    warn "io_uring engine unusable; falling back to libaio"
  fi
fi
rm -f "$probe"
# io_uring needs root or io_uring enabled; verify once with a 1-op read on the first drive.
first_block="$(echo "${DRIVES[0]}" | awk '{print $2}')"
if [ "$ENGINE" = io_uring ] && ! fio --name=probe --ioengine=io_uring --direct=1 --rw=read --bs=4k --size=64k --filename="/dev/$first_block" --output-format=terse >/dev/null 2>&1; then
  warn "io_uring probe failed on /dev/$first_block; falling back to libaio"
  ENGINE=libaio
fi
log "engine=$ENGINE numa_pin=$NUMA_PIN secs=$SECS tests=$TESTS"

# ---- NVMe snapshots ------------------------------------------------------------------
snapshot() { # snapshot <dir>
  local dir="$1" d block ctrl
  command -v nvme >/dev/null 2>&1 || { warn "nvme-cli missing; no smart-log snapshot"; return 0; }
  local amzn=false
  nvme amzn help 2>&1 | grep -q stats && amzn=true
  for d in "${DRIVES[@]}"; do
    block="$(echo "$d" | awk '{print $2}')"
    ctrl="${block%n*}"
    nvme smart-log "/dev/$ctrl" -o json > "$dir/$ctrl.smart-log.json" 2>/dev/null || warn "smart-log failed for $ctrl"
    if [ "$amzn" = true ]; then
      nvme amzn stats "/dev/$ctrl" > "$dir/$ctrl.amzn-stats.txt" 2>/dev/null || rm -f "$dir/$ctrl.amzn-stats.txt"
    fi
  done
  date -u +%Y-%m-%dT%H:%M:%SZ > "$dir/timestamp.txt"
}

# ---- job files ----------------------------------------------------------------------------
test_params() { # test -> "rw bs iodepth numjobs extra"
  case "$1" in
    randread4k)   echo "randread 4k 64 4 ";;
    randwrite4k)  echo "randwrite 4k 64 4 ";;
    seqwrite128k) echo "write 128k 32 1 ";;
    seqread128k)  echo "read 128k 32 1 ";;
    randrw70_4k)  echo "randrw 4k 64 4 rwmixread=70";;
    precondition) echo "write 1m 32 1 time_based=0";;
    *) return 1;;
  esac
}

write_job() { # write_job <test> <path>
  local params; params="$(test_params "$1")" || die "unknown test: $1"
  local rw bs iodepth numjobs extra
  read -r rw bs iodepth numjobs extra <<< "$params"
  {
    echo "[global]"
    echo "ioengine=$ENGINE"
    echo "direct=1"
    echo "time_based=1"
    echo "runtime=$SECS"
    echo "ramp_time=5"
    echo "group_reporting=1"
    echo "norandommap=1"
    echo "randrepeat=0"
    echo "rw=$rw"
    echo "bs=$bs"
    echo "iodepth=$iodepth"
    echo "numjobs=$numjobs"
    [ -n "$SIZE" ] && echo "size=$SIZE"
    [ -n "$extra" ] && echo "$extra"
    local d pci block numa
    for d in "${DRIVES[@]}"; do
      read -r pci block numa <<< "$d"
      echo
      echo "[$block]"
      echo "filename=/dev/$block"
      echo "new_group=1"
      echo "# pci=$pci numa=$numa"
      if [ "$NUMA_PIN" = true ] && [ -n "$numa" ]; then
        echo "numa_cpu_nodes=$numa"
        echo "numa_mem_policy=bind:$numa"
      fi
    done
  } > "$2"
}

# ---- run ------------------------------------------------------------------------------------
snapshot "$RAW/nvme-pre"
START="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RAN=(); FAILED=()
IFS=',' read -r -a TEST_LIST <<< "$TESTS"
for t in "${TEST_LIST[@]}"; do
  [ -n "$t" ] || continue
  job="$RAW/jobs/$t.fio"
  write_job "$t" "$job"
  log "running $t (${SECS}s + 5s ramp) on ${#DRIVES[@]} drives"
  if fio --output-format=json+ --output="$RAW/fio-$t.json" "$job" >/dev/null 2>"$RAW/fio-$t.stderr"; then
    RAN+=("$t"); rm -f "$RAW/fio-$t.stderr"
  else
    FAILED+=("$t"); warn "fio failed for $t (see $RAW/fio-$t.stderr)"
  fi
done
snapshot "$RAW/nvme-post"
END="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

# ---- summary ----------------------------------------------------------------------------------
SUMMARY="$(python3 - "$RAW" "$ENGINE" "$NUMA_PIN" "$SECS" "$START" "$END" "${RAN[@]}" <<'PY'
import json, os, sys
raw, engine, numa_pin, secs, start, end = sys.argv[1:7]
tests = sys.argv[7:]

def pct_from_bins(bins, pcts):
    """Exact percentiles from merged json+ clat_ns histograms."""
    items = sorted((int(k), v) for k, v in bins.items())
    total = sum(v for _, v in items)
    out, acc, i = {}, 0, 0
    if total == 0:
        return {p: None for p in pcts}
    for p in sorted(pcts):
        target = total * p / 100.0
        while i < len(items) and acc + items[i][1] < target:
            acc += items[i][1]
            i += 1
        out[p] = items[min(i, len(items) - 1)][0] / 1000.0  # us
    return out

summary = {'engine': engine, 'numa_pinned': numa_pin == 'true', 'secs': int(secs),
           'start': start, 'end': end, 'tests': {}}
rows = []
for t in tests:
    path = os.path.join(raw, 'fio-%s.json' % t)
    try:
        with open(path) as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        summary['tests'][t] = {'error': str(e)}
        continue
    agg = {'iops': 0.0, 'mbps': 0.0, 'drives': {}}
    bins, fallback = {}, []
    for job in data.get('jobs', []):
        name = job.get('jobname')
        dd = {}
        for d in ('read', 'write'):
            s = job.get(d, {})
            if not s or s.get('io_bytes', 0) == 0:
                continue
            clat = s.get('clat_ns', {})
            pc = clat.get('percentile', {})
            dd[d] = {'iops': s.get('iops', 0.0), 'mbps': s.get('bw_bytes', 0) / 1e6,
                     'p50_us': pc.get('50.000000', 0) / 1000.0, 'p99_us': pc.get('99.000000', 0) / 1000.0,
                     'p999_us': pc.get('99.900000', 0) / 1000.0, 'mean_us': clat.get('mean', 0) / 1000.0}
            agg['iops'] += s.get('iops', 0.0)
            agg['mbps'] += s.get('bw_bytes', 0) / 1e6
            for k, v in clat.get('bins', {}).items():
                bins[k] = bins.get(k, 0) + v
            fallback.append(dd[d])
        dd['numa_pinned'] = bool(job.get('job options', {}).get('numa_cpu_nodes'))
        dd['errors'] = job.get('error', 0)
        agg['drives'][name] = dd
    if bins:
        p = pct_from_bins(bins, (50, 99, 99.9))
        agg.update(p50_us=p[50], p99_us=p[99], p999_us=p[99.9], latency_source='merged histograms')
    elif fallback:
        agg.update(p50_us=max(x['p50_us'] for x in fallback), p99_us=max(x['p99_us'] for x in fallback),
                   p999_us=max(x['p999_us'] for x in fallback), latency_source='worst drive')
    agg['disk_util'] = {d.get('name'): d.get('util') for d in data.get('disk_util', [])}
    summary['tests'][t] = agg
    rows.append((t, agg['iops'], agg['mbps'], agg.get('p50_us'), agg.get('p99_us'), agg.get('p999_us')))
with open(os.path.join(raw, 'summary.json'), 'w') as f:
    json.dump(summary, f, indent=2)
    f.write('\n')
with open(os.path.join(raw, 'summary.txt'), 'w') as f:
    f.write('%-14s %12s %10s %9s %9s %9s\n' % ('test', 'IOPS', 'MB/s', 'p50us', 'p99us', 'p99.9us'))
    for r in rows:
        f.write('%-14s %12.0f %10.1f %9.1f %9.1f %9.1f\n' % (r[0], r[1], r[2], r[3] or 0, r[4] or 0, r[5] or 0))
compact = {t: {k: v for k, v in a.items() if k in ('iops', 'mbps', 'p50_us', 'p99_us', 'p999_us')}
           for t, a in summary['tests'].items()}
print(json.dumps(compact, separators=(',', ':')))
PY
)"
log "=== per-test aggregate over ${#DRIVES[@]} drives (latency from merged histograms) ==="
sed 's/^/  /' "$RAW/summary.txt"
fix_owner "$RAW"
OK=true; [ ${#FAILED[@]} -eq 0 ] || OK=false
echo "RESULT $(python3 -c 'import json,sys; print(json.dumps({"ok": sys.argv[1]=="true", "engine": sys.argv[2], "numa_pinned": sys.argv[3]=="true", "secs": int(sys.argv[4]), "drives": int(sys.argv[5]), "ran": sys.argv[6].split(), "failed": sys.argv[7].split(), "summary": json.loads(sys.argv[8]), "dir": sys.argv[9]}, separators=(",",":")))' \
  "$OK" "$ENGINE" "$NUMA_PIN" "$SECS" "${#DRIVES[@]}" "${RAN[*]:-}" "${FAILED[*]:-}" "$SUMMARY" "$RAW")"
[ "$OK" = true ]
