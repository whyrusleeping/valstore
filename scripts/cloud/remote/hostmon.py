#!/usr/bin/env python3
"""hostmon: stdlib-only host sampler for vals cloud stress-test fleets.

Runs on every machine of a benchmark fleet (storage nodes and clients) for the
whole session and records cumulative raw counters (never deltas, so gaps and
restarts stay harmless; `summarize` computes rates).  Works unprivileged: any
source that cannot be read is skipped and reported once as an error record.

CLI
---
  hostmon.py run --out DIR [--interval 1.0] [--slow-every 5] [--vslow-every 60]
                 [--metrics NAME=URL]... [--proc NAME=REGEX]... [--iface IFACE]...
                 [--latest PATH] [--watch FAMILY]... [--duration SECS]
                 [--rotate-secs 3600]
  hostmon.py summarize DIR [--from UNIX_TS] [--to UNIX_TS] [--json]

`run` samples until SIGTERM/SIGINT or --duration, then flushes and exits 0.
Fast ticks are scheduled against monotonic deadlines; overrun ticks are skipped
and the next record carries "late": seconds.  The slow and very-slow tiers run
on a worker thread (triggered every --slow-every / --vslow-every fast ticks) so
a hung scrape or ethtool never delays the fast tier; if the worker is still
busy when the next slow tick is due, that slow tick is skipped and the next
slow record carries "skipped": n.

Output files
------------
DIR/samples-<YYYYmmddTHHMMSSZ>.jsonl, one JSON object per line, flushed after
every record.  Files rotate every --rotate-secs; rotated files are gzipped by a
background thread (written to .gz.tmp, renamed to .jsonl.gz, then the plain
file is removed).  The current file is left as plain .jsonl on exit.
Non-finite floats (NaN/Inf metric values) are written as Python's json does
(NaN / Infinity), which Python's json.loads accepts.

Record schema (all records have "t": unix seconds float and "tier"):
  header: {"tier":"header","version":1,"hostname","boot_id","argv","ncpu",
           "clk_tck","page_size","pid","kernel","interval","slow_every",
           "vslow_every","numa": {"node0": [cpu, ...], ...}}
  error:  {"tier":"error","source","error"}  -- once per (source, error)
  fast:   {"tier":"fast","late"?: s,
           "cpu":  {"all": [user,nice,system,idle,iowait,irq,softirq,steal],
                    "0": [...], "1": [...], ...},          (jiffies, clk_tck)
           "sys":  {"ctxt","intr","procs_running","procs_blocked"},
           "net":  {IF: [rx_bytes,rx_packets,rx_errs,rx_drop,
                         tx_bytes,tx_packets,tx_errs,tx_drop]},
           "disk": {DEV: [reads,reads_merged,sectors_read,ms_reading,
                          writes,writes_merged,sectors_written,ms_writing,
                          in_flight,ms_io,weighted_ms_io]}, (nvme*n*/xvd*/sd*)
           "mem":  {MemTotal,...: bytes; HugePages_Total/Free: pages},
           "load": [load1, load5, load15]}
  slow:   {"tier":"slow","skipped"?: n,
           "procs": {NAME: {"pid": int|null, "alive": bool, "cmdline"?: str
                            (only when the pid changed), "start": starttime
                            jiffies, "utime","stime" (process total, jiffies),
                            "status": {VmRSS,VmHWM,VmSwap (bytes), Threads,
                                       voluntary_ctxt_switches,
                                       nonvoluntary_ctxt_switches},
                            "io": {rchar,wchar,syscr,syscw,read_bytes,
                                   write_bytes,cancelled_write_bytes},
                            "threads": [[tid, comm, state, utime, stime,
                                         last_cpu], ...]}},
           "metrics": {NAME: {"up": bool, "ms": scrape ms, "error"?: str,
                              "full": bool, "s": {series: float},
                              "types"?: {family: type}}},
           "ethtool": {IF: {counter: int}},
           "snmp": {"Tcp": {...}, "Udp": {...}, ...},
           "netstat": {"TcpExt": {...}, "IpExt": {...}, ...},
           "softnet": [[processed, dropped, time_squeeze], ...]  (per CPU row),
           "vmstat": {...}, "pressure": {"cpu": {"some": {"avg10",...,"total"},
                                                 "full": {...}}, ...},
           "numastat": {"node0": {...}}, "self": {"utime","stime"} (seconds)}
  vslow:  {"tier":"vslow","irq": {"45/nvme0q0" | "LOC": total_all_cpus},
           "irq_cpu": [per-CPU total], "chrony"?: {...}, "fs": {"path",
           "total_bytes","free_bytes","avail_bytes"}, "boot_id"}

Per-thread voluntary/nonvoluntary ctxt switches (/proc/PID/task/TID/status)
are NOT sampled: that would double the per-thread file opens for a few hundred
threads every slow tick (and status is ~20x larger than stat to generate).
Note the kernel reports /proc/PID/status ctxt switches for the thread-group
leader (main thread) only, so the per-process "vol/nonvol_ctxt_per_s" in
summarize is main-thread-only; use the fast-tier system-wide ctxt rate for
totals.  Per-thread stat reads cost ~10-15 us each (734 threads ~10 ms).

Metrics storage: each endpoint is stored as a full snapshot ("full": true,
with "types" from # TYPE lines) on its first successful scrape, after any
failed scrape, and every 12th slow tick; in between only series whose value
changed since the previous scrape are stored ("full": false).  Reconstruction
(summarize): state = s on full records, state.update(s) on deltas.  Series that
disappear are dropped at the next full snapshot.  Series keys are
`name{labels}`: the label text exactly as exposed when the line has no
backslash or '#' (fast path), otherwise re-serialized as `k="v",...` with
values re-escaped; either way a given exposition line always maps to the same
key, and split_series() parses both forms.

--latest PATH (rewritten atomically every slow tick):
  {"t","hostname","procs": {NAME: {"pid","alive","rss_bytes","restarts"}},
   "metrics": {NAME: {"up": bool, "watch": {FAMILY: float}}},
   "nic": {IF: {counter: int}}  (ethtool counters containing "allowance" or
                                 "exceeded"),
   "mem": {"available_bytes","swap_used_bytes"}, "load1": float}
  "restarts" counts observed pid changes since hostmon started.  Watch values
  are sums over all label sets of series named exactly FAMILY.

summarize --json keys (stable):
  {"window": {"from","to","seconds","files","records","bad_lines","hosts"},
   "cpu": {"ncpu","busy_pct","iowait_pct","steal_pct","softirq_pct",
           "irq_pct","per_cpu_busy_pct": {cpu: pct},
           "top10": [[cpu, pct], ...], "numa": {node: busy_pct},
           "ctxt_per_s","intr_per_s"},
   "net": {IF: {"rx_gbps","tx_gbps","rx_kpps","tx_kpps","peak_rx_gbps",
                "peak_tx_gbps","peak_rx_kpps","peak_tx_kpps","rx_drop",
                "tx_drop","rx_errs","tx_errs"}},
   "ena": {IF: {counter: delta}}  (allowance/exceeded counters; plus
                                   conntrack_allowance_available_min),
   "tcp": {"RetransSegs","TCPLostRetransmit","ListenDrops",
           "ListenOverflows","TCPTimeouts": delta, "InSegs_per_s",
           "OutSegs_per_s", "retrans_pct"},
   "disk": {DEV: {"read_mbps","write_mbps","read_iops","write_iops",
                  "avg_queue","util_pct"}},
   "mem": {"min_available_bytes","max_swap_used_bytes",
           "hugepages_total","hugepages_free_min"},
   "procs": {NAME: {"cores","seconds","pid_changes","pids","rss_min_bytes",
                    "rss_max_bytes","vol_ctxt_per_s","nonvol_ctxt_per_s",
                    "read_mbps","write_mbps",
                    "threads_by_comm": {comm: cores},
                    "threads_by_group": {comm_without_trailing_digits: cores}}},
       (cores = CPU seconds / seconds the same pid was observed alive;
        group names also drop trailing "-_:/.# " after the digits)
   "metrics": {NAME: {"seconds","scrapes","down_scrapes",
                      "counters": {series: {"delta","rate","resets"}},
                      "gauges": {series: {"first","last","min","max"}},
                      "histograms": {name{labels-without-le}: {"count",
                                     "mean","p50","p90","p99","p999"}}}},
   "softnet": {"processed","dropped","squeezed"},
   "irq_top": [[irq, delta], ...],      (top 20 from the vslow tier)
   "errors": [[source, error], ...]}    (distinct in-window error records)
  Counters with a reset (decrease) add the post-reset value.  Counter-like =
  TYPE counter/histogram/summary _sum/_count, else suffix _total/_count/_sum/
  _bucket, else (untyped) never decreased in-window.  Histogram quantiles
  interpolate linearly inside the bucket (like Prometheus histogram_quantile).
  Non-finite values are emitted as null.
"""

import argparse
import gzip
import json
import math
import os
import queue
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.request

VERSION = 1
FULL_EVERY = 12

DEFAULT_WATCH = [
    "vals_memtable_integrity_failures_total", "vals_encode_integrity_failures_total",
    "vals_store_corrupt_reads_total", "vals_run_build_verify_failures_total",
    "vals_integrity_fail_fast_total", "vals_watermark_persist_errors", "vals_snapshot_errors",
    "vals_compaction_errors", "vals_compaction_plan_failures", "vals_server_errors",
    "vals_busy_rejections", "vals_ram_busy_rejections", "vals_region_exhausted_rejections",
    "vals_transport_request_rejections", "vals_sweep_passes", "vals_sweep_pass_age_seconds",
    "vals_ae_dirty_buckets", "vals_ae_dirty_oldest_ms", "vals_ae_sync_errors",
    "vals_process_resident_bytes", "vals_process_swap_bytes", "vals_min_zone_free_segments",
    "vals_used_segments", "vals_free_segments", "vals_live_keys", "vals_pd_recovery_down_nodes",
    "vals_pd_drive_errors", "vals_pd_push_failures", "vals_control_incarnation_rejected",
]

MEMINFO_KEYS = ("MemTotal", "MemFree", "MemAvailable", "Buffers", "Cached", "SwapTotal",
                "SwapFree", "Dirty", "Writeback", "AnonPages", "Shmem", "HugePages_Total",
                "HugePages_Free", "Hugepagesize")
STATUS_KEYS = ("VmRSS", "VmHWM", "VmSwap", "Threads", "voluntary_ctxt_switches",
               "nonvoluntary_ctxt_switches")
DISK_RE = re.compile(r"^(nvme\d+n\d+|xvd[a-z]+|sd[a-z]+)$")
CHRONY_FIELDS = ("ref_id", "ref_name", "stratum", "ref_time", "system_time_s", "last_offset_s",
                 "rms_offset_s", "freq_ppm", "residual_freq_ppm", "skew_ppm", "root_delay_s",
                 "root_dispersion_s", "update_interval_s", "leap_status")


# ---------------------------------------------------------------- parsers --
# All parsers are pure: they take file contents as strings.

def parse_proc_stat(text):
    """/proc/stat -> {"cpu": {"all": [8 ints], "N": [8 ints]}, "sys": {...}}."""
    cpu, sysd = {}, {}
    for line in text.splitlines():
        if line.startswith("cpu"):
            parts = line.split()
            name = parts[0][3:] or "all"
            vals = [int(x) for x in parts[1:9]]
            vals += [0] * (8 - len(vals))
            cpu[name] = vals
        elif line.startswith("intr "):
            sysd["intr"] = int(line.split(None, 2)[1])
        elif line.startswith(("ctxt ", "procs_running ", "procs_blocked ")):
            k, v = line.split()[:2]
            sysd[k] = int(v)
    return {"cpu": cpu, "sys": sysd}


def parse_net_dev(text):
    """/proc/net/dev -> {iface: [rx_bytes,rx_packets,rx_errs,rx_drop,tx_bytes,...]}."""
    out = {}
    for line in text.splitlines():
        if ":" not in line:
            continue
        name, _, rest = line.partition(":")
        f = rest.split()
        if len(f) < 16:
            continue
        try:
            v = [int(x) for x in f]
        except ValueError:
            continue
        out[name.strip()] = [v[0], v[1], v[2], v[3], v[8], v[9], v[10], v[11]]
    return out


def parse_diskstats(text):
    """/proc/diskstats -> {dev: [11 ints]} for whole nvme/xvd/sd devices."""
    out = {}
    for line in text.splitlines():
        f = line.split()
        if len(f) < 14 or not DISK_RE.match(f[2]):
            continue
        out[f[2]] = [int(x) for x in f[3:14]]
    return out


def parse_meminfo(text, keys=MEMINFO_KEYS):
    """/proc/meminfo -> {key: bytes} (kB fields converted; page counts kept)."""
    out = {}
    for line in text.splitlines():
        k, _, rest = line.partition(":")
        if keys is not None and k not in keys:
            continue
        f = rest.split()
        if not f:
            continue
        v = int(f[0])
        if len(f) > 1 and f[1] == "kB":
            v *= 1024
        out[k] = v
    return out


def parse_loadavg(text):
    f = text.split()
    return [float(f[0]), float(f[1]), float(f[2])]


def parse_kv_status(text, keys=STATUS_KEYS):
    """/proc/PID/status -> selected fields, kB converted to bytes."""
    return parse_meminfo(text, keys)


def parse_proc_io(text):
    out = {}
    for line in text.splitlines():
        k, _, v = line.partition(":")
        v = v.strip()
        if v.isdigit():
            out[k.strip()] = int(v)
    return out


def parse_pid_stat(text):
    """/proc/PID[/task/TID]/stat -> (comm, state, utime, stime, starttime, processor)."""
    lp = text.index("(")
    rp = text.rindex(")")
    comm = text[lp + 1:rp]
    rest = text[rp + 2:].split()
    # rest[0] is field 3 (state); field N is rest[N - 3].
    return (comm, rest[0], int(rest[11]), int(rest[12]), int(rest[19]), int(rest[36]))


def parse_snmp(text):
    """/proc/net/snmp or /proc/net/netstat -> {section: {field: int}}."""
    out = {}
    lines = text.splitlines()
    for i in range(0, len(lines) - 1, 2):
        hk, _, hnames = lines[i].partition(":")
        vk, _, vvals = lines[i + 1].partition(":")
        if hk != vk:
            continue
        sec = out.setdefault(hk.strip(), {})
        for n, v in zip(hnames.split(), vvals.split()):
            try:
                sec[n] = int(v)
            except ValueError:
                pass
    return out


def parse_softnet(text):
    """/proc/net/softnet_stat -> [[processed, dropped, time_squeeze], ...] (hex)."""
    out = []
    for line in text.splitlines():
        f = line.split()
        if len(f) >= 3:
            out.append([int(f[0], 16), int(f[1], 16), int(f[2], 16)])
    return out


def parse_vmstat(text):
    out = {}
    for line in text.splitlines():
        f = line.split()
        if len(f) == 2:
            try:
                out[f[0]] = int(f[1])
            except ValueError:
                pass
    return out


def parse_pressure(text):
    """/proc/pressure/X -> {"some": {"avg10": f, ..., "total": int}, "full": {...}}."""
    out = {}
    for line in text.splitlines():
        f = line.split()
        if not f:
            continue
        d = {}
        for kv in f[1:]:
            k, _, v = kv.partition("=")
            d[k] = int(v) if k == "total" else float(v)
        out[f[0]] = d
    return out


def parse_cpulist(text):
    """'0-3,8,10-11' -> [0,1,2,3,8,10,11]."""
    out = []
    for part in text.strip().split(","):
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return out


def parse_interrupts(text):
    """/proc/interrupts -> ({irq_key: total}, [per-CPU totals])."""
    lines = text.splitlines()
    if not lines:
        return {}, []
    ncpu = len(lines[0].split())
    per_cpu = [0] * ncpu
    totals = {}
    for line in lines[1:]:
        name, sep, rest = line.partition(":")
        if not sep:
            continue
        name = name.strip()
        f = rest.split()
        counts = []
        for tok in f[:ncpu]:
            if not tok.isdigit():
                break
            counts.append(int(tok))
        desc = f[len(counts):]
        key = name
        if name.isdigit() and desc:
            key = name + "/" + desc[-1]
        totals[key] = totals.get(key, 0) + sum(counts)
        if len(counts) == ncpu:
            for i, c in enumerate(counts):
                per_cpu[i] += c
    return totals, per_cpu


def parse_ethtool(text):
    """`ethtool -S IF` output -> {counter: int}."""
    out = {}
    for line in text.splitlines():
        k, sep, v = line.rpartition(":")
        k = k.strip()
        v = v.strip()
        if not sep or not k:
            continue
        try:
            out[k] = int(v)
        except ValueError:
            pass
    return out


def parse_chrony(text):
    """`chronyc -c tracking` CSV -> dict."""
    f = text.strip().split(",")
    out = {}
    for name, v in zip(CHRONY_FIELDS, f):
        try:
            out[name] = float(v) if name not in ("ref_id", "ref_name", "leap_status") else v
        except ValueError:
            out[name] = v
    return out


def _prom_float(s):
    s = s.strip()
    low = s.lower()
    if low in ("+inf", "inf"):
        return math.inf
    if low == "-inf":
        return -math.inf
    if low == "nan":
        return math.nan
    return float(s)


def _escape_label(v):
    return v.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def parse_labels(s, i=0):
    """Parse a label body starting just after '{'.  Returns ([(k, v)], index after '}')."""
    labels = []
    n = len(s)
    while True:
        while i < n and s[i] in " ,":
            i += 1
        if i >= n:
            raise ValueError("unterminated labels")
        if s[i] == "}":
            return labels, i + 1
        eq = s.index("=", i)
        key = s[i:eq].strip()
        i = eq + 1
        while i < n and s[i] == " ":
            i += 1
        if i >= n or s[i] != '"':
            raise ValueError("label value not quoted")
        i += 1
        end = s.find('"', i)
        if end < 0:
            raise ValueError("unterminated label value")
        if "\\" not in s[i:end]:  # fast path: no escapes
            labels.append((key, s[i:end]))
            i = end + 1
            continue
        buf = []
        while True:
            if i >= n:
                raise ValueError("unterminated label value")
            c = s[i]
            if c == "\\" and i + 1 < n:
                nxt = s[i + 1]
                buf.append("\n" if nxt == "n" else nxt)
                i += 2
            elif c == '"':
                i += 1
                break
            else:
                buf.append(c)
                i += 1
        labels.append((key, "".join(buf)))


def series_key(name, labels):
    if not labels:
        return name
    return name + "{" + ",".join('%s="%s"' % (k, _escape_label(v)) for k, v in labels) + "}"


def split_series(key):
    """Inverse of series_key: 'name{a="b"}' -> ('name', [('a', 'b')])."""
    br = key.find("{")
    if br < 0:
        return key, []
    labels, _ = parse_labels(key, br + 1)
    return key[:br], labels


def parse_prom(text):
    """Prometheus text exposition -> ({series_key: float}, {family: type})."""
    series, types = {}, {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            f = line.split(None, 3)
            if len(f) >= 4 and f[1] == "TYPE":
                types[f[2]] = f[3].strip()
            continue
        try:
            br = line.find("{")
            sp = line.find(" ")
            if br >= 0 and (sp < 0 or br < sp):
                rb = line.rfind("}")
                if "\\" not in line and "#" not in line and rb > br:
                    # Fast path (no escapes): keep the label text exactly as exposed.
                    series[line[:br].strip() + line[br:rb + 1]] = _prom_float(line[rb + 1:].split()[0])
                    continue
                name = line[:br].strip()
                labels, j = parse_labels(line, br + 1)
                rest = line[j:].split()
            else:
                f = line.split()
                name, labels, rest = f[0], [], f[1:]
            if not rest:
                continue
            series[series_key(name, labels)] = _prom_float(rest[0])
        except (ValueError, IndexError):
            continue
    return series, types


def watch_sums(series, families):
    """Sum all series of each exactly-named family."""
    out = {f: 0.0 for f in families}
    found = set()
    for k, v in series.items():
        br = k.find("{")
        name = k if br < 0 else k[:br]
        if name in out:
            out[name] += v
            found.add(name)
    return {f: out[f] for f in families if f in found}


# ---------------------------------------------------------------- sampler --

def read_text(path):
    # Raw os.open/os.read: noticeably cheaper than open() for thousands of tiny /proc files.
    fd = os.open(path, os.O_RDONLY)
    try:
        chunks = []
        while True:
            b = os.read(fd, 65536)
            if not b:
                break
            chunks.append(b)
    finally:
        os.close(fd)
    return b"".join(chunks).decode("utf-8", "replace")


def utc_stamp(t):
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime(t))


def gzip_file(path):
    tmp = path + ".gz.tmp"
    try:
        with open(path, "rb") as src, gzip.open(tmp, "wb", compresslevel=6) as dst:
            shutil.copyfileobj(src, dst, 1 << 20)
        os.rename(tmp, path + ".gz")
        os.unlink(path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass


class Writer:
    """JSONL writer with rotation, background gzip and once-per-pair error records."""

    def __init__(self, out_dir, rotate_secs, header):
        self.out_dir = out_dir
        self.rotate_secs = rotate_secs
        self.header = header
        self.lock = threading.Lock()
        self.errors_seen = set()
        self.gz_threads = []
        self.f = None
        self.path = None
        self.opened = 0.0
        self.bytes = 0
        os.makedirs(out_dir, exist_ok=True)
        self._open(time.time())

    def _open(self, t):
        path = os.path.join(self.out_dir, "samples-%s.jsonl" % utc_stamp(t))
        n = 1
        while os.path.exists(path) or os.path.exists(path + ".gz"):
            path = os.path.join(self.out_dir, "samples-%s-%d.jsonl" % (utc_stamp(t), n))
            n += 1
        self.f = open(path, "w")
        self.path = path
        self.opened = t
        hdr = dict(self.header)
        hdr["t"] = t
        self._write(hdr)

    def _write(self, rec):
        line = json.dumps(rec, separators=(",", ":")) + "\n"
        self.f.write(line)
        self.f.flush()
        self.bytes += len(line)

    def write(self, rec):
        with self.lock:
            if self.f is None:
                return  # closed; a straggling worker write after shutdown is dropped
            if rec["t"] - self.opened >= self.rotate_secs:
                old = self.path
                self.f.close()
                self._open(rec["t"])
                th = threading.Thread(target=gzip_file, args=(old,), daemon=True)
                th.start()
                self.gz_threads = [x for x in self.gz_threads if x.is_alive()] + [th]
            self._write(rec)

    def error(self, source, err):
        msg = "%s: %s" % (type(err).__name__, err) if isinstance(err, BaseException) else str(err)
        key = (source, msg)
        if key in self.errors_seen:
            return
        self.errors_seen.add(key)
        self.write({"t": time.time(), "tier": "error", "source": source, "error": msg})

    def close(self):
        for th in self.gz_threads:
            th.join()
        with self.lock:
            self.f.flush()
            self.f.close()
            self.f = None


def numa_cpus():
    out = {}
    base = "/sys/devices/system/node"
    try:
        names = sorted(os.listdir(base))
    except OSError:
        return out
    for name in names:
        if re.match(r"^node\d+$", name):
            try:
                out[name] = parse_cpulist(read_text(os.path.join(base, name, "cpulist")))
            except (OSError, ValueError):
                pass
    return out


def default_ifaces():
    out = []
    try:
        names = sorted(os.listdir("/sys/class/net"))
    except OSError:
        return out
    for name in names:
        if name != "lo" and os.path.exists(os.path.join("/sys/class/net", name, "device")):
            out.append(name)
    return out


def sample_fast(w, last):
    """One fast-tier record.  `last` (dict) receives mem/load for the latest snapshot."""
    rec = {"t": time.time(), "tier": "fast"}
    try:
        st = parse_proc_stat(read_text("/proc/stat"))
        rec["cpu"] = st["cpu"]
        rec["sys"] = st["sys"]
    except (OSError, ValueError, IndexError) as e:
        w.error("/proc/stat", e)
    try:
        rec["net"] = parse_net_dev(read_text("/proc/net/dev"))
    except (OSError, ValueError) as e:
        w.error("/proc/net/dev", e)
    try:
        rec["disk"] = parse_diskstats(read_text("/proc/diskstats"))
    except (OSError, ValueError) as e:
        w.error("/proc/diskstats", e)
    try:
        rec["mem"] = parse_meminfo(read_text("/proc/meminfo"))
        last["mem"] = rec["mem"]
    except (OSError, ValueError) as e:
        w.error("/proc/meminfo", e)
    try:
        rec["load"] = parse_loadavg(read_text("/proc/loadavg"))
        last["load"] = rec["load"]
    except (OSError, ValueError, IndexError) as e:
        w.error("/proc/loadavg", e)
    return rec


def list_cmdlines(skip_pid):
    """{pid: cmdline (NUL -> space)} for every readable process except skip_pid."""
    out = {}
    for name in os.listdir("/proc"):
        if not name.isdigit() or int(name) == skip_pid:
            continue
        cmd = read_cmdline(int(name))
        if cmd:
            out[int(name)] = cmd
    return out


def read_cmdline(pid):
    try:
        with open("/proc/%d/cmdline" % pid, "rb") as f:
            raw = f.read()
    except OSError:
        return None
    return raw.rstrip(b"\0").replace(b"\0", b" ").decode("utf-8", "replace")


class ProcTracker:
    """Tracks one named process by cmdline regex across restarts.

    Each slow tick first re-checks the tracked pid (same starttime and the
    cmdline still matches); only if it is gone does it scan every /proc/PID
    cmdline (a full scan costs ~10 ms per 1,000 processes).  When several
    processes match, the oldest (lowest starttime, then pid) wins.
    """

    def __init__(self, name, regex):
        self.name = name
        self.regex = re.compile(regex)
        self.pid = None
        self.start = None
        self.restarts = 0
        self.seen_any = False
        self.rss = None
        self.alive = False

    def still_valid(self):
        if self.pid is None:
            return False
        cmd = read_cmdline(self.pid)
        if not cmd or not self.regex.search(cmd):
            return False
        try:
            return parse_pid_stat(read_text("/proc/%d/stat" % self.pid))[4] == self.start
        except (OSError, ValueError, IndexError):
            return False

    def resolve(self, cmdlines):
        """Pick the pid from a full cmdline scan; returns cmdline if the pid changed, else None."""
        best = None
        for pid, cmd in cmdlines.items():
            if not self.regex.search(cmd):
                continue
            try:
                start = parse_pid_stat(read_text("/proc/%d/stat" % pid))[4]
            except (OSError, ValueError, IndexError):
                continue
            if best is None or (start, pid) < best[:2]:
                best = (start, pid, cmd)
        if best is None:
            self.pid = None
            self.start = None
            return None
        if (best[1], best[0]) == (self.pid, self.start):
            return None
        if self.seen_any:
            self.restarts += 1  # new pid, or the process came back after being gone
        self.seen_any = True
        self.start, self.pid = best[0], best[1]
        return best[2]

    def sample(self, get_cmdlines, w):
        """get_cmdlines() returns the (shared, lazily built) full cmdline map."""
        cmd = None
        if not self.still_valid():
            cmd = self.resolve(get_cmdlines())
        rec = {"pid": self.pid, "alive": False}
        self.alive = False
        self.rss = None
        if self.pid is None:
            return rec
        if cmd is not None:
            rec["cmdline"] = cmd
        base = "/proc/%d" % self.pid
        try:
            _, _, ut, stt, start, _ = parse_pid_stat(read_text(base + "/stat"))
            rec.update(alive=True, start=start, utime=ut, stime=stt)
        except (OSError, ValueError, IndexError) as e:
            w.error("proc:%s:stat" % self.name, e)
            return rec
        self.alive = True
        try:
            rec["status"] = parse_kv_status(read_text(base + "/status"))
            self.rss = rec["status"].get("VmRSS")
        except (OSError, ValueError) as e:
            w.error("proc:%s:status" % self.name, e)
        try:
            rec["io"] = parse_proc_io(read_text(base + "/io"))
        except (OSError, ValueError) as e:
            w.error("proc:%s:io" % self.name, type(e).__name__)
        threads = []
        try:
            tids = os.listdir(base + "/task")
        except OSError as e:
            w.error("proc:%s:task" % self.name, e)
            tids = []
        for tid in tids:
            try:
                comm, state, ut, stt, _, cpu = parse_pid_stat(read_text("%s/task/%s/stat" % (base, tid)))
            except (OSError, ValueError, IndexError):
                continue  # thread exited between listdir and read
            threads.append([int(tid), comm, state, ut, stt, cpu])
        rec["threads"] = threads
        return rec


def changed(old, new):
    """True if a series value changed (NaN -> NaN counts as unchanged)."""
    if old is None:
        return True
    return old != new and not (old != old and new != new)


class MetricsTracker:
    """Scrapes one Prometheus endpoint and delta-encodes snapshots."""

    def __init__(self, name, url, watch):
        self.name = name
        self.url = url
        self.watch = watch
        self.prev = None
        self.since_full = 0
        self.up = False
        self.watch_vals = {}

    def scrape(self, timeout=1.0):
        """Returns (text, error, ms); runs on a pool thread."""
        t0 = time.monotonic()
        try:
            with urllib.request.urlopen(self.url, timeout=timeout) as r:
                text = r.read().decode("utf-8", "replace")
            return text, None, (time.monotonic() - t0) * 1000.0
        except Exception as e:  # any failure just marks the endpoint down
            return None, "%s: %s" % (type(e).__name__, e), (time.monotonic() - t0) * 1000.0

    def record(self, text, err, ms):
        rec = {"up": False, "ms": round(ms, 2)}
        if err is not None:
            rec["error"] = err
            self.up = False
            self.prev = None
            return rec
        series, types = parse_prom(text)
        self.up = True
        self.watch_vals = watch_sums(series, self.watch)
        rec["up"] = True
        if self.prev is None or self.since_full >= FULL_EVERY - 1:
            rec["full"] = True
            rec["s"] = series
            rec["types"] = types
            self.since_full = 0
        else:
            prev = self.prev
            rec["full"] = False
            rec["s"] = {k: v for k, v in series.items() if changed(prev.get(k), v)}
            self.since_full += 1
        self.prev = series
        return rec


def run_cmd(argv, timeout):
    return subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout,
                          check=True).stdout.decode("utf-8", "replace")


def write_atomic_json(path, obj):
    tmp = "%s.tmp.%d" % (path, os.getpid())
    with open(tmp, "w") as f:
        json.dump(json_safe(obj), f, separators=(",", ":"))
        f.write("\n")
    os.replace(tmp, path)


def json_safe(obj):
    """Replace non-finite floats with None (for strict JSON consumers)."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    return obj


def nic_health(counters):
    return {k: v for k, v in counters.items() if "allowance" in k or "exceeded" in k}


class Sampler:
    def __init__(self, args, argv):
        self.args = args
        self.hostname = socket.gethostname()
        self.stop = threading.Event()
        self.last = {}
        self.procs = [ProcTracker(*split_kv(p, "--proc")) for p in args.proc]
        watch = args.watch if args.watch else DEFAULT_WATCH
        self.metrics = [MetricsTracker(n, u, watch) for n, u in
                        (split_kv(m, "--metrics") for m in args.metrics)]
        self.ifaces = args.iface if args.iface else default_ifaces()
        self.ethtool = shutil.which("ethtool") or (
            "/usr/sbin/ethtool" if os.access("/usr/sbin/ethtool", os.X_OK) else None)
        self.chronyc = shutil.which("chronyc")
        self.nic = {}
        self.slow_q = queue.Queue(maxsize=1)
        self.want_vslow = False
        self.slow_skipped = 0
        header = {
            "tier": "header", "version": VERSION, "hostname": self.hostname,
            "boot_id": self._boot_id(), "argv": argv, "ncpu": os.cpu_count(),
            "clk_tck": os.sysconf("SC_CLK_TCK"), "page_size": os.sysconf("SC_PAGE_SIZE"),
            "pid": os.getpid(), "kernel": " ".join(os.uname()), "interval": args.interval,
            "slow_every": args.slow_every, "vslow_every": args.vslow_every,
            "numa": numa_cpus(), "procs": {p.name: p.regex.pattern for p in self.procs},
            "metrics": {m.name: m.url for m in self.metrics}, "ifaces": self.ifaces,
        }
        self.w = Writer(args.out, args.rotate_secs, header)
        if self.ifaces and not self.ethtool:
            self.w.error("ethtool", "ethtool binary not found")

    @staticmethod
    def _boot_id():
        try:
            return read_text("/proc/sys/kernel/random/boot_id").strip()
        except OSError:
            return None

    def _read(self, path, parser, source=None):
        try:
            return parser(read_text(path))
        except (OSError, ValueError, IndexError) as e:
            self.w.error(source or path, e)
            return None

    # -- slow tier
    def sample_slow(self):
        rec = {"t": time.time(), "tier": "slow"}
        if self.slow_skipped:
            rec["skipped"] = self.slow_skipped
            self.slow_skipped = 0
        results = {}
        threads = []
        for m in self.metrics:
            th = threading.Thread(target=lambda m=m: results.__setitem__(m.name, m.scrape(1.0)),
                                  daemon=True)
            th.start()
            threads.append(th)
        if self.procs:
            cache = []

            def get_cmdlines():
                if not cache:
                    try:
                        cache.append(list_cmdlines(os.getpid()))
                    except OSError as e:
                        self.w.error("/proc", e)
                        cache.append({})
                return cache[0]

            rec["procs"] = {p.name: p.sample(get_cmdlines, self.w) for p in self.procs}
        if self.ethtool and self.ifaces:
            eth = {}
            for iface in self.ifaces:
                try:
                    eth[iface] = parse_ethtool(run_cmd([self.ethtool, "-S", iface], 2.0))
                except (OSError, subprocess.SubprocessError) as e:
                    self.w.error("ethtool:%s" % iface, e)
            rec["ethtool"] = eth
            self.nic = {k: nic_health(v) for k, v in eth.items()}
        for key, path, parser in (("snmp", "/proc/net/snmp", parse_snmp),
                                  ("netstat", "/proc/net/netstat", parse_snmp),
                                  ("softnet", "/proc/net/softnet_stat", parse_softnet),
                                  ("vmstat", "/proc/vmstat", parse_vmstat)):
            v = self._read(path, parser)
            if v is not None:
                rec[key] = v
        pressure = {}
        for res in ("cpu", "memory", "io"):
            if os.path.exists("/proc/pressure/" + res):
                v = self._read("/proc/pressure/" + res, parse_pressure)
                if v is not None:
                    pressure[res] = v
        if pressure:
            rec["pressure"] = pressure
        numastat = {}
        for node in self.w.header["numa"]:
            v = self._read("/sys/devices/system/node/%s/numastat" % node, parse_vmstat)
            if v is not None:
                numastat[node] = v
        if numastat:
            rec["numastat"] = numastat
        for th in threads:
            th.join(3.0)
        if self.metrics:
            mrec = {}
            for m in self.metrics:
                text, err, ms = results.get(m.name, (None, "scrape thread still running", 3000.0))
                mrec[m.name] = m.record(text, err, ms)
            rec["metrics"] = mrec
        tms = os.times()
        rec["self"] = {"utime": round(tms.user, 3), "stime": round(tms.system, 3)}
        return rec

    # -- very slow tier
    def sample_vslow(self):
        rec = {"t": time.time(), "tier": "vslow"}
        try:
            rec["irq"], rec["irq_cpu"] = parse_interrupts(read_text("/proc/interrupts"))
        except (OSError, ValueError) as e:
            self.w.error("/proc/interrupts", e)
        if self.chronyc:
            try:
                rec["chrony"] = parse_chrony(run_cmd([self.chronyc, "-c", "tracking"], 2.0))
            except (OSError, subprocess.SubprocessError) as e:
                self.w.error("chronyc", e)
        try:
            st = os.statvfs(self.args.out)
            rec["fs"] = {"path": os.path.abspath(self.args.out),
                         "total_bytes": st.f_blocks * st.f_frsize,
                         "free_bytes": st.f_bfree * st.f_frsize,
                         "avail_bytes": st.f_bavail * st.f_frsize}
        except OSError as e:
            self.w.error("statvfs", e)
        rec["boot_id"] = self._boot_id()
        return rec

    def latest(self):
        mem = self.last.get("mem", {})
        swap_used = None
        if "SwapTotal" in mem and "SwapFree" in mem:
            swap_used = mem["SwapTotal"] - mem["SwapFree"]
        load = self.last.get("load")
        return {
            "t": time.time(), "hostname": self.hostname,
            "procs": {p.name: {"pid": p.pid if p.alive else None, "alive": p.alive,
                               "rss_bytes": p.rss, "restarts": p.restarts} for p in self.procs},
            "metrics": {m.name: {"up": m.up, "watch": m.watch_vals if m.up else {}}
                        for m in self.metrics},
            "nic": self.nic,
            "mem": {"available_bytes": mem.get("MemAvailable"), "swap_used_bytes": swap_used},
            "load1": load[0] if load else None,
        }

    def slow_worker(self):
        while True:
            item = self.slow_q.get()
            if item is None:
                return
            try:
                self.w.write(self.sample_slow())
                if self.want_vslow:
                    self.want_vslow = False
                    self.w.write(self.sample_vslow())
                if self.args.latest:
                    write_atomic_json(self.args.latest, self.latest())
            except Exception as e:  # never let one bad tick kill the worker
                self.w.error("slow-worker", e)

    def run(self):
        a = self.args
        iv = a.interval
        worker = threading.Thread(target=self.slow_worker, daemon=True)
        worker.start()
        start = time.monotonic()
        k = 0
        next_slow = 0
        next_vslow = 0
        while not self.stop.is_set():
            deadline = start + k * iv
            if a.duration is not None and deadline - start > a.duration + 1e-9:
                break
            now = time.monotonic()
            if now < deadline and self.stop.wait(deadline - now):
                break
            now = time.monotonic()
            late = now - deadline
            behind = int(late // iv)
            rec = sample_fast(self.w, self.last)
            if behind > 0 or late > 0.1 * iv:
                rec["late"] = round(late, 4)
            if behind > 0:
                rec["skipped_ticks"] = behind
                k += behind
            self.w.write(rec)
            if k >= next_vslow:
                self.want_vslow = True
                next_vslow = k + a.vslow_every
            if k >= next_slow:
                next_slow = k + a.slow_every
                try:
                    self.slow_q.put_nowait(True)
                except queue.Full:
                    self.slow_skipped += 1
            k += 1
        try:
            self.slow_q.put(None, timeout=5.0)
        except queue.Full:
            pass
        worker.join(5.0)
        self.w.close()
        return 0


def split_kv(s, flag):
    name, sep, val = s.partition("=")
    if not sep or not name or not val:
        raise SystemExit("%s expects NAME=VALUE, got %r" % (flag, s))
    return name, val


def cmd_run(args, argv):
    if args.interval <= 0 or args.slow_every < 1 or args.vslow_every < 1:
        raise SystemExit("--interval must be > 0 and tier multipliers >= 1")
    s = Sampler(args, argv)

    def on_signal(signum, frame):
        s.stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    return s.run()


# -------------------------------------------------------------- summarize --

def sample_files(d):
    """samples-*.jsonl[.gz] in name (= time) order; a plain file whose .gz exists is skipped."""
    names = os.listdir(d)
    have = set(names)
    out = []
    for n in names:
        if not n.startswith("samples-"):
            continue
        if n.endswith(".jsonl.gz") or (n.endswith(".jsonl") and n + ".gz" not in have):
            out.append(n)
    out.sort(key=lambda n: n[:-3] if n.endswith(".gz") else n)
    return [os.path.join(d, n) for n in out]


def read_records(path, stats):
    """Yield parsed records; bad/truncated lines are counted in stats["bad_lines"]."""
    opener = gzip.open if path.endswith(".gz") else open
    try:
        with opener(path, "rt", errors="replace") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except ValueError:
                    stats["bad_lines"] += 1
                    continue
                if isinstance(rec, dict) and "t" in rec:
                    yield rec
    except (EOFError, OSError, gzip.BadGzipFile):
        stats["bad_lines"] += 1  # truncated gzip stream


def cdelta(a, b):
    """Counter delta; a decrease is a reset and counts the post-reset value."""
    return b - a if b >= a else b


def strip_digits(comm):
    g = comm.rstrip("0123456789")
    g2 = g.rstrip("-_:/.# ")
    return g2 or g or comm


def histogram_quantile(q, buckets):
    """buckets: [(le, cumulative_count)].  Prometheus-style linear interpolation."""
    b = sorted(buckets)
    if not b:
        return None
    cum = []
    run = 0.0
    for le, c in b:
        run = max(run, c)
        cum.append(run)
    total = cum[-1]
    if total <= 0:
        return None
    rank = q * total
    for i, (le, _) in enumerate(b):
        if cum[i] >= rank:
            if math.isinf(le):
                return b[i - 1][0] if i > 0 else None
            lo = b[i - 1][0] if i > 0 else min(0.0, le)
            prev = cum[i - 1] if i > 0 else 0.0
            if cum[i] == prev:
                return le
            return lo + (le - lo) * (rank - prev) / (cum[i] - prev)
    return b[-1][0]


class SeriesStats:
    __slots__ = ("first", "last", "min", "max", "delta", "resets")

    def __init__(self, v):
        self.first = self.last = self.min = self.max = v
        self.delta = 0.0
        self.resets = 0

    def add(self, v):
        if v != v:  # NaN: keep as last, skip arithmetic
            self.last = v
            return
        prev = self.last
        if prev == prev:
            if v >= prev:
                self.delta += v - prev
            else:
                self.delta += v
                self.resets += 1
        self.last = v
        if not (self.min <= v):
            self.min = v
        if not (self.max >= v):
            self.max = v


class EndpointAcc:
    """Reconstructs delta-encoded scrapes and accumulates in-window series stats."""

    def __init__(self):
        self.state = {}
        self.types = {}
        self.stats = {}
        self.t0 = None
        self.t1 = None
        self.scrapes = 0
        self.down = 0

    def add(self, t, m, in_window):
        if not m.get("up"):
            if in_window:
                self.down += 1
            return
        s = m.get("s", {})
        if m.get("full"):
            self.state = dict(s)
            if "types" in m:
                self.types.update(m["types"])
        else:
            self.state.update(s)
        if not in_window:
            return
        self.scrapes += 1
        if self.t0 is None:
            self.t0 = t
            for k, v in self.state.items():
                self.stats[k] = SeriesStats(v)
        else:
            stats = self.stats
            for k, v in s.items():
                st = stats.get(k)
                if st is None:
                    stats[k] = SeriesStats(v)
                elif st.last != v:
                    st.add(v)
        self.t1 = t

    def family_type(self, name):
        if name in self.types:
            return self.types[name]
        for suf in ("_bucket", "_sum", "_count", "_total", "_created"):
            if name.endswith(suf) and self.types.get(name[:-len(suf)]):
                return self.types[name[:-len(suf)]]
        return None

    def is_counter(self, key, st):
        name = key.split("{", 1)[0]
        typ = self.family_type(name)
        if typ in ("counter", "histogram", "summary"):
            if typ == "summary" and not name.endswith(("_sum", "_count")):
                return False  # summary quantile series are gauges
            return True
        if typ == "gauge":
            return False
        if name.endswith(("_total", "_count", "_sum", "_bucket")):
            return True
        return st.resets == 0

    def report(self):
        secs = (self.t1 - self.t0) if self.t0 is not None else 0.0
        counters, gauges, hist, sums = {}, {}, {}, {}
        for key, st in self.stats.items():
            if self.is_counter(key, st):
                counters[key] = {"delta": st.delta, "rate": st.delta / secs if secs > 0 else None,
                                 "resets": st.resets}
                name, labels = split_series(key)
                if name.endswith("_bucket"):
                    le = [v for k, v in labels if k == "le"]
                    if le:
                        try:
                            lev = _prom_float(le[0])
                        except ValueError:
                            continue
                        hk = series_key(name[:-7], [(k, v) for k, v in labels if k != "le"])
                        hist.setdefault(hk, []).append((lev, st.delta))
                elif name.endswith(("_sum", "_count")):
                    base = name.rsplit("_", 1)[0]
                    sums.setdefault(series_key(base, labels), {})[name.rsplit("_", 1)[1]] = st.delta
            else:
                gauges[key] = {"first": st.first, "last": st.last, "min": st.min, "max": st.max}
        hists = {}
        for hk, buckets in hist.items():
            total = max((c for _, c in buckets), default=0.0)
            sc = sums.get(hk, {})
            mean = sc["sum"] / sc["count"] if sc.get("count") else None
            hists[hk] = {"count": total, "mean": mean,
                         "p50": histogram_quantile(0.5, buckets),
                         "p90": histogram_quantile(0.9, buckets),
                         "p99": histogram_quantile(0.99, buckets),
                         "p999": histogram_quantile(0.999, buckets)}
        return {"seconds": secs, "scrapes": self.scrapes, "down_scrapes": self.down,
                "counters": counters, "gauges": gauges, "histograms": hists}


class PairAcc:
    """Sums counter deltas over consecutive samples per (group, index) with peak rates."""

    def __init__(self):
        self.prev = {}
        self.sum = {}
        self.secs = {}
        self.peak = {}

    def add(self, t, group, vals, peak_idx=()):
        p = self.prev.get(group)
        self.prev[group] = (t, vals)
        if p is None:
            return
        pt, pv = p
        dt = t - pt
        if dt <= 0:
            return
        if isinstance(vals, dict):
            acc = self.sum.setdefault(group, {})
            for k, v in vals.items():
                if k in pv:
                    acc[k] = acc.get(k, 0) + cdelta(pv[k], v)
        else:
            acc = self.sum.setdefault(group, [])
            if len(acc) < len(vals):
                acc.extend([0] * (len(vals) - len(acc)))
            for i, (a, b) in enumerate(zip(pv, vals)):
                acc[i] += cdelta(a, b)
            pk = self.peak.setdefault(group, {})
            for i in peak_idx:
                if i < len(vals) and i < len(pv):
                    r = cdelta(pv[i], vals[i]) / dt
                    if r > pk.get(i, 0):
                        pk[i] = r
        self.secs[group] = self.secs.get(group, 0.0) + dt


class ProcAcc:
    def __init__(self):
        self.prev = None
        self.t_prev = None
        self.jiffies = 0
        self.secs = 0.0
        self.vol = self.nonvol = self.rbytes = self.wbytes = 0
        self.rss_min = self.rss_max = None
        self.pids = []
        self.pid_changes = 0
        self.by_comm = {}
        self.by_group = {}

    def add(self, t, r):
        pid = r.get("pid")
        if pid is not None and r.get("alive"):
            if self.pids and self.pids[-1] != pid:
                self.pid_changes += 1
            if not self.pids or self.pids[-1] != pid:
                self.pids.append(pid)
        if not r.get("alive"):
            self.prev = None
            return
        rss = r.get("status", {}).get("VmRSS")
        if rss is not None:
            self.rss_min = rss if self.rss_min is None else min(self.rss_min, rss)
            self.rss_max = rss if self.rss_max is None else max(self.rss_max, rss)
        p = self.prev
        if p is not None and p.get("pid") == pid and p.get("start") == r.get("start"):
            self.secs += t - self.t_prev
            self.jiffies += cdelta(p.get("utime", 0) + p.get("stime", 0),
                                   r.get("utime", 0) + r.get("stime", 0))
            ps, cs = p.get("status", {}), r.get("status", {})
            if "voluntary_ctxt_switches" in ps and "voluntary_ctxt_switches" in cs:
                self.vol += cdelta(ps["voluntary_ctxt_switches"], cs["voluntary_ctxt_switches"])
            if "nonvoluntary_ctxt_switches" in ps and "nonvoluntary_ctxt_switches" in cs:
                self.nonvol += cdelta(ps["nonvoluntary_ctxt_switches"],
                                      cs["nonvoluntary_ctxt_switches"])
            pio, cio = p.get("io", {}), r.get("io", {})
            if "read_bytes" in pio and "read_bytes" in cio:
                self.rbytes += cdelta(pio["read_bytes"], cio["read_bytes"])
            if "write_bytes" in pio and "write_bytes" in cio:
                self.wbytes += cdelta(pio["write_bytes"], cio["write_bytes"])
            before = {th[0]: th[3] + th[4] for th in p.get("threads", [])}
            for th in r.get("threads", []):
                # A tid absent from the previous sample was born in between: count all of it.
                d = th[3] + th[4] - before.get(th[0], 0)
                if d > 0:
                    self.by_comm[th[1]] = self.by_comm.get(th[1], 0) + d
                    g = strip_digits(th[1])
                    self.by_group[g] = self.by_group.get(g, 0) + d
        self.prev = r
        self.t_prev = t

    def report(self, clk):
        s = self.secs

        def cores(j):
            return round(j / clk / s, 4) if s > 0 else None

        def rate(x, scale=1.0):
            return x / scale / s if s > 0 else None

        return {"cores": cores(self.jiffies), "seconds": s, "pid_changes": self.pid_changes,
                "pids": self.pids, "rss_min_bytes": self.rss_min, "rss_max_bytes": self.rss_max,
                "vol_ctxt_per_s": rate(self.vol), "nonvol_ctxt_per_s": rate(self.nonvol),
                "read_mbps": rate(self.rbytes, 1e6), "write_mbps": rate(self.wbytes, 1e6),
                "threads_by_comm": dict(sorted(((k, cores(v)) for k, v in self.by_comm.items()),
                                               key=lambda kv: -(kv[1] or 0))),
                "threads_by_group": dict(sorted(((k, cores(v)) for k, v in self.by_group.items()),
                                                key=lambda kv: -(kv[1] or 0)))}


TCP_FIELDS = (("Tcp", "RetransSegs"), ("Tcp", "InSegs"), ("Tcp", "OutSegs"),
              ("TcpExt", "TCPLostRetransmit"), ("TcpExt", "ListenDrops"),
              ("TcpExt", "ListenOverflows"), ("TcpExt", "TCPTimeouts"))


class Summary:
    def __init__(self, t_from=None, t_to=None):
        self.t_from = t_from
        self.t_to = t_to
        self.stats = {"bad_lines": 0}
        self.files = 0
        self.records = 0
        self.hosts = []
        self.header = None
        self.errors = []
        self.first_t = self.last_t = None
        self.cpu_first = self.cpu_prev = None
        self.cpu_tot = {}
        self.sys = PairAcc()
        self.net = PairAcc()
        self.disk = PairAcc()
        self.mem_avail_min = self.swap_max = self.huge_total = self.huge_free_min = None
        self.tcp = PairAcc()
        self.softnet = PairAcc()
        self.eth = PairAcc()
        self.conntrack_min = {}
        self.irq = PairAcc()
        self.procs = {}
        self.endpoints = {}

    def in_window(self, t):
        return (self.t_from is None or t >= self.t_from) and (self.t_to is None or t <= self.t_to)

    def load_dir(self, d):
        for path in sample_files(d):
            self.files += 1
            for rec in read_records(path, self.stats):
                self.add(rec)
        self._close_cpu_segment()

    def add(self, rec):
        tier = rec.get("tier")
        t = rec["t"]
        if tier == "header":
            if self.header is None:
                self.header = rec
            if rec.get("hostname") not in self.hosts:
                self.hosts.append(rec.get("hostname"))
            return
        if tier == "slow" and "metrics" in rec:
            inw = self.in_window(t)
            for name, m in rec["metrics"].items():
                self.endpoints.setdefault(name, EndpointAcc()).add(t, m, inw)
        if not self.in_window(t):
            return
        self.records += 1
        self.first_t = t if self.first_t is None else min(self.first_t, t)
        self.last_t = t if self.last_t is None else max(self.last_t, t)
        if tier == "fast":
            self.add_fast(t, rec)
        elif tier == "slow":
            self.add_slow(t, rec)
        elif tier == "vslow" and "irq" in rec:
            self.irq.add(t, "irq", rec["irq"])
        elif tier == "error":
            e = [rec.get("source"), rec.get("error")]
            if e not in self.errors:
                self.errors.append(e)

    def _close_cpu_segment(self):
        a, b = self.cpu_first, self.cpu_prev
        if a is None or b is None:
            return
        for name, vb in b.items():
            va = a.get(name)
            if va is None:
                continue
            acc = self.cpu_tot.setdefault(name, [0] * 8)
            for i in range(min(8, len(va), len(vb))):
                acc[i] += max(0, vb[i] - va[i])
        self.cpu_first = self.cpu_prev = None

    def add_fast(self, t, rec):
        c = rec.get("cpu")
        if c and "all" in c:
            if self.cpu_prev is not None and sum(c["all"]) < sum(self.cpu_prev["all"]):
                self._close_cpu_segment()  # reboot: counters restarted
            if self.cpu_first is None:
                self.cpu_first = c
            self.cpu_prev = c
        sysd = rec.get("sys")
        if sysd:
            self.sys.add(t, "sys", sysd)
        for iface, v in rec.get("net", {}).items():
            self.net.add(t, iface, v, peak_idx=(0, 1, 4, 5))
        for dev, v in rec.get("disk", {}).items():
            self.disk.add(t, dev, v)
        mem = rec.get("mem")
        if mem:
            if "MemAvailable" in mem:
                self.mem_avail_min = _min(self.mem_avail_min, mem["MemAvailable"])
            if "SwapTotal" in mem and "SwapFree" in mem:
                self.swap_max = _max(self.swap_max, mem["SwapTotal"] - mem["SwapFree"])
            if "HugePages_Total" in mem:
                self.huge_total = mem["HugePages_Total"]
            if "HugePages_Free" in mem:
                self.huge_free_min = _min(self.huge_free_min, mem["HugePages_Free"])

    def add_slow(self, t, rec):
        snmp = dict(rec.get("snmp", {}))
        snmp.update(rec.get("netstat", {}))
        tcp = {f: snmp[s][f] for s, f in TCP_FIELDS if f in snmp.get(s, {})}
        if tcp:
            self.tcp.add(t, "tcp", tcp)
        sn = rec.get("softnet")
        if sn:
            self.softnet.add(t, "softnet", [sum(r[i] for r in sn) for i in range(3)])
        for iface, ctr in rec.get("ethtool", {}).items():
            self.eth.add(t, iface, ctr)
            if "conntrack_allowance_available" in ctr:
                self.conntrack_min[iface] = _min(self.conntrack_min.get(iface),
                                                 ctr["conntrack_allowance_available"])
        for name, r in rec.get("procs", {}).items():
            self.procs.setdefault(name, ProcAcc()).add(t, r)


def _min(a, b):
    return b if a is None else min(a, b)


def _max(a, b):
    return b if a is None else max(a, b)


def _pct(part, total):
    return round(100.0 * part / total, 3) if total > 0 else None


def cpu_report(s):
    out = {"ncpu": (s.header or {}).get("ncpu")}
    tot = s.cpu_tot
    a = tot.get("all")
    if a:
        total = sum(a)
        out.update(busy_pct=_pct(total - a[3] - a[4], total), iowait_pct=_pct(a[4], total),
                   steal_pct=_pct(a[7], total), softirq_pct=_pct(a[6], total),
                   irq_pct=_pct(a[5], total))
    per = {}
    for name, v in tot.items():
        if name != "all":
            total = sum(v)
            per[name] = _pct(total - v[3] - v[4], total)
    out["per_cpu_busy_pct"] = dict(sorted(per.items(), key=lambda kv: int(kv[0])))
    out["top10"] = sorted(([k, v] for k, v in per.items() if v is not None),
                          key=lambda kv: -kv[1])[:10]
    numa = {}
    for node, cpus in ((s.header or {}).get("numa") or {}).items():
        busy = total = 0
        for c in cpus:
            v = tot.get(str(c))
            if v:
                busy += sum(v) - v[3] - v[4]
                total += sum(v)
        numa[node] = _pct(busy, total)
    out["numa"] = numa
    secs = s.sys.secs.get("sys", 0.0)
    sd = s.sys.sum.get("sys", {})
    if secs > 0:
        out["ctxt_per_s"] = sd.get("ctxt", 0) / secs
        out["intr_per_s"] = sd.get("intr", 0) / secs
    return out


def net_report(s):
    out = {}
    for iface, v in s.net.sum.items():
        secs = s.net.secs.get(iface, 0.0)
        if secs <= 0:
            continue
        pk = s.net.peak.get(iface, {})
        out[iface] = {"rx_gbps": v[0] * 8 / 1e9 / secs, "tx_gbps": v[4] * 8 / 1e9 / secs,
                      "rx_kpps": v[1] / 1e3 / secs, "tx_kpps": v[5] / 1e3 / secs,
                      "peak_rx_gbps": pk.get(0, 0) * 8 / 1e9, "peak_tx_gbps": pk.get(4, 0) * 8 / 1e9,
                      "peak_rx_kpps": pk.get(1, 0) / 1e3, "peak_tx_kpps": pk.get(5, 0) / 1e3,
                      "rx_errs": v[2], "rx_drop": v[3], "tx_errs": v[6], "tx_drop": v[7]}
    return out


def ena_report(s):
    out = {}
    for iface, ctr in s.eth.sum.items():
        d = {k: v for k, v in ctr.items() if "exceeded" in k}
        if iface in s.conntrack_min:
            d["conntrack_allowance_available_min"] = s.conntrack_min[iface]
        out[iface] = d
    return out


def tcp_report(s):
    d = s.tcp.sum.get("tcp", {})
    secs = s.tcp.secs.get("tcp", 0.0)
    out = {f: d.get(f) for f in ("RetransSegs", "TCPLostRetransmit", "ListenDrops",
                                 "ListenOverflows", "TCPTimeouts")}
    out["InSegs_per_s"] = d["InSegs"] / secs if secs > 0 and "InSegs" in d else None
    out["OutSegs_per_s"] = d["OutSegs"] / secs if secs > 0 and "OutSegs" in d else None
    out["retrans_pct"] = (_pct(d.get("RetransSegs", 0), d["OutSegs"])
                          if d.get("OutSegs") else None)
    return out


def disk_report(s):
    out = {}
    for dev, v in s.disk.sum.items():
        secs = s.disk.secs.get(dev, 0.0)
        if secs <= 0:
            continue
        out[dev] = {"read_mbps": v[2] * 512 / 1e6 / secs, "write_mbps": v[6] * 512 / 1e6 / secs,
                    "read_iops": v[0] / secs, "write_iops": v[4] / secs,
                    "avg_queue": v[10] / 1000.0 / secs, "util_pct": min(100.0, v[9] / 10.0 / secs)}
    return out


def build_report(s):
    clk = (s.header or {}).get("clk_tck") or 100
    sn = s.softnet.sum.get("softnet", [0, 0, 0])
    irq = s.irq.sum.get("irq", {})
    return {
        "window": {"from": s.first_t, "to": s.last_t,
                   "seconds": (s.last_t - s.first_t) if s.first_t is not None else 0.0,
                   "files": s.files, "records": s.records, "bad_lines": s.stats["bad_lines"],
                   "hosts": s.hosts},
        "cpu": cpu_report(s),
        "net": net_report(s),
        "ena": ena_report(s),
        "tcp": tcp_report(s),
        "disk": disk_report(s),
        "mem": {"min_available_bytes": s.mem_avail_min, "max_swap_used_bytes": s.swap_max,
                "hugepages_total": s.huge_total, "hugepages_free_min": s.huge_free_min},
        "procs": {n: p.report(clk) for n, p in s.procs.items()},
        "metrics": {n: e.report() for n, e in s.endpoints.items()},
        "softnet": {"processed": sn[0], "dropped": sn[1], "squeezed": sn[2]},
        "irq_top": sorted(([k, v] for k, v in irq.items() if v), key=lambda kv: -kv[1])[:20],
        "errors": s.errors,
    }


def summarize_dir(d, t_from=None, t_to=None):
    s = Summary(t_from, t_to)
    s.load_dir(d)
    return build_report(s)


def _f(v, fmt="%.2f"):
    if v is None:
        return "-"
    if isinstance(v, float):
        return fmt % v
    return str(v)


def format_text(r, max_series=40):
    L = []
    w = r["window"]
    L.append("window %s .. %s (%.0f s), %d files, %d records, %d bad lines, hosts %s" % (
        _f(w["from"], "%.1f"), _f(w["to"], "%.1f"), w["seconds"], w["files"], w["records"],
        w["bad_lines"], ",".join(str(h) for h in w["hosts"])))
    c = r["cpu"]
    L.append("CPU: busy %s%% iowait %s%% steal %s%% softirq %s%% irq %s%%  ctxt/s %s intr/s %s" % (
        _f(c.get("busy_pct")), _f(c.get("iowait_pct")), _f(c.get("steal_pct")),
        _f(c.get("softirq_pct")), _f(c.get("irq_pct")), _f(c.get("ctxt_per_s"), "%.0f"),
        _f(c.get("intr_per_s"), "%.0f")))
    if c.get("numa"):
        L.append("  numa: " + "  ".join("%s %s%%" % (k, _f(v)) for k, v in c["numa"].items()))
    if c.get("top10"):
        L.append("  top10: " + "  ".join("cpu%s %.1f%%" % (k, v) for k, v in c["top10"]))
    for iface, n in r["net"].items():
        if not (n["rx_kpps"] or n["tx_kpps"] or n["rx_drop"] or n["tx_drop"] or n["rx_errs"]
                or n["tx_errs"]):
            continue  # idle interface (still present in --json)
        L.append("NET %s: rx %.3f Gb/s %.1f kpps (peak %.3f / %.1f)  tx %.3f Gb/s %.1f kpps "
                 "(peak %.3f / %.1f)  drops rx %d tx %d errs rx %d tx %d" % (
                     iface, n["rx_gbps"], n["rx_kpps"], n["peak_rx_gbps"], n["peak_rx_kpps"],
                     n["tx_gbps"], n["tx_kpps"], n["peak_tx_gbps"], n["peak_tx_kpps"],
                     n["rx_drop"], n["tx_drop"], n["rx_errs"], n["tx_errs"]))
    for iface, d in r["ena"].items():
        if d:
            L.append("ENA %s: " % iface + "  ".join("%s %s" % (k, v) for k, v in d.items()))
    t = r["tcp"]
    L.append("TCP: " + "  ".join("%s %s" % (k, _f(v)) for k, v in t.items()))
    for dev, d in r["disk"].items():
        L.append("DISK %s: read %.1f MB/s %.0f iops  write %.1f MB/s %.0f iops  queue %.2f util "
                 "%.1f%%" % (dev, d["read_mbps"], d["read_iops"], d["write_mbps"],
                             d["write_iops"], d["avg_queue"], d["util_pct"]))
    m = r["mem"]
    L.append("MEM: min available %s B, max swap used %s B, hugepages %s (min free %s)" % (
        _f(m["min_available_bytes"]), _f(m["max_swap_used_bytes"]), _f(m["hugepages_total"]),
        _f(m["hugepages_free_min"])))
    sn = r["softnet"]
    L.append("SOFTNET: processed %d dropped %d squeezed %d" % (
        sn["processed"], sn["dropped"], sn["squeezed"]))
    for name, p in r["procs"].items():
        L.append("PROC %s: cores %s pids %s (changes %d) rss %s..%s B vol/s %s nonvol/s %s "
                 "read %s MB/s write %s MB/s" % (
                     name, _f(p["cores"], "%.3f"), p["pids"], p["pid_changes"],
                     _f(p["rss_min_bytes"]), _f(p["rss_max_bytes"]), _f(p["vol_ctxt_per_s"], "%.0f"),
                     _f(p["nonvol_ctxt_per_s"], "%.0f"), _f(p["read_mbps"]), _f(p["write_mbps"])))
        groups = list(p["threads_by_group"].items())[:15]
        if groups:
            L.append("  thread groups: " + "  ".join("%s=%s" % (k, _f(v, "%.3f")) for k, v in groups))
    for name, e in r["metrics"].items():
        L.append("METRICS %s: %d scrapes (%d down) over %.0f s, %d counters, %d gauges, "
                 "%d histograms" % (name, e["scrapes"], e["down_scrapes"], e["seconds"],
                                    len(e["counters"]), len(e["gauges"]), len(e["histograms"])))
        nz = [(k, v) for k, v in e["counters"].items()
              if v["delta"] and not k.split("{", 1)[0].endswith("_bucket")]
        nz.sort(key=lambda kv: -(kv[1]["rate"] or 0))
        for k, v in nz[:max_series]:
            L.append("  %s delta %s rate %s/s%s" % (k, _f(v["delta"], "%.6g"), _f(v["rate"], "%.6g"),
                                                    " resets %d" % v["resets"] if v["resets"] else ""))
        if len(nz) > max_series:
            L.append("  ... %d more nonzero counters (use --json)" % (len(nz) - max_series))
        for k, h in list(e["histograms"].items())[:max_series]:
            if h["count"]:
                L.append("  hist %s n=%s mean %s p50 %s p90 %s p99 %s p99.9 %s" % (
                    k, _f(h["count"], "%.0f"), _f(h["mean"], "%.6g"), _f(h["p50"], "%.6g"),
                    _f(h["p90"], "%.6g"), _f(h["p99"], "%.6g"), _f(h["p999"], "%.6g")))
        ch = [(k, g) for k, g in e["gauges"].items() if g["min"] != g["max"]]
        for k, g in ch[:max_series]:
            L.append("  gauge %s first %s last %s min %s max %s" % (
                k, _f(g["first"], "%.6g"), _f(g["last"], "%.6g"), _f(g["min"], "%.6g"),
                _f(g["max"], "%.6g")))
    if r["irq_top"]:
        L.append("IRQ top: " + "  ".join("%s=%d" % (k, v) for k, v in r["irq_top"][:10]))
    for src, err in r["errors"]:
        L.append("ERROR %s: %s" % (src, err))
    return "\n".join(L) + "\n"


def cmd_summarize(args):
    if not os.path.isdir(args.dir):
        raise SystemExit("summarize: %s is not a directory" % args.dir)
    r = summarize_dir(args.dir, args.t_from, args.t_to)
    if args.json:
        json.dump(json_safe(r), sys.stdout, separators=(",", ":"), allow_nan=False)
        sys.stdout.write("\n")
    else:
        sys.stdout.write(format_text(r))
    return 0


def build_parser():
    p = argparse.ArgumentParser(prog="hostmon.py", description="vals fleet host sampler")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="sample until SIGTERM/SIGINT or --duration")
    r.add_argument("--out", required=True)
    r.add_argument("--interval", type=float, default=1.0)
    r.add_argument("--slow-every", type=int, default=5)
    r.add_argument("--vslow-every", type=int, default=60)
    r.add_argument("--metrics", action="append", default=[], metavar="NAME=URL")
    r.add_argument("--proc", action="append", default=[], metavar="NAME=REGEX")
    r.add_argument("--iface", action="append", default=[])
    r.add_argument("--latest")
    r.add_argument("--watch", action="append", default=[], metavar="FAMILY")
    r.add_argument("--duration", type=float)
    r.add_argument("--rotate-secs", type=float, default=3600.0)
    s = sub.add_parser("summarize", help="summarize a samples directory")
    s.add_argument("dir")
    s.add_argument("--from", dest="t_from", type=float)
    s.add_argument("--to", dest="t_to", type=float)
    s.add_argument("--json", action="store_true")
    return p


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    args = build_parser().parse_args(argv)
    if args.cmd == "run":
        return cmd_run(args, [sys.argv[0]] + list(argv))
    return cmd_summarize(args)


if __name__ == "__main__":
    sys.exit(main())
