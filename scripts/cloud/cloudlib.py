"""Shared helpers for the vals cloud harness (plans/cloud-aws-runbook.md).

Pure functions here (config generation, cost math, output parsing, live-check
rules) are unit-tested by scripts/test_cloud_harness.py; the process helpers
run commands on fleet hosts over ssh, or locally for the dry-run fleet.
"""
import datetime
import json
import os
import re
import shlex
import subprocess
import threading
import time

LOCAL = "local"
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# On-demand / spot USD per hour, Linux, from instances.vantage.sh (us-east-1) on
# 2026-11-02; on-demand i4i.metal, c6in.16xlarge and c6i.8xlarge are identical in us-east-2
# (AWS pricing API, same day). Spot moves; `fleet.py plan` prints these with their date.
PRICES = {
    "i4i.metal": (10.982, 2.644),
    "i3en.metal": (10.848, 2.507),
    "i7ie.metal-24xl": (12.475, 2.927),
    "i7ie.metal-48xl": (24.950, 6.649),
    "c6in.16xlarge": (3.629, 0.992),
    "c6in.32xlarge": (7.258, 1.984),
    "c6i.2xlarge": (0.340, 0.150),
    "c6i.8xlarge": (1.360, 0.600),
    "c7i.8xlarge": (1.428, 0.800),
    "c7i.large": (0.0893, 0.040),
}
PRICES_DATE = "2026-11-02"
VCPUS = {
    "i4i.metal": 128, "i3en.metal": 96, "i7ie.metal-24xl": 96, "i7ie.metal-48xl": 192,
    "c6in.16xlarge": 64, "c6in.32xlarge": 128, "c6i.2xlarge": 8, "c6i.8xlarge": 32, "c7i.8xlarge": 32,
    "c7i.large": 2,
}

# Families that must never move: any increase is a correctness/integrity event.
CRITICAL_FAMILIES = (
    "vals_memtable_integrity_failures_total",
    "vals_encode_integrity_failures_total",
    "vals_store_corrupt_reads_total",
    "vals_run_build_verify_failures_total",
    "vals_integrity_fail_fast_total",
    "vals_watermark_persist_errors",
    "vals_snapshot_errors",
    "vals_compaction_errors",
    "vals_control_incarnation_rejected",
    "vals_pd_drive_errors",
)
WARN_FAMILIES = (
    "vals_server_errors",
    "vals_region_exhausted_rejections",
    "vals_compaction_plan_failures",
    "vals_transport_request_rejections",
)
NOTE_FAMILIES = ("vals_busy_rejections", "vals_ram_busy_rejections", "vals_ae_sync_errors")


ISA_FLAG = re.compile(r"^(sse|ssse3|avx|fma|f16c|bmi|adx|aes|pclmulqdq|sha_ni|vaes|vpclmulqdq|gfni|movbe|"
                      r"popcnt|abm|xsave|rdrand|rdseed|clflushopt|clwb|3dnowprefetch|amx|waitpkg|movdir|"
                      r"serialize|cx16|lahf_lm|lzcnt|prefetchw)")


def isa_flags(flags):
    """The /proc/cpuinfo flags that name instruction-set extensions a compiler may target."""
    return {f for f in flags if ISA_FLAG.match(f)}


def missing_isa(builder_flags, host_flags):
    return sorted(isa_flags(builder_flags) - set(host_flags))


def utcnow():
    return datetime.datetime.now(datetime.timezone.utc)


def stamp(t=None):
    return (t or utcnow()).strftime("%Y%m%dT%H%M%SZ")


# ---------------------------------------------------------------- fleet model

class Host:
    """One fleet machine. `ssh` is user@address, or LOCAL for the dry-run fleet."""

    def __init__(self, name, role, ssh, private_ip, root, port_base=7000, node_id=None,
                 instance_type=None, key=None, known_hosts=None):
        self.name, self.role, self.ssh = name, role, ssh
        self.private_ip, self.root, self.port_base = private_ip, root, port_base
        self.node_id, self.instance_type = node_id, instance_type
        self.key, self.known_hosts = key, known_hosts

    @classmethod
    def from_json(cls, d, key=None, known_hosts=None):
        return cls(d["name"], d["role"], d["ssh"], d["private_ip"], d["root"],
                   d.get("port_base", 7000), d.get("node_id"), d.get("instance_type"),
                   key, known_hosts)

    def ports(self, shards):
        return [self.port_base + 1 + i for i in range(shards)]

    def addrs(self, shards):
        return ["%s:%d" % (self.private_ip, p) for p in self.ports(shards)]

    @property
    def metrics_port(self):
        return self.port_base + 476

    @property
    def pd_metrics_port(self):
        return self.port_base + 465

    @property
    def raft_port(self):
        return self.port_base + 100

    def __repr__(self):
        return "Host(%s)" % self.name


def load_fleet(run_dir):
    with open(os.path.join(run_dir, "fleet.json")) as f:
        fleet = json.load(f)
    key = fleet.get("ssh_key")
    kh = os.path.join(run_dir, "known_hosts")
    hosts = [Host.from_json(h, key, kh) for h in fleet["hosts"]]
    return fleet, hosts


# ---------------------------------------------------------------- configs

def node_shards(profile):
    return profile["drives_per_node"] * profile["parts_per_drive"] if profile["backend"] == "spdk" else 1


def peer_spec(nodes, shards):
    """vals-bench sharded peer string: id@a1,a2;id@a1,a2."""
    return ";".join("%d@%s" % (n.node_id, ",".join(n.addrs(shards))) for n in nodes)


def region_mib(namespace_bytes, parts_per_drive, margin_mib=64):
    """Largest whole-MiB partition region that fits parts_per_drive times on a drive."""
    per = namespace_bytes // parts_per_drive // (1 << 20) - margin_mib
    if per <= 1024:
        raise ValueError("drive too small: %d bytes" % namespace_bytes)
    return per


def node_conf(node, nodes, profile, pcis=None, ns_bytes=None):
    """valsd node config lines for `node` in a cluster of `nodes`."""
    shards = node_shards(profile)
    lines = ["node_id = %d" % node.node_id, "profile = %s" % profile.get("vals_profile", "capacity")]
    lines += ["listen = %s" % a for a in node.addrs(shards)]
    if profile["backend"] == "spdk":
        if not pcis or len(pcis) != profile["drives_per_node"]:
            raise ValueError("%s: need %d instance-store PCIs, got %r" % (node.name, profile["drives_per_node"], pcis))
        lines.append("backend = spdk")
        lines += ["pci = %s" % p for p in pcis]
        region = region_mib(ns_bytes, profile["parts_per_drive"])
        default_zone = int(region * profile.get("default_zone_fraction", 0.6))
        named = int(region * profile.get("named_zone_fraction", 0.15))
        lines += ["region_mib = %d" % region, "zone_region_mib = %d" % named,
                  "zone_region_mib_override = 0:%d" % default_zone]
        if profile.get("spdk_mem_mb"):
            lines.append("spdk_mem_mb = %d" % profile["spdk_mem_mb"])
        lines.append("spdk_shm_id = %d" % node.node_id)
    else:
        lines += ["backend = file", "data_dir = data", "region_mib = %d" % profile.get("region_mib", 1024)]
    for p in nodes:
        if p is not node:
            lines.append("peer = %d @ %s" % (p.node_id, ",".join(p.addrs(shards))))
    lines += ["rf = %d" % profile["rf"], "sweep = %s" % profile.get("sweep", "5s"),
              "gc_grace = %s" % profile.get("gc_grace", "0s"),
              "secret_file = secret", "admin_socket = node.sock",
              "metrics = 0.0.0.0:%d" % node.metrics_port, "log_level = info"]
    lines += profile.get("node_extra", [])
    return lines


def pd_conf(node, nodes, profile):
    """One voting PD per data node (the three-PD reference layout)."""
    shards = node_shards(profile)
    lines = ["pd_id = %d" % node.node_id, "data_dir = pd"]
    lines += ["data_node = %d @ %s" % (n.node_id, ",".join(n.addrs(shards))) for n in nodes]
    lines += ["rf = %d" % profile["rf"], "admin_socket = pd.sock", "secret_file = secret",
              "metrics = 0.0.0.0:%d" % node.pd_metrics_port, "log_level = info"]
    for z in profile.get("zones", []):
        lines.append("zone = %s" % z)
    if len(nodes) > 1:
        lines.append("listen = 0.0.0.0:%d" % node.raft_port)
        lines += ["pd_peer = %d @ %s:%d" % (n.node_id, n.private_ip, n.raft_port) for n in nodes]
        if node is nodes[0]:
            lines.append("campaign = true")
    return lines


# ---------------------------------------------------------------- cost

def hourly(instance_type, market="on-demand"):
    od, spot = PRICES[instance_type]
    return spot if market == "spot" else od


def fleet_hourly(roles):
    """roles: [{type, count, market}] -> USD/h."""
    return sum(hourly(r["type"], r.get("market", "on-demand")) * r["count"] for r in roles)


def fleet_vcpus(roles, market="on-demand"):
    return sum(VCPUS[r["type"]] * r["count"] for r in roles if r.get("market", "on-demand") == market)


def accrued(hosts_json, now=None):
    """USD accrued so far from each host's launch time and hourly price (60 s minimum)."""
    now = now or utcnow()
    total = 0.0
    for h in hosts_json:
        if not h.get("launch_time"):
            continue
        start = datetime.datetime.fromisoformat(h["launch_time"].replace("Z", "+00:00"))
        end = datetime.datetime.fromisoformat(h["end_time"].replace("Z", "+00:00")) if h.get("end_time") else now
        secs = max(60.0, (end - start).total_seconds())
        total += secs / 3600.0 * h["price_per_hour"]
    return total


# ---------------------------------------------------------------- output parsing

_NUM = r"([0-9]+(?:\.[0-9]+)?)"


def parse_netload(text):
    """Pull the summary lines out of `vals-bench x netload|netread|netmultiread|netbatchwrite` output."""
    out = {}
    m = re.search(r"write Quorum\(\d+\)\s*:\s*%s logical writes/s.*?\((\d+) logical writes, (\d+) calls, (\d+) errors" % _NUM, text)
    if m:
        out["write_ops_s"] = float(m.group(1))
        out["write_ops"], out["write_calls"], out["write_errors"] = int(m.group(2)), int(m.group(3)), int(m.group(4))
    m = re.search(r"read\s+R=\d+ strong\s*:\s*%s keys/s aggregate\s*\((\d+) keys, (\d+) batch calls, %s calls/s, (\d+) errors, (\d+) misses, (\d+) mismatches" % (_NUM, _NUM), text)
    if m:
        out["read_keys_s"] = float(m.group(1))
        out["read_keys"] = int(m.group(2))
        out["read_errors"], out["read_misses"], out["read_mismatches"] = int(m.group(5)), int(m.group(6)), int(m.group(7))
    for kind in ("write ack latency", "read latency", "batch latency"):
        m = re.search(r"%s\s*:\s*(\d+) samples.*?p50=(\d+) p99=(\d+) p99\.9=(\d+) max=(\d+) ns" % re.escape(kind), text)
        if m:
            key = kind.split()[0] + "_lat_us"
            out[key] = {"n": int(m.group(1)), "p50": int(m.group(2)) / 1e3, "p99": int(m.group(3)) / 1e3,
                        "p999": int(m.group(4)) / 1e3, "max": int(m.group(5)) / 1e3}
    m = re.search(r"correctness\s*:\s*(\d+) matched / (\d+) mismatched", text)
    if m:
        out["verify_ok"], out["verify_bad"] = int(m.group(1)), int(m.group(2))
    return out


def parse_np(text):
    """netprogress: NP_CONFIG/NP_SAMPLE.../NP_SUMMARY/NP_VERIFY JSON lines."""
    out = {"samples": []}
    for line in text.splitlines():
        tag, _, body = line.partition(" ")
        if not tag.startswith("NP_") or not body.startswith("{"):
            continue
        try:
            obj = json.loads(body)
        except ValueError:
            continue
        if tag == "NP_SAMPLE":
            out["samples"].append(obj)
        else:
            out[tag[3:].lower()] = obj
    return out


def merge_netload(results):
    """Sum per-client netload results into a cluster figure."""
    agg = {}
    for r in results:
        for k, v in r.items():
            if isinstance(v, (int, float)):
                agg[k] = agg.get(k, 0) + v
            elif isinstance(v, dict) and k.endswith("_lat_us"):
                agg.setdefault(k + "_by_client", []).append(v)
    return agg


# ---------------------------------------------------------------- live checks

def evaluate_latest(prev, cur, expect_down=()):
    """Compare two hostmon `latest` snapshots of one host; return [(severity, message)].

    severity: critical (stop the campaign), warn (annotate), note.
    expect_down: tracked process names that a chaos step has stopped on purpose.
    """
    issues = []
    for name, p in cur.get("procs", {}).items():
        if name in expect_down:
            continue
        if not p.get("alive"):
            issues.append(("critical", "process %s is not running" % name))
        old = (prev or {}).get("procs", {}).get(name, {})
        if old and p.get("restarts", 0) > old.get("restarts", 0):
            issues.append(("critical", "process %s restarted (pid %s)" % (name, p.get("pid"))))
    for ep, m in cur.get("metrics", {}).items():
        oldw = (prev or {}).get("metrics", {}).get(ep, {}).get("watch", {})
        w = m.get("watch", {})
        for fam in CRITICAL_FAMILIES:
            if w.get(fam, 0) > oldw.get(fam, 0) or (prev is None and w.get(fam, 0) > 0):
                issues.append(("critical", "%s %s rose to %s" % (ep, fam, w[fam])))
        for fam in WARN_FAMILIES:
            if w.get(fam, 0) > oldw.get(fam, 0):
                issues.append(("warn", "%s %s +%d" % (ep, fam, w[fam] - oldw.get(fam, 0))))
        for fam in NOTE_FAMILIES:
            if w.get(fam, 0) > oldw.get(fam, 0):
                issues.append(("note", "%s %s +%d" % (ep, fam, w[fam] - oldw.get(fam, 0))))
        if ep not in expect_down and not m.get("up", True):
            issues.append(("warn", "metrics endpoint %s down" % ep))
    for iface, counters in cur.get("nic", {}).items():
        old = (prev or {}).get("nic", {}).get(iface, {})
        for c, v in counters.items():
            if c.endswith("exceeded") and v > old.get(c, v if prev is None else 0):
                issues.append(("warn", "%s %s +%d" % (iface, c, v - old.get(c, 0))))
    swap = cur.get("mem", {}).get("swap_used_bytes") or 0
    old_swap = ((prev or {}).get("mem", {}).get("swap_used_bytes") or 0) if prev else swap
    if swap - old_swap > (64 << 20):
        issues.append(("warn", "swap use grew by %d MiB to %d MiB" % ((swap - old_swap) >> 20, swap >> 20)))
    return issues


def np_sample_issues(sample):
    """netprogress NP_SAMPLE: any bad read is a correctness failure."""
    if sample.get("bad", 0) > 0:
        return [("critical", "netprogress saw %d bad reads" % sample["bad"])]
    return []


# ---------------------------------------------------------------- execution

def ssh_argv(host, script, tty=False):
    if host.ssh == LOCAL:
        return ["sh", "-c", script]
    argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "ServerAliveInterval=15",
            "-o", "ServerAliveCountMax=8", "-o", "StrictHostKeyChecking=accept-new"]
    if host.known_hosts:
        argv += ["-o", "UserKnownHostsFile=%s" % host.known_hosts]
    if host.key:
        argv += ["-i", host.key]
    if tty:
        argv.append("-t")
    return argv + [host.ssh, "sh -c " + shlex.quote(script)]


def sh(host, script, check=True, timeout=300):
    """Run a POSIX shell script on host; returns stdout."""
    res = subprocess.run(ssh_argv(host, script), capture_output=True, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError("%s: exit %d: %s\n%s" % (host.name, res.returncode, script[:300], res.stderr[-2000:]))
    return res.stdout


def try_sh(host, script, timeout=60):
    try:
        return sh(host, script, check=False, timeout=timeout)
    except (subprocess.TimeoutExpired, OSError):
        return ""


def push(host, src, dst, delete=False):
    """Copy a local file or directory (trailing slash = contents) to host:dst."""
    if host.ssh == LOCAL:
        os.makedirs(os.path.dirname(dst.rstrip("/")) or ".", exist_ok=True)
        argv = ["rsync", "-a"] + (["--delete"] if delete else []) + [src, dst]
    else:
        e = "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
        if host.known_hosts:
            e += " -o UserKnownHostsFile=%s" % host.known_hosts
        if host.key:
            e += " -i %s" % host.key
        argv = ["rsync", "-az", "-e", e] + (["--delete"] if delete else []) + [src, "%s:%s" % (host.ssh, dst)]
    subprocess.run(argv, check=True, timeout=1800)


def pull(host, src, dst):
    os.makedirs(dst, exist_ok=True)
    if host.ssh == LOCAL:
        argv = ["rsync", "-a", src, dst]
    else:
        e = "ssh -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
        if host.known_hosts:
            e += " -o UserKnownHostsFile=%s" % host.known_hosts
        if host.key:
            e += " -i %s" % host.key
        argv = ["rsync", "-az", "-e", e, "%s:%s" % (host.ssh, src), dst]
    return subprocess.run(argv, timeout=3600).returncode


def parallel(fn, items):
    """Run fn(item) on every item concurrently; re-raise the first failure."""
    results, errors = {}, []

    def work(i, item):
        try:
            results[i] = fn(item)
        except Exception as e:  # noqa: BLE001 - reported below with the item
            errors.append((item, e))

    threads = [threading.Thread(target=work, args=(i, it)) for i, it in enumerate(items)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise RuntimeError("; ".join("%s: %s" % (it, e) for it, e in errors))
    return [results[i] for i in range(len(items))]


class RemoteJob:
    """A detached command on a host: survives ssh drops; output and exit code land in files."""

    def __init__(self, host, name, command, env=None, sudo=False, cwd=None):
        self.host, self.name = host, name
        self.log = "%s/out/jobs/%s.log" % (host.root, name)
        self.rc = "%s/out/jobs/%s.rc" % (host.root, name)
        self.pidf = "%s/out/jobs/%s.pid" % (host.root, name)
        envs = " ".join("%s=%s" % (k, shlex.quote(str(v))) for k, v in sorted((env or {}).items()))
        inner = "cd %s && ulimit -c unlimited; %s %s" % (shlex.quote(cwd or host.root),
                                                          "env " + envs if envs else "", command)
        wrapped = "echo $$ > %s; %s; echo $? > %s" % (shlex.quote(self.pidf), inner, shlex.quote(self.rc))
        prefix = "sudo -E " if sudo and host.ssh != LOCAL else ""
        self.script = ("mkdir -p %s/out/jobs && rm -f %s %s && %ssetsid nohup sh -c %s > %s 2>&1 < /dev/null &"
                       % (shlex.quote(host.root), shlex.quote(self.rc), shlex.quote(self.pidf), prefix,
                          shlex.quote(wrapped), shlex.quote(self.log)))
        self.started = None

    def start(self):
        sh(self.host, self.script, timeout=60)
        self.started = time.time()
        return self

    def done(self):
        out = try_sh(self.host, "cat %s 2>/dev/null" % shlex.quote(self.rc), timeout=30).strip()
        return int(out) if out.lstrip("-").isdigit() else None

    def wait(self, timeout, poll=5.0, on_poll=None):
        deadline = time.time() + timeout
        while time.time() < deadline:
            rc = self.done()
            if rc is not None:
                return rc
            if on_poll:
                on_poll()
            time.sleep(poll)
        return None

    def output(self):
        return try_sh(self.host, "cat %s 2>/dev/null" % shlex.quote(self.log), timeout=120)

    def kill(self, sig="TERM"):
        try_sh(self.host, "pid=$(cat %s 2>/dev/null) && %spkill --signal %s -g $pid; true"
               % (shlex.quote(self.pidf), "sudo " if self.host.ssh != LOCAL else "", sig))

    def stop(self, timeout=60):
        """TERM the job's process group, KILL it after timeout; True once nothing is left."""
        self.kill("TERM")
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.alive():
                return True
            time.sleep(1)
        self.kill("KILL")
        time.sleep(2)
        return not self.alive()

    def alive(self):
        out = try_sh(self.host, "pid=$(cat %s 2>/dev/null) && pgrep -g $pid >/dev/null && echo yes; true"
                     % shlex.quote(self.pidf), timeout=20)
        return "yes" in out


class EventLog:
    """Append-only JSONL of everything the orchestrator did, with UTC timestamps."""

    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def __call__(self, kind, quiet=False, **fields):
        rec = {"t": time.time(), "utc": utcnow().isoformat(timespec="milliseconds"), "kind": kind}
        rec.update(fields)
        with self.lock, open(self.path, "a") as f:
            f.write(json.dumps(rec, sort_keys=True) + "\n")
        if quiet:
            return rec
        msg = " ".join("%s=%s" % (k, v) for k, v in fields.items() if not isinstance(v, (dict, list)))
        print("[%s] %-10s %s" % (rec["utc"][11:19], kind, msg), flush=True)
        return rec


def read_events(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(l) for l in f if l.strip()]


def step_windows(events):
    """{step: (start_t, end_t, status)} from step_start/step_end events (last attempt wins)."""
    out = {}
    for e in events:
        if e["kind"] == "step_start":
            out[e["step"]] = [e["t"], None, "running"]
        elif e["kind"] == "step_end" and e["step"] in out:
            out[e["step"]][1] = e["t"]
            out[e["step"]][2] = e.get("status", "ok")
    return {k: tuple(v) for k, v in out.items()}


def valsd_pids_script(root, what):
    """Shell that prints the PIDs of `<root>/bin/valsd <what> ...` without matching itself."""
    want = "%s/bin/valsd %s " % (root, what)
    return ("for p in $(pgrep -x valsd); do c=$(tr '\\0' ' ' < /proc/$p/cmdline 2>/dev/null); "
            "case \"$c\" in %s*) echo $p;; esac; done" % shlex.quote(want))


def write_lines(host, path, lines, mode="600"):
    text = "\n".join(lines) + "\n"
    sh(host, "umask 077; mkdir -p %s && cat > %s <<'VALS_EOF'\n%sVALS_EOF\nchmod %s %s"
       % (shlex.quote(os.path.dirname(path)), shlex.quote(path), text, mode, shlex.quote(path)))
