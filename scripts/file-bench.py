#!/usr/bin/env python3
"""Reproducible file-backed valsd benchmark (docs/benchmarks.md).

Starts a three-node, one-PD, file-backed `valsd` cluster with the default
node settings (capacity profile, anti-entropy on), loads a dataset, and runs
`vals-bench vals netprogress` phases against it: buffered and device-durable
writes, warm-cache and cold-cache reads, and a mixed workload, each closed-loop
(saturation) and then open-loop at a fraction of the saturated rate (moderate
load). Every read is checked against the values the client wrote. Each phase
records throughput, errors, p50/p95/p99 latency, valsd CPU and RSS, disk I/O
and client CPU.

  scripts/file-bench.py start --dir /tmp/vals-bench          # one host, three nodes
  scripts/file-bench.py run   --dir /tmp/vals-bench --out results/
  scripts/file-bench.py stop  --dir /tmp/vals-bench

For one node per host, pass the ssh names and listen addresses of three hosts
(the PD runs on the first). The script copies valsd to each host and runs the
client on the machine it runs on:

  scripts/file-bench.py start --dir /var/tmp/vals-bench --hosts h1,h2,h3 --addrs 10.0.0.1,10.0.0.2,10.0.0.3
  scripts/file-bench.py run   --dir /var/tmp/vals-bench --hosts h1,h2,h3 --addrs 10.0.0.1,10.0.0.2,10.0.0.3 --out results/

Cold-cache reads drop each node host's page cache, which needs passwordless
`sudo` there; without it that phase is skipped.
"""
import argparse
import datetime
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCAL = "local"

# name, read %, batch-read %, persistence, cold page cache.
PHASES = {
    "write-buffered": (0, 0, "buffered", False),
    "write-device": (0, 0, "device", False),
    "read-warm": (100, 0, "buffered", False),
    "read-cold": (100, 0, "buffered", True),
    "mixed-device": (65, 10, "device", False),
    "mixed-buffered": (65, 10, "buffered", False),
}


def sh(host, script, check=True, capture=True, timeout=120):
    """Run a POSIX shell script on host (locally for LOCAL, else over ssh)."""
    argv = ["sh", "-c", script] if host == LOCAL else ["ssh", "-o", "BatchMode=yes", host, "sh -c " + shlex.quote(script)]
    res = subprocess.run(argv, capture_output=capture, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise SystemExit("command on %s failed (%d): %s\n%s" % (host, res.returncode, script, res.stderr))
    return res.stdout if capture else ""


def layout(args):
    hosts = args.hosts.split(",") if args.hosts else [LOCAL] * 3
    addrs = args.addrs.split(",") if args.addrs else ["127.0.0.1"] * len(hosts)
    if len(hosts) != 3 or len(addrs) != 3:
        raise SystemExit("--hosts and --addrs need three entries each")
    nodes = [(i + 1, hosts[i], "%s:%d" % (addrs[i], args.port + i)) for i in range(3)]
    return nodes


def peer_spec(nodes):
    return ",".join("%d@%s" % (nid, addr) for nid, _, addr in nodes)


def cmd_start(args):
    nodes = layout(args)
    valsd = os.path.abspath(args.valsd)
    pd_host = nodes[0][1]
    d = args.dir
    for host in sorted({h for _, h, _ in nodes}):
        sh(host, "test ! -e %s/pd.conf -a ! -e %s/node1.conf -a ! -e %s/node2.conf" % ((shlex.quote(d),) * 3))
        sh(host, "mkdir -p %s && umask 077 && echo %s > %s/secret" % (shlex.quote(d), shlex.quote(args.secret), shlex.quote(d)))
        if host == LOCAL:
            sh(host, "cp %s %s/valsd" % (shlex.quote(valsd), shlex.quote(d)))
        else:
            subprocess.run(["scp", "-q", valsd, "%s:%s/valsd" % (host, d)], check=True)
    for nid, host, addr in nodes:
        lines = ["node_id = %d" % nid, "listen = %s" % addr, "data_dir = node%d" % nid]
        lines += ["peer = %d @ %s" % (p, paddr) for p, _, paddr in nodes if p != nid]
        lines += ["region_mib = %d" % args.region_mib, "secret_file = secret"]
        if args.metrics_offset:
            ip, port = addr.rsplit(":", 1)
            lines.append("metrics = %s:%d" % (ip, int(port) + args.metrics_offset))
        lines += args.node_line
        write_file(host, "%s/node%d.conf" % (d, nid), lines)
    pd = ["pd_id = 1", "data_dir = pd"]
    pd += ["data_node = %d @ %s" % (nid, addr) for nid, _, addr in nodes]
    pd += ["rf = 3", "admin_socket = pd.sock", "secret_file = secret"]
    write_file(pd_host, "%s/pd.conf" % d, pd)
    for nid, host, _ in nodes:
        spawn(host, d, "node%d" % nid, "node -c node%d.conf" % nid)
    spawn(pd_host, d, "pd", "pd -c pd.conf")
    for _ in range(60):
        out = sh(pd_host, "cd %s && ./valsd admin -s pd.sock members" % shlex.quote(d), check=False)
        if len([l for l in out.splitlines() if l.strip()]) >= 3:
            print("cluster up: %s" % peer_spec(nodes))
            return
        time.sleep(1)
    raise SystemExit("PD did not report three members; see %s/*.log" % d)


def write_file(host, path, lines):
    text = "\n".join(lines) + "\n"
    sh(host, "cat > %s <<'EOF'\n%sEOF" % (shlex.quote(path), text))


def spawn(host, d, name, argv):
    sh(host, "cd %s || exit 1\nnohup ./valsd %s > %s.log 2>&1 < /dev/null & echo $! > %s.pid"
       % (shlex.quote(d), argv, name, name))


def cmd_stop(args):
    nodes = layout(args)
    for host in sorted({h for _, h, _ in nodes}):
        sh(host, "cd %s && for f in *.pid; do [ -f \"$f\" ] && kill $(cat \"$f\") 2>/dev/null; rm -f \"$f\"; done; true"
           % shlex.quote(args.dir), check=False)
    if args.wipe:
        time.sleep(3)
        for host in sorted({h for _, h, _ in nodes}):
            sh(host, "rm -rf %s" % shlex.quote(args.dir))


SAMPLE = r"""
cd %(dir)s
for f in *.pid; do
  p=$(cat "$f"); [ -r /proc/$p/stat ] || continue
  echo "P $f $(cut -d' ' -f14,15 /proc/$p/stat) $(awk '/^VmRSS/{print $2}' /proc/$p/status)"
done
dev=$(lsblk -no KNAME "$(findmnt -no SOURCE -T .)" 2>/dev/null | head -1)
awk -v d="$dev" '$3==d{print "D",$4,$6,$8,$10}' /proc/diskstats
echo "T $(getconf CLK_TCK) $(nproc)"
head -1 /proc/stat
awk '/^Cached:/{print "M",$2}' /proc/meminfo
du -sk node[0-9] 2>/dev/null | awk '{s+=$1} END {print "S",s}'
"""


def sample(nodes, d):
    snap = {}
    for host in sorted({h for _, h, _ in nodes}):
        out = sh(host, SAMPLE % {"dir": shlex.quote(d)})
        s = {"procs": {}, "time": time.time()}
        for line in out.splitlines():
            f = line.split()
            if not f:
                continue
            if f[0] == "P" and len(f) == 5:
                s["procs"][f[1]] = (int(f[2]) + int(f[3]), int(f[4]))
            elif f[0] == "D" and len(f) == 5:
                s["disk"] = [int(x) for x in f[1:]]
            elif f[0] == "T":
                s["hz"], s["ncpu"] = int(f[1]), int(f[2])
            elif f[0] == "cpu":
                vals = [int(x) for x in f[1:]]
                s["cpu"] = (sum(vals) - vals[3] - vals[4], sum(vals))
            elif f[0] == "M":
                s["cached_kb"] = int(f[1])
            elif f[0] == "S" and len(f) == 2:
                s["data_kb"] = int(f[1])
        snap[host] = s
    return snap


def usage(before, after):
    """Per-host resource deltas between two samples."""
    out = {}
    for host, a in after.items():
        b = before[host]
        secs = max(a["time"] - b["time"], 1e-6)
        ticks = sum(a["procs"][k][0] - b["procs"].get(k, (0, 0))[0] for k in a["procs"])
        u = {
            "valsd_cpu_cores": round(ticks / a["hz"] / secs, 2),
            "valsd_rss_mb": round(sum(v[1] for v in a["procs"].values()) / 1024, 1),
            "page_cache_gb": round(a.get("cached_kb", 0) / 1048576, 1),
            "data_dir_gb": round(a.get("data_kb", 0) / 1048576, 2),
        }
        if "cpu" in a and "cpu" in b:
            total = a["cpu"][1] - b["cpu"][1]
            u["host_cpu_busy_pct"] = round(100.0 * (a["cpu"][0] - b["cpu"][0]) / max(total, 1), 1)
            u["host_cpus"] = a["ncpu"]
        if "disk" in a and "disk" in b:
            r, rs, w, ws = [x - y for x, y in zip(a["disk"], b["disk"])]
            u.update({
                "disk_read_iops": round(r / secs), "disk_read_mbps": round(rs * 512 / 1e6 / secs, 1),
                "disk_write_iops": round(w / secs), "disk_write_mbps": round(ws * 512 / 1e6 / secs, 1),
            })
        out[host] = u
    return out


def settle(args, nodes):
    """Wait until every valsd process is near idle, so a phase does not measure
    the flushes, merges and anti-entropy left over from the one before.
    Returns the seconds waited (capped at --settle-max)."""
    t0 = time.time()
    prev = sample(nodes, args.dir)
    while time.time() - t0 < args.settle_max:
        time.sleep(5)
        cur = sample(nodes, args.dir)
        cores = sum(u["valsd_cpu_cores"] for u in usage(prev, cur).values())
        procs = sum(len(c["procs"]) for c in cur.values())
        prev = cur
        if cores <= args.settle_cores * procs:
            break
    return round(time.time() - t0)


def save_meta(path, meta):
    with open(path, "w") as f:
        json.dump(meta, f, indent=2)


def run_np(args, nodes, name, env_extra, out_dir, meta=None, cold=False):
    env = dict(os.environ)
    env.update({
        "VALS_NP_THREADS": str(args.threads), "VALS_NP_SLOTS": str(args.slots),
        "VALS_NP_KEYS": str(args.keys_per_slot), "VALS_NP_VSIZE": str(args.vsize),
        "VALS_NP_MULTI": str(args.batch), "VALS_NP_FD": "default", "VALS_NP_PREFIX": args.prefix,
        "VALS_NP_SAMPLE_SECS": "1", "VALS_NP_BUSY_BACKOFF_MS": str(args.busy_backoff_ms),
    })
    env.update(env_extra)
    settled = settle(args, nodes) if args.settle_max > 0 else 0
    if cold and not drop_caches(nodes):
        raise SystemExit("%s: could not drop the page cache" % name)
    before = sample(nodes, args.dir)
    log = os.path.join(out_dir, "%s-%s.log" % (args.run_id, name))
    summary, verify, after = None, None, None
    with open(log, "w") as f:
        f.write("# env %s\n" % json.dumps({k: v for k, v in env.items() if k.startswith("VALS_")}, sort_keys=True))
        f.flush()
        proc = subprocess.Popen([args.vals_bench, "vals", "netprogress", peer_spec(nodes), "default"],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            f.write(line)
            if line.startswith("NP_WINDOW_END "):
                # The measured window has just ended; the client's
                # verification reads come after it.
                after = sample(nodes, args.dir)
            elif line.startswith("NP_SUMMARY "):
                summary = json.loads(line[len("NP_SUMMARY "):])
            elif line.startswith("NP_VERIFY "):
                verify = json.loads(line[len("NP_VERIFY "):])
        rc = proc.wait()
    if summary is None or after is None:
        raise SystemExit("%s: no NP_WINDOW_END/NP_SUMMARY (exit %d); see %s" % (name, rc, log))
    secs = max(summary["elapsed"], 1e-6)
    ops = summary["ops"]
    total_ok = sum(o["ok"] for o in ops.values())
    errors = sum(sum(o["err"].values()) for o in ops.values())
    result = {
        "phase": name, "exit": rc, "secs": secs,
        "ops_per_s": round(total_ok / secs), "errors": errors,
        "per_op": {k: dict(v, ops_per_s=round(v["ok"] / secs)) for k, v in ops.items() if v["ok"] or v["err"]},
        "bad_reads": summary["bad_during_run"], "verify": verify,
        "client_cpu_cores": round(summary["client_cpu_s"] / secs, 2), "client_rss_mb": summary["client_rss_mb"],
        "run": args.run_id, "hosts": usage(before, after), "log": os.path.basename(log), "settle_secs": settled,
        "target_ops_per_s": float(env.get("VALS_NP_RATE", "0")),
    }
    if meta is not None:
        # Dataset age at the start of this run: minutes since the load and
        # acknowledged writes (load included) before it.
        result["dataset_age_min"] = round((before[nodes[0][1]]["time"] - meta["loaded"]) / 60, 1)
        result["dataset_writes_before"] = meta["writes"]
        meta["writes"] += result["per_op"].get("write", {}).get("ok", 0)
    with open(os.path.join(out_dir, "results.jsonl"), "a") as f:
        f.write(json.dumps(result, sort_keys=True) + "\n")
    print(line_for(result), flush=True)
    if rc != 0 or not verify or not verify.get("ok"):
        raise SystemExit("%s: value check failed or client exited %d; see %s" % (name, rc, log))
    return result


def line_for(r):
    lat = " ".join("%s p50/p95/p99 %.2f/%.2f/%.2f ms retries %d" % (k, v["p50_ms"], v["p95_ms"], v["p99_ms"],
                                                                   v["busy_retries"])
                   for k, v in r["per_op"].items())
    cpu = ",".join("%.1f" % h["valsd_cpu_cores"] for h in r["hosts"].values())
    target = " (target %d)" % r["target_ops_per_s"] if r["target_ops_per_s"] else ""
    return "%-26s %8d ops/s%s  err %d  %s  valsd cpu %s cores  settled %ds" % (
        r["phase"], r["ops_per_s"], target, r["errors"], lat, cpu, r["settle_secs"])


def can_sudo(nodes):
    for host in sorted({h for _, h, _ in nodes}):
        argv = ["sh", "-c"] if host == LOCAL else ["ssh", "-o", "BatchMode=yes", host]
        if subprocess.run(argv + ["sudo -n true"], capture_output=True).returncode != 0:
            return False
    return True


def drop_caches(nodes):
    for host in sorted({h for _, h, _ in nodes}):
        argv = ["sh", "-c"] if host == LOCAL else ["ssh", "-o", "BatchMode=yes", host]
        script = "sync && sudo -n sh -c 'echo 3 > /proc/sys/vm/drop_caches'"
        if subprocess.run(argv + [script], capture_output=True).returncode != 0:
            return False
    return True


def environment(args, nodes):
    env = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")}
    env["revision"] = args.revision or subprocess.run(["git", "-C", ROOT, "describe", "--always", "--dirty"],
                                                      capture_output=True, text=True).stdout.strip()
    with open(args.vals_bench, "rb") as f:
        env["vals_bench_sha256"] = hashlib.sha256(f.read()).hexdigest()
    env["client_host"] = os.uname().nodename
    env["hosts"] = {}
    for host in sorted({h for _, h, _ in nodes}):
        out = sh(host, "cd %s && sha256sum valsd | cut -c1-64; uname -n; nproc; "
                       "awk '/^MemTotal/{print $2}' /proc/meminfo; "
                       "lscpu | sed -n 's/^Model name: *//p' | head -1; "
                       "lsblk -snlo TYPE,MODEL \"$(findmnt -no SOURCE -T .)\" | awk '$1==\"disk\"{$1=\"\"; print}' | sort -u | paste -sd+; "
                       "findmnt -no FSTYPE -T .; uname -r" % shlex.quote(args.dir), check=False).splitlines()
        out += [""] * 8
        env["hosts"][host] = {"valsd_sha256": out[0], "name": out[1], "cpus": out[2],
                              "mem_gb": round(int(out[3] or 0) / 1048576), "cpu": out[4],
                              "disk": out[5], "fs": out[6], "kernel": out[7]}
    return env


def cmd_run(args):
    nodes = layout(args)
    args.run_id = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    n_slots = args.threads * args.slots
    os.makedirs(args.out, exist_ok=True)
    meta_path = os.path.join(args.out, "dataset.json")
    meta = None
    if os.path.exists(meta_path):
        with open(meta_path) as f:
            meta = json.load(f)
        if meta["vsize"] != args.vsize or meta["prefix"] != args.prefix \
                or meta["threads"] < args.threads or meta["slots"] < args.slots:
            raise SystemExit("dataset.json in %s does not cover this run's key layout" % args.out)
        args.keys_per_slot = meta["keys_per_slot"]
    else:
        args.keys_per_slot = max(1, args.keys // n_slots)
    keys = args.keys_per_slot * n_slots
    env = environment(args, nodes)
    env["config"] = {"nodes": peer_spec(nodes), "rf": 3, "write": "majority", "read": "majority",
                     "keys": keys, "vsize": args.vsize, "batch_read_keys": args.batch,
                     "in_flight": n_slots, "client_threads": args.threads, "phase_secs": args.secs,
                     "moderate_fraction": args.moderate,
                     "node_conf": sh(nodes[0][1], "cat %s/node1.conf" % shlex.quote(args.dir)).splitlines(),
                     "failure_detector": "default", "busy_backoff_ms": args.busy_backoff_ms}
    with open(os.path.join(args.out, "environment-%s.json" % args.run_id), "w") as f:
        json.dump(env, f, indent=2, sort_keys=True)
    print(json.dumps(env, sort_keys=True))
    if meta is None:
        r = run_np(args, nodes, "load", {"VALS_NP_PREFILL": "1", "VALS_NP_RATE": "0", "VALS_NP_READ_PCT": "0",
                                         "VALS_NP_MULTI_PCT": "0", "VALS_NP_SECS": "86400"}, args.out)
        meta = {"loaded": time.time(), "keys": keys, "keys_per_slot": args.keys_per_slot, "vsize": args.vsize,
                "prefix": args.prefix, "threads": args.threads, "slots": args.slots, "load_ops_per_s": r["ops_per_s"],
                "writes": r["per_op"]["write"]["ok"]}
        save_meta(meta_path, meta)
    for name in args.phases.split(","):
        read_pct, multi_pct, persistence, cold = PHASES[name]
        base = {"VALS_NP_EXISTING": "1", "VALS_NP_READ_PCT": str(read_pct), "VALS_NP_MULTI_PCT": str(multi_pct),
                "VALS_NP_PERSISTENCE": persistence, "VALS_NP_SECS": str(args.secs)}
        if cold and not can_sudo(nodes):
            print("%s: skipped (needs passwordless sudo on every node host to drop the page cache)" % name)
            continue
        sat = run_np(args, nodes, name + "-saturated", dict(base, VALS_NP_RATE="0"), args.out, meta, cold)
        save_meta(meta_path, meta)
        rate = max(1, int(sat["ops_per_s"] * args.moderate))
        run_np(args, nodes, name + "-moderate", dict(base, VALS_NP_RATE=str(rate)), args.out, meta, cold)
        save_meta(meta_path, meta)
    print("results in %s/results.jsonl" % args.out)


def cmd_report(args):
    """Print results.jsonl as a markdown table."""
    print("| Phase | Load | ops/s | p50 / p95 / p99 ms | Busy retries | Errors | valsd CPU (cores) | Disk read / write MB/s |")
    print("|---|---|---:|---|---:|---:|---:|---:|")
    with open(os.path.join(args.out, "results.jsonl")) as f:
        for line in f:
            r = json.loads(line)
            phase, _, load = r["phase"].rpartition("-")
            if not phase:
                phase, load = r["phase"], "saturated"
            lat = "<br>".join("%s %.2f / %.2f / %.2f" % (k, v["p50_ms"], v["p95_ms"], v["p99_ms"])
                              for k, v in r["per_op"].items())
            hosts = list(r["hosts"].values())
            cpu = " + ".join("%.1f" % h["valsd_cpu_cores"] for h in hosts)
            disk = " + ".join("%.0f / %.0f" % (h.get("disk_read_mbps", 0), h.get("disk_write_mbps", 0)) for h in hosts)
            retries = sum(v["busy_retries"] for v in r["per_op"].values())
            print("| %s | %s | %d | %s | %d | %d | %s | %s |" % (
                phase, load, r["ops_per_s"], lat, retries, r["errors"] + r["bad_reads"], cpu, disk))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["start", "run", "stop", "report"])
    p.add_argument("--dir", help="cluster directory on every node host")
    p.add_argument("--hosts", help="three ssh names, one per node (default: all on this host)")
    p.add_argument("--addrs", help="three listen addresses (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=7001, help="node i listens on port + i - 1")
    p.add_argument("--valsd", default=os.path.join(ROOT, "target/release/valsd"))
    p.add_argument("--vals-bench", default=os.path.join(ROOT, "target/release/vals-bench"))
    p.add_argument("--secret", default="bench-secret")
    p.add_argument("--revision", help="source revision to record (default: git describe of this checkout)")
    p.add_argument("--region-mib", type=int, default=65536, help="per-zone file size (sparse)")
    p.add_argument("--metrics-offset", type=int, default=100,
                   help="node i serves Prometheus metrics on its port + this (0: off)")
    p.add_argument("--node-line", action="append", default=[], help="extra node config line (repeatable)")
    p.add_argument("--out", default="file-bench-results")
    p.add_argument("--keys", type=int, default=4_000_000, help="dataset keys")
    p.add_argument("--vsize", type=int, default=1024, help="value bytes")
    p.add_argument("--threads", type=int, default=8, help="client coordinator threads")
    p.add_argument("--slots", type=int, default=32, help="requests in flight per thread")
    p.add_argument("--batch", type=int, default=8, help="keys per batch read")
    p.add_argument("--secs", type=float, default=60, help="seconds per phase")
    p.add_argument("--moderate", type=float, default=0.5, help="moderate rate as a fraction of saturated")
    p.add_argument("--phases", default=",".join(PHASES))
    p.add_argument("--prefix", default="fb/")
    p.add_argument("--busy-backoff-ms", type=int, default=1,
                   help="retry Busy/Overloaded after this backoff, doubling (0: count them as errors)")
    p.add_argument("--settle-max", type=float, default=900,
                   help="before each phase, wait up to this many seconds for valsd to go idle (0: never wait)")
    p.add_argument("--settle-cores", type=float, default=0.05,
                   help="idle threshold: CPU cores per valsd process, averaged over 5 s")
    p.add_argument("--wipe", action="store_true", help="stop: also delete --dir")
    args = p.parse_args()
    if args.command != "report" and not args.dir:
        p.error("--dir is required")
    {"start": cmd_start, "run": cmd_run, "stop": cmd_stop, "report": cmd_report}[args.command](args)


if __name__ == "__main__":
    main()
