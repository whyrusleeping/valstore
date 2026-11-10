#!/usr/bin/env python3
"""Run an instrumented benchmark campaign on a fleet from fleet.py (plans/cloud-aws-runbook.md).

The profile (scripts/cloud/profiles/<name>.json) lists the steps. Every step
is logged with start/end timestamps in events.jsonl, so the per-second host
samples (hostmon.py, running on every host for the whole session) can be cut
per step afterwards. Live checks poll each host's hostmon `latest.json` and
stop the campaign on an integrity or correctness signal.

  campaign.py run     --run RUN --artifact bench/cloud/artifacts/vals-<rev>.tar.gz [--from STEP] [--only STEP,...]
  campaign.py collect --run RUN          # pull every host's out/ (safe to repeat)
  campaign.py report  --run RUN          # per-step summaries + report.md
  campaign.py health  --run RUN          # one-shot live check of every host
  campaign.py stop    --run RUN          # stop valsd/PD/hostmon on every host

Artifacts: bench/cloud/runs/RUN/{events.jsonl, incidents.jsonl, steps/<step>/,
hosts/<host>/ (each host's out/ tree), summaries/, report.md}.
"""
import argparse
import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import cloudlib as cl  # noqa: E402

RUNS = os.path.join(cl.REPO, "bench", "cloud", "runs")
HOSTMON = os.path.join(HERE, "remote", "hostmon.py")


class Abort(Exception):
    pass


class Campaign:
    def __init__(self, run, artifact=None):
        self.run = run
        self.dir = os.path.join(RUNS, run)
        self.fleet, self.hosts = cl.load_fleet(self.dir)
        self.profile = self.fleet["profile_body"]
        self.cluster = self.profile["cluster"]
        self.local = bool(self.fleet.get("local"))
        self.nodes = [h for h in self.hosts if h.role == "node"]
        self.machines = [h for h in self.hosts if not h.colocated_with]
        self.clients = [h for h in self.hosts if h.role == "client"]
        self.artifact = artifact
        self.log = cl.EventLog(os.path.join(self.dir, "events.jsonl"))
        self.incidents = cl.EventLog(os.path.join(self.dir, "incidents.jsonl"))
        self.shards = cl.node_shards(self.cluster)
        self.abort_reason = None
        self.expect_down = {}
        self.latest = {}
        self.monitor_stop = threading.Event()
        self.hostmon_started = False
        self.proc_started = {}
        self.results = {}
        res = os.path.join(self.dir, "results.json")
        if os.path.exists(res):
            with open(res) as f:
                self.results = json.load(f)

    # ------------------------------------------------------------ helpers

    def sudo(self, host):
        return "" if host.ssh == cl.LOCAL else "sudo -E "

    def bin(self, host, name):
        return "%s/bin/%s" % (host.root, name)

    def env_lib(self, host):
        return "LD_LIBRARY_PATH=%s/lib" % host.root

    def peers(self):
        return cl.peer_spec(self.nodes, self.shards)

    def save_results(self):
        with open(os.path.join(self.dir, "results.json"), "w") as f:
            json.dump(self.results, f, indent=2, sort_keys=True)

    def inventory(self, host):
        path = os.path.join(self.dir, "hosts", host.name, "inventory.json")
        if not os.path.exists(path):
            cl.pull(host, "%s/out/inventory.json" % host.root, os.path.dirname(path) + "/")
        with open(path) as f:
            return json.load(f)

    def check_abort(self):
        if self.abort_reason:
            raise Abort(self.abort_reason)

    # ------------------------------------------------------------ live monitor

    def monitor(self):
        """Poll every host's hostmon latest.json; stop on critical signals or when monitoring goes blind."""
        blind = {}
        while not self.monitor_stop.wait(15):
            if not self.hostmon_started:
                continue
            for h in self.machines:
                raw = cl.try_sh(h, "cat %s/out/mon/latest.json 2>/dev/null; echo; date +%%s" % h.root, timeout=20)
                lines = raw.strip().splitlines()
                cur = None
                try:
                    cur = json.loads("\n".join(lines[:-1]))
                    age = float(lines[-1]) - float(cur.get("t", 0))
                except (ValueError, IndexError, TypeError):
                    age = None
                if cur is None or age is None or age > 60:
                    blind[h.name] = blind.get(h.name, 0) + 1
                    if blind[h.name] == 4:
                        msg = "hostmon snapshot missing or stale for 60 s (age %s)" % age
                        sev = "critical" if h.role == "node" else "warn"
                        self.incidents("incident", severity=sev, host=h.name, msg=msg, step=self.current_step)
                        if sev == "critical" and not self.abort_reason:
                            self.abort_reason = "%s: %s" % (h.name, msg)
                    continue
                blind[h.name] = 0
                down = set(self.expect_down.get(h.name, ())) | {"bench"}
                if float(cur.get("t", 0)) < self.proc_started.get(h.name, 0) + 15:
                    down |= {"node", "pd"}
                if h.role == "node" and not self.cluster_running:
                    down |= {"node", "pd"}
                for sev, msg in cl.evaluate_latest(self.latest.get(h.name), cur, down):
                    self.incidents("incident", quiet=(sev == "note"), severity=sev, host=h.name, msg=msg,
                                   step=self.current_step)
                    if sev == "critical" and not self.abort_reason:
                        self.abort_reason = "%s: %s" % (h.name, msg)
                self.latest[h.name] = cur

    # ------------------------------------------------------------ steps

    def step_prep(self, step):
        """Deploy binaries + remote scripts, host prep/inventory, start hostmon everywhere."""
        art_meta = None
        if not self.local:
            if not self.artifact:
                raise SystemExit("--artifact is required on a cloud fleet")
            with subprocess.Popen(["tar", "-xzOf", self.artifact, "--wildcards", "*/artifact.json"],
                                  stdout=subprocess.PIPE) as p:
                art_meta = json.loads(p.stdout.read())
            self.log("artifact", path=self.artifact, rev=art_meta["rev"], builder=art_meta.get("builder_instance_type"))

        def deploy(h):
            cl.sh(h, "mkdir -p %s/bin %s/cloud %s/run %s/out/cores %s/out/jobs" % ((h.root,) * 5))
            cl.push(h, os.path.join(HERE, "remote") + "/", "%s/cloud/remote/" % h.root, delete=True)
            if self.local or h.colocated_with:
                if self.local:
                    for b in ("valsd", "vals-bench"):
                        cl.push(h, os.path.join(cl.REPO, "target", "release", b), "%s/bin/%s" % (h.root, b))
                else:
                    cl.push(h, self.artifact, "%s/artifact.tar.gz" % h.root)
                    cl.sh(h, "cd %s && tar --strip-components=1 -xzf artifact.tar.gz && rm artifact.tar.gz" % h.root)
                cl.sh(h, "bash %s/cloud/remote/hostprep.sh --inventory-only --root %s" % (h.root, h.root), timeout=600)
                return
            cl.push(h, self.artifact, "%s/artifact.tar.gz" % h.root)
            cl.sh(h, "cd %s && tar --strip-components=1 -xzf artifact.tar.gz && rm artifact.tar.gz" % h.root)
            if art_meta.get("pmd_path"):
                cl.sh(h, "sudo mkdir -p %s && sudo ln -sfn %s/pmds %s"
                      % (shlex.quote(os.path.dirname(art_meta["pmd_path"])), h.root, shlex.quote(art_meta["pmd_path"])))
            flags = set(cl.sh(h, "grep -m1 '^flags' /proc/cpuinfo").split(":", 1)[1].split())
            missing = cl.missing_isa(art_meta["cpu_flags"], flags)
            if missing:
                raise RuntimeError("%s lacks ISA extensions the builder had: %s" % (h.name, " ".join(missing[:20])))
            cl.sh(h, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/hostprep.sh" % (h.root, h.root), timeout=1800)
            out = cl.sh(h, "%s ldd %s/bin/valsd | grep 'not found' || true" % (self.env_lib(h), h.root))
            if out.strip():
                raise RuntimeError("%s: unresolved libraries: %s" % (h.name, out))

        cl.parallel(deploy, self.machines)
        cl.parallel(deploy, [h for h in self.hosts if h.colocated_with])
        for h in self.hosts:
            self.inventory(h)
        self.start_hostmon()
        self.results["prep"] = {"artifact": art_meta and {k: v for k, v in art_meta.items() if k != "cpu_flags"}}

    def start_hostmon(self):
        def start(h):
            args = ["run", "--out", "%s/out/mon" % h.root, "--latest", "%s/out/mon/latest.json" % h.root,
                    "--interval", str(self.profile.get("hostmon_interval", 1.0))]
            if h.role == "node":
                args += ["--metrics", "node=http://127.0.0.1:%d/metrics" % h.metrics_port,
                         "--metrics", "pd=http://127.0.0.1:%d/metrics" % h.pd_metrics_port,
                         "--proc", "node=^%s" % re.escape("%s/bin/valsd node" % h.root),
                         "--proc", "pd=^%s" % re.escape("%s/bin/valsd pd" % h.root)]
            args += ["--proc", "bench=^[^ ]*/bin/vals-bench "]
            job = cl.RemoteJob(h, "hostmon", "python3 %s/cloud/remote/hostmon.py %s"
                               % (h.root, " ".join(shlex.quote(a) for a in args)), sudo=True)
            job.kill()
            job.start()
        cl.parallel(start, self.machines)
        self.hostmon_started = True
        self.log("hostmon", hosts=len(self.hosts))

    def step_net(self, step):
        """Network ceilings: iperf3 client->node, node mesh, fan-in; TCP_RR latency."""
        secs = step.get("secs", 10)
        streams = step.get("streams", 16)
        if len(self.machines) < 2:
            self.log("skipped", step=step["name"], reason="one machine: no network to measure")
            return
        cl.parallel(lambda h: cl.sh(h, "bash %s/cloud/remote/netbench.sh server" % h.root), self.hosts)
        try:
            pairs = [(c, n) for c in self.clients for n in self.nodes[:1]]
            for c, n in pairs:
                for s in (1, streams):
                    cl.sh(c, "bash %s/cloud/remote/netbench.sh client --peer %s --name %s-%s-p%d --streams %d --secs %d"
                          % (c.root, n.private_ip, c.name, n.name, s, s, secs), timeout=secs + 60)
                cl.sh(c, "bash %s/cloud/remote/netbench.sh rr --peer %s --name %s-%s --secs %d"
                      % (c.root, n.private_ip, c.name, n.name, secs), timeout=secs + 60)
                cl.sh(c, "sudo bash %s/cloud/remote/netbench.sh ping --peer %s --name %s-%s"
                      % (c.root, n.private_ip, c.name, n.name), timeout=120)
            # node mesh: every node sends to the next one at once (replication-like), distinct ports
            mesh = [(n, self.nodes[(i + 1) % len(self.nodes)], 5201 + i % 4) for i, n in enumerate(self.nodes)]
            cl.parallel(lambda t: cl.sh(t[0], "bash %s/cloud/remote/netbench.sh client --peer %s --name mesh-%s-%s --streams %d --secs %d --port %d"
                                        % (t[0].root, t[1].private_ip, t[0].name, t[1].name, streams, secs, t[2]),
                                        timeout=secs + 60), mesh)
            # fan-in: every client to node1 at once
            fan = [(c, self.nodes[0], 5201 + i % 4) for i, c in enumerate(self.clients)]
            cl.parallel(lambda t: cl.sh(t[0], "bash %s/cloud/remote/netbench.sh client --peer %s --name fanin-%s --streams %d --secs %d --port %d"
                                        % (t[0].root, t[1].private_ip, t[0].name, streams, secs, t[2]),
                                        timeout=secs + 60), fan)
        finally:
            cl.parallel(lambda h: cl.try_sh(h, "bash %s/cloud/remote/netbench.sh stop" % h.root), self.hosts)

    def step_rawdisk(self, step):
        tests = step.get("tests", "precondition,randread4k,randwrite4k,seqwrite128k,seqread128k,randrw70_4k")
        cl.parallel(lambda h: cl.sh(h, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/rawdisk.sh --secs %d --size %s --tests %s"
                                    % (h.root, h.root, step.get("secs", 30), step.get("size", "64G"), tests),
                                    timeout=3600), self.nodes)

    def bind(self):
        cl.parallel(lambda h: cl.sh(h, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/spdk-bind.sh bind --hugemem %d"
                                    % (h.root, h.root, self.cluster.get("hugemem_mib", 65536)), timeout=600), self.nodes)

    def pcis(self, h):
        inv = self.inventory(h)
        pcis = inv["instance_store_pcis"][: self.cluster["drives_per_node"]]
        sizes = {ns["size_bytes"] for d in inv["nvme"] if d["pci"] in pcis for ns in d["namespaces"]}
        serials = {d["pci"]: d["serial"] for d in inv["nvme"] if d["pci"] in pcis}
        return pcis, min(sizes) if sizes else None, serials

    def step_spdkbench(self, step):
        """Engine-on-drives ceilings with vals-bench mdwrite/mdread (destroys drive contents)."""
        self.bind()

        def bench(h):
            pcis, _, _ = self.pcis(h)
            for wl, n, vsize in step.get("runs", [["mdwrite", 40000000, 100], ["mdwrite", 4000000, 4096],
                                                  ["mdread", 20000000, 100]]):
                job = cl.RemoteJob(h, "%s-%s-%d" % (step["name"], wl, vsize),
                                   "%s spdk %s %d %d %s %d" % (self.bin(h, "vals-bench"), wl, n, vsize,
                                                               ",".join(pcis), step.get("ppd", 2)),
                                   env={"LD_LIBRARY_PATH": "%s/lib" % h.root}, sudo=True)
                job.start()
                try:
                    rc = job.wait(step.get("timeout", 1200))
                finally:
                    if job.alive() and not job.stop():
                        self.abort_reason = "%s: %s survived SIGKILL; drives may still be in use" % (h.name, job.name)
                self.save_output(step, h.name + "-" + wl + "-%d" % vsize, job.output())
                if rc != 0:
                    raise RuntimeError("%s %s rc=%s" % (h.name, wl, rc))

        cl.parallel(bench, self.nodes)

    def save_output(self, step, name, text):
        d = os.path.join(self.dir, "steps", step["name"])
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, name + ".log"), "w") as f:
            f.write(text)

    # ------------------------------------------------------------ cluster lifecycle

    cluster_running = False

    def write_configs(self):
        secret_path = os.path.join(self.dir, "secret")
        if not os.path.exists(secret_path):
            with open(secret_path, "w") as f:
                f.write(secrets.token_hex(16) + "\n")
        secret = open(secret_path).read().strip()

        def write(h):
            pcis, ns_bytes, _ = self.pcis(h) if self.cluster["backend"] == "spdk" else (None, None, None)
            conf = cl.node_conf(h, self.nodes, self.cluster, pcis, ns_bytes)
            cl.write_lines(h, "%s/run/node.conf" % h.root, conf)
            cl.write_lines(h, "%s/run/pd.conf" % h.root, cl.pd_conf(h, self.nodes, self.cluster))
            cl.write_lines(h, "%s/run/secret" % h.root, [secret])
            cl.sh(h, "%s%s %s node -c %s/run/node.conf --check-config > %s/out/node-check-config.txt 2>&1"
                  % (self.sudo(h), self.env_lib(h), self.bin(h, "valsd"), h.root, h.root))
            with open(os.path.join(self.dir, "configs-%s-node.conf" % h.name), "w") as f:
                f.write("\n".join(l for l in conf if not l.startswith("secret")) + "\n")

        cl.parallel(write, self.nodes)

    def provision(self):
        def prov(h):
            _, _, serials = self.pcis(h)
            devs = " ".join("--device %s=%s" % (p, shlex.quote(s)) for p, s in sorted(serials.items()))
            t0 = time.time()
            cl.sh(h, "cd %s/run && %s%s %s provision -c %s/run/node.conf %s --confirm-destroy node-%d > %s/out/provision.txt 2>&1"
                  % (h.root, self.sudo(h), self.env_lib(h), self.bin(h, "valsd"), h.root, devs, h.node_id, h.root),
                  timeout=3600)
            self.log("provisioned", host=h.name, secs=round(time.time() - t0, 1))

        cl.parallel(prov, self.nodes)

    def node_job(self, h):
        return cl.RemoteJob(h, "node-%s" % cl.stamp(), "%s node -c %s/run/node.conf" % (self.bin(h, "valsd"), h.root),
                            env={"LD_LIBRARY_PATH": "%s/lib" % h.root}, sudo=True, cwd="%s/run" % h.root)

    def pd_job(self, h):
        return cl.RemoteJob(h, "pd-%s" % cl.stamp(), "%s pd -c %s/run/pd.conf" % (self.bin(h, "valsd"), h.root),
                            env={"LD_LIBRARY_PATH": "%s/lib" % h.root}, cwd="%s/run" % h.root)

    def pids(self, h, what):
        """PIDs of this host's `valsd node|pd` (matched on argv, not with pgrep -f)."""
        return cl.try_sh(h, cl.valsd_pids_script(h.root, what), timeout=20).split()

    def signal(self, h, what, sig):
        pids = self.pids(h, what)
        if pids:
            cl.try_sh(h, "%skill -%s %s" % (self.sudo(h), sig, " ".join(pids)))
        return pids

    def start_node(self, h):
        self.proc_started[h.name] = time.time()
        self.node_job(h).start()

    def start_pd(self, h):
        self.proc_started[h.name] = time.time()
        self.pd_job(h).start()

    def wait_node_up(self, h, timeout=1800):
        """Seconds until the node's first anti-entropy sweep pass (engines open and serving).

        The metrics endpoint and the SPDK listeners come up before recovery
        finishes, so neither marks readiness; the sweep loop does. Resolution is
        one `sweep` interval.
        """
        t0 = time.time()
        metrics_at = None
        while time.time() - t0 < timeout:
            self.check_abort()
            out = cl.try_sh(h, "curl -sf --max-time 2 http://127.0.0.1:%d/metrics | "
                               "awk '/^vals_sweep_passes[ {]/{s+=$NF; n++} END{if (n) print s+0; else print \"none\"}'"
                            % h.metrics_port, timeout=20).strip()
            if out and metrics_at is None:
                metrics_at = time.time() - t0
            if out not in ("", "none") and float(out) > 0:
                ready = time.time() - t0
                self.log("node_ready", host=h.name, metrics_secs=round(metrics_at, 1), ready_secs=round(ready, 1))
                return ready
            time.sleep(1)
        raise RuntimeError("%s did not come up in %ds" % (h.name, timeout))

    def pd_status(self, h):
        return cl.try_sh(h, "%s %s admin -s %s/run/pd.sock status" % (self.env_lib(h), self.bin(h, "valsd"), h.root),
                         timeout=20)

    def pd_leader(self):
        for h in self.nodes:
            m = re.search(r"leader=(\d+)", self.pd_status(h))
            if m and m.group(1) != "0":
                return int(m.group(1))
        return None

    def step_cluster(self, step):
        if any(self.pids(h, "node") or self.pids(h, "pd") for h in self.nodes):
            self.log("stopping_leftover_cluster")
            self.stop_cluster(graceful=True)
        if self.cluster["backend"] == "spdk":
            self.bind()
        self.write_configs()
        if self.cluster["backend"] == "spdk":
            self.provision()
        t0 = time.time()
        cl.parallel(self.start_node, self.nodes)
        cl.parallel(self.start_pd, self.nodes)
        self.cluster_running = True
        ups = cl.parallel(self.wait_node_up, self.nodes)
        deadline = time.time() + 300
        while time.time() < deadline:
            out = cl.try_sh(self.nodes[0], "%s %s admin -s %s/run/pd.sock members" % (
                self.env_lib(self.nodes[0]), self.bin(self.nodes[0], "valsd"), self.nodes[0].root))
            if len([l for l in out.splitlines()[1:] if l.strip()]) >= len(self.nodes):
                break
            time.sleep(2)
        else:
            raise RuntimeError("PD never reported %d members" % len(self.nodes))
        self.log("cluster_up", secs=round(time.time() - t0, 1), node_up_secs=[round(u, 1) for u in ups],
                 leader=self.pd_leader(), peers=self.peers())
        self.results["cluster"] = {"up_secs": time.time() - t0, "peers": self.peers(), "shards": self.shards}

    def stop_cluster(self, graceful=True):
        self.cluster_running = False

        def stop(h):
            t0 = time.time()
            self.signal(h, "node", "TERM" if graceful else "KILL")
            for _ in range(240):
                if not self.pids(h, "node"):
                    break
                time.sleep(1)
            else:
                self.incidents("incident", severity="warn", host=h.name, msg="node did not exit within 240 s of SIGTERM")
            self.signal(h, "pd", "TERM")
            for _ in range(60):
                if not self.pids(h, "pd"):
                    break
                time.sleep(1)
            else:
                self.signal(h, "pd", "KILL")
            self.log("node_stopped", host=h.name, secs=round(time.time() - t0, 1))
        cl.parallel(stop, self.nodes)
        self.latest.clear()

    # ------------------------------------------------------------ load steps

    def bench_job(self, c, idx, step, workload, env):
        n = step["n"]
        cmd = "%s x %s %d %d %s %d %d %d" % (self.bin(c, "vals-bench"), workload, n, step.get("vsize", 100),
                                             shlex.quote(self.peers()), step.get("conc", 64),
                                             self.cluster["rf"], step.get("coords", 8))
        env = dict(env)
        env.update({"LD_LIBRARY_PATH": "%s/lib" % c.root,
                    "VALS_NETLOAD_KEY_PREFIX": "%s-c%d" % (step.get("prefix", step["name"]), idx),
                    "VALS_NET_TRANSPORT_STATS": "1"})
        return cl.RemoteJob(c, "%s-%s" % (step["name"], c.name.rsplit("-", 1)[-1]), cmd, env=env)

    def run_jobs(self, step, jobs, timeout, np_jobs=(), actions=()):
        """Start jobs together; run timed actions; live-check until all exit; return outputs."""
        for j in jobs:
            j.start()
        t0 = time.time()
        done = {}
        chaos = None
        self.chaos_cancel = threading.Event()
        self.chaos_finished = not actions
        if actions:
            chaos = threading.Thread(target=self.chaos_sequence, args=(step, actions), daemon=True)
            chaos.start()
        try:
            while len(done) < len(jobs):
                self.check_abort()
                el = time.time() - t0
                for j in jobs:
                    if j.name not in done:
                        rc = j.done()
                        if rc is not None:
                            done[j.name] = rc
                for j in np_jobs:
                    if j.name not in done:
                        tail = cl.try_sh(j.host, "tail -n 3 %s 2>/dev/null" % shlex.quote(j.log), timeout=20)
                        for s in cl.parse_np(tail)["samples"]:
                            for sev, msg in cl.np_sample_issues(s):
                                self.incidents("incident", severity=sev, host=j.host.name, msg=msg, step=step["name"])
                                self.abort_reason = self.abort_reason or msg
                if el > timeout:
                    raise RuntimeError("step %s timed out after %ds" % (step["name"], timeout))
                time.sleep(3)
        except BaseException:
            self.chaos_cancel.set()
            for j in jobs:
                j.kill()
            if chaos is not None:
                chaos.join(timeout=300)
            raise
        if chaos is not None:
            chaos.join(timeout=1800)
            if chaos.is_alive() or not self.chaos_finished:
                self.chaos_cancel.set()
                chaos.join(timeout=300)
                raise RuntimeError("chaos actions did not finish while the load ran")
        outs = {}
        for j in jobs:
            outs[j.host.name] = j.output()
            self.save_output(step, j.host.name, outs[j.host.name])
        return outs, done

    def profile_later(self, step, delay):
        def go():
            time.sleep(delay)
            if self.abort_reason:
                return
            h = self.nodes[0]
            self.log("profile", host=h.name, step=step["name"])
            cl.try_sh(h, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/profile.sh %s %d %s"
                      % (h.root, h.root, step["name"], step.get("profile_secs", 10),
                         shlex.quote("%s/bin/valsd node" % h.root)), timeout=900)
            if self.clients and not self.clients[0].colocated_with:
                c = self.clients[0]
                cl.try_sh(c, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/profile.sh %s-client %d --system"
                          % (c.root, c.root, step["name"], step.get("profile_secs", 10)), timeout=900)
        if step.get("profile") and not self.local:
            threading.Thread(target=go, daemon=True).start()

    def step_netload(self, step):
        """vals-bench netload family on every client at once; time-bound unless step has no secs."""
        workload = step.get("workload", "netload")
        env = {"VALS_NETWRITE_LAT_SAMPLE_EVERY": step.get("lat_every", 64),
               "VALS_NETREAD_LAT_SAMPLE_EVERY": step.get("lat_every", 64)}
        if step.get("write_only", workload in ("netload", "netbatchwrite")):
            env["VALS_NETLOAD_SKIP_READ_PHASE"] = "1"
        if step.get("secs"):
            env["VALS_NETWRITE_SECS" if workload in ("netload", "netbatchwrite") else "VALS_NETREAD_SECS"] = step["secs"]
        for k, v in step.get("env", {}).items():
            env[k] = v
        jobs = [self.bench_job(c, i + 1, step, workload, env) for i, c in enumerate(self.clients)]
        self.profile_later(step, step.get("profile_at", max(5, step.get("secs", 30) // 3)))
        outs, rcs = self.run_jobs(step, jobs, step.get("timeout", step.get("secs", 600) + 600))
        parsed = {h: cl.parse_netload(o) for h, o in outs.items()}
        agg = cl.merge_netload(parsed.values())
        bad = [h for h, rc in rcs.items() if rc != 0]
        res = {"per_client": parsed, "total": agg, "rc": rcs}
        self.results[step["name"]] = res
        headline = {k: agg.get(k) for k in ("write_ops_s", "write_errors", "read_keys_s", "read_errors",
                                            "read_misses", "read_mismatches", "verify_bad") if k in agg}
        self.log("result", step=step["name"], **headline)
        if agg.get("verify_bad") or agg.get("read_mismatches"):
            self.abort_reason = "%s: value mismatches %s" % (step["name"], headline)
        if bad:
            raise RuntimeError("clients failed: %s" % bad)
        if step.get("strict"):
            problems = strict_problems(step, parsed, len(self.clients))
            if problems:
                self.abort_reason = "%s: %s" % (step["name"], "; ".join(problems))

    def np_env(self, step, idx):
        env = {"VALS_NP_THREADS": step.get("threads", 8), "VALS_NP_SLOTS": step.get("slots", 16),
               "VALS_NP_KEYS": step.get("keys", 4096), "VALS_NP_SECS": step.get("secs", 60),
               "VALS_NP_SAMPLE_SECS": 1, "VALS_NP_READ_PCT": step.get("read_pct", 65),
               "VALS_NP_MULTI_PCT": step.get("multi_pct", 10), "VALS_NP_MULTI": step.get("multi", 8),
               "VALS_NP_VSIZE": step.get("vsize", 256), "VALS_NP_PERSISTENCE": step.get("persistence", "buffered"),
               "VALS_NP_PREFIX": "%s-c%d/" % (step.get("prefix", step["name"]), idx),
               "VALS_NP_FD": step.get("fd", "default"), "VALS_NP_HEDGE_MS": step.get("hedge_ms", 50),
               "VALS_NP_BUSY_BACKOFF_MS": step.get("busy_backoff_ms", 2)}
        rate = step.get("rate", 0)
        if step.get("rate_from"):
            ref = self.results.get(step["rate_from"], {}).get("total_ops_s")
            if not ref:
                raise RuntimeError("rate_from %s has no measured rate" % step["rate_from"])
            rate = ref * step.get("rate_frac", 0.5) / max(1, len(self.clients))
        env["VALS_NP_RATE"] = rate
        return env

    def np_jobs(self, step):
        jobs = []
        for i, c in enumerate(self.clients):
            env = self.np_env(step, i + 1)
            env["LD_LIBRARY_PATH"] = "%s/lib" % c.root
            jobs.append(cl.RemoteJob(c, "%s-%s" % (step["name"], c.name.rsplit("-", 1)[-1]),
                                     "%s vals netprogress %s default" % (self.bin(c, "vals-bench"), shlex.quote(self.peers() + ";")),
                                     env=env))
        return jobs

    def step_np(self, step):
        """netprogress: verified mixed load with per-second samples; optional chaos actions."""
        jobs = self.np_jobs(step)
        self.profile_later(step, step.get("profile_at", 30))
        outs, rcs = self.run_jobs(step, jobs, step.get("secs", 60) + step.get("timeout_extra", 900),
                                  np_jobs=jobs, actions=step.get("actions", []))
        parsed = {h: cl.parse_np(o) for h, o in outs.items()}
        ops = 0.0
        for p in parsed.values():
            s = p.get("summary", {})
            el = s.get("elapsed") or 1
            ops += sum(v.get("ok", 0) for v in s.get("ops", {}).values()) / el
        verify = {h: p.get("verify") for h, p in parsed.items()}
        self.results[step["name"]] = {"total_ops_s": ops, "verify": verify, "rc": rcs,
                                      "summary": {h: p.get("summary") for h, p in parsed.items()}}
        self.log("result", step=step["name"], total_ops_s=round(ops), verify_ok=all(v and v.get("ok") for v in verify.values()))
        if not all(v and v.get("ok") for v in verify.values()):
            self.abort_reason = "%s: netprogress verification failed %s" % (step["name"], verify)

    # ------------------------------------------------------------ chaos actions

    def node_by_name(self, name):
        if name == "pd-leader":
            lid = self.pd_leader()
            return next(h for h in self.nodes if h.node_id == lid)
        return next(h for h in self.nodes if h.name.endswith(name))

    def chaos_sequence(self, step, actions):
        """Run chaos actions one after another; each waits `wait` s after the previous one settled."""
        for a in actions:
            if self.chaos_cancel.wait(a.get("wait", 60)) or self.abort_reason:
                return
            try:
                self._action(step, a)
            except Exception as e:  # noqa: BLE001 - recorded as an incident
                self.incidents("incident", severity="critical", host=a.get("target"),
                               msg="action %s failed: %s" % (a, e), step=step["name"])
                self.abort_reason = self.abort_reason or "chaos action failed: %s" % e
                return
        self.chaos_finished = True
        self.log("chaos_done", step=step["name"])

    def _action(self, step, a):
        h = self.node_by_name(a["target"])
        kind, down = a["action"], a.get("down_secs", 30)
        self.log("chaos", step=step["name"], action=kind, host=h.name, down_secs=down)
        if kind in ("kill9", "sigterm"):
            self.expect_down[h.name] = ("node",)
            t0 = time.time()
            self.signal(h, "node", "KILL" if kind == "kill9" else "TERM")
            while self.pids(h, "node"):
                time.sleep(0.5)
            self.log("chaos_down", host=h.name, stop_secs=round(time.time() - t0, 2))
            self.chaos_cancel.wait(down)
            t1 = time.time()
            self.start_node(h)
            up = self.wait_node_up(h)
            self.log("chaos_up", host=h.name, restart_to_ready_secs=round(time.time() - t1, 2))
        elif kind == "pd-kill":
            self.expect_down[h.name] = ("pd",)
            self.signal(h, "pd", "KILL")
            while self.pids(h, "pd"):
                time.sleep(0.5)
            self.chaos_cancel.wait(down)
            self.start_pd(h)
            self.log("chaos_up", host=h.name, pd_leader=self.pd_leader())
        elif kind == "freeze":
            self.expect_down[h.name] = ()
            self.signal(h, "node", "STOP")
            try:
                self.chaos_cancel.wait(down)
            finally:
                self.signal(h, "node", "CONT")
            self.log("chaos_up", host=h.name)
        elif kind == "blackhole":
            others = [o.private_ip for o in self.hosts if o is not h]
            rules = " ".join("-s %s" % ip for ip in others)
            add = "; ".join("sudo iptables -I INPUT -s %s -j DROP; sudo iptables -I OUTPUT -d %s -j DROP" % (ip, ip)
                            for ip in others)
            rm = "; ".join("sudo iptables -D INPUT -s %s -j DROP; sudo iptables -D OUTPUT -d %s -j DROP" % (ip, ip)
                           for ip in others)
            cl.sh(h, add)
            try:
                self.chaos_cancel.wait(down)
            finally:
                cl.sh(h, rm)
            self.log("chaos_up", host=h.name, rules=rules)
        else:
            raise ValueError("unknown chaos action %s" % kind)
        self.settle_ae(h, a.get("settle_timeout", 600))
        self.chaos_cancel.wait(30)
        self.expect_down.pop(h.name, None)
        self.latest.pop(h.name, None)

    def settle_ae(self, h, timeout):
        """Seconds until the host's anti-entropy dirty-bucket gauge reaches 0 (repair caught up)."""
        t0 = time.time()
        while time.time() - t0 < timeout:
            out = cl.try_sh(h, "curl -sf --max-time 2 http://127.0.0.1:%d/metrics | "
                               "awk '/^vals_ae_dirty_buckets[ {]/{s+=$NF; n++} END{if (n) print s+0; else print \"none\"}'"
                            % h.metrics_port, timeout=20).strip()
            if out == "0":
                self.log("ae_settled", host=h.name, secs=round(time.time() - t0, 1))
                return time.time() - t0
            time.sleep(2)
        self.incidents("incident", severity="warn", host=h.name, msg="AE did not settle in %ds" % timeout)
        return None

    # ------------------------------------------------------------ verification / teardown

    def step_settle(self, step):
        for h in self.nodes:
            self.settle_ae(h, step.get("timeout", 600))

    def step_converge(self, step):
        """Every node holds every key at RF = node count: compare live_keys per node from zone-stats."""
        time.sleep(step.get("wait", 20))
        totals = {}
        for h in self.nodes:
            out = cl.try_sh(h, "%s%s %s admin -s %s/run/node.sock zone-stats" % (
                self.sudo(h), self.env_lib(h), self.bin(h, "valsd"), h.root))
            self.save_output(step, h.name + "-zone-stats", out)
            live = 0
            for line in out.splitlines():
                parts = line.strip().split(",")
                if len(parts) == 6 and parts[4].isdigit():
                    live += int(parts[4])
            totals[h.name] = live
        self.results[step["name"]] = {"live_keys": totals}
        self.log("converge", **totals)
        if len(self.nodes) == self.cluster["rf"] and len(set(totals.values())) > 1:
            self.incidents("incident", severity="warn", host="cluster",
                           msg="live_keys differ across full replicas: %s" % totals, step=step["name"])

    def step_restart(self, step):
        """Whole-cluster graceful stop and cold start: shutdown and reopen times with the data on the drives."""
        self.stop_cluster(graceful=True)
        t0 = time.time()
        cl.parallel(self.start_node, self.nodes)
        cl.parallel(self.start_pd, self.nodes)
        self.cluster_running = True
        self.latest.clear()
        ups = cl.parallel(self.wait_node_up, self.nodes)
        self.log("cluster_restarted", secs=round(time.time() - t0, 1), node_up_secs=[round(u, 1) for u in ups])
        self.results[step["name"]] = {"node_up_secs": ups}

    def step_stop(self, step):
        self.stop_cluster(graceful=True)

    def step_nvme_post(self, step):
        if self.local or self.cluster["backend"] != "spdk":
            return
        cl.parallel(lambda h: cl.try_sh(h, "sudo -E env VALS_CLOUD_ROOT=%s bash %s/cloud/remote/spdk-bind.sh reset && "
                                        "mkdir -p %s/out/nvme-post && for d in /dev/nvme[0-9]*n1; do "
                                        "sudo nvme smart-log -o json $d > %s/out/nvme-post/$(basename $d).json 2>&1; done"
                                        % (h.root, h.root, h.root, h.root), timeout=600), self.nodes)

    def step_sleep(self, step):
        time.sleep(step.get("secs", 10))

    # ------------------------------------------------------------ driver

    current_step = None

    def run_steps(self, steps):
        mon = threading.Thread(target=self.monitor, daemon=True)
        mon.start()
        self.log("campaign_start", run=self.run, steps=[s["name"] for s in steps])
        status = "ok"
        try:
            for step in steps:
                self.check_abort()
                self.current_step = step["name"]
                self.log("step_start", step=step["name"], step_kind=step["kind"])
                t0 = time.time()
                try:
                    getattr(self, "step_" + step["kind"])(step)
                except Abort:
                    self.log("step_end", step=step["name"], status="aborted", secs=round(time.time() - t0, 1))
                    raise
                except Exception as e:
                    self.log("step_end", step=step["name"], status="failed", error=str(e)[:500],
                             secs=round(time.time() - t0, 1))
                    if step.get("required", True):
                        raise
                    continue
                finally:
                    self.save_results()
                self.check_abort()
                self.log("step_end", step=step["name"], status="ok", secs=round(time.time() - t0, 1))
        except Abort as e:
            status = "aborted: %s" % e
        except Exception as e:  # noqa: BLE001 - recorded and re-raised after collection
            status = "failed: %s" % e
        finally:
            self.monitor_stop.set()
            self.log("campaign_end", status=status)
            self.save_results()
        return status


def strict_problems(step, parsed, nclients):
    """A verification/prefill step must be complete: every client parsed, no errors, misses or mismatches,
    and (for untimed reads) every one of the n keys per client read."""
    out = []
    if len(parsed) != nclients:
        out.append("results from %d of %d clients" % (len(parsed), nclients))
    for host, r in sorted(parsed.items()):
        if not r:
            out.append("%s: no parseable summary" % host)
            continue
        for k in ("write_errors", "read_errors", "read_misses", "read_mismatches", "verify_bad"):
            if r.get(k):
                out.append("%s: %s=%d" % (host, k, r[k]))
        if "read_keys" in r and not step.get("secs") and r["read_keys"] != step["n"]:
            out.append("%s: read %d of %d keys" % (host, r["read_keys"], step["n"]))
        if "write_ops" in r and not step.get("secs") and r["write_ops"] != step["n"]:
            out.append("%s: wrote %d of %d keys" % (host, r["write_ops"], step["n"]))
    return out


def select_steps(profile, args):
    steps = profile["steps"]
    if args.only:
        names = args.only.split(",")
        steps = [s for s in steps if s["name"] in names]
    elif args.from_step:
        names = [s["name"] for s in steps]
        if args.from_step not in names:
            raise SystemExit("no step %s" % args.from_step)
        steps = steps[names.index(args.from_step):]
    return steps


def cmd_run(args):
    c = Campaign(args.run, args.artifact)
    steps = select_steps(c.profile, args)
    if c.local:
        steps = [s for s in steps if not s.get("cloud_only")]
    c.cluster_running = bool(c.nodes) and all(c.pids(h, "node") for h in c.nodes)
    c.hostmon_started = not any(s["kind"] == "prep" for s in steps)
    if c.hostmon_started:
        dead = [h for h in c.machines if not cl.RemoteJob(h, "hostmon", "true").alive()]
        if dead:
            c.log("hostmon_restart", hosts=[h.name for h in dead])
            c.start_hostmon()
    status = c.run_steps(steps)
    if not args.no_collect:
        collect(c)
        report(c)
    if c.local:
        stop_all(c)
    elif status != "ok":
        print("fleet left running for diagnosis; `fleet.py down --run %s` when done" % args.run)
    print("campaign %s: %s" % (args.run, status))
    if status != "ok":
        sys.exit(1)


def collect(c):
    def pull(h):
        cl.try_sh(h, "sudo chown -R $(id -u):$(id -g) %s/out 2>/dev/null; true" % h.root if h.ssh != cl.LOCAL else "true")
        rc = cl.pull(h, "%s/out/" % h.root, os.path.join(c.dir, "hosts", h.name) + "/")
        c.log("collected", host=h.name, rc=rc)
    cl.parallel(pull, c.hosts)


def report(c):
    """Cut every host's samples per step (hostmon summarize) and write report.md."""
    windows = cl.step_windows(cl.read_events(c.log.path))
    sdir = os.path.join(c.dir, "summaries")
    os.makedirs(sdir, exist_ok=True)
    rows = []
    for name, (t0, t1, status) in windows.items():
        if t1 is None:
            continue
        row = {"step": name, "status": status, "secs": round(t1 - t0, 1), "hosts": {}}
        for h in c.machines:
            mon = os.path.join(c.dir, "hosts", h.name, "mon")
            if not os.path.isdir(mon):
                continue
            res = subprocess.run([sys.executable, HOSTMON, "summarize", mon, "--from", str(t0), "--to", str(t1), "--json"],
                                 capture_output=True, text=True)
            if res.returncode == 0 and res.stdout.strip():
                with open(os.path.join(sdir, "%s.%s.json" % (name, h.name)), "w") as f:
                    f.write(res.stdout)
                try:
                    row["hosts"][h.name] = json.loads(res.stdout)
                except ValueError:
                    pass
        row["result"] = c.results.get(name)
        rows.append(row)
    with open(os.path.join(c.dir, "report.json"), "w") as f:
        json.dump(rows, f, indent=2, sort_keys=True, default=str)
    lines = ["# Campaign %s" % c.run, "", "| step | status | secs | client result |", "|---|---|---|---|"]
    for r in rows:
        res = r["result"] or {}
        tot = res.get("total") or {}
        bits = []
        for k in ("write_ops_s", "read_keys_s", "write_errors", "read_errors", "read_mismatches"):
            if k in tot:
                bits.append("%s=%s" % (k, round(tot[k])))
        if "total_ops_s" in res:
            bits.append("ops/s=%d" % res["total_ops_s"])
        lines.append("| %s | %s | %s | %s |" % (r["step"], r["status"], r["secs"], " ".join(bits)))
    lines += ["", "## Hosts per step", "",
              "| step | host | cpu busy % | valsd cores | top threads (cores) | server ops/s | net rx/tx Gbit/s | ENA exceeded |",
              "|---|---|---|---|---|---|---|---|"]
    for r in rows:
        for hname, s in sorted(r["hosts"].items()):
            lines.append("| %s | %s |" % (r["step"], hname) + host_cells(s))
    incidents = cl.read_events(os.path.join(c.dir, "incidents.jsonl"))
    loud = [i for i in incidents if i.get("severity") != "note"]
    notes = {}
    for i in incidents:
        if i.get("severity") == "note":
            key = re.sub(r"[+-]?\d+", "N", i.get("msg", ""))
            notes[key] = notes.get(key, 0) + 1
    lines += ["", "## Incidents (%d warn/critical, %d notes)" % (len(loud), len(incidents) - len(loud)), ""]
    lines += ["- %s %s %s [%s]: %s" % (i["utc"], i.get("severity"), i.get("host"), i.get("step"), i.get("msg"))
              for i in loud[:300]]
    lines += ["- note x%d: %s" % (n, k) for k, n in sorted(notes.items(), key=lambda kv: -kv[1])]
    with open(os.path.join(c.dir, "report.md"), "w") as f:
        f.write("\n".join(lines) + "\n")
    print("report: %s" % os.path.join(c.dir, "report.md"))


def host_cells(s):
    """Markdown cells for one host's hostmon summary of one step."""
    cpu = s.get("cpu", {})
    node = s.get("procs", {}).get("node") or {}
    bench = s.get("procs", {}).get("bench") or {}
    proc = node if node.get("cores") is not None else bench
    groups = sorted((proc.get("threads_by_group") or {}).items(), key=lambda kv: -kv[1])[:3]
    ops = (((s.get("metrics") or {}).get("node") or {}).get("counters") or {}).get("vals_server_ops", {}).get("rate")
    rx = sum(v.get("rx_gbps") or 0 for k, v in (s.get("net") or {}).items() if k != "lo")
    tx = sum(v.get("tx_gbps") or 0 for k, v in (s.get("net") or {}).items() if k != "lo")
    ena = 0
    for counters in (s.get("ena") or {}).values():
        if isinstance(counters, dict):
            ena += sum(v for k, v in counters.items() if k.endswith("exceeded") and isinstance(v, (int, float)))
    fmt = lambda v, f="%.1f": "" if v is None else f % v  # noqa: E731
    return " %s | %s | %s | %s | %.2f / %.2f | %s |" % (
        fmt(cpu.get("busy_pct")), fmt(proc.get("cores"), "%.2f"),
        ", ".join("%s %.2f" % kv for kv in groups), fmt(ops, "%.0f"), rx, tx, ena or "")


def cmd_collect(args):
    collect(Campaign(args.run))


def cmd_report(args):
    report(Campaign(args.run))


def cmd_health(args):
    c = Campaign(args.run)
    for h in c.machines:
        raw = cl.try_sh(h, "cat %s/out/mon/latest.json 2>/dev/null" % h.root, timeout=20)
        try:
            cur = json.loads(raw)
        except ValueError:
            print("%-24s no hostmon snapshot" % h.name)
            continue
        issues = cl.evaluate_latest(None, cur, ("bench",))
        procs = " ".join("%s=%s" % (k, "up" if v.get("alive") else "DOWN") for k, v in cur.get("procs", {}).items())
        print("%-24s load1=%-6s %s %s" % (h.name, cur.get("load1"), procs,
                                          "; ".join("%s: %s" % i for i in issues) or "ok"))


def stop_all(c):
    c.stop_cluster(graceful=True)
    cl.parallel(lambda h: cl.RemoteJob(h, "hostmon", "true", sudo=True).kill(), c.machines)


def cmd_stop(args):
    stop_all(Campaign(args.run))


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("run")
    s.add_argument("--run", required=True)
    s.add_argument("--artifact")
    s.add_argument("--from", dest="from_step")
    s.add_argument("--only")
    s.add_argument("--no-collect", action="store_true")
    for name in ("collect", "report", "health", "stop"):
        sub.add_parser(name).add_argument("--run", required=True)
    args = ap.parse_args()
    globals()["cmd_" + args.cmd](args)


if __name__ == "__main__":
    main()
