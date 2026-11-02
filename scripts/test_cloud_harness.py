"""Unit tests for the cloud harness helpers (scripts/cloud/cloudlib.py, campaign/fleet logic)."""
import datetime
import importlib.util
import json
import os
import sys
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


cl = load("cloudlib", os.path.join(HERE, "cloud", "cloudlib.py"))
PROFILES = os.path.join(HERE, "cloud", "profiles")


def nodes(n=3, base=7000):
    return [cl.Host("n%d" % i, "node", "ubuntu@x", "10.0.0.%d" % i, "/r", base, node_id=i) for i in range(1, n + 1)]


SPDK = {"backend": "spdk", "drives_per_node": 8, "parts_per_drive": 2, "rf": 3, "default_zone_fraction": 0.6,
        "named_zone_fraction": 0.15}


def conf_dict(lines):
    out = {}
    for line in lines:
        k, _, v = line.partition(" = ")
        out.setdefault(k, []).append(v)
    return out


class ConfigTests(unittest.TestCase):
    def test_peer_spec_lists_every_shard(self):
        spec = cl.peer_spec(nodes(2), 2)
        self.assertEqual(spec, "1@10.0.0.1:7001,10.0.0.1:7002;2@10.0.0.2:7001,10.0.0.2:7002")

    def test_region_mib_fits_partitions(self):
        ns = 3750000000000
        r = cl.region_mib(ns, 2)
        self.assertLessEqual(2 * r * (1 << 20), ns)
        self.assertGreater(r, 1700000)
        with self.assertRaises(ValueError):
            cl.region_mib(1 << 30, 2)

    def test_spdk_node_conf(self):
        ns = nodes()
        pcis = ["0000:%02x:00.0" % i for i in range(8)]
        c = conf_dict(cl.node_conf(ns[0], ns, SPDK, pcis, 3750000000000))
        self.assertEqual(len(c["listen"]), 16)
        self.assertEqual(c["pci"], pcis)
        self.assertEqual(c["backend"], ["spdk"])
        self.assertEqual(len(c["peer"]), 2)
        self.assertTrue(all(p.count(",") == 15 for p in c["peer"]))
        region = int(c["region_mib"][0])
        default = int(c["zone_region_mib_override"][0].split(":")[1])
        named = int(c["zone_region_mib"][0])
        self.assertTrue(c["zone_region_mib_override"][0].startswith("0:"))
        self.assertLess(default + 2 * named, region)
        self.assertEqual(c["metrics"], ["0.0.0.0:7476"])
        self.assertNotIn("format", c)

    def test_spdk_conf_needs_every_drive(self):
        ns = nodes()
        with self.assertRaises(ValueError):
            cl.node_conf(ns[0], ns, SPDK, ["0000:01:00.0"], 3750000000000)

    def test_file_node_conf_single_listen(self):
        ns = nodes(3, 27000)
        c = conf_dict(cl.node_conf(ns[1], ns, {"backend": "file", "rf": 3}))
        self.assertEqual(c["listen"], ["10.0.0.2:27001"])
        self.assertEqual(c["data_dir"], ["data"])
        self.assertEqual(c["peer"], ["1 @ 10.0.0.1:27001", "3 @ 10.0.0.3:27001"])

    def test_pd_conf_three_voters_one_campaigner(self):
        ns = nodes()
        confs = [conf_dict(cl.pd_conf(n, ns, SPDK)) for n in ns]
        self.assertEqual([c.get("campaign") for c in confs], [["true"], None, None])
        for c in confs:
            self.assertEqual(len(c["pd_peer"]), 3)
            self.assertEqual(len(c["data_node"]), 3)
            self.assertEqual(c["listen"], ["0.0.0.0:7100"])
        self.assertIn("1 @ 10.0.0.1:7100", confs[2]["pd_peer"])

    def test_profiles_are_consistent(self):
        kinds = {"prep", "net", "rawdisk", "spdkbench", "cluster", "netload", "np", "settle", "converge",
                 "restart", "stop", "nvme_post", "sleep"}
        for name in os.listdir(PROFILES):
            with open(os.path.join(PROFILES, name)) as f:
                p = json.load(f)
            names = [s["name"] for s in p["steps"]]
            self.assertEqual(len(names), len(set(names)), name)
            for s in p["steps"]:
                self.assertIn(s["kind"], kinds, (name, s))
                if s.get("rate_from"):
                    self.assertLess(names.index(s["rate_from"]), names.index(s["name"]))
                if s["kind"] == "netload":
                    self.assertEqual(s["n"] % s.get("coords", 8), 0, (name, s["name"]))
                if s.get("prefix") == "pf" and s["kind"] == "netload":
                    self.assertEqual(s["n"], next(x["n"] for x in p["steps"] if x["name"] == "prefill"))
            for r in p.get("fleet", {}).values():
                self.assertIn(r["type"], cl.PRICES)


class FleetTests(unittest.TestCase):
    def test_user_data_arms_deadline_before_anything_else(self):
        fleet = load("fleet", os.path.join(HERE, "cloud", "fleet.py"))
        ud = fleet.user_data(3 * 3600)
        lines = ud.splitlines()
        self.assertEqual(lines[0], "#!/bin/bash")
        self.assertIn("+ 10800 )) > /var/lib/vals-deadline-epoch", lines[1])
        enable = ud.index("systemctl enable --now vals-deadline.timer")
        self.assertLess(enable, ud.index("apt-get"))
        self.assertIn("WantedBy=timers.target", ud)
        self.assertIn("exec /usr/bin/systemctl poweroff", ud)
        self.assertLess(ud.index("apt-get"), ud.index("vals-userdata-done"))

    def test_isa_check_ignores_hypervisor_flags(self):
        builder = ["fpu", "hypervisor", "avx512f", "sse4_2", "aes", "constant_tsc"]
        self.assertEqual(cl.missing_isa(builder, ["fpu", "avx512f", "sse4_2", "aes"]), [])
        self.assertEqual(cl.missing_isa(builder, ["fpu", "sse4_2", "aes"]), ["avx512f"])


class StrictTests(unittest.TestCase):
    def test_strict_verification_requires_every_key(self):
        camp = load("campaign", os.path.join(HERE, "cloud", "campaign.py"))
        step = {"n": 100}
        ok = {"read_keys": 100, "read_errors": 0, "read_misses": 0, "read_mismatches": 0}
        self.assertEqual(camp.strict_problems(step, {"c1": ok, "c2": ok}, 2), [])
        missing = dict(ok, read_misses=3)
        self.assertEqual(len(camp.strict_problems(step, {"c1": ok, "c2": missing}, 2)), 1)
        short = dict(ok, read_keys=90)
        self.assertIn("read 90 of 100", camp.strict_problems(step, {"c1": short, "c2": ok}, 2)[0])
        self.assertIn("results from 1 of 2", camp.strict_problems(step, {"c1": ok}, 2)[0])
        self.assertTrue(camp.strict_problems(step, {"c1": {}, "c2": ok}, 2))
        timed = {"n": 100, "secs": 60}
        self.assertEqual(camp.strict_problems(timed, {"c1": dict(ok, read_keys=5000)}, 1), [])


class CostTests(unittest.TestCase):
    def test_fleet_hourly_and_vcpus(self):
        roles = [{"type": "i4i.metal", "count": 3}, {"type": "c6in.16xlarge", "count": 2, "market": "spot"}]
        self.assertAlmostEqual(cl.fleet_hourly(roles), 3 * 10.982 + 2 * 0.992)
        self.assertEqual(cl.fleet_vcpus(roles), 384)
        self.assertEqual(cl.fleet_vcpus(roles, "spot"), 128)

    def test_accrued_has_minute_minimum(self):
        now = datetime.datetime(2026, 11, 2, 12, 0, tzinfo=datetime.timezone.utc)
        hosts = [{"launch_time": "2026-11-02T11:00:00Z", "price_per_hour": 10.0},
                 {"launch_time": "2026-11-02T11:59:50+00:00", "price_per_hour": 60.0},
                 {"launch_time": "2026-11-02T10:00:00Z", "end_time": "2026-11-02T10:30:00Z", "price_per_hour": 2.0}]
        self.assertAlmostEqual(cl.accrued(hosts, now), 10.0 + 1.0 + 1.0)


NETLOAD = """--- netload: 3 nodes x 16 shards, RF=3, W=Quorum(2), R=2 ---
  write Quorum(2)  : 1234567 logical writes/s, 2469134 replica writes/s, 38580 calls/s aggregate  (74074020 logical writes, 2314813 calls, 3 errors, batch=32, persistence=buffered)
  write ack latency : 36169 samples (every 64 calls/worker), p50=812345 p99=2500000 p99.9=4000000 max=9000000 ns
  read  R=2 strong : 4960000 keys/s aggregate  (297600000 keys, 1162500 batch calls, 19375 calls/s, 0 errors, 0 misses, 0 mismatches)
  batch latency      : 18164 samples (every 64 calls/worker), p50=1200000 p99=3000000 p99.9=5000000 max=8000000 ns
  correctness       : 1000 matched / 0 mismatched (of 1000 sampled)
"""


class ParseTests(unittest.TestCase):
    def test_parse_netload(self):
        r = cl.parse_netload(NETLOAD)
        self.assertEqual(r["write_ops_s"], 1234567)
        self.assertEqual(r["write_errors"], 3)
        self.assertEqual(r["read_keys_s"], 4960000)
        self.assertEqual(r["read_mismatches"], 0)
        self.assertAlmostEqual(r["write_lat_us"]["p99"], 2500.0)
        self.assertAlmostEqual(r["batch_lat_us"]["p50"], 1200.0)
        self.assertEqual((r["verify_ok"], r["verify_bad"]), (1000, 0))

    def test_merge_sums_clients(self):
        r = cl.parse_netload(NETLOAD)
        agg = cl.merge_netload([r, r])
        self.assertEqual(agg["read_keys_s"], 2 * 4960000)
        self.assertEqual(len(agg["write_lat_us_by_client"]), 2)

    def test_parse_np(self):
        text = "\n".join([
            'NP_CONFIG {"threads":2}',
            'noise',
            'NP_SAMPLE {"elapsed":1.0,"bad":0,"ops":{"write":{"ok":10}}}',
            'NP_SAMPLE {"elapsed":2.0,"bad":2,"ops":{}}',
            'NP_SUMMARY {"elapsed":2.0,"ops":{"write":{"ok":20}}}',
            'NP_VERIFY {"ok":false,"checked":5,"bad":1,"errors":0}',
        ])
        r = cl.parse_np(text)
        self.assertEqual(len(r["samples"]), 2)
        self.assertEqual(r["config"]["threads"], 2)
        self.assertFalse(r["verify"]["ok"])
        self.assertEqual(cl.np_sample_issues(r["samples"][1])[0][0], "critical")
        self.assertEqual(cl.np_sample_issues(r["samples"][0]), [])


def snap(restarts=0, alive=True, corrupt=0, busy=0, ena=0, up=True, swap=0):
    return {"procs": {"node": {"pid": 5, "alive": alive, "restarts": restarts}},
            "metrics": {"node": {"up": up, "watch": {"vals_store_corrupt_reads_total": corrupt,
                                                     "vals_busy_rejections": busy}}},
            "nic": {"ens5": {"pps_allowance_exceeded": ena}},
            "mem": {"swap_used_bytes": swap}}


class LiveCheckTests(unittest.TestCase):
    def sev(self, issues):
        return sorted({s for s, _ in issues})

    def test_quiet_host_has_no_issues(self):
        self.assertEqual(cl.evaluate_latest(snap(), snap()), [])
        self.assertEqual(cl.evaluate_latest(None, snap()), [])

    def test_integrity_counter_is_critical(self):
        self.assertEqual(self.sev(cl.evaluate_latest(snap(), snap(corrupt=1))), ["critical"])
        self.assertEqual(self.sev(cl.evaluate_latest(None, snap(corrupt=1))), ["critical"])

    def test_dead_or_restarted_process(self):
        self.assertEqual(self.sev(cl.evaluate_latest(snap(), snap(alive=False))), ["critical"])
        self.assertEqual(self.sev(cl.evaluate_latest(snap(), snap(restarts=1))), ["critical"])
        self.assertEqual(cl.evaluate_latest(snap(), snap(restarts=1, alive=False), expect_down=("node",)), [])

    def test_soft_signals(self):
        self.assertEqual(self.sev(cl.evaluate_latest(snap(), snap(busy=5))), ["note"])
        self.assertEqual(self.sev(cl.evaluate_latest(snap(), snap(ena=7))), ["warn"])
        self.assertEqual(cl.evaluate_latest(None, snap(ena=7)), [])
        self.assertEqual(cl.evaluate_latest(snap(), snap(swap=4096)), [])
        self.assertEqual(cl.evaluate_latest(None, snap(swap=1 << 30)), [])
        self.assertEqual(self.sev(cl.evaluate_latest(snap(swap=1 << 20), snap(swap=1 << 30))), ["warn"])


class EventTests(unittest.TestCase):
    def test_step_windows_last_attempt_wins(self):
        ev = [{"kind": "step_start", "step": "a", "t": 1}, {"kind": "step_end", "step": "a", "t": 2, "status": "failed"},
              {"kind": "step_start", "step": "a", "t": 5}, {"kind": "step_end", "step": "a", "t": 9, "status": "ok"},
              {"kind": "step_start", "step": "b", "t": 10}]
        w = cl.step_windows(ev)
        self.assertEqual(w["a"], (5, 9, "ok"))
        self.assertEqual(w["b"], (10, None, "running"))

    def test_remote_job_runs_detached_and_records_rc(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            h = cl.Host("c", "client", cl.LOCAL, "127.0.0.1", d)
            j = cl.RemoteJob(h, "j", "sh -c 'echo \"$A\"; exit 3'", env={"A": "b 'c'"}).start()
            self.assertEqual(j.wait(timeout=10, poll=0.05), 3)
            self.assertEqual(j.output().strip(), "b 'c'")
            with open(j.pidf) as f:
                self.assertTrue(f.read().strip().isdigit())

    def test_remote_job_kill_stops_the_whole_group(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            h = cl.Host("c", "client", cl.LOCAL, "127.0.0.1", d)
            j = cl.RemoteJob(h, "sleeper", "sleep 30").start()
            for _ in range(100):
                if os.path.exists(j.pidf) and open(j.pidf).read().strip():
                    break
                time.sleep(0.02)
            with open(j.pidf) as f:
                pgid = int(f.read())
            j.kill()
            for _ in range(200):
                try:
                    os.killpg(pgid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.02)
            else:
                self.fail("process group %d survived kill()" % pgid)

    def test_pid_script_does_not_match_itself(self):
        s = cl.valsd_pids_script("/r", "node")
        self.assertIn("pgrep -x valsd", s)
        self.assertIn("'/r/bin/valsd node '*", s)


if __name__ == "__main__":
    unittest.main()
