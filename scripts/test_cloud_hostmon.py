import gzip
import http.server
import importlib.util
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import threading
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
HOSTMON = os.path.join(HERE, "cloud", "remote", "hostmon.py")
_spec = importlib.util.spec_from_file_location("hostmon", HOSTMON)
hm = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(hm)

PROM = r'''# HELP vals_put_total Puts.
# TYPE vals_put_total counter
vals_put_total{shard="0"} 10
vals_put_total{shard="1"} 5 1700000000000
# TYPE vals_lat_seconds histogram
vals_lat_seconds_bucket{op="get",le="0.001"} 1
vals_lat_seconds_bucket{op="get",le="+Inf"} 2
vals_lat_seconds_sum{op="get"} 0.5
vals_lat_seconds_count{op="get"} 2
weird{path="a\"b,c",x="y\\z", nl="1\n2"} NaN
neg -Inf
vals_server_errors 3
vals_server_errors{kind="x"} 4
vals_server_errors_extra 100

garbage line without value
'''

PROC_STAT = """cpu  100 1 20 1000 5 2 3 0 0 0
cpu0 60 1 10 500 2 1 2 0 0 0
cpu1 40 0 10 500 3 1 1 0 0 0
intr 123456 1 2 3
ctxt 999
btime 1700000000
processes 42
procs_running 3
procs_blocked 1
softirq 1 2 3
"""

NET_DEV = """Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:    1000      10    0    0    0     0          0         0     1000      10    0    0    0     0       0          0
  ens5: 123456789 98765 1 2 0 0 0 0 987654321 56789 3 4 0 0 0 0
"""

DISKSTATS = """ 259       0 nvme0n1 100 1 2000 50 200 2 4000 60 0 70 110 0 0 0 0
 259       1 nvme0n1p1 10 0 200 5 20 0 400 6 0 7 11 0 0 0 0
   8       0 sda 1 0 8 1 2 0 16 1 0 2 2
   8       1 sda1 1 0 8 1 2 0 16 1 0 2 2
   7       0 loop0 1 0 8 1 2 0 16 1 0 2 2
"""

ETHTOOL = """NIC statistics:
     tx_timeout: 0
     bw_in_allowance_exceeded: 12
     bw_out_allowance_exceeded: 0
     pps_allowance_exceeded: 3
     conntrack_allowance_available: 41000
     queue_0_tx_cnt: 123456
     queue_0_rx_bad_desc_num: 0
     not a counter line
"""


class ParseTest(unittest.TestCase):
    def test_prom(self):
        s, types = hm.parse_prom(PROM)
        self.assertEqual(types["vals_put_total"], "counter")
        self.assertEqual(types["vals_lat_seconds"], "histogram")
        self.assertEqual(s['vals_put_total{shard="0"}'], 10.0)
        self.assertEqual(s['vals_put_total{shard="1"}'], 5.0)  # timestamp ignored
        self.assertEqual(s['vals_lat_seconds_bucket{op="get",le="+Inf"}'], 2.0)
        self.assertEqual(s["neg"], -math.inf)
        key = [k for k in s if k.startswith("weird")][0]
        self.assertTrue(math.isnan(s[key]))
        name, labels = hm.split_series(key)
        self.assertEqual(name, "weird")
        self.assertEqual(labels, [("path", 'a"b,c'), ("x", "y\\z"), ("nl", "1\n2")])
        self.assertEqual(hm.series_key(name, labels), key)
        self.assertNotIn("garbage", " ".join(s))
        self.assertEqual(len(s), 11)

    def test_watch_sums_exact_family(self):
        s, _ = hm.parse_prom(PROM)
        w = hm.watch_sums(s, ["vals_server_errors", "vals_put_total", "absent"])
        self.assertEqual(w, {"vals_server_errors": 7.0, "vals_put_total": 15.0})

    def test_proc_stat(self):
        st = hm.parse_proc_stat(PROC_STAT)
        self.assertEqual(st["cpu"]["all"], [100, 1, 20, 1000, 5, 2, 3, 0])
        self.assertEqual(st["cpu"]["1"], [40, 0, 10, 500, 3, 1, 1, 0])
        self.assertEqual(st["sys"], {"intr": 123456, "ctxt": 999, "procs_running": 3,
                                     "procs_blocked": 1})

    def test_net_dev_and_diskstats(self):
        n = hm.parse_net_dev(NET_DEV)
        self.assertEqual(n["ens5"], [123456789, 98765, 1, 2, 987654321, 56789, 3, 4])
        self.assertIn("lo", n)
        d = hm.parse_diskstats(DISKSTATS)
        self.assertEqual(sorted(d), ["nvme0n1", "sda"])
        self.assertEqual(d["nvme0n1"], [100, 1, 2000, 50, 200, 2, 4000, 60, 0, 70, 110])

    def test_ethtool(self):
        e = hm.parse_ethtool(ETHTOOL)
        self.assertEqual(e["bw_in_allowance_exceeded"], 12)
        self.assertEqual(e["queue_0_tx_cnt"], 123456)
        self.assertEqual(len(e), 7)
        self.assertEqual(hm.nic_health(e), {"bw_in_allowance_exceeded": 12,
                                            "bw_out_allowance_exceeded": 0,
                                            "pps_allowance_exceeded": 3,
                                            "conntrack_allowance_available": 41000})

    def test_misc_proc_files(self):
        m = hm.parse_meminfo("MemTotal: 100 kB\nMemAvailable: 50 kB\nHugePages_Total: 4\nFoo: 1 kB\n")
        self.assertEqual(m, {"MemTotal": 102400, "MemAvailable": 51200, "HugePages_Total": 4})
        self.assertEqual(hm.parse_softnet("0000000a 00000001 000000ff 0\n"), [[10, 1, 255]])
        stat = "42 (my (odd) comm) S 1 2 3 4 5 6 7 8 9 10 111 222 0 0 20 0 7 0 5555 " + \
               " ".join(["0"] * 16) + " 17 0"
        self.assertEqual(hm.parse_pid_stat(stat), ("my (odd) comm", "S", 111, 222, 5555, 17))
        snmp = hm.parse_snmp("Tcp: RtoMin RetransSegs\nTcp: 200 7\nUdp: InDatagrams\nUdp: 9\n")
        self.assertEqual(snmp, {"Tcp": {"RtoMin": 200, "RetransSegs": 7}, "Udp": {"InDatagrams": 9}})
        irq, per = hm.parse_interrupts(
            "     CPU0 CPU1\n 45: 1 2 IR-PCI-MSI 1-edge nvme0q0\nLOC: 10 20 Local timer\n"
            "ERR: 0\n")
        self.assertEqual(irq, {"45/nvme0q0": 3, "LOC": 30, "ERR": 0})
        self.assertEqual(per, [11, 22])
        self.assertEqual(hm.parse_cpulist("0-2,8,10-11\n"), [0, 1, 2, 8, 10, 11])
        p = hm.parse_pressure("some avg10=1.5 avg60=0.0 avg300=0.0 total=12\n")
        self.assertEqual(p["some"]["total"], 12)
        self.assertEqual(hm.strip_digits("shard-12"), "shard")
        self.assertEqual(hm.strip_digits("tokio-runtime-w"), "tokio-runtime-w")


class SummarizeMathTest(unittest.TestCase):
    def test_histogram_quantile(self):
        b = [(0.1, 50.0), (1.0, 90.0), (math.inf, 100.0)]
        self.assertAlmostEqual(hm.histogram_quantile(0.5, b), 0.1)
        self.assertAlmostEqual(hm.histogram_quantile(0.7, b), 0.55)
        self.assertAlmostEqual(hm.histogram_quantile(0.9, b), 1.0)
        self.assertEqual(hm.histogram_quantile(0.99, b), 1.0)  # +Inf bucket -> highest finite le
        self.assertIsNone(hm.histogram_quantile(0.5, [(1.0, 0.0), (math.inf, 0.0)]))

    def test_counter_reset(self):
        e = hm.EndpointAcc()
        e.add(0, {"up": True, "full": True, "s": {"c_total": 10.0, "g": 1.0}}, True)
        e.add(5, {"up": True, "full": False, "s": {"c_total": 20.0, "g": 3.0}}, True)
        e.add(10, {"up": False}, True)
        e.add(15, {"up": True, "full": True, "s": {"c_total": 5.0, "g": 2.0}}, True)
        e.add(20, {"up": True, "full": False, "s": {"c_total": 8.0}}, True)
        r = e.report()
        self.assertEqual(r["counters"]["c_total"], {"delta": 18.0, "rate": 0.9, "resets": 1})
        self.assertEqual(r["gauges"]["g"], {"first": 1.0, "last": 2.0, "min": 1.0, "max": 3.0})
        self.assertEqual((r["scrapes"], r["down_scrapes"]), (4, 1))


def fast(t, k):
    """Synthetic fast record: cpu0 50% busy, cpu1 idle; eth0 rx rate doubles at the end."""
    rx = [0, 125_000_000, 125_000_000, 375_000_000][k]
    return {"t": t, "tier": "fast",
            "cpu": {"all": [50 * k, 0, 0, 150 * k, 0, 0, 0, 0], "0": [50 * k, 0, 0, 50 * k, 0, 0, 0, 0],
                    "1": [0, 0, 0, 100 * k, 0, 0, 0, 0]},
            "sys": {"ctxt": 1000 * k, "intr": 10 * k},
            "net": {"eth0": [rx, rx // 1000, 0, k, 0, 0, 0, 0]},
            "disk": {"nvme0n1": [100 * k, 0, 2048 * k, 0, 0, 0, 0, 0, 0, 1000 * k, 2000 * k]},
            "mem": {"MemAvailable": 1000 - k, "SwapTotal": 100, "SwapFree": 100 - k,
                    "HugePages_Total": 8, "HugePages_Free": 8 - k}}


def proc(pid, start, ut, threads):
    return {"pid": pid, "alive": True, "start": start, "utime": ut, "stime": 0,
            "status": {"VmRSS": 1000 * pid, "voluntary_ctxt_switches": ut,
                       "nonvoluntary_ctxt_switches": 0},
            "threads": threads}


def bucket(le, v):
    return {'lat_bucket{op="get",le="%s"}' % le: v}


def hist(a, b, c, s, n):
    d = {}
    d.update(bucket("0.1", a))
    d.update(bucket("1", b))
    d.update(bucket("+Inf", c))
    d['lat_sum{op="get"}'] = s
    d['lat_count{op="get"}'] = n
    return d


class SyntheticDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        d = self.tmp.name
        hdr = {"tier": "header", "version": 1, "hostname": "h1", "ncpu": 2, "clk_tck": 100,
               "numa": {"node0": [0], "node1": [1]}}
        types = {"c": "counter", "g": "gauge", "lat": "histogram"}
        m0 = {"c": 10.0, "g": 5.0}
        m0.update(hist(0, 0, 0, 0, 0))
        m1 = {"c": 20.0}
        m1.update(hist(50, 90, 100, 25, 100))
        m2 = {"c": 5.0, "g": 7.0}
        m2.update(hist(50, 90, 100, 25, 100))
        recs_a = [dict(hdr, t=1000.0), fast(1000.0, 0), fast(1001.0, 1),
                  {"t": 1000.0, "tier": "slow",
                   "metrics": {"node": {"up": True, "full": True, "s": m0, "types": types}},
                   "procs": {"p": proc(10, 1, 100, [[10, "main", "S", 50, 0, 0],
                                                    [11, "worker-1", "R", 50, 0, 1]])}}]
        recs_b = [dict(hdr, t=1002.0), fast(1002.0, 2), fast(1003.0, 3),
                  {"t": 1005.0, "tier": "slow",
                   "metrics": {"node": {"up": True, "full": False, "s": m1}},
                   "procs": {"p": proc(10, 1, 300, [[10, "main", "S", 150, 0, 0],
                                                    [11, "worker-1", "R", 100, 0, 1],
                                                    [12, "worker-2", "R", 50, 0, 1]])}},
                  {"t": 1010.0, "tier": "slow", "metrics": {"node": {"up": False, "error": "x"}}},
                  {"t": 1015.0, "tier": "slow",
                   "metrics": {"node": {"up": True, "full": True, "s": m2, "types": types}},
                   "procs": {"p": proc(20, 2, 10, [[20, "main", "S", 10, 0, 0]])}},
                  {"t": 1020.0, "tier": "slow",
                   "metrics": {"node": {"up": True, "full": False, "s": {"c": 8.0}}},
                   "procs": {"p": proc(20, 2, 110, [[20, "main", "S", 110, 0, 0]])}},
                  {"t": 1020.5, "tier": "error", "source": "ethtool:eth0", "error": "boom"}]
        a = os.path.join(d, "samples-19700101T001640Z.jsonl")
        with gzip.open(a + ".gz", "wt") as f:
            for r in recs_a:
                f.write(json.dumps(r) + "\n")
        with open(a, "w") as f:  # stale plain copy of a gzipped file: must be ignored
            f.write(json.dumps(recs_a[1]) + "\n")
        with open(os.path.join(d, "samples-19700101T001642Z.jsonl"), "w") as f:
            for r in recs_b:
                f.write(json.dumps(r) + "\n")
            f.write('{"t": 1021.0, "tier": "fa')  # SIGKILLed mid-write
        with open(os.path.join(d, "latest.json"), "w") as f:
            f.write("{}")

    def tearDown(self):
        self.tmp.cleanup()

    def test_full_window(self):
        r = hm.summarize_dir(self.tmp.name)
        w = r["window"]
        self.assertEqual((w["files"], w["bad_lines"], w["hosts"]), (2, 1, ["h1"]))
        self.assertEqual((w["from"], w["to"]), (1000.0, 1020.5))
        c = r["cpu"]
        self.assertAlmostEqual(c["busy_pct"], 25.0)
        self.assertEqual(c["per_cpu_busy_pct"], {"0": 50.0, "1": 0.0})
        self.assertEqual(c["top10"][0], ["0", 50.0])
        self.assertEqual(c["numa"], {"node0": 50.0, "node1": 0.0})
        self.assertAlmostEqual(c["ctxt_per_s"], 1000.0)
        n = r["net"]["eth0"]
        self.assertAlmostEqual(n["rx_gbps"], 1.0)
        self.assertAlmostEqual(n["peak_rx_gbps"], 2.0)
        self.assertAlmostEqual(n["rx_kpps"], 125.0)
        self.assertEqual(n["rx_drop"], 3)
        dk = r["disk"]["nvme0n1"]
        self.assertAlmostEqual(dk["read_iops"], 100.0)
        self.assertAlmostEqual(dk["read_mbps"], 2048 * 512 / 1e6)
        self.assertAlmostEqual(dk["avg_queue"], 2.0)
        self.assertAlmostEqual(dk["util_pct"], 100.0)
        self.assertEqual(r["mem"], {"min_available_bytes": 997, "max_swap_used_bytes": 3,
                                    "hugepages_total": 8, "hugepages_free_min": 5})
        m = r["metrics"]["node"]
        self.assertEqual(m["counters"]["c"], {"delta": 18.0, "rate": 0.9, "resets": 1})
        self.assertEqual(m["gauges"]["g"], {"first": 5.0, "last": 7.0, "min": 5.0, "max": 7.0})
        h = m["histograms"]['lat{op="get"}']
        self.assertEqual(h["count"], 100.0)
        self.assertAlmostEqual(h["mean"], 0.25)
        self.assertAlmostEqual(h["p50"], 0.1)
        self.assertAlmostEqual(h["p90"], 1.0)
        self.assertAlmostEqual(h["p99"], 1.0)
        self.assertEqual((m["scrapes"], m["down_scrapes"]), (4, 1))
        p = r["procs"]["p"]
        self.assertAlmostEqual(p["cores"], 0.3)  # 300 jiffies over 10 s of same-pid pairs
        self.assertEqual((p["pids"], p["pid_changes"]), ([10, 20], 1))
        self.assertEqual(p["threads_by_comm"], {"main": 0.2, "worker-1": 0.05, "worker-2": 0.05})
        self.assertEqual(p["threads_by_group"], {"main": 0.2, "worker": 0.1})
        self.assertEqual((p["rss_min_bytes"], p["rss_max_bytes"]), (10000, 20000))
        self.assertEqual(r["errors"], [["ethtool:eth0", "boom"]])
        json.dumps(hm.json_safe(r), allow_nan=False)
        self.assertIn("CPU: busy 25.00%", hm.format_text(r))

    def test_window_reconstructs_delta_state(self):
        r = hm.summarize_dir(self.tmp.name, t_from=1004.0, t_to=1020.0)
        m = r["metrics"]["node"]
        # baseline c=20 at 1005 comes from the pre-window full snapshot + delta
        self.assertEqual(m["counters"]["c"]["delta"], 8.0)
        self.assertEqual(m["histograms"]['lat{op="get"}']["count"], 0.0)
        self.assertNotIn("eth0", r["net"])
        self.assertEqual(r["errors"], [])
        self.assertAlmostEqual(r["procs"]["p"]["cores"], 0.2)

    def test_cli_json(self):
        out = subprocess.run([sys.executable, HOSTMON, "summarize", self.tmp.name, "--json"],
                             stdout=subprocess.PIPE, check=True).stdout
        r = json.loads(out)
        self.assertEqual(sorted(r), sorted(["window", "cpu", "net", "ena", "tcp", "disk", "mem",
                                            "procs", "metrics", "softnet", "irq_top", "errors"]))


class ProcTrackerTest(unittest.TestCase):
    def test_restart_counted(self):
        marker = "hostmon-restart-target-%d" % os.getpid()
        argv = [sys.executable, "-c", "import time; time.sleep(30)", marker]
        p = hm.ProcTracker("x", re.escape(marker) + "$")

        class W:
            def error(self, *a):
                pass

        cmdlines = lambda: hm.list_cmdlines(os.getpid())
        a = subprocess.Popen(argv)
        try:
            r = p.sample(cmdlines, W())
            self.assertEqual((r["pid"], r["alive"], p.restarts), (a.pid, True, 0))
            self.assertIn(marker, r["cmdline"])
            self.assertEqual(len(r["threads"]), 1)
            self.assertNotIn("cmdline", p.sample(cmdlines, W()))
        finally:
            a.kill()
            a.wait()
        r = p.sample(cmdlines, W())
        self.assertEqual((r["pid"], r["alive"]), (None, False))
        b = subprocess.Popen(argv)
        try:
            r = p.sample(cmdlines, W())
            self.assertEqual((r["pid"], p.restarts), (b.pid, 1))
        finally:
            b.kill()
            b.wait()


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = PROM.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


class RunTest(unittest.TestCase):
    def test_run_writes_latest_and_samples(self):
        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        th = threading.Thread(target=srv.serve_forever, daemon=True)
        th.start()
        marker = "hostmon-test-target-%d" % os.getpid()
        target = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)", marker])
        try:
            with tempfile.TemporaryDirectory() as d:
                out = os.path.join(d, "out")
                latest = os.path.join(d, "latest.json")
                url = "http://127.0.0.1:%d/metrics" % srv.server_address[1]
                subprocess.run([sys.executable, HOSTMON, "run", "--out", out, "--duration", "2.5",
                                "--interval", "0.5", "--slow-every", "2", "--vslow-every", "4",
                                "--metrics", "node=" + url,
                                "--metrics", "dead=http://127.0.0.1:1/metrics",
                                "--proc", "tgt=" + marker, "--proc", "none=^no-such-proc-xyz$",
                                "--iface", "hostmon-noif0", "--latest", latest,
                                "--rotate-secs", "1"], check=True, timeout=10)
                self.assertEqual([n for n in os.listdir(d) if ".tmp" in n], [])
                with open(latest) as f:
                    lt = json.load(f)
                self.assertEqual(sorted(lt), ["hostname", "load1", "mem", "metrics", "nic",
                                              "procs", "t"])
                self.assertEqual(lt["procs"]["tgt"]["pid"], target.pid)
                self.assertTrue(lt["procs"]["tgt"]["alive"])
                self.assertIsInstance(lt["procs"]["tgt"]["rss_bytes"], int)
                self.assertEqual(lt["procs"]["tgt"]["restarts"], 0)
                self.assertEqual(lt["procs"]["none"], {"pid": None, "alive": False,
                                                       "rss_bytes": None, "restarts": 0})
                self.assertEqual(lt["metrics"]["node"]["up"], True)
                self.assertEqual(lt["metrics"]["node"]["watch"]["vals_server_errors"], 7.0)
                self.assertEqual(lt["metrics"]["dead"], {"up": False, "watch": {}})
                self.assertIn("available_bytes", lt["mem"])
                self.assertIn("swap_used_bytes", lt["mem"])
                names = sorted(os.listdir(out))
                self.assertTrue(any(n.endswith(".jsonl.gz") for n in names), names)
                self.assertEqual(sum(n.endswith(".jsonl") for n in names), 1, names)
                recs = []
                for n in names:
                    op = gzip.open if n.endswith(".gz") else open
                    with op(os.path.join(out, n), "rt") as f:
                        recs.extend(json.loads(line) for line in f)
                tiers = [r["tier"] for r in recs]
                self.assertEqual(tiers[0], "header")
                self.assertEqual(tiers.count("fast"), 6)
                self.assertGreaterEqual(tiers.count("slow"), 2)
                self.assertGreaterEqual(tiers.count("vslow"), 1)
                hdr = recs[0]
                for k in ("hostname", "boot_id", "argv", "ncpu", "clk_tck", "page_size", "version"):
                    self.assertIn(k, hdr)
                slow = [r for r in recs if r["tier"] == "slow"]
                self.assertTrue(slow[0]["metrics"]["node"]["full"])
                self.assertFalse(slow[1]["metrics"]["node"]["full"])
                self.assertEqual(slow[1]["metrics"]["node"]["s"], {})  # nothing changed
                self.assertIn("cmdline", slow[0]["procs"]["tgt"])
                self.assertNotIn("cmdline", slow[1]["procs"]["tgt"])
                errs = [(r["source"], r["error"]) for r in recs if r["tier"] == "error"]
                self.assertEqual(len(errs), len(set(errs)))
                r = hm.summarize_dir(out)
                self.assertEqual(r["window"]["bad_lines"], 0)
                self.assertEqual(r["metrics"]["node"]["scrapes"], len(slow))
                self.assertEqual(r["procs"]["tgt"]["pids"], [target.pid])
        finally:
            target.kill()
            target.wait()
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
