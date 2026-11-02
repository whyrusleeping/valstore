"""Fast, unprivileged, offline checks for scripts/cloud/remote/*.sh.

Run from the repo root (CI does):
    python3 -m unittest discover -s scripts -p 'test_*.py'
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

REMOTE = Path(__file__).with_name("cloud") / "remote"
# The host-side harness scripts this test owns (strict checks: shellcheck at
# warning level, --help, header conventions). Every other *.sh in the directory
# still gets a syntax check and shellcheck at error level.
OWNED = ["hostprep.sh", "spdk-bind.sh", "rawdisk.sh", "profile.sh", "netbench.sh"]
OWNED_SCRIPTS = [REMOTE / n for n in OWNED]
ALL_SCRIPTS = sorted(REMOTE.glob("*.sh"))
HOSTPREP = REMOTE / "hostprep.sh"
REQUIRED_KEYS = ["hostname", "imds", "kernel", "cmdline", "cpu", "mem_bytes", "iommu",
                 "nics", "nvme", "instance_store_pcis"]
NVME_KEYS = ["ctrl", "model", "serial", "firmware", "pci", "numa_node", "driver", "role",
             "namespaces", "mounted"]
NIC_KEYS = ["iface", "driver", "pci", "numa_node", "mtu", "speed", "queues_combined"]


def run(args, **kw):
    env = {**os.environ, "VALS_CLOUD_NO_IMDS": "1"}
    env.pop("VALS_CLOUD_ROOT", None)
    return subprocess.run(args, capture_output=True, text=True, timeout=120, env=env, **kw)


class RemoteScriptsTest(unittest.TestCase):
    def test_scripts_exist(self):
        for s in OWNED_SCRIPTS:
            self.assertTrue(s.is_file(), s)

    def test_bash_syntax(self):
        for s in ALL_SCRIPTS:
            with self.subTest(script=s.name):
                r = run(["bash", "-n", str(s)])
                self.assertEqual(r.returncode, 0, r.stderr)

    def test_header_conventions(self):
        for s in OWNED_SCRIPTS:
            with self.subTest(script=s.name):
                head = s.read_text().splitlines()[:60]
                self.assertEqual(head[0], "#!/usr/bin/env bash")
                self.assertIn("set -euo pipefail", head)
                self.assertTrue(any(l.startswith("# Usage") for l in head), "usage header")
                self.assertIn("RESULT ", s.read_text())

    def test_shellcheck(self):
        sc = shutil.which("shellcheck")
        if not sc:
            self.skipTest("shellcheck not installed")
        r = run([sc, "-S", "warning", *map(str, OWNED_SCRIPTS)])
        self.assertEqual(r.returncode, 0, r.stdout + r.stderr)
        others = [str(s) for s in ALL_SCRIPTS if s not in OWNED_SCRIPTS]
        if others:
            r = run([sc, "-S", "error", *others])
            self.assertEqual(r.returncode, 0, r.stdout + r.stderr)

    def test_help(self):
        for s in OWNED_SCRIPTS:
            with self.subTest(script=s.name):
                r = run(["bash", str(s), "--help"])
                self.assertEqual(r.returncode, 0, r.stderr)
                self.assertIn("Usage", r.stdout)

    def test_classify_model(self):
        cases = {"Amazon EC2 NVMe Instance Storage": "instance_store",
                 "Amazon Elastic Block Store": "ebs",
                 "SAMSUNG MZQL27T6HBLA-00A07": "other",
                 "": "other"}
        for model, role in cases.items():
            with self.subTest(model=model):
                r = run(["bash", str(HOSTPREP), "--classify-model", model])
                self.assertEqual(r.returncode, 0, r.stderr)
                self.assertEqual(r.stdout.strip(), role)

    def test_inventory_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "vals"
            r = run(["bash", str(HOSTPREP), "--inventory-only", "--root", str(root)])
            self.assertEqual(r.returncode, 0, r.stderr)
            result_lines = [l for l in r.stdout.splitlines() if l.startswith("RESULT ")]
            self.assertEqual(len(result_lines), 1, r.stdout)
            result = json.loads(result_lines[0][len("RESULT "):])
            self.assertTrue(result["ok"])
            self.assertTrue(result["inventory_only"])

            inv_path = root / "out" / "inventory.json"
            self.assertEqual(result["inventory"], str(inv_path))
            inv = json.loads(inv_path.read_text())
            for k in REQUIRED_KEYS:
                self.assertIn(k, inv)
            self.assertIsInstance(inv["cpu"]["numa_nodes"], list)
            for k in ("model", "sockets", "cores_per_socket", "threads", "numa_nodes"):
                self.assertIn(k, inv["cpu"])
            self.assertIn("groups", inv["iommu"])
            self.assertIn("cmdline_flags", inv["iommu"])
            self.assertFalse(inv["imds"]["available"])
            self.assertIsInstance(inv["mem_bytes"], int)
            for n in inv["nics"]:
                for k in NIC_KEYS:
                    self.assertIn(k, n)
            for e in inv["nvme"]:
                for k in NVME_KEYS:
                    self.assertIn(k, e)
                self.assertIn(e["role"], ("instance_store", "ebs", "other"))
                for ns in e["namespaces"]:
                    for k in ("name", "size_bytes", "lba_bytes"):
                        self.assertIn(k, ns)
            self.assertEqual(inv["instance_store_pcis"], sorted(inv["instance_store_pcis"]))
            for p in inv["instance_store_pcis"]:
                self.assertTrue(any(e["pci"] == p and e["role"] == "instance_store" for e in inv["nvme"]))
            # raw text files land under out/inventory/
            inv_dir = root / "out" / "inventory"
            for f in ("uname.txt", "cmdline.txt", "imds.txt", "mounts.txt"):
                self.assertTrue((inv_dir / f).exists(), f)

            # Re-run merges: a previous entry for a PCI that no longer exists is
            # dropped, real entries are kept, and the file stays valid.
            fake = dict(inv)
            fake["nvme"] = list(inv["nvme"]) + [{
                "ctrl": "nvme99", "model": "Amazon EC2 NVMe Instance Storage", "serial": "AWS1234",
                "firmware": "0", "pci": "0000:ff:1f.7", "numa_node": 0, "driver": "vfio-pci",
                "role": "instance_store", "namespaces": [], "mounted": False}]
            inv_path.write_text(json.dumps(fake))
            r = run(["bash", str(HOSTPREP), "--inventory-only", "--root", str(root)])
            self.assertEqual(r.returncode, 0, r.stderr)
            inv2 = json.loads(inv_path.read_text())
            self.assertNotIn("0000:ff:1f.7", [e["pci"] for e in inv2["nvme"]])
            self.assertEqual([e["pci"] for e in inv2["nvme"]], [e["pci"] for e in inv["nvme"]])
            self.assertEqual([e["serial"] for e in inv2["nvme"]], [e["serial"] for e in inv["nvme"]])

    def test_spdk_bind_status_without_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(["bash", str(REMOTE / "spdk-bind.sh"), "status", "--root", tmp])
            self.assertNotEqual(r.returncode, 0)
            last = r.stdout.strip().splitlines()[-1]
            self.assertTrue(last.startswith("RESULT "), r.stdout)
            self.assertFalse(json.loads(last[len("RESULT "):])["ok"])

    def test_spdk_bind_status_with_empty_inventory(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "out"
            out.mkdir()
            (out / "inventory.json").write_text(json.dumps({"nvme": [], "instance_store_pcis": []}))
            r = run(["bash", str(REMOTE / "spdk-bind.sh"), "status", "--root", tmp])
            self.assertEqual(r.returncode, 0, r.stderr)
            last = r.stdout.strip().splitlines()[-1]
            res = json.loads(last[len("RESULT "):])
            self.assertTrue(res["ok"])
            self.assertEqual(res["drivers"], {})
            self.assertIn("hugepages_total", res)

    def test_netbench_stop_without_servers(self):
        with tempfile.TemporaryDirectory() as tmp:
            r = run(["bash", str(REMOTE / "netbench.sh"), "stop", "--root", tmp])
            self.assertEqual(r.returncode, 0, r.stderr)
            last = r.stdout.strip().splitlines()[-1]
            self.assertEqual(json.loads(last[len("RESULT "):]), {"ok": True, "mode": "stop", "killed": []})

    def test_usage_errors_emit_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            for args in (["netbench.sh", "client", "--root", tmp],
                         ["profile.sh", "--root", tmp],
                         ["spdk-bind.sh", "--root", tmp]):
                with self.subTest(args=args):
                    r = run(["bash", str(REMOTE / args[0]), *args[1:]])
                    self.assertNotEqual(r.returncode, 0)
                    last = r.stdout.strip().splitlines()[-1]
                    self.assertTrue(last.startswith("RESULT "), r.stdout + r.stderr)
                    self.assertFalse(json.loads(last[len("RESULT "):])["ok"])


if __name__ == "__main__":
    unittest.main()
