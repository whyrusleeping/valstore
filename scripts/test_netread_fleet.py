import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("netread-fleet.sh")
RESULT = "  read  R=2 strong : 123 keys/s aggregate  (123 keys, 1 batch calls, 1 calls/s, 0 errors, 0 misses, 0 mismatches)"


class FleetTest(unittest.TestCase):
    def run_fleet(self, body, nproc="2"):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            client = root / "client"
            client.write_text("#!/usr/bin/env bash\n" + body + "\n")
            client.chmod(0o755)
            logs = root / "logs"
            logs.mkdir()
            result = subprocess.run(
                ["bash", str(SCRIPT), nproc, "1", "1"],
                env={**os.environ, "BIN": str(client), "PEERS": "unused", "TMPDIR": str(logs)},
                capture_output=True, text=True, timeout=10,
            )
            retained = [p.read_text() for p in logs.glob("*/p*.log")]
            return result, retained

    def assert_failed(self, body):
        result, logs = self.run_fleet(body)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn("TOTAL:", result.stdout)
        self.assertIn("logs retained", result.stderr)
        self.assertEqual(len(logs), 2)

    def test_success(self):
        result, logs = self.run_fleet(f"echo '{RESULT}'")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("TOTAL: 246 ops/s", result.stdout)
        self.assertEqual(logs, [])

    def test_all_children_fail(self):
        self.assert_failed("echo crashed >&2; exit 7")

    def test_valid_output_does_not_mask_exit_failure(self):
        self.assert_failed(f"echo '{RESULT}'; exit 7")

    def test_missing_output(self):
        self.assert_failed("exit 0")

    def test_duplicate_output(self):
        self.assert_failed(f"echo '{RESULT}'; echo '{RESULT}'")

    def test_missing_error_field(self):
        self.assert_failed("echo '  read  R=2 strong : 123 keys/s'")

    def test_errors_misses_mismatches_and_no_work(self):
        for old, new in [("0 errors", "1 errors"), ("0 misses", "1 misses"),
                         ("0 mismatches", "1 mismatches"), ("123 keys/s", "0 keys/s")]:
            with self.subTest(field=new):
                self.assert_failed(f"echo '{RESULT.replace(old, new)}'")

    def test_one_failure_is_not_hidden_by_a_success(self):
        body = f"if mkdir \"$TMPDIR/first\" 2>/dev/null; then exit 7; fi\necho '{RESULT}'"
        self.assert_failed(body)

    def test_invalid_process_count(self):
        for count in ["0", "-1", "nope"]:
            with self.subTest(count=count):
                result, logs = self.run_fleet("exit 0", count)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(logs, [])


if __name__ == "__main__":
    unittest.main()
