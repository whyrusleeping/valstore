#!/usr/bin/env python3
"""Sample local 0316 scope cgroups and selected process metrics at two-second intervals."""
import json
import pathlib
import subprocess
import sys
import time
import urllib.request

prefix, out, *ports = sys.argv[1:]
out = pathlib.Path(out)
selected = (
    'process_resident_bytes', 'ram_charged_bytes', 'ram_reserved_',
    'ram_reserve_', 'ram_budget_bytes', 'ae_seed_', 'ae_entries_applied',
    'ae_sync_errors', 'ae_digests_examined', 'run_merges',
    'flush_build_bytes', 'run_flushes', 'resident_runs', 'live_keys',
    'pd_rehomes_', 'ram_busy_rejections',
)

def read_int(path):
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None

with (out / 'samples.jsonl').open('w', buffering=1) as samples:
    while not (out / 'stop-sampling').exists():
        for node, port in enumerate(ports, 1):
            unit = f'{prefix}-node{node}.scope'
            group = subprocess.run(
                ['systemctl', '--user', 'show', '-P', 'ControlGroup', unit],
                capture_output=True, text=True, timeout=3,
            ).stdout.strip()
            cg = pathlib.Path('/sys/fs/cgroup') / group.lstrip('/') if group else None
            record = {'time': time.time(), 'phase': (out / 'phase').read_text().strip(),
                      'node': node, 'cgroup': group}
            if cg:
                record['memory_current'] = read_int(cg / 'memory.current')
                record['memory_peak'] = read_int(cg / 'memory.peak')
                try:
                    record['memory_events'] = dict(line.split() for line in
                                                   (cg / 'memory.events').read_text().splitlines())
                except OSError:
                    pass
            try:
                body = urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics', timeout=1).read().decode()
                record['metrics'] = {
                    line.split()[0].removeprefix('vals_'): int(float(line.split()[1]))
                    for line in body.splitlines()
                    if line.startswith('vals_') and line.split()[0].removeprefix('vals_').startswith(selected)
                    and '{' not in line.split()[0]
                }
            except (OSError, ValueError, IndexError):
                record['metrics_error'] = True
            samples.write(json.dumps(record, separators=(',', ':')) + '\n')
        time.sleep(2)
