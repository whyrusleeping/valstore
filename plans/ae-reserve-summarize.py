#!/usr/bin/env python3
"""Reduce the local gate samples to measured peaks, counter deltas, and ACK rates."""
import json
import pathlib
import re
import sys

root = pathlib.Path(sys.argv[1])
summary = {}
for arm in sorted(root.iterdir()):
    if not (arm / 'samples.jsonl').exists():
        continue
    rows = [json.loads(line) for line in (arm / 'samples.jsonl').read_text().splitlines()]
    nodes = {i: [r for r in rows if r['node'] == i] for i in (1, 2, 3)}
    phases = {}
    for label in ('initial', 'degraded', 'post-restart', 'continued-1', 'continued-2', 'continued-3'):
        if not (arm / f'{label}.log').exists():
            continue
        log = (arm / f'{label}.log').read_text() if (arm / f'{label}.log').exists() else ''
        match = re.search(r'write Quorum\(\d+\)\s*:\s*(\d+) logical writes/s.*\((\d+) logical writes, (\d+) calls, (\d+) errors', log)
        phases[label] = dict(zip(('reported_ops_s', 'attempted', 'calls', 'errors'), map(int, match.groups()))) if match else {'error': log[-350:]}
        if match:
            phases[label]['acked'] = int(match[2]) - int(match[4])
            phases[label]['useful_ack_ops_s'] = round(int(match[1]) * phases[label]['acked'] / int(match[2]))
    def max_metric(data, name):
        return max((r.get('metrics', {}).get(name, 0) for r in data), default=0)
    def delta(data, name):
        values = [r['metrics'][name] for r in data if name in r.get('metrics', {})]
        return max(values) - min(values) if values else None
    per_node = {}
    for node, data in nodes.items():
        by_scope = {}
        for row in data:
            if row['cgroup']:
                by_scope.setdefault(row['cgroup'], []).append(row)
        per_node[str(node)] = {
            'scopes': list(by_scope),
            'peak_rss': max_metric(data, 'process_resident_bytes'),
            'peak_cgroup_memory': max((r.get('memory_peak') or 0 for r in data), default=0) or None,
            'max_sampled_memory_current': max((r.get('memory_current') or 0 for r in data), default=0),
            'peak_charged': max_metric(data, 'ram_charged_bytes'),
            'peak_reserved': {c: max_metric(data, 'ram_reserved_' + c + '_bytes') for c in ('ae_seed', 'flush', 'merge')},
            'peak_debt': max_metric(data, 'ram_reserve_debt_bytes'),
            'peak_wait_ms': max_metric(data, 'ram_reserve_oldest_wait_ms'),
            'oom_kills': sum(max((int(r.get('memory_events', {}).get('oom_kill', 0)) for r in group), default=0) for group in by_scope.values()),
            'run_merges': sum(delta(group, 'run_merges') or 0 for group in by_scope.values()),
            'ae_entries_applied': max_metric(data, 'ae_entries_applied'),
            'seed_starts': sum(delta(group, 'ae_seed_starts') or 0 for group in by_scope.values()),
            'seed_sheds': sum(delta(group, 'ae_seed_ram_sheds') or 0 for group in by_scope.values()),
            'seed_fallbacks': sum(delta(group, 'ae_seed_sync_fallbacks') or 0 for group in by_scope.values()),
            'seed_restarts': sum(delta(group, 'ae_seed_restarts') or 0 for group in by_scope.values()),
            'reserve_deferrals': sum(delta(group, 'ram_reserve_deferrals') or 0 for group in by_scope.values()),
        }
    rebuild_start = re.search(r'rebuild_start=(\d+)', (arm / 'provenance.txt').read_text())
    start = float(rebuild_start[1]) if rebuild_start else 0
    after = [r for r in nodes[3] if r['time'] >= start and 'metrics' in r]
    completed = next((r['time'] for i, r in enumerate(after) if i and
                      r['metrics'].get('ae_seed_active', 0) == 0 and
                      (r['metrics'].get('ae_seed_wall_us', 0) > 0 or
                       r['metrics'].get('ae_seed_starts', 0) > 0)), None)
    read_log = (arm / 'rebuild-read.log').read_text() if (arm / 'rebuild-read.log').exists() else ''
    read_ok = '1000 matched / 0 mismatched' in read_log and re.search(
        r'read  R=\d+ strong.*\(1000 keys, .* 0 errors, 0 misses, 0 mismatches\)', read_log) is not None
    repair = json.loads((arm / 'repair-wait.json').read_text()) if (arm / 'repair-wait.json').exists() else None
    phase_lines = [line.split() for line in (arm / 'phases.txt').read_text().splitlines()] if (arm / 'phases.txt').exists() else []
    repair_seconds = (int(phase_lines[-1][2]) - start + repair['wait_seconds']) if repair and phase_lines and start else None
    summary[arm.name] = {'phases': phases, 'nodes': per_node,
                         'rebuild_to_repair_settled_seconds': round(repair_seconds, 1) if repair and repair['settled_proxy'] else None,
                         'seed_published_seconds_observed': round(completed - start, 1) if completed else None,
                         'rebuild_read_sample_pass': read_ok,
                         'repair_wait': repair,
                         'exact_rebuild_seconds': repair.get('exact_completed_seconds') if repair else None,
                         'exact_rebuild_completed': repair is not None and repair.get('exact_completed_seconds') is not None,
                         'samples': len(rows),
                         'metrics_samples': sum('metrics' in r for r in rows)}
(root / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
