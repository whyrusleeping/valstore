# Local overwrite-cliff repro (file-backend netload passes)

Reproduces the task-0145 overwrite cliff / merge-cadence behavior on a workstation
in ~3 minutes, no hardware. Used to A/B merge-scheduling changes (0145, follow-on
0166) with the probe-amplification metrics.

## Steps

1. Build: `cargo build --release -p vals-server -p vals-bench`
2. One-node file-backed cluster:

```sh
mkdir -p /tmp/vals-repro && cd /tmp/vals-repro
echo demo-secret > secret
cat > node1.conf <<EOF
node_id  = 1
listen   = 127.0.0.1:7101
backend  = file
data_dir = node1
secret_file = secret
metrics  = 127.0.0.1:9471
sweep    = 1s
region_mib = 24576
zone_region_mib = 20480
EOF
cat > pd.conf <<EOF
pd_id     = 1
data_dir  = pd
data_node = 1 @ 127.0.0.1:7101
rf        = 1
admin_socket = pd.sock
secret_file  = secret
EOF
rm -rf node1 pd
setsid nohup ~/code/valstore/target/release/valsd node -c node1.conf > node1.log 2>&1 < /dev/null &
sleep 0.5
setsid nohup ~/code/valstore/target/release/valsd pd -c pd.conf > pd.log 2>&1 < /dev/null &
sleep 3
~/code/valstore/target/release/valsd admin -s pd.sock ping   # expect: ok
```

3. Run identical passes (pass 1 = insert, rest = overwrite), sampling metrics:

```sh
sample() { curl -s 127.0.0.1:9471/metrics | grep -E '^vals_(apply_run_reads|apply_probe_runs|apply_probe_blocks|read_probe_(runs|blocks)|resident_runs|run_merges|run_merge_(input|output)_bytes|used_segments|live_keys|segments_freed) '; }
for p in 1 2 3 4 5; do
  ~/code/valstore/target/release/vals-bench x netload 2000000 256 "1@127.0.0.1:7101" 64 1 8 > pass$p.log 2>&1
  grep 'write Quorum' pass$p.log; sample > pass$p.metrics
done
```

4. Teardown: kill only the release valsd pids
   (`readlink /proc/$pid/exe` == `target/release/valsd`; NEVER `pkill -f`).

## Expected outcome (post-0145 build)

- Pass 1 (insert) ~420-490k ops/s, `apply_probe_runs` ≈ 0.
- Overwrite passes flat at ~140-156k ops/s; `resident_runs` bounded 1-3;
  `run_merges` increments continuously; `used_segments` bounded (frees happen).
- `apply_probe_blocks`/pass ≈ 1.005 per put (early-exit works); probes/put grows
  with `resident_runs` — if overwrite rate *degrades* pass-over-pass while
  `run_merges` stays 0 and `resident_runs` climbs, the merge scheduling regressed
  (the pre-0145 bug shape: 154k → 133k → 120k with runs 2→4→6→8).

Hardware analogue + baselines: `bench/lab-bench-20260810-0145/notes.md` and
`bench/lab-bench-20260806/F-insert-vs-overwrite.txt`.
