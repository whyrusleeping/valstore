# Local 3-node file-backed valsd (tool checks and crash/resume integration)

Two workstation recipes for a 3-node file-backend cluster plus PD. Neither needs
hardware. Both were moved here from memory.md on 2026-10-12.

## Common facts

- The file backend is unsharded: each node has exactly one `listen`.
- Zones come from the PD config, e.g. `zone = records 2 sorted`. Expect PD
  auto-split and rehome churn for a minute or two after loading data. Wait for it to
  settle before comparing nodes.
- Find node PIDs with `pgrep -x valsd` plus `/proc/<pid>/cmdline`. Never use
  `pgrep -f`/`pkill -f` with a pattern such as `node -c node3.conf`: it also matches
  your own shell and kills it.

## Tool checks (aediag and other per-shard tools)

- Bind the nodes to 127.0.0.11, 127.0.0.12 and 127.0.0.13 on the **same port**, and
  run the tools with `--shards 1`.
- Example use: the 0432 L1 validation in
  `bench/0363-compact-values/R3-AEDIAG-0432.md` ("Local L1 validation").
- Build tool crates with `CARGO_TARGET_DIR` outside the tree. `bench/*/aediag/target`
  is not gitignored (only the root `/target` is).

## Bootes crash/resume integration (task 0408)

- Driver: `/data/dataset/task0408-results/IT-L1-e27dde7/run_all.py`, with
  `start-cluster.sh`, `node{1,2,3}.conf` and `pd.conf` next to it.
- Shape: 3 nodes plus PD on `127.0.0.1:1710x`, RF 2, with a 2-segment slice of the
  1B cache (6.15M events at about 300k ev/s).
- One full run takes about 25 min, including two `samplecheck --every 1` passes.
