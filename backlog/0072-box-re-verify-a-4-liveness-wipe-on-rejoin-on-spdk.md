---
id: "0072"
title: 'Box re-verify A.4: liveness wipe-on-rejoin on SPDK after the 0070 fence-gate fix'
status: done
priority: 3
created: "2026-07-06"
updated: "2026-07-06"
depends_on:
    - "0070"
spec_refs: []
---

## Description
Re-run scenario A.4 from `bench/spdk-liveness-box-notes.md` on the SPDK box (oldbox or equivalent) against the fixed liveness gate (task 0070: born-fenced nodes hold the fence until membership is adopted or a peer verifies them; see `ServerShared::lone_unfence_allowed`).

Procedure (per the notes, Scenario A setup: 3 nodes, rf=3, sweep=5s, gc_grace=45s, `backend = spdk`, no `data_dir`):
1. Bring the cluster up, load data, stop node 1 gracefully (watermark freezes).
2. Keep writing to nodes 2/3 for well past the 45 s grace (several minutes).
3. Restart node 1 (`format = false`).

## Acceptance criteria
- Node 1 stays fenced through the pre-push window (no premature `unfenced` log line before `adopting ring config`).
- The liveness wipe fires (`vals_wipes 1`, wipe log line with pre-wipe stats).
- Node 1 re-syncs via AE and unfences only after the refill; read-verify passes cluster-wide (no resurrection of GC'd deletes).
- Update `bench/spdk-liveness-box-notes.md` §A.4 / Verdicts with the re-run result.
- Box left clean (daemons stopped, drives vfio-bound, no hugepage residue).

## Plan

Operational box-validation run (no product code change) — I (coordinator) drive it directly over SSH to oldbox, reusing the intact task-0041 harness at ~/run/cluster/t0041 (start.sh / restart_nodes.sh / hygiene.sh, Scenario-A confs: 3 nodes, rf=3, sweep=5s, gc_grace=45s, backend=spdk, no data_dir, drives c1/05/c3+07, PD zones events/sessions).

1. **Sync + build**: rsync local source at HEAD 500e406 (the 0070 fence-gate fix) to box ~/vals (crates/, Cargo.toml/lock); `cargo build --release --features spdk` (bin valsd). Confirm the new gate string (`holding born-fence — membership not yet adopted`) is in the binary.
2. **Fresh cluster**: hygiene.sh, start.sh true (format=true). Verify all 3 nodes born-fenced → unfenced (A.1 behavior still good post-fix).
3. **Load**: kvchurn (~/vals-examples/target/release/kvchurn) --count 50000 --rf 3 → ALL CHECKS PASSED; put a sentinel key `doomed` via valsq (will test tombstone-GC non-resurrection).
4. **Hold-down**: SIGTERM node1 (confirm `graceful shutdown complete ... watermark Wms`, note frozen watermark). Delete `doomed` via node2/3. Keep nodes 2/3 taking writes for ≥3–5 min (kvchurn rf=2 against 7002/7003, repeated rounds), well past the 45 s grace so 2/3's watermarks advance far ahead and the tombstone is GC'd.
5. **Rejoin**: restart node1 alone with format=false (manual sed of node1.run.conf + same sudo/LD_LIBRARY_PATH invocation). Verify from node1.log ordering + metrics (:9461):
   - stays fenced through the pre-push window: NO `unfenced` line before `adopting ring config` (ideally the `holding born-fence` line appears);
   - liveness wipe fires: wipe log line with pre-wipe stats, `vals_wipes 1`;
   - re-syncs via AE and unfences only after refill (`unfenced` with a fresh watermark, after adoption).
6. **Read-verify**: kvchurn verify round passes cluster-wide; `doomed` stays absent on all nodes (no resurrection of the GC'd delete); named-zone data still served.
7. **Teardown**: SIGTERM all daemons, hygiene check — no /var/run/dpdk or hugepage residue, drives still vfio-bound.
8. **Docs**: update bench/spdk-liveness-box-notes.md §A.4 (re-run result) + Verdicts line; mark task done; review tier 'simple' (doc-only diff in-repo); commit.

### Starting points
- bench/spdk-liveness-box-notes.md — A.4 procedure, exact log strings, prior FAIL evidence
- box harness: oldbox:~/run/cluster/t0041/{start.sh,restart_nodes.sh,hygiene.sh,node*.conf,pdc.conf}
- fix marker: crates/vals-cluster/src/node.rs ~L2778 gate + ~L2823 'holding born-fence' log; server.rs lone_unfence_allowed ~L728
- client bins: oldbox:~/vals-examples/target/release/{kvchurn,valsq}

## Work log
- 2026-07-06 plan: Operational box-validation run (no product code change) — I (coordinator) drive it directly over SSH to oldbox, reusing the intact task-0041 harness at ~/run/cluster/t0041 (start.sh / 
…[truncated]
- 2026-07-06 context hints: 4 recorded with plan
- 2026-07-06 review tier: simple (coordinator self-review)
- 2026-07-06 decision: accept — commit: bench: A.4 re-verified on the SPDK box — 0070 fence-gate holds, wipe-before-rejoin fires, no resurrection (task 0072)  Re-ran the held-down-past-grace scenario on oldbox against valsd rebu
…[truncated]
- 2026-07-06 usage: 21,183 tok (in 78, out 21,105, cache_r 1,736,571, cache_w 46,249) · cost n/a (unpriced)
