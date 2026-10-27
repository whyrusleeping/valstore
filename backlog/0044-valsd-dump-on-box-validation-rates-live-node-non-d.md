---
id: "0044"
title: 'valsd dump on-box validation: rates + live-node non-disturbance (+ valsdump tombstone replay)'
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-06"
depends_on:
    - "0035"
    - "0043"
spec_refs:
    - Current focus areas
---

## Description
Split from task 0035 — everything that needs the SPDK box or the sibling repo. **UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; drives free to claim. Box access verified: `ssh oldbox` (repo at `~/vals`, passwordless sudo); gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives. The sibling repo `vals-examples` exists at `~/code/vals-examples` locally and `~/vals-examples` on the box (still outside this workspace root — the valsdump tombstone-replay item may need shell-level edits there or a user hand-off; documenting the limitation is an acceptable fallback per the criteria).

- **On-box rate validation**: dump a multi-GB zone from the SPDK cluster (2.12B-record-class corpus) and confirm device-sequential rates — single-digit minutes per node vs ~9 h via the scan path (~76k rec/s, BACKLOG.md §Efficiency/healing 2026-06-26). Sweep `--parallel <N>` (per-segment workers, one qpair each) to find the knee; consider making the measured best the default.
- **Live-node non-disturbance**: dump against a live node (via task 0043's live path; file backend already live-safe) while it serves at nominal rates; confirm serving is undisturbed.
- **Whole-cluster capture**: one dump per node concurrently (nodes/zones independent) — sanity-check aggregate wall time.
- **Sibling tooling**: teach `vals-examples/valsdump replay` the tombstone sentinel (`val_len == 0xFFFF_FFFF` → coordinator delete); until then `--skip-tombstones` for put-only corpora. (Implement in the sibling repo; track here.)

## Acceptance criteria
- 2.12B-record-class corpus exports in single-digit minutes per node, measured on the box.
- Live-node dump on SPDK completes while the node keeps serving at nominal rates.
- `valsdump replay` handles tombstone records (or the limitation is documented where users hit it).

## Acceptance criteria

## Plan

## Situation (verified 2026-07-07)
- Box oldbox reachable, **clean**: no valsd/vals-bench running, no SPDK residue, 4 vfio NVMe drives free, 2.5T free on /. The old Jul-03 cluster named in the task is already gone (later bench sessions used the drives), so **no pre-existing corpus survives — we load a fresh one**.
- Box repo ~/vals at 24b87a3 (stale, pre-dump-admin) → must rsync local HEAD (62f77f2) and rebuild.
- Cluster harness exists at box `~/run/cluster` (node{1,2,3}.conf spdk pci c1/05?/07, pdc.conf rf=3, secret, r1b.sh recipe: sweep=86400s + VALS_FLUSH_INTERVAL_MS=1000 + region_mib=1048576, kvchurn load at ~135k client wr/s ⇒ ~405k summed replica wr/s at rf=3).
- Surfaces: offline `valsd dump -c <conf> --zone-id 0 --out <p> [--parallel N]`; live `valsd admin -s <sock> dump <zone> <out> [skip-tombstones] [parallel=N]` with node conf key `admin_socket`. DEFAULT_ZONE id = 0. dump_spdk attaches read-only (no format).
- Sibling repo ~/code/vals-examples (local) is writable via shell; valsdump replay currently mis-reads the 0xFFFF_FFFF tombstone sentinel as a 4GiB length. Coordinator has `delete_in(zone, key, tier)`.

## Part A — sibling repo: valsdump replay tombstone support (local, do while the box load runs)
1. In `~/code/vals-examples/valsdump/src/main.rs`: change the record reader so after reading the key, a raw `val_len == 0xFFFF_FFFF` yields `Tombstone(key)` (no value bytes follow) instead of trying to read 4 GiB; puts unchanged. Update module doc/usage.
2. In `do_replay`: preserve last-wins ordering — on a tombstone, flush the pending put batch first, then `coord.delete_in(zone_id, &key, tier)` (count into recs/errs). No new flag needed.
3. Verify locally: file-backend cluster via the vals repo binaries (cluster-lib.sh / run-local.sh pattern): write keys, delete a subset (kvchurn's delete phase or direct client), graceful-stop, `valsd dump` the zone (contains tombstones — confirm `tombstones=N>0` in the summary), `valsdump replay` into a FRESH cluster, verify deleted keys read absent and survivors present.
4. Commit the sibling-repo change there via git (clear message referencing vals task 0044).

## Part B — box validation (ops over ssh; long-running steps as background jobs)
1. **Sync+build**: rsync local vals → box `~/vals` (exclude target/.git); `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk` → stash as `/tmp/valsd.0044`. Smoke-test existing box kvchurn binary for wire compat against a tiny cluster (50k run); if incompatible, rsync vals-examples → box and rebuild kvchurn there (path dep resolves to ~/vals).
2. **Cluster up** (in ~/run/cluster): derived confs `node{1..3}.0044.conf` = node*.conf + `sweep = 86400s`, `region_mib = 1048576`, `admin_socket = /tmp/vals-n<i>.sock` (short path), format=true for the ONE initial boot (never reboot with these confs after load — format would wipe). Start pd + 3 nodes: `sudo env LD_LIBRARY_PATH=$PWD/third_party/spdk/dpdk/build/lib VALS_LOG=warn VALS_FLUSH_INTERVAL_MS=1000 setsid /tmp/valsd.0044 ...` with per-node logs. Verify `valsd admin -s /tmp/vals-n1.sock ping` (may need sudo — socket owned by root).
3. **Load the 2.12B-record-class corpus**: kvchurn `--count 700000000 --vsize 128 --rounds 0 --threads 8 --pipeline 64` (rf=3 placement ⇒ ~700M records/node, ~2.1B replica records cluster-wide = 2.12B-class; est ~90 min at ~135k/s client). Run in background w/ generous timeout; sample per-node `vals_records_written` + RSS periodically; kill kvchurn once load phase done (or ≥700M/node). Floor if wall-clock forces it: ≥500M/node, recorded honestly. Then a short churn/delete pass (e.g. kvchurn --count 20000000 --rounds 1) to sprinkle overwrites + tombstones into the vlog.
4. **Live-node non-disturbance** (cluster still up): start a sustained kvchurn churn workload; record ~2 min baseline (client ops/s + per-node wr/s from metrics 9461-3). While it runs, `valsd admin -s /tmp/vals-n1.sock dump 0 /home/user/dumps/live-n1.vdump parallel=4` (admin client blocks until done — background it; confirm `ping` still answers mid-dump). Compare serving rates during vs before, kvchurn errors must stay 0. Record dump wall time + summary.
5. **Whole-cluster capture**: concurrent admin dumps on all 3 nodes → per-node + aggregate (max) wall time. Record.
6. **Graceful stop**: SIGTERM nodes + pd, confirm clean drain + SPDK detach in logs (task 0073 fix is in HEAD).
7. **Offline rate validation + --parallel knee**: `valsd dump -c node1.0044.conf --zone-id 0 --out /home/user/dumps/n1.vdump` (sudo + LD_LIBRARY_PATH). Sweep `--parallel {1,2,4,8,16}` on node1 (out to /dev/null or deleted between runs) → find the knee. Real-file dumps of node2/node3 at the best N. Sanity: puts+tombstones ≈ final `vals_records_written` per node (modulo un-synced tail). Acceptance math: per-node wall time in single-digit minutes; contrast with the ~76k rec/s scan baseline (~2.5h+/node for this corpus).
8. **Default --parallel**: if the knee shows a clear, safe win (e.g. ≥2× at N=4 with a flat/safe tail), make the measured best the default for `valsd dump` CLI + admin (`crates/vals-server/src/dump.rs`, node admin default, USAGE, docs/deploy.md); else keep 1 and document the measured recommendation in docs/deploy.md. Judgement by data.
9. **Clean the box**: kill any stragglers, rm /home/user/dumps (huge files), no spdk residue, drives untouched/vfio-bound; keep derived confs+logs or note them.
10. **Record**: new `bench/dump-box-notes.md` (recipe, numbers, knee table, live-node deltas, aggregate capture, box state before/after); update BACKLOG.md §Efficiency/healing bulk-export item with the measured resolution; note the valsdump tombstone commit hash from Part A.

## Acceptance mapping
- 2.12B-class corpus, single-digit minutes/node → Part B step 7 (measured, real files).
- Live-node dump w/o serving disturbance → step 4 (baseline vs during, error count 0).
- valsdump replay tombstones → Part A (implemented + verified locally, committed in the sibling repo).

## Risks / notes
- ~3–4 h wall total, dominated by the load; run it in background and do Part A meanwhile.
- Never restart nodes on format=true confs post-load; live tests BEFORE stop, offline dumps AFTER, no reboot in between.
- If load collapses / node crashes: capture logs, stop, report honestly (partial corpus may still support the rate measurement if ≥500M/node).

### Starting points
- bench/r1b.sh — the exact box cluster recipe to derive (killall/residue/conf-sed/start/sampler patterns)
- plans/distnode-box-bench.md §Box run — box prereqs, residue cleanup, leave-the-box-clean checklist
- docs/deploy.md §Bulk vlog export + §Live-node dump — CLI + admin dump syntax
- crates/vals-server/src/dump.rs — offline dump CLI (USAGE, dump_spdk); crates/vals-cluster/src/dump.rs — emit + DumpStats summary (puts/tombstones/value_bytes)
- crates/vals-cluster/src/node.rs ~1491 — admin dump zone resolution (numeric id ok, so `dump 0 <out>`)
- ~/code/vals-examples/valsdump/src/main.rs — read_lenbuf/read_record (~line 156-178) and do_replay batch flush (~line 392+); Coordinator::delete_in in crates/vals-client/src/coord/mod.rs:1894
- box: ~/run/cluster (node{1..3}.conf spdk, pdc.conf rf=3), repo ~/vals (stale @24b87a3), kvchurn at ~/vals-examples/target/release/kvchurn, metrics ports 9461-9463
- kvchurn flags: --count --vsize --rounds --threads --pipeline --zone (default zone id 0); r1b measured ~135k/s client load rate at rf=3, sweep off, flush 1000ms

## Work log
- 2026-07-06 plan: ## Situation (verified 2026-07-07) - Box oldbox reachable, **clean**: no valsd/vals-bench running, no SPDK residue, 4 vfio NVMe drives free, 2.5T free on /. The old Jul-03 cluster named in t
…[truncated]
- 2026-07-06 context hints: 8 recorded with plan
- 2026-07-06 context hints: bench/r1b.sh — exact box cluster recipe to derive (killall_vrun/clean_residue/conf-sed/start/sampler patterns); box cluster dir ~/run/cluster with node{1..3}.conf (spdk, pci pinned), pdc.conf (r
…[truncated]
- 2026-07-06 implementer report: Task 0044 complete — `valsd dump` device-sequential export validated on the SPDK box, valsdump tombstone replay shipped, findings documented honestly.  ## Part A — sibling repo (vals-examples): va
…[truncated]
- 2026-07-06 revision: Addressed both reviewer follow-ups on the box (~90 min), documented honestly including where results were inconclusive or downgraded prior claims. No code changes — docs/notes/BACKLOG only, as instr
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — This is an on-box validation + documentation task; the dump code primitive shipped earlier, so the diff appropriately touches only BACKLOG.md, the 0044 backlog file, docs/deploy.md, and a new bench/du
…[truncated]
- 2026-07-06 decision: accept — commit: bench: on-box validation of valsd dump — 2.53B-rec/302GiB node exports in 7.5min serial / 1.7min at --parallel 16 (74–329× over scan path); live dump 0 errors (~25% bw-contention dip, documented)
…[truncated]
- 2026-07-06 usage: 207,826 tok (in 428, out 207,398, cache_r 31,132,623, cache_w 1,831,136) · cost n/a (unpriced)
  implementer: 184,740 tok (in 380, out 184,360, cache_r 29,634,508, cache_w 1,568,973) · cost n/a (unpriced)
  coordinator: 18,769 tok (in 30, out 18,739, cache_r 1,365,831, cache_w 232,160) · cost n/a (unpriced)
  reviewer:Claude: 4,317 tok (in 18, out 4,299, cache_r 132,284, cache_w 30,003) · cost n/a (unpriced)
