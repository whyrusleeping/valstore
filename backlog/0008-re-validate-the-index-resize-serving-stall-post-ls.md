---
id: "0008"
title: Re-validate the index-resize serving stall post-LSM (close or fix)
status: done
priority: 3
created: "2026-07-02"
updated: "2026-07-06"
depends_on: []
spec_refs: []
---

## Description

**RESOLVED 2026-07-06 (HEAD `664397b`).** Re-ran the 1B-key SPDK ingest on
oldbox (3-node RF=3, sweep off, flush decoupled at 1 s via
`VALS_FLUSH_INTERVAL_MS=1000`, `region_mib=1 TiB`, kvchurn `--count 1e9 --rounds 0`,
to hundreds of millions of keys/node). **Full write-up + dip table:
`bench/resize-stall-1b-revalidation.md`; reusable harness `bench/r1b.sh`.**

Outcome: the **original mechanism (in-memory `HashMap`/`BTreeMap` index O(n) resize on
the single serving core) is dissolved** — the byte-capped memtable bounds the live
index, the per-key version index was retired, and flush/run-merge builds are off-core;
RSS grows smoothly/linearly (run block-index+bloom RAM, the out-of-core item), with no
single-core doubling event. **But the *symptom* (periodic deep cluster-wide ingest dips,
tens of seconds) still reproduces**, re-rooted: all three nodes stall **in lockstep** at
flush/run-merge commits because the **single per-shard maintenance worker serializes
run-merge ahead of flush** (deepening ~10 s @47 M → ~40 s @117 M keys/node). Split to
**task 0068** with the evidence + candidate fixes. This item (the index-resize diagnosis)
is closed with evidence; the surviving stall is tracked in 0068.

---

BACKLOG.md §Scaling recorded periodic whole-cluster ingest collapse at 1B-key scale: `HashMap`/`BTreeMap` doubling on the serving core is a multi-second O(n) stall (observed with sweep disabled, hundreds of millions of keys/node). **This diagnosis predates the LSM**: the memtable is now byte-capped (flush drains it) and the per-key in-RAM version index was retired (C.5a-3/4), which plausibly dissolves the unbounded-map-growth mechanism. 

Task: re-run the 1B-key SPDK ingest (rsweep/kvchurn recipe in BACKLOG.md) on current HEAD; if the periodic deep dips are gone, close this with the measurement; if not, fix (pre-size from key-count hints / incremental-sharded growth / off-core growth).

**UNBLOCKED (2026-07-05)**: owner approved tearing down the live 4-node valsd cluster on oldbox; the four vfio-bound NVMe drives are free to claim. Box access verified from this environment: `ssh oldbox` (repo checkout at `~/vals`, passwordless sudo). First box session must gracefully stop the old cluster (valsd pd + node1–4, root PIDs up since Jul 03) before claiming the drives.

## Acceptance criteria
- A/B ingest measurement at ≥hundreds of millions of keys/node recorded (dips present/absent).
- Either the item is closed with evidence, or a fix lands with a regression measurement.

## Acceptance criteria

## Plan

Re-run the 1B-key SPDK ingest on current HEAD (664397b) on oldbox and decide: dips gone → close the BACKLOG §Scaling "index resize stall" item with the measurement; dips present → diagnose/fix.

**Box state (verified this session):** ssh oldbox works (passwordless sudo); the old valsd cluster is already stopped (no valsd/vrun procs); 4 data drives vfio-bound (0000:05:00.0, 07:00.0, c1:00.0, c3:00.0); repo at ~/vals is at 24b87a3 (2 commits behind local HEAD 664397b); /tmp/valsd.0067 (built Jul 6 06:43) is likely already the 664397b build from task 0067's verification; harness ~/run/cluster/rsweep.sh (3-node RF=2 SPDK cluster + kvchurn + 10s wr/s sampling) is in place; box RAM 755 GB.

**Run design (the key methodological points):**
1. Binary = current HEAD 664397b. Verify /tmp/valsd.0067 really is that build (git fetch on box; if in doubt rebuild: `VALS_SPDK_LINK=1 cargo build --release -p vals-server --features spdk`, cp to /tmp/valsd.1b; see box ~/vals/buildall.sh for the exact recipe). Leave the box checkout on a clean commit afterwards.
2. Reproduce the ORIGINAL isolation (AE sweep disabled) **without** accidentally disabling the LSM flush: at HEAD the flush cadence derives from `sweep` (4× sweep), so `sweep=86400s` alone would let the memtable grow unbounded and trivially "reproduce" the old stall by misconfiguration. Pass `VALS_FLUSH_INTERVAL_MS=1000` in the node env (the documented decoupling knob, crates/vals-cluster/src/node.rs ~1709) so the 4 GiB memtable byte-cap actually drains. Note this explicitly in the write-up.
3. Conf deltas vs the checked-in node confs: `region_mib = 1048576` (1 TiB — the current 8192 from the A/B session is far too small for ~70 GB/node of records; post-0067 the 1 TiB wipe deep-queues in ~1.1 s so it's cheap), sweep=86400s. Do NOT edit node1-3.conf in place; generate derived confs like rsweep.sh does.
4. Workload: kvchurn `--count 1000000000 --rf 2 --rounds 0 --threads 8 --pipeline 64` (rounds=0 → load + one full verify; the LOAD phase is the measurement). At the previously measured ~0.9–1 M load ops/s the load is ~20–30 min; verify (~265 k/s) ~60+ min. Generous timeout (e.g. 9000 s). If verify would blow the session budget it may be cut short — the dips question is answered by the load phase — but report that honestly.
5. Monitoring (the actual measurement): a variant of rsweep.sh's sampler — every 10 s, per-node and summed `vals_records_written` deltas (wr/s), plus per-node RSS and any flush/run-merge/compaction counters exposed on /metrics — so any dip can be attributed (memtable flush vs run-merge vs compaction). Write a new script (e.g. ~/run/cluster/r1b.sh) modeled on rsweep.sh rather than hacking it; clean stale /var/run/dpdk + /dev/hugepages/spdk* residue first (known issue). No dd wipe needed post-0067 (format=true handles stale superblocks).
6. Decision rule: original symptom = periodic deep dips every ~60–90 s to 1–8 k/s lasting tens of seconds while one node pegs a core. "Gone" = sustained load with no 10 s window collapsing to that class of dip (transient ±30–50% wobble around flush/merge boundaries is fine and should be characterized, not spun). "Present" = dips of the original class recur — then attribute (which node, which counter moved, RSS) and either fix (pre-size / incremental growth / off-core) or file a precise follow-up task if the fix is large.

**Deliverables (in-repo, this is what gets committed):**
- `bench/resize-stall-1b-revalidation.md`: methodology (incl. the sweep/flush-decoupling point + exact commands/confs), the wr/s time series summary (steady rate, min windows, dip table if any), RSS/flush/merge counters, and the verdict.
- BACKLOG.md §Scaling: update the "In-memory index resize stalls the serving core at scale" item (~line 163) to resolved-with-evidence (match the repo's existing resolved-item style, e.g. the interning avenue), or annotate with the new findings if not resolved.
- If a fix is needed and is small, land it with a re-run; if large, create a follow-up task with the evidence and keep this task to the measurement + decision.

**Run everything long over ssh as background jobs; never let a 2-min tool timeout kill the cluster.** Leave the box clean: cluster stopped, no stray vrun procs, checkout clean.

### Starting points
- box: ssh oldbox; harness ~/run/cluster/rsweep.sh (read it first — killall_vrun, conf templating, 10s sumc sampler are all there to copy)
- box repo ~/vals (currently 24b87a3; needs 664397b); build recipe in box ~/vals/buildall.sh (VALS_SPDK_LINK=1, -p vals-server --features spdk); /tmp/valsd.0067 probably already = 664397b
- bench/b1-box-ab-notes.md — box-recipe pitfalls (stale dpdk/hugepages residue; post-0067 no dd wipe needed; region_mib) and prior measured rates (load ~0.9-1M ops/s, verify ~265k/s at 2M count)
- crates/vals-cluster/src/node.rs:1636-1723 — FLUSH_MAX_BYTES_DEFAULT 4GiB, VALS_FLUSH_MAX_BYTES, snapshot_interval = 4×sweep unless VALS_FLUSH_INTERVAL_MS (THE key knob: sweep off must not disable flush)
- crates/vals-store/src/engine.rs:3257 memtable_bytes() — the byte-cap trigger; HashIndex (Unsorted) is the memtable rsweep exercises
- BACKLOG.md lines 163-191 — the item under test: periodic dips every ~60-90s to 1-8k/s, tens of seconds, one node pegs a core, observed sweep-disabled at hundreds of millions keys/node
- kvchurn CLI (box /home/user/why/vals-examples/target/release/kvchurn): --count --vsize(64) --rounds --threads --pipeline --rf; rounds=0 → load+verify only
- metrics: curl 127.0.0.1:946{1,2,3}/metrics → vals_records_written etc; check what flush/merge/compaction counters exist to attribute dips

## Work log
- 2026-07-05 plan: Re-run the 1B-key SPDK ingest on current HEAD (664397b) on oldbox and decide: dips gone → close the BACKLOG §Scaling "index resize stall" item with the measurement; dips present → diagn
…[truncated]
- 2026-07-05 context hints: 8 recorded with plan
- 2026-07-05 context hints: box: ssh oldbox; harness ~/run/cluster/rsweep.sh (read it first — killall_vrun, conf templating, 10s sumc sampler are all there to copy); box repo ~/vals (currently 24b87a3; needs 
…[truncated]
- 2026-07-06 implementer report: Re-validated the BACKLOG §Scaling "in-memory index resize stalls the serving core" 1B-key ingest item on current HEAD (664397b), on oldbox. **Decision: closed the index-resize item with evi
…[truncated]
- 2026-07-06 revision: Addressed both reviewer doc fixes:  1. **backlog/0068 (~line 22):** Corrected the leaked-in original-run figure. "the cluster held ~1.3–1.6 M replica-writes/s (≈130–160 k/s per node × 3)" → "
…[truncated]
- 2026-07-06 review tier: single-opus — reviewers: Claude
- 2026-07-06 review (Claude): accept — Documentation/measurement task completed correctly. The 1B-key SPDK re-validation is recorded (bench/resize-stall-1b-revalidation.md), with a reusable harness (bench/r1b.sh), the BACKLOG §Scaling ite
…[truncated]
- 2026-07-06 revision: Addressed both final documentation items, each verified against the box and source first.  **Verification done before editing:** - Box `kvchurn.1b.log`: exactly 10 `MISMATCH [load: write failed]` line
…[truncated]
- 2026-07-06 review (Claude): accept — Documentation/measurement task, revised well. The prior nit (RF-factor ambiguity) is resolved with a precise explanation in both the write-up and task 0068, and the implementer added a substantiated a
…[truncated]
- 2026-07-06 decision: accept — commit: bench: 1B-key re-validation closes the index-resize stall with evidence; residual merge-blocks-flush stall filed as 0068 (task 0008)
- 2026-07-06 usage: 105,530 tok (in 290, out 105,240, cache_r 12,554,701, cache_w 311,600) · cost n/a (unpriced)
  implementer: 80,212 tok (in 216, out 79,996, cache_r 10,226,025, cache_w 155,623) · cost n/a (unpriced)
  coordinator: 18,636 tok (in 46, out 18,590, cache_r 1,989,641, cache_w 116,166) · cost n/a (unpriced)
  reviewer:Claude: 6,682 tok (in 28, out 6,654, cache_r 339,035, cache_w 39,811) · cost n/a (unpriced)
