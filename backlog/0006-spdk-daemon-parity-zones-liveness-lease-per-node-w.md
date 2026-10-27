---
id: "0006"
title: 'SPDK daemon parity: zones + liveness lease + per-node weight on backend=spdk'
status: done
priority: 2
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Productization: the `backend = spdk` daemon path serves the **default zone only** and **without the liveness lease** (`serve_multidrive` has neither zones nor a `_with_liveness` variant), and the PD's `data_node` config has no per-node `weight` (weight-1 today; SPDK nodes go through `admin add-node`). This is the headline productization gap — the production backend lacks the features the file backend has.

Related deferred follow-ons that naturally ride along (BACKLOG.md §Deferred): persist the liveness watermark on the SPDK superblock; sharded/multidrive `serve_with_liveness` variants; durable drive-layout fingerprint on the SPDK backend (needs a reserved block).

## Acceptance criteria
- `valsd node` with `backend = spdk` serves named zones (catalog-pushed and boot-reopened) and honors `gc_grace > 0` → liveness lease, matching the file backend's semantics.
- Per-node `weight` accepted in PD config `data_node` entries.
- Daemon e2e test extended (off-hardware where possible) + box verification for the SPDK path.

## Acceptance criteria

## Plan

Scope assessment: the single-drive SPDK path ALREADY serves the full named-zone catalog (run_node.rs `serve_spdk_zoned`, catalog-pushed + boot-reopened via the region table's persisted per-zone ordering) — that half of the criterion is done. The remaining gaps this session closes:

1. **Liveness lease on backend=spdk** (today `gc_grace > 0` is a hard boot error):
   - Config: `backend = spdk` with `gc_grace > 0` now REQUIRES `data_dir` (the durable watermark home, same as the file backend). New cross-field validation + clear error in `NodeConfig::parse`. The SPDK-superblock watermark stays a follow-on (separate task).
   - Zoned single-drive path (`serve_spdk_zoned`): when grace > 0, call `Node::serve_zoned_with_liveness` with a `FileLivenessLog` at `data_dir/liveness` (create_dir_all first); grace is stamped onto zone0 + lazily materialized zones by construction (ServerShared::with_liveness).
   - Multidrive/sharded path: add `Node::serve_sharded_with_liveness` and `Node::serve_multidrive_with_liveness` (thin variants mirroring serve_sharded/serve_multidrive; per-shard liveness log — suggest a `mk_log: impl Fn(usize) -> L` factory or a Vec of logs, one per shard, each shard's ServerShared chained with `.with_liveness(grace_ms, log)`). run_node's multidrive branch uses it with per-shard `FileLivenessLog` at `data_dir/liveness-shard{s}` when grace > 0.
   - Remove the `gc_grace must be 0` rejection in `serve_spdk`.

2. **Per-node `weight` in PD `data_node` config**: extend the syntax to `data_node = <id>[*<weight>] @ <addr>[,addr...]` (weight >= 1, default 1; weight 0 or garbage is a line-numbered error). `PdConfig.data_nodes` becomes `Vec<(NodeId, u32, Vec<SocketAddr>)>` (or parallel weight field); `seed_boot_state` passes the weight to `pd.add_node_addressed(id, weight, addrs)`. `peer`/`pd_peer` syntax unchanged.

3. **Tests (off-hardware)**:
   - config.rs: weight parses (explicit + default-1), weight-0/garbage errors, spdk+gc_grace>0 without data_dir errors.
   - node.rs: multidrive liveness test on `MemDriveSet` (no hardware needed): serve_multidrive_with_liveness node comes up fenced, sweep verifies against peers / watermark persists per shard, mirroring the existing serve_with_liveness tests (~line 1981+).
   - daemon e2e (tests/daemon.rs): give one data_node an explicit weight in pd.conf and assert the `members` admin reply reflects it (file backend — exercises the parse→seed→raft-commit→push path end to end).
   - Full `cargo build && cargo test` (default features; the spdk feature needs the SPDK toolchain/hardware and is compile-gated as today — do not break the non-spdk stub).

4. **Docs**: docs/deploy.md — data_node weight syntax, spdk liveness (gc_grace>0 needs data_dir), update the "SPDK JBOD nodes go through add-node" weight note. BACKLOG.md §Productization: mark the follow-on line resolved with a short note (multidrive zones + superblock watermark tracked in the ycc backlog).

Out of scope (split into follow-on tasks after implementation): named zones on the multidrive/sharded path (zone × partition design, overlaps task 0029), liveness watermark on the SPDK superblock (drops the data_dir requirement), on-hardware box verification (no SPDK box available in this session).

### Starting points
- crates/vals-server/src/run_node.rs — serve_spdk (gc_grace rejection, multidrive branch) + serve_spdk_zoned
- crates/vals-server/src/config.rs — parse_node_addrs, NodeConfig cross-field validation, PdConfig data_nodes
- crates/vals-server/src/run_pd.rs — seed_boot_state → pd.add_node_addressed(id, 1, addrs)
- crates/vals-cluster/src/node.rs — serve_sharded (l.238), serve_multidrive (l.283), serve_zoned_with_liveness (l.202); existing liveness tests ~l.1981, multidrive test ~l.2671 (MemDriveSet)
- crates/vals-cluster/src/net/server.rs with_liveness (l.451) — grace stamped onto all zone engines by construction
- crates/vals-cluster/src/liveness.rs — FileLivenessLog
- crates/vals-server/tests/daemon.rs — e2e; admin(&sock, "members") reply asserts
- docs/deploy.md lines ~57, ~91-100, ~213 (weight + gc_grace notes)

## Work log
- 2026-07-04 plan: Scope assessment: the single-drive SPDK path ALREADY serves the full named-zone catalog (run_node.rs `serve_spdk_zoned`, catalog-pushed + boot-reopened via the region table's persisted per-zone orderi
…[truncated]
- 2026-07-04 context hints: 8 recorded with plan
- 2026-07-04 context hints: crates/vals-server/src/run_node.rs — serve_spdk (gc_grace rejection l.161, multidrive branch l.190-225) + serve_spdk_zoned (l.234); crates/vals-server/src/config.rs — parse_node_addrs (l.154), Nod
…[truncated]
- 2026-07-04 implementer report: Closed the `backend = spdk` daemon-parity gaps that don't need hardware or a new on-device format.  ## What changed  **1. Liveness lease on `backend = spdk`** (was a hard boot error): - `crates/vals-c
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change correctly closes the in-scope SPDK daemon-parity gaps. It (1) adds the wipe-before-rejoin liveness lease to both SPDK paths — single-drive per-zone via serve_zoned_with_liveness and multi
…[truncated]
- 2026-07-04 decision: accept — commit: feat(server,cluster): SPDK daemon liveness lease + per-node weight in PD data_node config (task 0006)  - backend=spdk now honors gc_grace > 0: single-drive zoned path via   serve_zoned_with_liveness, 
…[truncated]
- 2026-07-04 usage: 48,944 tok (in 218, out 48,726, cache_r 7,751,394, cache_w 269,261) · cost n/a (unpriced)
  implementer: 34,084 tok (in 142, out 33,942, cache_r 5,332,239, cache_w 103,630) · cost n/a (unpriced)
  coordinator: 9,657 tok (in 36, out 9,621, cache_r 2,052,129, cache_w 134,830) · cost n/a (unpriced)
  reviewer:Claude: 5,203 tok (in 40, out 5,163, cache_r 367,026, cache_w 30,801) · cost n/a (unpriced)
