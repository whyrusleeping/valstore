---
id: "0043"
title: 'valsd dump: live SPDK node dump (secondary-process attach or in-daemon admin dump)'
status: done
priority: 3
created: "2026-07-04"
updated: "2026-07-04"
depends_on:
    - "0035"
spec_refs:
    - Current focus areas
---

## Description
Split from task 0035. `valsd dump`'s SPDK path currently attaches the NVMe controller itself, so it only works against an **offline/idle** drive; a dump of a live SPDK node needs one of:

1. **SPDK secondary-process attach**: extend `vals-spdk` (`SpdkEnv`, `spdk_env_init` — no `proc_type` support today) to init as a DPDK *secondary* process sharing the primary's hugepages/controller (same `shm_id` / `--file-prefix`), then open its own qpair over the shared controller and run the existing read-only scan. Keeps `valsd dump` a separate process; needs multi-process plumbing and careful teardown.
2. **In-daemon dump via admin**: add an admin-socket command that makes the running daemon dump a zone itself, on a dedicated thread using the sibling-qpair trick (`BlockDevice::sibling()` — must be created on the thread that polls it, see crates/vals-spdk/src/spdk.rs:717, same pattern as the maintenance worker). Testable in-repo with the file backend (dump while serving continues), live-safe on SPDK by construction.

The file backend is already live-safe (independent read handle), and the parallel scan (`scan_vlog_segments_parallel`, landed with 0035) already creates one sibling handle per worker — whichever option is chosen should compose with `--parallel`.

## Acceptance criteria
- A documented, supported way to dump a zone from a **live** SPDK node exists (either option above).
- In-repo test coverage for whatever is testable off-box (e.g. admin-triggered dump on the file backend while the node serves traffic).
- Box verification of live-SPDK safety/rates is tracked by the on-box validation follow-on task (not here).

## Acceptance criteria

## Plan

Chosen design: **Option 2 — in-daemon dump via a node admin socket** (testable in-repo on the file backend; live-safe on SPDK by construction via the sibling-qpair pattern the maintenance worker already uses). Option 1 (DPDK secondary-process attach) needs multi-process env plumbing and cannot be verified off-box.

## Changes

### 1. vals-store: expose the engine's scan config
- Add a public getter on `Engine` (e.g. `pub fn config(&self) -> Config`, `Config` is small/Copy) so an in-daemon dump can build the vlog-scan `Config { seg_blocks, reserved_blocks, ordering }` from the live engine instead of re-deriving it from the daemon config. Optionally a passthrough on `vals_cluster::replica::VersionedStore`.

### 2. Move the `.vdump` emit into vals-cluster (shared by CLI + daemon)
- New module `crates/vals-cluster/src/dump.rs`: move the format constants (`MAGIC`, `TOMB_SENTINEL`), `write_header`/`write_record`/`write_tombstone`, `encode_record` (uses `split_stored_blob`, which lives in vals-cluster — why the code moves here, not vals-store), `Fragment`, progress reporting, and the `emit_dump` driver (serial `scan_vlog_records` / parallel `scan_vlog_segments_parallel`) out of `crates/vals-server/src/dump.rs`. Return a `Send` stats struct (segments, puts, tombstones, value_bytes, elapsed) instead of printing, so callers format their own summary (CLI prints to stderr; admin returns it in the reply).
- `crates/vals-server/src/dump.rs` keeps arg parsing + backend attach/zone resolution and delegates to the shared emit. **The `.vdump` byte format must stay byte-identical** (the existing round-trip e2e pins it).

### 3. vals-cluster: per-shard control channel + off-core dump thread
- In `node.rs`, give each shard a cross-thread control path mirroring the `stop` notify: a `std::sync::mpsc::Sender<NodeCtl>` + `Arc<Notify>` created in `spawn_shard_at`; a small `ctl_loop` task on the shard executor parks on the notify (raced against shutdown) and drains requests.
- `NodeCtl` is a **non-generic** plain-data enum (so `NodeHandle` stays untyped): `Dump { zone: String (id or name), out: PathBuf, skip_tombstones: bool, parallel: usize, reply: mpsc::Sender<Result<String, String>> }`.
- Handler on the serving core (cheap, borrow-brief): resolve the zone (numeric id, or name via the catalog the daemon holds), `shared.zone_store(id)`; clone the serving device handle (`store.borrow().device_handle()` → `Arc<D>`) + the engine's scan `Config` + a display name; then spawn a named std thread (`vals-dump`) that:
  1. creates `dev.sibling()` **on the dump thread** (SPDK qpair is thread-bound; File dups the fd; Mem shares the buffer) — never reads through the serving handle;
  2. runs the shared `emit_dump(&sib, ...)` — for `--parallel`, `scan_vlog_segments_parallel`'s workers each `sibling()` off the passed device on their own threads (existing behavior), composing as required;
  3. sends the summary/error through `reply`.
- **Shutdown safety**: dump threads hold device handles, so they must not outlive the SPDK detach. Track their `JoinHandle`s (e.g. `Arc<Mutex<Vec<JoinHandle>>>` shared with the ctl task) and join them in `spawn_shard_at` after `ex.run` returns (next to `maint_join.join()`), before `finalize_shutdown`. Also refuse new dumps once shutdown began.
- `NodeHandle`: expose a cloneable requester (per-shard ctl senders + notifies), e.g. `NodeHandle::dumper() -> NodeDumper` with `dump_zone(shard, zone, out, skip_tombstones, parallel) -> Result<String, String>` (send request, signal notify, block on reply recv). Shard index defaults to 0; error if out of range.

### 4. vals-server: node admin socket + `dump` command
- `NodeConfig`: new optional `admin_socket` key (node admin socket path). Enabled only when set (no silent default — avoids colliding with a co-located PD socket). Document in config docs/deploy docs.
- `node_main`: when configured, reuse `admin::spawn_listener`; main loop becomes a `recv_timeout` loop (like `pd_main`) still polling SHUTDOWN. Node admin commands: `ping`, and `dump <zone-id-or-name> <out-path> [skip-tombstones] [parallel=N] [shard=S]`. The dump is dispatched on its own thread with the moved `reply_tx` + a cloned `NodeDumper` so a long dump never blocks `ping`/signal handling; the reply is `ok <summary>` on completion or `err: <reason>`.
- Update `valsd` usage text + module docs: live-node dump = admin command (works for file AND SPDK backends — the daemon dumps through its own sibling handle); offline drive dump = existing `valsd dump` subcommand. Note the point-in-time caveats (un-synced tail absent; a concurrent compaction can race the scan → the dump may fail and should be retried; it never disturbs serving).

### 5. Tests
- Extend `crates/vals-server/tests/dump.rs` (or a sibling test file, reusing its harness): boot a file-backend node (+PD) with `admin_socket` set, write a corpus with overwrites/deletes, then — while the node is LIVE and serving — issue the admin `dump` command; assert (a) reply is `ok ...`, (b) the node keeps serving during/after (reads + writes succeed), (c) the `.vdump` parses and last-wins-replays to the expected state for all records written before the dump (sync/flush considerations: write corpus, wait for it to be durable via the existing quiesce/sync helpers or accept the synced-prefix and verify the dumped set is a consistent last-wins prefix — simplest: pause writes, give the engine a moment/force sync via graceful means, dump, then resume writes and assert liveness).
- Also cover `parallel=N` on the admin path (byte-identical to the serial admin dump of the same quiesced zone, if cheap to assert).
- Existing offline dump e2e must stay green.

### 6. Docs + follow-on
- Box verification of live-SPDK rates/non-disturbance stays in task 0044 (already filed, depends on this).

## Acceptance mapping
- "documented, supported way to dump a zone from a live SPDK node": the node admin `dump` command, documented; live-safe by construction (dedicated thread + sibling qpair, same pattern as the maintenance worker).
- "in-repo test coverage": the live file-backend admin-dump e2e.

## Verification
`cargo build`, `cargo test` workspace-wide (file/mem paths), `cargo check --features spdk` for the SPDK type-surface if the feature builds off-box.

### Starting points
- crates/vals-server/src/dump.rs — current CLI dump; emit_dump/encode_record to be moved to vals-cluster
- crates/vals-cluster/src/maint.rs — the sibling-handle-created-on-worker-thread pattern (resolve_handle_b) and cross-thread Notify wiring to copy
- crates/vals-cluster/src/node.rs spawn_shard_at — where the ctl channel + join-before-finalize goes (see maint_join.join() ordering)
- crates/vals-cluster/src/replica.rs:1638 VersionedStore::device_handle() -> Arc<D>; :858 device_sibling()
- crates/vals-store/src/dump.rs scan_vlog_records / scan_vlog_segments_parallel (workers sibling() off the passed dev)
- crates/vals-store/src/engine.rs:855 struct Config; Engine has no public config getter yet
- crates/vals-server/src/admin.rs — spawn_listener/client reusable for the node admin socket
- crates/vals-server/src/main.rs pd_main — the recv_timeout admin main-loop shape to mirror in node_main
- crates/vals-server/tests/dump.rs — e2e harness (spawn valsd, admin(), parse_vdump) to reuse
- ServerShared is pub(crate) in crates/vals-cluster/src/net/server.rs — zone_store(zone_id), zone_config(zone_id)

## Work log
- 2026-07-04 plan: Chosen design: **Option 2 — in-daemon dump via a node admin socket** (testable in-repo on the file backend; live-safe on SPDK by construction via the sibling-qpair pattern the maintenance worker alr
…[truncated]
- 2026-07-04 context hints: 10 recorded with plan
- 2026-07-04 context hints: crates/vals-server/src/dump.rs — current CLI dump; emit_dump/encode_record to move to vals-cluster; crates/vals-cluster/src/maint.rs — sibling-created-on-worker-thread pattern (resolve_handle_b) +
…[truncated]
- 2026-07-04 implementer report: Implemented **Option 2 — live in-daemon zone dump via a node admin socket** (the plan's chosen design). A running node now dumps a zone through its own device handle on a dedicated thread over a `si
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements Option 2 (in-daemon live zone dump via a node admin socket) exactly as planned and satisfies the acceptance criteria. The .vdump format encoder + scan driver are factored into va
…[truncated]
- 2026-07-04 decision: accept — commit: valsd: live in-daemon zone dump via node admin socket (task 0043)  A running node (file or SPDK backend) can now dump a zone while serving: a `dump` command on a new optional node admin socket resolve
…[truncated]
- 2026-07-04 usage: 107,709 tok (in 334, out 107,375, cache_r 24,154,701, cache_w 568,915) · cost n/a (unpriced)
  implementer: 75,584 tok (in 210, out 75,374, cache_r 19,947,504, cache_w 249,875) · cost n/a (unpriced)
  coordinator: 23,228 tok (in 64, out 23,164, cache_r 3,234,994, cache_w 265,881) · cost n/a (unpriced)
  reviewer:Claude: 8,897 tok (in 60, out 8,837, cache_r 972,203, cache_w 53,159) · cost n/a (unpriced)
