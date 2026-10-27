---
id: "0020"
title: Clean SPDK teardown for bench servers
status: done
priority: 4
created: "2026-07-02"
updated: "2026-07-05"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Hardware/benchmarking: the bench's detached server threads (`ShardedServer`/passive `ReplicaServer`, infinite `ex.run`) can segfault during process exit racing SPDK detach (benign, results print first); a SIGKILL'd run leaks `/var/run/dpdk/*` + a claimed controller, blocking the next attach. The mechanism exists (`NodeHandle` graceful shutdown) — give the passive bench servers the same handle or port the benches to `Node::serve*`, and validate teardown on hardware.

## Acceptance criteria
- Bench processes exit 0 with clean SPDK detach; repeated runs need no manual `/var/run/dpdk` cleanup.

## Acceptance criteria

## Plan

Goal: bench processes exit 0 with a clean SPDK detach instead of racing process exit against detached server threads. The graceful-shutdown mechanism already exists (`NodeHandle::shutdown` → drain + `ServerShared::finalize_shutdown`, which drops devices and detaches SPDK); the passive bench servers (`ReplicaServer::spawn`/`spawn_zoned`, `ShardedServer::spawn`) lack it — their executor threads run forever and the process exit races SPDK detach.

Approach (library):
1. In `crates/vals-cluster/src/net/server.rs`, add handle-returning spawn variants for the passive servers, e.g. `ReplicaServer::spawn_with_handle`, `ReplicaServer::spawn_zoned_with_handle`, `ShardedServer::spawn_with_handle`, returning `(addr(s), handle)`. Keep the existing detached `spawn*` API untouched (heavily used by unit tests) — implement it as the handle variant with the handle leaked/ignored, or leave as-is; implementer's choice, no behavior change for existing callers.
2. The handle should reuse the existing graceful-drain machinery rather than reinventing it: a cross-thread stop `Notify` + a shutdown watcher task on the server's executor (the existing `shutdown_loop` in node.rs does exactly this — `begin_shutdown`, poke the accept loop via loopback connect, bounded in-flight drain, wake/force-close conns), then after `ex.run` returns call `shared.finalize_shutdown()` (group-commit + device drop = SPDK detach), and `shutdown()` joins the thread. Options: (a) share `shutdown_loop` (make it pub(crate), it's generic over D) and add a small `ServerHandle { stop, join }` type with `shutdown(self)`; or (b) construct a real `NodeHandle` via a pub(crate) constructor — but NOTE the `NodeHandle` ctl channel feeds `ctl_loop`/dumper, which the passive server doesn't run, so a dedicated minimal `ServerHandle` is safer (a NodeHandle whose dumper hangs would be a trap). Prefer (a).
3. The passive server today doesn't call `finalize_shutdown` and its `ex.run(server_poll)` never returns; wire the watcher so `run` unblocks after drain (same as spawn_shard does: all tasks exit → run returns → finalize).

Approach (bench):
4. In `crates/vals-bench/src/main.rs`, switch every workload that spawns passive servers to the handle variants and gracefully shut them down at the end (AFTER results print, so a drain hiccup can never eat results): SPDK-gated `workload_distspdk`, `workload_distresil`, `workload_distspdkmc`, `workload_distmdmc`, `workload_distbatch`, `workload_distzone`, `workload_distsorted`; also `workload_distmd` currently ignores its `Node::serve_multidrive` handle (`let (weight, _node) = ...`) — keep it and call `.shutdown()` at the end. Survey for any other ignored handles (distmdmove/distmdfail). The non-SPDK loopback benches (dist, distmc, distmcp, distshard) can switch too for uniformity — cheap and exercises the path in CI-ish runs — but don't contort them.
5. Careful with ordering: shut servers down after the coordinator/transport side is done issuing RPCs; drop coordinators/transports first where that's natural, then shutdown handles, then return (exit 0).

Testing / verification (local, no hardware):
6. Add a regression test (file or mem backend) that a handle-based passive server serves a request, then `shutdown()` returns (thread joined) and the process/test doesn't hang — e.g. in net/tests.rs, mirroring the existing NodeHandle shutdown tests. If practical, assert post-shutdown connects fail.
7. `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace --all-targets` (zero warnings), `cargo fmt --check` on touched files (note: 17 pre-existing fmt drift sites exist elsewhere — task 0053 — don't fix them here; just don't add new drift), and `cargo check -p vals-bench --features spdk` to type-check the spdk-gated bench changes (SPDK type-checks without linking when VALS_SPDK_LINK is unset).

Out of scope (split to a follow-on box task): validating on the SPDK box that repeated bench runs need no manual /var/run/dpdk cleanup and exit 0 with clean detach.

### Starting points
- crates/vals-cluster/src/net/server.rs:1099-1201 — ReplicaServer/ShardedServer spawn/spawn_zoned/spawn_with (detached, ex.run never returns, no finalize_shutdown)
- crates/vals-cluster/src/node.rs:60-200 — NodeHandle/ShardHandle shape; :963-1023 shutdown_loop (the graceful drain watcher, generic over D); :925-948 spawn_shard tail — ex.run returns after drain, then shared.finalize_shutdown()
- ServerShared::begin_shutdown / finalize_shutdown / wake_conns / force_close_conns / inflight — already exist in server.rs
- crates/vals-bench/src/main.rs spawn sites: 941 (distspdk), 1445 (distresil), 1625 (distspdkmc), 1847 (distmdmc), 2095 (distbatch), 2334 (distzone), 2555 (distsorted), 3350/3470/3571/3735 (loopback dist/distmc/distmcp/distshard); 1074 distmd ignores its NodeHandle
- cargo check -p vals-bench --features spdk type-checks without SPDK linked (VALS_SPDK_LINK unset)
- existing NodeHandle shutdown tests: rg 'shutdown' crates/vals-cluster/src/net/tests.rs and tests/

## Work log
- 2026-07-05 plan: Goal: bench processes exit 0 with a clean SPDK detach instead of racing process exit against detached server threads. The graceful-shutdown mechanism already exists (`NodeHandle::shutdown` → drain +
…[truncated]
- 2026-07-05 context hints: 6 recorded with plan
- 2026-07-05 context hints: crates/vals-cluster/src/net/server.rs:1099-1201 — ReplicaServer/ShardedServer spawn/spawn_zoned/spawn_with (detached, ex.run never returns, no finalize_shutdown); crates/vals-cluster/src/node.rs:60-
…[truncated]
- 2026-07-05 implementer report: Gave the passive bench servers the same graceful-shutdown/clean-SPDK-detach path the co-resident Node already had.  ## Library (crates/vals-cluster) - `net/server.rs`: added a public `ServerHandle` (a
…[truncated]
- 2026-07-05 review tier: single-opus — reviewers: Claude
- 2026-07-05 review (Claude): accept — The change correctly gives the passive bench servers the same graceful-shutdown/clean-SPDK-detach path the co-resident Node already had. In vals-cluster/net/server.rs a new public `ServerHandle` is ad
…[truncated]
- 2026-07-05 decision: accept — commit: feat(cluster,bench): graceful ServerHandle shutdown for passive bench servers (task 0020)  - vals-cluster: ServerHandle + ReplicaServer::spawn_with_handle /   spawn_zoned_with_handle / ShardedServer::
…[truncated]
- 2026-07-05 usage: 73,510 tok (in 300, out 73,210, cache_r 10,891,134, cache_w 256,479) · cost n/a (unpriced)
  implementer: 56,775 tok (in 232, out 56,543, cache_r 9,711,547, cache_w 148,976) · cost n/a (unpriced)
  reviewer:Claude: 8,456 tok (in 44, out 8,412, cache_r 479,568, cache_w 34,372) · cost n/a (unpriced)
  coordinator: 8,279 tok (in 24, out 8,255, cache_r 700,019, cache_w 73,131) · cost n/a (unpriced)
