---
id: "0016"
title: valsd config reload on SIGHUP
status: done
priority: 4
created: "2026-07-02"
updated: "2026-07-04"
depends_on: []
spec_refs: []
---

## Description
BACKLOG.md §Productization: config is read once at boot; add SIGHUP reload for the reloadable subset (log level, sweep/compaction cadences, metrics addr?) with explicit rejection of non-reloadable keys (backend, data_dir, addresses). Decide the reloadable set at plan time.

## Acceptance criteria
- SIGHUP re-reads the config, applies reloadable keys, logs and ignores (or refuses) non-reloadable changes.
- Daemon e2e test covers a live reload.

## Acceptance criteria

## Plan

Goal: `valsd` (node and PD) re-reads its config file on SIGHUP, applies the reloadable subset live, and logs-and-ignores changes to non-reloadable keys. A daemon e2e test covers a live reload.

DECIDED RELOADABLE SET (per the task's "decide at plan time"):
- `log_level` (NEW config key, node + PD): `error|warn|info|debug|trace|off`. At boot it overrides `VALS_LOG` when present (absent → today's VALS_LOG behavior). On reload, applied via `log::set_max_level` (the built-in stderr logger already filters through `log::max_level()`, see vals-client/src/observe.rs:514).
- `sweep` (node only): live-updates the anti-entropy sweep cadence AND the cadences derived from it (compaction via `compact_interval`, flush/snapshot via `snapshot_interval` — both re-derive from the new sweep, preserving the VALS_*_MS env overrides since those functions re-check env).
- Everything else (backend, data_dir, listen, node_id, peers, rf, gc_grace, secret, block/seg/region geometry, pci/spdk_*, format, metrics, admin_socket; all PD keys) is NON-reloadable: on reload, a changed non-reloadable key is logged at warn ("`<key>` changed but is not reloadable; keeping the running value") and ignored. `metrics` is deliberately non-reloadable (rebinding a live HTTP listener is not worth the machinery).
- A re-read config that fails to parse/validate: log the error, keep the entire running config (never crash or partially apply).

IMPLEMENTATION

1. crates/vals-server/src/config.rs
   - Add `log_level: Option<log::LevelFilter>` to `NodeConfig` and `PdConfig`; parse key `log_level` with a `parse_log_level` helper (same value set as VALS_LOG: error|warn|info|debug|trace|off; bad value = line-numbered error). Unit tests: parses, bad value errors.

2. crates/vals-cluster/src/node.rs — live sweep-cadence plumbing
   - `ServerShared` gains a `sweep_every: Cell<Duration>` (per-shard, single-thread; seeded at build). Simplest seam: set it where shared is constructed / in spawn_shard_at right after `build()`.
   - New `NodeCtl::SetSweepEvery(Duration)` variant; `ctl_loop` handles it by setting the Cell (+ an info log).
   - `NodeHandle::set_sweep_every(&self, d: Duration)`: send the ctl to every shard + fire its ctl_notify (mirror `dumper()`'s wiring).
   - `periodic_loop`, `compact_loop`, `snapshot_loop`: after each tick, read `shared.sweep_every.get()`; if the effective interval changed (for compact/snapshot, re-derive via `compact_interval` / `snapshot_interval`), recreate their `Timer::every(...)`. Document that a cadence change takes effect after the in-flight tick (bounded by the old period).
   - Test coverage in vals-cluster for the ctl path (e.g. a node started with a very slow sweep whose gauge sampling / AE behavior speeds up after `set_sweep_every`, or the most direct observable the implementer finds robust — must be deterministic-ish, poll with a deadline).

3. crates/vals-server/src/main.rs — SIGHUP wiring
   - Add `static RELOAD: AtomicBool`; `on_signal` dispatches on the sig number (SIGHUP → RELOAD, SIGTERM/SIGINT → SHUTDOWN); register SIGHUP in `install_signal_handlers`.
   - `init_common` (or right after it): apply `cfg.log_level` when present via `log::set_max_level`.
   - `node_main`: keep the active `NodeConfig` mutable; in BOTH wait loops (admin-socket and plain), poll `RELOAD.swap(false)` and run `reload_node(&config_path, &mut cfg, &handle)`:
     * `NodeConfig::load` — on Err: `log::error!("config reload failed ... keeping the running config")`, return.
     * log_level changed → apply + info log.
     * sweep changed → `handle.set_sweep_every(new)` + info log.
     * every other changed field → warn per key (compare field-by-field; a small helper returning the changed non-reloadable key names; never log secret values).
     * always end with one summary info line, e.g. `node N: config reloaded (applied: [...]; ignored non-reloadable: [...])` — the e2e test greps for it.
     * update the stored cfg's reloadable fields only (so subsequent reload diffs are against what is actually running).
   - `pd_main`: same shape, log_level only (helper `reload_pd`).
   - Update the crate-level doc comment (SIGTERM/SIGINT paragraph) to mention SIGHUP reload.

4. Daemon e2e test (crates/vals-server/tests/daemon.rs, new #[test], single node + no PD to keep it light):
   - Write a node config (file backend, sweep = 200ms, log_level = info), spawn `valsd node`, wait_listening.
   - Rewrite the config: log_level = debug, sweep = 100ms, and change a non-reloadable key (e.g. region_mib). `kill(pid, SIGHUP)`. Poll the stderr log file until it contains the reload summary + the applied lines + the non-reloadable warning naming the key.
   - Rewrite the config with a syntax error, SIGHUP again, poll for the "reload failed ... keeping" error line and assert the process is still alive and still accepting TCP.
   - SIGTERM → clean exit (stop()).

5. docs/deploy.md: document the `log_level` key (node + PD tables/examples) and add a short "Config reload (SIGHUP)" subsection naming the reloadable set and the ignore-non-reloadable behavior.

VERIFY: `cargo build`, `cargo test -p vals-server`, `cargo test -p vals-cluster` (non-spdk default features), plus `cargo clippy` on touched crates if cheap. The spdk-gated code paths must still compile untouched (no signature changes to serve_* needed — the Cell is seeded inside spawn, not passed through the public constructors).

### Starting points
- crates/vals-server/src/config.rs — NodeConfig/PdConfig parse; add `log_level` key
- crates/vals-server/src/main.rs — on_signal/install_signal_handlers, node_main/pd_main wait loops
- crates/vals-cluster/src/node.rs:83 NodeCtl enum; :1001 ctl_loop; :1133 periodic_loop; compact_loop ~:1355; snapshot_loop :1677; spawn_shard_at ~:805-880 (loops spawned with sweep_every / compact_interval(sweep_every) / snapshot_interval(sweep_every))
- crates/vals-client/src/observe.rs:561-582 init_stderr_logger / init_stderr_logger_at (log level = log::set_max_level)
- crates/vals-server/tests/daemon.rs — spawn/wait_listening/eventually/stop harness to reuse
- docs/deploy.md — key reference to extend

## Work log
- 2026-07-04 plan: Goal: `valsd` (node and PD) re-reads its config file on SIGHUP, applies the reloadable subset live, and logs-and-ignores changes to non-reloadable keys. A daemon e2e test covers a live reload.  DECIDE
…[truncated]
- 2026-07-04 context hints: 6 recorded with plan
- 2026-07-04 context hints: crates/vals-server/src/config.rs — NodeConfig/PdConfig parse; add `log_level` key; crates/vals-server/src/main.rs — on_signal/install_signal_handlers, node_main/pd_main wait loops; crates/vals-clu
…[truncated]
- 2026-07-04 implementer report: Implemented SIGHUP config reload for `valsd` (node + PD), applying a reloadable subset live and logging-and-ignoring non-reloadable changes.  ## Reloadable set (as decided in the plan) - `log_level` (
…[truncated]
- 2026-07-04 review tier: single-opus — reviewers: Claude
- 2026-07-04 review (Claude): accept — The change implements SIGHUP config reload for valsd (node + PD) exactly as planned: a reloadable subset (log_level node+PD, sweep node — which also retunes derived compaction/flush cadences) is app
…[truncated]
- 2026-07-04 decision: accept — commit: feat(valsd): SIGHUP config reload — live log_level (node+PD) and sweep cadence (node, retunes derived compaction/flush); non-reloadable changes logged and ignored; bad reload keeps running config; d
…[truncated]
- 2026-07-04 usage: 73,974 tok (in 362, out 73,612, cache_r 14,722,433, cache_w 322,857) · cost n/a (unpriced)
  implementer: 53,409 tok (in 278, out 53,131, cache_r 13,080,510, cache_w 146,579) · cost n/a (unpriced)
  coordinator: 15,600 tok (in 50, out 15,550, cache_r 1,339,716, cache_w 145,015) · cost n/a (unpriced)
  reviewer:Claude: 4,965 tok (in 34, out 4,931, cache_r 302,207, cache_w 31,263) · cost n/a (unpriced)
