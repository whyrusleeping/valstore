# Test suite notes (flakes and test seams)

Observations about the test suites, moved here from memory.md on 2026-10-21. Check them
against the current tests before relying on them.

## Known flakes (fail in full parallel runs, pass alone)

Before blaming a diff, check `uptime` and rerun the failing test alone. These failures are more
common when devbox's load average is high (about 50 or more, from other sessions' builds).

- **Process-global counters and gauges:** a vals-cluster test that asserts an exact value or
  an unchanged value of a process-global counter or gauge (`crate::observe`, vals-store
  statics such as `rt_max_tree_built_blocks_total`) fails when other tests bump it in parallel.
  Start such a test with `if crate::testutil::isolated_env_test(&[]) { return; }`, which reruns
  it alone in a child process. `>= before + n` checks on monotonic counters are safe without
  it. 0506 isolated the four known cases: `ram::tests::swap_counts_against_budget_and_headroom`,
  `pd::tests::range_metrics_warn_on_concentrated_multi_range_zone`,
  `net::server::corrupt_read` `merge_rewrite_corruption_counts_logs_once_and_excludes_the_segment`
  and both `coord::range_delete_scan_cost_tests::head_of_queue_scan_cost_*` tests.
- **Timing under load:** vals-cluster `node::tests` liveness_wipe and
  `multidrive_*_liveness_*`, `pd::tests::rival_and_obsolete_pd_groups…` and
  `net::tests::timed_out_network_write_releases_credits`. In vals-server: the pd_replace,
  live_dump and backup tests.
- **CPU saturation (fixed in 0526):** beside `stress -c 160` on devbox (load about 167 on 192
  cores) the maint cover-scan test, `antientropy::tests::cleanup_owner_probe_cuts_a_grown_window_when_the_peer_slows_down`,
  `net::tests::a_connection_silent_for_a_deadline_is_replaced_and_the_next_request_succeeds`,
  vals-server `rehome_regroup.rs` `knockout_whole_engine_invalidation_reseeds_peers_on_a_rehome`
  and `daemon.rs` `three_node_cluster_with_pd_serves_zones_and_survives_restart` failed. Now:
  the cover-scan test drives the production compaction dispatcher synchronously; the cleanup
  probe uses a test-only virtual elapsed time (`CLEANUP_TEST_ELAPSED`); the silent-connection
  test has a 5 s healthy-path deadline and still requires exactly one reconnect; the re-home
  harness holds all seven port reservations while allocating (a retained timeout was node 2
  and the PD metrics endpoint sharing a port); the daemon test polls node 1's op count while
  sending it reads. All five passed 5/5 beside `stress -c 160` (targeted runs, not a full
  `scripts/ci.sh` under stress).

## Long tests and `VALS_TEST_FULL`

- `VALS_TEST_FULL=1` (any value but `0`) runs the long tests at their original size (0537).
  By default the keyfilter property tests run a few seeds and check 10 of the 40 sampled keys
  per step; `rebuilt_filter_has_scaled_resident_and_compound_fpr_bounds` uses 4M keys, not 50M
  (and measures `RssAnon`); the FrontCodedIndex model tests check only the blocks each step
  touches, with a whole-index check every 211 (or 97) steps; `churn_reclaim` uses a
  448-segment region, not 2048 (below about 384 segments many batches fail during churn).
- Full lane, debug: about 112 s for the keyfilter property test, 72 s for rebuilt_filter, 37 s
  for the index remove_if_eq test, 64 s for churn_reclaim. Use it after changing the key
  filter, the front-coded index or reclaim, e.g. `VALS_TEST_FULL=1 cargo test -p vals-store --lib keyfilter::`.
- The vals-store lib binary went from 111 s to 29 s; its slowest tests are now runformat
  oversized-block tests, `stress::concurrent_in_place_forced_windows_never_corrupt_merge_outputs`
  and `scan_rev::reverse_memtable_refill_crosses_4096_…` (roughly 20–27 s each in a parallel run).

## Crash-point harness (0532)

- `engine::tests::crash_points` (vals-store lib) runs a seeded engine workload on
  `CrashSimDevice`, which records every write and device flush. The device first holds a
  previous store (puts, a flush and a reopen, at higher versions than the workload's), then
  `Engine::create` formats it (0540), so a replay of an old record shows up as a wrong value.
  The workload: puts, tombstone puts, range
  deletes (sorted test), syncs, value-log GC (`compact`), flushes, a whole-run merge, a
  tombstone-GC merge (with value rewrite in the sorted test, which runs under
  `VALS_VLOG_CO_COMPACT=1`) and two clean reopens, so later crashes hit a recovered store. It
  then replays the events into a `WriteBackCache` and crashes
  before every event from the format on: a flush makes earlier writes durable, and `crash(seed, tear)` keeps each
  unflushed block with probability 1/2; with `tear`, one unflushed block keeps a random subset
  of its 512-byte sectors.
- Each crash image must open without error or panic and pass `storage_audit_for_test`. Every
  key must read its value as of the last sync that returned before the crash, or a value
  written after that sync and before the crash. A put, flush and reopen after recovery must
  leave every key unchanged.
- It runs at 4096-byte blocks (torn blocks) and 512-byte blocks (manifests and runs spanning
  several blocks), about 1,000 crash cases per test, 2–4 s in debug. `VALS_CRASH_SEED=N`
  sets the first workload seed (default 1330), `VALS_CRASH_SEEDS=K` runs K seeds (30 seeds
  take about 2 min). A failure names the block size, seed, event index, tear flag and image
  seed.
- Evidence: undoing 0529's write order for the manifest head (header after a flushed payload)
  fails it at one crash point in each test with `ManifestLost`. Before 0532's `RECOVERY_EPOCH_GAP`, about
  6% of crash cases failed the reopen check; without the first-segment epoch fence after a
  recovery, up to 9 cases per seed and test fail it after a reopen (4 at the default seed,
  none at seed 1; docs/formats.md §2.1). Starting new stores at epoch 0 again (before 0540's
  random epoch base) fails 364 of about 1,000 cases per test with old-store values.
- A recovery change that moves log epochs must keep `Engine::tail_epochs` (the node flush
  trigger) unchanged: vals-cluster `former_owner_tests` break when a reopen triggers an
  immediate flush.

## Test seams and assertions

- **Range-tombstone convergence:** AE reconcile never pulls a peer's covered entries, because
  `local_version` returns the range-tombstone cover, so they are not "behind". A replica that is
  missing an RT therefore shows permanently differing leaves with zero value pulls. Assert RT/AE
  convergence by equal F and scoped leaves (as `rt_sync_tests.rs` does), not by pull counts.
- **Isolating a fragment-carrying path:** in multi-node vals-cluster tests, a SetRangeMaps push
  kicks a sweep whose range-tombstone exchange delivers F to pending owners, even when the
  sweep interval is 3600 s. To test that another path (re-home copy, cleanup handoff) carries
  fragments, call `rt_sync::pause_exchange(zone, true)` for that zone.
- **Stress reproducibility:** `vals-store` `engine/tests/stress.rs` `run_stress` commits lane
  results at seeded ops (0431), so its per-seed stats reproduce exactly. A changed merge-kind
  count there is caused by the diff.
- **Scan paging:** `server::set_zone_scan_reply_budget` forces resume-key pages
  deterministically, whatever the fold and flush state.
- **Node cluster test reads:** a Read with epoch 0 gets `StaleEpoch` once any ring push has
  happened, so a `_ => None` read helper makes absence asserts vacuous; use epoch `u64::MAX`
  and panic on non-Read replies. After a cleanup lap that keeps nothing, the zone is not lapped
  again until a placement change or a CleanupStatus recheck (at most once per
  `min(gc_grace/2, 10 min)`), so data planted after a clean lap needs a SetMembers push first.
- **Tombstone GC paths:** `VersionedStore::compact_step` is a no-op once the engine has any
  run, and in sorted zones a range delete creates a fence run, so GC through `compact_step`
  only happens on stores that never flushed and never took a range delete; everything else
  collects through `compact_runs`. The sorted-zone GC sim (vals-cluster
  `src/netsim_gc_tests.rs`, about 2 s per seed in debug) checks the persisted-cutoff invariant
  on every compaction.
- **Faking engine fullness** via `alloc.allocate()` needs orphan reclaim disabled.
- **SPDK async races** can't be hit by file-backed repros; use forced `yield_now` plus counter
  A/Bs.
