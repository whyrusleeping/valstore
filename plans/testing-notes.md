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
  `multidrive_*_liveness_*`, `pd::tests::rival_and_obsolete_pd_groups…`,
  `maint::tests::cover_scans_progress_while_merges_are_continuously_queued` and
  `net::tests::timed_out_network_write_releases_credits`. In vals-server: the pd_replace,
  live_dump and backup tests, and `tests/daemon.rs`
  `three_node_cluster_with_pd_serves_zones_and_survives_restart` (`ops >= 100` on node 1, line
  ~416) when it runs alongside the full vals-cluster and vals-server suites. Rerun that one with
  `--test daemon`.
- **CPU saturation:** on 2026-10-29 `scripts/ci.sh` passed on devbox once unloaded and three
  times beside `stress -c 48` (load averages 54 to 100). Beside `stress -c 160` (load about
  167 on 192 cores) all three runs failed on timing tests: the maint cover-scan test above,
  `antientropy::tests::cleanup_owner_probe_cuts_a_grown_window_when_the_peer_slows_down` ("one
  pass took 784ms"), `net::tests::a_connection_silent_for_a_deadline_is_replaced_and_the_next_request_succeeds`
  (QuorumFailed) and vals-server `tests/rehome_regroup.rs`
  `knockout_whole_engine_invalidation_reseeds_peers_on_a_rehome` (timed out waiting for the
  re-home).

## Crash-point harness (0532)

- `engine::tests::crash_points` (vals-store lib) runs a seeded engine workload on
  `CrashSimDevice`, which records every write and device flush: puts, tombstone puts, range
  deletes (sorted test), syncs, value-log GC (`compact`), flushes, a whole-run merge, a
  tombstone-GC merge (with value rewrite in the sorted test, which runs under
  `VALS_VLOG_CO_COMPACT=1`) and two clean reopens, so later crashes hit a recovered store. It
  then replays the events into a `WriteBackCache` and crashes
  before every event: a flush makes earlier writes durable, and `crash(seed, tear)` keeps each
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
  none at seed 1; docs/formats.md §2.1).
- The workload starts on a zeroed device. Starting from a device that holds an older store is
  0540.

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
