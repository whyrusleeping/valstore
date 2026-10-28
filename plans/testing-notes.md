# Test suite notes (flakes and test seams)

Observations about the test suites, moved here from memory.md on 2026-10-21. Check them
against the current tests before relying on them.

## Known flakes (fail in full parallel runs, pass alone)

Before blaming a diff, check `uptime` and rerun the failing test alone. These failures are more
common when devbox's load average is high (about 50 or more, from other sessions' builds).

- **Process-global counters and gauges** (other tests bump them during a full
  `cargo test -p vals-cluster --lib`): `ram::tests::swap_counts_against_budget_and_headroom`,
  `pd::tests::range_metrics_warn_on_concentrated_multi_range_zone`,
  `net::server::corrupt_read` `merge_rewrite_corruption_counts_logs_once_and_excludes_the_segment`,
  and `coord::range_delete_scan_cost_tests::head_of_queue_scan_cost_does_not_depend_on_the_deleted_span_three_nodes_r2`
  (panics at line ~209: "max_tree_built_blocks_total moved during every measured scan").
- **Timing under load:** vals-cluster `node::tests` liveness_wipe and
  `multidrive_*_liveness_*`, `pd::tests::rival_and_obsolete_pd_groups…`,
  `maint::tests::cover_scans_progress_while_merges_are_continuously_queued` and
  `net::tests::timed_out_network_write_releases_credits`. In vals-server: the pd_replace,
  live_dump and backup tests, and `tests/daemon.rs`
  `three_node_cluster_with_pd_serves_zones_and_survives_restart` (`ops >= 100` on node 1, line
  ~416) when it runs alongside the full vals-cluster and vals-server suites. Rerun that one with
  `--test daemon`.

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
