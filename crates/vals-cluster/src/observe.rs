use std::io::Write;

pub use vals_client::observe::{Counter, Gauge, Histogram, LATENCY_BUCKETS_US};

macro_rules! counters {
    (client: [$($client:ident,)+]; $(#[doc = $doc:literal] $name:ident,)+) => {
        pub struct Counters {
            $(#[doc = $doc] pub $name: Counter,)+
        }

        impl Counters {
            const fn new() -> Self {
                Counters { $($name: Counter::new(),)+ }
            }
        }

        impl std::ops::Deref for Counters {
            type Target = vals_client::observe::Counters;

            fn deref(&self) -> &Self::Target {
                vals_client::observe::counters()
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct CountersSnapshot {
            $(#[doc = $doc] pub $name: u64,)+
            $(pub $client: u64,)+
        }

        pub fn snapshot() -> CountersSnapshot {
            let c = counters();
            CountersSnapshot {
                $($name: c.$name.get(),)+
                $($client: c.$client.get(),)+
            }
        }

        pub fn snapshot_named() -> Vec<(&'static str, &'static str, u64)> {
            let c = counters();
            let mut named = vec![ $( (stringify!($name), $doc, c.$name.get()), )+ ];
            named.extend(vals_client::observe::snapshot_named());
            named
        }

        impl std::fmt::Display for CountersSnapshot {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $(writeln!(f, "{}: {}", stringify!($name), self.$name)?;)+
                $(writeln!(f, "{}: {}", stringify!($client), self.$client)?;)+
                Ok(())
            }
        }

        #[cfg(test)]
        mod registry_tests {
            use super::*;

            #[test]
            fn client_counter_instances_are_shared() {
                if crate::testutil::isolated_env_test(&[]) { return; }
                let cluster = counters();
                let client = vals_client::observe::counters();
                $(assert!(std::ptr::eq(&cluster.$client, &client.$client));)+
                let before = vals_client::observe::snapshot();
                $(cluster.$client.inc();)+
                let after = vals_client::observe::snapshot();
                $(assert!(after.$client > before.$client);)+
                let before = snapshot();
                $(client.$client.inc();)+
                let after = snapshot();
                $(assert!(after.$client > before.$client);)+
            }

            #[test]
            fn exported_counter_registry_is_complete_and_unique() {
                use std::collections::BTreeSet;
                let named = snapshot_named();
                let expected = BTreeSet::from([
                    $(stringify!($name),)+
                    $(stringify!($client),)+
                ]);
                let actual: BTreeSet<_> = named.iter().map(|(name, _, _)| *name).collect();
                assert_eq!(actual.len(), named.len());
                assert_eq!(actual, expected);
                let client = vals_client::observe::snapshot_named();
                let expected_client = BTreeSet::from([$(stringify!($client),)+]);
                assert_eq!(client.len(), expected_client.len());
                assert_eq!(client.iter().map(|(name, _, _)| *name).collect::<BTreeSet<_>>(), expected_client);
                for (name, help, _) in client {
                    let exported = named.iter().find(|(n, _, _)| *n == name).unwrap();
                    assert_eq!(exported.1, help);
                }
                let displayed = snapshot().to_string();
                let displayed_names: BTreeSet<_> = displayed.lines()
                    .map(|line| line.split_once(':').unwrap().0).collect();
                assert_eq!(displayed_names, expected);
                assert_eq!(displayed.lines().count(), expected.len());
            }
        }
    };
}
counters! {
    client: [
        credit_blocked, digest_hedges_fired, stale_ring_refreshes,
        proactive_ring_refreshes, detector_trips, detector_recoveries,
        hints_dropped, hints_recorded, peers_added, handshake_failures,
        idle_conn_retries,
    ];
    /// Requests served by replica servers (every decoded frame).
    server_ops,
    /// Requests answered with an in-band error (`Response::Err`).
    server_errors,
    /// Writes refused at the maintenance-reserve floor and answered with the retryable `Response::Busy`: the replica is healthy but holding free segments back for an imminent reclaim merge.
    busy_rejections,
    /// Writes refused early by graduated reserve-floor admission shaping (probabilistic retryable `Response::Busy` in the pre-floor band); every shed is also counted in busy_rejections.
    admit_shed_rejections,
    /// Writes refused because a zone region is genuinely exhausted and has no reclaimable headroom; answered with a terminal `Response::Err`.
    region_exhausted_rejections,
    /// Writes rejected because this node/shard no longer owns the key's range in its current range map, or holds a PD-managed sorted zone whose range maps it has not adopted yet (stale-coordinator fence); clients refresh and retry.
    range_fence_rejections,
    /// Sorted-zone reads (Read, Digest, MixedBatchRead, Scan) refused with `StaleEpoch` because this node/shard does not own every requested range in its current range map (or has not adopted range maps yet), so a coordinator routing by an outdated map refreshes instead of reading a former owner's leftover copy.
    range_read_fence_rejections,
    /// Client writes refused because process RSS plus anonymous swap reached `VALS_RAM_BUDGET_BYTES` and answered with the retryable `Response::Busy`; maintenance and reads remain admitted.
    ram_busy_rejections,
    /// Calls to malloc_trim from the dedicated node allocator-maintenance thread; excludes seed-path trims.
    allocator_trim_calls_total,
    /// Wall nanoseconds inside malloc_trim on the dedicated allocator-maintenance thread, including allocator lock waits; excludes RSS sampling.
    allocator_trim_ns_total,
    /// Sum of nonnegative process RSS drops across background malloc_trim calls; concurrent allocations/frees can affect this net estimate, not exact freed bytes.
    allocator_trim_rss_returned_bytes_total,
    /// Anti-entropy sweep passes started.
    sweep_passes,
    /// Sorted zones a sweep pass skipped (no reconcile, no over-replication cleanup) because the shard has not adopted its first range-map push yet.
    sweep_sorted_zones_held,
    /// Per-peer anti-entropy reconcile failures (best-effort; retried next pass).
    ae_sync_errors,
    /// Per-peer anti-entropy reconciles that gave up with Busy because the local tree scope token was invalidated during each of 3 consecutive peer tree round trips at one level: an in-scope Merkle bucket generation moved (writes or fold retires on the zone), the placement generation moved, or a span regroup or range-tombstone fold became pending; each is also counted in ae_sync_errors.
    ae_tree_token_exhaustions,
    /// Folded Merkle tree-node digests an anti-entropy reconcile examined during the hierarchical descent (no device reads; sparse diffs stay O(divergent + log)).
    ae_tree_nodes_examined,
    /// `(key, version)` digests an anti-entropy reconcile examined (phase 1, no device reads).
    ae_digests_examined,
    /// Entries an anti-entropy reconcile read back off the device (phase 2; near zero under transient churn).
    ae_value_reads,
    /// Anti-entropy reconcile candidates acknowledged after the peer exposed a greater version; includes candidates already dominated by a concurrent local winner.
    ae_entries_applied,
    /// Cooperative Merkle-leaf seed steps executed: source folding, placement projection, journal replay, and disposal each touch at most the fixed in-memory record budget per step; opening a run's version stream (sidecar for v3/v4 runs, data blocks for v5) is a separate step.
    ae_seed_steps,
    /// Cooperative seed turns yielded to in-flight scans on the same shard, with a 10 ms maximum seed deferral.
    ae_seed_scan_yields,
    /// Memtable and run version-stream `(key, version)` records folded by cooperative Merkle-leaf seeding.
    ae_seed_fold_ops,
    /// Cooperative seed restarts from a fresh, exact snapshot because the seed's input runs became unreadable: runs removed without a pin (`ae_seed_merge_restarts`), pin cap (`ae_seed_pin_cap_fallbacks`), pin revocation (`ae_seed_pin_revocations`), or a legacy v2 sidecar that could not be materialized within the seed's reservation. Placement and logical-range generation resets are counted separately.
    ae_seed_restarts,
    /// Placement invalidations deferred to an in-flight cooperative seed; the seed driver restarts with the latest placement.
    ae_seed_placement_coalesces,
    /// Zone preparation transitions retaining placement generation on a strictly newer nonzero range authority with identical inputs; not sweep calls or keys.
    ae_seed_epoch_suppressions,
    /// Cooperative seed source snapshots started, including restart snapshots; denominator for fold elimination, not successful publications.
    ae_seed_starts,
    /// Published logical counts invalidated by an apply with unknown old winner; epoch rebasing must not restore these counts.
    logical_range_unknown_invalidations,
    /// Zone preparation transitions installing placement and invalidating seed readiness, including first installation and conservative authority changes; not necessarily full folds.
    ae_seed_placement_invalidations,
    /// Zone preparation transitions on a strictly newer nonzero range authority whose only input change is a range map yielding identical replica sets for every key; placement generation and leaves retained, no fold.
    ae_seed_placement_reuses,
    /// Zone preparation transitions on a strictly newer nonzero range authority whose replica sets changed only for ranges in which this store holds no key (memtable or runs, tombstones included); the new placement is installed with leaves retained, no fold.
    ae_seed_placement_local_reuses,
    /// Zone preparation transitions on a strictly newer nonzero range authority whose replica sets changed only for range-map spans (a rehome prepare, commit or abort) in which this seeded store holds keys, where the store kept its leaves and started (or composed into) a span regroup instead of invalidating them (`ae_span_regroup=1`): only the changed spans' winner digests move between replica groups, no full seed.
    ae_seed_placement_regroups,
    /// Bounded winner-version pages scanned by span regroups, each one serving-core turn capped by winners and a turn deadline (includes prefetched and synchronous pages).
    ae_regroup_pages,
    /// Winners (tombstones included) scanned by span regroup pages.
    ae_regroup_keys,
    /// Scanned winners in clean buckets whose digest a span regroup moved from the old to the new sorted replica group.
    ae_regroup_moves,
    /// Span regroup pages discarded because the engine storage generation or the regroup cursor changed while their prefetched run-block reads were in flight; the page is re-planned.
    ae_regroup_prefetch_discards,
    /// Span regroup pages whose prefetched run blocks failed to read or decode; the page was read synchronously instead, and only a synchronous page error invalidates the leaves.
    ae_regroup_prefetch_errors,
    /// Placement changes that landed on an in-flight span regroup and were composed into it: the regroup's current mixed routing is frozen as its old placement and its unscanned spans are unioned with the newly changed spans; a subset of `ae_seed_placement_regroups`.
    ae_regroup_compositions,
    /// Placement changes that would have composed into a span regroup already composed 8 times and fell back to a whole-engine invalidation and reseed instead; a subset of `ae_regroup_fallbacks`.
    ae_regroup_depth_fallbacks,
    /// Span regroups that finished every changed span; AE tree serving and dirty-bucket rebuilds resume for the store.
    ae_regroup_completions,
    /// Span regroups abandoned for a full leaf invalidation and reseed: a placement change that cannot compose (composition depth, `ae_span_regroup=0`, or not a range-map-only change), any other leaf invalidation, or a synchronous page error while the regroup was in flight.
    ae_regroup_fallbacks,
    /// AE tree requests and reconciles answered not-ready (Busy) because a span regroup was in flight: on the cooperative tree paths only with `ae_seed_isolation=1` and an attached seed driver (otherwise the caller drives the regroup inline), and always on the synchronous control-plane SyncTree path.
    ae_regroup_not_ready_busy,
    /// Bounded winner pages scanned by range-tombstone folds (1,024 winners or 1 ms per serving-core turn, prefetched or synchronous), which move newly covered winners out of the leaves and range counts.
    rt_fold_pages,
    /// Winners (tombstones included) read by range-tombstone fold pages; blocks wholly covered by the accounted set are skipped and not counted.
    rt_fold_winners_total,
    /// Winners a range-tombstone fold removed from the leaves and range counts because its target cover hides them.
    rt_fold_retired,
    /// Covered memtable entries a range-tombstone fold removed (RAM only; their value-log records stay and replay finds them covered).
    rt_fold_memtable_removed,
    /// Range-tombstone fold pages whose covered memtable entries were left for the next flush because the store was borrowed elsewhere.
    rt_fold_memtable_deferred,
    /// Range-tombstone folds completed: the accounted set advanced to the fold's target.
    rt_fold_completions,
    /// Range-tombstone fold pages discarded because the engine storage generation or the fold cursor changed while their prefetched run-block reads were in flight; the page is re-planned.
    rt_fold_prefetch_discards,
    /// Range-delete slices a replica applied (`Won`: the overlay raised its cover or the slice was already implied), from clients and re-home mirrors.
    range_deletes_applied_total,
    /// Scan winners a replica dropped because a range tombstone covers them (version at or below the cover); they are not returned and not charged to the page's entry or byte budget.
    scan_rt_filtered_entries_total,
    /// Run blocks replica scans skipped without reading because the block lies wholly inside one range-tombstone fragment and its newest entry is older than the fragment.
    scan_rt_skipped_blocks_total,
    /// Run blocks replica scans read while the zone held range tombstones (boundary blocks, blocks with newer entries, and blocks of runs without a max tree).
    scan_rt_block_reads_total,
    /// Fence-index binary-search steps and max-tree nodes replica scans visited to skip covered blocks.
    scan_rt_index_probes_total,
    /// Scan pages cut by the covered-work budget or the fragment byte budget that returned a resume key (feature bit 3 peers only).
    scan_rt_resume_pages_total,
    /// AE tree requests, dirty-bucket rebuilds and bucket-digest serves answered not-ready (Busy) because a range-tombstone fold was pending with an attached seed driver, and synchronous SyncTree requests during any pending fold.
    rt_fold_not_ready_busy,
    /// Logical accounting transitions rebased to a strictly newer nonzero range authority with identical map, whether ready or in flight; not stats polls or keys.
    logical_range_epoch_rebases,
    /// Logical accounting transitions adopting a strictly newer nonzero range authority whose map differs only in tokens/pending/debt, not boundaries; counts and generation retained, no fold.
    logical_range_boundary_rebases,
    /// Logical accounting transitions clearing counts and advancing generation, including first map installation and conservative authority changes; not necessarily full folds.
    logical_range_map_invalidations,
    /// Nonzero-count range groups of a boundary change under a strictly newer nonzero range authority whose exact total was kept as a pending range-bounded recount span (two or more children not derivable from exact counts) instead of clearing counts and leaves; spans, not keys.
    logical_range_split_recounts,
    /// Zero-count range groups of a recognized boundary change resolved to exact zero children with no recount; groups, not keys.
    logical_range_split_empty,
    /// Bounded winner-count pages scanned by pending split recounts; each page is one serving-core turn capped by winners and a turn deadline (includes prefetched and synchronous pages).
    logical_range_split_recount_pages,
    /// Split recount pages merged from run blocks read ahead asynchronously under the shard read gate, bounded by the prefetch horizon; the merge itself stays one synchronous serving-core turn.
    logical_range_split_recount_prefetch_pages,
    /// Run blocks read asynchronously ahead of split recount pages, including reads of discarded prefetches.
    logical_range_split_recount_prefetch_blocks,
    /// Split recount prefetches discarded because the engine storage generation or the recount page changed while their run-block reads were in flight; the page is re-planned.
    logical_range_split_recount_prefetch_discards,
    /// Split recount turns that fell back to a synchronous on-core page after consecutive prefetch discards, guaranteeing progress; expected near 0.
    logical_range_split_recount_sync_fallbacks,
    /// Distinct logical winners, including tombstones, counted by split recount pages; expected near the sum of all but the largest child per span.
    logical_range_split_recount_keys,
    /// Split recount spans finalized with exact scanned and derived child counts.
    logical_range_split_recount_done,
    /// Boundary changes that re-partitioned a pending recount span (nested split of a pending child, a further cut in the span, or a merge at or across a span edge) into a new exact total without a full reseed; groups, not keys.
    logical_range_split_recount_repartitions,
    /// Genuine split/merge transitions (strictly newer nonzero epoch with published counts) or pending split recounts abandoned for the full count-and-leaf invalidation path (wrong-length counts, arithmetic mismatch, unknown old winner, or engine error); first installs, stale epochs and transitions while counts are unready are not counted.
    logical_range_split_recount_fallbacks,
    /// Split recounts whose scanned child exceeded the tracked parent total at finalize; each also falls back to full invalidation. Must stay 0.
    logical_range_split_recount_mismatches,
    /// Split recount turns paused because process RSS plus swap reached the RAM budget; the accounting loop retries on a timer without an external kick.
    logical_range_split_recount_ram_pauses,
    /// Range groups merged into one range under a strictly newer nonzero range authority and resolved in O(1) from exact counts or pending span totals, without clearing counts or leaves.
    logical_range_merge_sums,
    /// Streaming cooperative seeds restarted because placement or logical range generations changed.
    ae_seed_projection_retries,
    /// Canonical winners assigned to placement groups and logical ranges during cooperative seed projection.
    ae_seed_projection_ops,
    /// V3 logical-accounting polls that found an endpoint unready and scheduled an anti-entropy sweep to rebuild exact counts.
    logical_accounting_reseed_requests,
    /// Bounded-restart guard trips: after `MAX_SEED_RESTARTS` the cooperative seed gives up on yielding and falls back to the one-shot synchronous pass (`ensure_seeded`), accepting the serving stall as a last resort to guarantee forward progress under pathological churn.
    ae_seed_sync_fallbacks,
    /// Run version-stream folds (v3/v4 sidecars or v5 data blocks) aborted with a decode/CRC corruption error during cooperative seeding (decode-health canary).
    ae_seed_corrupt_sidecars,
    /// Mutations journaled while a cooperative seed was in flight and replayed at finalize: the count of buffered ops applied atop the staged accumulator before it swaps into the live index.
    ae_seed_journal_ops,
    /// Total wall-clock microseconds from a cooperative seed's first step to its completion, including inter-step yields and restarts.
    ae_seed_wall_us,
    /// Cooperative Merkle-leaf seeds discarded before publication because the process RAM footprint reached its budget.
    ae_seed_ram_sheds,
    /// Seed starts deferred because the node-wide seed limit or shed cooldown was reached.
    ae_seed_permit_deferrals,
    /// Microseconds the per-shard seed driver spent inside cooperative seed steps and split-recount turns; its rate divided by the shard count is the mean seed duty per shard.
    ae_seed_busy_us,
    /// Seed-driver pauses taken to hold seeding and split recounts to `ae_seed_duty_pct` of wall time while the shard had foreground work (a request in the last 200 ms, or BatchApply, reads or scans in flight).
    ae_seed_duty_pauses,
    /// Microseconds the per-shard seed driver paused to leave shard time to foreground work.
    ae_seed_duty_pause_us,
    /// In-flight seeds restarted from scratch because the logical-range generation changed (`ae_seed_isolation=0`, an unsorted or recount-disabled engine, or an unremappable change); a subset of `ae_seed_projection_retries`.
    ae_seed_generation_resets,
    /// In-flight seeds restarted from scratch because the placement generation changed; a subset of `ae_seed_projection_retries`.
    ae_seed_placement_resets,
    /// Seeds published across one or more logical-range generation bumps without a restart: counts were remapped onto the current map (pending split recounts) or the exactly tracked live counts were kept.
    ae_seed_generation_carries,
    /// Anti-entropy tree requests (peer SyncTree, local tree readiness, eager sweep rebuilds) answered not-ready/Busy because the zone's seed is driven by the shard's accounting task and is not finished.
    ae_seed_not_ready_busy,
    /// Anti-entropy reconcile passes skipped for a zone whose seed is not finished, or (with `ae_seed_isolation=1` and an attached seed driver) whose span regroup or range-tombstone fold is in flight; the zone is retried on a later sweep pass.
    ae_seed_reconcile_skips,
    /// In-flight seeds whose write journal could not grow without the node's journals exceeding `ae_seed_journal_max_bytes` and switched to applying writes directly to the staged accumulator.
    ae_seed_journal_spills,
    /// Writes applied directly to an in-flight seed's staged accumulator after its journal reached the cap.
    ae_seed_journal_spill_ops,
    /// Run-set removal commits (merges, forgets) an in-flight seed continued across on its pinned input runs instead of restarting, counted at the seed's next step as the advance of the engine's removal epoch; the removed runs' segments stay allocated until the seed publishes or resets and are then freed by a reap (`ae_seed_pin_reaped_segments`).
    ae_seed_merge_carries,
    /// In-flight seeds restarted from scratch because runs were removed while the seed held no run pin (`ae_seed_pin_max_bytes=0`, or the node was at the pin cap or shedding when the seed started); a subset of `ae_seed_restarts`.
    ae_seed_merge_restarts,
    /// In-flight seeds restarted from scratch by the node-wide pin cap `ae_seed_pin_max_bytes`: a seed that is not the oldest pinned seed restarts once its pinned bytes plus those of all older pinned seeds exceed the cap (85% of it while the node is shedding after exceeding it), checked at each step and at the merge or forget commit that retires its runs; a subset of `ae_seed_restarts`.
    ae_seed_pin_cap_fallbacks,
    /// Seed steps where the node was over `ae_seed_pin_max_bytes` but the stepping seed was the oldest pinned seed and kept its pinned runs (the cap never restarts the oldest seed, so one seed always completes; region pressure still revokes it).
    ae_seed_pin_cap_exempt_steps,
    /// In-flight seeds restarted from scratch because the engine revoked their run pin (region space pressure or a truncate) and freed the pinned runs; a subset of `ae_seed_restarts`.
    ae_seed_pin_revocations,
    /// Cooperative Merkle-leaf seeds published (completed), including synchronous fallbacks. Every started seed ends exactly once: `ae_seed_starts` = completions + `ae_seed_restarts` + `ae_seed_placement_resets` + `ae_seed_generation_resets` + `ae_seed_ram_sheds` + `ae_seed_corrupt_sidecars` + `ae_seed_error_resets` + `ae_seed_dropped` + seeds in flight.
    ae_seed_completions,
    /// In-flight seeds reset by a run version-stream or sidecar error other than corruption (device read errors); the seed driver logs the error and retries on a later accounting pass.
    ae_seed_error_resets,
    /// In-flight seeds discarded without a restart or publication: store truncate, leaf invalidation, or the zone store being dropped (zone removal or shutdown).
    ae_seed_dropped,
    /// Run segments freed by reaping retired runs whose seed pins were released (seed published or reset), including pressure revocations at reap time; these frees are not counted in `segments_freed`.
    ae_seed_pin_reaped_segments,
    /// Maintenance and AE byte reservations deferred by the process-wide RAM broker.
    ram_reserve_deferrals,
    /// Dirty logical-winner Merkle bucket rebuild requests.
    ae_dirty_rebuild_requests,
    /// Dirty logical-winner Merkle bucket rebuild attempts discarded because a mutation, manifest, mutable, or placement generation changed.
    ae_dirty_rebuild_retries,
    /// Dirty logical-winner Merkle bucket rebuilds that failed because of device or metadata errors.
    ae_dirty_rebuild_errors,
    /// Logical-winner `(key, version)` outputs emitted by exact run version-stream argmax pages while rebuilding dirty Merkle buckets; loser candidates read and collapsed inside the page executor are not included.
    ae_dirty_rebuild_winner_outputs,
    /// Key plus version bytes in logical-winner outputs emitted by exact run version-stream argmax pages while rebuilding dirty Merkle buckets; this is an output-volume proxy, not device bytes read or loser-candidate work.
    ae_dirty_rebuild_winner_output_bytes,
    /// Per-zone over-replication cleanup failures (best-effort; retried next pass).
    cleanup_errors,
    /// Keys the over-replication cleanup scan examined (placement checked); the rate is the cleanup scan speed.
    cleanup_scanned_keys,
    /// Not-owned keys over-replication cleanup confirmed with a batched digest probe (every owner already at or above the listed version), without reading the value or sending an apply.
    cleanup_probe_released_keys,
    /// Over-replication cleanup traversals that lapped a zone back to their sampled start key.
    cleanup_traversals,
    /// Extra cleanup pages a sweep pass ran in an idle burst (`cleanup_idle_burst_ms`) after the zone's first page of the pass.
    cleanup_idle_burst_pages,
    /// Committed over-replication cleanup forget compactions.
    cleanup_forget_compactions,
    /// Current exact-version winners removed by committed cleanup forget compactions.
    cleanup_forget_keys,
    /// Cleanup forget batches or submitted plans discarded on placement mismatch, abort, or error; keys are rediscovered by the next cleanup cycle.
    cleanup_forget_discards,
    /// Over-replication cleanup release-`forget`s deferred: in-flight reads did not drain within the quiesce bound, so the segment-freeing forget was skipped and retried next tick (the handoffs already made stand).
    cleanup_quiesce_timeouts,
    /// Background compaction passes that completed.
    compactions,
    /// Background compaction passes that failed (rolled back or poisoned).
    compaction_errors,
    /// Run-compaction plans that could not reserve their bounded output and manifest headroom; warnings are throttled independently.
    compaction_plan_failures,
    /// Background compaction passes skipped: in-flight reads did not drain within the quiesce bound (retried next tick).
    compaction_quiesce_timeouts,
    /// Segments freed by no-runs value-log compaction and by run-merge and forget commits (input runs, co-compacted value segments, orphans, and runs revoked from seed pins at the commit); runs a seed pin retires are freed later and counted in `ae_seed_pin_reaped_segments` instead.
    segments_freed,
    /// Durable index snapshots written by the maintenance loop (Stage A boot-recovery acceleration).
    snapshots,
    /// Durable index snapshot writes that failed (device I/O or no free segments); non-fatal — recovery falls back to full replay.
    snapshot_errors,
    /// Maintenance handle-B (sibling qpair) allocations that failed — the zone's off-core flush/compaction fell back and retries; sustained non-zero indicates NVMe I/O-qpair exhaustion starving compaction.
    maint_qpair_errors,
    /// Off-core run-merge compactions committed (LSM run consolidation; includes the value-log co-compaction that reclaims overwrite garbage — its freed segments are counted in `segments_freed`).
    run_merges,
    /// Cumulative wall-clock milliseconds spent in off-core run-merge passes (the merge-monolith duration).
    run_merge_ms,
    /// Cumulative bytes an off-core run-merge had to read: Σ merged inputs' index-segment footprint + Σ their live value bytes (whole-segment footprint convention — see `CompactRunsStats::input_bytes`).
    run_merge_input_bytes,
    /// Cumulative bytes an off-core run-merge rewrote: merged run index footprint + co-compacted value-log output footprint (see `CompactRunsStats::output_bytes`).
    run_merge_output_bytes,
    /// Bounded engine-wide key-filter cover-scan chunks executed on maintenance compaction lanes (each reads at most 16 MiB of one uncovered run's key stream).
    key_filter_cover_scan_chunks,
    /// Bytes of run key streams (sidecar or data blocks) read by key-filter cover scans.
    key_filter_cover_scan_bytes,
    /// Cumulative microseconds key-filter cover-scan chunks spent reading and hashing run keys.
    key_filter_cover_scan_us,
    /// Key-filter cover scans that completed and marked their run covered by the engine-wide key filter.
    key_filter_cover_scans_committed,
    /// Key-filter cover scans discarded because their run was merged away, the filter generation changed, or the zone was replaced.
    key_filter_cover_scans_discarded,
    /// Key-filter cover-scan chunks that failed with an I/O or corruption error while their run was still current (retried on a later tick).
    key_filter_cover_scan_errors,
    /// Flushes scheduled by the region-pressure trigger (free segments low + reclaimable garbage present) rather than the epoch-lag/byte-cap thresholds.
    pressure_flushes,
    /// Run-merges scheduled by the region-pressure trigger (below the normal run-count threshold).
    pressure_merges,
    /// Liveness fence trips (node lost cluster contact past the GC grace).
    fence_trips,
    /// Liveness unfences (node re-verified against peers).
    unfences,
    /// Liveness wipes (all local data discarded before re-sync).
    wipes,
    /// Failures persisting the liveness watermark (durability degraded).
    watermark_persist_errors,
    /// Failures persisting the adopted zone catalog (pre-replay optimization degraded).
    catalog_persist_errors,
    /// Ring (`SetMembers`) configs adopted.
    ring_adoptions,
    /// Zone catalog (`SetZones`) configs adopted.
    catalog_adoptions,
    /// Range map (`SetRangeMaps`) configs adopted.
    range_map_adoptions,
    /// Stale (lower-epoch) control pushes ignored.
    stale_pushes_ignored,
    /// Mutating control ops (`Set*`) refused on a cluster-control-secret mismatch.
    control_denied,
    /// Node shards that bound themselves to a cluster incarnation id carried by their first fenced control push.
    control_incarnation_adoptions,
    /// Mutating control ops (`Set*`) refused because they carried a different (or no) cluster incarnation than the one this node shard is bound to: an obsolete or independently bootstrapped PD is pushing.
    control_incarnation_rejected,
    /// Node shards that completed a graceful shutdown (drained + synced).
    graceful_shutdowns,
    /// Persistent in-RAM memtable integrity failures (the same front-coded block failing its pre-flush check on consecutive flush attempts past the configured threshold) that made the node fail fast: log the evidence, shut down without flushing the corrupt memtable and exit with code 75 so the supervisor restarts it and it replays the intact value log.
    integrity_fail_fast_total,
    /// Zone engines torn down (zone left the catalog).
    zones_dropped,
    /// Zone teardown failures (provider could not release the zone's storage).
    zone_drop_errors,
    /// Zone teardowns deferred until an in-flight sweep/read released the engine.
    zone_drops_deferred,
    /// Requests rejected because the target zone's prior incarnation is still tearing down.
    zone_teardown_rejections,
    /// PD → data-node control pushes that failed (retried until confirmed).
    pd_push_failures,
    /// Opaque range-map capability preflight handshakes attempted by the PD.
    pd_range_map_preflight_attempts,
    /// Opaque range-map delivery attempts blocked because an endpoint could not freshly certify V4 support.
    pd_range_map_preflight_blocks,
    /// Range-map SetRangeMaps requests attempted after any required capability preflight.
    pd_range_map_push_attempts,
    /// Range-map SetRangeMaps requests that failed or did not confirm the requested epoch.
    pd_range_map_push_failures,
    /// Restarted/regressed shards detected by the PD beacon (config re-pushed).
    pd_repushes,
    /// PD raft drive/step errors.
    pd_drive_errors,
    /// PD raft messages dropped without reaching the peer: its sender queue was full, a connect failed, or the write failed (raft retransmits).
    pd_raft_send_drops,
    /// PD proposals that failed to submit (retried while pending).
    pd_propose_errors,
    /// PD range ops dropped after stalling at the head of the control queue (proposal returned no-op and the effect never reflected): un-wedges the FIFO so later aborts/commits can drain; the policy re-derives the dropped action on later polls.
    pd_range_ops_dropped,
    /// Loud single-range warnings the PD emitted for a sorted zone still on one range while over the warn threshold (the concentration footgun); throttled to ≤1/zone/60s.
    pd_single_range_warnings,
    /// Loud concentration warnings the PD emitted for a multi-range sorted zone whose hottest range holds at least the configured share of entries; throttled to ≤1/zone/60s.
    pd_range_concentration_warnings,
    /// Oversized sorted-zone ranges the PD auto-split (size-based resharding); see `VALS_PD_RANGE_SPLIT_THRESHOLD`.
    pd_range_splits,
    /// Adjacent under-full sorted-zone range pairs the PD auto-merged (shrink-reclaim of a fragmented range map); see `VALS_PD_RANGE_MERGE_THRESHOLD`.
    pd_range_merges,
    /// Adjacent sorted-zone ranges the PD merged because both hold no uncovered entries, take no requests, and every owner reports their whole span covered by range tombstones (queue/ring debris behind a deleted head); see `VALS_PD_RANGE_MERGE_COVERED`.
    pd_range_covered_merges,
    /// Canonical sorted-range placements assigned a searched opaque token that balances replica slots, primary roles, and available shard topology.
    pd_canonical_placements_balanced,
    /// Canonical sorted-range placements that retained legacy lower-bound or inherited placement because opaque placement was unavailable or no valid token was found.
    pd_canonical_placements_fallback,
    /// Dry-run balance polls skipped because the operator/capability/range-map acknowledgement gate was not satisfied.
    pd_balance_noop_capability,
    /// Dry-run balance polls skipped because fresh, consistent range estimates or shard topology were unavailable.
    pd_balance_noop_blocked_input,
    /// Dry-run balance polls skipped while any confirmed re-home was active.
    pd_balance_noop_active_rehome,
    /// Dry-run balance polls with normalized hottest node and engine loads at or below the configured high-water threshold.
    pd_balance_noop_below_high,
    /// Dry-run balance polls whose best safe move did not clear the configured minimum improvement.
    pd_balance_noop_below_improvement,
    /// Dry-run balance polls with no RF/scope-exact realizable placement that lexicographically improves hottest engine, hottest node, then normalized deviation.
    pd_balance_noop_no_feasible,
    /// Dry-run balance polls attributed to an oversized indivisible range that should be split before placement can converge.
    pd_balance_noop_split_first,
    /// RF/scope-exact target node sets not discovered within the global per-range opaque-token nonce bounds.
    pd_balance_token_search_exhausted,
    /// Deterministic dry-run balance proposals submitted to the confirmed re-home protocol by the opt-in executor.
    pd_balance_exec_prepares,
    /// Planned range entries copied to newly selected engines by submitted balance proposals.
    pd_balance_exec_copy_entries,
    /// Opt-in balance moves observed to commit their exact proposed opaque token.
    pd_balance_exec_commits,
    /// Opt-in balance moves observed to abort while retaining the old authoritative token.
    pd_balance_exec_aborts,
    /// Complete fresh statistics polls consumed after a balance prepare completed, aborted, or was rejected before replanning.
    pd_balance_exec_cooldown_polls,
    /// Balance prepare commands rejected because their immutable proposal snapshot was stale.
    pd_balance_exec_stale,
    /// Stale balance proposals rejected because their immutable identity did not verify.
    pd_balance_exec_stale_identity,
    /// Stale balance proposals rejected because the execution or opaque-map operator gate was disabled.
    pd_balance_exec_stale_gate,
    /// Stale balance proposals rejected because endpoint opaque-token capability or range-map acknowledgements were lost.
    pd_balance_exec_stale_capability,
    /// Stale balance proposals rejected because their complete endpoint-statistics poll generation was no longer authoritative.
    pd_balance_exec_stale_poll,
    /// Stale balance proposals rejected because membership, weight, address, ring epoch, or ring configuration changed.
    pd_balance_exec_stale_membership,
    /// Stale balance proposals rejected because the zone catalog, RF, scope, or catalog epoch changed.
    pd_balance_exec_stale_catalog,
    /// Stale balance proposals rejected because the selected range map, bounds, token, debt, pending state, or map epoch changed.
    pd_balance_exec_stale_range_map,
    /// Stale balance proposals rejected because the fresh endpoint shard topology no longer realized the planned targets.
    pd_balance_exec_stale_topology,
    /// Polls with an eligible debt range and free slot where load was balanced, range counts were incomplete, or no debt range touched the hottest over-threshold node or engine.
    pd_rehome_debt_deferred_balanced,
    /// Polls that held back starting a debt re-home or balance proposal because a data node was unreachable or reported a restart-caused (recovery) seed in progress (`rehome_recovery_defer_ms`); copy-complete commits are never held.
    pd_rehome_starts_deferred_recovery,
    /// Range re-homes prepared by the PD.
    pd_rehomes_started,
    /// Range re-homes confirmed and cut over by the PD.
    pd_rehomes_completed,
    /// Range re-homes aborted by the PD after an unsafe mirror or report gap.
    pd_rehomes_aborted,
    /// Range re-home aborts attributed to a required owner staying silent past the report deadline (node/shard unreachable or its stats replies lost).
    pd_rehomes_aborted_report_gap,
    /// Range re-home aborts attributed to a current owner positively reporting mirror failures at the prepare epoch.
    pd_rehomes_aborted_mirror_failure,
    /// Range re-home aborts attributed to a pending owner that kept reporting but made no copy progress for a full deadline period.
    pd_rehomes_aborted_copy_stall,
    /// Re-home mirror RPC entries positively accepted by a pending owner.
    rehome_mirror_applied,
    /// Re-home mirror RPC entries refused with retryable serving backpressure.
    rehome_mirror_busy,
    /// Re-home mirror RPC entries refused because the pending owner had not adopted the required routing epoch.
    rehome_mirror_stale,
    /// Re-home mirror RPC entries whose response exceeded the peer deadline; each is retried with the same LWW version before it can poison a move.
    rehome_mirror_timeouts,
    /// Re-home mirror RPC entries that failed because the target connection was unavailable.
    rehome_mirror_unreachable,
    /// Re-home mirror RPC entries that returned a malformed, unexpected, or explicit remote error response.
    rehome_mirror_remote_errors,
    /// Re-home mirror RPC entries rejected by target-side version admission.
    rehome_mirror_rejected,
    /// LWW-idempotent re-home mirror retries issued after an RPC deadline.
    rehome_mirror_timeout_retries,
    /// Timed-out re-home mirrors resolved by a later positive acknowledgement of the same version.
    rehome_mirror_timeout_retry_successes,
    /// Timed-out re-home mirrors whose later retry returned a non-applied result; the earlier uncertainty remains and poisons the move.
    rehome_mirror_timeout_retry_unresolved,
    /// Re-home mirrors that still timed out after the bounded retry budget and therefore poisoned the move.
    rehome_mirror_timeout_retry_exhausted,
    /// Re-home copy pages a pending owner pulled from a current owner and applied.
    rehome_copy_pages,
    /// Re-home copies resumed from a retained cursor after a re-prepare, validated against the source's unmirrored-write mark.
    rehome_copy_resumes,
    /// Re-home copies restarted from the range start across an abort gap: the source's unmirrored-write mark moved, the pinned source was lost, or local cleanup forgot keys in the zone since the cursor was cut.
    rehome_copy_restarts,
    /// Failures persisting the durable hint store.
    hint_persist_errors,
    /// Inbound connections dropped by a server at the protocol handshake (bad magic, or an unsupported/too-old version).
    handshake_denied,
    /// Inbound connections dropped by a server because a frame's declared length exceeded MAX_FRAME (64 MiB) — typically a client request that was not split under the cap (e.g. an oversized `PutBatch`).
    oversized_frames,
    /// Inbound connections dropped by a server on malformed framing (a frame too short to carry a request id).
    malformed_frames,
    /// Compaction passes skipped because the zone's region lacks the free space to hold both the old and new copies of the live data at once (a full rewrite needs free segments ≥ live data, so it cannot run once a region is more than ~half live).
    compaction_headroom_skips,
    /// Compaction passes skipped because too little of the zone was dead (reclaimable) to justify a stop-the-world full rewrite.
    compaction_skipped_clean,
    /// Records written to local engines (each `Apply`/`BatchApply` entry — a put or a tombstone).
    records_written,
    /// Value bytes written to local engines (Σ value length over `records_written`; tombstones contribute 0).
    record_bytes_written,
    /// Records returned to clients by point reads (`Read`/`multi_get` hits; a miss or tombstone does not count).
    records_read,
    /// Scan requests rejected with retryable Busy because the shard's scan admission limit is full.
    scan_admission_busy,
    /// Entries returned by range scans (Σ page length over `Scan` replies).
    scan_entries_returned,
    /// Staged range-scan merge attempts restarted because their captured storage state was invalidated.
    scan_staged_restarts,
    /// Staged range scans that exhausted their bounded restart budget and fell back to synchronous planning.
    scan_sync_fallbacks,
    /// Range-scan span executions or finalize passes retried after their captured storage state was invalidated.
    scan_span_retries,
    /// Returned range-scan keys individually re-resolved because their winner advanced during the scan.
    scan_finalize_patches,
    /// Group-commit durability barriers executed for device-persistent single or batch applies.
    sync_commits,
    /// Records acknowledged through a device-persistent group-commit barrier.
    synced_writes,
    /// Device cache flushes submitted by the node-wide flush domain; each can release group commits of every shard on the device.
    domain_flushes,
    /// Group commits that fell back to their own blocking flush after the node-wide flush domain made no progress for 2 s.
    domain_flush_stalls,
    #[doc = "Total microseconds from domain Flush submit entry to publication/release of the inflight slot, including errors and abandoned claims; rate divided by 1e6 gives domain busy duty summed across controllers."]
    domain_flush_busy_us_total,
    /// Estimated server-poll busy iterations, sampled one in 256 (shard threads).
    server_poll_busy_iterations,
    /// Estimated microseconds spent inside busy server polls, sampled one in 256 (shard threads).
    server_poll_busy_us_total,
    /// Blocking server polls (shard threads).
    server_poll_blocking_iterations,
    /// Single `Apply` writes whose LWW old-version needed an async run-block read (the cold-churn read-before-write): a flushed, bloom-positive key not resident in the memtable.
    apply_run_reads,
    /// `BatchApply` entries whose LWW old-version needed an async run-block read (the batch analogue of `apply_run_reads`): flushed, bloom-positive keys not resident in the memtable, resolved off-borrow with the reads overlapped instead of one synchronous QD1 read per record on the serving core.
    batch_apply_run_reads,
    /// Run locators collected for write-path LWW old-version probes (Σ bloom-positive resident runs over every `Apply`/`BatchApply` entry that missed the memtable): the write-side run-probe amplification, growing with resident run count.
    apply_probe_runs,
    /// Candidate run index blocks actually read and collapsed to settle write-path LWW old-version probes; authoritative argmax resolution reads every collected candidate locator.
    apply_probe_blocks,
    /// Device bytes of the run blocks counted by `apply_probe_blocks` (block lengths as stored, before LBA alignment).
    apply_probe_bytes,
    /// Feature-enabled blind unconditional candidates successfully appended before any requested batch durability barrier; a later sync or mirror failure can still prevent request acknowledgement.
    blind_candidates_appended,
    /// Feature-enabled blind unconditional candidates rejected before append for exceeding the local clock-skew bound.
    blind_rejected_skew,
    /// Feature-enabled blind live-value candidates rejected before append by the monotone GC admission floor; tombstones do not contribute.
    blind_rejected_gc_floor,
    /// Incoming tombstones older than the GC admission floor for a key with no local version, acknowledged without being stored because tombstone GC would only drop them again.
    expired_tombstones_skipped,
    /// Feature-enabled blind candidate append attempts that returned an engine error, including backpressure and device-full failures.
    blind_append_errors,
    /// Requested durability barriers that failed after feature-enabled blind batch candidates had been appended; those candidates were not acknowledged by the failed request.
    blind_sync_errors,
    /// Run locators collected for point-read and digest probes (Σ bloom-positive resident runs over every `Read`/`Digest` that missed the memtable): the read-side run-probe amplification, growing with resident run count.
    read_probe_runs,
    /// Candidate run index blocks actually read and collapsed to settle point-read and digest probes; authoritative argmax resolution reads every collected candidate locator.
    read_probe_blocks,
    /// Device bytes of the run blocks counted by `read_probe_blocks` (block lengths as stored, before LBA alignment); divide by Get/Digest probes for bytes read per lookup.
    read_probe_bytes,
    /// Value-log device read operations submitted for point-read values after winner resolution; retries are counted because they issue another device read.
    point_value_device_reads,
    /// Device blocks requested by `point_value_device_reads`, including alignment/framing blocks around each value-log record.
    point_value_device_blocks,
    /// `BatchApply` requests shed with retryable Busy instead of waiting behind a full staged-apply gate long enough to hit the caller's RPC deadline.
    batch_apply_gate_busy,
    /// Transient `accept(2)` errors a shard's accept loop shrugged off and retried: resource-exhaustion / interrupted accepts (`EMFILE`/`ENFILE`/`ENOMEM`/`ENOBUFS`/`ECONNABORTED`/`EINTR`) that used to permanently kill the listener.
    accept_transient_errors,
    /// Inbound transport connections rejected because the process-wide connection budget was exhausted.
    transport_connection_rejections,
    /// Inbound requests rejected because the process-wide in-flight request budget was exhausted.
    transport_request_rejections,
    /// Connections closed because a reply could not reserve process-wide queued-byte credit.
    transport_queued_byte_rejections,
    /// Bucket-digest requests rejected because the dedicated serving byte budget was exhausted.
    ae_digest_serve_rejections,
    /// Bucket-digest pages cut short by the per-request serve time budget and returned with a resume key.
    ae_digest_serve_time_truncations,
    /// Connections closed before completing their initial protocol frame deadline.
    transport_initial_timeouts,
    /// Connections closed after stalling partway through a declared frame.
    transport_frame_timeouts,
    /// Established connections closed after the finite idle deadline.
    transport_idle_timeouts,
    /// Connections closed after queued output made no write progress through the write deadline.
    transport_write_timeouts,
    /// Keys in the shard-scoped one-of-1024 batched winner-directory probe sample; all winner-probe category and length counters use the same sample across connections.
    winner_probe_lookups,
    /// Batched winner-directory lookups resolved by an authoritative full-key match in the hash home slot.
    winner_probe_home_slot_hits,
    /// Batched winner-directory absent-key lookups terminated by an empty control bucket.
    winner_probe_empty_misses,
    /// Occupied winner-directory buckets skipped because their control fingerprint differed from the requested key.
    winner_probe_fingerprint_mismatches,
    /// Deleted winner-directory control buckets traversed while probing.
    winner_probe_deleted_buckets,
    /// Batched winner-directory lookups that continued beyond the home control bucket after a hash-table collision or deletion.
    winner_probe_collision_fallbacks,
    /// Matching-fingerprint winner-directory candidates rejected by the authoritative full-key comparison.
    winner_probe_full_key_collisions,
    /// Total winner-directory control buckets examined across batched lookups.
    winner_probe_length_total,
    /// Batched winner-directory lookups examining at most one control bucket.
    winner_probe_length_le_1,
    /// Batched winner-directory lookups examining two through four control buckets.
    winner_probe_length_2_4,
    /// Batched winner-directory lookups examining five through eight control buckets.
    winner_probe_length_5_8,
    /// Batched winner-directory lookups examining more than eight control buckets.
    winner_probe_length_gt_8,
}
static COUNTERS: Counters = Counters::new();

pub fn counters() -> &'static Counters {
    &COUNTERS
}

pub struct DirtyAgeGauge;

impl DirtyAgeGauge {
    pub fn get(&self) -> u64 {
        match gauges().ae_dirty_oldest_since_ms.get() {
            0 => 0,
            since => monotonic_ms().saturating_sub(since),
        }
    }
}

pub struct Gauges {
    pub live_keys: Gauge,
    pub used_segments: Gauge,
    pub free_segments: Gauge,
    pub min_zone_free_segments: Gauge,
    pub resident_runs: Gauge,
    pub frozen_runs: Gauge,
    pub stranded_runs: Gauge,
    pub wipe_pending: Gauge,
    pub max_run_bytes: Gauge,
    pub shadowed_value_bytes_est: Gauge,
    pub shadow_folds: Gauge,
    pub shadow_fold_disarms: Gauge,
    pub key_filter_probes: Gauge,
    pub key_filter_negatives: Gauge,
    pub key_filter_run_probes: Gauge,
    pub key_filter_sub_filters: Gauge,
    pub key_filter_max_sub_filters: Gauge,
    pub key_filter_max_est_fpr_ppm: Gauge,
    pub key_filter_max_uncovered_runs: Gauge,
    pub key_filter_rebuild_wait_seconds_max: Gauge,
    pub key_filter_rebuild_wait_seconds_total: Gauge,
    pub key_filter_rebuild_urgent_grants: Gauge,
    pub key_filter_rebuild_open_seconds_max: Gauge,
    pub key_filter_bytes: Gauge,
    pub key_filter_resident_bytes: Gauge,
    pub key_filter_retired_bytes: Gauge,
    pub key_filter_retired_resident_bytes: Gauge,
    pub key_filter_inserted_keys: Gauge,
    pub key_filter_capacity_keys: Gauge,
    pub key_filter_rebuild_bytes: Gauge,
    pub key_filter_rebuild_resident_bytes: Gauge,
    pub key_filter_rebuilding: Gauge,
    pub key_filter_rebuild_covered_runs: Gauge,
    pub key_filter_rebuilds_started: Gauge,
    pub key_filter_rebuilds_completed: Gauge,
    pub key_filter_rebuilds_abandoned: Gauge,
    pub key_filter_rebuild_peak_bytes: Gauge,
    pub key_filter_uncovered_runs: Gauge,
    pub key_filter_uncovered_run_bytes: Gauge,
    pub key_filter_est_fpr_ppm_sum: Gauge,
    pub key_filter_resets: Gauge,
    pub key_filter_cover_scan_failures: Gauge,
    pub key_filter_cover_scan_mismatches: Gauge,
    pub key_filter_cover_scan_backoff_runs: Gauge,
    pub process_resident_bytes: Gauge,
    pub process_swap_bytes: Gauge,
    pub ram_budget_bytes: Gauge,
    pub ram_headroom_bytes: Gauge,
    pub ram_charged_bytes: Gauge,
    pub ram_reserved_ae_seed_bytes: Gauge,
    pub ram_reserved_flush_bytes: Gauge,
    pub ram_reserved_merge_bytes: Gauge,
    pub ram_reserve_capacity_bytes: Gauge,
    pub ram_reserve_debt_bytes: Gauge,
    pub ram_reserve_oldest_wait_ms: Gauge,
    pub merge_in_flight: Gauge,
    pub merge_in_flight_input_bytes: Gauge,
    pub ae_seed_staged_bytes: Gauge,
    pub ae_seed_active: Gauge,
    pub ae_seed_journal_bytes: Gauge,
    pub ae_seed_pinned_bytes: Gauge,
    pub ae_seed_drivers: Gauge,
    pub ae_digest_serve_bytes: Gauge,
    pub ae_dirty_buckets: Gauge,
    pub ae_dirty_oldest_ms: DirtyAgeGauge,
    pub ae_dirty_oldest_since_ms: Gauge,
    pub memtable_bytes_total: Gauge,
    pub memtable_allocated_bytes_total: Gauge,
    pub flush_protection_bytes: Gauge,
    pub allocator_in_use_bytes: Gauge,
    pub allocator_free_bytes: Gauge,
    pub allocator_trim_max_ns: Gauge,
    pub run_metadata_resident_bytes: Gauge,
    pub run_fence_resident_bytes: Gauge,
    pub run_filter_resident_bytes: Gauge,
    pub run_filter_payload_bytes: Gauge,
    pub run_fence_payload_bytes: Gauge,
    pub run_entries: Gauge,
    pub winner_directory_keys: Gauge,
    pub winner_directory_bytes: Gauge,
    pub flush_build_bytes: Gauge,
    pub zone_engines: Gauge,
    pub zone_drop_pending_age_seconds: Gauge,
    pub sweep_pass_age_seconds: Gauge,
    pub pd_state_capacity_bytes: Gauge,
    pub pd_state_used_bytes: Gauge,
    pub pd_state_reserved_bytes: Gauge,
    pub pd_state_headroom_bytes: Gauge,
    pub pd_sorted_ranges: Gauge,
    pub pd_single_range_sorted_zones: Gauge,
    pub pd_max_range_entries: Gauge,
    pub pd_oversized_single_range_zones: Gauge,
    pub pd_concentrated_range_zones: Gauge,
    pub pd_range_skew_pct: Gauge,
    pub pd_node_load_skew_pct: Gauge,
    pub pd_engine_load_skew_pct: Gauge,
    pub pd_unrehomed_ranges: Gauge,
    pub pd_rehomes_active: Gauge,
    pub pd_rehomes_delivery_stalled: Gauge,
    pub pd_recovery_down_nodes: Gauge,
    pub pd_recovery_seeding_nodes: Gauge,
    pub pd_recovery_restarting_nodes: Gauge,
    pub pd_rehome_recovery_hold: Gauge,
    pub pd_balance_before_node_pct: Gauge,
    pub pd_balance_after_node_pct: Gauge,
    pub pd_balance_before_engine_pct: Gauge,
    pub pd_balance_after_engine_pct: Gauge,
    pub pd_balance_proposed_entries: Gauge,
    pub pd_balance_proposed_copy_entries: Gauge,
    pub pd_balance_movable_ranges: Gauge,
    pub pd_balance_blocked_ranges: Gauge,
    pub pd_balance_proposal: Gauge,
    pub pd_balance_token_exhaustions: Gauge,
}

impl Gauges {
    const fn new() -> Self {
        Gauges {
            live_keys: Gauge::new(),
            used_segments: Gauge::new(),
            free_segments: Gauge::new(),
            min_zone_free_segments: Gauge::new(),
            resident_runs: Gauge::new(),
            frozen_runs: Gauge::new(),
            stranded_runs: Gauge::new(),
            wipe_pending: Gauge::new(),
            max_run_bytes: Gauge::new(),
            shadowed_value_bytes_est: Gauge::new(),
            shadow_folds: Gauge::new(),
            shadow_fold_disarms: Gauge::new(),
            key_filter_probes: Gauge::new(),
            key_filter_negatives: Gauge::new(),
            key_filter_run_probes: Gauge::new(),
            key_filter_sub_filters: Gauge::new(),
            key_filter_max_sub_filters: Gauge::new(),
            key_filter_max_est_fpr_ppm: Gauge::new(),
            key_filter_max_uncovered_runs: Gauge::new(),
            key_filter_rebuild_wait_seconds_max: Gauge::new(),
            key_filter_rebuild_wait_seconds_total: Gauge::new(),
            key_filter_rebuild_urgent_grants: Gauge::new(),
            key_filter_rebuild_open_seconds_max: Gauge::new(),
            key_filter_bytes: Gauge::new(),
            key_filter_resident_bytes: Gauge::new(),
            key_filter_retired_bytes: Gauge::new(),
            key_filter_retired_resident_bytes: Gauge::new(),
            key_filter_inserted_keys: Gauge::new(),
            key_filter_capacity_keys: Gauge::new(),
            key_filter_rebuild_bytes: Gauge::new(),
            key_filter_rebuild_resident_bytes: Gauge::new(),
            key_filter_rebuilding: Gauge::new(),
            key_filter_rebuild_covered_runs: Gauge::new(),
            key_filter_rebuilds_started: Gauge::new(),
            key_filter_rebuilds_completed: Gauge::new(),
            key_filter_rebuilds_abandoned: Gauge::new(),
            key_filter_rebuild_peak_bytes: Gauge::new(),
            key_filter_uncovered_runs: Gauge::new(),
            key_filter_uncovered_run_bytes: Gauge::new(),
            key_filter_est_fpr_ppm_sum: Gauge::new(),
            key_filter_resets: Gauge::new(),
            key_filter_cover_scan_failures: Gauge::new(),
            key_filter_cover_scan_mismatches: Gauge::new(),
            key_filter_cover_scan_backoff_runs: Gauge::new(),
            process_resident_bytes: Gauge::new(),
            process_swap_bytes: Gauge::new(),
            ram_budget_bytes: Gauge::new(),
            ram_headroom_bytes: Gauge::new(),
            ram_charged_bytes: Gauge::new(),
            ram_reserved_ae_seed_bytes: Gauge::new(),
            ram_reserved_flush_bytes: Gauge::new(),
            ram_reserved_merge_bytes: Gauge::new(),
            ram_reserve_capacity_bytes: Gauge::new(),
            ram_reserve_debt_bytes: Gauge::new(),
            ram_reserve_oldest_wait_ms: Gauge::new(),
            merge_in_flight: Gauge::new(),
            merge_in_flight_input_bytes: Gauge::new(),
            ae_seed_staged_bytes: Gauge::new(),
            ae_seed_active: Gauge::new(),
            ae_seed_journal_bytes: Gauge::new(),
            ae_seed_pinned_bytes: Gauge::new(),
            ae_seed_drivers: Gauge::new(),
            ae_digest_serve_bytes: Gauge::new(),
            ae_dirty_buckets: Gauge::new(),
            ae_dirty_oldest_ms: DirtyAgeGauge,
            ae_dirty_oldest_since_ms: Gauge::new(),
            memtable_bytes_total: Gauge::new(),
            memtable_allocated_bytes_total: Gauge::new(),
            flush_protection_bytes: Gauge::new(),
            allocator_in_use_bytes: Gauge::new(),
            allocator_free_bytes: Gauge::new(),
            allocator_trim_max_ns: Gauge::new(),
            run_metadata_resident_bytes: Gauge::new(),
            run_fence_resident_bytes: Gauge::new(),
            run_filter_resident_bytes: Gauge::new(),
            run_filter_payload_bytes: Gauge::new(),
            run_fence_payload_bytes: Gauge::new(),
            run_entries: Gauge::new(),
            winner_directory_keys: Gauge::new(),
            winner_directory_bytes: Gauge::new(),
            flush_build_bytes: Gauge::new(),
            zone_engines: Gauge::new(),
            zone_drop_pending_age_seconds: Gauge::new(),
            sweep_pass_age_seconds: Gauge::new(),
            pd_state_capacity_bytes: Gauge::new(),
            pd_state_used_bytes: Gauge::new(),
            pd_state_reserved_bytes: Gauge::new(),
            pd_state_headroom_bytes: Gauge::new(),
            pd_sorted_ranges: Gauge::new(),
            pd_single_range_sorted_zones: Gauge::new(),
            pd_max_range_entries: Gauge::new(),
            pd_oversized_single_range_zones: Gauge::new(),
            pd_concentrated_range_zones: Gauge::new(),
            pd_range_skew_pct: Gauge::new(),
            pd_node_load_skew_pct: Gauge::new(),
            pd_engine_load_skew_pct: Gauge::new(),
            pd_unrehomed_ranges: Gauge::new(),
            pd_rehomes_active: Gauge::new(),
            pd_rehomes_delivery_stalled: Gauge::new(),
            pd_recovery_down_nodes: Gauge::new(),
            pd_recovery_seeding_nodes: Gauge::new(),
            pd_recovery_restarting_nodes: Gauge::new(),
            pd_rehome_recovery_hold: Gauge::new(),
            pd_balance_before_node_pct: Gauge::new(),
            pd_balance_after_node_pct: Gauge::new(),
            pd_balance_before_engine_pct: Gauge::new(),
            pd_balance_after_engine_pct: Gauge::new(),
            pd_balance_proposed_entries: Gauge::new(),
            pd_balance_proposed_copy_entries: Gauge::new(),
            pd_balance_movable_ranges: Gauge::new(),
            pd_balance_blocked_ranges: Gauge::new(),
            pd_balance_proposal: Gauge::new(),
            pd_balance_token_exhaustions: Gauge::new(),
        }
    }
}

static GAUGES: Gauges = Gauges::new();

pub const RUN_MERGES_BY_KIND_HELP: &str = "Committed off-core run merges by plan kind: recurring (tiered newest suffix), forced (run-count valve window), pressure, shadow (pre-floor shadow fold), vlog_gc (aged value-log GC), lonegc (tombstone-GC merge of every run), forget (cleanup forget, including over-replication cleanup).";

pub const RUN_MERGE_INPUT_BYTES_BY_KIND_HELP: &str = "Input bytes of committed off-core run merges by plan kind (same whole-segment convention as run_merge_input_bytes: input index segments plus rewritten value bytes).";

struct MergeKindCounters {
    merges: [Counter; MERGE_KINDS],
    input_bytes: [Counter; MERGE_KINDS],
    output_bytes: [Counter; MERGE_KINDS],
}

const MERGE_KINDS: usize = vals_store::MergeKind::ALL.len();

const MERGE_KIND_COUNTER: Counter = Counter::new();

static MERGE_KIND_COUNTERS: MergeKindCounters = MergeKindCounters {
    merges: [MERGE_KIND_COUNTER; MERGE_KINDS],
    input_bytes: [MERGE_KIND_COUNTER; MERGE_KINDS],
    output_bytes: [MERGE_KIND_COUNTER; MERGE_KINDS],
};

pub fn record_merge_kind(kind: vals_store::MergeKind, input_bytes: u64, output_bytes: u64) {
    let Some(i) = vals_store::MergeKind::ALL.iter().position(|k| *k == kind) else {
        return;
    };
    MERGE_KIND_COUNTERS.merges[i].inc();
    MERGE_KIND_COUNTERS.input_bytes[i].add(input_bytes);
    MERGE_KIND_COUNTERS.output_bytes[i].add(output_bytes);
}

pub fn merge_output_kind_metrics() -> Vec<(&'static str, u64)> {
    vals_store::MergeKind::ALL.iter().enumerate()
        .map(|(i, kind)| (kind.label(), MERGE_KIND_COUNTERS.output_bytes[i].get())).collect()
}

pub fn merge_kind_metrics() -> Vec<(&'static str, u64, u64)> {
    vals_store::MergeKind::ALL
        .iter()
        .enumerate()
        .map(|(i, kind)| {
            (
                kind.label(),
                MERGE_KIND_COUNTERS.merges[i].get(),
                MERGE_KIND_COUNTERS.input_bytes[i].get(),
            )
        })
        .collect()
}

const CORRUPT_READ_KINDS: usize = vals_store::vlog::CORRUPT_READ_KINDS.len();

static CORRUPT_READS: [Counter; CORRUPT_READ_KINDS] = [MERGE_KIND_COUNTER; CORRUPT_READ_KINDS];

pub const CORRUPT_READS_HELP: &str = "Served value reads (get, scan, mixed batch, AE sync values) that failed record validation against the index entry, by kind: magic, kind, length, crc, key, version, other. Counted once per failed reply after the storage generation was confirmed unchanged; retried races are excluded.";

pub fn record_corrupt_read(kind: &str) {
    let i = vals_store::vlog::CORRUPT_READ_KINDS
        .iter()
        .position(|k| *k == kind)
        .unwrap_or(CORRUPT_READ_KINDS - 1);
    CORRUPT_READS[i].inc();
}

pub fn corrupt_read_metrics() -> Vec<(&'static str, u64)> {
    vals_store::vlog::CORRUPT_READ_KINDS
        .iter()
        .zip(&CORRUPT_READS)
        .map(|(kind, c)| (*kind, c.get()))
        .collect()
}

#[repr(align(64))]
pub struct ShardWriteCounters {
    pub writes: Counter,
    pub ops: Counter,
    pub lookups: std::sync::Mutex<[[u64; 5]; 4]>,
}

fn shard_write_registry() -> &'static std::sync::Mutex<std::collections::BTreeMap<usize, std::sync::Arc<ShardWriteCounters>>> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeMap<usize, std::sync::Arc<ShardWriteCounters>>>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

pub fn shard_write_counters(shard: usize) -> std::sync::Arc<ShardWriteCounters> {
    shard_write_registry().lock().unwrap().entry(shard).or_insert_with(|| {
        std::sync::Arc::new(ShardWriteCounters { writes: Counter::new(), ops: Counter::new(), lookups: std::sync::Mutex::new([[0; 5]; 4]) })
    }).clone()
}

pub fn shard_write_metrics() -> Vec<(usize, u64, u64)> {
    shard_write_registry().lock().unwrap().iter()
        .map(|(id, c)| (*id, c.writes.get(), c.ops.get())).collect()
}

pub fn lookup_metrics() -> [[u64; 5]; 4] {
    let mut totals = [[0; 5]; 4];
    for shard in shard_write_registry().lock().unwrap().values() {
        let counts = shard.lookups.lock().unwrap();
        for i in 0..4 { for j in 0..5 { totals[i][j] += counts[i][j]; } }
    }
    totals
}

#[derive(Clone)]
pub struct EngineMetrics {
    pub resident_runs: u64,
    pub frozen_runs: u64,
    pub stranded_runs: u64,
    pub free_fraction_ppm: u64,
    pub rt_records: u64,
}

fn engine_registry() -> &'static std::sync::Mutex<std::collections::BTreeMap<(usize, u32), EngineMetrics>> {
    static REGISTRY: std::sync::OnceLock<std::sync::Mutex<std::collections::BTreeMap<(usize, u32), EngineMetrics>>> = std::sync::OnceLock::new();
    REGISTRY.get_or_init(Default::default)
}

pub fn set_engine_metrics(shard: usize, zone: u32, metrics: EngineMetrics) {
    engine_registry().lock().unwrap().insert((shard, zone), metrics);
}

pub fn remove_engine_metrics(shard: usize, zone: u32) {
    engine_registry().lock().unwrap().remove(&(shard, zone));
}

pub fn engine_metrics() -> Vec<(String, EngineMetrics)> {
    engine_registry().lock().unwrap().iter()
        .map(|((shard, zone), m)| (format!("{shard}/{zone}"), m.clone())).collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeDeleteRefusal {
    Stale,
    Skew,
    Cap,
    Unsupported,
    Space,
    Mirror,
    Error,
}

pub const RANGE_DELETE_REFUSAL_REASONS: [&str; 7] =
    ["stale", "skew", "cap", "unsupported", "space", "mirror", "error"];

static RANGE_DELETE_REFUSED: [std::sync::atomic::AtomicU64; 7] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 7];

pub fn note_range_delete_refused(reason: RangeDeleteRefusal) {
    RANGE_DELETE_REFUSED[reason as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub fn range_delete_refused_metrics() -> [(&'static str, u64); 7] {
    std::array::from_fn(|i| {
        (
            RANGE_DELETE_REFUSAL_REASONS[i],
            RANGE_DELETE_REFUSED[i].load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RtExchangeResult {
    Same,
    Pulled,
    Error,
}

pub const RT_EXCHANGE_RESULTS: [&str; 3] = ["same", "pulled", "error"];

static RT_EXCHANGES: [std::sync::atomic::AtomicU64; 3] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 3];

pub fn note_rt_exchange(result: RtExchangeResult) {
    RT_EXCHANGES[result as usize].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub fn rt_exchange_metrics() -> [(&'static str, u64); 3] {
    std::array::from_fn(|i| {
        (
            RT_EXCHANGE_RESULTS[i],
            RT_EXCHANGES[i].load(std::sync::atomic::Ordering::Relaxed),
        )
    })
}

pub fn pending_merge_input_bytes() -> u64 { crate::ram_broker::pending_merge_input_bytes() }

pub fn ram_class_metrics() -> Vec<(&'static str, u64, u64, u64, u64)> {
    crate::ram_broker::class_metrics()
}

pub fn gauges() -> &'static Gauges {
    &GAUGES
}

pub fn monotonic_ms() -> u64 {
    static STARTED: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    STARTED
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis()
        .saturating_add(1) as u64
}

#[derive(Clone, Copy)]
pub enum PdRangeMapDeliveryEvent {
    PreflightAttempt,
    PreflightBlock,
    PushAttempt,
    PushFailure,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PdRangeMapEndpointMetrics {
    pub endpoint: String,
    pub preflight_attempts: u64,
    pub preflight_blocks: u64,
    pub push_attempts: u64,
    pub push_failures: u64,
    pub last_confirmed_age_seconds: Option<u64>,
}

#[derive(Default)]
struct PdRangeMapEndpointState {
    preflight_attempts: u64,
    preflight_blocks: u64,
    push_attempts: u64,
    push_failures: u64,
    last_confirmed_ms: Option<u64>,
}

fn pd_range_map_endpoints(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, PdRangeMapEndpointState>> {
    static ENDPOINTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, PdRangeMapEndpointState>>,
    > = std::sync::OnceLock::new();
    ENDPOINTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

pub fn record_pd_range_map_delivery(
    endpoint: std::net::SocketAddr,
    event: PdRangeMapDeliveryEvent,
) {
    let mut endpoints = pd_range_map_endpoints().lock().unwrap();
    let state = endpoints.entry(endpoint.to_string()).or_default();
    match event {
        PdRangeMapDeliveryEvent::PreflightAttempt => {
            state.preflight_attempts = state.preflight_attempts.saturating_add(1);
        }
        PdRangeMapDeliveryEvent::PreflightBlock => {
            state.preflight_blocks = state.preflight_blocks.saturating_add(1);
        }
        PdRangeMapDeliveryEvent::PushAttempt => {
            state.push_attempts = state.push_attempts.saturating_add(1);
        }
        PdRangeMapDeliveryEvent::PushFailure => {
            state.push_failures = state.push_failures.saturating_add(1);
        }
    }
}

pub fn confirm_pd_range_map_delivery(endpoint: std::net::SocketAddr) {
    pd_range_map_endpoints()
        .lock()
        .unwrap()
        .entry(endpoint.to_string())
        .or_default()
        .last_confirmed_ms = Some(monotonic_ms());
}

pub fn pd_range_map_endpoint_metrics() -> Vec<PdRangeMapEndpointMetrics> {
    let now = monotonic_ms();
    let endpoints = pd_range_map_endpoints().lock().unwrap();
    let mut metrics: Vec<_> = endpoints
        .iter()
        .map(|(endpoint, state)| PdRangeMapEndpointMetrics {
            endpoint: endpoint.clone(),
            preflight_attempts: state.preflight_attempts,
            preflight_blocks: state.preflight_blocks,
            push_attempts: state.push_attempts,
            push_failures: state.push_failures,
            last_confirmed_age_seconds: state
                .last_confirmed_ms
                .map(|at| now.saturating_sub(at) / 1000),
        })
        .collect();
    metrics.sort_unstable_by(|a, b| a.endpoint.cmp(&b.endpoint));
    metrics
}

const ALLOCATOR_SAMPLE_INTERVAL_MS: u64 = 4_000;
static ALLOCATOR_SAMPLED_AT_MS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg_attr(not(all(target_os = "linux", target_env = "gnu")), allow(dead_code))]
fn allocator_sample_due(last: &std::sync::atomic::AtomicU64, now_ms: u64) -> bool {
    use std::sync::atomic::Ordering::Relaxed;
    let prev = last.load(Relaxed);
    (prev == 0 || now_ms.saturating_sub(prev) >= ALLOCATOR_SAMPLE_INTERVAL_MS)
        && last.compare_exchange(prev, now_ms, Relaxed, Relaxed).is_ok()
}

pub fn sample_allocator_gauges() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    if allocator_sample_due(&ALLOCATOR_SAMPLED_AT_MS, monotonic_ms()) {
        // SAFETY: mallinfo2 takes no pointers and glibc serializes allocator access.
        let info = unsafe { libc::mallinfo2() };
        let g = gauges();
        g.allocator_in_use_bytes.set((info.uordblks + info.hblkhd) as u64);
        g.allocator_free_bytes.set(info.fordblks as u64);
    }
}

pub fn gauges_named() -> Vec<(&'static str, &'static str, u64)> {
    sample_allocator_gauges();
    let g = gauges();
    vec![
        (
            "live_keys",
            "Distinct resident keys (live puts and not-yet-GC'd tombstones, in runs and memtable) across all local zone engines.",
            g.live_keys.get(),
        ),
        (
            "used_segments",
            "Value-log segments in use across all local zones.",
            g.used_segments.get(),
        ),
        (
            "free_segments",
            "Free value-log segments across all local zone engines, including maintenance-reserved segments.",
            g.free_segments.get(),
        ),
        (
            "min_zone_free_segments",
            "Smallest free-segment count among local zone engines; approaching the maintenance floor signals a region-capacity wall.",
            g.min_zone_free_segments.get(),
        ),
        (
            "resident_runs",
            "LSM runs resident across all local zone engines; every cold LWW resolve and point read probes up to this many runs per key, so sustained growth here is the write-amplification signature of the overwrite cliff.",
            g.resident_runs.get(),
        ),
        (
            "frozen_runs",
            "LSM runs no recurring or forced merge may take as input, across local zone engines: at or above VALS_MERGE_FREEZE_BYTES (default the merge input cap) or too large for any size-ratio-balanced window under VALS_MERGE_INPUT_CAP_BYTES. Only space reclaim (pressure, shadow folds, value-log GC, tombstone GC, forget) rewrites them; grows with data / cap by design.",
            g.frozen_runs.get(),
        ),
        (
            "stranded_runs",
            "Non-frozen LSM runs older than the newest frozen run, across local zone engines; excluded from recurring and forced merges, but eligible for space reclaim.",
            g.stranded_runs.get(),
        ),
        (
            "wipe_pending",
            "Local shards waiting for in-flight maintenance to drain before a liveness wipe; new maintenance planning is paused.",
            g.wipe_pending.get(),
        ),
        (
            "max_run_bytes",
            "Largest resident LSM run (index bytes) on any local zone engine.",
            g.max_run_bytes.get(),
        ),
        (
            "shadowed_value_bytes_est",
            "Sampled estimate of value-log bytes still counted live by their owning runs but shadowed by newer runs or the memtable (cross-run LWW garbage invisible to per-run accounting), summed across local zone engines.",
            g.shadowed_value_bytes_est.get(),
        ),
        (
            "shadow_folds",
            "Capped shadow-reclaim folds planned across local zone engines (one window of at most the merge input cap, newer runs streamed read-only); monotonic engine-sourced count exported as a gauge.",
            g.shadow_folds.get(),
        ),
        (
            "shadow_fold_disarms",
            "Shadow folds whose realized reclaim fell below half the sampled prediction, triggering trigger backoff; monotonic engine-sourced count exported as a gauge.",
            g.shadow_fold_disarms.get(),
        ),
        ("key_filter_probes", "Write-path version probes that consulted the engine-wide key filter, summed across local zone engines; monotonic engine-sourced count exported as a gauge.", g.key_filter_probes.get()),
        ("key_filter_negatives", "Write-path version probes the engine-wide key filter answered negative, so covered runs were skipped; monotonic engine-sourced count exported as a gauge.", g.key_filter_negatives.get()),
        ("key_filter_run_probes", "Per-run filter probes made by write-path version probes (uncovered runs, plus every in-range run on a key-filter hit); monotonic engine-sourced count exported as a gauge.", g.key_filter_run_probes.get()),
        ("key_filter_sub_filters", "Sub-filters in the scalable engine-wide key filters across local zone engines.", g.key_filter_sub_filters.get()),
        ("key_filter_max_sub_filters", "Maximum active key-filter chain length over local zone engines, excluding shadow and retired generations.", g.key_filter_max_sub_filters.get()),
        ("key_filter_max_est_fpr_ppm", "Maximum estimated active-generation key-filter false-positive rate over local zone engines, in parts per million rounded up.", g.key_filter_max_est_fpr_ppm.get()),
        ("key_filter_max_uncovered_runs", "Maximum uncovered resident runs in any local zone engine; uncovered runs are probed individually.", g.key_filter_max_uncovered_runs.get()),
        ("key_filter_rebuild_wait_seconds_max", "Longest current rebuild-permit wait by an eligible local zone engine, in whole seconds.", g.key_filter_rebuild_wait_seconds_max.get()),
        ("key_filter_rebuild_wait_seconds_total", "Cumulative completed and ongoing rebuild-permit wait across the process, in whole seconds; monotonic including engine drops, reset on process restart.", g.key_filter_rebuild_wait_seconds_total.get()),
        ("key_filter_rebuild_urgent_grants", "Process-wide monotonic count of rebuild permits granted beyond the normal concurrency cap; reset on process restart.", g.key_filter_rebuild_urgent_grants.get()),
        ("key_filter_rebuild_open_seconds_max", "Age of the oldest open shadow key-filter rebuild over local zone engines, in whole seconds.", g.key_filter_rebuild_open_seconds_max.get()),
        ("key_filter_bytes", "Allocated capacity bytes of the engine-wide key filters across local zone engines, including shadow rebuild generations and retired sub-filters held by in-flight sinks.", g.key_filter_bytes.get()),
        ("key_filter_resident_bytes", "Estimated touched (resident) bytes of the engine-wide key filters across local zone engines; charged to RAM with run metadata.", g.key_filter_resident_bytes.get()),
        ("key_filter_retired_bytes", "Allocated capacity bytes of retired key-filter sub-filters held by in-flight sinks across local zone engines; a subset of key_filter_bytes.", g.key_filter_retired_bytes.get()),
        ("key_filter_retired_resident_bytes", "Estimated touched bytes of retired key-filter sub-filters held by in-flight sinks across local zone engines; a subset of key_filter_resident_bytes.", g.key_filter_retired_resident_bytes.get()),
        ("key_filter_inserted_keys", "Keys that set at least one new bit in active key-filter generations across local zone engines.", g.key_filter_inserted_keys.get()),
        ("key_filter_capacity_keys", "Key capacity of active key-filter generations across local zone engines; excludes rebuild and retired generations.", g.key_filter_capacity_keys.get()),
        ("key_filter_rebuild_bytes", "Allocated bytes of shadow rebuild generations; a subset of key_filter_bytes.", g.key_filter_rebuild_bytes.get()),
        ("key_filter_rebuild_resident_bytes", "Estimated touched bytes of shadow rebuild generations; a subset of key_filter_resident_bytes.", g.key_filter_rebuild_resident_bytes.get()),
        ("key_filter_rebuilding", "Local zone engines with a shadow key-filter rebuild in flight.", g.key_filter_rebuilding.get()),
        ("key_filter_rebuild_covered_runs", "Runs covered by current shadow key-filter generations across local zone engines.", g.key_filter_rebuild_covered_runs.get()),
        ("key_filter_rebuilds_started", "Shadow rebuilds started; monotonic engine-sourced count exported as a gauge.", g.key_filter_rebuilds_started.get()),
        ("key_filter_rebuilds_completed", "Shadow rebuilds swapped into service; monotonic engine-sourced count exported as a gauge.", g.key_filter_rebuilds_completed.get()),
        ("key_filter_rebuilds_abandoned", "Shadow rebuilds abandoned by resets; monotonic engine-sourced count exported as a gauge.", g.key_filter_rebuilds_abandoned.get()),
        ("key_filter_rebuild_peak_bytes", "Sum of per-engine allocated key-filter high waters observed during rebuilds, including active, shadow and sink-held retired generations; not a simultaneous node peak.", g.key_filter_rebuild_peak_bytes.get()),
        ("key_filter_uncovered_runs", "Resident runs not yet covered by their engine-wide key filter (always probed individually) across local zone engines.", g.key_filter_uncovered_runs.get()),
        ("key_filter_uncovered_run_bytes", "Bytes of resident runs not yet covered by their engine-wide key filter across local zone engines.", g.key_filter_uncovered_run_bytes.get()),
        ("key_filter_est_fpr_ppm_sum", "Sum over local zone engines of the estimated engine-wide key-filter false-positive rate in parts per million; divide by zone_engines for the mean.", g.key_filter_est_fpr_ppm_sum.get()),
        ("key_filter_resets", "Engine-wide key-filter generations discarded because stale keys dominated the filter; monotonic engine-sourced count exported as a gauge.", g.key_filter_resets.get()),
        ("key_filter_cover_scan_failures", "Key-filter cover scans that failed (read/corruption error or key-count mismatch) and put their run into retry backoff; monotonic engine-sourced count exported as a gauge.", g.key_filter_cover_scan_failures.get()),
        ("key_filter_cover_scan_mismatches", "Key-filter cover scans discarded because the streamed key count differed from the run's entry count; monotonic engine-sourced count exported as a gauge.", g.key_filter_cover_scan_mismatches.get()),
        ("key_filter_cover_scan_backoff_runs", "Uncovered runs whose key-filter cover scan failed and is waiting out its retry backoff, across local zone engines.", g.key_filter_cover_scan_backoff_runs.get()),
        (
            "process_resident_bytes",
            "Resident set size of this process in bytes, sampled independently of the anti-entropy sweep cadence by RAM admission checks.",
            g.process_resident_bytes.get(),
        ),
        (
            "process_swap_bytes",
            "Anonymous process memory currently swapped out in bytes, sampled with RSS by RAM admission checks.",
            g.process_swap_bytes.get(),
        ),
        (
            "ram_budget_bytes",
            "Configured per-process RSS-plus-swap budget in bytes; 0 means RAM admission control is disabled.",
            g.ram_budget_bytes.get(),
        ),
        (
            "ram_headroom_bytes",
            "Bytes remaining between current process RSS plus swap and the configured RAM budget; 0 when at/over budget or disabled.",
            g.ram_headroom_bytes.get(),
        ),
        ("ram_charged_bytes", "Serving memtable, run metadata and in-flight AE digest page estimates plus configured DMA/scratch bytes charged to RAM reservations.", g.ram_charged_bytes.get()),
        ("ram_reserved_ae_seed_bytes", "Reserved anti-entropy seed bytes.", g.ram_reserved_ae_seed_bytes.get()),
        ("ram_reserved_flush_bytes", "Reserved off-core flush working-set bytes.", g.ram_reserved_flush_bytes.get()),
        ("ram_reserved_merge_bytes", "Reserved off-core merge working-set bytes.", g.ram_reserved_merge_bytes.get()),
        ("ram_reserve_capacity_bytes", "Maintenance pool capacity after serving charges.", g.ram_reserve_capacity_bytes.get()),
        ("ram_reserve_debt_bytes", "Liveness-critical reservations beyond available capacity.", g.ram_reserve_debt_bytes.get()),
        ("ram_reserve_oldest_wait_ms", "Age of the oldest unsatisfied RAM reservation attempt at last broker update.", g.ram_reserve_oldest_wait_ms.get()),
        (
            "merge_in_flight",
            "Off-core run-merge jobs currently executing.",
            g.merge_in_flight.get(),
        ),
        (
            "merge_in_flight_input_bytes",
            "Sum of index-segment input bytes owned by currently executing off-core run merges.",
            g.merge_in_flight_input_bytes.get(),
        ),
        (
            "ae_seed_staged_bytes",
            "Estimated resident bytes retained by in-flight cooperative Merkle-leaf seed accumulators.",
            g.ae_seed_staged_bytes.get(),
        ),
        (
            "ae_seed_active",
            "Cooperative Merkle-leaf seeds holding a node-wide permit (shard-driver seeds under ae_seed_isolation=1 are counted but not capped by ae_seed_concurrency).",
            g.ae_seed_active.get(),
        ),
        (
            "ae_seed_journal_bytes",
            "Estimated bytes held by all in-flight seed write journals on the node, including reserved capacity (charged to the RAM broker and capped node-wide by ae_seed_journal_max_bytes when ae_seed_isolation=1).",
            g.ae_seed_journal_bytes.get(),
        ),
        (
            "ae_seed_pinned_bytes",
            "Run segment bytes merged or forgotten away but still allocated because an in-flight seed pins them as its input snapshot (node-wide; capped by ae_seed_pin_max_bytes, released when the seed publishes or resets).",
            g.ae_seed_pinned_bytes.get(),
        ),
        (
            "ae_seed_drivers",
            "Running per-shard seed driver tasks; equals the shard count on a serving node.",
            g.ae_seed_drivers.get(),
        ),
        (
            "ae_digest_serve_bytes",
            "Estimated resident bytes held by in-flight anti-entropy bucket-digest page serving.",
            g.ae_digest_serve_bytes.get(),
        ),
        (
            "ae_dirty_buckets",
            "Logical-winner Merkle buckets whose cached digest is unavailable until an exact run version-stream rebuild completes.",
            g.ae_dirty_buckets.get(),
        ),
        (
            "ae_dirty_oldest_ms",
            "Scrape-time age in milliseconds of the oldest dirty logical-winner Merkle bucket across local stores.",
            g.ae_dirty_oldest_ms.get(),
        ),
        (
            "memtable_bytes_total",
            "Aggregate memtable bytes reported by all local zone engines.",
            g.memtable_bytes_total.get(),
        ),
        (
            "memtable_allocated_bytes_total",
            "Estimated allocated mutable index bytes including retained map capacity, Arc keys and tree nodes; overlaps memtable_bytes_total and does not alter flush thresholds.",
            g.memtable_allocated_bytes_total.get(),
        ),
        (
            "flush_protection_bytes",
            "Estimated allocated capacity of flush-protected value segment sets across local engines.",
            g.flush_protection_bytes.get(),
        ),
        (
            "allocator_in_use_bytes",
            "Process-wide glibc arena in-use plus mmap allocation bytes sampled when metrics are rendered, at most every 4 s and never on a shard thread; includes serving and worker heap, not additive to resident gauges.",
            g.allocator_in_use_bytes.get(),
        ),
        (
            "allocator_free_bytes",
            "Process-wide free bytes in glibc arenas sampled when metrics are rendered, at most every 4 s and never on a shard thread; includes nonresident pages and is not RSS.",
            g.allocator_free_bytes.get(),
        ),
        (
            "allocator_trim_max_ns",
            "Maximum wall nanoseconds inside background malloc_trim since node startup, including allocator lock waits; not a shard stall measurement.",
            g.allocator_trim_max_ns.get(),
        ),
        (
            "merge_worker_metadata_bytes",
            "Process-wide allocated reopened-input and builder/output bloom and fence capacity in off-core merges, through publication or drop; excludes scan buffers.",
            vals_store::attribution::merge_metadata_bytes(),
        ),
        (
            "run_metadata_resident_bytes",
            "Estimated resident run serving metadata across local engines: filters, fences, indices, run parts and manifest copies.",
            g.run_metadata_resident_bytes.get(),
        ),
        (
            "run_fence_resident_bytes",
            "Resident block-index (fence) bytes of open runs across local zone engines: one first-key copy plus offset and length per run block; a subset of run_metadata_resident_bytes.",
            g.run_fence_resident_bytes.get(),
        ),
        (
            "run_filter_resident_bytes",
            "Allocated vector capacity bytes of per-run bloom filters of open runs across local zone engines, excluding the engine-wide key filter (key_filter_resident_bytes); a subset of run_metadata_resident_bytes.",
            g.run_filter_resident_bytes.get(),
        ),
        (
            "run_filter_payload_bytes",
            "Per-run bloom word payload bytes of open runs across local zone engines, excluding vector capacity slack and encoded headers/trailers; a subset of run_filter_resident_bytes.",
            g.run_filter_payload_bytes.get(),
        ),
        (
            "run_fence_payload_bytes",
            "Block-index tuple and first-key payload bytes of open runs across local zone engines, excluding vector/key capacity slack; a subset of run_fence_resident_bytes, not encoded disk bytes.",
            g.run_fence_payload_bytes.get(),
        ),
        (
            "run_entries",
            "Entries stored in open runs across local zone engines, counting every run's copy of a key (the denominator for per-entry fence and filter bytes).",
            g.run_entries.get(),
        ),
        (
            "winner_directory_keys",
            "Keys held in the optional collision-safe RAM winner directories across local zone engines.",
            g.winner_directory_keys.get(),
        ),
        (
            "winner_directory_bytes",
            "Allocated dense-key arena, packed winner-slot, and lookup-table capacity bytes held by optional RAM winner directories across local zone engines.",
            g.winner_directory_bytes.get(),
        ),
        (
            "flush_build_bytes",
            "Estimated memtable-snapshot bytes held by in-flight off-core flush builds across local zone engines.",
            g.flush_build_bytes.get(),
        ),
        (
            "zone_engines",
            "Number of live local zone engines sharing the per-node memtable budget.",
            g.zone_engines.get(),
        ),
        (
            "zone_drop_pending_age_seconds",
            "Age in seconds of the oldest local zone storage teardown still waiting for engine references to drain; zero when none are pending.",
            g.zone_drop_pending_age_seconds.get(),
        ),
        (
            "sweep_pass_age_seconds",
            "Seconds since the slowest local shard last started an anti-entropy sweep pass; growth well beyond the sweep interval means a frozen sweep.",
            g.sweep_pass_age_seconds.get(),
        ),
        (
            "pd_state_capacity_bytes",
            "(PD) Configured live materialized-state capacity in bytes.",
            g.pd_state_capacity_bytes.get(),
        ),
        (
            "pd_state_used_bytes",
            "(PD) Accounted bytes used by live materialized-state keys, values, and per-entry overhead.",
            g.pd_state_used_bytes.get(),
        ),
        (
            "pd_state_reserved_bytes",
            "(PD) Worst-case live-state growth reserved for locally queued Raft proposals.",
            g.pd_state_reserved_bytes.get(),
        ),
        (
            "pd_state_headroom_bytes",
            "(PD) Unused materialized-state bytes after queued-proposal reservations.",
            g.pd_state_headroom_bytes.get(),
        ),
        (
            "pd_sorted_ranges",
            "(PD) Total committed ranges summed over all sorted zones.",
            g.pd_sorted_ranges.get(),
        ),
        (
            "pd_single_range_sorted_zones",
            "(PD) Sorted zones still on exactly one range (one replica set/partition).",
            g.pd_single_range_sorted_zones.get(),
        ),
        (
            "pd_max_range_entries",
            "(PD) Hottest single range's approximate live-entry estimate across sorted zones.",
            g.pd_max_range_entries.get(),
        ),
        (
            "pd_oversized_single_range_zones",
            "(PD) Single-range sorted zones over the warn threshold (the footgun alert; >0 means split them).",
            g.pd_oversized_single_range_zones.get(),
        ),
        (
            "pd_concentrated_range_zones",
            "(PD) Multi-range sorted zones whose hottest range exceeds the entry floor and holds at least VALS_PD_RANGE_CONCENTRATION_WARN_PCT percent of zone entries.",
            g.pd_concentrated_range_zones.get(),
        ),
        (
            "pd_range_skew_pct",
            "(PD) Worst multi-range sorted zone's max_range/mean_range fill ratio x100 (100=even; 0=no multi-range zone).",
            g.pd_range_skew_pct.get(),
        ),
        (
            "pd_node_load_skew_pct",
            "(PD) Hottest current member's owned-entry total divided by the mean across all current members, x100 (100=even; 0=no members or no reported load). Includes every replica of sorted ranges and local totals for hash zones.",
            g.pd_node_load_skew_pct.get(),
        ),
        (
            "pd_engine_load_skew_pct",
            "(PD) Hottest current routed engine's capacity-normalized owned-entry load divided by its target, x100 (100=even; 0=no reported load). Counts only current owners' ready logical counts, as debt re-home admission does; holds its last value on polls missing such a count for some sorted range.",
            g.pd_engine_load_skew_pct.get(),
        ),
        (
            "pd_unrehomed_ranges",
            "(PD) Sorted ranges whose placement token is inherited rather than their lower bound.",
            g.pd_unrehomed_ranges.get(),
        ),
        (
            "pd_rehomes_active",
            "(PD) Sorted ranges currently prepared for confirmed re-home.",
            g.pd_rehomes_active.get(),
        ),
        (
            "pd_rehomes_delivery_stalled",
            "(PD) Active re-homes whose pending range map is not yet confirmed on every required owner; report and copy deadlines are paused.",
            g.pd_rehomes_delivery_stalled.get(),
        ),
        (
            "pd_recovery_down_nodes",
            "(PD) Data nodes with at least one shard endpoint that did not answer the last range-stats poll.",
            g.pd_recovery_down_nodes.get(),
        ),
        (
            "pd_recovery_seeding_nodes",
            "(PD) Data nodes reporting at least one engine reopened at process start whose anti-entropy leaf seed has not finished, in the last range-stats poll.",
            g.pd_recovery_seeding_nodes.get(),
        ),
        (
            "pd_recovery_restarting_nodes",
            "(PD) Data nodes a beacon saw restart (ring- or catalog-epoch regression) that the last range-stats poll counted as recovering although neither down nor recovery-seeding: no recovery seed seen yet and the restart grace not yet over.",
            g.pd_recovery_restarting_nodes.get(),
        ),
        (
            "pd_rehome_recovery_hold",
            "(PD) 1 while new re-home starts and balance proposals are held because a node is down, recovery-seeding or newly restarted (cumulative per episode, bounded by rehome_recovery_defer_ms), else 0.",
            g.pd_rehome_recovery_hold.get(),
        ),
        (
            "pd_balance_before_node_pct",
            "(PD) Dry-run projected hottest capacity-normalized node load/target x100 before a proposal; 100 is target and 0 means no load/input.",
            g.pd_balance_before_node_pct.get(),
        ),
        (
            "pd_balance_after_node_pct",
            "(PD) Dry-run projected hottest capacity-normalized node load/target x100 after the proposal; reset to 0 when no proposal (or retained for a below-improvement candidate).",
            g.pd_balance_after_node_pct.get(),
        ),
        (
            "pd_balance_before_engine_pct",
            "(PD) Dry-run projected hottest routed engine load/its equal share of node target x100 before a proposal; 100 is target.",
            g.pd_balance_before_engine_pct.get(),
        ),
        (
            "pd_balance_after_engine_pct",
            "(PD) Dry-run projected hottest routed engine load/target x100 after the proposal; 0 when no realizable candidate was evaluated.",
            g.pd_balance_after_engine_pct.get(),
        ),
        (
            "pd_balance_proposed_entries",
            "(PD) Estimated indivisible range entries in the current dry-run proposal; reset to 0 without a proposal.",
            g.pd_balance_proposed_entries.get(),
        ),
        (
            "pd_balance_proposed_copy_entries",
            "(PD) Estimated entry copies for changed target node/shard pairs in the current dry-run proposal; reset to 0 without a proposal.",
            g.pd_balance_proposed_copy_entries.get(),
        ),
        (
            "pd_balance_movable_ranges",
            "(PD) Sorted ranges with fresh all-owner estimates, exact RF/scope, opaque-map support, and complete shard topology in the current dry-run poll.",
            g.pd_balance_movable_ranges.get(),
        ),
        (
            "pd_balance_blocked_ranges",
            "(PD) Sorted ranges excluded from the current dry-run poll by missing/stale/inconsistent inputs, unsupported maps, active moves, or capability gating.",
            g.pd_balance_blocked_ranges.get(),
        ),
        (
            "pd_balance_proposal",
            "(PD) 1 when the current poll has a read-only balance proposal, otherwise 0; no range-map mutation is implied.",
            g.pd_balance_proposal.get(),
        ),
        (
            "pd_balance_token_exhaustions",
            "(PD) RF/scope-exact target node sets not found within the global per-range deterministic opaque-token bounds in this poll; resets each poll.",
            g.pd_balance_token_exhaustions.get(),
        ),
    ]
}

pub struct FlushWriteDepth {
    count: Counter,
    sum: Counter,
}

static FLUSH_WRITE_DEPTH: FlushWriteDepth = FlushWriteDepth {
    count: Counter::new(),
    sum: Counter::new(),
};

pub fn domain_flush_vlog_writes_inflight() -> &'static FlushWriteDepth {
    &FLUSH_WRITE_DEPTH
}

impl FlushWriteDepth {
    pub fn observe(&self, writes: u64) {
        self.sum.add(writes);
        self.count.inc();
    }

    pub fn snapshot(&self) -> (u64, u64) {
        (self.count.get(), self.sum.get())
    }
}

pub const DOMAIN_FLUSH_VLOG_WRITES_INFLIGHT_HELP: &str = "Vlog writes not yet completed on the submitting shard's serving device(s) in this domain at Flush submit entry; all local zones are sampled without locks, not other shards or maintenance qpairs. Summary count is flushes and sum is writes; no quantiles.";

pub struct Histograms {
    pub batch_apply_latency_us: Histogram,
    pub batch_apply_response_ready_latency_us: Histogram,
    pub apply_latency_us: Histogram,
    pub scan_latency_us: Histogram,
    pub sync_flush_us: Histogram,
    pub domain_flush_latency_us: Histogram,
    pub domain_flush_device_us: Histogram,
    pub domain_flush_publish_us: Histogram,
    pub flush_plan_us: Histogram,
    pub flush_plan_sync_us: Histogram,
    pub flush_plan_snapshot_us: Histogram,
    pub flush_commit_us: Histogram,
    pub compact_commit_us: Histogram,
    pub ae_dirty_rebuild_us: Histogram,
    pub logical_range_split_recount_turn_us: Histogram,
    pub ae_seed_duration_ms: Histogram,
    pub ae_digest_serve_duration_ms: Histogram,
}

impl Histograms {
    const fn new() -> Self {
        Histograms {
            batch_apply_latency_us: Histogram::new(),
            batch_apply_response_ready_latency_us: Histogram::new(),
            apply_latency_us: Histogram::new(),
            scan_latency_us: Histogram::new(),
            sync_flush_us: Histogram::new(),
            domain_flush_latency_us: Histogram::new(),
            domain_flush_device_us: Histogram::new(),
            domain_flush_publish_us: Histogram::new(),
            flush_plan_us: Histogram::new(),
            flush_plan_sync_us: Histogram::new(),
            flush_plan_snapshot_us: Histogram::new(),
            flush_commit_us: Histogram::new(),
            compact_commit_us: Histogram::new(),
            ae_dirty_rebuild_us: Histogram::new(),
            logical_range_split_recount_turn_us: Histogram::new(),
            ae_seed_duration_ms: Histogram::new(),
            ae_digest_serve_duration_ms: Histogram::new(),
        }
    }
}

static HISTOGRAMS: Histograms = Histograms::new();

pub fn histograms() -> &'static Histograms {
    &HISTOGRAMS
}

pub struct HistogramSnapshot {
    pub name: &'static str,
    pub help: &'static str,
    pub bounds: &'static [u64],
    pub buckets: [u64; LATENCY_BUCKETS_US.len()],
    pub count: u64,
    pub sum_us: u64,
    pub sum_units_per_second: f64,
}

pub fn histograms_named() -> Vec<HistogramSnapshot> {
    let h = histograms();
    let mk = |name, help, hist: &Histogram| {
        let (buckets, count, sum_us) = hist.snapshot();
        HistogramSnapshot {
            name,
            help,
            bounds: &LATENCY_BUCKETS_US,
            buckets,
            count,
            sum_us,
            sum_units_per_second: 1_000_000.0,
        }
    };
    vec![
        mk(
            "batch_apply_latency_us",
            "Local BatchApply execution through its optional persistence barrier; excludes rehome mirroring and response enqueue readiness, microseconds.",
            &h.batch_apply_latency_us,
        ),
        mk(
            "batch_apply_response_ready_latency_us",
            "Latency from BatchApply handler dispatch through local apply and awaited rehome mirroring until response enqueue readiness; excludes pre-dispatch rejections and response transmission, microseconds.",
            &h.batch_apply_response_ready_latency_us,
        ),
        mk(
            "apply_latency_us",
            "Latency of serving one single-write Apply (non-batched write path), microseconds.",
            &h.apply_latency_us,
        ),
        mk(
            "scan_latency_us",
            "Latency of serving one Scan page (index range walk + page device reads), microseconds.",
            &h.scan_latency_us,
        ),
        mk(
            "sync_flush_us",
            "Duration of a device-persistence group-commit barrier (store.sync including NVMe FLUSH), microseconds.",
            &h.sync_flush_us,
        ),
        mk(
            "domain_flush_latency_us",
            "Device domain NVMe flush submit-to-completion latency, microseconds.",
            &h.domain_flush_latency_us,
        ),
        mk(
            "domain_flush_device_us",
            "Domain Flush submit entry to device completion callback (including shim submission retries and callback polling delay), microsecond buckets; exported sum is seconds. Callback-bearing completions only, including errors.",
            &h.domain_flush_device_us,
        ),
        mk(
            "domain_flush_publish_us",
            "Domain Flush completion callback to publication of completed/failed, microsecond buckets; exported sum is seconds. Callback-bearing completions only, including errors.",
            &h.domain_flush_publish_us,
        ),
        mk(
            "flush_plan_us",
            "Duration of one bounded Engine::flush_plan_step serving-core slice, microseconds.",
            &h.flush_plan_us,
        ),
        mk(
            "flush_plan_sync_us",
            "The log drain-and-flush-tail durability portion of flush_plan, microseconds.",
            &h.flush_plan_sync_us,
        ),
        mk(
            "flush_plan_snapshot_us",
            "The bounded memtable snapshot portion of flush_plan, microseconds.",
            &h.flush_plan_snapshot_us,
        ),
        mk(
            "flush_commit_us",
            "Duration of one Engine flush-commit serving-core slice (manifest/install or bounded key-drop page), microseconds.",
            &h.flush_commit_us,
        ),
        mk(
            "compact_commit_us",
            "Duration of Engine::compact_commit on the serving core (reconcile merged run, free inputs), microseconds.",
            &h.compact_commit_us,
        ),
        mk(
            "ae_dirty_rebuild_us",
            "Wall time of an exact dirty logical-winner Merkle bucket rebuild request, microseconds.",
            &h.ae_dirty_rebuild_us,
        ),
        mk(
            "logical_range_split_recount_turn_us",
            "Serving-core duration of one split recount turn (one bounded winner-count page, capped at 1024 winners and a 1 ms deadline, plus finalize; prefetched pages count plan and merge time, not the awaited run-block reads), microseconds.",
            &h.logical_range_split_recount_turn_us,
        ),
        HistogramSnapshot {
            sum_units_per_second: 1_000.0,
            ..mk(
                "ae_seed_duration_ms",
                "Wall time from a cooperative Merkle-leaf seed's first step to its publication, including restarts and pauses; bucket bounds are milliseconds (10 ms to 1000 s) and the exported sum is seconds.",
                &h.ae_seed_duration_ms,
            )
        },
        HistogramSnapshot {
            sum_units_per_second: 1_000.0,
            ..mk(
                "ae_digest_serve_duration_ms",
                "Wall time of serving one AE bucket-digest request (read-gate wait, planning, run scan and restarts) for every outcome; bucket bounds are milliseconds (10 ms to 1000 s) and the exported sum is seconds.",
                &h.ae_digest_serve_duration_ms,
            )
        },
    ]
}

pub fn process_resident_bytes() -> Option<u64> {
    let statm = std::fs::read_to_string("/proc/self/statm").ok()?;
    let resident_pages: u64 = statm.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf is a pure lookup with no preconditions.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    (page_size > 0).then(|| resident_pages * page_size as u64)
}

fn parse_process_swap_bytes(status: &str) -> Option<u64> {
    let line = status.lines().find(|line| line.starts_with("VmSwap:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

pub fn process_swap_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    Some(parse_process_swap_bytes(&status).unwrap_or(0))
}

pub fn spawn_abort_on_panic<F>(name: &str, f: F) -> std::thread::JoinHandle<()>
where
    F: FnOnce() + Send + 'static,
{
    std::thread::Builder::new()
        .name(name.to_string())
        .spawn(move || {
            if let Err(_payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
                std::process::abort();
            }
        })
        .expect("spawning a named thread cannot fail")
}

struct StderrLogger;

impl log::Log for StderrLogger {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        metadata.level() <= log::max_level()
    }

    fn log(&self, record: &log::Record) {
        if !self.enabled(record.metadata()) {
            return;
        }
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let _ = writeln!(
            std::io::stderr().lock(),
            "[{} {:5} {}] {}",
            fmt_utc_ms(ms),
            record.level(),
            record.target(),
            record.args()
        );
    }

    fn flush(&self) {}
}

fn fmt_utc_ms(ms: u128) -> String {
    let secs = (ms / 1000) as i64;
    let ms_part = (ms % 1000) as u32;
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms_part:03}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

pub fn init_stderr_logger() {
    let level = match std::env::var("VALS_LOG").as_deref() {
        Ok("error") => log::LevelFilter::Error,
        Ok("warn") => log::LevelFilter::Warn,
        Ok("debug") => log::LevelFilter::Debug,
        Ok("trace") => log::LevelFilter::Trace,
        Ok("off") => log::LevelFilter::Off,
        _ => log::LevelFilter::Info,
    };
    init_stderr_logger_at(level);
}

pub fn init_stderr_logger_at(level: log::LevelFilter) {
    static LOGGER: StderrLogger = StderrLogger;
    if log::set_logger(&LOGGER).is_ok() {
        log::set_max_level(level);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocator_sample_runs_once_per_interval_across_callers() {
        let last = std::sync::atomic::AtomicU64::new(0);
        assert!(allocator_sample_due(&last, 1));
        assert!(!allocator_sample_due(&last, 1));
        assert!(!allocator_sample_due(&last, ALLOCATOR_SAMPLE_INTERVAL_MS));
        assert!(allocator_sample_due(&last, ALLOCATOR_SAMPLE_INTERVAL_MS + 1));
        let next = 3 * ALLOCATOR_SAMPLE_INTERVAL_MS;
        let granted = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8).map(|_| s.spawn(|| allocator_sample_due(&last, next))).collect();
            handles.into_iter().filter_map(|h| h.join().ok()).filter(|&due| due).count()
        });
        assert_eq!(granted, 1);
    }

    #[test]
    fn snapshot_reflects_increments() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        let before = snapshot().wipes;
        counters().wipes.inc();
        counters().wipes.add(2);
        assert_eq!(snapshot().wipes, before + 3);
    }

    #[test]
    fn point_value_device_counters_export_prometheus_help() {
        let named = snapshot_named();
        let reads = named
            .iter()
            .find(|(name, _, _)| *name == "point_value_device_reads")
            .unwrap();
        assert!(reads.1.contains("Value-log device read operations submitted"));
        let blocks = named
            .iter()
            .find(|(name, _, _)| *name == "point_value_device_blocks")
            .unwrap();
        assert!(blocks.1.contains("Device blocks requested"));
    }

    #[test]
    fn split_recount_metrics_export_names_and_help() {
        let named = snapshot_named();
        let expected = [
            ("logical_range_split_recounts", "pending range-bounded recount span"),
            ("logical_range_split_empty", "no recount"),
            ("logical_range_split_recount_pages", "turn deadline"),
            ("logical_range_split_recount_keys", "including tombstones"),
            ("logical_range_split_recount_done", "derived child counts"),
            ("logical_range_split_recount_repartitions", "without a full reseed"),
            ("logical_range_split_recount_fallbacks", "full count-and-leaf invalidation"),
            ("logical_range_split_recount_mismatches", "Must stay 0"),
            ("logical_range_split_recount_ram_pauses", "RAM budget"),
            ("logical_range_split_recount_prefetch_pages", "prefetch horizon"),
            ("logical_range_split_recount_prefetch_blocks", "discarded prefetches"),
            ("logical_range_split_recount_prefetch_discards", "storage generation"),
            ("logical_range_split_recount_sync_fallbacks", "consecutive prefetch discards"),
            ("logical_range_merge_sums", "O(1)"),
        ];
        for (name, help) in expected {
            let matches: Vec<_> = named.iter().filter(|(n, _, _)| *n == name).collect();
            assert_eq!(matches.len(), 1, "{name}");
            assert!(matches[0].1.contains(help), "{name}");
        }
        let hist = histograms_named();
        let turn: Vec<_> = hist
            .iter()
            .filter(|h| h.name == "logical_range_split_recount_turn_us")
            .collect();
        assert_eq!(turn.len(), 1);
        assert!(turn[0].help.contains("bounded winner-count page"));
    }

    #[test]
    fn key_filter_urgent_grants_exported() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        gauges().key_filter_rebuild_urgent_grants.set(7);
        let named = gauges_named();
        let values: Vec<_> = named.iter().filter(|(name, _, _)| *name == "key_filter_rebuild_urgent_grants").collect();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].2, 7);
        assert!(!values[0].1.is_empty());
    }

    #[test]
    fn parses_process_swap_from_status() {
        assert_eq!(
            parse_process_swap_bytes("Name:\tvalsd\nVmSwap:\t  1234 kB\n"),
            Some(1_263_616)
        );
        assert_eq!(parse_process_swap_bytes("Name:\tvalsd\nVmRSS:\t 4 kB\n"), None);
        assert_eq!(parse_process_swap_bytes("VmSwap:\tinvalid kB\n"), None);
    }

    #[test]
    fn utc_formatting_is_civil() {
        assert_eq!(fmt_utc_ms(1_780_963_200_000), "2026-06-09T00:00:00.000Z");
        assert_eq!(fmt_utc_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(fmt_utc_ms(1_709_251_199_999), "2024-02-29T23:59:59.999Z");
    }
}
