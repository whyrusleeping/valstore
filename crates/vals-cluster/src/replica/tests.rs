    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
    use vals_spdk::{BlockError, FileBlockDevice, MemBlockDevice};
    use vals_store::{Config, Engine, Ordering};

    const BS: usize = 256;

    #[test]
    fn profile_degradation_permanently_falls_back_to_resolving_applies() {
        const CHILD: &str = "VALS_PROFILE_DEGRADE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "replica::tests::profile_degradation_permanently_falls_back_to_resolving_applies",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success(), "isolated degradation test failed: {status}");
            return;
        }

        configure_blind_append(true).unwrap();
        vals_store::configure_winner_directory(true).unwrap();
        assert_eq!(unconditional_apply_mode(), ApplyMode::Blind);
        vals_store::degrade_winner_directory();
        assert_eq!(unconditional_apply_mode(), ApplyMode::Legacy);

        let mut engine = Engine::create_prewiped(MemBlockDevice::new(BS, 256), Config::default());
        engine.put(b"k", b"v", 1).unwrap();
        engine.rebuild_winner_directory().unwrap();
        assert!(engine.winner_directory_enabled());
        assert!(!vals_store::winner_directory_operational());
        assert_eq!(unconditional_apply_mode(), ApplyMode::Legacy);
    }

    #[test]
    fn split_stored_blob_strips_kind_and_flags_tombstones() {
        assert_eq!(
            split_stored_blob(&encode(KIND_PUT, b"hello")),
            (true, &b"hello"[..])
        );
        assert_eq!(
            split_stored_blob(&encode(KIND_TOMB, b"")),
            (false, &b""[..])
        );
        assert_eq!(split_stored_blob(&[]), (false, &b""[..]));
    }

    struct ReadFailDevice {
        inner: MemBlockDevice,
        armed: AtomicBool,
    }
    impl BlockDevice for ReadFailDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.armed.load(AtomicOrdering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected run-read failure",
                )));
            }
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }

    fn temp_path(tag: &str) -> std::path::PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, AtomicOrdering::Relaxed);
        std::env::temp_dir().join(format!("vals-replica-{}-{}-{}", std::process::id(), tag, n))
    }

    fn store(node: u32) -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    }

    fn sorted_store(node: u32) -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    }

    fn v(hlc: u64, node: u32) -> Version {
        Version { hlc, node: node.into() }
    }

    fn rebuild_all<D: BlockDevice + AsyncRead>(store: &std::cell::RefCell<VersionedStore<D>>) {
        vals_rt::block_on(
            async {
                assert!(
                    ensure_tree_ready_cooperative_cell(store, 1, &std::collections::HashSet::from([0]))
                        .await
                        .unwrap()
                );
                while store.borrow().logical_recount_step().unwrap() != RecountStep::Done {}
            },
            || {},
        );
    }

    fn rebuild_range_counts<D: BlockDevice + AsyncRead>(
        store: VersionedStore<D>,
    ) -> VersionedStore<D> {
        let cell = std::cell::RefCell::new(store);
        rebuild_all(&cell);
        cell.into_inner()
    }

    #[test]
    fn logical_range_counts_ignore_layout_history_and_follow_map_lifecycle() {
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mem_a = MemBlockDevice::new(BS, 4096);
        let mem_b = MemBlockDevice::new(BS, 4096);
        let mut a = VersionedStore::new(
            Engine::create_prewiped(mem_a.share(), cfg),
            1,
        ).unwrap();
        let mut b = VersionedStore::new(
            Engine::create_prewiped(mem_b.share(), cfg),
            2,
        ).unwrap();
        let left: Vec<Vec<u8>> = (0..80).map(|i| format!("a{i:03}").into_bytes()).collect();
        let right: Vec<Vec<u8>> = (0..80)
            .map(|i| format!("c{i:03}-{}", "x".repeat(96)).into_bytes())
            .collect();
        for key in left.iter().chain(&right) {
            a.apply_at(key, Some(b"old"), v(1, 9), 0).unwrap();
        }
        a.flush().unwrap();
        for key in left.iter().chain(&right).take(40) {
            a.apply_at(key, Some(b"final"), v(2, 9), 0).unwrap();
        }
        a.apply_at(&left[0], None, v(3, 9), 0).unwrap();
        a.flush().unwrap();
        a.compact_runs(0).unwrap();

        for (index, key) in left.iter().chain(&right).enumerate() {
            let (value, version) = if index == 0 {
                (None, v(3, 9))
            } else if index < 40 {
                (Some(&b"final"[..]), v(2, 9))
            } else {
                (Some(&b"old"[..]), v(1, 9))
            };
            b.apply_at(key, value, version, 0).unwrap();
            if index + 1 == left.len() {
                b.flush().unwrap();
            }
        }
        b.flush().unwrap();

        let map = RangeMap::from_splits([b"b".to_vec()]);
        let a_estimate = a.engine().range_estimate(None, Some(b"b")).0;
        let b_estimate = b.engine().range_estimate(None, Some(b"b")).0;
        assert_ne!(a_estimate, b_estimate, "fixture must reproduce layout-dependent estimates");
        a.set_logical_range_map(1, map.clone());
        b.set_logical_range_map(1, map.clone());
        assert_eq!(a.logical_range_counts(1), None);
        assert_eq!(b.logical_range_counts(1), None);
        a = rebuild_range_counts(a);
        b = rebuild_range_counts(b);
        assert_eq!(a.logical_range_counts(1), Some(vec![80, 80]));
        assert_eq!(a.logical_range_counts(1), b.logical_range_counts(1));

        a.sync().unwrap();
        drop(a);
        let mut reopened = VersionedStore::open(mem_a.share(), cfg, 1).unwrap();
        reopened.set_logical_range_map(1, map.clone());
        assert_eq!(reopened.logical_range_counts(1), None);
        reopened = rebuild_range_counts(reopened);
        assert_eq!(reopened.logical_range_counts(1), Some(vec![80, 80]));

        let split = RangeMap::from_splits([b"b".to_vec(), b"c".to_vec()]);
        reopened.set_logical_range_map(2, split.clone());
        assert_eq!(reopened.logical_range_counts(2), None);
        reopened = rebuild_range_counts(reopened);
        assert_eq!(reopened.logical_range_counts(2), Some(vec![80, 0, 80]));

        let mut pending = split;
        pending.enable_opaque_tokens();
        assert!(pending.set_pending(2, crate::partition::opaque_placement_token([7; 16])));
        let generation = reopened.logical_range_counts.borrow().as_ref().unwrap().generation;
        reopened.set_logical_range_map(3, pending);
        assert_eq!(reopened.logical_range_counts(2), None);
        assert_eq!(reopened.logical_range_counts(3), Some(vec![80, 0, 80]));
        assert_eq!(reopened.logical_range_counts.borrow().as_ref().unwrap().generation, generation);
        assert!(reopened.is_leaf_seeded(), "boundary-preserving map change keeps the seed");

        reopened.set_gc_grace_ms(1);
        reopened.advance_durable_gc_watermark(10);
        reopened
            .apply_at(b"z-anchor", Some(b"v"), vp(10, 1), 10)
            .unwrap();
        reopened.flush().unwrap();
        assert_eq!(
            reopened.compact_runs(10).unwrap().current_dropped_tombstones.len(),
            1,
        );
        assert_eq!(reopened.logical_range_counts(3), Some(vec![79, 0, 81]));
    }

    fn epoch_zone_config() -> crate::zone::ZoneConfig {
        crate::zone::ZoneConfig {
            name: "epoch-test".into(),
            rf: 2,
            ordering: crate::zone::Ordering::Sorted,
            scope: None,
        }
    }

    fn prepare_epoch_zone<D: BlockDevice>(
        store: &VersionedStore<D>, epoch: u64, map: &RangeMap,
    ) {
        store.prepare_zone_seed(
            (1, 1, epoch),
            &crate::ring::Ring::from_members(2, 64, [1, 2, 3]),
            42,
            Some(epoch_zone_config()),
            Some(map.clone()),
        );
    }

    #[test]
    fn epoch_only_seed_rebase_preserves_ready_counts_and_digest() {
        let mut store = sorted_store(1);
        store.apply_at(b"a", Some(b"old"), v(1, 1), 0).unwrap();
        store.apply_at(b"a", None, v(2, 1), 0).unwrap();
        store.apply_at(b"z", Some(b"value"), v(3, 1), 0).unwrap();
        let map = RangeMap::from_splits([b"m".to_vec()]);
        prepare_epoch_zone(&store, 10, &map);
        store = rebuild_range_counts(store);
        let digest = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let logical_generation = store.logical_range_counts.borrow().as_ref().unwrap().generation;
        let placement_generation = store.placement_generation.get();
        let scoped = store.scoped_leaf_digests(
            INDEX_BUCKETS, 1, &std::collections::HashSet::from([0]), Some(2),
        ).unwrap();
        assert_eq!(store.logical_range_counts(10), Some(vec![1, 1]));
        assert!(store.seed_progress.borrow().is_none());
        prepare_epoch_zone(&store, 11, &map);
        assert_eq!(store.logical_range_counts(10), None);
        assert_eq!(store.logical_range_counts(11), Some(vec![1, 1]));
        assert_eq!(store.logical_range_counts.borrow().as_ref().unwrap().generation, logical_generation);
        assert_eq!(store.placement_generation.get(), placement_generation);
        assert!(store.is_leaf_seeded());
        assert!(store.seed_step().unwrap());
        assert!(store.seed_progress.borrow().is_none(), "no new full fold");
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), digest);
        store.invalidate_leaf_seed();
        store = rebuild_range_counts(store);
        assert_eq!(store.logical_range_counts(11), Some(vec![1, 1]));
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), digest);
        assert_eq!(store.scoped_leaf_digests(
            INDEX_BUCKETS, 1, &std::collections::HashSet::from([0]), Some(2),
        ).unwrap(), scoped);

        store.append_unknown_for_test(b"new", Some(b"v"), v(4, 1)).unwrap();
        assert_eq!(store.logical_range_counts(11), None);
        prepare_epoch_zone(&store, 12, &map);
        assert_eq!(store.logical_range_counts(12), None, "rebase cannot restore readiness");
        assert!(!store.is_leaf_seeded());
        store = rebuild_range_counts(store);
        assert_eq!(store.logical_range_counts(12), Some(vec![1, 2]));
        prepare_epoch_zone(&store, 9, &map);
        assert!(!store.is_leaf_seeded());
        assert_eq!(store.logical_range_counts(9), None);
        assert_ne!(store.logical_range_counts.borrow().as_ref().unwrap().generation, logical_generation);
        assert_ne!(store.placement_generation.get(), placement_generation);
    }

    #[test]
    fn inherited_split_lifecycle_recounts_without_refold_on_two_node_rf2() {
        let ring = crate::ring::Ring::from_members(2, 64, [1, 2]);
        let cfg = epoch_zone_config();
        let prepare = |store: &VersionedStore<MemBlockDevice>, epoch: u64, map: &RangeMap| {
            store.prepare_zone_seed((1, 1, epoch), &ring, 42, Some(cfg.clone()), Some(map.clone()));
        };
        let mut store = sorted_store(1);
        for (i, key) in [b"b".as_slice(), b"h", b"n", b"t"].into_iter().enumerate() {
            store.apply_at(key, Some(b"v"), v(i as u64 + 1, 1), 0).unwrap();
        }
        let mut map = RangeMap::from_splits([b"k".to_vec()]);
        map.enable_opaque_tokens();
        prepare(&store, 10, &map);
        store = rebuild_range_counts(store);
        assert_eq!(store.logical_range_counts(10), Some(vec![2, 2]));
        let counters = crate::observe::counters();
        let before = (
            counters.ae_seed_placement_reuses.get(),
            counters.logical_range_boundary_rebases.get(),
            counters.ae_seed_placement_invalidations.get(),
            counters.logical_range_split_recounts.get(),
            counters.logical_range_split_recount_done.get(),
        );
        let seeded_digest = store.version_leaf_digests(INDEX_BUCKETS).unwrap();

        let mut split = map.clone();
        assert!(split.split(b"p"));
        assert_eq!(split.token_of(2), split.token_of(1), "inherit split copies the parent token");
        let placement_generation = store.placement_generation.get();
        prepare(&store, 11, &split);
        assert_eq!(store.logical_range_counts(11), None, "a populated split is unready until recounted");
        assert!(store.is_leaf_seeded(), "an inherited split keeps the seeded leaves");
        assert!(store.seed_progress.borrow().is_none(), "no full seed starts");
        assert!(store.logical_recount_pending());
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), seeded_digest);
        assert_eq!(store.placement_generation.get(), placement_generation,
            "identical replica sets keep the placement while counts recount");
        store = rebuild_range_counts(store);
        assert!(store.seed_progress.borrow().is_none());
        assert_eq!(store.logical_range_counts(11), Some(vec![2, 1, 1]));
        let digest = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let placement_generation = store.placement_generation.get();
        let logical_generation = store.logical_range_counts.borrow().as_ref().unwrap().generation;

        let mut pending = split.clone();
        assert!(pending.set_pending(2, crate::partition::opaque_placement_token([3; 16])));
        prepare(&store, 12, &pending);
        let mut committed = pending.clone();
        assert!(committed.commit_pending(2));
        prepare(&store, 13, &committed);
        for epoch in [12u64, 13] {
            assert_eq!(store.logical_range_counts(epoch), if epoch == 13 { Some(vec![2, 1, 1]) } else { None });
        }
        assert!(store.is_leaf_seeded(), "pending and commit epochs keep the seed");
        assert!(store.seed_progress.borrow().is_none());
        assert_eq!(store.placement_generation.get(), placement_generation);
        assert_eq!(store.logical_range_counts.borrow().as_ref().unwrap().generation, logical_generation);
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), digest);
        assert_eq!(digest, recompute_leaves(&store, INDEX_BUCKETS));
        assert_eq!(store.logical_range_counts.borrow().as_ref().unwrap().map, committed);
        let after = (
            counters.ae_seed_placement_reuses.get(),
            counters.logical_range_boundary_rebases.get(),
            counters.ae_seed_placement_invalidations.get(),
            counters.logical_range_split_recounts.get(),
            counters.logical_range_split_recount_done.get(),
        );
        assert!(after.0 - before.0 >= 2, "pending and commit epochs reuse placement (the split epoch is asserted by its retained generation above)");
        assert!(after.1 - before.1 >= 2, "pending and commit epochs rebase counts");
        assert!(after.3 - before.3 >= 1, "the populated split records one recount span");
        assert!(after.4 - before.4 >= 1, "the recount finalizes");
        let _ = after.2;

        store.apply_at(b"x", Some(b"v"), v(9, 1), 0).unwrap();
        assert_eq!(store.logical_range_counts(13), Some(vec![2, 1, 2]));
        for key in [b"b".as_slice(), b"n", b"t", b"x"] {
            let mut owners = committed.replicas_for_key_while_rehoming(&ring, 42, key, 2, None);
            owners.sort_unstable();
            assert_eq!(store.group_of(key), owners);
        }
        prepare(&store, 13, &committed);
        assert!(store.is_leaf_seeded());
        prepare(&store, 12, &pending);
        assert!(!store.is_leaf_seeded(), "a stale authority never reuses");
    }

    #[test]
    fn node_set_rehome_of_a_range_this_store_does_not_hold_keeps_leaves() {
        let ring = crate::ring::Ring::from_members(2, 64, [1, 2, 3]);
        let cfg = epoch_zone_config();
        let prepare = |store: &VersionedStore<MemBlockDevice>, epoch: u64, map: &RangeMap| {
            store.prepare_zone_seed((1, 1, epoch), &ring, 42, Some(cfg.clone()), Some(map.clone()));
        };
        let mut store = sorted_store(1);
        for (i, key) in [b"b".as_slice(), b"h"].into_iter().enumerate() {
            store.apply_at(key, Some(b"v"), v(i as u64 + 1, 1), 0).unwrap();
        }
        let mut map = RangeMap::from_splits([b"k".to_vec()]);
        map.enable_opaque_tokens();
        prepare(&store, 10, &map);
        store = rebuild_range_counts(store);
        assert!(store.is_leaf_seeded());
        let sorted = |mut v: Vec<u32>| { v.sort_unstable(); v };
        let moved = |index: usize, map: &RangeMap| {
            let current = sorted(map.replicas_of_range(&ring, 42, index, 2, None));
            (0u8..=255)
                .map(|seed| crate::partition::opaque_placement_token([seed; 16]))
                .find(|token| sorted(ring.replicas_in_zone(42, token, 2, None)) != current)
                .expect("some opaque token moves the range to another node set")
        };
        let counters = crate::observe::counters();
        let local_before = counters.ae_seed_placement_local_reuses.get();
        let placement_generation = store.placement_generation.get();
        let mut pending = map.clone();
        assert!(pending.set_pending(1, moved(1, &map)));
        prepare(&store, 11, &pending);
        assert!(store.is_leaf_seeded(), "a node-set change on a range this store holds no key of keeps the leaves");
        assert!(store.seed_progress.borrow().is_none(), "no full seed starts");
        assert_ne!(store.placement_generation.get(), placement_generation, "the new placement is installed");
        let mut committed = pending.clone();
        assert!(committed.commit_pending(1));
        prepare(&store, 12, &committed);
        assert!(store.is_leaf_seeded());
        assert!(counters.ae_seed_placement_local_reuses.get() - local_before >= 2);
        store.apply_at(b"t", Some(b"v"), v(9, 1), 0).unwrap();
        for key in [b"b".as_slice(), b"h", b"t"] {
            let owners = sorted(committed.replicas_for_key_while_rehoming(&ring, 42, key, 2, None));
            assert_eq!(store.group_of(key), owners);
        }
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&store, INDEX_BUCKETS));
        assert_eq!(store.logical_range_counts(12), Some(vec![2, 1]));

        let mut again = committed.clone();
        assert!(again.set_pending(0, moved(0, &committed)));
        prepare(&store, 13, &again);
        assert!(store.is_leaf_seeded() && store.regroup_pending(), "a node-set change on a range holding keys regroups that span");
        store = rebuild_range_counts(store);
        assert!(!store.regroup_pending());
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&store, INDEX_BUCKETS));
    }

    #[test]
    fn rehome_commit_that_only_reorders_replicas_keeps_seed_and_counts() {
        let ring = crate::ring::Ring::from_members(2, 64, [1, 2]);
        let cfg = epoch_zone_config();
        let prepare = |store: &VersionedStore<MemBlockDevice>, epoch: u64, map: &RangeMap| {
            store.prepare_zone_seed((1, 1, epoch), &ring, 42, Some(cfg.clone()), Some(map.clone()));
        };
        let mut store = sorted_store(1);
        for (i, key) in [b"b".as_slice(), b"h", b"n", b"t"].into_iter().enumerate() {
            store.apply_at(key, Some(b"v"), v(i as u64 + 1, 1), 0).unwrap();
        }
        let mut map = RangeMap::from_splits([b"k".to_vec()]);
        map.enable_opaque_tokens();
        prepare(&store, 10, &map);
        store = rebuild_range_counts(store);
        assert_eq!(store.logical_range_counts(10), Some(vec![2, 2]));
        let digest = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let placement_generation = store.placement_generation.get();
        let logical_generation = store.logical_range_counts.borrow().as_ref().unwrap().generation;
        let current = map.replicas_of_range(&ring, 42, 1, 2, None);
        let flipped = (0u8..=255)
            .map(|seed| crate::partition::opaque_placement_token([seed; 16]))
            .find(|token| {
                let target = ring.replicas_in_zone(42, token, 2, None);
                let (mut a, mut b) = (target.clone(), current.clone());
                a.sort_unstable();
                b.sort_unstable();
                target != current && a == b
            })
            .expect("some opaque token reverses the two-node replica order");
        let mut pending = map.clone();
        assert!(pending.set_pending(1, flipped));
        prepare(&store, 11, &pending);
        let mut committed = pending.clone();
        assert!(committed.commit_pending(1));
        assert_ne!(
            committed.replicas_for_key_while_rehoming(&ring, 42, b"n", 2, None),
            map.replicas_for_key_while_rehoming(&ring, 42, b"n", 2, None),
            "the commit changes replica order"
        );
        prepare(&store, 12, &committed);
        assert!(store.is_leaf_seeded(), "an order-only placement change keeps the seed");
        assert!(store.seed_progress.borrow().is_none(), "no full seed starts");
        assert_eq!(store.placement_generation.get(), placement_generation);
        assert_eq!(store.logical_range_counts(12), Some(vec![2, 2]));
        assert_eq!(store.logical_range_counts.borrow().as_ref().unwrap().generation, logical_generation);
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), digest);
        assert_eq!(digest, recompute_leaves(&store, INDEX_BUCKETS));
        for key in [b"b".as_slice(), b"h", b"n", b"t"] {
            let mut owners = committed.replicas_for_key_while_rehoming(&ring, 42, key, 2, None);
            owners.sort_unstable();
            assert_eq!(store.group_of(key), owners);
        }

        let ring3 = crate::ring::Ring::from_members(2, 64, [1, 2, 3]);
        let moved = (0u8..=255)
            .map(|seed| crate::partition::opaque_placement_token([seed; 16]))
            .find(|token| {
                let mut a = ring3.replicas_in_zone(42, token, 2, None);
                let mut b = committed.replicas_of_range(&ring3, 42, 1, 2, None);
                a.sort_unstable();
                b.sort_unstable();
                a != b
            })
            .expect("some opaque token moves the range to another node set");
        store.prepare_zone_seed((1, 1, 13), &ring3, 42, Some(cfg.clone()), Some(committed.clone()));
        store = rebuild_range_counts(store);
        let mut elsewhere = committed.clone();
        assert!(elsewhere.set_pending(1, moved));
        assert!(elsewhere.commit_pending(1));
        store.prepare_zone_seed((1, 1, 14), &ring3, 42, Some(cfg.clone()), Some(elsewhere));
        assert!(store.is_leaf_seeded() && store.regroup_pending(), "a real node-set change regroups the span");
        store = rebuild_range_counts(store);
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&store, INDEX_BUCKETS));
    }

    #[test]
    fn boundary_rebase_keeps_in_flight_seed_and_unready_state() {
        let ring = crate::ring::Ring::from_members(2, 64, [1, 2]);
        let cfg = epoch_zone_config();
        let mut store = sorted_store(1);
        store.apply_at(b"b", Some(b"v"), v(1, 1), 0).unwrap();
        store.apply_at(b"t", Some(b"v"), v(2, 1), 0).unwrap();
        let mut map = RangeMap::from_splits([b"k".to_vec()]);
        map.enable_opaque_tokens();
        store.prepare_zone_seed((1, 1, 10), &ring, 42, Some(cfg.clone()), Some(map.clone()));
        assert!(!store.seed_step().unwrap());
        assert!(store.versions.seeding.get());
        let mut pending = map.clone();
        assert!(pending.set_pending(1, crate::partition::opaque_placement_token([5; 16])));
        let generation = store.logical_range_counts.borrow().as_ref().unwrap().generation;
        store.prepare_zone_seed((1, 1, 11), &ring, 42, Some(cfg.clone()), Some(pending.clone()));
        assert!(store.versions.seeding.get(), "in-flight seed survives a boundary rebase");
        assert_eq!(store.logical_range_counts.borrow().as_ref().unwrap().generation, generation);
        let mut steps = 0;
        while !store.seed_step().unwrap() {
            steps += 1;
            assert!(steps < 10_000);
        }
        assert_eq!(store.logical_range_counts(11), Some(vec![1, 1]));
        assert_eq!(store.logical_range_counts(10), None);
        assert!(store.is_leaf_seeded());
    }

    #[test]
    fn pending_target_on_other_replicas_regroups() {
        let ring = crate::ring::Ring::from_members(1, 64, [1, 2, 3]);
        let mut cfg = epoch_zone_config();
        cfg.rf = 1;
        let mut store = sorted_store(1);
        store.apply_at(b"b", Some(b"v"), v(1, 1), 0).unwrap();
        store.apply_at(b"t", Some(b"v"), v(2, 1), 0).unwrap();
        let mut map = RangeMap::from_splits([b"k".to_vec()]);
        map.enable_opaque_tokens();
        store.prepare_zone_seed((1, 1, 10), &ring, 42, Some(cfg.clone()), Some(map.clone()));
        store = rebuild_range_counts(store);
        let current = map.replicas_of_range(&ring, 42, 1, 1, None);
        let seed = (0u8..=255)
            .find(|seed| {
                let token = crate::partition::opaque_placement_token([*seed; 16]);
                ring.replicas_in_zone(42, &token, 1, None) != current
            })
            .expect("some opaque token lands on another replica");
        let mut pending = map.clone();
        assert!(pending.set_pending(1, crate::partition::opaque_placement_token([seed; 16])));
        let placement_generation = store.placement_generation.get();
        store.prepare_zone_seed((1, 1, 11), &ring, 42, Some(cfg.clone()), Some(pending.clone()));
        assert_ne!(store.placement_generation.get(), placement_generation);
        assert!(store.is_leaf_seeded() && store.regroup_pending(), "replica set changed for the pending range");
        assert_eq!(store.logical_range_counts(11), Some(vec![1, 1]), "counts follow boundaries, not placement");
        store = rebuild_range_counts(store);
        assert_eq!(store.logical_range_counts(11), Some(vec![1, 1]));
        for key in [b"b".as_slice(), b"t"] {
            let mut owners = pending.replicas_for_key_while_rehoming(&ring, 42, key, 1, None);
            owners.sort_unstable();
            assert_eq!(store.group_of(key), owners);
        }
    }

    #[test]
    fn epoch_seed_actual_inputs_and_conservative_authorities_invalidate() {
        let mut store = sorted_store(1);
        store.apply_at(b"a", None, v(1, 1), 0).unwrap();
        store.apply_at(b"z", Some(b"v"), v(2, 1), 0).unwrap();
        let ring = crate::ring::Ring::from_members(2, 64, [1, 2, 3]);
        let cfg = epoch_zone_config();
        let initial = RangeMap::single();
        let split = RangeMap::from_splits([b"m".to_vec()]);
        let mut pending = split.clone();
        pending.enable_opaque_tokens();
        pending.set_pending(1, crate::partition::opaque_placement_token([9; 16]));
        let mut committed = pending.clone();
        assert!(committed.commit_pending(1));
        let maps = [initial, split.clone(), pending.clone(), split, pending, committed];
        let probes: [&[u8]; 7] = [b"", b"a", b"l", b"m", b"m\0", b"n", b"z"];
        let mut previous: Option<RangeMap> = None;
        for (i, map) in maps.iter().enumerate() {
            let generation = store.placement_generation.get();
            let same_boundaries = previous.as_ref().is_some_and(|p| p.splits() == map.splits());
            let same_replicas = previous.as_ref().is_some_and(|p| probes.iter().all(|key| {
                let mut before = p.replicas_for_key_while_rehoming(&ring, 42, key, 2, None);
                let mut after = map.replicas_for_key_while_rehoming(&ring, 42, key, 2, None);
                before.sort_unstable();
                after.sort_unstable();
                before == after
            }));
            let counts_before = store.logical_range_counts(i as u64 + 9);
            prepare_epoch_zone(&store, i as u64 + 10, map);
            if same_replicas {
                assert_eq!(store.placement_generation.get(), generation, "map {i} reuses placement");
                assert!(store.is_leaf_seeded(), "map {i} keeps the seed");
            } else {
                assert_ne!(store.placement_generation.get(), generation, "map {i} invalidates");
                assert!(!store.is_leaf_seeded());
            }
            assert_eq!(store.logical_range_counts(i as u64 + 10),
                if same_boundaries { counts_before } else { None });
            previous = Some(map.clone());
            store = rebuild_range_counts(store);
            let mut exact = vec![0; map.num_ranges()];
            exact[map.range_index(b"a")] += 1;
            exact[map.range_index(b"z")] += 1;
            assert_eq!(store.logical_range_counts(i as u64 + 10), Some(exact));
            for key in [b"a".as_slice(), b"z".as_slice()] {
                let mut owners = map.replicas_for_key_while_rehoming(&ring, 42, key, 2, None);
                owners.sort_unstable();
                assert_eq!(store.group_of(key), owners);
            }
            assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(),
                recompute_leaves(&store, INDEX_BUCKETS));
        }
        let map = maps.last().unwrap();
        let mut scoped = cfg.clone();
        scoped.scope = Some(std::collections::BTreeSet::from([1, 3]));
        let mut rf = cfg.clone();
        rf.rf = 1;
        let mut ordering = cfg.clone();
        ordering.ordering = crate::zone::Ordering::Unsorted;
        for (epochs, topology, zone, config, ranges) in [
            ((1, 1, 16), crate::ring::Ring::from_members(2, 64, [1, 2, 4]), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 17), crate::ring::Ring::from_weighted(2, 64, [(1, 2), (2, 1), (3, 1)]), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 18), crate::ring::Ring::from_members(2, 32, [1, 2, 3]), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 19), crate::ring::Ring::from_members(1, 64, [1, 2, 3]), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 20), ring.clone(), 42, scoped, Some(map.clone())),
            ((1, 1, 21), ring.clone(), 42, rf, Some(map.clone())),
            ((1, 1, 22), ring.clone(), 42, ordering, None),
            ((1, 1, 23), ring.clone(), 43, cfg.clone(), Some(map.clone())),
            ((2, 1, 24), ring.clone(), 42, cfg.clone(), Some(map.clone())),
            ((1, 2, 25), ring.clone(), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 0), ring.clone(), 42, cfg.clone(), Some(map.clone())),
            ((1, 1, 9), ring.clone(), 42, cfg.clone(), Some(map.clone())),
        ] {
            prepare_epoch_zone(&store, 10, map);
            store = rebuild_range_counts(store);
            let generation = store.placement_generation.get();
            store.prepare_zone_seed(epochs, &topology, zone, Some(config), ranges);
            assert_ne!(store.placement_generation.get(), generation);
            assert!(!store.is_leaf_seeded());
            store = rebuild_range_counts(store);
        }
        let fresh = sorted_store(1);
        prepare_epoch_zone(&fresh, 24, map);
        assert!(!fresh.is_leaf_seeded(), "new store cannot inherit the old identity's seed");
        assert_eq!(fresh.logical_range_counts(24), None);
        store.set_placement(std::sync::Arc::new(|_| vec![99]));
        prepare_epoch_zone(&store, 25, map);
        assert!(!store.is_leaf_seeded(), "untracked placement cannot reuse the cached inputs");
    }

    #[test]
    #[ignore = "measurement: compares logical-range accounting apply overhead"]
    fn logical_range_count_apply_cost() {
        fn arm(accounting: bool) -> u128 {
            let cfg = Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            };
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(BS, 1 << 18),
                cfg,
            );
            let mut store = VersionedStore::new(engine, 1).unwrap();
            if accounting {
                store.set_logical_range_map(
                    1,
                    RangeMap::from_splits([b"g".to_vec(), b"n".to_vec(), b"t".to_vec()]),
                );
                store = rebuild_range_counts(store);
            }
            let started = std::time::Instant::now();
            for i in 0..50_000u64 {
                let key = format!("k{i:08}");
                store.apply_at(key.as_bytes(), Some(b"v"), v(i + 1, 1), 0).unwrap();
            }
            started.elapsed().as_nanos() / 50_000
        }
        let mut baseline = Vec::new();
        let mut candidate = Vec::new();
        for round in 0..7 {
            if round % 2 == 0 {
                baseline.push(arm(false));
                candidate.push(arm(true));
            } else {
                candidate.push(arm(true));
                baseline.push(arm(false));
            }
        }
        baseline.sort_unstable();
        candidate.sort_unstable();
        eprintln!(
            "logical range apply cost: baseline_median_ns={} candidate_median_ns={} delta_pct={:.2}",
            baseline[baseline.len() / 2],
            candidate[candidate.len() / 2],
            (candidate[candidate.len() / 2] as f64 / baseline[baseline.len() / 2] as f64 - 1.0)
                * 100.0,
        );
    }

    #[test]
    fn packed_versions_preserve_legacy_nodes_and_wide_writers() {
        let legacy = Version { hlc: 17, node: 42 };
        assert_eq!(unpack_version(pack_version(legacy)), legacy);
        assert_eq!(unpack_version(((17u128) << 64) | 42), legacy);

        let wide = Version {
            hlc: 18,
            node: 0xfedc_ba98_7654_3210,
        };
        assert_eq!(unpack_version(pack_version(wide)), wide);
    }

    #[test]
    fn unknown_old_fails_closed_and_exact_rebuild_ignores_loser_history() {
        let mut left = sorted_store(1);
        let mut right = sorted_store(2);
        let winner = v(30, 7);
        left.apply(b"same", Some(b"winner"), winner).unwrap();
        right.apply(b"same", Some(b"winner"), winner).unwrap();
        left.append_unknown_for_test(b"same", Some(b"left-loser"), v(10, 1))
            .unwrap();
        right
            .append_unknown_for_test(b"same", Some(b"right-loser"), v(20, 2))
            .unwrap();

        assert!(matches!(
            left.version_leaf_digests(INDEX_BUCKETS),
            Err(LeafDigestError::Dirty)
        ));
        assert!(matches!(
            right.version_leaf_digests(INDEX_BUCKETS),
            Err(LeafDigestError::Dirty)
        ));

        let left = std::cell::RefCell::new(left);
        rebuild_all(&left);
        vals_rt::block_on(
            async {
                assert!(ensure_tree_ready_cooperative(
                    &right,
                    1,
                    &std::collections::HashSet::from([0]),
                )
                .await
                .unwrap());
            },
            || {},
        );
        assert_eq!(
            left.borrow().version_leaf_digests(INDEX_BUCKETS).unwrap(),
            right.version_leaf_digests(INDEX_BUCKETS).unwrap()
        );
        let slot = VersionIndex::slot(b"same");
        assert_eq!(left.borrow().versions.buckets.borrow()[slot].logical_keys, 1);
    }

    #[test]
    fn divergent_unknown_old_buckets_cannot_compare_until_exact_rebuild() {
        let mut left = sorted_store(1);
        let mut right = sorted_store(2);
        left.append_unknown_for_test(b"different", Some(b"left"), v(40, 1))
            .unwrap();
        right
            .append_unknown_for_test(b"different", Some(b"right"), v(50, 2))
            .unwrap();
        assert!(left.version_leaf_digests(INDEX_BUCKETS).is_err());
        assert!(right.version_leaf_digests(INDEX_BUCKETS).is_err());

        let left = std::cell::RefCell::new(left);
        let right = std::cell::RefCell::new(right);
        rebuild_all(&left);
        rebuild_all(&right);
        assert_ne!(
            left.borrow().version_leaf_digests(INDEX_BUCKETS).unwrap(),
            right.borrow().version_leaf_digests(INDEX_BUCKETS).unwrap()
        );
    }

    fn exported_gauge(name: &str) -> u64 {
        crate::observe::gauges_named()
            .into_iter()
            .find_map(|(metric, _, value)| (metric == name).then_some(value))
            .unwrap()
    }

    #[test]
    fn repeated_unknown_old_in_one_bucket_does_not_republish_dirty_gauges() {
        let versions = VersionIndex::new();
        let slot = 7;
        versions.dirty_bucket(slot);
        let first = dirty_gauge_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&versions.gauge_id)
            .copied()
            .unwrap();
        let generation = versions.buckets.borrow()[slot].generation;

        versions.dirty_bucket(slot);

        let second = dirty_gauge_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&versions.gauge_id)
            .copied()
            .unwrap();
        assert_eq!(second.count, first.count);
        assert_eq!(second.oldest, first.oldest);
        assert_eq!(versions.buckets.borrow()[slot].generation, generation + 1);
    }

    #[test]
    fn dirty_oldest_age_advances_at_scrape_and_clears_per_store() {
        let mut older = sorted_store(1);
        older
            .append_unknown_for_test(b"older-dirty", Some(b"value"), v(61, 1))
            .unwrap();
        let older = std::cell::RefCell::new(older);
        let older_id = older.borrow().versions.gauge_id;
        std::thread::sleep(std::time::Duration::from_millis(10));
        let first_age = exported_gauge("ae_dirty_oldest_ms");
        std::thread::sleep(std::time::Duration::from_millis(10));
        let second_age = exported_gauge("ae_dirty_oldest_ms");
        assert!(first_age > 0);
        assert!(second_age > first_age);

        let mut newer = sorted_store(2);
        newer
            .append_unknown_for_test(b"newer-dirty", Some(b"value"), v(62, 2))
            .unwrap();
        let newer = std::cell::RefCell::new(newer);
        let newer_id = newer.borrow().versions.gauge_id;
        rebuild_all(&newer);
        drop(newer);
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(exported_gauge("ae_dirty_oldest_ms") > second_age);
        assert!(dirty_gauge_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&older_id));

        rebuild_all(&older);
        drop(older);
        let registry = dirty_gauge_registry()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert!(!registry.contains_key(&older_id));
        assert!(!registry.contains_key(&newer_id));
        assert_eq!(
            crate::observe::gauges().ae_dirty_buckets.get(),
            registry.values().map(|state| state.count).sum()
        );
    }

    #[test]
    fn cooperative_seed_journals_unknown_old_as_dirty() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 40);
        let mut store = VersionedStore::open(mem.share(), cfg, 1).unwrap();
        assert!(!store.seed_step().unwrap());
        store
            .append_unknown_for_test(b"during-seed", Some(b"value"), v(65, 1))
            .unwrap();
        while !store.seed_step().unwrap() {}
        assert!(matches!(
            store.version_leaf_digests(INDEX_BUCKETS),
            Err(LeafDigestError::Dirty)
        ));
        let cell = std::cell::RefCell::new(store);
        rebuild_all(&cell);
        assert!(cell.borrow().version_leaf_digests(INDEX_BUCKETS).is_ok());
        let store = cell.borrow();
        assert_eq!(store.versions.live.get(), store.engine().recovered_versions().unwrap().len() as u64);
        let slot = VersionIndex::slot(b"during-seed");
        let count = store.engine().recovered_versions().unwrap().iter()
            .filter(|(key, _)| VersionIndex::slot(key) == slot).count() as u64;
        assert_eq!(store.versions.buckets.borrow()[slot].logical_keys, count);
        assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&store, INDEX_BUCKETS));
    }

    #[test]
    fn stale_source_and_bucket_generations_cannot_publish_a_rebuild() {
        let mut store = sorted_store(1);
        store.append_unknown_for_test(b"raced", Some(b"one"), v(70, 1))
            .unwrap();
        let scope = std::collections::HashSet::from([0]);
        let mutation_snapshot = store.dirty_snapshot(1, &scope).unwrap();
        store.apply(b"raced", Some(b"two"), v(80, 1)).unwrap();
        assert!(!store.publish_dirty_rebuild(
            &mutation_snapshot,
            std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        ));
        assert!(store.version_leaf_digests(INDEX_BUCKETS).is_err());

        let flush_snapshot = store.dirty_snapshot(1, &scope).unwrap();
        store.flush().unwrap();
        assert!(!store.publish_dirty_rebuild(
            &flush_snapshot,
            std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        ));

        store.apply(b"second-run", Some(b"value"), v(85, 1)).unwrap();
        store.flush().unwrap();
        let merge_snapshot = store.dirty_snapshot(1, &scope).unwrap();
        store.compact_runs(0).unwrap();
        assert!(!store.publish_dirty_rebuild(
            &merge_snapshot,
            std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        ));

        let placement_snapshot = store.dirty_snapshot(1, &scope).unwrap();
        store.set_placement(std::sync::Arc::new(|_| vec![1, 2]));
        assert!(!store.publish_dirty_rebuild(
            &placement_snapshot,
            std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        ));

        let retirement_snapshot = store.dirty_snapshot(1, &scope).unwrap();
        store
            .forget(&std::collections::HashSet::from([b"raced".to_vec()]))
            .unwrap();
        assert!(!store.publish_dirty_rebuild(
            &retirement_snapshot,
            std::collections::HashMap::new(),
            &std::collections::HashMap::new(),
        ));
        let cell = std::cell::RefCell::new(store);
        rebuild_all(&cell);
        assert!(cell.borrow().version_leaf_digests(INDEX_BUCKETS).is_ok());
    }

    #[test]
    fn cancelled_dirty_rebuild_keeps_the_bucket_fail_closed() {
        use std::future::Future;
        let mut store = sorted_store(1);
        store.apply(b"cancel", Some(b"winner"), v(90, 1)).unwrap();
        store.flush().unwrap();
        store
            .append_unknown_for_test(b"cancel", Some(b"new"), v(100, 1))
            .unwrap();
        let cell = std::cell::RefCell::new(store);
        let scope = std::collections::HashSet::from([0]);
        let mut future = Box::pin(ensure_tree_ready_cooperative_cell(&cell, 1, &scope));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        let _ = future.as_mut().poll(&mut cx);
        drop(future);
        assert!(matches!(
            cell.borrow().version_leaf_digests(INDEX_BUCKETS),
            Err(LeafDigestError::Dirty)
        ));
        rebuild_all(&cell);
    }

    #[test]
    fn dirty_rebuild_io_error_keeps_bucket_dirty_and_retryable() {
        let fail = std::sync::Arc::new(AtomicBool::new(false));
        let device = FailableDevice {
            inner: MemBlockDevice::new(BS, 512),
            fail: fail.clone(),
        };
        let engine = Engine::create_prewiped(
            device,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store.apply(b"fault", Some(b"old"), v(110, 1)).unwrap();
        store.flush().unwrap();
        store
            .append_unknown_for_test(b"fault", Some(b"new"), v(120, 1))
            .unwrap();
        let cell = std::cell::RefCell::new(store);
        let scope = std::collections::HashSet::from([0]);
        fail.store(true, AtomicOrdering::Relaxed);
        let failed = vals_rt::block_on(
            ensure_tree_ready_cooperative_cell(&cell, 1, &scope),
            || {},
        );
        assert!(failed.is_err());
        assert!(cell.borrow().version_leaf_digests(INDEX_BUCKETS).is_err());
        fail.store(false, AtomicOrdering::Relaxed);
        rebuild_all(&cell);
    }

    #[test]
    fn reopen_discards_dirty_cache_and_seeds_exact_winners() {
        let mem = MemBlockDevice::new(BS, 512);
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let mut store = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 1).unwrap();
        store
            .append_unknown_for_test(b"restart", Some(b"value"), v(130, 1))
            .unwrap();
        assert!(store.version_leaf_digests(INDEX_BUCKETS).is_err());
        store.sync().unwrap();
        drop(store);

        let reopened = VersionedStore::open(mem.share(), cfg, 1).unwrap();
        assert_eq!(
            reopened.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&reopened, INDEX_BUCKETS)
        );
    }

    #[test]
    fn placement_invalidation_never_serves_prechange_dirty_digest() {
        let mut store = sorted_store(1);
        store.set_placement(std::sync::Arc::new(|_| vec![1, 2]));
        store.append_unknown_for_test(b"placed", Some(b"value"), v(60, 1))
            .unwrap();
        assert!(store.version_leaf_digests(INDEX_BUCKETS).is_err());
        store.set_placement(std::sync::Arc::new(|_| vec![1, 3]));
        store.invalidate_leaf_seed();
        assert!(!store.is_leaf_seeded());
        let cell = std::cell::RefCell::new(store);
        rebuild_all(&cell);
        let scope = std::collections::HashSet::from([0]);
        assert!(cell
            .borrow()
            .scoped_leaf_digests(INDEX_BUCKETS, 1, &scope, Some(3))
            .is_ok());
    }

    fn drive_staged(
        s: &mut VersionedStore<MemBlockDevice>,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        wall_ms: u64,
    ) -> Applied {
        vals_rt::block_on(
            async {
                loop {
                    match s.apply_stage(key, value, version, wall_ms).unwrap() {
                        StagedApply::Done(a) => return a,
                        StagedApply::NeedsRunRead { probe, gen } => {
                            let dev = s.device_handle();
                            let run_old = VersionedStore::version_from_probe(probe, &*dev)
                                .await
                                .unwrap();
                            match s
                                .apply_commit_staged(key, value, version, run_old, gen, wall_ms)
                                .unwrap()
                            {
                                StagedApplyOutcome::Applied(a) => return a,
                                StagedApplyOutcome::Retry => continue,
                            }
                        }
                    }
                }
            },
            || {},
        )
    }

    fn sorted_store_big(node: u32) -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(BS, 8192);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    }

    #[test]
    fn run_merge_respects_lww_across_nodes() {
        let mut s = sorted_store_big(7);
        s.apply(b"k", Some(b"older"), v(10, 5)).unwrap();
        s.flush().unwrap();
        assert_eq!(
            s.apply(b"k", Some(b"newer"), v(11, 0)).unwrap(),
            Applied::Won
        );
        s.flush().unwrap();
        assert_eq!(s.engine().len(), 0, "k lives only in the two runs now");

        assert_eq!(s.current_version(b"k").unwrap(), Some(v(11, 0)));
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"newer"[..]));
        let want: std::collections::HashSet<u32> =
            std::iter::once(crate::antientropy::bucket_of(b"k", INDEX_BUCKETS)).collect();
        let (eng, _) = s
            .engine()
            .version_digest_page(
                &want,
                |key| crate::antientropy::bucket_of(key, INDEX_BUCKETS),
                None,
                usize::MAX,
                DIGEST_WIRE_OVERHEAD,
            )
            .unwrap();
        let merged: Vec<(Vec<u8>, Version)> = eng
            .into_iter()
            .map(|(k, raw)| (k, unpack_version(raw)))
            .collect();
        assert_eq!(
            merged,
            vec![(b"k".to_vec(), v(11, 0))],
            "engine digest picks the LWW winner"
        );
    }

    #[test]
    fn memtable_bytes_drives_byte_cap_flush_trigger() {
        let mut s = sorted_store_big(1);
        assert_eq!(
            s.memtable_bytes(),
            0,
            "empty store has no memtable footprint"
        );

        for i in 0..50u32 {
            let k = format!("key-{i:05}");
            s.apply(k.as_bytes(), Some(b"v"), v(i as u64 + 1, 1))
                .unwrap();
        }
        let buffered = s.memtable_bytes();
        assert!(buffered > 0, "footprint grows as keys buffer");

        assert!(s.memtable_bytes() >= buffered, "cap below footprint -> due");
        assert!(
            s.memtable_bytes() < buffered.saturating_mul(2),
            "cap above footprint -> not due"
        );

        s.flush().unwrap();
        assert_eq!(s.engine().len(), 0, "memtable drained by the flush");
        assert_eq!(s.memtable_bytes(), 0, "footprint released after flush");
    }

    #[test]
    fn compact_runs_is_read_transparent_and_leaves_leaf_unchanged() {
        let mut s = sorted_store_big(3);
        for k in [b"a" as &[u8], b"b", b"c", b"d"] {
            s.apply(k, Some(b"v1"), v(1, 1)).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(s.apply(b"a", Some(b"v2"), v(5, 1)).unwrap(), Applied::Won);
        assert_eq!(s.apply(b"b", None, v(8, 1)).unwrap(), Applied::Won);
        assert_eq!(s.apply(b"e", Some(b"v2"), v(2, 1)).unwrap(), Applied::Won);
        s.flush().unwrap();
        assert_eq!(s.run_count(), 2);

        let leaf_before = s.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
        let digest_before = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        let reads_before: Vec<Option<Vec<u8>>> = [b"a" as &[u8], b"b", b"c", b"d", b"e"]
            .iter()
            .map(|k| s.get(k).unwrap())
            .collect();

        let stats = s.compact_runs(0).unwrap();
        assert_eq!(stats.runs_merged, 2);
        assert_eq!(s.run_count(), 1);

        let reads_after: Vec<Option<Vec<u8>>> = [b"a" as &[u8], b"b", b"c", b"d", b"e"]
            .iter()
            .map(|k| s.get(k).unwrap())
            .collect();
        assert_eq!(reads_after, reads_before, "reads changed across the merge");
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
        assert_eq!(s.get(b"b").unwrap(), None, "tombstone survives the merge");

        assert_eq!(
            s.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            leaf_before,
            "run-merge must not change the Merkle leaf accumulator"
        );

        let digest_after = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        assert_eq!(
            digest_after, digest_before,
            "AE digest page changed across the merge"
        );

        assert_eq!(
            s.apply(b"a", Some(b"stale"), v(4, 1)).unwrap(),
            Applied::Lost(v(5, 1))
        );
        assert_eq!(s.apply(b"a", Some(b"v3"), v(9, 1)).unwrap(), Applied::Won);
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v3"[..]));
    }

    #[test]
    fn nonblocking_flush_is_read_transparent_and_leaves_leaf_unchanged() {
        let mut s = sorted_store_big(3);
        for k in [b"a" as &[u8], b"b", b"c", b"d"] {
            s.apply(k, Some(b"v1"), v(1, 1)).unwrap();
        }

        let leaf_before = s.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
        let digest_before = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        let reads_before: Vec<Option<Vec<u8>>> = [b"a" as &[u8], b"b", b"c", b"d"]
            .iter()
            .map(|k| s.get(k).unwrap())
            .collect();

        let plan = s
            .flush_plan()
            .unwrap()
            .expect("non-empty memtable plans a flush");
        let wdev = s.device_sibling().expect("mem sibling");
        let written =
            vals_rt::block_on(vals_store::build_and_write_flush(plan, &wdev), || {}).unwrap();
        let meta = s.flush_commit(written).unwrap().unwrap();
        assert_eq!(meta.entry_count, 4);
        assert_eq!(s.run_count(), 1);

        let reads_after: Vec<Option<Vec<u8>>> = [b"a" as &[u8], b"b", b"c", b"d"]
            .iter()
            .map(|k| s.get(k).unwrap())
            .collect();
        assert_eq!(reads_after, reads_before, "reads changed across the flush");

        assert_eq!(
            s.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            leaf_before,
            "flush must not change the Merkle leaf accumulator"
        );
        let digest_after = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        assert_eq!(
            digest_after, digest_before,
            "AE digest page changed across the flush"
        );

        assert_eq!(
            s.apply(b"a", Some(b"stale"), v(0, 1)).unwrap(),
            Applied::Lost(v(1, 1))
        );
        assert_eq!(s.apply(b"a", Some(b"v2"), v(5, 1)).unwrap(), Applied::Won);
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn async_staged_serve_matches_sync_flushed_and_mixed() {
        let mut s = sorted_store_big(1);
        for k in [b"a" as &[u8], b"b", b"c", b"d"] {
            s.apply(k, Some(b"v1"), v(1, 1)).unwrap();
        }
        s.flush().unwrap();
        s.apply(b"a", Some(b"v2"), v(5, 1)).unwrap();
        s.apply(b"e", Some(b"v2"), v(2, 1)).unwrap();
        s.flush().unwrap();
        s.apply(b"f", Some(b"v3"), v(3, 1)).unwrap();
        s.apply(b"c", Some(b"v9"), v(9, 1)).unwrap();
        s.apply(b"b", None, v(8, 1)).unwrap();

        let dev = s.device_handle();

        for key in [b"a" as &[u8], b"b", b"c", b"d", b"e", b"f", b"z"] {
            let got = vals_rt::block_on(
                async {
                    let plan = match s.probe_get(key) {
                        vals_store::StagedGet::Memtable(p) => Some(*p),
                        vals_store::StagedGet::WinnerDirectory { plan, .. } => Some(*plan),
                        vals_store::StagedGet::Absent | vals_store::StagedGet::Covered(_) => None,
                        vals_store::StagedGet::Err(e) => panic!("{e}"),
                        vals_store::StagedGet::Runs {
                            key,
                            probes,
                            mutable,
                            ..
                        } => {
                            let run = vals_store::resolve_run_probe(&key, &probes, &*dev)
                                .await
                                .unwrap();
                            let ptr = match (mutable, run) {
                                (Some(mem), Some(cold)) if cold.version > mem.version => cold.ptr,
                                (Some(mem), _) => mem.ptr,
                                (None, Some(cold)) => cold.ptr,
                                (None, None) => return None,
                            };
                            Some(s.plan_get_ptr(&ptr).unwrap())
                        }
                    };
                    match plan {
                        None => None,
                        Some(p) => {
                            crate::replica::execute_read_plan(&p, &*dev)
                                .await
                                .unwrap()
                                .value
                        }
                    }
                },
                || {},
            );
            assert_eq!(
                got,
                s.get(key).unwrap(),
                "async Read != sync get for {key:?}"
            );
        }

        for key in [b"a" as &[u8], b"b", b"c", b"d", b"e", b"f", b"z"] {
            let probe = s.probe_version(key);
            let got =
                vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
            assert_eq!(
                got,
                s.version(key).unwrap(),
                "async Digest != sync version for {key:?}"
            );
        }

        let ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
        let sync_full = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        let plan = s
            .plan_bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        let async_full = vals_rt::block_on(
            VersionedStore::bucket_digest_page_from_plan(
                plan,
                INDEX_BUCKETS,
                &ids,
                None,
                usize::MAX,
                &*dev,
            ),
            || {},
        )
        .unwrap();
        assert_eq!(
            async_full, sync_full,
            "async digest page != sync (unbounded)"
        );

        let mut after: Option<Vec<u8>> = None;
        loop {
            let sync_pg = s
                .bucket_digest_page(INDEX_BUCKETS, &ids, after.as_deref(), 32)
                .unwrap();
            let plan = s
                .plan_bucket_digest_page(INDEX_BUCKETS, &ids, after.as_deref(), 32)
                .unwrap();
            let async_pg = vals_rt::block_on(
                VersionedStore::bucket_digest_page_from_plan(
                    plan,
                    INDEX_BUCKETS,
                    &ids,
                    after.as_deref(),
                    32,
                    &*dev,
                ),
                || {},
            )
            .unwrap();
            assert_eq!(async_pg, sync_pg, "async page != sync page at {after:?}");
            let (pg, more) = async_pg;
            assert!(!pg.is_empty());
            after = Some(pg.last().unwrap().0.clone());
            if !more {
                break;
            }
        }
    }

    #[test]
    fn staged_apply_matches_sync_across_cases() {
        let build = || {
            let mut s = sorted_store_big(1);
            for k in [b"a" as &[u8], b"b", b"c", b"d"] {
                s.apply_at(k, Some(b"v1"), v(1, 1), 0).unwrap();
            }
            s.flush().unwrap();
            s.apply_at(b"a", Some(b"v2"), v(5, 1), 0).unwrap();
            s.apply_at(b"e", Some(b"v2"), v(2, 1), 0).unwrap();
            s.flush().unwrap();
            s.apply_at(b"f", Some(b"v3"), v(3, 1), 0).unwrap();
            s.apply_at(b"c", Some(b"v9"), v(9, 1), 0).unwrap();
            s.apply_at(b"b", None, v(8, 1), 0).unwrap();
            s
        };
        let ops: &[(&[u8], Option<&[u8]>, Version)] = &[
            (b"a", Some(b"win"), v(9, 1)),
            (b"a", Some(b"stale"), v(2, 1)),
            (b"d", None, v(7, 1)),
            (b"e", Some(b"eq"), v(2, 1)),
            (b"c", Some(b"cwin"), v(12, 1)),
            (b"c", Some(b"clo"), v(4, 1)),
            (b"b", Some(b"bwin"), v(20, 1)),
            (b"f", Some(b"fwin"), v(9, 1)),
            (b"z", Some(b"new"), v(3, 1)),
            (b"y", None, v(3, 1)),
        ];
        for (key, value, version) in ops {
            let mut staged = build();
            let mut sync = build();
            let a_staged = drive_staged(&mut staged, key, *value, *version, 0);
            let a_sync = sync.apply_at(key, *value, *version, 0).unwrap();
            assert_eq!(a_staged, a_sync, "Applied mismatch for {key:?} {version:?}");
            assert_eq!(
                staged.get(key).unwrap(),
                sync.get(key).unwrap(),
                "post-apply read mismatch for {key:?}"
            );
            assert_eq!(
                staged.version_leaf_digests(INDEX_BUCKETS).unwrap(),
                sync.version_leaf_digests(INDEX_BUCKETS).unwrap(),
                "leaf-accumulator drift for {key:?}"
            );
        }
    }

    #[test]
    fn staged_older_write_loses_after_early_exit_resolve() {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = CountingDevice {
            inner: MemBlockDevice::new(BS, 8192),
            reads: reads.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        s.apply_at(b"k", Some(b"v-old"), v(10, 1), 0).unwrap();
        s.flush().unwrap();
        s.apply_at(b"k", Some(b"v-new"), v(20, 1), 0).unwrap();
        s.flush().unwrap();
        assert_eq!(s.engine().len(), 0, "k lives only in the two runs");

        let (probe, gen) = match s.apply_stage(b"k", Some(b"stale"), v(15, 1), 0).unwrap() {
            StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
            _ => panic!("expected NeedsRunRead for a flushed key"),
        };
        match &probe {
            vals_store::StagedVersion::Runs { probes, .. } => {
                assert_eq!(probes.len(), 2, "k must be a candidate in both runs")
            }
            _ => panic!("expected a run probe"),
        }
        let dev = s.device_handle();
        let before = reads.load(AtomicOrdering::Relaxed);
        let run_old =
            vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
        assert_eq!(
            run_old,
            Some(v(20, 1)),
            "the early-exit resolve returns the newest run's version"
        );
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed) - before,
            2,
            "every candidate block is read before resolving by version"
        );
        match s
            .apply_commit_staged(b"k", Some(b"stale"), v(15, 1), run_old, gen, 0)
            .unwrap()
        {
            StagedApplyOutcome::Applied(a) => assert_eq!(
                a,
                Applied::Lost(v(20, 1)),
                "the out-of-order older write must lose"
            ),
            StagedApplyOutcome::Retry => panic!("no run-set change happened"),
        }

        let before = reads.load(AtomicOrdering::Relaxed);
        let (key, probes) = match s.probe_get(b"k") {
            vals_store::StagedGet::Runs { key, probes, .. } => (key, probes),
            _ => panic!("expected a run probe (nothing may be memtable-resident)"),
        };
        let entry = vals_rt::block_on(vals_store::resolve_run_probe(&key, &probes, &*dev), || {})
            .unwrap()
            .expect("k is present");
        assert_eq!(unpack_version(entry.version), v(20, 1));
        assert_eq!(reads.load(AtomicOrdering::Relaxed) - before, 2);
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(20, 1)));
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v-new"[..]));
    }

    #[test]
    fn blind_apply_accepts_without_run_reads_and_reads_keep_argmax() {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = CountingDevice {
            inner: MemBlockDevice::new(BS, 8192),
            reads: reads.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        s.apply_at(b"k", Some(b"winner"), v(20, 1), 0).unwrap();
        s.flush().unwrap();

        let before = reads.load(AtomicOrdering::Relaxed);
        assert_eq!(
            s.apply_blind_at(b"k", Some(b"stale"), v(10, 1), 0).unwrap(),
            Applied::Won
        );
        assert_eq!(reads.load(AtomicOrdering::Relaxed), before);
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"winner"[..]));
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(20, 1)));

        let before = reads.load(AtomicOrdering::Relaxed);
        assert_eq!(s.apply_blind_at(b"k", None, v(30, 1), 0).unwrap(), Applied::Won);
        assert_eq!(s.apply_blind_at(b"k", None, v(30, 1), 0).unwrap(), Applied::Won);
        assert_eq!(
            s.apply_blind_at(b"k", Some(b"lower"), v(25, 1), 0).unwrap(),
            Applied::Won
        );
        assert_eq!(reads.load(AtomicOrdering::Relaxed), before);
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(30, 1)));
        assert_eq!(s.get(b"k").unwrap(), None);
    }

    #[test]
    fn winner_directory_makes_blind_apply_authoritative_without_run_reads() {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = CountingDevice {
            inner: MemBlockDevice::new(BS, 8192),
            reads: reads.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        s.apply_at(b"k", Some(b"winner"), v(20, 1), 0).unwrap();
        s.flush().unwrap();
        s.engine_mut().rebuild_winner_directory().unwrap();
        s.ensure_seeded().unwrap();
        assert!(!s.versions.has_dirty());

        let before = reads.load(AtomicOrdering::Relaxed);
        assert!(matches!(
            s.probe_version(b"k"),
            vals_store::StagedVersion::Memtable(raw) if unpack_version(raw) == v(20, 1)
        ));
        assert_eq!(
            s.apply_blind_at(b"k", Some(b"stale"), v(10, 1), 0).unwrap(),
            Applied::Won
        );
        assert_eq!(reads.load(AtomicOrdering::Relaxed), before);
        assert_eq!(s.engine().len(), 0, "a known loser is not appended");
        assert!(!s.versions.has_dirty());

        assert_eq!(
            s.apply_blind_at(b"k", None, v(30, 1), 0).unwrap(),
            Applied::Won
        );
        assert_eq!(reads.load(AtomicOrdering::Relaxed), before);
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(30, 1)));
        assert!(!s.versions.has_dirty());
    }

    #[test]
    fn cas_resolves_flushed_winner_after_a_blind_loser() {
        let mut s = sorted_store_big(1);
        s.apply_at(b"k", Some(b"winner"), v(20, 1), 0).unwrap();
        s.flush().unwrap();
        assert_eq!(
            s.apply_unconditional_at(
                b"k",
                Some(b"blind-loser"),
                v(10, 1),
                0,
                ApplyMode::Blind,
                LiveFloor::Enforce,
            )
            .unwrap(),
            Applied::Won
        );
        assert_eq!(
            s.cas_at(b"k", b"bad", Some(v(10, 1)), 0).unwrap(),
            CasResult::Mismatch {
                current: Some(v(20, 1))
            }
        );
        let CasResult::Applied(applied) = s.cas_at(b"k", b"cas", Some(v(20, 1)), 0).unwrap()
        else {
            panic!("authoritative winner must satisfy CAS");
        };
        assert!(applied.wins_over(&v(20, 1)));
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"cas"[..]));
    }

    #[test]
    fn mixed_blind_and_legacy_replicas_keep_identical_lww_reads() {
        let build = || {
            let mut s = sorted_store_big(1);
            s.apply_at(b"k", Some(b"winner"), v(20, 1), 0).unwrap();
            s.flush().unwrap();
            s
        };
        let mut blind = build();
        let mut legacy = build();
        assert_eq!(
            blind
                .apply_unconditional_at(b"k", Some(b"stale"), v(10, 1), 0, ApplyMode::Blind, LiveFloor::Enforce)
                .unwrap(),
            Applied::Won
        );
        assert_eq!(
            legacy
                .apply_unconditional_at(b"k", Some(b"stale"), v(10, 1), 0, ApplyMode::Legacy, LiveFloor::Enforce)
                .unwrap(),
            Applied::Lost(v(20, 1))
        );
        assert_eq!(blind.get_versioned(b"k").unwrap(), legacy.get_versioned(b"k").unwrap());

        assert_eq!(
            blind
                .apply_unconditional_at(b"k", None, v(30, 1), 0, ApplyMode::Blind, LiveFloor::Enforce)
                .unwrap(),
            Applied::Won
        );
        assert_eq!(
            legacy
                .apply_unconditional_at(b"k", None, v(30, 1), 0, ApplyMode::Legacy, LiveFloor::Enforce)
                .unwrap(),
            Applied::Won
        );
        assert_eq!(blind.get_versioned(b"k").unwrap(), legacy.get_versioned(b"k").unwrap());
        assert_eq!(blind.get(b"k").unwrap(), None);
    }

    #[test]
    fn blind_apply_preserves_skew_and_gc_floor_rejections() {
        let mut s = sorted_store_big(1).with_gc_grace_ms(1_000);
        let anchor = Version {
            hlc: 5_000u64 << 16,
            node: 1,
        };
        assert_eq!(
            s.apply_blind_at(b"anchor", Some(b"a"), anchor, 5_000).unwrap(),
            Applied::Won
        );
        let stale = Version {
            hlc: 1u64 << 16,
            node: 1,
        };
        assert_eq!(
            s.apply_blind_at(b"old-put", Some(b"x"), stale, 5_000).unwrap(),
            Applied::Rejected
        );
        assert_eq!(
            s.apply_blind_at(b"old-tomb", None, stale, 5_000).unwrap(),
            Applied::Won
        );
        let future = Version {
            hlc: 7_000u64 << 16,
            node: 1,
        };
        assert_eq!(
            s.apply_blind_at(b"future", Some(b"x"), future, 5_000).unwrap(),
            Applied::Rejected
        );
    }

    #[test]
    fn expired_tombstones_are_stored_only_over_an_older_local_version() {
        let at = |ms: u64| Version { hlc: ms << 16, node: 2 };
        let build = |directory: bool| {
            let mut s = sorted_store_big(1).with_gc_grace_ms(1_000);
            s.apply_at(b"older", Some(b"o"), at(3_500), 3_500).unwrap();
            s.apply_at(b"anchor", Some(b"a"), at(5_000), 5_000).unwrap();
            s.flush().unwrap();
            if directory {
                s.engine_mut().rebuild_winner_directory().unwrap();
            }
            s
        };
        let expired = at(3_600);
        let fresh = at(4_500);
        type Apply = fn(&mut VersionedStore<MemBlockDevice>, &[u8], Version) -> Applied;
        let paths: [(&str, bool, Apply); 4] = [
            ("legacy", false, |s, k, v| s.apply_at(k, None, v, 5_000).unwrap()),
            ("staged", false, |s, k, v| drive_staged(s, k, None, v, 5_000)),
            ("blind+directory", true, |s, k, v| s.apply_blind_at(k, None, v, 5_000).unwrap()),
            ("blind", false, |s, k, v| s.apply_blind_at(k, None, v, 5_000).unwrap()),
        ];
        for (name, directory, apply) in paths {
            let mut s = build(directory);
            assert_eq!(apply(&mut s, b"absent", expired), Applied::Won, "{name}");
            let skipped_absent = s.get_versioned(b"absent").unwrap().is_none();
            assert_eq!(skipped_absent, name != "blind", "{name}: only a path that knows the key is absent may skip it");
            assert_eq!(apply(&mut s, b"older", expired), Applied::Won, "{name}");
            assert_eq!(
                s.get_versioned(b"older").unwrap(),
                Some(Stored { version: expired, value: None }),
                "{name}: an expired tombstone still shadows an older local value"
            );
            assert_eq!(apply(&mut s, b"new", fresh), Applied::Won, "{name}");
            assert_eq!(
                s.get_versioned(b"new").unwrap(),
                Some(Stored { version: fresh, value: None }),
                "{name}: a tombstone inside grace is stored"
            );
        }
    }

    #[test]
    fn staged_apply_matches_sync_rejections() {
        let skew_ver = Version {
            hlc: 1000u64 << 16,
            node: 1,
        };
        let mut staged = sorted_store_big(1);
        let mut sync = sorted_store_big(1);
        assert_eq!(
            drive_staged(&mut staged, b"k", Some(b"x"), skew_ver, 0),
            sync.apply_at(b"k", Some(b"x"), skew_ver, 0).unwrap(),
        );
        assert_eq!(
            sync.apply_at(b"k", Some(b"x"), skew_ver, 0).unwrap(),
            Applied::Rejected
        );

        let grace = 1000u64;
        let build_floor = || {
            let mut s = sorted_store_big(1).with_gc_grace_ms(grace);
            s.apply_at(
                b"anchor",
                Some(b"a"),
                Version {
                    hlc: 5000u64 << 16,
                    node: 1,
                },
                5_000,
            )
            .unwrap();
            s
        };
        let mut staged = build_floor();
        let mut sync = build_floor();
        let stale = Version {
            hlc: 1u64 << 16,
            node: 1,
        };
        let a_staged = drive_staged(&mut staged, b"gone", Some(b"resurrect"), stale, 5_000);
        let a_sync = sync
            .apply_at(b"gone", Some(b"resurrect"), stale, 5_000)
            .unwrap();
        assert_eq!(a_staged, a_sync, "gc-floor rejection mismatch");
        assert_eq!(
            a_sync,
            Applied::Rejected,
            "stale put below the GC floor must be Rejected"
        );
    }

    #[test]
    fn staged_apply_retries_on_flush_between_stage_and_commit() {
        let mut s = sorted_store_big(1);
        s.apply_at(b"a", Some(b"v1"), v(1, 1), 0).unwrap();
        s.flush().unwrap();
        let dev = s.device_handle();
        let (probe, gen) = match s.apply_stage(b"a", Some(b"v2"), v(5, 1), 0).unwrap() {
            StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
            _ => panic!("expected NeedsRunRead for a flushed key"),
        };
        let run_old =
            vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
        s.apply_at(b"b", Some(b"x"), v(1, 1), 0).unwrap();
        s.flush().unwrap();
        match s
            .apply_commit_staged(b"a", Some(b"v2"), v(5, 1), run_old, gen, 0)
            .unwrap()
        {
            StagedApplyOutcome::Retry => {}
            StagedApplyOutcome::Applied(_) => panic!("a stale generation must Retry"),
        }
        assert_eq!(
            drive_staged(&mut s, b"a", Some(b"v2"), v(5, 1), 0),
            Applied::Won
        );
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn staged_apply_uses_memtable_version_landed_during_await() {
        let mut s = sorted_store_big(1);
        s.apply_at(b"a", Some(b"v1"), v(1, 1), 0).unwrap();
        s.flush().unwrap();
        let dev = s.device_handle();
        let (probe, gen) = match s.apply_stage(b"a", Some(b"mid"), v(5, 1), 0).unwrap() {
            StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
            _ => panic!("expected NeedsRunRead"),
        };
        let run_old =
            vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
        assert_eq!(run_old, Some(v(1, 1)));
        assert_eq!(
            s.apply_at(b"a", Some(b"newest"), v(20, 1), 0).unwrap(),
            Applied::Won
        );
        match s
            .apply_commit_staged(b"a", Some(b"mid"), v(5, 1), run_old, gen, 0)
            .unwrap()
        {
            StagedApplyOutcome::Applied(applied) => {
                assert_eq!(applied, Applied::Lost(v(20, 1)))
            }
            StagedApplyOutcome::Retry => panic!("run/storage generation remained stable"),
        }
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"newest"[..]));
        let mut twin = sorted_store_big(1);
        twin.apply_at(b"a", Some(b"v1"), v(1, 1), 0).unwrap();
        twin.flush().unwrap();
        twin.apply_at(b"a", Some(b"newest"), v(20, 1), 0).unwrap();
        assert_eq!(
            s.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            twin.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            "leaf-accumulator drift after a concurrent memtable insert"
        );
    }

    #[test]
    fn staged_apply_retries_on_truncate_between_stage_and_commit() {
        let mut s = sorted_store_big(1);
        s.apply_at(b"a", Some(b"v1"), v(1, 1), 0).unwrap();
        s.flush().unwrap();
        let dev = s.device_handle();
        let (probe, gen) = match s.apply_stage(b"a", Some(b"v2"), v(5, 1), 0).unwrap() {
            StagedApply::NeedsRunRead { probe, gen } => (probe, gen),
            _ => panic!("expected NeedsRunRead"),
        };
        let _run_old =
            vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
        s.clear().unwrap();
        match s
            .apply_commit_staged(b"a", Some(b"v2"), v(5, 1), _run_old, gen, 0)
            .unwrap()
        {
            StagedApplyOutcome::Retry => {}
            StagedApplyOutcome::Applied(_) => panic!("a truncate must force Retry"),
        }
        assert_eq!(
            drive_staged(&mut s, b"a", Some(b"v2"), v(5, 1), 0),
            Applied::Won
        );
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn engine_version_digest_page_matches_versionindex_digest_page() {
        let mut s = sorted_store_big(1);

        for i in 0..200u32 {
            let key = format!("key-{i:05}");
            s.apply(key.as_bytes(), Some(b"val"), v(1000 + i as u64, 1))
                .unwrap();
        }
        s.flush().unwrap();
        assert_eq!(s.engine().len(), 0, "flush drained the memtable");

        s.apply(b"key-00000", Some(b"newer"), v(9999, 1)).unwrap();
        s.apply(b"key-90000", Some(b"fresh"), v(2000, 1)).unwrap();
        s.apply(b"key-90001", Some(b"fresh"), v(2001, 1)).unwrap();
        s.apply(b"key-00050", None, v(9998, 1)).unwrap();

        assert!(!s.engine().is_empty(), "tail repopulated the memtable");

        let all_ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
        let some_ids: Vec<u32> = vec![3, 7, 11, 42, 100, 1000, 2048, 4095]
            .into_iter()
            .filter(|&b| b < INDEX_BUCKETS)
            .collect();
        let empty_ids: Vec<u32> = Vec::new();

        let afters: Vec<Option<&[u8]>> = vec![
            None,
            Some(b"key-00050" as &[u8]),
            Some(b"key-00099"),
            Some(b"key-00123x"),
        ];
        let budgets = [usize::MAX, 64usize];

        for ids in [&all_ids[..], &some_ids[..], &empty_ids[..]] {
            let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
            for &after in &afters {
                for &budget in &budgets {
                    let (want_page, want_more) = s
                        .bucket_digest_page(INDEX_BUCKETS, ids, after, budget)
                        .unwrap();
                    let (got_raw, got_more) = s
                        .engine()
                        .version_digest_page(
                            &want,
                            |k| crate::antientropy::bucket_of(k, INDEX_BUCKETS),
                            after,
                            budget,
                            DIGEST_WIRE_OVERHEAD,
                        )
                        .unwrap();
                    let got_page: Vec<(Vec<u8>, Version)> = got_raw
                        .into_iter()
                        .map(|(k, raw)| (k, unpack_version(raw)))
                        .collect();
                    assert_eq!(
                        got_page, want_page,
                        "digest page differs: ids={ids:?} after={after:?} budget={budget}"
                    );
                    assert_eq!(
                        got_more, want_more,
                        "more flag differs: ids={ids:?} after={after:?} budget={budget}"
                    );
                }
            }
        }
    }

    #[test]
    fn engine_version_digest_page_paging_reassembles_like_versionindex() {
        let mut s = sorted_store_big(1);
        for i in 0..120u32 {
            let key = format!("k{i:04}");
            s.apply(key.as_bytes(), Some(b"v"), v(1000 + i as u64, 1))
                .unwrap();
        }
        s.flush().unwrap();
        for i in 120..180u32 {
            let key = format!("k{i:04}");
            s.apply(key.as_bytes(), Some(b"v"), v(1000 + i as u64, 1))
                .unwrap();
        }

        let ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();

        let (reference, more) = s
            .bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX)
            .unwrap();
        assert!(!more);

        let budget_that_fit_two_legacy_versions = 2 * (5 + 4 + 12);
        let (boundary, more) = s
            .engine()
            .version_digest_page(
                &want,
                |k| crate::antientropy::bucket_of(k, INDEX_BUCKETS),
                None,
                budget_that_fit_two_legacy_versions,
                DIGEST_WIRE_OVERHEAD,
            )
            .unwrap();
        assert_eq!(boundary.len(), 1, "wire budget includes the 16-byte version");
        assert!(more);

        let mut acc: Vec<(Vec<u8>, Version)> = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (raw, more) = s
                .engine()
                .version_digest_page(
                    &want,
                    |k| crate::antientropy::bucket_of(k, INDEX_BUCKETS),
                    after.as_deref(),
                    48,
                    DIGEST_WIRE_OVERHEAD,
                )
                .unwrap();
            assert!(!raw.is_empty(), "a page always makes progress");
            after = Some(raw.last().unwrap().0.clone());
            let last = !more;
            acc.extend(raw.into_iter().map(|(k, r)| (k, unpack_version(r))));
            if last {
                break;
            }
        }
        assert_eq!(
            acc, reference,
            "paged engine result must equal the RAM-index page"
        );
    }

    #[test]
    fn open_seeds_versions_without_reread_matching_new() {
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let path = temp_path("open-aux-seed");

        let (kept_ver, tomb_ver) = {
            let dev = FileBlockDevice::create(&path, BS, 512).unwrap();
            let mut s = VersionedStore::new(Engine::create_prewiped(dev, cfg), 7).unwrap();
            s.put_at(b"a", b"v-a", 1000).unwrap();
            s.put_at(b"b", b"v-b-old", 1001).unwrap();
            let kept = s.put_at(b"b", b"v-b-new", 2000).unwrap();
            s.put_at(b"gone", b"x", 1500).unwrap();
            let tomb = s.delete_at(b"gone", 3000).unwrap();
            s.sync().unwrap();
            (kept, tomb)
        };

        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let reread = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 7).unwrap();

        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let seeded = VersionedStore::open(dev, cfg, 7).unwrap();

        for s in [&reread, &seeded] {
            assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v-a"[..]));
            assert_eq!(s.get(b"b").unwrap().as_deref(), Some(&b"v-b-new"[..]));
            assert_eq!(s.get(b"gone").unwrap(), None, "tombstone reads as absent");
            assert_eq!(s.current_version(b"b").unwrap(), Some(kept_ver));
            assert_eq!(
                s.current_version(b"gone").unwrap(),
                Some(tomb_ver),
                "tombstone's version must be retained so an older write still loses"
            );
            assert_eq!(s.high_water_ms(), 3000);
        }
    }

    #[test]
    fn put_get_roundtrip_with_version() {
        let mut s = store(1);
        let ver = s.put_at(b"k", b"v1", 1000).unwrap();
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
        let stored = s.get_versioned(b"k").unwrap().unwrap();
        assert_eq!(stored.version, ver);
        assert_eq!(stored.value.as_deref(), Some(&b"v1"[..]));
        assert_eq!(s.get(b"missing").unwrap(), None);
    }

    #[test]
    fn borrow_free_read_matches_get_versioned() {
        let mut s = store(1);
        let small_ver = s.put_at(b"small", b"hello", 1000).unwrap();
        let big_val = vec![0xABu8; BS * 3 + 50];
        let big_ver = s.put_at(b"big", &big_val, 1001).unwrap();
        s.put_at(b"gone", b"x", 1002).unwrap();
        let gone_ver = s.delete_at(b"gone", 1003).unwrap();

        let dev = s.device_handle();
        let via_plan = |key: &[u8]| {
            s.plan_get(key)
                .unwrap()
                .map(|p| vals_rt::block_on(execute_read_plan(&p, &*dev), || {}).unwrap())
        };
        for key in [&b"small"[..], b"big", b"gone", b"missing"] {
            assert_eq!(via_plan(key), s.get_versioned(key).unwrap(), "key {key:?}");
        }
        let small = via_plan(b"small").unwrap();
        assert_eq!(small.version, small_ver);
        assert_eq!(small.value.as_deref(), Some(&b"hello"[..]));
        let big = via_plan(b"big").unwrap();
        assert_eq!(big.version, big_ver);
        assert_eq!(big.value.as_deref(), Some(&big_val[..]));
        let gone = via_plan(b"gone").unwrap();
        assert_eq!(gone.version, gone_ver);
        assert_eq!(gone.value, None);
    }

    fn exec_plans(
        plans: Vec<(Vec<u8>, ValuePlan)>,
        dev: &MemBlockDevice,
    ) -> Vec<(Vec<u8>, Stored)> {
        plans
            .into_iter()
            .map(|(k, p)| match p {
                ValuePlan::Read(p) => (k, vals_rt::block_on(execute_read_plan(&p, dev), || {}).unwrap()),
                ValuePlan::Covered(version) => (k, Stored { version, value: None }),
            })
            .collect()
    }

    fn exec_span_page(
        (keys, plan, _more): (Vec<(Vec<u8>, Version)>, vals_store::SpanReadPlan, bool),
        dev: &MemBlockDevice,
    ) -> Vec<(Vec<u8>, Stored)> {
        let mut blobs: Vec<Option<Vec<u8>>> = vec![None; keys.len()];
        for i in 0..plan.num_spans() {
            let recs = vals_rt::block_on(plan.execute_span(i, dev), || {}).unwrap();
            for (slot, rec) in recs {
                blobs[slot] = Some(rec.value);
            }
        }
        keys.into_iter()
            .zip(blobs)
            .map(|((key, version), blob)| {
                let value = adopt_blob(blob.expect("slot decoded"));
                (key, Stored { version, value })
            })
            .collect()
    }

    #[test]
    fn borrow_free_scan_matches_scan_page() {
        let mut s = sorted_store(1);
        s.put_at(b"a", b"alpha", 1000).unwrap();
        let big = vec![0x5Au8; BS * 2 + 7];
        s.put_at(b"b", &big, 1001).unwrap();
        s.put_at(b"c", b"gamma", 1002).unwrap();
        s.delete_at(b"c", 2000).unwrap();
        s.put_at(b"d", b"delta", 1003).unwrap();
        let dev = s.device_handle();

        let mut cursor: Option<Vec<u8>> = None;
        let mut via_plan = Vec::new();
        loop {
            let span_page = s.plan_scan(cursor.as_deref(), None, usize::MAX, 1).unwrap();
            let more_p = span_page.2;
            let (sync_page, more_s) = s.scan_page(cursor.as_deref(), None, usize::MAX, 1).unwrap();
            let page = exec_span_page(span_page, &dev);
            assert_eq!(page, sync_page, "page at cursor {cursor:?}");
            assert_eq!(more_p, more_s, "more flag at cursor {cursor:?}");
            via_plan.extend(page);
            if !more_s {
                break;
            }
            let mut next = via_plan.last().unwrap().0.clone();
            next.push(0);
            cursor = Some(next);
        }
        assert_eq!(via_plan, s.scan(None, None).unwrap());
        assert_eq!(via_plan[1].1.value.as_deref(), Some(&big[..]));
        assert!(via_plan
            .iter()
            .any(|(k, st)| k == b"c" && st.value.is_none()));
    }

    #[test]
    fn borrow_free_plan_values_match_sync() {
        let mut s = store(1);
        for (i, k) in ["a", "bb", "ccc", "dddd", "eeeee"].iter().enumerate() {
            s.put_at(k.as_bytes(), format!("val-{i}").as_bytes(), 1000 + i as u64)
                .unwrap();
        }
        s.delete_at(b"ccc", 2000).unwrap();
        let dev = s.device_handle();
        let keys: Vec<Vec<u8>> = ["dddd", "a", "ccc", "missing", "bb"]
            .iter()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        let sync: Vec<(Vec<u8>, Stored)> = keys
            .iter()
            .filter_map(|k| s.stored(k).unwrap().map(|st| (k.clone(), st)))
            .collect();

        let (plans, more) = s.plan_values(&keys, usize::MAX).unwrap();
        assert!(!more);
        assert_eq!(exec_plans(plans, &dev), sync);

        let mut start = 0;
        let mut paged: Vec<(Vec<u8>, Stored)> = Vec::new();
        loop {
            let (plans, more) = s.plan_values(&keys[start..], 1).unwrap();
            let page = exec_plans(plans, &dev);
            start += 1;
            paged.extend(page);
            if !more {
                break;
            }
        }
        assert_eq!(paged, sync);
    }

    #[test]
    fn local_overwrite_always_wins_and_advances_version() {
        let mut s = store(1);
        let v1 = s.put_at(b"k", b"a", 1000).unwrap();
        let v2 = s.put_at(b"k", b"b", 1000).unwrap();
        assert!(v2.wins_over(&v1));
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"b"[..]));
    }

    #[test]
    fn apply_store_if_newer_lww() {
        let mut s = store(1);
        assert_eq!(
            s.apply(b"k", Some(b"new"), v(200, 1)).unwrap(),
            Applied::Won
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(
            s.apply(b"k", Some(b"old"), v(100, 1)).unwrap(),
            Applied::Lost(v(200, 1))
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn apply_is_idempotent() {
        let mut s = store(1);
        assert_eq!(s.apply(b"k", Some(b"x"), v(50, 1)).unwrap(), Applied::Won);
        assert_eq!(
            s.apply(b"k", Some(b"x"), v(50, 1)).unwrap(),
            Applied::Lost(v(50, 1))
        );
    }

    #[test]
    fn replicas_converge_regardless_of_order() {
        let (w1, w2) = (v(100, 1), v(200, 2));
        let mut a = store(1);
        let mut b = store(2);

        a.apply(b"k", Some(b"first"), w1).unwrap();
        a.apply(b"k", Some(b"second"), w2).unwrap();

        b.apply(b"k", Some(b"second"), w2).unwrap();
        b.apply(b"k", Some(b"first"), w1).unwrap();

        assert_eq!(a.get(b"k").unwrap(), b.get(b"k").unwrap());
        assert_eq!(a.get(b"k").unwrap().as_deref(), Some(&b"second"[..]));
    }

    #[test]
    fn writer_identity_breaks_ties() {
        let mut s = store(1);
        assert_eq!(
            s.apply(b"k", Some(b"node1"), v(100, 1)).unwrap(),
            Applied::Won
        );
        assert_eq!(
            s.apply(b"k", Some(b"node2"), v(100, 2)).unwrap(),
            Applied::Won
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"node2"[..]));
        assert_eq!(
            s.apply(b"k", Some(b"node1again"), v(100, 1)).unwrap(),
            Applied::Lost(v(100, 2))
        );
    }

    #[test]
    fn delete_is_a_versioned_tombstone() {
        let mut s = store(1);
        s.apply(b"k", Some(b"v"), v(100, 1)).unwrap();
        assert_eq!(s.apply(b"k", None, v(200, 1)).unwrap(), Applied::Won);
        assert_eq!(s.get(b"k").unwrap(), None);
        let stored = s.get_versioned(b"k").unwrap().unwrap();
        assert_eq!(stored.value, None);
        assert_eq!(stored.version, v(200, 1));

        assert_eq!(
            s.apply(b"k", Some(b"stale"), v(150, 1)).unwrap(),
            Applied::Lost(v(200, 1))
        );
        assert_eq!(s.get(b"k").unwrap(), None);

        assert_eq!(
            s.apply(b"k", Some(b"fresh"), v(300, 1)).unwrap(),
            Applied::Won
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"fresh"[..]));
    }

    #[test]
    fn cas_success_and_mismatch() {
        let mut s = store(1);
        let r = s.cas_at(b"k", b"v1", None, 1000).unwrap();
        let ver1 = match r {
            CasResult::Applied(v) => v,
            other => panic!("expected Applied, got {other:?}"),
        };
        assert_eq!(
            s.cas_at(b"k", b"v2", None, 1000).unwrap(),
            CasResult::Mismatch {
                current: Some(ver1)
            }
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
        let r = s.cas_at(b"k", b"v2", Some(ver1), 1000).unwrap();
        match r {
            CasResult::Applied(v2) => assert!(v2.wins_over(&ver1)),
            other => panic!("expected Applied, got {other:?}"),
        }
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn cas_over_a_flushed_run_resident_key() {
        let mut s = sorted_store_big(7);

        s.apply(b"k", Some(b"v1"), v(1000, 7)).unwrap();
        let ver1 = s
            .current_version(b"k")
            .unwrap()
            .expect("k present after put");
        s.flush().unwrap();
        assert_eq!(
            s.engine().len(),
            0,
            "memtable empty after flush — k is run-resident"
        );
        assert!(
            s.run_count() >= 1,
            "k must live in a run for CAS to do the run read"
        );
        assert_eq!(s.current_version(b"k").unwrap(), Some(ver1));

        let stale = v(1, 7);
        assert_ne!(stale, ver1);
        assert_eq!(
            s.cas_at(b"k", b"v-should-not-land", Some(stale), 2000)
                .unwrap(),
            CasResult::Mismatch {
                current: Some(ver1)
            },
            "stale precondition over a flushed key must mismatch on the run-read version"
        );
        assert_eq!(
            s.get(b"k").unwrap().as_deref(),
            Some(&b"v1"[..]),
            "mismatch must not mutate"
        );
        assert_eq!(
            s.current_version(b"k").unwrap(),
            Some(ver1),
            "mismatch must not advance version"
        );

        let v2 = match s.cas_at(b"k", b"v2", Some(ver1), 3000).unwrap() {
            CasResult::Applied(v2) => v2,
            other => panic!("expected Applied over a flushed key, got {other:?}"),
        };
        assert!(
            v2.wins_over(&ver1),
            "successful CAS must advance the version"
        );
        assert_eq!(
            s.get(b"k").unwrap().as_deref(),
            Some(&b"v2"[..]),
            "value advanced"
        );
        assert_eq!(
            s.current_version(b"k").unwrap(),
            Some(v2),
            "version advanced"
        );
    }

    #[test]
    fn run_read_failure_is_a_non_ack_not_a_false_new() {
        let dev = MemBlockDevice::new(BS, 8192);
        let engine = Engine::create_prewiped(
            ReadFailDevice {
                inner: dev,
                armed: AtomicBool::new(false),
            },
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 7).unwrap();

        s.apply(b"k", Some(b"v1"), v(1000, 7)).unwrap();
        s.flush().unwrap();
        assert_eq!(
            s.engine().len(),
            0,
            "memtable empty after flush — k is run-resident"
        );
        assert!(
            s.run_count() >= 1,
            "k must live in a run for the read to be a device read"
        );

        s.engine()
            .device()
            .armed
            .store(true, AtomicOrdering::Relaxed);

        match s.current_version(b"k") {
            Err(_) => {}
            Ok(other) => {
                panic!("current_version on a run-read failure must be Err, got Ok({other:?})")
            }
        }
        match s.apply(b"k", Some(b"v2"), v(2000, 7)) {
            Err(_) => {}
            Ok(other) => panic!("store-if-newer on a run-read failure must be Err, got {other:?}"),
        }
        match s.cas_at(b"k", b"v3", Some(v(1000, 7)), 4000) {
            Err(_) => {}
            Ok(other) => panic!("CAS on a run-read failure must be Err, got {other:?}"),
        }
    }

    fn vp(phys_ms: u64, node: u32) -> Version {
        Version {
            hlc: phys_ms << 16,
            node: node.into(),
        }
    }

    #[test]
    fn apply_refuses_far_future_write_and_keeps_the_key_unpoisoned() {
        let mut s = store(1).with_max_skew_ms(500);
        let wall = 1_000_000u64;
        assert_eq!(
            s.apply_at(b"k", Some(b"v"), vp(wall, 1), wall).unwrap(),
            Applied::Won
        );
        assert_eq!(
            s.apply_at(b"k", Some(b"poison"), vp(wall + 10_000, 2), wall)
                .unwrap(),
            Applied::Rejected
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(s.get_versioned(b"k").unwrap().unwrap().version, vp(wall, 1));
        assert_eq!(
            s.apply_at(b"k", Some(b"v2"), vp(wall + 100, 1), wall)
                .unwrap(),
            Applied::Won
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    }

    #[test]
    fn apply_accepts_within_skew_and_disabled_cap_accepts_anything() {
        let wall = 1_000_000u64;
        let mut s = store(1).with_max_skew_ms(500);
        assert_eq!(
            s.apply_at(b"k", Some(b"v"), vp(wall + 300, 1), wall)
                .unwrap(),
            Applied::Won
        );
        let mut s0 = store(1).with_max_skew_ms(0);
        assert_eq!(
            s0.apply_at(b"k", Some(b"v"), vp(wall + 9_000_000, 1), wall)
                .unwrap(),
            Applied::Won
        );
    }

    #[test]
    fn compact_gcs_aged_tombstones_only() {
        let wall = 10_000_000u64;
        let mut s = store(1).with_gc_grace_ms(1000);
        s.apply_at(b"old", None, vp(wall - 5000, 1), wall - 5000).unwrap();
        s.advance_durable_gc_watermark(wall);
        s.apply_at(b"live", Some(b"v"), vp(wall, 1), wall).unwrap();
        s.apply_at(b"fresh", None, vp(wall - 100, 1), wall).unwrap();
        assert!(
            s.get_versioned(b"old").unwrap().is_some(),
            "tombstone present pre-GC"
        );

        let stats = s.compact(wall).unwrap();
        assert_eq!(stats.gc_dropped, 1, "only the aged tombstone is collected");
        assert_eq!(
            s.get_versioned(b"old").unwrap(),
            None,
            "aged tombstone forgotten entirely"
        );
        assert!(
            s.get_versioned(b"fresh").unwrap().is_some(),
            "fresh tombstone (within grace) retained"
        );
        assert_eq!(
            s.get(b"live").unwrap().as_deref(),
            Some(&b"v"[..]),
            "live key retained"
        );
    }

    #[test]
    fn admission_floor_rejects_stale_new_put_below_gc_horizon() {
        let wall = 10_000_000u64;
        let grace = 1000u64;
        let mut s = store(1).with_gc_grace_ms(grace);
        s.apply_at(b"anchor", Some(b"a"), vp(wall, 1), wall)
            .unwrap();

        assert_eq!(
            s.apply_at(b"new", Some(b"v"), vp(wall - grace - 1, 5), wall)
                .unwrap(),
            Applied::Rejected
        );
        assert_eq!(s.get(b"new").unwrap(), None, "floored put must not land");
        assert_eq!(
            s.apply_at(b"new2", Some(b"v"), vp(wall - grace + 1, 5), wall)
                .unwrap(),
            Applied::Won
        );
        assert_eq!(
            s.apply_at(b"td", None, vp(wall - 5000, 5), wall).unwrap(),
            Applied::Won
        );
        assert_eq!(
            s.apply_at(b"anchor", Some(b"a2"), vp(wall - 5000, 9), wall)
                .unwrap(),
            Applied::Rejected,
            "the read-free floor applies to every unconditional live candidate"
        );

        let mut s0 = store(1);
        s0.apply_at(b"anchor", Some(b"a"), vp(wall, 1), wall)
            .unwrap();
        assert_eq!(
            s0.apply_at(b"ancient", Some(b"v"), vp(1, 5), wall).unwrap(),
            Applied::Won,
            "no floor when GC is disabled"
        );
    }

    #[test]
    fn tombstone_gc_over_runs_collects_aged_keeps_fresh_and_blocks_resurrection() {
        let wall = 10_000_000u64;
        let grace = 1000u64;
        let mut s = sorted_store_big(1).with_gc_grace_ms(grace);
        s.apply_at(b"aged", None, vp(wall - 5000, 2), wall - 5000).unwrap();
        s.advance_durable_gc_watermark(wall);
        s.apply_at(b"anchor", Some(b"a"), vp(wall, 1), wall)
            .unwrap();
        s.apply_at(b"fresh", None, vp(wall - 100, 2), wall).unwrap();
        s.flush().unwrap();
        s.apply_at(b"filler", Some(b"f"), vp(wall, 3), wall)
            .unwrap();
        s.flush().unwrap();
        assert_eq!(s.run_count(), 2);
        assert!(
            s.get_versioned(b"aged").unwrap().is_some(),
            "present pre-GC"
        );

        let stats = s.compact_runs(wall).unwrap();
        assert_eq!(
            stats.current_dropped_tombstones,
            vec![(b"aged".to_vec(), pack_version(vp(wall - 5000, 2)))],
            "only the aged tombstone winner is collected + reported"
        );
        assert_eq!(s.run_count(), 1);
        assert_eq!(s.current_version(b"aged").unwrap(), None, "aged GC'd");
        assert!(
            s.get_versioned(b"fresh").unwrap().is_some(),
            "fresh tombstone within grace is retained"
        );
        assert_eq!(s.get(b"anchor").unwrap().as_deref(), Some(&b"a"[..]));
        assert_eq!(s.get(b"filler").unwrap().as_deref(), Some(&b"f"[..]));
        assert_index_matches_engine(&s);

        assert_eq!(
            s.apply_at(b"aged", Some(b"pre-delete"), vp(wall - 6000, 9), wall)
                .unwrap(),
            Applied::Rejected,
            "pre-delete straggler must not resurrect the GC'd key"
        );
        assert_eq!(s.get(b"aged").unwrap(), None, "stays deleted");
    }

    #[test]
    fn lone_run_gc_collects_aged_tombstone_without_a_second_run() {
        let wall = 10_000_000u64;
        let grace = 1000u64;
        let mut s = sorted_store_big(1).with_gc_grace_ms(grace);
        s.apply_at(b"aged", None, vp(wall - 5000, 2), wall - 5000).unwrap();
        s.advance_durable_gc_watermark(wall);
        s.apply_at(b"anchor", Some(b"a"), vp(wall, 1), wall)
            .unwrap();
        s.apply_at(b"fresh", None, vp(wall - 100, 2), wall).unwrap();
        s.flush().unwrap();
        assert_eq!(s.run_count(), 1);
        assert!(
            s.get_versioned(b"aged").unwrap().is_some(),
            "present pre-GC"
        );

        assert!(s.tombstone_gc_due(wall), "lone-run GC should be due");

        let plan = s
            .compact_plan_tombstone_gc(wall)
            .unwrap()
            .expect("lone-run GC plans a 1-run rewrite");
        let wdev = std::sync::Arc::new(s.device_sibling().expect("mem sibling"));
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        let stats = s.compact_commit(written).unwrap();

        assert_eq!(
            stats.current_dropped_tombstones,
            vec![(b"aged".to_vec(), pack_version(vp(wall - 5000, 2)))],
            "only the aged tombstone is GC'd"
        );
        assert_eq!(s.run_count(), 1, "no second run appeared");
        assert_eq!(s.current_version(b"aged").unwrap(), None, "aged GC'd");
        assert!(
            s.get_versioned(b"fresh").unwrap().is_some(),
            "fresh tombstone within grace is retained"
        );
        assert_eq!(s.get(b"anchor").unwrap().as_deref(), Some(&b"a"[..]));
        assert_index_matches_engine(&s);

        assert!(
            !s.tombstone_gc_due(wall),
            "trigger must not re-fire once only younger tombstones remain"
        );
    }

    #[test]
    fn tombstone_gc_flushes_the_tail_and_collects_across_runs() {
        let wall = 10_000_000u64;
        let grace = 1000u64;
        let mut s = sorted_store_big(1).with_gc_grace_ms(grace);
        s.apply_at(b"aged", None, vp(wall - 5000, 2), wall - 5000).unwrap();
        s.apply_at(b"anchor", Some(b"a"), vp(wall - 5000, 1), wall - 5000).unwrap();
        s.flush().unwrap();
        s.apply_at(b"tail", None, vp(wall - 4000, 2), wall - 4000).unwrap();
        s.apply_at(b"recent", None, vp(wall - 500, 2), wall - 500).unwrap();
        s.advance_durable_gc_watermark(wall);
        assert_eq!(s.run_count(), 1);

        assert!(s.tombstone_gc_flush_due(wall), "an expired tombstone in the tail needs a flush");
        s.flush().unwrap();
        assert!(!s.tombstone_gc_flush_due(wall));
        assert_eq!(s.run_count(), 2);
        assert!(s.tombstone_gc_due(wall), "GC is due across more than one run");

        let plan = s.compact_plan_tombstone_gc(wall).unwrap().expect("a whole-engine GC merge");
        let wdev = std::sync::Arc::new(s.device_sibling().expect("mem sibling"));
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        let mut dropped = s.compact_commit(written).unwrap().current_dropped_tombstones;
        dropped.sort();
        assert_eq!(
            dropped,
            vec![
                (b"aged".to_vec(), pack_version(vp(wall - 5000, 2))),
                (b"tail".to_vec(), pack_version(vp(wall - 4000, 2))),
            ]
        );
        assert_eq!(s.run_count(), 1);
        assert_eq!(s.get(b"anchor").unwrap().as_deref(), Some(&b"a"[..]));
        assert!(s.get_versioned(b"recent").unwrap().is_some(), "inside grace");
        assert_index_matches_engine(&s);
        s.advance_durable_gc_watermark(wall + 600);
        assert!(!s.tombstone_gc_due(wall + 600), "at most one GC merge per grace period");
        assert!(!s.tombstone_gc_flush_due(wall + 600));
        assert!(s.tombstone_gc_due(wall + 1000), "then the newly expired tombstone is collected");
    }

    #[derive(Default)]
    struct GcFault {
        mode: std::sync::atomic::AtomicU8,
        failed: AtomicU64,
    }

    struct GcFaultDevice {
        inner: MemBlockDevice,
        fault: std::sync::Arc<GcFault>,
    }

    impl GcFault {
        const PASS: u8 = 0;
        const FAIL_NEXT_WRITE: u8 = 1;
        const FAIL_ZEROING: u8 = 2;

        fn arm(&self, mode: u8) {
            self.mode.store(mode, AtomicOrdering::Relaxed);
        }
    }

    impl BlockDevice for GcFaultDevice {
        fn block_size(&self) -> usize { self.inner.block_size() }
        fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            let fault = &self.fault;
            let fail = match fault.mode.load(AtomicOrdering::Relaxed) {
                GcFault::FAIL_NEXT_WRITE => {
                    fault.mode.store(GcFault::PASS, AtomicOrdering::Relaxed);
                    true
                }
                GcFault::FAIL_ZEROING => buf.len() == self.inner.block_size() && buf.iter().all(|b| *b == 0),
                _ => false,
            };
            if fail {
                fault.failed.fetch_add(1, AtomicOrdering::Relaxed);
                return Err(BlockError::Io(std::io::Error::other("injected compaction write failure")));
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    }

    impl vals_spdk::AsyncRead for GcFaultDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
    }

    const GC_T: u64 = 10_000_000;

    fn gc_fault_store() -> (VersionedStore<GcFaultDevice>, std::sync::Arc<GcFault>) {
        let fault = std::sync::Arc::new(GcFault::default());
        let dev = GcFaultDevice { inner: MemBlockDevice::new(BS, 512), fault: fault.clone() };
        let engine = Engine::create_prewiped(
            dev,
            Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap().with_gc_grace_ms(1000);
        for i in 0..6u64 {
            s.apply_at(format!("dead{i}").as_bytes(), None, vp(GC_T + i, 1), GC_T).unwrap();
        }
        for i in 0..10u64 {
            s.apply_at(format!("live{i}").as_bytes(), Some(&[b'x'; 200]), vp(GC_T + 2_000 + i, 1), GC_T + 2_000)
                .unwrap();
        }
        s.advance_durable_gc_watermark(GC_T + 2_000);
        s.sync().unwrap();
        assert_eq!(s.run_count(), 0);
        (s, fault)
    }

    fn dead_keys_present(s: &VersionedStore<GcFaultDevice>) -> usize {
        (0..6).filter(|i| s.engine().version(format!("dead{i}").as_bytes()).unwrap().is_some()).count()
    }

    #[test]
    fn no_runs_compaction_journals_gc_drops_after_a_zeroing_failure_but_not_after_a_rollback() {
        let full = |s: &mut VersionedStore<GcFaultDevice>| s.compact(GC_T + 2_000).map(|stats| stats.gc_dropped);
        let step = |s: &mut VersionedStore<GcFaultDevice>| s.compact_step(GC_T + 2_000, 64).map(|(stats, _)| stats.gc_dropped);
        let arms: [(&str, &dyn Fn(&mut VersionedStore<GcFaultDevice>) -> Result<usize, EngineError>); 2] =
            [("compact", &full), ("compact_step", &step)];
        for (what, compact) in arms {
            let (mut s, dev) = gc_fault_store();
            dev.arm(GcFault::FAIL_NEXT_WRITE);
            assert!(compact(&mut s).is_err(), "{what}: the copy fails before the index update");
            assert_eq!(dev.failed.load(AtomicOrdering::Relaxed), 1);
            assert_eq!(dead_keys_present(&s), 6, "{what}: a failed copy keeps every tombstone in the index");
            assert_index_matches_engine(&s);
            assert_eq!(s.versions.live.get(), 16, "{what}: nothing journaled on the rollback path");

            let (mut s, dev) = gc_fault_store();
            dev.arm(GcFault::FAIL_ZEROING);
            let err = compact(&mut s).expect_err("zeroing the old segments fails");
            dev.arm(GcFault::PASS);
            assert!(!matches!(err, EngineError::PartialForget { .. }), "{what}: the wrapper unwraps the source: {err}");
            assert!(dev.failed.load(AtomicOrdering::Relaxed) >= 1);
            assert_eq!(dead_keys_present(&s), 0, "{what}: the GC drops reached the index before the failure");
            assert_index_matches_engine(&s);
            assert_eq!(s.versions.live.get(), 10, "{what}: every GC drop is journaled (retired)");
        }
    }

    #[test]
    fn memtable_partial_forget_error_retires_successful_prefix_leaves() {
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        use vals_spdk::WriteHandle;

        struct FailNthWrite {
            inner: MemBlockDevice,
            writes: AtomicUsize,
            fail_at: AtomicUsize,
        }
        impl BlockDevice for FailNthWrite {
            fn block_size(&self) -> usize { self.inner.block_size() }
            fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
            fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                self.inner.read_at(lba, buf)
            }
            fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
                let n = self.writes.fetch_add(1, AtomicOrdering::Relaxed) + 1;
                if n == self.fail_at.load(AtomicOrdering::Relaxed) {
                    Err(vals_spdk::BlockError::Io(std::io::Error::other(
                        "injected partial forget failure",
                    )))
                } else {
                    self.inner.write_at(lba, buf)
                }
            }
            fn submit_write(&self, lba: u64, buf: &[u8]) -> WriteHandle {
                WriteHandle::ready(self.write_at(lba, buf))
            }
            fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
        }
        impl vals_spdk::AsyncRead for FailNthWrite {
            async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                self.inner.read_at(lba, buf)
            }
        }
        let dev = FailNthWrite {
            inner: MemBlockDevice::new(BS, 8192),
            writes: AtomicUsize::new(0),
            fail_at: AtomicUsize::new(usize::MAX),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..6u64 {
            s.apply_at(
                format!("k{i}").as_bytes(),
                Some(&vec![1; 850]),
                vp(100 + i, 1),
                100 + i,
            )
            .unwrap();
        }
        let baseline = s.engine().device().writes.load(AtomicOrdering::Relaxed);
        s.engine().device().fail_at.store(baseline + 1, AtomicOrdering::Relaxed);
        let keys: std::collections::HashSet<Vec<u8>> =
            (0..6).map(|i| format!("k{i}").into_bytes()).collect();
        assert!(s.forget(&keys).is_err());
        assert_index_matches_engine(&s);
        s.engine().device().fail_at.store(usize::MAX, AtomicOrdering::Relaxed);
        let _ = s.forget(&keys).unwrap();
        assert_index_matches_engine(&s);
        assert!(s.keys().unwrap().is_empty());
    }

    #[test]
    fn memtable_forget_is_bounded_and_does_not_rewrite_the_vlog() {
        let mut s = sorted_store_big(1);
        for i in 0..2_000u64 {
            s.apply_at(
                format!("k{i:05}").as_bytes(),
                Some(&vec![7; 200]),
                vp(100 + i, 1),
                100 + i,
            )
            .unwrap();
        }
        assert_eq!(s.run_count(), 0);
        let before_segments = s.engine().used_segments();
        let keys: std::collections::HashSet<Vec<u8>> =
            (0..8).map(|i| format!("k{i:05}").into_bytes()).collect();
        assert_eq!(s.forget(&keys).unwrap(), keys.len());
        assert_eq!(s.run_count(), 0, "bounded forget must not build/rewrite a run");
        assert!(
            s.engine().used_segments() >= before_segments,
            "old vlog segments remain for normal reclamation, not full-log rewrite"
        );
        for key in &keys {
            assert!(s.get_versioned(key).unwrap().is_none());
        }
        assert_eq!(s.get(b"k01999").unwrap().as_deref(), Some(&vec![7; 200][..]));
        assert_index_matches_engine(&s);
    }

    #[test]
    fn offcore_forget_plan_drops_exact_flushed_versions() {
        let mut s = sorted_store_big(1);
        let old = vp(100, 1);
        s.apply_at(b"drop", Some(b"old"), old, 100).unwrap();
        s.apply_at(b"keep", Some(b"v"), vp(101, 1), 101).unwrap();
        s.flush().unwrap();

        let plan = s
            .forget_plan(&[(b"drop".to_vec(), old)])
            .unwrap()
            .expect("flushed forget plans an off-core rewrite");
        let wdev = std::sync::Arc::new(s.device_sibling().expect("mem sibling"));
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();

        assert!(s.get_versioned(b"drop").unwrap().is_none());
        assert_eq!(s.get(b"keep").unwrap().as_deref(), Some(&b"v"[..]));
        assert_index_matches_engine(&s);
    }

    #[test]
    fn changed_version_cleanup_commit_consumes_chunk_and_allows_replan() {
        let mut s = sorted_store_big(1);
        let old = vp(100, 1);
        s.apply_at(b"race", Some(b"old"), old, 100).unwrap();
        s.apply_at(b"other", Some(b"v"), vp(101, 1), 101).unwrap();
        s.flush().unwrap();
        let plan = s
            .forget_plan(&[(b"race".to_vec(), old)])
            .unwrap()
            .expect("cleanup plan");
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        let newer = vp(200, 1);
        s.apply_at(b"race", Some(b"new"), newer, 200).unwrap();
        s.compact_commit(written).unwrap();
        assert_eq!(s.get(b"race").unwrap().as_deref(), Some(&b"new"[..]));
        assert_eq!(s.pending_forget_len(), 0);
        let next = s
            .forget_plan(&[(b"race".to_vec(), newer)])
            .unwrap()
            .expect("newer winner can replan");
        s.abort_forget_plan(&next.segments());
    }

    #[test]
    fn forget_plan_allocation_error_rolls_back_pending_and_replans() {
        let mut failed: Option<(VersionedStore<MemBlockDevice>, Version)> = None;
        for count in (40..180usize).step_by(5) {
            let dev = MemBlockDevice::new(BS, 256);
            let engine = Engine::create_prewiped(
                dev,
                Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Sorted },
            );
            let mut candidate = VersionedStore::new(engine, 1).unwrap();
            let version = vp(100, 1);
            let mut setup_ok = true;
            for i in 0..count {
                if candidate
                    .apply_at(
                        format!("k{i:04}").as_bytes(),
                        Some(&vec![3; 220]),
                        vp(100 + i as u64, 1),
                        100 + i as u64,
                    )
                    .is_err()
                {
                    setup_ok = false;
                    break;
                }
            }
            if !setup_ok || candidate.flush().is_err() {
                continue;
            }
            match candidate.forget_plan(&[(b"k0000".to_vec(), version)]) {
                Err(_) => {
                    failed = Some((candidate, version));
                    break;
                }
                Ok(Some(plan)) => candidate.abort_forget_plan(&plan.segments()),
                Ok(None) => {}
            }
        }
        let (mut s, _old_version) = failed.expect("fixture must hit run-output allocation failure");
        assert_eq!(s.pending_forget_len(), 0, "planning error must roll back chunk");
        assert!(!s.compact_in_flight(), "planning error must not latch compact gate");

        s.clear().unwrap();
        let version = vp(10_000, 1);
        s.apply_at(b"retry", Some(b"v"), version, 10_000).unwrap();
        s.flush().unwrap();
        let retry = s
            .forget_plan(&[(b"retry".to_vec(), version)])
            .unwrap()
            .expect("replanning succeeds after capacity is restored");
        assert_eq!(s.pending_forget_len(), 1);
        s.abort_forget_plan(&retry.segments());
    }

    #[test]
    fn failed_or_aborted_cleanup_consumes_chunk_and_allows_replan() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        s.apply_at(b"k", Some(b"v"), version, 100).unwrap();
        s.flush().unwrap();
        let plan = s
            .forget_plan(&[(b"k".to_vec(), version)])
            .unwrap()
            .expect("first plan");
        s.abort_forget_plan(&plan.segments());
        assert_eq!(s.pending_forget_len(), 0);
        let retry = s
            .forget_plan(&[(b"k".to_vec(), version)])
            .unwrap()
            .expect("abort permits retry");
        s.abort_forget_plan(&retry.segments());
        assert_eq!(s.pending_forget_len(), 0);
    }

    #[test]
    fn cleanup_batch_accumulates_while_maintenance_blocks_planning() {
        let mut s = sorted_store_big(1);
        for i in 0..600u64 {
            s.apply_at(
                format!("k{i:04}").as_bytes(),
                Some(b"v"),
                vp(100 + i, 1),
                100 + i,
            )
            .unwrap();
            if i % 150 == 149 {
                s.flush().unwrap();
            }
        }
        let held = s
            .compact_plan(10_000)
            .unwrap()
            .expect("hold compact_in_flight");
        let held_segs = held.segments();
        let keys: Vec<(Vec<u8>, Version)> = (0..600u64)
            .map(|i| (format!("k{i:04}").into_bytes(), vp(100 + i, 1)))
            .collect();
        for _ in 0..20 {
            s.accumulate_forget(&keys, true);
            assert!(s.cleanup_forget_batch().unwrap().1.is_none());
            assert_eq!(s.pending_forget_len(), 0);
            assert_eq!(s.accumulating_forget.len(), keys.len(), "releases deduplicate while blocked");
        }
        s.free_compact_segs(&held_segs);
        let plan = s.cleanup_forget_batch().unwrap().1.expect("planning re-enabled");
        assert_eq!(s.pending_forget_len(), keys.len(), "one rewrite captures the whole cycle");
        let segs = plan.segments();
        s.abort_forget_plan(&segs);
    }

    #[test]
    fn cleanup_batch_cap_and_inflight_releases_form_separate_batches() {
        let mut s = sorted_store_big(1);
        let old = vp(100, 1);
        for key in [b"a", b"b", b"c"] {
            s.apply_at(key, Some(b"v"), old, 100).unwrap();
        }
        s.flush().unwrap();
        s.set_forget_batch_cap(130);
        s.accumulate_forget(&[(b"a".to_vec(), old)], false);
        assert!(s.cleanup_forget_batch().unwrap().1.is_none());
        s.accumulate_forget(&[(b"a".to_vec(), old)], false);
        assert!(s.cleanup_forget_batch().unwrap().1.is_none(), "duplicates do not reach cap");
        s.accumulate_forget(&[(b"b".to_vec(), old)], false);
        let first = s.cleanup_forget_batch().unwrap().1.expect("cap reached before wrap");
        assert_eq!(s.pending_forget_len(), 2);
        s.accumulate_forget(&[(b"a".to_vec(), old), (b"b".to_vec(), old), (b"c".to_vec(), old)], true);
        assert!(s.cleanup_forget_batch().unwrap().1.is_none(), "only one plan in flight");
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(first, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();
        assert!(s.get(b"a").unwrap().is_none());
        assert!(s.get(b"b").unwrap().is_none());
        assert!(s.get(b"c").unwrap().is_some());
        assert!(s.cleanup_forget_pending(), "commit preserves next batch");
        let next = s.cleanup_forget_batch().unwrap().1.expect("wrapped next batch plans without new releases");
        assert_eq!(s.pending_forget_len(), 1);
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(next, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();
        assert!(s.get(b"c").unwrap().is_none());
        assert!(!s.cleanup_forget_pending());
        assert_index_matches_engine(&s);
    }

    #[test]
    fn cleanup_batch_abort_retains_newer_inflight_releases() {
        let mut s = sorted_store_big(1);
        let old = vp(100, 1);
        let new = vp(200, 1);
        s.apply_at(b"race", Some(b"old"), old, 100).unwrap();
        s.apply_at(b"keep", Some(b"v"), old, 100).unwrap();
        s.flush().unwrap();
        s.accumulate_forget(&[(b"race".to_vec(), old)], true);
        let first = s.cleanup_forget_batch().unwrap().1.unwrap();
        s.apply_at(b"race", Some(b"new"), new, 200).unwrap();
        s.accumulate_forget(&[(b"race".to_vec(), new)], true);
        s.accumulate_forget(&[(b"race".to_vec(), old)], true);
        s.abort_forget_plan(&first.segments());
        assert!(s.cleanup_forget_pending(), "abort preserves next batch");
        s.flush().unwrap();
        let next = s.cleanup_forget_batch().unwrap().1.unwrap();
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(next, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();
        assert!(s.get(b"race").unwrap().is_none(), "dedup retained latest handed-off version");
        assert!(s.get(b"keep").unwrap().is_some());
        assert!(!s.cleanup_forget_pending());
        assert_index_matches_engine(&s);
    }

    #[test]
    fn cleanup_batch_placement_change_discards_accumulated_keys() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        s.apply_at(b"last-copy", Some(b"v"), version, 100).unwrap();
        s.flush().unwrap();
        s.set_cleanup_placement(CleanupPlacement { base: (1, 1, 1), map: None });
        s.accumulate_forget(&[(b"last-copy".to_vec(), version)], false);
        s.set_cleanup_placement(CleanupPlacement { base: (2, 1, 2), map: None });
        assert!(s.cleanup_forget_batch().unwrap().1.is_none());
        assert!(!s.cleanup_forget_pending());
        assert!(s.get(b"last-copy").unwrap().is_some());
        s.accumulate_forget(&[(b"last-copy".to_vec(), version)], false);
        s.reset_pending_forget();
        s.accumulate_forget(&[], true);
        assert!(s.cleanup_forget_batch().unwrap().1.is_none());
        assert!(s.get(b"last-copy").unwrap().is_some());
    }

    fn cleanup_token(seed: u8) -> Vec<u8> {
        crate::partition::opaque_placement_token([seed; crate::partition::OPAQUE_TOKEN_SEED_LEN])
    }

    fn cleanup_map(splits: &[&[u8]], tokens: &[u8], pending: &[Option<u8>]) -> RangeMap {
        RangeMap::from_opaque_parts(
            splits.iter().map(|s| s.to_vec()).collect(),
            tokens.iter().map(|&t| cleanup_token(t)).collect(),
            pending.iter().map(|p| p.map(cleanup_token)).collect(),
            vec![false; tokens.len()],
        )
        .unwrap()
    }

    fn cleanup_at(base: (u64, u64, u64), map: &RangeMap) -> CleanupPlacement {
        CleanupPlacement { base, map: Some(std::sync::Arc::new(map.clone())) }
    }

    #[test]
    fn cleanup_placement_changes_ignore_inherited_splits_and_find_moved_spans() {
        let base = (1, 7, 1);
        let map = cleanup_map(&[b"m"], &[1, 2], &[None, None]);
        let at = cleanup_at(base, &map);
        let mut split = map.clone();
        assert!(split.split(b"c") && split.split(b"p") && split.split(b"x"));
        assert!(at.unchanged_at(base, Some(&split)));
        assert!(at.changes_to((2, 7, 1), Some(&map)).is_none(), "membership invalidates everything");
        assert!(at.changes_to((1, 8, 1), Some(&map)).is_none(), "zone config invalidates everything");
        assert!(at.changes_to(base, None).is_none());
        let moved = cleanup_map(&[b"c", b"m", b"p", b"x"], &[1, 1, 2, 3, 2], &[None; 5]);
        assert_eq!(at.changes_to(base, Some(&moved)), Some(vec![(b"p".to_vec(), Some(b"x".to_vec()))]));
        let pending = cleanup_map(&[b"m"], &[1, 2], &[Some(3), None]);
        assert_eq!(at.changes_to(base, Some(&pending)), Some(vec![(Vec::new(), Some(b"m".to_vec()))]));
        let both = cleanup_map(&[b"m"], &[3, 4], &[None, None]);
        assert_eq!(at.changes_to(base, Some(&both)), Some(vec![(Vec::new(), None)]));
    }

    #[test]
    fn cleanup_batch_survives_inherited_splits_and_drops_only_keys_whose_range_moved() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        let keys: [&[u8]; 3] = [b"a-gone", b"m-gone", b"z-gone"];
        for key in keys {
            s.apply_at(key, Some(b"v"), version, 100).unwrap();
        }
        s.flush().unwrap();
        let base = (1, 7, 1);
        let map = cleanup_map(&[b"m"], &[1, 2], &[None, None]);
        s.set_cleanup_placement(cleanup_at(base, &map));
        let batch: Vec<(Vec<u8>, Version)> = keys.iter().map(|k| (k.to_vec(), version)).collect();
        s.accumulate_forget(&batch, false);
        s.accumulate_released(&[vals_store::range_tombstone::Fragment { lo: b"zz".to_vec(), hi: Some(b"zzz".to_vec()), version: 1 }]);
        let discards = crate::observe::counters().cleanup_forget_discards.get();
        let mut split = map.clone();
        for at in [b"c" as &[u8], b"p", b"q", b"r"] {
            assert!(split.split(at));
            s.set_cleanup_placement(cleanup_at(base, &split));
        }
        assert!(s.cleanup_forget_pending(), "inherited splits keep the batch");
        let moved = cleanup_map(&[b"c", b"m", b"p", b"q", b"r"], &[1, 1, 2, 2, 2, 5], &[None; 6]);
        s.set_cleanup_placement(cleanup_at(base, &moved));
        assert!(crate::observe::counters().cleanup_forget_discards.get() > discards);
        s.accumulate_forget(&[], true);
        let plan = s.cleanup_forget_batch().unwrap().1.expect("the unmoved keys still plan");
        assert!(s.forget_plan_placement_valid(base, Some(&moved)));
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();
        assert!(s.get(b"a-gone").unwrap().is_none());
        assert!(s.get(b"m-gone").unwrap().is_none());
        assert_eq!(s.get(b"z-gone").unwrap().as_deref(), Some(&b"v"[..]), "a key whose range moved is re-judged, not forgotten");
        assert!(!s.cleanup_forget_pending());
        assert_index_matches_engine(&s);
    }

    #[test]
    fn cleanup_released_piece_is_dropped_when_its_range_moves() {
        let mut s = sorted_store_big(1);
        let base = (1, 7, 1);
        let map = cleanup_map(&[b"m"], &[1, 2], &[None, None]);
        s.set_cleanup_placement(cleanup_at(base, &map));
        s.accumulate_released(&[vals_store::range_tombstone::Fragment { lo: b"n".to_vec(), hi: Some(b"o".to_vec()), version: 1 }]);
        let mut split = map.clone();
        assert!(split.split(b"n0"));
        s.set_cleanup_placement(cleanup_at(base, &split));
        assert!(s.cleanup_forget_pending());
        let mut back = split.clone();
        assert!(back.set_pending(2, cleanup_token(1)));
        s.set_cleanup_placement(cleanup_at(base, &back));
        assert!(!s.cleanup_forget_pending(), "a piece over a range we may own again is dropped");
    }

    #[test]
    fn cleanup_forget_plan_is_invalidated_only_by_placement_changes_over_its_keys() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        s.apply_at(b"keep", Some(b"v"), version, 100).unwrap();
        s.apply_at(b"z-gone", Some(b"v"), version, 100).unwrap();
        s.flush().unwrap();
        let base = (1, 7, 1);
        let map = cleanup_map(&[b"m"], &[1, 2], &[None, None]);
        s.set_cleanup_placement(cleanup_at(base, &map));
        s.accumulate_forget(&[(b"z-gone".to_vec(), version)], true);
        let plan = s.cleanup_forget_batch().unwrap().1.expect("plan");
        assert!(s.forget_plan_placement_valid(base, Some(&map)));
        let mut split = map.clone();
        assert!(split.split(b"c") && split.split(b"y"));
        assert!(s.forget_plan_placement_valid(base, Some(&split)), "inherited splits");
        let other = cleanup_map(&[b"c", b"m", b"y"], &[3, 1, 2, 2], &[None; 4]);
        assert!(s.forget_plan_placement_valid(base, Some(&other)), "a re-home of another range");
        let mut back = split.clone();
        assert!(back.set_pending(3, cleanup_token(1)));
        assert!(!s.forget_plan_placement_valid(base, Some(&back)), "a re-home back to this replica");
        let moved = cleanup_map(&[b"c", b"m", b"y"], &[1, 1, 2, 4], &[None; 4]);
        assert!(!s.forget_plan_placement_valid(base, Some(&moved)));
        assert!(!s.forget_plan_placement_valid((2, 7, 1), Some(&map)), "membership change");
        assert!(!s.forget_plan_placement_valid(base, None));
        s.abort_forget_plan(&plan.segments());
        assert!(!s.forget_plan_placement_valid(base, Some(&map)), "no plan in flight");
        assert_eq!(s.get(b"z-gone").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn cleanup_batch_exact_versions_preserve_newer_writes() {
        for flushed in [false, true] {
            let mut s = sorted_store_big(1);
            let old = vp(100, 1);
            s.apply_at(b"race", Some(b"old"), old, 100).unwrap();
            if flushed {
                s.flush().unwrap();
            }
            s.accumulate_forget(&[(b"race".to_vec(), old)], false);
            s.apply_at(b"race", Some(b"new"), vp(200, 1), 200).unwrap();
            s.accumulate_forget(&[], true);
            let (released, plan) = s.cleanup_forget_batch().unwrap();
            assert_eq!(released, 0);
            if let Some(plan) = plan {
                let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
                let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
                s.compact_commit(written).unwrap();
            }
            assert_eq!(s.get(b"race").unwrap().as_deref(), Some(&b"new"[..]));
            assert!(!s.cleanup_forget_pending());
            assert_index_matches_engine(&s);
        }
    }

    #[test]
    fn cleanup_batch_with_no_runs_revalidates_and_forgets_immediately() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        s.apply_at(b"drop", Some(b"v"), version, 100).unwrap();
        s.accumulate_forget(&[(b"drop".to_vec(), version)], false);
        let (released, plan) = s.cleanup_forget_batch().unwrap();
        assert_eq!(released, 1);
        assert!(plan.is_none());
        assert_eq!(s.run_count(), 0);
        assert!(s.get(b"drop").unwrap().is_none());
        assert!(!s.cleanup_forget_pending());
    }

    #[test]
    fn stale_placement_cleanup_rewrite_is_aborted_without_dropping_last_copy() {
        let mut s = sorted_store_big(1);
        let version = vp(100, 1);
        s.apply_at(b"newly-mine", Some(b"only-copy"), version, 100)
            .unwrap();
        s.flush().unwrap();

        let placement_a = (1, 1, 1, 1);
        let placement_b = (2, 1, 1, 2);
        let plan = s
            .forget_plan(&[(b"newly-mine".to_vec(), version)])
            .unwrap()
            .expect("old placement plans cleanup");
        let wdev = std::sync::Arc::new(s.device_sibling().expect("mem sibling"));
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();

        assert_ne!(placement_a, placement_b);
        let outputs = written.output_segments();
        s.abort_forget_plan(&outputs);
        assert!(!s.compact_in_flight(), "stale abort must re-arm compaction");
        assert_eq!(s.get(b"newly-mine").unwrap().as_deref(), Some(&b"only-copy"[..]));

        assert_eq!(s.current_version(b"newly-mine").unwrap(), Some(version));
    }

    #[test]
    fn lone_run_gc_not_due_when_disabled_or_no_tombstones() {
        let wall = 10_000_000u64;
        let mut off = sorted_store_big(1);
        off.apply_at(b"anchor", Some(b"a"), vp(wall, 1), wall)
            .unwrap();
        off.apply_at(b"aged", None, vp(wall - 5000, 2), wall)
            .unwrap();
        off.flush().unwrap();
        assert_eq!(off.run_count(), 1);
        assert!(!off.tombstone_gc_due(wall), "GC disabled → never due");
        assert!(
            off.compact_plan_tombstone_gc(wall).unwrap().is_none(),
            "GC disabled → no plan"
        );

        let mut clean = sorted_store_big(1).with_gc_grace_ms(1000);
        clean.apply_at(b"a", Some(b"1"), vp(wall, 1), wall).unwrap();
        clean.apply_at(b"b", Some(b"2"), vp(wall, 1), wall).unwrap();
        clean.flush().unwrap();
        assert_eq!(clean.run_count(), 1);
        assert!(!clean.tombstone_gc_due(wall), "no tombstones → never due");
    }

    #[test]
    fn forget_reclaims_flushed_keys_over_runs() {
        let mut s = sorted_store_big(1);
        for k in [b"a" as &[u8], b"b", b"c", b"d"] {
            s.apply(k, Some(b"v1"), v(1000, 1)).unwrap();
        }
        s.flush().unwrap();
        s.apply(b"e", Some(b"v1"), v(1001, 1)).unwrap();
        s.apply(b"f", Some(b"v1"), v(1002, 1)).unwrap();
        s.flush().unwrap();
        assert_eq!(s.run_count(), 2);
        let live_before = s.live_keys();

        let mut keys = std::collections::HashSet::new();
        keys.insert(b"b".to_vec());
        keys.insert(b"e".to_vec());
        let released = s.forget(&keys).unwrap();
        assert_eq!(released, 2, "both flushed keys were present and reclaimed");

        assert_eq!(s.get(b"b").unwrap(), None);
        assert_eq!(s.get(b"e").unwrap(), None);
        assert_eq!(
            s.current_version(b"b").unwrap(),
            None,
            "no lingering tombstone"
        );
        assert_eq!(s.current_version(b"e").unwrap(), None);
        assert_eq!(s.get(b"a").unwrap().as_deref(), Some(&b"v1"[..]));
        assert_eq!(s.get(b"f").unwrap().as_deref(), Some(&b"v1"[..]));
        assert_eq!(s.live_keys(), live_before - 2, "live count decremented");

        assert_index_matches_engine(&s);

        assert_eq!(
            s.apply(b"b", Some(b"back"), v(2000, 7)).unwrap(),
            Applied::Won
        );
        assert_eq!(s.get(b"b").unwrap().as_deref(), Some(&b"back"[..]));

        let mut none = std::collections::HashSet::new();
        none.insert(b"absent".to_vec());
        assert_eq!(s.forget(&none).unwrap(), 0);
    }

    #[test]
    fn randomized_interleavings_no_resurrection_and_leaf_consistency() {
        struct Rng(u64);
        impl Rng {
            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x >> 12;
                x ^= x << 25;
                x ^= x >> 27;
                self.0 = x;
                x.wrapping_mul(0x2545_F491_4F6C_DD1D)
            }
            fn below(&mut self, n: u64) -> u64 {
                self.next() % n
            }
        }

        let grace = 30u64;
        let mut s = sorted_store_big(1).with_gc_grace_ms(grace);
        let mut model: std::collections::HashMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::HashMap::new();
        let keyspace: Vec<Vec<u8>> = (0..8u32).map(|i| format!("k{i}").into_bytes()).collect();
        let mut rng = Rng(0xDEAD_BEEF_1234_5678);
        let mut clock = 1_000_000u64;

        for step in 0..500u64 {
            clock += 1 + rng.below(50);
            let wall = clock;
            let op = rng.below(100);
            if op < 40 {
                let k = keyspace[rng.below(8) as usize].clone();
                let val = format!("v{step}").into_bytes();
                if let Applied::Won = s.apply_at(&k, Some(&val), vp(clock, 1), wall).unwrap() {
                    model.insert(k, Some(val));
                }
            } else if op < 65 {
                let k = keyspace[rng.below(8) as usize].clone();
                if let Applied::Won = s.apply_at(&k, None, vp(clock, 1), wall).unwrap() {
                    model.insert(k, None);
                }
            } else if op < 75 {
                let k = keyspace[rng.below(8) as usize].clone();
                let val = b"straggler".to_vec();
                if let Applied::Won = s
                    .apply_at(&k, Some(&val), vp(clock.saturating_sub(10_000), 1), wall)
                    .unwrap()
                {
                    model.insert(k, Some(val));
                }
            } else if op < 85 {
                s.flush().unwrap();
            } else if op < 95 {
                s.compact_runs(wall).unwrap();
            } else {
                let k = keyspace[rng.below(8) as usize].clone();
                let mut set = std::collections::HashSet::new();
                set.insert(k.clone());
                s.forget(&set).unwrap();
                model.remove(&k);
            }

            for k in &keyspace {
                let expect = model.get(k).cloned().flatten();
                assert_eq!(
                    s.get(k).unwrap(),
                    expect,
                    "read mismatch for {k:?} at step {step}"
                );
            }
            assert_index_matches_engine(&s);
        }
    }

    #[test]
    fn live_keys_counts_new_puts_and_ignores_overwrites() {
        let mut s = store(1);
        assert_eq!(s.live_keys(), 0, "empty store");
        for i in 0..10u32 {
            s.apply(format!("k{i}").as_bytes(), Some(b"v"), v(100 + i as u64, 1))
                .unwrap();
        }
        assert_eq!(s.live_keys(), 10, "one per distinct new key");
        for i in 0..10u32 {
            s.apply(
                format!("k{i}").as_bytes(),
                Some(b"v2"),
                v(200 + i as u64, 1),
            )
            .unwrap();
        }
        assert_eq!(s.live_keys(), 10, "overwrites keep the count");
        s.apply(b"k0", Some(b"stale"), v(1, 1)).unwrap();
        assert_eq!(s.live_keys(), 10, "a lost write does not add a key");
    }

    #[test]
    fn live_keys_keeps_tombstones_resident() {
        let mut s = store(1);
        s.apply(b"a", Some(b"v"), v(100, 1)).unwrap();
        s.apply(b"b", Some(b"v"), v(100, 1)).unwrap();
        assert_eq!(s.live_keys(), 2);
        s.delete(b"a").unwrap();
        assert_eq!(s.live_keys(), 2, "tombstone-put keeps the key resident");
    }

    #[test]
    fn live_keys_exact_across_flush_and_run_resident_overwrite() {
        let mut s = sorted_store_big(7);
        for i in 0..8u32 {
            s.apply(
                format!("k{i}").as_bytes(),
                Some(b"v1"),
                v(100 + i as u64, 7),
            )
            .unwrap();
        }
        assert_eq!(s.live_keys(), 8);
        s.flush().unwrap();
        assert_eq!(
            s.engine().len(),
            0,
            "memtable emptied by flush — keys are run-resident"
        );
        assert_eq!(
            s.live_keys(),
            8,
            "flush is key-set-transparent: count unchanged"
        );
        s.apply(b"k0", Some(b"v2"), v(500, 7)).unwrap();
        assert_eq!(
            s.live_keys(),
            8,
            "overwriting a run-resident key does not add a key"
        );
        s.apply(b"fresh", Some(b"v"), v(600, 7)).unwrap();
        assert_eq!(s.live_keys(), 9, "a genuinely new key increments");
        s.flush().unwrap();
        assert_eq!(s.live_keys(), 9);
    }

    #[test]
    fn live_keys_decrements_on_gc_and_forget_and_zeros_on_clear() {
        let wall = 10_000_000u64;
        let mut s = store(1).with_gc_grace_ms(1000);
        s.apply_at(b"old", None, vp(wall - 5000, 1), wall - 5000).unwrap();
        s.advance_durable_gc_watermark(wall);
        s.apply_at(b"live", Some(b"v"), vp(wall, 1), wall).unwrap();
        s.apply_at(b"fresh", None, vp(wall - 100, 1), wall).unwrap();
        assert_eq!(s.live_keys(), 3, "puts and tombstones are all resident");

        let stats = s.compact(wall).unwrap();
        assert_eq!(stats.gc_dropped, 1);
        assert_eq!(s.live_keys(), 2, "GC'd tombstone retired from the count");

        let mut keys = std::collections::HashSet::new();
        keys.insert(b"live".to_vec());
        assert_eq!(s.forget(&keys).unwrap(), 1);
        assert_eq!(s.live_keys(), 1, "forgotten key retired from the count");

        s.clear().unwrap();
        assert_eq!(s.live_keys(), 0, "clear wipes the count");
    }

    #[test]
    fn live_keys_reseeds_on_reopen() {
        let dev = MemBlockDevice::new(BS, 8192);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        let expect;
        {
            let engine = Engine::create_prewiped(dev.share(), cfg);
            let mut s = VersionedStore::new(engine, 7).unwrap();
            for i in 0..12u32 {
                s.apply(format!("k{i}").as_bytes(), Some(b"v"), v(100 + i as u64, 7))
                    .unwrap();
            }
            s.flush().unwrap();
            for i in 12..18u32 {
                s.apply(format!("k{i}").as_bytes(), Some(b"v"), v(100 + i as u64, 7))
                    .unwrap();
            }
            s.delete(b"k0").unwrap();
            expect = s.live_keys();
            assert_eq!(expect, 18, "12 + 6 keys, delete keeps k0 resident");
            s.sync().unwrap();
        }
        let reopened = VersionedStore::open(dev, cfg, 7).unwrap();
        assert!(
            reopened.live_keys() >= expect,
            "unseeded live_keys is an upper bound"
        );
        reopened.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert_eq!(
            reopened.live_keys(),
            expect,
            "first AE leaf touch reseeds the exact resident-key count from side-data"
        );
    }

    struct CountingDevice {
        inner: MemBlockDevice,
        reads: std::sync::Arc<AtomicU64>,
    }
    impl BlockDevice for CountingDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.reads.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }
    impl vals_spdk::AsyncRead for CountingDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.read_at(lba, buf)
        }
    }

    #[test]
    fn cleanup_run_pages_seek_and_read_bounded_blocks() {
        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = CountingDevice {
            inner: MemBlockDevice::new(BS, 32768),
            reads: reads.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 256,
                reserved_blocks: 0,
                ordering: Ordering::Unsorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        const N: usize = 20_000;
        const PAGE: usize = 64;
        for i in 0..N {
            s.apply_at(
                format!("k{i:06}").as_bytes(),
                Some(b"v"),
                vp(100 + i as u64, 1),
                100 + i as u64,
            )
            .unwrap();
        }
        s.flush().unwrap();
        reads.store(0, AtomicOrdering::Relaxed);
        let store = std::cell::RefCell::new(s);
        let dev = store.borrow().device_handle();
        let mut cursor: Option<Vec<u8>> = None;
        let mut total = 0;
        let mut pages = 0;
        loop {
            let staged = store
                .borrow()
                .stage_scan_page(cursor.as_deref(), None, PAGE, usize::MAX)
                .unwrap();
            let mut merge = match staged {
                vals_store::StagedScanPage::Done(done) => {
                    let (page, more) = store.borrow().finish_cleanup_page(done).unwrap();
                    total += page.len();
                    cursor = page.last().map(|(k, _)| k.clone());
                    pages += 1;
                    if !more { break }
                    continue;
                }
                vals_store::StagedScanPage::Runs(m) => m,
            };
            let before = reads.load(AtomicOrdering::Relaxed);
            loop {
                match vals_rt::block_on(merge.step(&dev), || {}).unwrap() {
                    vals_store::StagedScanStep::Done => break,
                    vals_store::StagedScanStep::NeedMem { from } => {
                        store.borrow().scan_supply_mem(&mut merge, &from).unwrap();
                    }
                    vals_store::StagedScanStep::NeedBlocks { cursor } => {
                        store.borrow().scan_supply_blocks(&mut merge, cursor).unwrap();
                    }
                }
            }
            let (page, more) = store
                .borrow()
                .finish_cleanup_page(merge.finish())
                .unwrap();
            let page_reads = reads.load(AtomicOrdering::Relaxed) - before;
            assert!(page_reads <= 8, "page decoded too many run blocks: {page_reads}");
            assert!(page.len() <= PAGE);
            total += page.len();
            cursor = page.last().map(|(k, _)| k.clone());
            pages += 1;
            if !more { break }
        }
        assert_eq!(total, N);
        assert!(pages > 20, "fixture must require many resumed pages");
        assert!(
            reads.load(AtomicOrdering::Relaxed) < (pages * 8) as u64,
            "paging must seek, not re-read/decode every run prefix"
        );
    }

    struct FailableDevice {
        inner: MemBlockDevice,
        fail: std::sync::Arc<AtomicBool>,
    }
    impl BlockDevice for FailableDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.fail.load(AtomicOrdering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other("injected read fault")));
            }
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }
    }
    impl vals_spdk::AsyncRead for FailableDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.read_at(lba, buf)
        }
    }

    #[test]
    fn streaming_seed_legacy_v2_runs_merge_exactly() {
        use vals_store::alloc::{Allocator, SegmentId};
        use vals_store::run::{build_run_versioned, legacy_fixture::build_run_v2, write_run, RUN_SIDECAR_VERSION, RUN_VERSION};
        use vals_store::vlog::ValuePtr;
        use vals_store::{Manifest, RunBuildOpts, RunMeta, SnapshotEntry};

        for ordering in [Ordering::Sorted, Ordering::Unsorted] {
            let mem = MemBlockDevice::new(BS, 32768);
            let cfg = Config { seg_blocks: 32, reserved_blocks: 0, ordering };
            let mut alloc = Allocator::new(mem.num_blocks(), cfg.seg_blocks, cfg.reserved_blocks);
            let opts = RunBuildOpts { block_target_bytes: 512, bloom_bits_per_key: 10, num_buckets: 32 };
            let mut runs = Vec::new();
            for round in 0..4u64 {
                let entries: Vec<_> = (round * 100..round * 100 + 250).map(|i| SnapshotEntry {
                    key: format!("legacy-{i:04}").into_bytes(),
                    ptr: ValuePtr { segment: SegmentId(0), offset: 0, len: 0 },
                    seq: round * 250 + i,
                    version: pack_version(v(1000 + round * 1000 + i, 7)),
                    tombstone: true,
                }).collect();
                let bytes = match round {
                    0 | 1 => build_run_v2(&entries, opts, |key| VersionIndex::slot(key) as u32),
                    2 => build_run_versioned(&entries, opts, RUN_SIDECAR_VERSION),
                    _ => build_run_versioned(&entries, opts, RUN_VERSION),
                };
                let handle = write_run(&mem, &mut alloc, &bytes).unwrap();
                runs.push(RunMeta {
                    handle, min_key: entries.first().unwrap().key.clone(),
                    max_key: entries.last().unwrap().key.clone(), entry_count: entries.len() as u64,
                    level: 0, value_segs: Some(Vec::new()), value_bytes: Some(0),
                    tombstone_min_version: Some(entries[0].version), value_seg_bytes: Some(Vec::new()),
                });
            }
            let manifest = Manifest { generation: 1, covered_seq: 1299, covered_epoch: 1, runs };
            vals_store::write_manifest(&mem, &mut alloc, &manifest, 1, None).unwrap();
            let mut s = VersionedStore::open(mem.share(), cfg, 7).unwrap();
            let handles = s.engine().run_handles();
            assert!(s.engine().run_version_stream(&handles[0]).unwrap().is_none());
            assert!(matches!(s.engine().run_version_stream(&handles[2]).unwrap(),
                Some(vals_store::run::RunVersionStream::Sidecar(_))));
            assert!(matches!(s.engine().run_version_stream(&handles[3]).unwrap(),
                Some(vals_store::run::RunVersionStream::Blocks(_))));
            let mut pager = s.engine().run_sidecar_versions(&handles[0]).unwrap().unwrap();
            let mut legacy_order = Vec::new();
            pager.fold_page(usize::MAX, |key, _| legacy_order.push(key.to_vec())).unwrap();
            assert!(legacy_order.windows(2).any(|w| w[0] > w[1]));
            s.set_logical_range_map(1, RangeMap::from_splits([b"legacy-0225".to_vec()]));
            assert!(!s.seed_step().unwrap());
            assert_eq!(s.apply(b"legacy-0001", Some(b"new"), v(9000, 7)).unwrap(), Applied::Won);
            let mut steps = 0;
            while !s.seed_step().unwrap() {
                steps += 1;
                assert!(steps < 10_000);
            }
            assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&s, INDEX_BUCKETS));
            assert_eq!(s.live_keys(), 550);
            s.invalidate_leaf_seed();
            s.ensure_seeded().unwrap();
            assert_eq!(s.logical_range_counts(1), Some(vec![225, 325]));
            assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&s, INDEX_BUCKETS));
        }
    }

    #[test]
    fn streaming_seed_exact_overlaps_journal_and_rehome() {
        for ordering in [Ordering::Sorted, Ordering::Unsorted] {
            let mem = MemBlockDevice::new(BS, 32768);
            let cfg = Config { seg_blocks: 32, reserved_blocks: 0, ordering };
            let mut s = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
            for round in 0..2 {
                for i in 0..250 {
                    let key = format!("key-{i:04}");
                    let value = if i % 73 == 0 { None } else { Some(b"v".as_slice()) };
                    let stamp = 1000 + round * 1000 + i;
                    assert_eq!(s.apply_at(key.as_bytes(), value, vp(stamp, 7), stamp).unwrap(), Applied::Won);
                }
                s.flush().unwrap();
            }
            assert!(s.engine().run_count() >= 2);
            s.sync().unwrap();
            drop(s);
            let mut s = VersionedStore::open(mem.share(), cfg, 7).unwrap();
            for i in 250..310 {
                let stamp = 3000 + i;
                assert_eq!(s.apply_at(format!("key-{i:04}").as_bytes(), Some(b"v"), vp(stamp, 7), stamp).unwrap(), Applied::Won);
            }
            let ring = crate::ring::Ring::from_members(2, 64, [7, 8, 9]);
            let mut config = epoch_zone_config();
            config.rf = 2;
            let mut map = RangeMap::from_splits([b"key-0125".to_vec()]);
            map.enable_opaque_tokens();
            for seed in 1..=32 {
                let mut candidate = map.clone();
                assert!(candidate.set_opaque_pending(1, [seed; 16]));
                assert!(candidate.commit_pending(1));
                if candidate.replicas_for_key_while_rehoming(&ring, 42, b"key-0050", 2, None)
                    != candidate.replicas_for_key_while_rehoming(&ring, 42, b"key-0200", 2, None)
                {
                    map = candidate;
                    break;
                }
            }
            assert_ne!(
                map.replicas_for_key_while_rehoming(&ring, 42, b"key-0050", 2, None),
                map.replicas_for_key_while_rehoming(&ring, 42, b"key-0200", 2, None),
            );
            s.prepare_zone_seed((1, 1, 1), &ring, 42, Some(config.clone()), Some(map.clone()));
            assert!(!s.seed_step().unwrap());
            let retries = crate::observe::snapshot().ae_seed_projection_retries;
            assert_eq!(s.apply_at(b"key-0001", Some(b"new"), vp(9000, 7), 9000).unwrap(), Applied::Won);
            assert_eq!(s.apply_at(b"key-0309", None, vp(9001, 7), 9001).unwrap(), Applied::Won);
            assert_eq!(s.apply_at(b"key-0400", Some(b"new"), vp(9002, 7), 9002).unwrap(), Applied::Won);
            s.applied_high_ms.set(0);
            let mut moved = map.clone();
            assert!(moved.split(b"key-0180"));
            assert!(moved.set_opaque_pending(2, [99; 16]));
            s.prepare_zone_seed((2, 2, 2), &ring, 42, Some(config), Some(moved.clone()));
            let mut steps = 0;
            while !s.seed_step().unwrap() {
                steps += 1;
                assert!(steps < 10_000);
            }
            assert!(crate::observe::snapshot().ae_seed_projection_retries > retries);
            let mut expected = std::collections::HashMap::<Box<[NodeId]>, Box<[u64]>>::new();
            let mut buckets = vec![0u64; INDEX_BUCKETS as usize];
            let mut counts = vec![0u64; moved.num_ranges()];
            let mut high = 0;
            for (key, raw) in s.engine().recovered_versions().unwrap() {
                let group = s.group_of(&key).into_boxed_slice();
                let b = VersionIndex::slot(&key);
                expected.entry(group).or_insert_with(VersionIndex::empty_group)[b] ^=
                    crate::antientropy::entry_digest(&key, unpack_version(raw));
                buckets[b] += 1;
                counts[moved.range_index(&key)] += 1;
                high = high.max(unpack_version(raw).physical_ms());
            }
            assert!(expected.len() > 1, "fixture must exercise multiple placement groups");
            assert_eq!(*s.versions.leaves.borrow(), expected);
            assert_eq!(s.versions.buckets.borrow().iter().map(|m| m.logical_keys).collect::<Vec<_>>(), buckets);
            assert!(s.versions.dirty.borrow().is_empty());
            assert_eq!(s.versions.live.get(), counts.iter().sum());
            assert_eq!(s.logical_range_counts(2), Some(counts));
            assert_eq!(s.applied_high_ms.get(), high);
        }
    }

    fn seed_run_backed_store(mem: &MemBlockDevice, cfg: &Config, runs: u32, per_run: u32) {
        let mut s = VersionedStore::new(Engine::create_prewiped(mem.share(), *cfg), 7).unwrap();
        for run in 0..runs {
            for i in 0..per_run {
                let k = format!("k{run:02}{i:03}");
                let stamp = 1000 + (run * per_run + i) as u64;
                s.apply(k.as_bytes(), Some(b"v"), v(stamp, 7)).unwrap();
            }
            s.flush().unwrap();
        }
        s.sync().unwrap();
        assert!(s.engine().run_count() >= 2, "expected multiple runs");
    }

    fn recompute_leaves(s: &VersionedStore<impl BlockDevice>, num_buckets: u32) -> Vec<u64> {
        let live: Vec<(Vec<u8>, Version)> = s
            .engine()
            .recovered_versions()
            .unwrap()
            .into_iter()
            .map(|(k, raw)| (k.to_vec(), unpack_version(raw)))
            .collect();
        crate::antientropy::leaf_digests_versioned(
            live.iter().map(|(k, ver)| (&k[..], *ver)),
            num_buckets,
        )
    }

    #[test]
    fn lazy_boot_defers_sidecar_pass_until_first_leaf_touch() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);

        let reads = std::sync::Arc::new(AtomicU64::new(0));
        let dev = CountingDevice {
            inner: mem.share(),
            reads: reads.clone(),
        };
        let store = VersionedStore::open(dev, cfg, 7).unwrap();
        assert!(
            !store.versions.seeded.get(),
            "boot must not seed the leaf index (no O(dataset) sidecar pass)"
        );
        let after_open = reads.load(AtomicOrdering::Relaxed);

        let digests = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert!(store.versions.seeded.get(), "first touch seeds the index");
        let touch_reads = reads.load(AtomicOrdering::Relaxed) - after_open;
        assert!(
            touch_reads > 0,
            "the first leaf touch performs the deferred sidecar reads"
        );
        assert_eq!(
            digests,
            recompute_leaves(&store, INDEX_BUCKETS),
            "lazy seed diverged from the from-scratch recompute"
        );

        let before = reads.load(AtomicOrdering::Relaxed);
        let _ = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert_eq!(
            reads.load(AtomicOrdering::Relaxed),
            before,
            "a seeded index serves digests with no device reads"
        );
    }

    #[test]
    fn unseeded_window_mutations_match_recompute_after_first_touch() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 40);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        assert!(
            !store.versions.seeded.get(),
            "reopen leaves the index unseeded"
        );

        store.apply(b"k00000", Some(b"v2"), v(9000, 7)).unwrap();
        store.apply(b"brandnew", Some(b"v"), v(9100, 7)).unwrap();
        store.delete_at(b"k00001", 9200).unwrap();
        let mut forget = std::collections::HashSet::new();
        forget.insert(b"k00002".to_vec());
        store.forget(&forget).unwrap();
        assert!(
            !store.versions.seeded.get(),
            "mutations skip maintenance — still unseeded until the first leaf touch"
        );

        let digests = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert_eq!(
            digests,
            recompute_leaves(&store, INDEX_BUCKETS),
            "seed after unseeded-window mutations diverged from the recompute"
        );
        let exact = store.engine().recovered_versions().unwrap().len() as u64;
        assert_eq!(store.live_keys(), exact, "live_keys exact after seeding");
    }

    #[test]
    fn lazy_seed_failure_is_fail_closed() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 40);

        let fail = std::sync::Arc::new(AtomicBool::new(false));
        let dev = FailableDevice {
            inner: mem.share(),
            fail: fail.clone(),
        };
        let store = VersionedStore::open(dev, cfg, 7).unwrap();
        assert!(!store.versions.seeded.get());

        fail.store(true, AtomicOrdering::Relaxed);
        assert!(
            store.version_leaf_digests(INDEX_BUCKETS).is_err(),
            "a sidecar read error surfaces as Err"
        );
        assert!(
            !store.versions.seeded.get(),
            "a failed seed leaves the index unseeded for retry (never zeroed digests)"
        );

        fail.store(false, AtomicOrdering::Relaxed);
        let digests = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert!(
            store.versions.seeded.get(),
            "retry seeds after the fault clears"
        );
        assert_eq!(
            digests,
            recompute_leaves(&store, INDEX_BUCKETS),
            "post-fault seed diverged from the recompute"
        );
    }

    #[test]
    fn cooperative_seed_matches_recompute_with_mid_seed_mutations() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        assert!(!store.is_leaf_seeded());
        for i in 0..(SEED_MEMORY_OPS_PER_STEP + 17) {
            let key = format!("mem-{i:05}");
            store
                .apply(key.as_bytes(), Some(b"v"), v(8000 + i as u64, 7))
                .unwrap();
        }

        assert!(!store.seed_step().unwrap(), "start step is not the last");
        assert!(!store.is_leaf_seeded(), "still unseeded mid-seed");

        store
            .apply(b"mem-00000", Some(b"v2"), v(8999, 7))
            .unwrap();
        store.apply(b"k00000", Some(b"v2"), v(9000, 7)).unwrap();
        store.apply(b"brandnew", Some(b"v"), v(9100, 7)).unwrap();
        store.delete_at(b"k00001", 9200).unwrap();
        let mut forget = std::collections::HashSet::new();
        forget.insert(b"k00002".to_vec());
        store.forget(&forget).unwrap();

        let mut steps = 0;
        loop {
            let done = store.seed_step().unwrap();
            steps += 1;
            if steps == 2 {
                store.apply(b"k05010", Some(b"v9"), v(9300, 7)).unwrap();
            }
            if done {
                break;
            }
        }
        assert!(store.is_leaf_seeded(), "seed completes");

        assert_eq!(
            store.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&store, INDEX_BUCKETS),
            "cooperative seed with mid-seed mutations diverged from the recompute"
        );
        let exact = store.engine().recovered_versions().unwrap().len() as u64;
        assert_eq!(
            store.live_keys(),
            exact,
            "live_keys exact after cooperative seed with mid-seed mutations"
        );
    }

    #[test]
    fn stream_multi_budget_insert_retire_readd() {
        if crate::testutil::isolated_env_test(&[]) { return; }

        let mem = MemBlockDevice::new(BS, 1 << 16);
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        let mut store = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
        for i in 0..320 {
            let key = format!("k{i:05}");
            assert_eq!(store.apply(key.as_bytes(), Some(b"v"), v(1000 + i, 7)).unwrap(), Applied::Won);
        }
        let ring = crate::ring::Ring::from_members(2, 64, [7, 8, 9]);
        let placement_ring = ring.clone();
        store.set_placement(std::sync::Arc::new(move |key| placement_ring.replicas(key)));
        store.invalidate_leaf_seed();
        let map = RangeMap::from_splits([
            b"journal-new-00100".to_vec(), b"k00000".to_vec(),
        ]);
        store.set_logical_range_map(1, map.clone());
        assert!(!store.seed_step().unwrap());

        let ops = SEED_MEMORY_OPS_PER_STEP * 3 + 17;
        for i in 0..ops {
            let key = format!("journal-new-{i:05}");
            assert_eq!(store.apply(key.as_bytes(), Some(b"one"), v(1_000_000 + i as u64, 7)).unwrap(), Applied::Won);
        }
        assert_eq!(store.apply(b"k00000", Some(b"new"), v(2_000_000, 7)).unwrap(), Applied::Won);
        let epoch = store.engine().runs_removed_epoch();
        let retire = std::collections::HashSet::from([
            b"journal-new-00003".to_vec(), b"journal-new-00103".to_vec(),
        ]);
        assert_eq!(store.forget(&retire).unwrap(), 2);
        assert_eq!(store.engine().runs_removed_epoch(), epoch);
        for (i, key) in [b"journal-new-00003".as_slice(), b"journal-new-00103"].iter().enumerate() {
            assert_eq!(store.apply(key, Some(b"readd"), v(2_000_001 + i as u64, 7)).unwrap(), Applied::Won);
        }

        let before = crate::observe::snapshot().ae_seed_journal_ops;
        let mut turns = 0;
        let mut replay_turns = 0;
        let mut replayed = 0;
        loop {
            let done = store.seed_step().unwrap();
            turns += 1;
            assert!(turns < 10_000);
            let total = crate::observe::snapshot().ae_seed_journal_ops - before;
            assert!(total - replayed <= SEED_MEMORY_OPS_PER_STEP as u64);
            replay_turns += usize::from(total > replayed);
            replayed = total;
            if done { break; }
        }
        assert!(replay_turns >= 4);
        assert!(crate::observe::snapshot().ae_seed_journal_ops - before >= (ops + 5) as u64);
        let versions = store.engine().recovered_versions().unwrap();
        let mut leaves = std::collections::HashMap::<Box<[NodeId]>, Box<[u64]>>::new();
        let mut buckets = vec![0u64; INDEX_BUCKETS as usize];
        let mut counts = vec![0u64; map.num_ranges()];
        for (key, raw) in &versions {
            let b = VersionIndex::slot(key);
            leaves.entry(store.group_of(key).into_boxed_slice())
                .or_insert_with(VersionIndex::empty_group)[b] ^=
                crate::antientropy::entry_digest(key, unpack_version(*raw));
            buckets[b] += 1;
            counts[map.range_index(key)] += 1;
        }
        assert!(leaves.len() > 1);
        assert!(counts.iter().all(|count| *count > 0));
        assert_eq!(*store.versions.leaves.borrow(), leaves);
        assert_eq!(store.versions.buckets.borrow().iter().map(|m| m.logical_keys).collect::<Vec<_>>(), buckets);
        assert!(store.versions.dirty.borrow().is_empty());
        assert_eq!(store.versions.live.get(), versions.len() as u64);
        assert_eq!(store.logical_range_counts(1), Some(counts));
        assert!(store.versions.journal.borrow().is_empty());
        assert_eq!(store.versions.journal.borrow().capacity(), 0);
    }

    #[test]
    fn streaming_seed_journal_retire_is_exact() {
        let mut s = sorted_store_big(7);
        let retired = vp(200, 7);
        let kept = vp(100, 7);
        assert_eq!(s.apply_at(b"a-retired", None, retired, 200).unwrap(), Applied::Won);
        assert_eq!(s.apply_at(b"z-kept", Some(b"v"), kept, 100).unwrap(), Applied::Won);
        s.set_logical_range_map(1, RangeMap::from_splits([b"m".to_vec()]));
        assert!(!s.seed_step().unwrap());
        let group = s.group_of(b"a-retired");
        s.retire_winner(b"a-retired", &group, retired);
        while !s.seed_step().unwrap() {}
        let expected = crate::antientropy::entry_digest(b"z-kept", kept);
        let b = VersionIndex::slot(b"z-kept");
        assert_eq!(s.versions.leaves.borrow().values().next().unwrap()[b], expected);
        assert_eq!(s.versions.live.get(), 1);
        assert_eq!(s.logical_range_counts(1), Some(vec![0, 1]));
        assert!(s.applied_high_ms.get() >= retired.physical_ms());
    }

    #[test]
    fn streaming_seed_corrupt_sidecar_never_publishes() {
        struct CorruptRead {
            inner: MemBlockDevice,
            armed: std::sync::Arc<AtomicBool>,
        }
        impl BlockDevice for CorruptRead {
            fn block_size(&self) -> usize { self.inner.block_size() }
            fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
            fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
                self.inner.read_at(lba, buf)?;
                if self.armed.load(AtomicOrdering::Relaxed) && buf.len() > 16 {
                    let mid = buf.len() / 2;
                    buf[mid] ^= 1;
                }
                Ok(())
            }
            fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.inner.write_at(lba, buf) }
            fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
        }
        use vals_store::run::{with_run_write_version, RunVersionStream, RUN_SIDECAR_VERSION, RUN_VERSION};
        for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
            let mem = MemBlockDevice::new(BS, 1 << 16);
            let cfg = Config { seg_blocks: 64, reserved_blocks: 0, ordering: Ordering::Sorted };
            with_run_write_version(version, || seed_run_backed_store(&mem, &cfg, 2, 100));
            let armed = std::sync::Arc::new(AtomicBool::new(false));
            let dev = CorruptRead { inner: mem.share(), armed: armed.clone() };
            let _limit = seed_permit::limit_for_test(1, 0);
            let store = VersionedStore::open(dev, cfg, 7).unwrap();
            let handles = store.engine().run_handles();
            let stream = store.engine().run_version_stream(&handles[0]).unwrap().unwrap();
            assert_eq!(matches!(stream, RunVersionStream::Blocks(_)), version == RUN_VERSION);
            drop(stream);
            assert!(!store.seed_step().unwrap());
            assert_eq!(seed_permit::active_for_test(), 1);
            let corrupt_before = crate::observe::counters().ae_seed_corrupt_sidecars.get();
            armed.store(true, AtomicOrdering::Relaxed);
            let mut failed = false;
            for _ in 0..100 {
                if matches!(store.seed_step(), Err(EngineError::Snapshot(vals_store::SnapshotError::Corrupt(_)))) {
                    failed = true;
                    break;
                }
            }
            assert!(failed, "version {version}");
            assert!(crate::observe::counters().ae_seed_corrupt_sidecars.get() > corrupt_before);
            assert_eq!(seed_permit::active_for_test(), 0);
            assert!(!store.is_leaf_seeded());
            assert!(store.versions.leaves.borrow().is_empty());
            armed.store(false, AtomicOrdering::Relaxed);
            while !store.seed_step().unwrap() {}
            assert_eq!(seed_permit::active_for_test(), 0);
            assert_eq!(store.version_leaf_digests(INDEX_BUCKETS).unwrap(), recompute_leaves(&store, INDEX_BUCKETS));
        }
    }

    #[test]
    fn cooperative_seed_is_fail_closed_mid_seed() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 40);

        let fail = std::sync::Arc::new(AtomicBool::new(false));
        let dev = FailableDevice {
            inner: mem.share(),
            fail: fail.clone(),
        };
        let _limit = seed_permit::limit_for_test(1, 0);
        let store = VersionedStore::open(dev, cfg, 7).unwrap();

        assert!(!store.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        fail.store(true, AtomicOrdering::Relaxed);
        let mut saw_error = false;
        for _ in 0..10 {
            if store.seed_step().is_err() {
                saw_error = true;
                break;
            }
        }
        assert!(saw_error, "a mid-seed sidecar read error surfaces as Err");
        assert_eq!(seed_permit::active_for_test(), 0);
        assert!(
            !store.is_leaf_seeded(),
            "a failed seed leaves the index unseeded (never zeroed digests)"
        );
        assert!(
            store.version_leaf_digests(INDEX_BUCKETS).is_err(),
            "a serve during the fault still errs, never zeroed digests"
        );
        assert!(!store.is_leaf_seeded());

        fail.store(false, AtomicOrdering::Relaxed);
        let digests = store.version_leaf_digests(INDEX_BUCKETS).unwrap();
        assert!(store.is_leaf_seeded(), "retry seeds after the fault clears");
        assert_eq!(
            digests,
            recompute_leaves(&store, INDEX_BUCKETS),
            "post-fault seed diverged from the recompute"
        );
    }

    #[test]
    fn cooperative_seed_carries_its_pinned_runs_across_a_run_set_change() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let epoch_before = store.engine().runs_removed_epoch();

        assert!(!store.seed_step().unwrap());
        assert!(!store.seed_step().unwrap());

        store.compact_runs(0).unwrap();
        assert_eq!(
            store.engine().run_count(),
            1,
            "compaction merged to one run"
        );
        assert!(
            store.engine().runs_removed_epoch() > epoch_before,
            "compaction bumps the removal epoch"
        );

        while !store.seed_step().unwrap() {}
        assert!(store.is_leaf_seeded());
        let [carries, merge_restarts, cap, revoked] = store.seed_run_causes();
        assert!(carries >= 1, "the pinned seed carried the merge");
        assert_eq!((merge_restarts, cap, revoked), (0, 0, 0), "and did not restart");
        assert_eq!(
            store.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&store, INDEX_BUCKETS),
            "seed after a run-set change diverged from the recompute"
        );
    }

    #[test]
    fn cooperative_seed_bounded_restart_falls_back_to_sync() {
        let mem = MemBlockDevice::new(BS, 1 << 16);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);
        let _unpinned = seed_policy::pin_cap_for_test(0);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let before = crate::observe::snapshot();

        let mut steps = 0u32;
        let mut stamp = 500_000u64;
        loop {
            let done = store.seed_step().unwrap();
            steps += 1;
            assert!(
                steps < 200,
                "bounded-restart guard never fired — the cooperative seed livelocked"
            );
            if done {
                break;
            }
            stamp += 1;
            store
                .apply(format!("churn{stamp}").as_bytes(), Some(b"v"), v(stamp, 7))
                .unwrap();
            store.flush().unwrap();
            store.compact_runs(0).unwrap();
        }

        let after = crate::observe::snapshot();
        assert!(
            store.is_leaf_seeded(),
            "seed completed via the sync fallback"
        );
        assert!(
            after.ae_seed_sync_fallbacks - before.ae_seed_sync_fallbacks >= 1,
            "the bounded-restart guard should have tripped the sync fallback at least once"
        );
        assert!(
            after.ae_seed_restarts - before.ae_seed_restarts >= 1,
            "continuous churn should have driven at least one restart"
        );
        assert_eq!(
            store.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&store, INDEX_BUCKETS),
            "seed after the bounded fallback diverged from the recompute"
        );
    }

    #[test]
    fn cooperative_seed_without_fallback_stays_fail_closed_under_run_churn() {
        let mem = MemBlockDevice::new(BS, 1 << 16);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);
        let _unpinned = seed_policy::pin_cap_for_test(0);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let mut steps = 0u32;
        let mut stamp = 600_000u64;
        while store.seed_restart_count.get() <= MAX_SEED_RESTARTS {
            assert!(!store.seed_step_cooperative().unwrap());
            steps += 1;
            assert!(steps < 200, "run churn did not exercise the restart cap");
            stamp += 1;
            store
                .apply(format!("churn{stamp}").as_bytes(), Some(b"v"), v(stamp, 7))
                .unwrap();
            store.flush().unwrap();
            store.compact_runs(0).unwrap();
        }

        assert!(
            !store.is_leaf_seeded(),
            "the cancellable accounting path must not enter synchronous fallback"
        );
    }

    #[test]
    fn cooperative_seed_sheds_unpublished_state_under_ram_pressure() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 80);
        let store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let counter_before = crate::observe::snapshot().ae_seed_ram_sheds;
        let map = RangeMap::single();
        prepare_epoch_zone(&store, 10, &map);

        {
            let _headroom = crate::ram::override_for_test(0, u64::MAX);
            assert!(!store.seed_step().unwrap());
            assert!(!store.seed_step().unwrap());
            let progress = store.seed_progress.borrow();
            assert!(progress.as_ref().unwrap().estimated_bytes > 0);
        }
        {
            let _pressure = crate::ram::override_for_test(2, 1);
            assert!(!store.seed_step().unwrap());
        }
        assert!(store.seed_progress.borrow().is_none());
        assert_eq!(
            crate::observe::snapshot().ae_seed_ram_sheds,
            counter_before + 1
        );
        prepare_epoch_zone(&store, 11, &map);
        assert!(!store.is_leaf_seeded());
        assert_eq!(store.logical_range_counts(11), None);
        assert!(store.seed_progress.borrow().is_none());

        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        while !store.seed_step().unwrap() {}
        assert!(store.is_leaf_seeded());
        assert!(store.seed_progress.borrow().is_none());
    }

    #[test]
    fn seed_permit_defers_and_releases_on_shed_restart_completion_and_drop() {
        let _limit = seed_permit::limit_for_test(1, 20);
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        seed_run_backed_store(&mem, &cfg, 4, 80);
        let first = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let second = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        assert!(!first.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        assert!(!second.seed_step().unwrap());
        assert!(second.seed_deferred());
        assert!(second.seed_progress.borrow().is_none());
        first.restart_or_fallback(false).unwrap();
        assert_eq!(seed_permit::active_for_test(), 0);
        assert!(!second.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        {
            let _pressure = crate::ram::override_for_test(2, 1);
            assert!(!second.seed_step().unwrap());
            assert!(second.seed_deferred());
        }
        assert_eq!(seed_permit::active_for_test(), 0);
        assert!(!first.seed_step().unwrap());
        assert!(first.seed_deferred());
        std::thread::sleep(std::time::Duration::from_millis(25));
        assert!(!first.seed_step().unwrap());
        while !first.seed_step().unwrap() {}
        assert_eq!(seed_permit::active_for_test(), 0);
        assert!(!second.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        drop(second);
        assert_eq!(seed_permit::active_for_test(), 0);
    }

    #[test]
    fn seed_failed_reservation_growth_sheds_and_releases_permit() {
        const CHILD: &str = "VALS_SEED_GROW_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "replica::tests::seed_failed_reservation_growth_sheds_and_releases_permit"])
                .env(CHILD, "1").status().unwrap();
            assert!(status.success());
            return;
        }
        let _limit = seed_permit::limit_for_test(1, 0);
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        let _capacity = crate::ram_broker::override_for_test(Some(u64::MAX), 4 * 1024 * 1024);
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        seed_run_backed_store(&mem, &cfg, 2, 80);
        let store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        assert!(!store.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        let bytes = store.seed_progress.borrow().as_ref().unwrap().reservation.bytes();
        let _small = crate::ram_broker::override_for_test(Some(u64::MAX), bytes);
        let staged = crate::observe::gauges().ae_seed_staged_bytes.get();
        let estimated = store.seed_progress.borrow().as_ref().unwrap().estimated_bytes;
        assert!(!store.seed_progress.borrow_mut().as_mut().unwrap().try_adjust_estimate(0, bytes + 1));
        assert_eq!(store.seed_progress.borrow().as_ref().unwrap().estimated_bytes, estimated);
        assert_eq!(crate::observe::gauges().ae_seed_staged_bytes.get(), staged);
        assert!(!store.seed_step().unwrap());
        assert!(store.seed_progress.borrow().is_none());
        assert_eq!(crate::observe::gauges().ae_seed_staged_bytes.get(), 0);
        assert_eq!(seed_permit::active_for_test(), 0);
    }

    #[test]
    fn stream_seed_permit_projection_retry_and_completion() {
        let _limit = seed_permit::limit_for_test(1, 0);
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        seed_run_backed_store(&mem, &cfg, 4, 80);
        let store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        assert!(!store.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 1);
        let sync = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        sync.ensure_seeded().unwrap();
        assert!(sync.is_leaf_seeded());
        assert_eq!(seed_permit::active_for_test(), 1);
        store.set_placement(std::sync::Arc::new(|_| vec![7]));
        assert!(!store.seed_step().unwrap());
        assert_eq!(seed_permit::active_for_test(), 0);
        while !store.seed_step().unwrap() {}
        assert_eq!(seed_permit::active_for_test(), 0);
        assert!(store.is_leaf_seeded());
    }

    #[test]
    fn seed_permit_sync_ensure_seeded_bypasses_cap() {
        let _limit = seed_permit::limit_for_test(1, 0);
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted };
        seed_run_backed_store(&mem, &cfg, 4, 80);
        let first = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let second = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        assert!(!first.seed_step().unwrap());
        second.ensure_seeded().unwrap();
        assert!(second.is_leaf_seeded());
        assert_eq!(seed_permit::active_for_test(), 1);
        first.seed_restart_count.set(MAX_SEED_RESTARTS);
        assert!(first.restart_or_fallback(true).unwrap());
        assert!(first.is_leaf_seeded());
        assert_eq!(seed_permit::active_for_test(), 0);
    }

    #[test]
    fn cooperative_seed_counters_track_progress() {
        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 4, 40);

        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let run_count = store.engine().run_count() as u64;
        let before = crate::observe::snapshot();

        assert!(!store.seed_step().unwrap());
        store.apply(b"mid1", Some(b"v"), v(60_000, 7)).unwrap();
        store.apply(b"mid2", Some(b"v"), v(60_001, 7)).unwrap();
        while !store.seed_step().unwrap() {}

        let after = crate::observe::snapshot();
        assert!(store.is_leaf_seeded());
        assert!(
            after.ae_seed_steps - before.ae_seed_steps >= run_count,
            "expected at least one seed step per run (runs {run_count})"
        );
        assert!(
            after.ae_seed_journal_ops - before.ae_seed_journal_ops >= 2,
            "the two mid-seed mutations should have been journaled and replayed"
        );
        assert!(
            after.ae_seed_projection_ops > before.ae_seed_projection_ops,
            "the canonical winners should have been projected"
        );
        assert_eq!(
            store.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&store, INDEX_BUCKETS),
            "seed with mid-seed journaled mutations diverged from the recompute"
        );
    }

    #[test]
    fn cooperative_seed_yields_between_run_steps() {
        use crate::reactor::poll_io_nonblocking;
        use std::cell::Cell;
        use std::rc::Rc;
        use vals_rt::LocalExecutor;

        let mem = MemBlockDevice::new(BS, 1 << 15);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, 8, 40);

        let store = std::cell::RefCell::new(VersionedStore::open(mem.share(), cfg, 7).unwrap());
        let run_count = store.borrow().engine().run_count();

        let ex = LocalExecutor::new();
        const WITNESS_TURNS: u64 = 10_000_000;
        let counter = Rc::new(Cell::new(0u64));
        {
            let counter = counter.clone();
            ex.spawn(async move {
                for _ in 0..WITNESS_TURNS {
                    counter.set(counter.get() + 1);
                    vals_rt::yield_now().await;
                }
            });
        }

        let witness_at_done = Cell::new(0u64);
        ex.run_until(
            async {
                ensure_seeded_cooperative_cell(&store).await.unwrap();
                witness_at_done.set(counter.get());
            },
            poll_io_nonblocking,
        );

        assert!(store.borrow().is_leaf_seeded(), "seed completed");
        assert!(
            witness_at_done.get() as usize >= run_count,
            "serving witness advanced at least once per run step while the seed was in \
             flight (got {}, runs {run_count}) — the seed yielded between chunks",
            witness_at_done.get()
        );
    }

    #[test]
    #[ignore = "measurement: prints cooperative seed resident scaling and placement-churn retention"]
    fn cooperative_seed_resident_measurement() {
        for per_run in [2_000u32, 8_000] {
            const RUNS: u32 = 4;
            let mem = MemBlockDevice::new(BS, 1 << 22);
            let cfg = Config {
                seg_blocks: 32,
                reserved_blocks: 0,
                ordering: Ordering::Sorted,
            };
            seed_run_backed_store(&mem, &cfg, RUNS, per_run);
            let store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
            let rss_before = crate::observe::process_resident_bytes().unwrap_or(0);
            assert!(!store.seed_step().unwrap());
            while !store.stream_seed.borrow().as_ref().unwrap().pending.is_empty() {
                assert!(!store.seed_step().unwrap());
            }
            assert!(!store.seed_step().unwrap());
            let staged = store
                .seed_progress
                .borrow()
                .as_ref()
                .unwrap()
                .estimated_bytes;
            let rss_delta = crate::observe::process_resident_bytes()
                .unwrap_or(0)
                .saturating_sub(rss_before);
            let mut churn_floor = staged;
            for generation in 0..16u32 {
                store.set_placement(std::sync::Arc::new(move |_| vec![7, generation]));
                store.invalidate_leaf_seed();
                assert!(!store.seed_step().unwrap());
                assert!(!store.seed_step().unwrap());
                churn_floor = churn_floor.min(
                    store
                        .seed_progress
                        .borrow()
                        .as_ref()
                        .unwrap()
                        .estimated_bytes,
                );
            }
            eprintln!(
                "seed resident measurement: keys={} estimated_bytes={staged} rss_delta={rss_delta} churn_floor={churn_floor}",
                RUNS * per_run
            );
        }
    }

    #[test]
    #[ignore = "measurement: prints one-shot vs per-step seed durations, no assert"]
    fn cooperative_seed_step_budget_measurement() {
        const RUNS: u32 = 4;
        const PER_RUN: u32 = 50_000;
        let mem = MemBlockDevice::new(BS, 1 << 20);
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        seed_run_backed_store(&mem, &cfg, RUNS, PER_RUN);

        let oneshot = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let t0 = std::time::Instant::now();
        oneshot.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let oneshot_dur = t0.elapsed();

        let coop = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        let mut steps = 0u32;
        let mut max_step = std::time::Duration::ZERO;
        loop {
            let t = std::time::Instant::now();
            let done = coop.seed_step().unwrap();
            max_step = max_step.max(t.elapsed());
            steps += 1;
            if done {
                break;
            }
        }
        assert_eq!(
            coop.version_leaf_digests(INDEX_BUCKETS).unwrap(),
            recompute_leaves(&coop, INDEX_BUCKETS)
        );
        eprintln!(
            "seed measurement: runs={RUNS} one_shot={oneshot_dur:?} \
             steps={steps} max_step={max_step:?}"
        );
    }

    #[test]
    fn cooperative_seed_matches_per_key_placement_oracle_across_runs_and_ranges() {
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        let mem = MemBlockDevice::new(BS, 1 << 16);
        let cfg = Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted };
        let key = |i: u32| format!("k{:04}", (i * 37) % 600).into_bytes();
        {
            let mut s = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
            let mut stamp = 1000;
            for i in 0..12_000u32 {
                s.apply(format!("k9{i:05}").as_bytes(), Some(b"v"), v(500 + i as u64, 7)).unwrap();
            }
            s.flush().unwrap();
            for run in 0..6u32 {
                for i in (run * 90)..(run * 90 + 160) {
                    stamp += 1;
                    let value = (i % 11 != 0).then_some(b"v".as_slice());
                    s.apply(&key(i), value, v(stamp, 7)).unwrap();
                }
                s.flush().unwrap();
            }
            for i in 500..560 {
                stamp += 1;
                s.apply(&key(i), Some(b"m"), v(stamp, 7)).unwrap();
            }
            s.sync().unwrap();
            assert!(s.engine().run_count() >= 4);
        }
        let mut store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
        for i in 560..620 {
            store.apply(&key(i), Some(b"n"), v(5000 + i as u64, 7)).unwrap();
        }
        let ring = crate::ring::Ring::from_members(2, 16, [1, 2, 3, 7]);
        let zone = crate::zone::ZoneConfig {
            name: "seed-oracle".into(),
            rf: 2,
            ordering: crate::zone::Ordering::Sorted,
            scope: None,
        };
        let mut map = RangeMap::from_splits((1..24u32).map(|r| format!("k{:04}", r * 25).into_bytes()));
        assert!(map.set_pending(5, b"k0025".to_vec()));
        assert!(map.set_pending(17, Vec::new()));
        store.prepare_zone_seed((1, 1, 1), &ring, 42, Some(zone.clone()), Some(map.clone()));
        assert!(store.placement_ranges.borrow().is_some());
        let folds = store.seed_test_counts().1;
        while !store.seed_step().unwrap() {}

        let mut groups: std::collections::HashMap<Vec<NodeId>, Vec<u64>> = std::collections::HashMap::new();
        let mut counts = vec![0u64; map.num_ranges()];
        let mut distinct = std::collections::HashSet::new();
        for (k, raw) in store.engine().recovered_versions().unwrap() {
            let mut group = map.replicas_for_key_while_rehoming(&ring, 42, &k, 2, None);
            group.sort_unstable();
            distinct.insert(group.clone());
            groups.entry(group).or_insert_with(|| vec![0; INDEX_BUCKETS as usize])[VersionIndex::slot(&k)] ^=
                crate::antientropy::entry_digest(&k, unpack_version(raw));
            counts[map.range_index(&k)] += 1;
        }
        assert!(distinct.len() >= 3, "the oracle spans several replica groups");
        assert_eq!(store.seed_test_counts().1 - folds, counts.iter().sum::<u64>(), "every seeded key is counted once");
        let seeded: std::collections::HashMap<Vec<NodeId>, Vec<u64>> = store
            .versions
            .leaves
            .borrow()
            .iter()
            .map(|(group, leaves)| (group.to_vec(), leaves.to_vec()))
            .collect();
        assert_eq!(seeded, groups);
        assert_eq!(store.logical_range_counts(1), Some(counts));
    }

    #[test]
    #[ignore = "measurement (0422): cooperative seed ns per seeded key by run count, ranged placement, reservation floor above the window churn as in production; run with --release"]
    fn cooperative_seed_merge_cost_measurement() {
        const KEYS: u32 = 400_000;
        const RANGES: usize = 2048;
        let _headroom = crate::ram::override_for_test(0, u64::MAX);
        let key_of = |i: u32| {
            let did = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 8;
            format!("did:plc:{did:014x}/app.bsky.feed.like/3l{i:011}").into_bytes()
        };
        let mut sorted: Vec<Vec<u8>> = (0..KEYS).map(key_of).collect();
        sorted.sort_unstable();
        let map = RangeMap::from_splits(
            (1..RANGES).map(|r| sorted[r * KEYS as usize / RANGES].clone()),
        );
        drop(sorted);
        let ring = crate::ring::Ring::from_members(1, 128, [7]);
        let zone = crate::zone::ZoneConfig {
            name: "seed-bench".into(),
            rf: 1,
            ordering: crate::zone::Ordering::Sorted,
            scope: None,
        };
        let repeats: usize = std::env::var("VALS_SEED_BENCH_REPEATS").ok().and_then(|v| v.parse().ok()).unwrap_or(5);
        for runs in [4u32, 12, 24, 40] {
            let mem = MemBlockDevice::new(4096, 1 << 17);
            let cfg = Config { seg_blocks: 256, reserved_blocks: 0, ordering: Ordering::Sorted };
            {
                let mut s = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 7).unwrap();
                for run in 0..runs {
                    for i in (run..KEYS).step_by(runs as usize) {
                        s.apply(&key_of(i), Some(b"v"), v(1000 + i as u64, 7)).unwrap();
                    }
                    s.flush().unwrap();
                }
                s.sync().unwrap();
                assert_eq!(s.engine().run_count(), runs as usize);
            }
            let mut best = f64::MAX;
            for _ in 0..repeats {
                let store = VersionedStore::open(mem.share(), cfg, 7).unwrap();
                store.prepare_zone_seed((1, 1, 1), &ring, 42, Some(zone.clone()), Some(map.clone()));
                let t0 = std::time::Instant::now();
                assert!(!store.seed_step().unwrap());
                store.seed_progress.borrow_mut().as_mut().unwrap().reservation_floor = 1 << 30;
                while !store.seed_step().unwrap() {}
                let ns = t0.elapsed().as_nanos() as f64;
                assert_eq!(store.logical_range_counts(1).map(|c| c.iter().sum::<u64>()), Some(KEYS as u64));
                best = best.min(ns / KEYS as f64);
            }
            eprintln!("seed merge measurement: runs={runs} keys={KEYS} ranges={RANGES} best_of={repeats} ns_per_key={best:.1}");
        }
    }

    #[test]
    fn compact_retains_tombstones_when_gc_disabled() {
        let wall = 10_000_000u64;
        let mut s = store(1);
        s.apply_at(b"old", None, vp(wall - 5_000_000, 1), wall)
            .unwrap();
        let stats = s.compact(wall).unwrap();
        assert_eq!(stats.gc_dropped, 0);
        assert!(
            s.get_versioned(b"old").unwrap().is_some(),
            "tombstone retained when GC disabled"
        );
    }

    #[test]
    fn compact_does_not_gc_tombstones_during_a_write_idle_window() {
        let t = 10_000_000u64;
        let mut s = store(1).with_gc_grace_ms(1000);
        s.apply_at(b"k", Some(b"v"), vp(t - 10, 1), t).unwrap();
        s.apply_at(b"k", None, vp(t, 1), t).unwrap();
        assert_eq!(s.high_water_ms(), t);

        let stats = s.compact(t + 1_000_000).unwrap();
        assert_eq!(stats.gc_dropped, 0, "idle window: tombstone must not age");
        assert!(
            s.get_versioned(b"k").unwrap().is_some(),
            "tombstone retained while the watermark is frozen at T"
        );
    }

    type GcCompact = fn(&mut VersionedStore<MemBlockDevice>, u64) -> Result<usize, EngineError>;

    const GC_COMPACTIONS: [(&str, GcCompact); 2] = [
        ("compact", |s, wall| s.compact(wall).map(|stats| stats.gc_dropped)),
        ("compact_step", |s, wall| {
            let mut dropped = 0;
            loop {
                let (stats, done) = s.compact_step(wall, 2)?;
                dropped += stats.gc_dropped;
                if done {
                    return Ok(dropped);
                }
            }
        }),
    ];

    #[test]
    fn compact_never_uses_an_unpersisted_gc_cutoff() {
        let t = 10_000_000u64;
        for (what, compact) in GC_COMPACTIONS {
            let mut s = store(1).with_gc_grace_ms(1000);
            s.apply_at(b"k", None, vp(t, 1), t).unwrap();
            for i in 0..20u64 {
                s.apply_at(format!("other{i:02}").as_bytes(), Some(&[b'x'; 200]), vp(t + 2000, 1), t + 2000)
                    .unwrap();
            }

            assert_eq!(compact(&mut s, t + 2000).unwrap(), 0, "{what}");
            assert!(s.get_versioned(b"k").unwrap().is_some(), "{what}");

            s.advance_durable_gc_watermark(t + 2000);
            assert_eq!(compact(&mut s, t + 2000).unwrap(), 1, "{what}");
            assert_eq!(s.get_versioned(b"k").unwrap(), None, "{what}");
        }
    }

    #[test]
    fn gc_compaction_never_lets_an_old_value_back_in_after_a_restart() {
        let t = 10_000_000u64;
        let grace = 1000;
        let cfg = Config { seg_blocks: 4, reserved_blocks: 0, ordering: Ordering::Unsorted };
        for (what, compact) in GC_COMPACTIONS {
            let mem = MemBlockDevice::new(BS, 256);
            let mut s = VersionedStore::new(Engine::create_prewiped(mem.share(), cfg), 1)
                .unwrap()
                .with_gc_grace_ms(grace);
            s.apply_at(b"k", Some(b"old"), vp(t - 10, 1), t).unwrap();
            s.apply_at(b"k", None, vp(t, 1), t).unwrap();
            let moved: std::collections::HashSet<Vec<u8>> =
                (0..20u64).map(|i| format!("moved{i:02}").into_bytes()).collect();
            for key in &moved {
                s.apply_at(key, Some(&[b'x'; 200]), vp(t + 2000, 1), t + 2000).unwrap();
            }
            s.advance_durable_gc_watermark(t);
            assert_eq!(
                s.apply_at(b"k", Some(b"old"), vp(t - 10, 1), t + 2000).unwrap(),
                Applied::Rejected,
                "{what}: the live floor (high water - grace) refuses the old value before the restart"
            );

            compact(&mut s, t + 2000).unwrap();
            assert!(
                s.get_versioned(b"k").unwrap().is_some(),
                "{what}: a tombstone above the persisted cutoff survives GC"
            );
            assert_eq!(s.forget(&moved).unwrap(), moved.len());
            s.sync().unwrap();
            drop(s);

            let mut reopened = VersionedStore::open(mem.share(), cfg, 1).unwrap().with_gc_grace_ms(grace);
            reopened.advance_durable_gc_watermark(t);
            reopened.apply_at(b"k", Some(b"old"), vp(t - 10, 1), t + 2000).unwrap();
            assert_eq!(reopened.get(b"k").unwrap(), None, "{what}: the deleted value stays deleted after the restart");
        }
    }

    #[test]
    fn compact_gcs_tombstones_once_data_progress_passes_the_grace() {
        let t = 10_000_000u64;
        let mut s = store(1).with_gc_grace_ms(1000);
        s.apply_at(b"k", None, vp(t, 1), t).unwrap();
        s.apply_at(b"other", Some(b"x"), vp(t + 2000, 1), t + 2000)
            .unwrap();
        assert_eq!(s.high_water_ms(), t + 2000);
        s.advance_durable_gc_watermark(t + 2000);

        let stats = s.compact(t + 2000).unwrap();
        assert_eq!(stats.gc_dropped, 1, "aged tombstone collected");
        assert_eq!(s.get_versioned(b"k").unwrap(), None);
        assert!(s.get_versioned(b"other").unwrap().is_some());
    }

    #[test]
    fn scan_is_ordered_versioned_and_surfaces_tombstones() {
        let mut s = sorted_store(1);
        s.put_at(b"banana", b"yellow", 1000).unwrap();
        s.put_at(b"apple", b"red", 1001).unwrap();
        s.put_at(b"cherry", b"dark", 1002).unwrap();
        s.put_at(b"date", b"brown", 1003).unwrap();
        let tomb_ver = s.delete_at(b"cherry", 1004).unwrap();

        let all = s.scan(None, None).unwrap();
        let keys: Vec<&[u8]> = all.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(keys, vec![&b"apple"[..], b"banana", b"cherry", b"date"]);
        let cherry = all.iter().find(|(k, _)| k == b"cherry").unwrap();
        assert_eq!(cherry.1.value, None, "tombstone surfaced");
        assert_eq!(cherry.1.version, tomb_ver);
        let apple = all.iter().find(|(k, _)| k == b"apple").unwrap();
        assert_eq!(apple.1.value.as_deref(), Some(&b"red"[..]));

        let mid = s.scan(Some(b"banana"), Some(b"date")).unwrap();
        let mid_keys: Vec<&[u8]> = mid.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(mid_keys, vec![&b"banana"[..], b"cherry"]);
    }

    #[test]
    fn scan_rejects_unsorted_zone() {
        let s = store(1);
        assert!(matches!(s.scan(None, None), Err(EngineError::NotSorted)));
    }

    #[test]
    fn scan_page_resumes_past_the_last_key() {
        let mut s = sorted_store(1);
        for (i, k) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            s.put_at(k.as_bytes(), b"v", 1000 + i as u64).unwrap();
        }
        s.delete_at(b"c", 2000).unwrap();

        let mut pages = Vec::new();
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let (page, more) = s.scan_page(cursor.as_deref(), None, 2, usize::MAX).unwrap();
            assert!(page.len() <= 2);
            pages.extend(page);
            if !more {
                break;
            }
            let mut next = pages.last().unwrap().0.clone();
            next.push(0);
            cursor = Some(next);
        }
        assert_eq!(pages, s.scan(None, None).unwrap());

        let (page, more) = s.scan_page(None, None, usize::MAX, 1).unwrap();
        assert_eq!(page.len(), 1);
        assert!(more);
    }

    #[test]
    fn bucket_digest_page_is_key_ordered_and_resumable() {
        let mut s = sorted_store(1);
        for (i, k) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            s.put_at(k.as_bytes(), b"v", 1000 + i as u64).unwrap();
        }
        s.delete_at(b"d", 2000).unwrap();
        let all_ids: Vec<u32> = (0..8).collect();

        let expect: Vec<(Vec<u8>, Version)> = {
            let mut e: Vec<(Vec<u8>, Version)> = ["a", "b", "c", "d", "e"]
                .iter()
                .map(|k| {
                    (
                        k.as_bytes().to_vec(),
                        s.version(k.as_bytes()).unwrap().unwrap(),
                    )
                })
                .collect();
            e.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            e
        };

        let (page, more) = s.bucket_digest_page(8, &all_ids, None, usize::MAX).unwrap();
        assert!(!more);
        assert_eq!(page, expect);

        let mut paged = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            let (page, more) = s
                .bucket_digest_page(8, &all_ids, after.as_deref(), 1)
                .unwrap();
            assert_eq!(page.len(), 1);
            after = Some(page.last().unwrap().0.clone());
            paged.extend(page);
            if !more {
                break;
            }
        }
        assert_eq!(paged, expect);

        let some_ids = [crate::antientropy::bucket_of(b"b", 8)];
        let (page, _) = s
            .bucket_digest_page(8, &some_ids, None, usize::MAX)
            .unwrap();
        assert!(page.iter().any(|(k, _)| k == b"b"));
        assert!(page
            .iter()
            .all(|(k, _)| crate::antientropy::bucket_of(k, 8) == some_ids[0]));
    }

    #[test]
    fn bucket_digest_page_merges_keyspace_in_order() {
        let mut s = sorted_store(1);
        let mut keys: Vec<Vec<u8>> = (0..200u32)
            .map(|i| format!("key{i:04}").into_bytes())
            .collect();
        for (i, k) in keys.iter().enumerate() {
            s.put_at(k, b"v", 1000 + i as u64).unwrap();
        }
        keys.sort_unstable();
        let nb = crate::antientropy::DEFAULT_BUCKETS;
        let all_ids: Vec<u32> = (0..nb).collect();

        for budget in [40usize, 115, 512] {
            let mut paged: Vec<Vec<u8>> = Vec::new();
            let mut after: Option<Vec<u8>> = None;
            loop {
                let (page, more) = s
                    .bucket_digest_page(nb, &all_ids, after.as_deref(), budget)
                    .unwrap();
                assert!(
                    !page.is_empty(),
                    "budget={budget}: every page makes progress"
                );
                assert!(
                    page.windows(2).all(|w| w[0].0 < w[1].0),
                    "budget={budget}: page is ascending"
                );
                after = Some(page.last().unwrap().0.clone());
                paged.extend(page.into_iter().map(|(k, _)| k));
                if !more {
                    break;
                }
            }
            assert_eq!(
                paged, keys,
                "budget={budget}: resumed pages reassemble the full sorted keyspace"
            );
        }
    }

    #[test]
    fn scoped_leaf_digests_fold_and_scope_correctly() {
        use crate::antientropy::{bucket_of, root};
        use std::collections::HashSet;
        let mut s = sorted_store(1);
        for i in 0..500u32 {
            s.put_at(format!("k{i:04}").as_bytes(), b"v", 1000 + i as u64)
                .unwrap();
        }
        let l = INDEX_BUCKETS;

        for level in [16u32, 256, l] {
            let scoped = s
                .scoped_leaf_digests(level, 1, &HashSet::from([0]), None)
                .unwrap();
            let ids: Vec<u32> = scoped.iter().map(|(id, _)| *id).collect();
            assert_eq!(ids, (0..level).collect::<Vec<_>>(), "all ids, in order");
            let flat = s.version_leaf_digests(level).unwrap();
            assert_eq!(
                scoped.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
                flat,
                "root-round fold matches version_leaf_digests at level {level}"
            );
        }
        let finest = s
            .scoped_leaf_digests(l, 1, &HashSet::from([0]), None)
            .unwrap();
        assert_eq!(
            root(&finest.iter().map(|(_, d)| *d).collect::<Vec<_>>()),
            root(&s.version_leaf_digests(l).unwrap())
        );

        let full256 = s.version_leaf_digests(256).unwrap();
        let scope: HashSet<u32> = HashSet::from([3, 11]);
        let scoped = s.scoped_leaf_digests(256, 16, &scope, None).unwrap();
        assert!(
            scoped.iter().all(|(id, _)| scope.contains(&(id % 16))),
            "only in-scope buckets returned"
        );
        assert_eq!(
            scoped.len(),
            2 * (256 / 16),
            "two subtrees × 16 children each"
        );
        for (id, d) in &scoped {
            assert_eq!(
                *d, full256[*id as usize],
                "scoped digest == unrestricted fold"
            );
        }

        let kb = bucket_of(b"k0042", 256);
        let parent = kb % 16;
        let scoped = s
            .scoped_leaf_digests(256, 16, &HashSet::from([parent]), None)
            .unwrap();
        assert!(scoped.iter().any(|(id, _)| *id == kb));
    }

    #[test]
    fn version_survives_reopen() {
        let path = temp_path("reopen");
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let ver = {
            let dev = FileBlockDevice::create(&path, BS, 256).unwrap();
            let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
            let ver = s.put_at(b"k", b"durable", 5000).unwrap();
            assert_ne!(ver.node >> 32, 0, "new writers occupy the legacy-free range");
            s.sync().unwrap();
            ver
        };

        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
        let stored = s.get_versioned(b"k").unwrap().unwrap();
        assert_eq!(stored.version, ver);
        assert_eq!(stored.value.as_deref(), Some(&b"durable"[..]));
        let after_restart = s.put_at(b"other", b"rollback", 4000).unwrap();
        assert_ne!(after_restart.node, ver.node);
        assert_ne!(after_restart, ver);
    }

    #[test]
    fn synced_blind_candidates_survive_drop_and_reopen_without_shutdown() {
        let path = temp_path("blind-reopen");
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Sorted,
        };
        {
            let dev = FileBlockDevice::create(&path, BS, 512).unwrap();
            let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
            assert_eq!(s.apply_at(b"k", Some(b"winner"), v(20, 1), 0).unwrap(), Applied::Won);
            s.flush().unwrap();
            assert_eq!(
                s.apply_blind_at(b"k", Some(b"stale"), v(10, 1), 0).unwrap(),
                Applied::Won
            );
            s.sync().unwrap();
            drop(s);
        }
        {
            let dev = FileBlockDevice::open(&path, BS).unwrap();
            let mut s = VersionedStore::open(dev, cfg, 1).unwrap();
            let stored = s.get_versioned(b"k").unwrap().unwrap();
            assert_eq!(stored.version, v(20, 1));
            assert_eq!(stored.value.as_deref(), Some(&b"winner"[..]));
            assert_eq!(s.apply_blind_at(b"k", None, v(30, 1), 0).unwrap(), Applied::Won);
            s.sync().unwrap();
            drop(s);
        }
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let s = VersionedStore::open(dev, cfg, 1).unwrap();
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(30, 1)));
        assert_eq!(s.get(b"k").unwrap(), None);
    }

    fn assert_index_matches_engine<D: BlockDevice>(s: &VersionedStore<D>) {
        use crate::antientropy::{leaf_digests_versioned, DEFAULT_BUCKETS};
        let live: Vec<(Vec<u8>, Version)> = s
            .engine()
            .recovered_versions()
            .unwrap()
            .into_iter()
            .map(|(k, raw)| (k.to_vec(), unpack_version(raw)))
            .collect();
        for (k, ver) in &live {
            assert_eq!(
                s.current_version(k).unwrap(),
                Some(*ver),
                "current_version disagrees with the engine for {k:?}"
            );
        }
        let recomputed =
            leaf_digests_versioned(live.iter().map(|(k, v)| (&k[..], *v)), DEFAULT_BUCKETS);
        assert_eq!(
            s.version_leaf_digests(DEFAULT_BUCKETS).unwrap(),
            recomputed,
            "leaf accumulator diverged from the engine"
        );
    }

    #[test]
    fn version_index_tracks_every_mutation() {
        let mut s = store(1);
        s.put_at(b"a", b"1", 1000).unwrap();
        assert_index_matches_engine(&s);
        assert_eq!(s.apply(b"b", Some(b"x"), v(2000, 5)).unwrap(), Applied::Won);
        assert_index_matches_engine(&s);
        let a_tomb = s.delete_at(b"a", 1001).unwrap();
        assert_index_matches_engine(&s);
        s.cas_at(b"c", b"y", None, 1002).unwrap();
        assert_index_matches_engine(&s);

        let before = s.current_version(b"b").unwrap();
        assert!(matches!(
            s.apply(b"b", Some(b"old"), v(1, 5)).unwrap(),
            Applied::Lost(_)
        ));
        assert_eq!(s.current_version(b"b").unwrap(), before, "Lost left it");
        assert_index_matches_engine(&s);

        let mut drop = std::collections::HashSet::new();
        drop.insert(b"c".to_vec());
        s.forget(&drop).unwrap();
        assert_eq!(s.current_version(b"c").unwrap(), None);
        assert_index_matches_engine(&s);

        let mut s = s.with_gc_grace_ms(10);
        s.put_at(b"z", b"later", a_tomb.physical_ms() + 12).unwrap();
        s.advance_durable_gc_watermark(a_tomb.physical_ms() + 12);
        assert_index_matches_engine(&s);
        s.compact(a_tomb.physical_ms() + 11).unwrap();
        assert_eq!(
            s.current_version(b"a").unwrap(),
            None,
            "aged tombstone GC'd"
        );
        assert_index_matches_engine(&s);
    }

    #[test]
    fn placement_change_reseeds_without_stale_group_residue() {
        use crate::antientropy::{leaf_digests_versioned, DEFAULT_BUCKETS};
        use crate::ring::Ring;
        use std::collections::HashSet;

        fn wire(s: &VersionedStore<MemBlockDevice>, ring: &Ring) {
            let r = ring.clone();
            let f: PlacementFn = std::sync::Arc::new(move |k: &[u8]| r.replicas(k));
            s.set_placement(f);
            s.invalidate_leaf_seed();
        }

        let mut s = store(1);
        let ring_a = Ring::from_members(2, 64, [1, 2, 3]);
        wire(&s, &ring_a);
        for i in 0..250u32 {
            s.put_at(format!("k{i:04}").as_bytes(), b"v", 1000 + i as u64)
                .unwrap();
        }
        assert_index_matches_engine(&s);

        let ring_b = Ring::from_members(2, 64, [1, 2, 3, 4]);
        wire(&s, &ring_b);
        let _ = s.version_leaf_digests(DEFAULT_BUCKETS).unwrap();
        for i in 250..350u32 {
            s.put_at(format!("k{i:04}").as_bytes(), b"v", 2000 + i as u64)
                .unwrap();
        }
        s.put_at(b"k0000", b"v2", 5000).unwrap();
        assert_index_matches_engine(&s);

        let live: Vec<(Vec<u8>, Version)> = s
            .engine()
            .recovered_versions()
            .unwrap()
            .into_iter()
            .map(|(k, raw)| (k.to_vec(), unpack_version(raw)))
            .collect();
        let shared: Vec<(&[u8], Version)> = live
            .iter()
            .filter(|(k, _)| {
                let r = ring_b.replicas(k);
                r.contains(&1) && r.contains(&4)
            })
            .map(|(k, v)| (&k[..], *v))
            .collect();
        let want = leaf_digests_versioned(shared.iter().copied(), DEFAULT_BUCKETS);
        let scoped = s
            .scoped_leaf_digests(DEFAULT_BUCKETS, 1, &HashSet::from([0]), Some(4))
            .unwrap();
        let got: Vec<u64> = scoped.iter().map(|(_, d)| *d).collect();
        assert_eq!(
            got, want,
            "peer-scoped fold matches a from-scratch recompute over the shared keyset"
        );
    }

    #[test]
    fn incremental_leaves_match_recompute() {
        use crate::antientropy::{leaf_digests_versioned, DEFAULT_BUCKETS};
        let recompute = |s: &VersionedStore<MemBlockDevice>, n| {
            let live = s
                .engine()
                .recovered_versions()
                .unwrap()
                .into_iter()
                .map(|(k, raw)| (k.to_vec(), unpack_version(raw)));
            leaf_digests_versioned(
                live.collect::<Vec<_>>().iter().map(|(k, v)| (&k[..], *v)),
                n,
            )
        };
        let mut s = sorted_store_big(1).with_gc_grace_ms(10);
        let mut state = 0x9e3779b97f4a7c15u64;
        let mut next = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let mut phys = 1_000_000u64;
        for step in 0..4000u64 {
            phys += 1;
            let key = format!("k{:03}", next() % 250);
            match next() % 8 {
                0 => {
                    s.delete_at(key.as_bytes(), phys).unwrap();
                }
                _ => {
                    s.put_at(key.as_bytes(), b"v", phys).unwrap();
                }
            }
            if step % 900 == 0 && step != 0 {
                s.flush().unwrap();
                s.compact(phys + 1000).unwrap();
            }
            if step % 200 == 0 {
                assert_eq!(
                    s.version_leaf_digests(DEFAULT_BUCKETS).unwrap(),
                    recompute(&s, DEFAULT_BUCKETS),
                    "maintained leaves diverged at step {step}"
                );
                assert_eq!(
                    s.version_leaf_digests(DEFAULT_BUCKETS / 4).unwrap(),
                    recompute(&s, DEFAULT_BUCKETS / 4),
                    "folded leaves diverged at step {step}"
                );
            }
        }
        s.clear().unwrap();
        assert!(s
            .version_leaf_digests(DEFAULT_BUCKETS)
            .unwrap()
            .iter()
            .all(|&x| x == 0));
        assert_eq!(
            s.version_leaf_digests(DEFAULT_BUCKETS).unwrap(),
            recompute(&s, DEFAULT_BUCKETS)
        );
    }

    #[test]
    fn store_if_newer_is_correct_after_reopen() {
        let path = temp_path("reopen-lww");
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        {
            let dev = FileBlockDevice::create(&path, BS, 256).unwrap();
            let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
            s.apply(b"k", Some(b"new"), v(500, 9)).unwrap();
            s.sync().unwrap();
        }
        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
        assert_eq!(s.current_version(b"k").unwrap(), Some(v(500, 9)));
        assert_eq!(
            s.apply(b"k", Some(b"older"), v(1, 9)).unwrap(),
            Applied::Lost(v(500, 9))
        );
        assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
    }

    #[test]
    fn flush_recovery_seeds_versions_and_lww() {
        let path = temp_path("flush-recover-lww");
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        {
            let dev = FileBlockDevice::create(&path, BS, 1024).unwrap();
            let mut s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
            for i in 0..20u32 {
                s.apply(
                    format!("k{i:02}").as_bytes(),
                    Some(format!("v{i:02}").as_bytes()),
                    v(100 + i as u64, 9),
                )
                .unwrap();
            }
            s.flush().unwrap();
            s.apply(b"k00", Some(b"k00-new"), v(1000, 9)).unwrap();
            s.apply(b"k01", None, v(1001, 9)).unwrap();
            s.apply(b"k99", Some(b"v99"), v(1002, 9)).unwrap();
            s.sync().unwrap();
        }

        let dev = FileBlockDevice::open(&path, BS).unwrap();
        let mut s = VersionedStore::open(dev, cfg, 1).unwrap();
        assert_eq!(s.current_version(b"k05").unwrap(), Some(v(105, 9)));
        assert_eq!(s.current_version(b"k00").unwrap(), Some(v(1000, 9)));
        assert_eq!(s.current_version(b"k99").unwrap(), Some(v(1002, 9)));
        assert_eq!(s.get(b"k05").unwrap().as_deref(), Some(&b"v05"[..]));
        assert_eq!(s.get(b"k00").unwrap().as_deref(), Some(&b"k00-new"[..]));
        assert_eq!(s.get(b"k01").unwrap(), None);
        assert_eq!(
            s.apply(b"k05", Some(b"older"), v(1, 9)).unwrap(),
            Applied::Lost(v(105, 9))
        );
    }
