use super::*;

#[test]
fn authoritative_put_skips_the_redundant_memtable_lookup() {
    let mut e = engine(Ordering::Unsorted);
    e.put(b"k", b"new", 20).unwrap();
    let write = e.put_kind_authoritative(b"k", b"stale", 10, false).unwrap();
    assert!(write.written);
    assert_eq!(write.old_version, None);
    assert_eq!(e.mutable_entry(b"k").unwrap().version, 10);
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"stale"[..]));
}

#[test]
fn authoritative_put_combines_directory_resolution_and_update() {
    let mut e = engine(Ordering::Unsorted);
    e.rebuild_winner_directory().unwrap();
    let first = e.put_kind_authoritative(b"k", b"new", 20, false).unwrap();
    assert!(first.written);
    assert_eq!(first.old_version, None);
    let stale = e.put_kind_authoritative(b"k", b"stale", 10, false).unwrap();
    assert!(!stale.written);
    assert_eq!(stale.old_version, Some(20));
    assert_eq!(e.mutable_entry(b"k").unwrap().version, 20);
    assert_eq!(e.get(b"k").unwrap().as_deref(), Some(&b"new"[..]));
}

#[test]
fn winner_directory_multi_get_rejects_wrong_keys_sync_and_async() {
    let mut e = engine(Ordering::Unsorted);
    e.put(b"alpha", b"a", 10).unwrap();
    e.put(b"beta", b"b", 10).unwrap();
    e.rebuild_winner_directory().unwrap();
    let keys: [&[u8]; 2] = [b"alpha", b"beta"];
    assert_eq!(
        e.multi_get(&keys).unwrap(),
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())]
    );
    assert_eq!(
        vals_rt::block_on(e.multi_get_async(&keys), || {}).unwrap(),
        vec![Some(b"a".to_vec()), Some(b"b".to_vec())]
    );

    let beta = e.winner_directory_entry(b"beta").unwrap();
    e.winner_directory
        .as_mut()
        .unwrap()
        .insert(b"alpha", beta);
    let sync = e.multi_get(&keys).unwrap_err().to_string();
    assert!(sync.contains("record key mismatch"));
    let asynchronous = vals_rt::block_on(e.multi_get_async(&keys), || {})
        .unwrap_err()
        .to_string();
    assert!(asynchronous.contains("record key mismatch"));
}

#[test]
fn winner_directory_multi_get_rejects_wrong_versions_sync_and_async() {
    let mut e = engine(Ordering::Unsorted);
    e.put(b"alpha", b"a", 10).unwrap();
    e.put(b"beta", b"b", 20).unwrap();
    e.rebuild_winner_directory().unwrap();
    let alpha = e.winner_directory_entry(b"alpha").unwrap();
    e.winner_directory
        .as_mut()
        .unwrap()
        .insert(
            b"alpha",
            IndexEntry {
                version: alpha.version + 1,
                ..alpha
            },
        );
    let keys: [&[u8]; 2] = [b"alpha", b"beta"];
    let sync = e.multi_get(&keys).unwrap_err().to_string();
    assert!(sync.contains("record version mismatch"));
    let asynchronous = vals_rt::block_on(e.multi_get_async(&keys), || {})
        .unwrap_err()
        .to_string();
    assert!(asynchronous.contains("record version mismatch"));
}

#[test]
fn winner_directory_dense_storage_is_collision_safe_and_keeps_argmax() {
    let entry = |version: u128| IndexEntry {
        ptr: ValuePtr {
            segment: SegmentId(version as u64),
            offset: version as u32,
            len: 1,
        },
        seq: version as u64,
        version,
        tombstone: version & 1 != 0,
    };
    let mut colliding = Vec::new();
    for i in 0u64.. {
        let key = i.to_le_bytes();
        if WinnerDirectory::hash(&key) as usize & 7 == 3 {
            colliding.push(key);
            if colliding.len() == 6 {
                break;
            }
        }
    }

    let mut directory = WinnerDirectory::with_capacity(4);
    for (i, key) in colliding.iter().enumerate() {
        assert_eq!(directory.insert(key, entry(i as u128 + 10)), None);
    }
    for (i, key) in colliding.iter().enumerate() {
        assert_eq!(directory.get(key), Some(entry(i as u128 + 10)));
    }
    assert!(!directory.insert_max(&colliding[2], entry(1)));
    assert_eq!(directory.get(&colliding[2]), Some(entry(12)));
    assert_eq!(directory.remove(&colliding[1]), Some(entry(11)));
    assert_eq!(directory.get(&colliding[1]), None);
    for (i, key) in colliding.iter().enumerate().filter(|(i, _)| *i != 1) {
        assert_eq!(directory.get(key), Some(entry(i as u128 + 10)));
    }
    let mut keys: Vec<&[u8]> = colliding.iter().map(|key| key.as_slice()).collect();
    keys.push(b"missing");
    let batched = directory.read_tokens(&keys);
    for (key, token) in keys.iter().zip(batched) {
        assert_eq!(token.map(|token| token.entry), directory.get(key));
    }

    let mut measured = WinnerDirectory::with_capacity(1000);
    for i in 0..1000u128 {
        let key = format!("key-{i:012}");
        measured.insert(key.as_bytes(), entry(i));
    }
    assert_eq!(measured.len(), 1000);
    assert_eq!(measured.controls.len(), measured.table.len());
    assert_eq!(measured.allocated_bytes(), 74_624);
    for i in 0..1000u128 {
        let key = format!("key-{i:012}");
        assert_eq!(measured.remove(key.as_bytes()), Some(entry(i)));
    }
    assert_eq!(measured.allocated_bytes(), 0);
}

#[test]
fn winner_directory_representative_probe_distribution() {
    const KEYS: usize = 700_000;
    const QUERIES: usize = 1_000_000;
    let entry = IndexEntry {
        ptr: ValuePtr {
            segment: SegmentId(1),
            offset: 0,
            len: 100,
        },
        seq: 1,
        version: 1,
        tombstone: false,
    };
    let key = |i: usize| format!("t0196-production-{i:012}");
    let mut directory = WinnerDirectory::with_capacity(KEYS);
    for i in 0..KEYS {
        directory.insert(key(i).as_bytes(), entry);
    }
    assert_eq!(directory.table.len(), 1 << 20);
    assert!((directory.len() as f64 / directory.table.len() as f64) > 0.66);

    let mut hits = WinnerProbeDistribution::default();
    let mut misses = WinnerProbeDistribution::default();
    let mut state = 0x0196_c017_ba5e_5eedu64;
    for i in 0..QUERIES {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let hit = i & 1 == 0;
        let index = if hit {
            state as usize % KEYS
        } else {
            KEYS + state as usize % KEYS
        };
        let profile = directory.probe_profile(key(index).as_bytes());
        assert_eq!(profile.found, hit);
        if hit {
            hits.record(profile);
        } else {
            misses.record(profile);
        }
    }
    let hit_summary = WinnerProbeDistribution::summary(hits.profiles.iter().map(|p| p.probes));
    let miss_summary = WinnerProbeDistribution::summary(misses.profiles.iter().map(|p| p.probes));
    let hit_fingerprint_summary = WinnerProbeDistribution::summary(
        hits.profiles.iter().map(|p| p.fingerprint_matches),
    );
    let miss_fingerprint_summary = WinnerProbeDistribution::summary(
        misses.profiles.iter().map(|p| p.fingerprint_matches),
    );
    let hit_baseline_summary = WinnerProbeDistribution::summary(
        hits.profiles.iter().map(|p| p.baseline_full_key_comparisons),
    );
    let miss_baseline_summary = WinnerProbeDistribution::summary(
        misses.profiles.iter().map(|p| p.baseline_full_key_comparisons),
    );
    let hit_candidate_summary = WinnerProbeDistribution::summary(
        hits.profiles.iter().map(|p| p.full_key_comparisons),
    );
    let miss_candidate_summary = WinnerProbeDistribution::summary(
        misses.profiles.iter().map(|p| p.full_key_comparisons),
    );
    println!(
        "winner probe fixture keys={KEYS} table={} load={:.6} allocated={} queries={QUERIES}",
        directory.table.len(),
        directory.len() as f64 / directory.table.len() as f64,
        directory.allocated_bytes()
    );
    println!(
        "hits queries={} probes_total={} mean={:.6} p50={} p99={} max={} fingerprints={} baseline_full_keys={} candidate_full_keys={}",
        hits.queries,
        hit_summary.0,
        hit_summary.1,
        hit_summary.2,
        hit_summary.3,
        hit_summary.4,
        hits.fingerprint_matches,
        hits.baseline_full_key_comparisons,
        hits.full_key_comparisons
    );
    println!(
        "misses queries={} probes_total={} mean={:.6} p50={} p99={} max={} fingerprints={} baseline_full_keys={} candidate_full_keys={}",
        misses.queries,
        miss_summary.0,
        miss_summary.1,
        miss_summary.2,
        miss_summary.3,
        miss_summary.4,
        misses.fingerprint_matches,
        misses.baseline_full_key_comparisons,
        misses.full_key_comparisons
    );
    for (kind, metric, summary) in [
        ("hits", "fingerprints", hit_fingerprint_summary),
        ("hits", "baseline_full_keys", hit_baseline_summary),
        ("hits", "candidate_full_keys", hit_candidate_summary),
        ("misses", "fingerprints", miss_fingerprint_summary),
        ("misses", "baseline_full_keys", miss_baseline_summary),
        ("misses", "candidate_full_keys", miss_candidate_summary),
    ] {
        println!(
            "{kind} {metric} total={} mean={:.6} p50={} p99={} max={}",
            summary.0, summary.1, summary.2, summary.3, summary.4
        );
    }
    assert_eq!(hits.queries, QUERIES / 2);
    assert_eq!(hits.found, hits.queries);
    assert_eq!(misses.found, 0);
    assert!(hits.full_key_comparisons < hits.baseline_full_key_comparisons);
    assert!(misses.full_key_comparisons * 20 < misses.baseline_full_key_comparisons);
}

#[test]
fn winner_directory_batch_probe_telemetry_accounts_collisions_and_deletions() {
    let entry = |version: u128| IndexEntry {
        ptr: ValuePtr {
            segment: SegmentId(version as u64),
            offset: version as u32,
            len: 1,
        },
        seq: version as u64,
        version,
        tombstone: false,
    };
    let mut directory = WinnerDirectory::with_capacity(64);
    let mask = directory.table.len() - 1;
    let mut groups = std::collections::HashMap::<(usize, u8), Vec<[u8; 8]>>::new();
    let mut homes = std::collections::HashMap::<usize, Vec<[u8; 8]>>::new();
    for i in 0u64.. {
        let key = i.to_le_bytes();
        let hash = WinnerDirectory::hash(&key);
        groups
            .entry((hash as usize & mask, WinnerDirectory::fingerprint(hash)))
            .or_default()
            .push(key);
        homes.entry(hash as usize & mask).or_default().push(key);
        if groups.iter().any(|((home, _), keys)| {
            keys.len() >= 3
                && homes[home]
                    .iter()
                    .map(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(key)))
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    >= 2
        }) {
            break;
        }
    }
    let same_fingerprint = groups
        .iter()
        .find(|((home, _), keys)| {
            keys.len() >= 3
                && homes[home]
                    .iter()
                    .map(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(key)))
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    >= 2
        })
        .unwrap()
        .1
        .clone();
    let home = WinnerDirectory::hash(&same_fingerprint[0]) as usize & mask;
    let fingerprint = WinnerDirectory::fingerprint(WinnerDirectory::hash(&same_fingerprint[0]));
    let different_fingerprint = homes[&home]
        .iter()
        .find(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(*key)) != fingerprint)
        .copied()
        .unwrap();
    directory.insert(&same_fingerprint[0], entry(10));
    directory.insert(&different_fingerprint, entry(20));

    let query = [same_fingerprint[0].as_slice(), same_fingerprint[1].as_slice()];
    let mut scratch = WinnerReadScratch::with_capacity(query.len());
    let stats = directory.read_tokens_profiled_into(query.len(), |i| query[i], &mut scratch);
    assert_eq!(scratch.tokens()[0].map(|token| token.entry), Some(entry(10)));
    assert_eq!(scratch.tokens()[1], None);
    assert_eq!(stats.lookups, 2);
    assert_eq!(stats.home_slot_hits, 1);
    assert_eq!(stats.empty_misses, 1);
    assert_eq!(stats.fingerprint_mismatches, 1);
    assert_eq!(stats.deleted_buckets, 0);
    assert_eq!(stats.collision_fallbacks, 1);
    assert_eq!(stats.full_key_collisions, 1);
    assert_eq!(stats.total_probe_len, 4);
    assert_eq!(stats.probe_len_le_1, 1);
    assert_eq!(stats.probe_len_2_4, 1);
    assert_eq!(stats.probe_len_5_8, 0);
    assert_eq!(stats.probe_len_gt_8, 0);
    let hash_ptr = scratch.hashes.as_ptr();
    let candidate_ptr = scratch.candidates.as_ptr();
    let token_ptr = scratch.tokens.as_ptr();

    for i in 0..50u128 {
        directory.insert(format!("probe-filler-{i}").as_bytes(), entry(100 + i));
    }
    directory.remove(&same_fingerprint[0]).unwrap();
    let query = [different_fingerprint.as_slice()];
    let stats = directory.read_tokens_profiled_into(1, |i| query[i], &mut scratch);
    assert_eq!(scratch.tokens()[0].map(|token| token.entry), Some(entry(20)));
    assert_eq!(stats.deleted_buckets, 1);
    assert_eq!(stats.collision_fallbacks, 1);
    assert_eq!(stats.total_probe_len, 2);
    assert_eq!(stats.probe_len_2_4, 1);
    assert_eq!(scratch.hashes.as_ptr(), hash_ptr);
    assert_eq!(scratch.candidates.as_ptr(), candidate_ptr);
    assert_eq!(scratch.tokens.as_ptr(), token_ptr);
}

#[test]
fn winner_directory_controls_reject_misses_and_survive_all_table_lifecycle_paths() {
    let entry = |version: u128| IndexEntry {
        ptr: ValuePtr {
            segment: SegmentId(version as u64),
            offset: version as u32,
            len: 1,
        },
        seq: version as u64,
        version,
        tombstone: false,
    };
    let mut directory = WinnerDirectory::with_capacity(64);
    let mask = directory.table.len() - 1;
    let mut groups = std::collections::HashMap::<(usize, u8), Vec<[u8; 8]>>::new();
    let mut same_home = std::collections::HashMap::<usize, Vec<[u8; 8]>>::new();
    for i in 0u64.. {
        let key = i.to_le_bytes();
        let hash = WinnerDirectory::hash(&key);
        let home = hash as usize & mask;
        let fingerprint = WinnerDirectory::fingerprint(hash);
        groups.entry((home, fingerprint)).or_default().push(key);
        same_home.entry(home).or_default().push(key);
        if groups.values().any(|keys| keys.len() >= 3)
            && same_home.values().any(|keys| {
                keys.iter()
                    .map(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(key)))
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    >= 8
            })
        {
            break;
        }
    }
    let same_fingerprint = groups.values().find(|keys| keys.len() >= 3).unwrap().clone();
    let home = WinnerDirectory::hash(&same_fingerprint[0]) as usize & mask;
    let mut clustered = same_home.remove(&home).unwrap();
    clustered.sort_by_key(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(key)));
    clustered.dedup_by_key(|key| WinnerDirectory::fingerprint(WinnerDirectory::hash(key)));
    assert!(clustered.len() >= 2);

    directory.insert(&same_fingerprint[0], entry(10));
    directory.insert(&clustered[0], entry(20));
    let colliding_miss = directory.probe_profile(&same_fingerprint[1]);
    assert!(!colliding_miss.found);
    assert!(colliding_miss.probes >= 3);
    assert!(colliding_miss.baseline_full_key_comparisons >= 2);
    assert_eq!(colliding_miss.fingerprint_matches, 1);
    assert_eq!(colliding_miss.full_key_comparisons, 1);

    directory.insert(&same_fingerprint[1], entry(30));
    let hit = directory.probe_profile(&same_fingerprint[1]);
    assert!(hit.found);
    assert!(hit.probes >= 2);
    assert!(hit.baseline_full_key_comparisons > hit.full_key_comparisons);
    assert_eq!(directory.get(&same_fingerprint[0]), Some(entry(10)));
    assert_eq!(directory.get(&same_fingerprint[1]), Some(entry(30)));
    assert_eq!(directory.get(&clustered[0]), Some(entry(20)));

    assert_eq!(directory.remove(&clustered[0]), Some(entry(20)));
    assert_eq!(directory.get(&clustered[0]), None);
    assert_eq!(directory.controls.len(), directory.table.len());
    directory.insert(&clustered[1], entry(40));
    assert_eq!(directory.get(&clustered[1]), Some(entry(40)));
    assert_eq!(directory.get(&same_fingerprint[0]), Some(entry(10)));
    assert_eq!(directory.get(&same_fingerprint[1]), Some(entry(30)));

    let mut reuse = WinnerDirectory::with_capacity(32);
    reuse.insert(&clustered[0], entry(50));
    for i in 1_000..1_020u128 {
        reuse.insert(&i.to_le_bytes(), entry(i));
    }
    let removed_bucket = reuse.find(&clustered[0]).unwrap().0;
    reuse.remove(&clustered[0]).unwrap();
    assert_eq!(reuse.controls[removed_bucket], WINNER_CTRL_DELETED);
    assert_eq!(reuse.table[removed_bucket], WINNER_DELETED);
    assert_eq!(reuse.deleted_buckets, 1);
    reuse.insert(&clustered[1], entry(60));
    assert_eq!(reuse.deleted_buckets, 0);
    assert_eq!(
        reuse.controls[removed_bucket],
        WinnerDirectory::fingerprint(WinnerDirectory::hash(&clustered[1]))
    );
    assert_eq!(reuse.get(&clustered[1]), Some(entry(60)));

    for i in 10_000..10_200u128 {
        directory.insert(&i.to_le_bytes(), entry(i));
    }
    let grown = directory.table.len();
    assert_eq!(directory.controls.len(), grown);
    for i in 10_000..10_200u128 {
        assert_eq!(directory.get(&i.to_le_bytes()), Some(entry(i)));
    }
    for i in 10_000..10_200u128 {
        directory.remove(&i.to_le_bytes()).unwrap();
    }
    assert!(directory.table.len() < grown);
    assert_eq!(directory.controls.len(), directory.table.len());
    directory.finish_build();
    assert_eq!(directory.controls.len(), directory.table.len());
    assert_eq!(directory.get(&same_fingerprint[0]), Some(entry(10)));
    directory.clear();
    assert_eq!(directory.allocated_bytes(), 0);
    assert!(directory.controls.is_empty());
}

#[test]
fn winner_directory_sync_and_async_reads_validate_key_and_version() {
    let dev = MemBlockDevice::new(BS, 256);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put(b"alpha", b"a", 10).unwrap();
    e.put(b"beta", b"b", 10).unwrap();
    e.rebuild_winner_directory().unwrap();
    assert_eq!(e.get(b"alpha").unwrap().as_deref(), Some(&b"a"[..]));

    let alpha = e.winner_directory_entry(b"alpha").unwrap();
    let beta = e.winner_directory_entry(b"beta").unwrap();
    e.winner_directory
        .as_mut()
        .unwrap()
        .insert(b"alpha", beta);
    let wrong_key = e.get(b"alpha").unwrap_err().to_string();
    assert!(wrong_key.contains("record key mismatch"));

    e.winner_directory
        .as_mut()
        .unwrap()
        .insert(
            b"alpha",
            IndexEntry {
                version: alpha.version + 1,
                ..alpha
            },
        );
    let wrong_version = e.get(b"alpha").unwrap_err().to_string();
    assert!(wrong_version.contains("record version mismatch"));

    e.winner_directory
        .as_mut()
        .unwrap()
        .insert(b"alpha", alpha);
    let StagedGet::WinnerDirectory { plan, token } = e.probe_get(b"alpha") else {
        panic!("winner-directory read must plan the value directly");
    };
    let wrong_key = vals_rt::block_on(
        plan.read_value_checked(e.device(), b"wrong", token.entry.version, |_, _| ()),
        || {},
    )
    .unwrap_err()
    .to_string();
    assert!(wrong_key.contains("record key mismatch"));

    let StagedGet::WinnerDirectory { plan, token } = e.probe_get(b"alpha") else {
        panic!("winner-directory read must plan the value directly");
    };
    let wrong_version = vals_rt::block_on(
        plan.read_value_checked(e.device(), b"alpha", token.entry.version + 1, |_, _| ()),
        || {},
    )
    .unwrap_err()
    .to_string();
    assert!(wrong_version.contains("record version mismatch"));
}

#[test]
fn winner_directory_staged_plan_detects_winner_and_storage_generation_changes() {
    let dev = MemBlockDevice::new(BS, 512);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put(b"key", b"old", 10).unwrap();
    e.rebuild_winner_directory().unwrap();

    let StagedGet::WinnerDirectory { plan, token } = e.probe_get(b"key") else {
        panic!("winner-directory read must plan the value directly");
    };
    let generation = e.storage_generation();
    e.put(b"key", b"new", 20).unwrap();
    let old = std::cell::RefCell::new(None);
    vals_rt::block_on(
        plan.read_value_checked(e.device(), b"key", token.entry.version, |version, value| {
            *old.borrow_mut() = Some((version, value.to_vec()));
        }),
        || {},
    )
    .unwrap();
    assert_eq!(old.into_inner(), Some((10, b"old".to_vec())));
    assert_eq!(e.storage_generation(), generation);
    assert!(!e.winner_read_token_matches(b"key", token));
    assert_eq!(async_get(&e, b"key").as_deref(), Some(&b"new"[..]));

    let StagedGet::WinnerDirectory { plan, token } = e.probe_get(b"key") else {
        panic!("winner-directory read must plan the value directly");
    };
    let generation = e.storage_generation();
    e.compact().unwrap();
    assert_ne!(e.storage_generation(), generation);
    assert!(!e.winner_read_token_matches(b"key", token));
    let staged = vals_rt::block_on(
        plan.read_value_checked(e.device(), b"key", token.entry.version, |_, value| value.to_vec()),
        || {},
    );
    assert!(
        staged.is_err()
            || e.storage_generation() != generation
            || !e.winner_read_token_matches(b"key", token)
    );
    assert_eq!(async_get(&e, b"key").as_deref(), Some(&b"new"[..]));
}

#[test]
fn winner_directory_eliminates_flushed_point_and_version_probes() {
    let (mut e, _) = mixed_engine();
    assert_eq!(e.winner_directory_version(b"a"), None);
    let expected: Vec<(&[u8], Option<Vec<u8>>, Option<u128>)> = [
        b"a" as &[u8],
        b"b",
        b"c",
        b"d",
        b"e",
        b"f",
        b"z",
    ]
    .into_iter()
    .map(|key| (key, e.get(key).unwrap(), e.version(key).unwrap()))
    .collect();

    assert!(e.rebuild_winner_directory().unwrap() >= 6);
    assert!(e.winner_directory_enabled());
    assert!(e.winner_directory_bytes() > 0);
    for (key, value, version) in expected {
        assert_eq!(e.winner_directory_version(key), Some(version));
        assert_eq!(async_get(&e, key), value);
        assert_eq!(async_version(&e, key), version);
        assert!(!matches!(e.probe_get(key), StagedGet::Runs { .. }));
        assert!(!matches!(e.probe_version(key), StagedVersion::Runs { .. }));
    }
}

#[test]
fn winner_directory_tracks_tombstones_index_merge_and_truncate() {
    let dev = MemBlockDevice::new(BS, 4096);
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put_kind(b"winner", b"live", 10, false).unwrap();
    e.rebuild_winner_directory().unwrap();
    e.put_kind(b"winner", b"stale", 5, false).unwrap();
    assert_eq!(e.winner_directory_entry(b"winner").unwrap().version, 10);
    e.put_kind(b"winner", b"", 20, true).unwrap();
    let tombstone = e.winner_directory_entry(b"winner").unwrap();
    assert_eq!(tombstone.version, 20);
    assert!(tombstone.tombstone);
    e.flush(opts).unwrap();
    assert_eq!(e.winner_directory_entry(b"winner"), Some(tombstone));

    e.put(b"other", b"value", 30).unwrap();
    e.flush(opts).unwrap();
    assert_eq!(e.run_count(), 2);
    let stats = e.compact_runs(opts, None).unwrap();
    assert_eq!(stats.runs_merged, 2);
    assert_eq!(e.winner_directory_entry(b"winner"), Some(tombstone));
    assert_eq!(e.get(b"other").unwrap().as_deref(), Some(&b"value"[..]));

    e.truncate().unwrap();
    assert!(e.winner_directory_enabled());
    assert_eq!(e.winner_directory_len(), 0);
    assert_eq!(e.winner_directory_bytes(), 0);
    assert_eq!(e.version(b"winner").unwrap(), None);
}

#[test]
fn winner_directory_keeps_argmax_across_lower_flush_and_reopen() {
    let path = temp_path("winner-directory-reopen");
    let dev = FileBlockDevice::create(&path, BS, 4096).unwrap();
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts {
        num_buckets: VDP_BUCKETS,
        ..Default::default()
    };
    let mut e = Engine::create_prewiped(dev, cfg);
    e.put(b"k", b"winner", 20).unwrap();
    e.flush(opts).unwrap();
    e.rebuild_winner_directory().unwrap();
    e.put(b"k", b"loser", 10).unwrap();
    assert_eq!(e.version(b"k").unwrap(), Some(20));
    assert_eq!(e.get(b"k").unwrap(), Some(b"winner".to_vec()));
    e.flush(opts).unwrap();
    e.sync().unwrap();

    drop(e);
    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let mut reopened = Engine::open(dev, cfg).unwrap();
    reopened.rebuild_winner_directory().unwrap();
    assert_eq!(reopened.version(b"k").unwrap(), Some(20));
    assert_eq!(reopened.get(b"k").unwrap(), Some(b"winner".to_vec()));
    assert!(matches!(reopened.probe_version(b"k"), StagedVersion::Memtable(20)));
}

#[test]
fn winner_directory_recovery_capacity_tracks_unique_keys_not_run_history() {
    let path = temp_path("winner-directory-overlap-capacity");
    let cfg = Config {
        seg_blocks: 8,
        reserved_blocks: 0,
        ordering: Ordering::Sorted,
    };
    let opts = RunBuildOpts::default();
    {
        let dev = FileBlockDevice::create(&path, BS, 8192).unwrap();
        let mut e = Engine::create_prewiped(dev, cfg);
        for round in 0..12u128 {
            for key in 0..32u128 {
                let name = format!("overlap-{key:03}");
                let value = format!("round-{round:03}");
                e.put(name.as_bytes(), value.as_bytes(), round * 1000 + key)
                    .unwrap();
            }
            e.flush(opts).unwrap();
        }
        assert_eq!(e.run_count(), 12);
        e.sync().unwrap();
    }

    let dev = FileBlockDevice::open(&path, BS).unwrap();
    let mut e = Engine::open(dev, cfg).unwrap();
    assert_eq!(e.run_count(), 12);
    assert_eq!(e.rebuild_winner_directory().unwrap(), 32);
    let directory = e.winner_directory.as_ref().unwrap();
    assert_eq!(directory.entries.len(), 32);
    assert!(directory.entries.capacity() <= 64);
    assert!(directory.table.len() <= 64);
    assert!(directory.keys.capacity() <= directory.live_key_bytes * 2);
    assert!(directory.allocated_bytes() < 4 * 1024);
    for key in 0..32u128 {
        let name = format!("overlap-{key:03}");
        assert_eq!(e.version(name.as_bytes()).unwrap(), Some(11_000 + key));
    }
}

