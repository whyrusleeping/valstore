use super::*;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Ordering};

const WALL: u64 = 10_000;

fn v(hlc: u64) -> Version {
    Version { hlc, node: 1 }
}

fn cfg() -> Config {
    Config { seg_blocks: 8, reserved_blocks: 0, ordering: Ordering::Sorted }
}

fn sorted(dev: MemBlockDevice) -> VersionedStore<MemBlockDevice> {
    VersionedStore::new(Engine::create_prewiped(dev, cfg()), 1).unwrap()
}

fn put(s: &mut VersionedStore<MemBlockDevice>, key: &[u8], hlc: u64) {
    assert_eq!(s.apply_at(key, Some(key), v(hlc), WALL).unwrap(), Applied::Won, "{key:?}");
}

fn live(key: &[u8], hlc: u64) -> Stored {
    Stored { version: v(hlc), value: Some(key.to_vec()) }
}

fn tomb(hlc: u64) -> Stored {
    Stored { version: v(hlc), value: None }
}

fn physical(s: &VersionedStore<MemBlockDevice>, key: &[u8]) -> Option<u128> {
    s.engine().physical_version(key).unwrap()
}

fn physical_leaves(s: &VersionedStore<MemBlockDevice>) -> (Vec<u64>, u64) {
    let winners: Vec<(Vec<u8>, Version)> = s
        .engine()
        .recovered_versions()
        .unwrap()
        .into_iter()
        .map(|(k, raw)| (k.to_vec(), unpack_version(raw)))
        .collect();
    let leaves = crate::antientropy::leaf_digests_versioned(winners.iter().map(|(k, ver)| (&k[..], *ver)), INDEX_BUCKETS);
    (leaves, winners.len() as u64)
}

fn planned_values(s: &VersionedStore<MemBlockDevice>, keys: &[Vec<u8>]) -> Vec<(Vec<u8>, Stored)> {
    let dev = s.device_handle();
    let (plans, more) = s.plan_values(keys, usize::MAX).unwrap();
    assert!(!more);
    plans
        .into_iter()
        .map(|(k, plan)| match plan {
            ValuePlan::Read(plan) => (k, vals_rt::block_on(execute_read_plan(&plan, &*dev), || {}).unwrap()),
            ValuePlan::Covered(version) => (k, Stored { version, value: None }),
        })
        .collect()
}

#[test]
fn covered_keys_read_as_a_tombstone_at_the_cover_and_newer_writes_win() {
    let mut s = sorted(MemBlockDevice::new(256, 4096));
    put(&mut s, b"a", 10);
    put(&mut s, b"b", 10);
    s.flush().unwrap();
    put(&mut s, b"c", 25);
    put(&mut s, b"d", 10);
    assert_eq!(s.apply_range_delete(Some(b"b"), Some(b"d"), v(20), WALL).unwrap(), Applied::Won);

    for (key, want) in [
        (&b"a"[..], Some(live(b"a", 10))),
        (b"b", Some(tomb(20))),
        (b"bz", Some(tomb(20))),
        (b"c", Some(live(b"c", 25))),
        (b"d", Some(live(b"d", 10))),
        (b"e", None),
    ] {
        assert_eq!(s.get_versioned(key).unwrap(), want, "{key:?}");
        assert_eq!(s.version(key).unwrap(), want.as_ref().map(|st| st.version), "{key:?}");
        assert_eq!(s.digest_version(key).unwrap(), want.as_ref().map(|st| st.version), "{key:?}");
    }
    let keys: Vec<Vec<u8>> = [&b"a"[..], b"b", b"bz", b"c", b"d", b"e"].iter().map(|k| k.to_vec()).collect();
    let want = vec![
        (b"a".to_vec(), live(b"a", 10)),
        (b"b".to_vec(), tomb(20)),
        (b"bz".to_vec(), tomb(20)),
        (b"c".to_vec(), live(b"c", 25)),
        (b"d".to_vec(), live(b"d", 10)),
    ];
    assert_eq!(s.values_page(&keys, usize::MAX).unwrap().0, want);
    assert_eq!(planned_values(&s, &keys), want);

    assert_eq!(s.apply_range_delete(Some(b"b"), Some(b"d"), v(20), WALL).unwrap(), Applied::Won, "re-applying is implied");
    put(&mut s, b"bb", 30);
    assert_eq!(s.get_versioned(b"bb").unwrap(), Some(live(b"bb", 30)));

    assert_eq!(s.cas_at(b"b", b"cas", None, WALL).unwrap(), CasResult::Mismatch { current: Some(v(20)) });
    let CasResult::Applied(cas) = s.cas_at(b"b", b"cas", Some(v(20)), WALL).unwrap() else {
        panic!("CAS against the cover version applies");
    };
    assert!(cas > v(20));
    assert_eq!(s.get(b"b").unwrap().as_deref(), Some(&b"cas"[..]));
}

#[test]
fn local_writes_after_reopen_stamp_above_the_cover() {
    let mem = MemBlockDevice::new(256, 4096);
    let rt = Version { hlc: 5_000 << crate::hlc::COUNTER_BITS, node: 1 };
    {
        let mut s = sorted(mem.share());
        put(&mut s, b"k", 10);
        assert_eq!(s.apply_range_delete(None, None, rt, 5_000).unwrap(), Applied::Won);
        s.sync().unwrap();
    }
    let mut s = VersionedStore::open(mem.share(), cfg(), 1).unwrap();
    assert_eq!(s.get_versioned(b"k").unwrap(), Some(Stored { version: rt, value: None }));
    let put = s.put_at(b"k", b"after", 4_800).unwrap();
    assert!(put > rt, "{put:?} must be stamped above the cover {rt:?}");
    assert_eq!(s.get(b"k").unwrap().as_deref(), Some(&b"after"[..]));
    let fresh = s.put_at(b"never-seen", b"x", 4_800).unwrap();
    assert!(fresh > rt);
    assert_eq!(s.get(b"never-seen").unwrap().as_deref(), Some(&b"x"[..]));
    let deleted = s.delete_at(b"other", 4_800).unwrap();
    assert!(deleted > rt);
    assert_eq!(s.get_versioned(b"other").unwrap(), Some(Stored { version: deleted, value: None }));
}

#[test]
fn stragglers_lose_on_every_apply_path_without_appending_or_reading_runs() {
    let mut s = sorted(MemBlockDevice::new(256, 4096));
    put(&mut s, b"a", 10);
    put(&mut s, b"b", 10);
    s.flush().unwrap();
    put(&mut s, b"c", 10);
    put(&mut s, b"z", 10);
    assert_eq!(s.apply_range_delete(Some(b"a"), Some(b"d"), v(20), WALL).unwrap(), Applied::Won);
    let seq = s.engine().next_log_seq();
    let keys = [&b"a"[..], b"b", b"bx", b"c"];
    let before: Vec<Option<u128>> = keys.iter().map(|k| physical(&s, k)).collect();
    for key in keys {
        for hlc in [15, 20] {
            for value in [Some(&b"late"[..]), None] {
                let lost = Applied::Lost(v(20));
                assert_eq!(s.apply_at(key, value, v(hlc), WALL).unwrap(), lost, "apply_at {key:?}@{hlc}");
                assert!(
                    matches!(s.apply_stage(key, value, v(hlc), WALL).unwrap(), StagedApply::Done(a) if a == lost),
                    "apply_stage {key:?}@{hlc} decides in RAM"
                );
                assert_eq!(s.apply_blind_at(key, value, v(hlc), WALL).unwrap(), lost, "blind {key:?}@{hlc}");
                assert_eq!(s.apply_commit_staged_snapshot(key, value, v(hlc), None, WALL).unwrap(), lost);
            }
        }
    }
    assert_eq!(s.engine().next_log_seq(), seq, "a covered candidate appends nothing");
    assert_eq!(keys.iter().map(|k| physical(&s, k)).collect::<Vec<_>>(), before);
    assert_eq!(s.apply_at(b"z", Some(b"old"), v(5), WALL).unwrap(), Applied::Lost(v(10)), "outside the span: point LWW");
    assert_eq!(s.apply_at(b"b", Some(b"new"), v(21), WALL).unwrap(), Applied::Won);
    assert_eq!(s.get_versioned(b"b").unwrap(), Some(Stored { version: v(21), value: Some(b"new".to_vec()) }));
}

#[test]
fn a_range_tombstone_landing_during_the_probe_await_wins_at_commit() {
    let mut s = sorted(MemBlockDevice::new(256, 4096));
    put(&mut s, b"k", 10);
    s.flush().unwrap();
    let dev = s.device_handle();
    let StagedApply::NeedsRunRead { probe, gen } = s.apply_stage(b"k", Some(b"late"), v(15), WALL).unwrap() else {
        panic!("a flushed key needs a run read");
    };
    let run_old = vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
    assert_eq!(run_old, Some(v(10)));
    s.apply_range_delete(Some(b"k"), Some(b"l"), v(20), WALL).unwrap();
    assert!(matches!(
        s.apply_commit_staged(b"k", Some(b"late"), v(15), run_old, gen, WALL).unwrap(),
        StagedApplyOutcome::Retry
    ));
    assert_eq!(
        s.apply_commit_staged_snapshot(b"k", Some(b"late"), v(15), run_old, WALL).unwrap(),
        Applied::Lost(v(20)),
        "the commit re-tests the cover after the await"
    );

    put(&mut s, b"m", 10);
    s.flush().unwrap();
    let store = std::cell::RefCell::new(s);
    let gate = crate::net::ReadGate::new();
    let applied = vals_rt::block_on(
        apply_staged_with_retry(
            &store,
            &*dev,
            &gate,
            b"m",
            Some(b"late"),
            v(25),
            WALL,
            ApplyRetryMode::Fill,
            || {
                store.borrow_mut().apply_range_delete(Some(b"m"), Some(b"n"), v(30), WALL).unwrap();
            },
        ),
        || {},
    )
    .unwrap_or_else(|_| panic!("staged apply failed"));
    assert_eq!(applied, Some(Applied::Lost(v(30))));
    assert_eq!(store.borrow().get_versioned(b"m").unwrap(), Some(tomb(30)));
}

#[test]
fn leaves_follow_physical_winners_while_nothing_is_accounted() {
    let mut s = sorted(MemBlockDevice::new(256, 4096));
    for key in [&b"a"[..], b"b", b"c", b"d", b"e", b"f"] {
        put(&mut s, key, 10);
    }
    s.flush().unwrap();
    put(&mut s, b"c", 12);
    put(&mut s, b"g", 10);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), physical_leaves(&s).0);
    s.apply_range_delete(Some(b"b"), Some(b"g"), v(20), WALL).unwrap();
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), physical_leaves(&s).0);

    assert_eq!(s.apply_at(b"b", Some(b"b2"), v(30), WALL).unwrap(), Applied::Won);
    assert_eq!(s.apply_at(b"b", Some(b"stale"), v(25), WALL).unwrap(), Applied::Lost(v(30)));
    assert_eq!(s.apply_at(b"b", Some(b"stale"), v(15), WALL).unwrap(), Applied::Lost(v(20)));
    let dev = s.device_handle();
    let staged = match s.apply_stage(b"c", Some(b"c2"), v(31), WALL).unwrap() {
        StagedApply::Done(a) => a,
        StagedApply::NeedsRunRead { probe, gen } => {
            let run_old = vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
            let StagedApplyOutcome::Applied(a) = s.apply_commit_staged(b"c", Some(b"c2"), v(31), run_old, gen, WALL).unwrap() else {
                panic!("no concurrent change");
            };
            a
        }
    };
    assert_eq!(staged, Applied::Won);
    s.put_at(b"d", b"d2", WALL).unwrap();
    assert!(matches!(s.cas_at(b"e", b"e2", Some(v(20)), WALL).unwrap(), CasResult::Applied(_)));
    s.delete_at(b"f", WALL).unwrap();
    assert_eq!(s.apply_at(b"bq", Some(b"fresh"), v(32), WALL).unwrap(), Applied::Won);
    s.put_at(b"br", b"fresh", WALL).unwrap();

    let (leaves, winners) = physical_leaves(&s);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves);
    assert_eq!(s.exact_live_keys(), Some(winners));
    s.invalidate_leaf_seed();
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves, "a reseed agrees");
}

#[test]
fn range_delete_replies_rejected_beyond_skew_and_errors_on_refusal() {
    let mut s = sorted(MemBlockDevice::new(256, 4096));
    let future = Version { hlc: (WALL + 10_000) << crate::hlc::COUNTER_BITS, node: 1 };
    assert_eq!(s.apply_range_delete(None, None, future, WALL).unwrap(), Applied::Rejected);
    assert!(s.engine().range_tombstones().is_empty());
    assert!(s.apply_range_delete(Some(b"b"), Some(b"a"), v(20), WALL).is_err());
    s.engine_mut().set_range_tombstone_max_fragments(1);
    s.apply_range_delete(Some(b"a"), Some(b"b"), v(20), WALL).unwrap();
    assert!(s.apply_range_delete(Some(b"c"), Some(b"d"), v(20), WALL).is_err(), "cap");
    assert_eq!(s.apply_range_delete(Some(b"a"), Some(b"b"), v(19), WALL).unwrap(), Applied::Won, "implied");

    let mut unsorted = VersionedStore::new(
        Engine::create_prewiped(MemBlockDevice::new(256, 4096), Config { ordering: Ordering::Unsorted, ..cfg() }),
        1,
    )
    .unwrap();
    assert!(unsorted.apply_range_delete(None, None, v(20), WALL).is_err());
}

type Store = VersionedStore<MemBlockDevice>;
type Groups = std::collections::BTreeMap<Vec<NodeId>, Vec<u64>>;

fn acct_store() -> Store {
    acct_store_on(MemBlockDevice::new(256, 1 << 16))
}

fn acct_store_on(dev: MemBlockDevice) -> Store {
    acct_store_as(dev, 1)
}

fn acct_store_as(dev: MemBlockDevice, node: NodeId) -> Store {
    Store::new(Engine::create_prewiped(dev, cfg()), node).unwrap().with_max_skew_ms(0)
}

fn ring() -> crate::ring::Ring {
    crate::ring::Ring::from_members(2, 64, [1, 2, 3])
}

fn prepare(s: &Store, epoch: u64, map: &RangeMap) {
    let zone = crate::zone::ZoneConfig {
        name: "rt-accounting".into(),
        rf: 2,
        ordering: crate::zone::Ordering::Sorted,
        scope: None,
    };
    s.prepare_zone_seed((1, 1, epoch), &ring(), 42, Some(zone), Some(map.clone()));
}

fn k(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn at(i: u64) -> Version {
    Version { hlc: i << crate::hlc::COUNTER_BITS, node: 1 }
}

fn base_map() -> RangeMap {
    let mut map = RangeMap::from_splits([k(250), k(500), k(800)]);
    map.enable_opaque_tokens();
    map
}

fn moved(map: &RangeMap, index: usize) -> Vec<u8> {
    let sorted = |mut g: Vec<NodeId>| {
        g.sort_unstable();
        g
    };
    let current = sorted(map.replicas_of_range(&ring(), 42, index, 2, None));
    (0..256u64)
        .map(|n| crate::partition::opaque_placement_token([n as u8; 16]))
        .find(|token| token.as_slice() != map.token_of(index) && sorted(ring().replicas_in_zone(42, token, 2, None)) != current)
        .expect("some opaque token moves the range to another node set")
}

fn rd(s: &mut Store, lo: u64, hi: Option<u64>, version: Version) {
    let hi = hi.map(k);
    assert_eq!(s.apply_range_delete(Some(&k(lo)), hi.as_deref(), version, 0).unwrap(), Applied::Won);
}

fn drain_seed(s: &Store) {
    for _ in 0..100_000 {
        if s.seed_step_cooperative().unwrap() {
            return;
        }
    }
    panic!("seed did not converge");
}

fn turns(s: &Store, mut prefetched: impl FnMut(&Store) -> Option<RecountStep>, sync: impl Fn(&Store) -> RecountStep) {
    for turn in 0..100_000 {
        let step = if turn % 2 == 0 { prefetched(s).unwrap_or_else(|| sync(s)) } else { sync(s) };
        match step {
            RecountStep::Done => return,
            RecountStep::More => {}
            other => panic!("unexpected step {other:?}"),
        }
    }
    panic!("accounting turns did not converge");
}

fn recount_turns(s: &Store) {
    turns(
        s,
        |s| {
            let plan = s.logical_recount_prefetch_plan().unwrap()?;
            let fetched = vals_rt::block_on(plan.read(&*s.device_handle()), || {});
            Some(s.logical_recount_prefetched_step(fetched, std::time::Duration::ZERO).expect("unchanged store").unwrap())
        },
        |s| s.logical_recount_step().unwrap(),
    );
}

fn regroup_turns(s: &Store) {
    turns(
        s,
        |s| {
            let plan = s.regroup_prefetch_plan().unwrap()?;
            let fetched = vals_rt::block_on(plan.read(&*s.device_handle()), || {});
            Some(s.regroup_prefetched_step(fetched).expect("unchanged store").unwrap())
        },
        |s| s.regroup_step().unwrap(),
    );
}

fn ready(s: &Store) {
    drain_seed(s);
    regroup_turns(s);
    recount_turns(s);
}

fn uncovered(s: &Store, cover: &vals_store::range_tombstone::FragmentSet) -> Vec<(Vec<u8>, Version)> {
    s.engine()
        .recovered_versions()
        .unwrap()
        .into_iter()
        .filter(|(key, raw)| !rt_accounting::covers(cover, key, *raw))
        .map(|(key, raw)| (key.to_vec(), unpack_version(raw)))
        .collect()
}

fn assert_accounted(s: &Store, cover: &vals_store::range_tombstone::FragmentSet, epoch: u64, map: &RangeMap, what: &str) {
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(s, 1, &std::collections::HashSet::from([0])), || {}).unwrap(), "{what}");
    assert!(!s.regroup_pending(), "{what}");
    let winners = uncovered(s, cover);
    let mut groups = Groups::new();
    let mut buckets = vec![0u64; INDEX_BUCKETS as usize];
    let mut ranges = vec![0u64; map.num_ranges()];
    for (key, version) in &winners {
        let b = VersionIndex::slot(key);
        groups.entry(s.group_of(key)).or_insert_with(|| vec![0; INDEX_BUCKETS as usize])[b] ^=
            crate::antientropy::entry_digest(key, *version);
        buckets[b] += 1;
        ranges[map.range_index(key)] += 1;
    }
    groups.retain(|_, leaves| leaves.iter().any(|&leaf| leaf != 0));
    let leaves: Groups = s
        .versions
        .leaves
        .borrow()
        .iter()
        .filter(|(_, leaves)| leaves.iter().any(|&leaf| leaf != 0))
        .map(|(group, leaves)| (group.to_vec(), leaves.to_vec()))
        .collect();
    assert_eq!(leaves, groups, "{what}: group leaves");
    let counts: Vec<u64> = s.versions.buckets.borrow().iter().map(|b| b.logical_keys).collect();
    assert_eq!(counts, buckets, "{what}: bucket counts");
    assert_eq!(s.exact_live_keys(), Some(winners.len() as u64), "{what}: live keys");
    assert_eq!(s.logical_range_counts(epoch), Some(ranges), "{what}: range counts");
}

fn populated() -> Store {
    populated_on(MemBlockDevice::new(256, 1 << 16))
}

fn populated_on(dev: MemBlockDevice) -> Store {
    populated_as(dev, 1)
}

fn populated_as(dev: MemBlockDevice, node: NodeId) -> Store {
    let mut s = acct_store_as(dev, node);
    for i in 0..1000 {
        s.apply_at(&k(i), Some(b"v"), at(i + 1), 0).unwrap();
        if i % 150 == 149 {
            s.flush().unwrap();
        }
    }
    for i in (0..1000).step_by(7) {
        s.apply_at(&k(i), Some(b"v2"), at(1001 + i), 0).unwrap();
    }
    s.flush().unwrap();
    for i in (0..1000).step_by(11) {
        s.apply_at(&k(i), None, at(2001 + i), 0).unwrap();
    }
    for i in (3..1000).step_by(13) {
        s.apply_at(&k(i), Some(b"mt"), at(3001 + i), 0).unwrap();
    }
    s
}

fn deleted(map: &RangeMap) -> Store {
    let mut s = populated();
    prepare(&s, 10, map);
    ready(&s);
    assert_accounted(&s, &Default::default(), 10, map, "no range tombstones");
    rd(&mut s, 100, Some(400), at(1600));
    rd(&mut s, 300, Some(650), at(2500));
    s
}

fn staged(s: &mut Store, key: &[u8], value: Option<&[u8]>, version: Version) -> Applied {
    let dev = s.device_handle();
    match s.apply_stage(key, value, version, 0).unwrap() {
        StagedApply::Done(applied) => applied,
        StagedApply::NeedsRunRead { probe, gen } => {
            let run_old = vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
            let StagedApplyOutcome::Applied(applied) = s.apply_commit_staged(key, value, version, run_old, gen, 0).unwrap() else {
                panic!("no concurrent change");
            };
            applied
        }
    }
}

#[test]
fn reseeded_leaves_and_counts_exclude_accounted_covered_winners_through_writes_and_compaction() {
    let map = base_map();
    let mut s = deleted(&map);
    assert_eq!(s.accounted_range_tombstones().1, 0);
    assert!(s.rt_fold_pending());
    assert_eq!(s.apply_at(&k(150), Some(b"w"), at(5000), 0).unwrap(), Applied::Won);

    s.invalidate_leaf_seed();
    ready(&s);
    assert!(!s.rt_fold_pending(), "a seed accounts every range tombstone applied before it starts");
    let f = s.engine().range_tombstones().clone();
    let seq = s.engine().range_tombstone_max_seq();
    assert_eq!(s.accounted_range_tombstones(), (f.clone(), seq));
    assert_eq!(s.engine().range_tombstone_accounted_seq(), seq);
    assert!(uncovered(&s, &f).len() + 300 < s.engine().recovered_versions().unwrap().len());
    assert_accounted(&s, &f, 10, &map, "reseed");

    let mut next = 6000;
    let mut tick = || {
        next += 1;
        at(next)
    };
    for i in [110u64, 120, 310, 320, 360, 420, 600] {
        assert_eq!(s.apply_at(&k(i), Some(b"w"), tick(), 0).unwrap(), Applied::Won, "{i}");
    }
    assert_eq!(s.apply_at(&k(130), None, tick(), 0).unwrap(), Applied::Won);
    assert_eq!(s.apply_at(b"k00105-new", Some(b"n"), tick(), 0).unwrap(), Applied::Won);
    assert_eq!(staged(&mut s, &k(200), Some(b"s"), tick()), Applied::Won);
    s.put_at(&k(210), b"p", 0).unwrap();
    s.delete_at(&k(220), 0).unwrap();
    assert!(matches!(s.cas_at(&k(230), b"c", Some(at(1600)), 0).unwrap(), CasResult::Applied(_)));
    assert_eq!(s.apply_at(&k(240), Some(b"late"), at(1500), 0).unwrap(), Applied::Lost(at(1600)));
    s.engine_mut().rebuild_winner_directory().unwrap();
    assert_eq!(s.apply_blind_at(&k(260), Some(b"b"), tick(), 0).unwrap(), Applied::Won);
    assert_eq!(s.apply_at(&k(330), None, at(2331), 0).unwrap(), Applied::Lost(at(2500)));
    assert_eq!(s.apply_blind_at(&k(340), None, at(2341), 0).unwrap(), Applied::Lost(at(2500)));
    assert_accounted(&s, &f, 10, &map, "writes over accounted covered keys");

    let leaves = s.version_leaf_digests(INDEX_BUCKETS).unwrap();
    assert!(physical(&s, &k(170)).is_some() && physical(&s, &k(870)).is_some());
    s.flush().unwrap();
    let stats = s.compact_runs(0).unwrap();
    assert!(stats.covered_dropped > 0);
    assert_eq!(physical(&s, &k(170)), None, "compaction drops accounted covered entries");
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves, "without changing leaves");
    assert_accounted(&s, &f, 10, &map, "compaction");

    rd(&mut s, 850, None, at(2200));
    for i in [860u64, 900] {
        assert_eq!(s.apply_at(&k(i), Some(b"w"), tick(), 0).unwrap(), Applied::Won);
    }
    assert!(s.rt_fold_pending());
    let folded = s.engine().range_tombstones().clone();
    assert_accounted(&s, &folded, 10, &map, "writes over keys covered only by an unaccounted range tombstone, then its fold");
    assert!(physical(&s, &k(870)).is_some(), "a fold leaves run entries to compaction");

    s.invalidate_leaf_seed();
    ready(&s);
    let f = s.engine().range_tombstones().clone();
    assert_eq!(s.accounted_range_tombstones().0, f);
    assert_accounted(&s, &f, 10, &map, "reseed with every range tombstone");
}

#[test]
fn retiring_an_accounted_covered_winner_leaves_the_leaves_alone() {
    let map = base_map();
    let mut s = deleted(&map);
    s.invalidate_leaf_seed();
    ready(&s);
    let f = s.engine().range_tombstones().clone();
    assert!(s.mutable_entry(&k(330)).is_some_and(|e| e.version == pack_version(at(2331))), "a covered memtable tombstone");
    assert_eq!(s.forget(&std::collections::HashSet::from([k(330), k(440)])).unwrap(), 2);
    assert_accounted(&s, &f, 10, &map, "forgetting covered and uncovered keys");
}

#[test]
fn dirty_rebuilds_split_recounts_and_regroups_agree_with_a_seed_under_the_accounted_set() {
    let map = base_map();
    let s = deleted(&map);
    s.invalidate_leaf_seed();
    ready(&s);
    let f = s.engine().range_tombstones().clone();
    assert_accounted(&s, &f, 10, &map, "seed");

    for i in [100u64, 150, 320, 350, 640] {
        s.versions.dirty_bucket(VersionIndex::slot(&k(i)));
    }
    assert!(s.versions.has_dirty());
    assert_accounted(&s, &f, 10, &map, "dirty-bucket rebuild");

    let mut split = map.clone();
    assert!(split.split(&k(350)));
    prepare(&s, 11, &split);
    assert!(s.is_leaf_seeded() && s.logical_recount_pending(), "an inherited split recounts");
    recount_turns(&s);
    assert_accounted(&s, &f, 11, &split, "split recount");

    let index = split.range_index(&k(400));
    let mut pending = split.clone();
    assert!(pending.set_pending(index, moved(&split, index)));
    prepare(&s, 12, &pending);
    assert!(s.is_leaf_seeded() && s.regroup_pending(), "a re-home regroups");
    regroup_turns(&s);
    assert!(s.is_leaf_seeded());
    assert_accounted(&s, &f, 12, &pending, "regroup");
}

#[test]
fn inherited_splits_inside_fragments_and_merges_across_fragment_edges_keep_reads_leaves_and_counts() {
    let map = base_map();
    let s = deleted(&map);
    s.invalidate_leaf_seed();
    ready(&s);
    let f = s.engine().range_tombstones().clone();
    let reads = |s: &Store| (0..1000).map(|i| s.get_versioned(&k(i)).unwrap()).collect::<Vec<_>>();
    let before = reads(&s);
    assert_eq!(before[150], Some(Stored { version: at(1600), value: None }));
    assert_eq!(before[641], Some(Stored { version: at(2500), value: None }));

    let mut split = map.clone();
    for at in [k(200), k(600), k(650)] {
        assert!(split.split(&at));
    }
    prepare(&s, 11, &split);
    assert!(s.is_leaf_seeded() && s.logical_recount_pending(), "an inherited split recounts");
    recount_turns(&s);
    assert_accounted(&s, &f, 11, &split, "inherited splits inside and at the edge of fragments");
    assert_eq!(reads(&s), before);

    let mut merged = split.clone();
    assert!(merged.merge_at(&k(650)));
    prepare(&s, 12, &merged);
    assert!(!s.logical_recount_pending(), "a merge sums its children");
    assert_accounted(&s, &f, 12, &merged, "a same-placement merge across a fragment edge");

    assert!(merged.merge_at(&k(500)));
    prepare(&s, 13, &merged);
    regroup_turns(&s);
    recount_turns(&s);
    assert_accounted(&s, &f, 13, &merged, "a placement-changing merge across a fragment");
    assert_eq!(reads(&s), before);
}

#[test]
fn writes_during_a_seed_are_accounted_under_the_seed_cover() {
    let map = base_map();
    let mut s = deleted(&map);
    let seq = s.engine().range_tombstone_max_seq();
    let seed_cover = s.engine().range_tombstones().clone();
    s.invalidate_leaf_seed();
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(s.versions.seeding.get());
    rd(&mut s, 850, None, at(2200));
    let mut next = 6000u64;
    let writes = [110u64, 320, 860, 360, 130, 900, 230, 610];
    for turn in 0..100_000 {
        if let Some(&i) = writes.get(turn) {
            next += 1;
            let value: Option<&[u8]> = if i % 2 == 0 { Some(b"w") } else { None };
            assert_eq!(s.apply_at(&k(i), value, at(next), 0).unwrap(), Applied::Won);
        }
        if turn == 3 {
            assert_eq!(s.forget(&std::collections::HashSet::from([k(330)])).unwrap(), 1);
        }
        if turn >= writes.len() && s.seed_step_cooperative().unwrap() {
            break;
        }
        if turn < writes.len() {
            assert!(!s.seed_step_cooperative().unwrap(), "the seed spans the writes");
        }
    }
    recount_turns(&s);
    assert_eq!(s.accounted_range_tombstones(), (seed_cover.clone(), seq));
    assert_eq!(s.engine().range_tombstone_accounted_seq(), seq);
    assert!(s.rt_fold_pending(), "the range tombstone applied during the seed waits for its fold");
    let folded = s.engine().range_tombstones().clone();
    assert_accounted(&s, &folded, 10, &map, "a seed with concurrent writes, forgets and a later range tombstone, then its fold");
    assert_eq!(s.accounted_range_tombstones().0, folded);
}

#[test]
fn collected_range_tombstones_leave_the_accounted_set() {
    let ms = |m: u64| Version { hlc: m << crate::hlc::COUNTER_BITS, node: 1 };
    for seed_in_flight in [false, true] {
        let mut s = acct_store().with_gc_grace_ms(1_000);
        for i in 0..20 {
            s.apply_at(format!("a{i:02}").as_bytes(), Some(b"v"), ms(100), 10_000).unwrap();
        }
        s.flush().unwrap();
        assert_eq!(s.apply_range_delete(Some(b"a"), Some(b"b"), ms(200), 10_000).unwrap(), Applied::Won);
        for i in 0..20 {
            s.apply_at(format!("m{i:02}").as_bytes(), Some(b"v"), ms(300), 10_000).unwrap();
        }
        s.flush().unwrap();
        s.invalidate_leaf_seed();
        drain_seed(&s);
        assert!(!s.accounted_range_tombstones().0.is_empty());
        if seed_in_flight {
            s.invalidate_leaf_seed();
            assert!(!s.seed_step_cooperative().unwrap());
        }
        s.advance_durable_gc_watermark(5_000);
        let stats = s.compact_runs(10_000).unwrap();
        assert_eq!(stats.collected_range_tombstones.len(), 1, "seed in flight: {seed_in_flight}");
        assert!(s.engine().range_tombstones().is_empty());
        assert!(s.accounted_range_tombstones().0.is_empty());
        if seed_in_flight {
            assert!(!s.seed_step_cooperative().unwrap());
        }
        assert_eq!(s.apply_at(b"a06", Some(b"old"), ms(150), 10_000).unwrap(), Applied::Rejected, "a live straggler below the cutoff");
        assert_eq!(s.get(b"a06").unwrap(), None);
        assert_eq!(s.apply_at(b"a05", None, ms(150), 10_000).unwrap(), Applied::Won, "a straggler tombstone below the collected cover");
        assert_eq!(s.apply_at(b"a05", Some(b"new"), ms(6_000), 10_000).unwrap(), Applied::Won);
        drain_seed(&s);
        let (leaves, winners) = physical_leaves(&s);
        assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves, "seed in flight: {seed_in_flight}");
        assert_eq!(s.exact_live_keys(), Some(winners));
    }
}

#[test]
fn clearing_the_store_forgets_the_accounted_set() {
    let mut s = acct_store();
    for i in 0..20 {
        put(&mut s, &k(i), 10);
    }
    s.flush().unwrap();
    rd(&mut s, 0, None, v(20));
    s.invalidate_leaf_seed();
    drain_seed(&s);
    assert!(!s.accounted_range_tombstones().0.is_empty());
    s.clear().unwrap();
    assert_eq!(s.accounted_range_tombstones(), (Default::default(), 0));
    put(&mut s, &k(3), 10);
    put(&mut s, &k(3), 11);
    let (leaves, winners) = physical_leaves(&s);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves);
    assert_eq!(s.exact_live_keys(), Some(winners));
}

#[test]
fn a_seed_that_changes_the_accounted_set_replaces_counts_a_split_recounted_meanwhile() {
    let _policy = seed_policy::policy_for_test(true, 25, 64 << 20);
    let map = base_map();
    let s = deleted(&map);
    let cover = s.engine().range_tombstones().clone();
    s.invalidate_leaf_seed();
    for _ in 0..6 {
        assert!(!s.seed_step_cooperative().unwrap());
    }
    let mut split = map.clone();
    assert!(split.split(&k(350)));
    prepare(&s, 11, &split);
    assert!(s.logical_recount_pending());
    recount_turns(&s);
    assert!(!s.logical_recount_pending(), "the recount finishes under the old accounted set");
    assert!(s.rt_fold_pending() && s.logical_range_counts(11).is_none(), "range tombstones wait for the seed's accounting");
    drain_seed(&s);
    assert_eq!(s.seed_test_counts().0, 0, "the in-flight seed survives the generation bump");
    recount_turns(&s);
    assert_eq!(s.accounted_range_tombstones().0, cover);
    assert_accounted(&s, &cover, 11, &split, "a carried seed publishes counts under its own cover");
}

#[test]
fn digest_pages_list_uncovered_winners_under_the_accounted_set() {
    let mut s = acct_store();
    for i in 0..200 {
        s.apply_at(&k(i), Some(b"v"), at(i + 1), 0).unwrap();
        if i % 60 == 59 {
            s.flush().unwrap();
        }
    }
    for i in 30..60 {
        s.apply_at(&k(i), Some(b"m"), at(300 + i), 0).unwrap();
    }
    rd(&mut s, 20, Some(120), at(500));
    s.invalidate_leaf_seed();
    drain_seed(&s);
    rd(&mut s, 150, Some(180), at(500));
    let (accounted, _) = s.accounted_range_tombstones();
    let ids: Vec<u32> = (0..INDEX_BUCKETS).collect();
    let mut want = uncovered(&s, &accounted);
    want.sort();
    assert_eq!(want.len(), 100);
    assert!(want.iter().any(|(key, _)| *key == k(160)), "an unaccounted range tombstone filters nothing");
    assert_eq!(s.bucket_digest_page(INDEX_BUCKETS, &ids, None, usize::MAX).unwrap(), (want.clone(), false));
    let dev = s.device_handle();
    for budget in [1usize, 64, 200, usize::MAX] {
        for planned in [false, true] {
            let (mut got, mut after, mut pages) = (Vec::new(), None::<Vec<u8>>, 0);
            loop {
                let (page, more) = if planned {
                    let plan = s.plan_bucket_digest_page(INDEX_BUCKETS, &ids, after.as_deref(), budget).unwrap();
                    vals_rt::block_on(
                        VersionedStore::bucket_digest_page_from_plan(plan, INDEX_BUCKETS, &ids, after.as_deref(), budget, &*dev),
                        || {},
                    )
                    .unwrap()
                } else {
                    s.bucket_digest_page(INDEX_BUCKETS, &ids, after.as_deref(), budget).unwrap()
                };
                pages += 1;
                assert!(page.iter().all(|(key, version)| !rt_accounting::covers(&accounted, key, pack_version(*version))));
                let next = more.then(|| page.last().expect("a truncated page has an entry").0.clone());
                assert!(next.as_ref().is_none_or(|next| after.as_ref().is_none_or(|after| next > after)));
                got.extend(page);
                match next {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
            assert_eq!(got, want, "budget {budget}, planned {planned}");
            assert!(budget == usize::MAX || pages > 10, "budget {budget}, planned {planned}: {pages} pages");
        }
    }
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn fold_turn(s: &mut Store, turn: usize) -> RecountStep {
    fold_turn_with(s, turn.is_multiple_of(2))
}

fn fold_turn_with(s: &mut Store, prefetch: bool) -> RecountStep {
    if prefetch {
        s.begin_rt_fold_with_trees();
        if let Some(plan) = s.rt_fold_prefetch_plan().unwrap() {
            let fetched = vals_rt::block_on(plan.read(&*s.device_handle()), || {});
            if let Some(step) = s.rt_fold_prefetched_step(fetched) {
                return step.unwrap();
            }
        }
    }
    s.rt_fold_step().unwrap()
}

fn fold_all(s: &mut Store) {
    fold_all_with(s, |turn| turn % 2 == 0);
}

fn fold_all_with(s: &mut Store, prefetch: impl Fn(usize) -> bool) {
    for turn in 0..100_000 {
        if fold_turn_with(s, prefetch(turn)) == RecountStep::Done {
            assert!(!s.rt_fold_pending());
            return;
        }
    }
    panic!("folds did not converge");
}

fn covered_mutable(s: &Store) -> Vec<Vec<u8>> {
    let f = s.engine().range_tombstones();
    s.engine()
        .scan_mutable_entries(None, None)
        .unwrap()
        .into_iter()
        .filter(|(key, e)| rt_accounting::covers(f, key, e.version))
        .map(|(key, _)| key)
        .collect()
}

type Accounting = (Groups, Vec<u64>, Option<u64>, Option<Vec<u64>>);

fn accounting(s: &Store, epoch: u64) -> Accounting {
    let leaves: Groups = s
        .versions
        .leaves
        .borrow()
        .iter()
        .filter(|(_, leaves)| leaves.iter().any(|&leaf| leaf != 0))
        .map(|(group, leaves)| (group.to_vec(), leaves.to_vec()))
        .collect();
    let buckets = s.versions.buckets.borrow().iter().map(|b| b.logical_keys).collect();
    (leaves, buckets, s.exact_live_keys(), s.logical_range_counts(epoch))
}

fn between(s: &mut Store) {
    for i in (300..480).step_by(7) {
        assert_eq!(s.apply_at(&k(i), Some(b"between"), at(5_000 + i), 0).unwrap(), Applied::Won, "{i}");
    }
}

#[test]
fn folds_account_overlapping_range_tombstones_and_interleaved_writes_like_a_seed() {
    let map = base_map();
    for rt2_turn in [None, Some(0), Some(2), Some(5)] {
        for seed in 1..=4u64 {
            let what = format!("second range tombstone after fold turn {rt2_turn:?}, seed {seed}");
            let mut s = populated();
            prepare(&s, 10, &map);
            ready(&s);
            let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
            let mut fresh = 9_000u64;
            rd(&mut s, 100, Some(500), at(5_000));
            between(&mut s);
            if rt2_turn.is_none() {
                rd(&mut s, 300, Some(700), at(6_000));
            }
            let mut rt2 = rt2_turn;
            let mut overlapped = false;
            for turn in 0..100_000 {
                fold_turn(&mut s, turn);
                if rt2 == Some(turn) {
                    let (target, _, cursor) = s.rt_fold_state().expect("the first fold is in flight");
                    assert!(target.cover(&k(450)).is_some() && target.cover(&k(600)).is_none(), "{what}: T is fixed at the first fold's start");
                    overlapped |= cursor < k(300);
                    rd(&mut s, 300, Some(700), at(6_000));
                    assert_eq!(s.rt_fold_state().unwrap().0, target, "{what}: a later range tombstone waits for the next fold");
                    rt2 = None;
                }
                for _ in 0..rng.below(3) {
                    match rng.below(8) {
                        0 if rt2.is_some() => {
                            let key = k(100 + rng.below(600));
                            s.apply_at(&key, Some(b"between"), at(5_500 + rng.below(400)), 0).unwrap();
                        }
                        1 => {
                            s.flush().unwrap();
                        }
                        _ => {
                            fresh += 1;
                            let value: Option<&[u8]> = if rng.below(4) == 0 { None } else { Some(b"fresh") };
                            assert_eq!(s.apply_at(&k(rng.below(1000)), value, at(fresh), 0).unwrap(), Applied::Won, "{what}");
                        }
                    }
                }
                if rt2.is_none() && !s.rt_fold_pending() {
                    break;
                }
            }
            assert!(rt2_turn.is_none_or(|turn| turn > 2 || overlapped), "{what}: test premise");
            let f = s.engine().range_tombstones().clone();
            let seq = s.engine().range_tombstone_max_seq();
            assert_eq!(s.accounted_range_tombstones(), (f.clone(), seq), "{what}");
            assert_eq!(s.engine().range_tombstone_accounted_seq(), seq, "{what}");
            assert_eq!(covered_mutable(&s), Vec::<Vec<u8>>::new(), "{what}: folds remove covered memtable entries");
            assert_accounted(&s, &f, 10, &map, &what);
            let folded = accounting(&s, 10);
            s.invalidate_leaf_seed();
            ready(&s);
            assert_eq!(accounting(&s, 10), folded, "{what}: a from-scratch seed agrees");
        }
    }
}

#[test]
fn leaves_after_a_reopen_and_reseed_equal_the_folded_leaves() {
    let map = base_map();
    for crash_mid_fold in [false, true] {
        let what = format!("crash mid fold: {crash_mid_fold}");
        let mem = MemBlockDevice::new(256, 1 << 16);
        let mut s = populated_on(mem.share());
        prepare(&s, 10, &map);
        ready(&s);
        rd(&mut s, 100, Some(500), at(5_000));
        between(&mut s);
        let cell = std::cell::RefCell::new(s);
        let gate = crate::net::ReadGate::new();
        let turn = || vals_rt::block_on(rt_fold_turn_with(&cell, Some(&gate), || {}), || {}).unwrap();
        assert_eq!(turn(), RecountStep::More);
        assert_eq!(turn(), RecountStep::More);
        rd(&mut cell.borrow_mut(), 300, Some(700), at(6_000));
        for i in [120u64, 350, 450, 650, 900] {
            assert_eq!(cell.borrow_mut().apply_at(&k(i), Some(b"after"), at(9_000 + i), 0).unwrap(), Applied::Won);
        }
        if !crash_mid_fold {
            for _ in 0..1_000 {
                if turn() == RecountStep::Done {
                    break;
                }
            }
            assert!(!cell.borrow().rt_fold_pending());
            assert_eq!(covered_mutable(&cell.borrow()), Vec::<Vec<u8>>::new(), "the accounting turn removes covered memtable entries");
        }
        let mut s = cell.into_inner();
        let want = (!crash_mid_fold).then(|| accounting(&s, 10));
        s.sync().unwrap();
        drop(s);
        let s = VersionedStore::open(mem.share(), cfg(), 1).unwrap().with_max_skew_ms(0);
        prepare(&s, 10, &map);
        ready(&s);
        let f = s.engine().range_tombstones().clone();
        assert!(f.cover(&k(150)).is_some() && f.cover(&k(650)).is_some(), "{what}");
        assert_eq!(s.accounted_range_tombstones().0, f, "{what}");
        assert!(!s.rt_fold_pending(), "{what}");
        assert_accounted(&s, &f, 10, &map, &what);
        if let Some(want) = want {
            assert_eq!(accounting(&s, 10), want, "{what}: reseeded leaves equal the folded leaves");
        }
    }
}

#[test]
fn a_pending_fold_answers_trees_busy_and_counts_not_ready() {
    let map = base_map();
    let mut s = deleted(&map);
    let all = std::collections::HashSet::from([0]);
    s.set_seed_driver(true);
    assert!(s.rt_fold_pending() && s.rt_fold_not_ready_for_caller());
    let token = s.tree_scope_token(1, &all);
    assert!(!vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &all), || {}).unwrap(), "tree requests are Busy while a fold is pending");
    assert!(!s.tree_scope_token_valid(&token));
    assert_eq!(s.logical_range_counts(10), None, "accounting is not ready while a fold is pending");
    assert_eq!(s.logical_range_count_bounds(10), None);
    assert_eq!(fold_turn(&mut s, 1), RecountStep::More);
    assert!(s.rt_fold_state().is_some() && s.rt_fold_not_ready_for_caller());
    fold_all(&mut s);
    rd(&mut s, 600, Some(900), at(7_000));
    assert_eq!(fold_turn(&mut s, 1), RecountStep::More);
    s.invalidate_leaf_seed();
    ready(&s);
    assert!(!s.rt_fold_pending(), "a seed absorbs an in-flight fold");
    assert_eq!(s.accounted_range_tombstones().0, *s.engine().range_tombstones());
    assert!(!s.rt_fold_not_ready_for_caller());
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &all), || {}).unwrap());
    assert!(s.logical_range_counts(10).is_some());
    s.set_seed_driver(false);
    let f = s.engine().range_tombstones().clone();
    assert_accounted(&s, &f, 10, &map, "after the folds");
}

#[test]
fn a_cumulative_delete_folds_only_what_was_written_since_the_previous_fold() {
    for prefetch in [false, true] {
        cumulative_delete_folds(prefetch);
    }
}

fn cumulative_delete_folds(prefetch: bool) {
    let fold_all = |s: &mut Store| fold_all_with(s, |_| prefetch);
    let mut s = acct_store();
    for i in 0..2_000 {
        s.apply_at(&k(i), Some(b"value"), at(i + 1), 0).unwrap();
    }
    let opts = vals_store::RunBuildOpts { num_buckets: INDEX_BUCKETS, block_target_bytes: 128, ..Default::default() };
    s.engine_mut().flush(opts).unwrap();
    for i in (1_000..1_100).step_by(3) {
        s.apply_at(&k(i), Some(b"memtable"), at(3_000 + i), 0).unwrap();
    }
    s.engine_mut().rebuild_winner_directory().unwrap();
    s.invalidate_leaf_seed();
    drain_seed(&s);
    assert_eq!(s.engine().range_tombstone_max_tree_blocks(), 0);
    rd(&mut s, 0, Some(1_500), at(5_000));
    assert!(!covered_mutable(&s).is_empty());
    fold_all(&mut s);
    assert!(s.engine().range_tombstone_max_tree_blocks() > 0, "the first fold builds the max trees");
    assert_eq!(covered_mutable(&s), Vec::<Vec<u8>>::new(), "the fold removes covered memtable entries");
    let removed = |raw: &u128| (pack_version(at(4_000))..pack_version(at(4_100))).contains(raw);
    assert!(
        !s.engine().recovered_versions().unwrap().iter().any(|(_, raw)| removed(raw)),
        "the winner directory forgets the removed memtable entries"
    );
    let first = s.rt_fold_winners();
    assert!(first >= 1_500, "{first}");
    for i in 2_000..2_050 {
        s.apply_at(&k(i), Some(b"value"), at(6_000 + i), 0).unwrap();
    }
    rd(&mut s, 0, Some(1_600), at(9_000));
    fold_all(&mut s);
    let second = s.rt_fold_winners() - first;
    assert!(second < 400, "prefetch {prefetch}: the second fold skips run blocks the first fold accounted: {second} winners read");
    let (leaves, _) = physical_leaves(&s);
    let f = s.engine().range_tombstones().clone();
    let winners = uncovered(&s, &f);
    assert_eq!(winners.len(), 400 + 50);
    let want = crate::antientropy::leaf_digests_versioned(winners.iter().map(|(key, ver)| (&key[..], *ver)), INDEX_BUCKETS);
    assert_ne!(want, leaves);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), want);
    assert_eq!(s.exact_live_keys(), Some(450));
}

#[test]
fn a_collection_during_a_fold_recomputes_its_target() {
    let ms = |m: u64| Version { hlc: m << crate::hlc::COUNTER_BITS, node: 1 };
    let key = |i: u64| format!("a{i:03}").into_bytes();
    let mut s = acct_store().with_gc_grace_ms(1_000);
    for i in 0..300 {
        s.apply_at(&key(i), Some(b"v"), ms(100), 10_000).unwrap();
    }
    s.flush().unwrap();
    assert_eq!(s.apply_range_delete(Some(b"a"), Some(&key(100)), ms(200), 10_000).unwrap(), Applied::Won);
    for i in 0..20 {
        s.apply_at(format!("m{i:02}").as_bytes(), Some(b"v"), ms(300), 10_000).unwrap();
    }
    s.flush().unwrap();
    s.invalidate_leaf_seed();
    drain_seed(&s);
    assert_eq!(s.apply_range_delete(Some(b"a"), Some(b"b"), ms(150), 10_000).unwrap(), Applied::Won);
    assert_eq!(fold_turn(&mut s, 1), RecountStep::More);
    let (target, _, cursor) = s.rt_fold_state().unwrap();
    assert!(cursor > key(100) && cursor < key(299) && target.cover(&key(50)) == Some(pack_version(ms(200))));
    s.advance_durable_gc_watermark(5_000);
    assert_eq!(s.compact_runs(10_000).unwrap().collected_range_tombstones.len(), 1);
    let (target, _, _) = s.rt_fold_state().expect("the fold survives the collection");
    assert_eq!(target, *s.engine().range_tombstones(), "the target drops the collected record");
    assert_eq!(s.apply_at(&key(50), None, ms(170), 10_000).unwrap(), Applied::Won, "a straggler tombstone above the remaining cover");
    assert_eq!(s.apply_at(&key(50), Some(b"new"), ms(6_000), 10_000).unwrap(), Applied::Won);
    fold_all(&mut s);
    let f = s.engine().range_tombstones().clone();
    let winners = uncovered(&s, &f);
    let want = crate::antientropy::leaf_digests_versioned(winners.iter().map(|(k, ver)| (&k[..], *ver)), INDEX_BUCKETS);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), want);
    assert_eq!(s.exact_live_keys(), Some(winners.len() as u64));
    s.invalidate_leaf_seed();
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), want, "a reseed agrees");
}

fn poll<F: std::future::Future>(f: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    f.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

#[test]
fn a_dirty_rebuild_never_publishes_across_a_pending_fold() {
    let mut s = acct_store();
    for i in 0..4_096 {
        s.apply_at(&k(i), Some(b"v"), at(i + 1), 0).unwrap();
    }
    s.flush().unwrap();
    drain_seed(&s);
    assert_eq!(s.apply_blind_at(&k(0), Some(b"dirty"), at(10_000), 0).unwrap(), Applied::Won);
    assert!(s.versions.has_dirty());
    let cell = std::cell::RefCell::new(s);
    let scope = std::collections::HashSet::from([0]);
    let mut rebuild = Box::pin(ensure_tree_ready_cooperative_cell(&cell, 1, &scope));
    assert!(poll(rebuild.as_mut()).is_pending(), "test premise: the rebuild yields between pages");
    rd(&mut cell.borrow_mut(), 0, Some(2_000), at(20_000));
    let mut fold = Box::pin(ensure_tree_ready_cooperative_cell(&cell, 1, &scope));
    assert!(poll(fold.as_mut()).is_pending(), "test premise: the inline fold yields after its first page");
    assert!(cell.borrow().rt_fold_state().is_some_and(|(_, _, cursor)| cursor > k(0)));
    let rebuilt = loop {
        if let std::task::Poll::Ready(r) = poll(rebuild.as_mut()) {
            break r.unwrap();
        }
    };
    assert!(!rebuilt, "a dirty rebuild does not publish while a fold is pending");
    let folded = loop {
        if let std::task::Poll::Ready(r) = poll(fold.as_mut()) {
            break r.unwrap();
        }
    };
    assert!(folded);
    drop((rebuild, fold));
    let s = cell.into_inner();
    assert!(!s.versions.has_dirty() && !s.rt_fold_pending());
    assert_eq!(covered_mutable(&s), Vec::<Vec<u8>>::new(), "a cell-driven inline fold removes covered memtable entries");
    assert!(s.engine().range_tombstone_max_tree_blocks() > 0, "and builds the max trees");
    let f = s.engine().range_tombstones().clone();
    let winners = uncovered(&s, &f);
    assert_eq!(winners.len(), 2_096);
    let want = crate::antientropy::leaf_digests_versioned(winners.iter().map(|(key, ver)| (&key[..], *ver)), INDEX_BUCKETS);
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), want);
    assert_eq!(s.exact_live_keys(), Some(2_096));
    s.invalidate_leaf_seed();
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), want, "a reseed agrees");
}

type Cluster = crate::coord::LocalCluster<MemBlockDevice>;

struct ReleasedSpan {
    map: RangeMap,
    local: NodeId,
    released: Vec<usize>,
    mem: MemBlockDevice,
    cell: std::cell::RefCell<Store>,
    lc: Cluster,
}

impl ReleasedSpan {
    fn owners(&self, index: usize) -> Vec<NodeId> {
        self.map.replicas_of_range(&ring(), 42, index, 2, None)
    }

    fn released(&self, key: &[u8]) -> bool {
        self.released.contains(&self.map.range_index(key))
    }

    fn bounds(&self, index: usize) -> (&[u8], Option<&[u8]>) {
        let (lo, hi) = self.map.range_bounds(index);
        (lo.unwrap_or_default(), hi)
    }

    fn pass(&self, gate: bool) -> crate::antientropy::CleanupProgress<MemBlockDevice> {
        self.pass_from(None, gate)
    }

    fn pass_from(&self, start: Option<Vec<u8>>, gate: bool) -> crate::antientropy::CleanupProgress<MemBlockDevice> {
        let (mut cursor, mut deferred, mut rpcs) = (start, false, 0);
        for _ in 0..100 {
            let mut progress = vals_rt::block_on(
                crate::antientropy::cleanup_scoped_zone(
                    &self.cell,
                    &self.lc,
                    &ring(),
                    self.local,
                    42,
                    2,
                    None,
                    usize::MAX,
                    Some(&self.map),
                    (0, 1),
                    cursor.as_deref(),
                    None,
                    || gate,
                    |_| false,
                    || true,
                    || false,
                ),
                || {},
            )
            .unwrap();
            deferred |= progress.deferred;
            rpcs += progress.handoff_rpcs;
            assert!(progress.handoff_rpcs <= crate::antientropy::CLEANUP_MAX_HANDOFF_RPCS_PER_PASS);
            if progress.wrapped || progress.forget_plan.is_some() || progress.next_cursor.is_none() {
                progress.deferred = deferred;
                progress.handoff_rpcs = rpcs;
                return progress;
            }
            cursor = progress.next_cursor;
        }
        panic!("cleanup did not complete its lap");
    }

    fn reopen(self) -> Self {
        let mut s = self.cell.into_inner();
        s.sync().unwrap();
        drop(s);
        let s = VersionedStore::open(self.mem.share(), cfg(), self.local).unwrap().with_max_skew_ms(0);
        prepare(&s, 10, &self.map);
        ready(&s);
        ReleasedSpan { cell: std::cell::RefCell::new(s), ..self }
    }
}

fn released_span(rts_in_runs: bool) -> ReleasedSpan {
    let map = base_map();
    let owners = |index: usize| map.replicas_of_range(&ring(), 42, index, 2, None);
    let local = (1..=3)
        .find(|n| !owners(1).contains(n) && (owners(0).contains(n) || owners(2).contains(n)))
        .expect("a node that lost range 1 and kept a neighbour inside the deleted span");
    let released: Vec<usize> = (0..map.num_ranges()).filter(|&i| !owners(i).contains(&local)).collect();
    let mem = MemBlockDevice::new(256, 1 << 16);
    let mut s = populated_as(mem.share(), local);
    assert_eq!(s.apply_at(&k(330), Some(b"mem"), at(2_400), 0).unwrap(), Applied::Won);
    prepare(&s, 10, &map);
    ready(&s);
    rd(&mut s, 100, Some(400), at(1_600));
    rd(&mut s, 300, Some(650), at(2_500));
    assert_eq!(s.apply_at(&k(350), Some(b"newer"), at(9_000), 0).unwrap(), Applied::Won);
    if rts_in_runs {
        s.flush().unwrap();
    }
    let mut lc = Cluster::new();
    for n in (1..=3).filter(|&n| n != local) {
        lc.insert_zone(n, 42, acct_store_as(MemBlockDevice::new(256, 1 << 16), n));
    }
    ReleasedSpan { map, local, released, mem, cell: std::cell::RefCell::new(s), lc }
}

fn commit_forget(t: &ReleasedSpan, plan: vals_store::CompactPlan<MemBlockDevice>) -> vals_store::CompactRunsStats {
    let wdev = std::sync::Arc::new(t.cell.borrow().device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).map_err(|(_, e)| e).unwrap();
    t.cell.borrow_mut().compact_commit(written).unwrap()
}

fn account(t: &ReleasedSpan) {
    let s = t.cell.borrow();
    s.invalidate_leaf_seed();
    ready(&s);
    assert!(!s.rt_fold_pending());
}

#[test]
fn cleanup_hands_released_fragments_to_owners_then_forgets_covered_entries_and_clips_records() {
    for rts_in_runs in [false, true] {
        let what = format!("range tombstones in runs: {rts_in_runs}");
        let t = released_span(rts_in_runs);
        assert!(t.cell.borrow().rt_fold_pending());
        let early = t.pass(false);
        assert!(early.deferred && early.forget_plan.is_none(), "{what}: unaccounted fragments are handed off but not released");
        assert!(t.cell.borrow().accumulating_released.is_empty(), "{what}: nothing is released before its fold");
        account(&t);
        if !rts_in_runs {
            assert!(covered_mutable(&t.cell.borrow()).contains(&k(330)), "{what}: premise: a seed leaves covered memtable entries");
        }
        let want_f = t.cell.borrow().engine().range_tombstones().clone();
        let before: Vec<(Vec<u8>, Option<Stored>, bool)> = (0..1_000)
            .map(|i| {
                let s = t.cell.borrow();
                let raw = physical(&s, &k(i)).unwrap();
                (k(i), s.get_versioned(&k(i)).unwrap(), rt_accounting::covers(&want_f, &k(i), raw))
            })
            .collect();
        assert!(before.iter().any(|(key, _, covered)| *covered && t.released(key)), "{what}: premise: covered keys in a released range");

        let stats = commit_forget(&t, t.pass(true).forget_plan.expect("a wrapped pass plans the forget"));
        assert!(stats.released_range_tombstones > 0, "{what}");

        let check = |t: &ReleasedSpan, reopened: bool| {
            let s = t.cell.borrow();
            let f = s.engine().range_tombstones().clone();
            for index in 0..t.map.num_ranges() {
                let (lo, hi) = t.bounds(index);
                if !t.released.contains(&index) {
                    assert_eq!(f.clipped(lo, hi), want_f.clipped(lo, hi), "{what}: kept range {index} keeps its fragments");
                    continue;
                }
                for owner in t.owners(index) {
                    let o = t.lc.store_zone(owner, 42).unwrap().borrow();
                    assert_eq!(o.engine().range_tombstones().clipped(lo, hi), want_f.clipped(lo, hi), "{what}: owner {owner} holds range {index}'s fragments");
                }
                if reopened && !rts_in_runs {
                    assert_eq!(f.clipped(lo, hi), want_f.clipped(lo, hi), "{what}: the unflushed records replay over range {index}, covering nothing");
                    continue;
                }
                assert_eq!(f.clipped(lo, hi), Vec::new(), "{what}: range {index}'s fragments leave F (reopened: {reopened})");
                assert!(
                    s.engine()
                        .range_tombstone_records()
                        .iter()
                        .all(|r| hi.is_some_and(|hi| r.lo.as_deref().unwrap_or_default() >= hi) || r.hi.as_deref().is_some_and(|rhi| rhi <= lo)),
                    "{what}: range {index} is clipped out of every record (reopened: {reopened})"
                );
            }
            for (key, want, covered) in &before {
                if !t.released(key) {
                    assert_eq!(s.get_versioned(key).unwrap(), *want, "{what}: kept {key:?} (reopened: {reopened})");
                    continue;
                }
                assert_eq!(physical(&s, key), None, "{what}: released {key:?} is forgotten (reopened: {reopened})");
                assert_eq!(s.get(key).unwrap(), None, "{what}: released {key:?} (reopened: {reopened})");
                for owner in t.owners(t.map.range_index(key)) {
                    let o = t.lc.store_zone(owner, 42).unwrap().borrow();
                    assert_eq!(o.get_versioned(key).unwrap(), *want, "{what}: owner {owner} reads {key:?} as the releasing node did");
                    if *covered {
                        assert_eq!(physical(&o, key), None, "{what}: covered {key:?} is never handed off as a point entry");
                    }
                }
            }
            assert_eq!(s.accounted_range_tombstones().0, f, "{what}: A follows F (reopened: {reopened})");
            assert_accounted(&s, &f, 10, &t.map, &format!("{what} (reopened: {reopened})"));
        };
        check(&t, false);
        assert_eq!(t.cell.borrow().get_versioned(&k(330)).unwrap(), None, "{what}: a covered memtable entry does not resurrect");
        let next = t.pass(true);
        assert!(next.wrapped && !next.deferred && next.forget_plan.is_none() && next.handoff_rpcs == 0, "{what}: the next lap finds nothing to release");
        let t = t.reopen();
        check(&t, true);
    }
}

#[test]
fn a_reopen_between_the_forget_plan_and_its_rewrite_keeps_every_read() {
    for rts_in_runs in [false, true] {
        let what = format!("range tombstones in runs: {rts_in_runs}");
        let t = released_span(rts_in_runs);
        account(&t);
        let want_f = t.cell.borrow().engine().range_tombstones().clone();
        let before: Vec<(Vec<u8>, Option<Stored>)> = (0..1_000).map(|i| (k(i), t.cell.borrow().get_versioned(&k(i)).unwrap())).collect();
        let plan = t.pass(true).forget_plan.expect("a wrapped pass plans the forget");
        t.cell.borrow_mut().abort_forget_plan(&plan.segments());
        let t = t.reopen();
        let s = t.cell.borrow();
        assert_eq!(*s.engine().range_tombstones(), want_f, "{what}: nothing is clipped before the rewrite commits");
        for (key, want) in &before {
            assert_eq!(s.get_versioned(key).unwrap(), *want, "{what}: {key:?}");
        }
        assert_accounted(&s, &want_f, 10, &t.map, &what);
    }
}

#[test]
fn fragment_handoffs_split_the_window_at_the_per_pass_rpc_cap() {
    let t = released_span(true);
    for i in 0..150 {
        let (lo, hi) = ([k(260 + i), b"a".to_vec()].concat(), [k(260 + i), b"b".to_vec()].concat());
        assert_eq!(t.cell.borrow_mut().apply_range_delete(Some(&lo), Some(&hi), at(3_000 + i), 0).unwrap(), Applied::Won);
    }
    account(&t);
    let want_f = t.cell.borrow().engine().range_tombstones().clone();
    let (lo, hi) = t.bounds(1);
    assert!(t.owners(1).len() * want_f.clipped(lo, hi).len() > crate::antientropy::CLEANUP_MAX_HANDOFF_RPCS_PER_PASS, "premise");
    let stats = commit_forget(&t, t.pass_from(Some(k(0)), true).forget_plan.expect("the lap plans the forget"));
    assert!(stats.released_range_tombstones > 150);
    for owner in t.owners(1) {
        let o = t.lc.store_zone(owner, 42).unwrap().borrow();
        assert_eq!(o.engine().range_tombstones().clipped(lo, hi), want_f.clipped(lo, hi), "owner {owner}");
    }
    assert_eq!(t.cell.borrow().engine().range_tombstones().clipped(lo, hi), Vec::new());
}
