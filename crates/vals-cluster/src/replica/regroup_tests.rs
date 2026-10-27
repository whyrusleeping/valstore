use super::*;
use crate::ring::Ring;
use crate::zone::{Ordering, ZoneConfig};
use std::collections::{BTreeMap, HashSet};
use vals_spdk::MemBlockDevice;

type Store = VersionedStore<MemBlockDevice>;
type Groups = BTreeMap<Vec<NodeId>, Vec<u64>>;

fn store() -> Store {
    Store::new(Engine::create_prewiped(MemBlockDevice::new(256, 1 << 16), vals_store::Config {
        seg_blocks: 8, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }), 1).unwrap().with_max_skew_ms(0)
}

fn cfg() -> ZoneConfig {
    ZoneConfig { name: "regroup-test".into(), rf: 2, ordering: Ordering::Sorted, scope: None }
}

fn ring() -> Ring {
    Ring::from_members(2, 64, [1, 2, 3])
}

fn prepare(s: &Store, epoch: u64, map: &RangeMap) {
    s.prepare_zone_seed((1, 1, epoch), &ring(), 42, Some(cfg()), Some(map.clone()));
}

fn ver(i: u64) -> Version {
    Version { hlc: i << 16, node: 1 }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn base_map() -> RangeMap {
    let mut map = RangeMap::from_splits([key(400), key(800)]);
    map.enable_opaque_tokens();
    map
}

fn drain_seed(s: &Store) {
    for _ in 0..100_000 {
        if s.seed_step_cooperative().unwrap() { return; }
    }
    panic!("seed did not converge");
}

fn recount(s: &Store) {
    for _ in 0..100_000 {
        if s.logical_recount_step().unwrap() == RecountStep::Done { return; }
    }
    panic!("recount did not converge");
}

fn ready(s: &Store) {
    drain_seed(s);
    s.finish_regroup().unwrap();
    recount(s);
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(s, 1, &HashSet::from([0])), || {}).unwrap());
}

fn populated(n: u64) -> Store {
    let mut s = store();
    for i in 0..n {
        s.apply_at(&key(i), Some(b"v"), ver(i + 1), 0).unwrap();
        if i % 150 == 149 { s.flush().unwrap(); }
    }
    for i in (0..n).step_by(7) {
        s.apply_at(&key(i), Some(b"v2"), ver(n + i + 1), 0).unwrap();
    }
    s.flush().unwrap();
    for i in (0..n).step_by(11) {
        s.apply_at(&key(i), None, ver(2 * n + i + 1), 0).unwrap();
    }
    s
}

fn seeded(n: u64) -> Store {
    let s = populated(n);
    prepare(&s, 10, &base_map());
    ready(&s);
    s
}

fn sorted(mut group: Vec<NodeId>) -> Vec<NodeId> {
    group.sort_unstable();
    group
}

fn moved(map: &RangeMap, index: usize, salt: u64) -> Vec<u8> {
    let current = sorted(map.replicas_of_range(&ring(), 42, index, 2, None));
    (0..256u64)
        .map(|n| crate::partition::opaque_placement_token([(salt + n) as u8; 16]))
        .find(|token| token.as_slice() != map.token_of(index)
            && sorted(ring().replicas_in_zone(42, token, 2, None)) != current)
        .expect("some opaque token moves the range to another node set")
}

fn leaves(s: &Store) -> Groups {
    s.versions.leaves.borrow().iter()
        .filter(|(_, leaves)| leaves.iter().any(|&leaf| leaf != 0))
        .map(|(group, leaves)| (group.to_vec(), leaves.to_vec()))
        .collect()
}

fn bucket_counts(s: &Store) -> Vec<u64> {
    s.versions.buckets.borrow().iter().map(|b| b.logical_keys).collect()
}

fn golden(s: &Store) -> (Groups, Vec<u64>, u64) {
    assert!(!s.regroup_pending());
    let mut groups = Groups::new();
    let mut counts = vec![0u64; INDEX_BUCKETS as usize];
    let mut live = 0;
    for (key, raw) in s.engine().recovered_versions().unwrap() {
        let b = VersionIndex::slot(&key);
        groups.entry(s.group_of(&key)).or_insert_with(|| vec![0; INDEX_BUCKETS as usize])[b] ^=
            crate::antientropy::entry_digest(&key, unpack_version(raw));
        counts[b] += 1;
        live += 1;
    }
    groups.retain(|_, leaves| leaves.iter().any(|&leaf| leaf != 0));
    (groups, counts, live)
}

fn exact_counts(s: &Store, map: &RangeMap) -> Vec<u64> {
    let mut counts = vec![0; map.num_ranges()];
    for (key, _) in s.engine().recovered_versions().unwrap() {
        counts[map.range_index(&key)] += 1;
    }
    counts
}

fn exact(s: &Store) -> bool {
    let (groups, counts, live) = golden(s);
    leaves(s) == groups && bucket_counts(s) == counts && s.versions.live.get() == live
}

fn mutate(s: &mut Store, next: &mut u64, keys: &[u64], tag: &str) {
    for (n, &k) in keys.iter().enumerate() {
        *next += 1;
        s.apply_at(format!("k{k:05}-{tag}").as_bytes(), Some(b"n"), ver(*next), 0).unwrap();
        *next += 1;
        let value: Option<&[u8]> = if n % 2 == 0 { None } else { Some(b"o") };
        s.apply_at(&key(k), value, ver(*next), 0).unwrap();
    }
}

fn cursor(s: &Store) -> Option<Vec<u8>> {
    s.regroup.borrow().as_ref()?.spans.first()?.after.as_deref().map(<[u8]>::to_vec)
}

fn cursor_index(s: &Store) -> Option<u64> {
    let after = cursor(s)?;
    std::str::from_utf8(after.get(1..6)?).ok()?.parse().ok()
}

fn prefetched_step(s: &Store) -> RecountStep {
    let Some(plan) = s.regroup_prefetch_plan().unwrap() else { return s.regroup_step().unwrap() };
    let dev = s.device_handle();
    let fetched = vals_rt::block_on(plan.read(dev.as_ref()), || {});
    s.regroup_prefetched_step(fetched).expect("an unchanged store accepts its own prefetch").unwrap()
}

#[derive(Debug, Default)]
struct Stats {
    regroups: u64,
    compositions: u64,
    overlaps: u64,
    depth_fallbacks: u64,
    unexpected_reseeds: u64,
    mismatches: u64,
    in_flight_writes: u64,
    steps_mid_span: u64,
    async_mid_span: u64,
    staged_mid_span: u64,
    discards: u64,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.regroups += o.regroups;
        self.compositions += o.compositions;
        self.overlaps += o.overlaps;
        self.depth_fallbacks += o.depth_fallbacks;
        self.unexpected_reseeds += o.unexpected_reseeds;
        self.mismatches += o.mismatches;
        self.in_flight_writes += o.in_flight_writes;
        self.steps_mid_span += o.steps_mid_span;
        self.async_mid_span += o.async_mid_span;
        self.staged_mid_span += o.staged_mid_span;
        self.discards += o.discards;
    }
}

fn change_placement(s: &Store, epoch: &mut u64, map: &RangeMap, stats: &mut Stats, reseeded: &mut bool) {
    *epoch += 1;
    let overlap = s.regroup_pending();
    let capped = s.regroup.borrow().as_ref().is_some_and(|r| r.depth >= super::lifecycle::MAX_REGROUP_COMPOSITIONS);
    let seeded_before = s.is_leaf_seeded();
    prepare(s, *epoch, map);
    stats.overlaps += u64::from(overlap);
    if s.regroup_pending() {
        if overlap {
            stats.compositions += u64::from(s.regroup.borrow().as_ref().is_some_and(|r| r.depth > 0));
        } else {
            stats.regroups += 1;
        }
    }
    if seeded_before && !s.is_leaf_seeded() {
        *reseeded = true;
        if overlap && capped {
            stats.depth_fallbacks += 1;
        } else {
            stats.unexpected_reseeds += 1;
        }
    }
}

fn random_rehomes(seed: u64, knockout: RegroupKnockout) -> Stats {
    let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut rand = |n: u64| {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng % n
    };
    let mut stats = Stats::default();
    let mut s = seeded(1200);
    s.set_gc_grace_ms(1);
    s.regroup_knockout.set(knockout);
    let (_, folds_before, _) = s.seed_test_counts();
    let mut map = base_map();
    let mut epoch = 10;
    let mut next = 10_000_000u64 * seed;
    let mut pending: Option<usize> = None;
    let mut reseeded = false;
    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
    for op in 0..240 {
        match rand(17) {
            0 | 1 if pending.is_none() => {
                let index = rand(map.num_ranges() as u64) as usize;
                let token = moved(&map, index, rand(200));
                assert!(map.set_pending(index, token));
                pending = Some(index);
                change_placement(&s, &mut epoch, &map, &mut stats, &mut reseeded);
            }
            0 | 1 => {
                let index = pending.take().unwrap();
                if rand(4) == 0 { assert!(map.clear_pending(index)) } else { assert!(map.commit_pending(index)) }
                change_placement(&s, &mut epoch, &map, &mut stats, &mut reseeded);
            }
            2 => {
                if map.split(&key(rand(1200))) {
                    change_placement(&s, &mut epoch, &map, &mut stats, &mut reseeded);
                }
            }
            3 | 4 => {
                let keys = [rand(1200), rand(1200), rand(1200)];
                mutate(&mut s, &mut next, &keys, &format!("r{op}"));
            }
            5 => {
                let mut keys: HashSet<Vec<u8>> = (0..3).map(|_| key(rand(1200)))
                    .filter(|key| s.current_version(key).unwrap().is_some()).collect();
                if let Some(at) = cursor(&s) { keys.insert(at); }
                s.forget(&keys).unwrap();
            }
            6 => {
                s.advance_durable_gc_watermark(next + 2);
                next += 10;
                s.flush().unwrap();
                s.compact_runs(0).unwrap();
            }
            7 => { s.flush().unwrap(); }
            8 => {
                let cell = std::cell::RefCell::new(s);
                let gate = crate::net::ReadGate::new();
                let at = cursor_index(&cell.borrow());
                let flush = rand(3) == 0;
                let step = vals_rt::block_on(regroup_turn_with(&cell, Some(&gate), || {
                    let mut s = cell.borrow_mut();
                    if let Some(at) = at {
                        for (k, value) in [(at, None), (at + 1, Some(b"n".as_slice())), (at + 2, Some(b"m".as_slice()))] {
                            next += 1;
                            s.apply_at(&key(k), value, ver(next), 0).unwrap();
                        }
                        stats.in_flight_writes += 3;
                    }
                    if flush { s.flush().unwrap(); }
                }), || {}).unwrap();
                assert_ne!(step, RecountStep::Abandoned);
                s = cell.into_inner();
            }
            9 => {
                for _ in 0..3 {
                    if let Some(index) = pending.take() {
                        assert!(map.commit_pending(index));
                    } else {
                        let index = rand(map.num_ranges() as u64) as usize;
                        assert!(map.set_pending(index, moved(&map, index, rand(200))));
                        pending = Some(index);
                    }
                    change_placement(&s, &mut epoch, &map, &mut stats, &mut reseeded);
                    if rand(2) == 0 {
                        let _ = s.regroup_step().unwrap();
                    }
                }
            }
            10 => {
                if let Some(plan) = s.flush_plan().unwrap() {
                    let written = vals_rt::block_on(vals_store::build_and_write_flush(plan, wdev.as_ref()), || {}).unwrap();
                    stats.async_mid_span += u64::from(cursor(&s).is_some());
                    let _ = s.regroup_step().unwrap();
                    let near = cursor_index(&s).unwrap_or(5) + 1;
                    mutate(&mut s, &mut next, &[rand(1200), near], &format!("f{op}"));
                    let _ = s.regroup_step().unwrap();
                    s.flush_commit(written).unwrap();
                }
            }
            11 => {
                s.flush().unwrap();
                s.advance_durable_gc_watermark(next + 2);
                next += 10;
                if let Some(plan) = s.compact_plan(0).unwrap() {
                    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev.clone()), || {}).unwrap();
                    stats.async_mid_span += u64::from(cursor(&s).is_some());
                    let _ = s.regroup_step().unwrap();
                    let near = cursor_index(&s).unwrap_or(5) + 1;
                    mutate(&mut s, &mut next, &[rand(1200), near], &format!("c{op}"));
                    let _ = s.regroup_step().unwrap();
                    s.compact_commit(written).unwrap();
                }
            }
            12 => {
                let k = key(cursor_index(&s).map_or(rand(1200), |at| at + 1 + rand(3)));
                next += 1;
                let version = ver(next);
                match s.apply_stage(&k, Some(b"s"), version, 0).unwrap() {
                    StagedApply::Done(_) => {}
                    StagedApply::NeedsRunRead { probe, gen } => {
                        stats.staged_mid_span += u64::from(cursor(&s).is_some());
                        let _ = s.regroup_step().unwrap();
                        let dev = s.device_handle();
                        let run_old = vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
                        if let StagedApplyOutcome::Retry = s.apply_commit_staged(&k, Some(b"s"), version, run_old, gen, 0).unwrap() {
                            s.apply_at(&k, Some(b"s"), version, 0).unwrap();
                        }
                    }
                }
            }
            13 => {
                if let Some(plan) = s.regroup_prefetch_plan().unwrap() {
                    let dev = s.device_handle();
                    let blocks = vals_rt::block_on(plan.read(dev.as_ref()), || {});
                    if s.regroup_step().unwrap() == RecountStep::More {
                        assert!(s.regroup_prefetched_step(blocks).is_none(), "a prefetch planned before the cursor moved is discarded");
                        stats.discards += 1;
                    }
                }
            }
            _ => {
                for _ in 0..1 + rand(4) {
                    stats.steps_mid_span += u64::from(cursor(&s).is_some());
                    let step = if rand(2) == 0 { prefetched_step(&s) } else { s.regroup_step().unwrap() };
                    if step == RecountStep::Done { break; }
                }
            }
        }
        if !s.is_leaf_seeded() {
            drain_seed(&s);
        }
        if !s.regroup_pending() && s.is_leaf_seeded() && !exact(&s) {
            stats.mismatches += 1;
        }
    }
    ready(&s);
    if !exact(&s) {
        stats.mismatches += 1;
    }
    let (_, folds_after, _) = s.seed_test_counts();
    if !reseeded {
        assert_eq!(folds_after, folds_before, "seed {seed}: no full seed folds without a reseed");
    }
    let counts = s.logical_range_counts(epoch);
    let snapshot = (leaves(&s), bucket_counts(&s), s.versions.live.get());
    if counts.as_ref() != Some(&exact_counts(&s, &map)) {
        stats.mismatches += 1;
    }
    s.regroup_knockout.set(RegroupKnockout::None);
    s.invalidate_leaf_seed();
    s.logical_range_counts.borrow_mut().as_mut().unwrap().counts = None;
    ready(&s);
    if s.logical_range_counts(epoch) != counts
        || (leaves(&s), bucket_counts(&s), s.versions.live.get()) != snapshot
    {
        stats.mismatches += 1;
    }
    stats
}

#[test]
fn rehome_regroups_match_a_from_scratch_seed_without_reseeding() {
    let mut total = Stats::default();
    for seed in 1..=24u64 {
        let stats = random_rehomes(seed, RegroupKnockout::None);
        assert_eq!(stats.mismatches, 0, "seed {seed}: leaves, counts and live equal a from-scratch seed: {stats:?}");
        assert_eq!(stats.unexpected_reseeds, 0, "seed {seed}: back-to-back placement changes compose instead of reseeding: {stats:?}");
        total.add(&stats);
    }
    assert!(total.regroups >= 150, "rehome transitions must start regroups: {total:?}");
    assert!(total.compositions >= 150, "back-to-back changes must compose into in-flight regroups: {total:?}");
    assert!(total.in_flight_writes >= 100 && total.steps_mid_span >= 200, "{total:?}");
    assert!(total.async_mid_span >= 20 && total.staged_mid_span >= 20 && total.discards >= 20, "{total:?}");
}

#[test]
fn knockout_routing_unscanned_keys_with_the_new_placement_is_detected() {
    let mismatches: u64 = (1..=8u64).map(|seed| random_rehomes(seed, RegroupKnockout::Misroute).mismatches).sum();
    assert!(mismatches > 0, "the exactness check must catch new-placement routing of unscanned keys");
}

#[test]
fn knockout_composing_without_the_unscanned_spans_is_detected() {
    let mismatches: u64 = (1..=8u64).map(|seed| random_rehomes(seed, RegroupKnockout::ComposeNewSpansOnly).mismatches).sum();
    assert!(mismatches > 0, "a composition must keep rescanning the old spans' unscanned keys");
}

#[test]
fn knockout_no_composition_reseeds_on_back_to_back_changes() {
    let mut total = Stats::default();
    for seed in 1..=4u64 {
        total.add(&random_rehomes(seed, RegroupKnockout::NoCompose));
    }
    assert_eq!(total.mismatches, 0);
    assert!(total.unexpected_reseeds > 0, "without composition a change during a regroup reseeds: {total:?}");
}

#[test]
fn knockout_whole_engine_invalidation_reseeds_on_every_rehome() {
    let _off = seed_policy::span_regroup_for_test(false);
    let stats = random_rehomes(3, RegroupKnockout::None);
    assert_eq!(stats.mismatches, 0);
    assert_eq!(stats.regroups, 0);
    assert!(stats.unexpected_reseeds > 0, "the old path reseeds the whole engine: {stats:?}");
}

#[test]
fn single_rehome_moves_only_the_changed_span_and_gates_tree_serving() {
    let mut s = seeded(1200);
    let map = base_map();
    let leaves_before = leaves(&s);
    let (_, folds, _) = s.seed_test_counts();
    let mut pending = map.clone();
    assert!(pending.set_pending(1, moved(&map, 1, 0)));
    let generation = s.placement_generation.get();
    prepare(&s, 11, &pending);
    assert!(s.is_leaf_seeded() && s.regroup_pending(), "the rehome keeps the seed and starts a regroup");
    assert_ne!(s.placement_generation.get(), generation);
    assert_eq!(s.logical_range_counts(11), Some(exact_counts(&s, &pending)));
    assert_eq!(leaves(&s), leaves_before, "no leaf moves before the first page");
    s.set_seed_driver(true);
    let busy = vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap();
    assert!(!busy, "tree serving answers not-ready while the regroup is in flight");
    let token = s.tree_scope_token(1, &HashSet::from([0]));
    assert!(!s.tree_scope_token_valid(&token));
    let mut next = 50_000_000;
    let mut pages = 0;
    loop {
        let at = cursor_index(&s);
        let step = s.regroup_step().unwrap();
        pages += 1;
        if let Some(at) = at {
            mutate(&mut s, &mut next, &[at, at + 1, 400 + (at * 7) % 400, 1100], &format!("p{pages}"));
        }
        if step == RecountStep::Done { break; }
    }
    assert!(pages >= 4, "the 400-key span takes several bounded pages: {pages}");
    assert!(!s.regroup_pending());
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap());
    assert!(exact(&s));
    let (_, folds_after, _) = s.seed_test_counts();
    assert_eq!(folds_after, folds, "no full seed");
    let moved_groups: Vec<_> = leaves(&s).into_keys().filter(|g| !leaves_before.contains_key(g)).collect();
    assert!(!moved_groups.is_empty(), "the span's winners moved into the new replica group");
    let mut committed = pending.clone();
    assert!(committed.commit_pending(1));
    prepare(&s, 12, &committed);
    assert!(s.is_leaf_seeded() && s.regroup_pending());
    s.set_seed_driver(false);
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap(),
        "without a seed driver the caller drives the regroup inline");
    assert!(!s.regroup_pending());
    assert!(exact(&s));
    assert_eq!(s.logical_range_counts(12), Some(exact_counts(&s, &committed)));
}

#[test]
fn back_to_back_changes_compose_until_the_depth_cap() {
    let s = seeded(1200);
    let mut map = base_map();
    let mut epoch = 10;
    let mut pending: Option<usize> = None;
    for change in 0..=super::lifecycle::MAX_REGROUP_COMPOSITIONS {
        if let Some(index) = pending.take() {
            assert!(map.commit_pending(index));
        } else {
            let index = change as usize % map.num_ranges();
            assert!(map.set_pending(index, moved(&map, index, change as u64 * 7)));
            pending = Some(index);
        }
        epoch += 1;
        prepare(&s, epoch, &map);
        assert!(s.is_leaf_seeded(), "change {change} composes instead of reseeding");
        assert_eq!(s.regroup.borrow().as_ref().unwrap().depth, change);
        assert_eq!(s.regroup_step().unwrap(), RecountStep::More);
    }
    let index = pending.take().unwrap();
    assert!(map.commit_pending(index));
    prepare(&s, epoch + 1, &map);
    assert!(!s.regroup_pending() && !s.is_leaf_seeded(), "past the depth cap a change falls back to a reseed");
    ready(&s);
    assert!(exact(&s));

    let s = seeded(1200);
    let mut pending = base_map();
    assert!(pending.set_pending(0, moved(&base_map(), 0, 0)));
    prepare(&s, 11, &pending);
    assert_eq!(s.regroup_step().unwrap(), RecountStep::More);
    let mut committed = pending.clone();
    assert!(committed.commit_pending(0));
    prepare(&s, 12, &committed);
    assert!(s.regroup_pending() && s.is_leaf_seeded());
    s.finish_regroup().unwrap();
    assert!(exact(&s));
}

#[test]
fn dirty_buckets_during_a_regroup_are_skipped_and_rebuilt_exactly() {
    let mut s = seeded(1200);
    s.logical_range_counts.borrow_mut().as_mut().unwrap().counts = None;
    let mut next = 60_000_000u64;
    for k in [410u64, 455, 610, 777, 20] {
        next += 1;
        s.append_unknown_for_test(&key(k), Some(b"u"), ver(next)).unwrap();
    }
    assert!(s.is_leaf_seeded());
    let dirty: HashSet<usize> = s.versions.dirty.borrow().keys().copied().collect();
    assert!(!dirty.is_empty());
    let same_bucket: Vec<Vec<u8>> = (0..200_000u64)
        .map(|i| format!("k{:05}-d{i}", 400 + i % 400).into_bytes())
        .filter(|k| dirty.contains(&VersionIndex::slot(k)))
        .take(24)
        .collect();
    assert!(same_bucket.len() >= 8);
    let mut pending = base_map();
    assert!(pending.set_pending(1, moved(&base_map(), 1, 0)));
    prepare(&s, 11, &pending);
    assert!(s.regroup_pending() && s.is_leaf_seeded());
    let mut written = 0;
    loop {
        for k in same_bucket.iter().skip(written).take(3) {
            next += 1;
            s.apply_at(k, Some(b"d"), ver(next), 0).unwrap();
        }
        written += 3;
        next += 1;
        s.append_unknown_for_test(&key(400 + written as u64 * 11 % 400), Some(b"u2"), ver(next)).unwrap();
        if written % 6 == 0 {
            s.flush().unwrap();
        }
        if s.regroup_step().unwrap() == RecountStep::Done { break; }
    }
    assert!(written >= 9, "writes landed in dirty buckets across several pages");
    assert!(s.versions.has_dirty());
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap());
    assert!(!s.versions.has_dirty());
    assert!(exact(&s), "the rebuild after the regroup groups every key by the final placement");
}

struct FlakyAsyncRead {
    inner: MemBlockDevice,
    failing: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl BlockDevice for FlakyAsyncRead {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> { self.inner.read_at(lba, buf) }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.inner.write_at(lba, buf) }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
}

impl vals_spdk::AsyncRead for FlakyAsyncRead {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        if self.failing.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(vals_spdk::BlockError::Io(std::io::Error::other("injected prefetch read failure")));
        }
        self.inner.read_at(lba, buf)
    }
}

#[test]
fn a_failed_prefetch_read_falls_back_to_the_sync_page() {
    let failing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let dev = FlakyAsyncRead { inner: MemBlockDevice::new(256, 1 << 16), failing: failing.clone() };
    let mut s = VersionedStore::new(Engine::create_prewiped(dev, vals_store::Config {
        seg_blocks: 8, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }), 1).unwrap().with_max_skew_ms(0);
    for i in 0..1200 {
        s.apply_at(&key(i), Some(b"v"), ver(i + 1), 0).unwrap();
        if i % 150 == 149 { s.flush().unwrap(); }
    }
    s.flush().unwrap();
    s.prepare_zone_seed((1, 1, 10), &ring(), 42, Some(cfg()), Some(base_map()));
    while !s.seed_step_cooperative().unwrap() {}
    let mut pending = base_map();
    assert!(pending.set_pending(1, moved(&base_map(), 1, 0)));
    s.prepare_zone_seed((1, 1, 11), &ring(), 42, Some(cfg()), Some(pending));
    assert!(s.regroup_pending());
    let errors = crate::observe::counters().ae_regroup_prefetch_errors.get();
    let mut failed_pages = 0;
    loop {
        let plan = s.regroup_prefetch_plan().unwrap().expect("a pending regroup plans a prefetch");
        failing.store(true, std::sync::atomic::Ordering::Relaxed);
        let dev = s.device_handle();
        let blocks = vals_rt::block_on(plan.read(dev.as_ref()), || {});
        failing.store(false, std::sync::atomic::Ordering::Relaxed);
        let step = s.regroup_prefetched_step(blocks).expect("a fresh prefetch is not stale").unwrap();
        failed_pages += 1;
        assert!(s.is_leaf_seeded(), "a prefetch read error never invalidates the leaves");
        if step == RecountStep::Done { break; }
    }
    assert!(failed_pages >= 4);
    assert!(crate::observe::counters().ae_regroup_prefetch_errors.get() > errors);
    let mut groups = Groups::new();
    for (key, raw) in s.engine().recovered_versions().unwrap() {
        groups.entry(s.group_of(&key)).or_insert_with(|| vec![0; INDEX_BUCKETS as usize])[VersionIndex::slot(&key)] ^=
            crate::antientropy::entry_digest(&key, unpack_version(raw));
    }
    let got: Groups = s.versions.leaves.borrow().iter()
        .map(|(group, leaves)| (group.to_vec(), leaves.to_vec())).collect();
    assert_eq!(got, groups);
}

#[test]
fn rehome_of_a_span_this_store_holds_no_key_of_neither_regroups_nor_reseeds() {
    let mut s = store();
    for i in 0..300 {
        s.apply_at(&key(i), Some(b"v"), ver(i + 1), 0).unwrap();
    }
    prepare(&s, 10, &base_map());
    ready(&s);
    let mut pending = base_map();
    assert!(pending.set_pending(2, moved(&base_map(), 2, 0)));
    prepare(&s, 11, &pending);
    assert!(s.is_leaf_seeded() && !s.regroup_pending(), "local reuse needs no regroup");
    assert!(exact(&s));
}
