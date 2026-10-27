use super::*;
use crate::ring::Ring;
use crate::zone::{Ordering, ZoneConfig};
use std::collections::HashSet;
use vals_spdk::MemBlockDevice;

type Store = VersionedStore<MemBlockDevice>;

fn store() -> Store {
    Store::new(Engine::create_prewiped(MemBlockDevice::new(256, 1 << 16), vals_store::Config {
        seg_blocks: 8, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }), 1).unwrap().with_max_skew_ms(0)
}

fn cfg() -> ZoneConfig {
    ZoneConfig { name: "recount-test".into(), rf: 2, ordering: Ordering::Sorted, scope: None }
}

fn ring() -> Ring {
    Ring::from_members(2, 64, [1, 2, 3])
}

fn prepare(s: &Store, epoch: u64, map: &RangeMap) {
    s.prepare_zone_seed((1, 1, epoch), &ring(), 42, Some(cfg()), Some(map.clone()));
}

fn drain(s: &Store) {
    for _ in 0..100_000 {
        if s.seed_step_cooperative().unwrap() { return; }
    }
    panic!("seed did not converge");
}

thread_local! {
    static STEP_PARITY: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

fn prefetch_step(s: &Store) -> Result<RecountStep, EngineError> {
    let Some(plan) = s.logical_recount_prefetch_plan()? else { return s.logical_recount_step() };
    let dev = s.device_handle();
    let fetched = vals_rt::block_on(plan.read(dev.as_ref()), || {});
    s.logical_recount_prefetched_step(fetched, std::time::Duration::ZERO)
        .expect("an unchanged store accepts its own prefetch")
}

fn step(s: &Store) -> bool {
    let parity = STEP_PARITY.with(|p| {
        p.set(p.get() + 1);
        p.get()
    });
    let stepped = if parity.is_multiple_of(2) { prefetch_step(s) } else { s.logical_recount_step() };
    match stepped.unwrap() {
        RecountStep::Done => true,
        RecountStep::More => false,
        other => panic!("unexpected recount step {other:?}"),
    }
}

fn recount(s: &Store) -> usize {
    let mut steps = 0;
    while !step(s) {
        steps += 1;
        assert!(steps < 100_000, "recount did not converge");
    }
    steps
}

fn rebuild(s: &Store) {
    drain(s);
    recount(s);
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(s, 1, &HashSet::from([0])), || {}).unwrap());
}

fn ver(i: u64) -> Version {
    Version { hlc: i << 16, node: 1 }
}

fn key(i: u64) -> Vec<u8> {
    format!("k{i:05}").into_bytes()
}

fn exact_counts(s: &Store, map: &RangeMap) -> Vec<u64> {
    let mut counts = vec![0; map.num_ranges()];
    for (key, _) in s.engine().recovered_versions().unwrap() {
        counts[map.range_index(&key)] += 1;
    }
    counts
}

fn golden(s: &Store) -> Vec<u64> {
    let entries: Vec<_> = s.engine().recovered_versions().unwrap().into_iter()
        .map(|(k, v)| (k.to_vec(), unpack_version(v))).collect();
    crate::antientropy::leaf_digests_versioned(entries.iter().map(|(k, v)| (k.as_slice(), *v)), INDEX_BUCKETS)
}

fn assert_exact(s: &Store, epoch: u64, map: &RangeMap) {
    assert!(vals_rt::block_on(ensure_tree_ready_cooperative(s, 1, &HashSet::from([0])), || {}).unwrap());
    assert_eq!(s.logical_range_counts(epoch), Some(exact_counts(s, map)));
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), golden(s));
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
    for i in (3..n).step_by(13) {
        s.apply_at(&key(i), Some(b"mt"), ver(3 * n + i + 1), 0).unwrap();
    }
    s
}

fn old_map() -> RangeMap {
    RangeMap::from_splits([key(100), key(1000)])
}

fn split_at(map: &RangeMap, cut: &[u8]) -> RangeMap {
    let mut next = map.clone();
    assert!(next.split(cut));
    next
}

fn seeded(n: u64) -> Store {
    let s = populated(n);
    prepare(&s, 10, &old_map());
    rebuild(&s);
    assert_eq!(s.logical_range_counts(10), Some(exact_counts(&s, &old_map())));
    s
}

fn scanning(s: &Store) -> Option<(usize, Option<Vec<u8>>, u64)> {
    let accounting = s.logical_range_counts.borrow();
    let r = accounting.as_ref()?.recounts.first()?;
    let i = r.next_scan()?;
    match r.children[i] {
        RecountChild::Scan { ref after, covered, .. } => Some((i, after.as_deref().map(<[u8]>::to_vec), covered)),
        RecountChild::Derived => None,
    }
}

fn cursor(s: &Store) -> Option<(bool, u64)> {
    let (i, after, _) = scanning(s)?;
    let after = after?;
    let n = std::str::from_utf8(&after[1..6]).ok()?.parse().ok()?;
    Some((i == 1, n))
}

fn pending(s: &Store) -> usize {
    s.logical_range_counts.borrow().as_ref().map_or(0, |state| state.recounts.len())
}

fn kinds(s: &Store) -> Vec<String> {
    let accounting = s.logical_range_counts.borrow();
    accounting.as_ref().map_or(Vec::new(), |state| {
        state.recounts.iter().map(|r| r.children.iter().map(|child| match child {
            RecountChild::Derived => 'D',
            RecountChild::Scan { done: true, .. } => 'K',
            RecountChild::Scan { after: Some(_), .. } => 'P',
            RecountChild::Scan { .. } => 'F',
        }).collect()).collect()
    })
}

fn progress(s: &Store) -> u64 {
    let accounting = s.logical_range_counts.borrow();
    accounting.as_ref().map_or(0, |state| {
        state.recounts.iter().flat_map(|r| &r.children).map(|child| match child {
            RecountChild::Scan { covered, done: false, .. } => *covered,
            _ => 0,
        }).sum()
    })
}

fn assert_bounds_bracket_exact(s: &Store, epoch: u64, map: &RangeMap) {
    let bounds = s.logical_range_count_bounds(epoch).expect("published counts");
    let exact = exact_counts(s, map);
    assert_eq!(bounds.len(), exact.len());
    for (i, ((lo, hi), n)) in bounds.into_iter().zip(exact).enumerate() {
        assert!(lo <= n && n <= hi, "range {i}: {n} outside [{lo}, {hi}]");
        let accounting = s.logical_range_counts.borrow();
        let accounting = accounting.as_ref().unwrap();
        match accounting.span_child(map.range_bounds(i).0) {
            None => assert_eq!(lo, hi, "range {i} outside every span reports exactly"),
            Some((span, child)) if matches!(accounting.recounts[span].children[child],
                RecountChild::Scan { done: true, .. }) => {
                assert_eq!(lo, hi, "finished range {i} reports exactly");
            }
            _ => {}
        }
    }
}

fn mutate(s: &mut Store, next: &mut u64, keys: &[u64], tag: &str) -> u64 {
    let mut applies = 0;
    for (n, &k) in keys.iter().enumerate() {
        *next += 1;
        s.apply_at(format!("k{k:05}-{tag}").as_bytes(), Some(b"n"), ver(*next), 0).unwrap();
        *next += 1;
        let value: Option<&[u8]> = if n % 2 == 0 { None } else { Some(b"o") };
        s.apply_at(&key(k), value, ver(*next), 0).unwrap();
        applies += 2;
    }
    applies
}

fn assert_no_reseed(s: &Store, fallbacks: u64) {
    assert!(s.is_leaf_seeded(), "leaves stay seeded");
    assert!(s.seed_progress.borrow().is_none(), "no full seed starts");
    assert!(s.logical_range_counts.borrow().as_ref().unwrap().counts.is_some());
    assert_eq!(s.split_recount_fallbacks.get(), fallbacks, "no split recount fallback");
}

#[test]
fn populated_split_recounts_smaller_child_under_concurrent_mutation() {
    for (cut, expect_upper) in [(400u64, false), (700, true)] {
        let mut s = seeded(1200);
        assert!(s.run_count() >= 3);
        let old = old_map();
        let new = split_at(&old, &key(cut));
        let digest = s.version_leaf_digests(INDEX_BUCKETS).unwrap();
        let pgen = s.placement_generation.get();
        let lgen = s.logical_range_counts.borrow().as_ref().unwrap().generation;
        let smaller = {
            let exact = exact_counts(&s, &new);
            exact[1].min(exact[2])
        };
        prepare(&s, 11, &new);
        assert_eq!(s.logical_range_counts(11), None);
        assert!(s.is_leaf_seeded());
        assert!(s.seed_progress.borrow().is_none());
        assert_eq!(pending(&s), 1);
        assert_eq!(s.placement_generation.get(), pgen);
        assert_eq!(s.logical_range_counts.borrow().as_ref().unwrap().generation, lgen.wrapping_add(1));
        assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), digest);
        s.split_recount_visited.set(0);
        s.set_gc_grace_ms(1);
        let mut next = 1_000_000u64;
        let mut applies = 0u64;
        let mut dropped = 0;
        let mut forgotten = 0;
        let mut turn = 0u64;
        loop {
            let done = step(&s);
            assert!(s.seed_progress.borrow().is_none(), "no full seed during the recount");
            assert!(s.is_leaf_seeded());
            if done { break; }
            turn += 1;
            let Some((upper, at)) = cursor(&s) else { panic!("cursor missing") };
            assert_eq!(upper, expect_upper);
            let (lo, hi) = if upper { (cut, 1000) } else { (100, cut) };
            let other = if upper { 150 + turn } else { 900 - turn };
            let below = at.saturating_sub(3).max(lo);
            let above = (at + 3).min(hi - 1);
            for k in [below, above, other] {
                next += 1;
                s.apply_at(format!("k{k:05}-n{turn}").as_bytes(), Some(b"n"), ver(next), 0).unwrap();
            }
            next += 1;
            s.apply_at(&key(below), Some(b"over"), ver(next), 0).unwrap();
            next += 1;
            s.apply_at(&key(above), None, ver(next), 0).unwrap();
            next += 1;
            s.apply_at(&key(other), None, ver(next), 0).unwrap();
            applies += 6;
            match turn % 4 {
                1 => {
                    let after = scanning(&s).unwrap().1.unwrap();
                    let mut forget: HashSet<Vec<u8>> = [below.saturating_sub(1).max(lo), above + 1, other + 1]
                        .into_iter().map(key).collect();
                    forget.insert(after.clone());
                    forgotten += s.forget(&forget).unwrap();
                    if turn % 8 == 5 {
                        next += 1;
                        s.apply_at(&after, Some(b"back"), ver(next), 0).unwrap();
                        applies += 1;
                    }
                }
                2 => {
                    s.flush().unwrap();
                }
                3 => {
                    s.advance_durable_gc_watermark(next + 2);
                    next += 10;
                    s.flush().unwrap();
                    dropped += s.compact_runs(0).unwrap().current_dropped_tombstones.len();
                }
                _ => {
                    next += 1;
                    s.apply_at(&key(at), None, ver(next), 0).unwrap();
                    applies += 1;
                }
            }
        }
        assert!(turn >= 4, "fixture must span several pages");
        assert!(dropped > 0 && forgotten > 0);
        assert!(s.split_recount_visited.get() <= smaller + applies,
            "visited {} > smaller child {smaller} + applies {applies}", s.split_recount_visited.get());
        assert!(s.is_leaf_seeded());
        assert_exact(&s, 11, &new);
        let counts = s.logical_range_counts(11).unwrap();
        s.invalidate_leaf_seed();
        rebuild(&s);
        assert_eq!(s.logical_range_counts(11), Some(counts));
    }
}

#[test]
fn empty_parent_split_is_ready_immediately() {
    let s = seeded(90);
    let before = crate::observe::counters().logical_range_split_empty.get();
    let new = split_at(&old_map(), &key(500));
    prepare(&s, 11, &new);
    assert_eq!(s.logical_range_counts(11), Some(vec![90, 0, 0, 0]));
    assert_eq!(pending(&s), 0);
    assert!(s.is_leaf_seeded());
    assert!(crate::observe::counters().logical_range_split_empty.get() > before);
    assert_exact(&s, 11, &new);
}

#[test]
fn two_splits_before_any_step_recount_fifo() {
    let mut s = seeded(1200);
    let first = split_at(&old_map(), &key(400));
    let second = split_at(&first, &key(1100));
    prepare(&s, 11, &first);
    prepare(&s, 12, &second);
    assert_eq!(pending(&s), 2);
    assert!(s.is_leaf_seeded());
    assert_eq!(s.logical_range_counts(12), None);
    s.apply_at(b"k01150-late", Some(b"v"), ver(5_000_000), 0).unwrap();
    s.apply_at(b"k00050-late", Some(b"v"), ver(5_000_001), 0).unwrap();
    assert!(!step(&s));
    s.apply_at(b"k01050-late", Some(b"v"), ver(5_000_002), 0).unwrap();
    recount(&s);
    assert_eq!(pending(&s), 0);
    assert!(s.is_leaf_seeded());
    assert_exact(&s, 12, &second);
}

#[test]
fn unqualified_shapes_fall_back_to_full_invalidation() {
    for case in 0..7 {
        let mut s = seeded(1200);
        let fallbacks = s.split_recount_fallbacks.get();
        let first = split_at(&old_map(), &key(400));
        let (epoch, map, counted) = match case {
            0 => {
                prepare(&s, 11, &first);
                s.append_unknown_for_test(b"k00500-u", Some(b"v"), ver(9_000_000)).unwrap();
                (11, first.clone(), true)
            }
            1 => {
                s.logical_range_counts.borrow_mut().as_mut().unwrap().counts = None;
                (11, first.clone(), false)
            }
            2 => {
                s.split_recount_disabled.set(true);
                (11, first.clone(), true)
            }
            3 => (10, first.clone(), false),
            4 => {
                s.logical_range_counts.borrow_mut().as_mut().unwrap().counts = Some(vec![0]);
                (11, first.clone(), true)
            }
            5 => {
                s.logical_range_counts.borrow_mut().as_mut().unwrap().epoch = 0;
                (11, first.clone(), false)
            }
            6 => {
                *s.logical_range_counts.borrow_mut() = None;
                (11, first.clone(), false)
            }
            _ => unreachable!(),
        };
        s.set_logical_range_map(epoch, map.clone());
        assert_eq!(s.logical_range_counts(epoch), None, "case {case}");
        assert_eq!(pending(&s), 0, "case {case}");
        assert!(!s.is_leaf_seeded(), "case {case}");
        assert_eq!(s.split_recount_fallbacks.get() > fallbacks, counted,
            "case {case}: only genuine transitions count as fallbacks");
        rebuild(&s);
        assert_exact(&s, epoch, &map);
    }
}

#[test]
fn first_install_and_boot_reinstall_do_not_count_as_fallbacks() {
    let s = populated(300);
    prepare(&s, 0, &old_map());
    prepare(&s, 10, &old_map());
    let split = split_at(&old_map(), &key(200));
    s.set_logical_range_map(10, split.clone());
    s.set_logical_range_map(9, old_map());
    assert_eq!(s.split_recount_fallbacks.get(), 0);
    rebuild(&s);
    assert_exact(&s, 9, &old_map());
}

#[test]
fn merges_sum_in_constant_time_and_resolve_pending_cut_to_total() {
    let mut s = seeded(1200);
    let sums = crate::observe::counters().logical_range_merge_sums.get();
    let mut merged = old_map();
    assert!(merged.merge_at(&key(1000)));
    s.set_logical_range_map(11, merged.clone());
    assert_eq!(s.logical_range_counts(11), Some(exact_counts(&s, &merged)));
    assert!(s.is_leaf_seeded());
    let split = split_at(&merged, &key(400));
    s.set_logical_range_map(12, split);
    assert_eq!(pending(&s), 1);
    s.apply_at(b"k00500-x", Some(b"v"), ver(7_000_000), 0).unwrap();
    assert!(!step(&s));
    s.apply_at(b"k00150-x", Some(b"v"), ver(7_000_001), 0).unwrap();
    s.set_logical_range_map(13, merged.clone());
    assert_eq!(pending(&s), 0);
    assert_eq!(s.logical_range_counts(13), Some(exact_counts(&s, &merged)));
    assert!(s.is_leaf_seeded());
    assert!(crate::observe::counters().logical_range_merge_sums.get() - sums >= 2);
    assert_exact(&s, 13, &merged);
}

#[test]
fn split_during_in_flight_seed_with_counts_publishes_exact() {
    for (drive_recount_first, isolation) in [(false, true), (true, true), (false, false), (true, false)] {
        let _policy = seed_policy::policy_for_test(isolation, 25, 64 << 20);
        let mut s = seeded(1200);
        s.invalidate_leaf_seed();
        assert!(s.logical_range_counts(10).is_some());
        for _ in 0..6 {
            assert!(!s.seed_step_cooperative().unwrap());
        }
        assert!(s.seed_progress.borrow().is_some());
        let new = split_at(&old_map(), &key(400));
        prepare(&s, 11, &new);
        assert_eq!(pending(&s), 1);
        s.apply_at(b"k00200-s", Some(b"v"), ver(8_000_000), 0).unwrap();
        if drive_recount_first {
            recount(&s);
            assert_eq!(s.logical_range_counts(11), Some(exact_counts(&s, &new)));
        }
        drain(&s);
        let (resets, _, _) = s.seed_test_counts();
        if isolation {
            assert_eq!(resets, 0, "the in-flight seed survives the generation bump");
            recount(&s);
        } else {
            assert!(resets > 0, "knockout restarts the seed on the generation bump");
        }
        assert_eq!(pending(&s), 0);
        assert_eq!(s.logical_range_counts(11), Some(exact_counts(&s, &new)));
        assert_exact(&s, 11, &new);
    }
}

fn mid_seed_store(n: u64) -> Store {
    let s = populated(n);
    prepare(&s, 10, &old_map());
    assert!(!s.is_leaf_seeded());
    assert_eq!(s.logical_range_counts(10), None);
    s
}

fn assert_matches_from_scratch(s: &Store, epoch: u64, map: &RangeMap) {
    recount(s);
    assert_exact(s, epoch, map);
    let counts = s.logical_range_counts(epoch).unwrap();
    let leaves = s.version_leaf_digests(INDEX_BUCKETS).unwrap();
    let live = s.versions.live.get();
    s.invalidate_leaf_seed();
    s.logical_range_counts.borrow_mut().as_mut().unwrap().counts = None;
    rebuild(s);
    assert_eq!(s.logical_range_counts(epoch), Some(counts), "counts equal a from-scratch seed");
    assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), leaves, "leaves equal a from-scratch seed");
    assert_eq!(s.versions.live.get(), live);
}

#[test]
fn in_flight_seed_survives_random_generation_bumps_exactly() {
    let mut totals = [(0u64, 0u64, 0u64); 2];
    for seed in 1..=24u64 {
        for (arm, isolation) in [(0, true), (1, false)] {
            let _policy = seed_policy::policy_for_test(isolation, 25, 64 << 20);
            let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let mut rand = |n: u64| {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng % n
            };
            let mut s = mid_seed_store(1200);
            let mut map = old_map();
            let mut epoch = 10;
            let mut next = 20_000_000u64 * seed;
            let mut bumps_in_flight = 0;
            for op in 0..300 {
                let mut changed = false;
                match rand(10) {
                    0 => changed = map.split(&key(rand(1200))),
                    1 => {
                        for _ in 0..2 + rand(2) {
                            changed |= map.split(&key(rand(1200)));
                        }
                    }
                    2 => {
                        if !map.splits().is_empty() {
                            let at = map.splits()[rand(map.splits().len() as u64) as usize].clone();
                            changed = map.merge_at(&at);
                        }
                    }
                    3 => {
                        let keys = [rand(1200), rand(1200), rand(1200)];
                        mutate(&mut s, &mut next, &keys, &format!("g{op}"));
                    }
                    4 => {
                        let keys: HashSet<Vec<u8>> = (0..3).map(|_| key(rand(1200)))
                            .filter(|key| s.current_version(key).unwrap().is_some()).collect();
                        s.forget(&keys).unwrap();
                    }
                    5 => {
                        s.flush().unwrap();
                    }
                    _ => {
                        for _ in 0..1 + rand(4) {
                            if s.seed_step_cooperative().unwrap() { break; }
                        }
                    }
                }
                if changed {
                    epoch += 1;
                    bumps_in_flight += usize::from(s.seed_progress.borrow().is_some());
                    s.set_logical_range_map(epoch, map.clone());
                }
                if s.is_leaf_seeded() && pending(&s) == 0 {
                    assert_eq!(s.logical_range_counts(epoch), Some(exact_counts(&s, &map)),
                        "seed {seed} op {op} isolation {isolation}");
                }
            }
            drain(&s);
            let (resets, folds, _) = s.seed_test_counts();
            if isolation {
                assert_eq!(resets, 0, "seed {seed}: no generation reset under isolation");
            }
            totals[arm].0 += resets;
            totals[arm].1 += folds;
            totals[arm].2 += bumps_in_flight as u64;
            assert_matches_from_scratch(&s, epoch, &map);
        }
    }
    let [on, off] = totals;
    assert!(on.2 >= 48 && off.2 >= 48, "generation bumps must land on in-flight seeds: {totals:?}");
    assert!(off.0 > 0, "knockout restarts seeds on generation bumps: {totals:?}");
    assert!(on.1 < off.1, "carrying seeds folds less than restarting them: {totals:?}");
}

#[test]
fn ram_pressure_pauses_and_resumes_recount() {
    let s = seeded(1200);
    let new = split_at(&old_map(), &key(400));
    prepare(&s, 11, &new);
    let pauses = crate::observe::counters().logical_range_split_recount_ram_pauses.get();
    {
        let _pressure = crate::ram::override_for_test(2, 1);
        for _ in 0..3 {
            assert_eq!(s.logical_recount_step().unwrap(), RecountStep::Paused);
        }
        assert_eq!(kinds(&s), vec!["FD"]);
        assert_eq!(progress(&s), 0);
    }
    assert!(crate::observe::counters().logical_range_split_recount_ram_pauses.get() - pauses >= 3);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    assert!(!step(&s));
    let covered = progress(&s);
    assert!(covered > 0);
    {
        let _pressure = crate::ram::override_for_test(2, 1);
        assert_eq!(s.logical_recount_step().unwrap(), RecountStep::Paused);
        assert_eq!(progress(&s), covered);
    }
    recount(&s);
    assert_exact(&s, 11, &new);
}

#[test]
fn clear_while_recount_pending_is_exact_zero() {
    let mut s = seeded(1200);
    let new = split_at(&old_map(), &key(400));
    prepare(&s, 11, &new);
    assert!(!step(&s));
    s.clear().unwrap();
    assert_eq!(pending(&s), 0);
    assert_eq!(s.logical_range_counts(11), Some(vec![0; 4]));
    assert!(step(&s));
    s.apply_at(b"k00300-c", Some(b"v"), ver(9), 0).unwrap();
    assert_eq!(s.logical_range_counts(11), Some(vec![0, 1, 0, 0]));
}

#[test]
fn rehome_rebases_keep_pending_recount() {
    let s = seeded(1200);
    let mut new = split_at(&old_map(), &key(400));
    new.enable_opaque_tokens();
    s.set_logical_range_map(11, new.clone());
    assert_eq!(pending(&s), 1);
    assert!(!step(&s));
    let mut rehoming = new.clone();
    assert!(rehoming.set_opaque_pending(2, [4; 16]));
    s.set_logical_range_map(12, rehoming.clone());
    assert_eq!(pending(&s), 1);
    assert_eq!(s.logical_range_counts(12), None);
    assert!(!step(&s));
    let mut committed = rehoming.clone();
    assert!(committed.commit_pending(2));
    s.set_logical_range_map(13, committed.clone());
    assert_eq!(pending(&s), 1);
    assert!(s.is_leaf_seeded());
    recount(&s);
    assert_eq!(s.logical_range_counts(12), None);
    assert_exact(&s, 13, &committed);
}

fn finish_with_mutations(s: &mut Store, next: &mut u64, keys: &[u64], tag: &str) -> u64 {
    let mut applies = 0;
    let mut turn = 0usize;
    while !step(s) {
        turn += 1;
        assert!(turn < 100_000, "recount did not converge");
        let k = keys[turn % keys.len()];
        applies += mutate(s, next, &[k + turn as u64 % 7], &format!("{tag}{turn}"));
    }
    applies
}

#[test]
fn nested_split_of_pending_child_repartitions_without_reseed() {
    for variant in 0..3 {
        let mut s = seeded(1200);
        let fallbacks = s.split_recount_fallbacks.get();
        let repartitions = crate::observe::counters().logical_range_split_recount_repartitions.get();
        let mut next = 2_000_000u64;
        let mut applies = 0;
        s.split_recount_visited.set(0);
        let (epoch, map) = match variant {
            0 => {
                let first = split_at(&old_map(), &key(400));
                prepare(&s, 11, &first);
                assert_eq!(kinds(&s), vec!["FD"]);
                assert!(!step(&s));
                assert!(progress(&s) > 0);
                applies += mutate(&mut s, &mut next, &[150, 350, 600, 900], "a");
                let nested = split_at(&first, &key(800));
                prepare(&s, 12, &nested);
                assert_no_reseed(&s, fallbacks);
                assert_eq!(kinds(&s), vec!["PDF"], "the split derived child keeps one derived half");
                assert!(progress(&s) > 0, "scan progress in an untouched child is carried");
                assert_bounds_bracket_exact(&s, 12, &nested);
                assert!(!step(&s));
                applies += mutate(&mut s, &mut next, &[450, 650, 850], "b");
                let again = split_at(&nested, &key(600));
                prepare(&s, 13, &again);
                assert_no_reseed(&s, fallbacks);
                let k = kinds(&s);
                assert_eq!(k.len(), 1);
                assert_eq!(k[0].len(), 4);
                assert_eq!(k[0].matches('D').count(), 1);
                (13, again)
            }
            1 => {
                let first = split_at(&old_map(), &key(400));
                prepare(&s, 11, &first);
                assert!(!step(&s));
                assert!(!step(&s));
                applies += mutate(&mut s, &mut next, &[120, 390, 700], "a");
                let nested = split_at(&first, &key(250));
                prepare(&s, 12, &nested);
                assert_no_reseed(&s, fallbacks);
                assert_eq!(kinds(&s), vec!["FFD"], "a split scanned child restarts both halves");
                (12, nested)
            }
            _ => {
                let mut first = old_map();
                assert!(first.split(&key(300)));
                assert!(first.split(&key(600)));
                prepare(&s, 11, &first);
                assert_eq!(kinds(&s), vec!["FFD"], "two cuts in one parent share one span");
                while !kinds(&s)[0].starts_with('K') {
                    assert!(!step(&s));
                }
                applies += mutate(&mut s, &mut next, &[150, 250, 450, 800], "a");
                let nested = split_at(&first, &key(200));
                prepare(&s, 12, &nested);
                assert_no_reseed(&s, fallbacks);
                let k = kinds(&s);
                assert!(k == vec!["FFFD"] || k == vec!["FFPD"], "{k:?}");
                (12, nested)
            }
        };
        assert!(crate::observe::counters().logical_range_split_recount_repartitions.get() > repartitions);
        applies += finish_with_mutations(&mut s, &mut next, &[130, 330, 530, 730, 930], "z");
        assert_no_reseed(&s, fallbacks);
        assert_eq!(pending(&s), 0);
        assert!(s.split_recount_visited.get() <= 900 + applies,
            "variant {variant}: visited {} exceeds the parent plus applies {applies}", s.split_recount_visited.get());
        assert_exact(&s, epoch, &map);
    }
}

#[test]
fn cuts_in_different_parents_in_one_epoch_recount_independently() {
    let mut s = seeded(1200);
    let fallbacks = s.split_recount_fallbacks.get();
    let mut map = old_map();
    for k in [50, 400, 700, 1100] {
        assert!(map.split(&key(k)));
    }
    prepare(&s, 11, &map);
    assert_no_reseed(&s, fallbacks);
    let k = kinds(&s);
    assert_eq!(k.len(), 3);
    assert_eq!(k.iter().map(String::len).collect::<Vec<_>>(), vec![2, 3, 2]);
    assert!(k.iter().all(|span| span.matches('D').count() == 1));
    assert_bounds_bracket_exact(&s, 11, &map);
    let mut next = 5_000_000u64;
    mutate(&mut s, &mut next, &[10, 60, 150, 450, 750, 1050, 1150], "a");
    assert!(!step(&s));
    assert_bounds_bracket_exact(&s, 11, &map);
    finish_with_mutations(&mut s, &mut next, &[20, 70, 500, 800, 1120], "z");
    assert_no_reseed(&s, fallbacks);
    assert_exact(&s, 11, &map);
}

#[test]
fn merges_elsewhere_and_at_span_edge_keep_pending_progress() {
    let base = RangeMap::from_splits([key(100), key(1000), key(1100)]);
    let mut s = populated(1200);
    prepare(&s, 10, &base);
    rebuild(&s);
    let fallbacks = s.split_recount_fallbacks.get();
    let split = split_at(&base, &key(400));
    prepare(&s, 11, &split);
    assert_eq!(kinds(&s), vec!["FD"]);
    assert!(!step(&s));
    let mut next = 6_000_000u64;
    mutate(&mut s, &mut next, &[150, 500, 1050, 1150], "a");
    let covered = progress(&s);
    assert!(covered > 0);
    let mut merged = split.clone();
    assert!(merged.merge_at(&key(1100)));
    s.set_logical_range_map(12, merged.clone());
    assert_no_reseed(&s, fallbacks);
    assert_eq!(kinds(&s), vec!["PD"], "an unrelated merge leaves the span untouched");
    assert_eq!(progress(&s), covered);
    assert_bounds_bracket_exact(&s, 12, &merged);
    mutate(&mut s, &mut next, &[160, 1060], "b");
    let covered = progress(&s);
    let mut edge = merged.clone();
    assert!(edge.merge_at(&key(1000)));
    s.set_logical_range_map(13, edge.clone());
    assert_no_reseed(&s, fallbacks);
    assert_eq!(kinds(&s), vec!["PD"], "a merge at the span edge folds into the derived child");
    assert_eq!(progress(&s), covered);
    assert_bounds_bracket_exact(&s, 13, &edge);
    finish_with_mutations(&mut s, &mut next, &[170, 600, 1070, 1170], "z");
    assert_no_reseed(&s, fallbacks);
    assert_exact(&s, 13, &edge);
}

#[test]
fn cooperative_compaction_and_forget_plan_during_pending_span() {
    let mut s = seeded(1200);
    s.set_gc_grace_ms(1);
    let fallbacks = s.split_recount_fallbacks.get();
    let split = split_at(&old_map(), &key(400));
    prepare(&s, 11, &split);
    assert!(!step(&s));
    let mut next = 7_000_000u64;
    mutate(&mut s, &mut next, &[110, 300, 450, 900], "a");
    s.flush().unwrap();
    s.advance_durable_gc_watermark(next + 2);
    next += 10;
    let runs = s.run_count();
    let plan = s.compact_plan(0).unwrap().expect("a multi-run compaction plan");
    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev.clone()), || {}).unwrap();
    assert!(!step(&s));
    mutate(&mut s, &mut next, &[115, 310, 460, 910], "b");
    s.compact_commit(written).unwrap();
    assert!(s.run_count() < runs, "the cooperative plan merged runs under the pending span");
    assert_no_reseed(&s, fallbacks);
    assert_bounds_bracket_exact(&s, 11, &split);
    s.flush().unwrap();
    let keys: Vec<(Vec<u8>, Version)> = [120u64, 125, 470, 820, 990]
        .into_iter()
        .filter_map(|k| s.current_version(&key(k)).unwrap().map(|v| (key(k), v)))
        .collect();
    assert!(keys.len() >= 3);
    let plan = s.forget_plan(&keys).unwrap().expect("a forget plan");
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
    assert!(!step(&s));
    mutate(&mut s, &mut next, &[125, 320, 480], "c");
    s.compact_commit(written).unwrap();
    assert!(s.current_version(&key(120)).unwrap().is_none(), "forget dropped its exact version");
    assert_no_reseed(&s, fallbacks);
    finish_with_mutations(&mut s, &mut next, &[130, 330, 530], "z");
    assert_no_reseed(&s, fallbacks);
    assert_exact(&s, 11, &split);
}

#[test]
fn blind_apply_with_winner_directory_during_pending_span() {
    let mut s = seeded(1200);
    s.engine_mut().rebuild_winner_directory().unwrap();
    assert!(s.engine().winner_directory_enabled());
    let fallbacks = s.split_recount_fallbacks.get();
    for cut in [400u64, 700] {
        let split = split_at(&old_map(), &key(cut));
        let epoch = if cut == 400 { 11 } else { 13 };
        if cut == 700 {
            let mut merged = split_at(&old_map(), &key(400));
            assert!(merged.merge_at(&key(400)));
            s.set_logical_range_map(12, merged);
            assert_eq!(pending(&s), 0);
        }
        prepare(&s, epoch, &split);
        let mut next = 8_000_000u64 + cut * 1000;
        let mut turn = 0u64;
        while !step(&s) {
            turn += 1;
            let (upper, at) = cursor(&s).expect("cursor");
            let (lo, hi) = if upper { (cut, 1000) } else { (100, cut) };
            let other = if upper { 100 + turn } else { 999 - turn };
            for k in [at.saturating_sub(2).max(lo), (at + 2).min(hi - 1), other] {
                next += 1;
                let fresh = format!("k{k:05}-b{cut}-{turn}");
                s.apply_blind_at(fresh.as_bytes(), Some(b"n"), ver(next), 0).unwrap();
                next += 1;
                let value: Option<&[u8]> = if turn.is_multiple_of(2) { None } else { Some(b"o") };
                s.apply_blind_at(&key(k), value, ver(next), 0).unwrap();
                s.apply_blind_at(&key(k), Some(b"stale"), ver(1), 0).unwrap();
            }
            assert_no_reseed(&s, fallbacks);
        }
        assert!(turn >= 2);
        assert_no_reseed(&s, fallbacks);
        assert_exact(&s, epoch, &split);
    }
}

#[test]
fn random_split_merge_sequences_stay_exact_without_reseed() {
    let mut while_pending = 0;
    let mut forgotten = 0;
    let mut dropped = 0;
    for seed in 1..=32u64 {
        let mut rng = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        let mut rand = |n: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % n
        };
        let mut s = seeded(1200);
        s.set_gc_grace_ms(1);
        let fallbacks = s.split_recount_fallbacks.get();
        let mut map = old_map();
        let mut epoch = 10;
        let mut next = 10_000_000u64 * seed;
        for op in 0..200 {
            let mut changed = false;
            match rand(10) {
                0 => changed = map.split(&key(rand(1200))),
                1 => {
                    for _ in 0..2 + rand(2) {
                        changed |= map.split(&key(rand(1200)));
                    }
                }
                2 => {
                    if !map.splits().is_empty() {
                        let at = map.splits()[rand(map.splits().len() as u64) as usize].clone();
                        assert!(map.merge_at(&at));
                        changed = true;
                    }
                    changed |= map.split(&key(rand(1200)));
                }
                3 => {
                    if !map.splits().is_empty() {
                        let at = map.splits()[rand(map.splits().len() as u64) as usize].clone();
                        changed = map.merge_at(&at);
                    }
                }
                4 => {
                    let keys = [rand(1200), rand(1200), rand(1200)];
                    mutate(&mut s, &mut next, &keys, &format!("r{op}"));
                }
                5 => {
                    let live_cursor = scanning(&s).and_then(|(_, after, _)| after);
                    let mut keys: HashSet<Vec<u8>> = (0..3).map(|_| key(rand(1200)))
                        .filter(|key| s.current_version(key).unwrap().is_some()).collect();
                    if let Some(ref after) = live_cursor { keys.insert(after.clone()); }
                    forgotten += s.forget(&keys).unwrap();
                    if let Some(after) = live_cursor.filter(|_| rand(2) == 0) {
                        next += 1;
                        s.apply_at(&after, Some(b"back"), ver(next), 0).unwrap();
                    }
                }
                6 => {
                    s.advance_durable_gc_watermark(next + 2);
                    next += 10;
                    s.flush().unwrap();
                    dropped += s.compact_runs(0).unwrap().current_dropped_tombstones.len();
                }
                7 => { s.flush().unwrap(); }
                _ => {
                    for _ in 0..rand(11) {
                        if step(&s) { break; }
                    }
                }
            }
            if changed {
                while_pending += usize::from(pending(&s) > 0);
                epoch += 1;
                s.set_logical_range_map(epoch, map.clone());
            }
            assert_no_reseed(&s, fallbacks);
            assert_bounds_bracket_exact(&s, epoch, &map);
            if pending(&s) == 0 {
                assert_eq!(s.logical_range_counts(epoch), Some(exact_counts(&s, &map)), "seed {seed} op {op}");
            }
        }
        recount(&s);
        assert_no_reseed(&s, fallbacks);
        assert_exact(&s, epoch, &map);
    }
    assert!(while_pending >= 900, "only {while_pending} map changes landed on pending spans");
    assert!(forgotten > 0 && dropped > 0, "forgot {forgotten} keys and dropped {dropped} tombstones");
}

fn turn_with(cell: &std::cell::RefCell<Store>, gate: Option<&crate::net::ReadGate>, hook: impl FnMut()) -> bool {
    let turned = vals_rt::block_on(super::lifecycle::logical_recount_turn_with(cell, gate, hook), || {});
    match turned.unwrap() {
        RecountStep::Done => true,
        RecountStep::More => false,
        other => panic!("unexpected recount turn {other:?}"),
    }
}

fn cursor_or(s: &Store, fallback: u64) -> (bool, u64) {
    cursor(s).unwrap_or((false, fallback))
}

fn seeded_wide(n: u64) -> Store {
    let wide = |i: u64| format!("k{i:05}-{}", "w".repeat(1100)).into_bytes();
    let mut s = store();
    for i in 0..n {
        s.apply_at(&wide(i), Some(b"v"), ver(i + 1), 0).unwrap();
        if i % 97 == 96 { s.flush().unwrap(); }
    }
    for i in (0..n).step_by(3) {
        s.apply_at(&wide(i), Some(b"v2"), ver(n + i + 1), 0).unwrap();
    }
    s.flush().unwrap();
    for i in (0..n).step_by(5) {
        s.apply_at(&wide(i), None, ver(2 * n + i + 1), 0).unwrap();
    }
    prepare(&s, 10, &old_map());
    rebuild(&s);
    assert_eq!(s.logical_range_counts(10), Some(exact_counts(&s, &old_map())));
    s
}

#[test]
fn prefetched_recount_with_writes_in_flight_is_exact() {
    for (cut, wide) in [(400u64, false), (700, false), (300, true)] {
        let s = if wide { seeded_wide(600) } else { seeded(1200) };
        let fallbacks = s.split_recount_fallbacks.get();
        let new = split_at(&old_map(), &key(cut));
        prepare(&s, 11, &new);
        let before = s.storage_generation();
        let cell = std::cell::RefCell::new(s);
        let gate = crate::net::ReadGate::new();
        let pages = crate::observe::counters().logical_range_split_recount_prefetch_pages.get();
        let mut next = 3_000_000u64;
        let mut turns = 0u64;
        let mut in_flight = 0u64;
        let mut short_pages = 0u64;
        loop {
            let visited = cell.borrow().split_recount_visited.get();
            let done = turn_with(&cell, Some(&gate), || {
                let mut s = cell.borrow_mut();
                let (_, at) = cursor_or(&s, 100 + turns);
                in_flight += 1;
                for (k, value) in [
                    (at + 1, Some(b"n".as_slice())),
                    (at + 2, None),
                    (at.saturating_sub(2).max(1), None),
                    (1150 - turns % 100, Some(b"far".as_slice())),
                ] {
                    next += 1;
                    s.apply_at(format!("k{k:05}-f{turns}").as_bytes(), value, ver(next), 0).unwrap();
                    next += 1;
                    s.apply_at(&key(k), value, ver(next), 0).unwrap();
                }
                next += 1;
                s.apply_at(&key(at + 3), Some(b"over"), ver(next), 0).unwrap();
            });
            assert_eq!(gate.inflight(), 0, "the read gate is released before the merge");
            if done {
                break;
            }
            if cell.borrow().split_recount_visited.get() - visited < 64 {
                short_pages += 1;
            }
            turns += 1;
            assert!(turns < 100_000, "recount did not converge");
        }
        let s = cell.into_inner();
        assert!(turns >= 4, "fixture must span several prefetched pages");
        assert_eq!(in_flight, turns + 1, "every turn merged its first prefetch");
        assert_eq!(s.storage_generation(), before, "memtable writes never change the run set");
        assert!(crate::observe::counters().logical_range_split_recount_prefetch_pages.get() - pages >= turns);
        if wide {
            assert!(short_pages > 0, "wide keys make two-block windows end pages at the horizon");
        }
        assert_no_reseed(&s, fallbacks);
        assert_exact(&s, 11, &new);
    }
}

#[test]
fn prefetch_discards_on_storage_generation_change_and_falls_back() {
    for always in [false, true] {
        let s = seeded(1200);
        let fallbacks = s.split_recount_fallbacks.get();
        let new = split_at(&old_map(), &key(400));
        prepare(&s, 11, &new);
        let cell = std::cell::RefCell::new(s);
        let counters = crate::observe::counters();
        let discards = counters.logical_range_split_recount_prefetch_discards.get();
        let sync = counters.logical_range_split_recount_sync_fallbacks.get();
        let mut next = 4_000_000u64;
        let mut calls = 0u64;
        let mut turns = 0u64;
        loop {
            let mut turn_calls = 0u64;
            let done = turn_with(&cell, None, || {
                calls += 1;
                turn_calls += 1;
                if !always && turn_calls > 1 {
                    return;
                }
                let mut s = cell.borrow_mut();
                let (_, at) = cursor_or(&s, 150);
                next += 1;
                s.apply_at(format!("k{:05}-g{calls}", at + 1).as_bytes(), Some(b"n"), ver(next), 0).unwrap();
                next += 1;
                s.apply_at(&key(at + 2), None, ver(next), 0).unwrap();
                let before = s.storage_generation();
                if calls.is_multiple_of(5) {
                    s.set_gc_grace_ms(1);
                    s.advance_durable_gc_watermark(next + 2);
                    next += 10;
                    s.flush().unwrap();
                    s.compact_runs(0).unwrap();
                } else {
                    s.flush().unwrap();
                }
                assert_ne!(s.storage_generation(), before);
            });
            if always {
                assert_eq!(turn_calls, 4, "a sync fallback follows four consecutive discards");
            } else {
                assert!(turn_calls <= 2, "a discard re-plans once the run set is stable");
            }
            if done {
                break;
            }
            turns += 1;
            assert!(turns < 100_000, "recount did not converge");
        }
        let s = cell.into_inner();
        assert!(turns >= 2, "fixture must span several pages");
        assert!(counters.logical_range_split_recount_prefetch_discards.get() - discards >= turns);
        if always {
            assert!(counters.logical_range_split_recount_sync_fallbacks.get() - sync > turns);
        }
        assert_no_reseed(&s, fallbacks);
        assert_exact(&s, 11, &new);
    }
}

#[test]
fn journal_cap_spills_into_staged_seed_and_stays_exact() {
    for (isolation, unknown) in [(true, false), (true, true), (false, false)] {
        let cap = 4096u64;
        let _policy = seed_policy::policy_for_test(isolation, 25, cap);
        let _ledger = crate::ram_broker::override_for_test(None, 0);
        let mut s = mid_seed_store(1200);
        for _ in 0..4 {
            assert!(!s.seed_step_cooperative().unwrap());
        }
        let charged = crate::ram_broker::charged_for_test();
        let mut next = 30_000_000u64;
        let (mut peak, mut peak_charge) = (0, 0);
        for round in 0..12u64 {
            let keys: Vec<u64> = (0..15).map(|i| (round * 97 + i * 79) % 1200).collect();
            mutate(&mut s, &mut next, &keys, &format!("j{round}"));
            if unknown && round == 6 {
                s.append_unknown_for_test(&key(5), Some(b"u"), ver(next + 1)).unwrap();
                next += 1;
            }
            if round % 3 == 2 {
                s.flush().unwrap();
            }
            peak = peak.max(s.seed_journal_bytes());
            peak_charge = peak_charge.max(crate::ram_broker::charged_for_test() - charged);
            assert!(!s.seed_step_cooperative().unwrap());
        }
        let (_, _, spills) = s.seed_test_counts();
        if isolation {
            assert_eq!(spills, 1, "the journal spills once it reaches the cap");
            assert!(peak <= cap, "journal never grows past the cap: {peak}");
            assert!(peak_charge > 0 && peak_charge <= peak, "journal bytes are charged: {peak_charge} of {peak}");
        } else {
            assert_eq!(spills, 0);
            assert!(peak > 4 * cap, "knockout journal grows unbounded: {peak}");
            assert_eq!(peak_charge, 0, "knockout journal is not charged");
        }
        drain(&s);
        assert_eq!(s.seed_journal_bytes(), 0);
        assert_eq!(crate::ram_broker::charged_for_test(), charged, "journal charge released");
        if unknown {
            recount(&s);
            assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap());
            assert_eq!(s.logical_range_counts(10), None, "unknown old winner withholds counts");
            assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), golden(&s));
            assert_eq!(s.versions.live.get(), s.engine().recovered_versions().unwrap().len() as u64);
        } else {
            assert_matches_from_scratch(&s, 10, &old_map());
        }
    }
}

#[test]
fn journal_cap_is_node_wide_across_concurrent_seeds() {
    for isolation in [true, false] {
        let cap = 6144u64;
        let _policy = seed_policy::policy_for_test(isolation, 25, cap);
        let _ledger = crate::ram_broker::override_for_test(None, 0);
        let mut stores: Vec<Store> = (0..3).map(|_| mid_seed_store(600)).collect();
        for s in &stores {
            assert!(!s.seed_step_cooperative().unwrap());
        }
        let mut next = 40_000_000u64;
        let mut peak = 0;
        for round in 0..10u64 {
            for (i, s) in stores.iter_mut().enumerate() {
                let keys: Vec<u64> = (0..12).map(|k| (round * 53 + k * 41 + i as u64 * 7) % 600).collect();
                mutate(s, &mut next, &keys, &format!("n{round}"));
            }
            let total: u64 = stores.iter().map(Store::seed_journal_bytes).sum();
            peak = peak.max(total);
            if isolation {
                let reserved = super::version_index::journal_node_bytes_for_test();
                assert!(reserved <= cap, "node-wide journal reservations never exceed the cap: {reserved}");
                assert!(total <= reserved, "every journal byte is reserved: {total} > {reserved}");
            }
        }
        let spills: u64 = stores.iter().map(|s| s.seed_test_counts().2).sum();
        if isolation {
            assert!(peak <= cap, "three seeds share one cap: {peak}");
            assert!(spills >= 2, "later seeds spill once the node-wide cap is reached: {spills}");
        } else {
            assert!(peak > 3 * cap, "knockout journals grow per seed without a cap: {peak}");
            assert_eq!(spills, 0);
        }
        for s in &stores {
            drain(s);
            assert_matches_from_scratch(s, 10, &old_map());
        }
        assert_eq!(super::version_index::journal_node_bytes_for_test(), 0);
    }
}

#[test]
fn carried_seed_spills_and_unknown_old_winners_stay_exact() {
    for (unknown, isolation) in [(false, true), (true, true), (false, false)] {
        let _policy = seed_policy::policy_for_test(isolation, 25, 2048);
        let _ledger = crate::ram_broker::override_for_test(None, 0);
        let mut s = mid_seed_store(1200);
        for _ in 0..3 {
            assert!(!s.seed_step_cooperative().unwrap());
        }
        let mut next = 50_000_000u64;
        let mut map = old_map();
        let mut epoch = 10;
        for round in 0..8u64 {
            let keys: Vec<u64> = (0..12).map(|i| (round * 89 + i * 67) % 1200).collect();
            mutate(&mut s, &mut next, &keys, &format!("c{round}"));
            if round % 2 == 1 {
                let cut = key(200 + round * 97);
                if map.split(&cut) {
                    epoch += 1;
                    s.set_logical_range_map(epoch, map.clone());
                    assert!(s.seed_progress.borrow().is_some() || !isolation, "the seed stays in flight across the bump");
                }
            }
            if unknown && round == 4 {
                s.append_unknown_for_test(&key(17), Some(b"u"), ver(next + 1)).unwrap();
                next += 1;
            }
            if round % 2 == 0 {
                s.seed_step_cooperative().unwrap();
            }
        }
        drain(&s);
        let (resets, _, spills) = s.seed_test_counts();
        if isolation {
            assert_eq!(resets, 0, "carried seed survives generation bumps while spilling");
            assert!(spills >= 1, "the carried seed spilled at the journal cap");
        }
        if unknown {
            recount(&s);
            assert!(vals_rt::block_on(ensure_tree_ready_cooperative(&s, 1, &HashSet::from([0])), || {}).unwrap());
            assert_eq!(s.logical_range_counts(epoch), None, "an unknown old winner withholds counts");
            assert_eq!(s.version_leaf_digests(INDEX_BUCKETS).unwrap(), golden(&s));
            assert_eq!(s.versions.live.get(), s.engine().recovered_versions().unwrap().len() as u64);
        } else {
            assert_matches_from_scratch(&s, epoch, &map);
        }
    }
}

#[test]
fn driver_seeds_start_beyond_the_node_permit_cap_under_isolation() {
    for isolation in [true, false] {
        let _policy = seed_policy::policy_for_test(isolation, 25, 64 << 20);
        let _limit = seed_permit::limit_for_test(1, 0);
        let stores: Vec<Store> = (0..3).map(|_| mid_seed_store(300)).collect();
        for s in &stores {
            s.set_seed_driver(true);
            assert!(!s.seed_step_cooperative().unwrap());
        }
        let started = stores.iter().filter(|s| s.seed_progress.borrow().is_some()).count();
        if isolation {
            assert_eq!(started, 3, "each shard driver seeds without waiting for a node-wide permit");
            assert_eq!(seed_permit::active_for_test(), 3);
        } else {
            assert_eq!(started, 1, "knockout: the node-wide permit serializes seeds");
            assert!(stores[1].seed_deferred());
        }
        for s in &stores {
            drain(s);
        }
        assert_eq!(seed_permit::active_for_test(), 0);
    }
}
