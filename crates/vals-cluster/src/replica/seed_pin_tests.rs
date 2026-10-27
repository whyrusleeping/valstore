use super::*;
use crate::ring::Ring;
use crate::zone::{Ordering, ZoneConfig};
use vals_spdk::MemBlockDevice;

type Store = VersionedStore<MemBlockDevice>;

fn vp(phys_ms: u64, node: u32) -> Version {
    Version { hlc: phys_ms << crate::hlc::COUNTER_BITS, node: node.into() }
}

fn store() -> Store {
    Store::new(Engine::create_prewiped(MemBlockDevice::new(256, 1 << 17), vals_store::Config {
        seg_blocks: 8, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }), 1).unwrap().with_max_skew_ms(0)
}

fn cfg() -> ZoneConfig {
    ZoneConfig { name: "seed-pin-test".into(), rf: 2, ordering: Ordering::Sorted, scope: None }
}

fn ring() -> Ring {
    Ring::from_members(2, 64, [1, 2, 3])
}

fn map() -> RangeMap {
    let mut map = RangeMap::from_splits([b"k0100".to_vec(), b"k0200".to_vec(), b"n".to_vec()]);
    map.enable_opaque_tokens();
    map
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:04}").into_bytes()
}

fn base<D: BlockDevice>(s: &mut VersionedStore<D>) {
    for run in 0..5u32 {
        for i in 0..200u32 {
            let k = (i * 7 + run * 13) % 600;
            let phys = 100 + run as u64 * 100 + i as u64;
            let value = if (k + run) % 9 == 0 { None } else { Some(&b"base-value"[..]) };
            s.apply_at(&key(k), value, vp(phys, 2 + run % 2), phys).unwrap();
        }
        s.flush().unwrap();
    }
    for i in 0..20u32 {
        s.apply_at(format!("m{i:03}").as_bytes(), Some(b"mem"), vp(700 + i as u64, 1), 700).unwrap();
    }
}

fn prepare<D: BlockDevice>(s: &VersionedStore<D>) {
    s.prepare_zone_seed((1, 1, 10), &ring(), 42, Some(cfg()), Some(map()));
}

fn interleave(s: &mut Store, round: u32, phys: &mut u64) -> bool {
    *phys += 10;
    let fresh = format!("n{round:04}").into_bytes();
    s.apply_at(&fresh, Some(b"new"), vp(*phys, 1), *phys).unwrap();
    s.apply_at(&key(round * 11 % 600), Some(b"over"), vp(*phys + 1, 3), *phys).unwrap();
    s.flush().unwrap();
    s.advance_durable_gc_watermark((200 + round as u64 * 40).min(5_000));
    match round % 4 {
        0 => {
            s.compact_runs(*phys).unwrap();
        }
        1 => {
            s.apply_at(&key(round * 17 % 600), None, vp(*phys + 2, 2), *phys).unwrap();
            s.flush().unwrap();
            match s.compact_plan(*phys).unwrap() {
                Some(plan) => {
                    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
                    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {})
                        .unwrap();
                    s.compact_commit(written).unwrap();
                    return true;
                }
                None => {
                    s.compact_runs(*phys).unwrap();
                }
            }
        }
        2 => {
            let forget = std::collections::HashSet::from([key(round * 5 % 600), fresh.clone()]);
            s.forget(&forget).unwrap();
        }
        _ => {
            s.compact_runs(*phys).unwrap();
            s.apply_at(format!("m{:03}", round % 20).as_bytes(), Some(b"mem2"), vp(*phys + 3, 1), *phys)
                .unwrap();
        }
    }
    false
}

struct Exact {
    leaves: std::collections::HashMap<Box<[NodeId]>, Box<[u64]>>,
    buckets: Vec<u64>,
    live: u64,
    counts: Option<Vec<u64>>,
}

fn exact_state<D: BlockDevice>(s: &VersionedStore<D>) -> Exact {
    Exact {
        leaves: s.versions.leaves.borrow().clone(),
        buckets: s.versions.buckets.borrow().iter().map(|m| m.logical_keys).collect(),
        live: s.versions.live.get(),
        counts: s.logical_range_counts(10),
    }
}

fn recompute<D: BlockDevice>(s: &VersionedStore<D>) -> Exact {
    let map = map();
    let mut leaves = std::collections::HashMap::<Box<[NodeId]>, Box<[u64]>>::new();
    let mut buckets = vec![0u64; INDEX_BUCKETS as usize];
    let mut counts = vec![0u64; map.num_ranges()];
    let versions = s.engine().recovered_versions().unwrap();
    for (key, raw) in &versions {
        let b = VersionIndex::slot(key);
        leaves.entry(s.group_of(key).into_boxed_slice()).or_insert_with(VersionIndex::empty_group)[b] ^=
            crate::antientropy::entry_digest(key, unpack_version(*raw));
        buckets[b] += 1;
        counts[map.range_index(key)] += 1;
    }
    Exact { leaves, buckets, live: versions.len() as u64, counts: Some(counts) }
}

fn assert_exact(got: &Exact, want: &Exact, what: &str) {
    assert_eq!(got.leaves, want.leaves, "{what}: leaves");
    assert_eq!(got.buckets, want.buckets, "{what}: bucket counts");
    assert_eq!(got.live, want.live, "{what}: live");
    assert_eq!(got.counts, want.counts, "{what}: range counts");
}

fn seed_under_merges(s: &mut Store, max_steps: u32) -> (bool, u32, u32) {
    let mut phys = 10_000u64;
    let mut offcore = 0;
    for round in 0..max_steps {
        if s.seed_step_cooperative().unwrap() {
            return (true, round, offcore);
        }
        let epoch = s.engine().runs_removed_epoch();
        offcore += u32::from(interleave(s, round, &mut phys));
        assert!(s.engine().runs_removed_epoch() > epoch, "every step is interleaved with a run-set change");
    }
    (false, max_steps, offcore)
}

#[test]
fn seed_completes_exactly_under_continuous_merges() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let c = crate::observe::counters();
    let mut s = store();
    base(&mut s);
    prepare(&s);
    s.set_gc_grace_ms(1);
    let keys_before: std::collections::HashSet<Box<[u8]>> =
        s.engine().recovered_versions().unwrap().into_iter().map(|(k, _)| k).collect();
    let commits_before = c.ae_seed_completions.get();
    let (done, steps, offcore) = seed_under_merges(&mut s, 400);
    assert!(done, "the pinned seed completes while every step races a merge commit");
    assert!(offcore >= 2, "off-core merge commits ran during the seed: {offcore}");
    assert!(steps >= 10, "the seed spans many interleaved merges: {steps}");
    let [carries, merge_restarts, cap, revoked] = s.seed_run_causes();
    assert!(carries >= 20, "merges carried: {carries}");
    assert_eq!((merge_restarts, cap, revoked), (0, 0, 0));
    assert_eq!(s.seed_test_counts().0, 0, "no generation resets");
    assert!(c.ae_seed_completions.get() > commits_before);
    assert!(s.versions.dirty.borrow().is_empty());
    assert!(s.engine().pinned_retired_bytes() > 0, "the published seed's pin is released lazily");
    let mid = exact_state(&s);
    assert_exact(&mid, &recompute(&s), "pinned seed vs recompute");
    assert!(mid.counts.as_ref().unwrap().iter().all(|n| *n > 0));
    assert!(mid.leaves.len() > 1);
    let keys_after: std::collections::HashSet<Box<[u8]>> =
        s.engine().recovered_versions().unwrap().into_iter().map(|(k, _)| k).collect();
    let forgotten: std::collections::HashSet<Box<[u8]>> = (0..steps)
        .filter(|round| round % 4 == 2)
        .map(|round| key(round * 5 % 600).into_boxed_slice())
        .collect();
    let gc_dropped = keys_before.iter().filter(|k| !keys_after.contains(*k) && !forgotten.contains(*k)).count();
    assert!(gc_dropped > 0, "merges GC'd current tombstone winners during the seed");
    assert!(keys_before.iter().any(|k| forgotten.contains(k) && !keys_after.contains(k)), "forgets ran during the seed");

    s.reap_seed_pins();
    assert_eq!(s.engine().pinned_retired_bytes(), 0);
    assert_eq!(seed_policy::pinned_node_bytes(), 0);
    let mut phys = 900_000u64;
    for round in 400..404 {
        interleave(&mut s, round, &mut phys);
    }
    let live = exact_state(&s);
    assert_exact(&live, &recompute(&s), "incremental maintenance after publish");
    s.invalidate_leaf_seed();
    while !s.seed_step_cooperative().unwrap() {}
    assert_exact(&exact_state(&s), &live, "pinned seed vs from-scratch seed");

    let _knockout = seed_policy::pin_cap_for_test(0);
    let mut k = store();
    base(&mut k);
    prepare(&k);
    k.set_gc_grace_ms(1);
    let (done, _, _) = seed_under_merges(&mut k, 400);
    assert!(!done, "knockout (restart on every runs_removed_epoch bump) never completes");
    let [carries, merge_restarts, _, _] = k.seed_run_causes();
    assert_eq!(carries, 0);
    assert!(merge_restarts >= 150, "every other step restarts the unpinned seed: {merge_restarts}");
}

#[test]
fn pinned_bytes_are_released_on_publish_reset_clear_and_drop() {
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let gauge = || seed_policy::pinned_node_bytes();
    let mut phys = 10_000u64;
    let mut s = store();
    base(&mut s);
    prepare(&s);
    assert!(!s.seed_step_cooperative().unwrap());
    let held: Vec<vals_store::alloc::SegmentId> =
        s.engine().run_handles().iter().flat_map(|h| h.segs.iter().copied()).collect();
    interleave(&mut s, 0, &mut phys);
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(gauge() > 0);
    assert_eq!(gauge(), s.engine().pinned_retired_bytes());
    let free = s.engine().free_segments();
    for round in 1..6 {
        s.apply_at(format!("fill{round}").as_bytes(), Some(&[7u8; 600]), vp(phys + round as u64, 1), phys).unwrap();
        s.flush().unwrap();
    }
    assert!(s.engine().free_segments() < free, "writes allocate new segments");
    for seg in &held {
        assert!(s.engine().segment_in_use(*seg), "pinned segments are not reused");
    }

    s.reset_seed_progress(SeedEnd::Dropped);
    assert!(gauge() > 0, "held until reaped");
    s.reap_seed_pins();
    assert_eq!(gauge(), 0, "a reset releases the pin");
    assert!(held.iter().all(|seg| !s.engine().segment_in_use(*seg)), "released segments are free again");

    while !s.seed_step_cooperative().unwrap() {
        interleave(&mut s, 1, &mut phys);
    }
    assert!(gauge() > 0);
    s.reap_seed_pins();
    assert_eq!(gauge(), 0, "publish releases the pin");

    s.invalidate_leaf_seed();
    assert!(!s.seed_step_cooperative().unwrap());
    interleave(&mut s, 3, &mut phys);
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(gauge() > 0);
    s.clear().unwrap();
    assert_eq!(gauge(), 0, "a truncate drops pinned runs");

    base(&mut s);
    s.invalidate_leaf_seed();
    assert!(!s.seed_step_cooperative().unwrap());
    interleave(&mut s, 4, &mut phys);
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(gauge() > 0);
    drop(s);
    assert_eq!(gauge(), 0, "dropping the store (zone drop) releases its pinned bytes");
}

#[test]
fn pin_cap_and_revocation_fall_back_to_counted_restarts() {
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let c = crate::observe::counters();
    let mut phys = 10_000u64;
    let mut s = store();
    base(&mut s);
    prepare(&s);
    let mut t = store();
    base(&mut t);
    prepare(&t);
    {
        let _cap = seed_policy::pin_cap_for_test(1);
        let (restarts, exempt) = (c.ae_seed_restarts.get(), c.ae_seed_pin_cap_exempt_steps.get());
        assert!(!s.seed_step_cooperative().unwrap());
        assert!(!t.seed_step_cooperative().unwrap());
        assert!(t.stream_seed.borrow().as_ref().unwrap().pin.is_some());
        interleave(&mut s, 0, &mut phys);
        interleave(&mut t, 0, &mut phys);
        assert!(s.engine().pinned_retired_bytes() > 1, "the oldest seed holds past the cap");
        assert_eq!(t.engine().pinned_retired_bytes(), 0, "the younger seed's runs are freed at the retiring commit");
        assert!(!s.seed_step_cooperative().unwrap());
        assert_eq!(s.seed_run_causes()[2], 0, "the oldest pinned seed is never restarted by the cap");
        assert!(c.ae_seed_pin_cap_exempt_steps.get() > exempt);
        assert!(!t.seed_step_cooperative().unwrap());
        assert_eq!(t.seed_run_causes()[2], 1, "over the cap: the younger seed restarts and is counted");
        assert!(c.ae_seed_restarts.get() > restarts);
        assert!(c.ae_seed_pin_cap_fallbacks.get() >= 1);
        assert!(t.stream_seed.borrow().is_none());
        while !s.seed_step_cooperative().unwrap() {
            interleave(&mut s, 3, &mut phys);
        }
        assert_eq!(s.seed_run_causes()[2], 0);
        s.reap_seed_pins();
        t.reap_seed_pins();
        assert_eq!(seed_policy::pinned_node_bytes(), 0);
    }
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    assert!(!t.seed_step_cooperative().unwrap());
    interleave(&mut t, 1, &mut phys);
    assert!(!t.seed_step_cooperative().unwrap());
    assert_eq!(t.seed_run_causes()[0], 1);
    t.engine_mut().revoke_run_pins();
    assert!(!t.seed_step_cooperative().unwrap());
    assert_eq!(t.seed_run_causes()[3], 1, "a revoked pin restarts the seed");
    assert!(c.ae_seed_pin_revocations.get() >= 1);
    while !t.seed_step_cooperative().unwrap() {}
    assert_exact(&exact_state(&t), &recompute(&t), "seed after fallbacks");
}

#[test]
fn restarted_seed_is_not_rejected_by_its_own_unreaped_bytes() {
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let mut phys = 10_000u64;
    let mut probe = store();
    base(&mut probe);
    prepare(&probe);
    let _probe_cap = seed_policy::pin_cap_for_test(1 << 40);
    assert!(!probe.seed_step_cooperative().unwrap());
    interleave(&mut probe, 0, &mut phys);
    let set = probe.engine().pinned_retired_bytes();
    drop(probe);
    let _cap = seed_policy::pin_cap_for_test(set * 3 / 2);
    let mut s = store();
    base(&mut s);
    prepare(&s);
    let mut t = store();
    base(&mut t);
    prepare(&t);
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(!t.seed_step_cooperative().unwrap());
    interleave(&mut t, 0, &mut phys);
    assert_eq!(t.engine().pinned_retired_bytes(), set, "under the cap at its commit");
    interleave(&mut s, 0, &mut phys);
    assert!(seed_policy::pinned_node_bytes() > set * 3 / 2, "the oldest seed's commit pushed the node over the cap");
    assert!(!t.seed_step_cooperative().unwrap());
    assert_eq!(t.seed_run_causes()[2], 1, "the younger seed sheds at its next step");
    assert_eq!(t.engine().pinned_retired_bytes(), set, "not reaped yet");
    assert!(!t.seed_step_cooperative().unwrap());
    assert!(
        t.stream_seed.borrow().as_ref().unwrap().pin.is_some(),
        "the restarted seed pins: its own unreaped bytes do not count against the cap"
    );
    assert_eq!(t.seed_run_causes()[2], 1);
}

fn step_like_driver(s: &mut Store) -> bool {
    let done = s.seed_step_cooperative().unwrap();
    if s.engine().run_pins_reapable() {
        s.reap_seed_pins();
    }
    done
}

fn concurrent_seeds_under_merges(n: usize, cap_quarters: u64, rounds: u32) -> (Vec<Store>, Vec<Option<u32>>) {
    let mut phys = 10_000u64;
    let mut probe = store();
    base(&mut probe);
    prepare(&probe);
    let set = {
        let _cap = seed_policy::pin_cap_for_test(1 << 40);
        assert!(!probe.seed_step_cooperative().unwrap());
        interleave(&mut probe, 0, &mut phys);
        probe.engine().pinned_retired_bytes()
    };
    drop(probe);
    assert!(set > 0);
    let _cap = seed_policy::pin_cap_for_test(set * cap_quarters / 4);
    let mut stores: Vec<Store> = (0..n).map(|_| {
        let mut s = store();
        base(&mut s);
        prepare(&s);
        s.set_gc_grace_ms(1);
        s
    }).collect();
    let mut done = vec![None; n];
    for round in 0..rounds {
        for (i, s) in stores.iter_mut().enumerate() {
            if done[i].is_none() && step_like_driver(s) {
                done[i] = Some(round);
            }
            interleave(s, round, &mut phys);
            assert!(
                seed_policy::pinned_node_bytes() <= set * cap_quarters / 4 + set,
                "overshoot is bounded by one seed's run set"
            );
        }
        if done.iter().all(Option::is_some) {
            break;
        }
    }
    (stores, done)
}

fn assert_seeds_exact(stores: &mut [Store]) {
    for (i, s) in stores.iter_mut().enumerate() {
        assert_eq!(s.seed_run_causes()[3], 0, "no pressure revocations");
        s.reap_seed_pins();
        let published = exact_state(s);
        assert_exact(&published, &recompute(s), &format!("seed {i} vs recompute"));
        s.invalidate_leaf_seed();
        while !s.seed_step_cooperative().unwrap() {}
        assert_exact(&exact_state(s), &published, &format!("seed {i} vs from-scratch seed"));
    }
}

#[test]
fn concurrent_seeds_over_the_pin_cap_restart_only_the_youngest_and_all_complete() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    let _limit = seed_permit::limit_for_test(8, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let c = crate::observe::counters();
    for cap_quarters in [6, 1] {
        let (fallbacks, exempt) = (c.ae_seed_pin_cap_fallbacks.get(), c.ae_seed_pin_cap_exempt_steps.get());
        let (mut stores, done) = concurrent_seeds_under_merges(3, cap_quarters, 1_000);
        let causes: Vec<[u64; 4]> = stores.iter().map(|s| s.seed_run_causes()).collect();
        assert!(done.iter().all(Option::is_some), "cap {cap_quarters}/4 sets: every seed completes: {done:?} {causes:?}");
        assert_eq!(causes[0][2], 0, "the oldest seed is never restarted by the cap");
        assert!(causes[0][0] >= 10, "the oldest rode merges: {causes:?}");
        assert!(causes[1..].iter().all(|c| c[1] + c[2] > 0), "younger seeds restarted: {causes:?}");
        if cap_quarters == 6 {
            assert!(causes[1][2] + causes[2][2] > 0, "younger pinned seeds shed by the cap: {causes:?}");
            assert!(c.ae_seed_pin_cap_fallbacks.get() > fallbacks);
        } else {
            assert!(c.ae_seed_pin_cap_exempt_steps.get() > exempt, "the oldest seed held past the cap");
            assert!(done[0] < done[1] && done[0] < done[2], "one at a time, oldest first: {done:?}");
        }
        assert_seeds_exact(&mut stores);
        drop(stores);
        assert_eq!(seed_policy::pinned_node_bytes(), 0);
    }

    let _knockout = seed_policy::pin_revoke_all_for_test();
    let (stores, done) = concurrent_seeds_under_merges(3, 1, 400);
    assert!(
        done.iter().all(Option::is_none),
        "knockout (every holder restarts while the node is over the cap) completes nothing in 400 rounds: {done:?} {:?}",
        stores.iter().map(|s| s.seed_run_causes()).collect::<Vec<_>>()
    );
    let causes: Vec<[u64; 4]> = stores.iter().map(|s| s.seed_run_causes()).collect();
    assert!(causes.iter().all(|c| c[1] + c[2] >= 50), "every seed keeps restarting: {causes:?}");
    assert!(causes[0][2] >= 50, "the oldest seed is cap-restarted whenever it holds: {causes:?}");
}

#[test]
fn peer_only_rehome_changes_groups_so_local_leaves_are_regrouped() {
    let ring = ring();
    let cfg = cfg();
    let mut s = store();
    for i in 0..40u32 {
        s.apply_at(&key(i), Some(b"v"), vp(100 + i as u64, 1), 100).unwrap();
    }
    let sorted = |mut v: Vec<u32>| { v.sort_unstable(); v };
    let with_local: Vec<(Vec<u32>, Vec<u8>)> = (0u8..=255)
        .map(|seed| crate::partition::opaque_placement_token([seed; 16]))
        .map(|token| (sorted(ring.replicas_in_zone(42, &token, 2, None)), token))
        .filter(|(set, _)| set.contains(&1))
        .collect();
    let (first_set, first) = with_local[0].clone();
    let (_, second) = with_local.iter().find(|(set, _)| *set != first_set).cloned()
        .expect("two replica sets that both keep node 1");
    let mut map = RangeMap::from_splits([b"k0020".to_vec()]);
    map.enable_opaque_tokens();
    assert!(map.set_pending(1, first));
    assert!(map.commit_pending(1));
    s.prepare_zone_seed((1, 1, 10), &ring, 42, Some(cfg.clone()), Some(map.clone()));
    while !s.seed_step_cooperative().unwrap() {}
    let retained = s.versions.leaves.borrow().clone();
    let mut committed = map.clone();
    assert!(committed.set_pending(1, second));
    assert!(committed.commit_pending(1));
    s.prepare_zone_seed((1, 1, 11), &ring, 42, Some(cfg), Some(committed));
    assert!(s.is_leaf_seeded() && s.regroup_pending(), "this node keeps the span but its replica group changed: leaves must be regrouped");
    s.finish_regroup().unwrap();
    let regrouped = s.versions.leaves.borrow().clone();
    assert_ne!(regrouped, retained, "skipping the invalidation would keep the keys under the old group");
    assert_eq!(regrouped, recompute(&s).leaves);
}

#[derive(Default)]
struct PostManifestReadFault {
    armed: std::sync::atomic::AtomicBool,
    failing: std::sync::atomic::AtomicBool,
    failed_reads: std::sync::atomic::AtomicU64,
}

impl PostManifestReadFault {
    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::Relaxed);
    }

    fn disarm(&self) -> u64 {
        self.armed.store(false, std::sync::atomic::Ordering::Relaxed);
        self.failing.store(false, std::sync::atomic::Ordering::Relaxed);
        self.failed_reads.swap(0, std::sync::atomic::Ordering::Relaxed)
    }
}

struct PostManifestReadFailDevice {
    inner: MemBlockDevice,
    fault: std::sync::Arc<PostManifestReadFault>,
}

impl BlockDevice for PostManifestReadFailDevice {
    fn block_size(&self) -> usize { self.inner.block_size() }
    fn num_blocks(&self) -> u64 { self.inner.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        if self.fault.failing.load(std::sync::atomic::Ordering::Relaxed) {
            self.fault.failed_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(vals_spdk::BlockError::Io(std::io::Error::other("injected post-commit read failure")));
        }
        self.inner.read_at(lba, buf)
    }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
        const MAN_MAGIC_LE: [u8; 4] = [0x56, 0x4d, 0x4e, 0x43];
        self.inner.write_at(lba, buf)?;
        if buf.starts_with(&MAN_MAGIC_LE) && self.fault.armed.swap(false, std::sync::atomic::Ordering::Relaxed) {
            self.fault.failing.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }
    fn flush(&self) -> vals_spdk::Result<()> { self.inner.flush() }
    fn deallocate(&self, ranges: &[(u64, u64)]) -> vals_spdk::Result<bool> { self.inner.deallocate(ranges) }
    fn sibling(&self) -> vals_spdk::Result<Self> {
        Ok(PostManifestReadFailDevice { inner: self.inner.share(), fault: self.fault.clone() })
    }
}

fn faulty_store() -> (VersionedStore<PostManifestReadFailDevice>, std::sync::Arc<PostManifestReadFault>) {
    let fault = std::sync::Arc::new(PostManifestReadFault::default());
    let dev = PostManifestReadFailDevice { inner: MemBlockDevice::new(256, 1 << 17), fault: fault.clone() };
    let s = VersionedStore::new(Engine::create_prewiped(dev, vals_store::Config {
        seg_blocks: 8, reserved_blocks: 0, ordering: vals_store::Ordering::Sorted,
    }), 1).unwrap().with_max_skew_ms(0);
    (s, fault)
}

#[test]
fn failed_post_commit_directory_rebuild_in_merge_all_runs_still_journals_retired_winners() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    failed_post_commit_directory_rebuild_still_journals_retired_winners(true);
}

#[test]
fn failed_post_commit_directory_rebuild_in_compact_commit_still_journals_retired_winners() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    failed_post_commit_directory_rebuild_still_journals_retired_winners(false);
}

fn failed_post_commit_directory_rebuild_still_journals_retired_winners(merge_fails: bool) {
    vals_store::configure_winner_directory(true).unwrap();
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let (mut s, fault) = faulty_store();
    base(&mut s);
    prepare(&s);
    s.set_gc_grace_ms(1);
    s.advance_durable_gc_watermark(5_000);
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(s.stream_seed.borrow().as_ref().unwrap().pin.is_some());
    assert!(s.engine().winner_directory_enabled());

    if merge_fails {
        fault.arm();
    }
    let merged = s.compact_runs(10_000);
    let merge_failed_reads = fault.disarm();
    assert_eq!(s.engine().winner_directory_enabled(), !merge_fails);
    assert!(!s.seed_step_cooperative().unwrap());

    if merge_fails {
        s.engine_mut().rebuild_winner_directory().unwrap();
    }
    for i in 0..12u32 {
        let phys = 6_000 + i as u64;
        s.apply_at(format!("t{i:03}").as_bytes(), None, vp(phys, 2), phys).unwrap();
        if i % 4 == 3 {
            s.flush().unwrap();
        }
    }
    s.advance_durable_gc_watermark(9_000);
    let plan = s.compact_plan_tombstone_gc(10_000).unwrap().expect("a bottommost GC merge");
    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
    fault.arm();
    let committed = s.compact_commit(written);
    let commit_failed_reads = fault.disarm();
    assert!(!s.engine().winner_directory_enabled());

    while !s.seed_step_cooperative().unwrap() {}
    let [carries, merge_restarts, cap, revoked] = s.seed_run_causes();
    assert!(carries >= 2, "the pinned seed carried both commits: {carries}");
    assert_eq!((merge_restarts, cap, revoked), (0, 0, 0));
    assert_exact(&exact_state(&s), &recompute(&s), "pinned seed vs recompute after failed directory rebuilds");
    let merged = merged.expect("merge_all_runs commit is reported");
    let committed = committed.expect("off-core commit is reported");
    assert!(!merged.current_dropped_tombstones.is_empty());
    assert!(!committed.current_dropped_tombstones.is_empty());
    if merge_fails {
        assert!(merge_failed_reads > 0, "{merge_failed_reads}");
        assert_eq!(commit_failed_reads, 0, "no post-commit rebuild is attempted once the directory degraded");
    } else {
        assert_eq!(merge_failed_reads, 0);
        assert!(commit_failed_reads > 0, "{commit_failed_reads}");
    }
    assert!(!vals_store::winner_directory_operational());
}

#[test]
fn partial_sync_forget_after_the_run_commit_journals_every_removed_winner() {
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let poison = b"m005".to_vec();
    let (mut unreached_run_key, mut processed_mutable) = (false, false);
    for _ in 0..32 {
        let mut s = store();
        base(&mut s);
        prepare(&s);
        assert!(!s.seed_step_cooperative().unwrap());
        assert!(s.stream_seed.borrow().as_ref().unwrap().pin.is_some());
        for i in [3u32, 4] {
            s.apply_at(&key(i), Some(b"mem"), vp(10_000 + i as u64, 1), 10_000).unwrap();
        }
        let mut keys: std::collections::HashSet<Vec<u8>> = (0..12u32).map(key).collect();
        keys.extend((0..8u32).map(|i| format!("m{i:03}").into_bytes()));
        let mutable = |k: &[u8]| k.starts_with(b"m") || k == key(3) || k == key(4);
        let present: std::collections::HashSet<Vec<u8>> =
            keys.iter().filter(|k| s.engine().version(k).unwrap().is_some()).cloned().collect();
        let order: Vec<Vec<u8>> = keys.iter().cloned().collect();
        let at = order.iter().position(|k| *k == poison).unwrap();
        unreached_run_key |= order[at..].iter().any(|k| !mutable(k) && present.contains(k));
        processed_mutable |= order[..at].iter().any(|k| mutable(k));

        let epoch = s.engine().runs_removed_epoch();
        s.engine_mut().fail_forget_append_for_test(Some(poison.clone()));
        let err = s.forget(&keys).expect_err("the poisoned append fails after the run commit");
        s.engine_mut().fail_forget_append_for_test(None);
        assert!(err.is_device_full(), "{err}");
        assert!(s.engine().runs_removed_epoch() > epoch);
        for (i, k) in order.iter().enumerate() {
            let live = mutable(k) && i >= at;
            assert_eq!(s.engine().version(k).unwrap().is_some(), live, "{:?} at {i}, poison at {at}", String::from_utf8_lossy(k));
        }

        while !s.seed_step_cooperative().unwrap() {}
        let [carries, merge_restarts, cap, revoked] = s.seed_run_causes();
        assert!(carries >= 1);
        assert_eq!((merge_restarts, cap, revoked), (0, 0, 0));
        assert_exact(&exact_state(&s), &recompute(&s), "pinned seed vs recompute after a partial forget");
        if unreached_run_key && processed_mutable {
            break;
        }
    }
    assert!(unreached_run_key && processed_mutable);
}

fn assert_seed_ends_reconcile(before: &crate::observe::CountersSnapshot, in_flight: u64, what: &str) {
    let a = crate::observe::snapshot();
    let ended = (a.ae_seed_completions - before.ae_seed_completions)
        + (a.ae_seed_restarts - before.ae_seed_restarts)
        + (a.ae_seed_placement_resets - before.ae_seed_placement_resets)
        + (a.ae_seed_generation_resets - before.ae_seed_generation_resets)
        + (a.ae_seed_ram_sheds - before.ae_seed_ram_sheds)
        + (a.ae_seed_corrupt_sidecars - before.ae_seed_corrupt_sidecars)
        + (a.ae_seed_error_resets - before.ae_seed_error_resets)
        + (a.ae_seed_dropped - before.ae_seed_dropped);
    assert_eq!(a.ae_seed_starts - before.ae_seed_starts, ended + in_flight, "{what}: starts = ends + in flight");
}

fn start_seed<D: BlockDevice>(s: &VersionedStore<D>) {
    assert!(!s.seed_step_cooperative().unwrap());
    assert!(s.stream_seed.borrow().as_ref().is_some_and(|ss| ss.pin.is_some()), "a pinned seed is in flight");
}

#[test]
fn seed_starts_reconcile_with_every_end_and_reap_frees_are_counted() {
    if crate::testutil::isolated_env_test(&[]) {
        return;
    }
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let c = crate::observe::counters();
    let before = crate::observe::snapshot();
    let delta = |f: fn(&crate::observe::CountersSnapshot) -> u64| f(&crate::observe::snapshot()) - f(&before);
    let (mut s, fault) = faulty_store();
    base(&mut s);
    prepare(&s);
    let seg_bytes = s.engine_config().seg_blocks as u64 * 256;

    start_seed(&s);
    let epoch = s.engine().runs_removed_epoch();
    assert_eq!(s.forget(&std::collections::HashSet::from([key(7)])).unwrap(), 1);
    assert_eq!(s.forget(&std::collections::HashSet::from([key(14)])).unwrap(), 1);
    assert_eq!(s.engine().runs_removed_epoch() - epoch, 2);
    while !s.seed_step_cooperative().unwrap() {}
    assert_eq!(delta(|c| c.ae_seed_merge_carries), 2, "one carry per removal commit, not per step");
    let pinned = s.engine().pinned_retired_bytes();
    assert!(pinned > 0);
    let reaped_before = c.ae_seed_pin_reaped_segments.get();
    let reaped = s.reap_seed_pins();
    assert_eq!(reaped as u64 * seg_bytes, pinned, "the reap frees every retired segment");
    assert_eq!(c.ae_seed_pin_reaped_segments.get() - reaped_before, reaped as u64);
    assert_eq!(delta(|c| c.ae_seed_completions), 1);
    assert_seed_ends_reconcile(&before, 0, "completion");

    s.invalidate_leaf_seed();
    start_seed(&s);
    fault.failing.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut failed = false;
    for _ in 0..10_000 {
        match s.seed_step_cooperative() {
            Ok(done) => assert!(!done),
            Err(e) => {
                assert!(!matches!(e, EngineError::Snapshot(vals_store::SnapshotError::Corrupt(_))), "{e}");
                failed = true;
                break;
            }
        }
    }
    fault.disarm();
    assert!(failed, "a read error reaches the seed");
    assert!(s.stream_seed.borrow().is_none());
    assert_eq!(delta(|c| c.ae_seed_error_resets), 1);
    assert_seed_ends_reconcile(&before, 0, "error reset");

    start_seed(&s);
    s.set_placement(std::sync::Arc::new(|_| vec![1]));
    assert!(!s.seed_step_cooperative().unwrap());
    assert_eq!(delta(|c| c.ae_seed_placement_resets), 1);
    assert_seed_ends_reconcile(&before, 0, "placement reset");

    {
        let _policy = seed_policy::policy_for_test(false, 25, 64 << 20);
        start_seed(&s);
        let mut split = map();
        assert!(split.split(b"k0300"));
        s.set_logical_range_map(11, split);
        assert!(!s.seed_step_cooperative().unwrap());
    }
    assert_eq!(delta(|c| c.ae_seed_generation_resets), 1);
    assert_seed_ends_reconcile(&before, 0, "generation reset");

    start_seed(&s);
    s.engine_mut().revoke_run_pins();
    assert!(!s.seed_step_cooperative().unwrap());
    assert_eq!(delta(|c| c.ae_seed_restarts), 1);
    assert_seed_ends_reconcile(&before, 0, "pin revocation restart");

    start_seed(&s);
    {
        let _pressure = crate::ram::override_for_test(2, 1);
        assert!(!s.seed_step_cooperative().unwrap());
    }
    assert_eq!(delta(|c| c.ae_seed_ram_sheds), 1);
    assert_seed_ends_reconcile(&before, 0, "RAM shed");

    start_seed(&s);
    s.seed_restart_count.set(MAX_SEED_RESTARTS);
    assert!(s.restart_or_fallback(true).unwrap());
    assert!(s.is_leaf_seeded());
    assert_eq!((delta(|c| c.ae_seed_restarts), delta(|c| c.ae_seed_completions)), (2, 2));
    assert_seed_ends_reconcile(&before, 0, "synchronous fallback");

    s.invalidate_leaf_seed();
    start_seed(&s);
    s.clear().unwrap();
    assert_eq!(delta(|c| c.ae_seed_dropped), 1);
    assert_seed_ends_reconcile(&before, 0, "truncate");

    base(&mut s);
    s.invalidate_leaf_seed();
    start_seed(&s);
    assert_seed_ends_reconcile(&before, 1, "in flight");
    drop(s);
    assert_eq!(delta(|c| c.ae_seed_dropped), 2);
    assert_seed_ends_reconcile(&before, 0, "store drop");
    assert_eq!(delta(|c| c.ae_seed_corrupt_sidecars), 0);
}

fn cleanup_forget_and_lone_gc_commits(s: &mut Store) -> Vec<Vec<u8>> {
    let victims: Vec<(Vec<u8>, Version)> = (0..60u32)
        .map(key)
        .filter_map(|k| s.get_versioned(&k).unwrap().map(|cur| (k, cur.version)))
        .collect();
    assert!(victims.len() > 10);
    s.accumulate_forget(&victims, true);
    let (released, plan) = s.cleanup_forget_batch().unwrap();
    assert_eq!(released, 0);
    let plan = plan.expect("an off-core cleanup forget plan over the runs");
    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
    s.compact_commit(written).unwrap();
    assert!(!s.seed_step_cooperative().unwrap());

    s.compact_runs(10_000).unwrap();
    for i in 0..12u32 {
        let phys = 6_000 + i as u64;
        s.apply_at(format!("t{i:03}").as_bytes(), None, vp(phys, 2), phys).unwrap();
        if i % 4 == 3 {
            s.flush().unwrap();
        }
    }
    s.advance_durable_gc_watermark(9_000);
    let plan = s.compact_plan_tombstone_gc(10_000).unwrap().expect("a bottommost lone-GC merge");
    let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
    let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
    let committed = s.compact_commit(written).unwrap();
    assert!(!committed.current_dropped_tombstones.is_empty(), "the lone-GC merge dropped current tombstones");
    victims.into_iter().map(|(k, _)| k).collect()
}

#[test]
fn seed_carries_cleanup_forget_and_lone_gc_commits() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    for pinned in [true, false] {
        let _cap = seed_policy::pin_cap_for_test(if pinned { 1 << 40 } else { 0 });
        let mut s = store();
        base(&mut s);
        prepare(&s);
        s.set_gc_grace_ms(1);
        assert!(!s.seed_step_cooperative().unwrap());
        let epoch = s.engine().runs_removed_epoch();
        let forgotten = cleanup_forget_and_lone_gc_commits(&mut s);
        let removals = s.engine().runs_removed_epoch() - epoch;
        assert!(removals >= 2, "the cleanup forget and lone-GC commits removed runs: {removals}");
        assert!(forgotten.iter().all(|k| s.engine().version(k).unwrap().is_none()), "the cleanup forget removed its keys");
        while !s.seed_step_cooperative().unwrap() {}
        let [carries, merge_restarts, cap, revoked] = s.seed_run_causes();
        if pinned {
            assert!(carries >= 2, "the pinned seed carried the commits: {carries}");
            assert_eq!((merge_restarts, cap, revoked), (0, 0, 0));
        } else {
            assert_eq!(carries, 0);
            assert!(merge_restarts >= 1, "knockout (no pin): the seed restarts on the commits");
        }
        assert_exact(&exact_state(&s), &recompute(&s), "seed across cleanup forget and lone GC vs recompute");
    }
}

#[test]
fn spilled_seed_journal_carries_merges_and_stays_exact() {
    if crate::testutil::isolated_env_test(&[("VALS_MERGE_TIER_MIN_K", "2"), ("VALS_MERGE_FORCE_RUNS", "")]) {
        return;
    }
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let _cap = seed_policy::pin_cap_for_test(1 << 40);
    let _policy = seed_policy::policy_for_test(true, 25, 1024);
    let mut s = store();
    base(&mut s);
    prepare(&s);
    s.set_gc_grace_ms(1);
    let (done, steps, _) = seed_under_merges(&mut s, 400);
    assert!(done, "the spilled seed completes under merges");
    assert!(steps >= 10, "{steps}");
    let (_, _, spills) = s.seed_test_counts();
    assert_eq!(spills, 1, "the journal reached its cap and spilled");
    let [carries, merge_restarts, cap, revoked] = s.seed_run_causes();
    assert!(carries >= 10, "merges carried while spilling: {carries}");
    assert_eq!((merge_restarts, cap, revoked), (0, 0, 0));
    assert_exact(&exact_state(&s), &recompute(&s), "spilled and carried seed vs recompute");
}

#[test]
fn pressure_revocation_of_the_oldest_seed_passes_seniority_to_the_next() {
    let _limit = seed_permit::limit_for_test(4, 0);
    let _headroom = crate::ram::override_for_test(0, u64::MAX);
    let mut phys = 10_000u64;
    let mut s = store();
    base(&mut s);
    prepare(&s);
    let mut t = store();
    base(&mut t);
    prepare(&t);
    let _cap = seed_policy::pin_cap_for_test(1);
    start_seed(&s);
    start_seed(&t);
    s.engine_mut().revoke_run_pins();
    assert!(!s.seed_step_cooperative().unwrap());
    assert_eq!(s.seed_run_causes()[3], 1, "region pressure revoked the oldest seed's pin");
    start_seed(&s);
    assert!(!t.seed_step_cooperative().unwrap());
    interleave(&mut t, 0, &mut phys);
    assert!(t.engine().pinned_retired_bytes() > 1, "the next seed is now the oldest and holds past the cap");
    assert!(!t.seed_step_cooperative().unwrap());
    assert_eq!(t.seed_run_causes()[2], 0, "the new oldest seed is never restarted by the cap");
    interleave(&mut s, 0, &mut phys);
    assert_eq!(s.engine().pinned_retired_bytes(), 0, "the restarted seed is the youngest and over the cap at its commit");
    assert!(!s.seed_step_cooperative().unwrap());
    assert_eq!(s.seed_run_causes()[2], 1);
    while !t.seed_step_cooperative().unwrap() {
        interleave(&mut t, 3, &mut phys);
    }
    assert_exact(&exact_state(&t), &recompute(&t), "the senior seed vs recompute");
}
