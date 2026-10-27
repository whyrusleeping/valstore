use super::*;

#[test]
fn priority_and_earliest_waiter_win() {
    let mut r = RebuildRegistry::default();
    let now = Instant::now();
    r.refresh(1, 100, 0, now);
    r.refresh(2, 200, 0, now + Duration::from_secs(1));
    assert!(!r.grant(1, 1, 0, now + Duration::from_secs(2), REBUILD_WAITER_STALE));
    assert!(r.grant(2, 1, 0, now + Duration::from_secs(2), REBUILD_WAITER_STALE));
    assert!(!r.grant(1, 1, 0, now + Duration::from_secs(3), REBUILD_WAITER_STALE));
    r.in_flight -= 1;
    r.refresh(3, 100, 0, now + Duration::from_secs(3));
    assert!(!r.grant(3, 1, 0, now + Duration::from_secs(3), REBUILD_WAITER_STALE));
    assert!(r.grant(1, 1, 0, now + Duration::from_secs(3), REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 0);
}

#[test]
fn stale_top_waiter_is_bypassed() {
    let mut r = RebuildRegistry::default();
    let now = Instant::now();
    let stale = Duration::from_secs(5);
    r.refresh(1, 200, 0, now);
    r.refresh(2, 100, 0, now + stale);
    assert!(r.grant(2, 1, 0, now + stale, stale));
    r.in_flight -= 1;
    r.refresh(1, 200, 0, now + stale);
    assert!(r.grant(1, 1, 0, now + stale, stale));
}

#[test]
fn urgent_extra_is_prioritized_and_bounded() {
    let mut r = RebuildRegistry::default();
    let now = Instant::now();
    r.refresh(1, 40_000, 25_000, now);
    assert!(r.grant(1, 1, 1, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 0, "urgent engines using normal permits do not count");
    r.refresh(2, 24_999, 25_000, now);
    assert!(!r.grant(2, 1, 1, now, REBUILD_WAITER_STALE));
    r.refresh(3, 25_000, 25_000, now);
    r.refresh(4, 30_000, 25_000, now);
    assert!(!r.grant(3, 1, 1, now, REBUILD_WAITER_STALE));
    assert!(!r.grant(4, 1, 0, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 0, "denied grants do not count");
    assert!(r.grant(4, 1, 1, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 1);
    assert!(!r.grant(3, 1, 1, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 1);
    r.in_flight -= 1;
    assert!(r.grant(3, 1, 1, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 2, "the threshold is inclusive");
}

#[test]
fn urgent_threshold_disable_and_refresh() {
    let mut r = RebuildRegistry::default();
    let now = Instant::now();
    r.in_flight = 1;
    r.refresh(1, 500_000, 0, now);
    assert!(!r.grant(1, 1, 1, now, REBUILD_WAITER_STALE));
    r.refresh(1, 25_000, 25_000, now);
    r.refresh(1, 24_999, 25_000, now);
    assert!(!r.grant(1, 1, 1, now, REBUILD_WAITER_STALE));
    r.refresh(1, 25_000, 25_000, now);
    assert!(r.grant(1, 1, 1, now, REBUILD_WAITER_STALE));
    assert_eq!(r.urgent_grants, 1);
}

#[test]
fn completed_and_ongoing_wait_survives_removal() {
    let mut r = RebuildRegistry::default();
    let now = Instant::now();
    r.refresh(1, 100, 0, now);
    r.refresh(2, 100, 0, now + Duration::from_millis(500));
    let end = now + Duration::from_secs(2);
    let total = r.total_wait(end);
    assert_eq!(total, Duration::from_millis(3500));
    r.remove(1, end);
    r.remove(1, end);
    assert_eq!(r.total_wait(end), total);
    r.remove(2, end);
    assert_eq!(r.total_wait(end + Duration::from_secs(5)), total);
}

#[test]
#[cfg(target_os = "linux")]
fn large_sub_filters_map_huge_page_aligned_and_small_ones_stay_on_heap() {
    let small = SubFilter::new(1_000_000, 12, 7);
    assert!(small.bytes() < HUGE_PAGE_BYTES as u64);
    assert!(matches!(small.words, Words::Heap(_)));
    let capacity = 3_000_000;
    let sub = SubFilter::new(capacity, 12, 7);
    let Words::Mapped(m) = &sub.words else { panic!("expected a mapped sub-filter") };
    assert_eq!(m.ptr.as_ptr() as usize % HUGE_PAGE_BYTES, 0);
    assert!(m.huge_bytes as u64 <= sub.bytes() && m.huge_bytes % HUGE_PAGE_BYTES == 0);
    assert_eq!(sub.words.len() as u64 * 8, sub.bytes());
    assert!(sub.words.iter().all(|w| w.load(Ordering::Relaxed) == 0));
    assert_eq!(sub.resident_bytes(), 0);
    let keys: Vec<BloomKey> = (0..capacity).map(|i| BloomKey::new(&i.to_le_bytes())).collect();
    for k in &keys {
        sub.insert(k);
    }
    sub.inserted.store(capacity, Ordering::Relaxed);
    assert!(keys.iter().all(|k| sub.contains(k)));
    let fp = (capacity..capacity + 100_000).filter(|i| sub.contains(&BloomKey::new(&i.to_le_bytes()))).count();
    assert!(fp < 2_000, "{fp} false positives");
    assert_eq!(sub.resident_bytes(), sub.bytes());
    let mut kf = KeyFilter::new(KeyFilterParams { min_keys: capacity, ..KeyFilterParams::from_lookup(|_| None) });
    let mut sink = kf.sink(capacity, 0).unwrap();
    for i in 0..1000u64 {
        sink.insert(&i.to_le_bytes());
    }
    drop(sink);
    assert!((0..1000u64).all(|i| kf.may_contain(&i.to_le_bytes())));
    assert!(kf.resident_bytes() <= kf.filter_bytes());
    kf.reset();
    kf.prune_retired();
    assert_eq!(kf.filter_bytes(), 0);
}
