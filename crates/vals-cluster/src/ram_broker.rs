use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum Class { Seed, Flush, Merge }

type Waiter = (Class, usize);
struct Wait { first: u64, touched: u64, bytes: u64, input_bytes: u64 }

impl Class {
    fn index(self) -> usize { match self { Self::Seed => 0, Self::Flush => 1, Self::Merge => 2 } }
}
static LONGEST_SWEEP_MS: AtomicU64 = AtomicU64::new(5_000);

fn wait_expiry_ms() -> u64 {
    60_000u64.max(LONGEST_SWEEP_MS.load(Ordering::Relaxed).saturating_mul(4))
}

pub(crate) fn note_sweep_interval(interval: std::time::Duration) {
    LONGEST_SWEEP_MS.fetch_max(interval.as_millis().min(u64::MAX as u128) as u64, Ordering::Relaxed);
}

fn now_ms() -> u64 {
    #[cfg(test)]
    if let Some(now) = TEST_NOW_MS.with(|v| v.get()) { return now; }
    crate::observe::monotonic_ms()
}

#[cfg(test)]
thread_local! {
    static TEST_NOW_MS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

struct Ledger {
    charged: AtomicU64,
    reserved: AtomicU64,
    seed: AtomicU64,
    flush: AtomicU64,
    merge: AtomicU64,
    deferrals: AtomicU64,
    deferred_bytes: [AtomicU64; 3],
    wait_ms: [AtomicU64; 3],
    waits: [AtomicU64; 3],
    waiters: Mutex<HashMap<Waiter, Wait>>,
}

impl Ledger {
    fn new() -> Self {
        Self {
            charged: AtomicU64::new(0), reserved: AtomicU64::new(0),
            seed: AtomicU64::new(0), flush: AtomicU64::new(0), merge: AtomicU64::new(0),
            deferrals: AtomicU64::new(0), waiters: Mutex::new(HashMap::new()),
            deferred_bytes: [const { AtomicU64::new(0) }; 3],
            wait_ms: [const { AtomicU64::new(0) }; 3],
            waits: [const { AtomicU64::new(0) }; 3],
        }
    }

    fn class(&self, class: Class) -> &AtomicU64 {
        match class { Class::Seed => &self.seed, Class::Flush => &self.flush, Class::Merge => &self.merge }
    }

    fn capacity(&self, budget: u64, pool: u64) -> u64 {
        pool.min(budget.saturating_sub(self.charged.load(Ordering::Acquire)))
    }

    fn finish_wait(&self, class: Class, wait: &Wait, now: u64) {
        self.wait_ms[class.index()].fetch_add(now.saturating_sub(wait.first), Ordering::Relaxed);
        self.waits[class.index()].fetch_add(1, Ordering::Relaxed);
    }

    fn clear_wait(&self, waiter: Waiter) {
        if let Some(wait) = self.waiters.lock().unwrap().remove(&waiter) {
            self.finish_wait(waiter.0, &wait, now_ms());
        }
    }

    fn defer(&self, waiter: Waiter, bytes: u64) {
        self.deferrals.fetch_add(1, Ordering::Relaxed);
        let now = now_ms();
        let mut waiters = self.waiters.lock().unwrap();
        self.deferred_bytes[waiter.0.index()].fetch_add(bytes, Ordering::Relaxed);
        waiters.retain(|(class, _), wait| {
            let keep = now.saturating_sub(wait.touched) < wait_expiry_ms();
            if !keep { self.finish_wait(*class, wait, now); }
            keep
        });
        let wait = waiters.entry(waiter).or_insert(Wait { first: now, touched: now, bytes, input_bytes: 0 });
        wait.touched = now;
        wait.bytes = bytes;
    }

    fn pending_merge_input_bytes(&self) -> u64 {
        self.waiters.lock().unwrap().iter()
            .filter(|((class, _), _)| *class == Class::Merge).map(|(_, w)| w.input_bytes).sum()
    }

    fn oldest_wait_ms(&self) -> u64 {
        let now = now_ms();
        let mut waiters = self.waiters.lock().unwrap();
        waiters.retain(|(class, _), wait| {
            let keep = now.saturating_sub(wait.touched) < wait_expiry_ms();
            if !keep { self.finish_wait(*class, wait, now); }
            keep
        });
        waiters.values().map(|wait| now.saturating_sub(wait.first)).max().unwrap_or(0)
    }
}

fn global() -> &'static Ledger {
    static LEDGER: OnceLock<Ledger> = OnceLock::new();
    LEDGER.get_or_init(Ledger::new)
}

pub(crate) fn note_deferred_merge_input(id: usize, input_bytes: u64) {
    let source = ledger_ref();
    let mut waiters = source.get().waiters.lock().unwrap();
    if let Some(wait) = waiters.get_mut(&(Class::Merge, id)) {
        wait.input_bytes = input_bytes;
    }
}

pub(crate) fn pending_merge_input_bytes() -> u64 {
    global().pending_merge_input_bytes()
}

pub(crate) fn class_metrics() -> Vec<(&'static str, u64, u64, u64, u64)> {
    let ledger = global();
    let waiters = ledger.waiters.lock().unwrap();
    ["seed", "flush", "merge"].iter().enumerate().map(|(i, class)| {
        let pending = waiters.iter().filter(|((c, _), _)| c.index() == i)
            .map(|(_, wait)| wait.bytes).sum();
        (*class, ledger.deferred_bytes[i].load(Ordering::Relaxed),
            ledger.wait_ms[i].load(Ordering::Relaxed), ledger.waits[i].load(Ordering::Relaxed), pending)
    }).collect()
}

#[cfg(test)]
thread_local! {
    static TEST_LEDGER: std::cell::RefCell<Option<(std::sync::Arc<Ledger>, Option<(u64, u64)>)>> =
        const { std::cell::RefCell::new(None) };
}

#[derive(Clone)]
enum LedgerRef {
    Global(&'static Ledger),
    #[cfg(test)]
    Local(std::sync::Arc<Ledger>),
}

impl LedgerRef {
    fn get(&self) -> &Ledger {
        match self {
            Self::Global(ledger) => ledger,
            #[cfg(test)]
            Self::Local(ledger) => ledger,
        }
    }

    fn publish(&self) {
        if !matches!(self, Self::Global(_)) { return; }
        let limits = limits();
        let ledger = self.get();
        let g = crate::observe::gauges();
        g.ram_charged_bytes.set(ledger.charged.load(Ordering::Relaxed));
        g.ram_reserved_ae_seed_bytes.set(ledger.seed.load(Ordering::Relaxed));
        g.ram_reserved_flush_bytes.set(ledger.flush.load(Ordering::Relaxed));
        g.ram_reserved_merge_bytes.set(ledger.merge.load(Ordering::Relaxed));
        if let Some((budget, pool)) = limits {
            let cap = ledger.capacity(budget, pool);
            g.ram_reserve_capacity_bytes.set(cap);
            g.ram_reserve_debt_bytes.set(ledger.reserved.load(Ordering::Relaxed).saturating_sub(cap));
        }
        g.ram_reserve_oldest_wait_ms.set(ledger.oldest_wait_ms());
    }
}

fn ledger_ref() -> LedgerRef {
    #[cfg(test)]
    if let Some(ledger) = TEST_LEDGER.with(|v| v.borrow().as_ref().map(|(ledger, _)| ledger.clone())) {
        return LedgerRef::Local(ledger);
    }
    LedgerRef::Global(global())
}

fn limits() -> Option<(u64, u64)> {
    #[cfg(test)]
    if let Some(limits) = TEST_LEDGER.with(|v| v.borrow().as_ref().map(|(_, limits)| *limits)) {
        return limits;
    }
    static LIMITS: OnceLock<Option<(u64, u64)>> = OnceLock::new();
    *LIMITS.get_or_init(|| {
        let budget = crate::ram::budget_bytes()?;
        let settings = crate::node_settings::process();
        let pool = settings.ram_maint_reserve_bytes.unwrap_or(budget / 100 * 19);
        let dma = settings.ram_dma_scratch_bytes;
        global().charged.fetch_add(dma, Ordering::AcqRel);
        Some((budget, pool))
    })
}

pub(crate) fn clear_waiter(class: Class, id: usize) {
    let source = ledger_ref();
    source.get().clear_wait((class, id));
    source.publish();
}

pub(crate) fn clear_maintenance_waiters(id: usize) {
    let source = ledger_ref();
    source.get().clear_wait((Class::Flush, id));
    source.get().clear_wait((Class::Merge, id));
    source.publish();
}

pub(crate) fn charge(old: u64, new: u64) {
    let source = ledger_ref();
    let charged = &source.get().charged;
    if new >= old { charged.fetch_add(new - old, Ordering::AcqRel); }
    else { charged.fetch_sub(old - new, Ordering::AcqRel); }
    source.publish();
}

fn grow(source: &LedgerRef, waiter: Waiter, bytes: u64) -> bool {
    let ledger = source.get();
    let limits = limits();
    if limits.is_some() && crate::ram::ram_over_budget() {
        ledger.defer(waiter, bytes);
        if matches!(source, LedgerRef::Global(_)) { crate::observe::counters().ram_reserve_deferrals.inc(); }
        source.publish();
        return false;
    }
    let mut current = ledger.reserved.load(Ordering::Acquire);
    loop {
        if limits.is_some_and(|(budget, pool)|
            bytes > ledger.capacity(budget, pool).saturating_sub(current)) {
            ledger.defer(waiter, bytes);
            if matches!(source, LedgerRef::Global(_)) { crate::observe::counters().ram_reserve_deferrals.inc(); }
            source.publish();
            return false;
        }
        let Some(next) = current.checked_add(bytes) else { return false; };
        match ledger.reserved.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break,
            Err(actual) => current = actual,
        }
    }
    finish_grow(source, waiter, bytes);
    true
}

#[track_caller]
fn grow_overdraw(source: &LedgerRef, waiter: Waiter, bytes: u64) -> u64 {
    let ledger = source.get();
    let mut current = ledger.reserved.load(Ordering::Acquire);
    let applied = loop {
        let next = current.saturating_add(bytes);
        match ledger.reserved.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => break next - current,
            Err(actual) => current = actual,
        }
    };
    finish_grow(source, waiter, applied);
    if applied < bytes || limits().is_some_and(|(budget, _)| bytes > budget) {
        static LAST_WARN_MS: AtomicU64 = AtomicU64::new(u64::MAX);
        let now = crate::observe::monotonic_ms();
        if LAST_WARN_MS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
            (last == u64::MAX || now.saturating_sub(last) >= 60_000).then_some(now)
        }).is_ok() {
            let caller = std::panic::Location::caller();
            log::warn!(
                "RAM broker oversized overdraw: class={:?} requested_bytes={bytes} applied_bytes={applied} reserved_bytes={} caller={}:{}",
                waiter.0, ledger.reserved.load(Ordering::Acquire), caller.file(), caller.line(),
            );
        }
    }
    applied
}

fn finish_grow(source: &LedgerRef, waiter: Waiter, bytes: u64) {
    let ledger = source.get();
    ledger.class(waiter.0).fetch_add(bytes, Ordering::AcqRel);
    ledger.clear_wait(waiter);
    source.publish();
}

pub(crate) struct Reservation {
    source: LedgerRef,
    waiter: Waiter,
    bytes: u64,
}

impl Reservation {
    pub(crate) fn bytes(&self) -> u64 { self.bytes }
    pub(crate) fn class(&self) -> Class { self.waiter.0 }

    pub(crate) fn try_grow(&mut self, bytes: u64) -> bool {
        if grow(&self.source, self.waiter, bytes) { self.bytes += bytes; true } else { false }
    }

    #[track_caller]
    pub(crate) fn overdraw_grow(&mut self, bytes: u64) {
        self.bytes += grow_overdraw(&self.source, self.waiter, bytes);
    }

    pub(crate) fn shrink(&mut self, bytes: u64) {
        let bytes = bytes.min(self.bytes);
        self.bytes -= bytes;
        let ledger = self.source.get();
        ledger.class(self.waiter.0).fetch_sub(bytes, Ordering::AcqRel);
        ledger.reserved.fetch_sub(bytes, Ordering::AcqRel);
        self.source.publish();
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.shrink(self.bytes);
        self.source.get().clear_wait(self.waiter);
        self.source.publish();
    }
}

pub(crate) fn try_reserve(class: Class, id: usize, bytes: u64) -> Option<Reservation> {
    let source = ledger_ref();
    let waiter = (class, id);
    grow(&source, waiter, bytes).then(|| Reservation { source, waiter, bytes })
}

#[track_caller]
pub(crate) fn reserve_overdraw(class: Class, bytes: u64) -> Reservation {
    let source = ledger_ref();
    let waiter = (class, 0);
    let bytes = grow_overdraw(&source, waiter, bytes);
    Reservation { source, waiter, bytes }
}

#[cfg(test)]
pub(crate) struct TestOverride(Option<(std::sync::Arc<Ledger>, Option<(u64, u64)>)>);
#[cfg(test)]
impl Drop for TestOverride {
    fn drop(&mut self) { TEST_LEDGER.with(|v| *v.borrow_mut() = self.0.take()); }
}
#[cfg(test)]
pub(crate) fn override_for_test(budget: Option<u64>, pool: u64) -> TestOverride {
    let previous = TEST_LEDGER.with(|v| {
        let mut v = v.borrow_mut();
        let ledger = v.as_ref().map_or_else(|| std::sync::Arc::new(Ledger::new()), |(ledger, _)| ledger.clone());
        v.replace((ledger, budget.map(|budget| (budget, pool))))
    });
    TestOverride(previous)
}

#[cfg(test)]
pub(crate) fn charged_for_test() -> u64 {
    test_snapshot().0
}

#[cfg(test)]
fn test_snapshot() -> (u64, u64, u64, u64) {
    TEST_LEDGER.with(|v| {
        let v = v.borrow();
        let (ledger, limits) = v.as_ref().unwrap();
        let cap = limits.map_or(u64::MAX, |(budget, pool)| ledger.capacity(budget, pool));
        (ledger.charged.load(Ordering::Relaxed), ledger.reserved.load(Ordering::Relaxed),
            ledger.reserved.load(Ordering::Relaxed).saturating_sub(cap), ledger.deferrals.load(Ordering::Relaxed))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ClockGuard;
    impl Drop for ClockGuard {
        fn drop(&mut self) { TEST_NOW_MS.with(|v| v.set(None)); }
    }

    #[test]
    fn repeated_deferrals_finish_one_unique_wait() {
        let _clock = ClockGuard;
        let _limit = override_for_test(None, 0);
        let source = ledger_ref();
        let ledger = source.get();
        TEST_NOW_MS.with(|v| v.set(Some(10)));
        ledger.defer((Class::Merge, 1), 100);
        TEST_NOW_MS.with(|v| v.set(Some(20)));
        ledger.defer((Class::Merge, 1), 200);
        note_deferred_merge_input(1, 800);
        assert_eq!(ledger.pending_merge_input_bytes(), 800);
        TEST_NOW_MS.with(|v| v.set(Some(45)));
        ledger.clear_wait((Class::Merge, 1));
        ledger.clear_wait((Class::Merge, 1));
        assert_eq!(ledger.deferred_bytes[2].load(Ordering::Relaxed), 300);
        assert_eq!(ledger.waits[2].load(Ordering::Relaxed), 1);
        assert_eq!(ledger.wait_ms[2].load(Ordering::Relaxed), 35);
        assert!(ledger.waiters.lock().unwrap().is_empty());
        assert_eq!(ledger.pending_merge_input_bytes(), 0);
    }

    #[test]
    fn expired_wait_finishes_only_once() {
        let _clock = ClockGuard;
        let ledger = Ledger::new();
        TEST_NOW_MS.with(|v| v.set(Some(10)));
        ledger.defer((Class::Seed, 1), 100);
        let elapsed = wait_expiry_ms();
        TEST_NOW_MS.with(|v| v.set(Some(10 + elapsed)));
        assert_eq!(ledger.oldest_wait_ms(), 0);
        assert_eq!(ledger.wait_ms[0].load(Ordering::Relaxed), elapsed);
        ledger.oldest_wait_ms();
        ledger.clear_wait((Class::Seed, 1));
        assert_eq!(ledger.waits[0].load(Ordering::Relaxed), 1);
    }

    #[test]
    fn serving_charge_reduces_capacity() {
        let _limit = override_for_test(Some(30), 20);
        let _rss = crate::ram::override_for_test(0, 30);
        charge(0, 15);
        assert_eq!(test_snapshot().0, 15);
        assert!(try_reserve(Class::Flush, 1, 16).is_none());
        let held = reserve_overdraw(Class::Flush, 20);
        assert_eq!(test_snapshot().2, 5);
        drop(held);
        charge(15, 0);
    }

    #[test]
    fn flush_and_merge_defer_then_run_when_capacity_returns() {
        let _limit = override_for_test(Some(100), 10);
        let _rss = crate::ram::override_for_test(0, 100);
        let seed = try_reserve(Class::Seed, 1, 10).unwrap();
        assert!(try_reserve(Class::Flush, 2, 1).is_none());
        assert!(try_reserve(Class::Merge, 3, 1).is_none());
        drop(seed);
        let flush = try_reserve(Class::Flush, 2, 10).unwrap();
        assert!(try_reserve(Class::Merge, 3, 1).is_none());
        drop(flush);
        assert!(try_reserve(Class::Merge, 3, 10).is_some());
        assert_eq!(test_snapshot().1, 0);
    }

    #[test]
    fn overdraw_bypasses_rss_gate_without_deferring() {
        let _limit = override_for_test(Some(100), 10);
        let _rss = crate::ram::override_for_test(100, 100);
        let before = test_snapshot().3;
        let held = reserve_overdraw(Class::Flush, 12);
        assert_eq!(test_snapshot().3, before);
        assert_eq!(test_snapshot().2, 2);
        drop(held);
        assert_eq!(test_snapshot().1, 0);
    }

    #[test]
    fn overdraw_above_budget_preserves_requested_bytes() {
        let _limit = override_for_test(Some(100), 20);
        let _rss = crate::ram::override_for_test(100, 100);
        let mut held = reserve_overdraw(Class::Flush, 101);
        assert_eq!(held.bytes(), 101);
        held.overdraw_grow(102);
        assert_eq!(held.bytes(), 203);
        assert_eq!(test_snapshot().1, 203);
        assert_eq!(ledger_ref().get().flush.load(Ordering::Relaxed), 203);
        drop(held);
        assert_eq!(test_snapshot().1, 0);
        assert_eq!(ledger_ref().get().flush.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn overdraw_overflow_releases_only_applied_bytes() {
        for class in [Class::Seed, Class::Flush, Class::Merge] {
            for original_first in [false, true] {
                let _limit = override_for_test(Some(100), 20);
                let _rss = crate::ram::override_for_test(0, 100);
                let original = try_reserve(class, 1, 12).unwrap();
                let mut huge = reserve_overdraw(class, u64::MAX);
                assert_eq!(huge.bytes(), u64::MAX - 12);
                assert_eq!(test_snapshot().1, u64::MAX);
                assert_eq!(ledger_ref().get().class(class).load(Ordering::Relaxed), u64::MAX);
                let extra = reserve_overdraw(Class::Merge, 1);
                assert_eq!(extra.bytes(), 0);
                drop(extra);
                huge.shrink(7);
                assert_eq!(test_snapshot().1, u64::MAX - 7);
                if original_first {
                    drop(original);
                    drop(huge);
                } else {
                    drop(huge);
                    assert_eq!(test_snapshot().1, 12);
                    assert_eq!(ledger_ref().get().class(class).load(Ordering::Relaxed), 12);
                    drop(original);
                }
                assert_eq!(test_snapshot().1, 0);
                assert_eq!(ledger_ref().get().class(class).load(Ordering::Relaxed), 0);
                assert!(try_reserve(class, 2, 20).is_some());
            }
        }
    }

    #[test]
    fn overdraw_growth_overflow_preserves_other_reservations() {
        let _limit = override_for_test(None, 0);
        let original = reserve_overdraw(Class::Merge, 12);
        let mut seed = reserve_overdraw(Class::Seed, 8);
        assert!(!seed.try_grow(u64::MAX));
        assert_eq!(seed.bytes(), 8);
        seed.overdraw_grow(u64::MAX);
        assert_eq!(seed.bytes(), u64::MAX - 12);
        assert_eq!(test_snapshot().1, u64::MAX);
        seed.overdraw_grow(u64::MAX);
        assert_eq!(seed.bytes(), u64::MAX - 12);
        drop(seed);
        assert_eq!(test_snapshot().1, 12);
        assert_eq!(ledger_ref().get().seed.load(Ordering::Relaxed), 0);
        assert_eq!(ledger_ref().get().merge.load(Ordering::Relaxed), 12);
        drop(original);
        assert_eq!(test_snapshot().1, 0);
    }

    #[test]
    fn reserves_and_releases_debt() {
        let _limit = override_for_test(Some(100), 20);
        let _rss = crate::ram::override_for_test(0, 100);
        let mut seed = try_reserve(Class::Seed, 1, 12).unwrap();
        assert!(seed.try_grow(8));
        assert!(!seed.try_grow(1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(ledger_ref().get().oldest_wait_ms() > 0);
        assert_eq!(test_snapshot().2, 0);
        let before = test_snapshot().3;
        let merge = reserve_overdraw(Class::Merge, 9);
        assert_eq!(test_snapshot().3, before);
        assert_eq!(test_snapshot().2, 9);
        seed.shrink(10);
        assert_eq!(test_snapshot().2, 0);
        drop(merge);
        drop(seed);
        assert_eq!(test_snapshot().1, 0);
    }

    #[test]
    fn abandoned_waiter_expires() {
        let _limit = override_for_test(Some(100), 10);
        let _rss = crate::ram::override_for_test(0, 100);
        let _clock = ClockGuard;
        let now = wait_expiry_ms().saturating_add(10_000);
        TEST_NOW_MS.with(|v| v.set(Some(now)));
        let held = try_reserve(Class::Seed, 1, 10).unwrap();
        assert!(try_reserve(Class::Flush, 2, 1).is_none());
        let ledger = ledger_ref();
        ledger.get().waiters.lock().unwrap().get_mut(&(Class::Flush, 2)).unwrap().touched =
            now.saturating_sub(wait_expiry_ms() + 1);
        assert_eq!(ledger.get().oldest_wait_ms(), 0);
        drop(held);
    }

    #[test]
    fn sweep_retries_preserve_first_wait_until_success() {
        let _limit = override_for_test(Some(100), 10);
        let _rss = crate::ram::override_for_test(0, 100);
        let _clock = ClockGuard;
        let first = wait_expiry_ms().saturating_add(10_000);
        TEST_NOW_MS.with(|v| v.set(Some(first)));
        let held = try_reserve(Class::Seed, 1, 10).unwrap();
        for sweep in 0..6 {
            TEST_NOW_MS.with(|v| v.set(Some(first + sweep * 5_000)));
            assert!(try_reserve(Class::Flush, 2, 1).is_none());
            assert_eq!(ledger_ref().get().oldest_wait_ms(), sweep * 5_000);
        }
        drop(held);
        assert!(try_reserve(Class::Flush, 2, 1).is_some());
        assert_eq!(ledger_ref().get().oldest_wait_ms(), 0);
    }

    #[test]
    fn different_waiters_do_not_clear_oldest() {
        let _limit = override_for_test(Some(100), 10);
        let _rss = crate::ram::override_for_test(0, 100);
        let held = try_reserve(Class::Seed, 1, 10).unwrap();
        assert!(try_reserve(Class::Flush, 2, 10).is_none());
        std::thread::sleep(std::time::Duration::from_millis(2));
        drop(held);
        let age = ledger_ref().get().oldest_wait_ms();
        let other = try_reserve(Class::Merge, 3, 1).unwrap();
        assert!(ledger_ref().get().oldest_wait_ms() >= age);
        drop(other);
        let _flush = try_reserve(Class::Flush, 2, 1).unwrap();
        assert_eq!(ledger_ref().get().oldest_wait_ms(), 0);
    }
}
