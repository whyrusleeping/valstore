use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

#[cfg(target_env = "gnu")]
static LAST_TRIM_MS: AtomicU64 = AtomicU64::new(0);
#[cfg(target_env = "gnu")]
static LAST_SEED_EVENT_MS: AtomicU64 = AtomicU64::new(0);
static ORIGIN: OnceLock<Instant> = OnceLock::new();

fn now_ms() -> u64 {
    ORIGIN.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}

struct Limiter {
    active: AtomicUsize,
    shed_until_ms: AtomicU64,
    cap: usize,
    cooldown_ms: u64,
}

impl Limiter {
    fn new(cap: usize, cooldown_ms: u64) -> Self {
        Self {
            active: AtomicUsize::new(0),
            shed_until_ms: AtomicU64::new(0),
            cap,
            cooldown_ms,
        }
    }

    fn shed(&self, now: u64) {
        if self.cooldown_ms == 0 { return; }
        self.shed_until_ms.fetch_max(now.saturating_add(self.cooldown_ms), Ordering::Relaxed);
    }

    fn try_acquire(&self, now: u64, capped: bool) -> bool {
        if now < self.shed_until_ms.load(Ordering::Relaxed) {
            crate::observe::counters().ae_seed_permit_deferrals.inc();
            return false;
        }
        let mut active = self.active.load(Ordering::Relaxed);
        loop {
            if capped && self.cap != 0 && active >= self.cap {
                crate::observe::counters().ae_seed_permit_deferrals.inc();
                return false;
            }
            match self.active.compare_exchange_weak(active, active + 1, Ordering::AcqRel, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => active = current,
            }
        }
        crate::observe::gauges().ae_seed_active.adjust(0, 1);
        true
    }

    fn release(&self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        crate::observe::gauges().ae_seed_active.adjust(1, 0);
    }
}

#[cfg(not(test))]
type LimiterRef = &'static Limiter;
#[cfg(test)]
type LimiterRef = std::sync::Arc<Limiter>;

#[cfg(not(test))]
fn limiter() -> LimiterRef {
    static LIMITER: OnceLock<Limiter> = OnceLock::new();
    LIMITER.get_or_init(|| Limiter::new(
        crate::node_settings::process().ae_seed_concurrency,
        crate::node_settings::process().ae_seed_shed_cooldown_ms,
    ))
}

#[cfg(test)]
fn limiter() -> LimiterRef {
    TEST_LIMITER.with(|state| state.borrow().clone())
}

pub(super) fn shed() {
    limiter().shed(now_ms());
}

pub(super) fn trim_after_seed() {
    #[cfg(target_env = "gnu")]
    {
        if std::env::var_os("VALS_AE_SEED_TRIM").is_some_and(|value| value == "0") {
            return;
        }
        let now = now_ms();
        LAST_SEED_EVENT_MS.store(now, Ordering::Relaxed);
        let mut last = LAST_TRIM_MS.load(Ordering::Relaxed);
        loop {
            if last != 0 && now.saturating_sub(last) < 30_000 { return; }
            match LAST_TRIM_MS.compare_exchange_weak(last, now, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(current) => last = current,
            }
        }
        let _ = std::thread::Builder::new().name("ae-seed-trim".into()).spawn(|| {
            let deadline = Instant::now() + std::time::Duration::from_secs(5);
            while Instant::now() < deadline
                && now_ms().saturating_sub(LAST_SEED_EVENT_MS.load(Ordering::Relaxed)) < 500
            {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            unsafe { libc::malloc_trim(0); }
        });
    }
}

pub(super) struct SeedPermit {
    limiter: LimiterRef,
}

impl SeedPermit {
    pub(super) fn try_acquire() -> Option<Self> {
        Self::acquire_from(limiter(), now_ms())
    }

    pub(super) fn try_acquire_driver() -> Option<Self> {
        Self::acquire_capped(limiter(), now_ms(), false)
    }

    fn acquire_from(limiter: LimiterRef, now: u64) -> Option<Self> {
        Self::acquire_capped(limiter, now, true)
    }

    fn acquire_capped(limiter: LimiterRef, now: u64, capped: bool) -> Option<Self> {
        if !limiter.try_acquire(now, capped) { return None; }
        Some(Self { limiter })
    }
}

impl Drop for SeedPermit {
    fn drop(&mut self) {
        self.limiter.release();
    }
}

#[cfg(test)]
thread_local! {
    static TEST_LIMITER: std::cell::RefCell<LimiterRef> = std::cell::RefCell::new(
        std::sync::Arc::new(Limiter::new(0, 0))
    );
}

#[cfg(test)]
pub(super) struct TestLimit {
    previous: LimiterRef,
    _local: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
impl Drop for TestLimit {
    fn drop(&mut self) {
        assert_eq!(active_for_test(), 0);
        TEST_LIMITER.with(|state| state.replace(self.previous.clone()));
    }
}

#[cfg(test)]
pub(super) fn limit_for_test(cap: usize, cooldown_ms: u64) -> TestLimit {
    let previous = TEST_LIMITER.with(|state| state.replace(std::sync::Arc::new(Limiter::new(cap, cooldown_ms))));
    TestLimit { previous, _local: std::marker::PhantomData }
}

#[cfg(test)]
pub(super) fn active_for_test() -> usize {
    limiter().active.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Barrier};

    #[test]
    fn contended_cap_and_raii_release() {
        const THREADS: usize = 16;
        const CAP: usize = 4;
        let limiter = Arc::new(Limiter::new(CAP, 0));
        let barrier = Barrier::new(THREADS);
        let acquired = AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..THREADS {
                scope.spawn(|| {
                    barrier.wait();
                    let permit = SeedPermit::acquire_from(limiter.clone(), 1);
                    if permit.is_some() { acquired.fetch_add(1, Ordering::Relaxed); }
                    barrier.wait();
                    let active = limiter.active.load(Ordering::Relaxed);
                    barrier.wait();
                    drop(permit);
                    assert_eq!(active, CAP);
                });
            }
        });
        assert_eq!(acquired.load(Ordering::Relaxed), CAP);
        assert_eq!(limiter.active.load(Ordering::Relaxed), 0);
        let permit = SeedPermit::acquire_from(limiter.clone(), 1).unwrap();
        std::thread::spawn(move || drop(permit)).join().unwrap();
        assert_eq!(limiter.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn cooldown_extends_monotonically_and_expires_at_deadline() {
        let limiter = Arc::new(Limiter::new(1, 100));
        let permit = SeedPermit::acquire_from(limiter.clone(), 1).unwrap();
        std::thread::scope(|scope| {
            for now in [10, 50, 20, 40] {
                let limiter = &limiter;
                scope.spawn(move || limiter.shed(now));
            }
        });
        assert_eq!(limiter.shed_until_ms.load(Ordering::Relaxed), 150);
        limiter.shed(5);
        assert_eq!(limiter.shed_until_ms.load(Ordering::Relaxed), 150);
        assert_eq!(limiter.active.load(Ordering::Relaxed), 1);
        drop(permit);
        assert!(SeedPermit::acquire_from(limiter.clone(), 149).is_none());
        assert_eq!(limiter.active.load(Ordering::Relaxed), 0);
        drop(SeedPermit::acquire_from(limiter.clone(), 150).unwrap());
        limiter.shed(u64::MAX - 10);
        assert_eq!(limiter.shed_until_ms.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn zero_limits_disable_cap_and_cooldown() {
        let limiter = Arc::new(Limiter::new(0, 0));
        limiter.shed(100);
        assert_eq!(limiter.shed_until_ms.load(Ordering::Relaxed), 0);
        let permits: Vec<_> = (0..32)
            .map(|_| SeedPermit::acquire_from(limiter.clone(), 1).unwrap())
            .collect();
        assert_eq!(limiter.active.load(Ordering::Relaxed), 32);
        drop(permits);
        assert_eq!(limiter.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn driver_permits_skip_the_cap_but_honour_the_shed_cooldown() {
        let limiter = Arc::new(Limiter::new(1, 100));
        let capped = SeedPermit::acquire_from(limiter.clone(), 1).unwrap();
        assert!(SeedPermit::acquire_from(limiter.clone(), 1).is_none());
        let drivers: Vec<_> = (0..8)
            .map(|_| SeedPermit::acquire_capped(limiter.clone(), 1, false).unwrap())
            .collect();
        assert_eq!(limiter.active.load(Ordering::Relaxed), 9);
        drop(capped);
        assert!(SeedPermit::acquire_from(limiter.clone(), 1).is_none(), "driver seeds still count as active");
        limiter.shed(10);
        assert!(SeedPermit::acquire_capped(limiter.clone(), 50, false).is_none());
        drop(drivers);
        assert!(SeedPermit::acquire_capped(limiter.clone(), 110, false).is_some());
        assert_eq!(limiter.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_limits_restore_isolated_state() {
        let outer = limit_for_test(1, 100);
        let permit = SeedPermit::try_acquire().unwrap();
        {
            let _inner = limit_for_test(2, 0);
            assert_eq!(active_for_test(), 0);
            let _permit = SeedPermit::try_acquire().unwrap();
        }
        assert_eq!(active_for_test(), 1);
        drop(permit);
        drop(outer);
        assert_eq!(active_for_test(), 0);
    }
}
