use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

const SAMPLE_INTERVAL_MS: u64 = 100;
const WARN_INTERVAL_MS: u64 = 60_000;

static BUDGET: OnceLock<Option<u64>> = OnceLock::new();
static SAMPLE_ORIGIN: OnceLock<Instant> = OnceLock::new();
static LAST_SAMPLE_MS: AtomicU64 = AtomicU64::new(0);
static CACHED_RSS: AtomicU64 = AtomicU64::new(0);
static CACHED_SWAP: AtomicU64 = AtomicU64::new(0);
static LAST_NEAR_WARN_MS: AtomicU64 = AtomicU64::new(0);
static LAST_BUSY_WARN_MS: AtomicU64 = AtomicU64::new(0);
static CACHED_OVER_BUDGET: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn parse_mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut fields = line.split_whitespace();
    fields.next()?;
    let kib: u64 = fields.next()?.parse().ok()?;
    kib.checked_mul(1024)
}

fn derive_budget(configured: Option<u64>, mem_total: Option<u64>) -> Option<u64> {
    match configured {
        Some(0) => None,
        Some(bytes) => Some(bytes),
        None => mem_total.map(|bytes| ((bytes as u128 * 70) / 100) as u64),
    }
}

pub(crate) fn default_budget() -> Option<u64> {
    let mem_total = std::fs::read_to_string("/proc/meminfo").ok()
        .and_then(|contents| parse_mem_total(&contents));
    derive_budget(None, mem_total)
}

fn resolve_budget() -> Option<u64> {
    let configured = crate::node_settings::process().ram_budget_bytes;
    let budget = configured.map_or_else(default_budget, |bytes| (bytes != 0).then_some(bytes));
    let explicitly_disabled = configured == Some(0);
    if budget.is_none() && !explicitly_disabled {
        log::warn!(
            "RAM admission control disabled: /proc/meminfo MemTotal is unavailable; set ram_budget_bytes explicitly"
        );
    }
    let bytes = budget.unwrap_or(0);
    let gauges = crate::observe::gauges();
    gauges.ram_budget_bytes.set(bytes);
    gauges.ram_headroom_bytes.set(0);
    budget
}

pub(crate) fn budget_bytes() -> Option<u64> {
    *BUDGET.get_or_init(resolve_budget)
}

fn monotonic_ms() -> u64 {
    SAMPLE_ORIGIN
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis() as u64
        + 1
}

fn sampled_memory() -> (u64, u64) {
    let now = monotonic_ms();
    let last = LAST_SAMPLE_MS.load(Ordering::Relaxed);
    if (last == 0 || now.saturating_sub(last) >= SAMPLE_INTERVAL_MS)
        && LAST_SAMPLE_MS
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    {
        if let Some(rss) = crate::observe::process_resident_bytes() {
            CACHED_RSS.store(rss, Ordering::Relaxed);
            crate::observe::gauges().process_resident_bytes.set(rss);
        }
        if let Some(swap) = crate::observe::process_swap_bytes() {
            CACHED_SWAP.store(swap, Ordering::Relaxed);
            crate::observe::gauges().process_swap_bytes.set(swap);
        }
        if let Some(budget) = budget_bytes() {
            CACHED_OVER_BUDGET.store(
                check(
                    CACHED_RSS.load(Ordering::Relaxed),
                    CACHED_SWAP.load(Ordering::Relaxed),
                    budget,
                ),
                Ordering::Relaxed,
            );
        }
    }
    (
        CACHED_RSS.load(Ordering::Relaxed),
        CACHED_SWAP.load(Ordering::Relaxed),
    )
}

fn warn_due(last: &AtomicU64, now: u64) -> bool {
    let mut old = last.load(Ordering::Relaxed);
    loop {
        if old != 0 && now.saturating_sub(old) < WARN_INTERVAL_MS {
            return false;
        }
        match last.compare_exchange_weak(old, now, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true,
            Err(current) => old = current,
        }
    }
}

pub(crate) fn over_budget(rss: u64, budget: u64) -> bool {
    budget != 0 && rss >= budget
}

fn check(rss: u64, swap: u64, budget: u64) -> bool {
    let footprint = rss.saturating_add(swap);
    let gauges = crate::observe::gauges();
    gauges.process_resident_bytes.set(rss);
    gauges.process_swap_bytes.set(swap);
    gauges.ram_budget_bytes.set(budget);
    gauges
        .ram_headroom_bytes
        .set(budget.saturating_sub(footprint));
    let now = monotonic_ms();
    if footprint >= budget.saturating_sub(budget / 10) && warn_due(&LAST_NEAR_WARN_MS, now) {
        log::warn!(
            "process RSS plus swap is approaching its RAM budget: {rss} RSS + {swap} swap of {budget} bytes in use"
        );
    }
    let over = over_budget(footprint, budget);
    if over && warn_due(&LAST_BUSY_WARN_MS, now) {
        log::warn!(
            "RAM admission backpressure engaged at RSS {rss} + swap {swap} / budget {budget} bytes; client writes will receive Busy"
        );
    }
    over
}

pub(crate) fn ram_over_budget() -> bool {
    #[cfg(test)]
    if let Some((rss, swap, budget)) = TEST_OVERRIDE.with(|value| value.get()) {
        return check(rss, swap, budget);
    }

    if budget_bytes().is_none() {
        return false;
    }
    sampled_memory();
    CACHED_OVER_BUDGET.load(Ordering::Relaxed)
}

pub(crate) fn add_zone_engines(count: u64) {
    let gauge = &crate::observe::gauges().zone_engines;
    gauge.adjust(0, count);
}

pub(crate) fn remove_zone_engines(count: u64) {
    let gauge = &crate::observe::gauges().zone_engines;
    gauge.adjust(count, 0);
}

pub(crate) fn zone_engine_count() -> usize {
    crate::observe::gauges().zone_engines.get() as usize
}

pub(crate) fn per_engine_flush_budget(node_budget: u64, engines: usize) -> u64 {
    node_budget / engines.max(1) as u64
}

#[cfg(test)]
thread_local! {
    static TEST_OVERRIDE: std::cell::Cell<Option<(u64, u64, u64)>> = const { std::cell::Cell::new(None) };
}

#[cfg(test)]
pub(crate) struct TestOverride {
    previous: Option<(u64, u64, u64)>,
    _not_send: std::marker::PhantomData<std::rc::Rc<()>>,
}

#[cfg(test)]
impl Drop for TestOverride {
    fn drop(&mut self) {
        TEST_OVERRIDE.with(|value| value.set(self.previous));
    }
}

#[cfg(test)]
pub(crate) fn override_for_test(rss: u64, budget: u64) -> TestOverride {
    override_with_swap_for_test(rss, 0, budget)
}

#[cfg(test)]
pub(crate) fn override_with_swap_for_test(rss: u64, swap: u64, budget: u64) -> TestOverride {
    let previous = TEST_OVERRIDE.with(|value| value.replace(Some((rss, swap, budget))));
    TestOverride {
        previous,
        _not_send: std::marker::PhantomData,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_memtotal_and_derives_default_budget() {
        let total = parse_mem_total("MemFree: 1 kB\nMemTotal:       1000 kB\n").unwrap();
        assert_eq!(total, 1_024_000);
        assert_eq!(derive_budget(None, Some(total)), Some(716_800));
        assert_eq!(derive_budget(Some(123), Some(total)), Some(123));
        assert_eq!(derive_budget(Some(0), Some(total)), None);
        assert_eq!(derive_budget(None, None), None);
    }

    #[test]
    fn budget_threshold_is_inclusive() {
        assert!(!over_budget(99, 100));
        assert!(over_budget(100, 100));
        assert!(over_budget(101, 100));
        assert!(!over_budget(u64::MAX, 0));
    }

    #[test]
    fn swap_counts_against_budget_and_headroom() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        let _guard = override_with_swap_for_test(60, 39, 100);
        assert!(!ram_over_budget());
        assert_eq!(crate::observe::gauges().ram_headroom_bytes.get(), 1);
        drop(_guard);

        let _guard = override_with_swap_for_test(60, 40, 100);
        assert!(ram_over_budget());
        assert_eq!(crate::observe::gauges().process_resident_bytes.get(), 60);
        assert_eq!(crate::observe::gauges().process_swap_bytes.get(), 40);
        assert_eq!(crate::observe::gauges().ram_headroom_bytes.get(), 0);
    }

    #[test]
    fn divides_node_flush_budget_across_live_engines() {
        assert_eq!(per_engine_flush_budget(4_096, 8), 512);
        assert_eq!(per_engine_flush_budget(4_096, 0), 4_096);
    }
}
