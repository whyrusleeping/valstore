use std::time::{Duration, Instant};

pub(crate) const SEED_DUTY_MAX_PAUSE: Duration = Duration::from_secs(1);
pub(crate) const SEED_DUTY_BURST: Duration = Duration::from_millis(20);
pub(crate) const SEED_DUTY_MIN_DEBT: Duration = Duration::from_millis(1);
pub(crate) const FOREGROUND_HOLD: Duration = Duration::from_millis(200);
pub(crate) const ESCALATED_DUTY_MAX_PCT: u64 = 50;
pub(crate) const PIN_SHED_TARGET_PCT: u64 = 85;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SeedPolicy {
    pub(crate) isolation: bool,
    pub(crate) duty_pct: u64,
    pub(crate) journal_max_bytes: u64,
}

#[cfg(test)]
thread_local! {
    static TEST_POLICY: std::cell::Cell<Option<SeedPolicy>> = const { std::cell::Cell::new(None) };
    static FOREGROUND_EVENTS_IGNORED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static TEST_PIN_CAP: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
    static PINNED_NODE_BYTES: std::sync::atomic::AtomicU64 = const { std::sync::atomic::AtomicU64::new(0) };
    static PIN_LEDGER: std::sync::Mutex<PinLedger> = const { std::sync::Mutex::new(PinLedger::new()) };
    static PIN_REVOKE_ALL: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(not(test))]
static PINNED_NODE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(not(test))]
static PIN_LEDGER: std::sync::Mutex<PinLedger> = std::sync::Mutex::new(PinLedger::new());

pub(crate) struct PinLedger {
    next: u64,
    seeds: std::collections::BTreeMap<u64, u64>,
    shedding: bool,
}

impl PinLedger {
    const fn new() -> Self {
        PinLedger { next: 1, seeds: std::collections::BTreeMap::new(), shedding: false }
    }

    fn observe(&mut self, node: u64, cap: u64) -> u64 {
        let target = cap / 100 * PIN_SHED_TARGET_PCT + cap % 100 * PIN_SHED_TARGET_PCT / 100;
        if node > cap {
            self.shedding = true;
        } else if node <= target {
            self.shedding = false;
        }
        if self.shedding { target } else { cap }
    }

    fn limit(&mut self, ticket: u64, held: u64, node: u64, cap: u64) -> Option<u64> {
        if let Some(bytes) = self.seeds.get_mut(&ticket) {
            *bytes = held;
        }
        let bound = self.observe(node, cap);
        if self.seeds.first_key_value().is_some_and(|(oldest, _)| *oldest == ticket) {
            return None;
        }
        let older: u64 = self.seeds.range(..ticket).map(|(_, bytes)| *bytes).sum();
        Some(bound.saturating_sub(older))
    }
}

fn with_pin_ledger<R>(f: impl FnOnce(&mut PinLedger) -> R) -> Option<R> {
    #[cfg(not(test))]
    return Some(f(&mut PIN_LEDGER.lock().unwrap_or_else(|e| e.into_inner())));
    #[cfg(test)]
    return PIN_LEDGER
        .try_with(|ledger| f(&mut ledger.lock().unwrap_or_else(|e| e.into_inner())))
        .ok();
}

pub(crate) struct PinTicket(u64);

impl PinTicket {
    pub(crate) fn id(&self) -> u64 {
        self.0
    }
}

impl Drop for PinTicket {
    fn drop(&mut self) {
        with_pin_ledger(|ledger| ledger.seeds.remove(&self.0));
    }
}

pub(crate) fn pin_ticket() -> PinTicket {
    let id = with_pin_ledger(|ledger| {
        let id = ledger.next;
        ledger.next += 1;
        ledger.seeds.insert(id, 0);
        id
    });
    PinTicket(id.unwrap_or(0))
}

pub(crate) fn note_pin_held(ticket: u64, held: u64) {
    with_pin_ledger(|ledger| {
        if let Some(bytes) = ledger.seeds.get_mut(&ticket) {
            *bytes = held;
        }
    });
}

pub(crate) fn pin_admits(node: u64) -> bool {
    let Some(cap) = pin_cap() else {
        return false;
    };
    #[cfg(test)]
    if PIN_REVOKE_ALL.with(|v| v.get()) {
        return node < cap;
    }
    with_pin_ledger(|ledger| {
        ledger.observe(node, cap);
        !ledger.shedding
    })
    .unwrap_or(true)
        && node < cap
}

pub(crate) struct PinLimit {
    pub(crate) step: Option<u64>,
    pub(crate) commit: Option<u64>,
    pub(crate) exempt_over_cap: bool,
}

pub(crate) fn pin_limit(ticket: u64, held: u64, node: u64) -> PinLimit {
    let Some(cap) = pin_cap() else {
        return PinLimit { step: Some(0), commit: Some(0), exempt_over_cap: false };
    };
    #[cfg(test)]
    if PIN_REVOKE_ALL.with(|v| v.get()) {
        let step = (node > cap).then_some(0);
        return PinLimit { step, commit: None, exempt_over_cap: false };
    }
    let limit = with_pin_ledger(|ledger| ledger.limit(ticket, held, node, cap)).unwrap_or(Some(0));
    PinLimit { step: limit, commit: limit, exempt_over_cap: limit.is_none() && node > cap }
}

#[cfg(test)]
pub(crate) struct TestPinRevokeAll(bool);

#[cfg(test)]
impl Drop for TestPinRevokeAll {
    fn drop(&mut self) {
        PIN_REVOKE_ALL.with(|v| v.set(self.0));
    }
}

#[cfg(test)]
pub(crate) fn pin_revoke_all_for_test() -> TestPinRevokeAll {
    TestPinRevokeAll(PIN_REVOKE_ALL.with(|v| v.replace(true)))
}

fn with_pinned_node<R>(f: impl FnOnce(&std::sync::atomic::AtomicU64) -> R) -> R {
    #[cfg(not(test))]
    return f(&PINNED_NODE_BYTES);
    #[cfg(test)]
    return PINNED_NODE_BYTES.with(f);
}

pub(crate) fn pin_cap() -> Option<u64> {
    #[cfg(test)]
    if let Some(cap) = TEST_PIN_CAP.with(|c| c.get()) {
        return (cap != 0).then_some(cap);
    }
    let cap = crate::node_settings::process().ae_seed_pin_max_bytes;
    (cap != 0).then_some(cap)
}

pub(crate) fn pinned_node_bytes() -> u64 {
    with_pinned_node(|n| n.load(std::sync::atomic::Ordering::Acquire))
}

pub(crate) fn adjust_pinned_node(old: u64, new: u64) {
    if old == new {
        return;
    }
    with_pinned_node(|n| n.fetch_add(new.wrapping_sub(old), std::sync::atomic::Ordering::AcqRel));
    crate::observe::gauges().ae_seed_pinned_bytes.adjust(old, new);
}

#[cfg(test)]
pub(crate) struct TestPinCap(Option<u64>);

#[cfg(test)]
impl Drop for TestPinCap {
    fn drop(&mut self) {
        TEST_PIN_CAP.with(|c| c.set(self.0.take()));
    }
}

#[cfg(test)]
pub(crate) fn pin_cap_for_test(bytes: u64) -> TestPinCap {
    TestPinCap(TEST_PIN_CAP.with(|c| c.replace(Some(bytes))))
}

pub(crate) fn policy() -> SeedPolicy {
    #[cfg(test)]
    if let Some(policy) = TEST_POLICY.with(|p| p.get()) {
        return policy;
    }
    let s = crate::node_settings::process();
    SeedPolicy {
        isolation: s.ae_seed_isolation != 0,
        duty_pct: s.ae_seed_duty_pct,
        journal_max_bytes: s.ae_seed_journal_max_bytes,
    }
}

pub(crate) fn isolation() -> bool {
    policy().isolation
}

#[cfg(test)]
thread_local! {
    static TEST_SPAN_REGROUP: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) };
}

pub(crate) fn span_regroup() -> bool {
    #[cfg(test)]
    if let Some(enabled) = TEST_SPAN_REGROUP.with(|v| v.get()) {
        return enabled;
    }
    crate::node_settings::process().ae_span_regroup != 0
}

#[cfg(test)]
pub(crate) struct TestSpanRegroup(Option<bool>);

#[cfg(test)]
impl Drop for TestSpanRegroup {
    fn drop(&mut self) {
        TEST_SPAN_REGROUP.with(|v| v.set(self.0));
    }
}

#[cfg(test)]
pub(crate) fn span_regroup_for_test(enabled: bool) -> TestSpanRegroup {
    TestSpanRegroup(TEST_SPAN_REGROUP.with(|v| v.replace(Some(enabled))))
}

pub(crate) fn journal_cap() -> Option<u64> {
    let p = policy();
    (p.isolation && p.journal_max_bytes != 0).then_some(p.journal_max_bytes)
}

pub(crate) fn effective_duty_pct(duty_pct: u64, restarts: u32) -> u64 {
    let base = duty_pct.clamp(1, 100);
    base.checked_shl(restarts.min(7))
        .unwrap_or(100)
        .min(base.max(ESCALATED_DUTY_MAX_PCT))
}

#[derive(Debug, Default)]
pub(crate) struct ForegroundSignal {
    seen: u64,
    active_at: Option<Instant>,
}

impl ForegroundSignal {
    pub(crate) fn busy(&mut self, now: Instant, events: u64, inflight: bool) -> bool {
        #[cfg(test)]
        let events = if FOREGROUND_EVENTS_IGNORED.with(|v| v.get()) { self.seen } else { events };
        if inflight || events != self.seen {
            self.seen = events;
            self.active_at = Some(now);
        }
        self.active_at
            .is_some_and(|at| now.saturating_duration_since(at) < FOREGROUND_HOLD)
    }
}

#[derive(Debug, Default)]
pub(crate) struct DutyBudget {
    debt: Duration,
    credit: Duration,
    last: Option<Instant>,
}

impl DutyBudget {
    pub(crate) fn charge(&mut self, now: Instant, cost: Duration, busy: bool, duty_pct: u64) -> Duration {
        let previous = self.last.replace(now);
        if !busy || duty_pct >= 100 {
            self.debt = Duration::ZERO;
            self.credit = SEED_DUTY_BURST;
            return Duration::ZERO;
        }
        let pct = duty_pct.max(1) as u32;
        let elapsed = previous.map_or(cost, |at| now.saturating_duration_since(at));
        let earned = elapsed.saturating_mul(pct) / 100;
        let paid = earned.min(self.debt);
        self.debt -= paid;
        self.credit = (self.credit + (earned - paid)).min(SEED_DUTY_BURST);
        let covered = cost.min(self.credit);
        self.credit -= covered;
        self.debt = self.debt.saturating_add(cost - covered);
        if self.debt < SEED_DUTY_MIN_DEBT {
            return Duration::ZERO;
        }
        (self.debt.saturating_mul(100) / pct).min(SEED_DUTY_MAX_PAUSE)
    }
}

#[cfg(test)]
pub(crate) struct TestPolicy(Option<SeedPolicy>);

#[cfg(test)]
impl Drop for TestPolicy {
    fn drop(&mut self) {
        TEST_POLICY.with(|p| p.set(self.0.take()));
    }
}

#[cfg(test)]
pub(crate) fn policy_for_test(isolation: bool, duty_pct: u64, journal_max_bytes: u64) -> TestPolicy {
    let previous = TEST_POLICY.with(|p| {
        p.replace(Some(SeedPolicy { isolation, duty_pct, journal_max_bytes }))
    });
    TestPolicy(previous)
}

#[cfg(test)]
pub(crate) fn ignore_foreground_events_for_test(ignored: bool) {
    FOREGROUND_EVENTS_IGNORED.with(|v| v.set(ignored));
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Shard {
        now: Instant,
        seed: Duration,
        foreground: Duration,
        events: u64,
        next_arrival: Instant,
        apply_cost: Duration,
        signal: ForegroundSignal,
        budget: DutyBudget,
    }

    const APPLY_EVERY: Duration = Duration::from_millis(1);
    const APPLY_COST: Duration = Duration::from_micros(600);

    impl Shard {
        fn new() -> Self {
            Self::with_apply_cost(APPLY_COST)
        }

        fn with_apply_cost(apply_cost: Duration) -> Self {
            let now = Instant::now();
            Shard {
                now, seed: Duration::ZERO, foreground: Duration::ZERO, events: 0, next_arrival: now, apply_cost,
                signal: ForegroundSignal::default(), budget: DutyBudget::default(),
            }
        }

        fn drain_applies(&mut self, applies: bool) {
            while applies && self.next_arrival <= self.now {
                self.next_arrival += APPLY_EVERY;
                self.events += 1;
                self.now += self.apply_cost;
                self.foreground += self.apply_cost;
            }
        }

        fn sleep(&mut self, pause: Duration, applies: bool) {
            let end = self.now + pause;
            loop {
                self.drain_applies(applies);
                if self.now >= end {
                    break;
                }
                self.now = if applies { self.next_arrival.min(end) } else { end };
            }
        }

        fn run(&mut self, cost: Duration, duty_pct: u64, restarts: u32, applies: bool, inflight_only: bool, total: Duration) -> f64 {
            let (seed0, wall0) = (self.seed, self.now);
            if !applies {
                self.next_arrival = self.now + total * 2;
            } else if self.next_arrival > self.now + APPLY_EVERY {
                self.next_arrival = self.now;
            }
            while self.now - wall0 < total {
                self.drain_applies(applies);
                let events = if inflight_only { 0 } else { self.events };
                let before = self.signal.busy(self.now, events, false);
                self.now += cost;
                self.seed += cost;
                let busy = before | self.signal.busy(self.now, events, false);
                let pct = effective_duty_pct(duty_pct, restarts);
                let pause = self.budget.charge(self.now, cost, busy, pct);
                self.sleep(pause, applies);
            }
            (self.seed - seed0).as_secs_f64() / (self.now - wall0).as_secs_f64()
        }
    }

    #[test]
    fn sustained_batch_apply_load_holds_seed_duty_without_gate_inflight() {
        for cost in [Duration::from_micros(40), Duration::from_millis(1), Duration::from_millis(6), Duration::from_millis(30)] {
            let mut shard = Shard::new();
            let share = shard.run(cost, 25, 0, true, false, Duration::from_secs(20));
            assert!(share <= 0.27, "seed share {share} at {cost:?} under sustained applies");
            assert!(share >= 0.15, "seed still progresses: {share} at {cost:?}");
            let mut knockout = Shard::new();
            let share = knockout.run(cost, 25, 0, true, true, Duration::from_secs(20));
            assert!(share >= 0.35, "knockout (gate-inflight-only signal) misses applies: {share} at {cost:?}");
        }
    }

    #[test]
    fn many_short_steps_cannot_evade_the_window() {
        let mut shard = Shard::new();
        let short = shard.run(Duration::from_micros(5), 25, 0, true, false, Duration::from_secs(5));
        assert!(short <= 0.27, "short steps share {short}");
        let mut budget = DutyBudget::default();
        let t0 = Instant::now();
        let mut now = t0;
        let mut seed = Duration::ZERO;
        let mut pauses = 0u32;
        while now - t0 < Duration::from_secs(2) {
            let cost = Duration::from_micros(3);
            now += cost;
            seed += cost;
            let pause = budget.charge(now, cost, true, 25);
            if !pause.is_zero() {
                pauses += 1;
                assert!(pause >= SEED_DUTY_MIN_DEBT * 4, "no sub-debt-threshold pauses: {pause:?}");
            }
            now += pause;
        }
        let share = seed.as_secs_f64() / (now - t0).as_secs_f64();
        assert!((0.20..=0.27).contains(&share), "back-to-back short steps share {share}");
        assert!(pauses <= 2 * 250 + 1, "pauses are batched to at least 1 ms of debt: {pauses} in 2 s");
    }

    #[test]
    fn idle_shard_runs_full_speed_and_resumes_budget_when_busy() {
        let mut shard = Shard::new();
        let idle = shard.run(Duration::from_millis(2), 25, 0, false, false, Duration::from_secs(2));
        assert_eq!(idle, 1.0, "an idle shard seeds at full speed");
        let busy = shard.run(Duration::from_millis(2), 25, 0, true, false, Duration::from_secs(10));
        assert!(busy <= 0.27, "busy share {busy}");
        let full = shard.run(Duration::from_millis(2), 100, 0, true, false, Duration::from_secs(2));
        assert!(full > 0.35, "100% duty is the unbudgeted loop: {full}");
        assert!(shard.foreground > Duration::ZERO);
    }

    #[test]
    fn run_churn_escalation_is_capped() {
        assert_eq!(effective_duty_pct(25, 0), 25);
        assert_eq!(effective_duty_pct(25, 1), 50);
        assert_eq!(effective_duty_pct(25, 2), 50);
        assert_eq!(effective_duty_pct(25, 37), 50);
        assert_eq!(effective_duty_pct(10, 2), 40);
        assert_eq!(effective_duty_pct(60, 3), 60);
        assert_eq!(effective_duty_pct(100, 0), 100);
        assert_eq!(effective_duty_pct(1, 40), 50);
        assert_eq!(effective_duty_pct(0, 0), 1);
        let light = Duration::from_micros(200);
        let mut shard = Shard::with_apply_cost(light);
        let share = shard.run(Duration::from_millis(6), 25, 37, true, false, Duration::from_secs(20));
        assert!(share <= 0.53, "37 restarts stay at the escalation cap: {share}");
        assert!(share >= 0.4, "escalation still applies: {share}");
        let mut base = Shard::with_apply_cost(light);
        let share = base.run(Duration::from_millis(6), 25, 0, true, false, Duration::from_secs(20));
        assert!(share <= 0.27, "no restarts keep the configured duty: {share}");
        let mut uncapped = Shard::with_apply_cost(light);
        let share = uncapped.run(Duration::from_millis(6), 100, 0, true, false, Duration::from_secs(20));
        assert!(share >= 0.7, "the round-1 doubling reached the unbudgeted share: {share}");
    }

    #[test]
    fn foreground_signal_decays_after_the_hold() {
        let mut signal = ForegroundSignal::default();
        let t0 = Instant::now();
        assert!(!signal.busy(t0, 0, false));
        assert!(signal.busy(t0, 1, false));
        assert!(signal.busy(t0 + FOREGROUND_HOLD / 2, 1, false));
        assert!(!signal.busy(t0 + FOREGROUND_HOLD, 1, false));
        assert!(signal.busy(t0 + FOREGROUND_HOLD * 2, 1, true));
        assert!(!signal.busy(t0 + FOREGROUND_HOLD * 4, 1, false));
    }

    #[test]
    fn pin_ledger_sheds_youngest_first_with_hysteresis_and_never_the_oldest() {
        let mut ledger = PinLedger::new();
        for (ticket, bytes) in [(1, 60), (2, 30), (3, 20)] {
            ledger.seeds.insert(ticket, bytes);
        }
        assert_eq!(ledger.limit(1, 60, 110, 100), None, "the oldest is exempt");
        assert!(ledger.shedding);
        assert_eq!(ledger.limit(2, 30, 110, 100), Some(25), "shed down to 85% of the cap");
        assert_eq!(ledger.limit(3, 20, 110, 100), Some(0));
        ledger.seeds.remove(&3);
        assert_eq!(ledger.limit(2, 30, 90, 100), Some(25), "still shedding above the target");
        ledger.seeds.remove(&2);
        assert_eq!(ledger.limit(1, 60, 60, 100), None);
        assert!(!ledger.shedding, "the target ends shedding");
        ledger.seeds.insert(4, 0);
        assert_eq!(ledger.limit(4, 0, 95, 100), Some(40), "back to the full cap below the trigger");
        assert!(!ledger.shedding);
        ledger.seeds.remove(&1);
        assert_eq!(ledger.limit(4, 500, 500, 100), None, "the next oldest inherits the exemption");
        assert_eq!(ledger.observe(u64::MAX, u64::MAX), u64::MAX / 100 * 85 + u64::MAX % 100 * 85 / 100);
    }

    #[test]
    fn test_policy_restores_previous() {
        let outer = policy_for_test(false, 100, 0);
        {
            let _inner = policy_for_test(true, 25, 10);
            assert_eq!(journal_cap(), Some(10));
        }
        assert!(!isolation());
        assert_eq!(journal_cap(), None);
        drop(outer);
    }
}
