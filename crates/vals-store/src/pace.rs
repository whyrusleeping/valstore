use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

const MAINT_DUTY_PCT_DEFAULT: u64 = 33;

const MAINT_FLUSH_BYTES_DEFAULT: u64 = 16 * 1024 * 1024;

const PACE_MIN_SLEEP: Duration = Duration::from_millis(1);

const PACE_MAX_SLEEP: Duration = Duration::from_millis(100);

const PACE_MAX_DEBT: Duration = Duration::from_millis(500);

fn maint_duty_share_pct() -> u64 {
    fn resolve() -> u64 {
        std::env::var("VALS_MAINT_DUTY_PCT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(MAINT_DUTY_PCT_DEFAULT)
    }
    #[cfg(test)]
    return resolve();
    #[cfg(not(test))]
    {
        static VALUE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        *VALUE.get_or_init(resolve)
    }
}

pub fn maint_pacing_enabled() -> bool {
    let share = maint_duty_share_pct();
    share > 0 && share < 100
}

pub fn maint_flush_bytes() -> u64 {
    std::env::var("VALS_MAINT_FLUSH_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MAINT_FLUSH_BYTES_DEFAULT)
}

pub struct MaintPacer {
    debt_ns: Mutex<u64>,
    charged_ns: AtomicU64,
    slept_ns: AtomicU64,
}

impl MaintPacer {
    pub const fn new() -> Self {
        MaintPacer {
            debt_ns: Mutex::new(0),
            charged_ns: AtomicU64::new(0),
            slept_ns: AtomicU64::new(0),
        }
    }

    pub fn charge(&self, share_pct: u64, work: Duration) -> Duration {
        let work_ns = work.as_nanos().min(u64::MAX as u128) as u64;
        self.charged_ns.fetch_add(work_ns, Ordering::Relaxed);
        if share_pct == 0 || share_pct >= 100 {
            return Duration::ZERO;
        }
        let owed_ns = work_ns.saturating_mul(100 - share_pct) / share_pct;
        let sleep_ns = {
            let mut debt = self.debt_ns.lock().unwrap();
            *debt = debt
                .saturating_add(owed_ns)
                .min(PACE_MAX_DEBT.as_nanos() as u64);
            if *debt < PACE_MIN_SLEEP.as_nanos() as u64 {
                return Duration::ZERO;
            }
            let sleep_ns = (*debt).min(PACE_MAX_SLEEP.as_nanos() as u64);
            *debt -= sleep_ns;
            sleep_ns
        };
        let dur = Duration::from_nanos(sleep_ns);
        std::thread::sleep(dur);
        self.slept_ns.fetch_add(sleep_ns, Ordering::Relaxed);
        dur
    }

    pub fn totals(&self) -> (u64, u64) {
        (
            self.charged_ns.load(Ordering::Relaxed),
            self.slept_ns.load(Ordering::Relaxed),
        )
    }
}

impl Default for MaintPacer {
    fn default() -> Self {
        Self::new()
    }
}

static PACER: MaintPacer = MaintPacer::new();

pub struct MaintQueue(AtomicU64);

impl MaintQueue {
    pub const fn new() -> Self {
        MaintQueue(AtomicU64::new(0))
    }

    pub fn queued(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    pub fn started(&self) {
        let _ = self
            .0
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| Some(n.saturating_sub(1)));
    }

    pub fn backlogged(&self) -> bool {
        self.0.load(Ordering::Relaxed) > 0
    }
}

impl Default for MaintQueue {
    fn default() -> Self {
        Self::new()
    }
}

static MAINT_QUEUE: MaintQueue = MaintQueue::new();

static BACKLOG_UNPACED_NS: AtomicU64 = AtomicU64::new(0);

pub fn maint_job_queued() {
    MAINT_QUEUE.queued();
}

pub fn maint_job_started() {
    MAINT_QUEUE.started();
}

pub fn maint_backlogged() -> bool {
    MAINT_QUEUE.backlogged()
}

pub fn maint_pacing_active() -> bool {
    maint_pacing_enabled() && !maint_backlogged()
}

pub fn maint_pace_charge(work: Duration) {
    if maint_pacing_enabled() && maint_backlogged() {
        BACKLOG_UNPACED_NS.fetch_add(
            work.as_nanos().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        PACER.charge(100, work);
        return;
    }
    PACER.charge(maint_duty_share_pct(), work);
}

pub fn maint_unpaced_charge(work: Duration) {
    PACER.charge(100, work);
}

pub fn maint_pace_totals() -> (u64, u64) {
    PACER.totals()
}

pub fn maint_pace_backlog_unpaced_ns() -> u64 {
    BACKLOG_UNPACED_NS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_shares_never_sleep() {
        let p = MaintPacer::new();
        assert_eq!(p.charge(0, Duration::from_millis(50)), Duration::ZERO);
        assert_eq!(p.charge(100, Duration::from_millis(50)), Duration::ZERO);
        assert_eq!(p.charge(150, Duration::from_millis(50)), Duration::ZERO);
        let (charged, slept) = p.totals();
        assert_eq!(charged, Duration::from_millis(150).as_nanos() as u64);
        assert_eq!(slept, 0);
    }

    #[test]
    fn share_50_owes_roughly_the_work() {
        let p = MaintPacer::new();
        let slept = p.charge(50, Duration::from_millis(4));
        assert_eq!(slept, Duration::from_millis(4));
    }

    #[test]
    fn sub_millisecond_debt_accumulates_before_sleeping() {
        let p = MaintPacer::new();
        assert_eq!(p.charge(50, Duration::from_micros(400)), Duration::ZERO);
        assert_eq!(p.charge(50, Duration::from_micros(400)), Duration::ZERO);
        let slept = p.charge(50, Duration::from_micros(400));
        assert_eq!(slept, Duration::from_micros(1200));
    }

    #[test]
    fn queue_is_backlogged_until_every_queued_job_starts() {
        let q = MaintQueue::new();
        assert!(!q.backlogged());
        q.queued();
        q.queued();
        assert!(q.backlogged());
        q.started();
        assert!(q.backlogged());
        q.started();
        assert!(!q.backlogged());
        q.started();
        assert!(!q.backlogged());
        q.queued();
        assert!(q.backlogged());
    }

    #[test]
    fn single_sleep_and_debt_are_capped() {
        let p = MaintPacer::new();
        let slept = p.charge(10, Duration::from_secs(2));
        assert_eq!(slept, PACE_MAX_SLEEP);
        let debt = *p.debt_ns.lock().unwrap();
        assert_eq!(
            debt,
            PACE_MAX_DEBT.as_nanos() as u64 - PACE_MAX_SLEEP.as_nanos() as u64
        );
    }
}
