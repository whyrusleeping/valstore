use std::io::Read;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[doc(hidden)]
pub const COUNTER_BITS: u32 = 16;
const COUNTER_MASK: u64 = (1 << COUNTER_BITS) - 1;

pub const DEFAULT_MAX_SKEW_MS: u64 = 500;

pub fn physical_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version {
    pub hlc: u64,
    pub node: u64,
}

impl Version {
    pub fn wins_over(&self, other: &Version) -> bool {
        self > other
    }

    pub fn physical_ms(&self) -> u64 {
        self.hlc >> COUNTER_BITS
    }
    pub fn counter(&self) -> u64 {
        self.hlc & COUNTER_MASK
    }
}

#[derive(Debug, Clone)]
pub struct Hlc {
    writer: u64,
    last: Arc<AtomicU64>,
    max_skew_ms: u64,
}

impl Hlc {
    pub fn new(_node: u32) -> Self {
        let mut file = std::fs::File::open("/dev/urandom")
            .expect("secure randomness is required for writer identity");
        let writer = loop {
            let mut bytes = [0u8; 8];
            file.read_exact(&mut bytes)
                .expect("secure randomness is required for writer identity");
            let candidate = u64::from_le_bytes(bytes);
            if candidate > u32::MAX as u64 {
                break candidate;
            }
        };
        Self::with_writer_for_test(writer)
    }

    #[doc(hidden)]
    pub fn with_writer_for_test(writer: u64) -> Self {
        Hlc {
            writer,
            last: Arc::new(AtomicU64::new(0)),
            max_skew_ms: DEFAULT_MAX_SKEW_MS,
        }
    }

    pub fn max_skew_ms(&self) -> u64 {
        self.max_skew_ms
    }

    pub fn set_max_skew_ms(&mut self, ms: u64) {
        self.max_skew_ms = ms;
    }

    pub fn now(&self, phys_ms: u64) -> Version {
        self.advance(|last| {
            let last_phys = last >> COUNTER_BITS;
            if phys_ms > last_phys {
                phys_ms << COUNTER_BITS
            } else {
                last + 1
            }
        })
    }

    pub fn observe(&self, remote_hlc: u64, phys_ms: u64) -> Version {
        let remote_hlc = if self.max_skew_ms != 0 {
            let cap = phys_ms.saturating_add(self.max_skew_ms);
            if (remote_hlc >> COUNTER_BITS) > cap {
                cap << COUNTER_BITS
            } else {
                remote_hlc
            }
        } else {
            remote_hlc
        };
        self.advance(|last| {
            let last_phys = last >> COUNTER_BITS;
            let rem_phys = remote_hlc >> COUNTER_BITS;
            let max_phys = last_phys.max(rem_phys).max(phys_ms);
            if max_phys == last_phys || max_phys == rem_phys {
                last.max(remote_hlc) + 1
            } else {
                max_phys << COUNTER_BITS
            }
        })
    }

    fn advance(&self, next: impl Fn(u64) -> u64) -> Version {
        let mut last = self.last.load(Ordering::Relaxed);
        loop {
            let new = next(last);
            match self
                .last
                .compare_exchange_weak(last, new, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => {
                    return Version {
                        hlc: new,
                        node: self.writer,
                    }
                }
                Err(current) => last = current,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn advances_within_same_millisecond() {
        let c = Hlc::new(1);
        let v1 = c.now(1000);
        let v2 = c.now(1000);
        let v3 = c.now(1000);
        assert!(v2 > v1 && v3 > v2, "monotonic within a ms");
        assert_eq!(v1.physical_ms(), 1000);
        assert_eq!(v1.counter(), 0);
        assert!(v1.node > u32::MAX as u64);
        assert_eq!(v2.counter(), 1);
    }

    #[test]
    fn clones_share_monotonic_state() {
        let a = Hlc::with_writer_for_test(7);
        let b = a.clone();
        let first = a.now(1000);
        let second = b.now(1000);
        assert!(second > first);
        assert_eq!(first.node, second.node);
    }

    #[test]
    fn jumps_with_wall_clock() {
        let c = Hlc::new(1);
        let v1 = c.now(1000);
        let v2 = c.now(2000);
        assert!(v2 > v1);
        assert_eq!(v2.physical_ms(), 2000);
        assert_eq!(v2.counter(), 0);
    }

    #[test]
    fn observe_caps_a_runaway_remote_clock() {
        let mut c = Hlc::new(1);
        c.set_max_skew_ms(500);
        let wall = 1_000_000u64;
        let runaway = (wall + 10_000) << COUNTER_BITS;
        let v = c.observe(runaway, wall);
        assert!(
            v.physical_ms() <= wall + 500,
            "runaway remote clamped to wall+ε, got {}",
            v.physical_ms()
        );
        let local = c.now(wall);
        assert!(local.physical_ms() <= wall + 500);
    }

    #[test]
    fn observe_adopts_a_within_epsilon_remote() {
        let mut c = Hlc::new(1);
        c.set_max_skew_ms(500);
        let wall = 1_000_000u64;
        let near = (wall + 200) << COUNTER_BITS;
        assert_eq!(c.observe(near, wall).physical_ms(), wall + 200);
    }

    #[test]
    fn observe_uncapped_when_skew_disabled() {
        let mut c = Hlc::new(1);
        c.set_max_skew_ms(0);
        let wall = 1_000u64;
        let remote = (wall + 10_000_000) << COUNTER_BITS;
        assert_eq!(
            c.observe(remote, wall).physical_ms(),
            wall + 10_000_000,
            "skew=0 adopts remote unconditionally (back-compat)"
        );
    }

    #[test]
    fn never_goes_backward_on_clock_regression() {
        let c = Hlc::new(1);
        let v1 = c.now(5000);
        let v2 = c.now(3000);
        assert!(v2 > v1, "HLC must not regress");
        assert_eq!(v2.physical_ms(), 5000);
    }

    #[test]
    fn observe_dominates_remote_causally() {
        let a = Hlc::new(1);
        let va = a.now(1000);
        let b = Hlc::new(2);
        let vb = b.observe(va.hlc, 900);
        assert!(
            vb > va,
            "observing a remote version yields a dominating one"
        );
        assert!(vb.physical_ms() >= va.physical_ms());
    }

    #[test]
    fn observe_carries_counter_overflow_into_physical_time() {
        let c = Hlc::new(1);
        let wall = 1_000u64;
        let mut prev = c.now(wall);
        for i in 0..65_537u64 {
            let v = c.observe(prev.hlc, wall);
            assert!(v > prev, "HLC regressed at observe #{i}: {prev:?} -> {v:?}");
            prev = v;
        }
        assert!(
            prev.physical_ms() > wall,
            "the counter overflow carried into the physical part"
        );
    }

    #[test]
    fn version_total_order_breaks_ties_by_writer() {
        let x = Version { hlc: 42, node: 1 };
        let y = Version { hlc: 42, node: 2 };
        assert!(y.wins_over(&x));
        assert!(!x.wins_over(&y));
        assert!(!x.wins_over(&x), "equal versions: neither wins");
    }
}
