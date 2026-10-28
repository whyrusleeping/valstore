//! Process-wide client counters and the logging facade, for diagnostics.
//! The counter names and set may change in any revision.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const STRIPES: usize = 16;

#[repr(align(64))]
struct Stripe(AtomicU64);

static NEXT_SLOT: AtomicUsize = AtomicUsize::new(0);

fn stripe_slot() -> usize {
    thread_local! {
        static SLOT: usize = NEXT_SLOT.fetch_add(1, Ordering::Relaxed) % STRIPES;
    }
    SLOT.with(|s| *s)
}

pub struct Counter {
    stripes: [Stripe; STRIPES],
}

impl Counter {
    pub const fn new() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const ZERO: Stripe = Stripe(AtomicU64::new(0));
        Counter {
            stripes: [ZERO; STRIPES],
        }
    }

    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    #[inline]
    pub fn add(&self, n: u64) {
        self.stripes[stripe_slot()]
            .0
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.stripes
            .iter()
            .map(|s| s.0.load(Ordering::Relaxed))
            .sum()
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Gauge(AtomicU64);

impl Gauge {
    pub const fn new() -> Self {
        Gauge(AtomicU64::new(0))
    }
    #[inline]
    pub fn set(&self, v: u64) {
        self.0.store(v, Ordering::Relaxed);
    }
    #[inline]
    pub fn adjust(&self, old: u64, new: u64) {
        self.0.fetch_add(new.wrapping_sub(old), Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

impl Default for Gauge {
    fn default() -> Self {
        Self::new()
    }
}

pub const LATENCY_BUCKETS_US: [u64; 16] = [
    10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 25_000, 50_000, 100_000, 250_000,
    500_000, 1_000_000,
];

pub struct Histogram {
    buckets: [Counter; LATENCY_BUCKETS_US.len()],
    count: Counter,
    sum_us: Counter,
}

impl Histogram {
    pub const fn new() -> Self {
        #[allow(clippy::declare_interior_mutable_const)]
        const Z: Counter = Counter::new();
        Histogram {
            buckets: [Z; LATENCY_BUCKETS_US.len()],
            count: Counter::new(),
            sum_us: Counter::new(),
        }
    }
    #[inline]
    pub fn observe_us(&self, us: u64) {
        let mut i = 0;
        while i < LATENCY_BUCKETS_US.len() && us > LATENCY_BUCKETS_US[i] {
            i += 1;
        }
        if i < LATENCY_BUCKETS_US.len() {
            self.buckets[i].inc();
        }
        self.count.inc();
        self.sum_us.add(us);
    }
    #[inline]
    pub fn observe(&self, d: std::time::Duration) {
        self.observe_us(d.as_micros() as u64)
    }
    pub fn snapshot(&self) -> ([u64; LATENCY_BUCKETS_US.len()], u64, u64) {
        let mut b = [0u64; LATENCY_BUCKETS_US.len()];
        for (i, slot) in self.buckets.iter().enumerate() {
            b[i] = slot.get();
        }
        (b, self.count.get(), self.sum_us.get())
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! counters {
    ($(#[doc = $doc:literal] $name:ident,)+) => {
        pub struct Counters {
            $(#[doc = $doc] pub $name: Counter,)+
        }

        impl Counters {
            const fn new() -> Self {
                Counters { $($name: Counter::new(),)+ }
            }
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub struct CountersSnapshot {
            $(#[doc = $doc] pub $name: u64,)+
        }

        pub fn snapshot() -> CountersSnapshot {
            let c = counters();
            CountersSnapshot { $($name: c.$name.get(),)+ }
        }

        pub fn snapshot_named() -> Vec<(&'static str, &'static str, u64)> {
            let c = counters();
            vec![ $( (stringify!($name), $doc, c.$name.get()), )+ ]
        }

        impl std::fmt::Display for CountersSnapshot {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                $(writeln!(f, "{}: {}", stringify!($name), self.$name)?;)+
                Ok(())
            }
        }
    };
}

counters! {
    /// Failure-detector circuit trips (node now fast-failed).
    detector_trips,
    /// Failure-detector recoveries (probe succeeded, node admitted again).
    detector_recoveries,
    /// Peers added to a transport after construction (membership-borne address).
    peers_added,
    /// Hints recorded for unreachable replicas.
    hints_recorded,
    /// Hints dropped because a node's hint queue was at capacity (saturation).
    hints_dropped,
    /// Writes that blocked on credit-pool exhaustion (backpressure engaged).
    credit_blocked,
    /// Automatic coordinator ring refreshes triggered by a stale-ring refusal (the op is retried once after adopting the current config).
    stale_ring_refreshes,
    /// Proactive coordinator ring refreshes from the background refresh loop (a periodic tick adopts the current config before any op is refused).
    proactive_ring_refreshes,
    /// Outbound connections a client failed at the protocol handshake (the server's hello was missing, malformed, or an unsupported/too-old version).
    handshake_failures,
    /// Digest strong-read hedges fired: a held-back reserve digest activated after the configured hedge delay (a slow-but-not-failed replica), not because a probe failed.
    digest_hedges_fired,
    /// Idempotent reads retried once on a freshly dialed connection after the reused, idle connection they were sent on failed (typically closed by the server's idle timeout).
    idle_conn_retries,
}

static COUNTERS: Counters = Counters::new();

pub fn counters() -> &'static Counters {
    &COUNTERS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_sums_across_threads() {
        static C: Counter = Counter::new();
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    for _ in 0..1000 {
                        C.inc();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(C.get(), 8000);
    }

    #[test]
    fn snapshot_reflects_increments() {
        let before = snapshot().credit_blocked;
        counters().credit_blocked.inc();
        counters().credit_blocked.add(2);
        assert!(snapshot().credit_blocked >= before + 3);
    }

    #[test]
    fn registry_contains_only_client_counters() {
        let mut names: Vec<_> = snapshot_named().into_iter().map(|(name, _, _)| name).collect();
        names.sort_unstable();
        assert_eq!(names, [
            "credit_blocked", "detector_recoveries", "detector_trips",
            "digest_hedges_fired", "handshake_failures", "hints_dropped",
            "hints_recorded", "idle_conn_retries", "peers_added",
            "proactive_ring_refreshes", "stale_ring_refreshes",
        ]);
    }

    #[test]
    fn primitive_defaults_are_empty() {
        assert_eq!(Counter::default().get(), 0);
        let gauge = Gauge::default();
        assert_eq!(gauge.get(), 0);
        gauge.set(7);
        gauge.adjust(7, 3);
        assert_eq!(gauge.get(), 3);
        let histogram = Histogram::default();
        assert_eq!(histogram.snapshot(), ([0; LATENCY_BUCKETS_US.len()], 0, 0));
        histogram.observe_us(10);
        histogram.observe_us(11);
        histogram.observe_us(1_000_001);
        let (buckets, count, sum) = histogram.snapshot();
        assert_eq!(buckets[0], 1);
        assert_eq!(buckets[1], 1);
        assert_eq!(buckets.iter().sum::<u64>(), 2);
        assert_eq!(count, 3);
        assert_eq!(sum, 1_000_022);
    }
}
