use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use vals_store::index::IndexIntegrityError;
use vals_store::run::EncodeIntegrityError;

use crate::zone::ZoneId;

pub const INTEGRITY_FAIL_FAST_EXIT_CODE: u8 = 75;

pub const FAIL_FAST_SHUTDOWN_DEADLINE: Duration = Duration::from_secs(30);

static REQUESTED: AtomicBool = AtomicBool::new(false);

pub fn fail_fast_requested() -> bool {
    REQUESTED.load(Ordering::SeqCst)
}

pub(crate) fn request_fail_fast() {
    REQUESTED.store(true, Ordering::SeqCst);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailFastPolicy {
    pub attempts: u32,
    pub window: Duration,
}

impl FailFastPolicy {
    pub fn from_settings(settings: &crate::node_settings::NodeSettings) -> Self {
        FailFastPolicy {
            attempts: settings.integrity_fail_fast_attempts,
            window: Duration::from_secs(settings.integrity_fail_fast_secs),
        }
    }

    pub fn enabled(&self) -> bool {
        self.attempts > 0 || !self.window.is_zero()
    }
}

pub(crate) trait IntegrityEvidence: std::fmt::Display {
    fn streak_key(&self) -> &[u8];
    fn single_bit_xor(&self) -> Option<u8>;
    fn single_bit_partial(&self) -> bool;
}

impl IntegrityEvidence for IndexIntegrityError {
    fn streak_key(&self) -> &[u8] {
        &self.first_key
    }

    fn single_bit_xor(&self) -> Option<u8> {
        self.finding.bit_flip_xor()
    }

    fn single_bit_partial(&self) -> bool {
        self.finding.search_partial()
    }
}

impl IntegrityEvidence for EncodeIntegrityError {
    fn streak_key(&self) -> &[u8] {
        &self.source_first_key
    }

    fn single_bit_xor(&self) -> Option<u8> {
        EncodeIntegrityError::single_bit_xor(self)
    }

    fn single_bit_partial(&self) -> bool {
        self.finding.as_ref().is_some_and(vals_store::index::IntegrityFinding::search_partial)
    }
}

#[derive(Clone, Debug)]
pub struct FailFastTrip {
    pub zone: ZoneId,
    pub first_key: Box<[u8]>,
    pub attempts: u32,
    pub elapsed: Duration,
    pub single_bit_xor: Option<u8>,
    pub single_bit_partial: bool,
    pub evidence: String,
}

struct Streak {
    engine: usize,
    since: Instant,
    attempts: u32,
}

#[derive(Default)]
pub(crate) struct IntegrityWatch {
    streaks: HashMap<ZoneId, Streak>,
    tripped: Option<FailFastTrip>,
}

impl IntegrityWatch {
    pub(crate) fn flush_failure(
        &mut self,
        policy: FailFastPolicy,
        zone: ZoneId,
        engine: usize,
        error: &vals_store::EngineError,
        now: Instant,
    ) -> Option<FailFastTrip> {
        if let Some(e) = error.memtable_integrity() { return self.failure(policy, zone, engine, e, now); }
        if let Some(e) = error.encode_integrity() { return self.failure(policy, zone, engine, e, now); }
        self.cleared(zone);
        None
    }

    pub(crate) fn failure(
        &mut self,
        policy: FailFastPolicy,
        zone: ZoneId,
        engine: usize,
        error: &impl IntegrityEvidence,
        now: Instant,
    ) -> Option<FailFastTrip> {
        let key = error.streak_key();
        let streak = self.streaks.entry(zone).or_insert_with(|| Streak {
            engine,
            since: now,
            attempts: 0,
        });
        if streak.engine != engine {
            *streak = Streak { engine, since: now, attempts: 0 };
        }
        streak.attempts = streak.attempts.saturating_add(1);
        let elapsed = now.saturating_duration_since(streak.since);
        let persistent = streak.attempts >= 2
            && ((policy.attempts > 0 && streak.attempts >= policy.attempts)
                || (!policy.window.is_zero() && elapsed >= policy.window));
        if !persistent || self.tripped.is_some() {
            return None;
        }
        let trip = FailFastTrip {
            zone,
            first_key: Box::from(key),
            attempts: streak.attempts,
            elapsed,
            single_bit_xor: error.single_bit_xor(),
            single_bit_partial: error.single_bit_partial(),
            evidence: error.to_string(),
        };
        self.tripped = Some(trip.clone());
        Some(trip)
    }

    pub(crate) fn cleared(&mut self, zone: ZoneId) {
        self.streaks.remove(&zone);
    }

    #[cfg(test)]
    pub(crate) fn streak(&self, zone: ZoneId) -> u32 {
        self.streaks.get(&zone).map_or(0, |s| s.attempts)
    }

    #[cfg(test)]
    pub(crate) fn tripped(&self) -> Option<&FailFastTrip> {
        self.tripped.as_ref()
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReadSite {
    pub(crate) zone: ZoneId,
    pub(crate) shard: usize,
}

const CORRUPT_READ_LOG_EVERY: Duration = Duration::from_secs(10);

const CORRUPT_READ_KEY_HEX_BYTES: usize = 48;

thread_local! {
    static CORRUPT_READ_LOG: std::cell::Cell<(Option<Instant>, u64)> = const { std::cell::Cell::new((None, 0)) };
}

#[cfg(test)]
thread_local! {
    pub(crate) static CORRUPT_READ_LINES: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn reset_corrupt_read_log() {
    CORRUPT_READ_LOG.with(|l| l.set((None, 0)));
    CORRUPT_READ_LINES.with(|l| l.borrow_mut().clear());
}

pub(crate) fn corrupt_read(
    site: ReadSite,
    path: &str,
    key: &[u8],
    ptr: vals_store::vlog::ValuePtr,
    error: &vals_store::vlog::LogError,
) {
    let vals_store::vlog::LogError::Corrupt(reason) = error else {
        return;
    };
    corrupt_record(site, path, "failing the read", key, ptr, reason);
}

pub(crate) fn corrupt_merge_record<D: vals_spdk::BlockDevice>(
    site: ReadSite,
    engine: &mut vals_store::Engine<D>,
    error: &vals_store::EngineError,
) {
    let Some((key, ptr, reason)) = error.corrupt_value_record() else {
        return;
    };
    corrupt_record(
        site,
        "merge rewrite",
        "failing the merge unpublished; the segment is excluded from value-log GC until restart",
        key,
        ptr,
        reason,
    );
    engine.exclude_from_value_gc(ptr.segment);
}

fn corrupt_record(
    site: ReadSite,
    path: &str,
    outcome: &str,
    key: &[u8],
    ptr: vals_store::vlog::ValuePtr,
    reason: &str,
) {
    crate::observe::record_corrupt_read(vals_store::vlog::corrupt_read_kind(reason));
    let now = Instant::now();
    let (last, suppressed) = CORRUPT_READ_LOG.with(std::cell::Cell::get);
    if last.is_some_and(|last| now.saturating_duration_since(last) < CORRUPT_READ_LOG_EVERY) {
        CORRUPT_READ_LOG.with(|l| l.set((last, suppressed + 1)));
        return;
    }
    CORRUPT_READ_LOG.with(|l| l.set((Some(now), 0)));
    let shown = &key[..key.len().min(CORRUPT_READ_KEY_HEX_BYTES)];
    let line = format!(
        "corrupt value read: zone {} shard {} {path} key {}{} ({} bytes) segment {} offset {} len {}: {reason}; \
         {outcome} ({suppressed} more on this thread suppressed since the previous line, at most one per {}s)",
        site.zone,
        site.shard,
        crate::keyscan::hex(shown),
        if shown.len() < key.len() { "…" } else { "" },
        key.len(),
        ptr.segment.0,
        ptr.offset,
        ptr.len,
        CORRUPT_READ_LOG_EVERY.as_secs(),
    );
    log::error!("{line}");
    #[cfg(test)]
    CORRUPT_READ_LINES.with(|l| l.borrow_mut().push(line));
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_store::index::IntegrityFinding;

    fn error(first: &[u8]) -> IndexIntegrityError {
        IndexIntegrityError {
            first_key: Box::from(first),
            entries: 3,
            packed_bytes: 64,
            finding: IntegrityFinding::BitFlip {
                region: "key",
                key_index: 0,
                offset: 19,
                actual: 0x7f,
                expected: 0x6f,
                candidates: 1,
                partial: false,
                window_start: 11,
                window: Box::from(&b"sim36rxr2pe\x7fjm7mt"[..]),
            },
        }
    }

    const POLICY: FailFastPolicy = FailFastPolicy { attempts: 3, window: Duration::from_secs(30) };

    #[test]
    fn persistent_same_block_trips_on_the_attempt_threshold() {
        let mut watch = IntegrityWatch::default();
        let t0 = Instant::now();
        let e = error(b"did:plc:sim36rxr2peo");
        assert!(watch.failure(POLICY, 2, 7, &e, t0).is_none());
        assert!(watch.failure(POLICY, 2, 7, &e, t0 + Duration::from_secs(1)).is_none());
        let trip = watch.failure(POLICY, 2, 7, &e, t0 + Duration::from_secs(2)).expect("trips");
        assert_eq!((trip.zone, trip.attempts, trip.single_bit_xor), (2, 3, Some(0x10)));
        assert_eq!(trip.elapsed, Duration::from_secs(2));
        assert!(trip.evidence.contains("xor=0x10"), "{}", trip.evidence);
        assert!(watch.failure(POLICY, 2, 7, &e, t0 + Duration::from_secs(3)).is_none());
        assert!(watch.tripped().is_some());
    }

    #[test]
    fn the_time_window_trips_on_the_second_failure_and_never_on_the_first() {
        let policy = FailFastPolicy { attempts: 0, window: Duration::from_secs(30) };
        let mut watch = IntegrityWatch::default();
        let t0 = Instant::now();
        let e = error(b"k");
        assert!(watch.failure(policy, 1, 1, &e, t0).is_none());
        assert!(watch.failure(policy, 1, 1, &e, t0 + Duration::from_secs(29)).is_none());
        assert!(watch.failure(policy, 1, 1, &e, t0 + Duration::from_secs(30)).is_some());
        let mut watch = IntegrityWatch::default();
        let one = FailFastPolicy { attempts: 1, window: Duration::ZERO };
        assert!(watch.failure(one, 1, 1, &e, t0).is_none());
    }

    #[test]
    fn a_cleared_retry_or_a_new_engine_restarts_the_streak() {
        let mut watch = IntegrityWatch::default();
        let t0 = Instant::now();
        let a = error(b"a");
        let b = error(b"b");
        assert!(watch.failure(POLICY, 1, 1, &a, t0).is_none());
        assert!(watch.failure(POLICY, 1, 1, &a, t0).is_none());
        watch.cleared(1);
        assert_eq!(watch.streak(1), 0);
        assert!(watch.failure(POLICY, 1, 1, &a, t0).is_none());
        assert!(watch.failure(POLICY, 1, 1, &b, t0).is_none());
        assert!(watch.failure(POLICY, 1, 2, &b, t0).is_none());
        assert_eq!(watch.streak(1), 1);
        assert!(watch.failure(POLICY, 3, 1, &b, t0).is_none());
        assert_eq!(watch.streak(1), 1);
        assert!(watch.tripped().is_none());
    }

    #[test]
    fn rekey_keeps_attempts_and_since_for_the_source_engine() {
        let t0 = Instant::now();
        for policy in [POLICY, FailFastPolicy { attempts: 0, window: Duration::from_secs(2) }] {
            let mut watch = IntegrityWatch::default();
            for (i, first) in [b"c", b"b", b"a"].into_iter().enumerate() {
                let trip = watch.failure(policy, 1, 7, &error(first), t0 + Duration::from_secs(i as u64));
                if i < 2 { assert!(trip.is_none()); } else {
                    let trip = trip.expect("re-key must not reset the streak");
                    assert_eq!(trip.attempts, 3);
                    assert_eq!(trip.elapsed, Duration::from_secs(2));
                    assert_eq!(&*trip.first_key, b"a");
                }
            }
        }
    }

    #[test]
    fn encode_order_and_decode_keep_the_source_engine_streak() {
        let t0 = Instant::now();
        for check in ["order", "decode"] {
            let mut watch = IntegrityWatch::default();
            for i in 0..3 {
                let e = EncodeIntegrityError {
                    path: vals_store::run::EncodePath::Flush, check, block: i, entry: i as u64,
                    source_first_key: vec![i as u8].into_boxed_slice(), finding: None, detail: String::new(),
                };
                let trip = watch.failure(POLICY, 1, 7, &e, t0 + Duration::from_secs(i as u64));
                if i < 2 { assert!(trip.is_none()); } else {
                    let trip = trip.expect("changing run block boundaries must not reset the streak");
                    assert_eq!(trip.attempts, 3);
                    assert_eq!(trip.elapsed, Duration::from_secs(2));
                }
            }
        }
    }

    #[test]
    fn non_integrity_flush_failures_restart_attempts_and_the_window() {
        use vals_store::EngineError;
        let errors = [
            EngineError::Log(vals_store::vlog::LogError::DeviceFull),
            EngineError::Snapshot(vals_store::snapshot_io::SnapshotError::DeviceFull),
            EngineError::InsufficientHeadroom { needed_segments: 2, free_segments: 0 },
            EngineError::Io(vals_spdk::BlockError::Io(std::io::Error::other("drain_and_flush_tail"))),
        ];
        let t0 = Instant::now();
        for e in &errors {
            for policy in [FailFastPolicy { attempts: 2, window: Duration::ZERO },
                FailFastPolicy { attempts: 0, window: Duration::from_secs(30) }]
            {
                let mut watch = IntegrityWatch::default();
                let x = EngineError::MemtableIntegrity(Box::new(error(b"x")));
                let y = EngineError::MemtableIntegrity(Box::new(error(b"y")));
                assert!(watch.flush_failure(policy, 1, 7, &x, t0).is_none());
                for secs in [1, 31, 59] {
                    assert!(watch.flush_failure(policy, 1, 7, e, t0 + Duration::from_secs(secs)).is_none());
                    assert_eq!(watch.streak(1), 0);
                }
                assert!(watch.flush_failure(policy, 1, 7, &y, t0 + Duration::from_secs(60)).is_none());
                let trip = watch.flush_failure(policy, 1, 7, &y, t0 + Duration::from_secs(91)).unwrap();
                assert_eq!(trip.attempts, 2);
                assert_eq!(trip.elapsed, Duration::from_secs(31));
            }
        }
    }

    #[test]
    fn a_disabled_policy_never_trips() {
        let off = FailFastPolicy { attempts: 0, window: Duration::ZERO };
        assert!(!off.enabled());
        let mut watch = IntegrityWatch::default();
        let t0 = Instant::now();
        let e = error(b"k");
        for i in 0..1000u64 {
            assert!(watch.failure(off, 1, 1, &e, t0 + Duration::from_secs(i)).is_none());
        }
        assert_eq!(watch.streak(1), 1000);
    }
}
