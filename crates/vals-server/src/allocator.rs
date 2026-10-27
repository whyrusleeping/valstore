use std::sync::mpsc::{self, Sender};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use vals_cluster::observe;

pub fn interval() -> Result<Duration, String> {
    match std::env::var("VALS_MALLOC_TRIM_SECS") {
        Ok(value) => parse_interval(&value),
        Err(std::env::VarError::NotPresent) => Ok(Duration::ZERO),
        Err(error) => Err(format!("VALS_MALLOC_TRIM_SECS: {error}")),
    }
}

fn parse_interval(value: &str) -> Result<Duration, String> {
    value.parse::<u32>().map(|seconds| Duration::from_secs(seconds as u64))
        .map_err(|_| "VALS_MALLOC_TRIM_SECS must be nonnegative seconds fitting u32 (0 disables)".into())
}

pub struct Trimmer {
    stop: Sender<()>,
    thread: Option<JoinHandle<()>>,
}

pub fn start(interval: Duration) -> Option<Trimmer> {
    if interval.is_zero() { return None; }
    let (stop, rx) = mpsc::channel();
    let thread = observe::spawn_abort_on_panic("vals-malloc-trim", move || {
        // SAFETY: on Linux, pid 0 with PRIO_PROCESS selects only the calling thread; increasing niceness needs no privilege.
        if unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, 19) } != 0 {
            log::warn!("malloc trim: could not lower thread priority: {}", std::io::Error::last_os_error());
        }
        let mut max_ns = 0;
        while matches!(rx.recv_timeout(interval), Err(mpsc::RecvTimeoutError::Timeout)) {
            let before = observe::process_resident_bytes();
            let started = Instant::now();
            // SAFETY: malloc_trim releases only free pages; glibc serializes allocator access and live pointers remain valid.
            unsafe { libc::malloc_trim(0); }
            let ns = started.elapsed().as_nanos() as u64;
            let after = observe::process_resident_bytes();
            let c = observe::counters();
            c.allocator_trim_calls_total.inc();
            c.allocator_trim_ns_total.add(ns);
            if let (Some(before), Some(after)) = (before, after) {
                c.allocator_trim_rss_returned_bytes_total.add(before.saturating_sub(after));
            }
            max_ns = max_ns.max(ns);
            observe::gauges().allocator_trim_max_ns.set(max_ns);
        }
    });
    Some(Trimmer { stop, thread: Some(thread) })
}

impl Drop for Trimmer {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() { thread.join().unwrap(); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trim_interval_and_disable() {
        assert_eq!(parse_interval("10").unwrap(), Duration::from_secs(10));
        assert!(start(parse_interval("0").unwrap()).is_none());
        for invalid in ["-1", "bad", "4294967296"] { assert!(parse_interval(invalid).is_err()); }
    }

    #[test]
    fn background_trim_preserves_live_bytes_and_stops_promptly() {
        let live = vec![0x5a; 1 << 20];
        let before = observe::counters().allocator_trim_calls_total.get();
        let trimmer = start(Duration::from_millis(10));
        let deadline = Instant::now() + Duration::from_secs(2);
        while observe::counters().allocator_trim_calls_total.get() == before {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(trimmer);
        assert!(live.iter().all(|&b| b == 0x5a));
        assert!(observe::counters().allocator_trim_ns_total.get() > 0);
        let idle = start(Duration::from_secs(3600));
        let started = Instant::now();
        drop(idle);
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
