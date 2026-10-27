use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_connections: usize,
    pub max_requests: usize,
    pub max_queued_bytes: usize,
    pub initial_timeout: Option<Duration>,
    pub frame_timeout: Option<Duration>,
    pub idle_timeout: Option<Duration>,
    pub write_timeout: Option<Duration>,
}

impl Default for TransportLimits {
    fn default() -> Self {
        TransportLimits {
            max_connections: 4096,
            max_requests: 32_768,
            max_queued_bytes: 512 * 1024 * 1024,
            initial_timeout: Some(Duration::from_secs(5)),
            frame_timeout: Some(Duration::from_secs(30)),
            idle_timeout: Some(Duration::from_secs(120)),
            write_timeout: Some(Duration::from_secs(30)),
        }
    }
}

impl TransportLimits {
    pub fn from_env() -> Result<Self, String> {
        let unbounded = match std::env::var("VALS_TRANSPORT_UNBOUNDED") {
            Ok(v) if v == "1" => true,
            Ok(v) if v == "0" => false,
            Ok(v) => return Err(format!("VALS_TRANSPORT_UNBOUNDED must be 0 or 1, got {v:?}")),
            Err(std::env::VarError::NotPresent) => false,
            Err(e) => return Err(format!("VALS_TRANSPORT_UNBOUNDED: {e}")),
        };
        if unbounded {
            return Ok(TransportLimits {
                max_connections: usize::MAX,
                max_requests: usize::MAX,
                max_queued_bytes: usize::MAX,
                initial_timeout: None,
                frame_timeout: None,
                idle_timeout: None,
                write_timeout: None,
            });
        }
        let defaults = Self::default();
        Ok(TransportLimits {
            max_connections: env_usize("VALS_TRANSPORT_MAX_CONNECTIONS", defaults.max_connections)?,
            max_requests: env_usize("VALS_TRANSPORT_MAX_REQUESTS", defaults.max_requests)?,
            max_queued_bytes: env_usize("VALS_TRANSPORT_MAX_QUEUED_BYTES", defaults.max_queued_bytes)?,
            initial_timeout: Some(env_duration("VALS_TRANSPORT_INITIAL_TIMEOUT_MS", defaults.initial_timeout.unwrap())?),
            frame_timeout: Some(env_duration("VALS_TRANSPORT_FRAME_TIMEOUT_MS", defaults.frame_timeout.unwrap())?),
            idle_timeout: Some(env_duration("VALS_TRANSPORT_IDLE_TIMEOUT_MS", defaults.idle_timeout.unwrap())?),
            write_timeout: Some(env_duration("VALS_TRANSPORT_WRITE_TIMEOUT_MS", defaults.write_timeout.unwrap())?),
        })
    }

    fn validate(&self) -> Result<(), String> {
        let unbounded = self.max_connections == usize::MAX
            && self.max_requests == usize::MAX
            && self.max_queued_bytes == usize::MAX
            && self.initial_timeout.is_none()
            && self.frame_timeout.is_none()
            && self.idle_timeout.is_none()
            && self.write_timeout.is_none();
        if unbounded {
            return Ok(());
        }
        for (name, value) in [
            ("max_connections", self.max_connections),
            ("max_requests", self.max_requests),
            ("max_queued_bytes", self.max_queued_bytes),
        ] {
            if value == 0 {
                return Err(format!("transport {name} must be positive"));
            }
        }
        for (name, value) in [
            ("initial_timeout", self.initial_timeout),
            ("frame_timeout", self.frame_timeout),
            ("idle_timeout", self.idle_timeout),
            ("write_timeout", self.write_timeout),
        ] {
            if value.is_none_or(|duration| duration.is_zero()) {
                return Err(format!("transport {name} must be finite and positive"));
            }
        }
        Ok(())
    }
}

fn env_usize(key: &str, default: usize) -> Result<usize, String> {
    match std::env::var(key) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|&v| v > 0)
            .ok_or_else(|| format!("{key} must be a positive integer, got {value:?}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(format!("{key}: {e}")),
    }
}

fn env_duration(key: &str, default: Duration) -> Result<Duration, String> {
    let ms = env_usize(key, default.as_millis() as usize)?;
    let ms = u64::try_from(ms).map_err(|_| format!("{key} is too large"))?;
    Ok(Duration::from_millis(ms))
}

static LIMITS: OnceLock<TransportLimits> = OnceLock::new();
static CONNECTIONS: AtomicUsize = AtomicUsize::new(0);
static REQUESTS: AtomicUsize = AtomicUsize::new(0);
static QUEUED_BYTES: AtomicUsize = AtomicUsize::new(0);

pub fn configure(limits: TransportLimits) -> Result<(), String> {
    limits.validate()?;
    if let Some(active) = LIMITS.get() {
        return if *active == limits {
            Ok(())
        } else {
            Err(format!(
                "transport limits conflict with process-stable configuration {active:?}"
            ))
        };
    }
    LIMITS.set(limits).map_err(|limits| {
        format!(
            "transport limits conflict with process-stable configuration {:?}",
            LIMITS.get().copied().unwrap_or(limits)
        )
    })
}

pub fn limits() -> TransportLimits {
    *LIMITS.get_or_init(TransportLimits::default)
}

#[derive(Clone, Copy)]
enum CreditKind {
    Connection,
    Request,
}

pub struct Credit(CreditKind);

impl Drop for Credit {
    fn drop(&mut self) {
        let counter = match self.0 {
            CreditKind::Connection => &CONNECTIONS,
            CreditKind::Request => &REQUESTS,
        };
        counter.fetch_sub(1, Ordering::Relaxed);
    }
}

fn acquire(counter: &AtomicUsize, max: usize, kind: CreditKind) -> Option<Credit> {
    counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            (used < max).then_some(used + 1)
        })
        .ok()
        .map(|_| Credit(kind))
}

pub fn acquire_connection() -> Option<Credit> {
    acquire(&CONNECTIONS, limits().max_connections, CreditKind::Connection)
}

pub fn acquire_request() -> Option<Credit> {
    acquire(&REQUESTS, limits().max_requests, CreditKind::Request)
}

pub fn reserve_queued_bytes(bytes: usize) -> bool {
    QUEUED_BYTES
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
            used.checked_add(bytes)
                .filter(|&next| next <= limits().max_queued_bytes)
        })
        .is_ok()
}

pub struct ByteCredit(usize);

impl Drop for ByteCredit {
    fn drop(&mut self) {
        release_queued_bytes(self.0);
    }
}

pub fn acquire_queued_bytes(bytes: usize) -> Option<ByteCredit> {
    if reserve_queued_bytes(bytes) {
        Some(ByteCredit(bytes))
    } else {
        None
    }
}

pub fn release_queued_bytes(bytes: usize) {
    if bytes > 0 {
        QUEUED_BYTES.fetch_sub(bytes, Ordering::Relaxed);
    }
}

pub fn usage() -> (usize, usize, usize) {
    (
        CONNECTIONS.load(Ordering::Relaxed),
        REQUESTS.load(Ordering::Relaxed),
        QUEUED_BYTES.load(Ordering::Relaxed),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credits_release_and_failed_byte_reservations_do_not_change_usage() {
        const CHILD: &str = "VALS_TRANSPORT_CREDIT_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("credits_release_and_failed_byte_reservations_do_not_change_usage")
                .arg("--nocapture")
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success(), "isolated transport credit test failed: {status}");
            return;
        }

        configure(TransportLimits { max_connections: 2, max_requests: 2, max_queued_bytes: 17, ..Default::default() }).unwrap();
        assert_eq!(usage(), (0, 0, 0));
        let conn = acquire_connection().unwrap();
        let req = acquire_request().unwrap();
        let bytes = acquire_queued_bytes(17).unwrap();
        assert_eq!(usage(), (1, 1, 17));
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        assert!(acquire_queued_bytes(1).is_none());
                    }
                });
            }
        });
        assert_eq!(usage(), (1, 1, 17));
        drop(conn);
        drop(req);
        drop(bytes);
        assert_eq!(usage(), (0, 0, 0));
    }

    #[test]
    fn configure_rejects_partially_unbounded_public_limits() {
        let mut invalid = TransportLimits::default();
        invalid.idle_timeout = None;
        assert!(invalid.validate().is_err());
    }
}
