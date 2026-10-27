#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RejoinDecision {
    Recover,
    WipeAndResync,
}

pub fn rejoin_decision(local_wm: u64, peer_wms: &[u64], gc_grace_ms: u64) -> RejoinDecision {
    if gc_grace_ms == 0 {
        return RejoinDecision::Recover;
    }
    let threshold = local_wm.saturating_add(gc_grace_ms);
    if peer_wms.iter().any(|&p| p > threshold) {
        RejoinDecision::WipeAndResync
    } else {
        RejoinDecision::Recover
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WipeStats {
    pub wall_ms: u64,
    pub local_watermark_ms: u64,
    pub max_peer_watermark_ms: u64,
    pub gc_grace_ms: u64,
    pub zones: usize,
    pub keys: usize,
}

pub trait LivenessLog {
    fn load(&self) -> Option<u64>;
    fn store(&self, watermark_ms: u64) -> std::io::Result<()>;
    fn record_wipe(&self, _stats: &WipeStats) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct MemLivenessLog {
    wm: std::cell::Cell<Option<u64>>,
    wipes: std::cell::RefCell<Vec<WipeStats>>,
}

impl MemLivenessLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn wipes(&self) -> Vec<WipeStats> {
        self.wipes.borrow().clone()
    }
}

impl LivenessLog for MemLivenessLog {
    fn load(&self) -> Option<u64> {
        self.wm.get()
    }
    fn store(&self, watermark_ms: u64) -> std::io::Result<()> {
        self.wm.set(Some(watermark_ms));
        Ok(())
    }
    fn record_wipe(&self, stats: &WipeStats) -> std::io::Result<()> {
        self.wipes.borrow_mut().push(*stats);
        Ok(())
    }
}

pub struct FileLivenessLog {
    path: std::path::PathBuf,
}

impl FileLivenessLog {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        FileLivenessLog { path: path.into() }
    }
}

const WM_MAGIC: u32 = 0x5657_4d4b;
const WM_VERSION: u16 = 1;
const WM_LEN: usize = 18;

fn encode_watermark(ms: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(WM_LEN);
    b.extend_from_slice(&WM_MAGIC.to_le_bytes());
    b.extend_from_slice(&WM_VERSION.to_le_bytes());
    b.extend_from_slice(&ms.to_le_bytes());
    let crc = vals_store::crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

fn decode_watermark(b: &[u8]) -> Result<u64, &'static str> {
    if b.len() != WM_LEN {
        return Err("wrong length");
    }
    if u32::from_le_bytes(b[0..4].try_into().unwrap()) != WM_MAGIC {
        return Err("bad magic");
    }
    if u16::from_le_bytes(b[4..6].try_into().unwrap()) != WM_VERSION {
        return Err("unsupported version");
    }
    let stored = u32::from_le_bytes(b[14..18].try_into().unwrap());
    if vals_store::crc::crc32(&b[0..14]) != stored {
        return Err("crc mismatch");
    }
    Ok(u64::from_le_bytes(b[6..14].try_into().unwrap()))
}

impl LivenessLog for FileLivenessLog {
    fn load(&self) -> Option<u64> {
        let bytes = std::fs::read(&self.path).ok()?;
        match decode_watermark(&bytes) {
            Ok(ms) => Some(ms),
            Err(why) => panic!(
                "liveness: watermark file {} is unreadable ({why}); refusing to \
                 boot rather than silently reseed (which could trigger an \
                 unnecessary wipe). Wipe this node's data and re-sync, or restore a \
                 compatible build.",
                self.path.display()
            ),
        }
    }

    fn store(&self, watermark_ms: u64) -> std::io::Result<()> {
        use std::io::Write;
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&encode_watermark(watermark_ms))?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                if let Err(e) = d.sync_all() {
                    log::warn!(
                        "liveness: directory fsync after watermark rename failed \
                         (a power loss may revert to the prior watermark): {e}"
                    );
                }
            }
        }
        Ok(())
    }

    fn record_wipe(&self, stats: &WipeStats) -> std::io::Result<()> {
        use std::io::Write;
        let path = self.path.with_extension("wipe");
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        writeln!(
            f,
            "wipe at_ms={} local_wm_ms={} max_peer_wm_ms={} gc_grace_ms={} zones={} keys={}",
            stats.wall_ms,
            stats.local_watermark_ms,
            stats.max_peer_watermark_ms,
            stats.gc_grace_ms,
            stats.zones,
            stats.keys
        )?;
        f.sync_all()?;
        if let Some(dir) = self.path.parent() {
            if let Ok(d) = std::fs::File::open(dir) {
                let _ = d.sync_all();
            }
        }
        Ok(())
    }
}

pub fn is_fenced(last_contact_ms: u64, now_ms: u64, gc_grace_ms: u64) -> bool {
    gc_grace_ms != 0 && now_ms.saturating_sub(last_contact_ms) > gc_grace_ms
}

#[cfg(test)]
mod tests {
    use super::*;

    const GRACE: u64 = 10_000;

    #[test]
    fn gc_disabled_always_recovers() {
        assert_eq!(rejoin_decision(0, &[u64::MAX], 0), RejoinDecision::Recover);
        assert!(!is_fenced(0, u64::MAX, 0));
    }

    #[test]
    fn peer_far_ahead_wipes() {
        assert_eq!(
            rejoin_decision(1_000, &[1_000 + GRACE + 1], GRACE),
            RejoinDecision::WipeAndResync
        );
    }

    #[test]
    fn peer_within_grace_recovers() {
        assert_eq!(
            rejoin_decision(1_000, &[1_000 + GRACE], GRACE),
            RejoinDecision::Recover
        );
        assert_eq!(
            rejoin_decision(1_000, &[1_000 + GRACE - 1], GRACE),
            RejoinDecision::Recover
        );
    }

    #[test]
    fn whole_cluster_restart_recovers() {
        let frozen = 5_000;
        let peers = [frozen, frozen, frozen];
        assert_eq!(
            rejoin_decision(frozen, &peers, GRACE),
            RejoinDecision::Recover
        );
    }

    #[test]
    fn one_ahead_peer_among_many_wipes() {
        let peers = [1_000, 1_000, 1_000 + GRACE + 1];
        assert_eq!(
            rejoin_decision(1_000, &peers, GRACE),
            RejoinDecision::WipeAndResync
        );
    }

    #[test]
    fn no_peers_recovers() {
        assert_eq!(rejoin_decision(1_000, &[], GRACE), RejoinDecision::Recover);
    }

    #[test]
    fn we_are_ahead_recovers() {
        assert_eq!(
            rejoin_decision(50_000, &[1_000], GRACE),
            RejoinDecision::Recover
        );
    }

    #[test]
    fn fence_trips_past_grace() {
        assert!(!is_fenced(1_000, 1_000 + GRACE, GRACE));
        assert!(is_fenced(1_000, 1_000 + GRACE + 1, GRACE));
    }

    #[test]
    fn fence_clock_going_backwards_does_not_trip() {
        assert!(!is_fenced(10_000, 9_000, GRACE));
    }

    #[test]
    fn mem_liveness_log_roundtrips() {
        let log = MemLivenessLog::new();
        assert_eq!(log.load(), None);
        log.store(12_345).unwrap();
        assert_eq!(log.load(), Some(12_345));
        log.store(67_890).unwrap();
        assert_eq!(log.load(), Some(67_890));
    }

    #[test]
    fn file_liveness_log_records_wipes_append_only() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vals-wipelog-{}.wm", std::process::id()));
        let marker = path.with_extension("wipe");
        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&marker).ok();

        let log = FileLivenessLog::new(&path);
        let stats = WipeStats {
            wall_ms: 1_000,
            local_watermark_ms: 10,
            max_peer_watermark_ms: 999,
            gc_grace_ms: 100,
            zones: 2,
            keys: 42,
        };
        log.record_wipe(&stats).unwrap();
        log.record_wipe(&WipeStats { keys: 7, ..stats }).unwrap();

        let contents = std::fs::read_to_string(&marker).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "append-only: both wipes recorded");
        assert!(lines[0].contains("keys=42") && lines[0].contains("max_peer_wm_ms=999"));
        assert!(lines[1].contains("keys=7"));

        std::fs::remove_file(&path).ok();
        std::fs::remove_file(&marker).ok();
    }

    #[test]
    fn mem_liveness_log_records_wipes() {
        let log = MemLivenessLog::new();
        assert!(log.wipes().is_empty());
        let stats = WipeStats {
            wall_ms: 5,
            local_watermark_ms: 1,
            max_peer_watermark_ms: 4,
            gc_grace_ms: 2,
            zones: 1,
            keys: 3,
        };
        log.record_wipe(&stats).unwrap();
        assert_eq!(log.wipes(), vec![stats]);
    }

    #[test]
    fn file_liveness_log_persists_durably() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("vals-liveness-{}.wm", std::process::id()));
        std::fs::remove_file(&path).ok();

        let log = FileLivenessLog::new(&path);
        assert_eq!(log.load(), None, "no file yet => None");
        log.store(999_000).unwrap();
        assert_eq!(log.load(), Some(999_000));

        let reopened = FileLivenessLog::new(&path);
        assert_eq!(reopened.load(), Some(999_000));

        log.store(1_000_500).unwrap();
        assert_eq!(FileLivenessLog::new(&path).load(), Some(1_000_500));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn watermark_codec_roundtrips_and_rejects_bad_envelopes() {
        assert_eq!(decode_watermark(&encode_watermark(42)), Ok(42));
        assert_eq!(decode_watermark(&[0u8; 8]), Err("wrong length"));
        let mut bad = encode_watermark(42);
        bad[0] ^= 0xFF;
        assert_eq!(decode_watermark(&bad), Err("bad magic"));
        let mut bad = encode_watermark(42);
        bad[6] ^= 0x01;
        assert_eq!(decode_watermark(&bad), Err("crc mismatch"));
        let mut future = encode_watermark(42);
        future[4..6].copy_from_slice(&(WM_VERSION + 1).to_le_bytes());
        let crc = vals_store::crc::crc32(&future[0..14]);
        future[14..18].copy_from_slice(&crc.to_le_bytes());
        assert_eq!(decode_watermark(&future), Err("unsupported version"));
    }

    #[test]
    #[should_panic(expected = "refusing to")]
    fn file_liveness_load_fails_loud_on_an_unreadable_watermark() {
        let path = std::env::temp_dir().join(format!(
            "vals-liveness-bad-{}-{:x}.wm",
            std::process::id(),
            WM_MAGIC
        ));
        std::fs::write(&path, b"not a watermark envelope").unwrap();
        let log = FileLivenessLog::new(&path);
        let _ = log.load();
    }
}
