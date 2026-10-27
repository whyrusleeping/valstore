use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::hlc::Version;
use crate::ring::NodeId;

pub use vals_client::hints::{Hint, HintStore, MemHintStore};

pub struct FileHintStore {
    mem: MemHintStore,
    path: PathBuf,
    file: Option<File>,
    ops: u64,
}

const COMPACT_MIN_OPS: u64 = 64;
const COMPACT_GARBAGE_FACTOR: u64 = 4;

impl FileHintStore {
    pub fn open(path: impl AsRef<Path>, cap_per_node: usize) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let cap = cap_per_node.max(1);
        let mut mem = MemHintStore::new(cap);
        if let Ok(bytes) = std::fs::read(&path) {
            if bytes.len() >= 6
                && u32::from_le_bytes(bytes[0..4].try_into().unwrap()) == HINT_MAGIC
            {
                let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
                if !matches!(version, HINT_VERSION_V1 | HINT_VERSION_V2 | HINT_VERSION) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported hint log version {version}"),
                    ));
                }
            }
            replay_into(&mut mem, &bytes, cap);
        }
        let mut store = FileHintStore {
            mem,
            path,
            file: None,
            ops: 0,
        };
        store.compact();
        Ok(store)
    }

    fn live_hints(&self) -> u64 {
        self.mem.queues().values().map(VecDeque::len).sum::<usize>() as u64
    }

    fn log_delta(&mut self, op: u8, payload: &[u8]) {
        if self.file.is_some() {
            match self.try_append(op, payload) {
                Ok(()) => {
                    self.ops += 1;
                    self.maybe_compact();
                    return;
                }
                Err(e) => {
                    log::warn!(
                        "hints: appending to the hint log {} failed (rewriting a \
                         fresh snapshot): {e}",
                        self.path.display()
                    );
                    crate::observe::counters().hint_persist_errors.inc();
                    self.file = None;
                }
            }
        }
        self.compact();
    }

    fn try_append(&mut self, op: u8, payload: &[u8]) -> io::Result<()> {
        let rec = serialize_record(op, payload);
        let f = self.file.as_mut().expect("append handle present");
        f.write_all(&rec)?;
        f.sync_all()?;
        Ok(())
    }

    fn compact(&mut self) {
        let bytes = encode_hint_log(self.mem.queues());
        let tmp = self.path.with_extension("tmp");
        if let Err(e) = persist_durable(&tmp, &self.path, &bytes) {
            log::warn!(
                "hints: compacting the hint log to {} failed (prior file kept; a \
                 crash loses the delta): {e}",
                self.path.display()
            );
            crate::observe::counters().hint_persist_errors.inc();
            self.file = None;
            return;
        }
        match std::fs::OpenOptions::new().append(true).open(&self.path) {
            Ok(f) => {
                self.file = Some(f);
                self.ops = 1;
            }
            Err(e) => {
                log::warn!(
                    "hints: reopening the compacted hint log {} for append failed: {e}",
                    self.path.display()
                );
                crate::observe::counters().hint_persist_errors.inc();
                self.file = None;
            }
        }
    }

    fn maybe_compact(&mut self) {
        if self.ops >= COMPACT_MIN_OPS && self.ops > COMPACT_GARBAGE_FACTOR * self.live_hints() {
            self.compact();
        }
    }
}

fn persist_durable(tmp: &Path, dst: &Path, bytes: &[u8]) -> io::Result<()> {
    {
        let mut f = std::fs::File::create(tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(tmp, dst)?;
    if let Some(dir) = dst.parent() {
        if let Ok(d) = std::fs::File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

impl HintStore for FileHintStore {
    fn record(&mut self, node: NodeId, hint: Hint) {
        let mut payload = Vec::new();
        put_u32(&mut payload, node);
        put_hint(&mut payload, &hint);
        self.mem.record(node, hint);
        self.log_delta(OP_RECORD, &payload);
    }

    fn drain(&mut self, node: NodeId) -> Vec<Hint> {
        let hints = self.mem.drain(node);
        if !hints.is_empty() {
            let mut payload = Vec::new();
            put_u32(&mut payload, node);
            self.log_delta(OP_DRAIN, &payload);
        }
        hints
    }

    fn restore(&mut self, node: NodeId, hints: Vec<Hint>) {
        if hints.is_empty() {
            return;
        }
        let mut payload = Vec::new();
        put_u32(&mut payload, node);
        put_u32(&mut payload, hints.len() as u32);
        for h in &hints {
            put_hint(&mut payload, h);
        }
        self.mem.restore(node, hints);
        self.log_delta(OP_RESTORE, &payload);
    }

    fn pending(&self, node: NodeId) -> usize {
        self.mem.pending(node)
    }
}

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}
fn put_bytes(b: &mut Vec<u8>, s: &[u8]) {
    put_u32(b, s.len() as u32);
    b.extend_from_slice(s);
}
fn put_hint(b: &mut Vec<u8>, h: &Hint) {
    b.extend_from_slice(&h.version.hlc.to_le_bytes());
    b.extend_from_slice(&h.version.node.to_le_bytes());
    put_bytes(b, &h.key);
    match &h.value {
        None => b.push(0),
        Some(v) => {
            b.push(1);
            put_bytes(b, v);
        }
    }
}

const HINT_MAGIC: u32 = 0x5648_4e54;
const HINT_VERSION_V1: u16 = 1;
const HINT_VERSION_V2: u16 = 2;
const HINT_VERSION: u16 = 3;

const OP_SNAPSHOT: u8 = 0;
const OP_RECORD: u8 = 1;
const OP_DRAIN: u8 = 2;
const OP_RESTORE: u8 = 3;

fn serialize_record(op: u8, payload: &[u8]) -> Vec<u8> {
    let mut rec = Vec::with_capacity(1 + 4 + payload.len() + 4);
    rec.push(op);
    put_u32(&mut rec, payload.len() as u32);
    rec.extend_from_slice(payload);
    let crc = vals_store::crc::crc32(&rec);
    put_u32(&mut rec, crc);
    rec
}

fn encode_snapshot_payload(queues: &HashMap<NodeId, VecDeque<Hint>>) -> Vec<u8> {
    let mut b = Vec::new();
    let nonempty: Vec<_> = queues.iter().filter(|(_, q)| !q.is_empty()).collect();
    put_u32(&mut b, nonempty.len() as u32);
    for (&node, q) in nonempty {
        put_u32(&mut b, node);
        put_u32(&mut b, q.len() as u32);
        for h in q {
            put_hint(&mut b, h);
        }
    }
    b
}

fn encode_hint_log(queues: &HashMap<NodeId, VecDeque<Hint>>) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&HINT_MAGIC.to_le_bytes());
    b.extend_from_slice(&HINT_VERSION.to_le_bytes());
    b.extend_from_slice(&serialize_record(
        OP_SNAPSHOT,
        &encode_snapshot_payload(queues),
    ));
    b
}

fn take<'a>(b: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    if b.len() < n {
        return None;
    }
    let (head, tail) = b.split_at(n);
    *b = tail;
    Some(head)
}
fn u32_(b: &mut &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(take(b, 4)?.try_into().unwrap()))
}
fn u64_(b: &mut &[u8]) -> Option<u64> {
    Some(u64::from_le_bytes(take(b, 8)?.try_into().unwrap()))
}
fn bytes_(b: &mut &[u8]) -> Option<Vec<u8>> {
    let n = u32_(b)? as usize;
    Some(take(b, n)?.to_vec())
}
fn hint_(b: &mut &[u8], legacy: bool) -> Option<Hint> {
    let hlc = u64_(b)?;
    let writer = if legacy { u32_(b)? as u64 } else { u64_(b)? };
    let key = bytes_(b)?;
    let value = match take(b, 1)?[0] {
        0 => None,
        _ => Some(bytes_(b)?),
    };
    Some(Hint {
        key,
        value,
        version: Version { hlc, node: writer },
    })
}

fn replay_into(mem: &mut MemHintStore, buf: &[u8], cap: usize) {
    if buf.len() < 6 || u32::from_le_bytes(buf[0..4].try_into().unwrap()) != HINT_MAGIC {
        return;
    }
    match u16::from_le_bytes(buf[4..6].try_into().unwrap()) {
        HINT_VERSION_V1 => {
            for (node, hints) in decode_v1(buf) {
                mem.restore(node, hints);
            }
        }
        HINT_VERSION_V2 => replay_log(mem, &buf[6..], cap, true),
        HINT_VERSION => replay_log(mem, &buf[6..], cap, false),
        _ => {}
    }
}

fn replay_log(mem: &mut MemHintStore, mut b: &[u8], cap: usize, legacy: bool) {
    while let Some((op, payload)) = parse_record(&mut b) {
        apply_op(mem, op, payload, cap, legacy);
    }
}

fn parse_record<'a>(b: &mut &'a [u8]) -> Option<(u8, &'a [u8])> {
    if b.len() < 5 {
        return None;
    }
    let op = b[0];
    let len = u32::from_le_bytes(b[1..5].try_into().unwrap()) as usize;
    let total = 5 + len + 4;
    if b.len() < total {
        return None;
    }
    let stored = u32::from_le_bytes(b[5 + len..total].try_into().unwrap());
    if vals_store::crc::crc32(&b[..5 + len]) != stored {
        return None;
    }
    let payload = &b[5..5 + len];
    *b = &b[total..];
    Some((op, payload))
}

fn apply_op(mem: &mut MemHintStore, op: u8, payload: &[u8], cap: usize, legacy: bool) {
    let mut c: &[u8] = payload;
    match op {
        OP_SNAPSHOT => {
            *mem = MemHintStore::new(cap);
            if let Some(queues) = parse_snapshot(&mut c, legacy) {
                for (node, hints) in queues {
                    mem.restore(node, hints);
                }
            }
        }
        OP_RECORD => {
            if let (Some(node), Some(hint)) = (u32_(&mut c), hint_(&mut c, legacy)) {
                mem.record(node, hint);
            }
        }
        OP_DRAIN => {
            if let Some(node) = u32_(&mut c) {
                mem.drain(node);
            }
        }
        OP_RESTORE => {
            if let Some((node, hints)) = parse_queue(&mut c, legacy) {
                mem.restore(node, hints);
            }
        }
        _ => {}
    }
}

fn parse_snapshot(b: &mut &[u8], legacy: bool) -> Option<Vec<(NodeId, Vec<Hint>)>> {
    let nodes = u32_(b)?;
    let mut out = Vec::with_capacity(nodes as usize);
    for _ in 0..nodes {
        out.push(parse_queue(b, legacy)?);
    }
    Some(out)
}

fn parse_queue(b: &mut &[u8], legacy: bool) -> Option<(NodeId, Vec<Hint>)> {
    let node = u32_(b)?;
    let count = u32_(b)?;
    let mut hints = Vec::with_capacity(count as usize);
    for _ in 0..count {
        hints.push(hint_(b, legacy)?);
    }
    Some((node, hints))
}

fn decode_v1(buf: &[u8]) -> Vec<(NodeId, Vec<Hint>)> {
    fn parse(buf: &[u8]) -> Option<Vec<(NodeId, Vec<Hint>)>> {
        if buf.len() < 4 + 2 + 4 {
            return None;
        }
        if u32::from_le_bytes(buf[0..4].try_into().unwrap()) != HINT_MAGIC {
            return None;
        }
        if u16::from_le_bytes(buf[4..6].try_into().unwrap()) != HINT_VERSION_V1 {
            return None;
        }
        let crc_off = buf.len() - 4;
        let stored = u32::from_le_bytes(buf[crc_off..].try_into().unwrap());
        if vals_store::crc::crc32(&buf[..crc_off]) != stored {
            return None;
        }
        let mut b: &[u8] = &buf[6..crc_off];
        parse_snapshot(&mut b, true)
    }
    parse(buf).unwrap_or_default()
}

#[cfg(test)]
fn encode_legacy_snapshot_payload(queues: &HashMap<NodeId, VecDeque<Hint>>) -> Vec<u8> {
    let mut payload = Vec::new();
    let nonempty: Vec<_> = queues.iter().filter(|(_, q)| !q.is_empty()).collect();
    put_u32(&mut payload, nonempty.len() as u32);
    for (&node, q) in nonempty {
        put_u32(&mut payload, node);
        put_u32(&mut payload, q.len() as u32);
        for h in q {
            payload.extend_from_slice(&h.version.hlc.to_le_bytes());
            payload.extend_from_slice(&(h.version.node as u32).to_le_bytes());
            put_bytes(&mut payload, &h.key);
            match &h.value {
                None => payload.push(0),
                Some(value) => {
                    payload.push(1);
                    put_bytes(&mut payload, value);
                }
            }
        }
    }
    payload
}

#[cfg(test)]
fn encode_v1(queues: &HashMap<NodeId, VecDeque<Hint>>) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&HINT_MAGIC.to_le_bytes());
    b.extend_from_slice(&HINT_VERSION_V1.to_le_bytes());
    b.extend_from_slice(&encode_legacy_snapshot_payload(queues));
    let crc = vals_store::crc::crc32(&b);
    b.extend_from_slice(&crc.to_le_bytes());
    b
}

#[cfg(test)]
fn encode_v2(queues: &HashMap<NodeId, VecDeque<Hint>>) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(&HINT_MAGIC.to_le_bytes());
    b.extend_from_slice(&HINT_VERSION_V2.to_le_bytes());
    b.extend_from_slice(&serialize_record(
        OP_SNAPSHOT,
        &encode_legacy_snapshot_payload(queues),
    ));
    b
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn v(hlc: u64) -> Version {
        Version { hlc, node: 1 }
    }
    fn hint(k: &str, hlc: u64) -> Hint {
        Hint {
            key: k.as_bytes().to_vec(),
            value: Some(b"v".to_vec()),
            version: v(hlc),
        }
    }

    fn temp_path() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("vals-hints-{}-{}", std::process::id(), n))
    }

    #[test]
    fn file_store_survives_reopen() {
        let path = temp_path();
        {
            let mut s = FileHintStore::open(&path, 100).unwrap();
            s.record(3, hint("a", 10));
            s.record(3, hint("b", 20));
            s.record(9, hint("c", 30));
        }

        let mut s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(3), 2);
        assert_eq!(s.pending(9), 1);
        let n3 = s.drain(3);
        assert_eq!(n3.len(), 2);
        assert_eq!(n3[0].key, b"a");
        assert_eq!(n3[1].version, v(20));

        s.record(
            5,
            Hint {
                key: b"d".to_vec(),
                value: None,
                version: v(40),
            },
        );
        drop(s);
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(5), 1);
        assert_eq!(s.pending(3), 0);
        assert_eq!(s.pending(9), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn wide_writer_identity_survives_hint_reopen() {
        let path = temp_path();
        let version = Version {
            hlc: 50,
            node: 0xfedc_ba98_7654_3210,
        };
        {
            let mut s = FileHintStore::open(&path, 100).unwrap();
            s.record(
                3,
                Hint {
                    key: b"wide".to_vec(),
                    value: Some(b"v".to_vec()),
                    version,
                },
            );
        }
        let mut s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.drain(3)[0].version, version);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn torn_tail_keeps_valid_prefix_and_stays_usable() {
        let path = temp_path();
        {
            let mut s = FileHintStore::open(&path, 100).unwrap();
            s.record(1, hint("a", 1));
            s.record(1, hint("b", 2));
            s.record(2, hint("c", 3));
        }
        {
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[0xAB, 0xCD, 0xEF]).unwrap();
            f.sync_all().unwrap();
        }
        let mut s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(1), 2);
        assert_eq!(s.pending(2), 1);
        s.record(2, hint("d", 4));
        drop(s);
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(1), 2);
        assert_eq!(s.pending(2), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn truncated_mid_record_recovers_prefix() {
        let path = temp_path();
        {
            let mut s = FileHintStore::open(&path, 100).unwrap();
            s.record(7, hint("a", 1));
            s.record(7, hint("b", 2));
        }
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 1).unwrap();
        f.sync_all().unwrap();
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(7), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn corrupt_record_crc_stops_replay_keeping_prefix() {
        let path = temp_path();
        {
            let mut s = FileHintStore::open(&path, 100).unwrap();
            s.record(1, hint("a", 1));
            s.record(1, hint("b", 2));
            s.record(1, hint("c", 3));
        }
        let mut bytes = std::fs::read(&path).unwrap();
        let n = bytes.len();
        bytes[n - 6] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(1), 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn compaction_shrinks_the_log() {
        let path = temp_path();
        let mut s = FileHintStore::open(&path, 100).unwrap();
        for i in 0..300 {
            s.record(1, hint("k", i));
            let drained = s.drain(1);
            assert_eq!(drained.len(), 1);
        }
        let size = std::fs::metadata(&path).unwrap().len();
        assert!(
            size < 4096,
            "log should have compacted, but is {size} bytes"
        );
        assert_eq!(s.pending(1), 0);
        drop(s);
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(1), 0);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn record_is_append_bounded() {
        let path = temp_path();
        let mut s = FileHintStore::open(&path, 10_000).unwrap();
        for i in 0..500 {
            s.record(1, hint(&format!("key-{i}"), i));
        }
        let before = std::fs::metadata(&path).unwrap().len();
        s.record(1, hint("one-more", 500));
        let after = std::fs::metadata(&path).unwrap().len();
        let delta = after - before;
        assert!(
            delta > 0 && delta < 256,
            "append should be O(1)-sized, grew by {delta} bytes"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn v1_legacy_file_still_loads() {
        let path = temp_path();
        let mut q: HashMap<NodeId, VecDeque<Hint>> = HashMap::new();
        q.insert(3, VecDeque::from(vec![hint("a", 10), hint("b", 20)]));
        q.insert(
            5,
            VecDeque::from(vec![Hint {
                key: b"t".to_vec(),
                value: None,
                version: v(30),
            }]),
        );
        std::fs::write(&path, encode_v1(&q)).unwrap();
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(3), 2);
        assert_eq!(s.pending(5), 1);
        drop(s);
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(3), 2);
        assert_eq!(s.pending(5), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn v2_hint_log_upgrades_without_reinterpreting_legacy_writer() {
        let path = temp_path();
        let mut q: HashMap<NodeId, VecDeque<Hint>> = HashMap::new();
        q.insert(3, VecDeque::from(vec![hint("a", 10)]));
        std::fs::write(&path, encode_v2(&q)).unwrap();

        let mut s = FileHintStore::open(&path, 100).unwrap();
        let restored = s.drain(3);
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].version, v(10));
        let bytes = std::fs::read(&path).unwrap();
        assert_eq!(u16::from_le_bytes(bytes[4..6].try_into().unwrap()), HINT_VERSION);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn v1_envelope_roundtrips_and_decodes_corruption_to_empty() {
        let mut q: HashMap<NodeId, VecDeque<Hint>> = HashMap::new();
        q.insert(3, VecDeque::from(vec![hint("a", 10), hint("b", 20)]));
        let enc = encode_v1(&q);
        assert_eq!(decode_v1(&enc).len(), 1, "a valid envelope round-trips");

        let mut bad = enc.clone();
        bad[0] ^= 0xFF;
        assert!(decode_v1(&bad).is_empty());

        let mut crc_bad = enc.clone();
        crc_bad[6] ^= 0x01;
        assert!(decode_v1(&crc_bad).is_empty());

        let mut future = enc.clone();
        future[4..6].copy_from_slice(&(HINT_VERSION + 1).to_le_bytes());
        let crc_off = future.len() - 4;
        let crc = vals_store::crc::crc32(&future[..crc_off]);
        future[crc_off..].copy_from_slice(&crc.to_le_bytes());
        assert!(decode_v1(&future).is_empty());
    }

    #[test]
    fn unknown_hint_version_is_rejected_without_mutation() {
        let path = temp_path();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&HINT_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&(HINT_VERSION + 1).to_le_bytes());
        bytes.extend_from_slice(b"future");
        std::fs::write(&path, &bytes).unwrap();

        let error = FileHintStore::open(&path, 100).err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn persist_leaves_no_temp_file_and_replaces_atomically() {
        let path = temp_path();
        let tmp = path.with_extension("tmp");
        let mut s = FileHintStore::open(&path, 100).unwrap();
        s.record(1, hint("x", 1));
        assert!(path.exists(), "destination written");
        assert!(!tmp.exists(), "temp file renamed away, not left dangling");
        for i in 0..300 {
            s.record(2, hint("y", i));
            s.drain(2);
        }
        assert!(!tmp.exists());
        drop(s);
        let s = FileHintStore::open(&path, 100).unwrap();
        assert_eq!(s.pending(1), 1);
        let _ = std::fs::remove_file(&path);
    }
}
