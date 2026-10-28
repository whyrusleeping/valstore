use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::Path;

use vals_store::crc::Crc32;

use crate::hlc::Version;
use crate::pd::{view_state, PdStateView};
use crate::wire::RtFragment;
use crate::zone::{Ordering, ZoneConfig, ZoneId, DEFAULT_ZONE};

mod create;
mod export;
mod restore;
#[cfg(test)]
mod tests;

pub use create::{
    capture_from_inspection, create_backup, placement_change, BackupCluster, NetCluster,
};
pub use export::{
    merge_replicas, merge_replicas_into, ring_params, zone_sources, MergeStats, NetReplicaSource,
    ReplicaPage, ReplicaSource, StaleCopies, ZonePlacement,
};
pub use restore::{
    check_restored, check_seed, compare_backup, lineage_of, load_backup, restore_pd, CompareReport, LoadReport,
    RestoreTarget, RestoredPd, SeedStatus, ZoneComparison,
};

pub const FORMAT_VERSION: u32 = 1;
pub const MANIFEST_FILE: &str = "MANIFEST";
pub const PD_STATE_FILE: &str = "pd-state";

const MANIFEST_HEADER: &str = "vals-backup";
const PD_STATE_MAGIC: u32 = 0x5650_4453;
const PD_STATE_VERSION: u16 = 1;
const RECORDS_MAGIC: u32 = 0x5652_4543;
const RECORDS_VERSION: u16 = 2;
const RECORDS_VERSION_POINTS_ONLY: u16 = 1;
const TAG_END: u8 = 0;
const TAG_PUT: u8 = 1;
const TAG_TOMBSTONE: u8 = 2;
const TAG_RANGE_TOMBSTONE: u8 = 3;

pub fn zone_file_name(zone_id: ZoneId) -> String {
    format!("zone-{zone_id}.vrec")
}

pub fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

pub fn random_id() -> io::Result<u128> {
    let mut f = File::open("/dev/urandom")?;
    loop {
        let mut bytes = [0u8; 16];
        f.read_exact(&mut bytes)?;
        let id = u128::from_le_bytes(bytes);
        if id != 0 {
            return Ok(id);
        }
    }
}

pub fn write_new_file_synced(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let dir = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let name = path.file_name().ok_or_else(|| io::Error::other("path has no file name"))?;
    if path.exists() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("{} exists", path.display())));
    }
    let tmp = dir.join(format!(".{}.tmp", name.to_string_lossy()));
    {
        let mut f = std::fs::OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    if let Err(e) = std::fs::hard_link(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::remove_file(&tmp)?;
    File::open(dir)?.sync_all()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PdStateCapture {
    pub applied: u64,
    pub term: u64,
    pub captured_unix_ms: u64,
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl PdStateCapture {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&PD_STATE_MAGIC.to_le_bytes());
        b.extend_from_slice(&PD_STATE_VERSION.to_le_bytes());
        b.extend_from_slice(&self.applied.to_le_bytes());
        b.extend_from_slice(&self.term.to_le_bytes());
        b.extend_from_slice(&self.captured_unix_ms.to_le_bytes());
        b.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for (key, value) in &self.entries {
            put_bytes(&mut b, key);
            put_bytes(&mut b, value);
        }
        let crc = vals_store::crc::crc32(&b);
        b.extend_from_slice(&crc.to_le_bytes());
        b
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("pd-state: shorter than its checksum".into());
        }
        let (body, crc) = bytes.split_at(bytes.len() - 4);
        if vals_store::crc::crc32(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
            return Err("pd-state: checksum mismatch".into());
        }
        let mut r = body;
        if take_u32(&mut r)? != PD_STATE_MAGIC {
            return Err("pd-state: bad magic".into());
        }
        let version = take_u16(&mut r)?;
        if version != PD_STATE_VERSION {
            return Err(format!("pd-state: unsupported version {version}"));
        }
        let applied = take_u64(&mut r)?;
        let term = take_u64(&mut r)?;
        let captured_unix_ms = take_u64(&mut r)?;
        let count = take_u32(&mut r)? as usize;
        let mut entries = Vec::with_capacity(count.min(r.len() / 8));
        for _ in 0..count {
            let key = take_bytes(&mut r)?;
            let value = take_bytes(&mut r)?;
            if entries.last().is_some_and(|(prev, _): &(Vec<u8>, Vec<u8>)| *prev >= key) {
                return Err("pd-state: keys are not strictly increasing".into());
            }
            entries.push((key, value));
        }
        if !r.is_empty() {
            return Err("pd-state: trailing bytes".into());
        }
        Ok(PdStateCapture { applied, term, captured_unix_ms, entries })
    }

    pub fn view(&self) -> PdStateView {
        view_state(&self.entries)
    }
}

fn put_bytes(b: &mut Vec<u8>, field: &[u8]) {
    b.extend_from_slice(&(field.len() as u32).to_le_bytes());
    b.extend_from_slice(field);
}

fn take<'a>(r: &mut &'a [u8], n: usize) -> Result<&'a [u8], String> {
    if r.len() < n {
        return Err("pd-state: truncated".into());
    }
    let (head, tail) = r.split_at(n);
    *r = tail;
    Ok(head)
}

fn take_u16(r: &mut &[u8]) -> Result<u16, String> {
    Ok(u16::from_le_bytes(take(r, 2)?.try_into().unwrap()))
}

fn take_u32(r: &mut &[u8]) -> Result<u32, String> {
    Ok(u32::from_le_bytes(take(r, 4)?.try_into().unwrap()))
}

fn take_u64(r: &mut &[u8]) -> Result<u64, String> {
    Ok(u64::from_le_bytes(take(r, 8)?.try_into().unwrap()))
}

fn take_bytes(r: &mut &[u8]) -> Result<Vec<u8>, String> {
    let n = take_u32(r)? as usize;
    Ok(take(r, n)?.to_vec())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub key: Vec<u8>,
    pub version: Version,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecordCounts {
    pub records: u64,
    pub puts: u64,
    pub tombstones: u64,
}

impl RecordCounts {
    fn note(&mut self, record: &Record) {
        self.records += 1;
        if record.value.is_some() {
            self.puts += 1;
        } else {
            self.tombstones += 1;
        }
    }
}

struct Checksummed<T> {
    inner: T,
    crc: Crc32,
    bytes: u64,
}

impl<W: Write> Write for Checksummed<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<R: Read> Read for Checksummed<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.crc.update(&buf[..n]);
        self.bytes += n as u64;
        Ok(n)
    }
}

pub struct RecordWriter<W: Write> {
    out: Checksummed<W>,
    last_key: Option<Vec<u8>>,
    counts: RecordCounts,
    ranges: RangeOrder,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordFileSummary {
    pub counts: RecordCounts,
    pub range_tombstones: u64,
    pub bytes: u64,
    pub crc32c: u32,
}

#[derive(Default)]
struct RangeOrder {
    count: u64,
    last_hi: Option<Option<Vec<u8>>>,
}

impl RangeOrder {
    fn note(&mut self, rt: &RtFragment) -> Result<(), &'static str> {
        let lo = rt.lo.as_deref().unwrap_or_default();
        if rt.hi.as_deref().is_some_and(|hi| lo >= hi) {
            return Err("range tombstone with an empty span");
        }
        match &self.last_hi {
            None => {}
            Some(Some(prev)) if lo >= prev.as_slice() => {}
            Some(_) => return Err("range tombstones are not in increasing, non-overlapping order"),
        }
        self.last_hi = Some(rt.hi.clone());
        self.count += 1;
        Ok(())
    }
}

fn put_opt_bytes(out: &mut impl Write, field: Option<&[u8]>) -> io::Result<()> {
    match field {
        None => out.write_all(&[0]),
        Some(bytes) => {
            let len = u32::try_from(bytes.len()).map_err(|_| io::Error::other("range bound too long"))?;
            out.write_all(&[1])?;
            out.write_all(&len.to_le_bytes())?;
            out.write_all(bytes)
        }
    }
}

impl<W: Write> RecordWriter<W> {
    pub fn new(inner: W, zone_id: ZoneId) -> io::Result<Self> {
        let mut out = Checksummed { inner, crc: Crc32::new(), bytes: 0 };
        out.write_all(&RECORDS_MAGIC.to_le_bytes())?;
        out.write_all(&RECORDS_VERSION.to_le_bytes())?;
        out.write_all(&zone_id.to_le_bytes())?;
        Ok(RecordWriter { out, last_key: None, counts: RecordCounts::default(), ranges: RangeOrder::default() })
    }

    pub fn push(&mut self, record: &Record) -> io::Result<()> {
        if self.ranges.count > 0 {
            return Err(io::Error::other("backup point records must precede the range tombstones"));
        }
        if self.last_key.as_ref().is_some_and(|last| *last >= record.key) {
            return Err(io::Error::other("backup records must be written in strictly increasing key order"));
        }
        let key_len = u32::try_from(record.key.len()).map_err(|_| io::Error::other("key too long"))?;
        self.out.write_all(&[if record.value.is_some() { TAG_PUT } else { TAG_TOMBSTONE }])?;
        self.out.write_all(&key_len.to_le_bytes())?;
        self.out.write_all(&record.key)?;
        self.out.write_all(&record.version.hlc.to_le_bytes())?;
        self.out.write_all(&record.version.node.to_le_bytes())?;
        if let Some(value) = &record.value {
            let len = u32::try_from(value.len()).map_err(|_| io::Error::other("value too long"))?;
            self.out.write_all(&len.to_le_bytes())?;
            self.out.write_all(value)?;
        }
        self.counts.note(record);
        self.last_key = Some(record.key.clone());
        Ok(())
    }

    pub fn push_range_tombstone(&mut self, rt: &RtFragment) -> io::Result<()> {
        let lo = rt.lo.as_deref().filter(|lo| !lo.is_empty());
        let rt = RtFragment { lo: lo.map(<[u8]>::to_vec), hi: rt.hi.clone(), version: rt.version };
        self.ranges.note(&rt).map_err(io::Error::other)?;
        self.out.write_all(&[TAG_RANGE_TOMBSTONE])?;
        put_opt_bytes(&mut self.out, rt.lo.as_deref())?;
        put_opt_bytes(&mut self.out, rt.hi.as_deref())?;
        self.out.write_all(&rt.version.hlc.to_le_bytes())?;
        self.out.write_all(&rt.version.node.to_le_bytes())
    }

    pub fn finish(mut self) -> io::Result<(W, RecordFileSummary)> {
        self.out.write_all(&[TAG_END])?;
        self.out.write_all(&(self.counts.records + self.ranges.count).to_le_bytes())?;
        let trailer = self.out.crc.clone().finalize();
        self.out.write_all(&trailer.to_le_bytes())?;
        self.out.flush()?;
        let summary = RecordFileSummary {
            counts: self.counts,
            range_tombstones: self.ranges.count,
            bytes: self.out.bytes,
            crc32c: self.out.crc.clone().finalize(),
        };
        Ok((self.out.inner, summary))
    }
}

pub struct RecordReader<R: Read> {
    input: Checksummed<R>,
    version: u16,
    zone_id: ZoneId,
    last_key: Option<Vec<u8>>,
    counts: RecordCounts,
    ranges: RangeOrder,
    range_tombstones: Vec<RtFragment>,
    finished: bool,
}

impl<R: Read> RecordReader<R> {
    pub fn new(inner: R) -> Result<Self, String> {
        let mut input = Checksummed { inner, crc: Crc32::new(), bytes: 0 };
        let mut header = [0u8; 10];
        input.read_exact(&mut header).map_err(|e| format!("record header: {e}"))?;
        if u32::from_le_bytes(header[0..4].try_into().unwrap()) != RECORDS_MAGIC {
            return Err("record file: bad magic".into());
        }
        let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
        if version != RECORDS_VERSION && version != RECORDS_VERSION_POINTS_ONLY {
            return Err(format!("record file: unsupported version {version}"));
        }
        let zone_id = u32::from_le_bytes(header[6..10].try_into().unwrap());
        Ok(RecordReader {
            input,
            version,
            zone_id,
            last_key: None,
            counts: RecordCounts::default(),
            ranges: RangeOrder::default(),
            range_tombstones: Vec::new(),
            finished: false,
        })
    }

    pub fn range_tombstones(&self) -> &[RtFragment] {
        &self.range_tombstones
    }

    pub fn zone_id(&self) -> ZoneId {
        self.zone_id
    }

    pub fn counts(&self) -> RecordCounts {
        self.counts
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let mut buf = [0u8; N];
        self.input.read_exact(&mut buf).map_err(|e| format!("record file truncated: {e}"))?;
        Ok(buf)
    }

    fn read_opt_field(&mut self) -> Result<Option<Vec<u8>>, String> {
        match self.read_array::<1>()? {
            [0] => Ok(None),
            [1] => self.read_field().map(Some),
            [other] => Err(format!("record file: bad optional-field tag {other}")),
        }
    }

    fn read_field(&mut self) -> Result<Vec<u8>, String> {
        let len = u32::from_le_bytes(self.read_array::<4>()?) as u64;
        let mut buf = Vec::new();
        (&mut self.input).take(len).read_to_end(&mut buf).map_err(|e| format!("record file: {e}"))?;
        if buf.len() as u64 != len {
            return Err("record file truncated inside a field".into());
        }
        Ok(buf)
    }

    pub fn next_record(&mut self) -> Result<Option<Record>, String> {
        loop {
            if self.finished {
                return Ok(None);
            }
            let [tag] = self.read_array::<1>()?;
            if tag == TAG_RANGE_TOMBSTONE && self.version != RECORDS_VERSION_POINTS_ONLY {
                let lo = self.read_opt_field()?;
                let hi = self.read_opt_field()?;
                let hlc = u64::from_le_bytes(self.read_array::<8>()?);
                let node = u64::from_le_bytes(self.read_array::<8>()?);
                let rt = RtFragment { lo, hi, version: Version { hlc, node } };
                self.ranges.note(&rt).map_err(|e| format!("record file: {e}"))?;
                self.range_tombstones.push(rt);
                continue;
            }
            return self.next_tagged(tag);
        }
    }

    fn next_tagged(&mut self, tag: u8) -> Result<Option<Record>, String> {
        match tag {
            TAG_END => {
                let count = u64::from_le_bytes(self.read_array::<8>()?);
                let expected = self.input.crc.clone().finalize();
                let stored = u32::from_le_bytes(self.read_array::<4>()?);
                if stored != expected {
                    return Err("record file: trailer checksum mismatch".into());
                }
                if count != self.counts.records + self.ranges.count {
                    return Err(format!(
                        "record file: trailer counts {count} records, read {}",
                        self.counts.records + self.ranges.count
                    ));
                }
                let mut extra = [0u8; 1];
                match self.input.read(&mut extra) {
                    Ok(0) => {}
                    Ok(_) => return Err("record file: bytes after the trailer".into()),
                    Err(e) => return Err(format!("record file: {e}")),
                }
                self.finished = true;
                Ok(None)
            }
            TAG_PUT | TAG_TOMBSTONE if self.ranges.count > 0 => {
                Err("record file: a point record follows a range tombstone".into())
            }
            TAG_PUT | TAG_TOMBSTONE => {
                let key = self.read_field()?;
                let hlc = u64::from_le_bytes(self.read_array::<8>()?);
                let node = u64::from_le_bytes(self.read_array::<8>()?);
                let value = if tag == TAG_PUT { Some(self.read_field()?) } else { None };
                if self.last_key.as_ref().is_some_and(|last| *last >= key) {
                    return Err("record file: keys are not strictly increasing".into());
                }
                let record = Record { key, version: Version { hlc, node }, value };
                self.counts.note(&record);
                self.last_key = Some(record.key.clone());
                Ok(Some(record))
            }
            other => Err(format!("record file: bad record tag {other}")),
        }
    }

    pub fn file_summary(&self) -> Option<(u64, u32)> {
        self.finished.then(|| (self.input.bytes, self.input.crc.clone().finalize()))
    }
}

pub fn open_records(path: &Path) -> Result<RecordReader<BufReader<File>>, String> {
    let file = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    RecordReader::new(BufReader::with_capacity(1 << 20, file))
        .map_err(|e| format!("{}: {e}", path.display()))
}

pub fn create_records(path: &Path, zone_id: ZoneId) -> io::Result<RecordWriter<BufWriter<File>>> {
    let file = std::fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    RecordWriter::new(BufWriter::with_capacity(1 << 20, file), zone_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileEntry {
    pub name: String,
    pub bytes: u64,
    pub crc32c: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZoneEntry {
    pub id: ZoneId,
    pub name: String,
    pub ordering: Ordering,
    pub rf: usize,
    pub file: FileEntry,
    pub counts: RecordCounts,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub backup_id: u128,
    pub created_unix_ms: u64,
    pub tool: String,
    pub source_incarnation: u128,
    pub pd_applied: u64,
    pub pd_applied_end: u64,
    pub ring_vnodes: u32,
    pub ring_rf: usize,
    pub pd_state: FileEntry,
    pub zones: Vec<ZoneEntry>,
}

fn ordering_name(ordering: Ordering) -> &'static str {
    match ordering {
        Ordering::Sorted => "sorted",
        Ordering::Unsorted => "unsorted",
    }
}

fn valid_token(s: &str) -> bool {
    !s.is_empty() && !s.chars().any(char::is_whitespace)
}

impl Manifest {
    pub fn render(&self) -> String {
        let mut out = format!("{MANIFEST_HEADER} {FORMAT_VERSION}\n");
        out.push_str(&format!("backup_id {:032x}\n", self.backup_id));
        out.push_str(&format!("created_unix_ms {}\n", self.created_unix_ms));
        out.push_str(&format!("tool {}\n", self.tool));
        out.push_str(&format!("source_incarnation {:032x}\n", self.source_incarnation));
        out.push_str(&format!("pd_applied {}\n", self.pd_applied));
        out.push_str(&format!("pd_applied_end {}\n", self.pd_applied_end));
        out.push_str(&format!("ring {} {}\n", self.ring_vnodes, self.ring_rf));
        out.push_str(&format!(
            "pd_state {} {} {:08x}\n",
            self.pd_state.name, self.pd_state.bytes, self.pd_state.crc32c
        ));
        for zone in &self.zones {
            out.push_str(&format!(
                "zone {} {} {} {} {} {} {:08x} {} {} {}\n",
                zone.id,
                zone.name,
                ordering_name(zone.ordering),
                zone.rf,
                zone.file.name,
                zone.file.bytes,
                zone.file.crc32c,
                zone.counts.records,
                zone.counts.puts,
                zone.counts.tombstones,
            ));
        }
        out.push_str("end\n");
        out
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let mut lines = text.lines();
        let header = lines.next().ok_or("MANIFEST is empty")?;
        let version = header
            .strip_prefix(MANIFEST_HEADER)
            .and_then(|rest| rest.strip_prefix(' '))
            .ok_or_else(|| format!("MANIFEST does not start with {MANIFEST_HEADER:?}"))?;
        if version != FORMAT_VERSION.to_string() {
            return Err(format!("unsupported backup format version {version:?} (this build reads {FORMAT_VERSION})"));
        }
        let mut backup_id = None;
        let mut created = None;
        let mut tool = None;
        let mut incarnation = None;
        let mut applied = None;
        let mut applied_end = None;
        let mut ring = None;
        let mut pd_state = None;
        let mut zones: Vec<ZoneEntry> = Vec::new();
        let mut ended = false;
        fn set<T>(slot: &mut Option<T>, value: T, key: &str) -> Result<(), String> {
            if slot.replace(value).is_some() {
                return Err(format!("MANIFEST repeats {key}"));
            }
            Ok(())
        }
        let hex128 = |s: &str| -> Result<u128, String> {
            if s.len() != 32 {
                return Err(format!("expected 32 hex digits, got {s:?}"));
            }
            u128::from_str_radix(s, 16).map_err(|_| format!("bad hex {s:?}"))
        };
        let num = |s: &str| -> Result<u64, String> { s.parse().map_err(|_| format!("bad number {s:?}")) };
        let crc = |s: &str| -> Result<u32, String> {
            if s.len() != 8 {
                return Err(format!("expected 8 hex digits, got {s:?}"));
            }
            u32::from_str_radix(s, 16).map_err(|_| format!("bad crc {s:?}"))
        };
        let file_name = |s: &str| -> Result<String, String> {
            if !valid_token(s) || s.contains('/') || s.starts_with('.') {
                return Err(format!("bad file name {s:?}"));
            }
            Ok(s.to_string())
        };
        for line in lines.by_ref() {
            let fields: Vec<&str> = line.split(' ').collect();
            match fields.as_slice() {
                ["end"] => {
                    ended = true;
                    break;
                }
                ["backup_id", id] => set(&mut backup_id, hex128(id)?, "backup_id")?,
                ["created_unix_ms", ms] => set(&mut created, num(ms)?, "created_unix_ms")?,
                ["tool", ..] if fields.len() > 1 => set(&mut tool, line["tool ".len()..].to_string(), "tool")?,
                ["source_incarnation", id] => set(&mut incarnation, hex128(id)?, "source_incarnation")?,
                ["pd_applied", n] => set(&mut applied, num(n)?, "pd_applied")?,
                ["pd_applied_end", n] => set(&mut applied_end, num(n)?, "pd_applied_end")?,
                ["ring", vnodes, rf] => set(
                    &mut ring,
                    (
                        u32::try_from(num(vnodes)?).map_err(|_| "bad ring vnodes")?,
                        usize::try_from(num(rf)?).map_err(|_| "bad ring rf")?,
                    ),
                    "ring",
                )?,
                ["pd_state", name, bytes, sum] => set(
                    &mut pd_state,
                    FileEntry { name: file_name(name)?, bytes: num(bytes)?, crc32c: crc(sum)? },
                    "pd_state",
                )?,
                ["zone", id, name, ordering, rf, file, bytes, sum, records, puts, tombstones] => {
                    let id = ZoneId::try_from(num(id)?).map_err(|_| format!("bad zone id {id:?}"))?;
                    if !valid_token(name) {
                        return Err(format!("bad zone name {name:?}"));
                    }
                    let ordering = match *ordering {
                        "sorted" => Ordering::Sorted,
                        "unsorted" => Ordering::Unsorted,
                        other => return Err(format!("bad zone ordering {other:?}")),
                    };
                    if zones.iter().any(|zone| zone.id == id) {
                        return Err(format!("MANIFEST repeats zone {id}"));
                    }
                    let counts = RecordCounts {
                        records: num(records)?,
                        puts: num(puts)?,
                        tombstones: num(tombstones)?,
                    };
                    if counts.puts.checked_add(counts.tombstones) != Some(counts.records) {
                        return Err(format!("MANIFEST zone {id}: puts + tombstones != records"));
                    }
                    zones.push(ZoneEntry {
                        id,
                        name: name.to_string(),
                        ordering,
                        rf: usize::try_from(num(rf)?).map_err(|_| "bad zone rf")?,
                        file: FileEntry { name: file_name(file)?, bytes: num(bytes)?, crc32c: crc(sum)? },
                        counts,
                    });
                }
                _ => return Err(format!("unrecognized MANIFEST line {line:?}")),
            }
        }
        if !ended {
            return Err("MANIFEST is incomplete (no end line)".into());
        }
        if lines.next().is_some() {
            return Err("MANIFEST has lines after end".into());
        }
        let missing = |what: &str| format!("MANIFEST lacks {what}");
        let (ring_vnodes, ring_rf) = ring.ok_or_else(|| missing("ring"))?;
        Ok(Manifest {
            backup_id: backup_id.ok_or_else(|| missing("backup_id"))?,
            created_unix_ms: created.ok_or_else(|| missing("created_unix_ms"))?,
            tool: tool.ok_or_else(|| missing("tool"))?,
            source_incarnation: incarnation.ok_or_else(|| missing("source_incarnation"))?,
            pd_applied: applied.ok_or_else(|| missing("pd_applied"))?,
            pd_applied_end: applied_end.ok_or_else(|| missing("pd_applied_end"))?,
            ring_vnodes,
            ring_rf,
            pd_state: pd_state.ok_or_else(|| missing("pd_state"))?,
            zones,
        })
    }
}

pub fn expected_zones(view: &PdStateView, ring_rf: usize) -> Vec<(ZoneId, ZoneConfig)> {
    let mut zones = vec![(DEFAULT_ZONE, ZoneConfig::default_config(ring_rf))];
    let mut catalog: Vec<(ZoneId, ZoneConfig)> = view
        .zones
        .iter()
        .filter(|(id, _)| *id != DEFAULT_ZONE)
        .map(|(id, cfg)| (id, cfg.clone()))
        .collect();
    catalog.sort_unstable_by_key(|(id, _)| *id);
    zones.extend(catalog);
    zones
}

fn check_file(dir: &Path, entry: &FileEntry, actual: (u64, u32)) -> Result<(), String> {
    if actual.0 != entry.bytes {
        return Err(format!(
            "{}: {} bytes on disk, MANIFEST records {}",
            dir.join(&entry.name).display(), actual.0, entry.bytes
        ));
    }
    if actual.1 != entry.crc32c {
        return Err(format!(
            "{}: CRC-32C {:08x} on disk, MANIFEST records {:08x}",
            dir.join(&entry.name).display(), actual.1, entry.crc32c
        ));
    }
    Ok(())
}

pub struct Backup {
    pub manifest: Manifest,
    pub pd_state: PdStateCapture,
}

pub fn open_backup(dir: &Path) -> Result<Backup, String> {
    let text = std::fs::read_to_string(dir.join(MANIFEST_FILE)).map_err(|e| {
        format!("{}: {e} (a backup without a MANIFEST is incomplete)", dir.join(MANIFEST_FILE).display())
    })?;
    let manifest = Manifest::parse(&text)?;
    let path = dir.join(&manifest.pd_state.name);
    let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    check_file(dir, &manifest.pd_state, (bytes.len() as u64, vals_store::crc::crc32(&bytes)))?;
    let pd_state = PdStateCapture::decode(&bytes)?;
    if pd_state.applied != manifest.pd_applied {
        return Err(format!(
            "pd-state was captured at index {}, MANIFEST records {}",
            pd_state.applied, manifest.pd_applied
        ));
    }
    let view = pd_state.view();
    if view.incarnation != Some(manifest.source_incarnation) {
        return Err("pd-state incarnation does not match the MANIFEST source_incarnation".into());
    }
    let expected = expected_zones(&view, manifest.ring_rf);
    if expected.len() != manifest.zones.len() {
        return Err(format!(
            "MANIFEST lists {} zones, the PD state defines {} (default zone included)",
            manifest.zones.len(),
            expected.len()
        ));
    }
    for ((id, cfg), zone) in expected.iter().zip(&manifest.zones) {
        if zone.id != *id || zone.name != cfg.name || zone.ordering != cfg.ordering || zone.rf != cfg.rf {
            return Err(format!(
                "MANIFEST zone {} ({} {} rf {}) does not match the PD state zone {id} ({} {} rf {})",
                zone.id, zone.name, ordering_name(zone.ordering), zone.rf,
                cfg.name, ordering_name(cfg.ordering), cfg.rf,
            ));
        }
        if zone.file.name != zone_file_name(zone.id) {
            return Err(format!("MANIFEST zone {} names file {:?}", zone.id, zone.file.name));
        }
    }
    Ok(Backup { manifest, pd_state })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VerifyReport {
    pub zones: usize,
    pub counts: RecordCounts,
    pub range_tombstones: u64,
    pub bytes: u64,
}

pub fn verify_zone_file(
    dir: &Path,
    zone: &ZoneEntry,
    mut each: impl FnMut(&Record),
) -> Result<Vec<RtFragment>, String> {
    let path = dir.join(&zone.file.name);
    let mut reader = open_records(&path)?;
    if reader.zone_id() != zone.id {
        return Err(format!("{}: holds zone {}, MANIFEST expects {}", path.display(), reader.zone_id(), zone.id));
    }
    while let Some(record) = reader.next_record().map_err(|e| format!("{}: {e}", path.display()))? {
        each(&record);
    }
    let summary = reader.file_summary().expect("finished reader");
    check_file(dir, &zone.file, summary)?;
    if reader.counts() != zone.counts {
        return Err(format!(
            "{}: holds {:?}, MANIFEST records {:?}",
            path.display(), reader.counts(), zone.counts
        ));
    }
    if zone.ordering != Ordering::Sorted && !reader.range_tombstones().is_empty() {
        return Err(format!("{}: holds range tombstones for unsorted zone {}", path.display(), zone.id));
    }
    Ok(std::mem::take(&mut reader.range_tombstones))
}

pub fn verify(dir: &Path) -> Result<(Manifest, VerifyReport), String> {
    let backup = open_backup(dir)?;
    let mut report = VerifyReport { bytes: backup.manifest.pd_state.bytes, ..VerifyReport::default() };
    for zone in &backup.manifest.zones {
        let range_tombstones = verify_zone_file(dir, zone, |_| {})?;
        report.zones += 1;
        report.range_tombstones += range_tombstones.len() as u64;
        report.counts.records += zone.counts.records;
        report.counts.puts += zone.counts.puts;
        report.counts.tombstones += zone.counts.tombstones;
        report.bytes += zone.file.bytes;
    }
    Ok((backup.manifest, report))
}
