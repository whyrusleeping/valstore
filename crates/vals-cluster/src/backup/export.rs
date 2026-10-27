use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

use vals_store::range_tombstone::FragmentSet;

use crate::antientropy::DEFAULT_BUCKETS;
use crate::pd::AddressedMembers;
use crate::replica::{pack_version, Stored};
use crate::ring::NodeId;
use crate::wire::{self, Request, Response, RtFragment};
use crate::zone::{ZoneConfig, ZoneId};

use super::{Record, RecordWriter};

const VALUES_CHUNK: usize = 256;
const MAX_RESPONSE_FRAME: usize = 64 << 20;
const IO_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_FOR: Duration = Duration::from_secs(120);

pub type ReplicaPage = Vec<(Vec<u8>, Stored)>;

type DigestKeys = (Vec<Vec<u8>>, Option<Vec<u8>>);

pub trait ReplicaSource {
    fn label(&self) -> String;
    fn next_page(&mut self) -> Result<Option<ReplicaPage>, String>;
    fn range_tombstones(&mut self) -> Result<Vec<RtFragment>, String> {
        Ok(Vec::new())
    }
}

pub(super) fn merged_range_tombstones(sources: &mut [Box<dyn ReplicaSource>]) -> Result<FragmentSet, String> {
    let mut set = FragmentSet::with_max_fragments(usize::MAX);
    for source in sources.iter_mut() {
        for rt in source.range_tombstones()? {
            if rt.hi.as_deref().is_some_and(|hi| rt.lo.as_deref().unwrap_or_default() >= hi) {
                return Err(format!("{}: returned a range tombstone with an empty span", source.label()));
            }
            set.overlay(rt.lo.as_deref(), rt.hi.as_deref(), pack_version(rt.version))
                .expect("an uncapped overlay never refuses");
        }
    }
    Ok(set)
}

pub(super) fn covered(set: &FragmentSet, record: &Record) -> bool {
    !set.is_empty() && set.cover(&record.key).is_some_and(|cover| pack_version(record.version) <= cover)
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MergeStats {
    pub replica_entries: u64,
    pub records: u64,
}

struct Cursor {
    source: Box<dyn ReplicaSource>,
    buf: VecDeque<(Vec<u8>, Stored)>,
    last: Option<Vec<u8>>,
    done: bool,
}

impl Cursor {
    fn head(&mut self) -> Result<Option<Vec<u8>>, String> {
        while self.buf.is_empty() && !self.done {
            match self.source.next_page()? {
                None => self.done = true,
                Some(page) => self.buf.extend(page),
            }
        }
        Ok(self.buf.front().map(|(key, _)| key.clone()))
    }

    fn pop(&mut self) -> Result<Stored, String> {
        let (key, stored) = self.buf.pop_front().expect("pop follows a non-empty head");
        if self.last.as_ref().is_some_and(|last| *last >= key) {
            return Err(format!(
                "{}: replica returned key {} out of order",
                self.source.label(),
                key_text(&key)
            ));
        }
        self.last = Some(key);
        Ok(stored)
    }
}

pub(super) fn key_text(key: &[u8]) -> String {
    let shown: String = key.iter().take(32).map(|b| format!("{b:02x}")).collect();
    if key.len() > 32 {
        format!("hex={shown}… ({} bytes)", key.len())
    } else {
        format!("hex={shown}")
    }
}

pub fn merge_replicas<W: Write>(
    sources: Vec<Box<dyn ReplicaSource>>,
    out: &mut RecordWriter<W>,
) -> Result<MergeStats, String> {
    merge_replicas_into(sources, &mut |record| {
        out.push(&record).map_err(|e| format!("write backup record: {e}"))
    })
}

pub fn merge_replicas_into(
    sources: Vec<Box<dyn ReplicaSource>>,
    sink: &mut dyn FnMut(Record) -> Result<(), String>,
) -> Result<MergeStats, String> {
    let mut cursors: Vec<Cursor> = sources
        .into_iter()
        .map(|source| Cursor { source, buf: VecDeque::new(), last: None, done: false })
        .collect();
    let mut heap: BinaryHeap<Reverse<(Vec<u8>, usize)>> = BinaryHeap::new();
    for (i, cursor) in cursors.iter_mut().enumerate() {
        if let Some(key) = cursor.head()? {
            heap.push(Reverse((key, i)));
        }
    }
    let mut stats = MergeStats::default();
    while let Some(Reverse((key, first))) = heap.pop() {
        let mut group = vec![first];
        while heap.peek().is_some_and(|Reverse((next, _))| *next == key) {
            group.push(heap.pop().expect("peeked").0 .1);
        }
        let mut held: Vec<(Stored, usize)> = Vec::with_capacity(group.len());
        for &i in &group {
            let stored = cursors[i].pop()?;
            stats.replica_entries += 1;
            if let Some((other, holder)) = held.iter().find(|(other, _)| other.version == stored.version) {
                if other.value != stored.value {
                    return Err(format!(
                        "replicas disagree at the identical version {:?} for key {}: {} and {} \
                         hold different {}; refusing to back up corrupt data",
                        stored.version,
                        key_text(&key),
                        cursors[*holder].source.label(),
                        cursors[i].source.label(),
                        if stored.value.is_some() != other.value.is_some() {
                            "kinds (put vs tombstone)"
                        } else {
                            "values"
                        },
                    ));
                }
            } else {
                held.push((stored, i));
            }
            if let Some(next) = cursors[i].head()? {
                heap.push(Reverse((next, i)));
            }
        }
        let (stored, _) = held
            .into_iter()
            .reduce(|best, next| if next.0.version.wins_over(&best.0.version) { next } else { best })
            .expect("a merge group is non-empty");
        sink(Record { key, version: stored.version, value: stored.value })?;
        stats.records += 1;
    }
    Ok(stats)
}

pub struct NetReplicaSource {
    node: NodeId,
    shard: usize,
    addr: SocketAddr,
    zone_id: ZoneId,
    stream: Option<TcpStream>,
    features: Option<u64>,
    after: Option<Vec<u8>>,
    unresolved: VecDeque<Vec<u8>>,
    done: bool,
    ids: Vec<u32>,
    retry_for: Duration,
    next_reqid: u64,
}

impl NetReplicaSource {
    pub fn new(node: NodeId, shard: usize, addr: SocketAddr, zone_id: ZoneId) -> Self {
        NetReplicaSource {
            node,
            shard,
            addr,
            zone_id,
            stream: None,
            features: None,
            after: None,
            unresolved: VecDeque::new(),
            done: false,
            ids: (0..DEFAULT_BUCKETS).collect(),
            retry_for: DEFAULT_RETRY_FOR,
            next_reqid: 1,
        }
    }

    pub fn with_retry_for(mut self, retry_for: Duration) -> Self {
        self.retry_for = retry_for;
        self
    }

    fn try_request(&mut self, req: &Request) -> Result<Response, String> {
        if self.stream.is_none() {
            let mut stream = TcpStream::connect_timeout(&self.addr, CONNECT_TIMEOUT)
                .map_err(|e| format!("connect: {e}"))?;
            stream.set_read_timeout(Some(IO_TIMEOUT)).map_err(|e| e.to_string())?;
            stream.set_write_timeout(Some(IO_TIMEOUT)).map_err(|e| e.to_string())?;
            stream.set_nodelay(true).map_err(|e| e.to_string())?;
            let negotiated =
                crate::proto::client_handshake_blocking(&mut stream).map_err(|e| format!("handshake: {e}"))?;
            self.features = Some(negotiated.features);
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().expect("connected above");
        let reqid = self.next_reqid;
        self.next_reqid += 1;
        let payload = wire::encode_request_payload(req);
        let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
        frame.extend_from_slice(&reqid.to_le_bytes());
        frame.extend_from_slice(&payload);
        stream.write_all(&frame).map_err(|e| format!("send: {e}"))?;
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).map_err(|e| format!("receive: {e}"))?;
        let len = u32::from_le_bytes(len) as usize;
        if !(8..=MAX_RESPONSE_FRAME).contains(&len) {
            return Err(format!("response frame of {len} bytes"));
        }
        let mut block = vec![0u8; len];
        stream.read_exact(&mut block).map_err(|e| format!("receive: {e}"))?;
        if u64::from_le_bytes(block[..8].try_into().unwrap()) != reqid {
            return Err("response for another request".into());
        }
        wire::decode_response(&block[8..]).map_err(|e| format!("bad response: {e:?}"))
    }

    fn request(&mut self, req: &Request) -> Result<Response, String> {
        let deadline = Instant::now() + self.retry_for;
        let mut backoff = Duration::from_millis(20);
        loop {
            let failure = match self.try_request(req) {
                Ok(Response::Busy) => "busy".to_string(),
                Ok(Response::Err(e)) => return Err(format!("{}: {e}", self.label())),
                Ok(response) => return Ok(response),
                Err(e) => {
                    self.stream = None;
                    e
                }
            };
            if Instant::now() >= deadline {
                return Err(format!(
                    "{}: no answer within {:?} (last: {failure})",
                    self.label(),
                    self.retry_for
                ));
            }
            log::debug!("backup: {}: {failure}; retrying", self.label());
            std::thread::sleep(backoff);
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
    }

    fn digest_page(&mut self) -> Result<DigestKeys, String> {
        let ids = std::mem::take(&mut self.ids);
        let after = self.after.clone();
        let response = self.request(&Request::SyncBucketDigests {
            zone_id: self.zone_id,
            num_buckets: DEFAULT_BUCKETS,
            ids: &ids,
            after: after.as_deref(),
        });
        self.ids = ids;
        match response? {
            Response::BucketDigests { digests, more, resume } => {
                let next = match (more, resume) {
                    (false, _) => None,
                    (true, Some(resume)) => Some(resume),
                    (true, None) => digests.last().map(|(key, _)| key.clone()),
                };
                if more && next.as_deref().is_none_or(|next| after.as_deref().is_some_and(|a| next <= a)) {
                    return Err(format!("{}: truncated digest page without progress", self.label()));
                }
                Ok((digests.into_iter().map(|(key, _)| key).collect(), next))
            }
            other => Err(format!("{}: expected a bucket-digest reply, got {other:?}", self.label())),
        }
    }

    fn values_chunk(&mut self) -> Result<ReplicaPage, String> {
        let take = self.unresolved.len().min(VALUES_CHUNK);
        let chunk: Vec<Vec<u8>> = self.unresolved.iter().take(take).cloned().collect();
        let refs: Vec<&[u8]> = chunk.iter().map(Vec::as_slice).collect();
        let (entries, more) = match self.request(&Request::SyncValues { zone_id: self.zone_id, keys: &refs })? {
            Response::Values { entries, more } => (entries, more),
            other => return Err(format!("{}: expected a values reply, got {other:?}", self.label())),
        };
        let resolved = if more {
            match entries.last() {
                Some((last, _)) => chunk
                    .iter()
                    .position(|key| key == last)
                    .map(|i| i + 1)
                    .ok_or_else(|| format!("{}: values reply names an unrequested key", self.label()))?,
                None => 1,
            }
        } else {
            chunk.len()
        };
        self.unresolved.drain(..resolved);
        Ok(entries)
    }
}

impl ReplicaSource for NetReplicaSource {
    fn label(&self) -> String {
        format!("node {} shard {} ({}) zone {}", self.node, self.shard, self.addr, self.zone_id)
    }

    fn range_tombstones(&mut self) -> Result<Vec<RtFragment>, String> {
        let unsupported = |features: Option<u64>| {
            features.is_some_and(|f| f & crate::proto::FEATURE_RANGE_TOMBSTONES == 0)
        };
        let mut out = Vec::new();
        let mut after: Option<Vec<u8>> = None;
        loop {
            if unsupported(self.features) {
                return Ok(out);
            }
            let request =
                Request::SyncRangeTombstones { zone_id: self.zone_id, requester: None, digest: 0, after: after.as_deref() };
            let (more, fragments) = match self.request(&request) {
                Ok(Response::RangeTombstones { same: true, .. }) => return Ok(out),
                Ok(Response::RangeTombstones { more, fragments, .. }) => (more, fragments),
                Ok(other) => return Err(format!("{}: expected a range-tombstones reply, got {other:?}", self.label())),
                Err(_) if unsupported(self.features) => return Ok(out),
                Err(e) => return Err(e),
            };
            let next = fragments.last().map(|rt| rt.lo.clone().unwrap_or_default());
            out.extend(fragments);
            if !more {
                return Ok(out);
            }
            if next.as_deref().is_none_or(|next| after.as_deref().is_some_and(|a| next <= a)) {
                return Err(format!("{}: truncated range-tombstone page without progress", self.label()));
            }
            after = next;
        }
    }

    fn next_page(&mut self) -> Result<Option<ReplicaPage>, String> {
        loop {
            if self.unresolved.is_empty() {
                if self.done {
                    return Ok(None);
                }
                let (keys, next) = self.digest_page()?;
                self.done = next.is_none();
                self.after = next;
                self.unresolved.extend(keys);
                continue;
            }
            let entries = self.values_chunk()?;
            if !entries.is_empty() {
                return Ok(Some(entries));
            }
        }
    }
}

pub fn zone_sources(
    members: &AddressedMembers,
    zone_id: ZoneId,
    cfg: &ZoneConfig,
) -> Result<Vec<Box<dyn ReplicaSource>>, String> {
    let mut sources: Vec<Box<dyn ReplicaSource>> = Vec::new();
    for (node, _, addrs) in members {
        if cfg.scope.as_ref().is_some_and(|scope| !scope.contains(node)) {
            continue;
        }
        if addrs.is_empty() {
            return Err(format!(
                "member {node} has no data address in the PD state; its replicas of zone {zone_id} cannot be read"
            ));
        }
        for (shard, addr) in addrs.iter().enumerate() {
            sources.push(Box::new(NetReplicaSource::new(*node, shard, *addr, zone_id)));
        }
    }
    if sources.is_empty() {
        return Err(format!("zone {zone_id} has no member to read from"));
    }
    Ok(sources)
}

pub fn ring_params(addr: SocketAddr, retry_for: Duration) -> Result<(u32, usize), String> {
    let mut source = NetReplicaSource::new(0, 0, addr, 0).with_retry_for(retry_for);
    match source.request(&Request::Members)? {
        Response::Members { vnodes, rf, .. } => Ok((vnodes, rf as usize)),
        other => Err(format!("{addr}: expected a members reply, got {other:?}")),
    }
}
