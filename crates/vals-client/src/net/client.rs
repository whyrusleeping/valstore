use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::BuildHasherDefault;
use std::io::{self, ErrorKind};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::RawFd;
use std::pin::Pin;
use std::rc::{Rc, Weak};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::ae::AePeer;
use crate::coord::{
    Acquire, BatchAck, BatchEntry, Credit, CreditPool, NodeCreditCost, RingView, ScanPage,
    Transport, TransportError,
};
use crate::partition::RangeMap;
use crate::reactor::{self, Interest};
use crate::ring::NodeId;
use crate::types::{Applied, MixedReadEntry, MixedReadReply, MixedReadRole, Stored};
use crate::wire::{self, Request, Response};
use crate::zone::{Catalog, ZoneId};

use super::{catalog_from_wire, frame_msg, FrameRead, MAX_CONN_SEND_BUF, MAX_FRAME};

type IdMap<V> = HashMap<u64, V, BuildHasherDefault<crate::net::FxHasher>>;

fn try_transport_credit(
    pool: &Rc<RefCell<CreditPool>>,
    node: NodeId,
    ops: usize,
    bytes: usize,
) -> Option<Credit> {
    let node_costs = vec![NodeCreditCost { node, ops, bytes }];
    let mut state = pool.borrow_mut();
    if !state.fits_all(&node_costs, ops, bytes) {
        return None;
    }
    state.reserve(&node_costs, ops, bytes);
    drop(state);
    Some(Credit {
        pool: pool.clone(),
        ops,
        bytes,
        node_costs,
    })
}

#[doc(hidden)]
pub struct MuxConn {
    addr: SocketAddr,
    st: RefCell<ConnState>,
}

#[doc(hidden)]
pub struct AccountedBytes {
    bytes: Vec<u8>,
    _credits: Vec<Credit>,
}

impl std::ops::Deref for AccountedBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.bytes
    }
}

impl std::fmt::Debug for AccountedBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.bytes.fmt(f)
    }
}

impl PartialEq<Vec<u8>> for AccountedBytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.bytes == *other
    }
}

impl<const N: usize> PartialEq<[u8; N]> for AccountedBytes {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.bytes == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for AccountedBytes {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.bytes == **other
    }
}

#[doc(hidden)]
#[derive(Default)]
pub struct Waiter {
    waker: Option<Waker>,
    result: Option<Result<Vec<u8>, TransportError>>,
    deadline_at: Option<Instant>,
    send_watermark: Option<u64>,
    frame_len: usize,
    send_only: bool,
    op_credit: Option<Credit>,
    request_credit: Option<Credit>,
    response_credits: Vec<Credit>,
}

#[doc(hidden)]
#[derive(Default)]
pub struct ConnState {
    #[doc(hidden)]
    pub sock: Option<super::Sock>,
    #[doc(hidden)]
    pub fd: RawFd,
    next_id: u64,
    pub out: Vec<u8>,
    out_pos: usize,
    enqueued_total: u64,
    sent_total: u64,
    min_send_watermark: Option<u64>,
    pub pending: IdMap<Waiter>,
    finished: IdMap<Result<AccountedBytes, TransportError>>,
    reader: FrameRead,
    driver: Option<u64>,
    dirty: bool,
    connect_deadline: Option<Duration>,
    rpc_deadline: Option<Duration>,
    next_deadline: Option<Instant>,
    negotiated: Option<crate::proto::Negotiated>,
    last_features: Option<u64>,
    pending_peak: usize,
    queued_bytes_peak: usize,
    write_calls: u64,
    read_drains: u64,
    connection_generations: u64,
    budget_pool: Option<Rc<RefCell<CreditPool>>>,
    budget_node: NodeId,
    reader_bytes: usize,
    last_io: Option<Instant>,
    idle_resumed: bool,
}

pub(crate) const IDLE_REUSE_AFTER: Duration = Duration::from_secs(1);

impl MuxConn {
    #[doc(hidden)]
    pub fn new(addr: SocketAddr) -> Self {
        MuxConn {
            addr,
            st: RefCell::new(ConnState::default()),
        }
    }

    fn new_accounted(
        addr: SocketAddr,
        node: NodeId,
        pool: Rc<RefCell<CreditPool>>,
    ) -> Self {
        let mut state = ConnState::default();
        state.budget_pool = Some(pool);
        state.budget_node = node;
        MuxConn {
            addr,
            st: RefCell::new(state),
        }
    }
}

impl Drop for MuxConn {
    fn drop(&mut self) {
        let st = self.st.get_mut();
        if st.sock.is_some() && st.fd != 0 {
            reactor::deregister(st.fd);
        }
        st.release_reader_budget();
    }
}

impl ConnState {
    fn release_reader_budget(&mut self) {
        if self.reader_bytes > 0 {
            if let Some(pool) = &self.budget_pool {
                let costs = [NodeCreditCost {
                    node: self.budget_node,
                    ops: 0,
                    bytes: self.reader_bytes,
                }];
                pool.borrow_mut().release(&costs, 0, self.reader_bytes);
            }
            self.reader_bytes = 0;
        }
        self.reader = FrameRead::default();
    }

    fn ensure_conn(&mut self, addr: SocketAddr) -> Result<(), TransportError> {
        if self.sock.is_none() {
            let s = connect_sock(addr).map_err(|_| TransportError::Unavailable)?;
            self.fd = s.as_raw_fd();
            self.sock = Some(s);
            self.connection_generations = self.connection_generations.saturating_add(1);
            self.out = Vec::new();
            self.out_pos = 0;
            self.release_reader_budget();
            self.driver = None;
            self.sent_total = 0;
            self.min_send_watermark = None;
            self.negotiated = None;
            self.last_io = Some(Instant::now());
            self.idle_resumed = false;
            self.out.extend_from_slice(&crate::proto::encode_hello());
            self.enqueued_total = self.out.len() as u64;
        }
        Ok(())
    }

    fn fail_expired(&mut self, now: Instant, wake: &mut Vec<Waker>) {
        let mut next: Option<Instant> = None;
        for w in self.pending.values_mut() {
            if w.result.is_some() {
                continue;
            }
            match w.deadline_at {
                Some(da) if da <= now => {
                    w.result = Some(Err(TransportError::Timeout));
                    if let Some(wk) = w.waker.take() {
                        wake.push(wk);
                    }
                }
                Some(da) => next = Some(next.map_or(da, |n| n.min(da))),
                None => {}
            }
        }
        self.next_deadline = next;
    }

    fn resolve_sent(&mut self, wake: &mut Vec<Waker>) {
        let sent = self.sent_total;
        let mut next: Option<u64> = None;
        for w in self.pending.values_mut() {
            let Some(wm) = w.send_watermark else { continue };
            if sent < wm {
                next = Some(next.map_or(wm, |n| n.min(wm)));
                continue;
            }
            w.request_credit = None;
            if w.send_only && w.result.is_none() {
                w.op_credit = None;
                w.result = Some(Ok(Vec::new()));
                if let Some(wk) = w.waker.take() {
                    wake.push(wk);
                }
            }
        }
        self.min_send_watermark = next;
    }

    fn take_wholly_unsent(&mut self, id: u64) -> Option<Waiter> {
        let waiter = self.pending.get(&id)?;
        let watermark = waiter.send_watermark?;
        let frame_len = u64::try_from(waiter.frame_len).ok()?;
        let frame_start = watermark.checked_sub(frame_len)?;
        if self.sent_total > frame_start {
            return None;
        }
        let base = self
            .enqueued_total
            .checked_sub(u64::try_from(self.out.len()).ok()?)?;
        let start = usize::try_from(frame_start.checked_sub(base)?).ok()?;
        let end = start.checked_add(waiter.frame_len)?;
        let new_enqueued_total = self.enqueued_total.checked_sub(frame_len)?;
        if start < self.out_pos || end > self.out.len() {
            return None;
        }

        let old_len = self.out.len();
        self.out.copy_within(end..old_len, start);
        self.out.truncate(old_len - waiter.frame_len);
        self.out = std::mem::take(&mut self.out)
            .into_boxed_slice()
            .into_vec();
        self.enqueued_total = new_enqueued_total;
        let waiter = self.pending.remove(&id)?;
        for pending in self.pending.values_mut() {
            if let Some(pending_watermark) = &mut pending.send_watermark {
                if *pending_watermark > watermark {
                    *pending_watermark -= frame_len;
                }
            }
        }
        self.min_send_watermark = self
            .pending
            .values()
            .filter_map(|pending| pending.send_watermark)
            .filter(|&pending_watermark| pending_watermark > self.sent_total)
            .min();
        self.next_deadline = self
            .pending
            .values()
            .filter(|pending| pending.result.is_none())
            .filter_map(|pending| pending.deadline_at)
            .min();
        Some(waiter)
    }

    #[doc(hidden)]
    pub fn set_negotiated_for_test(&mut self) {
        self.negotiated = Some(crate::proto::test_negotiated());
    }

    #[doc(hidden)]
    pub fn queue_send_probe(&mut self, out: Vec<u8>, id: u64) {
        let wm = self.enqueued_total + out.len() as u64;
        self.out.extend_from_slice(&out);
        self.enqueued_total += out.len() as u64;
        self.min_send_watermark = Some(self.min_send_watermark.map_or(wm, |m| m.min(wm)));
        self.pending.insert(
            id,
            Waiter {
                send_watermark: Some(wm),
                frame_len: out.len(),
                send_only: true,
                waker: Some(Waker::noop().clone()),
                ..Default::default()
            },
        );
    }

    #[doc(hidden)]
    pub fn waiter_outcome(&self, id: u64) -> Option<bool> {
        self.pending
            .get(&id)
            .and_then(|w| w.result.as_ref())
            .map(|r| r.is_ok())
    }

    fn reset_failed_as(&mut self, wake: &mut Vec<Waker>, error: TransportError) {
        if self.fd != 0 {
            reactor::deregister(self.fd);
        }
        self.next_deadline = None;
        self.sock = None;
        self.fd = 0;
        self.out = Vec::new();
        self.out_pos = 0;
        self.enqueued_total = 0;
        self.sent_total = 0;
        self.min_send_watermark = None;
        self.release_reader_budget();
        self.driver = None;
        self.negotiated = None;
        for (id, mut w) in self.pending.drain() {
            let outcome = match w.result.take() {
                Some(result) => result.map(|bytes| AccountedBytes {
                    bytes,
                    _credits: std::mem::take(&mut w.response_credits),
                }),
                None => Err(error.clone()),
            };
            self.finished.insert(id, outcome);
            if let Some(wk) = w.waker.take() {
                wake.push(wk);
            }
        }
    }

    fn reset_failed(&mut self, wake: &mut Vec<Waker>) {
        self.reset_failed_as(wake, TransportError::Unavailable);
    }

    fn send_end(&self) -> usize {
        if self.negotiated.is_none() {
            self.out.len().min(4 + crate::proto::HELLO_BODY_LEN)
        } else {
            self.out.len()
        }
    }

    fn has_sendable_bytes(&self) -> bool {
        self.out_pos < self.send_end()
    }

    #[doc(hidden)]
    pub fn drive(&mut self, wake: &mut Vec<Waker>) -> bool {
        let Some(sock) = self.sock.take() else {
            return false;
        };
        let mut ok = true;
        let write_end = self.send_end();
        let pos_before = self.out_pos;
        while self.out_pos < write_end {
            match sock.write(&self.out[self.out_pos..write_end]) {
                Ok(0) => {
                    ok = false;
                    break;
                }
                Ok(n) => {
                    self.write_calls += 1;
                    self.out_pos += n;
                    self.sent_total += n as u64;
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if self.out_pos != pos_before {
            self.last_io = Some(Instant::now());
        }
        if ok && self.out_pos == self.out.len() {
            self.out = Vec::new();
            self.out_pos = 0;
        } else if ok && self.negotiated.is_some() && self.out_pos > 0 {
            let unsent = self.out.split_off(self.out_pos);
            self.out = unsent;
            self.out_pos = 0;
        }
        if let Some(m) = self.min_send_watermark {
            if self.sent_total >= m {
                self.resolve_sent(wake);
            }
        }
        if ok {
            if let Err(error) = self.drain_inbound(&sock, wake) {
                self.reset_failed_as(wake, error);
                return false;
            }
        }
        if !ok {
            self.reset_failed(wake);
            return false;
        }
        if self.out.len() - self.out_pos > MAX_CONN_SEND_BUF {
            self.reset_failed(wake);
            return false;
        }
        let need_write = self.has_sendable_bytes();
        self.sock = Some(sock);
        need_write
    }

    fn read_block(&mut self, sock: &super::Sock) -> Poll<Result<(usize, usize), super::FrameErr>> {
        let Some(pool) = self.budget_pool.clone() else {
            return self.reader.read_block(sock);
        };
        let node = self.budget_node;
        let charged = Cell::new(0usize);
        let released = Cell::new(0usize);
        let result = self.reader.read_block_accounted(
            sock,
            |bytes| {
                let costs = [NodeCreditCost { node, ops: 0, bytes }];
                let mut state = pool.borrow_mut();
                if !state.fits_all(&costs, 0, bytes) {
                    return false;
                }
                state.reserve(&costs, 0, bytes);
                charged.set(charged.get() + bytes);
                true
            },
            |bytes| {
                let costs = [NodeCreditCost { node, ops: 0, bytes }];
                pool.borrow_mut().release(&costs, 0, bytes);
                released.set(released.get() + bytes);
            },
        );
        self.reader_bytes = self
            .reader_bytes
            .saturating_add(charged.get())
            .saturating_sub(released.get());
        result
    }

    fn drain_inbound(
        &mut self,
        sock: &super::Sock,
        wake: &mut Vec<Waker>,
    ) -> Result<(), TransportError> {
        self.read_drains += 1;
        let mut got_frame = false;
        loop {
            match self.read_block(sock) {
                Poll::Ready(Ok((off, len))) => {
                    got_frame = true;
                    self.idle_resumed = false;
                    let block = self.reader.frame_at(off, len);
                    if self.negotiated.is_none() {
                        match crate::proto::decode_hello(block)
                            .and_then(|(v, f)| crate::proto::negotiate(v, f))
                        {
                            Ok(n) => {
                                self.negotiated = Some(n);
                                self.last_features = Some(n.features);
                                let deadline_at = self.rpc_deadline.map(|d| Instant::now() + d);
                                self.next_deadline = deadline_at;
                                for waiter in self.pending.values_mut() {
                                    if waiter.result.is_none() {
                                        waiter.deadline_at = deadline_at;
                                    }
                                }
                            }
                            Err(e) => {
                                crate::observe::counters().handshake_failures.inc();
                                log::warn!(
                                    "transport: server handshake rejected ({e:?}); \
                                     dropping connection"
                                );
                                return Err(TransportError::Unavailable);
                            }
                        }
                        continue;
                    }
                    if block.len() < 8 {
                        return Err(TransportError::Unavailable);
                    }
                    let reqid = u64::from_le_bytes(block[0..8].try_into().unwrap());
                    if self.pending.contains_key(&reqid) {
                        let reply_credit = match &self.budget_pool {
                            Some(pool) => match try_transport_credit(
                                pool,
                                self.budget_node,
                                0,
                                block.len() - 8,
                            ) {
                                Some(credit) => Some(credit),
                                None => return Err(TransportError::Overloaded),
                            },
                            None => None,
                        };
                        let reply = block[8..].to_vec();
                        let w = self.pending.get_mut(&reqid).unwrap();
                        if let Some(credit) = reply_credit {
                            w.response_credits.push(credit);
                        }
                        w.request_credit = None;
                        w.op_credit = None;
                        w.result = Some(Ok(reply));
                        if let Some(wk) = w.waker.take() {
                            wake.push(wk);
                        }
                    }
                }
                Poll::Ready(Err(super::FrameErr::Overloaded)) => {
                    return Err(TransportError::Overloaded)
                }
                Poll::Ready(Err(_)) => return Err(TransportError::Unavailable),
                Poll::Pending => {
                    if got_frame {
                        self.last_io = Some(Instant::now());
                    }
                    return Ok(());
                }
            }
        }
    }

    fn drain_reads(&mut self, wake: &mut Vec<Waker>) {
        let Some(sock) = self.sock.take() else {
            return;
        };
        match self.drain_inbound(&sock, wake) {
            Ok(()) => self.sock = Some(sock),
            Err(error) => self.reset_failed_as(wake, error),
        }
    }
}

struct DeadlineDriver {
    conns: RefCell<Vec<Weak<MuxConn>>>,
    admission_pools: RefCell<Vec<std::rc::Weak<RefCell<CreditPool>>>>,
}

thread_local! {
    static DIRTY: RefCell<Vec<Rc<MuxConn>>> = const { RefCell::new(Vec::new()) };
    static DEADLINE_DRIVER: DeadlineDriver = const { DeadlineDriver {
        conns: RefCell::new(Vec::new()),
        admission_pools: RefCell::new(Vec::new()),
    } };
}

fn register_deadline_conn(conn: &Rc<MuxConn>) {
    DEADLINE_DRIVER.with(|driver| driver.conns.borrow_mut().push(Rc::downgrade(conn)));
}

fn register_admission_pool(pool: &Rc<RefCell<CreditPool>>) {
    DEADLINE_DRIVER.with(|driver| {
        driver
            .admission_pools
            .borrow_mut()
            .push(Rc::downgrade(pool));
    });
}

fn drive_deadlines() {
    let now = Instant::now();
    let mut wake = Vec::new();
    DEADLINE_DRIVER.with(|driver| {
        driver.conns.borrow_mut().retain(|weak| {
            let Some(conn) = weak.upgrade() else { return false };
            let mut st = conn.st.borrow_mut();
            if st.next_deadline.is_some_and(|deadline| deadline <= now) {
                st.fail_expired(now, &mut wake);
            }
            true
        });
        driver.admission_pools.borrow_mut().retain(|weak| {
            let Some(pool) = weak.upgrade() else { return false };
            wake.extend(pool.borrow().expired_wakers(now));
            true
        });
    });
    for waker in wake {
        waker.wake();
    }
}

#[doc(hidden)]
pub fn flush_dirty() {
    drive_deadlines();
    let conns = DIRTY.with(|d| std::mem::take(&mut *d.borrow_mut()));
    if conns.is_empty() {
        return;
    }
    let mut wake = Vec::new();
    for c in conns {
        let still_pending = {
            let mut st = c.st.borrow_mut();
            st.dirty = false;
            st.drive(&mut wake)
        };
        if still_pending {
            let mut st = c.st.borrow_mut();
            if !st.dirty {
                st.dirty = true;
                drop(st);
                DIRTY.with(|d| d.borrow_mut().push(c));
            }
        }
    }
    for w in wake {
        w.wake();
    }
}

fn mark_dirty(conn: &Rc<MuxConn>, st: &mut ConnState) {
    if !st.dirty {
        st.dirty = true;
        DIRTY.with(|d| d.borrow_mut().push(conn.clone()));
    }
}

pub static CONNECTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(crate) fn connect_sock(addr: SocketAddr) -> io::Result<super::Sock> {
    if addr.ip().is_loopback() {
        if let Some(path) = super::uds_path_for_port(addr.port()) {
            if let Ok(s) = std::os::unix::net::UnixStream::connect(&path) {
                s.set_nonblocking(true)?;
                CONNECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return Ok(super::Sock::Unix(s));
            }
        }
    }
    Ok(super::Sock::Tcp(connect_nonblocking(addr)?))
}

#[doc(hidden)]
pub fn connect_nonblocking(addr: SocketAddr) -> io::Result<TcpStream> {
    CONNECTS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let sock = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    sock.set_nonblocking(true)?;
    let _ = sock.set_nodelay(true);
    match sock.connect(&addr.into()) {
        Ok(()) => {}
        Err(e) if e.raw_os_error() == Some(libc::EINPROGRESS) => {}
        Err(e) if e.kind() == ErrorKind::WouldBlock => {}
        Err(e) => return Err(e),
    }
    Ok(sock.into())
}


mod transport;
mod multi_scan;
pub use transport::{
    ClientConfig, ClientConfigError, ConnectionStats, NetTransport, DEFAULT_CLIENT_MAX_BYTES,
    DEFAULT_CLIENT_MAX_OPS, DEFAULT_CLIENT_NODE_MAX_BYTES, DEFAULT_CLIENT_NODE_MAX_OPS,
    DEFAULT_CONNECT_DEADLINE, DEFAULT_RPC_DEADLINE,
};

pub struct NetPeer<'a> {
    transport: &'a NetTransport,
    node: NodeId,
    shard: usize,
}

impl AePeer for NetPeer<'_> {
    async fn tree(
        &self,
        zone_id: ZoneId,
        level: u32,
        scope_g: u32,
        scope: &[u32],
        requester: Option<NodeId>,
    ) -> Result<Vec<(u32, u64)>, TransportError> {
        match self
            .transport
            .rpc(
                self.node,
                self.shard,
                &Request::SyncTree {
                    zone_id,
                    level,
                    scope_g,
                    scope,
                    requester,
                },
                1,
            )
            .await?
            .response
        {
            Response::Tree(nodes) => Ok(nodes),
            Response::Busy => Err(TransportError::Busy),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected tree reply".into())),
        }
    }

    async fn bucket_digests(
        &self,
        zone_id: ZoneId,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
    ) -> Result<crate::types::DigestCursorPage, TransportError> {
        match self
            .transport
            .rpc(
                self.node,
                self.shard,
                &Request::SyncBucketDigests {
                    zone_id,
                    num_buckets,
                    ids,
                    after,
                },
                1,
            )
            .await?
            .response
        {
            Response::BucketDigests {
                digests,
                more,
                resume,
            } => {
                let next = match (more, resume) {
                    (false, _) => None,
                    (true, Some(resume)) => Some(resume),
                    (true, None) => digests.last().map(|(key, _)| key.clone()),
                };
                if more && next.as_deref().is_none_or(|next| after.is_some_and(|after| next <= after)) {
                    return Err(TransportError::Remote(
                        "peer returned a truncated digest page without progress".into(),
                    ));
                }
                Ok((digests, next))
            }
            Response::Busy => Err(TransportError::Busy),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote(
                "expected bucket-digests reply".into(),
            )),
        }
    }

    async fn values(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> Result<crate::types::EntryPage, TransportError> {
        let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        match self
            .transport
            .rpc(
                self.node,
                self.shard,
                &Request::SyncValues {
                    zone_id,
                    keys: &key_refs,
                },
                keys.len(),
            )
            .await?
            .response
        {
            Response::Values { entries, more } => Ok((entries, more)),
            Response::Busy => Err(TransportError::Busy),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected values reply".into())),
        }
    }

    async fn range_tombstones(
        &self,
        zone_id: ZoneId,
        requester: Option<NodeId>,
        digest: u64,
        after: Option<&[u8]>,
    ) -> Result<Option<crate::ae::RtSyncPage>, TransportError> {
        if self.transport.range_tombstones_supported(self.node) == Some(false) {
            return Ok(None);
        }
        let reply = self
            .transport
            .rpc(
                self.node,
                self.shard,
                &Request::SyncRangeTombstones {
                    zone_id,
                    requester,
                    digest,
                    after,
                },
                1,
            )
            .await?;
        match reply.response {
            Response::RangeTombstones {
                same,
                more,
                fragments,
            } => {
                if more && fragments.is_empty() {
                    return Err(TransportError::Remote(
                        "peer returned a truncated range-tombstone page without fragments".into(),
                    ));
                }
                Ok(Some(crate::ae::RtSyncPage {
                    same,
                    more,
                    fragments,
                }))
            }
            _ if self.transport.range_tombstones_supported(self.node) == Some(false) => Ok(None),
            Response::Busy => Err(TransportError::Busy),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote(
                "expected range-tombstones reply".into(),
            )),
        }
    }
}

impl Transport for NetTransport {
    fn client_budgets(&self) -> Option<crate::coord::ClientBudgets> {
        self.budgets
    }

    fn retain_response(
        &self,
        node: NodeId,
        bytes: usize,
    ) -> Result<Option<Credit>, TransportError> {
        if bytes == 0 {
            return Ok(None);
        }
        let Some(pool) = &self.transport_credits else {
            return Ok(None);
        };
        try_transport_credit(pool, node, 0, bytes)
            .map(Some)
            .ok_or(TransportError::Overloaded)
    }

    fn set_route_epoch(&self, epoch: u64) {
        self.route_epoch.set(epoch);
    }

    fn set_sorted_ranges(&self, ranges: &HashMap<ZoneId, RangeMap>) {
        *self.sorted_ranges.borrow_mut() = ranges.clone();
    }

    fn add_peer(&self, node: NodeId, addrs: &[SocketAddr]) {
        NetTransport::add_peer(self, node, addrs);
    }

    fn flush_requests(&self) {
        flush_dirty();
    }

    async fn apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: crate::hlc::Version,
    ) -> Result<Applied, TransportError> {
        let shard = self.data_shard(node, zone_id, key);
        match self
            .rpc(
                node,
                shard,
                &Request::Apply {
                    epoch: self.route_epoch.get(),
                    zone_id,
                    key,
                    value,
                    version,
                },
                1,
            )
            .await?
            .response
        {
            Response::Apply(a) => Ok(a),
            Response::StaleEpoch(e) => Err(TransportError::StaleEpoch(e)),
            Response::Busy => Err(TransportError::Busy),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected apply reply".into())),
        }
    }

    async fn apply_send(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: crate::hlc::Version,
    ) -> Result<(), TransportError> {
        self.rpc_send(
            node,
            self.data_shard(node, zone_id, key),
            &Request::Apply {
                epoch: self.route_epoch.get(),
                zone_id,
                key,
                value,
                version,
            },
        )
        .await
    }

    async fn read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.read_accounted(node, zone_id, key)
            .await
            .map(|(value, _credit)| value)
    }

    async fn digest(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<crate::hlc::Version>, TransportError> {
        match self
            .rpc(
                node,
                self.data_shard(node, zone_id, key),
                &Request::Digest {
                    epoch: self.route_epoch.get(),
                    zone_id,
                    key,
                },
                1,
            )
            .await?
            .response
        {
            Response::Digest(v) => Ok(v),
            Response::Busy => Err(TransportError::Busy),
            Response::StaleEpoch(e) => Err(TransportError::StaleEpoch(e)),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected digest reply".into())),
        }
    }

    async fn scan(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> Result<ScanPage, TransportError> {
        match self
            .rpc(
                node,
                self.scan_shard(node, zone_id, lo),
                &Request::Scan {
                    epoch: self.route_epoch.get(),
                    zone_id,
                    lo,
                    hi,
                    max,
                },
                1,
            )
            .await?
            .response
        {
            Response::Scan {
                entries,
                more,
                fragments,
                resume,
            } => Ok(ScanPage {
                entries,
                more,
                fragments,
                resume,
            }),
            Response::Busy => Err(TransportError::Busy),
            Response::StaleEpoch(e) => Err(TransportError::StaleEpoch(e)),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected scan reply".into())),
        }
    }

    async fn scan_many(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        scans: &[wire::WireScan<'_>],
        keys_only: bool,
    ) -> Vec<Result<ScanPage, TransportError>> {
        self.multi_scan::<false>(node, zone_id, scans, keys_only).await
    }

    async fn scan_rev(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
        keys_only: bool,
    ) -> Result<ScanPage, TransportError> {
        self.multi_scan::<true>(node, zone_id, &[(lo, hi, max)], keys_only).await.remove(0)
    }

    async fn scan_many_rev(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        scans: &[wire::WireScan<'_>],
        keys_only: bool,
    ) -> Vec<Result<ScanPage, TransportError>> {
        self.multi_scan::<true>(node, zone_id, scans, keys_only).await
    }

    fn supports_range_tombstones(&self, node: NodeId) -> Option<bool> {
        self.range_tombstones_supported(node)
    }

    async fn range_delete(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        version: crate::hlc::Version,
        slices: &[wire::WireSlice<'_>],
    ) -> Vec<BatchAck> {
        NetTransport::range_delete(self, node, zone_id, sync, version, slices).await
    }

    async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
        match self
            .rpc(node, 0, &Request::Members, 1)
            .await?
            .response
        {
            Response::Members {
                epoch,
                vnodes,
                rf,
                members,
                addrs,
            } => Ok(RingView {
                epoch,
                vnodes,
                rf,
                members,
                addrs,
            }),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected members reply".into())),
        }
    }

    async fn zones(&self, node: NodeId) -> Result<Catalog, TransportError> {
        match self
            .rpc(node, 0, &Request::Zones, 1)
            .await?
            .response
        {
            Response::Zones { zones, .. } => Ok(catalog_from_wire(zones)),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected zones reply".into())),
        }
    }

    async fn range_maps(&self, node: NodeId) -> Result<Vec<(ZoneId, RangeMap)>, TransportError> {
        match self
            .rpc(node, 0, &Request::RangeMaps, 1)
            .await?
            .response
        {
            Response::RangeMaps { maps, .. } => maps
                .into_iter()
                .map(|m| {
                    let map = if m.opaque_tokens {
                        RangeMap::from_opaque_parts(
                            m.splits, m.tokens, m.pending, m.placement_debt
                        )
                    } else {
                        RangeMap::from_splits_tokens_and_pending(m.splits, m.tokens, m.pending)
                    }.map_err(|_| TransportError::Remote("invalid range map".into()))?;
                    Ok((m.zone_id, map))
                })
                .collect(),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected range maps reply".into())),
        }
    }

    async fn apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> Vec<BatchAck> {
        if self.conn(node, 0).is_err() {
            return vec![BatchAck::Unreachable; writes.len()];
        }
        let by_shard =
            self.group_by_data_shard(node, zone_id, writes.iter().map(|entry| entry.key));
        let shard_ids: Vec<usize> = by_shard.keys().copied().collect();
        let futs = shard_ids
            .iter()
            .map(|&shard| {
                let wire_writes: Vec<wire::WireWrite> = by_shard[&shard]
                    .iter()
                    .map(|&i| (writes[i].key, writes[i].value, writes[i].version))
                    .collect();
                async move {
                    let reply = self
                        .rpc(
                            node,
                            shard,
                            &Request::BatchApply {
                                epoch: self.route_epoch.get(),
                                zone_id,
                                sync,
                                writes: &wire_writes,
                            },
                            wire_writes.len(),
                        )
                        .await;
                    (shard, reply)
                }
            })
            .collect();
        let replies = vals_rt::join_all(futs).await;

        let mut out: Vec<BatchAck> = vec![BatchAck::Unreachable; writes.len()];
        for (shard, reply) in replies {
            let indices = &by_shard[&shard];
            let reply = reply.map(|reply| reply.response);
            match reply {
                Ok(Response::Batch(results)) => {
                    for (j, &i) in indices.iter().enumerate() {
                        if let Some(&a) = results.get(j) {
                            out[i] = BatchAck::Applied(a);
                        } else {
                            out[i] = BatchAck::Remote;
                        }
                    }
                }
                Ok(Response::StaleEpoch(e)) => {
                    for &i in indices {
                        out[i] = BatchAck::Stale(e);
                    }
                }
                Ok(Response::Busy) => {
                    for &i in indices {
                        out[i] = BatchAck::Busy;
                    }
                }
                Err(TransportError::Timeout) => {
                    for &i in indices {
                        out[i] = BatchAck::Timeout;
                    }
                }
                Err(TransportError::Unavailable) => {}
                Err(TransportError::Overloaded) => {
                    for &i in indices {
                        out[i] = BatchAck::Overloaded;
                    }
                }
                Err(_) | Ok(_) => {
                    for &i in indices {
                        out[i] = BatchAck::Remote;
                    }
                }
            }
        }
        out
    }

    async fn read_mixed(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        reads: &[MixedReadEntry<'_>],
    ) -> Vec<Result<MixedReadReply, TransportError>> {
        if reads.is_empty() {
            return Vec::new();
        }
        if self.conn(node, 0).is_err() {
            return (0..reads.len())
                .map(|_| Err(TransportError::Unavailable))
                .collect();
        }
        let output_bytes = reads
            .len()
            .saturating_mul(std::mem::size_of::<Result<MixedReadReply, TransportError>>());
        let _output_credit = match self.acquire_transport(node, 0, output_bytes).await {
            Ok(credit) => credit,
            Err(_) => {
                return (0..reads.len())
                    .map(|_| Err(TransportError::Overloaded))
                    .collect()
            }
        };
        let by_shard =
            self.group_by_data_shard(node, zone_id, reads.iter().map(|entry| entry.key));
        let futs = by_shard.into_iter().map(|(shard, indices)| async move {
            let mut shard_out = Vec::with_capacity(indices.len());
            let mut response_guards = Vec::new();
            for chunk in indices.chunks(wire::MAX_MIXED_BATCH_READ_ENTRIES) {
                let mut chunk = chunk;
                if self.mixed_batch_reads
                    && self.mixed_batch_read_support(node, shard).is_none()
                {
                    let i = chunk[0];
                    self.mixed_scalar_probes
                        .set(self.mixed_scalar_probes.get() + 1);
                    let result = match reads[i].role {
                        MixedReadRole::Value => self
                            .read_accounted(node, zone_id, reads[i].key)
                            .await
                            .map(|(value, credit)| {
                                response_guards.extend(credit);
                                MixedReadReply::Value(value)
                            }),
                        MixedReadRole::Digest => self
                            .digest(node, zone_id, reads[i].key)
                            .await
                            .map(MixedReadReply::Digest),
                    };
                    shard_out.push((i, result));
                    chunk = &chunk[1..];
                    if chunk.is_empty() {
                        continue;
                    }
                }
                let scalar = !self.mixed_batch_reads
                    || self.mixed_batch_read_support(node, shard) == Some(false);
                let mut batch_failed = false;
                let mut chunk_out = Vec::with_capacity(chunk.len());
                if !scalar {
                    let wire_reads: Vec<(&[u8], MixedReadRole)> = chunk
                        .iter()
                        .map(|&i| (reads[i].key, reads[i].role))
                        .collect();
                    let mut start = 0usize;
                    while start < wire_reads.len() {
                        self.mixed_batch_rpcs.set(self.mixed_batch_rpcs.get() + 1);
                        let request = Request::MixedBatchRead {
                            epoch: self.route_epoch.get(),
                            zone_id,
                            start: start as u32,
                            reads: &wire_reads,
                        };
                        let reply = if start == 0 {
                            self.rpc(node, shard, &request, wire_reads.len()).await
                        } else {
                            self.rpc_nowait(
                                node,
                                shard,
                                &request,
                                wire_reads.len() - start,
                            )
                            .await
                        };
                        let reply = reply.map(|accounted| {
                            response_guards.extend(accounted.response_credit);
                            accounted.response
                        });
                        match reply {
                            Ok(Response::MixedBatchRead {
                                start: reply_start,
                                results,
                                more,
                            }) if reply_start as usize == start
                                && !results.is_empty()
                                && results.len() <= wire_reads.len() - start =>
                            {
                                let page_start = start;
                                for (j, result) in results.into_iter().enumerate() {
                                    chunk_out.push(match result {
                                        wire::MixedReadResult::Reply(reply)
                                            if matches!(
                                                (wire_reads[page_start + j].1, &reply),
                                                (MixedReadRole::Value, MixedReadReply::Value(_))
                                                    | (MixedReadRole::Digest, MixedReadReply::Digest(_))
                                            ) => Ok(reply),
                                        wire::MixedReadResult::Reply(_) => {
                                            let (error, credit) = transport::accounted_remote_error(
                                                "mixed-read reply role mismatch",
                                                node,
                                                self.transport_credits.as_ref(),
                                            );
                                            response_guards.extend(credit);
                                            Err(error)
                                        }
                                        wire::MixedReadResult::Busy => Err(TransportError::Busy),
                                        wire::MixedReadResult::Err(msg) => {
                                            Err(TransportError::Remote(msg))
                                        }
                                    });
                                }
                                start = chunk_out.len();
                                if !more {
                                    if start != wire_reads.len() {
                                        batch_failed = true;
                                    }
                                    break;
                                }
                            }
                            Ok(Response::StaleEpoch(epoch)) => {
                                while chunk_out.len() < chunk.len() {
                                    chunk_out.push(Err(TransportError::StaleEpoch(epoch)));
                                }
                                break;
                            }
                            Ok(Response::Busy) => {
                                while chunk_out.len() < chunk.len() {
                                    chunk_out.push(Err(TransportError::Busy));
                                }
                                break;
                            }
                            Err(TransportError::Overloaded) => {
                                while chunk_out.len() < chunk.len() {
                                    chunk_out.push(Err(TransportError::Overloaded));
                                }
                                break;
                            }
                            _ => {
                                batch_failed = true;
                                break;
                            }
                        }
                    }
                }
                if scalar || batch_failed {
                    chunk_out.clear();
                    for &i in chunk {
                        self.mixed_scalar_probes
                            .set(self.mixed_scalar_probes.get() + 1);
                        let result = match reads[i].role {
                            MixedReadRole::Value => self
                                .read_accounted(node, zone_id, reads[i].key)
                                .await
                                .map(|(value, credit)| {
                                    response_guards.extend(credit);
                                    MixedReadReply::Value(value)
                                }),
                            MixedReadRole::Digest => self
                                .digest(node, zone_id, reads[i].key)
                                .await
                                .map(MixedReadReply::Digest),
                        };
                        chunk_out.push(result);
                    }
                }
                if chunk_out.len() < chunk.len() {
                    chunk_out.resize_with(chunk.len(), || {
                        let (error, credit) = transport::accounted_remote_error(
                            "short mixed-read reply",
                            node,
                            self.transport_credits.as_ref(),
                        );
                        response_guards.extend(credit);
                        Err(error)
                    });
                }
                shard_out.extend(chunk.iter().copied().zip(chunk_out));
            }
            (shard_out, response_guards)
        });
        let replies = vals_rt::join_all(futs.collect()).await;
        let mut out: Vec<Option<Result<MixedReadReply, TransportError>>> =
            (0..reads.len()).map(|_| None).collect();
        let mut response_guards = Vec::new();
        for (shard, guards) in replies {
            response_guards.extend(guards);
            for (i, result) in shard {
                out[i] = Some(result);
            }
        }
        out.into_iter()
            .map(|result| {
                result.unwrap_or_else(|| Err(TransportError::Unavailable))
            })
            .collect()
    }

    async fn read_values(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> Vec<Result<Option<Stored>, TransportError>> {
        if keys.is_empty() {
            return Vec::new();
        }
        if self.conn(node, 0).is_err() {
            return (0..keys.len())
                .map(|_| Err(TransportError::Unavailable))
                .collect();
        }
        let output_bytes = keys
            .len()
            .saturating_mul(std::mem::size_of::<Result<Option<Stored>, TransportError>>());
        let _output_credit = match self.acquire_transport(node, 0, output_bytes).await {
            Ok(credit) => credit,
            Err(_) => {
                return (0..keys.len())
                    .map(|_| Err(TransportError::Overloaded))
                    .collect()
            }
        };
        let mut by_shard: HashMap<usize, Vec<usize>> = HashMap::new();
        for (i, k) in keys.iter().enumerate() {
            by_shard
                .entry(self.data_shard(node, zone_id, k))
                .or_default()
                .push(i);
        }
        let shard_ids: Vec<usize> = by_shard.keys().copied().collect();
        let futs: Vec<_> = shard_ids
            .iter()
            .map(|&shard| {
                let key_refs: Vec<&[u8]> = by_shard[&shard]
                    .iter()
                    .map(|&i| keys[i].as_slice())
                    .collect();
                async move {
                    let reply = self
                        .rpc(
                            node,
                            shard,
                            &Request::SyncValues {
                                zone_id,
                                keys: &key_refs,
                            },
                            key_refs.len(),
                        )
                        .await;
                    (shard, reply)
                }
            })
            .collect();
        let replies = vals_rt::join_all(futs).await;

        let mut out: Vec<Result<Option<Stored>, TransportError>> =
            (0..keys.len()).map(|_| Ok(None)).collect();
        let mut response_guards = Vec::new();
        for (shard, reply) in replies {
            let idxs = &by_shard[&shard];
            let reply = reply.map(|accounted| {
                response_guards.extend(accounted.response_credit);
                accounted.response
            });
            match reply {
                Ok(Response::Values { entries, more }) => {
                    let map: HashMap<Vec<u8>, Stored> = entries.into_iter().collect();
                    for &i in idxs {
                        out[i] = match map.get(keys[i].as_slice()) {
                            Some(s) => {
                                let bytes = crate::coord::stored_response_bytes(s);
                                let credit = self.transport_credits.as_ref().and_then(|pool| {
                                    try_transport_credit(pool, node, 0, bytes)
                                });
                                if bytes > 0 && self.transport_credits.is_some() && credit.is_none() {
                                    Err(TransportError::Overloaded)
                                } else {
                                    response_guards.extend(credit);
                                    Ok(Some(s.clone()))
                                }
                            }
                            None if more => {
                                let (error, credit) = transport::accounted_remote_error(
                                    "values page truncated",
                                    node,
                                    self.transport_credits.as_ref(),
                                );
                                response_guards.extend(credit);
                                Err(error)
                            }
                            None => Ok(None),
                        };
                    }
                }
                Ok(Response::Err(e)) => {
                    let remote = TransportError::Remote(e);
                    for &i in idxs {
                        out[i] = match transport::clone_accounted_error(
                            &remote,
                            node,
                            self.transport_credits.as_ref(),
                        ) {
                            Ok((error, credit)) => {
                                response_guards.extend(credit);
                                Err(error)
                            }
                            Err(error) => Err(error),
                        };
                    }
                }
                Ok(_) => {
                    for &i in idxs {
                        let (error, credit) = transport::accounted_remote_error(
                            "expected values reply",
                            node,
                            self.transport_credits.as_ref(),
                        );
                        response_guards.extend(credit);
                        out[i] = Err(error);
                    }
                }
                Err(e) => {
                    for &i in idxs {
                        out[i] = match transport::clone_accounted_error(
                            &e,
                            node,
                            self.transport_credits.as_ref(),
                        ) {
                            Ok((error, credit)) => {
                                response_guards.extend(credit);
                                Err(error)
                            }
                            Err(error) => Err(error),
                        };
                    }
                }
            }
        }
        out
    }
}

impl NetTransport {
    pub async fn range_delete(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        version: crate::hlc::Version,
        slices: &[wire::WireSlice<'_>],
    ) -> Vec<BatchAck> {
        if self.conn(node, 0).is_err() {
            return vec![BatchAck::Unreachable; slices.len()];
        }
        let by_shard = self.group_by_data_shard(
            node,
            zone_id,
            slices.iter().map(|(lo, _)| lo.unwrap_or_default()),
        );
        let futs = by_shard
            .iter()
            .flat_map(|(&shard, indices)| {
                indices.chunks(wire::MAX_RANGE_DELETE_SLICES).map(move |chunk| (shard, chunk))
            })
            .map(|(shard, chunk)| {
                let wire_slices: Vec<wire::WireSlice> = chunk.iter().map(|&i| slices[i]).collect();
                async move {
                    let reply = self
                        .rpc(
                            node,
                            shard,
                            &Request::RangeDelete {
                                epoch: self.route_epoch.get(),
                                zone_id,
                                sync,
                                version,
                                slices: &wire_slices,
                            },
                            wire_slices.len(),
                        )
                        .await;
                    (chunk, reply.map(|reply| reply.response))
                }
            })
            .collect::<Vec<_>>();
        let mut out = vec![BatchAck::Unreachable; slices.len()];
        for (indices, reply) in vals_rt::join_all(futs).await {
            let ack = match reply {
                Ok(Response::Batch(results)) if results.len() == indices.len() => {
                    for (&i, &a) in indices.iter().zip(&results) {
                        out[i] = BatchAck::Applied(a);
                    }
                    continue;
                }
                Ok(Response::StaleEpoch(e)) => BatchAck::Stale(e),
                Ok(Response::Busy) => BatchAck::Busy,
                Err(TransportError::Timeout) => BatchAck::Timeout,
                Err(TransportError::Unavailable) => BatchAck::Unreachable,
                Err(TransportError::Overloaded) => BatchAck::Overloaded,
                Err(_) | Ok(_) => BatchAck::Remote,
            };
            for &i in indices {
                out[i] = ack;
            }
        }
        out
    }
}

#[doc(hidden)]
pub struct MuxRpc {
    conn: Rc<MuxConn>,
    payload: Option<Vec<u8>>,
    op_credit: Option<Credit>,
    request_credit: Option<Credit>,
    id: u64,
    queued: bool,
    send_only: bool,
    after_idle: bool,
}

impl MuxRpc {
    #[doc(hidden)]
    pub fn new(conn: Rc<MuxConn>, payload: Vec<u8>) -> Self {
        MuxRpc {
            conn,
            payload: Some(payload),
            op_credit: None,
            request_credit: None,
            id: 0,
            queued: false,
            send_only: false,
            after_idle: false,
        }
    }

    #[doc(hidden)]
    pub fn new_send(conn: Rc<MuxConn>, payload: Vec<u8>) -> Self {
        MuxRpc {
            conn,
            payload: Some(payload),
            op_credit: None,
            request_credit: None,
            id: 0,
            queued: false,
            send_only: true,
            after_idle: false,
        }
    }

    fn new_accounted(
        conn: Rc<MuxConn>,
        payload: Vec<u8>,
        op_credit: Credit,
        request_credit: Credit,
        send_only: bool,
    ) -> Self {
        MuxRpc {
            conn,
            payload: Some(payload),
            op_credit: Some(op_credit),
            request_credit: Some(request_credit),
            id: 0,
            queued: false,
            send_only,
            after_idle: false,
        }
    }
}

impl Future for MuxRpc {
    type Output = Result<AccountedBytes, TransportError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut st = this.conn.st.borrow_mut();
        let mut wake: Vec<Waker> = Vec::new();

        if this.queued {
            if let Some(outcome) = st.finished.remove(&this.id) {
                return Poll::Ready(outcome);
            }
        }
        let reused = st.sock.is_some();
        if let Err(e) = st.ensure_conn(this.conn.addr) {
            return Poll::Ready(Err(e));
        }
        if !this.queued {
            if reused
                && !st.idle_resumed
                && st.pending.is_empty()
                && st.last_io.is_some_and(|at| at.elapsed() >= IDLE_REUSE_AFTER)
            {
                st.idle_resumed = true;
            }
            this.after_idle = reused && st.idle_resumed;
            this.id = st.next_id;
            st.next_id = st.next_id.wrapping_add(1);
            let payload = this.payload.take().unwrap();
            let Ok(frame) = frame_msg(this.id, &payload) else {
                return Poll::Ready(Err(TransportError::Remote(format!(
                    "request frame too large: {} bytes (max {MAX_FRAME})",
                    8 + payload.len()
                ))));
            };
            st.out.extend_from_slice(&frame);
            st.enqueued_total += frame.len() as u64;
            let deadline = if st.negotiated.is_some() {
                st.rpc_deadline
            } else {
                st.connect_deadline
            };
            let deadline_at = deadline.map(|d| Instant::now() + d);
            if let Some(da) = deadline_at {
                st.next_deadline = Some(st.next_deadline.map_or(da, |n| n.min(da)));
            }
            let send_watermark = st.enqueued_total;
            st.min_send_watermark = Some(
                st.min_send_watermark
                    .map_or(send_watermark, |m| m.min(send_watermark)),
            );
            st.pending.insert(
                this.id,
                Waiter {
                    deadline_at,
                    send_watermark: Some(send_watermark),
                    frame_len: frame.len(),
                    send_only: this.send_only,
                    op_credit: this.op_credit.take(),
                    request_credit: this.request_credit.take(),
                    ..Default::default()
                },
            );
            st.pending_peak = st.pending_peak.max(st.pending.len());
            st.queued_bytes_peak = st.queued_bytes_peak.max(st.out.len() - st.out_pos);
            this.queued = true;
            mark_dirty(&this.conn, &mut st);
        }
        let was_driver = st.driver == Some(this.id);
        if was_driver {
            st.driver = None;
        }

        if was_driver {
            let handshake_pending = st.negotiated.is_none();
            st.drain_reads(&mut wake);
            if handshake_pending
                && st.negotiated.is_some()
                && st.out_pos < st.out.len()
            {
                mark_dirty(&this.conn, &mut st);
            }
        }

        if let Some(dl) = st.next_deadline {
            let now = Instant::now();
            if now >= dl {
                st.fail_expired(now, &mut wake);
            }
        }

        let sent_total = st.sent_total;
        match st.pending.get_mut(&this.id) {
            None => {
                let outcome = st
                    .finished
                    .remove(&this.id)
                    .unwrap_or(Err(TransportError::Unavailable));
                drop(st);
                for w in wake {
                    w.wake();
                }
                return Poll::Ready(outcome);
            }
            Some(w) => {
                if let Some(res) = w.result.take() {
                    let request_sent = res.is_ok()
                        || w
                            .send_watermark
                            .is_some_and(|watermark| sent_total >= watermark);
                    if !request_sent {
                        if let Some(waiter) = st.take_wholly_unsent(this.id) {
                            drop(waiter);
                        } else {
                            st.reset_failed_as(&mut wake, TransportError::Overloaded);
                            st.finished.remove(&this.id);
                        }
                        if st.driver == Some(this.id) {
                            st.driver = None;
                        }
                        if st.driver.is_none() {
                            if let Some(wk) = st.pending.values_mut().find_map(|x| x.waker.take()) {
                                wake.push(wk);
                            }
                        }
                        drop(st);
                        for w in wake {
                            w.wake();
                        }
                        return Poll::Ready(res.map(|bytes| AccountedBytes {
                            bytes,
                            _credits: Vec::new(),
                        }));
                    }
                    let credits = std::mem::take(&mut w.response_credits);
                    st.pending.remove(&this.id);
                    if st.driver.is_none() {
                        if let Some(wk) = st.pending.values_mut().find_map(|x| x.waker.take()) {
                            wake.push(wk);
                        }
                    }
                    drop(st);
                    for w in wake {
                        w.wake();
                    }
                    return Poll::Ready(res.map(|bytes| AccountedBytes {
                        bytes,
                        _credits: credits,
                    }));
                }
            }
        }

        st.pending.get_mut(&this.id).unwrap().waker = Some(cx.waker().clone());
        if st.driver.is_none() && st.sock.is_some() {
            let interest = if st.has_sendable_bytes() {
                Interest::ReadWrite
            } else {
                Interest::Read
            };
            reactor::register(st.fd, interest, cx.waker().clone());
            st.driver = Some(this.id);
        }
        drop(st);
        for w in wake {
            w.wake();
        }
        Poll::Pending
    }
}


#[cfg(test)]
mod tests;

impl Drop for MuxRpc {
    fn drop(&mut self) {
        if !self.queued {
            return;
        }
        let mut st = self.conn.st.borrow_mut();
        if !st.pending.contains_key(&self.id) {
            st.finished.remove(&self.id);
            return;
        }
        let sent = st
            .pending
            .get(&self.id)
            .and_then(|w| w.send_watermark)
            .is_some_and(|watermark| st.sent_total >= watermark);
        let mut wake = Vec::new();
        if sent {
            st.pending.remove(&self.id);
        } else if let Some(waiter) = st.take_wholly_unsent(self.id) {
            drop(waiter);
        } else {
            st.reset_failed_as(&mut wake, TransportError::Overloaded);
            st.finished.remove(&self.id);
        }
        if st.driver == Some(self.id) {
            st.driver = None;
        }
        if st.driver.is_none() {
            if let Some(wk) = st.pending.values_mut().find_map(|x| x.waker.take()) {
                wake.push(wk);
            }
        }
        drop(st);
        for waker in wake {
            waker.wake();
        }
    }
}
