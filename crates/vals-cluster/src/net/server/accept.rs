use super::*;

pub struct ReplicaServer;

pub struct ServerHandle {
    shards: Vec<ServerShardHandle>,
}

struct ServerShardHandle {
    stop: Arc<reactor::Notify>,
    join: std::thread::JoinHandle<()>,
}

impl ServerHandle {
    pub fn shutdown(self) {
        for s in &self.shards {
            s.stop.signal();
        }
        for s in self.shards {
            let _ = s.join.join();
        }
    }
}

impl ReplicaServer {
    pub fn spawn<D>(store: VersionedStore<D>) -> io::Result<SocketAddr>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let (addr, _) = Self::serve(move || ServerShared::single_zone(store), false)?;
        Ok(addr)
    }

    pub fn spawn_with_handle<D>(store: VersionedStore<D>) -> io::Result<(SocketAddr, ServerHandle)>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let (addr, handle) = Self::serve(move || ServerShared::single_zone(store), true)?;
        Ok((addr, handle.expect("handle requested")))
    }

    #[cfg(test)]
    pub(crate) fn spawn_with_accept_faults<D>(
        store: VersionedStore<D>,
        accept_faults: u32,
    ) -> io::Result<SocketAddr>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let (addr, _) = Self::serve(
            move || {
                let shared = ServerShared::single_zone(store);
                shared.arm_accept_faults(accept_faults);
                shared
            },
            false,
        )?;
        Ok(addr)
    }

    pub fn spawn_zoned<D, Z>(
        node_id: NodeId,
        zone0_store: VersionedStore<D>,
        provider: Z,
        catalog: Catalog,
    ) -> io::Result<SocketAddr>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        Z: ZoneStore<D> + Send + 'static,
    {
        let (addr, _) = Self::serve(
            move || {
                ServerShared::zoned(
                    node_id,
                    zone0_store,
                    Rc::new(provider),
                    catalog,
                    Ring::new(1),
                )
            },
            false,
        )?;
        Ok(addr)
    }

    pub fn spawn_zoned_with_handle<D, Z>(
        node_id: NodeId,
        zone0_store: VersionedStore<D>,
        provider: Z,
        catalog: Catalog,
    ) -> io::Result<(SocketAddr, ServerHandle)>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
        Z: ZoneStore<D> + Send + 'static,
    {
        let (addr, handle) = Self::serve(
            move || {
                ServerShared::zoned(
                    node_id,
                    zone0_store,
                    Rc::new(provider),
                    catalog,
                    Ring::new(1),
                )
            },
            true,
        )?;
        Ok((addr, handle.expect("handle requested")))
    }

    fn serve<D>(
        build: impl FnOnce() -> ServerShared<D> + Send + 'static,
        with_handle: bool,
    ) -> io::Result<(SocketAddr, Option<ServerHandle>)>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        if !with_handle {
            crate::observe::spawn_abort_on_panic("vals-server", move || {
                let shared = Rc::new(build());
                let ex = LocalExecutor::new();
                ex.spawn(accept_loop(
                    super::Listener::Tcp(listener),
                    shared.clone(),
                    ex.spawner(),
                ));
                ex.run(move || server_poll(&shared));
            });
            return Ok((addr, None));
        }
        let stop = Arc::new(reactor::Notify::new());
        let stop_rx = stop.clone();
        let join = crate::observe::spawn_abort_on_panic("vals-server", move || {
            let shared = Rc::new(build());
            let mut ex = LocalExecutor::new();
            ex.spawn(accept_loop(
                super::Listener::Tcp(listener),
                shared.clone(),
                ex.spawner(),
            ));
            let (_, canceled) = ex.run_until_and_cancel(
                crate::node::shutdown_loop(stop_rx, shared.clone(), Some(addr)),
                {
                    let shared = shared.clone();
                    move || server_poll(&shared)
                },
            );
            log::info!(
                "node {}: connection drain complete; canceled {canceled} unfinished task(s), \
                 syncing engines",
                shared.node_id
            );
            shared.finalize_shutdown();
        });
        Ok((
            addr,
            Some(ServerHandle {
                shards: vec![ServerShardHandle { stop, join }],
            }),
        ))
    }
}

pub(crate) fn server_poll<D: BlockDevice>(shared: &ServerShared<D>) {
    shared.complete_pending_drops();
    shared.wake_expired_conns();
    let flushing = !shared.domain_flushes.borrow().is_empty();
    if shared.read_gate.inflight() > 0
        || shared.device_waits.get() > 0
        || flushing
        || vals_spdk::async_io_pending() > 0
    {
        thread_local! {
            static POLL_SAMPLE: std::cell::Cell<u16> = const { std::cell::Cell::new(0) };
        }
        let sample = POLL_SAMPLE.with(|n| {
            let next = n.get().wrapping_add(1);
            n.set(next);
            next & 255 == 0
        });
        let started = sample.then(std::time::Instant::now);
        shared.poll_devices();
        if flushing {
            shared.publish_domain_flushes();
        }
        reactor::poll_io_nonblocking();
        if let Some(started) = started {
            let counters = crate::observe::counters();
            counters.server_poll_busy_iterations.add(256);
            counters.server_poll_busy_us_total.add(started.elapsed().as_nanos() as u64 * 256 / 1000);
        }
    } else {
        crate::observe::counters().server_poll_blocking_iterations.inc();
        reactor::poll_io();
    }
}

pub struct ShardedServer;

impl ShardedServer {
    pub fn spawn<D>(shards: Vec<VersionedStore<D>>) -> io::Result<Vec<SocketAddr>>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        shards.into_iter().map(ReplicaServer::spawn).collect()
    }

    pub fn spawn_with_handle<D>(
        shards: Vec<VersionedStore<D>>,
    ) -> io::Result<(Vec<SocketAddr>, ServerHandle)>
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        let mut addrs = Vec::with_capacity(shards.len());
        let mut shard_handles = Vec::with_capacity(shards.len());
        for store in shards {
            let (addr, handle) = ReplicaServer::spawn_with_handle(store)?;
            addrs.push(addr);
            shard_handles.extend(handle.shards);
        }
        Ok((
            addrs,
            ServerHandle {
                shards: shard_handles,
            },
        ))
    }
}

pub(crate) async fn accept_loop<D: BlockDevice + AsyncRead + 'static>(
    listener: super::Listener,
    shared: Rc<ServerShared<D>>,
    spawner: vals_rt::Spawner,
) {
    let fd = listener.as_raw_fd();
    let mut backoff = AcceptBackoff::new();
    loop {
        let accepted = (Accept {
            listener: &listener,
            fd,
        })
        .await;
        if shared.is_shutdown() {
            reactor::deregister(fd);
            return;
        }
        #[cfg(test)]
        let accepted = shared.maybe_inject_accept_error(accepted);
        match accepted {
            Ok(stream) => {
                backoff.reset();
                let Some(connection_credit) = crate::transport::acquire_connection() else {
                    crate::observe::counters().transport_connection_rejections.inc();
                    continue;
                };
                spawner.spawn(handle_conn(
                    stream,
                    shared.clone(),
                    spawner.clone(),
                    connection_credit,
                ));
            }
            Err(e) if accept_error_is_transient(&e) => {
                crate::observe::counters().accept_transient_errors.inc();
                backoff.after_transient_error(fd, &e);
            }
            Err(e) => {
                log::error!(
                    "accept loop (fd {fd}) exiting on a fatal accept error: {e} (kind {:?})",
                    e.kind()
                );
                reactor::deregister(fd);
                return;
            }
        }
    }
}

fn accept_error_is_transient(e: &io::Error) -> bool {
    if matches!(
        e.kind(),
        ErrorKind::ConnectionAborted
            | ErrorKind::Interrupted
            | ErrorKind::WouldBlock
            | ErrorKind::OutOfMemory
    ) {
        return true;
    }
    matches!(
        e.raw_os_error(),
        Some(libc::EMFILE)
            | Some(libc::ENFILE)
            | Some(libc::ENOMEM)
            | Some(libc::ENOBUFS)
            | Some(libc::ECONNABORTED)
            | Some(libc::EINTR)
    )
}

struct AcceptBackoff {
    streak: u32,
}

impl AcceptBackoff {
    const MIN_MS: u64 = 1;
    const MAX_MS: u64 = 25;
    const LOG_EVERY: u32 = 256;

    fn new() -> Self {
        AcceptBackoff { streak: 0 }
    }

    fn reset(&mut self) {
        self.streak = 0;
    }

    fn after_transient_error(&mut self, fd: RawFd, e: &io::Error) {
        self.streak = self.streak.saturating_add(1);
        if self.streak == 1 || self.streak.is_multiple_of(Self::LOG_EVERY) {
            log::warn!(
                "accept loop (fd {fd}) hit a transient accept error (retrying, streak {}): {e} (kind {:?})",
                self.streak,
                e.kind()
            );
        }
        let shift = (self.streak - 1).min(20);
        let ms = (Self::MIN_MS << shift).min(Self::MAX_MS);
        std::thread::sleep(Duration::from_millis(ms));
    }
}

pub(crate) struct Accept<'a> {
    listener: &'a super::Listener,
    fd: RawFd,
}

impl Future for Accept<'_> {
    type Output = io::Result<super::Sock>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.listener.accept_sock() {
            Ok(stream) => Poll::Ready(Ok(stream)),
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                reactor::register(self.fd, Interest::Read, cx.waker().clone());
                Poll::Pending
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum DeadlineKind {
    Initial,
    Frame,
    Idle,
    Write,
}

pub(crate) struct ServerOut {
    pub(crate) buf: RefCell<Vec<u8>>,
    pub(super) pos: Cell<usize>,
    pub(super) waker: RefCell<Option<Waker>>,
    pub(crate) reads_inflight: Cell<usize>,
    queued_bytes: Cell<usize>,
    input_bytes: Cell<usize>,
    closed: Cell<bool>,
    negotiated: Cell<bool>,
    created: std::time::Instant,
    last_io: Cell<std::time::Instant>,
    partial_since: Cell<Option<std::time::Instant>>,
    write_since: Cell<Option<std::time::Instant>>,
    dispatch_credit: RefCell<Option<crate::transport::Credit>>,
    dispatch_bytes: RefCell<Option<crate::transport::ByteCredit>>,
}

impl Default for ServerOut {
    fn default() -> Self {
        let now = std::time::Instant::now();
        ServerOut {
            buf: RefCell::new(Vec::new()),
            pos: Cell::new(0),
            waker: RefCell::new(None),
            reads_inflight: Cell::new(0),
            queued_bytes: Cell::new(0),
            input_bytes: Cell::new(0),
            closed: Cell::new(false),
            negotiated: Cell::new(false),
            created: now,
            last_io: Cell::new(now),
            partial_since: Cell::new(None),
            write_since: Cell::new(None),
            dispatch_credit: RefCell::new(None),
            dispatch_bytes: RefCell::new(None),
        }
    }
}

impl Drop for ServerOut {
    fn drop(&mut self) {
        crate::transport::release_queued_bytes(self.queued_bytes.get() + self.input_bytes.get());
    }
}

impl ServerOut {
    fn enqueue(&self, frame: Vec<u8>) {
        if self.closed.get() {
            return;
        }
        let mut buf = self.buf.borrow_mut();
        let reserved = frame.len();
        if !crate::transport::reserve_queued_bytes(reserved) {
            crate::observe::counters().transport_queued_byte_rejections.inc();
            self.closed.set(true);
        } else {
            let old_capacity = buf.capacity();
            if buf.try_reserve_exact(frame.len()).is_err() {
                crate::transport::release_queued_bytes(reserved);
                crate::observe::counters().transport_queued_byte_rejections.inc();
                self.closed.set(true);
            } else {
                let actual = buf.capacity() - old_capacity;
                if actual > reserved
                    && !crate::transport::reserve_queued_bytes(actual - reserved)
                {
                    *buf = Vec::new();
                    crate::transport::release_queued_bytes(old_capacity + reserved);
                    self.queued_bytes.set(0);
                    crate::observe::counters().transport_queued_byte_rejections.inc();
                    self.closed.set(true);
                } else {
                    if actual < reserved {
                        crate::transport::release_queued_bytes(reserved - actual);
                    }
                    let was_empty = buf.is_empty();
                    self.queued_bytes.set(buf.capacity());
                    buf.extend_from_slice(&frame);
                    if was_empty {
                        self.write_since.set(Some(std::time::Instant::now()));
                    }
                }
            }
        }
        drop(buf);
        if let Some(w) = self.waker.borrow_mut().take() {
            w.wake();
        }
    }

    pub(super) fn push(&self, reqid: u64, payload: &[u8]) {
        let mut frame = Vec::with_capacity(12 + payload.len().min(MAX_FRAME));
        if write_framed(&mut frame, reqid, payload).is_err() {
            let err = wire::encode_response_payload(&Response::Err(format!(
                "reply too large: {} bytes (frame max {MAX_FRAME})",
                payload.len()
            )));
            write_framed(&mut frame, reqid, &err).expect("error reply is tiny");
        }
        self.enqueue(frame);
    }

    fn push_framed(&self, reqid: u64, encode: impl FnOnce(&mut Vec<u8>)) {
        let mut frame = Vec::new();
        frame.extend_from_slice(&[0u8; 4]);
        frame.extend_from_slice(&reqid.to_le_bytes());
        encode(&mut frame);
        let framed = frame.len() - 4;
        if framed > MAX_FRAME {
            frame.clear();
            let err = wire::encode_response_payload(&Response::Err(format!(
                "reply too large: {} bytes (frame max {MAX_FRAME})",
                framed - 8
            )));
            write_framed(&mut frame, reqid, &err).expect("error reply is tiny");
        } else {
            frame[..4].copy_from_slice(&(framed as u32).to_le_bytes());
        }
        self.enqueue(frame);
    }

    pub(crate) fn push_resp(&self, reqid: u64, resp: &Response) {
        self.push_framed(reqid, |b| wire::encode_response_into(b, resp));
    }

    pub(crate) fn push_read_reply(
        &self,
        reqid: u64,
        entry: Option<(crate::hlc::Version, Option<&[u8]>)>,
    ) {
        self.push_framed(reqid, |b| wire::put_read_payload(b, entry));
    }

    pub(super) fn push_raw(&self, frame: &[u8]) {
        self.enqueue(frame.to_vec());
    }

    pub(super) fn reserve_input(&self, bytes: usize) -> bool {
        if !crate::transport::reserve_queued_bytes(bytes) {
            crate::observe::counters().transport_queued_byte_rejections.inc();
            self.closed.set(true);
            return false;
        }
        self.input_bytes.set(self.input_bytes.get() + bytes);
        true
    }

    pub(super) fn release_input_bytes(&self, bytes: usize) {
        let released = bytes.min(self.input_bytes.get());
        crate::transport::release_queued_bytes(released);
        self.input_bytes.set(self.input_bytes.get() - released);
    }

    pub(super) fn release_input(&self) {
        self.release_input_bytes(self.input_bytes.get());
    }

    pub(super) fn begin_dispatch(
        &self,
        credit: crate::transport::Credit,
        bytes: crate::transport::ByteCredit,
    ) {
        debug_assert!(self.dispatch_credit.borrow().is_none());
        *self.dispatch_credit.borrow_mut() = Some(credit);
        *self.dispatch_bytes.borrow_mut() = Some(bytes);
    }

    pub(super) fn take_dispatch_credits(
        &self,
    ) -> (crate::transport::Credit, crate::transport::ByteCredit) {
        (
            self.dispatch_credit
                .borrow_mut()
                .take()
                .expect("request dispatch credit"),
            self.dispatch_bytes
                .borrow_mut()
                .take()
                .expect("request dispatch byte credit"),
        )
    }

    pub(super) fn finish_dispatch(&self) {
        self.dispatch_credit.borrow_mut().take();
        self.dispatch_bytes.borrow_mut().take();
    }

    pub(super) fn note_read(&self, partial: bool) {
        let now = std::time::Instant::now();
        self.last_io.set(now);
        if partial {
            if self.partial_since.get().is_none() {
                self.partial_since.set(Some(now));
            }
        } else {
            self.partial_since.set(None);
        }
    }

    pub(super) fn note_negotiated(&self) {
        self.negotiated.set(true);
        self.partial_since.set(None);
    }

    pub(super) fn note_written(&self, pending: bool) {
        let now = std::time::Instant::now();
        self.last_io.set(now);
        self.write_since.set(pending.then_some(now));
    }

    pub(super) fn release_output(&self, bytes: usize) {
        let released = bytes.min(self.queued_bytes.get());
        crate::transport::release_queued_bytes(released);
        self.queued_bytes.set(self.queued_bytes.get() - released);
    }

    pub(super) fn deadline_expired(&self, now: std::time::Instant) -> Option<DeadlineKind> {
        let limits = crate::transport::limits();
        if !self.negotiated.get()
            && limits.initial_timeout.is_some_and(|d| now.duration_since(self.created) >= d)
        {
            return Some(DeadlineKind::Initial);
        }
        if let Some(since) = self.partial_since.get() {
            if limits.frame_timeout.is_some_and(|d| now.duration_since(since) >= d) {
                return Some(DeadlineKind::Frame);
            }
        }
        if let Some(since) = self.write_since.get() {
            if limits.write_timeout.is_some_and(|d| now.duration_since(since) >= d) {
                return Some(DeadlineKind::Write);
            }
        }
        let dispatching = self.dispatch_credit.borrow().is_some() || self.reads_inflight.get() > 0;
        if !dispatching
            && limits
                .idle_timeout
                .is_some_and(|d| now.duration_since(self.last_io.get()) >= d)
        {
            return Some(DeadlineKind::Idle);
        }
        None
    }

    pub(super) fn close(&self) {
        self.closed.set(true);
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed.get()
    }
}

async fn handle_conn<D: BlockDevice + AsyncRead + 'static>(
    stream: super::Sock,
    shared: Rc<ServerShared<D>>,
    spawner: vals_rt::Spawner,
    connection_credit: crate::transport::Credit,
) {
    let fd = stream.as_raw_fd();
    let out = Rc::new(ServerOut::default());
    shared.register_conn(&out);
    ConnDriver {
        stream: Rc::new(stream),
        fd,
        reader: FrameRead::default(),
        out,
        shared,
        spawner,
        negotiated: None,
        #[cfg(test)]
        apply_mode_override: None,
    }
    .await;
    reactor::deregister(fd);
    drop(connection_credit);
}

