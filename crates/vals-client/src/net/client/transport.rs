use super::*;

/// Default [`ClientConfig::connect_deadline`].
pub const DEFAULT_CONNECT_DEADLINE: Duration = Duration::from_secs(5);
/// Default [`ClientConfig::rpc_deadline`].
pub const DEFAULT_RPC_DEADLINE: Duration = Duration::from_secs(5);
/// Default global operation budget.
pub const DEFAULT_CLIENT_MAX_OPS: usize = 32_768;
/// Default global byte budget (512 MiB).
pub const DEFAULT_CLIENT_MAX_BYTES: usize = 512 * 1024 * 1024;
/// Default per-node operation budget.
pub const DEFAULT_CLIENT_NODE_MAX_OPS: usize = 8_192;
/// Default per-node byte budget (128 MiB).
pub const DEFAULT_CLIENT_NODE_MAX_BYTES: usize = 128 * 1024 * 1024;

/// Deadlines and request budgets for a [`NetTransport`] and the coordinator
/// built on it.
///
/// The default has finite deadlines and budgets (the `DEFAULT_*`
/// constants). `None` disables a deadline or the budgets; that is meant for
/// benchmarks ([`ClientConfig::benchmark_unbounded`]), since a dead peer can
/// then hold a request, and its memory, forever.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// How long opening a connection and completing its protocol handshake
    /// may take, counted for each request sent while the connection is not
    /// yet ready.
    pub connect_deadline: Option<Duration>,
    /// How long each request may wait for its reply once the connection is
    /// ready. An expired request fails with [`TransportError::Timeout`]; its
    /// effect on the replica is unknown.
    pub rpc_deadline: Option<Duration>,
    /// Limits on outstanding requests and buffered bytes; see
    /// [`ClientBudgets`](crate::ClientBudgets).
    pub budgets: Option<crate::coord::ClientBudgets>,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            connect_deadline: Some(DEFAULT_CONNECT_DEADLINE),
            rpc_deadline: Some(DEFAULT_RPC_DEADLINE),
            budgets: Some(crate::coord::ClientBudgets {
                max_ops: DEFAULT_CLIENT_MAX_OPS,
                max_bytes: DEFAULT_CLIENT_MAX_BYTES,
                node_max_ops: DEFAULT_CLIENT_NODE_MAX_OPS,
                node_max_bytes: DEFAULT_CLIENT_NODE_MAX_BYTES,
            }),
        }
    }
}

impl ClientConfig {
    /// No deadlines and no budgets. For benchmarks only.
    pub fn benchmark_unbounded() -> Self {
        ClientConfig {
            connect_deadline: None,
            rpc_deadline: None,
            budgets: None,
        }
    }

    /// Checks that deadlines and budgets are positive and per-node budgets do
    /// not exceed the global ones. The `connect_*_with_config` constructors
    /// call this.
    pub fn validate(self) -> Result<Self, ClientConfigError> {
        if self.connect_deadline.is_some_and(|deadline| deadline.is_zero()) {
            return Err(ClientConfigError::ZeroConnectDeadline);
        }
        if self.rpc_deadline.is_some_and(|deadline| deadline.is_zero()) {
            return Err(ClientConfigError::ZeroRpcDeadline);
        }
        if let Some(budgets) = self.budgets {
            if budgets.max_ops == 0
                || budgets.max_bytes == 0
                || budgets.node_max_ops == 0
                || budgets.node_max_bytes == 0
            {
                return Err(ClientConfigError::ZeroBudget);
            }
            if budgets.node_max_ops > budgets.max_ops
                || budgets.node_max_bytes > budgets.max_bytes
            {
                return Err(ClientConfigError::NodeBudgetExceedsGlobal);
            }
        }
        Ok(self)
    }
}

/// Why [`ClientConfig::validate`] refused a configuration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClientConfigError {
    /// `connect_deadline` is `Some(0)`.
    ZeroConnectDeadline,
    /// `rpc_deadline` is `Some(0)`.
    ZeroRpcDeadline,
    /// A budget is zero.
    ZeroBudget,
    /// A per-node budget is larger than the matching global budget.
    NodeBudgetExceedsGlobal,
}

impl std::fmt::Display for ClientConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientConfigError::ZeroConnectDeadline => write!(f, "connect deadline must be positive"),
            ClientConfigError::ZeroRpcDeadline => write!(f, "RPC deadline must be positive"),
            ClientConfigError::ZeroBudget => write!(f, "client budgets must be positive"),
            ClientConfigError::NodeBudgetExceedsGlobal => {
                write!(f, "per-node client budgets cannot exceed global budgets")
            }
        }
    }
}

impl std::error::Error for ClientConfigError {}

/// Counters for one connection; see [`NetTransport::connection_stats`].
/// Diagnostic only: the fields may change.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionStats {
    pub node: NodeId,
    pub shard: usize,
    pub pending: usize,
    pub pending_peak: usize,
    pub queued_bytes: usize,
    pub queued_bytes_peak: usize,
    pub enqueued_bytes: u64,
    pub sent_bytes: u64,
    pub write_calls: u64,
    pub read_drains: u64,
    pub reconnects: u64,
}

pub(super) struct AccountedResponse {
    pub response: Response,
    pub response_credit: Option<Credit>,
}

fn stored_owned_bytes(stored: &Stored) -> usize {
    stored.value.as_ref().map_or(0, Vec::capacity)
}

fn entries_owned_bytes(entries: &Vec<(Vec<u8>, Stored)>) -> usize {
    entries
        .capacity()
        .saturating_mul(std::mem::size_of::<(Vec<u8>, Stored)>())
        .saturating_add(entries.iter().fold(0usize, |bytes, (key, stored)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(stored_owned_bytes(stored))
        }))
}

fn transport_error_owned_bytes(error: &TransportError) -> usize {
    match error {
        TransportError::Engine(message) | TransportError::Remote(message) => message.capacity(),
        _ => 0,
    }
}

pub(super) fn clone_accounted_error(
    error: &TransportError,
    node: NodeId,
    pool: Option<&Rc<RefCell<CreditPool>>>,
) -> Result<(TransportError, Option<Credit>), TransportError> {
    let bytes = transport_error_owned_bytes(error);
    let credit = match (pool, bytes) {
        (Some(pool), bytes) if bytes > 0 => Some(
            try_transport_credit(pool, node, 0, bytes).ok_or(TransportError::Overloaded)?,
        ),
        _ => None,
    };
    Ok((error.clone(), credit))
}

pub(super) fn accounted_remote_error(
    message: &'static str,
    node: NodeId,
    pool: Option<&Rc<RefCell<CreditPool>>>,
) -> (TransportError, Option<Credit>) {
    let credit = match pool {
        Some(pool) => match try_transport_credit(pool, node, 0, message.len()) {
            Some(credit) => Some(credit),
            None => return (TransportError::Overloaded, None),
        },
        None => None,
    };
    (TransportError::Remote(message.into()), credit)
}

pub(super) fn response_owned_bytes(response: &Response) -> usize {
    match response {
        Response::Read(Some(stored)) => stored_owned_bytes(stored),
        Response::Scan {
            entries,
            fragments,
            resume,
            ..
        } => entries_owned_bytes(entries)
            .saturating_add(
                fragments
                    .capacity()
                    .saturating_mul(std::mem::size_of::<crate::wire::RtFragment>()),
            )
            .saturating_add(fragments.iter().fold(0usize, |bytes, f| {
                bytes
                    .saturating_add(f.lo.as_ref().map_or(0, Vec::capacity))
                    .saturating_add(f.hi.as_ref().map_or(0, Vec::capacity))
            }))
            .saturating_add(resume.as_ref().map_or(0, Vec::capacity)),
        Response::RangeTombstones { fragments, .. } => fragments
            .capacity()
            .saturating_mul(std::mem::size_of::<crate::wire::RtFragment>())
            .saturating_add(fragments.iter().fold(0usize, |bytes, f| {
                bytes
                    .saturating_add(f.lo.as_ref().map_or(0, Vec::capacity))
                    .saturating_add(f.hi.as_ref().map_or(0, Vec::capacity))
            })),
        Response::Values { entries, .. } => entries_owned_bytes(entries),
        Response::Tree(nodes) => nodes
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, u64)>()),
        Response::BucketDigests {
            digests, resume, ..
        } => digests
            .capacity()
            .saturating_mul(std::mem::size_of::<(Vec<u8>, crate::hlc::Version)>())
            .saturating_add(
                digests
                    .iter()
                    .fold(0usize, |bytes, (key, _)| bytes.saturating_add(key.capacity())),
            )
            .saturating_add(resume.as_ref().map_or(0, Vec::capacity)),
        Response::Members { members, addrs, .. } => members
            .capacity()
            .saturating_mul(std::mem::size_of::<(u32, u32)>())
            .saturating_add(
                addrs
                    .capacity()
                    .saturating_mul(std::mem::size_of::<(u32, Vec<SocketAddr>)>()),
            )
            .saturating_add(addrs.iter().fold(0usize, |bytes, (_, node_addrs)| {
                bytes.saturating_add(
                    node_addrs
                        .capacity()
                        .saturating_mul(std::mem::size_of::<SocketAddr>()),
                )
            })),
        Response::Zones { zones, .. } => zones
            .capacity()
            .saturating_mul(std::mem::size_of::<wire::OwnedZone>())
            .saturating_add(zones.iter().fold(0usize, |bytes, zone| {
                bytes
                    .saturating_add(zone.name.capacity())
                    .saturating_add(zone.scope.as_ref().map_or(0, |scope| {
                        scope
                            .capacity()
                            .saturating_mul(std::mem::size_of::<u32>())
                    }))
            })),
        Response::RangeMaps { maps, .. } => maps
            .capacity()
            .saturating_mul(std::mem::size_of::<wire::OwnedRangeMap>())
            .saturating_add(maps.iter().fold(0usize, |bytes, map| {
                bytes
                    .saturating_add(
                        map.splits
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Vec<u8>>()),
                    )
                    .saturating_add(
                        map.splits
                            .iter()
                            .fold(0usize, |sum, split| sum.saturating_add(split.capacity())),
                    )
                    .saturating_add(
                        map.tokens
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Vec<u8>>()),
                    )
                    .saturating_add(
                        map.tokens
                            .iter()
                            .fold(0usize, |sum, token| sum.saturating_add(token.capacity())),
                    )
                    .saturating_add(
                        map.pending
                            .capacity()
                            .saturating_mul(std::mem::size_of::<Option<Vec<u8>>>()),
                    )
                    .saturating_add(map.pending.iter().fold(0usize, |sum, pending| {
                        sum.saturating_add(pending.as_ref().map_or(0, Vec::capacity))
                    }))
                    .saturating_add(map.placement_debt.capacity().saturating_add(7) / 8)
            })),
        Response::RangeStats { ranges, .. }
        | Response::RangeStatsV2 { ranges, .. }
        | Response::RangeStatsV3 { ranges, .. } => ranges
            .capacity()
            .saturating_mul(std::mem::size_of::<wire::OwnedRangeStat>())
            .saturating_add(ranges.iter().fold(0usize, |bytes, range| {
                bytes
                    .saturating_add(range.lower_bound.capacity())
                    .saturating_add(range.midpoint.as_ref().map_or(0, Vec::capacity))
            })),
        Response::Batch(results) => results
            .capacity()
            .saturating_mul(std::mem::size_of::<Applied>()),
        Response::MixedBatchRead { results, .. } => results
            .capacity()
            .saturating_mul(std::mem::size_of::<wire::MixedReadResult>())
            .saturating_add(results.iter().fold(0usize, |bytes, result| match result {
                wire::MixedReadResult::Reply(MixedReadReply::Value(Some(stored))) => {
                    bytes.saturating_add(stored_owned_bytes(stored))
                }
                wire::MixedReadResult::Err(message) => bytes.saturating_add(message.capacity()),
                _ => bytes,
            })),
        Response::MultiScan { pages, .. } => pages.capacity()
            .saturating_mul(std::mem::size_of::<ScanPage>())
            .saturating_add(pages.iter().map(crate::coord::scan_response_bytes).sum::<usize>()),
        Response::Err(message) => message.capacity(),
        _ => 0,
    }
}

fn is_idempotent_read(request: &Request<'_>) -> bool {
    matches!(
        request,
        Request::Read { .. }
            | Request::Digest { .. }
            | Request::Scan { .. }
            | Request::MultiScan { .. }
            | Request::MixedBatchRead { .. }
            | Request::SyncTree { .. }
            | Request::SyncBucketDigests { .. }
            | Request::SyncValues { .. }
            | Request::Members
            | Request::Zones
            | Request::RangeMaps
            | Request::RangeStats
            | Request::RangeStatsV2
            | Request::RangeStatsV3
            | Request::RangeWriteMark { .. }
            | Request::GetWatermark
    )
}

/// The TCP transport: one multiplexed connection per node address.
///
/// Constructors take the seed nodes as `(node id, address)` pairs; a node
/// that serves several shards on separate listeners is given all its
/// addresses (`connect_sharded*`). Nothing is dialed until the first request
/// to a node, and a failed connection is reopened by a later request. After a
/// ring refresh the transport also learns the addresses of members it was not
/// given. Like the coordinator, it is neither `Send` nor `Sync` and needs
/// [`reactor::poll_io`](crate::reactor::poll_io) on its thread.
///
/// Data requests carry no credentials: anyone who can reach a node's data
/// port can read and write. Run clients and nodes on a trusted network.
pub struct NetTransport {
    conns: RefCell<HashMap<NodeId, Vec<Rc<MuxConn>>>>,
    connect_deadline: Cell<Option<Duration>>,
    rpc_deadline: Cell<Option<Duration>>,
    pub(super) budgets: Option<crate::coord::ClientBudgets>,
    pub(super) transport_credits: Option<Rc<RefCell<CreditPool>>>,
    down: RefCell<HashSet<NodeId>>,
    pub(super) route_epoch: Cell<u64>,
    pub(super) sorted_ranges: RefCell<HashMap<ZoneId, RangeMap>>,
    pub(super) mixed_batch_reads: bool,
    pub(super) mixed_batch_rpcs: Cell<u64>,
    pub(super) mixed_scalar_probes: Cell<u64>,
    rpc_attempts: Cell<u64>,
}

impl NetTransport {
    /// A transport to single-address nodes with the default [`ClientConfig`].
    pub fn connect(addrs: &[(NodeId, SocketAddr)]) -> Self {
        Self::connect_with_config(addrs, ClientConfig::default())
            .expect("valid default client config")
    }

    /// [`connect`](Self::connect) with
    /// [`ClientConfig::benchmark_unbounded`]. For benchmarks only.
    pub fn connect_unbounded(addrs: &[(NodeId, SocketAddr)]) -> Self {
        Self::connect_with_config(addrs, ClientConfig::benchmark_unbounded())
            .expect("valid benchmark client config")
    }

    /// [`connect`](Self::connect) with an explicit, validated configuration.
    pub fn connect_with_config(
        addrs: &[(NodeId, SocketAddr)],
        config: ClientConfig,
    ) -> Result<Self, ClientConfigError> {
        Self::connect_sharded_with_config(addrs.iter().map(|&(n, a)| (n, vec![a])), config)
    }

    /// A transport to nodes with one or more shard addresses each, with the
    /// default [`ClientConfig`]. List a node's addresses in its shard order.
    pub fn connect_sharded(nodes: impl IntoIterator<Item = (NodeId, Vec<SocketAddr>)>) -> Self {
        Self::connect_sharded_with_config(nodes, ClientConfig::default())
            .expect("valid default client config")
    }

    /// [`connect_sharded`](Self::connect_sharded) with
    /// [`ClientConfig::benchmark_unbounded`]. For benchmarks only.
    pub fn connect_sharded_unbounded(
        nodes: impl IntoIterator<Item = (NodeId, Vec<SocketAddr>)>,
    ) -> Self {
        Self::connect_sharded_with_config(nodes, ClientConfig::benchmark_unbounded())
            .expect("valid benchmark client config")
    }

    /// [`connect_sharded`](Self::connect_sharded) with an explicit, validated
    /// configuration.
    pub fn connect_sharded_with_config(
        nodes: impl IntoIterator<Item = (NodeId, Vec<SocketAddr>)>,
        config: ClientConfig,
    ) -> Result<Self, ClientConfigError> {
        let config = config.validate()?;
        reactor::set_flush_hook(flush_dirty);
        let transport_credits = config
            .budgets
            .map(CreditPool::from_budgets)
            .map(|pool| Rc::new(RefCell::new(pool)));
        if let Some(pool) = &transport_credits {
            register_admission_pool(pool);
        }
        let conns = nodes
            .into_iter()
            .map(|(node, addrs)| {
                let shards = addrs
                    .into_iter()
                    .map(|a| Self::configured_conn(a, node, config, transport_credits.clone()))
                    .collect();
                (node, shards)
            })
            .collect();
        Ok(NetTransport {
            conns: RefCell::new(conns),
            connect_deadline: Cell::new(config.connect_deadline),
            rpc_deadline: Cell::new(config.rpc_deadline),
            budgets: config.budgets,
            transport_credits,
            down: RefCell::new(HashSet::new()),
            route_epoch: Cell::new(0),
            sorted_ranges: RefCell::new(HashMap::new()),
            mixed_batch_reads: !matches!(
                std::env::var("VALS_BATCH_DIGEST_READS").as_deref(),
                Ok("0") | Ok("false") | Ok("off")
            ),
            mixed_batch_rpcs: Cell::new(0),
            mixed_scalar_probes: Cell::new(0),
            rpc_attempts: Cell::new(0),
        })
    }

    fn configured_conn(
        addr: SocketAddr,
        node: NodeId,
        config: ClientConfig,
        pool: Option<Rc<RefCell<CreditPool>>>,
    ) -> Rc<MuxConn> {
        let conn = Rc::new(match pool {
            Some(pool) => MuxConn::new_accounted(addr, node, pool),
            None => MuxConn::new(addr),
        });
        {
            let mut state = conn.st.borrow_mut();
            state.connect_deadline = config.connect_deadline;
            state.rpc_deadline = config.rpc_deadline;
        }
        if config.connect_deadline.is_some() || config.rpc_deadline.is_some() {
            register_deadline_conn(&conn);
        }
        conn
    }

    /// Sets both the connect and RPC deadlines to `deadline`, for existing
    /// and future connections. Panics if `deadline` is zero. Prefer
    /// [`ClientConfig`], which sets them separately.
    pub fn with_rpc_deadline(self, deadline: Duration) -> Self {
        assert!(!deadline.is_zero(), "RPC deadline must be positive");
        self.connect_deadline.set(Some(deadline));
        self.rpc_deadline.set(Some(deadline));
        for shards in self.conns.borrow().values() {
            for c in shards {
                let mut state = c.st.borrow_mut();
                state.connect_deadline = Some(deadline);
                state.rpc_deadline = Some(deadline);
                drop(state);
                register_deadline_conn(c);
            }
        }
        self
    }

    /// Whether the transport knows an address for `node`.
    pub fn has_peer(&self, node: NodeId) -> bool {
        self.conns.borrow().contains_key(&node)
    }

    /// Sets `node`'s shard addresses, replacing its connections if they
    /// changed. Returns whether anything changed. Ring refreshes call this
    /// for every member.
    pub fn add_peer(&self, node: NodeId, addrs: &[SocketAddr]) -> bool {
        if addrs.is_empty() {
            return false;
        }
        let mut conns = self.conns.borrow_mut();
        if let Some(existing) = conns.get(&node) {
            let unchanged = existing.len() == addrs.len()
                && existing.iter().zip(addrs).all(|(c, a)| c.addr == *a);
            if unchanged {
                return false;
            }
        }
        let config = ClientConfig {
            connect_deadline: self.connect_deadline.get(),
            rpc_deadline: self.rpc_deadline.get(),
            budgets: self.budgets,
        };
        let shards: Vec<Rc<MuxConn>> = addrs
            .iter()
            .map(|&addr| {
                Self::configured_conn(addr, node, config, self.transport_credits.clone())
            })
            .collect();
        conns.insert(node, shards);
        log::info!(
            "transport: installed peer node {node} ({} shard addr(s)) from membership",
            addrs.len()
        );
        crate::observe::counters().peers_added.inc();
        true
    }

    #[doc(hidden)]
    pub fn set_down(&self, node: NodeId, down: bool) {
        if down {
            self.down.borrow_mut().insert(node);
        } else {
            self.down.borrow_mut().remove(&node);
        }
    }

    fn shard_count(&self, node: NodeId) -> usize {
        self.conns.borrow().get(&node).map_or(1, |s| s.len())
    }

    pub(super) fn data_shard(&self, node: NodeId, zone_id: ZoneId, key: &[u8]) -> usize {
        crate::partition::data_shard(
            self.sorted_ranges.borrow().get(&zone_id),
            key,
            self.shard_count(node),
        )
    }

    pub(super) fn group_by_data_shard<'a>(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        keys: impl Iterator<Item = &'a [u8]>,
    ) -> HashMap<usize, Vec<usize>> {
        let mut by_shard = HashMap::new();
        for (i, key) in keys.enumerate() {
            by_shard
                .entry(self.data_shard(node, zone_id, key))
                .or_insert_with(Vec::new)
                .push(i);
        }
        by_shard
    }

    pub(super) fn scan_shard(&self, node: NodeId, zone_id: ZoneId, lo: Option<&[u8]>) -> usize {
        crate::partition::scan_shard(
            self.sorted_ranges.borrow().get(&zone_id),
            lo,
            self.shard_count(node),
        )
    }

    pub(super) fn conn(&self, node: NodeId, shard: usize) -> Result<Rc<MuxConn>, TransportError> {
        if self.down.borrow().contains(&node) {
            return Err(TransportError::Unavailable);
        }
        self.conns
            .borrow()
            .get(&node)
            .and_then(|shards| shards.get(shard))
            .cloned()
            .ok_or(TransportError::Unavailable)
    }

    pub(super) fn multi_scan_support<const REVERSE: bool>(&self, node: NodeId, shard: usize) -> Option<bool> {
        let conn = self.conn(node, shard).ok()?;
        let required = crate::proto::FEATURE_MULTI_SCAN
            | if REVERSE { crate::proto::FEATURE_REVERSE_SCAN } else { 0 };
        let support = conn.st.borrow().negotiated.map(|n| {
            n.features & required == required
        });
        support
    }

    pub(super) fn mixed_batch_read_support(&self, node: NodeId, shard: usize) -> Option<bool> {
        let conn = self.conn(node, shard).ok()?;
        let support = conn.st.borrow().negotiated.map(|n| {
            n.features & crate::proto::FEATURE_MIXED_BATCH_READ != 0
        });
        support
    }

    #[doc(hidden)]
    pub fn range_tombstones_supported(&self, node: NodeId) -> Option<bool> {
        let shards = self.conns.borrow().get(&node).cloned()?;
        let mut supported = None;
        for conn in shards {
            let st = conn.st.borrow();
            if let Some(features) = st.negotiated.map(|n| n.features).or(st.last_features) {
                if features & crate::proto::FEATURE_RANGE_TOMBSTONES == 0 {
                    return Some(false);
                }
                supported = Some(true);
            }
        }
        supported
    }

    pub(super) async fn acquire_transport(
        &self,
        node: NodeId,
        ops: usize,
        bytes: usize,
    ) -> Result<Option<Credit>, TransportError> {
        let Some(pool) = &self.transport_credits else {
            return Ok(None);
        };
        if pool.borrow().waiters.is_empty() {
            if let Some(credit) = try_transport_credit(pool, node, ops, bytes) {
                return Ok(Some(credit));
            }
        }
        Acquire {
            pool: pool.clone(),
            ops,
            bytes,
            node_costs: vec![NodeCreditCost { node, ops, bytes }],
            ticket: None,
            deadline_at: Some(
                Instant::now() + self.rpc_deadline.get().unwrap_or(DEFAULT_RPC_DEADLINE),
            ),
        }
        .await
        .map(Some)
        .map_err(|_| TransportError::Overloaded)
    }

    pub(super) async fn rpc(
        &self,
        node: NodeId,
        shard: usize,
        request: &Request<'_>,
        ops: usize,
    ) -> Result<AccountedResponse, TransportError> {
        self.rpc_with_wait(node, shard, request, ops, true).await
    }

    pub(super) async fn rpc_nowait(
        &self,
        node: NodeId,
        shard: usize,
        request: &Request<'_>,
        ops: usize,
    ) -> Result<AccountedResponse, TransportError> {
        self.rpc_with_wait(node, shard, request, ops, false).await
    }

    async fn rpc_with_wait(
        &self,
        node: NodeId,
        shard: usize,
        request: &Request<'_>,
        ops: usize,
        wait: bool,
    ) -> Result<AccountedResponse, TransportError> {
        let mut after_idle = false;
        match self.rpc_attempt(node, shard, request, ops, wait, &mut after_idle).await {
            Err(TransportError::Unavailable) if after_idle && is_idempotent_read(request) => {
                crate::observe::counters().idle_conn_retries.inc();
                self.rpc_attempt(node, shard, request, ops, wait, &mut after_idle).await
            }
            result => result,
        }
    }

    async fn rpc_attempt(
        &self,
        node: NodeId,
        shard: usize,
        request: &Request<'_>,
        ops: usize,
        wait: bool,
        after_idle: &mut bool,
    ) -> Result<AccountedResponse, TransportError> {
        let conn = self.conn(node, shard)?;
        let mut payload = wire::encode_request_payload(request);
        let bytes = payload.len().saturating_add(12);
        let credit = if let Some(pool) = &self.transport_credits {
            if pool.borrow().waiters.is_empty() {
                try_transport_credit(pool, node, ops, bytes)
            } else {
                None
            }
        } else {
            None
        };
        let accounted = if self.transport_credits.is_some() && credit.is_none() {
            drop(payload);
            if !wait {
                return Err(TransportError::Overloaded);
            }
            let credit = self.acquire_transport(node, ops, bytes).await?.unwrap();
            payload = wire::encode_request_payload(request);
            Some(credit)
        } else {
            credit
        };
        self.rpc_attempts.set(self.rpc_attempts.get() + 1);
        let mut rpc = match accounted {
            Some(credit) => {
                let (op_credit, request_credit) = credit.split_ops_bytes();
                MuxRpc::new_accounted(conn, payload, op_credit, request_credit, false)
            }
            None => MuxRpc::new(conn, payload),
        };
        let reply = (&mut rpc).await;
        *after_idle = rpc.after_idle;
        drop(rpc);
        let reply = reply?;
        let response = wire::decode_response(&reply)
            .map_err(|e| TransportError::Remote(format!("{e:?}")))?;
        let owned_bytes = response_owned_bytes(&response);
        let response_credit = if owned_bytes == 0 {
            None
        } else {
            let Some(pool) = &self.transport_credits else {
                return Ok(AccountedResponse {
                    response,
                    response_credit: None,
                });
            };
            match try_transport_credit(pool, node, 0, owned_bytes) {
                Some(credit) => Some(credit),
                None => return Err(TransportError::Overloaded),
            }
        };
        drop(reply);
        Ok(AccountedResponse {
            response,
            response_credit,
        })
    }

    pub(super) async fn read_accounted(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<(Option<Stored>, Option<Credit>), TransportError> {
        let accounted = self
            .rpc(
                node,
                self.data_shard(node, zone_id, key),
                &Request::Read {
                    epoch: self.route_epoch.get(),
                    zone_id,
                    key,
                },
                1,
            )
            .await?;
        match accounted.response {
            Response::Read(value) => Ok((value, accounted.response_credit)),
            Response::Busy => Err(TransportError::Busy),
            Response::StaleEpoch(e) => Err(TransportError::StaleEpoch(e)),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected read reply".into())),
        }
    }

    pub(super) async fn rpc_send(
        &self,
        node: NodeId,
        shard: usize,
        request: &Request<'_>,
    ) -> Result<(), TransportError> {
        let conn = self.conn(node, shard)?;
        let mut payload = wire::encode_request_payload(request);
        let bytes = payload.len().saturating_add(12);
        let credit = if let Some(pool) = &self.transport_credits {
            if pool.borrow().waiters.is_empty() {
                try_transport_credit(pool, node, 1, bytes)
            } else {
                None
            }
        } else {
            None
        };
        let accounted = if self.transport_credits.is_some() && credit.is_none() {
            drop(payload);
            let credit = self.acquire_transport(node, 1, bytes).await?.unwrap();
            payload = wire::encode_request_payload(request);
            Some(credit)
        } else {
            credit
        };
        self.rpc_attempts.set(self.rpc_attempts.get() + 1);
        match accounted {
            Some(credit) => {
                let (op_credit, request_credit) = credit.split_ops_bytes();
                MuxRpc::new_accounted(conn, payload, op_credit, request_credit, true).await?
            }
            None => MuxRpc::new_send(conn, payload).await?,
        };
        Ok(())
    }

    #[doc(hidden)]
    pub fn peer(&self, node: NodeId) -> NetPeer<'_> {
        self.peer_shard(node, 0)
    }

    #[doc(hidden)]
    pub fn peer_shard(&self, node: NodeId, shard: usize) -> NetPeer<'_> {
        NetPeer {
            transport: self,
            node,
            shard,
        }
    }

    #[doc(hidden)]
    pub fn peer_shard_count(&self, node: NodeId) -> usize {
        self.conns.borrow().get(&node).map_or(0, Vec::len)
    }

    #[doc(hidden)]
    pub fn mixed_read_stats(&self) -> (u64, u64) {
        (self.mixed_batch_rpcs.get(), self.mixed_scalar_probes.get())
    }

    /// Requests sent so far, counting retries.
    pub fn rpc_attempts(&self) -> u64 {
        self.rpc_attempts.get()
    }

    /// A snapshot of the transport's request budget (`None` when unbounded).
    pub fn transport_credit_stats(&self) -> Option<crate::coord::CreditStats> {
        self.transport_credits
            .as_ref()
            .map(|pool| pool.borrow().stats())
    }

    /// Per-connection counters, for diagnostics.
    pub fn connection_stats(&self) -> Vec<ConnectionStats> {
        self.conns
            .borrow()
            .iter()
            .flat_map(|(&node, shards)| {
                shards.iter().enumerate().map(move |(shard, conn)| {
                    let state = conn.st.borrow();
                    ConnectionStats {
                        node,
                        shard,
                        pending: state.pending.len(),
                        pending_peak: state.pending_peak,
                        queued_bytes: state.out.len() - state.out_pos,
                        queued_bytes_peak: state.queued_bytes_peak,
                        enqueued_bytes: state.enqueued_total,
                        sent_bytes: state.sent_total,
                        write_calls: state.write_calls,
                        read_drains: state.read_drains,
                        reconnects: state.connection_generations.saturating_sub(1),
                    }
                })
            })
            .collect()
    }

    #[doc(hidden)]
    pub fn override_peer_features(&self, node: NodeId, shard: usize, features: u64) -> bool {
        let Ok(conn) = self.conn(node, shard) else {
            return false;
        };
        let mut state = conn.st.borrow_mut();
        let Some(mut negotiated) = state.negotiated else {
            return false;
        };
        negotiated.features = features;
        state.negotiated = Some(negotiated);
        true
    }

    #[doc(hidden)]
    pub async fn watermark(&self, node: NodeId) -> Result<u64, TransportError> {
        match self
            .rpc(
                node,
                0,
                &Request::GetWatermark,
                1,
            )
            .await?
            .response
        {
            Response::Watermark(ms) => Ok(ms),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected watermark reply".into())),
        }
    }

    #[doc(hidden)]
    pub async fn range_write_mark(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        epoch: u64,
        lower: &[u8],
    ) -> Result<u64, TransportError> {
        match self
            .rpc(
                node,
                self.scan_shard(node, zone_id, Some(lower)),
                &Request::RangeWriteMark {
                    epoch,
                    zone_id,
                    lower,
                },
                1,
            )
            .await?
            .response
        {
            Response::Watermark(mark) => Ok(mark),
            Response::StaleEpoch(e) => Err(TransportError::StaleEpoch(e)),
            Response::Err(e) => Err(TransportError::Remote(e)),
            _ => Err(TransportError::Remote("expected range write mark reply".into())),
        }
    }
}

