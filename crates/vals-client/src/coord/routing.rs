use super::*;

impl<T: Transport> Coordinator<T> {
    /// Creates a coordinator.
    ///
    /// `node` labels this coordinator (use an id that is not a data node's);
    /// versions carry a random writer id, not this label. `ring` is the boot
    /// placement, replaced by [`refresh_ring`](Self::refresh_ring) or by the
    /// automatic refresh after a stale-ring refusal. `r` is the replica count
    /// [`read`](Self::read), [`read_in`](Self::read_in) and
    /// [`read_many`](Self::read_many) wait for; with RF 3 use 2 so that
    /// majority writes and reads overlap. A coordinator over a
    /// [`NetTransport`](crate::NetTransport) takes its request budgets from
    /// the transport's [`ClientConfig`](crate::ClientConfig); other transports
    /// are unbounded unless you call [`with_credits`](Self::with_credits).
    ///
    /// The coordinator starts with digest reads, automatic stale-ring refresh,
    /// in-memory hints, and no failure detector.
    pub fn new(node: NodeId, ring: Ring, r: usize, transport: T) -> Self {
        Self::new_with_hlc(node, ring, r, transport, Hlc::new(node))
    }

    #[doc(hidden)]
    pub fn new_with_writer_for_test(
        node: NodeId,
        ring: Ring,
        r: usize,
        transport: T,
        writer: u64,
    ) -> Self {
        Self::new_with_hlc(node, ring, r, transport, Hlc::with_writer_for_test(writer))
    }

    fn new_with_hlc(node: NodeId, ring: Ring, r: usize, transport: T, hlc: Hlc) -> Self {
        let default_rf = ring.rf();
        let credits = Rc::new(RefCell::new(
            transport
                .client_budgets()
                .map(CreditPool::from_budgets)
                .unwrap_or_else(|| CreditPool::new(usize::MAX, usize::MAX)),
        ));
        Coordinator {
            node,
            ring: RefCell::new(ring),
            epoch: Cell::new(0),
            transport,
            hlc,
            r,
            read_fanout: ReadFanout::Digest,
            hints: RefCell::new(Box::new(MemHintStore::default())),
            read_rotor: Cell::new(0),
            credits,
            catalog: RefCell::new(Catalog::with_default(default_rf)),
            range_maps: RefCell::new(HashMap::new()),
            fd: None,
            clock: Box::new(physical_now_ms),
            auto_refresh: Cell::new(true),
            scan_prefetch: Cell::new(true),
            digest_hedge: None,
        }
    }

    /// Turns off the automatic ring refresh and single retry after a
    /// stale-ring refusal; such operations then fail with
    /// [`CoordError::StaleRing`] and you refresh yourself.
    pub fn with_manual_refresh(self) -> Self {
        self.auto_refresh.set(false);
        self
    }

    /// Whether unlimited scans request the next page while merging the
    /// current one (on by default).
    pub fn set_scan_prefetch(&self, on: bool) {
        self.scan_prefetch.set(on);
    }

    /// Adds a per-node circuit breaker: after `cfg.failure_threshold`
    /// consecutive failed requests to a node, requests to it fail immediately
    /// as unavailable, except one probe every `cfg.open_cooldown_ms`, until a
    /// request succeeds. This saves waiting for deadlines on a dead node.
    pub fn with_failure_detector(mut self, cfg: crate::failure::FdConfig) -> Self {
        self.fd = Some(RefCell::new(FailureDetector::new(cfg)));
        self
    }

    #[doc(hidden)]
    pub fn with_clock(mut self, clock: impl Fn() -> u64 + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    /// Whether the failure detector currently treats `node` as down (always
    /// false without [`with_failure_detector`](Self::with_failure_detector)).
    pub fn is_node_down(&self, node: NodeId) -> bool {
        match &self.fd {
            Some(fd) => fd.borrow().is_down(node, (self.clock)()),
            None => false,
        }
    }

    /// The nodes the failure detector currently treats as down.
    pub fn down_nodes(&self) -> Vec<NodeId> {
        match &self.fd {
            Some(fd) => fd.borrow().down_nodes((self.clock)()),
            None => Vec::new(),
        }
    }

    /// Replaces where hints for unreachable replicas are kept. The default
    /// [`MemHintStore`] is in memory, bounded per node, and lost with the
    /// process. Hints are kept only for default-zone writes.
    pub fn with_hint_store(mut self, hints: Box<dyn HintStore>) -> Self {
        self.hints = RefCell::new(hints);
        self
    }

    /// Sets the global write budget (operations and key-plus-value bytes in
    /// flight); see [`ClientBudgets`]. Zero is raised to one.
    pub fn with_credits(self, max_ops: usize, max_bytes: usize) -> Self {
        let mut credits = self.credits.borrow_mut();
        let node_max_ops = credits.node_max_ops;
        let node_max_bytes = credits.node_max_bytes;
        *credits = CreditPool::new(max_ops.max(1), max_bytes.max(1));
        credits.set_node_limits(node_max_ops, node_max_bytes);
        drop(credits);
        self
    }

    /// Sets the per-node write budget; see [`ClientBudgets`]. Zero is raised
    /// to one.
    pub fn with_node_credits(self, max_ops: usize, max_bytes: usize) -> Self {
        {
            let mut p = self.credits.borrow_mut();
            p.node_max_ops = max_ops.max(1);
            p.node_max_bytes = max_bytes.max(1);
        }
        self
    }

    /// Chooses how quorum reads contact replicas (default
    /// [`ReadFanout::Digest`]).
    pub fn with_read_fanout(mut self, mode: ReadFanout) -> Self {
        self.read_fanout = mode;
        self
    }

    /// In digest reads, also asks a spare replica if the needed replies have
    /// not arrived after `delay` (zero turns hedging off, the default).
    /// Needs a [`CoordinatorService`] to run the hedge timer.
    pub fn with_digest_hedge(mut self, delay: Duration) -> Self {
        if delay.is_zero() {
            self.digest_hedge = None;
            return self;
        }
        let tick_period = (delay / 4).clamp(Duration::from_micros(100), delay);
        let period_nanos = tick_period.as_nanos().max(1);
        let delay_ticks = (delay.as_nanos().div_ceil(period_nanos) as u64).saturating_add(1);
        self.digest_hedge = Some(DigestHedge {
            clock: HedgeClock::new(),
            delay_ticks,
            tick_period,
        });
        self
    }

    #[doc(hidden)]
    pub fn hedge_clock(&self) -> Option<Rc<HedgeClock>> {
        self.digest_hedge.as_ref().map(|h| h.clock.clone())
    }

    #[doc(hidden)]
    pub fn hedge_tick_period(&self) -> Option<Duration> {
        self.digest_hedge.as_ref().map(|h| h.tick_period)
    }

    /// A snapshot of this coordinator's write budget.
    pub fn credit_stats(&self) -> CreditStats {
        self.credits.borrow().stats()
    }

    pub(super) fn acquire(&self, ops: usize, bytes: usize) -> Acquire {
        self.acquire_for(Vec::new(), ops, bytes)
    }

    pub(super) fn acquire_for(
        &self,
        node_costs: Vec<NodeCreditCost>,
        ops: usize,
        bytes: usize,
    ) -> Acquire {
        Acquire {
            pool: self.credits.clone(),
            ops,
            bytes,
            node_costs,
            ticket: None,
            deadline_at: None,
        }
    }

    fn node_limits_enabled(&self) -> bool {
        let p = self.credits.borrow();
        p.node_max_ops != usize::MAX || p.node_max_bytes != usize::MAX
    }

    fn scalar_node_costs(&self, nodes: Vec<NodeId>, ops: usize, bytes: usize) -> Vec<NodeCreditCost> {
        nodes
            .into_iter()
            .map(|node| NodeCreditCost { node, ops, bytes })
            .collect()
    }

    pub(super) async fn acquire_write(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        ops: usize,
        bytes: usize,
    ) -> Result<(Credit, Option<Vec<NodeId>>), CoordError> {
        if self.node_limits_enabled() {
            loop {
                let nodes = self.resolve_replicas(zone_id, key).await?;
                let epoch = self.epoch.get();
                let credit = self
                    .acquire_for(self.scalar_node_costs(nodes.clone(), ops, bytes), ops, bytes)
                    .await?;
                if self.epoch.get() == epoch {
                    return Ok((credit, Some(nodes)));
                }
                drop(credit);
            }
        } else {
            Ok((self.acquire(ops, bytes).await?, None))
        }
    }

    pub(super) async fn try_acquire_write(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        ops: usize,
        bytes: usize,
    ) -> Result<(Credit, Option<Vec<NodeId>>), CoordError> {
        if self.node_limits_enabled() {
            let nodes = self.resolve_replicas(zone_id, key).await?;
            let credit = self
                .try_acquire_for(self.scalar_node_costs(nodes.clone(), ops, bytes), ops, bytes)
                .ok_or(CoordError::Overloaded)?;
            Ok((credit, Some(nodes)))
        } else {
            Ok((self.try_acquire(ops, bytes).ok_or(CoordError::Overloaded)?, None))
        }
    }

    pub(super) async fn batch_node_costs(
        &self,
        zone_id: ZoneId,
        writes: &[(&[u8], &[u8])],
    ) -> Result<Vec<NodeCreditCost>, CoordError> {
        if !self.node_limits_enabled() {
            return Ok(Vec::new());
        }
        self.ensure_zone(zone_id).await?;
        let mut costs = HashMap::<NodeId, (usize, usize)>::new();
        for &(key, value) in writes {
            let bytes = key.len().saturating_add(value.len());
            for node in self.replicas_for(zone_id, key)? {
                let cost = costs.entry(node).or_default();
                cost.0 = cost.0.saturating_add(1);
                cost.1 = cost.1.saturating_add(bytes);
            }
        }
        Ok(costs
            .into_iter()
            .map(|(node, (ops, bytes))| NodeCreditCost { node, ops, bytes })
            .collect())
    }

    pub(super) async fn acquire_batch(
        &self,
        zone_id: ZoneId,
        writes: &[(&[u8], &[u8])],
        shed: bool,
    ) -> Result<Credit, CoordError> {
        let (ops, bytes) = batch_cost(writes);
        loop {
            let node_costs = self.batch_node_costs(zone_id, writes).await?;
            let epoch = self.epoch.get();
            let credit = if shed {
                self.try_acquire_for(node_costs, ops, bytes)
                    .ok_or(CoordError::Overloaded)?
            } else {
                self.acquire_for(node_costs, ops, bytes).await?
            };
            if self.epoch.get() == epoch {
                return Ok(credit);
            }
            drop(credit);
        }
    }

    #[doc(hidden)]
    pub fn try_acquire(&self, ops: usize, bytes: usize) -> Option<Credit> {
        self.try_acquire_for(Vec::new(), ops, bytes)
    }

    #[doc(hidden)]
    pub fn try_acquire_for(
        &self,
        node_costs: Vec<NodeCreditCost>,
        ops: usize,
        bytes: usize,
    ) -> Option<Credit> {
        let mut p = self.credits.borrow_mut();
        if p.waiters.is_empty() && p.fits_all(&node_costs, ops, bytes) {
            p.reserve(&node_costs, ops, bytes);
            Some(Credit {
                pool: self.credits.clone(),
                ops,
                bytes,
                node_costs,
            })
        } else {
            None
        }
    }

    /// The label given to [`new`](Self::new).
    pub fn node_id(&self) -> NodeId {
        self.node
    }

    #[doc(hidden)]
    pub fn stamp_at(&self, physical_ms: u64) -> Version {
        self.hlc.now(physical_ms)
    }

    /// The transport, for its statistics and peer management.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// The current placement ring. Do not hold the borrow across an `.await`
    /// of this coordinator's operations; they may need to update it.
    pub fn ring(&self) -> std::cell::Ref<'_, Ring> {
        self.ring.borrow()
    }

    /// Mutable access to the ring, for setting up a boot ring by hand. The
    /// next refresh replaces it.
    pub fn ring_mut(&self) -> std::cell::RefMut<'_, Ring> {
        self.ring.borrow_mut()
    }

    /// Asks node `from` for the cluster's membership and adopts it (ring
    /// members, weights, replication factor, epoch and member addresses) if
    /// it is not older than what this coordinator already has. Returns the
    /// member ids. Call it, and [`refresh_catalog`](Self::refresh_catalog),
    /// once before traffic; fails with [`CoordError::Unavailable`] if `from`
    /// cannot be reached.
    pub async fn refresh_ring(&self, from: NodeId) -> Result<Vec<NodeId>, CoordError> {
        let view = self
            .transport
            .members(from)
            .await
            .map_err(|_| CoordError::Unavailable)?;
        Ok(self.adopt_ring_view(view))
    }

    pub(super) fn adopt_ring_view(&self, view: RingView) -> Vec<NodeId> {
        if view.epoch < self.epoch.get() {
            return self.ring.borrow().members().collect();
        }
        self.epoch.set(view.epoch);
        self.transport.set_route_epoch(self.epoch.get());
        if view.vnodes != 0 {
            self.ring.borrow_mut().reconfigure(
                view.rf as usize,
                view.vnodes,
                view.members.iter().copied(),
            );
            self.catalog.borrow_mut().insert(
                DEFAULT_ZONE,
                crate::zone::ZoneConfig::default_config(view.rf as usize),
            );
        } else {
            self.ring
                .borrow_mut()
                .set_members_weighted(view.members.iter().copied());
        }
        for (node, addrs) in &view.addrs {
            self.transport.add_peer(*node, addrs);
        }
        view.members.into_iter().map(|(n, _)| n).collect()
    }

    /// The epoch of the adopted ring (0 before any refresh).
    pub fn ring_epoch(&self) -> u64 {
        self.epoch.get()
    }

    /// The zone catalog as last refreshed. Look up a zone id by name with
    /// [`Catalog::by_name`]. The same borrow rule as [`ring`](Self::ring)
    /// applies.
    pub fn catalog(&self) -> std::cell::Ref<'_, Catalog> {
        self.catalog.borrow()
    }

    /// Asks node `from` for the zone catalog and the sorted zones' range maps
    /// and adopts them. Operations on a zone missing from the catalog refresh
    /// it once by themselves.
    pub async fn refresh_catalog(&self, from: NodeId) -> Result<(), CoordError> {
        let mut cat = self
            .transport
            .zones(from)
            .await
            .map_err(|_| CoordError::Unavailable)?;
        if !cat.contains(DEFAULT_ZONE) {
            cat.insert(
                DEFAULT_ZONE,
                crate::zone::ZoneConfig::default_config(self.ring.borrow().rf()),
            );
        }
        *self.catalog.borrow_mut() = cat;
        let _ = self.refresh_range_maps(from).await;
        self.push_sorted_ranges();
        Ok(())
    }

    fn push_sorted_ranges(&self) {
        let ranges: HashMap<ZoneId, RangeMap> = {
            let cat = self.catalog.borrow();
            cat.iter()
                .filter(|(_, cfg)| cfg.ordering == Ordering::Sorted)
                .map(|(id, _)| (id, self.range_map(id)))
                .collect()
        };
        self.transport.set_sorted_ranges(&ranges);
    }

    /// Asks node `from` for the sorted zones' range maps only.
    pub async fn refresh_range_maps(&self, from: NodeId) -> Result<(), CoordError> {
        let maps = self
            .transport
            .range_maps(from)
            .await
            .map_err(|_| CoordError::Unavailable)?;
        *self.range_maps.borrow_mut() = maps.into_iter().collect();
        self.push_sorted_ranges();
        Ok(())
    }

    /// The replicas this coordinator would send `key` in `zone_id` to, from
    /// its current ring and catalog.
    pub fn replicas_for(&self, zone_id: ZoneId, key: &[u8]) -> Result<Vec<NodeId>, CoordError> {
        let ring = self.ring.borrow();
        let reps = if zone_id == DEFAULT_ZONE {
            ring.replicas(key)
        } else {
            let cat = self.catalog.borrow();
            let cfg = cat.get(zone_id).ok_or(CoordError::UnknownZone(zone_id))?;
            match cfg.ordering {
                Ordering::Unsorted => {
                    ring.replicas_in_zone(zone_id, key, cfg.rf, cfg.scope.as_ref())
                }
                Ordering::Sorted => {
                    let token = self.range_token(zone_id, key);
                    ring.replicas_in_zone(zone_id, &token, cfg.rf, cfg.scope.as_ref())
                }
            }
        };
        if reps.is_empty() {
            Err(CoordError::NoReplicas)
        } else {
            Ok(reps)
        }
    }

    fn range_token(&self, zone_id: ZoneId, key: &[u8]) -> Vec<u8> {
        match self.range_maps.borrow().get(&zone_id) {
            Some(m) => m.range_token(key).to_vec(),
            None => Vec::new(),
        }
    }

    #[doc(hidden)]
    pub fn set_range_map(&self, zone_id: ZoneId, map: RangeMap) {
        self.range_maps.borrow_mut().insert(zone_id, map);
        self.push_sorted_ranges();
    }

    /// The adopted range map of sorted zone `zone_id` (a single range if none
    /// was received).
    pub fn range_map(&self, zone_id: ZoneId) -> RangeMap {
        self.range_maps
            .borrow()
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(RangeMap::single)
    }

    pub(super) async fn resolve_replicas(
        &self,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Vec<NodeId>, CoordError> {
        match self.replicas_for(zone_id, key) {
            Err(CoordError::UnknownZone(_)) => {
                let from = self.ring.borrow().members().next();
                if let Some(from) = from {
                    let _ = self.refresh_catalog(from).await;
                }
                self.replicas_for(zone_id, key)
            }
            other => other,
        }
    }

    pub(super) async fn ensure_zone(&self, zone_id: ZoneId) -> Result<(), CoordError> {
        if zone_id == DEFAULT_ZONE || self.catalog.borrow().contains(zone_id) {
            return Ok(());
        }
        let from = self.ring.borrow().members().next();
        if let Some(from) = from {
            let _ = self.refresh_catalog(from).await;
        }
        if self.catalog.borrow().contains(zone_id) {
            Ok(())
        } else {
            Err(CoordError::UnknownZone(zone_id))
        }
    }

    #[doc(hidden)]
    pub async fn adopt_from_any_member(&self) -> bool {
        let members: Vec<NodeId> = self.ring.borrow().members().collect();
        for from in members {
            if self.refresh_ring(from).await.is_ok() {
                let _ = self.refresh_catalog(from).await;
                return true;
            }
        }
        false
    }

    pub(super) async fn refresh_after_stale(&self) -> bool {
        crate::observe::counters().stale_ring_refreshes.inc();
        self.adopt_from_any_member().await
    }

    pub(super) async fn retry_stale<O, Fut, F>(&self, op: F) -> Result<O, CoordError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<O, CoordError>>,
    {
        let first = op().await;
        if !matches!(first, Err(CoordError::StaleRing { .. })) || !self.auto_refresh.get() {
            return first;
        }
        if !self.refresh_after_stale().await {
            return first;
        }
        op().await
    }

}
