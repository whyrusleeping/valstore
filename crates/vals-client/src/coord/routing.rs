use super::*;

impl<T: Transport> Coordinator<T> {
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

    pub fn with_manual_refresh(self) -> Self {
        self.auto_refresh.set(false);
        self
    }

    pub fn set_scan_prefetch(&self, on: bool) {
        self.scan_prefetch.set(on);
    }

    pub fn with_failure_detector(mut self, cfg: crate::failure::FdConfig) -> Self {
        self.fd = Some(RefCell::new(FailureDetector::new(cfg)));
        self
    }

    pub fn with_clock(mut self, clock: impl Fn() -> u64 + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    pub fn is_node_down(&self, node: NodeId) -> bool {
        match &self.fd {
            Some(fd) => fd.borrow().is_down(node, (self.clock)()),
            None => false,
        }
    }

    pub fn down_nodes(&self) -> Vec<NodeId> {
        match &self.fd {
            Some(fd) => fd.borrow().down_nodes((self.clock)()),
            None => Vec::new(),
        }
    }

    pub fn with_hint_store(mut self, hints: Box<dyn HintStore>) -> Self {
        self.hints = RefCell::new(hints);
        self
    }

    pub fn with_credits(self, max_ops: usize, max_bytes: usize) -> Self {
        let mut credits = self.credits.borrow_mut();
        let node_max_ops = credits.node_max_ops;
        let node_max_bytes = credits.node_max_bytes;
        *credits = CreditPool::new(max_ops.max(1), max_bytes.max(1));
        credits.set_node_limits(node_max_ops, node_max_bytes);
        drop(credits);
        self
    }

    pub fn with_node_credits(self, max_ops: usize, max_bytes: usize) -> Self {
        {
            let mut p = self.credits.borrow_mut();
            p.node_max_ops = max_ops.max(1);
            p.node_max_bytes = max_bytes.max(1);
        }
        self
    }

    pub fn with_read_fanout(mut self, mode: ReadFanout) -> Self {
        self.read_fanout = mode;
        self
    }

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

    pub fn node_id(&self) -> NodeId {
        self.node
    }

    #[doc(hidden)]
    pub fn stamp_at(&self, physical_ms: u64) -> Version {
        self.hlc.now(physical_ms)
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn ring(&self) -> std::cell::Ref<'_, Ring> {
        self.ring.borrow()
    }

    pub fn ring_mut(&self) -> std::cell::RefMut<'_, Ring> {
        self.ring.borrow_mut()
    }

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

    pub fn ring_epoch(&self) -> u64 {
        self.epoch.get()
    }

    pub fn catalog(&self) -> std::cell::Ref<'_, Catalog> {
        self.catalog.borrow()
    }

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

    pub fn set_range_map(&self, zone_id: ZoneId, map: RangeMap) {
        self.range_maps.borrow_mut().insert(zone_id, map);
        self.push_sorted_ranges();
    }

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
