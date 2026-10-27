use super::*;

const SHUTDOWN_FLUSH_DRAIN: Duration = Duration::from_secs(5);

#[cfg(test)]
thread_local! {
    static SHUTDOWN_FLUSH_DRAIN_OVERRIDE: Cell<Option<Duration>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(super) fn set_shutdown_flush_drain(limit: Option<Duration>) {
    SHUTDOWN_FLUSH_DRAIN_OVERRIDE.with(|s| s.set(limit));
}

#[cfg(test)]
fn shutdown_flush_drain() -> Duration {
    SHUTDOWN_FLUSH_DRAIN_OVERRIDE.with(|s| s.get()).unwrap_or(SHUTDOWN_FLUSH_DRAIN)
}

#[cfg(not(test))]
fn shutdown_flush_drain() -> Duration {
    SHUTDOWN_FLUSH_DRAIN
}

fn scan_max_inflight() -> usize {
    crate::node_settings::process().scan_max_inflight
}

impl<D: BlockDevice> ServerShared<D> {
    fn from_parts(
        node_id: NodeId,
        zones: FastMap<ZoneId, Rc<RefCell<VersionedStore<D>>>>,
        devices: FastMap<ZoneId, Arc<D>>,
        provider: Option<Rc<dyn ZoneStore<D>>>,
        catalog: Catalog,
        ring: Ring,
    ) -> Self {
        let engine_count = zones.len() as u64;
        let incarnation = provider.as_ref().and_then(|p| p.persisted_incarnation());
        ServerShared {
            settings: crate::node_settings::NodeSettings::default(),
            node_id,
            zones: RefCell::new(zones),
            devices: RefCell::new(devices),
            provider,
            catalog: Tracked::new(catalog),
            range_maps: Tracked::new(FastMap::default()),
            range_owners: RefCell::new(RangeOwners::default()),
            range_load: RefCell::new(RangeLoadTracker::new()),
            range_map_epoch: Cell::new(0),
            range_maps_expected: Cell::new(false),
            range_maps_adopted: Cell::new(false),
            uncataloged_sorted: RefCell::new(Vec::new()),
            ring: Tracked::new(ring),
            peer_addrs: RefCell::new(FastMap::default()),
            rehome_mirrors: RefCell::new(FastMap::default()),
            rehome_copy_progress: RefCell::new(FastMap::default()),
            range_write_marks: RefCell::new(FastMap::default()),
            rehome_copy_holds: RefCell::new(FastMap::default()),
            zone_forget_gens: RefCell::new(FastMap::default()),
            rehome_transport: RefCell::new(None),
            read_gate: ReadGate::new(),
            scan_gate: ReadGate::with_limit(scan_max_inflight()),
            winner_probe_sample_cursor: Cell::new(0),
            batch_apply_gate: ReadGate::with_limit(crate::node_settings::NodeSettings::default().batch_apply_gate),
            sync_groups: RefCell::new(FastMap::default()),
            device_waits: Cell::new(0),
            domain_flushes: RefCell::new(Vec::new()),
            sweep_notify: reactor::Notify::new(),
            logical_accounting_notify: reactor::Notify::new(),
            seed_driver: Cell::new(false),
            foreground_ops: Cell::new(0),
            rebalance_pending: Cell::new(false),
            epoch: Cell::new(0),
            catalog_epoch: Cell::new(0),
            liveness: None,
            catalog_store: None,
            incarnation: Cell::new(incarnation),
            fenced: Cell::new(false),
            resync_pending: Cell::new(false),
            wipe_pending: Cell::new(None),
            last_contact: Cell::new(0),
            membership_adopted: Cell::new(false),
            had_persisted_watermark: Cell::new(false),
            watermark_floor: Cell::new(0),
            durable_gc_watermark: Cell::new(0),
            shutdown: Cell::new(false),
            force_close: Cell::new(false),
            shutdown_notifies: RefCell::new(Vec::new()),
            conn_outs: RefCell::new(Vec::new()),
            pending_drops: RefCell::new(Vec::new()),
            maint_jobs: RefCell::new(FastMap::default()),
            maint_reservations: RefCell::new(FastMap::default()),
            flush_builds: RefCell::new(FastMap::default()),
            published_live_keys: Cell::new(0),
            published_used_segments: Cell::new(0),
            published_free_segments: Cell::new(0),
            published_resident_runs: Cell::new(0),
            published_frozen_runs: Cell::new(0),
            published_stranded_runs: Cell::new(0),
            published_shadowed_bytes: Cell::new(0),
            published_shadow_folds: Cell::new(0),
            published_shadow_fold_disarms: Cell::new(0),
            published_memtable_bytes: Cell::new(0),
            published_run_metadata_bytes: Cell::new(0),
            published_winner_directory_keys: Cell::new(0),
            published_winner_directory_bytes: Cell::new(0),
            published_key_filter: Cell::new([0; KEY_FILTER_GAUGES]),
            published_run_meta: Cell::new([0; RUN_META_GAUGES]),
            registered_engines: Cell::new(engine_count),
            shard: Cell::new((0, 1)),
            shard_write_counters: crate::observe::shard_write_counters(0),
            sweep_every: Cell::new(Duration::from_secs(5)),
            sweep_pass_at: Cell::new(std::time::Instant::now()),
            sweep_pass_count: Cell::new(0),
            sweep_held_count: Cell::new(0),
            transport_deadline_poll: Cell::new(std::time::Instant::now()),
            integrity_watch: RefCell::new(crate::integrity::IntegrityWatch::default()),
            #[cfg(test)]
            inject_accept_errors: Cell::new(0),
        }
    }

    pub(crate) fn with_settings(mut self, settings: crate::node_settings::NodeSettings) -> Self {
        self.batch_apply_gate = ReadGate::with_limit(settings.batch_apply_gate);
        self.settings = settings;
        self
    }

    pub(crate) fn single_zone(store: VersionedStore<D>) -> Self {
        Self::single_zone_on(0, store, Ring::new(1))
    }

    pub(crate) fn single_zone_on(node_id: NodeId, store: VersionedStore<D>, ring: Ring) -> Self {
        let dev = store.device_handle();
        let mut zones = FastMap::default();
        let mut devices = FastMap::default();
        zones.insert(DEFAULT_ZONE, Rc::new(RefCell::new(store)));
        devices.insert(DEFAULT_ZONE, dev);
        crate::ram::add_zone_engines(1);
        Self::from_parts(node_id, zones, devices, None, Catalog::new(), ring)
    }

    #[cfg(test)]
    pub(crate) fn insert_zone_for_test(&self, zone_id: ZoneId, store: VersionedStore<D>) -> Rc<RefCell<VersionedStore<D>>> {
        self.integrity_watch.borrow_mut().cleared(zone_id);
        let dev = store.device_handle();
        let rc = Rc::new(RefCell::new(store));
        self.zones.borrow_mut().insert(zone_id, rc.clone());
        self.devices.borrow_mut().insert(zone_id, dev);
        self.registered_engines.set(self.registered_engines.get() + 1);
        crate::ram::add_zone_engines(1);
        rc
    }

    #[cfg(test)]
    pub(crate) fn arm_accept_faults(&self, n: u32) {
        self.inject_accept_errors.set(n);
    }

    #[cfg(test)]
    pub(super) fn maybe_inject_accept_error(&self, real: io::Result<super::Sock>) -> io::Result<super::Sock> {
        let n = self.inject_accept_errors.get();
        if n == 0 {
            return real;
        }
        self.inject_accept_errors.set(n - 1);
        Err(io::Error::from_raw_os_error(libc::EMFILE))
    }

    pub(crate) fn bind_device_owners(&self) {
        for dev in self.devices.borrow().values() {
            dev.bind_io_owner();
        }
    }

    pub(crate) fn ring(&self) -> &Tracked<Ring> {
        &self.ring
    }

    pub(crate) fn peer_addrs_of(&self, node: NodeId) -> Option<Vec<SocketAddr>> {
        self.peer_addrs.borrow().get(&node).cloned()
    }

    pub(super) fn merge_peer_addrs(&self, pushed: &[(NodeId, Vec<SocketAddr>)]) {
        if pushed.is_empty() {
            return;
        }
        let mut book = self.peer_addrs.borrow_mut();
        for (node, addrs) in pushed {
            if !addrs.is_empty() {
                book.insert(*node, addrs.clone());
            }
        }
        *self.rehome_transport.borrow_mut() = None;
    }

    pub(crate) fn inflight(&self) -> usize {
        self.read_gate.inflight()
    }

    pub(crate) fn read_gate(&self) -> &ReadGate {
        &self.read_gate
    }

    pub(crate) fn scans_inflight(&self) -> usize {
        self.scan_gate.inflight()
    }

    pub(crate) fn seed_foreground_inflight(&self) -> bool {
        self.batch_apply_gate.inflight() > 0
            || self.read_gate.inflight() > 0
            || self.scan_gate.inflight() > 0
    }

    pub(crate) fn foreground_ops(&self) -> u64 {
        self.foreground_ops.get()
    }

    #[cfg(test)]
    pub(crate) fn note_foreground_for_test(&self) {
        self.foreground_ops.set(self.foreground_ops.get().wrapping_add(1));
    }

    pub(crate) fn attach_seed_driver(&self, attached: bool) {
        self.seed_driver.set(attached);
        for store in self.zones.borrow().values() {
            if let Ok(store) = store.try_borrow() {
                store.set_seed_driver(attached);
            }
        }
    }

    pub(crate) fn try_scan(&self) -> Option<InflightGuard<'_>> {
        let permit = self.scan_gate.try_acquire();
        if permit.is_none() {
            crate::observe::counters().scan_admission_busy.inc();
        }
        permit
    }

    pub(super) fn sample_winner_probes(&self) -> bool {
        let cursor = self.winner_probe_sample_cursor.get();
        self.winner_probe_sample_cursor
            .set(cursor.wrapping_add(1) & 1023);
        cursor == 0
    }

    pub(crate) fn ring_epoch(&self) -> u64 {
        self.epoch.get()
    }

    pub(crate) fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch.get()
    }

    pub(crate) fn range_map_epoch(&self) -> u64 {
        self.range_map_epoch.get()
    }

    pub(crate) fn range_map(&self, zone_id: ZoneId) -> RangeMap {
        self.range_maps
            .borrow()
            .get(&zone_id)
            .cloned()
            .unwrap_or_else(RangeMap::single)
    }

    pub(crate) fn range_maps_clone(&self) -> HashMap<ZoneId, RangeMap> {
        self.range_maps
            .borrow()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    pub(crate) fn sorted_placement_ready(&self) -> bool {
        self.range_maps_adopted.get() || !self.range_maps_expected.get()
    }

    pub(super) fn held_uncataloged_sorted(&self, zone_id: ZoneId) -> bool {
        self.uncataloged_sorted.borrow().contains(&zone_id)
    }

    pub(super) fn release_uncataloged_sorted(&self) {
        self.uncataloged_sorted.borrow_mut().clear();
    }

    pub(crate) fn sorted_range_map(&self, zone_id: ZoneId) -> Option<RangeMap> {
        self.sorted_placement_ready().then(|| self.range_map(zone_id))
    }

    pub(crate) fn with_sorted_range_map<R>(
        &self,
        zone_id: ZoneId,
        f: impl FnOnce(&RangeMap) -> R,
    ) -> Option<R> {
        if !self.sorted_placement_ready() {
            return None;
        }
        let maps = self.range_maps.borrow();
        Some(match maps.get(&zone_id) {
            Some(map) => f(map),
            None => f(&RangeMap::single()),
        })
    }

    pub(crate) fn sorted_routes(&self) -> HashMap<ZoneId, RangeMap> {
        let mut routes = self.range_maps_clone();
        for (zone_id, cfg) in self.catalog.borrow().iter() {
            if cfg.ordering == crate::zone::Ordering::Sorted {
                routes.entry(zone_id).or_insert_with(RangeMap::single);
            }
        }
        routes
    }

    pub(super) fn expect_range_maps(&self) {
        if self.range_maps_adopted.get() || self.range_maps_expected.get() {
            return;
        }
        let sorted = self
            .catalog
            .borrow()
            .iter()
            .any(|(_, cfg)| cfg.ordering == crate::zone::Ordering::Sorted);
        if sorted {
            log::info!(
                "node {}: holding sorted-zone sweep and writes until the first range-map push",
                self.node_id
            );
            self.range_maps_expected.set(true);
        }
    }

    pub(super) fn adopt_range_maps(&self) {
        if !self.range_maps_adopted.replace(true) && self.range_maps_expected.get() {
            log::info!(
                "node {}: range maps adopted; releasing the sorted-zone hold",
                self.node_id
            );
        }
    }

    pub(super) fn note_range_ops<'a>(&self, zone_id: ZoneId, keys: impl IntoIterator<Item = &'a [u8]>) {
        self.note_range_ops_at(zone_id, keys, None);
    }

    pub(super) fn note_fenced_range_ops<'a>(
        &self,
        zone_id: ZoneId,
        fenced: Option<&FencedWrites>,
        keys: impl IntoIterator<Item = &'a [u8]>,
    ) {
        let indices = fenced
            .filter(|fenced| fenced.stamp == self.placement_stamp())
            .map(|fenced| fenced.indices.as_slice());
        self.note_range_ops_at(zone_id, keys, indices);
    }

    fn note_range_ops_at<'a>(
        &self,
        zone_id: ZoneId,
        keys: impl IntoIterator<Item = &'a [u8]>,
        indices: Option<&[usize]>,
    ) {
        self.foreground_ops.set(self.foreground_ops.get().wrapping_add(1));
        let sorted = self
            .catalog
            .borrow()
            .get(zone_id)
            .is_some_and(|cfg| cfg.ordering == crate::zone::Ordering::Sorted);
        if !sorted {
            return;
        }
        let maps = self.range_maps.borrow();
        let map = maps.get(&zone_id);
        let num_ranges = map.map_or(1, RangeMap::num_ranges);
        let mut load = self.range_load.borrow_mut();
        let counts = load
            .counts
            .entry(zone_id)
            .or_insert_with(|| vec![0; num_ranges]);
        if counts.len() != num_ranges {
            counts.clear();
            counts.resize(num_ranges, 0);
        }
        match indices {
            Some(indices) => {
                for &index in indices {
                    counts[index] = counts[index].saturating_add(1);
                }
            }
            None => {
                for key in keys {
                    let index = map.map_or(0, |map| map.range_index(key));
                    counts[index] = counts[index].saturating_add(1);
                }
            }
        }
    }

    pub(super) fn sample_range_load(&self) -> FastMap<ZoneId, Vec<u64>> {
        let now = std::time::Instant::now();
        let mut load = self.range_load.borrow_mut();
        let elapsed_ns = now.duration_since(load.sampled_at).as_nanos().max(1);
        load.sampled_at = now;
        let counts = std::mem::take(&mut load.counts);
        counts
            .into_iter()
            .map(|(zone_id, counts)| {
                let rates = counts
                    .into_iter()
                    .map(|ops| {
                        ((ops as u128).saturating_mul(1_000_000_000) / elapsed_ns)
                            .min(u64::MAX as u128) as u64
                    })
                    .collect();
                (zone_id, rates)
            })
            .collect()
    }

    pub(super) fn reset_range_load(&self) {
        let mut load = self.range_load.borrow_mut();
        load.counts.clear();
        load.sampled_at = std::time::Instant::now();
    }

    pub(crate) fn zone_forget_gen(&self, zone_id: ZoneId) -> u64 {
        self.zone_forget_gens
            .borrow()
            .get(&zone_id)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn bump_zone_forget_gen(&self, zone_id: ZoneId) {
        *self.zone_forget_gens.borrow_mut().entry(zone_id).or_insert(0) += 1;
    }

    pub(crate) fn range_write_mark_register(
        &self,
        zone_id: ZoneId,
        lower: Vec<u8>,
        now_ms: u64,
    ) -> u64 {
        let mut all = self.range_write_marks.borrow_mut();
        let zone = all.entry(zone_id).or_default();
        if let Some(slot) = zone.marks.get_mut(lower.as_slice()) {
            slot.last_pending_ms = now_ms;
            slot.value
        } else {
            zone.counter += 1;
            let value = zone.counter;
            zone.marks.insert(
                lower,
                RangeWriteMarkSlot {
                    value,
                    last_pending_ms: now_ms,
                },
            );
            value
        }
    }

    pub(crate) fn invalidate_zone_write_marks(&self, zone_id: ZoneId) {
        let mut all = self.range_write_marks.borrow_mut();
        if let Some(zone) = all.get_mut(&zone_id) {
            let mut counter = zone.counter;
            for slot in zone.marks.values_mut() {
                counter += 1;
                slot.value = counter;
            }
            zone.counter = counter;
        }
    }

    pub(super) fn prune_range_write_marks(&self, now_ms: u64) {
        let maps = self.range_maps.borrow();
        let mut all = self.range_write_marks.borrow_mut();
        all.retain(|zone_id, zone_marks| {
            let Some(map) = maps.get(zone_id) else {
                return false;
            };
            zone_marks.marks.retain(|lower, slot| {
                let index = map.range_index(lower);
                if map.range_bounds(index).0.unwrap_or_default() != lower.as_slice() {
                    return false;
                }
                if map.pending_of(index).is_some() {
                    slot.last_pending_ms = now_ms;
                    true
                } else {
                    now_ms.saturating_sub(slot.last_pending_ms) <= REHOME_MARK_TTL_MS
                }
            });
            true
        });
    }

    pub(crate) fn hold_rehome_copy(&self, zone_id: ZoneId, lower: Vec<u8>, until_ms: u64) {
        self.rehome_copy_holds
            .borrow_mut()
            .insert((zone_id, lower), until_ms);
    }

    pub(crate) fn rehome_copy_hold_active(
        &self,
        zone_id: ZoneId,
        lower: &[u8],
        now_ms: u64,
    ) -> bool {
        self.rehome_copy_holds
            .borrow()
            .get(&(zone_id, lower.to_vec()))
            .is_some_and(|&until| until > now_ms)
    }

    pub(crate) fn prune_rehome_copy_holds(&self, now_ms: u64) {
        self.rehome_copy_holds
            .borrow_mut()
            .retain(|_, &mut until| until > now_ms);
    }

    pub(crate) fn watermark(&self) -> u64 {
        let mut wm = self.watermark_floor.get();
        for store in self.zones.borrow().values() {
            wm = wm.max(store.borrow().high_water_ms());
        }
        wm
    }

    fn set_watermark_floor(&self, ms: u64) {
        if ms > self.watermark_floor.get() {
            self.watermark_floor.set(ms);
        }
    }

    pub(crate) fn arm_maint_reserve(&self) {
        for store in self.zones.borrow().values() {
            let mut store = store.borrow_mut();
            store.engine_mut().set_maint_reserve_enabled(true);
            store.engine_mut().set_device_dealloc(self.settings.device_dealloc_segments_per_tick > 0);
        }
    }

    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown.get()
    }

    pub(crate) fn force_close(&self) -> bool {
        self.force_close.get()
    }

    pub(crate) fn force_close_conns(&self) {
        self.force_close.set(true);
    }

    pub(crate) fn add_shutdown_notify(&self) -> Rc<reactor::Notify> {
        let n = Rc::new(reactor::Notify::new());
        self.shutdown_notifies.borrow_mut().push(n.clone());
        n
    }

    pub(crate) fn begin_shutdown(&self) {
        self.shutdown.set(true);
        self.sweep_notify.signal();
        self.logical_accounting_notify.signal();
        for n in self.shutdown_notifies.borrow().iter() {
            n.signal();
        }
    }

    pub(crate) fn register_conn(&self, out: &Rc<ServerOut>) {
        self.conn_outs.borrow_mut().push(Rc::downgrade(out));
    }

    pub(crate) fn wake_expired_conns(&self) {
        let now = std::time::Instant::now();
        if now.duration_since(self.transport_deadline_poll.get()) < Duration::from_millis(250) {
            return;
        }
        self.transport_deadline_poll.set(now);
        let mut outs = self.conn_outs.borrow_mut();
        outs.retain(|w| w.upgrade().is_some());
        for weak in outs.iter() {
            if let Some(out) = weak.upgrade() {
                if out.deadline_expired(now).is_some() {
                    let waker = out.waker.borrow_mut().take();
                    if let Some(waker) = waker {
                        waker.wake();
                    }
                }
            }
        }
    }

    pub(crate) fn wake_conns(&self) -> usize {
        let mut outs = self.conn_outs.borrow_mut();
        outs.retain(|w| w.upgrade().is_some());
        for w in outs.iter() {
            if let Some(out) = w.upgrade() {
                if let Some(waker) = out.waker.borrow_mut().take() {
                    waker.wake();
                }
            }
        }
        outs.len()
    }

    pub(crate) fn finalize_shutdown(&self) {
        self.drain_device_flushes();
        self.complete_pending_drops();
        for (zone_id, store) in self.zones.borrow().iter() {
            if let Err(e) = store.borrow_mut().sync() {
                log::error!(
                    "node {}: zone {zone_id} final sync failed during shutdown: {e}",
                    self.node_id
                );
                crate::observe::counters().server_errors.inc();
            }
        }
        let wm = self.watermark();
        self.set_watermark_floor(wm);
        if let Some(l) = &self.liveness {
            if let Err(e) = l.log.store(wm) {
                log::error!(
                    "node {}: persisting liveness watermark {wm} during shutdown failed: {e}",
                    self.node_id
                );
                crate::observe::counters().watermark_persist_errors.inc();
            }
        }
        log::info!(
            "node {}: graceful shutdown complete (engines synced, watermark {wm}ms)",
            self.node_id
        );
        crate::observe::counters().graceful_shutdowns.inc();
        let g = crate::observe::gauges();
        g.live_keys.adjust(self.published_live_keys.replace(0), 0);
        g.used_segments
            .adjust(self.published_used_segments.replace(0), 0);
        g.free_segments
            .adjust(self.published_free_segments.replace(0), 0);
        g.resident_runs
            .adjust(self.published_resident_runs.replace(0), 0);
        g.frozen_runs
            .adjust(self.published_frozen_runs.replace(0), 0);
        g.stranded_runs
            .adjust(self.published_stranded_runs.replace(0), 0);
        let old_memtable = self.published_memtable_bytes.replace(0);
        crate::ram_broker::charge(old_memtable, 0);
        g.memtable_bytes_total.adjust(old_memtable, 0);
        let old_runs = self.published_run_metadata_bytes.replace(0);
        crate::ram_broker::charge(old_runs, 0);
        g.run_metadata_resident_bytes.adjust(old_runs, 0);
        g.winner_directory_keys
            .adjust(self.published_winner_directory_keys.replace(0), 0);
        g.winner_directory_bytes
            .adjust(self.published_winner_directory_bytes.replace(0), 0);
        self.publish_key_filter([0; KEY_FILTER_GAUGES]);
        self.publish_run_meta([0; RUN_META_GAUGES]);
    }

    pub(crate) fn with_liveness(
        mut self,
        grace_ms: u64,
        log: Box<dyn crate::liveness::LivenessLog>,
    ) -> Self {
        if grace_ms > 0 {
            self.liveness = Some(Liveness { grace_ms, log });
            self.fenced.set(true);
            for store in self.zones.get_mut().values() {
                let mut s = store.borrow_mut();
                let had = s.gc_grace_ms();
                if had != 0 && had != grace_ms {
                    log::warn!(
                        "node {}: engine tombstone-GC grace ({had}ms) disagrees with \
                         the liveness lease grace ({grace_ms}ms); overriding to the \
                         lease's — they must be the same grace",
                        self.node_id
                    );
                }
                s.set_gc_grace_ms(grace_ms);
            }
            log::info!(
                "node {}: liveness enforcement on (gc_grace {grace_ms}ms); born fenced \
                 until verified against peers",
                self.node_id
            );
        }
        self
    }

    pub(crate) fn with_catalog_store(
        mut self,
        store: Box<dyn crate::catalog_store::CatalogStore + Send>,
    ) -> Self {
        self.catalog_store = Some(store);
        self.expect_range_maps();
        self
    }

    pub(crate) fn with_shard(mut self, index: usize, count: usize) -> Self {
        self.shard.set((index, count.max(1)));
        self.shard_write_counters = crate::observe::shard_write_counters(index);
        self
    }

    pub(crate) fn record_synced_writes(&self, writes: u64) {
        self.shard_write_counters.writes.add(writes);
        self.shard_write_counters.ops.inc();
    }

    pub(crate) fn shard(&self) -> (usize, usize) {
        self.shard.get()
    }

    pub(super) fn placement_stamp(&self) -> PlacementStamp {
        PlacementStamp {
            ring: self.ring.generation(),
            catalog: self.catalog.generation(),
            range_maps: self.range_maps.generation(),
            shard: self.shard.get(),
        }
    }

    pub(super) fn range_owner_cache<R>(
        &self,
        zone_id: ZoneId,
        num_ranges: usize,
        f: impl FnOnce(&mut [Option<bool>]) -> R,
    ) -> R {
        let stamp = self.placement_stamp();
        let mut owners = self.range_owners.borrow_mut();
        if owners.stamp != Some(stamp) {
            owners.stamp = Some(stamp);
            owners.zones.clear();
        }
        let zone = owners.zones.entry(zone_id).or_default();
        if zone.len() != num_ranges {
            zone.clear();
            zone.resize(num_ranges, None);
        }
        f(zone)
    }

    pub(crate) fn read_site(&self, zone: ZoneId) -> crate::integrity::ReadSite {
        crate::integrity::ReadSite {
            zone,
            shard: self.shard.get().0,
        }
    }

    pub(crate) fn sweep_every(&self) -> Duration {
        self.sweep_every.get()
    }

    pub(crate) fn set_sweep_every(&self, d: Duration) {
        self.sweep_every.set(d);
        crate::ram_broker::note_sweep_interval(d);
    }

    #[cfg(test)]
    pub(crate) fn incarnation(&self) -> Option<u128> {
        self.incarnation.get()
    }

    pub(super) fn admit_incarnation(&self, pushed: Option<u128>, op: &str) -> Result<(), String> {
        let bound = self.incarnation.get();
        match (bound, pushed) {
            (None, None) => Ok(()),
            (Some(bound), Some(pushed)) if bound == pushed => Ok(()),
            (None, Some(pushed)) => {
                if let Some(provider) = &self.provider {
                    if let Err(e) = provider.persist_incarnation(Some(pushed)) {
                        log::error!(
                            "node {} shard {}: persisting cluster incarnation {pushed:032x} \
                             failed; {op} refused and the id not adopted: {e}",
                            self.node_id,
                            self.shard.get().0
                        );
                        return Err(format!(
                            "node could not persist cluster incarnation {pushed:032x}: {e}"
                        ));
                    }
                }
                self.incarnation.set(Some(pushed));
                crate::observe::counters().control_incarnation_adoptions.inc();
                log::info!(
                    "node {} shard {}: bound to cluster incarnation {pushed:032x} by its first \
                     {op}; control pushes carrying any other incarnation are refused from now on",
                    self.node_id,
                    self.shard.get().0
                );
                Ok(())
            }
            (Some(bound), pushed) => {
                crate::observe::counters().control_incarnation_rejected.inc();
                let pushed = pushed.map_or_else(|| "none".to_string(), |id| format!("{id:032x}"));
                if incarnation_reject_log_due() {
                    log::error!(
                        "node {} shard {}: REFUSED {op} from a different cluster incarnation \
                         (node bound to {bound:032x}, push carries {pushed}); an obsolete or \
                         independently bootstrapped PD is pushing to this node. Fence it. If \
                         this node was restored into a new cluster, re-enrol it offline \
                         (`valsd reenrol`)",
                        self.node_id,
                        self.shard.get().0
                    );
                }
                Err(format!(
                    "cluster incarnation mismatch: node bound to {bound:032x}, push carries {pushed}"
                ))
            }
        }
    }

    pub(super) fn persist_catalog(&self) {
        if let Some(store) = &self.catalog_store {
            if let Err(e) = store.store(&self.catalog.borrow()) {
                log::error!(
                    "node {}: persisting the adopted zone catalog failed: {e}",
                    self.node_id
                );
                crate::observe::counters().catalog_persist_errors.inc();
            }
        }
    }

    pub(crate) fn liveness_grace_ms(&self) -> Option<u64> {
        self.liveness.as_ref().map(|l| l.grace_ms)
    }

    pub(crate) fn fenced(&self) -> bool {
        self.fenced.get()
    }

    pub(crate) fn load_persisted_watermark(&self) {
        if let Some(l) = &self.liveness {
            if let Some(wm) = l.log.load() {
                self.had_persisted_watermark.set(true);
                self.set_watermark_floor(wm);
                self.durable_gc_watermark.set(wm);
                for store in self.zones.borrow().values() {
                    store.borrow().advance_durable_gc_watermark(wm);
                }
            }
        }
    }

    pub(crate) fn persist_gc_watermark(&self, wall_ms: u64) -> bool {
        let Some(l) = &self.liveness else {
            return false;
        };
        let watermark = wall_ms.min(self.watermark());
        if watermark > self.durable_gc_watermark.get() {
            if let Err(e) = l.log.store(watermark) {
                log::error!(
                    "node {}: persisting tombstone-GC watermark {watermark} failed: {e}",
                    self.node_id
                );
                crate::observe::counters().watermark_persist_errors.inc();
                return false;
            }
            self.set_watermark_floor(watermark);
            self.durable_gc_watermark.set(watermark);
        }
        let durable = self.durable_gc_watermark.get();
        for store in self.zones.borrow().values() {
            store.borrow().advance_durable_gc_watermark(durable);
        }
        true
    }

    pub(crate) fn record_contact(&self, now_ms: u64) {
        self.record_contact_inner(now_ms, true);
    }

    pub(crate) fn record_contact_fenced(&self, now_ms: u64) {
        self.record_contact_inner(now_ms, false);
    }

    fn record_contact_inner(&self, now_ms: u64, unfence: bool) {
        self.last_contact.set(now_ms);
        let wm = self.watermark();
        self.set_watermark_floor(wm);
        if let Some(l) = &self.liveness {
            match l.log.store(wm) {
                Ok(()) => {
                    if wm > self.durable_gc_watermark.get() {
                        self.durable_gc_watermark.set(wm);
                    }
                }
                Err(e) => {
                    log::error!(
                        "node {}: persisting liveness watermark {wm} failed: {e}",
                        self.node_id
                    );
                    crate::observe::counters().watermark_persist_errors.inc();
                }
            }
        }
        if unfence && self.fenced.replace(false) {
            log::info!(
                "node {}: verified in contact with the cluster; unfenced (watermark {wm}ms)",
                self.node_id
            );
            crate::observe::counters().unfences.inc();
        }
    }

    pub(crate) fn begin_resync(&self) {
        self.resync_pending.set(true);
    }

    pub(crate) fn resync_pending(&self) -> bool {
        self.resync_pending.get()
    }

    pub(crate) fn finish_resync(&self, now_ms: u64) {
        self.resync_pending.set(false);
        log::info!(
            "node {}: post-wipe re-sync complete (every zone refilled from a peer)",
            self.node_id
        );
        self.record_contact(now_ms);
    }

    pub(crate) fn lone_unfence_allowed(&self) -> bool {
        if !self.had_persisted_watermark.get() {
            return true;
        }
        self.membership_adopted.get() && self.ring().borrow().members().count() <= 1
    }

    pub(crate) fn refence_if_stale(&self, now_ms: u64) {
        if let Some(grace) = self.liveness_grace_ms() {
            if crate::liveness::is_fenced(self.last_contact.get(), now_ms, grace)
                && !self.fenced.replace(true)
            {
                log::warn!(
                    "node {}: out of cluster contact for more than the GC grace \
                     ({grace}ms); fenced — refusing to serve as a read/anti-entropy \
                     source until re-verified",
                    self.node_id
                );
                crate::observe::counters().fence_trips.inc();
            }
        }
    }

    pub(crate) fn maintenance_paused_for_wipe(&self) -> bool {
        self.wipe_pending.get().is_some()
    }

    pub(crate) fn clear_wipe_pending(&self) {
        if self.wipe_pending.take().is_some() {
            crate::observe::gauges().wipe_pending.adjust(1, 0);
        }
    }

    pub(crate) fn liveness_wipe_wanted(&self, contact_possible: bool, peer_wms: &[u64]) -> bool {
        let wanted = contact_possible
            && self.fenced()
            && !self.resync_pending()
            && self.liveness_grace_ms().is_some_and(|grace| {
                crate::liveness::rejoin_decision(self.watermark(), peer_wms, grace)
                    == crate::liveness::RejoinDecision::WipeAndResync
            });
        if !wanted {
            self.clear_wipe_pending();
        }
        wanted
    }

    pub(crate) fn check_wipe_timeout(&self, now: std::time::Instant) {
        let Some(since) = self.wipe_pending.get() else { return; };
        if now.saturating_duration_since(since) < Duration::from_secs(600)
            || crate::integrity::fail_fast_requested()
        {
            return;
        }
        let stuck: Vec<_> = self.zones.borrow().iter().filter_map(|(zone, store)| {
            let store = store.borrow();
            (store.flush_in_flight() || store.compact_in_flight())
                .then_some((*zone, store.flush_in_flight(), store.compact_in_flight()))
        }).collect();
        log::error!(
            "node {}: liveness wipe maintenance drain exceeded 10 minutes; zones still in flight \
             (zone, flush, merge): {stuck:?}; requesting supervisor restart with exit code {}",
            self.node_id, crate::integrity::INTEGRITY_FAIL_FAST_EXIT_CODE
        );
        log::logger().flush();
        crate::integrity::request_fail_fast();
    }

    pub(crate) fn wipe_all_zones(
        &self,
        now_ms: u64,
        peer_wms: &[u64],
    ) -> Result<(), vals_store::EngineError> {
        let stores: Vec<_> = self.zones.borrow().iter().map(|(zone, store)| (*zone, store.clone())).collect();
        if stores.iter().any(|(_, s)| {
            let s = s.borrow();
            s.flush_in_flight() || s.compact_in_flight()
        }) {
            if !self.maintenance_paused_for_wipe() {
                self.wipe_pending.set(Some(std::time::Instant::now()));
                crate::observe::gauges().wipe_pending.adjust(0, 1);
            }
            self.check_wipe_timeout(std::time::Instant::now());
            return Err(vals_store::EngineError::MaintenanceInFlight);
        }
        let stats = crate::liveness::WipeStats {
            wall_ms: now_ms,
            local_watermark_ms: self.watermark(),
            max_peer_watermark_ms: peer_wms.iter().copied().max().unwrap_or(0),
            gc_grace_ms: self.liveness_grace_ms().unwrap_or(0),
            zones: stores.len(),
            keys: stores.iter().map(|(_, s)| s.borrow().tracked_keys()).sum(),
        };
        log::warn!(
            "node {}: liveness wipe — discarding all local data before re-sync \
             ({} keys across {} zones; local watermark {}ms, most-ahead peer {}ms, \
             gc_grace {}ms)",
            self.node_id,
            stats.keys,
            stats.zones,
            stats.local_watermark_ms,
            stats.max_peer_watermark_ms,
            stats.gc_grace_ms
        );
        if let Some(l) = &self.liveness {
            if let Err(e) = l.log.record_wipe(&stats) {
                log::error!(
                    "node {}: writing the durable wipe marker failed (wiping anyway): {e}",
                    self.node_id
                );
            }
        }
        crate::observe::counters().wipes.inc();
        for (zone_id, store) in stores {
            store.borrow_mut().clear()?;
            self.integrity_watch.borrow_mut().cleared(zone_id);
        }
        self.clear_wipe_pending();
        Ok(())
    }

    pub(crate) fn zone_config(&self, zone_id: ZoneId) -> Option<ZoneConfig> {
        self.catalog.borrow().get(zone_id).cloned()
    }

    pub(crate) fn zone_id_by_name(&self, name: &str) -> Option<ZoneId> {
        self.catalog.borrow().by_name(name)
    }

    pub(crate) fn kick_sweep(&self) {
        self.rebalance_pending.set(true);
        self.sweep_notify.signal();
    }

    pub(crate) fn note_sweep_pass(&self) {
        crate::observe::counters().sweep_passes.inc();
        self.sweep_pass_at.set(std::time::Instant::now());
        self.sweep_pass_count.set(self.sweep_pass_count.get() + 1);
    }

    pub(crate) fn note_sweep_sorted_zone_held(&self) {
        crate::observe::counters().sweep_sorted_zones_held.inc();
        self.sweep_held_count.set(self.sweep_held_count.get() + 1);
    }

    #[cfg(test)]
    pub(crate) fn sweep_pass_count(&self) -> u64 {
        self.sweep_pass_count.get()
    }

    #[cfg(test)]
    pub(crate) fn sweep_held_count(&self) -> u64 {
        self.sweep_held_count.get()
    }

    pub(crate) fn signal_periodic(&self) {
        self.sweep_notify.signal();
    }

    pub(crate) fn sweep_notified(&self) -> reactor::Notified<'_> {
        self.sweep_notify.notified()
    }

    pub(crate) fn kick_logical_accounting(&self) {
        self.logical_accounting_notify.signal();
    }

    pub(crate) fn logical_accounting_notified(&self) -> reactor::Notified<'_> {
        self.logical_accounting_notify.notified()
    }

    pub(crate) fn take_rebalance_pending(&self) -> bool {
        self.rebalance_pending.replace(false)
    }

    pub(crate) fn sweep_zone_ids(&self, eager: bool) -> Vec<ZoneId> {
        let mut ids: Vec<ZoneId> = self.zones.borrow().keys().copied().collect();
        if eager {
            for (id, cfg) in self.catalog.borrow().iter() {
                let in_scope = cfg.scope.as_ref().is_none_or(|s| s.contains(&self.node_id));
                if in_scope {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    pub(crate) fn zoned(
        node_id: NodeId,
        zone0_store: VersionedStore<D>,
        provider: Rc<dyn ZoneStore<D>>,
        catalog: Catalog,
        ring: Ring,
    ) -> Self {
        let dev = zone0_store.device_handle();
        dev.bind_io_owner();
        let mut zones = FastMap::default();
        let mut devices = FastMap::default();
        zones.insert(DEFAULT_ZONE, Rc::new(RefCell::new(zone0_store)));
        devices.insert(DEFAULT_ZONE, dev);
        let mut uncataloged_sorted = Vec::new();
        for zone_id in provider.existing() {
            if zone_id == DEFAULT_ZONE {
                continue;
            }
            let cataloged = catalog.get(zone_id).map(|c| c.ordering);
            let Some(ordering) = cataloged.or_else(|| provider.zone_ordering(zone_id)) else {
                continue;
            };
            match provider.open(zone_id, ordering) {
                Ok(store) => {
                    log::info!("node {node_id}: re-opened persisted zone {zone_id} at boot");
                    devices.insert(zone_id, store.device_handle());
                    zones.insert(zone_id, Rc::new(RefCell::new(store)));
                    if cataloged.is_none() && ordering == crate::zone::Ordering::Sorted {
                        uncataloged_sorted.push(zone_id);
                    }
                }
                Err(e) => {
                    log::error!(
                        "node {node_id}: re-opening persisted zone {zone_id} at boot \
                         failed (left for lazy materialization): {e}"
                    );
                }
            }
        }
        let engine_count = zones.len() as u64;
        crate::ram::add_zone_engines(engine_count);
        if !uncataloged_sorted.is_empty() {
            log::info!(
                "node {node_id}: holding re-opened sorted zones {uncataloged_sorted:?} (writes, \
                 reads and sweep) until the first catalog and range-map pushes"
            );
        }
        let shared = Self::from_parts(node_id, zones, devices, Some(provider), catalog, ring);
        *shared.uncataloged_sorted.borrow_mut() = uncataloged_sorted;
        shared
    }

    pub(crate) fn zone_segment_stats(&self) -> Vec<(ZoneId, u64, u64, u64, usize)> {
        let mut stats = self
            .zones
            .borrow()
            .iter()
            .map(|(&zone_id, store)| {
                let store = store.borrow();
                (
                    zone_id,
                    store.engine().used_segments(),
                    store.engine().free_segments(),
                    store.live_keys(),
                    store.run_count(),
                )
            })
            .collect::<Vec<_>>();
        stats.sort_unstable_by_key(|stats| stats.0);
        stats
    }

    fn publish_engine_metrics(&self, zone_id: ZoneId, engine: &vals_store::Engine<D>) {
        *self.shard_write_counters.lookups.lock().unwrap() = vals_store::attribution::lookup_snapshot();
        crate::observe::set_engine_metrics(self.shard().0, zone_id, crate::observe::EngineMetrics {
            resident_runs: engine.run_count() as u64,
            frozen_runs: engine.frozen_run_count() as u64,
            stranded_runs: engine.stranded_run_count() as u64,
            free_fraction_ppm: (engine.free_fraction() * 1_000_000.0) as u64,
            rt_records: engine.range_tombstone_record_count() as u64,
        });
    }

    pub(crate) fn sample_gauges(&self) {
        let mut live_keys = 0u64;
        let mut used_segments = 0u64;
        let mut free_segments = 0u64;
        let mut min_zone_free_segments = u64::MAX;
        let mut resident_runs = 0u64;
        let mut frozen_runs = 0u64;
        let mut stranded_runs = 0u64;
        let mut max_run_bytes = 0u64;
        let mut memtable_bytes = 0u64;
        let mut run_metadata_bytes = 0u64;
        let mut winner_directory_keys = 0u64;
        let mut winner_directory_bytes = 0u64;
        let mut shadowed_bytes = 0u64;
        let mut shadow_folds = 0u64;
        let mut shadow_fold_disarms = 0u64;
        let mut key_filter = [0u64; KEY_FILTER_GAUGES];
        let mut key_filter_maxima = [0u64; 5];
        let mut run_meta = [0u64; RUN_META_GAUGES];
        for (&zone_id, store) in self.zones.borrow().iter() {
            let s = store.borrow();
            self.publish_engine_metrics(zone_id, s.engine());
            let zone_free = s.engine().free_segments();
            live_keys += s.live_keys();
            used_segments += s.engine().used_segments();
            free_segments += zone_free;
            min_zone_free_segments = min_zone_free_segments.min(zone_free);
            resident_runs += s.run_count() as u64;
            frozen_runs += s.engine().frozen_run_count() as u64;
            stranded_runs += s.engine().stranded_run_count() as u64;
            max_run_bytes = max_run_bytes.max(s.engine().max_run_bytes());
            memtable_bytes += s.memtable_bytes();
            let capacity = s.engine().capacity_stats();
            run_metadata_bytes += capacity.run_resident_bytes;
            winner_directory_keys += s.engine().winner_directory_len();
            winner_directory_bytes += s.engine().winner_directory_bytes();
            shadowed_bytes += s.engine().shadowed_bytes_estimate_cached();
            shadow_folds += s.engine().shadow_folds();
            shadow_fold_disarms += s.engine().shadow_fold_disarms();
            let kf = s.engine().key_filter_stats();
            for (slot, value) in key_filter_maxima.iter_mut().zip([
                kf.sub_filters, (kf.estimated_fpr * 1_000_000.0).ceil() as u64,
                kf.uncovered_runs, kf.rebuild_wait_seconds, kf.rebuild_open_seconds,
            ]) {
                *slot = (*slot).max(value);
            }
            for (slot, value) in key_filter.iter_mut().zip(key_filter_gauge_values(&kf)) {
                *slot += value;
            }
            for (slot, value) in run_meta.iter_mut().zip(run_meta_gauge_values(&capacity)) {
                *slot += value;
            }
        }
        self.publish_key_filter(key_filter);
        self.publish_run_meta(run_meta);
        if min_zone_free_segments == u64::MAX {
            min_zone_free_segments = 0;
        }
        let g = crate::observe::gauges();
        g.live_keys
            .adjust(self.published_live_keys.replace(live_keys), live_keys);
        g.used_segments.adjust(
            self.published_used_segments.replace(used_segments),
            used_segments,
        );
        g.free_segments.adjust(
            self.published_free_segments.replace(free_segments),
            free_segments,
        );
        g.resident_runs.adjust(
            self.published_resident_runs.replace(resident_runs),
            resident_runs,
        );
        g.frozen_runs
            .adjust(self.published_frozen_runs.replace(frozen_runs), frozen_runs);
        g.stranded_runs
            .adjust(self.published_stranded_runs.replace(stranded_runs), stranded_runs);
        g.shadowed_value_bytes_est.adjust(
            self.published_shadowed_bytes.replace(shadowed_bytes),
            shadowed_bytes,
        );
        g.shadow_folds.adjust(
            self.published_shadow_folds.replace(shadow_folds),
            shadow_folds,
        );
        g.shadow_fold_disarms.adjust(
            self.published_shadow_fold_disarms.replace(shadow_fold_disarms),
            shadow_fold_disarms,
        );
        let id = self as *const Self as usize;
        g.min_zone_free_segments
            .set(publish_zone_free_min(id, Some(min_zone_free_segments)));
        g.max_run_bytes.set(publish_max_run_bytes(id, Some(max_run_bytes)));
        publish_key_filter_maxima(id, Some(key_filter_maxima));
        let old_memtable = self.published_memtable_bytes.replace(memtable_bytes);
        crate::ram_broker::charge(old_memtable, memtable_bytes);
        g.memtable_bytes_total.adjust(old_memtable, memtable_bytes);
        let old_runs = self.published_run_metadata_bytes.replace(run_metadata_bytes);
        crate::ram_broker::charge(old_runs, run_metadata_bytes);
        g.run_metadata_resident_bytes.adjust(old_runs, run_metadata_bytes);
        g.winner_directory_keys.adjust(
            self.published_winner_directory_keys
                .replace(winner_directory_keys),
            winner_directory_keys,
        );
        g.winner_directory_bytes.adjust(
            self.published_winner_directory_bytes
                .replace(winner_directory_bytes),
            winner_directory_bytes,
        );
        let pending_drop_age = self
            .pending_drops
            .borrow()
            .iter()
            .map(|p| p.since.elapsed().as_secs())
            .max()
            .unwrap_or(0);
        g.zone_drop_pending_age_seconds
            .set(publish_zone_drop_pending_age(id, Some(pending_drop_age)));
        g.sweep_pass_age_seconds.set(publish_sweep_pass_age(
            id,
            Some(self.sweep_pass_at.get().elapsed().as_secs()),
        ));
        if let Some(rss) = crate::observe::process_resident_bytes() {
            g.process_resident_bytes.set(rss);
        }
    }

    fn publish_run_meta(&self, values: [u64; RUN_META_GAUGES]) {
        let old = self.published_run_meta.replace(values);
        for ((gauge, old), new) in run_meta_gauges().iter().zip(old).zip(values) {
            gauge.adjust(old, new);
        }
    }

    fn publish_key_filter(&self, values: [u64; KEY_FILTER_GAUGES]) {
        let old = self.published_key_filter.replace(values);
        for ((gauge, old), new) in key_filter_gauges().iter().zip(old).zip(values) {
            gauge.adjust(old, new);
        }
    }

    pub(crate) fn begin_maint_job(&self, zone_id: ZoneId) {
        let mut jobs = self.maint_jobs.borrow_mut();
        *jobs.entry(zone_id).or_default() += 1;
    }

    pub(crate) fn begin_reserved_maint_job(
        &self, zone_id: ZoneId, reservation: crate::ram_broker::Reservation,
    ) {
        self.begin_maint_job(zone_id);
        self.maint_reservations.borrow_mut().entry(zone_id).or_default().push(reservation);
    }

    pub(crate) fn finish_reserved_maint_job(
        &self, zone_id: ZoneId, class: crate::ram_broker::Class,
    ) {
        let mut reservations = self.maint_reservations.borrow_mut();
        if let Some(held) = reservations.get_mut(&zone_id) {
            if let Some(pos) = held.iter().position(|reservation| reservation.class() == class) {
                held.swap_remove(pos);
            }
            if held.is_empty() { reservations.remove(&zone_id); }
        }
        drop(reservations);
        self.finish_maint_job(zone_id);
        if let Some(store) = self.zones.borrow().get(&zone_id) {
            self.publish_engine_metrics(zone_id, store.borrow().engine());
        }
    }

    pub(crate) fn finish_maint_job(&self, zone_id: ZoneId) {
        let mut jobs = self.maint_jobs.borrow_mut();
        if let Some(count) = jobs.get_mut(&zone_id) {
            *count -= 1;
            if *count == 0 {
                jobs.remove(&zone_id);
            }
        }
        drop(jobs);
        self.complete_pending_drops();
    }

    pub(crate) fn begin_flush_build(&self, zone_id: ZoneId, bytes: u64) {
        let old = self
            .flush_builds
            .borrow_mut()
            .insert(zone_id, bytes)
            .unwrap_or(0);
        crate::observe::gauges().flush_build_bytes.adjust(old, bytes);
    }

    pub(crate) fn finish_flush_build(&self, zone_id: ZoneId) {
        if let Some(bytes) = self.flush_builds.borrow_mut().remove(&zone_id) {
            crate::observe::gauges().flush_build_bytes.adjust(bytes, 0);
        }
    }

    pub(crate) fn zone_store(
        &self,
        zone_id: ZoneId,
    ) -> Result<Rc<RefCell<VersionedStore<D>>>, String> {
        if let Some(s) = self.zones.borrow().get(&zone_id) {
            return Ok(s.clone());
        }
        if self
            .pending_drops
            .borrow()
            .iter()
            .any(|p| p.zone_id == zone_id)
        {
            crate::observe::counters().zone_teardown_rejections.inc();
            return Err(format!("zone {zone_id} teardown pending; retry"));
        }
        let provider = self
            .provider
            .as_ref()
            .ok_or_else(|| format!("no zone provider; zone {zone_id} unavailable"))?;
        let ordering = self
            .catalog
            .borrow()
            .get(zone_id)
            .map(|c| c.ordering)
            .ok_or_else(|| format!("unknown zone {zone_id}"))?;
        let mut store = provider
            .open(zone_id, ordering)
            .map_err(|e| format!("open zone {zone_id}: {e}"))?;
        if let Some(l) = &self.liveness {
            store.set_gc_grace_ms(l.grace_ms);
            store.advance_durable_gc_watermark(self.durable_gc_watermark.get());
        }
        store.engine_mut().set_maint_reserve_enabled(true);
        store.engine_mut().set_device_dealloc(self.settings.device_dealloc_segments_per_tick > 0);
        store.set_seed_driver(self.seed_driver.get());
        self.integrity_watch.borrow_mut().cleared(zone_id);
        let dev = store.device_handle();
        let rc = Rc::new(RefCell::new(store));
        self.zones.borrow_mut().insert(zone_id, rc.clone());
        self.devices.borrow_mut().insert(zone_id, dev);
        self.registered_engines.set(self.registered_engines.get() + 1);
        crate::ram::add_zone_engines(1);
        Ok(rc)
    }

    pub(crate) fn prepare_default_zone_swap(
        &self,
        new_dev: D,
        cfg: vals_store::Config,
        node: NodeId,
    ) -> Result<VersionedStore<D>, vals_store::EngineError> {
        let old_rc = self
            .zones
            .borrow()
            .get(&DEFAULT_ZONE)
            .expect("default zone is always materialized")
            .clone();
        old_rc.borrow_mut().sync()?;
        let mut new_store = VersionedStore::new(vals_store::Engine::create(new_dev, cfg)?, node)?;
        {
            let old = old_rc.borrow();
            for key in old.keys()? {
                if let Some(stored) = old.stored(&key)? {
                    new_store.apply(&key, stored.value.as_deref(), stored.version)?;
                }
            }
        }
        if let Some(l) = &self.liveness {
            new_store.set_gc_grace_ms(l.grace_ms);
            new_store.advance_durable_gc_watermark(self.durable_gc_watermark.get());
        }
        new_store.sync()?;
        Ok(new_store)
    }

    pub(crate) fn commit_default_zone_swap(&self, mut new_store: VersionedStore<D>) {
        new_store.engine_mut().set_device_dealloc(self.settings.device_dealloc_segments_per_tick > 0);
        new_store.set_seed_driver(self.seed_driver.get());
        let old_rc = self
            .zones
            .borrow()
            .get(&DEFAULT_ZONE)
            .expect("default zone is always materialized")
            .clone();
        let dev = new_store.device_handle();
        crate::ram_broker::clear_maintenance_waiters(Rc::as_ptr(&old_rc) as usize);
        *old_rc.borrow_mut() = new_store;
        self.integrity_watch.borrow_mut().cleared(DEFAULT_ZONE);
        self.devices.borrow_mut().insert(DEFAULT_ZONE, dev);
    }

    pub(super) fn drop_zone(&self, zone_id: ZoneId) {
        if zone_id == DEFAULT_ZONE {
            return;
        }
        log::warn!(
            "node {}: tearing down zone {zone_id} (left the catalog); its storage will be freed",
            self.node_id
        );
        crate::observe::counters().zones_dropped.inc();
        self.range_load.borrow_mut().counts.remove(&zone_id);
        self.integrity_watch.borrow_mut().cleared(zone_id);
        let store = self.zones.borrow_mut().remove(&zone_id);
        crate::observe::remove_engine_metrics(self.shard().0, zone_id);
        if let Some(store) = &store {
            crate::ram_broker::clear_maintenance_waiters(Rc::as_ptr(store) as usize);
        }
        if store.is_some() {
            self.registered_engines
                .set(self.registered_engines.get().saturating_sub(1));
            crate::ram::remove_zone_engines(1);
        }
        let dev = self.devices.borrow_mut().remove(&zone_id);
        match (store, dev) {
            (Some(store), Some(dev)) => {
                self.pending_drops.borrow_mut().push(PendingDrop {
                    zone_id,
                    store,
                    dev,
                    since: std::time::Instant::now(),
                    last_warned: None,
                });
                self.complete_pending_drops();
                if self
                    .pending_drops
                    .borrow()
                    .iter()
                    .any(|p| p.zone_id == zone_id)
                {
                    crate::observe::counters().zone_drops_deferred.inc();
                }
            }
            _ => self.release_zone_storage(zone_id),
        }
    }

    pub(crate) fn complete_pending_drops(&self) {
        if self.pending_drops.borrow().is_empty() {
            return;
        }
        let ready: Vec<PendingDrop<D>> = {
            let now = std::time::Instant::now();
            let maint_jobs = self.maint_jobs.borrow();
            let mut pending = self.pending_drops.borrow_mut();
            for p in pending.iter_mut() {
                let refs = Rc::strong_count(&p.store).saturating_sub(1);
                let jobs = maint_jobs.get(&p.zone_id).copied().unwrap_or(0);
                if (refs > 0 || jobs > 0)
                    && p.last_warned.is_none_or(|last| {
                        now.saturating_duration_since(last) >= Duration::from_secs(60)
                    })
                {
                    let age = now.saturating_duration_since(p.since);
                    log::warn!(
                        "node {}: zone {} storage teardown still waiting after {age:?} for {refs} engine reference(s) and {jobs} maintenance job(s) to drain",
                        self.node_id,
                        p.zone_id,
                    );
                    p.last_warned = Some(now);
                }
            }
            let (ready, keep) = pending.drain(..).partition(|p| {
                Rc::strong_count(&p.store) == 1 && !maint_jobs.contains_key(&p.zone_id)
            });
            *pending = keep;
            ready
        };
        for p in ready {
            let zone_id = p.zone_id;
            drop(p);
            self.release_zone_storage(zone_id);
        }
    }

    fn release_zone_storage(&self, zone_id: ZoneId) {
        if let Some(p) = &self.provider {
            if let Err(e) = p.drop_zone(zone_id) {
                log::error!(
                    "node {}: releasing zone {zone_id}'s storage failed (region may leak): {e}",
                    self.node_id
                );
                crate::observe::counters().zone_drop_errors.inc();
            }
        }
    }

    pub(crate) fn zone_is_current(
        &self,
        zone_id: ZoneId,
        store: &Rc<RefCell<VersionedStore<D>>>,
    ) -> bool {
        self.zones
            .borrow()
            .get(&zone_id)
            .is_some_and(|cur| Rc::ptr_eq(cur, store))
    }

    pub(super) fn publish_domain_flushes(&self) {
        self.domain_flushes.borrow_mut().retain_mut(DomainFlush::publish_if_done);
    }

    fn drain_device_flushes(&self) {
        let started = std::time::Instant::now();
        let limit = shutdown_flush_drain();
        loop {
            self.poll_devices();
            self.publish_domain_flushes();
            let left = self.domain_flushes.borrow().len();
            if left == 0 {
                return;
            }
            if started.elapsed() > limit {
                log::error!(
                    "node {}: {left} device flush(es) still in flight after {limit:?} of shutdown drain",
                    self.node_id
                );
                return;
            }
            std::hint::spin_loop();
        }
    }

    pub(super) fn poll_devices(&self) {
        for dev in self.devices.borrow().values() {
            dev.poll_device();
        }
        for p in self.pending_drops.borrow().iter() {
            p.dev.poll_device();
        }
    }
}

impl<D: BlockDevice> Drop for ServerShared<D> {
    fn drop(&mut self) {
        self.clear_wipe_pending();
        for zone in self.zones.get_mut().keys() {
            crate::observe::remove_engine_metrics(self.shard.get().0, *zone);
        }
        for store in self.zones.get_mut().values() {
            crate::ram_broker::clear_maintenance_waiters(Rc::as_ptr(store) as usize);
        }
        for pending in self.pending_drops.get_mut().iter() {
            crate::ram_broker::clear_maintenance_waiters(Rc::as_ptr(&pending.store) as usize);
        }
        let engines = self.registered_engines.replace(0);
        crate::ram::remove_zone_engines(engines);
        let flush_bytes = self.flush_builds.get_mut().values().copied().sum();
        crate::observe::gauges()
            .flush_build_bytes
            .adjust(flush_bytes, 0);
        let g = crate::observe::gauges();
        g.live_keys.adjust(self.published_live_keys.replace(0), 0);
        g.used_segments
            .adjust(self.published_used_segments.replace(0), 0);
        g.free_segments
            .adjust(self.published_free_segments.replace(0), 0);
        g.resident_runs
            .adjust(self.published_resident_runs.replace(0), 0);
        g.frozen_runs
            .adjust(self.published_frozen_runs.replace(0), 0);
        g.stranded_runs
            .adjust(self.published_stranded_runs.replace(0), 0);
        let id = self as *const Self as usize;
        g.min_zone_free_segments
            .set(publish_zone_free_min(id, None));
        g.max_run_bytes.set(publish_max_run_bytes(id, None));
        publish_key_filter_maxima(id, None);
        g.zone_drop_pending_age_seconds
            .set(publish_zone_drop_pending_age(id, None));
        g.sweep_pass_age_seconds
            .set(publish_sweep_pass_age(id, None));
        let old_memtable = self.published_memtable_bytes.replace(0);
        crate::ram_broker::charge(old_memtable, 0);
        g.memtable_bytes_total.adjust(old_memtable, 0);
        let old_runs = self.published_run_metadata_bytes.replace(0);
        crate::ram_broker::charge(old_runs, 0);
        g.run_metadata_resident_bytes.adjust(old_runs, 0);
        g.winner_directory_keys
            .adjust(self.published_winner_directory_keys.replace(0), 0);
        g.winner_directory_bytes
            .adjust(self.published_winner_directory_bytes.replace(0), 0);
        self.publish_key_filter([0; KEY_FILTER_GAUGES]);
        self.publish_run_meta([0; RUN_META_GAUGES]);
    }
}


pub(super) const KEY_FILTER_GAUGES: usize = 25;

pub(super) const RUN_META_GAUGES: usize = 7;

fn run_meta_gauges() -> [&'static crate::observe::Gauge; RUN_META_GAUGES] {
    let g = crate::observe::gauges();
    [
        &g.run_fence_resident_bytes,
        &g.run_filter_resident_bytes,
        &g.run_entries,
        &g.run_fence_payload_bytes,
        &g.run_filter_payload_bytes,
        &g.memtable_allocated_bytes_total,
        &g.flush_protection_bytes,
    ]
}

fn run_meta_gauge_values(c: &vals_store::CapacityStats) -> [u64; RUN_META_GAUGES] {
    [
        c.fence_resident_bytes,
        c.run_filter_resident_bytes,
        c.run_entries,
        c.fence_payload_bytes,
        c.run_filter_payload_bytes,
        c.mutable_allocated_bytes,
        c.flush_protection_bytes,
    ]
}

fn key_filter_gauges() -> [&'static crate::observe::Gauge; KEY_FILTER_GAUGES] {
    let g = crate::observe::gauges();
    [
        &g.key_filter_probes,
        &g.key_filter_negatives,
        &g.key_filter_run_probes,
        &g.key_filter_sub_filters,
        &g.key_filter_bytes,
        &g.key_filter_resident_bytes,
        &g.key_filter_retired_bytes,
        &g.key_filter_retired_resident_bytes,
        &g.key_filter_inserted_keys,
        &g.key_filter_capacity_keys,
        &g.key_filter_rebuild_bytes,
        &g.key_filter_rebuild_resident_bytes,
        &g.key_filter_rebuilding,
        &g.key_filter_rebuild_covered_runs,
        &g.key_filter_rebuilds_started,
        &g.key_filter_rebuilds_completed,
        &g.key_filter_rebuilds_abandoned,
        &g.key_filter_rebuild_peak_bytes,
        &g.key_filter_uncovered_runs,
        &g.key_filter_uncovered_run_bytes,
        &g.key_filter_est_fpr_ppm_sum,
        &g.key_filter_resets,
        &g.key_filter_cover_scan_failures,
        &g.key_filter_cover_scan_mismatches,
        &g.key_filter_cover_scan_backoff_runs,
    ]
}

fn key_filter_gauge_values(s: &vals_store::KeyFilterStats) -> [u64; KEY_FILTER_GAUGES] {
    [
        s.probes,
        s.negatives,
        s.run_probes,
        s.sub_filters,
        s.filter_bytes,
        s.resident_bytes,
        s.retired_bytes,
        s.retired_resident_bytes,
        s.inserted_keys,
        s.capacity_keys,
        s.rebuild_bytes,
        s.rebuild_resident_bytes,
        s.rebuilding,
        s.rebuild_covered_runs,
        s.rebuilds_started,
        s.rebuilds_completed,
        s.rebuilds_abandoned,
        s.rebuild_peak_bytes,
        s.uncovered_runs,
        s.uncovered_run_bytes,
        (s.estimated_fpr * 1_000_000.0).round() as u64,
        s.resets,
        s.cover_scan_failures,
        s.cover_scan_mismatches,
        s.cover_scan_backoff_runs,
    ]
}

fn incarnation_reject_log_due() -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    static LAST_MS: AtomicU64 = AtomicU64::new(0);
    let now = crate::hlc::physical_now_ms();
    let last = LAST_MS.load(Ordering::Relaxed);
    (last == 0 || now.saturating_sub(last) >= 10_000)
        && LAST_MS
            .compare_exchange(last, now.max(1), Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
}
