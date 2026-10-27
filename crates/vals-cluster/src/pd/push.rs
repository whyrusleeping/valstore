use super::*;

const PUSH_TIMEOUT: Duration = Duration::from_secs(1);

pub(super) const DEFAULT_RANGE_SPLIT_POLL_MS: u64 = 5000;
pub(super) const RANGE_ENTRY_ESTIMATE_TTL_POLLS: u64 = 3;

pub(super) const DEFAULT_RANGE_SPLIT_THRESHOLD: u64 = 1_000_000;

pub(super) const DEFAULT_RANGE_SPLITS_PER_POLL: usize = 16;

pub(super) const DEFAULT_RANGE_LOAD_SPLIT_OPS: u64 = 0;

pub(super) const DEFAULT_RANGE_MERGE_THRESHOLD: u64 = 0;

pub(super) const DEFAULT_SINGLE_RANGE_WARN_ENTRIES: u64 = 1_000_000;

pub(super) const DEFAULT_RANGE_CONCENTRATION_WARN_PCT: u64 = 80;

pub(super) const DEFAULT_BALANCE_HIGH_PCT: u64 = 120;
pub(super) const DEFAULT_BALANCE_LOW_PCT: u64 = 105;
pub(super) const DEFAULT_BALANCE_MIN_IMPROVEMENT_PCT: u64 = 5;
pub(super) const DEFAULT_BALANCE_TOKEN_SEARCH_LIMIT: u64 = 4096;

pub(super) const SINGLE_RANGE_WARN_EVERY: Duration = Duration::from_secs(60);

pub(super) const BEACON_INTERVAL: Duration = Duration::from_secs(1);

const BEACON_TIMEOUT: Duration = Duration::from_millis(250);

fn probe_config_epochs(addr: SocketAddr, with_catalog: bool) -> io::Result<(u64, Option<u64>, u64)> {
    let mut s = TcpStream::connect_timeout(&addr, BEACON_TIMEOUT)?;
    s.set_read_timeout(Some(BEACON_TIMEOUT))?;
    s.set_write_timeout(Some(BEACON_TIMEOUT))?;
    let negotiated = crate::proto::client_handshake_blocking(&mut s)?;
    let Response::Members { epoch: ring_epoch, .. } = blocking_request_on_stream(&mut s, &Request::Members)? else {
        return Err(io::Error::from(io::ErrorKind::InvalidData));
    };
    let catalog_epoch = if with_catalog {
        let Response::Zones { epoch, .. } = blocking_request_on_stream(&mut s, &Request::Zones)? else {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        };
        Some(epoch)
    } else {
        None
    };
    Ok((ring_epoch, catalog_epoch, negotiated.features))
}

pub(super) struct BeaconProbe {
    node: NodeId,
    addr: SocketAddr,
    at: u64,
    zones_at: Option<u64>,
    result: io::Result<(u64, Option<u64>, u64)>,
}

pub(super) fn beacon_probes(
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    confirmed: &HashMap<SocketAddr, u64>,
    confirmed_zones: &HashMap<SocketAddr, u64>,
) -> Vec<(NodeId, SocketAddr, u64, Option<u64>)> {
    data_nodes
        .iter()
        .flat_map(|(node, addrs)| addrs.iter().map(move |addr| (*node, *addr)))
        .filter_map(|(node, addr)| {
            let at = *confirmed.get(&addr)?;
            Some((node, addr, at, confirmed_zones.get(&addr).copied()))
        })
        .collect()
}

pub(super) fn run_beacon_probes(
    probes: Vec<(NodeId, SocketAddr, u64, Option<u64>)>,
) -> Vec<BeaconProbe> {
    thread::scope(|scope| {
        let handles: Vec<_> = probes
            .into_iter()
            .map(|(node, addr, at, zones_at)| {
                let probe = scope.spawn(move || probe_config_epochs(addr, zones_at.is_some()));
                (node, addr, at, zones_at, probe)
            })
            .collect();
        handles
            .into_iter()
            .map(|(node, addr, at, zones_at, probe)| BeaconProbe {
                node,
                addr,
                at,
                zones_at,
                result: probe
                    .join()
                    .unwrap_or_else(|_| Err(io::Error::other("beacon probe panicked"))),
            })
            .collect()
    })
}

pub(super) fn apply_beacon(
    probes: Vec<BeaconProbe>,
    confirmed: &mut HashMap<SocketAddr, u64>,
    confirmed_zones: &mut HashMap<SocketAddr, u64>,
    confirmed_range_maps: &mut HashMap<SocketAddr, u64>,
    range_map_features: &mut HashMap<SocketAddr, u64>,
) -> Vec<NodeId> {
    let mut restarted = Vec::new();
    for BeaconProbe { node, addr, at, zones_at, result } in probes {
        match result {
            Ok((reported, _, _)) if reported < at => {
                log::warn!(
                    "pd: node {node} shard {addr} reports ring epoch {reported} < \
                     its confirmed {at} (restarted?); re-pushing config"
                );
            }
            Ok((_, Some(reported), _)) if zones_at.is_some_and(|zones_at| reported < zones_at) => {
                log::warn!(
                    "pd: node {node} shard {addr} reports zone-catalog epoch {reported} < \
                     its confirmed {} (restarted behind a membership push?); re-pushing config",
                    zones_at.unwrap_or_default()
                );
            }
            Ok((_, _, features)) => {
                range_map_features.insert(addr, features);
                continue;
            }
            Err(_) => {
                range_map_features.remove(&addr);
                continue;
            }
        }
        crate::observe::counters().pd_repushes.inc();
        confirmed.remove(&addr);
        confirmed_zones.remove(&addr);
        confirmed_range_maps.remove(&addr);
        range_map_features.remove(&addr);
        if !restarted.contains(&node) {
            restarted.push(node);
        }
    }
    restarted
}

#[cfg(test)]
pub(super) fn beacon_repush(
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    confirmed: &mut HashMap<SocketAddr, u64>,
    confirmed_zones: &mut HashMap<SocketAddr, u64>,
    confirmed_range_maps: &mut HashMap<SocketAddr, u64>,
    range_map_features: &mut HashMap<SocketAddr, u64>,
) -> Vec<NodeId> {
    let probes = run_beacon_probes(beacon_probes(data_nodes, confirmed, confirmed_zones));
    apply_beacon(probes, confirmed, confirmed_zones, confirmed_range_maps, range_map_features)
}

pub(super) fn range_maps_confirmed_at(
    confirmed_range_maps: &HashMap<SocketAddr, u64>,
    addr: &SocketAddr,
    epoch: u64,
) -> bool {
    epoch == 0 || confirmed_range_maps.get(addr).is_some_and(|&at| at >= epoch)
}

pub(super) const PUSH_RETRY_MIN: Duration = Duration::from_millis(50);
const PUSH_RETRY_MAX: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
pub(super) struct PushRetry {
    pub(super) epoch: u64,
    pub(super) failures: u32,
    pub(super) next_attempt: std::time::Instant,
}

impl PushRetry {
    pub(super) fn immediate(epoch: u64, now: std::time::Instant) -> Self {
        PushRetry {
            epoch,
            failures: 0,
            next_attempt: now,
        }
    }

    pub(super) fn failed(&mut self, now: std::time::Instant) {
        let shift = self.failures.min(6);
        let delay = PUSH_RETRY_MIN
            .saturating_mul(1u32 << shift)
            .min(PUSH_RETRY_MAX);
        self.failures = self.failures.saturating_add(1);
        self.next_attempt = now + delay;
    }
}

fn push_due(
    epoch: u64,
    addr: SocketAddr,
    confirmed: &HashMap<SocketAddr, u64>,
    retries: &mut HashMap<SocketAddr, PushRetry>,
    now: std::time::Instant,
) -> bool {
    if confirmed.get(&addr).is_some_and(|&at| at >= epoch) {
        retries.remove(&addr);
        return false;
    }
    let retry = retries
        .entry(addr)
        .or_insert_with(|| PushRetry::immediate(epoch, now));
    if retry.epoch != epoch {
        *retry = PushRetry::immediate(epoch, now);
    }
    now >= retry.next_attempt
}

fn retry_failed(retries: &mut HashMap<SocketAddr, PushRetry>, addr: SocketAddr, epoch: u64) {
    if let Some(retry) = retries.get_mut(&addr).filter(|retry| retry.epoch == epoch) {
        retry.failed(std::time::Instant::now());
    }
}

fn apply_push_result(
    kind: &'static str,
    addr: SocketAddr,
    epoch: u64,
    result: io::Result<u64>,
    confirmed: &mut HashMap<SocketAddr, u64>,
    retries: &mut HashMap<SocketAddr, PushRetry>,
) {
    match result {
        Ok(reported) if reported >= epoch => {
            confirmed.insert(addr, reported);
            retries.remove(&addr);
            if push_failing(kind, addr, false) {
                log::info!(
                    "pd: {kind} push to {addr} recovered (confirmed at epoch {reported})"
                );
            }
        }
        Ok(_) => retry_failed(retries, addr, epoch),
        Err(e) => {
            crate::observe::counters().pd_push_failures.inc();
            retry_failed(retries, addr, epoch);
            if push_failing(kind, addr, true) {
                log::warn!(
                    "pd: {kind} push (epoch {epoch}) to {addr} failed; retrying \
                     with bounded backoff until confirmed: {e}"
                );
            }
        }
    }
}

#[cfg(test)]
pub(super) fn push_confirmed(
    kind: &'static str,
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    epoch: u64,
    confirmed: &mut HashMap<SocketAddr, u64>,
    retries: &mut HashMap<SocketAddr, PushRetry>,
    mut push_one: impl FnMut(SocketAddr) -> io::Result<u64>,
) {
    if epoch == 0 {
        return;
    }
    for (_, shard_addrs) in data_nodes {
        for &addr in shard_addrs {
            if push_due(epoch, addr, confirmed, retries, std::time::Instant::now()) {
                apply_push_result(kind, addr, epoch, push_one(addr), confirmed, retries);
            }
        }
    }
}

fn push_failing(kind: &'static str, addr: SocketAddr, failing: bool) -> bool {
    use std::sync::OnceLock;
    static FAILING: OnceLock<
        std::sync::Mutex<std::collections::HashSet<(&'static str, SocketAddr)>>,
    > = OnceLock::new();
    let mut set = FAILING
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
        .lock()
        .unwrap();
    if failing {
        set.insert((kind, addr))
    } else {
        set.remove(&(kind, addr))
    }
}

pub(super) fn mirror_if_changed<T: Clone + PartialEq>(
    current: T,
    last: &mut T,
    epoch: &mut u64,
    epoch_now: u64,
    mirror: &Mutex<T>,
) -> bool {
    if current != *last {
        *last = current.clone();
        *epoch = epoch_now;
        *mirror.lock().unwrap() = current;
        true
    } else {
        false
    }
}

fn blocking_push(
    addr: SocketAddr,
    req: &Request,
    epoch_of: impl FnOnce(Response) -> Option<u64>,
) -> io::Result<u64> {
    match blocking_request(addr, req)? {
        Response::Err(e) => Err(io::Error::other(format!("node refused the push: {e}"))),
        resp => epoch_of(resp).ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData)),
    }
}

pub(super) fn blocking_request(addr: SocketAddr, req: &Request) -> io::Result<Response> {
    blocking_request_negotiated(addr, req).map(|(response, _)| response)
}

fn blocking_request_negotiated(
    addr: SocketAddr,
    req: &Request,
) -> io::Result<(Response, crate::proto::Negotiated)> {
    let mut s = TcpStream::connect_timeout(&addr, PUSH_TIMEOUT)?;
    s.set_read_timeout(Some(PUSH_TIMEOUT))?;
    s.set_write_timeout(Some(PUSH_TIMEOUT))?;
    let negotiated = crate::proto::client_handshake_blocking(&mut s)?;
    let resp = blocking_request_on_stream(&mut s, req)?;
    Ok((resp, negotiated))
}

fn blocking_request_on_stream(s: &mut TcpStream, req: &Request) -> io::Result<Response> {
    let payload = wire::encode_request_payload(req);
    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
    frame.extend_from_slice(&1u64.to_le_bytes());
    frame.extend_from_slice(&payload);
    s.write_all(&frame)?;
    let mut lenb = [0u8; 4];
    s.read_exact(&mut lenb)?;
    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
    s.read_exact(&mut block)?;
    let response = wire::decode_response(&block[8..])
        .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
    if matches!(response, Response::Denied) {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control push DENIED: node refused our cluster control secret \
             (PD and node secrets differ — fix the cluster secret config)",
        ));
    }
    Ok(response)
}

fn push_set_zones(
    addr: SocketAddr,
    epoch: u64,
    zones: &[(ZoneId, ZoneConfig)],
    incarnation: Option<u128>,
) -> io::Result<u64> {
    let scopes: Vec<Option<Vec<u32>>> = zones
        .iter()
        .map(|(_, cfg)| cfg.scope.as_ref().map(|s| s.iter().copied().collect()))
        .collect();
    let wire: Vec<WireZone> = zones
        .iter()
        .zip(&scopes)
        .map(|((id, cfg), scope)| WireZone {
            id: *id,
            rf: cfg.rf as u32,
            ordering: ordering_to_u8(cfg.ordering),
            scope: scope.as_deref(),
            name: &cfg.name,
        })
        .collect();
    blocking_push(
        addr,
        &Request::SetZones {
            auth: crate::auth::token(),
            epoch,
            zones: &wire,
            incarnation,
        },
        |r| match r {
            Response::Zones { epoch, .. } => Some(epoch),
            _ => None,
        },
    )
}

fn range_maps_withheld(has_v4: bool, operator_enabled: bool) -> bool {
    if has_v4 && !operator_enabled {
        static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
        if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
            log::error!(
                "pd: committed range maps use opaque (V4) placement tokens but \
                 VALS_PD_OPAQUE_RANGE_MAPS is off; every range-map push is withheld, so \
                 restarted nodes stay held (sorted-zone sweep and writes refused) until \
                 the PD runs with VALS_PD_OPAQUE_RANGE_MAPS=1"
            );
        }
        return true;
    }
    false
}

pub(super) enum RangeMapAttempt {
    Blocked(io::Result<u64>),
    Pushed(Option<u64>, io::Result<(u64, u64)>),
}

fn attempt_range_maps(
    addr: SocketAddr,
    epoch: u64,
    ordered: &[(ZoneId, &RangeMap)],
    has_v4: bool,
    probe: impl FnOnce(SocketAddr) -> io::Result<u64>,
    push: impl FnOnce(SocketAddr, u64, &[(ZoneId, &RangeMap)], bool) -> io::Result<(u64, u64)>,
) -> RangeMapAttempt {
    let mut preflight = None;
    if has_v4 {
        range_map_delivery_event(addr, crate::observe::PdRangeMapDeliveryEvent::PreflightAttempt);
        match probe(addr) {
            Ok(bits) if bits & crate::proto::FEATURE_OPAQUE_RANGE_MAP != 0 => preflight = Some(bits),
            other => return RangeMapAttempt::Blocked(other),
        }
    }
    range_map_delivery_event(addr, crate::observe::PdRangeMapDeliveryEvent::PushAttempt);
    RangeMapAttempt::Pushed(preflight, push(addr, epoch, ordered, has_v4))
}

fn apply_range_map_attempt(
    addr: SocketAddr,
    epoch: u64,
    attempt: RangeMapAttempt,
    confirmed: &mut HashMap<SocketAddr, u64>,
    features: &mut HashMap<SocketAddr, u64>,
    retries: &mut HashMap<SocketAddr, PushRetry>,
) {
    let result = match attempt {
        RangeMapAttempt::Blocked(preflight) => {
            range_map_delivery_event(addr, crate::observe::PdRangeMapDeliveryEvent::PreflightBlock);
            retry_failed(retries, addr, epoch);
            match preflight {
                Ok(bits) => {
                    features.insert(addr, bits);
                    if push_failing("range-map-preflight", addr, true) {
                        log::warn!(
                            "pd: opaque range-map delivery to {addr} blocked: endpoint did not advertise V4 support"
                        );
                    }
                }
                Err(e) => {
                    features.remove(&addr);
                    if push_failing("range-map-preflight", addr, true) {
                        log::warn!(
                            "pd: opaque range-map delivery to {addr} blocked by capability preflight; retrying independently: {e}"
                        );
                    }
                }
            }
            return;
        }
        RangeMapAttempt::Pushed(preflight, result) => {
            if let Some(bits) = preflight {
                features.insert(addr, bits);
            }
            result
        }
    };
    match result {
        Ok((reported, negotiated_features)) if reported >= epoch => {
            confirmed.insert(addr, reported);
            features.insert(addr, negotiated_features);
            retries.remove(&addr);
            crate::observe::confirm_pd_range_map_delivery(addr);
            if push_failing("range-map-preflight", addr, false) {
                log::info!("pd: opaque range-map preflight to {addr} recovered");
            }
            if push_failing("range-maps", addr, false) {
                log::info!(
                    "pd: range-maps push to {addr} recovered (confirmed at epoch {reported})"
                );
            }
        }
        Ok(_) => {
            range_map_delivery_event(addr, crate::observe::PdRangeMapDeliveryEvent::PushFailure);
            retry_failed(retries, addr, epoch);
        }
        Err(e) => {
            crate::observe::counters().pd_push_failures.inc();
            range_map_delivery_event(addr, crate::observe::PdRangeMapDeliveryEvent::PushFailure);
            retry_failed(retries, addr, epoch);
            if push_failing("range-maps", addr, true) {
                log::warn!(
                    "pd: range-maps push (epoch {epoch}) to {addr} failed; retrying with bounded backoff until confirmed: {e}"
                );
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(super) fn push_range_maps_confirmed_with(
    data_nodes: &[(NodeId, Vec<SocketAddr>)],
    epoch: u64,
    maps: &HashMap<ZoneId, RangeMap>,
    confirmed: &mut HashMap<SocketAddr, u64>,
    features: &mut HashMap<SocketAddr, u64>,
    retries: &mut HashMap<SocketAddr, PushRetry>,
    operator_enabled: bool,
    mut probe: impl FnMut(SocketAddr) -> io::Result<u64>,
    mut push: impl FnMut(
        SocketAddr,
        u64,
        &[(ZoneId, &RangeMap)],
        bool,
    ) -> io::Result<(u64, u64)>,
) {
    let mut ordered: Vec<(ZoneId, &RangeMap)> = maps.iter().map(|(&z, m)| (z, m)).collect();
    ordered.sort_unstable_by_key(|(z, _)| *z);
    let has_v4 = ordered.iter().any(|(_, map)| map.supports_opaque_tokens());
    if range_maps_withheld(has_v4, operator_enabled) {
        return;
    }
    for (_, shard_addrs) in data_nodes {
        for &addr in shard_addrs {
            if !push_due(epoch, addr, confirmed, retries, std::time::Instant::now()) {
                continue;
            }
            let attempt = attempt_range_maps(addr, epoch, &ordered, has_v4, &mut probe, &mut push);
            apply_range_map_attempt(addr, epoch, attempt, confirmed, features, retries);
        }
    }
}

#[derive(Default)]
pub(super) struct PushLedger {
    pub(super) members: HashMap<SocketAddr, u64>,
    pub(super) zones: HashMap<SocketAddr, u64>,
    pub(super) range_maps: HashMap<SocketAddr, u64>,
    pub(super) range_map_features: HashMap<SocketAddr, u64>,
    member_retries: HashMap<SocketAddr, PushRetry>,
    zone_retries: HashMap<SocketAddr, PushRetry>,
    range_map_retries: HashMap<SocketAddr, PushRetry>,
}

struct LaneConfig {
    member_epoch: u64,
    ring: RingConfig,
    weighted: Vec<(NodeId, u32)>,
    member_addrs: Vec<(NodeId, Vec<SocketAddr>)>,
    zone_epoch: u64,
    zones: Vec<(ZoneId, ZoneConfig)>,
    range_map_epoch: u64,
    range_maps: Vec<(ZoneId, RangeMap)>,
    has_v4: bool,
    incarnation: Option<u128>,
}

#[derive(Default)]
struct LaneReport {
    members: Option<(u64, io::Result<u64>)>,
    zones: Option<(u64, io::Result<u64>)>,
    range_maps: Option<(u64, RangeMapAttempt)>,
}

impl LaneConfig {
    fn new(snap: &PushState, ring: RingConfig, has_v4: bool) -> Self {
        let mut range_maps: Vec<(ZoneId, RangeMap)> =
            snap.range_maps.iter().map(|(z, m)| (*z, m.clone())).collect();
        range_maps.sort_unstable_by_key(|(z, _)| *z);
        LaneConfig {
            member_epoch: snap.member_epoch,
            ring,
            weighted: snap.members.iter().map(|(n, w, _)| (*n, *w)).collect(),
            member_addrs: snap
                .members
                .iter()
                .filter(|(_, _, a)| !a.is_empty())
                .map(|(n, _, a)| (*n, a.clone()))
                .collect(),
            zone_epoch: snap.zone_epoch,
            zones: sorted_zones(&snap.zones),
            range_map_epoch: snap.range_map_epoch,
            range_maps,
            has_v4,
            incarnation: snap.incarnation,
        }
    }

    fn push(&self, addr: SocketAddr, members: bool, zones: bool, range_maps: bool) -> LaneReport {
        let mut report = LaneReport::default();
        if members {
            report.members = Some((
                self.member_epoch,
                push_set_members(
                    addr,
                    self.member_epoch,
                    self.ring,
                    &self.weighted,
                    &self.member_addrs,
                    self.incarnation,
                ),
            ));
        }
        if zones {
            report.zones = Some((
                self.zone_epoch,
                push_set_zones(addr, self.zone_epoch, &self.zones, self.incarnation),
            ));
        }
        if range_maps {
            let ordered: Vec<(ZoneId, &RangeMap)> =
                self.range_maps.iter().map(|(z, m)| (*z, m)).collect();
            report.range_maps = Some((
                self.range_map_epoch,
                attempt_range_maps(
                    addr,
                    self.range_map_epoch,
                    &ordered,
                    self.has_v4,
                    probe_endpoint_features,
                    |addr, epoch, maps, require_opaque| {
                        push_set_range_maps(addr, epoch, maps, require_opaque, self.incarnation)
                    },
                ),
            ));
        }
        report
    }
}

#[derive(Default)]
pub(super) struct PushLanes {
    inflight: HashMap<SocketAddr, mpsc::Receiver<LaneReport>>,
}

impl PushLanes {
    pub(super) fn clear(&mut self) {
        self.inflight.clear();
    }

    pub(super) fn collect(&mut self, ledger: &mut PushLedger) {
        self.inflight.retain(|&addr, lane| {
            let report = match lane.try_recv() {
                Ok(report) => report,
                Err(TryRecvError::Empty) => return true,
                Err(TryRecvError::Disconnected) => return false,
            };
            if let Some((epoch, result)) = report.members {
                apply_push_result(
                    "members", addr, epoch, result, &mut ledger.members, &mut ledger.member_retries,
                );
            }
            if let Some((epoch, result)) = report.zones {
                apply_push_result(
                    "zones", addr, epoch, result, &mut ledger.zones, &mut ledger.zone_retries,
                );
            }
            if let Some((epoch, attempt)) = report.range_maps {
                apply_range_map_attempt(
                    addr,
                    epoch,
                    attempt,
                    &mut ledger.range_maps,
                    &mut ledger.range_map_features,
                    &mut ledger.range_map_retries,
                );
            }
            false
        });
    }

    pub(super) fn dispatch(
        &mut self,
        targets: &[(NodeId, Vec<SocketAddr>)],
        snap: &PushState,
        ring: RingConfig,
        ledger: &mut PushLedger,
    ) {
        let has_v4 = snap.range_maps.values().any(|map| map.supports_opaque_tokens());
        let range_maps_wanted = (snap.range_map_epoch > 0 || snap.zone_epoch > 0)
            && !range_maps_withheld(has_v4, snap.opaque_range_maps_enabled);
        let now = std::time::Instant::now();
        let mut config: Option<Arc<LaneConfig>> = None;
        for &addr in targets.iter().flat_map(|(_, addrs)| addrs) {
            if self.inflight.contains_key(&addr) {
                continue;
            }
            let members = snap.member_epoch > 0
                && push_due(snap.member_epoch, addr, &ledger.members, &mut ledger.member_retries, now);
            let zones = snap.zone_epoch > 0
                && push_due(snap.zone_epoch, addr, &ledger.zones, &mut ledger.zone_retries, now);
            let range_maps = range_maps_wanted
                && push_due(
                    snap.range_map_epoch, addr, &ledger.range_maps, &mut ledger.range_map_retries, now,
                );
            if !(members || zones || range_maps) {
                continue;
            }
            let config = config
                .get_or_insert_with(|| Arc::new(LaneConfig::new(snap, ring, has_v4)))
                .clone();
            let (report, lane) = mpsc::channel();
            let spawned = thread::Builder::new()
                .name("vals-pd-push".into())
                .spawn(move || {
                    let _ = report.send(config.push(addr, members, zones, range_maps));
                });
            match spawned {
                Ok(_) => {
                    self.inflight.insert(addr, lane);
                }
                Err(e) => {
                    log::warn!("pd: could not start a config push to {addr}: {e}");
                    return;
                }
            }
        }
    }
}

fn range_map_delivery_event(
    addr: SocketAddr,
    event: crate::observe::PdRangeMapDeliveryEvent,
) {
    let counters = crate::observe::counters();
    match event {
        crate::observe::PdRangeMapDeliveryEvent::PreflightAttempt => {
            counters.pd_range_map_preflight_attempts.inc();
        }
        crate::observe::PdRangeMapDeliveryEvent::PreflightBlock => {
            counters.pd_range_map_preflight_blocks.inc();
        }
        crate::observe::PdRangeMapDeliveryEvent::PushAttempt => {
            counters.pd_range_map_push_attempts.inc();
        }
        crate::observe::PdRangeMapDeliveryEvent::PushFailure => {
            counters.pd_range_map_push_failures.inc();
        }
    }
    crate::observe::record_pd_range_map_delivery(addr, event);
}

fn probe_endpoint_features(addr: SocketAddr) -> io::Result<u64> {
    let mut stream = TcpStream::connect_timeout(&addr, PUSH_TIMEOUT)?;
    stream.set_read_timeout(Some(PUSH_TIMEOUT))?;
    stream.set_write_timeout(Some(PUSH_TIMEOUT))?;
    crate::proto::client_handshake_blocking(&mut stream).map(|negotiated| negotiated.features)
}

pub(super) fn push_set_range_maps(
    addr: SocketAddr,
    epoch: u64,
    maps: &[(ZoneId, &RangeMap)],
    require_opaque: bool,
    incarnation: Option<u128>,
) -> io::Result<(u64, u64)> {
    let wire: Vec<WireRangeMap> = maps
        .iter()
        .map(|(z, m)| WireRangeMap {
            zone_id: *z,
            splits: m.splits(),
            tokens: m.tokens(),
            pending: m.pending(),
            placement_debt: m.placement_debt(),
            opaque_tokens: m.supports_opaque_tokens(),
        })
        .collect();
    let mut stream = TcpStream::connect_timeout(&addr, PUSH_TIMEOUT)?;
    stream.set_read_timeout(Some(PUSH_TIMEOUT))?;
    stream.set_write_timeout(Some(PUSH_TIMEOUT))?;
    let negotiated = crate::proto::client_handshake_blocking(&mut stream)?;
    if require_opaque
        && negotiated.features & crate::proto::FEATURE_OPAQUE_RANGE_MAP == 0
    {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "peer does not support opaque range maps",
        ));
    }
    let response = blocking_request_on_stream(
        &mut stream,
        &Request::SetRangeMaps {
            auth: crate::auth::token(),
            epoch,
            maps: &wire,
            incarnation,
        },
    )?;
    match response {
        Response::RangeMaps { epoch, .. } => Ok((epoch, negotiated.features)),
        Response::Err(e) => Err(io::Error::other(format!("node refused the push: {e}"))),
        _ => Err(io::Error::from(io::ErrorKind::InvalidData)),
    }
}

fn push_set_members(
    addr: SocketAddr,
    epoch: u64,
    cfg: RingConfig,
    members: &[(NodeId, u32)],
    addrs: &[(NodeId, Vec<SocketAddr>)],
    incarnation: Option<u128>,
) -> io::Result<u64> {
    blocking_push(
        addr,
        &Request::SetMembers {
            auth: crate::auth::token(),
            epoch,
            vnodes: cfg.vnodes,
            rf: cfg.rf,
            members,
            addrs,
            incarnation,
        },
        |r| match r {
            Response::Members { epoch, .. } => Some(epoch),
            _ => None,
        },
    )
}

