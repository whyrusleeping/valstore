use super::*;
use write::quorum_shortfall_error;

const RANGE_DELETE_CONCURRENCY: usize = 8;
const RANGE_DELETE_NODE_CONCURRENCY: usize = RANGE_DELETE_CONCURRENCY / 2;

type Span = (Option<Vec<u8>>, Option<Vec<u8>>);

struct PlannedSlice {
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    reps: Vec<NodeId>,
}

impl<T: Transport> Coordinator<T> {
    pub async fn delete_range_in(
        &self,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        let options = options.into();
        if hi.is_some_and(|h| lo.unwrap_or_default() >= h) {
            return Err(CoordError::InvalidRange);
        }
        let span = [(lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec))];
        let slices = self.plan_range_delete(zone_id, &span).await?;
        for slice in &slices {
            options.required_acks(slice.reps.len())?;
        }
        let version = self.hlc.now((self.clock)());
        let sync = options.persistence == Persistence::Device;
        let mut missed = self
            .range_delete_round(zone_id, &slices, version, options, sync)
            .await;
        let stale = missed
            .iter()
            .any(|m| matches!(m.error, CoordError::StaleRing { .. }));
        if stale && self.auto_refresh.get() && self.refresh_after_stale().await {
            let spans: Vec<Span> = missed.iter().map(|m| (m.lo.clone(), m.hi.clone())).collect();
            if let Ok(retry) = self.plan_range_delete(zone_id, &spans).await {
                missed = self
                    .range_delete_round(zone_id, &retry, version, options, sync)
                    .await;
            }
        }
        if missed.is_empty() {
            Ok(version)
        } else {
            Err(CoordError::RangeDeleteIncomplete { version, missed })
        }
    }

    async fn plan_range_delete(
        &self,
        zone_id: ZoneId,
        spans: &[Span],
    ) -> Result<Vec<PlannedSlice>, CoordError> {
        if zone_id == DEFAULT_ZONE {
            return Err(CoordError::NotSorted(zone_id));
        }
        self.ensure_zone(zone_id).await?;
        let (rf, scope) = {
            let cat = self.catalog.borrow();
            let cfg = cat.get(zone_id).ok_or(CoordError::UnknownZone(zone_id))?;
            if cfg.ordering != Ordering::Sorted {
                return Err(CoordError::NotSorted(zone_id));
            }
            (cfg.rf, cfg.scope.clone())
        };
        let map = self.range_map(zone_id);
        let mut slices = Vec::new();
        {
            let ring = self.ring.borrow();
            for (lo, hi) in spans {
                for slice in map.ranges_overlapping(lo.as_deref(), hi.as_deref()) {
                    let reps = ring.replicas_in_zone(zone_id, &slice.token, rf, scope.as_ref());
                    if reps.is_empty() {
                        return Err(CoordError::NoReplicas);
                    }
                    slices.push(PlannedSlice {
                        lo: slice.lo,
                        hi: slice.hi,
                        reps,
                    });
                }
            }
        }
        let mut nodes: Vec<NodeId> = slices.iter().flat_map(|s| s.reps.iter().copied()).collect();
        nodes.sort_unstable();
        nodes.dedup();
        for node in nodes {
            if self.range_tombstone_support(node).await == Some(false) {
                return Err(CoordError::Unsupported(node));
            }
        }
        Ok(slices)
    }

    async fn range_tombstone_support(&self, node: NodeId) -> Option<bool> {
        if let Some(supported) = self.transport.supports_range_tombstones(node) {
            return Some(supported);
        }
        let _ = self
            .fd_call(
                node,
                || self.transport.members(node),
                |result| match result {
                    Err(TransportError::Overloaded) => None,
                    _ => Some(result.is_ok()),
                },
            )
            .await;
        self.transport.supports_range_tombstones(node)
    }

    async fn range_delete_round(
        &self,
        zone_id: ZoneId,
        slices: &[PlannedSlice],
        version: Version,
        options: WriteOptions,
        sync: bool,
    ) -> Vec<RangeDeleteMiss> {
        let mut by_node: BTreeMap<NodeId, Vec<usize>> = BTreeMap::new();
        for (i, slice) in slices.iter().enumerate() {
            if options.required_acks(slice.reps.len()).is_err() {
                continue;
            }
            for &node in &slice.reps {
                by_node.entry(node).or_default().push(i);
            }
        }
        let mut requests: Vec<(NodeId, &[usize])> = Vec::new();
        for round in 0.. {
            let before = requests.len();
            for (&node, indices) in &by_node {
                if let Some(chunk) = indices.chunks(crate::wire::MAX_RANGE_DELETE_SLICES).nth(round) {
                    requests.push((node, chunk));
                }
            }
            if requests.len() == before {
                break;
            }
        }

        let n = slices.len();
        let needed: Vec<usize> = slices
            .iter()
            .map(|slice| options.required_acks(slice.reps.len()).unwrap_or(0))
            .collect();
        let mut unmet = slices
            .iter()
            .filter(|slice| options.required_acks(slice.reps.len()).is_ok())
            .count();
        let mut acks = vec![0usize; n];
        let mut rejected = vec![0usize; n];
        let mut busy = vec![0usize; n];
        let mut overloaded = vec![false; n];
        let mut stale = vec![None::<u64>; n];
        let start = |r: usize| {
            let (node, indices) = requests[r];
            let wire: Vec<crate::wire::WireSlice<'_>> = indices
                .iter()
                .map(|&i| (slices[i].lo.as_deref(), slices[i].hi.as_deref()))
                .collect();
            async move {
                let out = self
                    .fd_range_delete(node, zone_id, sync, version, &wire)
                    .await;
                (r, out)
            }
        };
        let mut started = vec![false; requests.len()];
        let mut first_unstarted = 0;
        let mut node_in_flight: BTreeMap<NodeId, usize> = BTreeMap::new();
        let mut active = Vec::new();
        std::future::poll_fn(|cx| loop {
            for r in first_unstarted..requests.len() {
                if active.len() >= RANGE_DELETE_CONCURRENCY {
                    break;
                }
                if started[r] {
                    continue;
                }
                let (node, indices) = requests[r];
                if indices.iter().all(|&i| needed[i] > 0 && acks[i] >= needed[i]) {
                    started[r] = true;
                    continue;
                }
                let in_flight = node_in_flight.entry(node).or_insert(0);
                if *in_flight >= RANGE_DELETE_NODE_CONCURRENCY {
                    continue;
                }
                *in_flight += 1;
                started[r] = true;
                active.push(Box::pin(start(r)));
            }
            while first_unstarted < requests.len() && started[first_unstarted] {
                first_unstarted += 1;
            }
            let mut progressed = false;
            let mut k = 0;
            while k < active.len() {
                let Poll::Ready((r, out)) = active[k].as_mut().poll(cx) else {
                    k += 1;
                    continue;
                };
                drop(active.swap_remove(k));
                progressed = true;
                let (node, indices) = requests[r];
                if let Some(in_flight) = node_in_flight.get_mut(&node) {
                    *in_flight -= 1;
                }
                for (j, &i) in indices.iter().enumerate() {
                    match out.get(j) {
                        Some(BatchAck::Applied(Applied::Won))
                        | Some(BatchAck::Applied(Applied::Lost(_))) => {
                            acks[i] += 1;
                            if acks[i] == needed[i] {
                                unmet -= 1;
                            }
                        }
                        Some(BatchAck::Applied(Applied::Rejected)) => rejected[i] += 1,
                        Some(BatchAck::Stale(e)) => {
                            stale[i] = Some(stale[i].map_or(*e, |s| s.max(*e)));
                        }
                        Some(BatchAck::Busy) => busy[i] += 1,
                        Some(BatchAck::Overloaded) => overloaded[i] = true,
                        _ => {}
                    }
                }
            }
            if unmet == 0 || (active.is_empty() && first_unstarted == requests.len()) {
                active.clear();
                return Poll::Ready(());
            }
            if !progressed {
                return Poll::Pending;
            }
        })
        .await;
        slices
            .iter()
            .enumerate()
            .filter_map(|(i, slice)| {
                let needed = match options.required_acks(slice.reps.len()) {
                    Ok(needed) => needed,
                    Err(error) => {
                        return Some(RangeDeleteMiss {
                            lo: slice.lo.clone(),
                            hi: slice.hi.clone(),
                            error,
                        })
                    }
                };
                (acks[i] < needed).then(|| RangeDeleteMiss {
                    lo: slice.lo.clone(),
                    hi: slice.hi.clone(),
                    error: quorum_shortfall_error(
                        needed,
                        acks[i],
                        stale[i],
                        busy[i],
                        rejected[i],
                        overloaded[i],
                    ),
                })
            })
            .collect()
    }

    async fn fd_range_delete(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        version: Version,
        slices: &[crate::wire::WireSlice<'_>],
    ) -> Vec<BatchAck> {
        self.fd_call_vec(
            node,
            slices.len(),
            || BatchAck::Unreachable,
            || self.transport.range_delete(node, zone_id, sync, version, slices),
            |out| {
                if out
                    .iter()
                    .any(|ack| !matches!(ack, BatchAck::Unreachable | BatchAck::Overloaded))
                {
                    Some(true)
                } else if out.iter().all(|ack| matches!(ack, BatchAck::Overloaded)) {
                    None
                } else {
                    Some(false)
                }
            },
        )
        .await
    }
}
