use super::*;

impl<T: Transport> Coordinator<T> {
    /// Reads `key` from the default zone at this coordinator's `R` (the `r`
    /// given to [`Coordinator::new`]); see
    /// [`read_quorum_in`](Self::read_quorum_in).
    pub async fn read(&self, key: &[u8]) -> Result<Option<Stored>, CoordError> {
        self.read_quorum(key, self.r).await
    }

    /// Reads `key` from `zone_id` at this coordinator's `R`; see
    /// [`read_quorum_in`](Self::read_quorum_in).
    pub async fn read_in(&self, zone_id: ZoneId, key: &[u8]) -> Result<Option<Stored>, CoordError> {
        self.read_quorum_in(zone_id, key, self.r).await
    }

    /// Reads `key` from the default zone, waiting for `r` replicas; see
    /// [`read_quorum_in`](Self::read_quorum_in).
    pub async fn read_quorum(&self, key: &[u8], r: usize) -> Result<Option<Stored>, CoordError> {
        self.read_quorum_in(DEFAULT_ZONE, key, r).await
    }

    /// Reads `key` from `zone_id`, waiting for `r` of its replicas, and returns
    /// the value with the greatest version, or `None` if the key is absent or
    /// its newest version is a delete.
    ///
    /// `r` is capped at the key's replica count. If `r` plus the write
    /// concern's `W` exceeds the replica count, the read sees every write
    /// acknowledged by `W` home replicas (or something newer). Before
    /// returning, the call sends the winning version to any replica that
    /// answered with an older version or none (read repair). Fails with
    /// [`CoordError::QuorumFailed`] if fewer than `r` replicas answer. How the
    /// replicas are asked depends on [`ReadFanout`].
    pub async fn read_quorum_in(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        r: usize,
    ) -> Result<Option<Stored>, CoordError> {
        Ok(self
            .read_winner_in(zone_id, key, r)
            .await?
            .filter(|s| s.value.is_some()))
    }

    /// Reads many keys from `zone_id` at this coordinator's `R`, grouping the
    /// requests by node.
    ///
    /// Returns one result per key, in input order, each with the same meaning
    /// as [`read_quorum_in`](Self::read_quorum_in). The outer `Err` covers
    /// failures of the whole call, such as an unknown zone.
    pub async fn read_many_in(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Result<Option<Stored>, CoordError>>, CoordError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        self.ensure_zone(zone_id).await?;
        if self.read_fanout == ReadFanout::Digest {
            let first = self.read_many_digest_once(zone_id, keys).await?;
            let stale = first.iter().any(|r| matches!(r, Err(CoordError::StaleRing { .. })));
            if stale && self.auto_refresh.get() && self.refresh_after_stale().await {
                return self.read_many_digest_once(zone_id, keys).await;
            }
            return Ok(first);
        }
        let r = self.r;
        let mut reps: Vec<Vec<NodeId>> = Vec::with_capacity(keys.len());
        for k in keys {
            reps.push(self.replicas_for(zone_id, k)?);
        }
        let mut by_node: std::collections::HashMap<NodeId, Vec<usize>> =
            std::collections::HashMap::new();
        for (i, nodes) in reps.iter().enumerate() {
            let fanout = match self.read_fanout {
                ReadFanout::All => nodes.len(),
                _ => r,
            };
            for &n in nodes.iter().take(fanout) {
                by_node.entry(n).or_default().push(i);
            }
        }
        let futs = by_node
            .iter()
            .map(|(&node, positions)| {
                self.mixed_node_future(
                    zone_id,
                    keys,
                    node,
                    positions
                        .iter()
                        .map(|&i| (i, MixedReadRole::Value))
                        .collect(),
                )
            })
            .collect();
        let mut results = vals_rt::join_all(futs).await;

        let mut oks = vec![0usize; keys.len()];
        let mut overloaded = vec![false; keys.len()];
        let mut best: Vec<Option<Stored>> = vec![None; keys.len()];
        for (_, positions, node_res, _) in &mut results {
            for (j, &(i, _)) in positions.iter().enumerate() {
                match node_res.get_mut(j) {
                    Some(Ok(MixedReadReply::Value(entry))) => {
                        oks[i] += 1;
                        if let Some(s) = entry.take() {
                            let take = best[i].as_ref().is_none_or(|c| s.version > c.version);
                            if take {
                                best[i] = Some(s);
                            }
                        }
                    }
                    Some(Err(TransportError::Overloaded)) => overloaded[i] = true,
                    _ => {}
                }
            }
        }
        let mut out = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            let needed = r.min(reps[i].len());
            if oks[i] < needed {
                if overloaded[i] {
                    out.push(Err(CoordError::Overloaded));
                } else {
                    out.push(Err(CoordError::QuorumFailed {
                        needed,
                        got: oks[i],
                    }));
                }
            } else {
                out.push(Ok(best[i].take().filter(|s| s.value.is_some())));
            }
        }
        Ok(out)
    }

    fn mixed_node_future<'a>(
        &'a self,
        zone_id: ZoneId,
        keys: &'a [Vec<u8>],
        node: NodeId,
        positions: Vec<(usize, MixedReadRole)>,
    ) -> MixedNodeFuture<'a> {
        Box::pin(async move {
            let reads: Vec<MixedReadEntry<'_>> = positions
                .iter()
                .map(|&(i, role)| MixedReadEntry {
                    key: keys[i].as_slice(),
                    role,
                })
                .collect();
            let mut results = self.fd_read_mixed(node, zone_id, &reads).await;
            let credit = match self
                .transport
                .retain_response(node, mixed_response_bytes(&results))
            {
                Ok(credit) => credit,
                Err(TransportError::Overloaded) => {
                    results = (0..reads.len())
                        .map(|_| Err(TransportError::Overloaded))
                        .collect();
                    None
                }
                Err(_) => None,
            };
            (node, positions, results, credit)
        })
    }

    async fn read_many_mixed_round(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
        by_node: HashMap<NodeId, Vec<(usize, MixedReadRole)>>,
    ) -> Vec<MixedNodeResult> {
        let futs = by_node
            .into_iter()
            .map(|(node, positions)| self.mixed_node_future(zone_id, keys, node, positions))
            .collect();
        vals_rt::join_all(futs).await
    }

    async fn read_many_digest_probes(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
        replicas: &[Vec<NodeId>],
        needed: &[usize],
    ) -> (
        Vec<Vec<(NodeId, Option<Version>, Option<Stored>)>>,
        Vec<u64>,
        Vec<bool>,
        Vec<Credit>,
    ) {
        enum Event {
            Nodes(Vec<MixedNodeResult>),
            Hedge,
            Done,
        }

        let mut initial = HashMap::<NodeId, Vec<(usize, MixedReadRole)>>::new();
        let mut next_rank = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            for (rank, &node) in replicas[i].iter().take(needed[i]).enumerate() {
                initial.entry(node).or_default().push((
                    i,
                    if rank == 0 {
                        MixedReadRole::Value
                    } else {
                        MixedReadRole::Digest
                    },
                ));
            }
            next_rank.push(needed[i]);
        }
        let mut pending: Vec<Option<MixedNodeFuture<'_>>> = initial
            .into_iter()
            .map(|(node, positions)| {
                Some(self.mixed_node_future(zone_id, keys, node, positions))
            })
            .collect();
        let mut responses: Vec<Vec<(NodeId, Option<Version>, Option<Stored>)>> =
            (0..keys.len()).map(|_| Vec::new()).collect();
        let mut stale_epochs = vec![0u64; keys.len()];
        let mut overloaded = vec![false; keys.len()];
        let mut response_credits = Vec::new();
        let mut hedge_wait: Option<Pin<Box<dyn Future<Output = ()>>>> = None;

        loop {
            let all_complete = (0..keys.len()).all(|i| responses[i].len() >= needed[i]);
            if all_complete {
                break;
            }
            let has_reserve = (0..keys.len()).any(|i| {
                responses[i].len() < needed[i] && next_rank[i] < replicas[i].len()
            });
            if hedge_wait.is_none() && has_reserve {
                if let Some(hedge) = &self.digest_hedge {
                    let clock = hedge.clock.clone();
                    let delay_ticks = hedge.delay_ticks;
                    hedge_wait = Some(Box::pin(async move {
                        clock.wait_ticks(delay_ticks).await;
                    }));
                }
            }

            let event = std::future::poll_fn(|cx| {
                let mut completed = Vec::new();
                for slot in &mut pending {
                    if let Some(future) = slot.as_mut() {
                        if let Poll::Ready(result) = future.as_mut().poll(cx) {
                            *slot = None;
                            completed.push(result);
                        }
                    }
                }
                if !completed.is_empty() {
                    return Poll::Ready(Event::Nodes(completed));
                }
                if let Some(wait) = hedge_wait.as_mut() {
                    if wait.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(Event::Hedge);
                    }
                }
                if pending.iter().all(Option::is_none) {
                    Poll::Ready(Event::Done)
                } else {
                    Poll::Pending
                }
            })
            .await;

            let mut activate = vec![0usize; keys.len()];
            match event {
                Event::Nodes(completed) => {
                    for (node, positions, results, credit) in completed {
                        response_credits.extend(credit);
                        let mut results = results.into_iter();
                        for (i, role) in positions {
                            let result = results.next();
                            match (role, result) {
                                (
                                    MixedReadRole::Value,
                                    Some(Ok(MixedReadReply::Value(value))),
                                ) => {
                                    let version = value.as_ref().map(|s| s.version);
                                    responses[i].push((node, version, value));
                                }
                                (
                                    MixedReadRole::Digest,
                                    Some(Ok(MixedReadReply::Digest(version))),
                                ) => responses[i].push((node, version, None)),
                                (_, Some(Err(TransportError::Overloaded))) => {
                                    overloaded[i] = true;
                                    activate[i] += 1;
                                }
                                (_, Some(Err(TransportError::StaleEpoch(epoch)))) => {
                                    stale_epochs[i] = stale_epochs[i].max(epoch);
                                    activate[i] += 1;
                                }
                                _ => activate[i] += 1,
                            }
                        }
                    }
                }
                Event::Hedge => {
                    hedge_wait = None;
                    for i in 0..keys.len() {
                        if responses[i].len() < needed[i] && next_rank[i] < replicas[i].len() {
                            activate[i] = 1;
                        }
                    }
                    if activate.iter().any(|&count| count > 0) {
                        crate::observe::counters().digest_hedges_fired.inc();
                    }
                }
                Event::Done => {
                    for i in 0..keys.len() {
                        if responses[i].len() < needed[i] {
                            activate[i] = needed[i] - responses[i].len();
                        }
                    }
                }
            }

            let mut reserve = HashMap::<NodeId, Vec<(usize, MixedReadRole)>>::new();
            for i in 0..keys.len() {
                if responses[i].len() >= needed[i] {
                    continue;
                }
                for _ in 0..activate[i] {
                    let Some(&node) = replicas[i].get(next_rank[i]) else {
                        break;
                    };
                    next_rank[i] += 1;
                    reserve
                        .entry(node)
                        .or_default()
                        .push((i, MixedReadRole::Digest));
                }
            }
            pending.retain(Option::is_some);
            pending.extend(reserve.into_iter().map(|(node, positions)| {
                Some(self.mixed_node_future(zone_id, keys, node, positions))
            }));
            if pending.is_empty() && !has_reserve {
                break;
            }
        }
        (responses, stale_epochs, overloaded, response_credits)
    }

    async fn read_many_digest_once(
        &self,
        zone_id: ZoneId,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Result<Option<Stored>, CoordError>>, CoordError> {
        let needed_r = self.r;
        let mut replicas = Vec::with_capacity(keys.len());
        let mut needed = Vec::with_capacity(keys.len());
        for key in keys {
            let reps = self.replicas_for(zone_id, key)?;
            needed.push(needed_r.min(reps.len()));
            replicas.push(reps);
        }
        let (responses, mut stale_epochs, mut overloaded, mut response_credits) = self
            .read_many_digest_probes(zone_id, keys, &replicas, &needed)
            .await;

        let mut winners: Vec<Option<Stored>> = vec![None; keys.len()];
        let mut winner_versions = vec![None; keys.len()];
        let mut fallback_candidates: Vec<Vec<NodeId>> =
            (0..keys.len()).map(|_| Vec::new()).collect();
        for i in 0..keys.len() {
            if responses[i].len() < needed[i] {
                continue;
            }
            let Some(winner_version) = responses[i].iter().filter_map(|(_, v, _)| *v).max() else {
                continue;
            };
            winner_versions[i] = Some(winner_version);
            winners[i] = responses[i].iter().find_map(|(_, version, stored)| {
                stored
                    .as_ref()
                    .filter(|s| *version == Some(winner_version) && s.version == winner_version)
                    .cloned()
            });
            if winners[i].is_none() {
                for &node in &replicas[i] {
                    if responses[i]
                        .iter()
                        .any(|&(seen, version, _)| {
                            seen == node && version == Some(winner_version)
                        })
                    {
                        fallback_candidates[i].push(node);
                    }
                }
            }
        }

        for i in 0..keys.len() {
            let Some(winner) = winners[i].as_ref() else {
                continue;
            };
            let node = responses[i]
                .iter()
                .find_map(|(node, version, stored)| {
                    stored
                        .as_ref()
                        .filter(|stored| {
                            *version == winner_versions[i]
                                && Some(stored.version) == winner_versions[i]
                        })
                        .map(|_| *node)
                })
                .unwrap_or(replicas[i][0]);
            match self
                .transport
                .retain_response(node, stored_response_bytes(winner))
            {
                Ok(credit) => response_credits.extend(credit),
                Err(TransportError::Overloaded) => {
                    overloaded[i] = true;
                    winners[i] = None;
                }
                Err(_) => {
                    winners[i] = None;
                }
            }
        }

        let mut fallback_next = vec![0usize; keys.len()];
        loop {
            let mut fallback = HashMap::<NodeId, Vec<(usize, MixedReadRole)>>::new();
            for i in 0..keys.len() {
                if winners[i].is_some() {
                    continue;
                }
                if let Some(&node) = fallback_candidates[i].get(fallback_next[i]) {
                    fallback_next[i] += 1;
                    fallback
                        .entry(node)
                        .or_default()
                        .push((i, MixedReadRole::Value));
                }
            }
            if fallback.is_empty() {
                break;
            }
            for (_node, positions, results, credit) in
                self.read_many_mixed_round(zone_id, keys, fallback).await
            {
                response_credits.extend(credit);
                let mut results = results.into_iter();
                for (i, _) in positions {
                    match results.next() {
                        Some(Ok(MixedReadReply::Value(Some(stored))))
                            if winner_versions[i] == Some(stored.version) =>
                        {
                            winners[i] = Some(stored);
                        }
                        Some(Err(TransportError::Overloaded)) => overloaded[i] = true,
                        Some(Err(TransportError::StaleEpoch(epoch))) => {
                            stale_epochs[i] = stale_epochs[i].max(epoch);
                        }
                        _ => {}
                    }
                }
            }
        }

        let repair_futs: Vec<_> = (0..keys.len())
            .filter_map(|i| {
                let winner = winners[i].as_ref()?;
                let view: Vec<(NodeId, Option<Stored>)> = responses[i]
                    .iter()
                    .map(|&(node, version, _)| {
                        (
                            node,
                            version.map(|version| Stored {
                                version,
                                value: None,
                            }),
                        )
                    })
                    .collect();
                Some(async move {
                    self.hlc.observe(winner.version.hlc, (self.clock)());
                    self.read_repair(zone_id, &keys[i], &view, winner).await;
                })
            })
            .collect();
        vals_rt::join_all(repair_futs).await;

        let mut out = Vec::with_capacity(keys.len());
        for i in 0..keys.len() {
            if responses[i].len() < needed[i] {
                if overloaded[i] {
                    out.push(Err(CoordError::Overloaded));
                } else if stale_epochs[i] > 0 {
                    out.push(Err(CoordError::StaleRing {
                        epoch: stale_epochs[i],
                    }));
                } else {
                    out.push(Err(CoordError::QuorumFailed {
                        needed: needed[i],
                        got: responses[i].len(),
                    }));
                }
            } else if responses[i].iter().any(|(_, version, _)| version.is_some())
                && winners[i].is_none()
            {
                if overloaded[i] {
                    out.push(Err(CoordError::Overloaded));
                } else if stale_epochs[i] > 0 {
                    out.push(Err(CoordError::StaleRing {
                        epoch: stale_epochs[i],
                    }));
                } else {
                    out.push(Err(CoordError::QuorumFailed {
                        needed: needed[i],
                        got: 0,
                    }));
                }
            } else {
                out.push(Ok(winners[i].take().filter(|s| s.value.is_some())));
            }
        }
        Ok(out)
    }

    /// [`read_many_in`](Self::read_many_in) for the default zone.
    pub async fn read_many(
        &self,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Result<Option<Stored>, CoordError>>, CoordError> {
        self.read_many_in(DEFAULT_ZONE, keys).await
    }

    pub(super) async fn read_winner_in(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        r: usize,
    ) -> Result<Option<Stored>, CoordError> {
        self.retry_stale(|| self.read_winner_once(zone_id, key, r))
            .await
    }

    async fn read_winner_once(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        r: usize,
    ) -> Result<Option<Stored>, CoordError> {
        let reps = self.resolve_replicas(zone_id, key).await?;
        let needed = r.min(reps.len());
        let winner = match self.read_fanout {
            ReadFanout::Digest => self.read_winner_digest(zone_id, key, &reps, needed).await,
            ReadFanout::All | ReadFanout::Quorum => {
                self.read_winner_value(zone_id, key, &reps, needed).await
            }
        }?;
        if let Some(ref win) = winner {
            self.hlc.observe(win.version.hlc, (self.clock)());
        }
        Ok(winner)
    }

    async fn read_winner_value(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        reps: &[NodeId],
        needed: usize,
    ) -> Result<Option<Stored>, CoordError> {
        let stale_epoch = Cell::new(0u64);
        let overloaded = Cell::new(false);
        let read_one = |node: NodeId| {
            let stale_epoch = &stale_epoch;
            let overloaded = &overloaded;
            async move {
                match self.fd_read(node, zone_id, key).await {
                    Ok(entry) => {
                        let bytes = entry.as_ref().map_or(0, stored_response_bytes);
                        match self.transport.retain_response(node, bytes) {
                            Ok(credit) => Ok((node, entry, credit)),
                            Err(e) => {
                                if matches!(e, TransportError::Overloaded) {
                                    overloaded.set(true);
                                }
                                Err(e)
                            }
                        }
                    }
                    Err(e) => {
                        if let TransportError::StaleEpoch(ep) = e {
                            stale_epoch.set(stale_epoch.get().max(ep));
                        } else if matches!(e, TransportError::Overloaded) {
                            overloaded.set(true);
                        }
                        Err(e)
                    }
                }
            }
        };
        let fanout = match self.read_fanout {
            ReadFanout::Quorum => needed,
            _ => reps.len(),
        };
        let primary = reps[..fanout].iter().map(|&n| read_one(n)).collect();
        let retained: Vec<(NodeId, Option<Stored>, Option<Credit>)> =
            vals_rt::quorum_collect(primary, needed).await;
        let mut responses = Vec::with_capacity(retained.len());
        let mut response_credits = Vec::new();
        for (node, entry, credit) in retained {
            responses.push((node, entry));
            response_credits.extend(credit);
        }
        if responses.len() < needed && fanout < reps.len() {
            let short = needed - responses.len();
            let extra = reps[fanout..].iter().map(|&n| read_one(n)).collect();
            let retained: Vec<(NodeId, Option<Stored>, Option<Credit>)> =
                vals_rt::quorum_collect(extra, short).await;
            for (node, entry, credit) in retained {
                responses.push((node, entry));
                response_credits.extend(credit);
            }
        }

        if responses.len() < needed {
            if overloaded.get() {
                return Err(CoordError::Overloaded);
            }
            if stale_epoch.get() > 0 {
                return Err(CoordError::StaleRing {
                    epoch: stale_epoch.get(),
                });
            }
            return Err(CoordError::QuorumFailed {
                needed,
                got: responses.len(),
            });
        }

        let winner_source = responses
            .iter()
            .filter_map(|(node, entry)| entry.as_ref().map(|stored| (*node, stored)))
            .max_by_key(|(_, stored)| stored.version);
        let winner = winner_source.map(|(_, stored)| stored.clone());
        if let Some((node, stored)) = winner_source {
            let credit = self
                .transport
                .retain_response(node, stored_response_bytes(stored))
                .map_err(|e| match e {
                    TransportError::Overloaded => CoordError::Overloaded,
                    _ => CoordError::Unavailable,
                })?;
            response_credits.extend(credit);
        }

        if let Some(ref win) = winner {
            self.read_repair(zone_id, key, &responses, win).await;
        }

        Ok(winner)
    }

    async fn read_winner_digest(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        reps: &[NodeId],
        needed: usize,
    ) -> Result<Option<Stored>, CoordError> {
        let stale_epoch = Cell::new(0u64);
        let overloaded = Cell::new(false);
        let probe = |node: NodeId, want_value: bool| {
            let stale_epoch = &stale_epoch;
            let overloaded = &overloaded;
            async move {
                let note_stale = |e: &TransportError| {
                    if let TransportError::StaleEpoch(ep) = e {
                        stale_epoch.set(stale_epoch.get().max(*ep));
                    } else if matches!(e, TransportError::Overloaded) {
                        overloaded.set(true);
                    }
                };
                if want_value {
                    match self.fd_read(node, zone_id, key).await {
                        Ok(entry) => {
                            let bytes = entry.as_ref().map_or(0, stored_response_bytes);
                            match self.transport.retain_response(node, bytes) {
                                Ok(credit) => {
                                    Ok((node, entry.as_ref().map(|s| s.version), entry, credit))
                                }
                                Err(e) => {
                                    if matches!(e, TransportError::Overloaded) {
                                        overloaded.set(true);
                                    }
                                    Err(e)
                                }
                            }
                        }
                        Err(e) => {
                            note_stale(&e);
                            Err(e)
                        }
                    }
                } else {
                    match self.fd_digest(node, zone_id, key).await {
                        Ok(version) => Ok((node, version, None, None)),
                        Err(e) => {
                            note_stale(&e);
                            Err(e)
                        }
                    }
                }
            }
        };

        let mut futs = Vec::with_capacity(reps.len());
        futs.push(probe(reps[0], true));
        for &n in &reps[1..] {
            futs.push(probe(n, false));
        }
        let retained: Vec<(NodeId, Option<Version>, Option<Stored>, Option<Credit>)> = match &self.digest_hedge {
            Some(h) if reps.len() > needed => {
                let clock = h.clock.clone();
                let delay_ticks = h.delay_ticks;
                let hedge = move || {
                    let clock = clock.clone();
                    async move {
                        clock.wait_ticks(delay_ticks).await;
                        crate::observe::counters().digest_hedges_fired.inc();
                    }
                };
                vals_rt::quorum_collect_reserve_hedged(futs, needed, needed, hedge).await
            }
            _ => vals_rt::quorum_collect_reserve(futs, needed, needed).await,
        };
        let mut responses = Vec::with_capacity(retained.len());
        let mut response_credits = Vec::new();
        for (node, version, stored, credit) in retained {
            responses.push((node, version, stored));
            response_credits.extend(credit);
        }

        if responses.len() < needed {
            if overloaded.get() {
                return Err(CoordError::Overloaded);
            }
            if stale_epoch.get() > 0 {
                return Err(CoordError::StaleRing {
                    epoch: stale_epoch.get(),
                });
            }
            return Err(CoordError::QuorumFailed {
                needed,
                got: responses.len(),
            });
        }

        let Some(winner_ver) = responses.iter().filter_map(|(_, v, _)| *v).max() else {
            return Ok(None);
        };

        let winner_source = responses.iter().find_map(|(node, v, s)| match (v, s) {
            (Some(ver), Some(stored)) if *ver == winner_ver => Some((*node, stored)),
            _ => None,
        });
        let mut winner = winner_source.map(|(_, stored)| stored.clone());
        if let Some((node, stored)) = winner_source {
            let credit = self
                .transport
                .retain_response(node, stored_response_bytes(stored))
                .map_err(|e| match e {
                    TransportError::Overloaded => CoordError::Overloaded,
                    _ => CoordError::Unavailable,
                })?;
            response_credits.extend(credit);
        }
        if winner.is_none() {
            for &(node, ver, _) in responses.iter() {
                if ver == Some(winner_ver) {
                    if let Ok(Some(stored)) = self.fd_read(node, zone_id, key).await {
                        let credit = self
                            .transport
                            .retain_response(node, stored_response_bytes(&stored))
                            .map_err(|e| match e {
                                TransportError::Overloaded => CoordError::Overloaded,
                                _ => CoordError::Unavailable,
                            })?;
                        response_credits.extend(credit);
                        if stored.version == winner_ver {
                            winner = Some(stored);
                            break;
                        }
                    }
                }
            }
        }
        let Some(winner) = winner else {
            return Err(CoordError::QuorumFailed { needed, got: 0 });
        };

        let repair_view: Vec<(NodeId, Option<Stored>)> = responses
            .iter()
            .map(|&(node, ver, _)| {
                (
                    node,
                    ver.map(|version| Stored {
                        version,
                        value: None,
                    }),
                )
            })
            .collect();
        self.read_repair(zone_id, key, &repair_view, &winner).await;

        Ok(Some(winner))
    }

    /// Reads `key` from the default zone from a single replica, trying the
    /// key's replicas one at a time (starting at a rotating position) until
    /// one answers. Fast, but eventually consistent: the answer may be stale
    /// or miss a recent write.
    pub async fn read_any(&self, key: &[u8]) -> Result<Option<Stored>, CoordError> {
        self.retry_stale(|| self.read_any_once(key)).await
    }

    async fn read_any_once(&self, key: &[u8]) -> Result<Option<Stored>, CoordError> {
        let reps = self.ring.borrow().replicas(key);
        if reps.is_empty() {
            return Err(CoordError::NoReplicas);
        }
        let start = self.read_rotor.get();
        self.read_rotor.set(start.wrapping_add(1));
        let mut stale_epoch = 0u64;
        for i in 0..reps.len() {
            let node = reps[(start.wrapping_add(i)) % reps.len()];
            match self.fd_read(node, DEFAULT_ZONE, key).await {
                Ok(entry) => {
                    let bytes = entry.as_ref().map_or(0, stored_response_bytes);
                    let _credit = self
                        .transport
                        .retain_response(node, bytes)
                        .map_err(|e| match e {
                            TransportError::Overloaded => CoordError::Overloaded,
                            _ => CoordError::Unavailable,
                        })?;
                    return Ok(entry.filter(|s| s.value.is_some()));
                }
                Err(TransportError::Overloaded) => return Err(CoordError::Overloaded),
                Err(TransportError::StaleEpoch(e)) => stale_epoch = stale_epoch.max(e),
                Err(_) => {}
            }
        }
        if stale_epoch > 0 {
            return Err(CoordError::StaleRing { epoch: stale_epoch });
        }
        Err(CoordError::QuorumFailed { needed: 1, got: 0 })
    }

    async fn read_repair(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        responses: &[(NodeId, Option<Stored>)],
        win: &Stored,
    ) {
        let repairs: Vec<_> = responses
            .iter()
            .filter(|(_, entry)| match entry {
                None => true,
                Some(s) => win.version.wins_over(&s.version),
            })
            .map(|(node, _)| {
                self.fd_apply(
                    *node,
                    zone_id,
                    key,
                    win.value.as_deref(),
                    win.version,
                    Persistence::Buffered,
                )
            })
            .collect();
        if !repairs.is_empty() {
            let _ = vals_rt::join_all(repairs).await;
        }
    }

}
