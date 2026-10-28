use super::*;

const SLOPPY_FALLBACK_SLACK: usize = 2;

pub(super) enum AckKind {
    Acked,
    Skewed,
    Stale(u64),
    Busy,
    Overloaded,
    Unreachable,
}

pub(super) fn applied_is_ack(applied: Applied) -> bool {
    matches!(applied, Applied::Won | Applied::Lost(_))
}

pub(super) fn classify_apply(r: &Result<Applied, TransportError>) -> AckKind {
    match r {
        Ok(Applied::Won) | Ok(Applied::Lost(_)) => AckKind::Acked,
        Ok(Applied::Rejected) => AckKind::Skewed,
        Err(TransportError::StaleEpoch(e)) => AckKind::Stale(*e),
        Err(TransportError::Busy) => AckKind::Busy,
        Err(TransportError::Overloaded) => AckKind::Overloaded,
        Err(_) => AckKind::Unreachable,
    }
}

pub(super) fn quorum_shortfall_error(
    needed: usize,
    got: usize,
    stale_epoch: Option<u64>,
    busy: usize,
    rejected: usize,
    overloaded: bool,
) -> CoordError {
    if overloaded {
        CoordError::Overloaded
    } else if let Some(epoch) = stale_epoch {
        CoordError::StaleRing { epoch }
    } else if busy > 0 {
        CoordError::Busy { needed, got, busy }
    } else if rejected > 0 {
        CoordError::SkewRejected {
            needed,
            got,
            rejected,
        }
    } else {
        CoordError::QuorumFailed { needed, got }
    }
}


impl<T: Transport> Coordinator<T> {
    /// Writes `value` under `key` in the default zone with
    /// [`WriteOptions::default()`] (every replica acknowledges, buffered) and
    /// returns the version written.
    ///
    /// The `All` default fails the write when any replica is unreachable;
    /// most applications want [`write_with`](Self::write_with) and
    /// [`WriteConcern::Majority`].
    pub async fn write(&self, key: &[u8], value: &[u8]) -> Result<Version, CoordError> {
        self.write_in(DEFAULT_ZONE, key, value, WriteOptions::default())
            .await
    }

    /// Writes `value` under `key` in the default zone with the given concern
    /// and persistence, and returns the version written.
    pub async fn write_with(
        &self,
        key: &[u8],
        value: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        self.write_in(DEFAULT_ZONE, key, value, options).await
    }

    /// Writes `value` under `key` in `zone_id` and returns the version
    /// written.
    ///
    /// The write is stamped with a new version and sent to the key's
    /// replicas; it succeeds once the [`WriteOptions`] concern is met. It
    /// first waits for client credits if this coordinator's request budget is
    /// exhausted. On error the write may still be on some replicas (see
    /// [`CoordError`]). An unknown zone triggers one catalog refresh before
    /// failing with [`CoordError::UnknownZone`].
    ///
    /// Size limit: a key and value together must fit in one value-log segment
    /// of the replicas' engines (`seg_blocks * block_size`, 1 MiB with the
    /// `valsd` defaults, less a small record header). Replicas refuse larger
    /// records, so the write fails with [`CoordError::QuorumFailed`].
    pub async fn write_in(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        value: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        let bytes = key.len().saturating_add(value.len());
        let (credit, replicas) = self.acquire_write(zone_id, key, 1, bytes).await?;
        self.put_inner(
            zone_id,
            key,
            Some(value),
            options.into(),
            false,
            credit,
            replicas,
            false,
        )
        .await
        .map(|c| c.version)
    }

    /// Like [`write_with`](Self::write_with), but returns
    /// [`CoordError::Overloaded`] at once instead of waiting when the
    /// coordinator's request budget is exhausted. Use it to shed load.
    pub async fn try_write(
        &self,
        key: &[u8],
        value: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        let bytes = key.len().saturating_add(value.len());
        let (credit, replicas) = self
            .try_acquire_write(DEFAULT_ZONE, key, 1, bytes)
            .await?;
        self.put_inner(
            DEFAULT_ZONE,
            key,
            Some(value),
            options.into(),
            false,
            credit,
            replicas,
            true,
        )
        .await
        .map(|c| c.version)
    }

    /// Deletes `key` in the default zone; see [`delete_in`](Self::delete_in).
    pub async fn delete(
        &self,
        key: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        self.delete_in(DEFAULT_ZONE, key, options).await
    }

    /// Deletes `key` in `zone_id` by writing a tombstone at a new version,
    /// which it returns.
    ///
    /// The tombstone wins over every older version of the key, so a delayed
    /// older write cannot bring the value back. Deleting a missing key
    /// succeeds. Acknowledgement and failure rules are those of
    /// [`write_in`](Self::write_in).
    pub async fn delete_in(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<Version, CoordError> {
        let (credit, replicas) = self.acquire_write(zone_id, key, 1, key.len()).await?;
        self.put_inner(
            zone_id,
            key,
            None,
            options.into(),
            false,
            credit,
            replicas,
            false,
        )
        .await
        .map(|c| c.version)
    }

    /// Writes with a sloppy quorum in the default zone: if the key's home
    /// replicas cannot meet the concern, the write also goes to fallback nodes
    /// further along the ring, and the coordinator keeps hints to replay to
    /// the unreachable homes.
    ///
    /// This trades consistency for availability. Unless
    /// [`SloppyAck::is_strict`] is true, a quorum read may not see the write
    /// until the hints are delivered. Hints live in this coordinator's
    /// [`HintStore`] (in memory by default) and are lost if the process exits.
    pub async fn write_sloppy(
        &self,
        key: &[u8],
        value: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<SloppyAck, CoordError> {
        let bytes = key.len().saturating_add(value.len());
        let (credit, replicas) = self.acquire_write(DEFAULT_ZONE, key, 1, bytes).await?;
        self.put_inner(
            DEFAULT_ZONE,
            key,
            Some(value),
            options.into(),
            true,
            credit,
            replicas,
            false,
        )
        .await
        .map(Committed::into_sloppy_ack)
    }

    /// The delete counterpart of [`write_sloppy`](Self::write_sloppy).
    pub async fn delete_sloppy(
        &self,
        key: &[u8],
        options: impl Into<WriteOptions>,
    ) -> Result<SloppyAck, CoordError> {
        let (credit, replicas) = self
            .acquire_write(DEFAULT_ZONE, key, 1, key.len())
            .await?;
        self.put_inner(
            DEFAULT_ZONE,
            key,
            None,
            options.into(),
            true,
            credit,
            replicas,
            false,
        )
        .await
        .map(Committed::into_sloppy_ack)
    }

    /// Compare-and-set in the default zone; see [`cas_in`](Self::cas_in).
    pub async fn cas(
        &self,
        key: &[u8],
        value: &[u8],
        expected: Option<Version>,
    ) -> Result<CasResult, CoordError> {
        self.cas_in(DEFAULT_ZONE, key, value, expected).await
    }

    /// Writes `value` only if the key's current version is `expected`.
    ///
    /// The current version includes deletes: after a delete it is the
    /// delete's version (as returned by [`delete_in`](Self::delete_in)), not
    /// `None`. `None` matches only a key with no stored version at all
    /// (never written, or its tombstone has been garbage-collected).
    ///
    /// The coordinator reads the key from a majority of replicas, compares the
    /// winning version with `expected`, and if they match writes with
    /// [`WriteConcern::Majority`]. The check and the write are separate steps,
    /// so two racing calls can both succeed, and a write from another client
    /// can land between them. Use it for low-contention optimistic updates,
    /// not as a lock or a counter. Returns [`CasResult::Mismatch`] with the
    /// current version when the check fails.
    pub async fn cas_in(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        value: &[u8],
        expected: Option<Version>,
    ) -> Result<CasResult, CoordError> {
        let rf = self.resolve_replicas(zone_id, key).await?.len();
        let r = rf / 2 + 1;
        let current = self
            .read_winner_in(zone_id, key, r)
            .await?
            .map(|s| s.version);
        if current != expected {
            return Ok(CasResult::Mismatch { current });
        }
        if let Some(cur) = current {
            self.hlc.observe(cur.hlc, (self.clock)());
        }
        let version = self
            .write_in(zone_id, key, value, WriteConcern::Majority)
            .await?;
        Ok(CasResult::Applied(version))
    }

    pub(super) fn fd_admit(&self, node: NodeId) -> bool {
        match &self.fd {
            Some(fd) => fd.borrow_mut().admit(node, (self.clock)()),
            None => true,
        }
    }

    fn fd_record(&self, node: NodeId, ok: Option<bool>) {
        if let (Some(fd), Some(ok)) = (&self.fd, ok) {
            let now = (self.clock)();
            let mut fd = fd.borrow_mut();
            if ok {
                fd.record_success(node);
            } else {
                fd.record_failure(node, now);
            }
        }
    }

    pub(super) async fn fd_call<O, Fut>(
        &self,
        node: NodeId,
        call: impl FnOnce() -> Fut,
        classify: impl FnOnce(&Result<O, TransportError>) -> Option<bool>,
    ) -> Result<O, TransportError>
    where
        Fut: Future<Output = Result<O, TransportError>>,
    {
        if !self.fd_admit(node) {
            return Err(TransportError::Unavailable);
        }
        let result = call().await;
        self.fd_record(node, classify(&result));
        result
    }

    pub(super) async fn fd_call_vec<O, Fut>(
        &self,
        node: NodeId,
        len: usize,
        unreachable: impl Fn() -> O,
        call: impl FnOnce() -> Fut,
        classify: impl FnOnce(&[O]) -> Option<bool>,
    ) -> Vec<O>
    where
        Fut: Future<Output = Vec<O>>,
    {
        if !self.fd_admit(node) {
            return (0..len).map(|_| unreachable()).collect();
        }
        let result = call().await;
        if len > 0 {
            self.fd_record(node, classify(&result));
        }
        result
    }

    pub(super) async fn fd_apply(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        persistence: Persistence,
    ) -> Result<Applied, TransportError> {
        self.fd_call(
            node,
            || {
                self.transport
                    .apply_persistent(node, zone_id, key, value, version, persistence)
            },
            |result| match result {
                Err(TransportError::Overloaded) => None,
                _ => Some(result.is_ok() || matches!(result, Err(TransportError::Busy))),
            },
        )
        .await
    }

    pub(super) async fn fd_read(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Stored>, TransportError> {
        self.fd_call(
            node,
            || self.transport.read(node, zone_id, key),
            |result| match result {
                Err(TransportError::Overloaded) => None,
                _ => Some(result.is_ok()),
            },
        )
        .await
    }

    pub(super) async fn fd_digest(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        key: &[u8],
    ) -> Result<Option<Version>, TransportError> {
        self.fd_call(
            node,
            || self.transport.digest(node, zone_id, key),
            |result| match result {
                Err(TransportError::Overloaded) => None,
                _ => Some(result.is_ok()),
            },
        )
        .await
    }

    pub(super) async fn fd_read_mixed(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        reads: &[MixedReadEntry<'_>],
    ) -> Vec<Result<MixedReadReply, TransportError>> {
        self.fd_call_vec(
            node,
            reads.len(),
            || Err(TransportError::Unavailable),
            || self.transport.read_mixed(node, zone_id, reads),
            |out| {
                if out.iter().any(Result::is_ok) {
                    Some(true)
                } else if out
                    .iter()
                    .all(|result| matches!(result, Err(TransportError::Overloaded)))
                {
                    None
                } else {
                    Some(false)
                }
            },
        )
        .await
    }

    pub(super) async fn fd_scan(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> Result<ScanPage, TransportError> {
        self.fd_call(
            node,
            || self.transport.scan(node, zone_id, lo, hi, max),
            |result| match result {
                Err(TransportError::Overloaded) => None,
                _ => Some(result.is_ok()),
            },
        )
        .await
    }

    async fn fd_apply_batch(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        sync: bool,
        writes: &[BatchEntry<'_>],
    ) -> Vec<BatchAck> {
        self.fd_call_vec(
            node,
            writes.len(),
            || BatchAck::Unreachable,
            || self.transport.apply_batch(node, zone_id, sync, writes),
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

    async fn put_inner(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        options: WriteOptions,
        sloppy: bool,
        credit: Credit,
        replicas: Option<Vec<NodeId>>,
        shed: bool,
    ) -> Result<Committed, CoordError> {
        let first = self
            .put_once(zone_id, key, value, options, sloppy, replicas.as_deref())
            .await;
        if !matches!(first, Err(CoordError::StaleRing { .. })) || !self.auto_refresh.get() {
            return first;
        }
        drop(credit);
        if !self.refresh_after_stale().await {
            return first;
        }
        let bytes = key
            .len()
            .saturating_add(value.map_or(0, <[u8]>::len));
        let (retry_credit, replicas) = if shed {
            self.try_acquire_write(zone_id, key, 1, bytes).await?
        } else {
            self.acquire_write(zone_id, key, 1, bytes).await?
        };
        let result = self
            .put_once(zone_id, key, value, options, sloppy, replicas.as_deref())
            .await;
        drop(retry_credit);
        result
    }

    fn record_hints(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        version: Version,
        failed: &[NodeId],
    ) -> usize {
        if zone_id != DEFAULT_ZONE {
            return 0;
        }
        let mut hints = self.hints.borrow_mut();
        for &node in failed {
            hints.record(
                node,
                Hint {
                    key: key.to_vec(),
                    value: value.map(|v| v.to_vec()),
                    version,
                },
            );
        }
        failed.len()
    }

    async fn put_once(
        &self,
        zone_id: ZoneId,
        key: &[u8],
        value: Option<&[u8]>,
        options: WriteOptions,
        sloppy: bool,
        admitted_replicas: Option<&[NodeId]>,
    ) -> Result<Committed, CoordError> {
        let resolved;
        let reps = match admitted_replicas {
            Some(replicas) => replicas,
            None => {
                resolved = self.resolve_replicas(zone_id, key).await?;
                &resolved
            }
        };
        let needed = options.required_acks(reps.len())?;
        let persistence = options.persistence;
        let version = self.hlc.now((self.clock)());

        if needed == 0 {
            let failed = RefCell::new(Vec::new());
            let overloaded = Cell::new(false);
            let sends = reps
                .iter()
                .map(|&node| {
                    let failed = &failed;
                    let overloaded = &overloaded;
                    async move {
                        if !self.fd_admit(node) {
                            failed.borrow_mut().push(node);
                            return;
                        }
                        match self
                            .transport
                            .apply_send(node, zone_id, key, value, version)
                            .await
                        {
                            Ok(()) => {}
                            Err(TransportError::Overloaded) => overloaded.set(true),
                            Err(_) => {
                                self.fd_record(node, Some(false));
                                failed.borrow_mut().push(node);
                            }
                        }
                    }
                })
                .collect::<Vec<_>>();
            vals_rt::join_all(sends).await;
            if overloaded.get() {
                return Err(CoordError::Overloaded);
            }
            let failed = failed.into_inner();
            let hinted = self.record_hints(zone_id, key, value, version, &failed);
            return Ok(Committed {
                version,
                home_acks: 0,
                fallback_acks: 0,
                hinted,
            });
        }

        let failed = RefCell::new(Vec::new());
        let stale_epoch = Cell::new(None::<u64>);
        let rejected = Cell::new(0usize);
        let busy = Cell::new(0usize);
        let overloaded = Cell::new(false);
        let home_acks = {
            let failed = &failed;
            let stale_epoch = &stale_epoch;
            let rejected = &rejected;
            let busy = &busy;
            let applies = reps
                .iter()
                .map(|&node| {
                    let overloaded = &overloaded;
                    async move {
                        match classify_apply(
                            &self
                                .fd_apply(node, zone_id, key, value, version, persistence)
                                .await,
                        ) {
                            AckKind::Acked => Ok(()),
                            AckKind::Skewed => {
                                rejected.set(rejected.get() + 1);
                                Err(())
                            }
                            AckKind::Stale(e) => {
                                stale_epoch.set(Some(stale_epoch.get().map_or(e, |s| s.max(e))));
                                Err(())
                            }
                            AckKind::Busy => {
                                busy.set(busy.get() + 1);
                                Err(())
                            }
                            AckKind::Overloaded => {
                                overloaded.set(true);
                                Err(())
                            }
                            AckKind::Unreachable => {
                                failed.borrow_mut().push(node);
                                Err(())
                            }
                        }
                    }
                })
                .collect();
            vals_rt::quorum_ok(applies, needed).await
        };

        let mut fallback_acks = 0;
        if sloppy && home_acks < needed && !overloaded.get() && zone_id == DEFAULT_ZONE {
            let shortfall = needed - home_acks;
            let want = reps.len() + shortfall + SLOPPY_FALLBACK_SLACK;
            let fallbacks: Vec<NodeId> = self
                .ring
                .borrow()
                .replicas_n(key, want)
                .into_iter()
                .filter(|n| !reps.contains(n))
                .collect();
            if !fallbacks.is_empty() {
                let fb = fallbacks
                    .iter()
                    .map(|&node| {
                        let overloaded = &overloaded;
                        async move {
                            match classify_apply(
                                &self
                                    .fd_apply(node, zone_id, key, value, version, persistence)
                                    .await,
                            ) {
                                AckKind::Acked => Ok(()),
                                AckKind::Overloaded => {
                                    overloaded.set(true);
                                    Err(())
                                }
                                _ => Err::<(), ()>(()),
                            }
                        }
                    })
                    .collect();
                fallback_acks += vals_rt::quorum_ok(fb, shortfall).await;
            }
        }

        let acked = home_acks + fallback_acks;
        if acked < needed {
            return Err(quorum_shortfall_error(
                needed,
                acked,
                stale_epoch.get(),
                busy.get(),
                rejected.get(),
                overloaded.get(),
            ));
        }
        let failed = failed.into_inner();
        let hinted = self.record_hints(zone_id, key, value, version, &failed);
        if zone_id == DEFAULT_ZONE {
            for &node in reps {
                if !failed.contains(&node) && self.hints.borrow().pending(node) > 0 {
                    self.replay_hints(node).await;
                }
            }
        }
        Ok(Committed {
            version,
            home_acks,
            fallback_acks,
            hinted,
        })
    }

    /// Writes a batch in the default zone; see
    /// [`write_batch_in`](Self::write_batch_in).
    pub async fn write_batch(
        &self,
        writes: &[(&[u8], &[u8])],
        options: impl Into<WriteOptions>,
    ) -> Result<BatchResult, CoordError> {
        self.write_batch_in(DEFAULT_ZONE, writes, options).await
    }

    /// Writes many keys, grouping them into one request per destination node.
    ///
    /// Each write gets its own version and its own outcome, in input order; a
    /// batch is not atomic and its writes may land in any order. Per-write
    /// failures are in [`BatchResult::outcomes`]. The outer `Err` means the
    /// call produced no per-write outcomes: either it failed before sending
    /// (an unknown zone, or a batch larger than the client budget), or some
    /// writes were refused for a stale ring and the automatic refresh and
    /// retry of the batch failed. In the second case some writes may already
    /// be applied, so treat every write in the batch as unknown. With
    /// [`Persistence::Device`], concurrent batches share device syncs.
    pub async fn write_batch_in(
        &self,
        zone_id: ZoneId,
        writes: &[(&[u8], &[u8])],
        options: impl Into<WriteOptions>,
    ) -> Result<BatchResult, CoordError> {
        let credit = self.acquire_batch(zone_id, writes, false).await?;
        self.batch_inner(zone_id, writes, options.into(), credit, false)
            .await
    }

    /// Like [`write_batch`](Self::write_batch), but returns
    /// [`CoordError::Overloaded`] instead of waiting for credits.
    pub async fn try_write_batch(
        &self,
        writes: &[(&[u8], &[u8])],
        options: impl Into<WriteOptions>,
    ) -> Result<BatchResult, CoordError> {
        let credit = self.acquire_batch(DEFAULT_ZONE, writes, true).await?;
        self.batch_inner(DEFAULT_ZONE, writes, options.into(), credit, true)
            .await
    }

    pub(super) async fn batch_inner(
        &self,
        zone_id: ZoneId,
        writes: &[(&[u8], &[u8])],
        options: WriteOptions,
        credit: Credit,
        shed: bool,
    ) -> Result<BatchResult, CoordError> {
        let first = self
            .batch_once(zone_id, writes, options)
            .await?;
        let any_stale = first.outcomes.iter().any(|o| {
            matches!(
                o,
                WriteOutcome::Failed {
                    error: CoordError::StaleRing { .. },
                    ..
                }
            )
        });
        if !any_stale || !self.auto_refresh.get() {
            return Ok(first);
        }
        drop(credit);
        if !self.refresh_after_stale().await {
            return Ok(first);
        }
        let retry_credit = self.acquire_batch(zone_id, writes, shed).await?;
        let result = self.batch_once(zone_id, writes, options).await;
        drop(retry_credit);
        result
    }

    async fn batch_once(
        &self,
        zone_id: ZoneId,
        writes: &[(&[u8], &[u8])],
        options: WriteOptions,
    ) -> Result<BatchResult, CoordError> {
        let n = writes.len();
        if n == 0 {
            return Ok(BatchResult {
                outcomes: Vec::new(),
            });
        }
        self.ensure_zone(zone_id).await?;

        let phys = (self.clock)();
        let mut versions = Vec::with_capacity(n);
        let mut reps = Vec::with_capacity(n);
        let mut by_node: HashMap<NodeId, Vec<usize>> = HashMap::new();
        let mut needed = vec![0usize; n];
        let mut unmet = 0usize;
        for (i, &(key, _)) in writes.iter().enumerate() {
            let nodes = self.replicas_for(zone_id, key).unwrap_or_default();
            if let Ok(w) = options.required_acks(nodes.len()) {
                versions.push(self.hlc.now(phys));
                for &node in &nodes {
                    by_node.entry(node).or_default().push(i);
                }
                if !nodes.is_empty() {
                    needed[i] = w;
                    unmet += 1;
                }
            } else {
                versions.push(Version { hlc: 0, node: 0 });
            }
            reps.push(nodes);
        }

        let sync = options.persistence == Persistence::Device;

        let node_ids: Vec<NodeId> = by_node.keys().copied().collect();
        let futs = node_ids
            .iter()
            .map(|&node| {
                let entries: Vec<BatchEntry> = by_node[&node]
                    .iter()
                    .map(|&i| BatchEntry {
                        key: writes[i].0,
                        value: Some(writes[i].1),
                        version: versions[i],
                    })
                    .collect();
                async move {
                    (
                        node,
                        self.fd_apply_batch(node, zone_id, sync, &entries).await,
                    )
                }
            })
            .collect();

        let mut acks = vec![0usize; n];
        let mut rejected = vec![0usize; n];
        let mut busy = vec![0usize; n];
        let mut overloaded = vec![false; n];
        let mut stale = vec![None::<u64>; n];
        let mut failed_nodes: Vec<Vec<NodeId>> = vec![Vec::new(); n];
        vals_rt::join_until(futs, |(node, node_res)| {
            for (j, &i) in by_node[&node].iter().enumerate() {
                match node_res.get(j) {
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
                    _ => failed_nodes[i].push(node),
                }
            }
            unmet == 0
        })
        .await;

        let mut outcomes = Vec::with_capacity(n);
        for i in 0..n {
            if reps[i].is_empty() {
                outcomes.push(WriteOutcome::Failed {
                    error: CoordError::NoReplicas,
                    acks: 0,
                });
                continue;
            }
            let needed = match options.required_acks(reps[i].len()) {
                Ok(needed) => needed,
                Err(error) => {
                    outcomes.push(WriteOutcome::Failed { error, acks: 0 });
                    continue;
                }
            };
            if acks[i] >= needed {
                self.record_hints(
                    zone_id,
                    writes[i].0,
                    Some(writes[i].1),
                    versions[i],
                    &failed_nodes[i],
                );
                outcomes.push(WriteOutcome::Ok {
                    version: versions[i],
                    acks: acks[i],
                });
            } else {
                outcomes.push(WriteOutcome::Failed {
                    error: quorum_shortfall_error(
                        needed,
                        acks[i],
                        stale[i],
                        busy[i],
                        rejected[i],
                        overloaded[i],
                    ),
                    acks: acks[i],
                });
            }
        }
        Ok(BatchResult { outcomes })
    }

    /// How many hinted writes this coordinator holds for `node`.
    pub fn pending_hints(&self, node: NodeId) -> usize {
        self.hints.borrow().pending(node)
    }

    /// Tries to deliver the hints held for `node` now and returns how many
    /// were delivered; the rest stay queued. Hints are also replayed
    /// automatically when a later write reaches that node.
    pub async fn replay_hints(&self, node: NodeId) -> usize {
        let held = self.hints.borrow_mut().drain(node);
        let mut delivered = 0;
        let mut still_failed = Vec::new();
        for hint in held {
            match self
                .fd_apply(
                    node,
                    DEFAULT_ZONE,
                    &hint.key,
                    hint.value.as_deref(),
                    hint.version,
                    Persistence::Buffered,
                )
                .await
            {
                Ok(applied) if applied_is_ack(applied) => delivered += 1,
                Ok(_) | Err(_) => still_failed.push(hint),
            }
        }
        self.hints.borrow_mut().restore(node, still_failed);
        delivered
    }
}
