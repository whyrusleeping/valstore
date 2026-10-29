use super::*;

pub(crate) struct ConnDriver<D: BlockDevice> {
    pub(crate) stream: Rc<super::Sock>,
    pub(crate) fd: RawFd,
    pub(crate) reader: FrameRead,
    pub(crate) out: Rc<ServerOut>,
    pub(crate) shared: Rc<ServerShared<D>>,
    pub(crate) spawner: vals_rt::Spawner,
    pub(crate) negotiated: Option<crate::proto::Negotiated>,
    #[cfg(test)]
    pub(crate) apply_mode_override: Option<ApplyMode>,
}

struct InflightReadGuard {
    out: Rc<ServerOut>,
    _request_credit: crate::transport::Credit,
    _byte_credit: crate::transport::ByteCredit,
}

impl InflightReadGuard {
    fn new(out: &Rc<ServerOut>) -> Self {
        out.reads_inflight.set(out.reads_inflight.get() + 1);
        let (request_credit, byte_credit) = out.take_dispatch_credits();
        InflightReadGuard {
            out: out.clone(),
            _request_credit: request_credit,
            _byte_credit: byte_credit,
        }
    }
}

impl Drop for InflightReadGuard {
    fn drop(&mut self) {
        self.out
            .reads_inflight
            .set(self.out.reads_inflight.get().saturating_sub(1));
    }
}

struct DispatchCreditGuard<'a>(&'a ServerOut);

impl Drop for DispatchCreditGuard<'_> {
    fn drop(&mut self) {
        self.0.finish_dispatch();
    }
}

fn push_batch_apply_response(
    out: &ServerOut,
    reqid: u64,
    response: &Response,
    started: std::time::Instant,
) {
    crate::observe::histograms()
        .batch_apply_response_ready_latency_us
        .observe(started.elapsed());
    out.push_resp(reqid, response);
}

impl<D: BlockDevice + AsyncRead + 'static> ConnDriver<D> {
    fn effective_apply_mode(&self) -> ApplyMode {
        #[cfg(test)]
        if let Some(mode) = self.apply_mode_override {
            return mode;
        }
        unconditional_apply_mode()
    }

    #[cfg(test)]
    pub(super) fn degrade_apply_mode_for_test(&mut self) {
        self.apply_mode_override = Some(ApplyMode::Legacy);
    }

    fn poll_handshake(&mut self, cx: &mut Context<'_>, draining: bool) -> Option<Poll<()>> {
        if self.negotiated.is_some() {
            return None;
        }
        if draining {
            return Some(Poll::Ready(()));
        }
        let read = self.reader.read_block_accounted(
            &self.stream,
            |bytes| self.out.reserve_input(bytes),
            |bytes| self.out.release_input_bytes(bytes),
        );
        match read {
            Poll::Ready(Ok((off, len))) => {
                self.out.note_read(false);
                let block = self.reader.frame_at(off, len);
                match crate::proto::decode_hello(block)
                    .and_then(|(v, f)| crate::proto::negotiate(v, f))
                {
                    Ok(n) => {
                        self.negotiated = Some(n);
                        self.out.note_negotiated();
                        self.out.push_raw(&crate::proto::encode_hello());
                        None
                    }
                    Err(e) => {
                        crate::observe::counters().handshake_denied.inc();
                        log::warn!(
                            "server: client handshake rejected ({e:?}) from {:?}; \
                             dropping connection",
                            self.stream.peer_addr()
                        );
                        Some(Poll::Ready(()))
                    }
                }
            }
            Poll::Ready(Err(_)) => Some(Poll::Ready(())),
            Poll::Pending => {
                if self.reader.buffered_len() > 0 {
                    self.out.note_read(true);
                }
                *self.out.waker.borrow_mut() = Some(cx.waker().clone());
                reactor::register(self.fd, Interest::Read, cx.waker().clone());
                Some(Poll::Pending)
            }
        }
    }

    pub(crate) fn dispatch_frame(&self, block: &[u8]) -> bool {
        if block.len() < 8 {
            return false;
        }
        let reqid = u64::from_le_bytes(block[0..8].try_into().unwrap());
        let Some(request_credit) = crate::transport::acquire_request() else {
            crate::observe::counters().transport_request_rejections.inc();
            self.out.push_resp(reqid, &Response::Busy);
            return true;
        };
        let Some(byte_credit) = crate::transport::acquire_queued_bytes(block.len()) else {
            crate::observe::counters().transport_queued_byte_rejections.inc();
            self.out.close();
            return true;
        };
        self.out.begin_dispatch(request_credit, byte_credit);
        let _dispatch_credit = DispatchCreditGuard(&self.out);
        crate::observe::counters().server_ops.inc();
        match wire::decode_request(&block[8..]) {
            Ok(req)
                if req
                    .route_epoch()
                    .is_some_and(|e| e < self.shared.ring_epoch()) =>
            {
                let cur = self.shared.ring_epoch();
                self.out.push(
                    reqid,
                    &wire::encode_response_payload(&Response::StaleEpoch(cur)),
                );
            }
            Ok(ref req)
                if (self.shared.fenced() || self.shared.regain_fence_failed())
                    && matches!(
                        req,
                        OwnedRequest::Read { .. }
                            | OwnedRequest::Digest { .. }
                            | OwnedRequest::Scan { .. }
                            | OwnedRequest::MultiScan { .. }
                            | OwnedRequest::SyncTree { .. }
                            | OwnedRequest::SyncBucketDigests { .. }
                            | OwnedRequest::SyncValues { .. }
                            | OwnedRequest::SyncRangeTombstones { .. }
                            | OwnedRequest::MixedBatchRead { .. }
                    ) =>
            {
                crate::observe::counters().server_errors.inc();
                let why = if self.shared.fenced() {
                    "fenced: node is resyncing after being out of contact"
                } else {
                    "fenced: a former-owner copy this node owns again could not be deleted yet"
                };
                self.out.push_resp(reqid, &Response::Err(why.into()));
            }
            Ok(ref req)
                if matches!(
                    req,
                    OwnedRequest::Apply { .. }
                        | OwnedRequest::BatchApply { .. }
                        | OwnedRequest::RangeDelete { .. }
                ) && crate::ram::ram_over_budget() =>
            {
                crate::observe::counters().ram_busy_rejections.inc();
                self.out.push_resp(reqid, &Response::Busy);
            }
            Ok(OwnedRequest::Apply {
                zone_id,
                key,
                value,
                version,
                ..
            }) => {
                if let Some(resp) = rehome_write_fence_one(&self.shared, zone_id, &key) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let started = std::time::Instant::now();
                let store = match self.shared.zone_store(zone_id) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::observe::counters().server_errors.inc();
                        self.out.push_resp(reqid, &Response::Err(e));
                        return true;
                    }
                };
                if let Some(resp) = admission_shed_rejection(&store) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                self.shared
                    .note_range_ops(zone_id, std::iter::once(key.as_slice()));
                let wall_ms = crate::hlc::physical_now_ms();
                let mode = self.effective_apply_mode();
                let staged = store.borrow_mut().apply_unconditional_stage(
                    &key,
                    value.as_deref(),
                    version,
                    wall_ms,
                    mode,
                    crate::replica::LiveFloor::Enforce,
                );
                match staged {
                    Ok(StagedApply::Done(a)) => {
                        note_single_apply_written(a, value.as_ref().map_or(0, Vec::len));
                        crate::observe::histograms()
                            .apply_latency_us
                            .observe(started.elapsed());
                        if post_apply_needs_mirror(&self.shared, zone_id, &key) {
                            let (shared, out) = (self.shared.clone(), self.out.clone());
                            let inflight = InflightReadGuard::new(&out);
                            self.spawner.spawn(async move {
                                let _inflight = inflight;
                                let resp = mirror_apply_response(
                                    shared,
                                    zone_id,
                                    (key, value, version),
                                    a,
                                )
                                .await;
                                out.push_resp(reqid, &resp);
                            });
                        } else {
                            self.out.push_resp(reqid, &Response::Apply(a));
                        }
                    }
                    Ok(StagedApply::NeedsRunRead { .. }) => {
                        crate::observe::counters().apply_run_reads.inc();
                        let (shared, out) = (self.shared.clone(), self.out.clone());
                        let inflight = InflightReadGuard::new(&out);
                        self.spawner.spawn(async move {
                            let _inflight = inflight;
                            let dev = store.borrow().device_handle();
                            let resp = serve_apply(
                                &store,
                                &dev,
                                &shared.read_gate,
                                &key,
                                value.as_deref(),
                                version,
                                wall_ms,
                                mode,
                            )
                            .await;
                            crate::observe::histograms()
                                .apply_latency_us
                                .observe(started.elapsed());
                            let resp = match resp {
                                Response::Apply(a) => {
                                    note_single_apply_written(a, value.as_ref().map_or(0, Vec::len));
                                    mirror_apply_response(
                                        shared.clone(),
                                        zone_id,
                                        (key, value, version),
                                        a,
                                    )
                                    .await
                                }
                                other => other,
                            };
                            if matches!(resp, Response::Err(_)) {
                                crate::observe::counters().server_errors.inc();
                            }
                            out.push_resp(reqid, &resp);
                        });
                    }
                    Err(e) if e.is_backpressure() || e.is_device_full() => {
                        let resp = space_rejection(&store);
                        if matches!(resp, Response::Err(_)) {
                            crate::observe::counters().server_errors.inc();
                        }
                        self.out.push_resp(reqid, &resp);
                    }
                    Err(e) => {
                        crate::observe::counters().server_errors.inc();
                        self.out.push_resp(reqid, &Response::Err(format!("{e}")));
                    }
                }
            }
            Ok(OwnedRequest::BatchApply {
                zone_id,
                sync,
                fill,
                writes,
                ..
            }) => {
                let response_started = std::time::Instant::now();
                let fenced = match fence_writes(
                    &self.shared,
                    zone_id,
                    writes.iter().map(|(key, _, _)| key.as_slice()),
                ) {
                    Ok(fenced) => fenced,
                    Err(resp) => {
                        push_batch_apply_response(&self.out, reqid, &resp, response_started);
                        return true;
                    }
                };
                let store = match self.shared.zone_store(zone_id) {
                    Ok(s) => s,
                    Err(e) => {
                        crate::observe::counters().server_errors.inc();
                        push_batch_apply_response(
                            &self.out,
                            reqid,
                            &Response::Err(e),
                            response_started,
                        );
                        return true;
                    }
                };
                if let Some(resp) = admission_shed_rejection(&store) {
                    push_batch_apply_response(&self.out, reqid, &resp, response_started);
                    return true;
                }
                self.shared.note_fenced_range_ops(
                    zone_id,
                    fenced.as_ref(),
                    writes.iter().map(|(key, _, _)| key.as_slice()),
                );
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let Some(permit) = shared.batch_apply_gate.try_acquire() else {
                        crate::observe::counters().batch_apply_gate_busy.inc();
                        push_batch_apply_response(
                            &out,
                            reqid,
                            &Response::Busy,
                            response_started,
                        );
                        return;
                    };
                    let mut batch_permit = Some(permit);
                    let mode = unconditional_apply_mode();
                    #[cfg(test)]
                    let mode = if vals_store::winner_directory_operational() {
                        mode
                    } else {
                        ApplyMode::Legacy
                    };
                    let dev = store.borrow().device_handle();
                    let resp = serve_mirrored_batch_apply(
                        shared.clone(),
                        zone_id,
                        &store,
                        &dev,
                        &shared.read_gate,
                        &writes,
                        sync,
                        mode,
                        if fill { LiveFloor::Waive } else { LiveFloor::Enforce },
                        fenced.as_ref(),
                        &mut batch_permit,
                    )
                    .await;
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    push_batch_apply_response(&out, reqid, &resp, response_started);
                });
            }
            Ok(OwnedRequest::RangeDelete {
                zone_id,
                sync,
                version,
                slices,
                ..
            }) => {
                let refused = fence_range_delete(&self.shared, zone_id, &slices)
                    .err()
                    .or_else(|| match self.shared.zone_store(zone_id) {
                        Ok(store) => admission_shed_rejection(&store),
                        Err(e) => Some(Response::Err(e)),
                    });
                if let Some(resp) = refused {
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            serve_range_delete(shared.clone(), zone_id, &store, sync, version, &slices)
                                .await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::MixedBatchRead {
                epoch,
                zone_id,
                start,
                reads,
            }) => {
                if let Some(resp) = rehome_read_fence(
                    &self.shared,
                    epoch,
                    zone_id,
                    reads.iter().map(|(key, _)| key.as_slice()),
                ) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let sample_winner_probes = shared.sample_winner_probes();
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    shared.note_range_ops(
                        zone_id,
                        reads.iter().map(|(key, _)| key.as_slice()),
                    );
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            let dev = store.borrow().device_handle();
                            serve_mixed_batch_read(
                                &store,
                                &dev,
                                &shared.read_gate,
                                start,
                                &reads,
                                sample_winner_probes,
                                shared.read_site(zone_id),
                            )
                            .await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::Read { epoch, zone_id, key }) => {
                if let Some(resp) = rehome_read_fence(&self.shared, epoch, zone_id, std::iter::once(key.as_slice())) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    match shared.zone_store(zone_id) {
                        Ok(store) => {
                            shared.note_range_ops(zone_id, std::iter::once(key.as_slice()));
                            let dev = store.borrow().device_handle();
                            let site = shared.read_site(zone_id);
                            serve_read_into(&store, &dev, &shared.read_gate, &key, &out, reqid, site)
                                .await;
                        }
                        Err(e) => out.push_resp(reqid, &Response::Err(e)),
                    }
                });
            }
            Ok(OwnedRequest::MultiScan { epoch, zone_id, keys_only, start, scans, reverse }) => {
                let required = crate::proto::FEATURE_MULTI_SCAN
                    | if reverse { crate::proto::FEATURE_REVERSE_SCAN } else { 0 };
                if self.negotiated.is_none_or(|n| n.features & required != required) {
                    self.out.push_resp(reqid, &Response::Err("MultiScan direction feature not negotiated".into()));
                    return true;
                }
                for (lo, hi, _) in &scans[start as usize..] {
                    if let Some(resp) = rehome_scan_fence(&self.shared, epoch, zone_id, lo.as_deref(), hi.as_deref()) {
                        self.out.push_resp(reqid, &resp);
                        return true;
                    }
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                let range_tombstones = self.negotiated.is_some_and(|n| n.features & crate::proto::FEATURE_RANGE_TOMBSTONES != 0);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let Some(_scan_permit) = shared.try_scan() else {
                        out.push_resp(reqid, &Response::Busy);
                        return;
                    };
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            shared.note_range_ops(zone_id, scans[start as usize..].iter().map(|s| s.0.as_deref().unwrap_or_default()));
                            let dev = store.borrow().device_handle();
                            if reverse {
                                scan::serve_multi_scan::<_, true>(&store, &dev, &shared.read_gate, start, &scans,
                                    keys_only, range_tombstones, shared.read_site(zone_id)).await
                            } else {
                                scan::serve_multi_scan::<_, false>(&store, &dev, &shared.read_gate, start, &scans,
                                    keys_only, range_tombstones, shared.read_site(zone_id)).await
                            }
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::Scan {
                epoch,
                zone_id,
                lo,
                hi,
                max,
            }) => {
                if let Some(resp) = rehome_scan_fence(&self.shared, epoch, zone_id, lo.as_deref(), hi.as_deref()) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                let range_tombstones = self.negotiated.is_some_and(|n| {
                    n.features & crate::proto::FEATURE_RANGE_TOMBSTONES != 0
                });
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let Some(_scan_permit) = shared.try_scan() else {
                        out.push_resp(reqid, &Response::Busy);
                        return;
                    };
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            shared.note_range_ops(
                                zone_id,
                                std::iter::once(lo.as_deref().unwrap_or_default()),
                            );
                            let dev = store.borrow().device_handle();
                            serve_scan(
                                &store,
                                &dev,
                                &shared.read_gate,
                                lo.as_deref(),
                                hi.as_deref(),
                                max,
                                range_tombstones,
                                shared.read_site(zone_id),
                            )
                            .await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::SyncTree {
                zone_id,
                level,
                scope_g,
                scope,
                requester,
            }) => {
                if let Some(why) = sync_tree_geometry_error(level, scope_g, &scope) {
                    crate::observe::counters().server_errors.inc();
                    self.out.push_resp(reqid, &Response::Err(why));
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            let scope: std::collections::HashSet<u32> = scope.into_iter().collect();
                            serve_sync_tree(&store, level, scope_g, &scope, requester).await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::SyncValues { zone_id, keys }) => {
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            let dev = store.borrow().device_handle();
                            let site = shared.read_site(zone_id);
                            serve_sync_values(&store, &dev, &shared.read_gate, &keys, site).await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::Digest { epoch, zone_id, key }) => {
                if let Some(resp) = rehome_read_fence(&self.shared, epoch, zone_id, std::iter::once(key.as_slice())) {
                    self.out.push_resp(reqid, &resp);
                    return true;
                }
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            shared.note_range_ops(zone_id, std::iter::once(key.as_slice()));
                            let dev = store.borrow().device_handle();
                            serve_digest(&store, &dev, &shared.read_gate, &key).await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::SyncBucketDigests {
                zone_id,
                num_buckets,
                ids,
                after,
            }) => {
                if let Some(why) = bucket_digest_geometry_error(num_buckets) {
                    crate::observe::counters().server_errors.inc();
                    self.out.push_resp(reqid, &Response::Err(why));
                    return true;
                }
                let resumable = self.negotiated.is_some_and(|n| {
                    n.features & crate::proto::FEATURE_RESUMABLE_DIGESTS != 0
                });
                let (shared, out) = (self.shared.clone(), self.out.clone());
                let inflight = InflightReadGuard::new(&out);
                self.spawner.spawn(async move {
                    let _inflight = inflight;
                    let resp = match shared.zone_store(zone_id) {
                        Ok(store) => {
                            let dev = store.borrow().device_handle();
                            serve_bucket_digests(
                                &store,
                                &dev,
                                &shared.read_gate,
                                num_buckets,
                                &ids,
                                after.as_deref(),
                                resumable,
                            )
                            .await
                        }
                        Err(e) => Response::Err(e),
                    };
                    if matches!(resp, Response::Err(_)) {
                        crate::observe::counters().server_errors.inc();
                    }
                    out.push_resp(reqid, &resp);
                });
            }
            Ok(OwnedRequest::SyncRangeTombstones {
                zone_id,
                requester,
                digest,
                after,
            }) => {
                let resp = serve_sync_range_tombstones(
                    &self.shared,
                    zone_id,
                    requester,
                    digest,
                    after.as_deref(),
                );
                if matches!(resp, Response::Err(_)) {
                    crate::observe::counters().server_errors.inc();
                }
                self.out.push_resp(reqid, &resp);
            }
            Ok(req) => {
                let resp = serve_control_request(&self.shared, req);
                if matches!(resp, Response::Err(_)) {
                    crate::observe::counters().server_errors.inc();
                }
                self.out.push_resp(reqid, &resp);
            }
            Err(e) => {
                crate::observe::counters().server_errors.inc();
                let resp = Response::Err(format!("bad request: {e:?}"));
                self.out.push_resp(reqid, &resp);
            }
        }
        true
    }

    fn flush_out(&self) -> Result<bool, ()> {
        let mut buf = self.out.buf.borrow_mut();
        let mut pos = self.out.pos.get();
        let mut ok = true;
        while pos < buf.len() {
            match self.stream.write(&buf[pos..]) {
                Ok(0) => {
                    ok = false;
                    break;
                }
                Ok(n) => {
                    pos += n;
                    self.out.note_written(pos < buf.len());
                }
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            return Err(());
        }
        if buf.len() - pos > MAX_CONN_SEND_BUF {
            return Err(());
        }
        if pos == buf.len() {
            let capacity = buf.capacity();
            *buf = Vec::new();
            self.out.release_output(capacity);
            pos = 0;
        }
        self.out.pos.set(pos);
        Ok(pos < buf.len())
    }
}

impl<D: BlockDevice> Drop for ConnDriver<D> {
    fn drop(&mut self) {
        self.out.close();
        self.out.release_input();
    }
}

impl<D: BlockDevice + AsyncRead + 'static> Future for ConnDriver<D> {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.out.is_closed() {
            return Poll::Ready(());
        }
        if let Some(deadline) = this.out.deadline_expired(std::time::Instant::now()) {
            match deadline {
                DeadlineKind::Initial => crate::observe::counters().transport_initial_timeouts.inc(),
                DeadlineKind::Frame => crate::observe::counters().transport_frame_timeouts.inc(),
                DeadlineKind::Idle => crate::observe::counters().transport_idle_timeouts.inc(),
                DeadlineKind::Write => crate::observe::counters().transport_write_timeouts.inc(),
            }
            return Poll::Ready(());
        }
        let draining = this.shared.is_shutdown();
        if let Some(p) = this.poll_handshake(cx, draining) {
            return p;
        }
        #[allow(clippy::while_immutable_condition)]
        while !draining {
            if this.out.reads_inflight.get() >= MAX_CONN_INFLIGHT_READS {
                break;
            }
            let read = this.reader.read_block_accounted(
                &this.stream,
                |bytes| this.out.reserve_input(bytes),
                |bytes| this.out.release_input_bytes(bytes),
            );
            match read {
                Poll::Ready(Ok((off, len))) => {
                    this.out.note_read(false);
                    let block = this.reader.frame_at(off, len);
                    if !this.dispatch_frame(block) {
                        crate::observe::counters().malformed_frames.inc();
                        log::warn!(
                            "server: malformed request frame ({len} bytes) from {:?}; \
                             dropping connection",
                            this.stream.peer_addr()
                        );
                        return Poll::Ready(());
                    }
                    if this.out.is_closed() {
                        return Poll::Ready(());
                    }
                }
                Poll::Ready(Err(FrameErr::Oversized(len))) => {
                    crate::observe::counters().oversized_frames.inc();
                    log::warn!(
                        "server: oversized request frame ({len} bytes > MAX_FRAME \
                         {MAX_FRAME}) from {:?}; dropping connection — split the \
                         request (e.g. smaller PutBatch) under the frame cap",
                        this.stream.peer_addr()
                    );
                    return Poll::Ready(());
                }
                Poll::Ready(Err(FrameErr::Closed | FrameErr::Overloaded)) => {
                    return Poll::Ready(())
                }
                Poll::Pending => {
                    if this.reader.buffered_len() > 0 {
                        this.out.note_read(true);
                    }
                    break;
                }
            }
        }
        let need_write = match this.flush_out() {
            Ok(nw) => nw,
            Err(()) => return Poll::Ready(()),
        };
        if draining && (!need_write || this.shared.force_close()) {
            return Poll::Ready(());
        }
        *this.out.waker.borrow_mut() = Some(cx.waker().clone());
        let interest = if need_write {
            Interest::ReadWrite
        } else {
            Interest::Read
        };
        reactor::register(this.fd, interest, cx.waker().clone());
        Poll::Pending
    }
}

