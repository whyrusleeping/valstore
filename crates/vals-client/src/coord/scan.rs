use super::*;

const SCAN_RANGE_CONCURRENCY: usize = 8;

pub(super) struct ScanRound {
    pages: Vec<ScanPage>,
    stale_epoch: u64,
    overloaded: bool,
    credits: Vec<Credit>,
    failures: Vec<ReplicaError>,
}

fn page_coverage(page: &ScanPage) -> Option<Vec<u8>> {
    if let Some(resume) = &page.resume {
        return Some(resume.clone());
    }
    if !page.more {
        return None;
    }
    let (last, _) = page
        .entries
        .last()
        .expect("a truncated page without a resume key is non-empty");
    let mut succ = last.clone();
    succ.push(0);
    Some(succ)
}

#[doc(hidden)]
pub fn scan_merge_bound(responses: &[ScanPage]) -> Option<Vec<u8>> {
    let mut bound: Option<Vec<u8>> = None;
    for page in responses {
        if let Some(coverage) = page_coverage(page) {
            bound = Some(match bound {
                Some(b) if b <= coverage => b,
                _ => coverage,
            });
        }
    }
    bound
}

pub(super) struct WindowCover {
    lists: Vec<Vec<crate::wire::RtFragment>>,
}

impl WindowCover {
    pub(super) fn new(pages: &mut [ScanPage]) -> Self {
        let lists = pages
            .iter_mut()
            .filter(|page| !page.fragments.is_empty())
            .map(|page| {
                let mut list = std::mem::take(&mut page.fragments);
                list.sort_by(|a, b| a.lo.as_deref().unwrap_or_default().cmp(b.lo.as_deref().unwrap_or_default()));
                list
            })
            .collect();
        WindowCover { lists }
    }

    pub(super) fn covers(&self, key: &[u8], version: Version) -> bool {
        self.lists.iter().any(|list| {
            let i = list.partition_point(|f| f.lo.as_deref().unwrap_or_default() <= key);
            i > 0 && {
                let f = &list[i - 1];
                f.hi.as_deref().is_none_or(|hi| key < hi) && !version.wins_over(&f.version)
            }
        })
    }
}

#[doc(hidden)]
pub fn scan_merge_lww(mut responses: Vec<ScanPage>, bound: Option<&[u8]>) -> BTreeMap<Vec<u8>, Stored> {
    let cover = WindowCover::new(&mut responses);
    let mut merged: BTreeMap<Vec<u8>, Stored> = BTreeMap::new();
    for page in responses {
        for (key, stored) in page.entries {
            if bound.is_some_and(|b| key.as_slice() >= b) {
                continue;
            }
            match merged.get(&key) {
                Some(cur) if !stored.version.wins_over(&cur.version) => {}
                _ => {
                    merged.insert(key, stored);
                }
            }
        }
    }
    if !cover.lists.is_empty() {
        merged.retain(|key, stored| !cover.covers(key, stored.version));
    }
    merged
}

impl<T: Transport> Coordinator<T> {
    /// Returns the live keys of sorted zone `zone_id` in `[lo, hi)`, in
    /// ascending key order, with their values. `None` bounds mean the start or
    /// end of the zone; `limit` caps the number of keys returned.
    ///
    /// Each range of the zone is read page by page from `r` of its replicas
    /// (capped at the replica count) and the pages are merged by
    /// last-writer-wins, dropping deleted keys. With `W + r` above the replica
    /// count the scan sees every acknowledged write that finished before it
    /// started. It is not a snapshot: writes that land while the scan runs
    /// may or may not appear, and different pages can reflect different
    /// moments. Without a `limit` the whole result is held in memory, so page
    /// large ranges yourself by passing the last key returned plus a `0x00`
    /// byte as the next `lo`.
    ///
    /// Fails with [`CoordError::NotSorted`] on an unsorted zone and
    /// [`CoordError::ScanQuorumFailed`] when a range cannot reach `r`
    /// replicas. Scans do no read repair.
    pub async fn scan_in(
        &self,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        r: usize,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CoordError> {
        self.retry_stale(|| self.scan_in_once(zone_id, lo, hi, r, limit))
            .await
    }

    async fn scan_in_once(
        &self,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        r: usize,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CoordError> {
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
        let slices = map.ranges_overlapping(lo, hi);

        if limit.is_none() && slices.len() > 1 {
            let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            let mut response_credits = Vec::new();
            for group in slices.chunks(SCAN_RANGE_CONCURRENCY) {
                let futs: Vec<_> = group
                    .iter()
                    .map(|slice| self.scan_slice(zone_id, rf, scope.as_ref(), slice, r, None))
                    .collect();
                for res in vals_rt::join_all(futs).await {
                    let (entries, credits) = res?;
                    response_credits.extend(credits);
                    out.extend(entries);
                }
            }
            return Ok(out);
        }

        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut response_credits = Vec::new();
        for slice in &slices {
            let rem = limit.map(|lim| lim - out.len());
            let (entries, credits) = self
                .scan_slice(zone_id, rf, scope.as_ref(), slice, r, rem)
                .await?;
            response_credits.extend(credits);
            out.extend(entries);
            if limit.is_some_and(|lim| out.len() >= lim) {
                break;
            }
        }
        Ok(out)
    }

    async fn scan_round(
        &self,
        zone_id: ZoneId,
        reps: &[NodeId],
        lo: Option<Vec<u8>>,
        hi: Option<&[u8]>,
        req_max: u32,
        needed: usize,
    ) -> ScanRound {
        let stale_epoch = Cell::new(0u64);
        let overloaded = Cell::new(false);
        let failures = RefCell::new(Vec::new());
        let slo = lo.as_deref();
        let scan_one = |node: NodeId| {
            let stale_epoch = &stale_epoch;
            let overloaded = &overloaded;
            let failures = &failures;
            async move {
                let result = match self.fd_scan(node, zone_id, slo, hi, req_max).await {
                    Ok(page)
                        if page.more && page.entries.is_empty() && page.resume.is_none() =>
                    {
                        Err(TransportError::Remote("empty truncated scan page".into()))
                    }
                    Ok(page)
                        if page
                            .resume
                            .as_deref()
                            .is_some_and(|r| r <= slo.unwrap_or_default()) =>
                    {
                        Err(TransportError::Remote("scan resume key does not advance".into()))
                    }
                    Ok(page) => {
                        match self
                            .transport
                            .retain_response(node, scan_response_bytes(&page))
                        {
                            Ok(credit) => Ok((page, credit)),
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
                };
                if let Err(error) = &result {
                    failures.borrow_mut().push(ReplicaError {
                        node,
                        error: error.clone(),
                    });
                }
                result
            }
        };
        let futs = reps.iter().map(|&n| scan_one(n)).collect();
        let retained: Vec<(ScanPage, Option<Credit>)> =
            vals_rt::quorum_collect(futs, needed).await;
        let mut pages = Vec::with_capacity(retained.len());
        let mut credits = Vec::new();
        for (page, credit) in retained {
            pages.push(page);
            credits.extend(credit);
        }
        ScanRound {
            pages,
            stale_epoch: stale_epoch.get(),
            overloaded: overloaded.get(),
            credits,
            failures: failures.into_inner(),
        }
    }

    async fn scan_slice(
        &self,
        zone_id: ZoneId,
        rf: usize,
        scope: Option<&std::collections::BTreeSet<NodeId>>,
        slice: &crate::partition::RangeSlice,
        r: usize,
        limit: Option<usize>,
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Credit>), CoordError> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let reps = self
            .ring
            .borrow()
            .replicas_in_zone(zone_id, &slice.token, rf, scope);
        if reps.is_empty() {
            return Err(CoordError::NoReplicas);
        }
        let needed = r.min(reps.len());

        let shi = slice.hi.as_deref();

        let prefetch = limit.is_none() && self.scan_prefetch.get();
        let mut cursor: Option<Vec<u8>> = slice.lo.clone();
        let mut prefetched_fut: Option<PrefetchedRound<'_>> = None;
        let mut prefetched_ready: Option<ScanRound> = None;
        let mut response_credits = Vec::new();

        loop {
            let req_max = match limit {
                Some(lim) => u32::try_from(lim - out.len()).unwrap_or(u32::MAX),
                None => 0,
            };

            let round = if let Some(ready) = prefetched_ready.take() {
                ready
            } else if let Some(fut) = prefetched_fut.take() {
                fut.await
            } else {
                self.scan_round(zone_id, &reps, cursor.clone(), shi, req_max, needed)
                    .await
            };
            let responses = round.pages;
            response_credits.extend(round.credits);
            if responses.len() < needed {
                if round.overloaded {
                    return Err(CoordError::Overloaded);
                }
                if round.stale_epoch > 0 {
                    return Err(CoordError::StaleRing {
                        epoch: round.stale_epoch,
                    });
                }
                return Err(CoordError::ScanQuorumFailed {
                    zone_id,
                    cursor,
                    needed,
                    got: responses.len(),
                    replicas: round.failures,
                });
            }

            let seen_hlc = responses
                .iter()
                .flat_map(|page| {
                    page.entries
                        .iter()
                        .map(|(_, stored)| stored.version.hlc)
                        .chain(page.fragments.iter().map(|f| f.version.hlc))
                })
                .max();
            if let Some(hlc) = seen_hlc {
                self.hlc.observe(hlc, (self.clock)());
            }

            let bound = scan_merge_bound(&responses);
            let next_cursor = match &bound {
                None => None,
                Some(b) if shi.is_some_and(|h| b.as_slice() >= h) => None,
                Some(b) => Some(b.clone()),
            };

            if prefetch {
                if let Some(nc) = next_cursor.clone() {
                    let mut fut: PrefetchedRound<'_> =
                        Box::pin(self.scan_round(zone_id, &reps, Some(nc), shi, 0, needed));
                    match vals_rt::prime(fut.as_mut()).await {
                        Some(ready) => prefetched_ready = Some(ready),
                        None => prefetched_fut = Some(fut),
                    }
                    self.transport.flush_requests();
                }
            }

            if responses.len() == 1 {
                let mut responses = responses;
                let cover = WindowCover::new(&mut responses);
                let page = responses.into_iter().next().expect("len checked");
                for (key, stored) in page.entries {
                    if bound.as_deref().is_some_and(|b| key.as_slice() >= b) {
                        break;
                    }
                    if !cover.lists.is_empty() && cover.covers(&key, stored.version) {
                        continue;
                    }
                    if let Some(value) = stored.value {
                        out.push((key, value));
                        if limit.is_some_and(|lim| out.len() >= lim) {
                            return Ok((out, response_credits));
                        }
                    }
                }
            } else {
                let merged = scan_merge_lww(responses, bound.as_deref());
                for (key, stored) in merged {
                    if let Some(value) = stored.value {
                        out.push((key, value));
                        if limit.is_some_and(|lim| out.len() >= lim) {
                            return Ok((out, response_credits));
                        }
                    }
                }
            }

            match next_cursor {
                Some(nc) => cursor = Some(nc),
                None => break,
            }
        }
        Ok((out, response_credits))
    }

}
