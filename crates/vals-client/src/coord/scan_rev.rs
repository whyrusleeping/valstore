use super::*;
use super::scan::WindowCover;

#[cfg(test)]
mod tests;

struct Cursor {
    slices: Vec<crate::partition::RangeSlice>,
    slice: usize,
    hi: Option<Vec<u8>>,
    out: Vec<(Vec<u8>, Stored)>,
}

fn page_coverage(page: &ScanPage) -> Option<&[u8]> {
    page.resume.as_deref().or_else(|| {
        if page.more { page.entries.last().map(|(k, _)| k.as_slice()) } else { None }
    })
}

fn validate_page(page: &ScanPage, lo: Option<&[u8]>, hi: Option<&[u8]>) -> Result<(), TransportError> {
    let bound = page_coverage(page);
    if (page.more && bound.is_none()) || (!page.more && page.resume.is_some())
        || bound.is_some_and(|b| b < lo.unwrap_or_default() || hi.is_some_and(|h| b >= h)) {
        return Err(TransportError::Remote("non-advancing reverse scan page".into()));
    }
    Ok(())
}

fn merge_bound(pages: &[ScanPage]) -> Option<Vec<u8>> {
    pages.iter().filter_map(page_coverage).max().map(<[u8]>::to_vec)
}

fn merge_lww(mut pages: Vec<ScanPage>, bound: Option<&[u8]>) -> BTreeMap<Vec<u8>, Stored> {
    let cover = WindowCover::new(&mut pages);
    let mut merged: BTreeMap<Vec<u8>, Stored> = BTreeMap::new();
    for page in pages {
        for (key, stored) in page.entries {
            if bound.is_some_and(|b| key.as_slice() < b) { continue; }
            match merged.get(&key) {
                Some(cur) if !stored.version.wins_over(&cur.version) => {}
                _ => { merged.insert(key, stored); }
            }
        }
    }
    merged.retain(|key, stored| !cover.covers(key, stored.version));
    merged
}

impl<T: Transport> Coordinator<T> {
    /// [`scan_in`](Self::scan_in) in descending key order: the last `limit`
    /// live keys of `[lo, hi)`, largest first.
    pub async fn scan_in_rev(
        &self,
        zone_id: ZoneId,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        r: impl Into<ReadConcern>,
        limit: Option<usize>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, CoordError> {
        let mut out = self.scan_many_in_rev(zone_id, &[(lo, hi, limit)], r, false).await?;
        Ok(out.pop().expect("one query").into_iter()
            .map(|(k, s)| (k, s.value.expect("live winners only"))).collect())
    }

    /// [`scan_many_in`](Self::scan_many_in) in descending key order.
    pub async fn scan_many_in_rev(
        &self,
        zone_id: ZoneId,
        scans: &[ScanQuery<'_>],
        r: impl Into<ReadConcern>,
        keys_only: bool,
    ) -> Result<Vec<Vec<(Vec<u8>, Stored)>>, CoordError> {
        let r = r.into();
        self.retry_stale(|| self.scan_many_rev_once(zone_id, scans, r, keys_only)).await
    }

    async fn scan_many_rev_once(
        &self,
        zone_id: ZoneId,
        scans: &[ScanQuery<'_>],
        r: ReadConcern,
        keys_only: bool,
    ) -> Result<Vec<Vec<(Vec<u8>, Stored)>>, CoordError> {
        self.ensure_zone(zone_id).await?;
        let (rf, scope) = {
            let cat = self.catalog.borrow();
            let cfg = cat.get(zone_id).ok_or(CoordError::UnknownZone(zone_id))?;
            if cfg.ordering != Ordering::Sorted { return Err(CoordError::NotSorted(zone_id)); }
            (cfg.rf, cfg.scope.clone())
        };
        let map = self.range_map(zone_id);
        let mut out = Vec::with_capacity(scans.len());
        let mut response_credits = Vec::new();
        for scans in scans.chunks(crate::wire::MAX_MULTI_SCAN_ENTRIES) {
            let mut cursors: Vec<_> = scans.iter().map(|&(lo, hi, limit)| {
                let mut slices = if limit == Some(0) || lo.zip(hi).is_some_and(|(l, h)| l >= h) {
                    Vec::new()
                } else { map.ranges_overlapping(lo, hi) };
                slices.reverse();
                let hi = slices.first().and_then(|s| s.hi.clone());
                Cursor { slices, slice: 0, hi, out: Vec::new() }
            }).collect();
            loop {
                let mut groups = BTreeMap::<NodeId, Vec<(usize, crate::wire::OwnedScan)>>::new();
                let mut needed = vec![0; cursors.len()];
                for (i, c) in cursors.iter().enumerate() {
                    let Some(slice) = c.slices.get(c.slice) else { continue; };
                    let reps = self.ring.borrow().replicas_in_zone(zone_id, &slice.token, rf, scope.as_ref());
                    if reps.is_empty() { return Err(CoordError::NoReplicas); }
                    needed[i] = r.required_replies(reps.len());
                    let max = scans[i].2.map_or(0, |n| u32::try_from(n - c.out.len()).unwrap_or(u32::MAX));
                    for node in reps {
                        groups.entry(node).or_default().push((i, (slice.lo.clone(), c.hi.clone(), max)));
                    }
                }
                if groups.is_empty() { break; }
                let futures = groups.into_iter().map(|(node, group)| async move {
                    let wire: Vec<_> = group.iter().map(|(_, (lo, hi, max))| (lo.as_deref(), hi.as_deref(), *max)).collect();
                    let results = self.fd_call_vec(node, wire.len(), || Err(TransportError::Unavailable),
                        || self.transport.scan_many_rev(node, zone_id, &wire, keys_only),
                        |out| if out.iter().any(Result::is_ok) { Some(true) }
                            else if out.iter().all(|r| matches!(r, Err(TransportError::Overloaded))) { None }
                            else { Some(false) }).await;
                    (node, group, results)
                }).collect();
                let mut pages: Vec<Vec<ScanPage>> = (0..cursors.len()).map(|_| Vec::new()).collect();
                let mut failures: Vec<Vec<ReplicaError>> = (0..cursors.len()).map(|_| Vec::new()).collect();
                for (node, group, results) in vals_rt::join_all(futures).await {
                    let mut results = results.into_iter();
                    for (i, (lo, hi, _)) in group {
                        let result = results.next().unwrap_or_else(|| Err(TransportError::Remote("short reverse scan batch".into())));
                        if pages[i].len() >= needed[i] { continue; }
                        let result = result.and_then(|page| {
                            validate_page(&page, lo.as_deref(), hi.as_deref())?;
                            response_credits.extend(self.transport.retain_response(node, scan_response_bytes(&page))?);
                            Ok(page)
                        });
                        match result {
                            Ok(page) => pages[i].push(page),
                            Err(error) => failures[i].push(ReplicaError { node, error }),
                        }
                    }
                }
                for (i, responses) in pages.into_iter().enumerate() {
                    if needed[i] == 0 { continue; }
                    let c = &mut cursors[i];
                    if responses.len() < needed[i] {
                        if failures[i].iter().any(|f| matches!(f.error, TransportError::Overloaded)) {
                            return Err(CoordError::Overloaded);
                        }
                        if let Some(epoch) = failures[i].iter().filter_map(|f| match f.error {
                            TransportError::StaleEpoch(e) => Some(e), _ => None,
                        }).max() { return Err(CoordError::StaleRing { epoch }); }
                        return Err(CoordError::ScanQuorumFailed { zone_id, cursor: c.hi.clone(), needed: needed[i],
                            got: responses.len(), replicas: std::mem::take(&mut failures[i]) });
                    }
                    if let Some(hlc) = responses.iter().flat_map(|p| p.entries.iter().map(|(_, s)| s.version.hlc)
                        .chain(p.fragments.iter().map(|f| f.version.hlc))).max() {
                        self.hlc.observe(hlc, (self.clock)());
                    }
                    let bound = merge_bound(&responses);
                    for (key, stored) in merge_lww(responses, bound.as_deref()).into_iter().rev() {
                        if stored.value.is_some() {
                            c.out.push((key, stored));
                            if scans[i].2.is_some_and(|n| c.out.len() >= n) { break; }
                        }
                    }
                    if scans[i].2.is_some_and(|n| c.out.len() >= n) {
                        c.slice = c.slices.len();
                    } else if let Some(next) = bound.filter(|b| b.as_slice() > c.slices[c.slice].lo.as_deref().unwrap_or_default()) {
                        c.hi = Some(next);
                    } else {
                        c.slice += 1;
                        c.hi = c.slices.get(c.slice).and_then(|s| s.hi.clone());
                    }
                }
            }
            out.extend(cursors.into_iter().map(|c| c.out));
        }
        Ok(out)
    }
}
