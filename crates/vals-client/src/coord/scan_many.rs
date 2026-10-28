use super::*;
use super::scan::{scan_merge_bound, scan_merge_lww};

/// One scan in a [`Coordinator::scan_many_in`] call: `(lo, hi, limit)` with
/// the meaning of [`Coordinator::scan_in`]'s arguments.
pub type ScanQuery<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, Option<usize>);

struct Cursor {
    slices: Vec<crate::partition::RangeSlice>,
    slice: usize,
    lo: Option<Vec<u8>>,
    out: Vec<(Vec<u8>, Stored)>,
}

impl<T: Transport> Coordinator<T> {
    /// Runs several scans of sorted zone `zone_id` together, batching the
    /// requests each node receives.
    ///
    /// Returns one list per query, in input order, of `(key, Stored)` pairs in
    /// ascending key order; deleted keys are omitted. With `keys_only` the
    /// replicas send no values and each value is empty. Consistency is that of [`scan_in`](Self::scan_in); one
    /// failed query fails the whole call.
    pub async fn scan_many_in(
        &self,
        zone_id: ZoneId,
        scans: &[ScanQuery<'_>],
        r: impl Into<ReadConcern>,
        keys_only: bool,
    ) -> Result<Vec<Vec<(Vec<u8>, Stored)>>, CoordError> {
        let r = r.into();
        self.retry_stale(|| self.scan_many_once(zone_id, scans, r, keys_only)).await
    }

    async fn scan_many_once(
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
                let slices = if limit == Some(0) || lo.zip(hi).is_some_and(|(l, h)| l >= h) {
                    Vec::new()
                } else { map.ranges_overlapping(lo, hi) };
                let lo = slices.first().and_then(|s| s.lo.clone());
                Cursor { slices, slice: 0, lo, out: Vec::new() }
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
                        groups.entry(node).or_default().push((i, (c.lo.clone(), slice.hi.clone(), max)));
                    }
                }
                if groups.is_empty() { break; }
                let futures = groups.into_iter().map(|(node, group)| async move {
                    let wire: Vec<_> = group.iter().map(|(_, (lo, hi, max))| (lo.as_deref(), hi.as_deref(), *max)).collect();
                    let results = self.fd_call_vec(node, wire.len(), || Err(TransportError::Unavailable),
                        || self.transport.scan_many(node, zone_id, &wire, keys_only),
                        |out| if out.iter().any(Result::is_ok) { Some(true) }
                            else if out.iter().all(|r| matches!(r, Err(TransportError::Overloaded))) { None }
                            else { Some(false) }).await;
                    (node, group, results)
                }).collect();
                let mut pages: Vec<Vec<ScanPage>> = (0..cursors.len()).map(|_| Vec::new()).collect();
                let mut failures: Vec<Vec<ReplicaError>> = (0..cursors.len()).map(|_| Vec::new()).collect();
                for (node, group, results) in vals_rt::join_all(futures).await {
                    let mut results = results.into_iter();
                    for (i, _) in group {
                        let result = results.next().unwrap_or_else(|| Err(TransportError::Remote("short scan batch".into())));
                        if pages[i].len() >= needed[i] { continue; }
                        let result = result.and_then(|page| {
                            if (page.more && page.entries.is_empty() && page.resume.is_none())
                                || page.resume.as_deref().is_some_and(|r| r <= cursors[i].lo.as_deref().unwrap_or_default()) {
                                return Err(TransportError::Remote("non-advancing scan page".into()));
                            }
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
                        return Err(CoordError::ScanQuorumFailed { zone_id, cursor: c.lo.clone(), needed: needed[i],
                            got: responses.len(), replicas: std::mem::take(&mut failures[i]) });
                    }
                    if let Some(hlc) = responses.iter().flat_map(|p| p.entries.iter().map(|(_, s)| s.version.hlc)
                        .chain(p.fragments.iter().map(|f| f.version.hlc))).max() {
                        self.hlc.observe(hlc, (self.clock)());
                    }
                    let bound = scan_merge_bound(&responses);
                    for (key, stored) in scan_merge_lww(responses, bound.as_deref()) {
                        if stored.value.is_some() {
                            c.out.push((key, stored));
                            if scans[i].2.is_some_and(|n| c.out.len() >= n) { break; }
                        }
                    }
                    if scans[i].2.is_some_and(|n| c.out.len() >= n) {
                        c.slice = c.slices.len();
                    } else if let Some(next) = bound.filter(|b| c.slices[c.slice].hi.as_deref().is_none_or(|hi| b.as_slice() < hi)) {
                        c.lo = Some(next);
                    } else {
                        c.slice += 1;
                        c.lo = c.slices.get(c.slice).and_then(|s| s.lo.clone());
                    }
                }
            }
            out.extend(cursors.into_iter().map(|c| c.out));
        }
        Ok(out)
    }
}
