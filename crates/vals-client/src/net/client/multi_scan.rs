use super::*;

impl NetTransport {
    pub(super) async fn multi_scan<const REVERSE: bool>(
        &self,
        node: NodeId,
        zone_id: ZoneId,
        scans: &[wire::WireScan<'_>],
        keys_only: bool,
    ) -> Vec<Result<ScanPage, TransportError>> {
        let mut groups = std::collections::BTreeMap::<usize, Vec<usize>>::new();
        for (i, &(lo, _, _)) in scans.iter().enumerate() {
            groups.entry(self.scan_shard(node, zone_id, lo)).or_default().push(i);
        }
        let futures = groups.into_iter().map(|(shard, indices)| async move {
            let mut out = Vec::new();
            let mut credits = Vec::new();
            for chunk in indices.chunks(wire::MAX_MULTI_SCAN_ENTRIES) {
                if self.multi_scan_support::<REVERSE>(node, shard).is_none() {
                    let probe = self.rpc(node, shard, &Request::GetWatermark, 1).await;
                    if let Err(error) = probe {
                        out.extend(chunk.iter().map(|&i| (i, Err(error.clone()))));
                        continue;
                    }
                }
                if self.multi_scan_support::<REVERSE>(node, shard) != Some(true) {
                    if REVERSE {
                        out.extend(chunk.iter().map(|&i| (i, Err(TransportError::Remote(
                            "reverse scans require MULTI_SCAN and REVERSE_SCAN".into(),
                        )))));
                        continue;
                    }
                    for &i in chunk {
                        let (lo, hi, max) = scans[i];
                        let result = self.scan(node, zone_id, lo, hi, max).await.and_then(|page| {
                            credits.extend(self.retain_response(node, crate::coord::scan_response_bytes(&page))?);
                            Ok(page)
                        });
                        out.push((i, result));
                    }
                    continue;
                }
                let wire_scans: Vec<_> = chunk.iter().map(|&i| scans[i]).collect();
                let mut start = 0;
                while start < chunk.len() {
                    let request = Request::MultiScan {
                        epoch: self.route_epoch.get(), zone_id, keys_only,
                        start: start as u32, scans: &wire_scans, reverse: REVERSE,
                    };
                    let reply = if start == 0 {
                        self.rpc(node, shard, &request, chunk.len()).await
                    } else {
                        self.rpc_nowait(node, shard, &request, chunk.len() - start).await
                    };
                    let response = reply.map(|reply| {
                        credits.extend(reply.response_credit);
                        reply.response
                    });
                    let error = match response {
                        Ok(Response::MultiScan { start: got, more, keys_only: mode, pages })
                            if got as usize == start && mode == keys_only && !pages.is_empty()
                                && pages.len() <= chunk.len() - start
                                && more == (start + pages.len() < chunk.len()) => {
                            for page in pages {
                                out.push((chunk[start], Ok(page)));
                                start += 1;
                            }
                            continue;
                        }
                        Ok(Response::Busy) => TransportError::Busy,
                        Ok(Response::StaleEpoch(e)) => TransportError::StaleEpoch(e),
                        Ok(Response::Err(e)) => TransportError::Remote(e),
                        Err(e) => e,
                        _ => TransportError::Remote("invalid MultiScan reply".into()),
                    };
                    for &i in &chunk[start..] { out.push((i, Err(error.clone()))); }
                    break;
                }
            }
            for (_, result) in &mut out {
                if let Ok(page) = result {
                    if keys_only {
                        for (_, stored) in &mut page.entries {
                            stored.value = stored.value.as_ref().map(|_| Vec::new());
                        }
                    }
                }
            }
            (out, credits)
        }).collect();
        let mut out: Vec<_> = (0..scans.len()).map(|_| Err(TransportError::Unavailable)).collect();
        let mut credits = Vec::new();
        for (replies, held) in vals_rt::join_all(futures).await {
            credits.extend(held);
            for (i, result) in replies { out[i] = result; }
        }
        out
    }
}
