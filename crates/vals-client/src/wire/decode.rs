use super::*;

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], WireError> {
        let end = self.pos.checked_add(n).ok_or(WireError::Truncated)?;
        if end > self.buf.len() {
            return Err(WireError::Truncated);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn alloc_hint(&self, n: usize) -> usize {
        n.min(self.buf.len() - self.pos)
    }

    fn u8(&mut self) -> Result<u8, WireError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, WireError> {
        let b = self.take(4)?;
        Ok(u32::from_le_bytes(b.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, WireError> {
        let b = self.take(8)?;
        Ok(u64::from_le_bytes(b.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], WireError> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    fn list<T>(
        &mut self,
        f: impl FnMut(&mut Self) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        self.bounded_list(usize::MAX, f)
    }

    fn bounded_list<T>(
        &mut self,
        max: usize,
        mut f: impl FnMut(&mut Self) -> Result<T, WireError>,
    ) -> Result<Vec<T>, WireError> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(WireError::TooManyEntries);
        }
        let mut v = Vec::with_capacity(self.alloc_hint(n));
        for _ in 0..n {
            v.push(f(self)?);
        }
        Ok(v)
    }

    fn version(&mut self) -> Result<Version, WireError> {
        let hlc = self.u64()?;
        let node = self.u64()?;
        Ok(Version { hlc, node })
    }

    fn incarnation_trailer(&mut self) -> Result<Option<u128>, WireError> {
        if self.pos == self.buf.len() {
            return Ok(None);
        }
        match self.u8()? {
            TAG_SOME => {
                let id = u128::from_le_bytes(self.take(16)?.try_into().unwrap());
                if self.pos != self.buf.len() {
                    return Err(WireError::TrailingBytes);
                }
                Ok(Some(id))
            }
            tag => Err(WireError::BadTag(tag)),
        }
    }

    fn opt_bytes(&mut self) -> Result<Option<Vec<u8>>, WireError> {
        match self.u8()? {
            TAG_NONE => Ok(None),
            TAG_SOME => Ok(Some(self.bytes()?.to_vec())),
            other => Err(WireError::BadTag(other)),
        }
    }

    fn opt_u32s(&mut self) -> Result<Option<Vec<u32>>, WireError> {
        match self.u8()? {
            TAG_NONE => Ok(None),
            TAG_SOME => Ok(Some(self.list(|r| r.u32())?)),
            other => Err(WireError::BadTag(other)),
        }
    }

    fn opt_u32(&mut self) -> Result<Option<u32>, WireError> {
        match self.u8()? {
            TAG_NONE => Ok(None),
            TAG_SOME => Ok(Some(self.u32()?)),
            other => Err(WireError::BadTag(other)),
        }
    }

    fn zone(&mut self) -> Result<OwnedZone, WireError> {
        let id = self.u32()?;
        let rf = self.u32()?;
        let ordering = self.u8()?;
        let scope = self.opt_u32s()?;
        let name = std::str::from_utf8(self.bytes()?)
            .map_err(|_| WireError::BadUtf8)?
            .to_owned();
        Ok(OwnedZone {
            id,
            rf,
            ordering,
            scope,
            name,
        })
    }

    fn entries(&mut self) -> Result<Vec<(Vec<u8>, Stored)>, WireError> {
        self.list(|r| {
            let key = r.bytes()?.to_vec();
            let version = r.version()?;
            let value = r.opt_bytes()?;
            Ok((key, Stored { version, value }))
        })
    }

    fn digests(&mut self) -> Result<Vec<(Vec<u8>, Version)>, WireError> {
        self.list(|r| {
            let key = r.bytes()?.to_vec();
            let version = r.version()?;
            Ok((key, version))
        })
    }

    fn range_map(&mut self) -> Result<OwnedRangeMap, WireError> {
        let zone_id = self.u32()?;
        if self.u32()? != RANGE_MAP_WIRE_MAGIC {
            return Err(WireError::BadRangeMap);
        }
        let version = self.u32()?;
        if version != RANGE_MAP_WIRE_V2
            && version != RANGE_MAP_WIRE_V3
            && version != RANGE_MAP_WIRE_V4
        {
            return Err(WireError::BadRangeMap);
        }
        let splits = self.list(|r| Ok(r.bytes()?.to_vec()))?;
        let tokens = self.list(|r| Ok(r.bytes()?.to_vec()))?;
        let pending = if version >= RANGE_MAP_WIRE_V3 {
            self.list(|r| r.opt_bytes())?
        } else {
            vec![None; tokens.len()]
        };
        let opaque_tokens = version == RANGE_MAP_WIRE_V4;
        let placement_debt = if opaque_tokens {
            self.list(|r| match r.u8()? {
                0 => Ok(false),
                1 => Ok(true),
                tag => Err(WireError::BadTag(tag)),
            })?
        } else {
            tokens.iter().enumerate().map(|(i, token)| {
                let lower = if i == 0 {
                    b"".as_slice()
                } else {
                    splits.get(i - 1).map(Vec::as_slice).unwrap_or_default()
                };
                token.as_slice() != lower
            }).collect()
        };
        if opaque_tokens {
            RangeMap::from_opaque_parts(
                splits.clone(), tokens.clone(), pending.clone(), placement_debt.clone()
            )
        } else {
            RangeMap::from_splits_tokens_and_pending(
                splits.clone(), tokens.clone(), pending.clone()
            )
        }.map_err(|_| WireError::BadRangeMap)?;
        Ok(OwnedRangeMap {
            zone_id,
            splits,
            tokens,
            pending,
            placement_debt,
            opaque_tokens,
        })
    }

    fn range_maps(&mut self) -> Result<Vec<OwnedRangeMap>, WireError> {
        self.list(|r| r.range_map())
    }

    fn range_stat_v2(&mut self) -> Result<OwnedRangeStat, WireError> {
        let zone_id = self.u32()?;
        let lower_bound = self.bytes()?.to_vec();
        let entries = self.u64()?;
        let midpoint = self.opt_bytes()?;
        let ops_per_sec = self.u64()?;
        let shard_index = self.u32()?;
        let shard_count = self.u32()?;
        let rehome_mirror_epoch = self.u64()?;
        let rehome_mirror_ok = self.u64()?;
        let rehome_mirror_failures = self.u64()?;
        let rehome_copy_complete_epoch = self.u64()?;
        let rehome_copy_progress = self.u64()?;
        Ok(OwnedRangeStat {
            zone_id,
            lower_bound,
            entries,
            midpoint,
            ops_per_sec,
            shard_index,
            shard_count,
            rehome_mirror_epoch,
            rehome_mirror_ok,
            rehome_mirror_failures,
            rehome_copy_complete_epoch,
            rehome_copy_progress,
        })
    }

    fn node_addrs(&mut self) -> Result<Vec<(u32, Vec<SocketAddr>)>, WireError> {
        self.list(|r| {
            let id = r.u32()?;
            let mut pos = r.pos;
            let addrs = read_sock_addrs(r.buf, &mut pos).ok_or(WireError::Truncated)?;
            r.pos = pos;
            Ok((id, addrs))
        })
    }

    fn fragment(&mut self) -> Result<RtFragment, WireError> {
        let lo = self.opt_bytes()?;
        let hi = self.opt_bytes()?;
        let version = self.version()?;
        Ok(RtFragment { lo, hi, version })
    }

    fn applied(&mut self) -> Result<Applied, WireError> {
        match self.u8()? {
            APPLIED_WON => Ok(Applied::Won),
            APPLIED_LOST => Ok(Applied::Lost(self.version()?)),
            APPLIED_REJECTED => Ok(Applied::Rejected),
            other => Err(WireError::BadTag(other)),
        }
    }
}

pub fn decode_request(payload: &[u8]) -> Result<OwnedRequest, WireError> {
    let mut r = Reader::new(payload);
    match r.u8()? {
        OP_APPLY => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let version = r.version()?;
            let key = r.bytes()?.to_vec();
            let value = r.opt_bytes()?;
            Ok(OwnedRequest::Apply {
                epoch,
                zone_id,
                key,
                value,
                version,
            })
        }
        OP_READ => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let key = r.bytes()?.to_vec();
            Ok(OwnedRequest::Read {
                epoch,
                zone_id,
                key,
            })
        }
        OP_DIGEST => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let key = r.bytes()?.to_vec();
            Ok(OwnedRequest::Digest {
                epoch,
                zone_id,
                key,
            })
        }
        OP_SCAN => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let lo = r.opt_bytes()?;
            let hi = r.opt_bytes()?;
            let max = r.u32()?;
            Ok(OwnedRequest::Scan {
                epoch,
                zone_id,
                lo,
                hi,
                max,
            })
        }
        OP_SYNC_TREE => {
            let zone_id = r.u32()?;
            let level = r.u32()?;
            let scope_g = r.u32()?;
            let scope = r.list(|r| r.u32())?;
            let requester = r.opt_u32()?;
            Ok(OwnedRequest::SyncTree {
                zone_id,
                level,
                scope_g,
                scope,
                requester,
            })
        }
        OP_SYNC_BUCKET_DIGESTS => {
            let zone_id = r.u32()?;
            let num_buckets = r.u32()?;
            let ids = r.list(|r| r.u32())?;
            let after = r.opt_bytes()?;
            Ok(OwnedRequest::SyncBucketDigests {
                zone_id,
                num_buckets,
                ids,
                after,
            })
        }
        OP_SYNC_VALUES => {
            let zone_id = r.u32()?;
            let keys = r.list(|r| Ok(r.bytes()?.to_vec()))?;
            Ok(OwnedRequest::SyncValues { zone_id, keys })
        }
        OP_SET_MEMBERS => {
            let auth = r.bytes()?.to_vec();
            let epoch = r.u64()?;
            let vnodes = r.u32()?;
            let rf = r.u32()?;
            let members = r.list(|r| Ok((r.u32()?, r.u32()?)))?;
            let addrs = r.node_addrs()?;
            let incarnation = r.incarnation_trailer()?;
            Ok(OwnedRequest::SetMembers {
                auth,
                epoch,
                vnodes,
                rf,
                members,
                addrs,
                incarnation,
            })
        }
        OP_MEMBERS => Ok(OwnedRequest::Members),
        OP_SET_ZONES => {
            let auth = r.bytes()?.to_vec();
            let epoch = r.u64()?;
            let zones = r.list(|r| r.zone())?;
            let incarnation = r.incarnation_trailer()?;
            Ok(OwnedRequest::SetZones {
                auth,
                epoch,
                zones,
                incarnation,
            })
        }
        OP_ZONES => Ok(OwnedRequest::Zones),
        OP_SET_RANGE_MAPS => {
            let auth = r.bytes()?.to_vec();
            let epoch = r.u64()?;
            let maps = r.range_maps()?;
            let incarnation = r.incarnation_trailer()?;
            Ok(OwnedRequest::SetRangeMaps {
                auth,
                epoch,
                maps,
                incarnation,
            })
        }
        OP_RANGE_MAPS => Ok(OwnedRequest::RangeMaps),
        OP_RANGE_STATS => Ok(OwnedRequest::RangeStats),
        OP_RANGE_STATS_V2 => Ok(OwnedRequest::RangeStatsV2),
        OP_RANGE_STATS_V3 => Ok(OwnedRequest::RangeStatsV3),
        OP_RANGE_WRITE_MARK => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let lower = r.bytes()?.to_vec();
            Ok(OwnedRequest::RangeWriteMark {
                epoch,
                zone_id,
                lower,
            })
        }
        OP_GET_WATERMARK => Ok(OwnedRequest::GetWatermark),
        OP_BATCH_APPLY => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let flags = r.u8()?;
            let writes = r.list(|r| {
                let version = r.version()?;
                let key = r.bytes()?.to_vec();
                let value = r.opt_bytes()?;
                Ok((key, value, version))
            })?;
            Ok(OwnedRequest::BatchApply {
                epoch,
                zone_id,
                sync: flags & !2 != 0,
                fill: flags & 2 != 0,
                writes,
            })
        }
        OP_MULTI_SCAN => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let flags = r.u8()?;
            if flags & !3 != 0 { return Err(WireError::BadTag(flags)); }
            let keys_only = flags & 1 != 0;
            let reverse = flags & 2 != 0;
            let start = r.u32()?;
            let scans = r.bounded_list(MAX_MULTI_SCAN_ENTRIES, |r| {
                Ok((r.opt_bytes()?, r.opt_bytes()?, r.u32()?))
            })?;
            if start as usize > scans.len() { return Err(WireError::Truncated); }
            if r.pos != payload.len() { return Err(WireError::TrailingBytes); }
            Ok(OwnedRequest::MultiScan { epoch, zone_id, keys_only, start, scans, reverse })
        }
        OP_MIXED_BATCH_READ => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let start = r.u32()?;
            let reads = r.bounded_list(MAX_MIXED_BATCH_READ_ENTRIES, |r| {
                let role = match r.u8()? {
                    0 => MixedReadRole::Value,
                    1 => MixedReadRole::Digest,
                    tag => return Err(WireError::BadTag(tag)),
                };
                Ok((r.bytes()?.to_vec(), role))
            })?;
            if start as usize > reads.len() {
                return Err(WireError::Truncated);
            }
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(OwnedRequest::MixedBatchRead {
                epoch,
                zone_id,
                start,
                reads,
            })
        }
        OP_RANGE_DELETE => {
            let epoch = r.u64()?;
            let zone_id = r.u32()?;
            let sync = match r.u8()? {
                0 => false,
                1 => true,
                tag => return Err(WireError::BadTag(tag)),
            };
            let version = r.version()?;
            let slices = r.bounded_list(MAX_RANGE_DELETE_SLICES, |r| Ok((r.opt_bytes()?, r.opt_bytes()?)))?;
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(OwnedRequest::RangeDelete {
                epoch,
                zone_id,
                sync,
                version,
                slices,
            })
        }
        OP_SYNC_RANGE_TOMBSTONES => {
            let zone_id = r.u32()?;
            let requester = r.opt_u32()?;
            let digest = r.u64()?;
            let after = r.opt_bytes()?;
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(OwnedRequest::SyncRangeTombstones {
                zone_id,
                requester,
                digest,
                after,
            })
        }
        OP_GC_FLOOR => {
            let zone_id = r.u32()?;
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(OwnedRequest::GcFloor { zone_id })
        }
        OP_CLEANUP_STATUS => {
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(OwnedRequest::CleanupStatus)
        }
        other => Err(WireError::BadTag(other)),
    }
}

pub fn decode_response(payload: &[u8]) -> Result<Response, WireError> {
    let mut r = Reader::new(payload);
    match r.u8()? {
        RESP_APPLY => Ok(Response::Apply(r.applied()?)),
        RESP_READ => match r.u8()? {
            TAG_NONE => Ok(Response::Read(None)),
            TAG_SOME => {
                let version = r.version()?;
                let value = r.opt_bytes()?;
                Ok(Response::Read(Some(Stored { version, value })))
            }
            other => Err(WireError::BadTag(other)),
        },
        RESP_DIGEST => match r.u8()? {
            TAG_NONE => Ok(Response::Digest(None)),
            TAG_SOME => Ok(Response::Digest(Some(r.version()?))),
            other => Err(WireError::BadTag(other)),
        },
        RESP_TREE => Ok(Response::Tree(r.list(|r| Ok((r.u32()?, r.u64()?)))?)),
        RESP_BUCKET_DIGESTS => {
            let more = match r.u8()? {
                0 => false,
                1 => true,
                other => return Err(WireError::BadTag(other)),
            };
            let digests = r.digests()?;
            let resume = if r.pos == r.buf.len() { None } else { r.opt_bytes()? };
            Ok(Response::BucketDigests {
                digests,
                more,
                resume,
            })
        }
        RESP_VALUES => {
            let more = match r.u8()? {
                0 => false,
                1 => true,
                other => return Err(WireError::BadTag(other)),
            };
            Ok(Response::Values {
                entries: r.entries()?,
                more,
            })
        }
        RESP_MULTI_SCAN => {
            let start = r.u32()?;
            let flag = |r: &mut Reader<'_>| match r.u8()? {
                0 => Ok(false), 1 => Ok(true), tag => Err(WireError::BadTag(tag)),
            };
            let more = flag(&mut r)?;
            let keys_only = flag(&mut r)?;
            let pages = r.bounded_list(MAX_MULTI_SCAN_ENTRIES, |r| {
                let more = flag(r)?;
                let entries = if keys_only {
                    r.list(|r| {
                        let key = r.bytes()?.to_vec();
                        let version = r.version()?;
                        let value = flag(r)?.then(Vec::new);
                        Ok((key, Stored { version, value }))
                    })?
                } else { r.entries()? };
                let fragments = r.list(Reader::fragment)?;
                let resume = r.opt_bytes()?;
                Ok(crate::coord::ScanPage { entries, more, fragments, resume })
            })?;
            if r.pos != payload.len() { return Err(WireError::TrailingBytes); }
            Ok(Response::MultiScan { start, more, keys_only, pages })
        }
        RESP_SCAN => {
            let more = match r.u8()? {
                0 => false,
                1 => true,
                other => return Err(WireError::BadTag(other)),
            };
            let entries = r.entries()?;
            let (fragments, resume) = if r.pos == payload.len() {
                (Vec::new(), None)
            } else {
                let fragments = r.list(Reader::fragment)?;
                let resume = r.opt_bytes()?;
                if r.pos != payload.len() {
                    return Err(WireError::TrailingBytes);
                }
                (fragments, resume)
            };
            Ok(Response::Scan {
                entries,
                more,
                fragments,
                resume,
            })
        }
        RESP_RANGE_TOMBSTONES => {
            let same = match r.u8()? {
                0 => false,
                1 => true,
                other => return Err(WireError::BadTag(other)),
            };
            let more = match r.u8()? {
                0 => false,
                1 => true,
                other => return Err(WireError::BadTag(other)),
            };
            let fragments = r.list(Reader::fragment)?;
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(Response::RangeTombstones {
                same,
                more,
                fragments,
            })
        }
        RESP_MEMBERS => {
            let epoch = r.u64()?;
            let vnodes = r.u32()?;
            let rf = r.u32()?;
            let members = r.list(|r| Ok((r.u32()?, r.u32()?)))?;
            let addrs = r.node_addrs()?;
            Ok(Response::Members {
                epoch,
                vnodes,
                rf,
                members,
                addrs,
            })
        }
        RESP_ZONES => {
            let epoch = r.u64()?;
            let zones = r.list(|r| r.zone())?;
            Ok(Response::Zones { epoch, zones })
        }
        RESP_BATCH => Ok(Response::Batch(r.list(|r| r.applied())?)),
        RESP_MIXED_BATCH_READ => {
            let start = r.u32()?;
            let more = match r.u8()? {
                0 => false,
                1 => true,
                tag => return Err(WireError::BadTag(tag)),
            };
            let results = r.bounded_list(MAX_MIXED_BATCH_READ_ENTRIES, |r| match r.u8()? {
                0 => Ok(MixedReadResult::Reply(MixedReadReply::Value(None))),
                1 => {
                    let version = r.version()?;
                    let value = r.opt_bytes()?;
                    Ok(MixedReadResult::Reply(MixedReadReply::Value(Some(Stored {
                        version,
                        value,
                    }))))
                }
                2 => Ok(MixedReadResult::Reply(MixedReadReply::Digest(None))),
                3 => Ok(MixedReadResult::Reply(MixedReadReply::Digest(Some(
                    r.version()?,
                )))),
                4 => Ok(MixedReadResult::Busy),
                5 => {
                    let msg = std::str::from_utf8(r.bytes()?)
                        .map_err(|_| WireError::BadUtf8)?
                        .to_owned();
                    Ok(MixedReadResult::Err(msg))
                }
                tag => Err(WireError::BadTag(tag)),
            })?;
            if r.pos != payload.len() {
                return Err(WireError::TrailingBytes);
            }
            Ok(Response::MixedBatchRead {
                start,
                results,
                more,
            })
        }
        RESP_RANGE_MAPS => {
            let epoch = r.u64()?;
            let maps = r.range_maps()?;
            Ok(Response::RangeMaps { epoch, maps })
        }
        RESP_RANGE_STATS => {
            let epoch = r.u64()?;
            let ranges = r.list(|r| {
                let zone_id = r.u32()?;
                let lower_bound = r.bytes()?.to_vec();
                let entries = r.u64()?;
                let midpoint = r.opt_bytes()?;
                let shard_index = r.u32()?;
                let shard_count = r.u32()?;
                let rehome_mirror_epoch = r.u64()?;
                let rehome_mirror_ok = r.u64()?;
                let rehome_mirror_failures = r.u64()?;
                let rehome_copy_complete_epoch = r.u64()?;
                let rehome_copy_progress = r.u64()?;
                Ok(OwnedRangeStat {
                    zone_id,
                    lower_bound,
                    entries,
                    midpoint,
                    ops_per_sec: 0,
                    shard_index,
                    shard_count,
                    rehome_mirror_epoch,
                    rehome_mirror_ok,
                    rehome_mirror_failures,
                    rehome_copy_complete_epoch,
                    rehome_copy_progress,
                })
            })?;
            Ok(Response::RangeStats { epoch, ranges })
        }
        RESP_RANGE_STATS_V2 => {
            let epoch = r.u64()?;
            let ranges = r.list(Reader::range_stat_v2)?;
            Ok(Response::RangeStatsV2 { epoch, ranges })
        }
        RESP_RANGE_STATS_V3 => {
            let epoch = r.u64()?;
            let accounting_epoch = r.u64()?;
            let accounting_ready = match r.u8()? {
                0 => false,
                1 => true,
                tag => return Err(WireError::BadTag(tag)),
            };
            let ranges = r.list(Reader::range_stat_v2)?;
            let recovery_seeding = if r.pos < payload.len() {
                match r.u8()? {
                    0 => Some(false),
                    1 => Some(true),
                    tag => return Err(WireError::BadTag(tag)),
                }
            } else {
                None
            };
            let covered_empty = if r.pos < payload.len() {
                r.list(|r| r.u32())?
            } else {
                Vec::new()
            };
            Ok(Response::RangeStatsV3 {
                epoch,
                accounting_epoch,
                accounting_ready,
                ranges,
                recovery_seeding,
                covered_empty,
            })
        }
        RESP_STALE_EPOCH => Ok(Response::StaleEpoch(r.u64()?)),
        RESP_DENIED => Ok(Response::Denied),
        RESP_BUSY => Ok(Response::Busy),
        RESP_WATERMARK => Ok(Response::Watermark(r.u64()?)),
        RESP_GC_FLOOR => Ok(Response::GcFloor(r.u64()?)),
        RESP_CLEANUP_STATUS => {
            let ring_epoch = r.u64()?;
            let gc_grace_ms = r.u64()?;
            let records = r.u64()?;
            let unsettled = r.list(|r| {
                let zone_id = r.u32()?;
                let flags = r.u8()?;
                if flags > 0b11 {
                    return Err(WireError::BadTag(flags));
                }
                let (lapped, pending_rehome) = (flags & 1 != 0, flags & 2 != 0);
                let kept = r.u64()?;
                let samples = r.list(|r| Ok(r.bytes()?.to_vec()))?;
                Ok(UnsettledZone { zone_id, lapped, pending_rehome, kept, samples })
            })?;
            Ok(Response::CleanupStatus { ring_epoch, gc_grace_ms, records, unsettled })
        }
        RESP_ERR => {
            let msg = std::str::from_utf8(r.bytes()?)
                .map_err(|_| WireError::BadUtf8)?
                .to_owned();
            Ok(Response::Err(msg))
        }
        other => Err(WireError::BadTag(other)),
    }
}

