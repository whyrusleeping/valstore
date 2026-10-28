use super::*;

fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn put_bytes(b: &mut Vec<u8>, s: &[u8]) {
    put_u32(b, s.len() as u32);
    b.extend_from_slice(s);
}

fn put_list<T>(b: &mut Vec<u8>, items: &[T], mut f: impl FnMut(&mut Vec<u8>, &T)) {
    put_u32(b, items.len() as u32);
    for it in items {
        f(b, it);
    }
}

fn put_version(b: &mut Vec<u8>, v: Version) {
    b.extend_from_slice(&v.hlc.to_le_bytes());
    b.extend_from_slice(&v.node.to_le_bytes());
}

fn put_incarnation(b: &mut Vec<u8>, incarnation: Option<u128>) {
    if let Some(id) = incarnation {
        b.push(TAG_SOME);
        b.extend_from_slice(&id.to_le_bytes());
    }
}

fn put_opt(b: &mut Vec<u8>, v: Option<&[u8]>) {
    match v {
        None => b.push(TAG_NONE),
        Some(s) => {
            b.push(TAG_SOME);
            put_bytes(b, s);
        }
    }
}

fn put_opt_u32s(b: &mut Vec<u8>, v: Option<&[u32]>) {
    match v {
        None => b.push(TAG_NONE),
        Some(ids) => {
            b.push(TAG_SOME);
            put_list(b, ids, |b, &id| put_u32(b, id));
        }
    }
}

fn put_opt_u32(b: &mut Vec<u8>, v: Option<u32>) {
    match v {
        None => b.push(TAG_NONE),
        Some(x) => {
            b.push(TAG_SOME);
            put_u32(b, x);
        }
    }
}

fn put_zone(b: &mut Vec<u8>, z: &WireZone) {
    put_u32(b, z.id);
    put_u32(b, z.rf);
    b.push(z.ordering);
    put_opt_u32s(b, z.scope);
    put_bytes(b, z.name.as_bytes());
}

fn put_range_map(b: &mut Vec<u8>, m: &WireRangeMap) {
    put_u32(b, m.zone_id);
    put_u32(b, RANGE_MAP_WIRE_MAGIC);
    let has_pending = m.pending.iter().any(Option::is_some);
    let version = if m.opaque_tokens {
        RANGE_MAP_WIRE_V4
    } else if has_pending {
        RANGE_MAP_WIRE_V3
    } else {
        RANGE_MAP_WIRE_V2
    };
    put_u32(b, version);
    put_list(b, m.splits, |b, s| put_bytes(b, s));
    put_list(b, m.tokens, |b, token| put_bytes(b, token));
    if version >= RANGE_MAP_WIRE_V3 {
        put_list(b, m.pending, |b, token| put_opt(b, token.as_deref()));
    }
    if version == RANGE_MAP_WIRE_V4 {
        put_list(b, m.placement_debt, |b, &debt| b.push(u8::from(debt)));
    }
}

#[doc(hidden)]
pub fn put_sock_addr(b: &mut Vec<u8>, a: &SocketAddr) {
    match a.ip() {
        IpAddr::V4(ip) => {
            b.push(4);
            b.extend_from_slice(&ip.octets());
        }
        IpAddr::V6(ip) => {
            b.push(6);
            b.extend_from_slice(&ip.octets());
        }
    }
    b.extend_from_slice(&a.port().to_le_bytes());
}

#[doc(hidden)]
pub fn put_sock_addrs(b: &mut Vec<u8>, addrs: &[SocketAddr]) {
    put_list(b, addrs, put_sock_addr);
}

#[doc(hidden)]
pub fn read_sock_addr(buf: &[u8], pos: &mut usize) -> Option<SocketAddr> {
    let tag = *buf.get(*pos)?;
    *pos += 1;
    let ip: IpAddr = match tag {
        4 => {
            let o: [u8; 4] = buf.get(*pos..*pos + 4)?.try_into().ok()?;
            *pos += 4;
            Ipv4Addr::from(o).into()
        }
        6 => {
            let o: [u8; 16] = buf.get(*pos..*pos + 16)?.try_into().ok()?;
            *pos += 16;
            Ipv6Addr::from(o).into()
        }
        _ => return None,
    };
    let port = u16::from_le_bytes(buf.get(*pos..*pos + 2)?.try_into().ok()?);
    *pos += 2;
    Some(SocketAddr::new(ip, port))
}

#[doc(hidden)]
pub fn read_sock_addrs(buf: &[u8], pos: &mut usize) -> Option<Vec<SocketAddr>> {
    let n = u32::from_le_bytes(buf.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
    *pos += 4;
    let mut v = Vec::with_capacity(n.min(buf.len().saturating_sub(*pos)));
    for _ in 0..n {
        v.push(read_sock_addr(buf, pos)?);
    }
    Some(v)
}

fn put_node_addrs(b: &mut Vec<u8>, addrs: &[(u32, Vec<SocketAddr>)]) {
    put_list(b, addrs, |b, (id, list)| {
        put_u32(b, *id);
        put_sock_addrs(b, list);
    });
}

fn put_entries(b: &mut Vec<u8>, entries: &[(Vec<u8>, Stored)]) {
    put_list(b, entries, |b, (key, stored)| {
        put_bytes(b, key);
        put_version(b, stored.version);
        put_opt(b, stored.value.as_deref());
    });
}

fn put_digests(b: &mut Vec<u8>, digests: &[(Vec<u8>, Version)]) {
    put_list(b, digests, |b, (key, version)| {
        put_bytes(b, key);
        put_version(b, *version);
    });
}

fn put_fragment(b: &mut Vec<u8>, f: &RtFragment) {
    put_opt(b, f.lo.as_deref());
    put_opt(b, f.hi.as_deref());
    put_version(b, f.version);
}

fn frame(payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + payload.len());
    put_u32(&mut out, payload.len() as u32);
    out.extend_from_slice(&payload);
    out
}

pub fn encode_request(req: &Request) -> Vec<u8> {
    frame(encode_request_payload(req))
}

pub fn encode_request_payload(req: &Request) -> Vec<u8> {
    let mut b = Vec::new();
    match *req {
        Request::Apply {
            epoch,
            zone_id,
            key,
            value,
            version,
        } => {
            b.push(OP_APPLY);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_version(&mut b, version);
            put_bytes(&mut b, key);
            put_opt(&mut b, value);
        }
        Request::Read {
            epoch,
            zone_id,
            key,
        } => {
            b.push(OP_READ);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_bytes(&mut b, key);
        }
        Request::Digest {
            epoch,
            zone_id,
            key,
        } => {
            b.push(OP_DIGEST);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_bytes(&mut b, key);
        }
        Request::Scan {
            epoch,
            zone_id,
            lo,
            hi,
            max,
        } => {
            b.push(OP_SCAN);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_opt(&mut b, lo);
            put_opt(&mut b, hi);
            put_u32(&mut b, max);
        }
        Request::SyncTree {
            zone_id,
            level,
            scope_g,
            scope,
            requester,
        } => {
            b.push(OP_SYNC_TREE);
            put_u32(&mut b, zone_id);
            put_u32(&mut b, level);
            put_u32(&mut b, scope_g);
            put_list(&mut b, scope, |b, &id| put_u32(b, id));
            put_opt_u32(&mut b, requester);
        }
        Request::SyncBucketDigests {
            zone_id,
            num_buckets,
            ids,
            after,
        } => {
            b.push(OP_SYNC_BUCKET_DIGESTS);
            put_u32(&mut b, zone_id);
            put_u32(&mut b, num_buckets);
            put_list(&mut b, ids, |b, &id| put_u32(b, id));
            put_opt(&mut b, after);
        }
        Request::SyncValues { zone_id, keys } => {
            b.push(OP_SYNC_VALUES);
            put_u32(&mut b, zone_id);
            put_list(&mut b, keys, |b, &k| put_bytes(b, k));
        }
        Request::SetMembers {
            auth,
            epoch,
            vnodes,
            rf,
            members,
            addrs,
            incarnation,
        } => {
            b.push(OP_SET_MEMBERS);
            put_bytes(&mut b, auth);
            put_u64(&mut b, epoch);
            put_u32(&mut b, vnodes);
            put_u32(&mut b, rf);
            put_list(&mut b, members, |b, &(id, w)| {
                put_u32(b, id);
                put_u32(b, w);
            });
            put_node_addrs(&mut b, addrs);
            put_incarnation(&mut b, incarnation);
        }
        Request::Members => {
            b.push(OP_MEMBERS);
        }
        Request::SetZones {
            auth,
            epoch,
            zones,
            incarnation,
        } => {
            b.push(OP_SET_ZONES);
            put_bytes(&mut b, auth);
            put_u64(&mut b, epoch);
            put_list(&mut b, zones, |b, z| put_zone(b, z));
            put_incarnation(&mut b, incarnation);
        }
        Request::Zones => {
            b.push(OP_ZONES);
        }
        Request::SetRangeMaps {
            auth,
            epoch,
            maps,
            incarnation,
        } => {
            b.push(OP_SET_RANGE_MAPS);
            put_bytes(&mut b, auth);
            put_u64(&mut b, epoch);
            put_list(&mut b, maps, |b, m| put_range_map(b, m));
            put_incarnation(&mut b, incarnation);
        }
        Request::RangeMaps => {
            b.push(OP_RANGE_MAPS);
        }
        Request::RangeStats => {
            b.push(OP_RANGE_STATS);
        }
        Request::RangeStatsV2 => {
            b.push(OP_RANGE_STATS_V2);
        }
        Request::RangeStatsV3 => {
            b.push(OP_RANGE_STATS_V3);
        }
        Request::RangeWriteMark {
            epoch,
            zone_id,
            lower,
        } => {
            b.push(OP_RANGE_WRITE_MARK);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_bytes(&mut b, lower);
        }
        Request::GetWatermark => {
            b.push(OP_GET_WATERMARK);
        }
        Request::BatchApply {
            epoch,
            zone_id,
            sync,
            writes,
        } => {
            b.push(OP_BATCH_APPLY);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            b.push(sync as u8);
            put_list(&mut b, writes, |b, &(key, value, version)| {
                put_version(b, version);
                put_bytes(b, key);
                put_opt(b, value);
            });
        }
        Request::MultiScan { epoch, zone_id, keys_only, start, scans, reverse } => {
            b.push(OP_MULTI_SCAN);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            b.push(keys_only as u8 | ((reverse as u8) << 1));
            put_u32(&mut b, start);
            put_list(&mut b, scans, |b, &(lo, hi, max)| {
                put_opt(b, lo);
                put_opt(b, hi);
                put_u32(b, max);
            });
        }
        Request::MixedBatchRead {
            epoch,
            zone_id,
            start,
            reads,
        } => {
            b.push(OP_MIXED_BATCH_READ);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            put_u32(&mut b, start);
            put_list(&mut b, reads, |b, &(key, role)| {
                b.push(match role {
                    MixedReadRole::Value => 0,
                    MixedReadRole::Digest => 1,
                });
                put_bytes(b, key);
            });
        }
        Request::RangeDelete {
            epoch,
            zone_id,
            sync,
            version,
            slices,
        } => {
            b.push(OP_RANGE_DELETE);
            put_u64(&mut b, epoch);
            put_u32(&mut b, zone_id);
            b.push(sync as u8);
            put_version(&mut b, version);
            put_list(&mut b, slices, |b, &(lo, hi)| {
                put_opt(b, lo);
                put_opt(b, hi);
            });
        }
        Request::SyncRangeTombstones {
            zone_id,
            requester,
            digest,
            after,
        } => {
            b.push(OP_SYNC_RANGE_TOMBSTONES);
            put_u32(&mut b, zone_id);
            put_opt_u32(&mut b, requester);
            put_u64(&mut b, digest);
            put_opt(&mut b, after);
        }
        Request::GcFloor { zone_id } => {
            b.push(OP_GC_FLOOR);
            put_u32(&mut b, zone_id);
        }
        Request::CleanupStatus => {
            b.push(OP_CLEANUP_STATUS);
        }
    }
    b
}

fn put_applied(b: &mut Vec<u8>, applied: &Applied) {
    match applied {
        Applied::Won => b.push(APPLIED_WON),
        Applied::Lost(v) => {
            b.push(APPLIED_LOST);
            put_version(b, *v);
        }
        Applied::Rejected => b.push(APPLIED_REJECTED),
    }
}

fn put_range_stat_v2(b: &mut Vec<u8>, s: &OwnedRangeStat) {
    put_u32(b, s.zone_id);
    put_bytes(b, &s.lower_bound);
    put_u64(b, s.entries);
    put_opt(b, s.midpoint.as_deref());
    put_u64(b, s.ops_per_sec);
    put_u32(b, s.shard_index);
    put_u32(b, s.shard_count);
    put_u64(b, s.rehome_mirror_epoch);
    put_u64(b, s.rehome_mirror_ok);
    put_u64(b, s.rehome_mirror_failures);
    put_u64(b, s.rehome_copy_complete_epoch);
    put_u64(b, s.rehome_copy_progress);
}

pub fn encode_response(resp: &Response) -> Vec<u8> {
    frame(encode_response_payload(resp))
}

pub fn encode_response_payload(resp: &Response) -> Vec<u8> {
    let mut b = Vec::new();
    encode_response_into(&mut b, resp);
    b
}

pub fn put_read_payload(b: &mut Vec<u8>, entry: Option<(Version, Option<&[u8]>)>) {
    b.push(RESP_READ);
    match entry {
        None => b.push(TAG_NONE),
        Some((version, value)) => {
            b.push(TAG_SOME);
            put_version(b, version);
            put_opt(b, value);
        }
    }
}

pub fn encode_response_into(b: &mut Vec<u8>, resp: &Response) {
    match resp {
        Response::Apply(applied) => {
            b.push(RESP_APPLY);
            put_applied(b, applied);
        }
        Response::Read(entry) => {
            put_read_payload(b, entry.as_ref().map(|s| (s.version, s.value.as_deref())));
        }
        Response::Digest(version) => {
            b.push(RESP_DIGEST);
            match version {
                None => b.push(TAG_NONE),
                Some(v) => {
                    b.push(TAG_SOME);
                    put_version(b, *v);
                }
            }
        }
        Response::Tree(nodes) => {
            b.push(RESP_TREE);
            put_list(b, nodes, |b, &(id, d)| {
                put_u32(b, id);
                put_u64(b, d);
            });
        }
        Response::BucketDigests {
            digests,
            more,
            resume,
        } => {
            b.push(RESP_BUCKET_DIGESTS);
            b.push(*more as u8);
            put_digests(b, digests);
            if resume.is_some() {
                put_opt(b, resume.as_deref());
            }
        }
        Response::Values { entries, more } => {
            b.push(RESP_VALUES);
            b.push(*more as u8);
            put_entries(b, entries);
        }
        Response::MultiScan { start, more, keys_only, pages } => {
            b.push(RESP_MULTI_SCAN);
            put_u32(b, *start);
            b.push(*more as u8);
            b.push(*keys_only as u8);
            put_list(b, pages, |b, page| {
                b.push(page.more as u8);
                if *keys_only {
                    put_list(b, &page.entries, |b, (key, stored)| {
                        put_bytes(b, key);
                        put_version(b, stored.version);
                        b.push(stored.value.is_some() as u8);
                    });
                } else {
                    put_entries(b, &page.entries);
                }
                put_list(b, &page.fragments, put_fragment);
                put_opt(b, page.resume.as_deref());
            });
        }
        Response::Scan {
            entries,
            more,
            fragments,
            resume,
        } => {
            b.push(RESP_SCAN);
            b.push(*more as u8);
            put_entries(b, entries);
            if !fragments.is_empty() || resume.is_some() {
                put_list(b, fragments, put_fragment);
                put_opt(b, resume.as_deref());
            }
        }
        Response::RangeTombstones {
            same,
            more,
            fragments,
        } => {
            b.push(RESP_RANGE_TOMBSTONES);
            b.push(*same as u8);
            b.push(*more as u8);
            put_list(b, fragments, put_fragment);
        }
        Response::Members {
            epoch,
            vnodes,
            rf,
            members,
            addrs,
        } => {
            b.push(RESP_MEMBERS);
            put_u64(b, *epoch);
            put_u32(b, *vnodes);
            put_u32(b, *rf);
            put_list(b, members, |b, &(id, w)| {
                put_u32(b, id);
                put_u32(b, w);
            });
            put_node_addrs(b, addrs);
        }
        Response::Zones { epoch, zones } => {
            b.push(RESP_ZONES);
            put_u64(b, *epoch);
            put_list(b, zones, |b, z| {
                put_zone(
                    b,
                    &WireZone {
                        id: z.id,
                        rf: z.rf,
                        ordering: z.ordering,
                        scope: z.scope.as_deref(),
                        name: &z.name,
                    },
                );
            });
        }
        Response::RangeMaps { epoch, maps } => {
            b.push(RESP_RANGE_MAPS);
            put_u64(b, *epoch);
            put_list(b, maps, |b, m| {
                put_range_map(
                    b,
                    &WireRangeMap {
                        zone_id: m.zone_id,
                        splits: &m.splits,
                        tokens: &m.tokens,
                        pending: &m.pending,
                        placement_debt: &m.placement_debt,
                        opaque_tokens: m.opaque_tokens,
                    },
                );
            });
        }
        Response::RangeStats { epoch, ranges } => {
            b.push(RESP_RANGE_STATS);
            put_u64(b, *epoch);
            put_list(b, ranges, |b, s| {
                put_u32(b, s.zone_id);
                put_bytes(b, &s.lower_bound);
                put_u64(b, s.entries);
                put_opt(b, s.midpoint.as_deref());
                put_u32(b, s.shard_index);
                put_u32(b, s.shard_count);
                put_u64(b, s.rehome_mirror_epoch);
                put_u64(b, s.rehome_mirror_ok);
                put_u64(b, s.rehome_mirror_failures);
                put_u64(b, s.rehome_copy_complete_epoch);
                put_u64(b, s.rehome_copy_progress);
            });
        }
        Response::RangeStatsV2 { epoch, ranges } => {
            b.push(RESP_RANGE_STATS_V2);
            put_u64(b, *epoch);
            put_list(b, ranges, put_range_stat_v2);
        }
        Response::RangeStatsV3 {
            epoch,
            accounting_epoch,
            accounting_ready,
            ranges,
            recovery_seeding,
            covered_empty,
        } => {
            b.push(RESP_RANGE_STATS_V3);
            put_u64(b, *epoch);
            put_u64(b, *accounting_epoch);
            b.push(*accounting_ready as u8);
            put_list(b, ranges, put_range_stat_v2);
            if let Some(pending) = recovery_seeding {
                b.push(*pending as u8);
                if !covered_empty.is_empty() {
                    put_list(b, covered_empty, |b, index| put_u32(b, *index));
                }
            }
        }
        Response::Batch(results) => {
            b.push(RESP_BATCH);
            put_list(b, results, put_applied);
        }
        Response::MixedBatchRead {
            start,
            results,
            more,
        } => {
            b.push(RESP_MIXED_BATCH_READ);
            put_u32(b, *start);
            b.push(*more as u8);
            put_list(b, results, |b, result| match result {
                MixedReadResult::Reply(MixedReadReply::Value(None)) => b.push(0),
                MixedReadResult::Reply(MixedReadReply::Value(Some(stored))) => {
                    b.push(1);
                    put_version(b, stored.version);
                    put_opt(b, stored.value.as_deref());
                }
                MixedReadResult::Reply(MixedReadReply::Digest(None)) => b.push(2),
                MixedReadResult::Reply(MixedReadReply::Digest(Some(version))) => {
                    b.push(3);
                    put_version(b, *version);
                }
                MixedReadResult::Busy => b.push(4),
                MixedReadResult::Err(msg) => {
                    b.push(5);
                    put_bytes(b, msg.as_bytes());
                }
            });
        }
        Response::StaleEpoch(epoch) => {
            b.push(RESP_STALE_EPOCH);
            put_u64(b, *epoch);
        }
        Response::Watermark(ms) => {
            b.push(RESP_WATERMARK);
            put_u64(b, *ms);
        }
        Response::GcFloor(ms) => {
            b.push(RESP_GC_FLOOR);
            put_u64(b, *ms);
        }
        Response::CleanupStatus { ring_epoch, gc_grace_ms, records, unsettled } => {
            b.push(RESP_CLEANUP_STATUS);
            put_u64(b, *ring_epoch);
            put_u64(b, *gc_grace_ms);
            put_u64(b, *records);
            put_list(b, unsettled, |b, zone| {
                put_u32(b, zone.zone_id);
                b.push(zone.lapped as u8 | (zone.pending_rehome as u8) << 1);
                put_u64(b, zone.kept);
                put_list(b, &zone.samples, |b, key| put_bytes(b, key));
            });
        }
        Response::Denied => {
            b.push(RESP_DENIED);
        }
        Response::Busy => {
            b.push(RESP_BUSY);
        }
        Response::Err(msg) => {
            b.push(RESP_ERR);
            put_bytes(b, msg.as_bytes());
        }
    }
}

