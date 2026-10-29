use std::net::SocketAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};

use vals_cluster::wire::{
    decode_request, decode_response, encode_request_payload, encode_response_payload,
    OwnedRangeMap, OwnedRangeStat, OwnedRequest, OwnedWrite, OwnedZone, Request, Response,
    RtFragment, UnsettledZone, WireRangeMap, WireSlice, WireWrite, WireZone,
};
use vals_cluster::{Applied, Stored, Version};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    fn bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    fn buf(&mut self, max: usize) -> Vec<u8> {
        let n = self.below(max as u64 + 1) as usize;
        (0..n).map(|_| self.byte()).collect()
    }

    fn version(&mut self) -> Version {
        Version {
            hlc: self.next_u64(),
            node: self.next_u64(),
        }
    }

    fn opt_buf(&mut self, max: usize) -> Option<Vec<u8>> {
        if self.bool() {
            None
        } else {
            Some(self.buf(max))
        }
    }

    fn sockaddr(&mut self) -> SocketAddr {
        let port = self.next_u64() as u16;
        if self.bool() {
            SocketAddr::from(([self.byte(), self.byte(), self.byte(), self.byte()], port))
        } else {
            let mut o = [0u8; 16];
            for b in o.iter_mut() {
                *b = self.byte();
            }
            SocketAddr::from((o, port))
        }
    }

    fn sockaddrs(&mut self) -> Vec<SocketAddr> {
        let n = self.below(4) as usize;
        (0..n).map(|_| self.sockaddr()).collect()
    }

    fn incarnation(&mut self) -> Option<u128> {
        (self.below(2) == 1)
            .then(|| (u128::from(self.next_u64()) << 64) | u128::from(self.next_u64()))
    }

    fn node_addrs(&mut self) -> Vec<(u32, Vec<SocketAddr>)> {
        let n = self.below(4) as usize;
        (0..n)
            .map(|_| (self.next_u64() as u32, self.sockaddrs()))
            .collect()
    }

    fn members(&mut self) -> Vec<(u32, u32)> {
        let n = self.below(6) as usize;
        (0..n)
            .map(|_| (self.next_u64() as u32, self.next_u64() as u32))
            .collect()
    }

    fn name(&mut self) -> String {
        let n = self.below(10) as usize;
        (0..n).map(|_| (self.byte() & 0x7f) as char).collect()
    }

    fn scope(&mut self) -> Option<Vec<u32>> {
        if self.bool() {
            None
        } else {
            let n = self.below(5) as usize;
            Some((0..n).map(|_| self.next_u64() as u32).collect())
        }
    }

    fn zone(&mut self) -> OwnedZone {
        OwnedZone {
            id: self.next_u64() as u32,
            rf: self.below(8) as u32,
            ordering: self.byte(),
            scope: self.scope(),
            name: self.name(),
        }
    }

    fn range_map(&mut self) -> OwnedRangeMap {
        let n = self.below(5) as usize;
        let splits: Vec<Vec<u8>> = (0..n).map(|i| vec![(i + 1) as u8]).collect();
        let mut tokens = vec![Vec::new()];
        for split in &splits {
            if self.below(2) == 0 {
                tokens.push(tokens.last().unwrap().clone());
            } else {
                tokens.push(split.clone());
            }
        }
        let pending = vec![None; tokens.len()];
        let placement_debt = tokens.iter().enumerate().map(|(i, token)| {
            let lower = if i == 0 { b"".as_slice() } else { splits[i - 1].as_slice() };
            token.as_slice() != lower
        }).collect();
        OwnedRangeMap {
            zone_id: self.next_u64() as u32,
            splits,
            tokens,
            pending,
            placement_debt,
            opaque_tokens: false,
        }
    }

    fn range_stat(&mut self) -> OwnedRangeStat {
        OwnedRangeStat {
            zone_id: self.next_u64() as u32,
            lower_bound: self.buf(8),
            entries: self.next_u64(),
            midpoint: self.opt_buf(8),
            ops_per_sec: self.next_u64(),
            shard_index: self.next_u64() as u32,
            shard_count: self.next_u64() as u32,
            rehome_mirror_epoch: self.next_u64(),
            rehome_mirror_ok: self.next_u64(),
            rehome_mirror_failures: self.next_u64(),
            rehome_copy_complete_epoch: self.next_u64(),
            rehome_copy_progress: self.next_u64(),
        }
    }

    fn applied(&mut self) -> Applied {
        match self.below(3) {
            0 => Applied::Won,
            1 => Applied::Lost(self.version()),
            _ => Applied::Rejected,
        }
    }

    fn stored(&mut self) -> Stored {
        Stored {
            version: self.version(),
            value: self.opt_buf(20),
        }
    }

    fn entries(&mut self) -> Vec<(Vec<u8>, Stored)> {
        let n = self.below(6) as usize;
        (0..n).map(|_| (self.buf(8), self.stored())).collect()
    }
}

fn gen_owned_request(r: &mut Rng) -> OwnedRequest {
    match r.below(24) {
        23 => OwnedRequest::CleanupStatus,
        22 => OwnedRequest::GcFloor { zone_id: r.next_u64() as u32 },
        21 => {
            let n = r.below(8);
            OwnedRequest::MultiScan {
                epoch: r.next_u64(), zone_id: r.next_u64() as u32, keys_only: r.bool(), reverse: r.bool(),
                start: r.below(n + 1) as u32,
                scans: (0..n).map(|_| (r.opt_buf(16), r.opt_buf(16), r.below(100) as u32)).collect(),
            }
        }
        20 => OwnedRequest::SyncRangeTombstones {
            zone_id: r.next_u64() as u32,
            requester: r.bool().then(|| r.next_u64() as u32),
            digest: r.next_u64(),
            after: r.opt_buf(8),
        },
        19 => OwnedRequest::RangeDelete {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            sync: r.bool(),
            version: r.version(),
            slices: {
                let n = r.below(6) as usize;
                (0..n).map(|_| (r.opt_buf(8), r.opt_buf(8))).collect()
            },
        },
        0 => OwnedRequest::Apply {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            key: r.buf(12),
            value: r.opt_buf(20),
            version: r.version(),
        },
        1 => OwnedRequest::Read {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            key: r.buf(12),
        },
        2 => OwnedRequest::Digest {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            key: r.buf(12),
        },
        3 => OwnedRequest::Scan {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            lo: r.opt_buf(12),
            hi: r.opt_buf(12),
            max: r.next_u64() as u32,
        },
        4 => OwnedRequest::SyncTree {
            zone_id: r.next_u64() as u32,
            level: r.next_u64() as u32,
            scope_g: r.next_u64() as u32,
            scope: {
                let n = r.below(6) as usize;
                (0..n).map(|_| r.next_u64() as u32).collect()
            },
            requester: r.bool().then(|| r.next_u64() as u32),
        },
        5 => OwnedRequest::SyncBucketDigests {
            zone_id: r.next_u64() as u32,
            num_buckets: r.next_u64() as u32,
            ids: {
                let n = r.below(6) as usize;
                (0..n).map(|_| r.next_u64() as u32).collect()
            },
            after: r.opt_buf(12),
        },
        13 => OwnedRequest::SyncValues {
            zone_id: r.next_u64() as u32,
            keys: {
                let n = r.below(6) as usize;
                (0..n).map(|_| r.buf(12)).collect()
            },
        },
        6 => OwnedRequest::SetMembers {
            auth: r.buf(8),
            epoch: r.next_u64(),
            vnodes: r.next_u64() as u32,
            rf: r.below(8) as u32,
            members: r.members(),
            addrs: r.node_addrs(),
            incarnation: r.incarnation(),
        },
        7 => OwnedRequest::Members,
        8 => OwnedRequest::SetZones {
            auth: r.buf(8),
            epoch: r.next_u64(),
            zones: {
                let n = r.below(4) as usize;
                (0..n).map(|_| r.zone()).collect()
            },
            incarnation: r.incarnation(),
        },
        9 => OwnedRequest::Zones,
        10 => OwnedRequest::SetRangeMaps {
            auth: r.buf(8),
            epoch: r.next_u64(),
            maps: {
                let n = r.below(4) as usize;
                (0..n).map(|_| r.range_map()).collect()
            },
            incarnation: r.incarnation(),
        },
        11 => OwnedRequest::RangeMaps,
        12 => OwnedRequest::GetWatermark,
        14 => OwnedRequest::RangeStats,
        17 => OwnedRequest::RangeStatsV2,
        18 => OwnedRequest::RangeStatsV3,
        16 => OwnedRequest::RangeWriteMark {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            lower: r.buf(8),
        },
        _ => OwnedRequest::BatchApply {
            epoch: r.next_u64(),
            zone_id: r.next_u64() as u32,
            sync: r.bool(),
            fill: r.bool(),
            writes: {
                let n = r.below(6) as usize;
                (0..n)
                    .map(|_| -> OwnedWrite { (r.buf(8), r.opt_buf(12), r.version()) })
                    .collect()
            },
        },
    }
}

fn gen_response(r: &mut Rng) -> Response {
    match r.below(23) {
        22 => Response::CleanupStatus {
            ring_epoch: r.next_u64(),
            gc_grace_ms: r.next_u64(),
            records: r.next_u64(),
            unsettled: (0..r.below(4))
                .map(|_| UnsettledZone {
                    zone_id: r.next_u64() as u32,
                    lapped: r.bool(),
                    pending_rehome: r.bool(),
                    kept: r.next_u64(),
                    samples: (0..r.below(4)).map(|_| r.buf(8)).collect(),
                })
                .collect(),
        },
        21 => Response::GcFloor(r.next_u64()),
        20 => {
            let keys_only = r.bool();
            let n = r.below(8);
            Response::MultiScan {
                start: r.below(8) as u32, more: r.bool(), keys_only,
                pages: (0..n).map(|_| {
                    let count = r.below(8);
                    vals_cluster::coord::ScanPage {
                        entries: (0..count).map(|_| {
                            let key = r.buf(16);
                            let version = r.version();
                            let value = if keys_only { r.bool().then(Vec::new) } else { r.opt_buf(32) };
                            (key, Stored { version, value })
                        }).collect(),
                        more: r.bool(), fragments: vec![], resume: r.opt_buf(16),
                    }
                }).collect(),
            }
        }
        19 => Response::RangeTombstones {
            same: r.bool(),
            more: r.bool(),
            fragments: {
                let n = r.below(4) as usize;
                (0..n)
                    .map(|_| RtFragment { lo: r.opt_buf(8), hi: r.opt_buf(8), version: r.version() })
                    .collect()
            },
        },
        0 => Response::Apply(r.applied()),
        1 => Response::Read(if r.bool() { None } else { Some(r.stored()) }),
        2 => Response::Digest(if r.bool() { None } else { Some(r.version()) }),
        3 => Response::Scan {
            entries: r.entries(),
            more: r.bool(),
            fragments: {
                let n = if r.bool() { 0 } else { r.below(4) as usize };
                (0..n)
                    .map(|_| RtFragment { lo: r.opt_buf(8), hi: r.opt_buf(8), version: r.version() })
                    .collect()
            },
            resume: if r.bool() { None } else { r.opt_buf(8) },
        },
        4 => Response::Tree({
            let n = r.below(8) as usize;
            (0..n)
                .map(|_| (r.next_u64() as u32, r.next_u64()))
                .collect()
        }),
        5 => Response::BucketDigests {
            digests: {
                let n = r.below(6) as usize;
                (0..n).map(|_| (r.buf(12), r.version())).collect()
            },
            more: r.bool(),
            resume: r.bool().then(|| r.buf(12)),
        },
        13 => Response::Values {
            entries: r.entries(),
            more: r.bool(),
        },
        6 => Response::Members {
            epoch: r.next_u64(),
            vnodes: r.next_u64() as u32,
            rf: r.below(8) as u32,
            members: r.members(),
            addrs: r.node_addrs(),
        },
        7 => Response::Zones {
            epoch: r.next_u64(),
            zones: {
                let n = r.below(4) as usize;
                (0..n).map(|_| r.zone()).collect()
            },
        },
        8 => Response::RangeMaps {
            epoch: r.next_u64(),
            maps: {
                let n = r.below(4) as usize;
                (0..n).map(|_| r.range_map()).collect()
            },
        },
        9 => Response::Batch({
            let n = r.below(6) as usize;
            (0..n).map(|_| r.applied()).collect()
        }),
        10 => Response::StaleEpoch(r.next_u64()),
        11 => Response::Watermark(r.next_u64()),
        12 => Response::Denied,
        15 => Response::Busy,
        14 => Response::RangeStats {
            epoch: r.next_u64(),
            ranges: {
                let n = r.below(4) as usize;
                (0..n).map(|_| {
                    let mut stat = r.range_stat();
                    stat.ops_per_sec = 0;
                    stat
                }).collect()
            },
        },
        17 => Response::RangeStatsV2 {
            epoch: r.next_u64(),
            ranges: {
                let n = r.below(4) as usize;
                (0..n).map(|_| r.range_stat()).collect()
            },
        },
        18 => {
            let epoch = r.next_u64();
            let accounting_epoch = r.next_u64();
            let accounting_ready = r.bool();
            let n = r.below(4) as usize;
            let ranges: Vec<_> = (0..n).map(|_| r.range_stat()).collect();
            let recovery_seeding = match r.below(3) {
                0 => None,
                n => Some(n == 2),
            };
            let covered_empty = if recovery_seeding.is_some() {
                (0..n as u32).filter(|_| r.bool()).collect()
            } else {
                Vec::new()
            };
            Response::RangeStatsV3 {
                epoch,
                accounting_epoch,
                accounting_ready,
                ranges,
                recovery_seeding,
                covered_empty,
            }
        }
        _ => Response::Err(r.name()),
    }
}

fn encode_owned_req(o: &OwnedRequest) -> Vec<u8> {
    use OwnedRequest as O;
    match o {
        O::Apply {
            epoch,
            zone_id,
            key,
            value,
            version,
        } => encode_request_payload(&Request::Apply {
            epoch: *epoch,
            zone_id: *zone_id,
            key: key.as_slice(),
            value: value.as_deref(),
            version: *version,
        }),
        O::Read {
            epoch,
            zone_id,
            key,
        } => encode_request_payload(&Request::Read {
            epoch: *epoch,
            zone_id: *zone_id,
            key: key.as_slice(),
        }),
        O::Digest {
            epoch,
            zone_id,
            key,
        } => encode_request_payload(&Request::Digest {
            epoch: *epoch,
            zone_id: *zone_id,
            key: key.as_slice(),
        }),
        O::Scan {
            epoch,
            zone_id,
            lo,
            hi,
            max,
        } => encode_request_payload(&Request::Scan {
            epoch: *epoch,
            zone_id: *zone_id,
            lo: lo.as_deref(),
            hi: hi.as_deref(),
            max: *max,
        }),
        O::SyncTree {
            zone_id,
            level,
            scope_g,
            scope,
            requester,
        } => encode_request_payload(&Request::SyncTree {
            zone_id: *zone_id,
            level: *level,
            scope_g: *scope_g,
            scope: scope.as_slice(),
            requester: *requester,
        }),
        O::SyncBucketDigests {
            zone_id,
            num_buckets,
            ids,
            after,
        } => encode_request_payload(&Request::SyncBucketDigests {
            zone_id: *zone_id,
            num_buckets: *num_buckets,
            ids: ids.as_slice(),
            after: after.as_deref(),
        }),
        O::SyncValues { zone_id, keys } => {
            let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
            encode_request_payload(&Request::SyncValues {
                zone_id: *zone_id,
                keys: &key_refs,
            })
        }
        O::SetMembers {
            auth,
            epoch,
            vnodes,
            rf,
            members,
            addrs,
            incarnation,
        } => encode_request_payload(&Request::SetMembers {
            auth: auth.as_slice(),
            epoch: *epoch,
            vnodes: *vnodes,
            rf: *rf,
            members: members.as_slice(),
            addrs: addrs.as_slice(),
            incarnation: *incarnation,
        }),
        O::Members => encode_request_payload(&Request::Members),
        O::SetZones {
            auth,
            epoch,
            zones,
            incarnation,
        } => {
            let wz: Vec<WireZone> = zones
                .iter()
                .map(|z| WireZone {
                    id: z.id,
                    rf: z.rf,
                    ordering: z.ordering,
                    scope: z.scope.as_deref(),
                    name: z.name.as_str(),
                })
                .collect();
            encode_request_payload(&Request::SetZones {
                auth: auth.as_slice(),
                epoch: *epoch,
                zones: &wz,
                incarnation: *incarnation,
            })
        }
        O::Zones => encode_request_payload(&Request::Zones),
        O::SetRangeMaps {
            auth,
            epoch,
            maps,
            incarnation,
        } => {
            let wm: Vec<WireRangeMap> = maps
                .iter()
                .map(|m| WireRangeMap {
                    zone_id: m.zone_id,
                    splits: m.splits.as_slice(),
                    tokens: m.tokens.as_slice(),
                    pending: m.pending.as_slice(),
                    placement_debt: m.placement_debt.as_slice(),
                    opaque_tokens: m.opaque_tokens,
                })
                .collect();
            encode_request_payload(&Request::SetRangeMaps {
                auth: auth.as_slice(),
                epoch: *epoch,
                maps: &wm,
                incarnation: *incarnation,
            })
        }
        O::RangeMaps => encode_request_payload(&Request::RangeMaps),
        O::RangeStats => encode_request_payload(&Request::RangeStats),
        O::RangeStatsV2 => encode_request_payload(&Request::RangeStatsV2),
        O::RangeStatsV3 => encode_request_payload(&Request::RangeStatsV3),
        O::RangeWriteMark {
            epoch,
            zone_id,
            lower,
        } => encode_request_payload(&Request::RangeWriteMark {
            epoch: *epoch,
            zone_id: *zone_id,
            lower: lower.as_slice(),
        }),
        O::GetWatermark => encode_request_payload(&Request::GetWatermark),
        O::BatchApply {
            epoch,
            zone_id,
            sync,
            fill,
            writes,
        } => {
            let ww: Vec<WireWrite> = writes
                .iter()
                .map(|(k, v, ver)| (k.as_slice(), v.as_deref(), *ver))
                .collect();
            encode_request_payload(&Request::BatchApply {
                epoch: *epoch,
                zone_id: *zone_id,
                sync: *sync,
                fill: *fill,
                writes: &ww,
            })
        }
        O::MultiScan { epoch, zone_id, keys_only, start, scans, reverse } => {
            let scans: Vec<_> = scans.iter().map(|(lo, hi, max)| (lo.as_deref(), hi.as_deref(), *max)).collect();
            encode_request_payload(&Request::MultiScan {
                epoch: *epoch, zone_id: *zone_id, keys_only: *keys_only, start: *start, scans: &scans, reverse: *reverse,
            })
        }
        O::MixedBatchRead {
            epoch,
            zone_id,
            start,
            reads,
        } => {
            let wire_reads: Vec<(&[u8], vals_client::types::MixedReadRole)> = reads
                .iter()
                .map(|(key, role)| (key.as_slice(), *role))
                .collect();
            encode_request_payload(&Request::MixedBatchRead {
                epoch: *epoch,
                zone_id: *zone_id,
                start: *start,
                reads: &wire_reads,
            })
        }
        O::RangeDelete {
            epoch,
            zone_id,
            sync,
            version,
            slices,
        } => {
            let wire_slices: Vec<WireSlice> = slices
                .iter()
                .map(|(lo, hi)| (lo.as_deref(), hi.as_deref()))
                .collect();
            encode_request_payload(&Request::RangeDelete {
                epoch: *epoch,
                zone_id: *zone_id,
                sync: *sync,
                version: *version,
                slices: &wire_slices,
            })
        }
        O::SyncRangeTombstones {
            zone_id,
            requester,
            digest,
            after,
        } => encode_request_payload(&Request::SyncRangeTombstones {
            zone_id: *zone_id,
            requester: *requester,
            digest: *digest,
            after: after.as_deref(),
        }),
        O::GcFloor { zone_id } => encode_request_payload(&Request::GcFloor { zone_id: *zone_id }),
        O::CleanupStatus => encode_request_payload(&Request::CleanupStatus),
    }
}

fn mutate(r: &mut Rng, base: &[u8]) -> Vec<u8> {
    let mut b = base.to_vec();
    match r.below(5) {
        0 => {
            if !b.is_empty() {
                for _ in 0..1 + r.below(4) {
                    let i = r.below(b.len() as u64) as usize;
                    b[i] ^= r.byte();
                }
            }
        }
        1 => {
            if !b.is_empty() {
                let n = r.below(b.len() as u64) as usize;
                b.truncate(n);
            }
        }
        2 => {
            for _ in 0..r.below(8) {
                b.push(r.byte());
            }
        }
        3 => {
            if b.len() >= 4 {
                let i = r.below((b.len() - 3) as u64) as usize;
                b[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            }
        }
        _ => {
            for byte in b.iter_mut().skip(1) {
                *byte = r.byte();
            }
        }
    }
    b
}

fn assert_no_panic(label: &str, bytes: &[u8]) {
    let req = catch_unwind(AssertUnwindSafe(|| {
        let _ = decode_request(bytes);
    }));
    let resp = catch_unwind(AssertUnwindSafe(|| {
        let _ = decode_response(bytes);
    }));
    if req.is_err() || resp.is_err() {
        panic!(
            "decoder panicked ({label}) on {} bytes: {:02x?}",
            bytes.len(),
            bytes
        );
    }
}

#[test]
fn request_roundtrips_over_random_values() {
    for seed in 0..4000u64 {
        let mut r = Rng::new(seed.wrapping_mul(0xD1B5_4A32_D192_ED03).wrapping_add(1));
        let want = gen_owned_request(&mut r);
        let bytes = encode_owned_req(&want);
        match decode_request(&bytes) {
            Ok(got) => assert_eq!(got, want, "roundtrip mismatch at seed {seed}"),
            Err(e) => panic!("valid request failed to decode at seed {seed}: {e:?}"),
        }
    }
}

#[test]
fn response_roundtrips_over_random_values() {
    for seed in 0..4000u64 {
        let mut r = Rng::new(seed.wrapping_mul(0x2545_F491_4F6C_DD1D).wrapping_add(7));
        let want = gen_response(&mut r);
        let bytes = encode_response_payload(&want);
        match decode_response(&bytes) {
            Ok(got) => assert_eq!(got, want, "roundtrip mismatch at seed {seed}"),
            Err(e) => panic!("valid response failed to decode at seed {seed}: {e:?}"),
        }
    }
}

#[test]
fn decoders_never_panic_on_arbitrary_bytes() {
    for op in 0u16..=255 {
        assert_no_panic("single-byte", &[op as u8]);
        assert_no_panic("two-byte", &[op as u8, 0xff]);
    }
    assert_no_panic("empty", &[]);

    let mut r = Rng::new(0xFACE_FEED_1234_5678);
    for _ in 0..6000 {
        let bytes = r.buf(260);
        assert_no_panic("random", &bytes);
    }
}

#[test]
fn decoders_never_panic_on_mutated_valid_frames() {
    let mut r = Rng::new(0x0BAD_C0DE_DEAD_BEEF);
    for _ in 0..6000 {
        let req = gen_owned_request(&mut r);
        let base = encode_owned_req(&req);
        let m = mutate(&mut r, &base);
        assert_no_panic("mutated-request", &m);

        let resp = gen_response(&mut r);
        let base = encode_response_payload(&resp);
        let m = mutate(&mut r, &base);
        assert_no_panic("mutated-response", &m);
    }
}
