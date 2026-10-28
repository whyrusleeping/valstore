    use super::*;
    use crate::zone::DEFAULT_ZONE;

    fn v(hlc: u64, node: u64) -> Version {
        Version { hlc, node }
    }

    fn payload(frame: &[u8]) -> &[u8] {
        let len = u32::from_le_bytes(frame[0..4].try_into().unwrap()) as usize;
        assert_eq!(len, frame.len() - 4, "length prefix matches payload");
        &frame[4..]
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn apply_request_roundtrips_with_value() {
        let req = Request::Apply {
            epoch: 11,
            zone_id: DEFAULT_ZONE,
            key: b"key",
            value: Some(b"val"),
            version: v(42, 0xfedc_ba98_7654_3210),
        };
        let frame = encode_request(&req);
        assert_eq!(
            hex(payload(&frame)),
            "010b00000000000000000000002a000000000000001032547698badcfe030000006b6579010300000076616c"
        );
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::Apply {
                epoch: 11,
                zone_id: DEFAULT_ZONE,
                key: b"key".to_vec(),
                value: Some(b"val".to_vec()),
                version: v(42, 0xfedc_ba98_7654_3210),
            }
        );
    }

    #[test]
    fn protocol_v2_read_tombstone_and_batch_fixtures() {
        let read = encode_response(&Response::Read(Some(Stored {
            version: v(43, 0xfedc_ba98_7654_3210),
            value: Some(b"val".to_vec()),
        })));
        assert_eq!(
            hex(payload(&read)),
            "01012b000000000000001032547698badcfe010300000076616c"
        );
        let tombstone = encode_response(&Response::Read(Some(Stored {
            version: v(44, 0xabcd_ef01_2345_6789),
            value: None,
        })));
        assert_eq!(
            hex(payload(&tombstone)),
            "01012c000000000000008967452301efcdab00"
        );
        let writes: [WireWrite<'_>; 2] = [
            (b"a", Some(&b"x"[..]), v(42, 0xfedc_ba98_7654_3210)),
            (b"b", None, v(43, 0xabcd_ef01_2345_6789)),
        ];
        let batch = encode_request(&Request::BatchApply {
            epoch: 11,
            zone_id: DEFAULT_ZONE,
            sync: true,
            writes: &writes,
        });
        assert_eq!(
            hex(payload(&batch)),
            "070b000000000000000000000001020000002a000000000000001032547698badcfe01000000610101000000782b000000000000008967452301efcdab010000006200"
        );
    }

    #[test]
    fn get_watermark_request_and_reply_roundtrip() {
        let frame = encode_request(&Request::GetWatermark);
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::GetWatermark
        );
        let reply = encode_response(&Response::Watermark(1_700_000_000_123));
        assert_eq!(
            decode_response(payload(&reply)).unwrap(),
            Response::Watermark(1_700_000_000_123)
        );
    }

    #[test]
    fn gc_floor_request_and_reply_roundtrip() {
        let frame = encode_request(&Request::GcFloor { zone_id: 7 });
        assert_eq!(hex(payload(&frame)), "1807000000");
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::GcFloor { zone_id: 7 }
        );
        let mut trailing = payload(&frame).to_vec();
        trailing.push(0);
        assert_eq!(decode_request(&trailing), Err(WireError::TrailingBytes));
        let reply = encode_response(&Response::GcFloor(1_700_000_000_123));
        assert_eq!(payload(&reply)[0], RESP_GC_FLOOR);
        assert_eq!(
            decode_response(payload(&reply)).unwrap(),
            Response::GcFloor(1_700_000_000_123)
        );
    }

    #[test]
    fn cleanup_status_request_and_reply_roundtrip() {
        let frame = encode_request(&Request::CleanupStatus);
        assert_eq!(hex(payload(&frame)), "19");
        assert_eq!(decode_request(payload(&frame)).unwrap(), OwnedRequest::CleanupStatus);
        let mut trailing = payload(&frame).to_vec();
        trailing.push(0);
        assert_eq!(decode_request(&trailing), Err(WireError::TrailingBytes));
        let status = Response::CleanupStatus {
            ring_epoch: 9,
            gc_grace_ms: 60_000,
            records: 12,
            unsettled: vec![
                UnsettledZone {
                    zone_id: 3,
                    lapped: true,
                    pending_rehome: false,
                    kept: 2,
                    samples: vec![b"a".to_vec(), Vec::new()],
                },
                UnsettledZone { zone_id: 0, lapped: false, pending_rehome: true, kept: 0, samples: Vec::new() },
            ],
        };
        let reply = encode_response(&status);
        assert_eq!(payload(&reply)[0], RESP_CLEANUP_STATUS);
        assert_eq!(decode_response(payload(&reply)).unwrap(), status);
    }

    #[test]
    fn busy_reply_roundtrips() {
        let reply = encode_response(&Response::Busy);
        assert_eq!(payload(&reply)[0], RESP_BUSY);
        assert_eq!(decode_response(payload(&reply)).unwrap(), Response::Busy);
    }

    #[test]
    fn apply_carries_a_non_default_zone() {
        let frame = encode_request(&Request::Apply {
            epoch: 0,
            zone_id: 9,
            key: b"k",
            value: Some(b"v"),
            version: v(1, 1),
        });
        match decode_request(payload(&frame)).unwrap() {
            OwnedRequest::Apply { zone_id, .. } => assert_eq!(zone_id, 9),
            _ => panic!("expected apply"),
        }
    }

    #[test]
    fn apply_request_distinguishes_tombstone_from_empty_value() {
        let tomb = encode_request(&Request::Apply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"k",
            value: None,
            version: v(1, 1),
        });
        let empty = encode_request(&Request::Apply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"k",
            value: Some(b""),
            version: v(1, 1),
        });
        assert_ne!(tomb, empty);
        match decode_request(payload(&tomb)).unwrap() {
            OwnedRequest::Apply { value, .. } => assert_eq!(value, None),
            _ => panic!("expected apply"),
        }
        match decode_request(payload(&empty)).unwrap() {
            OwnedRequest::Apply { value, .. } => assert_eq!(value, Some(Vec::new())),
            _ => panic!("expected apply"),
        }
    }

    #[test]
    fn read_request_roundtrips() {
        let frame = encode_request(&Request::Read {
            epoch: 3,
            zone_id: DEFAULT_ZONE,
            key: b"abc",
        });
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::Read {
                epoch: 3,
                zone_id: DEFAULT_ZONE,
                key: b"abc".to_vec()
            }
        );
    }

    #[test]
    fn digest_request_roundtrips() {
        let frame = encode_request(&Request::Digest {
            epoch: 7,
            zone_id: DEFAULT_ZONE,
            key: b"abc",
        });
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::Digest {
                epoch: 7,
                zone_id: DEFAULT_ZONE,
                key: b"abc".to_vec()
            }
        );
    }

    #[test]
    fn responses_roundtrip() {
        let cases = [
            Response::Apply(Applied::Won),
            Response::Apply(Applied::Lost(v(99, 3))),
            Response::Read(None),
            Response::Digest(None),
            Response::Digest(Some(v(8, 1))),
            Response::Read(Some(Stored {
                version: v(5, 2),
                value: Some(b"hi".to_vec()),
            })),
            Response::Read(Some(Stored {
                version: v(6, 2),
                value: None,
            })),
            Response::Err("boom".to_owned()),
        ];
        for resp in cases {
            let frame = encode_response(&resp);
            assert_eq!(decode_response(payload(&frame)).unwrap(), resp);
        }
    }

    #[test]
    fn scan_request_and_reply_roundtrip() {
        let frame = encode_request(&Request::Scan {
            epoch: 4,
            zone_id: 9,
            lo: Some(b"banana"),
            hi: Some(b"date"),
            max: 100,
        });
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::Scan {
                epoch: 4,
                zone_id: 9,
                lo: Some(b"banana".to_vec()),
                hi: Some(b"date".to_vec()),
                max: 100,
            }
        );
        let unbounded = encode_request(&Request::Scan {
            epoch: 0,
            zone_id: 0,
            lo: None,
            hi: None,
            max: 0,
        });
        assert_eq!(
            decode_request(payload(&unbounded)).unwrap(),
            OwnedRequest::Scan {
                epoch: 0,
                zone_id: 0,
                lo: None,
                hi: None,
                max: 0,
            }
        );
        for more in [false, true] {
            let resp = Response::Scan {
                entries: vec![
                    (
                        b"apple".to_vec(),
                        Stored {
                            version: v(10, 1),
                            value: Some(b"red".to_vec()),
                        },
                    ),
                    (
                        b"cherry".to_vec(),
                        Stored {
                            version: v(12, 2),
                            value: None,
                        },
                    ),
                ],
                more,
                fragments: Vec::new(),
                resume: None,
            };
            assert_eq!(
                decode_response(payload(&encode_response(&resp))).unwrap(),
                resp
            );
        }
    }

    #[test]
    fn scan_reply_trailer_roundtrips_and_is_absent_without_fragments_or_resume() {
        let entries = vec![(
            b"apple".to_vec(),
            Stored {
                version: v(10, 1),
                value: Some(b"red".to_vec()),
            },
        )];
        let plain = Response::Scan {
            entries: entries.clone(),
            more: true,
            fragments: Vec::new(),
            resume: None,
        };
        let plain_frame = encode_response(&plain);
        let mut legacy = vec![RESP_SCAN, 1];
        legacy.extend_from_slice(&1u32.to_le_bytes());
        legacy.extend_from_slice(&5u32.to_le_bytes());
        legacy.extend_from_slice(b"apple");
        legacy.extend_from_slice(&10u64.to_le_bytes());
        legacy.extend_from_slice(&1u64.to_le_bytes());
        legacy.push(TAG_SOME);
        legacy.extend_from_slice(&3u32.to_le_bytes());
        legacy.extend_from_slice(b"red");
        assert_eq!(payload(&plain_frame), legacy.as_slice());
        assert_eq!(decode_response(&legacy).unwrap(), plain);
        let fragments = vec![
            RtFragment { lo: None, hi: Some(b"b".to_vec()), version: v(20, 2) },
            RtFragment { lo: Some(b"b".to_vec()), hi: Some(b"d".to_vec()), version: v(30, 3) },
            RtFragment { lo: Some(b"x".to_vec()), hi: None, version: v(40, 4) },
        ];
        let cases = [
            Response::Scan {
                entries: entries.clone(),
                more: true,
                fragments: fragments.clone(),
                resume: None,
            },
            Response::Scan {
                entries: Vec::new(),
                more: true,
                fragments: fragments.clone(),
                resume: Some(b"c".to_vec()),
            },
            Response::Scan {
                entries: Vec::new(),
                more: true,
                fragments: Vec::new(),
                resume: Some(Vec::new()),
            },
        ];
        for resp in cases {
            let frame = encode_response(&resp);
            assert_eq!(decode_response(payload(&frame)).unwrap(), resp);
            let mut trailing = payload(&frame).to_vec();
            trailing.push(0);
            assert_eq!(decode_response(&trailing), Err(WireError::TrailingBytes));
        }
    }

    #[test]
    fn range_delete_request_roundtrips_and_bounds_its_slices() {
        let slices: &[WireSlice] = &[
            (None, Some(&b"b"[..])),
            (Some(&b"b"[..]), Some(&b"m"[..])),
            (Some(&b"m"[..]), None),
            (Some(&b""[..]), None),
        ];
        for sync in [false, true] {
            let frame = encode_request(&Request::RangeDelete {
                epoch: 9,
                zone_id: 4,
                sync,
                version: v(77, 3),
                slices,
            });
            let decoded = decode_request(payload(&frame)).unwrap();
            assert_eq!(decoded.route_epoch(), Some(9));
            assert_eq!(
                decoded,
                OwnedRequest::RangeDelete {
                    epoch: 9,
                    zone_id: 4,
                    sync,
                    version: v(77, 3),
                    slices: vec![
                        (None, Some(b"b".to_vec())),
                        (Some(b"b".to_vec()), Some(b"m".to_vec())),
                        (Some(b"m".to_vec()), None),
                        (Some(Vec::new()), None),
                    ],
                }
            );
            let mut trailing = payload(&frame).to_vec();
            trailing.push(0);
            assert_eq!(decode_request(&trailing), Err(WireError::TrailingBytes));
        }
        let frame = encode_request(&Request::RangeDelete {
            epoch: 0,
            zone_id: 1,
            sync: true,
            version: v(1, 1),
            slices: &[(None, None)],
        });
        let mut bad_sync = payload(&frame).to_vec();
        bad_sync[13] = 2;
        assert_eq!(decode_request(&bad_sync), Err(WireError::BadTag(2)));
        let at_cap = vec![(None, None); MAX_RANGE_DELETE_SLICES];
        let over_cap = vec![(None, None); MAX_RANGE_DELETE_SLICES + 1];
        let request = |slices: &[WireSlice]| {
            encode_request(&Request::RangeDelete {
                epoch: 0,
                zone_id: 1,
                sync: false,
                version: v(1, 1),
                slices,
            })
        };
        assert!(decode_request(payload(&request(&at_cap))).is_ok());
        assert_eq!(
            decode_request(payload(&request(&over_cap))),
            Err(WireError::TooManyEntries)
        );
    }

    #[test]
    fn sync_range_tombstones_request_and_reply_roundtrip() {
        for (requester, after) in [(None, None), (Some(3), Some(&b""[..])), (Some(7), Some(&b"k/9"[..]))] {
            let frame = encode_request(&Request::SyncRangeTombstones {
                zone_id: 4,
                requester,
                digest: 0xdead_beef_0102,
                after,
            });
            let decoded = decode_request(payload(&frame)).unwrap();
            assert_eq!(decoded.route_epoch(), None);
            assert_eq!(
                decoded,
                OwnedRequest::SyncRangeTombstones {
                    zone_id: 4,
                    requester,
                    digest: 0xdead_beef_0102,
                    after: after.map(<[u8]>::to_vec),
                }
            );
            let mut trailing = payload(&frame).to_vec();
            trailing.push(0);
            assert_eq!(decode_request(&trailing), Err(WireError::TrailingBytes));
        }
        let fragments = vec![
            RtFragment { lo: None, hi: Some(b"b".to_vec()), version: v(5, 1) },
            RtFragment { lo: Some(b"c".to_vec()), hi: None, version: v(9, 2) },
        ];
        for (same, more, fragments) in [(true, false, Vec::new()), (false, true, fragments)] {
            let resp = Response::RangeTombstones { same, more, fragments };
            let frame = encode_response(&resp);
            assert_eq!(decode_response(payload(&frame)).unwrap(), resp);
            let mut trailing = payload(&frame).to_vec();
            trailing.push(0);
            assert_eq!(decode_response(&trailing), Err(WireError::TrailingBytes));
            let mut bad_same = payload(&frame).to_vec();
            bad_same[1] = 2;
            assert_eq!(decode_response(&bad_same), Err(WireError::BadTag(2)));
        }
    }

    #[test]
    fn range_map_ops_roundtrip() {
        let maps = [
            WireRangeMap {
                zone_id: 2,
                splits: &[b"m".to_vec(), b"t".to_vec()],
                tokens: &[Vec::new(), b"m".to_vec(), b"m".to_vec()],
                pending: &[None, None, Some(b"t".to_vec())],
                placement_debt: &[false, false, true],
                opaque_tokens: false,
            },
            WireRangeMap {
                zone_id: 5,
                splits: &[],
                tokens: &[Vec::new()],
                pending: &[None],
                placement_debt: &[false],
                opaque_tokens: false,
            },
        ];
        let set = encode_request(&Request::SetRangeMaps {
            auth: b"s3cret",
            epoch: 9,
            maps: &maps,
            incarnation: None,
        });
        let expected = vec![
            OwnedRangeMap {
                zone_id: 2,
                splits: vec![b"m".to_vec(), b"t".to_vec()],
                tokens: vec![Vec::new(), b"m".to_vec(), b"m".to_vec()],
                pending: vec![None, None, Some(b"t".to_vec())],
                placement_debt: vec![false, false, true],
                opaque_tokens: false,
            },
            OwnedRangeMap {
                zone_id: 5,
                splits: vec![],
                tokens: vec![Vec::new()],
                pending: vec![None],
                placement_debt: vec![false],
                opaque_tokens: false,
            },
        ];
        assert_eq!(
            decode_request(payload(&set)).unwrap(),
            OwnedRequest::SetRangeMaps {
                auth: b"s3cret".to_vec(),
                epoch: 9,
                maps: expected.clone(),
                incarnation: None,
            }
        );
        assert_eq!(
            decode_request(payload(&encode_request(&Request::RangeMaps))).unwrap(),
            OwnedRequest::RangeMaps
        );
        let resp = Response::RangeMaps {
            epoch: 9,
            maps: expected,
        };
        assert_eq!(
            decode_response(payload(&encode_response(&resp))).unwrap(),
            resp
        );
    }

    #[test]
    fn nonmonotonic_committed_tokens_roundtrip_over_v2_wire() {
        let splits = [b"m".to_vec(), b"t".to_vec()];
        let tokens = [Vec::new(), b"m".to_vec(), Vec::new()];
        let pending = [None, None, None];
        let maps = [WireRangeMap {
            zone_id: 2,
            splits: &splits,
            tokens: &tokens,
            pending: &pending,
            placement_debt: &[false, false, true],
            opaque_tokens: false,
        }];
        let encoded = encode_request(&Request::SetRangeMaps {
            auth: b"",
            epoch: 4,
            maps: &maps,
            incarnation: None,
        });
        let decoded = decode_request(payload(&encoded)).unwrap();
        assert_eq!(decoded, OwnedRequest::SetRangeMaps {
            auth: Vec::new(),
            epoch: 4,
            maps: vec![OwnedRangeMap {
                zone_id: 2,
                splits: splits.to_vec(),
                tokens: tokens.to_vec(),
                pending: pending.to_vec(),
                placement_debt: vec![false, false, true],
                opaque_tokens: false,
            }],
            incarnation: None,
        });
    }

    #[test]
    fn opaque_range_map_v4_wire_roundtrip() {
        let splits = [b"m".to_vec()];
        let opaque = crate::partition::opaque_placement_token(
            [5; crate::partition::OPAQUE_TOKEN_SEED_LEN]
        );
        let tokens = [Vec::new(), opaque.clone()];
        let pending = [None, Some(crate::partition::opaque_placement_token(
            [6; crate::partition::OPAQUE_TOKEN_SEED_LEN]
        ))];
        let debt = [false, true];
        let maps = [WireRangeMap {
            zone_id: 8,
            splits: &splits,
            tokens: &tokens,
            pending: &pending,
            placement_debt: &debt,
            opaque_tokens: true,
        }];
        let encoded = encode_request(&Request::SetRangeMaps {
            auth: b"",
            epoch: 12,
            maps: &maps,
            incarnation: None,
        });
        assert!(encoded.windows(4).any(|w| w == RANGE_MAP_WIRE_V4.to_le_bytes()));
        assert_eq!(decode_request(payload(&encoded)).unwrap(), OwnedRequest::SetRangeMaps {
            auth: Vec::new(),
            epoch: 12,
            maps: vec![OwnedRangeMap {
                zone_id: 8,
                splits: splits.to_vec(),
                tokens: tokens.to_vec(),
                pending: pending.to_vec(),
                placement_debt: debt.to_vec(),
                opaque_tokens: true,
            }],
            incarnation: None,
        });
    }

    #[test]
    fn range_map_wire_version_and_shape_mismatch_fail() {
        let maps = [WireRangeMap {
            zone_id: 2,
            splits: &[b"m".to_vec()],
            tokens: &[Vec::new(), b"x".to_vec()],
            pending: &[None, None],
            placement_debt: &[false, false],
            opaque_tokens: false,
        }];
        let invalid = encode_request(&Request::SetRangeMaps {
            auth: b"",
            epoch: 1,
            maps: &maps,
            incarnation: None,
        });
        assert_eq!(decode_request(payload(&invalid)), Err(WireError::BadRangeMap));

        let maps = [WireRangeMap {
            zone_id: 2,
            splits: &[b"m".to_vec()],
            tokens: &[Vec::new(), b"m".to_vec()],
            pending: &[None, None],
            placement_debt: &[false, false],
            opaque_tokens: false,
        }];
        let mut wrong_version = encode_request(&Request::SetRangeMaps {
            auth: b"",
            epoch: 1,
            maps: &maps,
            incarnation: None,
        });
        let magic = RANGE_MAP_WIRE_MAGIC.to_le_bytes();
        let pos = wrong_version
            .windows(magic.len())
            .position(|w| w == magic)
            .unwrap();
        wrong_version[pos + 4..pos + 8].copy_from_slice(&99u32.to_le_bytes());
        assert_eq!(
            decode_request(payload(&wrong_version)),
            Err(WireError::BadRangeMap)
        );
    }

    #[test]
    fn range_stats_ops_roundtrip() {
        assert_eq!(
            decode_request(payload(&encode_request(&Request::RangeStats))).unwrap(),
            OwnedRequest::RangeStats
        );
        let resp = Response::RangeStats {
            epoch: 7,
            ranges: vec![
                OwnedRangeStat {
                    zone_id: 2,
                    lower_bound: Vec::new(),
                    entries: 1234,
                    midpoint: Some(b"m".to_vec()),
                    ops_per_sec: 98,
                    shard_index: 1,
                    shard_count: 4,
                    rehome_mirror_epoch: 7,
                    rehome_mirror_ok: 12,
                    rehome_mirror_failures: 0,
                    rehome_copy_complete_epoch: 7,
                    rehome_copy_progress: 4096,
                },
                OwnedRangeStat {
                    zone_id: 2,
                    lower_bound: b"m".to_vec(),
                    entries: 5,
                    midpoint: None,
                    ops_per_sec: 3,
                    shard_index: 0,
                    shard_count: 1,
                    rehome_mirror_epoch: 0,
                    rehome_mirror_ok: 0,
                    rehome_mirror_failures: 0,
                    rehome_copy_complete_epoch: 0,
                    rehome_copy_progress: 0,
                },
            ],
        };
        let Response::RangeStats { ranges: legacy, .. } =
            decode_response(payload(&encode_response(&resp))).unwrap()
        else {
            panic!("legacy RangeStats response changed shape");
        };
        assert!(legacy.iter().all(|s| s.ops_per_sec == 0));
        let v2 = Response::RangeStatsV2 {
            epoch: 7,
            ranges: match resp {
                Response::RangeStats { ranges, .. } => ranges,
                _ => unreachable!(),
            },
        };
        assert_eq!(
            decode_response(payload(&encode_response(&v2))).unwrap(),
            v2
        );
        assert_eq!(
            decode_request(payload(&encode_request(&Request::RangeStatsV2))).unwrap(),
            OwnedRequest::RangeStatsV2
        );
        let v3 = Response::RangeStatsV3 {
            epoch: 7,
            accounting_epoch: 7,
            accounting_ready: true,
            ranges: match &v2 {
                Response::RangeStatsV2 { ranges, .. } => ranges.clone(),
                _ => unreachable!(),
            },
            recovery_seeding: None,
            covered_empty: Vec::new(),
        };
        assert_eq!(
            decode_response(payload(&encode_response(&v3))).unwrap(),
            v3
        );
        for recovery_seeding in [Some(false), Some(true)] {
            let Response::RangeStatsV3 { epoch, accounting_epoch, accounting_ready, ranges, .. } = &v3 else {
                unreachable!()
            };
            let flagged = Response::RangeStatsV3 {
                epoch: *epoch,
                accounting_epoch: *accounting_epoch,
                accounting_ready: *accounting_ready,
                ranges: ranges.clone(),
                recovery_seeding,
                covered_empty: Vec::new(),
            };
            let bytes = encode_response(&flagged);
            assert_eq!(decode_response(payload(&bytes)).unwrap(), flagged);
            assert_eq!(payload(&bytes).len(), payload(&encode_response(&v3)).len() + 1);
            let legacy = payload(&encode_response(&v3)).to_vec();
            assert!(payload(&bytes).starts_with(&legacy), "the seed flag is a trailing byte old decoders ignore");
            let covered = Response::RangeStatsV3 {
                epoch: *epoch,
                accounting_epoch: *accounting_epoch,
                accounting_ready: *accounting_ready,
                ranges: ranges.clone(),
                recovery_seeding,
                covered_empty: vec![0, 1],
            };
            let covered_bytes = encode_response(&covered);
            assert_eq!(decode_response(payload(&covered_bytes)).unwrap(), covered);
            assert!(
                payload(&covered_bytes).starts_with(payload(&bytes)),
                "the covered-empty list trails the seed flag, so older decoders ignore it"
            );
        }
        assert_eq!(
            decode_request(payload(&encode_request(&Request::RangeStatsV3))).unwrap(),
            OwnedRequest::RangeStatsV3
        );
        let mark = encode_request(&Request::RangeWriteMark {
            epoch: 12,
            zone_id: 2,
            lower: b"m",
        });
        assert_eq!(
            decode_request(payload(&mark)).unwrap(),
            OwnedRequest::RangeWriteMark {
                epoch: 12,
                zone_id: 2,
                lower: b"m".to_vec(),
            }
        );
    }

    #[test]
    fn sync_requests_roundtrip() {
        let tree = encode_request(&Request::SyncTree {
            zone_id: DEFAULT_ZONE,
            level: 256,
            scope_g: 16,
            scope: &[1, 7, 15],
            requester: Some(7),
        });
        assert_eq!(
            decode_request(payload(&tree)).unwrap(),
            OwnedRequest::SyncTree {
                zone_id: DEFAULT_ZONE,
                level: 256,
                scope_g: 16,
                scope: vec![1, 7, 15],
                requester: Some(7),
            }
        );
        let digests = encode_request(&Request::SyncBucketDigests {
            zone_id: 4,
            num_buckets: 256,
            ids: &[3, 17, 200],
            after: None,
        });
        assert_eq!(
            decode_request(payload(&digests)).unwrap(),
            OwnedRequest::SyncBucketDigests {
                zone_id: 4,
                num_buckets: 256,
                ids: vec![3, 17, 200],
                after: None,
            }
        );
        let resumed = encode_request(&Request::SyncBucketDigests {
            zone_id: 4,
            num_buckets: 256,
            ids: &[3],
            after: Some(b"kite"),
        });
        assert_eq!(
            decode_request(payload(&resumed)).unwrap(),
            OwnedRequest::SyncBucketDigests {
                zone_id: 4,
                num_buckets: 256,
                ids: vec![3],
                after: Some(b"kite".to_vec()),
            }
        );
        let values = encode_request(&Request::SyncValues {
            zone_id: 4,
            keys: &[b"alpha", b"kite"],
        });
        assert_eq!(
            decode_request(payload(&values)).unwrap(),
            OwnedRequest::SyncValues {
                zone_id: 4,
                keys: vec![b"alpha".to_vec(), b"kite".to_vec()],
            }
        );
    }

    #[test]
    fn membership_ops_roundtrip() {
        let addrs: Vec<(u32, Vec<SocketAddr>)> = vec![
            (
                1,
                vec![
                    "127.0.0.1:7001".parse().unwrap(),
                    "127.0.0.1:7002".parse().unwrap(),
                ],
            ),
            (5, vec!["[::1]:9000".parse().unwrap()]),
        ];
        let set = encode_request(&Request::SetMembers {
            auth: b"s3cret",
            epoch: 42,
            vnodes: 128,
            rf: 3,
            members: &[(1, 1), (5, 4), (9, 2)],
            addrs: &addrs,
            incarnation: None,
        });
        assert_eq!(
            decode_request(payload(&set)).unwrap(),
            OwnedRequest::SetMembers {
                auth: b"s3cret".to_vec(),
                epoch: 42,
                vnodes: 128,
                rf: 3,
                members: vec![(1, 1), (5, 4), (9, 2)],
                addrs: addrs.clone(),
                incarnation: None,
            }
        );
        let members = encode_request(&Request::Members);
        assert_eq!(
            decode_request(payload(&members)).unwrap(),
            OwnedRequest::Members
        );
        let resp = Response::Members {
            epoch: 7,
            vnodes: 64,
            rf: 2,
            members: vec![(2, 1), (4, 3), (6, 1), (8, 8)],
            addrs,
        };
        assert_eq!(
            decode_response(payload(&encode_response(&resp))).unwrap(),
            resp
        );
    }

    #[test]
    fn control_push_incarnation_trailer_roundtrips_and_is_absent_on_legacy_frames() {
        let id = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210u128;
        let zones = [WireZone {
            id: 1,
            rf: 1,
            ordering: 0,
            scope: None,
            name: "z",
        }];
        let pushes = |incarnation| {
            [
                encode_request_payload(&Request::SetMembers {
                    auth: b"s",
                    epoch: 3,
                    vnodes: 8,
                    rf: 1,
                    members: &[(1, 1)],
                    addrs: &[],
                    incarnation,
                }),
                encode_request_payload(&Request::SetZones {
                    auth: b"s",
                    epoch: 3,
                    zones: &zones,
                    incarnation,
                }),
                encode_request_payload(&Request::SetRangeMaps {
                    auth: b"s",
                    epoch: 3,
                    maps: &[],
                    incarnation,
                }),
            ]
        };
        let carried = |req: &OwnedRequest| match req {
            OwnedRequest::SetMembers { incarnation, .. }
            | OwnedRequest::SetZones { incarnation, .. }
            | OwnedRequest::SetRangeMaps { incarnation, .. } => *incarnation,
            other => panic!("unexpected {other:?}"),
        };
        for (legacy, fenced) in pushes(None).into_iter().zip(pushes(Some(id))) {
            assert_eq!(fenced.len(), legacy.len() + 17);
            assert_eq!(&fenced[..legacy.len()], legacy.as_slice());
            assert_eq!(carried(&decode_request(&legacy).unwrap()), None);
            assert_eq!(carried(&decode_request(&fenced).unwrap()), Some(id));

            let mut extra = fenced.clone();
            extra.push(0);
            assert_eq!(decode_request(&extra), Err(WireError::TrailingBytes));
            let mut bad_tag = legacy.clone();
            bad_tag.push(TAG_NONE);
            assert_eq!(decode_request(&bad_tag), Err(WireError::BadTag(TAG_NONE)));
            assert_eq!(
                decode_request(&fenced[..fenced.len() - 1]),
                Err(WireError::Truncated)
            );
        }
    }

    #[test]
    fn truncated_or_garbage_member_addrs_fail_cleanly() {
        let mut b = encode_request_payload(&Request::SetMembers {
            auth: b"",
            epoch: 1,
            vnodes: 8,
            rf: 1,
            members: &[(1, 1)],
            addrs: &[],
            incarnation: None,
        });
        let len = b.len();
        b[len - 4..].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_request(&b).is_err());
        let mut bad = Vec::new();
        put_sock_addr(&mut bad, &"127.0.0.1:80".parse().unwrap());
        bad[0] = 9;
        let mut pos = 0;
        assert_eq!(read_sock_addr(&bad, &mut pos), None);
    }

    #[test]
    fn zone_ops_roundtrip() {
        let scope: &[u32] = &[1, 2, 3];
        let zones = [
            WireZone {
                id: 1,
                rf: 3,
                ordering: 0,
                scope: None,
                name: "default",
            },
            WireZone {
                id: 2,
                rf: 2,
                ordering: 1,
                scope: Some(scope),
                name: "tenant-a",
            },
        ];
        let set = encode_request(&Request::SetZones {
            auth: b"s3cret",
            epoch: 7,
            zones: &zones,
            incarnation: None,
        });
        let expected = vec![
            OwnedZone {
                id: 1,
                rf: 3,
                ordering: 0,
                scope: None,
                name: "default".to_owned(),
            },
            OwnedZone {
                id: 2,
                rf: 2,
                ordering: 1,
                scope: Some(vec![1, 2, 3]),
                name: "tenant-a".to_owned(),
            },
        ];
        assert_eq!(
            decode_request(payload(&set)).unwrap(),
            OwnedRequest::SetZones {
                auth: b"s3cret".to_vec(),
                epoch: 7,
                zones: expected.clone(),
                incarnation: None,
            }
        );

        assert_eq!(
            decode_request(payload(&encode_request(&Request::Zones))).unwrap(),
            OwnedRequest::Zones
        );
        let resp = Response::Zones {
            epoch: 7,
            zones: expected,
        };
        assert_eq!(
            decode_response(payload(&encode_response(&resp))).unwrap(),
            resp
        );

        let empty = encode_request(&Request::SetZones {
            auth: &[],
            epoch: 0,
            zones: &[],
            incarnation: None,
        });
        assert_eq!(
            decode_request(payload(&empty)).unwrap(),
            OwnedRequest::SetZones {
                auth: vec![],
                epoch: 0,
                zones: vec![],
                incarnation: None,
            }
        );
    }

    #[test]
    fn sync_responses_roundtrip() {
        let cases = [
            Response::Tree(vec![(0, 0), (1, u64::MAX), (15, 42)]),
            Response::BucketDigests {
                digests: vec![(b"k1".to_vec(), v(10, 1)), (b"k2".to_vec(), v(11, 2))],
                more: false,
                resume: None,
            },
            Response::BucketDigests {
                digests: vec![(b"k3".to_vec(), v(12, 3))],
                more: true,
                resume: None,
            },
            Response::BucketDigests {
                digests: Vec::new(),
                more: true,
                resume: Some(b"k9".to_vec()),
            },
            Response::Values {
                entries: vec![
                    (
                        b"k1".to_vec(),
                        Stored {
                            version: v(10, 1),
                            value: Some(b"v1".to_vec()),
                        },
                    ),
                    (
                        b"k2".to_vec(),
                        Stored {
                            version: v(11, 2),
                            value: None,
                        },
                    ),
                ],
                more: false,
            },
            Response::Values {
                entries: vec![(
                    b"k3".to_vec(),
                    Stored {
                        version: v(12, 3),
                        value: Some(b"v3".to_vec()),
                    },
                )],
                more: true,
            },
        ];
        for resp in cases {
            let frame = encode_response(&resp);
            assert_eq!(decode_response(payload(&frame)).unwrap(), resp);
        }
    }

    #[test]
    fn batch_apply_request_roundtrips() {
        let writes: &[WireWrite] = &[
            (b"k1", Some(&b"v1"[..]), v(10, 1)),
            (b"k2", Some(&b""[..]), v(11, 1)),
            (b"k3", None, v(12, 2)),
        ];
        let frame = encode_request(&Request::BatchApply {
            epoch: 5,
            zone_id: DEFAULT_ZONE,
            sync: true,
            writes,
        });
        assert_eq!(
            decode_request(payload(&frame)).unwrap(),
            OwnedRequest::BatchApply {
                epoch: 5,
                zone_id: DEFAULT_ZONE,
                sync: true,
                writes: vec![
                    (b"k1".to_vec(), Some(b"v1".to_vec()), v(10, 1)),
                    (b"k2".to_vec(), Some(Vec::new()), v(11, 1)),
                    (b"k3".to_vec(), None, v(12, 2)),
                ],
            }
        );
        let nosync = encode_request(&Request::BatchApply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            sync: false,
            writes: &[(b"k", Some(&b"v"[..]), v(1, 1))],
        });
        match decode_request(payload(&nosync)).unwrap() {
            OwnedRequest::BatchApply { sync, .. } => assert!(!sync),
            _ => panic!("expected batch apply"),
        }
    }

    #[test]
    fn batch_response_roundtrips() {
        let resp = Response::Batch(vec![
            Applied::Won,
            Applied::Lost(v(7, 3)),
            Applied::Rejected,
            Applied::Won,
        ]);
        let frame = encode_response(&resp);
        assert_eq!(decode_response(payload(&frame)).unwrap(), resp);
        let rej = encode_response(&Response::Apply(Applied::Rejected));
        assert_eq!(
            decode_response(payload(&rej)).unwrap(),
            Response::Apply(Applied::Rejected)
        );
    }

    #[test]
    fn mixed_batch_read_roundtrips_roles_results_and_cursor() {
        let reads = [
            (&b"a"[..], MixedReadRole::Value),
            (&b"b"[..], MixedReadRole::Digest),
            (&b"a"[..], MixedReadRole::Digest),
        ];
        let request = Request::MixedBatchRead {
            epoch: 17,
            zone_id: 3,
            start: 1,
            reads: &reads,
        };
        assert_eq!(
            decode_request(payload(&encode_request(&request))).unwrap(),
            OwnedRequest::MixedBatchRead {
                epoch: 17,
                zone_id: 3,
                start: 1,
                reads: vec![
                    (b"a".to_vec(), MixedReadRole::Value),
                    (b"b".to_vec(), MixedReadRole::Digest),
                    (b"a".to_vec(), MixedReadRole::Digest),
                ],
            }
        );

        let response = Response::MixedBatchRead {
            start: 1,
            results: vec![
                MixedReadResult::Reply(MixedReadReply::Value(Some(Stored {
                    version: v(9, 2),
                    value: None,
                }))),
                MixedReadResult::Reply(MixedReadReply::Digest(Some(v(10, 3)))),
                MixedReadResult::Reply(MixedReadReply::Digest(None)),
                MixedReadResult::Busy,
                MixedReadResult::Err("read failed".into()),
            ],
            more: true,
        };
        assert_eq!(
            decode_response(payload(&encode_response(&response))).unwrap(),
            response
        );
    }

    #[test]
    fn mixed_batch_read_rejects_bad_roles_cursors_and_counts() {
        let mut bad_role = vec![OP_MIXED_BATCH_READ];
        bad_role.extend_from_slice(&0u64.to_le_bytes());
        bad_role.extend_from_slice(&DEFAULT_ZONE.to_le_bytes());
        bad_role.extend_from_slice(&0u32.to_le_bytes());
        bad_role.extend_from_slice(&1u32.to_le_bytes());
        bad_role.push(7);
        bad_role.extend_from_slice(&0u32.to_le_bytes());
        assert_eq!(decode_request(&bad_role), Err(WireError::BadTag(7)));

        let reads = [(&b"a"[..], MixedReadRole::Value)];
        let cursor = encode_request_payload(&Request::MixedBatchRead {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            start: 2,
            reads: &reads,
        });
        assert_eq!(decode_request(&cursor), Err(WireError::Truncated));

        let mut trailing = encode_request_payload(&Request::MixedBatchRead {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            start: 0,
            reads: &reads,
        });
        trailing.push(0);
        assert_eq!(decode_request(&trailing), Err(WireError::TrailingBytes));

        let mut too_many = vec![OP_MIXED_BATCH_READ];
        too_many.extend_from_slice(&0u64.to_le_bytes());
        too_many.extend_from_slice(&DEFAULT_ZONE.to_le_bytes());
        too_many.extend_from_slice(&0u32.to_le_bytes());
        too_many.extend_from_slice(&((MAX_MIXED_BATCH_READ_ENTRIES + 1) as u32).to_le_bytes());
        assert_eq!(decode_request(&too_many), Err(WireError::TooManyEntries));

        let mut response = encode_response_payload(&Response::MixedBatchRead {
            start: 0,
            results: vec![MixedReadResult::Reply(MixedReadReply::Digest(None))],
            more: false,
        });
        response.push(0);
        assert_eq!(decode_response(&response), Err(WireError::TrailingBytes));
    }

    #[test]
    fn truncated_payload_is_an_error() {
        let frame = encode_request(&Request::Read {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"abc",
        });
        let p = payload(&frame);
        assert_eq!(decode_request(&p[..p.len() - 1]), Err(WireError::Truncated));
    }

    #[test]
    fn bad_opcode_is_an_error() {
        assert_eq!(decode_request(&[200]), Err(WireError::BadTag(200)));
    }

    #[test]
    fn lying_huge_count_fails_promptly_without_allocating() {
        let huge = u32::MAX.to_le_bytes();

        let mut p = vec![RESP_TREE];
        p.extend_from_slice(&huge);
        assert_eq!(decode_response(&p), Err(WireError::Truncated));

        let mut p = vec![RESP_BATCH];
        p.extend_from_slice(&huge);
        assert_eq!(decode_response(&p), Err(WireError::Truncated));
        for tag in [RESP_BUCKET_DIGESTS, RESP_VALUES, RESP_SCAN] {
            let mut p = vec![tag, 0];
            p.extend_from_slice(&huge);
            assert_eq!(decode_response(&p), Err(WireError::Truncated));
        }
        let mut p = vec![RESP_MEMBERS];
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&1u32.to_le_bytes());
        p.extend_from_slice(&huge);
        assert_eq!(decode_response(&p), Err(WireError::Truncated));

        let mut p = vec![OP_BATCH_APPLY];
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(&DEFAULT_ZONE.to_le_bytes());
        p.push(1);
        p.extend_from_slice(&huge);
        assert_eq!(decode_request(&p), Err(WireError::Truncated));

        let mut p = vec![OP_SYNC_BUCKET_DIGESTS];
        p.extend_from_slice(&DEFAULT_ZONE.to_le_bytes());
        p.extend_from_slice(&64u32.to_le_bytes());
        p.extend_from_slice(&huge);
        assert_eq!(decode_request(&p), Err(WireError::Truncated));

        let mut p = vec![OP_SYNC_VALUES];
        p.extend_from_slice(&DEFAULT_ZONE.to_le_bytes());
        p.extend_from_slice(&huge);
        assert_eq!(decode_request(&p), Err(WireError::Truncated));

        let mut p = vec![OP_SET_RANGE_MAPS];
        p.extend_from_slice(&0u32.to_le_bytes());
        p.extend_from_slice(&0u64.to_le_bytes());
        p.extend_from_slice(&huge);
        assert_eq!(decode_request(&p), Err(WireError::Truncated));

        let mut p = vec![OP_SET_MEMBERS];
        p.extend_from_slice(&huge);
        assert_eq!(decode_request(&p), Err(WireError::Truncated));
    }
