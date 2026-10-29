    use super::*;
    use super::range_policy::{rehome_delivery_confirmed, rehome_owner_shard};
    use crate::node::Node;
    use crate::replica::VersionedStore;
    use crate::zone::Ordering;
    use std::time::Instant;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Ordering as SOrdering};

    #[test]
    fn raft_frames_larger_than_data_frame_cap_remain_compatible() {
        assert!(raft_frame_fits_budget(
            crate::net::MAX_FRAME + 1,
            crate::transport::TransportLimits::default().max_queued_bytes,
        ));
    }

    fn certify_opaque_capability(state: &mut PushState) {
        state.leader = true;
        state.opaque_capability_certified = Some((
            state.member_epoch,
            state.zone_epoch,
            state.range_map_epoch,
        ));
    }

    fn canonical_guard_fixture(
        observed_at: Instant,
        ranges: HashMap<(ZoneId, Vec<u8>), u64>,
    ) -> (PushState, AddressedMembers, Catalog, HashMap<ZoneId, RangeMap>) {
        let zone_id = 7;
        let mut state = PushState::new();
        state.leader = true;
        state.member_epoch = 3;
        state.zone_epoch = 4;
        state.range_map_epoch = 5;
        state.members = vec![(1, 1, Vec::new())];
        state.zones.insert(zone_id, ZoneConfig {
            name: "canonical-guard".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        });
        state.range_maps.insert(
            zone_id,
            RangeMap::from_splits([b"m".to_vec()]),
        );
        state.apply_range_entry_poll(RangeEntryPoll {
            member_epoch: state.member_epoch,
            zone_epoch: state.zone_epoch,
            range_map_epoch: state.range_map_epoch,
            observed_at,
            ranges,
        });
        (
            state.clone(),
            state.members.clone(),
            state.zones.clone(),
            state.range_maps.clone(),
        )
    }

    #[test]
    fn canonical_split_guard_uses_the_existing_parent_for_legacy_and_opaque_targets() {
        let now = Instant::now();
        let ranges = HashMap::from([
            ((7, Vec::new()), 9),
            ((7, b"m".to_vec()), 0),
        ]);
        let (state, members, zones, maps) = canonical_guard_fixture(now, ranges);
        let legacy = RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Legacy);
        let opaque = RangeOpKind::Split(
            SplitMode::Canonical,
            CanonicalTarget::Opaque(b"opaque-placement".to_vec()),
        );
        let inherited = RangeOpKind::Split(SplitMode::Inherit, CanonicalTarget::Legacy);

        assert_eq!(
            populated_canonical_split_parent(
                &state, &members, &zones, &maps, 7, b"g", &legacy, now,
            ),
            Some((Vec::new(), 9)),
        );
        assert_eq!(
            populated_canonical_split_parent(
                &state, &members, &zones, &maps, 7, b"g", &opaque, now,
            ),
            Some((Vec::new(), 9)),
        );
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"t", &legacy, now,
        ).is_none());
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"g", &inherited, now,
        ).is_none());
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"m", &legacy, now,
        ).is_none());

        state.range_entry_estimates.lock().unwrap().as_mut().unwrap()
            .ranges.remove(&(7, Vec::new()));
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"g", &legacy, now,
        ).is_none());
    }

    #[test]
    fn canonical_split_guard_invalidates_stale_epochs_and_topology() {
        let now = Instant::now();
        let stale = now - Duration::from_millis(
            DEFAULT_RANGE_SPLIT_POLL_MS * RANGE_ENTRY_ESTIMATE_TTL_POLLS + 1,
        );
        let ranges = HashMap::from([((7, Vec::new()), 9)]);
        let (state, members, zones, maps) = canonical_guard_fixture(stale, ranges.clone());
        let canonical = RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Legacy);
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"g", &canonical, now,
        ).is_none());

        let (state, members, zones, maps) = canonical_guard_fixture(now, ranges);
        for changed in 0..3 {
            let mut changed_state = state.clone();
            match changed {
                0 => changed_state.member_epoch += 1,
                1 => changed_state.zone_epoch += 1,
                _ => changed_state.range_map_epoch += 1,
            }
            assert!(populated_canonical_split_parent(
                &changed_state, &members, &zones, &maps, 7, b"g", &canonical, now,
            ).is_none());
        }
        let mut changed_members = members.clone();
        changed_members.push((2, 1, Vec::new()));
        assert!(populated_canonical_split_parent(
            &state, &changed_members, &zones, &maps, 7, b"g", &canonical, now,
        ).is_none());
        let mut changed_maps = maps.clone();
        changed_maps.insert(7, RangeMap::from_splits([b"h".to_vec(), b"m".to_vec()]));
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &changed_maps, 7, b"g", &canonical, now,
        ).is_none());
    }

    #[test]
    fn queued_canonical_split_rechecks_a_newly_known_populated_parent() {
        let now = Instant::now();
        let (mut state, members, zones, maps) = canonical_guard_fixture(now, HashMap::new());
        let canonical = RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Legacy);
        assert!(populated_canonical_split_parent(
            &state, &members, &zones, &maps, 7, b"g", &canonical, now,
        ).is_none());

        state.apply_range_entry_poll(RangeEntryPoll {
            member_epoch: state.member_epoch,
            zone_epoch: state.zone_epoch,
            range_map_epoch: state.range_map_epoch,
            observed_at: now,
            ranges: HashMap::from([((7, Vec::new()), 11)]),
        });
        assert_eq!(
            populated_canonical_split_parent(
                &state, &members, &zones, &maps, 7, b"g", &canonical, now,
            ),
            Some((Vec::new(), 11)),
        );
    }

    #[test]
    fn epoch_zero_range_map_push_is_delivered_once_so_nodes_release_their_hold() {
        let addr: SocketAddr = "127.0.0.1:41010".parse().unwrap();
        let nodes = vec![(1, vec![addr])];
        let mut confirmed = HashMap::new();
        let mut features = HashMap::new();
        let mut retries = HashMap::new();
        let mut pushes = Vec::new();
        for _ in 0..2 {
            push_range_maps_confirmed_with(
                &nodes,
                0,
                &HashMap::new(),
                &mut confirmed,
                &mut features,
                &mut retries,
                false,
                |_| unreachable!("no opaque preflight for an empty map set"),
                |addr, epoch, maps, _| {
                    pushes.push((addr, epoch, maps.len()));
                    Ok((epoch, 0))
                },
            );
        }
        assert_eq!(pushes, vec![(addr, 0, 0)]);
        assert_eq!(confirmed.get(&addr), Some(&0));
    }

    #[test]
    fn opaque_range_map_slow_endpoint_does_not_starve_healthy_delivery() {
        let slow: SocketAddr = "127.0.0.1:41001".parse().unwrap();
        let healthy: SocketAddr = "127.0.0.1:41002".parse().unwrap();
        let nodes = vec![(1, vec![slow]), (2, vec![healthy])];
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        let maps = HashMap::from([(7, map)]);
        let mut confirmed = HashMap::new();
        let mut features = HashMap::new();
        let mut retries = HashMap::new();
        let mut pushed = Vec::new();
        push_range_maps_confirmed_with(
            &nodes,
            9,
            &maps,
            &mut confirmed,
            &mut features,
            &mut retries,
            true,
            |addr| {
                if addr == slow {
                    Err(io::Error::from(io::ErrorKind::TimedOut))
                } else {
                    Ok(crate::proto::FEATURE_OPAQUE_RANGE_MAP)
                }
            },
            |addr, epoch, _, require_opaque| {
                pushed.push(addr);
                assert!(require_opaque);
                Ok((epoch, crate::proto::FEATURE_OPAQUE_RANGE_MAP))
            },
        );
        assert_eq!(pushed, vec![healthy]);
        assert_eq!(confirmed.get(&healthy), Some(&9));
        assert!(!confirmed.contains_key(&slow));
        let slow_metrics = crate::observe::pd_range_map_endpoint_metrics()
            .into_iter().find(|metric| metric.endpoint == slow.to_string()).unwrap();
        assert!(slow_metrics.preflight_attempts > 0);
        assert!(slow_metrics.preflight_blocks > 0);
        assert_eq!(slow_metrics.push_attempts, 0);
        let healthy_metrics = crate::observe::pd_range_map_endpoint_metrics()
            .into_iter().find(|metric| metric.endpoint == healthy.to_string()).unwrap();
        assert!(healthy_metrics.push_attempts > 0);
        assert!(healthy_metrics.last_confirmed_age_seconds.is_some());

        retries.get_mut(&slow).unwrap().next_attempt = Instant::now();
        push_range_maps_confirmed_with(
            &nodes,
            9,
            &maps,
            &mut confirmed,
            &mut features,
            &mut retries,
            true,
            |_| Ok(crate::proto::FEATURE_OPAQUE_RANGE_MAP),
            |_, epoch, _, require_opaque| {
                assert!(require_opaque);
                Ok((epoch, crate::proto::FEATURE_OPAQUE_RANGE_MAP))
            },
        );
        assert_eq!(confirmed.get(&slow), Some(&9));
    }

    #[test]
    fn opaque_range_map_stale_capability_and_connection_downgrade_fail_closed() {
        let old: SocketAddr = "127.0.0.1:41003".parse().unwrap();
        let nodes = vec![(1, vec![old])];
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        let maps = HashMap::from([(7, map)]);
        let mut confirmed = HashMap::new();
        let mut features = HashMap::from([(old, crate::proto::FEATURE_OPAQUE_RANGE_MAP)]);
        let mut retries = HashMap::new();
        let mut pushes = 0;
        push_range_maps_confirmed_with(
            &nodes,
            10,
            &maps,
            &mut confirmed,
            &mut features,
            &mut retries,
            true,
            |_| Ok(0),
            |_, _, _, _| {
                pushes += 1;
                unreachable!()
            },
        );
        assert_eq!(pushes, 0);
        assert_eq!(features.get(&old), Some(&0));
        assert!(confirmed.is_empty());

        retries.get_mut(&old).unwrap().next_attempt = Instant::now();
        push_range_maps_confirmed_with(
            &nodes,
            10,
            &maps,
            &mut confirmed,
            &mut features,
            &mut retries,
            true,
            |_| Ok(crate::proto::FEATURE_OPAQUE_RANGE_MAP),
            |_, _, _, require_opaque| {
                assert!(require_opaque);
                Err(io::Error::new(io::ErrorKind::Unsupported, "downgraded"))
            },
        );
        assert!(confirmed.is_empty());
        assert!(!PushState::new().opaque_range_maps_enabled);
        assert!(!PushState::new().balance_execution_enabled);
    }

    #[test]
    fn opaque_range_map_push_sends_no_request_to_a_featureless_peer() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut client_hello = [0u8; 4 + crate::proto::HELLO_BODY_LEN];
            stream.read_exact(&mut client_hello).unwrap();
            let mut hello = (crate::proto::HELLO_BODY_LEN as u32).to_le_bytes().to_vec();
            hello.extend_from_slice(&crate::proto::HELLO_MAGIC);
            hello.extend_from_slice(&crate::proto::PROTO_VERSION.to_le_bytes());
            hello.extend_from_slice(&0u64.to_le_bytes());
            stream.write_all(&hello).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
            let mut byte = [0u8; 1];
            matches!(stream.read(&mut byte), Ok(0) | Err(_))
        });
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        let maps = [(7, &map)];
        let error = push_set_range_maps(addr, 1, &maps, true, None).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(peer.join().unwrap());
    }

    #[test]
    fn range_protocol_log_identity_never_formats_key_material() {
        let lower = b"tenant/private/range";
        let rendered = format!("{:016x}", range_log_id(7, lower));
        assert_eq!(rendered.len(), 16);
        assert!(!rendered.as_bytes().windows(lower.len()).any(|window| window == lower));
        assert_ne!(range_log_id(7, lower), range_log_id(8, lower));
    }

    #[test]
    fn pending_control_mutations_block_balance_append_until_reflected() {
        assert!(!control_mutations_settled(40, 41, false, false));
        assert!(!control_mutations_settled(41, 41, true, false));
        assert!(!control_mutations_settled(41, 41, false, true));
        assert!(control_mutations_settled(41, 41, false, false));
    }

    fn engine() -> Engine<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 512);
        Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        )
    }

    fn data_store(node: NodeId) -> VersionedStore<MemBlockDevice> {
        VersionedStore::new(engine(), node).unwrap()
    }

    fn members_of(addr: SocketAddr) -> Vec<NodeId> {
        match push_query(addr, &Request::Members) {
            Response::Members { members, .. } => members.into_iter().map(|(n, _)| n).collect(),
            other => panic!("expected Members, got {other:?}"),
        }
    }

    fn config_of(addr: SocketAddr) -> (u64, u32, u32) {
        match push_query(addr, &Request::Members) {
            Response::Members {
                epoch, vnodes, rf, ..
            } => (epoch, vnodes, rf),
            other => panic!("expected Members, got {other:?}"),
        }
    }

    fn push_query(addr: SocketAddr, req: &Request) -> Response {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let payload = wire::encode_request_payload(req);
        let len = (8 + payload.len()) as u32;
        let mut frame = len.to_le_bytes().to_vec();
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
        wire::decode_response(&block[8..]).unwrap()
    }

    #[test]
    fn rehome_report_gap_aborts_only_missing_required_owner() {
        let start = Instant::now();
        let deadline = Duration::from_millis(100);
        let required: std::collections::HashSet<(NodeId, u32)> =
            [(1, 0), (2, 1)].into_iter().collect();
        let mut state = RehomeReportState {
            first_seen: start,
            pending_epoch: 0,
            delivery_confirmed: true,
            mirror_failures: HashMap::new(),
            owners: HashMap::new(),
        };
        rehome_owner_refresh(&mut state, (1, 0), false, false, 0, start);
        assert_eq!(
            rehome_abort_cause(
                &state, &required, start + Duration::from_millis(99), deadline
            ),
            None
        );
        assert!(matches!(
            rehome_abort_cause(
                &state, &required, start + Duration::from_millis(100), deadline
            ),
            Some(RehomeAbortCause::ReportGap(..))
        ));

        let now = start + Duration::from_millis(150);
        rehome_owner_refresh(&mut state, (1, 0), false, false, 0, now);
        rehome_owner_refresh(&mut state, (2, 1), false, false, 0, now);
        assert_eq!(rehome_abort_cause(&state, &required, now, deadline), None);
        let later = now + Duration::from_millis(90);
        rehome_owner_refresh(&mut state, (1, 0), false, false, 0, later);
        rehome_owner_refresh(&mut state, (2, 1), false, false, 0, later);
        assert_eq!(rehome_abort_cause(&state, &required, later, deadline), None);
    }

    #[test]
    fn rehome_target_refreshes_on_progress_or_completion_not_mere_reports() {
        let start = Instant::now();
        let deadline = Duration::from_millis(100);
        let required: std::collections::HashSet<(NodeId, u32)> =
            [(3, 2)].into_iter().collect();
        let mut state = RehomeReportState {
            first_seen: start,
            pending_epoch: 0,
            delivery_confirmed: true,
            mirror_failures: HashMap::new(),
            owners: HashMap::new(),
        };
        rehome_owner_refresh(&mut state, (3, 2), true, false, 100, start);

        let t1 = start + Duration::from_millis(60);
        rehome_owner_refresh(&mut state, (3, 2), true, false, 100, t1);
        let t2 = start + Duration::from_millis(120);
        rehome_owner_refresh(&mut state, (3, 2), true, false, 100, t2);
        assert_eq!(
            rehome_abort_cause(&state, &required, t2, deadline),
            Some(RehomeAbortCause::CopyStall(3, 2)),
            "reporting without progress must count as a stalled copy"
        );

        let t3 = t2 + Duration::from_millis(10);
        rehome_owner_refresh(&mut state, (3, 2), true, false, 200, t3);
        assert_eq!(
            rehome_abort_cause(&state, &required, t3 + Duration::from_millis(99), deadline),
            None,
            "advancing progress must reset the deadline"
        );

        let t4 = t3 + Duration::from_millis(99);
        rehome_owner_refresh(&mut state, (3, 2), true, true, 200, t4);
        assert_eq!(
            rehome_abort_cause(&state, &required, t4 + Duration::from_millis(99), deadline),
            None,
            "a completed copy needs liveness only"
        );
        assert_eq!(
            rehome_abort_cause(&state, &required, t4 + Duration::from_millis(100), deadline),
            Some(RehomeAbortCause::ReportGap(3, 2)),
            "a silent owner still aborts"
        );
    }

    #[test]
    fn rehome_delivery_without_reports_uses_multi_shard_target_topology() {
        let addrs: Vec<SocketAddr> = [
            "127.0.0.1:42001",
            "127.0.0.1:42002",
            "127.0.0.1:42003",
        ]
        .into_iter()
        .map(|addr| addr.parse().unwrap())
        .collect();
        let targets = vec![(1, addrs.clone())];
        let token = (0u64..)
            .map(u64::to_le_bytes)
            .find(|token| rehome_owner_shard(&targets, 1, token) != 0)
            .unwrap();
        let owner = rehome_owner_shard(&targets, 1, &token);
        let required = [(1, owner)].into_iter().collect();
        let mut confirmed = HashMap::from([(addrs[0], 12)]);
        assert!(!rehome_delivery_confirmed(
            &targets,
            &confirmed,
            &required,
            12,
        ));
        confirmed.insert(addrs[owner as usize], 12);
        assert!(rehome_delivery_confirmed(
            &targets,
            &confirmed,
            &required,
            12,
        ));
    }

    #[test]
    fn rehome_deadlines_start_after_delivery_and_reset_after_restart() {
        let start = Instant::now();
        let deadline = Duration::from_millis(100);
        let required: std::collections::HashSet<(NodeId, u32)> =
            [(1, 0)].into_iter().collect();
        let mut state = RehomeReportState {
            first_seen: start,
            pending_epoch: 8,
            delivery_confirmed: false,
            mirror_failures: HashMap::new(),
            owners: HashMap::new(),
        };
        assert_eq!(
            rehome_abort_cause(
                &state, &required, start + Duration::from_secs(10), deadline
            ),
            None
        );

        let delivered = start + Duration::from_secs(10);
        rehome_delivery_refresh(&mut state, true, delivered);
        assert_eq!(
            rehome_abort_cause(
                &state,
                &required,
                delivered + Duration::from_millis(99),
                deadline
            ),
            None
        );
        assert!(matches!(
            rehome_abort_cause(
                &state,
                &required,
                delivered + Duration::from_millis(100),
                deadline
            ),
            Some(RehomeAbortCause::ReportGap(1, 0))
        ));

        rehome_owner_refresh(&mut state, (1, 0), true, false, 10, delivered);
        rehome_delivery_refresh(&mut state, false, delivered + Duration::from_millis(50));
        assert_eq!(
            rehome_abort_cause(
                &state,
                &required,
                delivered + Duration::from_secs(10),
                deadline
            ),
            None
        );
        let redelivered = delivered + Duration::from_secs(10);
        rehome_delivery_refresh(&mut state, true, redelivered);
        assert_eq!(
            rehome_abort_cause(
                &state,
                &required,
                redelivered + Duration::from_millis(99),
                deadline
            ),
            None
        );
        rehome_owner_refresh(
            &mut state,
            (1, 0),
            true,
            false,
            10,
            redelivered + Duration::from_millis(100),
        );
        assert_eq!(
            rehome_abort_cause(
                &state,
                &required,
                redelivered + Duration::from_millis(100),
                deadline
            ),
            Some(RehomeAbortCause::CopyStall(1, 0))
        );
    }

    #[test]
    fn load_split_hysteresis_blocks_a_hot_pair_from_merging() {
        assert_eq!(load_merge_ceiling(0), u64::MAX);
        assert_eq!(load_merge_ceiling(100), 50);
        assert!(49 < load_merge_ceiling(100));
        assert!(50 >= load_merge_ceiling(100));
        assert!(80 >= load_merge_ceiling(100));
    }

    #[test]
    fn node_load_skew_distinguishes_balanced_and_skewed_membership() {
        let balanced = HashMap::from([(1, 100u128), (2, 100), (3, 100)]);
        assert_eq!(node_load_skew_pct(&balanced), 100);

        let skewed = HashMap::from([(1, 300u128), (2, 0), (3, 0)]);
        assert_eq!(node_load_skew_pct(&skewed), 300);
        assert_eq!(node_load_skew_pct(&HashMap::new()), 0);
        assert_eq!(
            node_load_skew_pct(&HashMap::from([(1, 0u128), (2, 0)])),
            0
        );
    }

    #[test]
    fn node_load_attributes_all_sorted_replicas_and_local_hash_shards() {
        let mut snap = PushState::new();
        snap.members = vec![(1, 1, Vec::new()), (2, 1, Vec::new()), (3, 1, Vec::new())];
        let scope: BTreeSet<NodeId> = [1, 2].into_iter().collect();
        snap.zones.insert(
            7,
            ZoneConfig {
                name: "sorted".into(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: Some(scope),
            },
        );
        snap.zones.insert(
            8,
            ZoneConfig {
                name: "hash".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        let map = RangeMap::from_splits([b"m".to_vec()]);
        snap.range_maps.insert(7, map);
        let agg = HashMap::from([
            ((7, Vec::new()), (10, None, 0, 0)),
            ((7, b"m".to_vec()), (20, None, 0, 0)),
        ]);
        let local_hash_entries = HashMap::from([
            ((3, 0, DEFAULT_ZONE), 11),
            ((3, 1, DEFAULT_ZONE), 19),
            ((1, 0, 8), 5),
        ]);
        let ring = Ring::from_members(2, 64, [1, 2, 3]);

        let owned = node_owned_entries(&snap, &agg, &local_hash_entries, &ring);

        assert_eq!(owned[&1], 35);
        assert_eq!(owned[&2], 30);
        assert_eq!(owned[&3], 30);
    }

    #[test]
    fn node_load_uses_legacy_unsalted_placement_for_sorted_zone_zero() {
        let mut snap = PushState::new();
        snap.zones.insert(
            DEFAULT_ZONE,
            ZoneConfig {
                name: "default".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: Some([1].into_iter().collect()),
            },
        );
        snap.range_maps.insert(DEFAULT_ZONE, RangeMap::single());
        let agg = HashMap::from([((DEFAULT_ZONE, Vec::new()), (17, None, 0, 0))]);
        let ring = Ring::from_members(2, 64, 1..=8);
        let unsalted = ring.replicas(b"");
        let salted = ring.replicas_in_zone(DEFAULT_ZONE, b"", 2, None);
        assert_ne!(unsalted, salted);

        let owned = node_owned_entries(&snap, &agg, &HashMap::new(), &ring);

        for node in ring.members() {
            assert_eq!(owned[&node], if unsalted.contains(&node) { 17 } else { 0 });
        }
    }

    #[test]
    fn balanced_canonical_tokens_cover_nodes_primaries_and_engines_evenly() {
        let ring = Ring::from_members(2, 128, [1, 2, 3]);
        let cfg = ZoneConfig {
            name: "sorted".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        };
        let topology = BTreeMap::from([(1, 4), (2, 4), (3, 4)]);
        let lowers: Vec<Vec<u8>> = (0..24).map(|i| vec![i]).collect();
        let choose = || {
            let mut tokens = Vec::new();
            for lower in &lowers {
                tokens.push(balanced_opaque_token(
                    &ring,
                    7,
                    &cfg,
                    &tokens,
                    Some(&topology),
                    lower,
                    DEFAULT_BALANCE_TOKEN_SEARCH_LIMIT,
                ).unwrap());
            }
            tokens
        };
        let tokens = choose();
        assert_eq!(tokens, choose());
        let mut nodes = BTreeMap::from([(1, 0usize), (2, 0), (3, 0)]);
        let mut primaries = nodes.clone();
        let mut engines: BTreeMap<(NodeId, u32), usize> = (1..=3)
            .flat_map(|node| (0..4).map(move |shard| ((node, shard), 0)))
            .collect();
        for token in &tokens {
            let replicas = ring.replicas_in_zone(7, token, 2, None);
            *primaries.get_mut(&replicas[0]).unwrap() += 1;
            for node in replicas {
                *nodes.get_mut(&node).unwrap() += 1;
                let shard = (crate::ring::hash64(token) % 4) as u32;
                *engines.get_mut(&(node, shard)).unwrap() += 1;
            }
        }
        assert_eq!(nodes.values().copied().collect::<Vec<_>>(), vec![16, 16, 16]);
        assert_eq!(primaries.values().copied().collect::<Vec<_>>(), vec![8, 8, 8]);
        assert!(engines.values().max().unwrap() - engines.values().min().unwrap() <= 1);
        assert!(balanced_opaque_token(
            &Ring::from_members(2, 64, std::iter::empty()),
            7,
            &cfg,
            &[],
            None,
            b"",
            64,
        ).is_none());
        let impossible = ZoneConfig { rf: 4, ..cfg };
        assert!(balanced_opaque_token(&ring, 7, &impossible, &[], None, b"", 64).is_none());
    }

    #[test]
    fn balanced_canonical_events_install_and_track_opaque_targets() {
        let ring = Ring::from_members(2, 128, [1, 2, 3]);
        let cfg = ZoneConfig {
            name: "sorted".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        };
        let topology = BTreeMap::from([(1, 4), (2, 4), (3, 4)]);
        let boundaries: Vec<Vec<u8>> = (1..24).map(|i| vec![i]).collect();
        let legacy = initial_range_map(
            9, &cfg, boundaries.clone(), false, &ring, Some(&topology), 4096,
        );
        assert_eq!(legacy, RangeMap::from_splits(boundaries.clone()));
        assert!(!legacy.supports_opaque_tokens());

        let mut map = initial_range_map(
            9, &cfg, boundaries, true, &ring, Some(&topology), 4096,
        );
        assert!(map.supports_opaque_tokens());
        assert_eq!(map.unrehomed_ranges(), 0);
        assert_eq!(RangeMap::decode(&map.encode()), Some(map.clone()));
        let mut slots = BTreeMap::from([(1, 0usize), (2, 0), (3, 0)]);
        let mut primaries = slots.clone();
        let mut engines: BTreeMap<(NodeId, u32), usize> = (1..=3)
            .flat_map(|node| (0..4).map(move |shard| ((node, shard), 0)))
            .collect();
        for index in 0..map.num_ranges() {
            let token = map.token_of(index);
            let replicas = map.replicas_of_range(&ring, 9, index, 2, None);
            *primaries.get_mut(&replicas[0]).unwrap() += 1;
            for node in replicas {
                *slots.get_mut(&node).unwrap() += 1;
                *engines.get_mut(&(node, (crate::ring::hash64(token) % 4) as u32)).unwrap() += 1;
            }
        }
        assert_eq!(slots.values().copied().collect::<Vec<_>>(), vec![16, 16, 16]);
        assert_eq!(primaries.values().copied().collect::<Vec<_>>(), vec![8, 8, 8]);
        assert!(engines.values().max().unwrap() - engines.values().min().unwrap() <= 1);

        let split_at = vec![23, 1];
        let split_token = balanced_opaque_token(
            &ring,
            9,
            &cfg,
            &effective_tokens_except(&map, None),
            Some(&topology),
            &split_at,
            4096,
        ).unwrap();
        assert!(map.split_canonical_with_token(&split_at, split_token));
        let split_index = map.range_index(&split_at);
        assert!(!map.placement_debt_of(split_index));

        let debt_at = vec![23, 2];
        assert!(map.split(&debt_at));
        let debt_index = map.range_index(&debt_at);
        assert!(map.placement_debt_of(debt_index));
        let target = balanced_opaque_token(
            &ring,
            9,
            &cfg,
            &effective_tokens_except(&map, Some(debt_index)),
            Some(&topology),
            &debt_at,
            4096,
        ).unwrap();
        assert!(map.set_pending(debt_index, target.clone()));
        let kind = RangeOpKind::PrepareRehome(CanonicalTarget::Opaque(target));
        assert!(range_op_reflected(&map, &debt_at, &kind));
        assert!(map.commit_pending(debt_index));
        assert!(!map.placement_debt_of(debt_index));

        let retry_at = vec![23, 3];
        assert!(map.split(&retry_at));
        let retry_index = map.range_index(&retry_at);
        let target = balanced_opaque_token(
            &ring,
            9,
            &cfg,
            &effective_tokens_except(&map, Some(retry_index)),
            Some(&topology),
            &retry_at,
            4096,
        ).unwrap();
        assert!(map.set_pending(retry_index, target));
        assert!(map.clear_pending(retry_index));
        assert!(map.placement_debt_of(retry_index));
    }

    #[test]
    fn opaque_canonical_events_fail_closed_when_capability_is_not_ready() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let zone_id = pd.create_zone(ZoneConfig {
            name: "fail-closed".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        settle(&mut pd);
        let current = crate::partition::opaque_placement_token([7; 16]);
        let opaque = RangeMap::from_opaque_parts(
            Vec::new(),
            vec![current],
            vec![None],
            vec![true],
        ).unwrap();
        pd.set_range_map(zone_id, &opaque).unwrap();
        settle(&mut pd);

        let mut stale_state = PushState::new();
        stale_state.leader = true;
        stale_state.opaque_capability_certified = Some((0, 0, 0));
        stale_state.member_epoch = 1;
        assert!(!stale_state.opaque_capability_current());
        let push_state = Mutex::new(stale_state);
        let authority = Mutex::new(BalancePollAuthority::default());
        let mut queued = RangeOpKind::PrepareRehome(CanonicalTarget::Opaque(
            crate::partition::opaque_placement_token([8; 16]),
        ));
        revalidate_queued_placement(
            &push_state,
            &pd.members_addressed(),
            true,
            &mut queued,
        );
        assert!(matches!(
            queued,
            RangeOpKind::PrepareRehome(CanonicalTarget::Legacy)
        ));
        certify_opaque_capability(&mut push_state.lock().unwrap());
        revalidate_queued_placement(
            &push_state,
            &pd.members_addressed(),
            true,
            &mut queued,
        );
        assert!(matches!(
            queued,
            RangeOpKind::PrepareRehome(CanonicalTarget::Legacy)
        ));
        push_state.lock().unwrap().opaque_capability_certified = Some((0, 0, 0));
        let mut queued_split = RangeOpKind::Split(
            SplitMode::Canonical,
            CanonicalTarget::Opaque(crate::partition::opaque_placement_token([9; 16])),
        );
        revalidate_queued_placement(
            &push_state,
            &pd.members_addressed(),
            true,
            &mut queued_split,
        );
        assert!(matches!(
            queued_split,
            RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Legacy)
        ));

        let mut prepare = RangeOpKind::PrepareRehome(CanonicalTarget::Unresolved);
        resolve_canonical_target(
            &pd,
            RingConfig { vnodes: 64, rf: 2 },
            &push_state,
            &authority,
            zone_id,
            b"",
            &mut prepare,
        );
        assert!(matches!(prepare, RangeOpKind::PrepareRehome(CanonicalTarget::Legacy)));
        assert!(propose_range_op(&mut pd, zone_id, b"", &prepare).unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zone_id).pending_of(0), Some(b"".as_slice()));
        assert!(pd.abort_rehome(zone_id, b"").unwrap());
        settle(&mut pd);

        let mut split = RangeOpKind::Split(
            SplitMode::Canonical,
            CanonicalTarget::Unresolved,
        );
        resolve_canonical_target(
            &pd,
            RingConfig { vnodes: 64, rf: 2 },
            &push_state,
            &authority,
            zone_id,
            b"m",
            &mut split,
        );
        assert!(matches!(
            split,
            RangeOpKind::Split(SplitMode::Canonical, CanonicalTarget::Legacy)
        ));
        assert!(propose_range_op(&mut pd, zone_id, b"m", &split).unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zone_id).token_of(1), b"m");
    }

    #[test]
    fn opaque_initial_map_downgrades_when_its_preflight_snapshot_stales() {
        let ring = Ring::from_members(2, 128, [1, 2, 3]);
        let cfg = ZoneConfig {
            name: "sorted".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        };
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let zone_id = pd.create_zone(cfg.clone()).unwrap();
        settle(&mut pd);
        let boundaries: Vec<Vec<u8>> = (1..4).map(|i| vec![i]).collect();
        let legacy = RangeMap::from_splits(boundaries.clone());
        let opaque = initial_range_map(
            zone_id,
            &cfg,
            boundaries,
            true,
            &ring,
            None,
            4096,
        );
        assert!(opaque.supports_opaque_tokens());
        let mut state = PushState::new();
        state.member_epoch = 9;
        certify_opaque_capability(&mut state);
        let push_state = Mutex::new(state);

        let mut capability_drop = RangeOpKind::InstallInitialMap {
            map: opaque.clone(),
            legacy: legacy.clone(),
            cfg: cfg.clone(),
            creation_index: pd.epoch(),
            creation_term: pd.partition.term(),
            authorized: true,
            placement: InitialMapPlacement::Opaque { member_epoch: 9 },
        };
        push_state.lock().unwrap().opaque_capability_certified = None;
        revalidate_queued_placement(
            &push_state,
            &pd.members_addressed(),
            true,
            &mut capability_drop,
        );
        assert!(matches!(
            capability_drop,
            RangeOpKind::InstallInitialMap { ref map, .. } if map == &legacy
        ));
        assert!(propose_range_op(&mut pd, zone_id, b"", &capability_drop).unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zone_id), legacy);

        let mut membership_change = RangeOpKind::InstallInitialMap {
            map: opaque,
            legacy: legacy.clone(),
            cfg: cfg.clone(),
            creation_index: pd.epoch(),
            creation_term: pd.partition.term(),
            authorized: true,
            placement: InitialMapPlacement::Opaque { member_epoch: 10 },
        };
        {
            let mut state = push_state.lock().unwrap();
            state.member_epoch = 9;
            certify_opaque_capability(&mut state);
            state.member_epoch = 10;
        }
        assert!(!push_state.lock().unwrap().opaque_capability_current());
        revalidate_queued_placement(
            &push_state,
            &pd.members_addressed(),
            true,
            &mut membership_change,
        );
        assert!(matches!(
            membership_change,
            RangeOpKind::InstallInitialMap { ref map, .. } if map == &legacy
        ));
        assert!(matches!(
            propose_range_op(&mut pd, zone_id, b"", &membership_change),
            Err(PartitionError::Invariant(_))
        ));
        assert_eq!(pd.range_map(zone_id), legacy);
    }

    fn balance_stat(
        zone_id: ZoneId,
        lower: Vec<u8>,
        entries: u64,
        shard_index: u32,
        shard_count: u32,
    ) -> wire::OwnedRangeStat {
        wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower,
            entries,
            midpoint: None,
            ops_per_sec: 0,
            shard_index,
            shard_count,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        }
    }

    fn balance_fixture(
        nodes: u32,
        shard_counts: &[u32],
        weights: &[u32],
        scope: Option<BTreeSet<NodeId>>,
    ) -> (PushState, Ring, Vec<PlannerEndpointStats>, Vec<(NodeId, Vec<SocketAddr>)>, RangeAgg) {
        let zone_id = 7;
        let mut snap = PushState::new();
        snap.member_epoch = 41;
        snap.range_map_epoch = 9;
        snap.balance_high_pct = 105;
        snap.balance_low_pct = 101;
        snap.balance_min_improvement_pct = 1;
        snap.balance_token_search_limit = 8192;
        let targets: Vec<(NodeId, Vec<SocketAddr>)> = (1..=nodes).map(|node| {
            let addrs = (0..shard_counts[(node - 1) as usize]).map(|shard| {
                format!("127.0.0.1:{}", 10_000 + node * 100 + shard).parse().unwrap()
            }).collect();
            (node, addrs)
        }).collect();
        snap.members = targets.iter().map(|(node, addrs)| {
            (*node, weights[(*node - 1) as usize], addrs.clone())
        }).collect();
        snap.zones.insert(DEFAULT_ZONE, ZoneConfig {
            name: "default".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        });
        snap.zones.insert(zone_id, ZoneConfig {
            name: "balance".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope,
        });
        let splits = vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
        let mut map = RangeMap::from_splits_and_tokens(
            splits, vec![Vec::new(), Vec::new(), Vec::new(), Vec::new()]
        ).unwrap();
        map.enable_opaque_tokens();
        snap.range_maps.insert(zone_id, map);
        let ring = Ring::from_weighted(
            2, 128, snap.members.iter().map(|(n, w, _)| (*n, *w))
        );
        let weights_by_range = [900, 300, 200, 100];
        let lowers = [Vec::new(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()];
        let map = &snap.range_maps[&zone_id];
        let mut endpoints = Vec::new();
        for (node, addrs) in &targets {
            let count = shard_counts[(*node - 1) as usize];
            for (shard, addr) in addrs.iter().enumerate() {
                let mut ranges = vec![balance_stat(
                    DEFAULT_ZONE, Vec::new(),
                    u64::from(*node == nodes && shard == 0) * 250,
                    shard as u32, count,
                )];
                let in_scope = snap.zones.get(zone_id).unwrap().scope.as_ref()
                    .is_none_or(|scope| scope.contains(node));
                if in_scope {
                    for (index, lower) in lowers.iter().enumerate() {
                        let owners = map.replicas_of_range(
                            &ring, zone_id, index, 2,
                            snap.zones.get(zone_id).unwrap().scope.as_ref()
                        );
                        let owner_shard = (crate::ring::hash64(map.token_of(index))
                            % count as u64) as usize;
                        let entries = if owners.contains(node) && owner_shard == shard {
                            weights_by_range[index]
                        } else {
                            0
                        };
                        ranges.push(balance_stat(
                            zone_id, lower.clone(), entries, shard as u32, count
                        ));
                    }
                }
                endpoints.push(PlannerEndpointStats {
                    node: *node,
                    addr: *addr,
                    ranges,
                });
            }
        }
        let agg = lowers.iter().cloned().zip(weights_by_range)
            .map(|(lower, entries)| ((zone_id, lower), (entries, None, 0, 0)))
            .collect();
        (snap, ring, endpoints, targets, agg)
    }

    #[test]
    fn layout_divergent_equal_winners_reach_planner_inputs() {
        let cfg = Config {
            seg_blocks: 8,
            reserved_blocks: 0,
            ordering: SOrdering::Sorted,
        };
        let mut a = VersionedStore::new(
            vals_store::Engine::create_prewiped(MemBlockDevice::new(256, 4096), cfg),
            1,
        ).unwrap();
        let mut b = VersionedStore::new(
            vals_store::Engine::create_prewiped(MemBlockDevice::new(256, 4096), cfg),
            2,
        ).unwrap();
        let left: Vec<Vec<u8>> = (0..80).map(|i| format!("a{i:03}").into_bytes()).collect();
        let right: Vec<Vec<u8>> = (0..80)
            .map(|i| format!("c{i:03}-{}", "x".repeat(96)).into_bytes())
            .collect();
        let version = crate::Version { hlc: 1, node: 9 };
        for key in left.iter().chain(&right) {
            a.apply_at(key, Some(b"v"), version, 0).unwrap();
        }
        a.flush().unwrap();
        for key in &left {
            b.apply_at(key, Some(b"v"), version, 0).unwrap();
        }
        b.flush().unwrap();
        for key in &right {
            b.apply_at(key, Some(b"v"), version, 0).unwrap();
        }
        b.flush().unwrap();
        assert_ne!(
            a.engine().range_estimate(None, Some(b"b")).0,
            b.engine().range_estimate(None, Some(b"b")).0,
        );

        let map = RangeMap::from_splits([b"b".to_vec()]);
        a.set_logical_range_map(9, map.clone());
        b.set_logical_range_map(9, map.clone());
        while !a.seed_step().unwrap() {}
        while !b.seed_step().unwrap() {}
        let a_counts = a.logical_range_counts(9).unwrap();
        let b_counts = b.logical_range_counts(9).unwrap();
        assert_eq!(a_counts, b_counts);

        let mut snap = PushState::new();
        snap.member_epoch = 3;
        snap.zone_epoch = 4;
        snap.range_map_epoch = 9;
        let targets = vec![
            (1, vec!["127.0.0.1:10100".parse().unwrap()]),
            (2, vec!["127.0.0.1:10200".parse().unwrap()]),
        ];
        snap.members = targets.iter().map(|(node, addrs)| (*node, 1, addrs.clone())).collect();
        snap.zones.insert(DEFAULT_ZONE, ZoneConfig {
            name: "default".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        });
        snap.zones.insert(7, ZoneConfig {
            name: "sorted".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        });
        snap.range_maps.insert(7, map.clone());
        let ring = Ring::from_members(2, 64, [1, 2]);
        let endpoints: Vec<_> = targets.iter().zip([a_counts, b_counts]).map(
            |((node, addrs), counts)| PlannerEndpointStats {
                node: *node,
                addr: addrs[0],
                ranges: vec![
                    balance_stat(DEFAULT_ZONE, Vec::new(), 0, 0, 1),
                    balance_stat(7, Vec::new(), counts[0], 0, 1),
                    balance_stat(7, b"b".to_vec(), counts[1], 0, 1),
                ],
            }
        ).collect();
        let agg = HashMap::from([
            ((7, Vec::new()), (80, None, 0, 0)),
            ((7, b"b".to_vec()), (80, None, 0, 0)),
        ]);
        assert!(planner_inputs(&snap, &endpoints, &targets, &agg, &ring).is_some());
    }

    #[test]
    fn balance_executor_preserves_exact_token_and_rejects_stale_snapshots() {
        let ring_cfg = RingConfig { vnodes: 128, rf: 2 };
        let (mut snap, ring, endpoints, targets, agg) = balance_fixture(
            4, &[2, 2, 2, 2], &[1, 1, 1, 2], None
        );
        snap.opaque_range_maps_enabled = true;
        certify_opaque_capability(&mut snap);
        snap.balance_execution_enabled = true;
        let outcome = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        let plan = outcome.plan.as_ref().expect("fixture must produce a move");
        let proposal = make_balance_proposal(
            &snap, ring_cfg, &endpoints, &targets, &agg, &ring, plan, true, 7
        ).expect("fresh deterministic proposal");
        let inputs = planner_inputs(&snap, &endpoints, &targets, &agg, &ring).unwrap();
        let authority = BalancePollAuthority {
            generation: 7,
            snapshot: Some(BalancePollSnapshot {
                member_epoch: snap.member_epoch,
                zone_epoch: snap.zone_epoch,
                range_map_epoch: snap.range_map_epoch,
                range_maps_acked: true,
                capability_ready: true,
                planner_fingerprint: complete_planner_fingerprint(&inputs, &endpoints),
                topology: inputs.topology.clone(),
            }),
        };

        assert_eq!(proposal.token, plan.token);
        assert_eq!(proposal.targets, plan.targets);
        assert_eq!(proposal.identity, balance_proposal_identity(&proposal));
        assert_eq!(validate_balance_snapshot(&snap, ring_cfg, &proposal), Ok(()));
        let mut authoritative = PlacementDriver::new_single(91, engine()).unwrap();
        settle(&mut authoritative);
        for (node, weight, addrs) in &proposal.members {
            authoritative.add_node_addressed(*node, *weight, addrs).unwrap();
            settle(&mut authoritative);
        }
        for (zone_id, cfg) in sorted_zones(&proposal.zones) {
            authoritative.propose_zone(zone_id, &cfg).unwrap();
            settle(&mut authoritative);
        }
        authoritative.set_range_map(proposal.zone_id, &proposal.map).unwrap();
        settle(&mut authoritative);
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &authority, ring_cfg, &proposal
            ),
            Ok(())
        );
        let newer_same_authority = BalancePollAuthority {
            generation: 8,
            snapshot: authority.snapshot.clone(),
        };
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &newer_same_authority, ring_cfg, &proposal
            ),
            Err(BalanceStaleReason::Poll),
            "any later poll invalidates a proposal before prepare"
        );
        let mut changed_entries = endpoints.clone();
        changed_entries[0].ranges[1].entries += 1;
        let changed_entry_authority = BalancePollAuthority {
            generation: 7,
            snapshot: Some(BalancePollSnapshot {
                planner_fingerprint: complete_planner_fingerprint(&inputs, &changed_entries),
                ..authority.snapshot.clone().unwrap()
            }),
        };
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &changed_entry_authority, ring_cfg, &proposal
            ),
            Err(BalanceStaleReason::Poll),
            "a newer entry-estimate poll must invalidate selection"
        );
        let mut changed_topology = authority.snapshot.clone().unwrap();
        *changed_topology.topology.get_mut(&1).unwrap() += 1;
        let changed_topology_authority = BalancePollAuthority {
            generation: 7,
            snapshot: Some(changed_topology),
        };
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &changed_topology_authority, ring_cfg, &proposal
            ),
            Err(BalanceStaleReason::Poll),
            "a newer shard-count poll must invalidate selection"
        );
        authoritative.acquire_balance_intent(proposal.identity).unwrap();
        settle(&mut authoritative);
        assert_eq!(authoritative.balance_intent().unwrap(), Some(proposal.identity));
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &changed_entry_authority, ring_cfg, &proposal
            ),
            Err(BalanceStaleReason::Poll)
        );
        authoritative.clear_balance_intent(proposal.identity).unwrap();
        settle(&mut authoritative);
        assert_eq!(authoritative.balance_intent().unwrap(), None);
        assert_eq!(authoritative.range_map(proposal.zone_id), proposal.map);
        let (node, weight, addrs) = &proposal.members[0];
        authoritative.add_node_addressed(*node, weight + 1, addrs).unwrap();
        settle(&mut authoritative);
        assert_eq!(
            validate_balance_proposal(
                &authoritative, &snap, &authority, ring_cfg, &proposal
            ),
            Err(BalanceStaleReason::Membership),
            "authoritative apply must reject churn even when the poll snapshot is unchanged"
        );
        let mut prepared = proposal.map.clone();
        assert!(prepared.set_pending(proposal.range_index, proposal.token.clone()));
        assert_eq!(prepared.pending_of(proposal.range_index), Some(proposal.token.as_slice()));
        assert!(prepared.clear_pending(proposal.range_index));
        assert_eq!(prepared.token_of(proposal.range_index), proposal.map.token_of(proposal.range_index));

        let mut stale = snap.clone();
        stale.member_epoch += 1;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Membership));
        let mut stale = snap.clone();
        stale.members[0].1 += 1;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Membership));
        assert_eq!(
            validate_balance_snapshot(&snap, RingConfig { vnodes: 64, rf: 2 }, &proposal),
            Err(BalanceStaleReason::Membership)
        );
        let mut stale = snap.clone();
        stale.zone_epoch += 1;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Catalog));
        let mut stale = snap.clone();
        if let Some(mut cfg) = stale.zones.get(proposal.zone_id).cloned() {
            cfg.rf = 1;
            stale.zones.insert(proposal.zone_id, cfg);
        }
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Catalog));
        let mut stale = snap.clone();
        stale.range_map_epoch += 1;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::RangeMap));
        let mut stale = snap.clone();
        stale.range_maps.get_mut(&proposal.zone_id).unwrap()
            .split(b"bb");
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::RangeMap));
        let mut stale = snap.clone();
        stale.opaque_capability_certified = None;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Capability));
        let mut stale = snap.clone();
        stale.balance_execution_enabled = false;
        assert_eq!(validate_balance_snapshot(&stale, ring_cfg, &proposal), Err(BalanceStaleReason::Gate));
        let mut altered = proposal.clone();
        altered.token[5] ^= 1;
        assert_eq!(validate_balance_snapshot(&snap, ring_cfg, &altered), Err(BalanceStaleReason::Identity));
    }

    #[test]
    fn dropped_zone_discards_queued_map_ops_but_leaves_balance_prepares_to_publish_rejection() {
        let ring_cfg = RingConfig { vnodes: 128, rf: 2 };
        let (mut snap, ring, endpoints, targets, agg) = balance_fixture(
            4, &[1, 1, 1, 1], &[1, 1, 1, 2], None
        );
        snap.opaque_range_maps_enabled = true;
        certify_opaque_capability(&mut snap);
        snap.balance_execution_enabled = true;
        let outcome = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        let proposal = make_balance_proposal(
            &snap, ring_cfg, &endpoints, &targets, &agg, &ring,
            outcome.plan.as_ref().unwrap(), true, 1
        ).unwrap();

        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let gone = pd.create_zone(ZoneConfig {
            name: "gone".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        settle(&mut pd);
        pd.drop_zone(gone).unwrap();
        settle(&mut pd);
        let mut ops = vec![
            (gone, b"m".to_vec(), RangeOpKind::Split(SplitMode::Inherit, CanonicalTarget::Legacy)),
            (gone, b"m".to_vec(), RangeOpKind::PrepareBalance(proposal, BalancePreparePhase::Acquire)),
        ];
        assert_eq!(initial_map_queue_head(&pd, &mut ops), InitialMapDecision::Discard);
        assert_eq!(ops.len(), 1);
        assert_eq!(
            initial_map_queue_head(&pd, &mut ops),
            InitialMapDecision::Ready,
            "a balance prepare must reach its own validation, which publishes the rejection \
             the executor waits for"
        );
        assert_eq!(ops.len(), 1);
    }

    #[test]
    fn balance_executor_serializes_and_consumes_a_fresh_poll_after_completion() {
        let ring_cfg = RingConfig { vnodes: 128, rf: 2 };
        let (mut snap, ring, endpoints, targets, agg) = balance_fixture(
            4, &[1, 1, 1, 1], &[1, 1, 1, 2], None
        );
        snap.opaque_range_maps_enabled = true;
        certify_opaque_capability(&mut snap);
        snap.balance_execution_enabled = true;
        let outcome = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        let proposal = make_balance_proposal(
            &snap, ring_cfg, &endpoints, &targets, &agg, &ring,
            outcome.plan.as_ref().unwrap(), true, 1
        ).unwrap();
        let mut executor = BalanceExecutorState::default();
        assert!(executor.may_prepare(&snap, true));
        executor.prepared(proposal.clone());
        for _ in 0..3 {
            assert!(
                !executor.may_prepare(&snap, true),
                "an unresolved drive/quorum result must retain the proposal"
            );
            assert!(executor.proposal.is_some());
        }
        snap.balance_prepare_result = Some((proposal.identity, true));
        assert!(!executor.may_prepare(&snap, true), "accepted prepare is awaiting visibility");
        let map = snap.range_maps.get_mut(&proposal.zone_id).unwrap();
        assert!(map.set_pending(proposal.range_index, proposal.token.clone()));
        snap.range_map_epoch += 1;
        assert!(!executor.may_prepare(&snap, true), "active move serializes globally");
        let mut restarted_executor = BalanceExecutorState::default();
        assert!(
            !restarted_executor.may_prepare(&snap, true),
            "a restarted PD reconstructs the global active-move block from pending state"
        );
        assert!(snap.range_maps.get_mut(&proposal.zone_id).unwrap().commit_pending(proposal.range_index));
        snap.range_map_epoch += 1;
        assert!(!executor.may_prepare(&snap, true), "first committed-map poll is cooldown");
        assert!(
            !restarted_executor.may_prepare(&snap, true),
            "restart/resume also consumes a complete fresh poll after cutover"
        );
        assert!(executor.may_prepare(&snap, true), "following fresh poll may converge again");
        assert!(restarted_executor.may_prepare(&snap, true));
    }

    #[test]
    fn balance_planner_is_repeatable_for_three_four_and_five_nodes() {
        for nodes in 3..=5 {
            let shard_counts: Vec<u32> = (1..=nodes).collect();
            let weights: Vec<u32> = (1..=nodes).map(|n| if n == nodes { 2 } else { 1 }).collect();
            let (snap, ring, endpoints, targets, agg) = balance_fixture(
                nodes, &shard_counts, &weights, None
            );
            let first = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
            let mut reversed = endpoints.clone();
            reversed.reverse();
            let second = balance_dry_run(&snap, &reversed, &targets, &agg, &ring, true);
            let (restart_snap, restart_ring, restart_endpoints, restart_targets, restart_agg) =
                balance_fixture(nodes, &shard_counts, &weights, None);
            let restarted = balance_dry_run(
                &restart_snap, &restart_endpoints, &restart_targets, &restart_agg,
                &restart_ring, true
            );
            let first = first.plan.expect("skewed fixture produced no proposal");
            let second = second.plan.expect("report order changed feasibility");
            let restarted = restarted.plan.expect("restart changed feasibility");
            assert_eq!(first.zone_id, second.zone_id);
            assert_eq!(first.range_index, second.range_index);
            assert_eq!(first.token, second.token);
            assert_eq!(first.targets, second.targets);
            assert_eq!(first.token, restarted.token);
            assert_eq!(first.targets, restarted.targets);
            assert!(first.after_engine.pct() < balance_dry_run(
                &snap, &endpoints, &targets, &agg, &ring, true
            ).before_engine);
            let realized: BTreeSet<_> = ring.replicas_in_zone(
                first.zone_id, &first.token, 2,
                snap.zones.get(first.zone_id).unwrap().scope.as_ref()
            ).into_iter().collect();
            assert_eq!(realized, first.targets.iter().map(|p| p.0).collect());
        }
    }

    #[test]
    fn balance_planner_is_repeatable_across_catalog_and_map_insertion_order() {
        let (mut snap, ring, mut endpoints, targets, mut agg) = balance_fixture(
            4, &[1, 2, 3, 4], &[1, 1, 1, 1], None
        );
        let mut extra = RangeMap::single();
        extra.enable_opaque_tokens();
        snap.zones.insert(8, ZoneConfig {
            name: "extra".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        });
        snap.range_maps.insert(8, extra.clone());
        for endpoint in &mut endpoints {
            let identity = endpoint.ranges[0].clone();
            endpoint.ranges.push(balance_stat(
                8, Vec::new(), 0, identity.shard_index, identity.shard_count
            ));
        }
        agg.insert((8, Vec::new()), (0, None, 0, 0));
        let first = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true)
            .plan.expect("ordered fixture produced no plan");

        let default_cfg = snap.zones.get(DEFAULT_ZONE).unwrap().clone();
        let main_cfg = snap.zones.get(7).unwrap().clone();
        let extra_cfg = snap.zones.get(8).unwrap().clone();
        let main_map = snap.range_maps.get(&7).unwrap().clone();
        let mut reordered = snap.clone();
        reordered.zones = Catalog::new();
        reordered.zones.insert(8, extra_cfg);
        reordered.zones.insert(7, main_cfg);
        reordered.zones.insert(DEFAULT_ZONE, default_cfg);
        reordered.range_maps = HashMap::new();
        reordered.range_maps.insert(8, extra);
        reordered.range_maps.insert(7, main_map);
        endpoints.reverse();
        let second = balance_dry_run(&reordered, &endpoints, &targets, &agg, &ring, true)
            .plan.expect("map insertion order changed feasibility");
        assert_eq!(first.token, second.token);
        assert_eq!(first.targets, second.targets);
        assert_eq!(first.zone_id, second.zone_id);
        assert_eq!(first.range_index, second.range_index);
    }

    #[test]
    fn balance_planner_honours_scope_gates_and_noise_thresholds() {
        let scope: BTreeSet<NodeId> = [1, 2, 3].into_iter().collect();
        let (mut snap, ring, endpoints, targets, agg) = balance_fixture(
            4, &[1, 2, 3, 4], &[1, 1, 1, 4], Some(scope.clone())
        );
        assert!(endpoints.iter().filter(|endpoint| endpoint.node == 4)
            .all(|endpoint| endpoint.ranges.iter().all(|report| report.zone_id == DEFAULT_ZONE)));
        let proposed = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true)
            .plan.expect("scoped move not found");
        assert!(proposed.targets.iter().all(|(node, _)| scope.contains(node)));

        snap.balance_high_pct = u64::MAX;
        let quiet = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        assert!(quiet.plan.is_none());
        assert!(matches!(quiet.noop, Some(BalanceNoop::BelowHigh)));

        snap.balance_high_pct = 100;
        snap.balance_min_improvement_pct = u64::MAX;
        let noise = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        assert!(noise.plan.is_none());
        assert!(matches!(noise.noop, Some(BalanceNoop::SplitFirst)));

        snap.balance_min_improvement_pct = 1;
        snap.balance_token_search_limit = 0;
        let constrained = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        assert!(constrained.plan.is_none());
        assert!(matches!(constrained.noop, Some(BalanceNoop::NoFeasible)));
    }

    #[test]
    fn balance_planner_fails_closed_and_search_exhaustion_is_attributed() {
        let (mut snap, ring, endpoints, targets, agg) = balance_fixture(
            5, &[1, 2, 3, 4, 5], &[1, 1, 1, 1, 1], None
        );
        let map_before = snap.range_maps.clone();
        let (_cmd_tx, cmd_rx) = mpsc::channel::<PdCmd>();
        let gated = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, false);
        assert_eq!(snap.range_maps, map_before, "dry-run mutated the committed map");
        assert!(cmd_rx.try_recv().is_err(), "dry-run emitted a PD command");
        assert!(gated.plan.is_none());
        assert_eq!(gated.blocked, 4);
        assert!(matches!(gated.noop, Some(BalanceNoop::Capability)));

        let missing = balance_dry_run(&snap, &[], &targets, &agg, &ring, true);
        assert!(missing.plan.is_none());
        assert!(matches!(missing.noop, Some(BalanceNoop::BlockedInput)));

        let mut inconsistent = endpoints.clone();
        let report = inconsistent.iter_mut()
            .flat_map(|endpoint| endpoint.ranges.iter_mut())
            .find(|report| report.zone_id == 7 && report.entries > 0).unwrap();
        report.entries = report.entries.saturating_sub(1);
        let inconsistent = balance_dry_run(&snap, &inconsistent, &targets, &agg, &ring, true);
        assert!(inconsistent.blocked > 0);
        assert!(inconsistent.plan.is_none());
        assert!(matches!(inconsistent.noop, Some(BalanceNoop::BlockedInput)));

        let mut retained_larger = endpoints.clone();
        let report = retained_larger.iter_mut()
            .flat_map(|endpoint| endpoint.ranges.iter_mut())
            .find(|report| report.zone_id == 7
                && report.lower_bound.is_empty() && report.entries == 0)
            .unwrap();
        report.entries = 901;
        let mut larger_agg = agg.clone();
        larger_agg.get_mut(&(7, Vec::new())).unwrap().0 = 901;
        let retained = balance_dry_run(
            &snap, &retained_larger, &targets, &larger_agg, &ring, true
        );
        assert!(retained.plan.is_none());
        assert!(matches!(retained.noop, Some(BalanceNoop::BlockedInput)));

        let inputs = planner_inputs(&snap, &endpoints, &targets, &agg, &ring).unwrap();
        let observed = node_load_skew_pct(&node_owned_entries(
            &snap, &inputs.agg, &inputs.fixed, &ring
        ));
        let predicted = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true).before_node;
        assert_eq!(predicted, observed);
        assert_eq!(predicted, 269);

        snap.balance_token_search_limit = 0;
        let bounded = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        assert!(bounded.exhausted > 0);
        assert!(bounded.plan.is_none());
        assert!(matches!(bounded.noop, Some(BalanceNoop::NoFeasible)));
    }

    fn objective_plan(engine: LoadRatio, node: LoadRatio) -> BalancePlan {
        BalancePlan {
            zone_id: 7,
            range_index: 0,
            lower: Vec::new(),
            entries: 10,
            copy_entries: 10,
            token: Vec::new(),
            nonce: 1,
            targets: vec![(1, 0), (2, 0)],
            owner_overlap: 1,
            after_node: node,
            after_engine: engine,
            deviation: 1,
        }
    }

    #[test]
    fn balance_objective_low_water_and_fractional_thresholds_are_exact() {
        let current_engine = LoadRatio { num: 13, den: 10 };
        let current_node = LoadRatio { num: 1, den: 1 };
        assert!(load_objective_better(
            LoadRatio { num: 6, den: 5 },
            LoadRatio { num: 11, den: 10 },
            2,
            current_engine,
            current_node,
            1,
        ), "a primary engine improvement must permit a secondary node increase");

        let bounded = objective_plan(
            LoadRatio { num: 9, den: 10 },
            LoadRatio { num: 101, den: 100 },
        );
        let engine_better_but_unbounded = objective_plan(
            LoadRatio { num: 4, den: 5 },
            LoadRatio { num: 106, den: 100 },
        );
        assert!(target_plan_better(&engine_better_but_unbounded, &bounded));
        assert!(plan_is_bounded(&bounded, 105));
        assert!(!plan_is_bounded(&engine_better_but_unbounded, 105));
        let (selected, crossed_low) = select_low_water_plan(
            Some(bounded.clone()), Some(engine_better_but_unbounded.clone())
        ).unwrap();
        assert!(crossed_low);
        assert_eq!(selected.targets, bounded.targets);
        assert_eq!(selected.after_engine, bounded.after_engine);
        let (selected, crossed_low) = select_low_water_plan(
            None, Some(engine_better_but_unbounded.clone())
        ).unwrap();
        assert!(!crossed_low);
        assert_eq!(selected.after_engine, engine_better_but_unbounded.after_engine);
        assert!(plan_is_bounded(&objective_plan(
            LoadRatio { num: 105, den: 100 },
            LoadRatio { num: 105, den: 100 },
        ), 105));
        assert!(!plan_is_bounded(&objective_plan(
            LoadRatio { num: 10501, den: 10000 },
            LoadRatio { num: 1, den: 1 },
        ), 105));

        let before = LoadRatio { num: 12001, den: 10000 };
        assert!(before.improves_by_pct(
            LoadRatio { num: 11501, den: 10000 }, 5
        ));
        assert!(!before.improves_by_pct(
            LoadRatio { num: 11502, den: 10000 }, 5
        ));
    }

    #[test]
    fn balance_target_ties_use_overlap_ids_and_nonce_not_copy_volume() {
        let ratio = LoadRatio { num: 1, den: 1 };
        let base = objective_plan(ratio, ratio);
        let mut overlap = base.clone();
        overlap.owner_overlap = 2;
        assert!(target_plan_better(&overlap, &base));

        let lexical = base.clone();
        let mut later_ids = base.clone();
        later_ids.targets = vec![(2, 0), (3, 0)];
        assert!(target_plan_better(&lexical, &later_ids));
        let mut later_shard = base.clone();
        later_shard.targets = vec![(1, 1), (2, 0)];
        assert!(target_plan_better(&lexical, &later_shard));

        let mut nonce = base.clone();
        nonce.nonce = 0;
        assert!(target_plan_better(&nonce, &base));

        let mut copy = base.clone();
        copy.copy_entries = 0;
        assert!(!target_plan_better(&copy, &base));
        assert!(!target_plan_better(&base, &copy));
    }

    #[test]
    fn balance_planner_skips_a_bound_with_only_the_current_shard_realization() {
        let (mut snap, ring, mut endpoints, targets, _) = balance_fixture(
            4, &[2, 3, 2, 1], &[1, 1, 1, 1], None
        );
        let token = crate::partition::opaque_placement_token(opaque_planner_seed(
            snap.member_epoch, 7, b"", 0
        ));
        snap.range_maps.insert(7, RangeMap::from_opaque_parts(
            Vec::new(), vec![token.clone()], vec![None], vec![false]
        ).unwrap());
        let owners = ring.replicas_in_zone(7, &token, 2, None);
        for endpoint in &mut endpoints {
            endpoint.ranges.retain(|report| {
                report.zone_id != 7 || report.lower_bound.is_empty()
            });
            let report = endpoint.ranges.iter_mut()
                .find(|report| report.zone_id == 7).unwrap();
            let shard = (crate::ring::hash64(&token) % report.shard_count as u64) as u32;
            report.entries = if owners.contains(&endpoint.node) && report.shard_index == shard {
                1_500
            } else {
                0
            };
        }
        snap.balance_high_pct = 100;
        snap.balance_token_search_limit = 1;
        let agg = HashMap::from([((7, Vec::new()), (1_500, None, 0, 0))]);
        let outcome = balance_dry_run(&snap, &endpoints, &targets, &agg, &ring, true);
        assert!(outcome.plan.is_none());
        assert!(outcome.exhausted > 0);
        assert!(matches!(outcome.noop, Some(BalanceNoop::NoFeasible)));
    }

    #[test]
    fn balance_planner_blocks_partial_ranges_shards_and_mixed_epochs() {
        let (snap, ring, endpoints, targets, agg) = balance_fixture(
            4, &[2, 3, 2, 1], &[1, 1, 1, 1], None
        );
        let exact: Vec<_> = endpoints.iter().filter_map(|endpoint| {
            exact_planner_endpoint(
                endpoint.node,
                endpoint.addr,
                snap.range_map_epoch,
                snap.range_map_epoch,
                true,
                snap.range_map_epoch,
                &endpoint.ranges,
            )
        }).collect();
        assert!(planner_inputs(&snap, &exact, &targets, &agg, &ring).is_some());
        assert!(exact_planner_endpoint(
            endpoints[0].node,
            endpoints[0].addr,
            snap.range_map_epoch,
            snap.range_map_epoch,
            false,
            snap.range_map_epoch,
            &endpoints[0].ranges,
        ).is_none());
        assert!(exact_planner_endpoint(
            endpoints[0].node,
            endpoints[0].addr,
            snap.range_map_epoch,
            snap.range_map_epoch - 1,
            true,
            snap.range_map_epoch,
            &endpoints[0].ranges,
        ).is_none());
        let mut missing_shard = endpoints.clone();
        let missing = missing_shard.iter().position(|endpoint| {
            endpoint.node == 2 && endpoint.ranges[0].shard_index == 1
        }).unwrap();
        missing_shard.remove(missing);
        let outcome = balance_dry_run(&snap, &missing_shard, &targets, &agg, &ring, true);
        assert!(outcome.plan.is_none());
        assert!(matches!(outcome.noop, Some(BalanceNoop::BlockedInput)));

        let mut missing_range = endpoints.clone();
        for endpoint in &mut missing_range {
            endpoint.ranges.retain(|report| {
                report.zone_id != 7 || report.lower_bound != b"b"
            });
        }
        let outcome = balance_dry_run(&snap, &missing_range, &targets, &agg, &ring, true);
        assert!(outcome.plan.is_none());
        assert!(matches!(outcome.noop, Some(BalanceNoop::BlockedInput)));

        let mixed: Vec<PlannerEndpointStats> = endpoints.iter().enumerate()
            .filter_map(|(index, endpoint)| exact_planner_endpoint(
                endpoint.node,
                endpoint.addr,
                if index % 2 == 0 { snap.range_map_epoch - 1 } else { snap.range_map_epoch + 1 },
                snap.range_map_epoch,
                true,
                snap.range_map_epoch,
                &endpoint.ranges,
            )).collect();
        let outcome = balance_dry_run(&snap, &mixed, &targets, &agg, &ring, true);
        assert!(outcome.plan.is_none());
        assert!(matches!(outcome.noop, Some(BalanceNoop::BlockedInput)));
    }

    #[test]
    fn node_load_metric_is_exported() {
        let metric = crate::observe::gauges_named()
            .into_iter()
            .find(|(name, _, _)| *name == "pd_node_load_skew_pct")
            .expect("pd node load skew gauge missing");
        assert!(metric.1.contains("no members or no reported load"));
    }

    fn serve_range_stats_v2(
        epoch: u64,
        ranges: Vec<wire::OwnedRangeStat>,
        polls: usize,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for _ in 0..2 * polls {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                if crate::proto::server_handshake_blocking(&mut stream).is_err() {
                    continue;
                }
                let mut lenb = [0u8; 4];
                if stream.read_exact(&mut lenb).is_err() {
                    continue;
                }
                let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                if stream.read_exact(&mut block).is_err() || block.len() < 8 {
                    continue;
                }
                let (reqid, payload) = block.split_at(8);
                if wire::decode_request(payload) != Ok(wire::OwnedRequest::RangeStatsV2) {
                    continue;
                }
                let response = Response::RangeStatsV2 { epoch, ranges: ranges.clone() };
                let payload = wire::encode_response_payload(&response);
                let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
                frame.extend_from_slice(reqid);
                frame.extend_from_slice(&payload);
                let _ = stream.write_all(&frame);
            }
        });
        (addr, server)
    }

    static DEBT_DEFERRAL_COUNTER: Mutex<()> = Mutex::new(());

    #[test]
    fn debt_rehome_defers_balanced_load_and_prefers_the_hot_engine() {
        let _counter = DEBT_DEFERRAL_COUNTER.lock().unwrap_or_else(|e| e.into_inner());
        let zone_id = 7;
        let epoch = 4;
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        assert!(map.split(b"m"));
        let base_shard = (crate::ring::hash64(map.token_of(0)) % 2) as u32;
        let other_token = (0u8..=255).map(|n| crate::partition::opaque_placement_token([n; 16]))
            .find(|token| crate::ring::hash64(token) % 2 != base_shard as u64)
            .unwrap();
        assert!(map.set_pending(1, other_token.clone()));
        assert!(map.commit_pending(1));
        assert!(map.split(b"g"));
        assert!(map.split(b"t"));
        assert!(map.placement_debt_of(map.range_index(b"g")));
        assert!(map.placement_debt_of(map.range_index(b"t")));

        let mut snap = PushState::new();
        snap.members = vec![(1, 1, Vec::new())];
        snap.zones.insert(zone_id, ZoneConfig {
            name: "debt-skew".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
        });
        snap.range_map_epoch = epoch;
        snap.range_maps.insert(zone_id, map);
        snap.auto_split_threshold = 0;
        snap.auto_merge_threshold = 0;
        snap.auto_load_split_ops = 0;
        snap.rehome_enabled = true;
        let stat = |lower: &[u8], entries| -> Vec<_> { (0..2).map(|shard_index| wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower.to_vec(),
            entries,
            midpoint: None,
            ops_per_sec: 0,
            shard_index,
            shard_count: 2,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        }).collect() };
        let poll = |snap: &PushState, stats: Vec<Vec<_>>| {
            let stats = stats.into_iter().flatten().collect();
            let (addr, server) = serve_range_stats_v2(epoch, stats, 2);
            let (tx, rx) = mpsc::channel();
            let before = crate::observe::counters().pd_rehome_debt_deferred_balanced.get();
            range_stats_poll(
                &[(1, vec![addr, addr])], snap, &tx, &mut HashMap::new(),
                &HashMap::new(), true, false, RingConfig { vnodes: 64, rf: 1 },
                &mut HashMap::new(), &mut HashMap::new(),
                &mut BalanceExecutorState::default(),
                &Mutex::new(BalancePollAuthority::default()),
                &mut RecoveryHold::default(),
            );
            server.join().unwrap();
            (rx.try_iter().collect::<Vec<_>>(),
                crate::observe::counters().pd_rehome_debt_deferred_balanced.get() - before)
        };
        let (balanced, deferred) = poll(&snap, vec![
            stat(b"", 10), stat(b"g", 10), stat(b"m", 10), stat(b"t", 10),
        ]);
        assert!(balanced.is_empty(), "balanced inherited ranges must not move");
        assert_eq!(deferred, 1);

        let (skewed, deferred) = poll(&snap, vec![
            stat(b"", 10), stat(b"g", 20), stat(b"m", 60), stat(b"t", 5),
        ]);
        assert_eq!(deferred, 0);
        assert!(matches!(skewed.as_slice(), [PdCmd::PrepareRehome(id, lower)]
            if *id == zone_id && lower == b"t"),
            "the smaller debt range on the hot engine must win");

        let (unmeasured, deferred) = poll(&snap, vec![
            stat(b"", 10), stat(b"g", 20), stat(b"m", 60),
        ]);
        assert!(unmeasured.is_empty());
        assert_eq!(deferred, 1);

        let mut no_hot_debt = RangeMap::single();
        no_hot_debt.enable_opaque_tokens();
        assert!(no_hot_debt.split(b"m"));
        assert!(no_hot_debt.set_pending(1, other_token));
        assert!(no_hot_debt.commit_pending(1));
        assert!(no_hot_debt.split(b"t"));
        assert!(!no_hot_debt.placement_debt_of(0));
        assert!(!no_hot_debt.placement_debt_of(1));
        assert!(no_hot_debt.placement_debt_of(2));
        let mut no_hot_snap = snap.clone();
        no_hot_snap.range_maps.insert(zone_id, no_hot_debt);
        let (cold_debt, deferred) = poll(&no_hot_snap, vec![
            stat(b"", 100), stat(b"m", 10), stat(b"t", 10),
        ]);
        assert!(cold_debt.is_empty(), "debt off the hot engine must not move");
        assert_eq!(deferred, 1);
    }

    #[test]
    fn debt_rehome_load_counts_only_exact_owner_reports() {
        let _counter = DEBT_DEFERRAL_COUNTER.lock().unwrap_or_else(|e| e.into_inner());
        let zone_id = 7;
        let epoch = 4;
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        assert!(map.split(b"m"));
        let base_shard = (crate::ring::hash64(map.token_of(0)) % 2) as u32;
        let other_shard = 1 - base_shard;
        let other_token = (0u8..=255).map(|n| crate::partition::opaque_placement_token([n; 16]))
            .find(|token| crate::ring::hash64(token) % 2 == other_shard as u64)
            .unwrap();
        assert!(map.set_pending(1, other_token));
        assert!(map.commit_pending(1));
        assert!(map.split(b"g"));
        assert!(map.split(b"t"));
        assert!(map.placement_debt_of(map.range_index(b"g")));
        assert!(map.placement_debt_of(map.range_index(b"t")));

        let mut snap = PushState::new();
        snap.members = vec![(1, 1, Vec::new())];
        snap.zones.insert(zone_id, ZoneConfig {
            name: "debt-owner-load".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
        });
        snap.range_map_epoch = epoch;
        snap.range_maps.insert(zone_id, map);
        snap.auto_split_threshold = 0;
        snap.auto_merge_threshold = 0;
        snap.auto_load_split_ops = 0;
        snap.rehome_enabled = true;
        let stat = |lower: &[u8], entries, shard_index| wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower.to_vec(),
            entries,
            midpoint: None,
            ops_per_sec: 0,
            shard_index,
            shard_count: 2,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        };
        let poll = |base: Vec<(&str, u64)>, other: Vec<(&str, u64)>, other_ready: bool| {
            let mut addrs = [None, None];
            let mut servers = Vec::new();
            for (shard, counts, ready) in [(base_shard, base, true), (other_shard, other, other_ready)] {
                let stats = counts.into_iter().map(|(lower, n)| stat(lower.as_bytes(), n, shard)).collect();
                let (addr, server) = serve_range_stats_v3(epoch, stats, Some(false), Vec::new(), 1, ready);
                addrs[shard as usize] = Some(addr);
                servers.push(server);
            }
            let (tx, rx) = mpsc::channel();
            let before = crate::observe::counters().pd_rehome_debt_deferred_balanced.get();
            range_stats_poll(
                &[(1, addrs.iter().map(|addr| addr.unwrap()).collect())], &snap, &tx,
                &mut HashMap::new(), &HashMap::new(), true, false, RingConfig { vnodes: 64, rf: 1 },
                &mut HashMap::new(), &mut HashMap::new(),
                &mut BalanceExecutorState::default(),
                &Mutex::new(BalancePollAuthority::default()),
                &mut RecoveryHold::default(),
            );
            for server in servers {
                server.join().unwrap();
            }
            let starts = rx.try_iter()
                .filter_map(|cmd| match cmd {
                    PdCmd::PrepareRehome(id, lower) if id == zone_id => Some(lower),
                    _ => None,
                })
                .collect::<Vec<_>>();
            (starts, crate::observe::counters().pd_rehome_debt_deferred_balanced.get() - before)
        };
        let base_live = || vec![("", 10), ("g", 10)];

        let (starts, deferred) = poll(base_live(), vec![("m", 60), ("t", 10)], true);
        assert_eq!(starts, vec![b"t".to_vec()], "a measured owner imbalance still moves hot debt");
        assert_eq!(deferred, 0);

        let (starts, deferred) = poll(base_live(), vec![("m", 60), ("t", 10)], false);
        assert!(starts.is_empty(), "a not-ready physical estimate must not authorize a move");
        assert_eq!(deferred, 1);

        let mut with_stray = base_live();
        with_stray.push(("m", 60));
        let (starts, deferred) = poll(with_stray, vec![("m", 10), ("t", 10)], true);
        assert!(starts.is_empty(), "a former owner's copy is not load on the current owner");
        assert_eq!(deferred, 1);
    }

    fn serve_range_stats_v3(
        epoch: u64,
        ranges: Vec<wire::OwnedRangeStat>,
        recovery_seeding: Option<bool>,
        covered_empty: Vec<u32>,
        endpoints: usize,
        accounting_ready: bool,
    ) -> (SocketAddr, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for _ in 0..endpoints {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                if crate::proto::server_handshake_blocking(&mut stream).is_err() {
                    continue;
                }
                let mut lenb = [0u8; 4];
                if stream.read_exact(&mut lenb).is_err() {
                    continue;
                }
                let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                if stream.read_exact(&mut block).is_err() || block.len() < 8 {
                    continue;
                }
                let (reqid, payload) = block.split_at(8);
                assert_eq!(wire::decode_request(payload), Ok(wire::OwnedRequest::RangeStatsV3));
                let response = Response::RangeStatsV3 {
                    epoch,
                    accounting_epoch: epoch,
                    accounting_ready,
                    ranges: ranges.clone(),
                    recovery_seeding,
                    covered_empty: covered_empty.clone(),
                };
                let payload = wire::encode_response_payload(&response);
                let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
                frame.extend_from_slice(reqid);
                frame.extend_from_slice(&payload);
                let _ = stream.write_all(&frame);
            }
        });
        (addr, server)
    }

    #[test]
    fn covered_empty_neighbours_merge_on_every_owner_report_without_moving_live_placement() {
        let zone_id = 7;
        let epoch = 4;
        let mut map = RangeMap::from_splits([b"b".to_vec(), b"d".to_vec(), b"f".to_vec()]);
        for at in [b"a1".as_slice(), b"a2", b"a3", b"c", b"e"] {
            assert!(map.split(at));
        }
        let lowers: Vec<Vec<u8>> = (0..map.num_ranges())
            .map(|index| map.range_bounds(index).0.unwrap_or_default().to_vec())
            .collect();
        assert_eq!(lowers, [&b""[..], b"a1", b"a2", b"a3", b"b", b"c", b"d", b"e", b"f"]);
        assert_eq!(map.token_of(5), b"b", "c inherits b's placement");
        assert_eq!(map.token_of(7), b"d", "e inherits d's placement");
        let stat = |lower: &[u8], entries, ops_per_sec| wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower.to_vec(),
            entries,
            midpoint: None,
            ops_per_sec,
            shard_index: 0,
            shard_count: 1,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        };
        let owner_shard: Vec<u32> = (0..map.num_ranges())
            .map(|index| (crate::ring::hash64(map.token_of(index)) % 2) as u32)
            .collect();
        let shard_stat = |lower: &[u8], entries, ops_per_sec, shard_index| wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower.to_vec(),
            entries,
            midpoint: None,
            ops_per_sec,
            shard_index,
            shard_count: 2,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        };
        let stats: Vec<wire::OwnedRangeStat> = lowers.iter().map(|lower| match lower.as_slice() {
            b"c" => stat(lower, 5, 0),
            b"d" => stat(lower, 0, 3),
            _ => stat(lower, 0, 0),
        }).collect();
        let all_but_live: Vec<u32> = (0..stats.len() as u32).filter(|&index| index != 5).collect();
        let without_a2: Vec<u32> = all_but_live.iter().copied().filter(|&index| index != 2).collect();
        let node2_shard = |shard: u32, stray_entries: u64| {
            let stats: Vec<wire::OwnedRangeStat> = stats.iter().enumerate().map(|(index, owned)| {
                if owner_shard[index] == shard {
                    shard_stat(&owned.lower_bound, owned.entries, owned.ops_per_sec, shard)
                } else {
                    let mut stat = shard_stat(&owned.lower_bound, 9, 0, shard);
                    if index == 1 {
                        stat.entries = stray_entries;
                        stat.midpoint = Some(b"a1x".to_vec());
                    }
                    stat
                }
            }).collect();
            let covered: Vec<u32> = without_a2.iter().copied()
                .filter(|&index| owner_shard[index as usize] == shard)
                .collect();
            (stats, covered)
        };
        let mut snap = PushState::new();
        snap.member_epoch = 1;
        snap.members = vec![(1, 1, Vec::new()), (2, 1, Vec::new())];
        snap.zone_epoch = 1;
        snap.zones.insert(zone_id, ZoneConfig {
            name: "ring".into(), rf: 2, ordering: Ordering::Sorted, scope: None,
        });
        snap.range_map_epoch = epoch;
        snap.range_maps.insert(zone_id, map);
        snap.single_range_warn_entries = 0;
        let poll = |snap: &PushState, stray_entries: u64| {
            let (a, server_a) = serve_range_stats_v3(epoch, stats.clone(), Some(false), all_but_live.clone(), 1, false);
            let (stats_b0, covered_b0) = node2_shard(0, stray_entries);
            let (stats_b1, covered_b1) = node2_shard(1, stray_entries);
            let (b0, server_b0) = serve_range_stats_v3(epoch, stats_b0, Some(false), covered_b0, 1, false);
            let (b1, server_b1) = serve_range_stats_v3(epoch, stats_b1, Some(false), covered_b1, 1, false);
            let (tx, rx) = mpsc::channel();
            range_stats_poll(
                &[(1, vec![a]), (2, vec![b0, b1])], snap, &tx, &mut HashMap::new(),
                &HashMap::new(), true, false, RingConfig { vnodes: 64, rf: 2 },
                &mut HashMap::new(), &mut HashMap::new(),
                &mut BalanceExecutorState::default(),
                &Mutex::new(BalancePollAuthority::default()),
                &mut RecoveryHold::default(),
            );
            server_a.join().unwrap();
            server_b0.join().unwrap();
            server_b1.join().unwrap();
            rx.try_iter().flat_map(|cmd| match cmd {
                PdCmd::MergeZones(id, boundaries) if id == zone_id => boundaries,
                _ => panic!("only one batched merge of the zone is expected"),
            }).collect::<Vec<_>>()
        };
        let counter = || crate::observe::counters().pd_range_covered_merges.get();
        let before = counter();
        assert_eq!(
            poll(&snap, 9),
            vec![b"a1".to_vec(), b"f".to_vec()],
            "a1 joins its covered-empty left neighbour and f re-points to e's owners; \
             a2 lacks one owner's cover, b still places live c, d takes requests, and the \
             stray copies node 2's non-owner shard still reports do not count"
        );
        assert!(counter() >= before + 2);

        snap.auto_split_threshold = 30;
        assert_eq!(poll(&snap, 9), vec![b"a1".to_vec(), b"f".to_vec()]);
        assert_eq!(
            poll(&snap, 100),
            vec![b"a1".to_vec(), b"f".to_vec()],
            "stray entries above the split threshold neither block covered merges nor \
             size-split a covered-empty range with a valid stray midpoint"
        );
        snap.auto_split_threshold = DEFAULT_RANGE_SPLIT_THRESHOLD;

        snap.auto_splits_per_poll = 1;
        assert_eq!(
            poll(&snap, 9),
            vec![b"a1".to_vec(), b"f".to_vec()],
            "covered merges are one batched map update, not capped by range_splits_per_poll"
        );

        snap.auto_merge_covered = false;
        assert!(poll(&snap, 9).is_empty(), "range_merge_covered=false turns covered merges off");
    }

    struct HoldFixture {
        zone_id: ZoneId,
        epoch: u64,
        debt: RangeMap,
        pending: RangeMap,
        snap: PushState,
    }

    impl HoldFixture {
        fn new() -> Self {
            let zone_id = 7;
            let epoch = 4;
            let mut debt = RangeMap::single();
            debt.enable_opaque_tokens();
            assert!(debt.split(b"m"));
            let base_shard = (crate::ring::hash64(debt.token_of(0)) % 2) as u32;
            let other_token = (0u8..=255).map(|n| crate::partition::opaque_placement_token([n; 16]))
                .find(|token| crate::ring::hash64(token) % 2 != base_shard as u64)
                .unwrap();
            assert!(debt.set_pending(1, other_token.clone()));
            assert!(debt.commit_pending(1));
            assert!(debt.split(b"g"));
            assert!(debt.split(b"t"));
            let mut pending = debt.clone();
            let t = pending.range_index(b"t");
            let fresh = (0u8..=255).map(|n| crate::partition::opaque_placement_token([n ^ 0x5a; 16]))
                .find(|token| token.as_slice() != pending.token_of(t))
                .unwrap();
            assert!(pending.set_pending(t, fresh));
            let mut snap = PushState::new();
            snap.members = vec![(1, 1, Vec::new())];
            snap.zones.insert(zone_id, ZoneConfig {
                name: "recovery-hold".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
            });
            snap.range_map_epoch = epoch;
            snap.auto_split_threshold = 0;
            snap.auto_merge_threshold = 0;
            snap.auto_load_split_ops = 0;
            snap.rehome_enabled = true;
            snap.rehome_recovery_defer_ms = 60_000;
            HoldFixture { zone_id, epoch, debt, pending, snap }
        }

        fn stats(&self) -> Vec<wire::OwnedRangeStat> {
            let stat = |lower: &[u8], entries, midpoint: Option<&[u8]>, shard_index| wire::OwnedRangeStat {
                zone_id: self.zone_id,
                lower_bound: lower.to_vec(),
                entries,
                midpoint: midpoint.map(<[u8]>::to_vec),
                ops_per_sec: 0,
                shard_index,
                shard_count: 2,
                rehome_mirror_epoch: self.epoch,
                rehome_mirror_ok: 1,
                rehome_mirror_failures: 0,
                rehome_copy_complete_epoch: self.epoch,
                rehome_copy_progress: 0,
            };
            (0..2).flat_map(|shard| [
                stat(b"", 10, None, shard), stat(b"g", 20, None, shard),
                stat(b"m", 60, Some(b"p".as_slice()), shard), stat(b"t", 5, None, shard),
            ]).collect()
        }

        fn poll(&self, recovery_seeding: Option<bool>, dead: bool, hold: &mut RecoveryHold) -> (Vec<PdCmd>, bool) {
            let counters = crate::observe::counters();
            let (addr, server) = serve_range_stats_v3(self.epoch, self.stats(), recovery_seeding, Vec::new(), 2, true);
            let mut targets = vec![(1, vec![addr, addr])];
            if dead {
                let gone = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
                targets.push((2, vec![gone]));
            }
            let (tx, rx) = mpsc::channel();
            let before = counters.pd_rehome_starts_deferred_recovery.get();
            range_stats_poll(
                &targets, &self.snap, &tx, &mut HashMap::new(),
                &HashMap::new(), true, false, RingConfig { vnodes: 64, rf: 1 },
                &mut HashMap::new(), &mut HashMap::new(),
                &mut BalanceExecutorState::default(),
                &Mutex::new(BalancePollAuthority::default()),
                hold,
            );
            server.join().unwrap();
            let deferred = counters.pd_rehome_starts_deferred_recovery.get() > before;
            (rx.try_iter().collect::<Vec<_>>(), deferred)
        }

        fn starts(&self, cmds: &[PdCmd]) -> bool {
            cmds.iter().any(|cmd| {
                matches!(cmd, PdCmd::PrepareRehome(id, lower) if *id == self.zone_id && lower == b"t")
            })
        }

        fn commits(&self, cmds: &[PdCmd]) -> bool {
            cmds.iter().any(|cmd| {
                matches!(cmd, PdCmd::CommitRehome(id, lower) if *id == self.zone_id && lower == b"t")
            })
        }

        fn splits(&self, cmds: &[PdCmd]) -> bool {
            cmds.iter().any(|cmd| matches!(cmd, PdCmd::SplitZone(id, at, _) if *id == self.zone_id && at == b"p"))
        }
    }

    fn holding(hold: &RecoveryHold) -> bool {
        matches!(hold.last, Some((_, true)))
    }

    #[test]
    fn recovery_seeding_or_a_down_node_holds_starts_but_never_commits() {
        let mut f = HoldFixture::new();
        f.snap.range_maps.insert(f.zone_id, f.debt.clone());
        let mut hold = RecoveryHold::default();
        let (cmds, deferred) = f.poll(Some(true), false, &mut hold);
        assert!(!f.starts(&cmds), "no new re-home while a node recovery-seeds");
        assert!(deferred && holding(&hold));
        let (cmds, deferred) = f.poll(Some(false), true, &mut hold);
        assert!(!f.starts(&cmds), "no new re-home while a node is down");
        assert!(deferred);
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(f.starts(&cmds), "starts resume once every node serves without a recovery seed");
        let (cmds, _) = f.poll(None, false, &mut hold);
        assert!(f.starts(&cmds), "endpoints without the flag never hold");

        f.snap.range_maps.insert(f.zone_id, f.pending.clone());
        let (cmds, _) = f.poll(Some(true), false, &mut hold);
        assert!(holding(&hold), "the hold is active in this poll");
        assert!(f.commits(&cmds), "a copy-complete commit is never held");
        let (cmds, _) = f.poll(Some(true), true, &mut hold);
        assert!(f.commits(&cmds), "a down node does not hold a commit either");

        f.snap.range_maps.insert(f.zone_id, f.debt.clone());
        f.snap.rehome_recovery_defer_ms = 0;
        let mut knockout = RecoveryHold::default();
        let (cmds, _) = f.poll(Some(true), true, &mut knockout);
        assert!(f.starts(&cmds) && !holding(&knockout), "knockout: rehome_recovery_defer_ms=0 never holds");
    }

    #[test]
    fn placement_change_reseeds_never_hold_commits_starts_or_splits() {
        let mut f = HoldFixture::new();
        let mut hold = RecoveryHold::default();
        f.snap.range_maps.insert(f.zone_id, f.pending.clone());
        f.snap.auto_split_threshold = 50;
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(f.commits(&cmds), "the re-home's own placement reseed does not hold its commit");
        assert!(!f.splits(&cmds), "splits wait only while the re-home is pending");
        assert!(!holding(&hold));

        f.snap.range_maps.insert(f.zone_id, f.debt.clone());
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(f.splits(&cmds), "splits proceed after the commit");
        f.snap.auto_split_threshold = 0;
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(f.starts(&cmds) && !holding(&hold), "the next start is not held by placement reseeds");

        let (cmds, deferred) = f.poll(Some(true), false, &mut RecoveryHold::default());
        assert!(!f.starts(&cmds) && deferred, "knockout: reporting every reseed (round 2) holds the next start");
    }

    #[test]
    fn a_beacon_detected_restart_holds_starts_until_its_recovery_seed_is_seen_to_finish() {
        let mut f = HoldFixture::new();
        f.snap.range_maps.insert(f.zone_id, f.debt.clone());
        let (cmds, _) = f.poll(Some(false), false, &mut RecoveryHold::default());
        assert!(f.starts(&cmds), "knockout: without the beacon's restart note the gap poll is clean and a start goes out");

        let mut hold = RecoveryHold::default();
        hold.note_restart(1, Instant::now());
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(!f.starts(&cmds) && holding(&hold), "a restarted node that reports no recovery seed yet holds starts");
        let (cmds, _) = f.poll(Some(true), false, &mut hold);
        assert!(!f.starts(&cmds) && holding(&hold));
        let (cmds, _) = f.poll(Some(false), false, &mut hold);
        assert!(f.starts(&cmds) && !holding(&hold), "starts resume once the seen recovery seed has finished");
        assert!(hold.restarts.is_empty());

        let mut silent = RecoveryHold::default();
        silent.note_restart(1, Instant::now().checked_sub(RESTART_RECOVERY_GRACE).unwrap());
        let (cmds, _) = f.poll(None, false, &mut silent);
        assert!(f.starts(&cmds) && !holding(&silent), "a restart that never reports a recovery seed stops holding after the grace");
        assert!(silent.restarts.is_empty());
    }

    #[test]
    fn restarting_nodes_gauge_attributes_a_hold_with_no_down_or_seeding_node() {
        if crate::testutil::isolated_env_test(&[]) {
            return;
        }
        let g = crate::observe::gauges();
        let state = || (
            g.pd_recovery_down_nodes.get(),
            g.pd_recovery_seeding_nodes.get(),
            g.pd_recovery_restarting_nodes.get(),
            g.pd_rehome_recovery_hold.get(),
        );
        let f = HoldFixture::new();
        let mut hold = RecoveryHold::default();
        hold.note_restart(1, Instant::now());
        f.poll(Some(false), false, &mut hold);
        assert_eq!(state(), (0, 0, 1, 1), "the hold is attributed to the restart watch");
        f.poll(Some(true), false, &mut hold);
        assert_eq!(state(), (0, 1, 0, 1), "a seeding node counts as seeding, not restarting");
        f.poll(Some(false), false, &mut hold);
        assert_eq!(state().2, 0, "the seen seed finished: no restart watch left");
    }

    #[test]
    fn restart_watch_refreshes_while_down_and_ends_after_its_grace() {
        let t0 = Instant::now();
        let grace = Duration::from_secs(30);
        let at = |s: u64| t0 + Duration::from_secs(s);
        let none = BTreeSet::new();
        let one = BTreeSet::from([1]);
        let mut hold = RecoveryHold::default();
        hold.note_restart(1, t0);
        assert_eq!(hold.restarting(&none, &none, grace, at(5)), one);
        assert!(hold.restarting(&one, &none, grace, at(40)).is_empty(), "a down node is counted as down");
        assert_eq!(hold.restarting(&none, &none, grace, at(60)), one, "a down poll restarts the grace");
        assert!(hold.restarting(&none, &none, grace, at(70)).is_empty());
        assert!(hold.restarts.is_empty());

        hold.note_restart(1, at(100));
        hold.note_restart(2, at(100));
        assert!(hold.restarting(&none, &one, grace, at(101)) == BTreeSet::from([2]), "a seeding node is counted as seeding");
        hold.note_restart(1, at(102));
        assert_eq!(hold.restarting(&none, &none, grace, at(103)), BTreeSet::from([1, 2]), "a new restart forgets the seen seed");
        hold.reset();
        assert!(hold.restarts.is_empty(), "leadership loss drops restart watches");
    }

    #[test]
    fn recovery_hold_bound_is_cumulative_across_flapping_episodes() {
        let bound = Duration::from_secs(60);
        let poll = Duration::from_secs(5);
        let t0 = Instant::now();
        let mut hold = RecoveryHold::default();
        let mut held_polls = 0;
        for i in 0..120u32 {
            let recovering = i % 2 == 0;
            if hold.holds(recovering, bound, t0 + poll * i) {
                held_polls += 1;
            }
        }
        assert!(held_polls <= 13, "flapping cannot extend the hold past the bound: {held_polls} polls");
        assert!(held_polls >= 6, "the hold applies while recovery flaps: {held_polls} polls");
        assert!(hold.episode, "single clean polls do not end the episode");
        let quiet = t0 + poll * 120;
        assert!(!hold.holds(false, bound, quiet));
        assert!(!hold.holds(false, bound, quiet + RECOVERY_CLEAN_INTERVAL / 2));
        assert!(!hold.holds(true, bound, quiet + RECOVERY_CLEAN_INTERVAL / 2 + poll), "exhausted episode");
        let later = quiet + RECOVERY_CLEAN_INTERVAL * 2;
        assert!(!hold.holds(false, bound, later));
        assert!(!hold.holds(false, bound, later + RECOVERY_CLEAN_INTERVAL));
        assert!(!hold.episode, "a sustained clean interval ends the episode");
        assert!(hold.holds(true, bound, later + RECOVERY_CLEAN_INTERVAL + poll), "a new episode holds again");

        let mut steady = RecoveryHold::default();
        let held = (0..40u32).filter(|i| steady.holds(true, bound, t0 + poll * *i)).count();
        assert_eq!(held, 12, "continuous recovery holds for exactly the bound");
        assert!(!steady.holds(true, Duration::ZERO, t0 + poll * 41), "a zero bound disables the hold");
        assert!(!steady.episode);
    }

    #[test]
    fn leadership_loss_resets_the_recovery_hold() {
        let bound = Duration::from_secs(60);
        let t0 = Instant::now();
        let mut hold = RecoveryHold::default();
        for i in 0..20u32 {
            hold.holds(true, bound, t0 + Duration::from_secs(5) * i);
        }
        assert!(!hold.holds(true, bound, t0 + Duration::from_secs(100)), "this leader exhausted the episode");
        let mut kept = RecoveryHold { last: hold.last, held: hold.held, episode: hold.episode, clean_since: None, restarts: HashMap::new() };
        hold.reset();
        assert!(hold.holds(true, bound, t0 + Duration::from_secs(400)), "a new leadership term starts a fresh episode");
        assert!(!kept.holds(true, bound, t0 + Duration::from_secs(400)), "knockout: without the reset the old term's budget carries over");
    }

    #[test]
    fn range_stats_poll_splits_every_oversized_range_up_to_the_per_poll_cap() {
        let zone_id = 7;
        let epoch = 4;
        let stat = |lower_bound: &[u8], midpoint: &[u8], entries: u64| wire::OwnedRangeStat {
            zone_id,
            lower_bound: lower_bound.to_vec(),
            entries,
            midpoint: Some(midpoint.to_vec()),
            ops_per_sec: 0,
            shard_index: 0,
            shard_count: 1,
            rehome_mirror_epoch: 0,
            rehome_mirror_ok: 0,
            rehome_mirror_failures: 0,
            rehome_copy_complete_epoch: 0,
            rehome_copy_progress: 0,
        };
        let ranges = vec![
            stat(b"", b"c", 5),
            stat(b"f", b"h", 30),
            stat(b"m", b"p", 20),
            stat(b"t", b"w", 40),
        ];
        let (addr, server) = serve_range_stats_v2(epoch, ranges, 2);
        let pd = PdDaemon::spawn_single_with_settings(
            1,
            engine(),
            RingConfig { rf: 1, ..Default::default() },
            vec![],
            PdPlacementSettings {
                range_split_threshold: 10,
                range_splits_per_poll: 2,
                range_merge_threshold: 100,
                single_range_warn_entries: 0,
                ..Default::default()
            },
        ).unwrap();
        let mut snap = pd.push_state.lock().unwrap().clone();
        pd.shutdown();
        snap.member_epoch = 1;
        snap.members = vec![(1, 1, vec![addr])];
        snap.zone_epoch = 1;
        snap.zones.insert(
            zone_id,
            ZoneConfig {
                name: "multi-split".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        snap.range_map_epoch = epoch;
        snap.range_maps.insert(
            zone_id,
            RangeMap::from_splits([b"f".to_vec(), b"m".to_vec(), b"t".to_vec()]),
        );
        let targets = vec![(1, vec![addr])];
        let poll = |snap: &PushState| {
            let (cmd_tx, cmd_rx) = mpsc::channel();
            let balance_poll_authority = Mutex::new(BalancePollAuthority::default());
            range_stats_poll(
                &targets,
                snap,
                &cmd_tx,
                &mut HashMap::new(),
                &HashMap::new(),
                true,
                false,
                RingConfig { vnodes: 64, rf: 1 },
                &mut HashMap::new(),
                &mut HashMap::new(),
                &mut BalanceExecutorState::default(),
                &balance_poll_authority,
                &mut RecoveryHold::default(),
            );
            cmd_rx.try_iter().collect::<Vec<_>>()
        };

        let capped = poll(&snap);
        let split_at = |cmds: &[PdCmd]| -> Vec<Vec<u8>> {
            cmds.iter()
                .map(|cmd| match cmd {
                    PdCmd::SplitZone(id, at, SplitMode::Inherit) if *id == zone_id => at.clone(),
                    _ => panic!("only inherit splits of the zone are expected"),
                })
                .collect()
        };
        assert_eq!(split_at(&capped), vec![b"w".to_vec(), b"h".to_vec()],
            "the cap keeps the largest oversized ranges, largest first");

        snap.auto_splits_per_poll = DEFAULT_RANGE_SPLITS_PER_POLL;
        let all = poll(&snap);
        assert_eq!(split_at(&all), vec![b"w".to_vec(), b"h".to_vec(), b"p".to_vec()],
            "every oversized range splits in one poll; no merge or rehome in a splitting zone");
        server.join().unwrap();
    }

    #[test]
    fn range_stats_poll_merges_only_below_the_load_hysteresis_ceiling() {
        let zone_id = 7;
        let epoch = 4;
        let stats = Arc::new(Mutex::new(Vec::<wire::OwnedRangeStat>::new()));
        let served_loads = Arc::new(Mutex::new(Vec::<Vec<u64>>::new()));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = {
            let stats = stats.clone();
            let served_loads = served_loads.clone();
            thread::spawn(move || {
                for _ in 0..4 {
                    let Ok((mut stream, _)) = listener.accept() else {
                        break;
                    };
                    if crate::proto::server_handshake_blocking(&mut stream).is_err() {
                        continue;
                    }
                    let mut lenb = [0u8; 4];
                    if stream.read_exact(&mut lenb).is_err() {
                        continue;
                    }
                    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if stream.read_exact(&mut block).is_err() || block.len() < 8 {
                        continue;
                    }
                    let (reqid, payload) = block.split_at(8);
                    if wire::decode_request(payload) != Ok(wire::OwnedRequest::RangeStatsV2) {
                        continue;
                    }
                    let ranges = stats.lock().unwrap().clone();
                    served_loads
                        .lock()
                        .unwrap()
                        .push(ranges.iter().map(|stat| stat.ops_per_sec).collect());
                    let response = Response::RangeStatsV2 { epoch, ranges };
                    let payload = wire::encode_response_payload(&response);
                    let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
                    frame.extend_from_slice(reqid);
                    frame.extend_from_slice(&payload);
                    let _ = stream.write_all(&frame);
                }
            })
        };

        let stat = |lower_bound: &[u8], midpoint: &[u8], ops_per_sec: u64| {
            wire::OwnedRangeStat {
                zone_id,
                lower_bound: lower_bound.to_vec(),
                entries: 10,
                midpoint: Some(midpoint.to_vec()),
                ops_per_sec,
                shard_index: 0,
                shard_count: 1,
                rehome_mirror_epoch: 0,
                rehome_mirror_ok: 0,
                rehome_mirror_failures: 0,
                rehome_copy_complete_epoch: 0,
                rehome_copy_progress: 0,
            }
        };
        *stats.lock().unwrap() = vec![stat(b"", b"g", 25), stat(b"m", b"t", 25)];

        let mut snap = PushState::new();
        snap.member_epoch = 1;
        snap.members = vec![(1, 1, vec![addr])];
        snap.zone_epoch = 1;
        snap.zones.insert(
            zone_id,
            ZoneConfig {
                name: "load-hysteresis".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        snap.range_map_epoch = epoch;
        snap.range_maps
            .insert(zone_id, RangeMap::from_splits([b"m".to_vec()]));
        snap.auto_split_threshold = 0;
        snap.auto_load_split_ops = 100;
        snap.auto_merge_threshold = 100;
        snap.single_range_warn_entries = 0;

        let targets = vec![(1, vec![addr])];
        let (cmd_tx, cmd_rx) = mpsc::channel();
        let mut last_warn = HashMap::new();
        let mut rehome_reports = HashMap::new();
        let mut rehome_backoff = HashMap::new();
        let mut balance_executor = BalanceExecutorState::default();
        let balance_poll_authority = Mutex::new(BalancePollAuthority::default());
        let entry_poll = range_stats_poll(
            &targets,
            &snap,
            &cmd_tx,
            &mut last_warn,
            &HashMap::new(),
            true,
            false,
            RingConfig { vnodes: 64, rf: 1 },
            &mut rehome_reports,
            &mut rehome_backoff,
            &mut balance_executor,
            &balance_poll_authority,
            &mut RecoveryHold::default(),
        );
        assert_eq!(entry_poll.ranges.get(&(zone_id, Vec::new())), Some(&10));
        assert_eq!(entry_poll.ranges.get(&(zone_id, b"m".to_vec())), Some(&10));
        assert!(cmd_rx.try_recv().is_err(), "a pair at half load merged");
        let first_generation = balance_poll_authority.lock().unwrap().generation;
        balance_executor.active_seen = true;

        *stats.lock().unwrap() = vec![stat(b"", b"g", 20), stat(b"m", b"t", 20)];
        let second_entry_poll = range_stats_poll(
            &targets,
            &snap,
            &cmd_tx,
            &mut last_warn,
            &HashMap::new(),
            true,
            false,
            RingConfig { vnodes: 64, rf: 1 },
            &mut rehome_reports,
            &mut rehome_backoff,
            &mut balance_executor,
            &balance_poll_authority,
            &mut RecoveryHold::default(),
        );
        assert_eq!(second_entry_poll.ranges.get(&(zone_id, Vec::new())), Some(&10));
        assert_eq!(second_entry_poll.ranges.get(&(zone_id, b"m".to_vec())), Some(&10));
        assert!(matches!(
            cmd_rx.try_recv(),
            Ok(PdCmd::MergeZone(id, boundary)) if id == zone_id && boundary == b"m"
        ));
        assert!(
            balance_poll_authority.lock().unwrap().generation > first_generation,
            "production range_stats_poll must advance authority while an executor operation is unresolved"
        );
        server.join().unwrap();
        assert_eq!(
            *served_loads.lock().unwrap(),
            vec![vec![25, 25], vec![20, 20]],
            "the second poll's 20 ops/s reports must drive the merge hysteresis"
        );
    }

    #[test]
    fn range_metrics_warn_on_concentrated_multi_range_zone() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        let zone_id = 7;
        let mut snap = PushState::new();
        snap.single_range_warn_entries = 50;
        snap.range_concentration_warn_pct = 80;
        snap.zones.insert(
            zone_id,
            ZoneConfig {
                name: "prefixed".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let map = RangeMap::from_splits([b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        snap.range_maps.insert(zone_id, map.clone());
        let mut agg = HashMap::new();
        agg.insert((zone_id, b"b".to_vec()), (100, None, 0, 0));
        let mut last_warn = HashMap::new();
        let before = crate::observe::snapshot().pd_range_concentration_warnings;
        let local_hash_entries = HashMap::new();
        let ring = Ring::from_members(1, 64, [1]);

        publish_range_metrics(
            &snap,
            &agg,
            &local_hash_entries,
            &ring,
            &mut last_warn,
        );

        assert_eq!(crate::observe::gauges().pd_concentrated_range_zones.get(), 1);
        assert_eq!(
            crate::observe::snapshot().pd_range_concentration_warnings,
            before + 1
        );
        assert_eq!(snap.range_maps[&zone_id], map);
        assert!(last_warn.contains_key(&zone_id));
        publish_range_metrics(
            &snap,
            &agg,
            &local_hash_entries,
            &ring,
            &mut last_warn,
        );
        assert_eq!(
            crate::observe::snapshot().pd_range_concentration_warnings,
            before + 1,
            "warning was not throttled"
        );
    }

    #[test]
    fn failed_pushes_back_off_but_a_superseding_epoch_is_immediate() {
        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let targets = vec![(1, vec![addr])];
        let mut confirmed = HashMap::new();
        let mut retries = HashMap::new();
        let mut attempts = 0;

        for _ in 0..100 {
            push_confirmed(
                "test-backoff",
                &targets,
                7,
                &mut confirmed,
                &mut retries,
                |_| {
                    attempts += 1;
                    Err(io::Error::from(io::ErrorKind::ConnectionRefused))
                },
            );
        }
        assert_eq!(attempts, 1, "failed target hot-looped without backoff");
        assert_eq!(retries[&addr].failures, 1);

        push_confirmed(
            "test-backoff",
            &targets,
            8,
            &mut confirmed,
            &mut retries,
            |_| {
                attempts += 1;
                Ok(8)
            },
        );
        assert_eq!(attempts, 2);
        assert_eq!(confirmed.get(&addr), Some(&8));
        assert!(!retries.contains_key(&addr));
    }

    #[test]
    fn failed_push_retries_after_backoff_until_confirmed() {
        let addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let targets = vec![(1, vec![addr])];
        let mut confirmed = HashMap::new();
        let mut retries = HashMap::new();
        let mut attempts = 0;

        push_confirmed(
            "test-eventual",
            &targets,
            11,
            &mut confirmed,
            &mut retries,
            |_| {
                attempts += 1;
                Err(io::Error::from(io::ErrorKind::ConnectionRefused))
            },
        );
        thread::sleep(PUSH_RETRY_MIN + Duration::from_millis(10));
        push_confirmed(
            "test-eventual",
            &targets,
            11,
            &mut confirmed,
            &mut retries,
            |_| {
                attempts += 1;
                Ok(11)
            },
        );
        assert_eq!(attempts, 2);
        assert_eq!(confirmed.get(&addr), Some(&11));
    }

    #[test]
    fn pd_retries_membership_push_until_a_down_node_acks() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let up = Arc::new(AtomicBool::new(false));
        #[allow(clippy::type_complexity)]
        let recorded: Arc<Mutex<Option<(u64, Vec<NodeId>)>>> = Arc::new(Mutex::new(None));
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        {
            let up = up.clone();
            let recorded = recorded.clone();
            thread::spawn(move || {
                for s in l.incoming() {
                    let mut s = match s {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    if !up.load(Ordering::SeqCst) {
                        drop(s);
                        continue;
                    }
                    if crate::proto::server_handshake_blocking(&mut s).is_err() {
                        continue;
                    }
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        continue;
                    }
                    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut block).is_err() {
                        continue;
                    }
                    let (reqid, payload) = block.split_at(8);
                    if let Ok(wire::OwnedRequest::SetMembers {
                        epoch,
                        vnodes,
                        rf,
                        members,
                        ..
                    }) = wire::decode_request(payload)
                    {
                        *recorded.lock().unwrap() =
                            Some((epoch, members.iter().map(|&(n, _)| n).collect()));
                        let resp = Response::Members {
                            epoch,
                            vnodes,
                            rf,
                            members,
                            addrs: Vec::new(),
                        };
                        let rp = wire::encode_response_payload(&resp);
                        let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                        out.extend_from_slice(reqid);
                        out.extend_from_slice(&rp);
                        let _ = s.write_all(&out);
                    }
                }
            });
        }

        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
                .unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        thread::sleep(Duration::from_millis(80));
        assert!(
            recorded.lock().unwrap().is_none(),
            "node down: nothing delivered yet"
        );

        up.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((epoch, members)) = recorded.lock().unwrap().clone() {
                assert!(epoch > 0, "delivered at a real epoch");
                assert_eq!(members, vec![1, 2, 3], "delivered the current membership");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "membership never delivered after the node came up (push was fire-once)"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn durable_pd_daemon_restart_resumes_the_config_epoch() {
        let adopted: Arc<Mutex<(u64, Vec<NodeId>)>> = Arc::new(Mutex::new((0, Vec::new())));
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        {
            let adopted = adopted.clone();
            thread::spawn(move || {
                for s in l.incoming() {
                    let Ok(mut s) = s else { break };
                    if crate::proto::server_handshake_blocking(&mut s).is_err() {
                        continue;
                    }
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        continue;
                    }
                    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut block).is_err() {
                        continue;
                    }
                    let (reqid, payload) = block.split_at(8);
                    if let Ok(wire::OwnedRequest::SetMembers {
                        epoch,
                        vnodes,
                        rf,
                        members,
                        ..
                    }) = wire::decode_request(payload)
                    {
                        let mut cur = adopted.lock().unwrap();
                        if epoch > cur.0 {
                            *cur = (epoch, members.iter().map(|&(n, _)| n).collect());
                        }
                        let resp = Response::Members {
                            epoch: cur.0,
                            vnodes,
                            rf,
                            members,
                            addrs: Vec::new(),
                        };
                        let rp = wire::encode_response_payload(&resp);
                        let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                        out.extend_from_slice(reqid);
                        out.extend_from_slice(&rp);
                        let _ = s.write_all(&out);
                    }
                }
            });
        }

        let dir = std::env::temp_dir().join(format!("vals-pd-restart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ring = RingConfig::default();

        let epoch_before = {
            let pd = PdDaemon::spawn_single_durable(1, engine(), ring, vec![(1, vec![addr])], &dir)
                .unwrap();
            bootstrap_daemon(&pd, &[1, 2, 3]);
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let (e, m) = adopted.lock().unwrap().clone();
                if m == vec![1, 2, 3] {
                    assert!(e > 0);
                    break e;
                }
                assert!(Instant::now() < deadline, "initial membership not adopted");
                thread::sleep(Duration::from_millis(20));
            }
        };
        thread::sleep(Duration::from_millis(100));

        let pd =
            PdDaemon::spawn_single_durable(1, engine(), ring, vec![(1, vec![addr])], &dir).unwrap();
        pd.add_node(4).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let (e, m) = adopted.lock().unwrap().clone();
            if m == vec![1, 2, 3, 4] {
                assert!(
                    e > epoch_before,
                    "restarted PD must push above its old epoch ({e} <= {epoch_before})"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "post-restart change never adopted (epoch reset bricked reconfiguration); \
                 node holds {:?}",
                adopted.lock().unwrap()
            );
            thread::sleep(Duration::from_millis(20));
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn incarnation_of(pd: &PdDaemon) -> u128 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(id) = pd.status().and_then(|status| status.incarnation) {
                return id;
            }
            assert!(Instant::now() < deadline, "the PD never committed a cluster incarnation");
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_members(addr: SocketAddr, want: &[NodeId], what: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while members_of(addr) != want {
            assert!(
                Instant::now() < deadline,
                "{what}: node holds {:?}, wanted {want:?}",
                members_of(addr)
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn rival_and_obsolete_pd_groups_are_fenced_and_a_restore_reenrols_the_node() {
        use crate::zonestore::{FileZoneStore, ZoneGeom, ZoneStore};
        let root = std::env::temp_dir().join(format!("vals-pd-incarnation-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let geom = ZoneGeom { block_size: 256, seg_blocks: 4, reserved_blocks: 0, region_blocks: 256 };
        let zones_dir = root.join("node-zones");
        let (listener, addr) = Node::bind().unwrap();
        let serve = |listener: TcpListener| {
            let provider = FileZoneStore::new(1, &zones_dir, geom).unwrap();
            let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
            crate::testutil::running(Node::serve_zoned(
                1, listener, zone0, provider, Catalog::new(), Ring::new(1), Vec::new(),
                Duration::from_secs(3600), None,
            ))
        };
        let ring = RingConfig { vnodes: 8, rf: 1 };
        let data_nodes = vec![(1, vec![addr])];

        let node = serve(listener);
        let pd_a = crate::testutil::running_pd(
            PdDaemon::spawn_single_durable(1, engine(), ring, data_nodes.clone(), root.join("pd-a"))
                .unwrap(),
        );
        bootstrap_daemon(&pd_a, &[1]);
        let a = incarnation_of(&pd_a);
        wait_members(addr, &[1], "group A configures the node");

        let pd_b = crate::testutil::running_pd(
            PdDaemon::spawn_single_durable(1, engine(), ring, data_nodes.clone(), root.join("pd-b"))
                .unwrap(),
        );
        bootstrap_daemon(&pd_b, &[1, 2]);
        for extra in 3..8 {
            pd_b.add_node(extra).unwrap();
        }
        let b = incarnation_of(&pd_b);
        assert_ne!(a, b, "independently bootstrapped groups mint different incarnations");
        let rejected = crate::observe::counters().control_incarnation_rejected.get();
        let deadline = Instant::now() + Duration::from_secs(10);
        while crate::observe::counters().control_incarnation_rejected.get() < rejected + 3 {
            assert!(Instant::now() < deadline, "group B's pushes were never refused");
            thread::sleep(Duration::from_millis(20));
        }
        let (node_epoch, _, _) = config_of(addr);
        assert!(
            pd_b.status().unwrap().applied > node_epoch,
            "the rival pushes at a higher epoch than the node holds"
        );
        assert_eq!(members_of(addr), vec![1], "a higher-epoch rival group is refused");

        drop(pd_a);
        node.shutdown();
        let reenrolled = FileZoneStore::new(1, &zones_dir, geom).unwrap();
        assert_eq!(reenrolled.persisted_incarnation(), Some(a));
        reenrolled.persist_incarnation(Some(b)).unwrap();
        drop(reenrolled);
        let node = serve(TcpListener::bind(addr).and_then(|l| l.set_nonblocking(true).map(|_| l)).unwrap());
        wait_members(addr, &[1, 2, 3, 4, 5, 6, 7], "the re-enrolled node follows group B");

        let pd_a = crate::testutil::running_pd(
            PdDaemon::spawn_single_durable(1, engine(), ring, data_nodes, root.join("pd-a"))
                .unwrap(),
        );
        assert_eq!(incarnation_of(&pd_a), a, "a PD restart keeps its incarnation");
        pd_a.add_node(9).unwrap();
        let rejected = crate::observe::counters().control_incarnation_rejected.get();
        let deadline = Instant::now() + Duration::from_secs(10);
        while crate::observe::counters().control_incarnation_rejected.get() < rejected + 3 {
            assert!(Instant::now() < deadline, "the returning old authority was never refused");
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(members_of(addr), vec![1, 2, 3, 4, 5, 6, 7]);
        drop(pd_a);
        drop(pd_b);
        drop(node);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pd_leader_failover_keeps_the_cluster_incarnation() {
        let (listener, addr) = Node::bind().unwrap();
        let _node = crate::testutil::running(Node::serve(
            1, listener, data_store(1), Ring::new(1), Vec::new(), Duration::from_secs(3600),
        ));
        let data_nodes = vec![(1, vec![addr])];
        let ring = RingConfig { vnodes: 8, rf: 1 };
        let bound: Vec<_> = (0..3).map(|_| PdDaemon::bind().unwrap()).collect();
        let peers: Vec<(u64, SocketAddr)> =
            bound.iter().enumerate().map(|(i, (_, a))| (i as u64 + 1, *a)).collect();
        let mut pds: Vec<Option<crate::testutil::RunningPd>> = bound
            .into_iter()
            .enumerate()
            .map(|(i, (listener, _))| {
                Some(crate::testutil::running_pd(
                    PdDaemon::spawn(
                        i as u64 + 1, listener, peers.clone(), data_nodes.clone(), engine(), ring,
                        i == 0,
                    )
                    .unwrap(),
                ))
            })
            .collect();
        pds[0].as_ref().unwrap().bootstrap_members(vec![(1, 1, Vec::new())]);
        wait_members(addr, &[1], "the first leader configures the node");
        let id = incarnation_of(pds[0].as_ref().unwrap());
        for pd in pds.iter().flatten() {
            assert_eq!(incarnation_of(pd), id, "every replica applies the same incarnation");
        }
        let leader = pds[0].as_ref().unwrap().status().unwrap().leader.unwrap();
        pds[leader as usize - 1] = None;
        let deadline = Instant::now() + Duration::from_secs(20);
        'added: loop {
            for pd in pds.iter().flatten() {
                if pd.add_node(2).is_ok() {
                    break 'added;
                }
            }
            assert!(Instant::now() < deadline, "no new leader accepted the change");
            thread::sleep(Duration::from_millis(50));
        }
        wait_members(addr, &[1, 2], "the new leader's push is accepted by the bound node");
        for pd in pds.iter().flatten() {
            assert_eq!(incarnation_of(pd), id);
        }
    }

    #[test]
    fn atomic_bootstrap_membership_reopens_at_global_rf() {
        let dir = std::env::temp_dir().join(format!(
            "vals-pd-bootstrap-restart-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let ring = RingConfig::default();
        let pd = PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
        bootstrap_daemon(&pd, &[10, 20, 30]);
        pd.shutdown();

        let reopened =
            PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while reopened.members() != [10, 20, 30] {
            assert!(Instant::now() < deadline, "reopened bootstrap membership was incomplete");
            thread::sleep(Duration::from_millis(5));
        }
        reopened.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_pd_refuses_to_restart_with_a_changed_rf() {
        let dir = std::env::temp_dir().join(format!("vals-pd-rf-change-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let ring = RingConfig { vnodes: 8, rf: 1 };
        let pd = PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
        bootstrap_daemon(&pd, &[10, 20, 30]);
        incarnation_of(&pd);
        pd.shutdown();

        let changed = RingConfig { rf: 3, ..ring };
        let refused = match PdDaemon::spawn_single_durable(1, engine(), changed, Vec::new(), &dir) {
            Ok(_) => panic!("a PD with committed state started with a changed rf"),
            Err(e) => e.to_string(),
        };
        assert!(
            refused.contains("ring rf 1") && refused.contains("rf = 3") && refused.contains("docs/growing.md"),
            "{refused}"
        );

        let reopened = PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while reopened.members() != [10, 20, 30] {
            assert!(Instant::now() < deadline, "the PD did not reopen with its unchanged rf");
            thread::sleep(Duration::from_millis(5));
        }
        reopened.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn durable_pd_restart_does_not_reuse_a_dropped_zone_id() {
        let dir = std::env::temp_dir().join(format!(
            "vals-pd-zone-id-restart-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let ring = RingConfig {
            rf: 1,
            ..RingConfig::default()
        };
        let dropped_id = {
            let pd = PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
            pd.add_node(1).unwrap();
            pd.create_zone(ZoneConfig {
                name: "old".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            }).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let id = loop {
                if let Some(id) = pd.catalog().by_name("old") {
                    break id;
                }
                assert!(Instant::now() < deadline, "initial zone was not committed");
                thread::sleep(Duration::from_millis(5));
            };
            pd.drop_zone(id).unwrap();
            while pd.catalog().contains(id) {
                assert!(Instant::now() < deadline, "zone drop was not committed");
                thread::sleep(Duration::from_millis(5));
            }
            id
        };
        thread::sleep(Duration::from_millis(100));

        {
            let pd = PdDaemon::spawn_single_durable(1, engine(), ring, Vec::new(), &dir).unwrap();
            pd.create_zone(ZoneConfig {
                name: "new".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            }).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(id) = pd.catalog().by_name("new") {
                    assert!(id > dropped_id, "restarted PD reused dropped zone {id}");
                    break;
                }
                assert!(Instant::now() < deadline, "recreated zone was not committed");
                thread::sleep(Duration::from_millis(5));
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pd_retries_zone_push_until_a_down_node_acks() {
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let up = Arc::new(AtomicBool::new(false));
        #[allow(clippy::type_complexity)]
        let recorded: Arc<Mutex<Option<(u64, Vec<String>)>>> = Arc::new(Mutex::new(None));
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        {
            let up = up.clone();
            let recorded = recorded.clone();
            thread::spawn(move || {
                for s in l.incoming() {
                    let mut s = match s {
                        Ok(s) => s,
                        Err(_) => break,
                    };
                    if !up.load(AtomicOrdering::SeqCst) {
                        drop(s);
                        continue;
                    }
                    if crate::proto::server_handshake_blocking(&mut s).is_err() {
                        continue;
                    }
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        continue;
                    }
                    let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut block).is_err() {
                        continue;
                    }
                    let (reqid, payload) = block.split_at(8);
                    if let Ok(wire::OwnedRequest::SetZones { epoch, zones, .. }) =
                        wire::decode_request(payload)
                    {
                        *recorded.lock().unwrap() =
                            Some((epoch, zones.iter().map(|z| z.name.clone()).collect()));
                        let resp = Response::Zones { epoch, zones };
                        let rp = wire::encode_response_payload(&resp);
                        let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                        out.extend_from_slice(reqid);
                        out.extend_from_slice(&rp);
                        let _ = s.write_all(&out);
                    }
                }
            });
        }

        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
                .unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        pd.create_zone(ZoneConfig {
            name: "tenant-a".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();
        thread::sleep(Duration::from_millis(80));
        assert!(
            recorded.lock().unwrap().is_none(),
            "node down: no catalog delivered yet"
        );

        up.store(true, AtomicOrdering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some((epoch, names)) = recorded.lock().unwrap().clone() {
                assert!(epoch > 0, "delivered at a real epoch");
                assert_eq!(names, vec!["tenant-a".to_owned()], "delivered the catalog");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "catalog never delivered after the node came up (push was fire-once)"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn pd_daemon_propagates_membership_to_live_nodes() {
        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let ring = Ring::from_members(2, 64, [1, 2]);
        let peers = vec![(1, a1), (2, a2)];
        let slow = Duration::from_secs(3600);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            data_store(1),
            ring.clone(),
            peers.clone(),
            slow,
        ));
        let _node = crate::testutil::running(Node::serve(2, l2, data_store(2), ring, peers, slow));

        let cfg = RingConfig { vnodes: 16, rf: 2 };
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), cfg, vec![(1, vec![a1]), (2, vec![a2])]).unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if members_of(a1) == [1, 2, 3] && members_of(a2) == [1, 2, 3] {
                break;
            }
            assert!(Instant::now() < deadline, "membership not propagated");
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(pd.members(), vec![1, 2, 3], "PD's committed view");

        for a in [a1, a2] {
            let (epoch, vnodes, rf) = config_of(a);
            assert_eq!((vnodes, rf), (16, 2), "node adopted the pushed ring config");
            assert!(epoch > 0, "node adopted a real (PD applied-index) epoch");
        }

        pd.remove_node(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if members_of(a1) == [1, 3] {
                break;
            }
            assert!(Instant::now() < deadline, "removal not propagated");
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn addressed_membership_commits_and_reads_back() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        let a: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let b: SocketAddr = "[::1]:7002".parse().unwrap();
        pd.add_node_addressed(7, 3, &[a, b]).unwrap();
        settle(&mut pd);
        pd.add_node(9).unwrap();
        settle(&mut pd);
        assert_eq!(
            pd.members_addressed(),
            vec![(7, 3, vec![a, b]), (9, 1, vec![])]
        );
        assert_eq!(pd.members_weighted(), vec![(7, 3), (9, 1)]);
    }

    #[test]
    fn cluster_grows_to_a_node_nobody_knew_at_boot() {
        use crate::hlc::Version;

        let sweep = Duration::from_millis(50);
        let (l1, a1) = Node::bind().unwrap();
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            data_store(1),
            Ring::from_members(1, 64, [1]),
            Vec::new(),
            sweep,
        ));

        let resp = push_query(
            a1,
            &Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"grow",
                value: Some(b"data"),
                version: Version { hlc: 10, node: 1 },
            },
        );
        assert!(matches!(resp, Response::Apply(_)), "seed write: {resp:?}");

        let (l2, a2) = Node::bind().unwrap();
        let _node = crate::testutil::running(Node::serve(
            2,
            l2,
            data_store(2),
            Ring::from_members(1, 64, [2]),
            Vec::new(),
            sweep,
        ));

        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                engine(),
                RingConfig { vnodes: 16, rf: 2 },
                vec![(1, vec![a1])],
            )
            .unwrap(),
        );
        pd.bootstrap_members(vec![(1, 1, vec![a1]), (2, 1, vec![a2])]);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if members_of(a1) == [1, 2] && members_of(a2) == [1, 2] {
                break;
            }
            assert!(Instant::now() < deadline, "membership not propagated");
            thread::sleep(Duration::from_millis(20));
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (e1, _, _) = config_of(a1);
            let (e2, _, _) = config_of(a2);
            let resp = push_query(
                a1,
                &Request::Apply {
                    epoch: e1,
                    zone_id: DEFAULT_ZONE,
                    key: b"grow",
                    value: Some(b"data"),
                    version: Version { hlc: 10, node: 1 },
                },
            );
            drop(resp);
            if let Response::Read(Some(stored)) = push_query(
                a2,
                &Request::Read {
                    epoch: e2,
                    zone_id: DEFAULT_ZONE,
                    key: b"grow",
                },
            ) {
                assert_eq!(stored.value.as_deref(), Some(&b"data"[..]));
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pre-growth data never replicated onto the grown-in node"
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn address_announcement_for_an_existing_member_is_not_dropped() {
        let pd = one_voter_daemon();
        let a: SocketAddr = "127.0.0.1:7100".parse().unwrap();
        pd.add_node(7).unwrap();
        pd.add_node_addressed(7, 1, vec![a]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if pd.members_addressed() == vec![(7, 1, vec![a])] {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "address announce dropped: {:?}",
                pd.members_addressed()
            );
            thread::sleep(Duration::from_millis(20));
        }
        pd.shutdown();
    }

    #[test]
    fn pd_pushes_to_every_shard_of_a_node() {
        let shards = Node::bind_n(2).unwrap();
        let shard_addrs: Vec<SocketAddr> = shards.iter().map(|(_, a)| *a).collect();
        let slow = Duration::from_secs(3600);
        let shard_serve: Vec<_> = shards
            .into_iter()
            .zip([data_store(1), data_store(1)])
            .map(|((l, _), s)| (l, s))
            .collect();
        let _node = crate::testutil::running(Node::serve_sharded(
            1,
            shard_serve,
            Ring::from_members(2, 64, [1]),
            vec![(1, shard_addrs.clone())],
            slow,
        ));

        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                engine(),
                RingConfig::default(),
                vec![(1, shard_addrs.clone())],
            )
            .unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if shard_addrs.iter().all(|&a| members_of(a) == [1, 2, 3]) {
                break;
            }
            assert!(Instant::now() < deadline, "shard membership not propagated");
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn multi_node_pd_over_tcp_agrees_and_propagates() {
        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let ring = Ring::from_members(2, 64, [1, 2]);
        let peers = vec![(1, a1), (2, a2)];
        let slow = Duration::from_secs(3600);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            data_store(1),
            ring.clone(),
            peers.clone(),
            slow,
        ));
        let _node = crate::testutil::running(Node::serve(2, l2, data_store(2), ring, peers, slow));

        let data_nodes = vec![(1, vec![a1]), (2, vec![a2])];
        let (pl1, pa1) = PdDaemon::bind().unwrap();
        let (pl2, pa2) = PdDaemon::bind().unwrap();
        let (pl3, pa3) = PdDaemon::bind().unwrap();
        let pd_peers = vec![(1u64, pa1), (2, pa2), (3, pa3)];
        let rc = RingConfig::default();
        let pd1 = crate::testutil::running_pd(
            PdDaemon::spawn(
                1,
                pl1,
                pd_peers.clone(),
                data_nodes.clone(),
                engine(),
                rc,
                true,
            )
            .unwrap(),
        );
        let pd2 = crate::testutil::running_pd(
            PdDaemon::spawn(
                2,
                pl2,
                pd_peers.clone(),
                data_nodes.clone(),
                engine(),
                rc,
                false,
            )
            .unwrap(),
        );
        let pd3 = crate::testutil::running_pd(
            PdDaemon::spawn(3, pl3, pd_peers, data_nodes, engine(), rc, false).unwrap(),
        );

        pd1.bootstrap_members(vec![
            (1, 1, Vec::new()),
            (2, 1, Vec::new()),
            (3, 1, Vec::new()),
        ]);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if members_of(a1) == [1, 2, 3] && members_of(a2) == [1, 2, 3] {
                break;
            }
            assert!(Instant::now() < deadline, "multi-node PD did not propagate");
            thread::sleep(Duration::from_millis(25));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if [&pd1, &pd2, &pd3]
                .iter()
                .all(|pd| pd.members() == [1, 2, 3])
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PD replicas did not converge: {:?} {:?} {:?}",
                pd1.members(),
                pd2.members(),
                pd3.members()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn delayed_multi_pd_election_preserves_dependent_boot_intents() {
        let (pl1, pa1) = PdDaemon::bind().unwrap();
        let (pl2, pa2) = PdDaemon::bind().unwrap();
        let (pl3, pa3) = PdDaemon::bind().unwrap();
        let peers = vec![(1u64, pa1), (2, pa2), (3, pa3)];
        let ring = RingConfig { vnodes: 64, rf: 2 };
        let pd1 = crate::testutil::running_pd(
            PdDaemon::spawn(
                1,
                pl1,
                peers.clone(),
                Vec::new(),
                engine(),
                ring,
                true,
            )
            .unwrap(),
        );
        pd1.bootstrap_members(vec![(10, 1, Vec::new()), (20, 1, Vec::new())]);
        pd1.create_boot_zone(ZoneConfig {
            name: "boot-zone".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        });
        thread::sleep(Duration::from_millis(100));
        assert!(pd1.members().is_empty());
        assert!(pd1.catalog().is_empty());

        let pd2 = crate::testutil::running_pd(
            PdDaemon::spawn(
                2,
                pl2,
                peers.clone(),
                Vec::new(),
                engine(),
                ring,
                false,
            )
            .unwrap(),
        );
        let pd3 = crate::testutil::running_pd(
            PdDaemon::spawn(3, pl3, peers, Vec::new(), engine(), ring, false).unwrap(),
        );
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if [&pd1, &pd2, &pd3].iter().all(|pd| {
                pd.members() == [10, 20] && pd.catalog().by_name("boot-zone").is_some()
            }) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "delayed election lost boot intents: {:?}/{:?}, {:?}/{:?}, {:?}/{:?}",
                pd1.members(),
                pd1.catalog().ids(),
                pd2.members(),
                pd2.catalog().ids(),
                pd3.members(),
                pd3.catalog().ids(),
            );
            thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(pd2.add_node(30), Err(PartitionError::NotLeader)));
        let follower = pd2.status().unwrap();
        assert_eq!(follower.role, "follower");
        assert_eq!(follower.leader, Some(1));
        assert!(pd2.inspect().unwrap().authoritative.is_none());
        thread::sleep(Duration::from_millis(100));
        assert_eq!(pd1.members(), vec![10, 20]);

        let queued = ZoneConfig {
            name: "queued-follower".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        };
        pd2.create_boot_zone(queued.clone());
        pd3.create_boot_zone(queued);
        pd1.create_zone(ZoneConfig {
            name: "leader-zone".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while [&pd1, &pd2, &pd3]
            .iter()
            .any(|pd| pd.catalog().by_name("leader-zone") != Some(2))
        {
            assert!(Instant::now() < deadline, "leader create did not replicate");
            thread::sleep(Duration::from_millis(10));
        }
        drop(pd1);

        let deadline = Instant::now() + Duration::from_secs(10);
        while [&pd2, &pd3].iter().any(|pd| {
            pd.catalog().by_name("leader-zone") != Some(2)
                || pd.catalog().by_name("queued-follower") != Some(3)
        }) {
            assert!(
                Instant::now() < deadline,
                "queued follower create overwrote a committed zone after transition"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let leader = [&pd2, &pd3].into_iter().find(|pd| pd.status().unwrap().role == "leader")
            .expect("remaining PD quorum did not elect a leader");
        let follower = if leader.status().unwrap().id == 2 { &pd3 } else { &pd2 };
        assert_eq!(follower.status().unwrap().leader, Some(leader.status().unwrap().id));
    }

    #[test]
    fn multi_pd_presplit_keeps_opaque_map_while_install_is_in_flight() {
        let (pl1, pa1) = PdDaemon::bind().unwrap();
        let (pl2, pa2) = PdDaemon::bind().unwrap();
        let (pl3, pa3) = PdDaemon::bind().unwrap();
        let peers = vec![(1u64, pa1), (2, pa2), (3, pa3)];
        let ring_cfg = RingConfig { vnodes: 128, rf: 2 };
        let pd1 = crate::testutil::running_pd(PdDaemon::spawn(
            1,
            pl1,
            peers.clone(),
            Vec::new(),
            engine(),
            ring_cfg,
            true,
        ).unwrap());
        let pd2 = crate::testutil::running_pd(PdDaemon::spawn(
            2,
            pl2,
            peers.clone(),
            Vec::new(),
            engine(),
            ring_cfg,
            false,
        ).unwrap());
        let pd3 = crate::testutil::running_pd(PdDaemon::spawn(
            3,
            pl3,
            peers,
            Vec::new(),
            engine(),
            ring_cfg,
            false,
        ).unwrap());
        pd1.bootstrap_members(vec![
            (1, 1, Vec::new()),
            (2, 1, Vec::new()),
            (3, 1, Vec::new()),
        ]);
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if [&pd1, &pd2, &pd3].iter().all(|pd| pd.members() == [1, 2, 3])
                && pd1.push_state.lock().unwrap().members.len() == 3
            {
                break;
            }
            assert!(Instant::now() < deadline, "multi-PD membership did not settle");
            thread::sleep(Duration::from_millis(5));
        }
        {
            let mut state = pd1.push_state.lock().unwrap();
            state.opaque_range_maps_enabled = true;
            certify_opaque_capability(&mut state);
        }
        let boundaries: Vec<Vec<u8>> = (1..24).map(|byte| vec![byte]).collect();
        pd1.create_zone_presplit(
            ZoneConfig {
                name: "opaque-multi-presplit".into(),
                rf: 2,
                ordering: Ordering::Sorted,
                scope: None,
            },
            boundaries,
        ).unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        let (zone_id, map) = loop {
            if let Some(zone_id) = pd1.catalog().by_name("opaque-multi-presplit") {
                let map = pd1.range_map(zone_id);
                if map.num_ranges() == 24 {
                    break (zone_id, map);
                }
            }
            assert!(Instant::now() < deadline, "opaque multi-PD presplit did not settle");
            thread::sleep(Duration::from_millis(5));
        };
        assert!(map.supports_opaque_tokens());
        assert_eq!(map.unrehomed_ranges(), 0);
        assert!(map.tokens().iter().all(|token| crate::partition::valid_opaque_token(token)));
        let ring = Ring::from_members(2, 128, [1, 2, 3]);
        let mut slots = BTreeMap::from([(1, 0usize), (2, 0), (3, 0)]);
        for index in 0..map.num_ranges() {
            for node in map.replicas_of_range(&ring, zone_id, index, 2, None) {
                *slots.get_mut(&node).unwrap() += 1;
            }
        }
        assert_eq!(slots.values().copied().collect::<Vec<_>>(), vec![16, 16, 16]);
    }

    #[test]
    fn placement_driver_create_drop_list_zones() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2, 5]);
        assert!(pd.zones().is_empty(), "no zones before any create");
        assert_eq!(pd.next_zone_id(), DEFAULT_ZONE + 1);

        let a = pd
            .create_zone(ZoneConfig {
                name: "a".into(),
                rf: 3,
                ordering: Ordering::Unsorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);
        assert_eq!(a, 1);

        let scoped = ZoneConfig {
            name: "b".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: Some([1, 2, 5].into_iter().collect()),
        };
        let b = pd.create_zone(scoped.clone()).unwrap();
        settle(&mut pd);
        assert_eq!(b, 2);

        let cat = pd.zones();
        assert_eq!(cat.len(), 2);
        assert_eq!(cat.by_name("a"), Some(1));
        assert_eq!(cat.by_name("b"), Some(2));
        assert_eq!(cat.get(2), Some(&scoped), "config roundtrips through raft");

        pd.drop_zone(b).unwrap();
        settle(&mut pd);
        assert!(!pd.zones().contains(b));
        assert_eq!(pd.zones().len(), 1);
        assert_eq!(pd.next_zone_id(), 3, "dropping the highest id must not reuse it");
        let c = pd
            .create_zone(ZoneConfig {
                name: "c".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);
        assert_eq!(c, 3, "the persisted high-water id must not be reused");
    }

    #[test]
    fn placement_driver_opaque_prepare_executes_exact_token_and_abort_keeps_old() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let zid = pd.create_zone(ZoneConfig {
            name: "balanced".into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        settle(&mut pd);
        let mut map = RangeMap::from_splits([b"m".to_vec()]);
        map.enable_opaque_tokens();
        pd.set_range_map(zid, &map).unwrap();
        settle(&mut pd);
        let target = crate::partition::opaque_placement_token([17; 16]);
        let old = pd.range_map(zid).token_of(1).to_vec();

        let submitted = pd.acquire_balance_intent(0x1234).unwrap();
        assert!(submitted > pd.epoch(), "enqueue is not reflected acceptance");
        assert_eq!(pd.balance_intent().unwrap(), None);
        settle(&mut pd);
        assert_eq!(pd.balance_intent().unwrap(), Some(0x1234));
        let submitted = pd.prepare_balance_map(
            zid, b"m", &target, &map, 0x1234
        ).unwrap().expect("conditional map prepare appended");
        assert!(submitted > pd.epoch());
        assert_eq!(pd.range_map(zid).pending_of(1), None);
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).pending_of(1), Some(target.as_slice()));
        assert!(pd.abort_rehome(zid, b"m").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).token_of(1), old);
        assert_eq!(pd.range_map(zid).pending_of(1), None);
        pd.clear_balance_intent(0x1234).unwrap();
        settle(&mut pd);
        assert_eq!(pd.balance_intent().unwrap(), None);

        assert!(pd.prepare_rehome_to(zid, b"m", &target).unwrap());
        settle(&mut pd);
        assert!(pd.commit_rehome(zid, b"m").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).token_of(1), target);
        assert_eq!(pd.range_map(zid).pending_of(1), None);
    }

    #[test]
    fn dropped_zone_range_map_and_pending_rehome_no_longer_gate_other_zones() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let sorted = |name: &str| ZoneConfig {
            name: name.into(),
            rf: 2,
            ordering: Ordering::Sorted,
            scope: None,
        };
        let map_records = |pd: &PlacementDriver<MemBlockDevice>, zone: ZoneId| {
            pd.state_entries().iter().filter(|(key, _)| range_map_zone_of_key(key) == Some(zone)).count()
        };
        let mut map = RangeMap::from_splits([b"m".to_vec()]);
        map.enable_opaque_tokens();
        let target = crate::partition::opaque_placement_token([17; 16]);
        let active = |pd: &PlacementDriver<MemBlockDevice>| {
            pd.range_maps().values().map(|map| map.active_rehomes()).sum::<usize>()
        };

        let gone = pd.create_zone(sorted("gone")).unwrap();
        settle(&mut pd);
        let kept = pd.create_zone(sorted("kept")).unwrap();
        settle(&mut pd);
        pd.set_range_map(gone, &map).unwrap();
        pd.set_range_map(kept, &map).unwrap();
        settle(&mut pd);
        assert!(pd.prepare_rehome_to(gone, b"m", &target).unwrap());
        settle(&mut pd);
        assert_eq!(active(&pd), 1);
        pd.drop_zone(gone).unwrap();
        settle(&mut pd);
        assert!(!pd.range_maps().contains_key(&gone));
        assert_eq!(map_records(&pd, gone), 0, "drop deletes the zone's range map records");
        assert_eq!(active(&pd), 0, "a dropped zone's pending re-home no longer blocks");
        assert!(pd.split_zone(gone, b"x").is_err(), "no range map write for a dropped zone");
        assert!(pd.prepare_rehome_to(kept, b"m", &target).unwrap());
        settle(&mut pd);
        assert_eq!(active(&pd), 1);
        assert!(pd.commit_rehome(kept, b"m").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(kept).token_of(1), target);
        assert_eq!(active(&pd), 0);

        let stale = pd.create_zone(sorted("stale")).unwrap();
        settle(&mut pd);
        pd.set_range_map(stale, &map).unwrap();
        settle(&mut pd);
        assert!(pd.prepare_rehome_to(stale, b"m", &target).unwrap());
        settle(&mut pd);
        pd.partition.propose(&Command::Delete(zone_key(stale))).unwrap();
        settle(&mut pd);
        assert!(!pd.zones().contains(stale));
        assert!(map_records(&pd, stale) > 0, "a pre-0479 drop left the map behind");
        assert!(!pd.range_maps().contains_key(&stale), "an orphan map is ignored");
        assert_eq!(active(&pd), 0, "an orphan pending re-home no longer blocks");
        assert!(pd.collect_orphan_range_maps().unwrap());
        settle(&mut pd);
        assert_eq!(map_records(&pd, stale), 0, "orphan map records are collected");
        assert!(!pd.collect_orphan_range_maps().unwrap());

        let raced = pd.create_zone(sorted("raced")).unwrap();
        settle(&mut pd);
        pd.drop_zone(raced).unwrap();
        assert!(pd.split_zone(raced, b"x").unwrap(), "proposed before the drop applies");
        settle(&mut pd);
        assert!(!pd.zones().contains(raced));
        assert_eq!(map_records(&pd, raced), 0, "a map write applied after the drop is refused");
    }

    #[test]
    fn placement_driver_splits_sorted_zones() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2, 3]);
        let zid = pd
            .create_zone(ZoneConfig {
                name: "s".into(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).num_ranges(), 1, "unsplit = one range");
        assert!(
            pd.range_maps().is_empty(),
            "no stored map until first split"
        );

        assert!(pd.split_zone(zid, b"m").unwrap());
        settle(&mut pd);
        assert!(pd.split_zone(zid, b"t").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).splits(), &[b"m".to_vec(), b"t".to_vec()]);
        assert_eq!(pd.range_map(zid).tokens(), &[Vec::new(), Vec::new(), Vec::new()]);
        assert_eq!(pd.range_map(zid).num_ranges(), 3);

        assert!(!pd.split_zone(zid, b"m").unwrap());
        assert!(!pd.split_zone(zid, b"").unwrap());
        settle(&mut pd);
        assert_eq!(
            pd.range_map(zid).num_ranges(),
            3,
            "no-op splits don't grow it"
        );

        let maps = pd.range_maps();
        assert_eq!(maps.len(), 1);
        assert_eq!(maps[&zid].splits(), &[b"m".to_vec(), b"t".to_vec()]);
        assert_eq!(pd.zones().len(), 1, "zone catalog unaffected by range maps");

        pd.set_range_map(zid, &RangeMap::single()).unwrap();
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).num_ranges(), 1, "reset to a single range");
    }

    #[test]
    fn bounded_state_materializes_target_range_count_incrementally() {
        let partition = Partition::new_single_bounded(1, 16 * 1024 * 1024)
            .unwrap()
            .with_snapshot_interval(1);
        let mut pd = PlacementDriver::from_partition(partition).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[3]);
        let zid = pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
        settle(&mut pd);
        let boundaries: Vec<Vec<u8>> = (1..24_000u64)
            .map(|i| format!("{:024}", i * 2).into_bytes())
            .collect();
        let map = RangeMap::from_splits(boundaries);
        pd.set_range_map(zid, &map).unwrap();
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).num_ranges(), 24_000);
        let (_, used_before, _) = pd.state_capacity().unwrap();

        let split = format!("{:024}", 23_999u64).into_bytes();
        assert!(pd.split_zone(zid, &split).unwrap());
        settle(&mut pd);
        let (_, used_after, reserved) = pd.state_capacity().unwrap();
        assert_eq!(pd.range_map(zid).num_ranges(), 24_001);
        assert_eq!(reserved, 0);
        assert!(
            used_after.saturating_sub(used_before) <= 128,
            "one split persisted more than one bounded range record: {used_before} -> {used_after}"
        );
        assert!(pd.partition.snapshot_index() > 0);
    }

    #[test]
    fn thousands_of_durable_splits_stay_incremental_and_recover_snapshot_tail() {
        const SPLITS: usize = 2_000;
        const MAX_SPLIT_COMMAND_BYTES: usize = 256;
        let dir = std::env::temp_dir().join(format!(
            "vals-pd-thousands-splits-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let partition = Partition::open_single_bounded(1, 2 * 1024 * 1024, &dir)
            .unwrap()
            .with_snapshot_interval(128);
        let mut pd = PlacementDriver::from_partition(partition).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[3]);
        let zid = pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
        settle(&mut pd);
        let mut boundaries = Vec::with_capacity(SPLITS + 1);
        let mut largest_command = 0;

        for i in 1..=SPLITS {
            let boundary = format!("account-{i:016}").into_bytes();
            let (index, encoded_len, write_count) = pd
                .split_zone_measured(zid, &boundary)
                .unwrap()
                .expect("new boundary must split its range");
            if i > 1 {
                assert_eq!(write_count, 2, "normal split writes only metadata and one range");
                assert!(
                    encoded_len <= MAX_SPLIT_COMMAND_BYTES,
                    "split {i} proposed an unbounded {encoded_len}-byte Raft command"
                );
            }
            largest_command = largest_command.max(encoded_len);
            boundaries.push(boundary);
            settle_through(&mut pd, index);
        }

        if pd.partition.snapshot_index() == pd.last_index() {
            let boundary = b"account-9999999999999999".to_vec();
            let (index, encoded_len, write_count) = pd
                .split_zone_measured(zid, &boundary)
                .unwrap()
                .expect("tail boundary must split its range");
            assert_eq!(write_count, 2);
            assert!(encoded_len <= MAX_SPLIT_COMMAND_BYTES);
            largest_command = largest_command.max(encoded_len);
            boundaries.push(boundary);
            settle_through(&mut pd, index);
        }

        assert!(pd.partition.snapshot_index() > 0, "split history produced a snapshot");
        assert!(
            pd.partition.snapshot_index() < pd.last_index(),
            "recovery fixture retains a committed log tail after its snapshot"
        );
        assert_eq!(pd.range_map(zid).splits(), boundaries);
        assert!(largest_command <= MAX_SPLIT_COMMAND_BYTES);
        let expected = pd.range_map(zid);
        drop(pd);

        let mut reopened = PlacementDriver::open_single_bounded(1, 2 * 1024 * 1024, &dir).unwrap();
        settle(&mut reopened);
        assert_eq!(reopened.range_map(zid), expected);
        assert_eq!(reopened.partition.state_reserved(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn range_map_capacity_rejection_leaves_pd_healthy_and_unchanged() {
        let mut pd = PlacementDriver::new_single_bounded(1, 256).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[3]);
        let zid = pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
        settle(&mut pd);
        let map = RangeMap::from_splits(
            (0..32).map(|i| format!("boundary-{i:04}").into_bytes()),
        );
        let before = pd.last_index();
        let error = pd.set_range_map(zid, &map).unwrap_err();
        assert!(matches!(error, PartitionError::CapacityExceeded { .. }));
        assert_eq!(pd.last_index(), before, "rejected proposal must not enter the Raft log");
        assert_eq!(pd.range_map(zid), RangeMap::single());
        pd.add_node(4).unwrap();
        settle(&mut pd);
        assert_eq!(pd.members(), vec![3, 4]);
    }

    #[test]
    fn legacy_whole_map_recovers_and_migrates_on_first_update() {
        let dir = std::env::temp_dir().join(format!(
            "vals-pd-legacy-state-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let legacy_map = RangeMap::from_splits([b"m".to_vec(), b"t".to_vec()]);
        let zid = {
            let mut pd = PlacementDriver::open_single(1, engine(), &dir).unwrap();
            settle(&mut pd);
            add_members(&mut pd, &[3]);
            let zid = pd.create_zone(ZoneConfig { name: "s".into(), rf: 1, ordering: Ordering::Sorted, scope: None }).unwrap();
            settle(&mut pd);
            let mut key = vec![b'R'];
            key.extend_from_slice(&zid.to_le_bytes());
            pd.partition.propose(&Command::Put(key, legacy_map.encode())).unwrap();
            settle(&mut pd);
            zid
        };

        let mut pd = PlacementDriver::open_single_bounded(1, 1024 * 1024, &dir).unwrap();
        settle(&mut pd);
        assert_eq!(pd.range_map(zid), legacy_map);
        assert!(pd.split_zone(zid, b"p").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).splits(), &[b"m".to_vec(), b"p".to_vec(), b"t".to_vec()]);
        let mut legacy_key = vec![b'R'];
        legacy_key.extend_from_slice(&zid.to_le_bytes());
        assert_eq!(pd.partition.state_get(&legacy_key).unwrap(), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn placement_driver_canonical_and_inherited_splits_are_distinct() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);
        let zid = pd
            .create_zone(ZoneConfig {
                name: "s".into(),
                rf: 2,
                ordering: Ordering::Sorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);

        assert!(pd.split_zone_canonical(zid, b"m").unwrap());
        settle(&mut pd);
        let map = pd.range_map(zid);
        assert_eq!(map.tokens(), &[Vec::new(), b"m".to_vec()]);
        assert!(map.is_canonical());

        assert!(pd.split_zone(zid, b"t").unwrap());
        settle(&mut pd);
        let map = pd.range_map(zid);
        assert_eq!(map.tokens(), &[Vec::new(), b"m".to_vec(), b"m".to_vec()]);
        assert!(!map.is_canonical());
        assert_eq!(map.unrehomed_ranges(), 1);

        assert!(pd.split_zone_canonical(zid, b"p").unwrap());
        settle(&mut pd);
        let map = pd.range_map(zid);
        assert_eq!(map.splits(), &[b"m".to_vec(), b"p".to_vec(), b"t".to_vec()]);
        assert_eq!(
            map.tokens(),
            &[Vec::new(), b"m".to_vec(), b"m".to_vec(), b"m".to_vec()]
        );
    }

    #[test]
    fn placement_driver_merges_a_covered_batch_in_one_map_update_and_drops_it_during_a_rehome() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2, 3]);
        let zid = pd
            .create_zone(ZoneConfig {
                name: "s".into(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);
        for at in [b"d", b"h", b"m", b"t"] {
            assert!(pd.split_zone(zid, at).unwrap());
            settle(&mut pd);
        }
        let batch = vec![b"h".to_vec(), b"t".to_vec(), b"nope".to_vec()];
        let kind = RangeOpKind::MergeAll(batch.clone());
        assert!(!range_op_reflected(&pd.range_map(zid), b"h", &kind));
        let before = pd.last_index();
        assert!(pd.merge_zone_all(zid, &batch).unwrap());
        assert_eq!(pd.last_index(), before + 1, "one proposal for the whole batch");
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).splits(), &[b"d".to_vec(), b"m".to_vec()]);
        assert!(range_op_reflected(&pd.range_map(zid), b"h", &kind));
        assert!(!pd.merge_zone_all(zid, &batch).unwrap(), "nothing left to merge");

        assert!(pd.prepare_rehome(zid, b"m").unwrap());
        settle(&mut pd);
        let blocked = RangeOpKind::MergeAll(vec![b"d".to_vec()]);
        assert!(
            range_op_reflected(&pd.range_map(zid), b"d", &blocked),
            "a batch reaching the queue head during a re-home is dropped, not stalled"
        );
        assert!(!pd.merge_zone_all(zid, &[b"d".to_vec()]).unwrap());
    }

    #[test]
    fn placement_driver_merges_sorted_zones() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2, 3]);
        let zid = pd
            .create_zone(ZoneConfig {
                name: "s".into(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            })
            .unwrap();
        settle(&mut pd);
        assert!(pd.split_zone(zid, b"m").unwrap());
        settle(&mut pd);
        assert!(pd.split_zone(zid, b"t").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).splits(), &[b"m".to_vec(), b"t".to_vec()]);

        assert!(pd.merge_zone(zid, b"m").unwrap());
        settle(&mut pd);
        assert_eq!(pd.range_map(zid).splits(), &[b"t".to_vec()]);
        assert_eq!(pd.range_map(zid).num_ranges(), 2);

        assert!(!pd.merge_zone(zid, b"m").unwrap());
        assert!(!pd.merge_zone(zid, b"nope").unwrap());
        settle(&mut pd);
        assert_eq!(
            pd.range_map(zid).num_ranges(),
            2,
            "no-op merges don't shrink it"
        );
    }

    #[allow(clippy::type_complexity)]
    fn spawn_zone_stub() -> (SocketAddr, Arc<Mutex<Vec<Vec<wire::OwnedZone>>>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<wire::OwnedZone>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = received.clone();
        thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut s) = conn else { continue };
                let sink = sink.clone();
                if crate::proto::server_handshake_blocking(&mut s).is_err() {
                    continue;
                }
                thread::spawn(move || loop {
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        return;
                    }
                    let mut frame = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut frame).is_err() {
                        return;
                    }
                    let (reqid, payload) = frame.split_at(8);
                    let resp = match wire::decode_request(payload).unwrap() {
                        wire::OwnedRequest::SetZones { epoch, zones, .. } => {
                            sink.lock().unwrap().push(zones.clone());
                            Response::Zones { epoch, zones }
                        }
                        wire::OwnedRequest::SetMembers {
                            epoch,
                            vnodes,
                            rf,
                            members,
                            ..
                        } => Response::Members {
                            epoch,
                            vnodes,
                            rf,
                            members,
                            addrs: Vec::new(),
                        },
                        _ => Response::Members {
                            epoch: 0,
                            vnodes: 0,
                            rf: 0,
                            members: Vec::new(),
                            addrs: Vec::new(),
                        },
                    };
                    let rp = wire::encode_response_payload(&resp);
                    let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                    out.extend_from_slice(reqid);
                    out.extend_from_slice(&rp);
                    if s.write_all(&out).is_err() {
                        return;
                    }
                });
            }
        });
        (addr, received)
    }

    #[test]
    fn pd_daemon_create_zone_commits_and_pushes() {
        let (addr, received) = spawn_zone_stub();
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
                .unwrap(),
        );

        let cfg = ZoneConfig {
            name: "tenant-a".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: Some([1, 2].into_iter().collect()),
        };
        bootstrap_daemon(&pd, &[1, 2, 3]);
        pd.create_zone(cfg.clone()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if pd.catalog().by_name("tenant-a").is_some() {
                break;
            }
            assert!(Instant::now() < deadline, "zone not committed");
            thread::sleep(Duration::from_millis(20));
        }
        let id = pd.catalog().by_name("tenant-a").unwrap();
        assert_eq!(id, 1);
        assert_eq!(pd.catalog().get(id), Some(&cfg));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let pushed = received.lock().unwrap().iter().any(|zs| {
                zs.iter().any(|z| {
                    z.name == "tenant-a" && z.rf == 2 && z.scope.as_deref() == Some(&[1, 2][..])
                })
            });
            if pushed {
                break;
            }
            assert!(Instant::now() < deadline, "zone not pushed to data node");
            thread::sleep(Duration::from_millis(20));
        }

        pd.drop_zone(id).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if !pd.catalog().contains(id) {
                break;
            }
            assert!(Instant::now() < deadline, "drop not committed");
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn single_node_pd_assigns_distinct_ids_to_concurrent_creates() {
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), Vec::new()).unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        for name in ["tenant-a", "tenant-b"] {
            pd.create_zone(ZoneConfig {
                name: name.into(),
                rf: 2,
                ordering: Ordering::Unsorted,
                scope: None,
            }).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let cat = pd.catalog();
            if let (Some(a), Some(b)) = (cat.by_name("tenant-a"), cat.by_name("tenant-b")) {
                assert_ne!(a, b, "concurrent creates must get distinct ids");
                break;
            }
            assert!(
                Instant::now() < deadline,
                "both zones must commit (a colliding id overwrote one): {:?}",
                pd.catalog().ids()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn create_zone_presplit_yields_k_ranges() {
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), Vec::new()).unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        let boundaries = crate::partition::even_boundaries(4);
        assert_eq!(boundaries.len(), 3);
        pd.create_zone_presplit(
            ZoneConfig {
                name: "sorted-presplit".into(),
                rf: 2,
                ordering: Ordering::Sorted,
                scope: None,
            },
            boundaries.clone(),
        ).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            let snap = pd.push_state.lock().unwrap().clone();
            if let Some(id) = snap.zones.by_name("sorted-presplit") {
                assert_eq!(
                    snap.range_maps.get(&id).map(RangeMap::num_ranges),
                    Some(4),
                    "new zone became push-visible before its complete presplit assignment"
                );
                break id;
            }
            assert!(
                Instant::now() < deadline,
                "presplit map never reached 4 ranges: {:?}",
                pd.catalog()
                    .by_name("sorted-presplit")
                    .map(|id| pd.range_map(id).num_ranges())
            );
            thread::sleep(Duration::from_millis(1));
        };
        let map = pd.range_map(zid);
        assert_eq!(map.splits(), boundaries.as_slice());
        assert!(map.is_canonical());
        assert_eq!(map.unrehomed_ranges(), 0);

        let ring = Ring::from_members(2, 128, [1, 2, 3, 4, 5]);
        let replica_sets: std::collections::HashSet<Vec<NodeId>> = (0..map.num_ranges())
            .map(|index| map.replicas_of_range(&ring, zid, index, 2, None))
            .collect();
        assert!(replica_sets.len() > 1, "canonical presplits must spread placement");
    }

    #[test]
    fn batched_add_then_presplit_does_not_use_pre_add_certification() {
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                engine(),
                RingConfig { vnodes: 128, rf: 1 },
                Vec::new(),
            )
            .unwrap(),
        );
        let boundaries = vec![b"g".to_vec(), b"n".to_vec(), b"u".to_vec()];
        let new_addr: SocketAddr = "127.0.0.1:9".parse().unwrap();
        {
            let mut state = pd.push_state.lock().unwrap();
            state.opaque_range_maps_enabled = true;
            certify_opaque_capability(&mut state);
            pd.cmd.send(PdCmd::Add(7, 1, vec![new_addr])).unwrap();
            pd.cmd.send(PdCmd::CreateZone(
                ZoneConfig {
                    name: "batched-presplit".into(),
                    rf: 1,
                    ordering: Ordering::Sorted,
                    scope: None,
                },
                boundaries.clone(),
            )).unwrap();
            thread::sleep(Duration::from_millis(20));
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        let zone_id = loop {
            let snap = pd.push_state.lock().unwrap().clone();
            if let Some(zone_id) = snap.zones.by_name("batched-presplit") {
                if snap.range_maps.get(&zone_id).map(RangeMap::num_ranges) == Some(4) {
                    break zone_id;
                }
            }
            assert!(Instant::now() < deadline, "batched presplit did not settle");
            thread::sleep(Duration::from_millis(1));
        };
        let map = pd.range_map(zone_id);
        assert_eq!(map, RangeMap::from_splits(boundaries));
        assert!(!map.supports_opaque_tokens());
        assert!(pd.members().contains(&7));
    }

    #[test]
    fn send_raft_gives_up_promptly_on_an_unreachable_peer() {
        let mut conn = None;
        let addr: SocketAddr = "10.255.255.1:1".parse().unwrap();
        let start = Instant::now();
        assert!(!send_raft(&mut conn, addr, &vals_raft::encode_message(&Message::default())));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "send_raft stalled {:?} on a black-holed peer",
            start.elapsed()
        );
        assert!(conn.is_none(), "no connection cached on failure");
    }

    #[test]
    fn raft_sends_to_an_unreachable_peer_never_block_the_drive_loop() {
        let addr: SocketAddr = "10.255.255.1:1".parse().unwrap();
        let mut peers = RaftPeers::default();
        let before = crate::observe::counters().pd_raft_send_drops.get();
        let start = Instant::now();
        for _ in 0..5000 {
            peers.send(7, addr, &Message::default());
        }
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "5000 sends to a black-holed peer took {:?}",
            start.elapsed()
        );
        assert!(crate::observe::counters().pd_raft_send_drops.get() > before, "the full queue dropped messages");
    }

    #[test]
    fn send_raft_reconnects_after_the_peer_closed_its_end() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let payload = vals_raft::encode_message(&Message::default());
        let mut conn = None;
        assert!(send_raft(&mut conn, addr, &payload));
        let (first, _) = listener.accept().unwrap();
        drop(first);
        thread::sleep(Duration::from_millis(100));
        assert!(send_raft(&mut conn, addr, &payload));
        listener.set_nonblocking(true).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut second = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Err(e) => panic!("the message after the peer closed was written to the dead connection: {e}"),
            }
        };
        second.set_nonblocking(false).unwrap();
        second.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut len = [0u8; 4];
        second.read_exact(&mut len).unwrap();
        let mut hello = vec![0u8; u32::from_le_bytes(len) as usize];
        second.read_exact(&mut hello).unwrap();
        second.read_exact(&mut len).unwrap();
        let mut frame = vec![0u8; u32::from_le_bytes(len) as usize];
        second.read_exact(&mut frame).unwrap();
        assert!(vals_raft::decode_message(&frame).is_some());
    }

    #[allow(clippy::type_complexity)]
    fn spawn_range_map_stub() -> (SocketAddr, Arc<Mutex<Vec<Vec<wire::OwnedRangeMap>>>>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let received: Arc<Mutex<Vec<Vec<wire::OwnedRangeMap>>>> = Arc::new(Mutex::new(Vec::new()));
        let sink = received.clone();
        thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut s) = conn else { continue };
                let sink = sink.clone();
                if crate::proto::server_handshake_blocking(&mut s).is_err() {
                    continue;
                }
                thread::spawn(move || loop {
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        return;
                    }
                    let mut frame = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut frame).is_err() {
                        return;
                    }
                    let (reqid, payload) = frame.split_at(8);
                    let resp = match wire::decode_request(payload).unwrap() {
                        wire::OwnedRequest::SetRangeMaps { epoch, maps, .. } => {
                            sink.lock().unwrap().push(maps.clone());
                            Response::RangeMaps { epoch, maps }
                        }
                        wire::OwnedRequest::SetMembers { epoch, .. } => Response::Members {
                            epoch,
                            vnodes: 0,
                            rf: 0,
                            members: Vec::new(),
                            addrs: Vec::new(),
                        },
                        wire::OwnedRequest::SetZones { epoch, zones, .. } => {
                            Response::Zones { epoch, zones }
                        }
                        _ => Response::RangeMaps {
                            epoch: 0,
                            maps: Vec::new(),
                        },
                    };
                    let rp = wire::encode_response_payload(&resp);
                    let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                    out.extend_from_slice(reqid);
                    out.extend_from_slice(&rp);
                    if s.write_all(&out).is_err() {
                        return;
                    }
                });
            }
        });
        (addr, received)
    }

    #[test]
    fn pusher_loop_withholds_every_push_until_the_cluster_incarnation_is_committed() {
        let (addr, received) = spawn_range_map_stub();
        let mut initial = PushState::new();
        initial.leader = true;
        initial.member_epoch = 1;
        initial.zone_epoch = 2;
        initial.members = vec![(1, 1, vec![addr])];
        initial.auto_split_poll_ms = 3_600_000;
        let state = Arc::new(Mutex::new(initial));
        let (cmd, _cmd_rx) = mpsc::channel();
        let pusher = {
            let state = state.clone();
            thread::spawn(move || {
                pusher_loop(
                    &[(1, vec![addr])],
                    RingConfig::default(),
                    &state,
                    &Mutex::new(BalancePollAuthority::default()),
                    &cmd,
                )
            })
        };

        thread::sleep(Duration::from_millis(300));
        assert!(
            received.lock().unwrap().is_empty(),
            "a leader without a committed incarnation pushed config"
        );

        state.lock().unwrap().incarnation = Some(7);
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "config never pushed once the incarnation existed");
            thread::sleep(Duration::from_millis(10));
        }

        state.lock().unwrap().stop = true;
        within_secs(10, move || pusher.join().unwrap());
    }

    #[test]
    fn pusher_loop_withholds_range_maps_until_a_zone_or_range_map_epoch_exists() {
        let (addr, received) = spawn_range_map_stub();
        let mut initial = PushState::new();
        initial.leader = true;
        initial.incarnation = Some(7);
        initial.member_epoch = 1;
        initial.members = vec![(1, 1, vec![addr])];
        initial.auto_split_poll_ms = 3_600_000;
        let state = Arc::new(Mutex::new(initial));
        let (cmd, _cmd_rx) = mpsc::channel();
        let pusher = {
            let state = state.clone();
            thread::spawn(move || {
                pusher_loop(
                    &[(1, vec![addr])],
                    RingConfig::default(),
                    &state,
                    &Mutex::new(BalancePollAuthority::default()),
                    &cmd,
                )
            })
        };

        thread::sleep(Duration::from_millis(300));
        assert!(
            received.lock().unwrap().is_empty(),
            "range maps pushed at zone epoch 0 and range-map epoch 0"
        );

        state.lock().unwrap().zone_epoch = 2;
        let deadline = Instant::now() + Duration::from_secs(5);
        while received.lock().unwrap().is_empty() {
            assert!(Instant::now() < deadline, "range maps never pushed once a zone epoch existed");
            thread::sleep(Duration::from_millis(10));
        }

        state.lock().unwrap().stop = true;
        within_secs(10, move || pusher.join().unwrap());
    }

    #[test]
    fn pd_daemon_discards_range_ops_of_a_dropped_zone_instead_of_blocking_the_queue() {
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), Vec::new()).unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        let mut ids = Vec::new();
        for name in ["gone", "kept"] {
            pd.create_zone(ZoneConfig {
                name: name.into(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            }).unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            ids.push(loop {
                if let Some(id) = pd.catalog().by_name(name) {
                    break id;
                }
                assert!(Instant::now() < deadline, "zone not committed");
                thread::sleep(Duration::from_millis(20));
            });
        }
        let (gone, kept) = (ids[0], ids[1]);
        pd.drop_zone(gone).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pd.catalog().contains(gone) {
            assert!(Instant::now() < deadline, "drop not committed");
            thread::sleep(Duration::from_millis(20));
        }
        pd.split_zone(gone, b"m".to_vec()).unwrap();
        pd.split_zone(kept, b"m".to_vec()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pd.range_map(kept).splits() != [b"m".to_vec()] {
            assert!(Instant::now() < deadline, "a dropped zone's split blocked the queue");
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(pd.range_map(gone).num_ranges(), 1);
    }

    #[test]
    fn pd_daemon_split_commits_and_pushes() {
        let (addr, received) = spawn_range_map_stub();
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
                .unwrap(),
        );

        bootstrap_daemon(&pd, &[1, 2, 3]);
        pd.create_zone(ZoneConfig {
            name: "sorted".into(),
            rf: 3,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("sorted") {
                break id;
            }
            assert!(Instant::now() < deadline, "zone not committed");
            thread::sleep(Duration::from_millis(20));
        };
        pd.split_zone(zid, b"m".to_vec()).unwrap();
        pd.split_zone(zid, b"t".to_vec()).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if pd.range_map(zid).splits() == [b"m".to_vec(), b"t".to_vec()] {
                break;
            }
            assert!(Instant::now() < deadline, "splits not committed");
            thread::sleep(Duration::from_millis(20));
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let pushed = received.lock().unwrap().iter().any(|maps| {
                maps.iter()
                    .any(|m| m.zone_id == zid && m.splits == [b"m".to_vec(), b"t".to_vec()])
            });
            if pushed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "split set not pushed to data node"
            );
            thread::sleep(Duration::from_millis(20));
        }

        pd.merge_zone(zid, b"m".to_vec()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if pd.range_map(zid).splits() == [b"t".to_vec()] {
                break;
            }
            assert!(Instant::now() < deadline, "merge not committed");
            thread::sleep(Duration::from_millis(20));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let pushed = received.lock().unwrap().iter().any(|maps| {
                maps.iter()
                    .any(|m| m.zone_id == zid && m.splits == [b"t".to_vec()])
            });
            if pushed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "merged set not pushed to data node"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn settle(pd: &mut PlacementDriver<MemBlockDevice>) {
        for _ in 0..50 {
            pd.tick();
            pd.drive().unwrap();
        }
        pd.assert_state_index_fresh();
    }

    fn bootstrap_daemon(pd: &PdDaemon, members: &[NodeId]) {
        pd.bootstrap_members(
            members
                .iter()
                .map(|&node| (node, 1, Vec::new()))
                .collect(),
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        while pd.members() != members {
            assert!(Instant::now() < deadline, "bootstrap membership did not commit");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn add_members(pd: &mut PlacementDriver<MemBlockDevice>, members: &[NodeId]) {
        for &member in members {
            pd.add_node(member).unwrap();
            settle(pd);
        }
    }

    fn settle_through(pd: &mut PlacementDriver<MemBlockDevice>, index: u64) {
        for _ in 0..50 {
            pd.tick();
            pd.drive().unwrap();
            if pd.epoch() >= index {
                pd.assert_state_index_fresh();
                return;
            }
        }
        panic!("PD did not apply proposed index {index}");
    }

    fn drive_cluster_once(pds: &mut [PlacementDriver<MemBlockDevice>]) {
        let mut msgs = Vec::new();
        for pd in pds.iter_mut() {
            pd.drive().unwrap();
            msgs.append(&mut pd.take_outgoing());
        }
        for m in msgs {
            let to = m.to;
            if let Some(pd) = pds.iter_mut().find(|p| p.id() == to) {
                pd.step(m).unwrap();
            }
        }
    }

    fn settle_cluster(pds: &mut [PlacementDriver<MemBlockDevice>]) {
        for _ in 0..100 {
            drive_cluster_once(pds);
        }
        for pd in pds.iter() {
            pd.assert_state_index_fresh();
        }
    }

    fn drive_partitioned(pds: &mut [PlacementDriver<MemBlockDevice>], isolated: u64) {
        let mut msgs = Vec::new();
        for pd in pds.iter_mut() {
            pd.drive().unwrap();
            msgs.append(&mut pd.take_outgoing());
        }
        for msg in msgs {
            if (msg.from == isolated) != (msg.to == isolated) { continue; }
            if let Some(pd) = pds.iter_mut().find(|pd| pd.id() == msg.to) {
                pd.step(msg).unwrap();
            }
        }
    }

    #[test]
    fn authoritative_pd_rejects_zone_and_membership_bypasses_while_proposals_are_pending() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        add_members(&mut pd, &[1, 2]);

        assert!(matches!(pd.add_node_weighted(3, 0), Err(PartitionError::Invariant(_))));
        assert!(matches!(
            pd.create_zone(ZoneConfig {
                name: "too-wide".into(),
                rf: 3,
                ordering: Ordering::Unsorted,
                scope: None,
            }),
            Err(PartitionError::Invariant(_))
        ));
        assert!(matches!(
            pd.create_zone(ZoneConfig {
                name: "unknown-scope".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: Some([1, 9].into_iter().collect()),
            }),
            Err(PartitionError::Invariant(_))
        ));

        let id = pd.create_zone(ZoneConfig {
            name: "protected".into(),
            rf: 2,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();
        let duplicate = ZoneConfig {
            name: "protected".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        };
        assert!(matches!(
            pd.propose_zone(id + 1, &duplicate),
            Err(PartitionError::NotReady)
        ));
        assert!(matches!(pd.remove_node(1), Err(PartitionError::NotReady)));
        settle(&mut pd);
        assert!(matches!(
            pd.propose_zone(id + 1, &duplicate),
            Err(PartitionError::Invariant(_))
        ));
        assert!(matches!(pd.remove_node(1), Err(PartitionError::Invariant(_))));
        assert_eq!(pd.zones().by_name("protected"), Some(id));
        assert_eq!(pd.members(), vec![1, 2]);

        let mut global = PlacementDriver::new_single(2, engine()).unwrap();
        settle(&mut global);
        global.set_global_rf(2).unwrap();
        assert!(matches!(
            global.add_node(1),
            Err(PartitionError::Invariant(_))
        ));
        global
            .bootstrap_members(&vec![(1, 1, Vec::new()), (2, 1, Vec::new())])
            .unwrap();
        settle(&mut global);
        assert!(matches!(
            global.set_global_rf(3),
            Err(PartitionError::Invariant(_))
        ));
        assert!(matches!(global.remove_node(1), Err(PartitionError::Invariant(_))));

        let invalid_ring = RingConfig {
            rf: 0,
            ..RingConfig::default()
        };
        assert!(matches!(
            PdDaemon::spawn_single(3, engine(), invalid_ring, Vec::new()),
            Err(PartitionError::Invariant(_))
        ));
    }

    fn run_pds(pds: &mut [PlacementDriver<MemBlockDevice>], rounds: usize) {
        for _ in 0..rounds {
            let mut msgs = Vec::new();
            for pd in pds.iter_mut() {
                pd.tick();
                pd.drive().unwrap();
                msgs.append(&mut pd.take_outgoing());
            }
            for m in msgs {
                if let Some(pd) = pds.iter_mut().find(|p| p.id() == m.to) {
                    pd.step(m).unwrap();
                }
            }
        }
    }

    #[test]
    fn cached_range_maps_follow_log_applies_snapshot_installs_and_zone_drops() {
        let sorted = |name: &str| ZoneConfig { name: name.into(), rf: 1, ordering: Ordering::Sorted, scope: None };
        let mut pds: Vec<_> = (1..=3)
            .map(|id| PlacementDriver::new_bounded(id, vec![1, 2, 3], 1 << 22).unwrap())
            .collect();
        pds[0].campaign().unwrap();
        run_pds(&mut pds, 50);
        pds[0].add_node(10).unwrap();
        run_pds(&mut pds, 20);
        let a = pds[0].create_zone(sorted("a")).unwrap();
        run_pds(&mut pds, 20);
        let b = pds[0].create_zone(sorted("b")).unwrap();
        run_pds(&mut pds, 20);
        assert!(pds[0].split_zone(b, b"m").unwrap());
        run_pds(&mut pds, 20);

        let lagging = pds.pop().unwrap();
        let lagged_at = lagging.epoch();
        for i in 0..150 {
            assert!(pds[0].split_zone(a, format!("k{i:04}").as_bytes()).unwrap());
            run_pds(&mut pds, 3);
        }
        assert!(pds[0].merge_zone(a, b"k0042").unwrap());
        run_pds(&mut pds, 10);
        pds[0].drop_zone(b).unwrap();
        run_pds(&mut pds, 10);
        let c = pds[0].create_zone(sorted("c")).unwrap();
        run_pds(&mut pds, 10);
        assert!(pds[0].split_zone(c, b"q").unwrap());
        run_pds(&mut pds, 10);
        assert!(pds[0].partition.snapshot_index() > lagged_at + 1, "the leader compacted past the lagging voter");

        pds.push(lagging);
        run_pds(&mut pds, 100);
        for i in 150..200 {
            assert!(pds[0].split_zone(a, format!("k{i:04}").as_bytes()).unwrap());
            run_pds(&mut pds, 3);
        }
        run_pds(&mut pds, 20);
        let maps = pds[0].range_maps();
        assert_eq!(maps.keys().copied().collect::<BTreeSet<_>>(), BTreeSet::from([a, c]));
        assert_eq!(maps[&a].num_ranges(), 200);
        for pd in &pds {
            pd.assert_state_index_fresh();
            assert_eq!(pd.range_maps(), maps);
            assert_eq!(pd.range_map(b), RangeMap::single());
            assert_eq!(pd.zones().ids(), vec![a, c]);
            assert_eq!(pd.members(), vec![10]);
        }
    }

    fn pd_invariant<T: std::fmt::Debug>(result: Result<T, PartitionError>, needle: &str) {
        match result {
            Err(PartitionError::Invariant(message)) => {
                assert!(message.contains(needle), "{message:?} lacks {needle:?}")
            }
            other => panic!("expected an invariant error containing {needle:?}, got {other:?}"),
        }
    }

    #[test]
    fn pd_voter_replacement_is_guarded_resumable_and_restores_redundancy() {
        let root = std::env::temp_dir().join(format!("vals-pd-replace-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let dir = |id: u64| root.join(id.to_string());
        let open = |id: u64| PlacementDriver::open_bounded(id, vec![1, 2, 3], 1 << 20, dir(id)).unwrap();
        let addr = |port: u16| -> SocketAddr { format!("127.0.0.1:{port}").parse().unwrap() };

        let mut single = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut single);
        pd_invariant(single.add_pd_learner(2, addr(2)), "multi-PD");

        let mut pds = vec![open(1), open(2), open(3)];
        pds[0].campaign().unwrap();
        run_pds(&mut pds, 50);
        assert!(pds[0].is_caught_up_leader());
        pds[0].add_node(10).unwrap();
        run_pds(&mut pds, 20);

        assert!(matches!(pds[1].add_pd_learner(4, addr(4)), Err(PartitionError::NotLeader)));
        drop(pds.pop());
        std::fs::remove_dir_all(dir(3)).unwrap();
        pd_invariant(pds[0].replace_pd_voter_step(3, 4), "not a learner");
        pd_invariant(pds[0].add_pd_learner(3, addr(3)), "already a voter");
        pd_invariant(pds[0].add_pd_learner(0, addr(3)), "reserved");

        pds.push(PlacementDriver::open_bounded_joining(4, 1 << 20, dir(4)).unwrap());
        assert!(pds[0].add_pd_learner(4, addr(4)).unwrap());
        assert!(matches!(pds[0].replace_pd_voter_step(3, 4), Err(PartitionError::NotReady)));
        run_pds(&mut pds, 50);
        assert!(!pds[0].add_pd_learner(4, addr(4)).unwrap(), "re-adding the same learner is a no-op");
        pd_invariant(pds[0].add_pd_learner(4, addr(5)), "already a learner at");
        for pd in &pds {
            assert_eq!(pd.pd_voters(), (vec![1, 2, 3], vec![4]));
            assert_eq!(pd.pd_peer_record(4), Some(PdPeerRecord::Member(addr(4))));
            assert_eq!(pd.members(), vec![10], "the learner holds the group's state");
        }
        pd_invariant(pds[0].replace_pd_voter_step(1, 4), "current leader");

        assert!(pds[0].replace_pd_voter_step(3, 4).unwrap(), "first step removes the lost voter");
        run_pds(&mut pds, 50);
        assert_eq!(pds[0].pd_voters(), (vec![1, 2], vec![4]));
        assert_eq!(pds[1].pd_peer_record(3), Some(PdPeerRecord::Retired { replaced_by: 4 }));

        drop(pds);
        let mut pds = vec![
            open(1),
            open(2),
            PlacementDriver::open_bounded_joining(4, 1 << 20, dir(4)).unwrap(),
        ];
        assert!(!pds[2].partition.is_joining(), "an existing directory restarts as itself");
        pds[0].campaign().unwrap();
        run_pds(&mut pds, 50);
        assert!(pds[0].is_caught_up_leader());
        assert_eq!(pds[2].pd_voters(), (vec![1, 2], vec![4]));
        pd_invariant(pds[0].replace_pd_voter_step(2, 4), "only 2 voters remain");
        pd_invariant(pds[0].replace_pd_voter_step(9, 4), "pd 9 is not a voter");
        assert!(pds[0].replace_pd_voter_step(3, 4).unwrap(), "the retry promotes the learner");
        run_pds(&mut pds, 50);
        assert!(!pds[0].replace_pd_voter_step(3, 4).unwrap(), "a finished replacement is a no-op");
        pd_invariant(pds[0].add_pd_learner(3, addr(3)), "never reused");
        pd_invariant(pds[0].replace_pd_voter_step(3, 5), "not a learner");
        for pd in &pds {
            assert_eq!(pd.pd_voters(), (vec![1, 2, 4], vec![]));
            assert_eq!(pd.incarnation(), pds[0].incarnation());
        }

        let mut survivors: Vec<_> = pds.drain(1..).collect();
        survivors[1].partition.force_campaign().unwrap();
        run_pds(&mut survivors, 50);
        assert!(survivors[1].is_caught_up_leader(), "the replacement leads with one original voter");
        survivors[1].add_node(20).unwrap();
        run_pds(&mut survivors, 20);
        assert_eq!(survivors[0].members(), vec![10, 20]);
        drop(survivors);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn pd_replacement_needs_an_initialized_learner_and_can_be_retargeted() {
        let root = std::env::temp_dir().join(format!("vals-pd-retarget-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let addr = |port: u16| -> SocketAddr { format!("127.0.0.1:{port}").parse().unwrap() };
        let mut pds: Vec<_> = (1..=3u64)
            .map(|id| PlacementDriver::new_bounded(id, vec![1, 2, 3], 1 << 20).unwrap())
            .collect();
        pds[0].campaign().unwrap();
        run_pds(&mut pds, 50);
        drop(pds.pop());

        assert!(pds[0].add_pd_learner(4, addr(4)).unwrap());
        run_pds(&mut pds, 50);
        assert!(pds[0].partition.commit_index() < 16, "low commit index: {}", pds[0].partition.commit_index());
        pd_invariant(pds[0].replace_pd_voter_step(3, 4), "has not caught up");

        pds.push(PlacementDriver::open_bounded_joining(4, 1 << 20, root.join("4")).unwrap());
        run_pds(&mut pds, 50);
        assert!(pds[0].replace_pd_voter_step(3, 4).unwrap());
        run_pds(&mut pds, 50);
        assert_eq!(pds[0].pd_voters(), (vec![1, 2], vec![4]));
        pd_invariant(pds[0].remove_pd_learner(4), "reserved to replace pd 3");

        assert!(pds[0].add_pd_learner(5, addr(5)).unwrap());
        pds.push(PlacementDriver::open_bounded_joining(5, 1 << 20, root.join("5")).unwrap());
        run_pds(&mut pds, 50);
        assert!(pds[0].replace_pd_voter_step(3, 5).unwrap(), "the pending replacement moves to learner 5");
        run_pds(&mut pds, 50);
        assert_eq!(pds[0].pd_voters(), (vec![1, 2, 5], vec![4]));
        assert_eq!(pds[1].pd_peer_record(3), Some(PdPeerRecord::Retired { replaced_by: 5 }));
        pd_invariant(pds[0].replace_pd_voter_step(3, 4), "already replaced by pd 5");
        assert!(!pds[0].replace_pd_voter_step(3, 5).unwrap());
        assert!(pds[0].remove_pd_learner(4).unwrap(), "an unreserved learner can be dropped");
        run_pds(&mut pds, 50);
        assert!(!pds[0].remove_pd_learner(4).unwrap());
        for pd in [&pds[0], &pds[1], &pds[3]] {
            assert_eq!(pd.pd_voters(), (vec![1, 2, 5], vec![]));
        }

        pds.remove(2);
        for id in [6, 7] {
            assert!(pds[0].add_pd_learner(id, addr(id as u16)).unwrap());
            pds.push(PlacementDriver::open_bounded_joining(id, 1 << 20, root.join(id.to_string())).unwrap());
            run_pds(&mut pds, 50);
        }
        assert!(pds[0].replace_pd_voter_step(5, 6).unwrap());
        run_pds(&mut pds, 50);
        assert!(pds[0].replace_pd_voter_step(5, 6).unwrap());
        run_pds(&mut pds, 50);
        assert_eq!(pds[0].pd_voters(), (vec![1, 2, 6], vec![7]));
        pd_invariant(pds[0].replace_pd_voter_step(3, 7), "already replaced by pd 5");
        assert_eq!(pds[0].pd_voters(), (vec![1, 2, 6], vec![7]), "a finished replacement never grows the group");
        drop(pds);
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_node_pd_agrees_on_membership() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        assert!(pds[0].is_leader());

        for n in [10, 20, 30, 40] {
            pds[0].add_node(n).unwrap();
            settle_cluster(&mut pds);
        }
        for pd in &pds {
            assert_eq!(pd.members(), vec![10, 20, 30, 40], "all PD replicas agree");
        }

        assert!(matches!(
            pds[1].remove_node(20),
            Err(PartitionError::NotLeader)
        ));
        pds[0].remove_node(20).unwrap();
        settle_cluster(&mut pds);
        for pd in &pds {
            assert_eq!(pd.members(), vec![10, 30, 40]);
        }

        let mut ring = Ring::with_vnodes(3, 64);
        pds[2].refresh_ring(&mut ring);
        assert_eq!(ring.members().collect::<Vec<_>>(), vec![10, 30, 40]);
    }

    #[test]
    fn leader_transition_waits_for_authoritative_state_before_validating_mutations() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        for pd in &mut pds {
            pd.set_global_rf(2).unwrap();
        }
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        pds[0]
            .bootstrap_members(&vec![
                (10, 1, Vec::new()),
                (20, 1, Vec::new()),
                (30, 1, Vec::new()),
            ])
            .unwrap();
        settle_cluster(&mut pds);
        pds[0]
            .create_zone(ZoneConfig {
                name: "protected".into(),
                rf: 2,
                ordering: Ordering::Unsorted,
                scope: None,
            })
            .unwrap();
        settle_cluster(&mut pds);

        pds[1].partition.force_campaign().unwrap();
        for _ in 0..100 {
            drive_cluster_once(&mut pds);
            if pds[1].is_leader() {
                break;
            }
        }
        assert!(pds[1].is_leader(), "second PD did not win leadership");
        assert!(matches!(
            pds[1].remove_node(30),
            Err(PartitionError::NotReady)
        ));
        settle_cluster(&mut pds);

        assert!(matches!(
            pds[0].create_zone(ZoneConfig {
                name: "protected".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            }),
            Err(PartitionError::NotLeader)
        ));
        assert!(matches!(
            pds[1].create_zone(ZoneConfig {
                name: "protected".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            }),
            Err(PartitionError::Invariant(_))
        ));
        pds[1].remove_node(30).unwrap();
        assert!(matches!(
            pds[1].remove_node(20),
            Err(PartitionError::NotReady)
        ));
        settle_cluster(&mut pds);
        let next = pds[1]
            .create_zone(ZoneConfig {
                name: "after-transition".into(),
                rf: 2,
                ordering: Ordering::Unsorted,
                scope: None,
            })
            .unwrap();
        assert_eq!(next, 2);
        settle_cluster(&mut pds);
        assert!(matches!(
            pds[1].remove_node(20),
            Err(PartitionError::Invariant(_))
        ));
        for pd in &pds {
            assert_eq!(pd.members(), vec![10, 20]);
            assert_eq!(pd.zones().by_name("protected"), Some(1));
            assert_eq!(pd.zones().by_name("after-transition"), Some(2));
        }
    }

    #[test]
    fn membership_changes_drive_the_ring() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        assert!(pd.is_leader());

        add_members(&mut pd, &[1, 2, 3]);
        assert_eq!(pd.members(), vec![1, 2, 3]);

        let mut ring = Ring::with_vnodes(3, 64);
        pd.refresh_ring(&mut ring);
        assert_eq!(ring.members().collect::<Vec<_>>(), vec![1, 2, 3]);
        let reps = ring.replicas(b"some-key");
        assert_eq!(reps.len(), 3);
        assert!(reps.iter().all(|n| [1, 2, 3].contains(n)));

        pd.remove_node(2).unwrap();
        settle(&mut pd);
        assert_eq!(pd.members(), vec![1, 3]);

        pd.refresh_ring(&mut ring);
        assert_eq!(ring.members().collect::<Vec<_>>(), vec![1, 3]);
        assert!(ring.replicas(b"some-key").iter().all(|n| *n != 2));
    }

    #[test]
    fn node_weights_commit_and_shape_the_ring() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);

        pd.add_node(1).unwrap();
        settle(&mut pd);
        pd.add_node_weighted(2, 5).unwrap();
        settle(&mut pd);
        pd.add_node(3).unwrap();
        settle(&mut pd);

        assert_eq!(pd.members(), vec![1, 2, 3]);
        assert_eq!(pd.members_weighted(), vec![(1, 1), (2, 5), (3, 1)]);

        let mut ring = Ring::with_vnodes(1, 128);
        pd.refresh_ring(&mut ring);
        assert_eq!(ring.member_weight(2), 5);
        let mut owns2 = 0;
        let total = 3500;
        for i in 0..total {
            if ring.primary(format!("k{i}").as_bytes()) == Some(2) {
                owns2 += 1;
            }
        }
        assert!(
            owns2 as f64 / total as f64 > 0.55,
            "weight-5 node should dominate placement: {owns2}/{total}"
        );
    }

    fn within_secs<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(Duration::from_secs(secs))
            .expect("operation hung past the watchdog")
    }

    #[test]
    fn single_node_daemon_shuts_down_promptly() {
        let pd = PdDaemon::spawn_single(
            1,
            engine(),
            RingConfig {
                rf: 1,
                ..RingConfig::default()
            },
            Vec::new(),
        )
        .unwrap();
        pd.add_node(1).unwrap();
        thread::sleep(Duration::from_millis(50));
        within_secs(10, move || pd.shutdown());
    }

    #[test]
    fn multi_node_daemon_shuts_down_promptly() {
        let (pl1, pa1) = PdDaemon::bind().unwrap();
        let (pl2, pa2) = PdDaemon::bind().unwrap();
        let peers = vec![(1u64, pa1), (2u64, pa2)];
        let rc = RingConfig::default();
        let pd1 = PdDaemon::spawn(1, pl1, peers.clone(), Vec::new(), engine(), rc, true).unwrap();
        let pd2 = PdDaemon::spawn(2, pl2, peers, Vec::new(), engine(), rc, false).unwrap();
        thread::sleep(Duration::from_millis(200));
        within_secs(10, move || {
            pd1.shutdown();
            pd2.shutdown();
        });
    }

    fn zones_of(addr: SocketAddr) -> Vec<wire::OwnedZone> {
        match push_query(addr, &Request::Zones) {
            Response::Zones { zones, .. } => zones,
            other => panic!("expected Zones, got {other:?}"),
        }
    }

    fn bind_reuse(at: Option<SocketAddr>) -> (TcpListener, SocketAddr) {
        use socket2::{Domain, Socket, Type};
        let sock = Socket::new(Domain::IPV4, Type::STREAM, None).unwrap();
        sock.set_reuse_address(true).unwrap();
        let want: SocketAddr = at.unwrap_or_else(|| "127.0.0.1:0".parse().unwrap());
        sock.bind(&want.into()).unwrap();
        sock.listen(128).unwrap();
        let l: TcpListener = sock.into();
        l.set_nonblocking(true).unwrap();
        let addr = l.local_addr().unwrap();
        (l, addr)
    }

    #[test]
    fn restarted_node_is_repushed_without_a_config_change() {
        let ring = Ring::from_members(1, 64, [1]);
        let (l1, addr) = bind_reuse(None);
        let handle = Node::serve(
            1,
            l1,
            data_store(1),
            ring.clone(),
            vec![(1, addr)],
            Duration::from_secs(3600),
        );

        let pd = PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
            .unwrap();
        bootstrap_daemon(&pd, &[1, 2, 3]);
        pd.create_zone(ZoneConfig {
            name: "z".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();

        let deadline = Instant::now() + Duration::from_secs(5);
        let epoch_before = loop {
            let (epoch, _, _) = config_of(addr);
            if epoch > 0 && members_of(addr) == vec![1, 2, 3] && !zones_of(addr).is_empty() {
                break epoch;
            }
            assert!(Instant::now() < deadline, "initial pushes never landed");
            thread::sleep(Duration::from_millis(20));
        };

        within_secs(10, move || handle.shutdown());
        let (l2, addr2) = bind_reuse(Some(addr));
        assert_eq!(addr2, addr);
        let handle2 = Node::serve(
            1,
            l2,
            data_store(1),
            ring,
            vec![(1, addr)],
            Duration::from_secs(3600),
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let (epoch, _, _) = config_of(addr);
            if epoch >= epoch_before
                && members_of(addr) == vec![1, 2, 3]
                && !zones_of(addr).is_empty()
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PD never re-pushed to the restarted node (still at epoch {})",
                config_of(addr).0
            );
            thread::sleep(Duration::from_millis(25));
        }
        assert!(crate::observe::snapshot().pd_repushes > 0);

        within_secs(10, move || {
            pd.shutdown();
            handle2.shutdown();
        });
    }

    fn restart_behind_a_current_members_push(split: bool) {
        let ring = Ring::from_members(1, 64, [1]);
        let (l1, addr) = bind_reuse(None);
        let handle = Node::serve(
            1,
            l1,
            data_store(1),
            ring.clone(),
            vec![(1, addr)],
            Duration::from_secs(3600),
        );

        let pd = PdDaemon::spawn_single(1, engine(), RingConfig::default(), vec![(1, vec![addr])])
            .unwrap();
        bootstrap_daemon(&pd, &[1, 2, 3]);
        pd.create_zone(ZoneConfig {
            name: "s".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let zone_id = loop {
            if let Some(id) = pd.catalog().by_name("s") {
                break id;
            }
            assert!(Instant::now() < deadline, "zone not committed");
            thread::sleep(Duration::from_millis(20));
        };
        if split {
            pd.split_zone(zone_id, b"m".to_vec()).unwrap();
        }

        let range_map_epoch = |addr| match push_query(addr, &Request::RangeMaps) {
            Response::RangeMaps { epoch, .. } => epoch,
            other => panic!("expected RangeMaps, got {other:?}"),
        };
        let deadline = Instant::now() + Duration::from_secs(5);
        let (member_epoch, vnodes, rf, maps_before) = loop {
            let (epoch, vnodes, rf) = config_of(addr);
            let maps = range_map_epoch(addr);
            if epoch > 0
                && (maps > 0) == split
                && members_of(addr) == vec![1, 2, 3]
                && !zones_of(addr).is_empty()
            {
                break (epoch, vnodes, rf, maps);
            }
            assert!(Instant::now() < deadline, "initial pushes never landed");
            thread::sleep(Duration::from_millis(20));
        };

        within_secs(10, move || handle.shutdown());
        let (l2, addr2) = bind_reuse(Some(addr));
        assert_eq!(addr2, addr);
        let handle2 = Node::serve(
            1,
            l2,
            data_store(1),
            ring,
            vec![(1, addr)],
            Duration::from_secs(3600),
        );
        let members = [(1, 1), (2, 1), (3, 1)];
        let addrs = [(1, vec![addr])];
        match push_query(addr, &Request::SetMembers {
            auth: crate::auth::token(),
            epoch: member_epoch,
            vnodes,
            rf,
            members: &members,
            addrs: &addrs,
            incarnation: None,
        }) {
            Response::Members { epoch, .. } => assert_eq!(epoch, member_epoch),
            other => panic!("expected Members, got {other:?}"),
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if range_map_epoch(addr) >= maps_before && !zones_of(addr).is_empty() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "PD never re-pushed zones and range maps to the restarted node \
                 whose ring epoch was already current (range-map epoch {})",
                range_map_epoch(addr)
            );
            thread::sleep(Duration::from_millis(25));
        }

        within_secs(10, move || {
            pd.shutdown();
            handle2.shutdown();
        });
    }

    #[test]
    fn restart_behind_a_current_members_push_is_repushed_after_a_split() {
        restart_behind_a_current_members_push(true);
    }

    #[test]
    fn restart_behind_a_current_members_push_is_repushed_at_range_map_epoch_zero() {
        restart_behind_a_current_members_push(false);
    }

    fn serve_config_epochs(ring_epoch: u64, catalog_epoch: u64) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for conn in listener.incoming() {
                let Ok(mut s) = conn else { continue };
                if crate::proto::server_handshake_blocking(&mut s).is_err() {
                    continue;
                }
                loop {
                    let mut lenb = [0u8; 4];
                    if s.read_exact(&mut lenb).is_err() {
                        break;
                    }
                    let mut frame = vec![0u8; u32::from_le_bytes(lenb) as usize];
                    if s.read_exact(&mut frame).is_err() {
                        break;
                    }
                    let (reqid, payload) = frame.split_at(8);
                    let resp = match wire::decode_request(payload).unwrap() {
                        wire::OwnedRequest::Members => Response::Members {
                            epoch: ring_epoch,
                            vnodes: 64,
                            rf: 1,
                            members: vec![(1, 1)],
                            addrs: Vec::new(),
                        },
                        wire::OwnedRequest::Zones => Response::Zones {
                            epoch: catalog_epoch,
                            zones: Vec::new(),
                        },
                        other => panic!("unexpected beacon request {other:?}"),
                    };
                    let rp = wire::encode_response_payload(&resp);
                    let mut out = ((8 + rp.len()) as u32).to_le_bytes().to_vec();
                    out.extend_from_slice(reqid);
                    out.extend_from_slice(&rp);
                    if s.write_all(&out).is_err() {
                        break;
                    }
                }
            }
        });
        addr
    }

    #[test]
    fn beacon_repushes_an_endpoint_whose_catalog_epoch_fell_behind_a_current_ring_epoch() {
        let beacon = |ring_epoch: u64, catalog_epoch: u64, zones_confirmed: Option<u64>| {
            let addr = serve_config_epochs(ring_epoch, catalog_epoch);
            let mut confirmed = HashMap::from([(addr, 5)]);
            let mut confirmed_zones: HashMap<SocketAddr, u64> =
                zones_confirmed.map(|at| (addr, at)).into_iter().collect();
            let mut confirmed_range_maps = HashMap::from([(addr, 0)]);
            let mut features = HashMap::new();
            let restarted = beacon_repush(
                &[(1, vec![addr])],
                &mut confirmed,
                &mut confirmed_zones,
                &mut confirmed_range_maps,
                &mut features,
            );
            (confirmed.contains_key(&addr), confirmed_range_maps.contains_key(&addr), restarted)
        };
        assert_eq!(beacon(5, 0, Some(3)), (false, false, vec![1]), "catalog epoch 0 < confirmed 3");
        assert_eq!(beacon(4, 3, Some(3)), (false, false, vec![1]), "ring epoch 4 < confirmed 5");
        assert_eq!(beacon(5, 3, Some(3)), (true, true, vec![]));
        assert_eq!(beacon(5, 0, None), (true, true, vec![]), "no zones confirmed, nothing to compare");
    }

    #[test]
    fn proposals_commit_while_a_push_is_stuck_on_a_black_holed_node() {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        thread::spawn(move || {
            let mut held = Vec::new();
            for s in l.incoming() {
                let Ok(s) = s else { continue };
                held.push(s);
            }
        });

        let pd = PdDaemon::spawn_single(
            1,
            engine(),
            RingConfig { vnodes: 128, rf: 1 },
            vec![(1, vec![addr])],
        )
        .unwrap();
        pd.add_node(1).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pd.members() != vec![1] {
            assert!(Instant::now() < deadline, "first change never committed");
            thread::sleep(Duration::from_millis(5));
        }
        thread::sleep(Duration::from_millis(100));
        let t0 = Instant::now();
        pd.add_node(2).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        while pd.members() != vec![1, 2] {
            assert!(Instant::now() < deadline, "second change never committed");
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            t0.elapsed() < Duration::from_millis(500),
            "proposal commit was held up by a stuck push ({:?}): pushes are back \
             on the drive thread",
            t0.elapsed()
        );
        within_secs(10, move || pd.shutdown());
    }

    #[test]
    fn a_black_holed_node_does_not_delay_zone_pushes_to_the_healthy_nodes() {
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let healthy: Vec<_> = (0..2).map(|_| Node::bind().unwrap()).collect();
        let healthy_addrs: Vec<SocketAddr> = healthy.iter().map(|(_, a)| *a).collect();
        let mut nodes = Vec::new();
        for (i, (listener, _)) in healthy.into_iter().enumerate() {
            let node = (i + 1) as NodeId;
            let provider = MemZoneStore::new(node, ZoneGeom {
                block_size: 256,
                seg_blocks: 4,
                reserved_blocks: 0,
                region_blocks: 4096,
            });
            let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
            nodes.push(crate::testutil::running(Node::serve_zoned(
                node, listener, zone0, provider, Catalog::new(), Ring::new(1), Vec::new(),
                Duration::from_secs(3600), None,
            )));
        }
        let black_holed: Vec<SocketAddr> = (0..8)
            .map(|_| {
                let l = TcpListener::bind("127.0.0.1:0").unwrap();
                let addr = l.local_addr().unwrap();
                thread::spawn(move || {
                    let mut held = Vec::new();
                    for s in l.incoming() {
                        let Ok(s) = s else { continue };
                        held.push(s);
                    }
                });
                addr
            })
            .collect();
        let data_nodes = vec![
            (1, vec![healthy_addrs[0]]),
            (2, vec![healthy_addrs[1]]),
            (3, black_holed),
        ];
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(1, engine(), RingConfig { vnodes: 16, rf: 3 }, data_nodes)
                .unwrap(),
        );
        bootstrap_daemon(&pd, &[1, 2, 3]);
        let t0 = Instant::now();
        while !healthy_addrs.iter().all(|&addr| members_of(addr) == [1, 2, 3]) {
            assert!(t0.elapsed() < Duration::from_secs(60), "the healthy nodes were never configured");
            thread::sleep(Duration::from_millis(5));
        }
        eprintln!("membership reached the healthy nodes after {:?}", t0.elapsed());
        let has_zone = |addr: SocketAddr, name: &str| match push_query(addr, &Request::Zones) {
            Response::Zones { zones, .. } => zones.iter().any(|z| z.name == name),
            other => panic!("expected Zones, got {other:?}"),
        };
        let mut worst = Duration::ZERO;
        for round in 0..6 {
            thread::sleep(Duration::from_millis(700));
            let name = format!("outage-{round}");
            let t0 = Instant::now();
            pd.create_zone(ZoneConfig {
                name: name.clone(),
                rf: 3,
                ordering: Ordering::Sorted,
                scope: None,
            })
            .unwrap();
            while !healthy_addrs.iter().all(|&addr| has_zone(addr, &name)) {
                assert!(
                    t0.elapsed() < Duration::from_secs(60),
                    "zone {name} never reached the healthy nodes"
                );
                thread::sleep(Duration::from_millis(5));
            }
            let took = t0.elapsed();
            eprintln!("round {round}: zone reached the healthy nodes after {took:?}");
            worst = worst.max(took);
        }
        assert!(
            worst < Duration::from_secs(1),
            "a zone took {worst:?} to reach the healthy nodes while one node was black-holed"
        );
        drop(pd);
        drop(nodes);
    }

    fn one_voter_daemon() -> PdDaemon {
        let (pl, pa) = PdDaemon::bind().unwrap();
        PdDaemon::spawn(
            1,
            pl,
            vec![(1, pa)],
            Vec::new(),
            engine(),
            RingConfig {
                rf: 1,
                ..RingConfig::default()
            },
            true,
        )
        .unwrap()
    }

    #[test]
    fn inverse_membership_intents_execute_in_order() {
        let pd = one_voter_daemon();
        pd.add_node(1).unwrap();
        pd.add_node(7).unwrap();
        pd.remove_node(7).unwrap();
        pd.add_node(8).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while pd.members() != vec![1, 8] {
            assert!(
                Instant::now() < deadline,
                "inverse intents not applied in order: members = {:?} (remove(7) \
                 was silently dropped?)",
                pd.members()
            );
            thread::sleep(Duration::from_millis(10));
        }

        pd.add_node_weighted(8, 5).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while pd.members_weighted() != vec![(1, 1), (8, 5)] {
            assert!(
                Instant::now() < deadline,
                "re-weight intent silently dropped: {:?}",
                pd.members_weighted()
            );
            thread::sleep(Duration::from_millis(10));
        }
        within_secs(10, move || pd.shutdown());
    }

    #[test]
    fn create_then_drop_zone_executes_both_in_order() {
        let pd = one_voter_daemon();
        pd.add_node(1).unwrap();
        pd.create_zone(ZoneConfig {
            name: "ephemeral".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();
        pd.drop_zone(1).unwrap();
        pd.create_zone(ZoneConfig {
            name: "keep".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let cat = pd.catalog();
            if cat.by_name("keep").is_some() && !cat.contains(1) && cat.len() == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "create-then-drop not applied in order: catalog ids = {:?} (the \
                 drop was silently dropped?)",
                pd.catalog().ids()
            );
            thread::sleep(Duration::from_millis(10));
        }
        within_secs(10, move || pd.shutdown());
    }

    #[test]
    fn admin_barrier_commits_or_times_out_without_reproposal() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        assert_eq!(pds[1].status().leader, Some(1));
        assert!(matches!(pds[1].add_node(7), Err(PartitionError::NotLeader)));
        let mut waits = Vec::new();
        let mut ops = Vec::new();
        let mut through = 0;
        let (tx, rx) = mpsc::channel();
        handle_admin(&mut pds[0], AdminMutation::Add(7, 1, Vec::new()),
            Instant::now() + Duration::from_secs(1), tx, &mut ops, &mut waits, &mut through);
        assert!(rx.try_recv().is_err());
        settle_cluster(&mut pds);
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(matches!(rx.try_recv().unwrap(), Ok(AdminOutcome::Committed { .. })));
        assert_eq!(pds[0].members(), vec![7]);

        let (tx, rx) = mpsc::channel();
        handle_admin(&mut pds[0], AdminMutation::Add(8, 1, Vec::new()),
            Instant::now() + Duration::from_secs(1), tx, &mut ops, &mut waits, &mut through);
        for _ in 0..10 { drive_partitioned(&mut pds, 1); }
        waits[0].deadline = Some(Instant::now());
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(matches!(rx.try_recv().unwrap(), Err(PartitionError::Unknown { .. })));
        for _ in 0..50 { pds[0].tick(); drive_cluster_once(&mut pds); }
        assert_eq!(pds[0].members(), vec![7, 8]);
    }

    #[test]
    fn admin_expired_queue_command_is_not_proposed() {
        let mut pd = PlacementDriver::new_single(1, engine()).unwrap();
        settle(&mut pd);
        let before = pd.last_index();
        let (tx, rx) = mpsc::channel();
        handle_admin(&mut pd, AdminMutation::Add(99, 1, Vec::new()),
            Instant::now() - Duration::from_millis(1), tx,
            &mut Vec::new(), &mut Vec::new(), &mut 0);
        assert!(matches!(rx.try_recv().unwrap(), Err(PartitionError::AdminQueueExpired)));
        assert_eq!(pd.last_index(), before);
        settle(&mut pd);
        assert!(pd.members().is_empty());
    }

    #[test]
    fn admin_committed_entry_survives_same_drive_snapshot_compaction() {
        let partition = Partition::new_single(1, engine()).unwrap().with_snapshot_interval(1);
        let mut pd = PlacementDriver::from_partition(partition).unwrap();
        settle(&mut pd);
        let (tx, rx) = mpsc::channel();
        let mut waits = Vec::new();
        let mut through = 0;
        handle_admin(&mut pd, AdminMutation::Add(7, 1, Vec::new()),
            Instant::now() + Duration::from_secs(1), tx,
            &mut Vec::new(), &mut waits, &mut through);
        let index = pd.last_index();
        settle(&mut pd);
        assert!(pd.partition.snapshot_index() >= index);
        assert_eq!(pd.partition.entry_term(index), None);
        settle_admin(&mut pd, &mut waits, &mut Vec::new(), &mut Vec::new());
        assert!(matches!(rx.try_recv().unwrap(), Ok(AdminOutcome::Committed { index: i, .. }) if i == index));
        assert_eq!(pd.members(), vec![7]);
    }

    #[test]
    fn timed_out_presplit_create_installs_only_after_its_entry_commits() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        pds[0].add_node(7).unwrap();
        settle_cluster(&mut pds);
        let cfg = ZoneConfig {
            name: "late".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
        };
        let id = pds[0].next_zone_id();
        let mut ops = Vec::new();
        let mut waits = Vec::new();
        let (tx, rx) = mpsc::channel();
        let mut through = 0;
        handle_admin(&mut pds[0], AdminMutation::CreateZone(cfg.clone(), vec![b"m".to_vec()]),
            Instant::now() + Duration::from_secs(1), tx, &mut ops, &mut waits, &mut through);
        for _ in 0..10 { drive_partitioned(&mut pds, 1); }
        waits[0].deadline = Some(Instant::now());
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(matches!(rx.try_recv().unwrap(), Err(PartitionError::Unknown { .. })));
        assert_eq!(waits.len(), 1);
        assert_eq!(initial_map_decision(&pds[0], id, &ops[0].2), InitialMapDecision::Wait);
        for _ in 0..50 { pds[0].tick(); drive_cluster_once(&mut pds); }
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(waits.is_empty());
        assert_eq!(pds[0].zones().get(id), Some(&cfg));
        assert_eq!(initial_map_decision(&pds[0], id, &ops[0].2), InitialMapDecision::Ready);
        let mut state = PushState::new();
        state.members = pds[0].members_addressed();
        let push_state = Mutex::new(state);
        let authority = Mutex::new(BalancePollAuthority::default());
        assert!(resolve_initial_map(&pds[0], RingConfig::default(),
            &push_state, &authority, id, true, &mut ops[0].2));
        assert!(propose_range_op(&mut pds[0], id, b"", &ops[0].2).unwrap());
        settle_cluster(&mut pds);
        assert_eq!(pds[0].range_map(id).splits(), [b"m".to_vec()]);
    }

    #[test]
    fn initial_map_apply_cannot_overwrite_a_newer_committed_layout() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        pds[0].add_node(7).unwrap();
        settle_cluster(&mut pds);
        let cfg = ZoneConfig {
            name: "layout".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
        };
        let id = pds[0].create_zone(cfg.clone()).unwrap();
        settle_cluster(&mut pds);
        let newer = RangeMap::from_splits([b"t".to_vec()]);
        let stale = RangeMap::from_splits([b"m".to_vec()]);
        pds[0].set_range_map(id, &newer).unwrap();
        pds[1].set_initial_range_map(id, &cfg, &stale).unwrap();
        pds[1].drive().unwrap();
        let before = pds[0].last_index();
        let forwarded: Vec<_> = pds[1].take_outgoing().into_iter()
            .filter(|msg| msg.to == 1).collect();
        assert!(!forwarded.is_empty());
        for msg in forwarded { pds[0].step(msg).unwrap(); }
        assert!(pds[0].last_index() > before);
        settle_cluster(&mut pds);
        for pd in &pds {
            assert_eq!(pd.range_map(id), newer);
        }
        let reflected = RangeOpKind::InstallInitialMap {
            map: newer.clone(), legacy: newer.clone(), cfg: cfg.clone(),
            creation_index: pds[0].epoch(), creation_term: pds[0].partition.term(),
            authorized: true, placement: InitialMapPlacement::Legacy,
        };
        assert_eq!(initial_map_decision(&pds[0], id, &reflected), InitialMapDecision::Ready);
        assert!(range_op_reflected(&pds[0].range_map(id), b"", &reflected));
        let mut ops = vec![
            (id, Vec::new(), RangeOpKind::InstallInitialMap {
                map: stale.clone(), legacy: stale, cfg,
                creation_index: pds[0].epoch(), creation_term: pds[0].partition.term(),
                authorized: true, placement: InitialMapPlacement::Legacy,
            }),
            (id, b"z".to_vec(), RangeOpKind::Split(
                SplitMode::Inherit, CanonicalTarget::Legacy,
            )),
        ];
        assert_eq!(initial_map_queue_head(&pds[0], &mut ops), InitialMapDecision::Discard);
        assert_eq!(ops.len(), 1);
        assert_eq!(initial_map_queue_head(&pds[0], &mut ops), InitialMapDecision::Ready);
        let (zone, at, kind) = ops.remove(0);
        assert!(propose_range_op(&mut pds[0], zone, &at, &kind).unwrap());
        settle_cluster(&mut pds);
        for pd in &pds {
            assert_eq!(pd.range_map(id).splits(), [b"t".to_vec(), b"z".to_vec()]);
        }
    }

    #[test]
    fn superseded_presplit_create_cannot_install_map_on_replacement_zone() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        pds[0].add_node(7).unwrap();
        settle_cluster(&mut pds);
        let x = ZoneConfig {
            name: "x".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
        };
        let y = x.clone();
        let id = pds[0].next_zone_id();
        let mut ops = Vec::new();
        let mut waits = Vec::new();
        let (tx, rx) = mpsc::channel();
        let mut through = 0;
        handle_admin(&mut pds[0], AdminMutation::CreateZone(x.clone(), vec![b"m".to_vec()]),
            Instant::now() + Duration::from_secs(1), tx, &mut ops, &mut waits, &mut through);
        assert_eq!(ops.len(), 1);
        assert_eq!(initial_map_decision(&pds[0], id, &ops[0].2), InitialMapDecision::Wait);
        pds[0].drive().unwrap();
        pds[0].take_outgoing();
        waits[0].deadline = Some(Instant::now());
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(matches!(rx.try_recv().unwrap(), Err(PartitionError::Unknown { .. })));
        assert_eq!(waits.len(), 1);
        assert_eq!(ops.len(), 1);

        pds[1].partition.force_campaign().unwrap();
        for _ in 0..100 { pds[1].tick(); drive_partitioned(&mut pds, 1); }
        assert!(pds[1].is_caught_up_leader());
        assert_eq!(pds[1].create_zone(y.clone()).unwrap(), id);
        for _ in 0..100 { pds[1].tick(); drive_partitioned(&mut pds, 1); }
        let replacement = RangeMap::from_splits([b"t".to_vec()]);
        pds[1].set_range_map(id, &replacement).unwrap();
        for _ in 0..100 { pds[1].tick(); drive_partitioned(&mut pds, 1); }
        assert_eq!(pds[1].zones().get(id), Some(&y));
        assert_eq!(pds[1].range_map(id), replacement);
        for _ in 0..100 { pds[1].tick(); drive_cluster_once(&mut pds); }
        assert_eq!(pds[0].zones().get(id), Some(&y));
        let stale = ops[0].2.clone();
        assert_eq!(initial_map_decision(&pds[0], id, &stale), InitialMapDecision::Discard);
        assert!(matches!(propose_range_op(&mut pds[1], id, b"", &stale),
            Err(PartitionError::Invariant(_))));
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(ops.is_empty());
        for pd in &pds {
            assert_eq!(pd.zones().get(id), Some(&y));
            assert_eq!(pd.range_map(id), replacement);
        }
        let mut wrong = stale;
        if let RangeOpKind::InstallInitialMap { cfg, .. } = &mut wrong {
            cfg.name = "wrong".into();
        }
        let mut wrong_ops = vec![(id, Vec::new(), wrong)];
        assert_eq!(initial_map_queue_head(&pds[1], &mut wrong_ops), InitialMapDecision::Discard);
        assert!(wrong_ops.is_empty());
    }

    #[test]
    fn admin_barrier_superseded_and_stale_leader_read_is_not_authoritative() {
        let mut pds = vec![
            PlacementDriver::new(1, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(2, vec![1, 2, 3], engine()).unwrap(),
            PlacementDriver::new(3, vec![1, 2, 3], engine()).unwrap(),
        ];
        pds[0].campaign().unwrap();
        settle_cluster(&mut pds);
        pds[0].mint_identity(Some(7), None).unwrap();
        settle_cluster(&mut pds);
        let (tx, rx) = mpsc::channel();
        let mut reads = Vec::new();
        let mut context = 0;
        inspect(&mut pds[0], tx, true, &mut reads, &mut context, Duration::from_secs(1));
        settle_cluster(&mut pds);
        settle_admin(&mut pds[0], &mut Vec::new(), &mut reads, &mut Vec::new());
        let confirmed = rx.try_recv().unwrap();
        assert!(confirmed.authoritative.is_some());
        assert_eq!(confirmed.state.as_deref(), Some(pds[0].state_entries().as_slice()));
        assert!(!pds[0].state_entries().is_empty());

        let (tx, rx) = mpsc::channel();
        inspect(&mut pds[1], tx, true, &mut reads, &mut context, Duration::from_secs(1));
        let follower = rx.try_recv().unwrap();
        assert!(follower.authoritative.is_none() && follower.state.is_none(), "a follower never exports state");

        let (tx, rx) = mpsc::channel();
        inspect(&mut pds[0], tx, true, &mut reads, &mut context, Duration::from_millis(1));
        let mut waits = Vec::new();
        let mut ops = Vec::new();
        let mut through = 0;
        let (mut_tx, mut_rx) = mpsc::channel();
        handle_admin(&mut pds[0], AdminMutation::Add(9, 1, Vec::new()),
            Instant::now() + Duration::from_secs(1), mut_tx, &mut ops, &mut waits, &mut through);
        let original_index = pds[0].last_index();
        let original_term = pds[0].partition.entry_term(original_index).unwrap();
        for _ in 0..10 { drive_partitioned(&mut pds, 1); }
        reads[0].deadline = Instant::now();
        settle_admin(&mut pds[0], &mut waits, &mut reads, &mut ops);
        let stale = rx.try_recv().unwrap();
        assert!(stale.authoritative.is_none());
        assert!(stale.state.is_none(), "an unconfirmed leader never exports state");

        pds[1].partition.force_campaign().unwrap();
        for _ in 0..100 {
            drive_partitioned(&mut pds, 1);
            if pds[1].is_leader() { break; }
        }
        assert!(pds[1].is_leader());
        pds[1].drive().unwrap();
        let replacement = pds[1].take_outgoing();
        for msg in replacement {
            if msg.to == 1 { pds[0].step(msg).unwrap(); }
        }
        pds[0].drive().unwrap();
        assert_ne!(pds[0].partition.entry_term(original_index), Some(original_term));
        assert!(pds[0].epoch() < original_index);
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(mut_rx.try_recv().is_err());
        for _ in 0..50 { pds[1].tick(); drive_partitioned(&mut pds, 1); }
        assert!(pds[1].is_caught_up_leader());
        pds[1].add_node(10).unwrap();
        for _ in 0..100 { drive_partitioned(&mut pds, 1); }
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(mut_rx.try_recv().is_err());
        for _ in 0..50 { pds[1].tick(); drive_cluster_once(&mut pds); }
        settle_admin(&mut pds[0], &mut waits, &mut Vec::new(), &mut ops);
        assert!(matches!(mut_rx.try_recv().unwrap(), Err(PartitionError::Superseded { .. })));
        assert_eq!(pds[0].members(), vec![10]);
    }

    #[test]
    fn restored_state_rewrites_addresses_incarnation_and_lineage_and_drops_in_flight_moves() {
        let mut pd = PlacementDriver::new_single_bounded(1, 1 << 20).unwrap();
        settle(&mut pd);
        let old: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        pd.add_node_addressed(1, 2, &[old]).unwrap();
        settle(&mut pd);
        let sorted = |name: &str| ZoneConfig { name: name.into(), rf: 1, ordering: Ordering::Sorted, scope: None };
        let zone = pd.create_zone(sorted("s")).unwrap();
        settle(&mut pd);
        assert_eq!(pd.create_zone(sorted("legacy")).unwrap(), zone + 1);
        settle(&mut pd);
        assert!(pd.split_zone(zone, b"m").unwrap());
        settle(&mut pd);
        assert!(pd.prepare_rehome(zone, b"m").unwrap());
        settle(&mut pd);
        let index = pd.acquire_balance_intent(7).unwrap();
        settle_through(&mut pd, index);
        let index = pd.mint_identity(Some(5), Some(1)).unwrap();
        settle_through(&mut pd, index);
        let mut entries = pd.state_entries();
        let mut legacy = RangeMap::single();
        assert!(legacy.split(b"k") && legacy.set_pending(1, b"k".to_vec()));
        entries.push((range_map_key(zone + 1), legacy.encode()));
        entries.push((pd_peer_key(4), encode_pd_peer(&PdPeerRecord::Member(old))));
        entries.push((pd_peer_key(3), encode_pd_peer(&PdPeerRecord::Retired { replaced_by: 4 })));
        entries.push(seed_voters_entry(&[1, 2, 3]));
        entries.push(seed_ack_entry(2, 7));
        entries.sort();
        assert!(restore_seed_of(&entries).is_some());
        let source = view_state(&entries);
        assert_eq!(source.range_maps[&zone].active_rehomes(), 1);
        assert_eq!(source.range_maps[&(zone + 1)].active_rehomes(), 1);

        let new: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let lineage = Lineage { predecessor: 5, backup_id: 77, source_applied: 12 };
        let target: AddressedMembers = vec![(1, 2, vec![new])];
        let restored = restored_state(&entries, &target, 9, lineage).unwrap();
        let view = view_state(&restored);
        assert_eq!(view.incarnation, Some(9));
        assert_eq!(view.lineage, Some(lineage));
        assert_eq!(view.members, target);
        assert_eq!(view.zones, source.zones);
        for id in [zone, zone + 1] {
            assert_eq!(view.range_maps[&id].active_rehomes(), 0, "zone {id}");
            assert_eq!(view.range_maps[&id], without_pending(&source.range_maps[&id]), "zone {id}");
        }
        assert!(!restored.iter().any(|(key, _)| *key == balance_lock_key()));
        assert!(!restored.iter().any(|(key, _)| pd_peer_id_of_key(key).is_some()),
            "the source PD group's replica records do not describe the restored group");
        assert_eq!(restore_seed_of(&restored), None, "the source group's restore seed records are dropped");
        assert!(restored.contains(&(ring_rf_key(), encode_ring_rf(1))), "a restore keeps the recorded ring rf");
        assert!(restored.windows(2).all(|pair| pair[0].0 < pair[1].0));

        let wrong_weight = restored_state(&entries, &vec![(1, 1, vec![new])], 9, lineage).unwrap_err();
        assert!(wrong_weight.contains("weight"), "{wrong_weight}");
        assert!(restored_state(&entries, &vec![(1, 2, vec![new]), (2, 1, vec![new])], 9, lineage).is_err());
        let no_addr = restored_state(&entries, &vec![(1, 2, Vec::new())], 9, lineage).unwrap_err();
        assert!(no_addr.contains("no data address"), "{no_addr}");
        assert!(restored_state(&entries, &target, 5, lineage).is_err(), "the source incarnation is refused");
        assert!(restored_state(&entries, &target, 0, lineage).is_err());

        let second = Lineage { predecessor: 9, backup_id: 78, source_applied: 30 };
        let again = restored_state(&restored, &vec![(1, 2, vec![old])], 11, second).unwrap();
        assert_eq!(view_state(&again).lineage, Some(second));
    }
