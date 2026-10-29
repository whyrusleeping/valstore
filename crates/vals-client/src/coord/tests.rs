use super::*;

    use crate::net::NetTransport;

    #[test]
    fn ring_view_adopts_weights_and_every_shard_address() {
        let seed: std::net::SocketAddr = "127.0.0.1:7001".parse().unwrap();
        let transport = NetTransport::connect(&[(1, seed)]);
        let mut ring = Ring::new(3);
        ring.set_members_weighted([(1, 1)]);
        let coord = Coordinator::new(1000, ring, 1, transport);
        let addrs = [
            (
                1,
                vec![
                    "127.0.0.1:7001".parse().unwrap(),
                    "127.0.0.1:7011".parse().unwrap(),
                ],
            ),
            (2, vec!["127.0.0.1:7002".parse().unwrap()]),
            (3, vec!["127.0.0.1:7003".parse().unwrap()]),
        ];

        let members = coord.adopt_ring_view(RingView {
            epoch: 9,
            vnodes: 64,
            rf: 2,
            members: vec![(1, 2), (2, 1), (3, 1)],
            addrs: addrs.to_vec(),
        });

        assert_eq!(members, vec![1, 2, 3]);
        assert_eq!(coord.ring_epoch(), 9);
        assert_eq!(coord.ring().rf(), 2);
        assert_eq!(coord.ring().vnodes(), 64);
        assert_eq!(
            coord.ring().members_weighted().collect::<Vec<_>>(),
            vec![(1, 2), (2, 1), (3, 1)]
        );
        for (node, expected) in addrs {
            assert_eq!(coord.transport().peer_shard_count(node), expected.len());
        }
    }

    #[test]
    fn coordinator_and_handoff_acknowledge_won_and_lost_but_not_rejected() {
        let lost = Applied::Lost(Version { hlc: 9, node: 2 });
        assert!(applied_is_ack(Applied::Won));
        assert!(applied_is_ack(lost));
        assert!(!applied_is_ack(Applied::Rejected));
        assert!(matches!(classify_apply(&Ok(Applied::Won)), AckKind::Acked));
        assert!(matches!(classify_apply(&Ok(lost)), AckKind::Acked));
        assert!(matches!(
            classify_apply(&Ok(Applied::Rejected)),
            AckKind::Skewed
        ));
    }

    struct BorrowCheckingReads {
        keys: Vec<(usize, usize)>,
    }

    impl Transport for BorrowCheckingReads {
        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            assert!(self
                .keys
                .iter()
                .any(|&(ptr, len)| ptr == key.as_ptr() as usize && len == key.len()));
            Ok(None)
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }
    }

    #[test]
    fn read_many_borrows_destination_keys_until_transport_admission() {
        let keys = vec![b"one".to_vec(), b"a-longer-second-key".to_vec()];
        let transport = BorrowCheckingReads {
            keys: keys
                .iter()
                .map(|key| (key.as_ptr() as usize, key.len()))
                .collect(),
        };
        let mut ring = Ring::new(1);
        ring.add_node(1);
        let coord = Coordinator::new(9, ring, 1, transport).with_read_fanout(ReadFanout::All);

        assert_eq!(
            run_ready(coord.read_many(&keys)).unwrap(),
            vec![Ok(None), Ok(None)]
        );
    }

    #[derive(Default)]
    struct RecordingReads {
        nodes: RefCell<Vec<NodeId>>,
    }

    impl Transport for RecordingReads {
        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            self.nodes.borrow_mut().push(node);
            Ok(None)
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }
    }

    #[test]
    fn all_read_many_contacts_owners_beyond_the_read_quorum() {
        let key = b"key".to_vec();
        let mut ring = Ring::new(3);
        ring.set_members([1, 2, 3]);
        let replicas = ring.replicas(&key);

        let quorum = Coordinator::new(9, ring.clone(), 2, RecordingReads::default())
            .with_read_fanout(ReadFanout::Quorum);
        assert_eq!(run_ready(quorum.read_many(std::slice::from_ref(&key))).unwrap(), vec![Ok(None)]);
        let mut quorum_nodes = quorum.transport().nodes.borrow().clone();
        quorum_nodes.sort_unstable();
        let mut expected_quorum = replicas[..2].to_vec();
        expected_quorum.sort_unstable();
        assert_eq!(quorum_nodes, expected_quorum);

        let all = Coordinator::new(9, ring, 2, RecordingReads::default())
            .with_read_fanout(ReadFanout::All);
        assert_eq!(run_ready(all.read_many(&[key])).unwrap(), vec![Ok(None)]);
        let mut all_nodes = all.transport().nodes.borrow().clone();
        all_nodes.sort_unstable();
        let mut expected_all = replicas;
        expected_all.sort_unstable();
        assert_eq!(all_nodes, expected_all);
        assert_eq!(all_nodes.len(), 3);
    }

    struct SelectiveRetention {
        rejected_bytes: usize,
    }

    impl Transport for SelectiveRetention {
        fn retain_response(
            &self,
            _node: NodeId,
            bytes: usize,
        ) -> Result<Option<Credit>, TransportError> {
            if bytes == self.rejected_bytes {
                Err(TransportError::Overloaded)
            } else {
                Ok(None)
            }
        }

        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            let len = if key == b"overloaded" {
                self.rejected_bytes
            } else {
                7
            };
            Ok(Some(Stored {
                version: Version { hlc: 7, node: 1 },
                value: Some(vec![b'x'; len]),
            }))
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }
    }

    #[test]
    fn digest_read_many_keeps_other_entries_when_winner_retention_overloads() {
        let mut ring = Ring::new(2);
        ring.set_members([1, 2]);
        let coord = Coordinator::new(
            9,
            ring,
            2,
            SelectiveRetention {
                rejected_bytes: 137,
            },
        );
        let keys = vec![b"overloaded".to_vec(), b"healthy".to_vec()];

        let got = run_ready(coord.read_many(&keys)).unwrap();
        assert_eq!(got[0], Err(CoordError::Overloaded));
        assert_eq!(
            got[1].as_ref().unwrap().as_ref().unwrap().value.as_deref(),
            Some(&vec![b'x'; 7][..])
        );
    }

    struct FallbackRetention {
        primary: NodeId,
        winner: NodeId,
        retained: RefCell<HashMap<NodeId, usize>>,
    }

    impl Transport for FallbackRetention {
        fn retain_response(
            &self,
            node: NodeId,
            _bytes: usize,
        ) -> Result<Option<Credit>, TransportError> {
            let mut retained = self.retained.borrow_mut();
            let count = retained.entry(node).or_default();
            *count += 1;
            if node == self.winner && *count == 2 {
                Err(TransportError::Overloaded)
            } else {
                Ok(None)
            }
        }

        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            let hlc = if node == self.primary { 1 } else { 2 };
            Ok(Some(Stored {
                version: Version { hlc, node: node.into() },
                value: Some(vec![hlc as u8]),
            }))
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }
    }

    #[test]
    fn digest_read_many_classifies_fallback_retention_overload_per_entry() {
        let mut ring = Ring::new(2);
        ring.set_members([1, 2]);
        let replicas = ring.replicas(b"key");
        let coord = Coordinator::new(
            9,
            ring,
            2,
            FallbackRetention {
                primary: replicas[0],
                winner: replicas[1],
                retained: RefCell::new(HashMap::new()),
            },
        );

        let got = run_ready(coord.read_many(&[b"key".to_vec()])).unwrap();
        assert_eq!(got, vec![Err(CoordError::Overloaded)]);
    }

    struct ScriptedFdTransport {
        applies: RefCell<std::collections::VecDeque<Result<Applied, TransportError>>>,
        batches: RefCell<std::collections::VecDeque<Vec<BatchAck>>>,
    }

    impl Transport for ScriptedFdTransport {
        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            self.applies.borrow_mut().pop_front().unwrap()
        }

        async fn read(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            Ok(None)
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }

        async fn apply_batch(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _sync: bool,
            _writes: &[BatchEntry<'_>],
        ) -> Vec<BatchAck> {
            self.batches.borrow_mut().pop_front().unwrap()
        }
    }

    fn scripted_fd_coord(transport: ScriptedFdTransport) -> Coordinator<ScriptedFdTransport> {
        let mut ring = Ring::new(1);
        ring.add_node(1);
        Coordinator::new(9, ring, 1, transport).with_failure_detector(crate::failure::FdConfig {
            failure_threshold: 2,
            open_cooldown_ms: 1000,
        })
    }

    #[test]
    fn local_write_overload_is_neutral_to_failure_detector_streak() {
        let coord = scripted_fd_coord(ScriptedFdTransport {
            applies: RefCell::new(
                vec![
                    Err(TransportError::Unavailable),
                    Err(TransportError::Overloaded),
                    Err(TransportError::Unavailable),
                ]
                .into(),
            ),
            batches: RefCell::new(Default::default()),
        });

        assert!(matches!(
            run_ready(coord.write(b"key", b"value")),
            Err(CoordError::QuorumFailed { .. })
        ));
        assert!(!coord.is_node_down(1));
        assert_eq!(
            run_ready(coord.write(b"key", b"value")),
            Err(CoordError::Overloaded)
        );
        assert!(!coord.is_node_down(1));
        assert!(matches!(
            run_ready(coord.write(b"key", b"value")),
            Err(CoordError::QuorumFailed { .. })
        ));
        assert!(coord.is_node_down(1));
    }

    #[test]
    fn local_batch_overload_is_neutral_to_failure_detector_streak() {
        let coord = scripted_fd_coord(ScriptedFdTransport {
            applies: RefCell::new(Default::default()),
            batches: RefCell::new(
                vec![
                    vec![BatchAck::Unreachable],
                    vec![BatchAck::Overloaded],
                    vec![BatchAck::Unreachable],
                ]
                .into(),
            ),
        });
        let writes = [(b"key".as_slice(), b"value".as_slice())];

        let first = run_ready(coord.write_batch(&writes, WriteConcern::All)).unwrap();
        assert!(matches!(first.outcomes[0], WriteOutcome::Failed { .. }));
        assert!(!coord.is_node_down(1));
        let overloaded = run_ready(coord.write_batch(&writes, WriteConcern::All)).unwrap();
        assert!(matches!(
            overloaded.outcomes[0],
            WriteOutcome::Failed {
                error: CoordError::Overloaded,
                ..
            }
        ));
        assert!(!coord.is_node_down(1));
        let second = run_ready(coord.write_batch(&writes, WriteConcern::All)).unwrap();
        assert!(matches!(second.outcomes[0], WriteOutcome::Failed { .. }));
        assert!(coord.is_node_down(1));
    }

    #[derive(Default)]
    struct CountingWrites {
        applies: Cell<usize>,
        batches: RefCell<Vec<(bool, usize)>>,
    }

    impl Transport for CountingWrites {
        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            self.applies.set(self.applies.get() + 1);
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            Ok(None)
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            Ok(RingView::default())
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            Ok(Catalog::new())
        }

        async fn apply_batch(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            sync: bool,
            writes: &[BatchEntry<'_>],
        ) -> Vec<BatchAck> {
            self.batches.borrow_mut().push((sync, writes.len()));
            vec![BatchAck::Applied(Applied::Won); writes.len()]
        }
    }

    fn rf3_coord() -> Coordinator<CountingWrites> {
        let mut ring = Ring::new(3);
        for node in 1..=3 {
            ring.add_node(node);
        }
        Coordinator::new(9, ring, 2, CountingWrites::default())
    }

    #[test]
    fn impossible_write_options_are_rejected_before_any_replica_is_written() {
        let coord = rf3_coord();
        let invalid = [
            WriteOptions::buffered(WriteConcern::Quorum(0)),
            WriteOptions::buffered(WriteConcern::Quorum(4)),
            WriteOptions::device_durable(WriteConcern::Quorum(0)),
            WriteOptions::device_durable(WriteConcern::Unacknowledged),
        ];
        for options in invalid {
            let want = Err(CoordError::InvalidWriteOptions {
                options,
                replicas: 3,
            });
            assert_eq!(run_ready(coord.write_with(b"k", b"v", options)), want);
            assert_eq!(run_ready(coord.try_write(b"k", b"v", options)), want);
            assert_eq!(run_ready(coord.delete(b"k", options)), want);
            assert_eq!(
                run_ready(coord.write_sloppy(b"k", b"v", options)).map(|ack| ack.version),
                want
            );
            let batch = run_ready(coord.write_batch(&[(b"k", b"v")], options)).unwrap();
            assert_eq!(
                batch.outcomes,
                vec![WriteOutcome::Failed {
                    error: CoordError::InvalidWriteOptions {
                        options,
                        replicas: 3,
                    },
                    acks: 0,
                }]
            );
        }
        assert_eq!(coord.transport().applies.get(), 0);
        assert!(coord.transport().batches.borrow().is_empty());

        assert!(run_ready(coord.write_with(b"k", b"v", WriteConcern::Quorum(3))).is_ok());
        assert!(run_ready(coord.write_with(b"k", b"v", WriteConcern::Unacknowledged)).is_ok());
        assert_eq!(coord.transport().applies.get(), 6);
        assert!(coord.transport().batches.borrow().is_empty());
    }

    #[test]
    fn default_writes_are_buffered_and_device_durability_is_explicit() {
        let coord = rf3_coord();
        assert_eq!(
            WriteOptions::default(),
            WriteOptions::buffered(WriteConcern::All)
        );
        assert_eq!(
            WriteOptions::from(WriteConcern::Majority),
            WriteOptions::buffered(WriteConcern::Majority)
        );

        run_ready(coord.write(b"k", b"v")).unwrap();
        run_ready(coord.write_batch(&[(b"k", b"v")], WriteConcern::All)).unwrap();
        assert_eq!(coord.transport().applies.get(), 3);
        assert_eq!(*coord.transport().batches.borrow(), vec![(false, 1); 3]);

        coord.transport().batches.borrow_mut().clear();
        let synced = WriteOptions::device_durable(WriteConcern::Majority);
        run_ready(coord.write_with(b"k", b"v", synced)).unwrap();
        run_ready(coord.write_batch(&[(b"k", b"v")], synced)).unwrap();
        assert_eq!(coord.transport().applies.get(), 3);
        assert_eq!(*coord.transport().batches.borrow(), vec![(true, 1); 6]);
    }

    #[test]
    fn a_rejected_batch_entry_is_not_stamped() {
        let coord = rf3_coord().with_clock(|| 1000);
        let before = coord.stamp_at(1000);
        let batch = run_ready(coord.write_batch(&[(b"k", b"v")], WriteConcern::Quorum(0))).unwrap();
        assert!(matches!(
            batch.outcomes[0],
            WriteOutcome::Failed {
                error: CoordError::InvalidWriteOptions { .. },
                acks: 0
            }
        ));
        assert_eq!(coord.stamp_at(1000).hlc, before.hlc + 1);
    }

    #[derive(Default)]
    struct ShrinkingZone {
        refreshed: Cell<bool>,
        retry_deletes: Cell<usize>,
    }

    impl Transport for ShrinkingZone {
        async fn apply(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            Ok(None)
        }

        async fn members(&self, _node: NodeId) -> Result<RingView, TransportError> {
            self.refreshed.set(true);
            Ok(RingView {
                epoch: 2,
                members: vec![(1, 1), (2, 1)],
                ..RingView::default()
            })
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            let mut cat = Catalog::new();
            cat.insert(
                1,
                crate::zone::ZoneConfig {
                    name: "sorted".into(),
                    rf: if self.refreshed.get() { 2 } else { 3 },
                    ordering: Ordering::Sorted,
                    scope: None,
                },
            );
            Ok(cat)
        }

        fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
            Some(true)
        }

        async fn range_delete(
            &self,
            _node: NodeId,
            _zone_id: ZoneId,
            _sync: bool,
            _version: Version,
            slices: &[crate::wire::WireSlice<'_>],
        ) -> Vec<BatchAck> {
            if self.refreshed.get() {
                self.retry_deletes.set(self.retry_deletes.get() + 1);
                vec![BatchAck::Applied(Applied::Won); slices.len()]
            } else {
                vec![BatchAck::Stale(2); slices.len()]
            }
        }
    }

    #[test]
    fn a_range_delete_retry_never_sends_slices_whose_replica_set_shrank_below_w() {
        let mut ring = Ring::new(3);
        for node in 1..=3 {
            ring.add_node(node);
        }
        let coord = Coordinator::new(9, ring, 2, ShrinkingZone::default());
        run_ready(coord.refresh_catalog(1)).unwrap();
        coord.transport().refreshed.set(false);

        let err = run_ready(coord.delete_range_in(1, None, None, WriteConcern::Quorum(3)))
            .unwrap_err();
        let CoordError::RangeDeleteIncomplete { missed, .. } = err else {
            panic!("expected an incomplete range delete, got {err:?}");
        };
        assert!(coord.transport().refreshed.get());
        assert!(missed.iter().all(|m| matches!(
            m.error,
            CoordError::InvalidWriteOptions { replicas: 2, .. }
        )));
        assert_eq!(coord.transport().retry_deletes.get(), 0);
    }

    fn run_ready<F: Future>(future: F) -> F::Output {
        vals_rt::block_on(future, || {})
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum DeadMode {
        Hang,
        TimeOut,
        Healed,
    }

    struct BlackholedReplica {
        dead: NodeId,
        mode: Cell<DeadMode>,
        delivered: RefCell<BTreeMap<NodeId, usize>>,
        reads: RefCell<BTreeMap<NodeId, usize>>,
    }

    impl BlackholedReplica {
        fn new(dead: NodeId) -> Self {
            BlackholedReplica {
                dead,
                mode: Cell::new(DeadMode::Hang),
                delivered: RefCell::default(),
                reads: RefCell::default(),
            }
        }

        async fn reach(&self, node: NodeId) -> Result<(), TransportError> {
            if node != self.dead {
                return Ok(());
            }
            match self.mode.get() {
                DeadMode::Hang => std::future::pending().await,
                DeadMode::TimeOut => Err(TransportError::Timeout),
                DeadMode::Healed => Ok(()),
            }
        }
    }

    impl Transport for BlackholedReplica {
        async fn apply(
            &self,
            node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
            _value: Option<&[u8]>,
            _version: Version,
        ) -> Result<Applied, TransportError> {
            self.reach(node).await?;
            *self.delivered.borrow_mut().entry(node).or_default() += 1;
            Ok(Applied::Won)
        }

        async fn read(
            &self,
            node: NodeId,
            _zone_id: ZoneId,
            _key: &[u8],
        ) -> Result<Option<Stored>, TransportError> {
            *self.reads.borrow_mut().entry(node).or_default() += 1;
            self.reach(node).await?;
            Ok(None)
        }

        async fn members(&self, node: NodeId) -> Result<RingView, TransportError> {
            self.reach(node).await?;
            Ok(RingView {
                epoch: 1,
                vnodes: 64,
                rf: 3,
                members: vec![(1, 1), (2, 1), (3, 1)],
                addrs: Vec::new(),
            })
        }

        async fn zones(&self, _node: NodeId) -> Result<Catalog, TransportError> {
            let mut cat = Catalog::new();
            cat.insert(
                7,
                crate::zone::ZoneConfig {
                    name: "sorted".into(),
                    rf: 3,
                    ordering: Ordering::Sorted,
                    scope: None,
                },
            );
            Ok(cat)
        }

        fn supports_range_tombstones(&self, _node: NodeId) -> Option<bool> {
            Some(true)
        }

        async fn range_delete(
            &self,
            node: NodeId,
            _zone_id: ZoneId,
            _sync: bool,
            _version: Version,
            slices: &[crate::wire::WireSlice<'_>],
        ) -> Vec<BatchAck> {
            if self.reach(node).await.is_err() {
                return vec![BatchAck::Unreachable; slices.len()];
            }
            *self.delivered.borrow_mut().entry(node).or_default() += slices.len();
            vec![BatchAck::Applied(Applied::Won); slices.len()]
        }
    }

    fn poll_once<F: Future>(future: F) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        match future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(out) => Some(out),
            Poll::Pending => None,
        }
    }

    #[test]
    fn quorum_batches_and_range_deletes_do_not_wait_for_a_black_holed_replica() {
        let ranges = 3 * crate::wire::MAX_RANGE_DELETE_SLICES as u32 + 1;
        for dead in [1, 3] {
            let transport = BlackholedReplica::new(dead);
            let coord = Coordinator::new(9, Ring::from_members(3, 64, [1, 2, 3]), 2, transport);
            run_ready(coord.refresh_catalog(1)).unwrap();
            coord.set_range_map(7, RangeMap::from_splits((1..ranges).map(|r| r.to_be_bytes().to_vec())));

            let keys: Vec<Vec<u8>> = (0..64u32).map(|i| (i * 1000).to_be_bytes().to_vec()).collect();
            let writes: Vec<(&[u8], &[u8])> = keys.iter().map(|k| (k.as_slice(), &b"v"[..])).collect();
            let batch = poll_once(coord.write_batch_in(7, &writes, WriteConcern::Majority))
                .unwrap_or_else(|| panic!("dead node {dead}: the batch waited for it"))
                .unwrap();
            assert!(batch.all_ok(), "dead node {dead}: {:?}", batch.outcomes);

            coord.transport().delivered.borrow_mut().clear();
            poll_once(coord.delete_range_in(7, None, None, WriteConcern::Majority))
                .unwrap_or_else(|| panic!("dead node {dead}: the range delete waited for it"))
                .unwrap();
            let delivered = coord.transport().delivered.borrow();
            for node in [1, 2, 3].into_iter().filter(|&n| n != dead) {
                assert_eq!(delivered.get(&node), Some(&(ranges as usize)), "node {node}");
            }
        }
    }

    fn fd_coordinator(
        dead: NodeId,
        fanout: ReadFanout,
    ) -> (Coordinator<BlackholedReplica>, Rc<Cell<u64>>) {
        let now = Rc::new(Cell::new(0u64));
        let clock = now.clone();
        let coord = Coordinator::new(
            9,
            Ring::from_members(3, 64, [1, 2, 3]),
            ReadConcern::Majority,
            BlackholedReplica::new(dead),
        )
        .with_read_fanout(fanout)
        .with_failure_detector(crate::failure::FdConfig {
            failure_threshold: 1,
            open_cooldown_ms: 1000,
        })
        .with_clock(move || clock.get());
        (coord, now)
    }

    fn keys_led_by(coord: &Coordinator<BlackholedReplica>, node: NodeId) -> Vec<Vec<u8>> {
        (0..4096u32)
            .map(|i| format!("k{i}").into_bytes())
            .filter(|k| coord.replicas_for(DEFAULT_ZONE, k).unwrap()[0] == node)
            .take(16)
            .collect()
    }

    #[test]
    fn reads_route_around_a_replica_the_failure_detector_marks_down() {
        for fanout in [ReadFanout::Digest, ReadFanout::Quorum] {
            for dead in [1, 2, 3] {
                let (coord, now) = fd_coordinator(dead, fanout);
                let keys = keys_led_by(&coord, dead);
                assert!(
                    poll_once(coord.read_in(DEFAULT_ZONE, &keys[0])).is_none(),
                    "the dead node leads the key's replicas, so an unknowing read waits for it"
                );

                coord.transport().mode.set(DeadMode::TimeOut);
                poll_once(coord.write_in(DEFAULT_ZONE, &keys[0], b"v", WriteConcern::Majority))
                    .unwrap()
                    .unwrap();
                assert_eq!(coord.down_nodes(), vec![dead]);
                coord.transport().mode.set(DeadMode::Hang);

                let contacted = || coord.transport().reads.borrow().get(&dead).copied().unwrap_or(0);
                let before = contacted();
                for key in &keys {
                    poll_once(coord.read_in(DEFAULT_ZONE, key))
                        .unwrap_or_else(|| panic!("{fanout:?} dead {dead}: the read waited"))
                        .unwrap();
                }
                let many = poll_once(coord.read_many_in(DEFAULT_ZONE, &keys))
                    .unwrap_or_else(|| panic!("{fanout:?} dead {dead}: read_many waited"))
                    .unwrap();
                assert!(many.iter().all(Result::is_ok));
                poll_once(coord.cas_in(DEFAULT_ZONE, &keys[1], b"c", None))
                    .unwrap_or_else(|| panic!("{fanout:?} dead {dead}: cas waited"))
                    .unwrap();
                assert_eq!(contacted(), before, "a node in cooldown is not read");

                now.set(1000);
                poll_once(coord.read_in(DEFAULT_ZONE, &keys[2]))
                    .unwrap_or_else(|| panic!("{fanout:?} dead {dead}: the read waited for the probe"))
                    .unwrap();
                assert_eq!(contacted(), before + 1, "the due probe rides along with the read");
                assert!(coord.is_node_down(dead));

                coord.transport().mode.set(DeadMode::Healed);
                now.set(2000);
                poll_once(coord.read_in(DEFAULT_ZONE, &keys[3])).unwrap().unwrap();
                assert!(coord.down_nodes().is_empty(), "a successful probe readmits the node");
            }
        }
    }

    #[test]
    fn default_zone_writes_replay_hints_only_to_replicas_that_acknowledged() {
        for dead in [1, 3] {
            let coord = Coordinator::new(
                9,
                Ring::from_members(3, 64, [1, 2, 3]),
                ReadConcern::Majority,
                BlackholedReplica::new(dead),
            );
            let keys = keys_led_by(&coord, dead);
            coord.transport().mode.set(DeadMode::TimeOut);
            for key in &keys[..4] {
                poll_once(coord.write_in(DEFAULT_ZONE, key, b"v", WriteConcern::Majority))
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(coord.pending_hints(dead), 4);

            coord.transport().mode.set(DeadMode::Hang);
            poll_once(coord.write_in(DEFAULT_ZONE, &keys[4], b"v", WriteConcern::Majority))
                .unwrap_or_else(|| panic!("dead {dead}: the write waited on hint replay"))
                .unwrap();
            assert_eq!(coord.pending_hints(dead), 4);

            coord.transport().mode.set(DeadMode::Healed);
            let delivered = || coord.transport().delivered.borrow().get(&dead).copied().unwrap_or(0);
            poll_once(coord.write_in(DEFAULT_ZONE, &keys[5], b"v", WriteConcern::Majority))
                .unwrap()
                .unwrap();
            assert_eq!(coord.pending_hints(dead), 0);
            assert_eq!(delivered(), 5, "the write itself plus four replayed hints");

            coord.transport().mode.set(DeadMode::TimeOut);
            for key in &keys[..3] {
                poll_once(coord.write_in(DEFAULT_ZONE, key, b"w", WriteConcern::Majority))
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(poll_once(coord.replay_hints(dead)), Some(0));
            assert_eq!(coord.pending_hints(dead), 3, "undelivered hints stay queued");
        }
    }

    #[test]
    fn membership_refresh_starts_with_members_not_marked_down() {
        let (coord, _now) = fd_coordinator(1, ReadFanout::Digest);
        let keys = keys_led_by(&coord, 1);
        assert!(poll_once(coord.adopt_from_any_member()).is_none(), "node 1 is asked first");

        coord.transport().mode.set(DeadMode::TimeOut);
        poll_once(coord.write_in(DEFAULT_ZONE, &keys[0], b"v", WriteConcern::Majority))
            .unwrap()
            .unwrap();
        coord.transport().mode.set(DeadMode::Hang);
        assert_eq!(poll_once(coord.adopt_from_any_member()), Some(true));
        assert_eq!(coord.ring_epoch(), 1);
    }

    fn poll_ticking<F: Future>(clock: &HedgeClock, future: F) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..32 {
            if let Poll::Ready(out) = future.as_mut().poll(&mut cx) {
                return Some(out);
            }
            clock.advance();
        }
        None
    }

    #[test]
    fn hedged_reads_teach_the_failure_detector_about_a_silent_replica() {
        for many in [false, true] {
            let (coord, _now) = fd_coordinator(2, ReadFanout::Digest);
            let coord = coord
                .with_digest_hedge(Duration::from_millis(40))
                .with_failure_detector(crate::failure::FdConfig {
                    failure_threshold: 2,
                    open_cooldown_ms: 1000,
                });
            let clock = coord.hedge_clock().unwrap();
            let keys = keys_led_by(&coord, 2);
            let read = |i: usize| {
                let coord = &coord;
                let keys = &keys;
                async move {
                    if many {
                        coord.read_many_in(DEFAULT_ZONE, &keys[i..i + 2]).await.map(|_| ())
                    } else {
                        coord.read_in(DEFAULT_ZONE, &keys[i]).await.map(|_| ())
                    }
                }
            };
            for i in 0..2 {
                assert!(poll_once(read(i)).is_none(), "many={many}: the silent leader holds the read");
                poll_ticking(&clock, read(i)).unwrap().unwrap();
            }
            assert_eq!(coord.down_nodes(), vec![2], "many={many}");
            poll_once(read(4))
                .unwrap_or_else(|| panic!("many={many}: a tripped node is routed around"))
                .unwrap();
        }
    }
