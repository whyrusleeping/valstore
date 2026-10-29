    use super::*;
    use crate::hlc::Version;
    use crate::partition::RangeMap;
    use crate::wire::{self, Request, Response};
    use crate::zone::Ordering;
    use std::cell::RefCell;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
    use std::thread;
    use std::time::Instant;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Engine, Ordering as SOrdering};

    fn store(node: u32) -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 512);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        VersionedStore::new(engine, node).unwrap()
    }

    #[test]
    fn tombstone_gc_runs_when_a_due_ordinary_merge_has_no_plan() {
        let wall = 10_000_000u64;
        let at = |ms: u64| Version { hlc: ms << crate::hlc::COUNTER_BITS, node: 1 };
        let mut s = store(1).with_gc_grace_ms(1_000);
        for i in 0..60u32 {
            s.apply_at(format!("base-{i:04}").as_bytes(), Some(b"v"), at(wall - 5_000), wall - 5_000).unwrap();
        }
        s.flush().unwrap();
        s.apply_at(b"aged", None, at(wall - 5_000), wall - 5_000).unwrap();
        s.flush().unwrap();
        assert_eq!(s.run_count(), 2);
        assert!(!s.recurring_merge_selectable(), "base plus a small tail has no tiered window");

        let due = zone_store_due(&s, 256, u64::MAX, 2, wall);
        assert!(due.compact && !due.tombstone_gc, "nothing expired yet");
        assert!(plan_zone_merge(&mut s, &due, wall).unwrap().is_none());

        s.advance_durable_gc_watermark(wall);
        let due = zone_store_due(&s, 256, u64::MAX, 2, wall);
        assert!(due.compact && due.tombstone_gc);
        let plan = plan_zone_merge(&mut s, &due, wall).unwrap().expect("tombstone GC instead of no merge");
        assert_eq!(plan.kind(), vals_store::MergeKind::LoneGc);
        let wdev = std::sync::Arc::new(s.device_sibling().unwrap());
        let written = vals_rt::block_on(vals_store::compact_and_write(plan, wdev), || {}).unwrap();
        s.compact_commit(written).unwrap();
        assert_eq!(s.get_versioned(b"aged").unwrap(), None);
        assert_eq!(s.run_count(), 1);
        assert!(!zone_store_due(&s, 256, u64::MAX, 2, wall).tombstone_gc);
    }

    struct AsyncOnlyState {
        panic_sync: AtomicBool,
        async_reads: AtomicUsize,
    }

    struct AsyncOnlyDevice {
        inner: MemBlockDevice,
        state: Arc<AsyncOnlyState>,
    }

    impl BlockDevice for AsyncOnlyDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }

        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }

        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            assert!(
                !self.state.panic_sync.load(AtomicOrdering::Acquire),
                "re-home copy used synchronous BlockDevice::read_at"
            );
            self.inner.read_at(lba, buf)
        }

        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }

        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(AsyncOnlyDevice {
                inner: self.inner.share(),
                state: self.state.clone(),
            })
        }
    }

    impl AsyncRead for AsyncOnlyDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.state
                .async_reads
                .fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.read_at(lba, buf)
        }
    }

    struct StalledReadState {
        stall: AtomicBool,
        started: AtomicBool,
    }

    struct StalledReadDevice {
        inner: MemBlockDevice,
        state: Arc<StalledReadState>,
    }

    impl BlockDevice for StalledReadDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }

        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }

        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }

        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }

        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(StalledReadDevice {
                inner: self.inner.share(),
                state: self.state.clone(),
            })
        }
    }

    impl AsyncRead for StalledReadDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.state.stall.load(AtomicOrdering::Acquire) {
                self.state.started.store(true, AtomicOrdering::Release);
                std::future::pending::<()>().await;
            }
            self.inner.read_at(lba, buf)
        }
    }

    fn preflushed_async_only_target() -> (
        Rc<RefCell<VersionedStore<AsyncOnlyDevice>>>,
        Arc<AsyncOnlyState>,
    ) {
        let state = Arc::new(AsyncOnlyState {
            panic_sync: AtomicBool::new(false),
            async_reads: AtomicUsize::new(0),
        });
        let dev = AsyncOnlyDevice {
            inner: MemBlockDevice::new(256, 8192),
            state: state.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store
            .apply(
                b"copy-key",
                Some(b"mirrored"),
                Version { hlc: 10, node: 1 },
            )
            .unwrap();
        store.flush().unwrap();
        state.panic_sync.store(true, AtomicOrdering::Release);
        (Rc::new(RefCell::new(store)), state)
    }

    #[test]
    fn rehome_copy_apply_is_async_for_a_preflushed_mirrored_target() {
        let (store, state) = preflushed_async_only_target();
        let gate = crate::net::ReadGate::new();
        let applied = vals_rt::block_on(
            rehome_copy_apply(
                store.as_ref(),
                &gate,
                b"copy-key",
                Some(b"copied"),
                Version { hlc: 20, node: 2 },
            ),
            crate::reactor::poll_io,
        )
        .unwrap();
        assert_eq!(applied, Some(crate::replica::Applied::Won));
        assert!(state.async_reads.load(AtomicOrdering::Relaxed) > 0);
        state.panic_sync.store(false, AtomicOrdering::Release);
        assert_eq!(
            store.borrow().get(b"copy-key").unwrap().as_deref(),
            Some(b"copied" as &[u8])
        );
    }

    #[test]
    fn rehome_copy_apply_defers_after_generation_retry_exhaustion() {
        let (store, state) = preflushed_async_only_target();
        let fires = Rc::new(Cell::new(0u64));
        {
            let store = store.clone();
            let state = state.clone();
            let fires = fires.clone();
            set_rehome_copy_commit_hook(Some(Box::new(move || {
                let attempt = fires.get();
                fires.set(attempt + 1);
                state.panic_sync.store(false, AtomicOrdering::Release);
                let key = format!("retry-{attempt:03}");
                let mut store = store.borrow_mut();
                store
                    .apply(
                        key.as_bytes(),
                        Some(b"generation"),
                        Version {
                            hlc: 100 + attempt,
                            node: 3,
                        },
                    )
                    .unwrap();
                store.flush().unwrap();
                state.panic_sync.store(true, AtomicOrdering::Release);
            })));
        }
        let gate = crate::net::ReadGate::new();
        let applied = vals_rt::block_on(
            rehome_copy_apply(
                store.as_ref(),
                &gate,
                b"copy-key",
                Some(b"copied"),
                Version { hlc: 20, node: 2 },
            ),
            crate::reactor::poll_io,
        )
        .unwrap();
        set_rehome_copy_commit_hook(None);

        assert_eq!(applied, None);
        assert_eq!(fires.get(), 8);
        assert!(state.async_reads.load(AtomicOrdering::Relaxed) >= 8);
    }

    #[test]
    #[allow(clippy::absurd_extreme_comparisons)]
    fn byte_cap_makes_flush_due_below_epoch_threshold() {
        let mut s = store(1);
        for i in 0..30u32 {
            let k = format!("key-{i:04}");
            s.put(k.as_bytes(), b"v").unwrap();
        }
        let footprint = s.memtable_bytes();
        assert!(footprint > 0);

        let tiny_cap = footprint - 1;
        assert!(s.tail_epochs() < u64::MAX);
        let due =
            (s.tail_epochs() >= u64::MAX || s.memtable_bytes() >= tiny_cap) && !s.flush_in_flight();
        assert!(
            due,
            "byte cap alone makes a flush due even with epoch lag below threshold"
        );

        let big_cap = footprint + 1;
        let not_due =
            (s.tail_epochs() >= u64::MAX || s.memtable_bytes() >= big_cap) && !s.flush_in_flight();
        assert!(
            !not_due,
            "below the cap and below epoch threshold -> not due"
        );
    }

    #[test]
    fn tiered_selectable_merges_below_run_threshold() {
        let quiet = MaintSignals {
            tail_epochs: 0,
            memtable_bytes: 0,
            flush_in_flight: false,
            flush_cleanup_pending: false,
            run_count: 4,
            merge_selectable: true,
            compact_in_flight: false,
            free_fraction: 0.90,
            at_maintenance_floor: false,
            lsm_dead_fraction: Some(0.0),
        };
        let due = zone_maint_due(&quiet, 256, u64::MAX, 8);
        assert!(
            due.compact && !due.pressure,
            "a selectable tiered merge schedules without waiting for the run threshold"
        );

        let unselectable = MaintSignals {
            merge_selectable: false,
            ..quiet
        };
        let due = zone_maint_due(&unselectable, 256, u64::MAX, 8);
        assert!(
            !due.compact,
            "below the threshold with nothing selectable, no merge is scheduled"
        );

        let in_flight = MaintSignals {
            compact_in_flight: true,
            ..quiet
        };
        let due = zone_maint_due(&in_flight, 256, u64::MAX, 8);
        assert!(!due.compact, "an in-flight compaction still gates scheduling");
    }

    #[test]
    fn region_pressure_makes_flush_and_early_merge_due() {
        let churn = MaintSignals {
            tail_epochs: 3,
            memtable_bytes: 1 << 20,
            flush_in_flight: false,
            flush_cleanup_pending: false,
            run_count: 3,
            merge_selectable: false,
            compact_in_flight: false,
            free_fraction: 0.10,
            at_maintenance_floor: false,
            lsm_dead_fraction: Some(0.70),
        };
        let due = zone_maint_due(&churn, 256, u64::MAX, 8);
        assert!(due.pressure, "low free + high dead = pressure");
        assert!(due.flush, "pressure flushes a non-empty tail early");
        assert!(due.compact, "pressure merges from 2 runs up");

        let unpressured = MaintSignals {
            free_fraction: 0.80,
            ..churn
        };
        let due = zone_maint_due(&unpressured, 256, u64::MAX, 8);
        assert!(!due.pressure && !due.flush && !due.compact);
    }

    #[test]
    fn region_pressure_skips_live_full_and_legacy_zones() {
        let live_full = MaintSignals {
            tail_epochs: 3,
            memtable_bytes: 1 << 20,
            flush_in_flight: false,
            flush_cleanup_pending: false,
            run_count: 3,
            merge_selectable: false,
            compact_in_flight: false,
            free_fraction: 0.10,
            at_maintenance_floor: false,
            lsm_dead_fraction: Some(0.02),
        };
        let due = zone_maint_due(&live_full, 256, u64::MAX, 8);
        assert!(!due.pressure && !due.flush && !due.compact);

        let at_floor = MaintSignals {
            at_maintenance_floor: true,
            ..live_full
        };
        let due = zone_maint_due(&at_floor, 256, u64::MAX, 8);
        assert!(due.pressure && due.flush && due.compact);

        let legacy = MaintSignals {
            lsm_dead_fraction: None,
            ..live_full
        };
        let due = zone_maint_due(&legacy, 256, u64::MAX, 8);
        assert!(!due.pressure && !due.flush && !due.compact);
    }

    #[test]
    fn region_pressure_respects_in_flight_gates_and_empty_tail() {
        let base = MaintSignals {
            tail_epochs: 3,
            memtable_bytes: 1 << 20,
            flush_in_flight: true,
            flush_cleanup_pending: false,
            run_count: 3,
            merge_selectable: false,
            compact_in_flight: true,
            free_fraction: 0.10,
            at_maintenance_floor: false,
            lsm_dead_fraction: Some(0.70),
        };
        let due = zone_maint_due(&base, 256, u64::MAX, 8);
        assert!(due.pressure && !due.flush && !due.compact);

        let empty_tail = MaintSignals {
            tail_epochs: 0,
            flush_in_flight: false,
            ..base
        };
        let due = zone_maint_due(&empty_tail, 256, u64::MAX, 8);
        assert!(!due.flush, "no tail -> nothing to flush");

        let cleanup_pending = MaintSignals {
            flush_cleanup_pending: true,
            ..empty_tail
        };
        let due = zone_maint_due(&cleanup_pending, 256, u64::MAX, 8);
        assert!(due.flush, "deferred snapshot cleanup must stay scheduled");
    }

    fn rpc(addr: SocketAddr, req: &Request) -> Response {
        let mut s = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut s).unwrap();
        let reqid: u64 = 1;
        let payload = wire::encode_request_payload(req);
        let len = (8 + payload.len()) as u32;
        let mut frame = Vec::new();
        frame.extend_from_slice(&len.to_le_bytes());
        frame.extend_from_slice(&reqid.to_le_bytes());
        frame.extend_from_slice(&payload);
        s.write_all(&frame).unwrap();
        let mut lenb = [0u8; 4];
        s.read_exact(&mut lenb).unwrap();
        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
        s.read_exact(&mut block).unwrap();
        wire::decode_response(&block[8..]).unwrap()
    }

    fn ver(physical_ms: u64, node: u32) -> Version {
        Version {
            hlc: physical_ms << 16,
            node: node.into(),
        }
    }

    fn read_from(addr: SocketAddr, key: &[u8]) -> Option<Vec<u8>> {
        read_zone(addr, DEFAULT_ZONE, key)
    }

    fn read_raw(addr: SocketAddr, key: &[u8]) -> Response {
        rpc(
            addr,
            &Request::Read {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key,
            },
        )
    }

    fn is_fenced_reply(resp: &Response) -> bool {
        matches!(resp, Response::Err(m) if m.contains("fenced"))
    }

    fn read_zone(addr: SocketAddr, zone_id: crate::zone::ZoneId, key: &[u8]) -> Option<Vec<u8>> {
        match rpc(
            addr,
            &Request::Read {
                epoch: 0,
                zone_id,
                key,
            },
        ) {
            Response::Read(Some(stored)) => stored.value,
            _ => None,
        }
    }

    fn set_members(addr: SocketAddr, members: &[NodeId]) -> Vec<NodeId> {
        let weighted: Vec<(NodeId, u32)> = members.iter().map(|&n| (n, 1)).collect();
        let req = Request::SetMembers {
            addrs: &[],
            auth: &[],
            epoch: 0,
            vnodes: 0,
            rf: 0,
            members: &weighted,
            incarnation: None,
        };
        match rpc(addr, &req) {
            Response::Members { members, .. } => members.into_iter().map(|(n, _)| n).collect(),
            other => panic!("expected Members, got {other:?}"),
        }
    }

    fn set_members_config_with_addrs(
        addr: SocketAddr,
        epoch: u64,
        vnodes: u32,
        rf: u32,
        members: &[NodeId],
        addrs: &[(NodeId, Vec<SocketAddr>)],
    ) {
        let weighted: Vec<(NodeId, u32)> = members.iter().map(|&n| (n, 1)).collect();
        let req = Request::SetMembers {
            addrs,
            auth: &[],
            epoch,
            vnodes,
            rf,
            members: &weighted,
            incarnation: None,
        };
        match rpc(addr, &req) {
            Response::Members { .. } => {}
            other => panic!("expected Members, got {other:?}"),
        }
    }

    fn set_zones(addr: SocketAddr, cat: &crate::zone::Catalog, epoch: u64) {
        set_zones_only(addr, cat, epoch);
        let _ = rpc(
            addr,
            &Request::SetRangeMaps {
                auth: &[],
                epoch: 0,
                maps: &[],
                incarnation: None,
            },
        );
    }

    fn set_zones_only(addr: SocketAddr, cat: &crate::zone::Catalog, epoch: u64) {
        let owned: Vec<wire::OwnedZone> = cat
            .iter()
            .map(|(id, cfg)| wire::OwnedZone {
                id,
                rf: cfg.rf as u32,
                ordering: crate::zone::ordering_to_u8(cfg.ordering),
                scope: cfg.scope.as_ref().map(|s| s.iter().copied().collect()),
                name: cfg.name.clone(),
            })
            .collect();
        let borrowed: Vec<wire::WireZone> = owned
            .iter()
            .map(|z| wire::WireZone {
                id: z.id,
                rf: z.rf,
                ordering: z.ordering,
                scope: z.scope.as_deref(),
                name: &z.name,
            })
            .collect();
        let req = Request::SetZones {
            auth: &[],
            epoch,
            zones: &borrowed,
            incarnation: None,
        };
        let _ = rpc(addr, &req);
    }

    #[test]
    fn periodic_sweep_heals_a_stale_node() {
        let ring = Ring::from_members(2, 64, [1, 2]);

        let mut s2 = store(2);
        for i in 0..40u32 {
            let k = format!("k{i}");
            s2.apply(k.as_bytes(), Some(b"v"), Version { hlc: 100, node: 2 })
                .unwrap();
        }
        let s1 = store(1);

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            ring.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(2, l2, s2, ring, peers, interval));

        let probe = b"k39";
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a1, probe).as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(Instant::now() < deadline, "node 1 did not heal in time");
            thread::sleep(Duration::from_millis(20));
        }

        for i in 0..40u32 {
            let k = format!("k{i}");
            assert_eq!(
                read_from(a1, k.as_bytes()).as_deref(),
                Some(&b"v"[..]),
                "node 1 missing {k} after sweep"
            );
        }
    }

    use crate::liveness::MemLivenessLog;

    #[test]
    fn without_liveness_a_straggler_resurrects_a_gcd_delete() {
        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();

        let s1 = store(1);
        let mut s2 = store(2);
        s2.apply(b"victim", Some(b"v1"), ver(now - 600_000, 2))
            .unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            ring.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(2, l2, s2, ring, peers, interval));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a1, b"victim").as_deref() == Some(&b"v1"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "expected the unguarded straggler to resurrect the delete"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    const MOCK_RT_ERR: u8 = 0;
    const MOCK_RT_SKEWED: u8 = 1;
    const MOCK_RT_SAME: u8 = 2;

    type MockRt = Option<(std::sync::Arc<std::sync::atomic::AtomicU8>, std::sync::Arc<std::sync::atomic::AtomicUsize>)>;

    fn spawn_mock_peer(
        healthy: std::sync::Arc<std::sync::atomic::AtomicBool>,
        syncs: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        rt: MockRt,
    ) -> SocketAddr {
        use std::sync::atomic::Ordering;
        let lst = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = lst.local_addr().unwrap();
        thread::spawn(move || {
            for conn in lst.incoming() {
                let Ok(mut s) = conn else { return };
                let healthy = healthy.clone();
                let syncs = syncs.clone();
                let rt = rt.clone();
                thread::spawn(move || {
                    if crate::proto::server_handshake_blocking(&mut s).is_err() {
                        return;
                    }
                    loop {
                        let mut lenb = [0u8; 4];
                        if s.read_exact(&mut lenb).is_err() {
                            return;
                        }
                        let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                        if s.read_exact(&mut block).is_err() {
                            return;
                        }
                        let reqid = u64::from_le_bytes(block[0..8].try_into().unwrap());
                        let resp = match wire::decode_request(&block[8..]) {
                            Ok(wire::OwnedRequest::GetWatermark) => Response::Watermark(1 << 40),
                            Ok(wire::OwnedRequest::SyncTree {
                                level,
                                scope_g,
                                scope,
                                ..
                            }) => {
                                syncs.fetch_add(1, Ordering::SeqCst);
                                if healthy.load(Ordering::SeqCst) {
                                    let scope: std::collections::HashSet<u32> =
                                        scope.into_iter().collect();
                                    let nodes: Vec<(u32, u64)> = (0..level)
                                        .filter(|f| scope.contains(&(f % scope_g)))
                                        .map(|f| (f, 0u64))
                                        .collect();
                                    Response::Tree(nodes)
                                } else {
                                    Response::Err("mock: refill refused".into())
                                }
                            }
                            Ok(wire::OwnedRequest::SyncRangeTombstones { .. }) if rt.is_some() => {
                                let (rt_mode, rt_calls) = rt.as_ref().unwrap();
                                rt_calls.fetch_add(1, Ordering::SeqCst);
                                match rt_mode.load(Ordering::SeqCst) {
                                    MOCK_RT_SAME => Response::RangeTombstones { same: true, more: false, fragments: Vec::new() },
                                    MOCK_RT_SKEWED => Response::RangeTombstones {
                                        same: false,
                                        more: false,
                                        fragments: vec![wire::RtFragment {
                                            lo: Some(b"empty/".to_vec()),
                                            hi: Some(b"empty0".to_vec()),
                                            version: ver(physical_now_ms() + 3_600_000, 2),
                                        }],
                                    },
                                    _ => Response::Err("mock: range tombstones refused".into()),
                                }
                            }
                            _ => Response::Err("mock: unsupported".into()),
                        };
                        let payload = wire::encode_response_payload(&resp);
                        let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
                        frame.extend_from_slice(&reqid.to_le_bytes());
                        frame.extend_from_slice(&payload);
                        if s.write_all(&frame).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    #[test]
    fn a_wiped_node_stays_fenced_until_its_refill_completes() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::Arc;

        let healthy = Arc::new(AtomicBool::new(false));
        let syncs = Arc::new(AtomicUsize::new(0));
        let peer_addr = spawn_mock_peer(healthy.clone(), syncs.clone(), None);

        let ring = Ring::from_members(2, 64, [1, 2]);
        let (l1, a1) = Node::bind().unwrap();
        let _node = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            store(1),
            ring,
            vec![(2, peer_addr)],
            Duration::from_millis(25),
            200,
            MemLivenessLog::new(),
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        while syncs.load(Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline, "node never attempted its refill");
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            is_fenced_reply(&read_raw(a1, b"x")),
            "a wiped node must stay fenced while its refill cannot complete"
        );

        healthy.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let r = read_raw(a1, b"x");
            if matches!(r, Response::Read(None)) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node did not unfence after a completed refill (last reply: {r:?})"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn a_wiped_node_with_a_sorted_zone_stays_fenced_until_a_range_tombstone_exchange_applies_everything() {
        use std::sync::atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering};
        use std::sync::Arc;
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let healthy = Arc::new(AtomicBool::new(true));
        let syncs = Arc::new(AtomicUsize::new(0));
        let rt_mode = Arc::new(AtomicU8::new(MOCK_RT_ERR));
        let rt_calls = Arc::new(AtomicUsize::new(0));
        let peer_addr = spawn_mock_peer(healthy, syncs.clone(), Some((rt_mode.clone(), rt_calls.clone())));

        let mut catalog = crate::zone::Catalog::new();
        catalog.insert(
            7,
            crate::zone::ZoneConfig { name: "rt".into(), rf: 2, ordering: crate::zone::Ordering::Sorted, scope: None },
        );
        let provider = MemZoneStore::new(1, ZoneGeom { block_size: 256, seg_blocks: 4, reserved_blocks: 0, region_blocks: 512 });
        let zone0 = provider.open(DEFAULT_ZONE, crate::zone::Ordering::Unsorted).unwrap();
        let (l1, a1) = Node::bind().unwrap();
        let _node = crate::testutil::running(Node::serve_zoned_with_liveness(
            1,
            l1,
            zone0,
            provider,
            catalog,
            Ring::from_members(2, 64, [1, 2]),
            vec![(2, peer_addr)],
            Duration::from_millis(25),
            200,
            MemLivenessLog::new(),
            None,
        ));

        let await_calls = |calls: usize| {
            let deadline = Instant::now() + Duration::from_secs(5);
            while syncs.load(Ordering::SeqCst) < 2 * calls || rt_calls.load(Ordering::SeqCst) < calls {
                assert!(Instant::now() < deadline, "node never attempted its refill");
                thread::sleep(Duration::from_millis(10));
            }
        };
        await_calls(2);
        assert!(
            is_fenced_reply(&read_raw(a1, b"x")),
            "a sorted zone whose range-tombstone exchange fails is not refilled"
        );
        rt_mode.store(MOCK_RT_SKEWED, Ordering::SeqCst);
        let skewed_from = rt_calls.load(Ordering::SeqCst);
        await_calls(skewed_from + 3);
        assert!(
            is_fenced_reply(&read_raw(a1, b"x")),
            "a fragment rejected beyond the clock-skew bound leaves the zone unrefilled"
        );

        rt_mode.store(MOCK_RT_SAME, Ordering::SeqCst);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let r = read_raw(a1, b"x");
            if matches!(r, Response::Read(None)) {
                break;
            }
            assert!(Instant::now() < deadline, "node did not unfence after its refill (last reply: {r:?})");
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn liveness_straggler_wipes_instead_of_resurrecting_a_gcd_delete() {
        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();
        let grace = 60_000u64;
        let recent = now - 1_000;
        let old = now - 600_000;

        let mut s1 = store(1);
        for i in 0..10u32 {
            s1.apply(format!("k{i}").as_bytes(), Some(b"v"), ver(recent, 1))
                .unwrap();
        }
        let mut s2 = store(2);
        s2.apply(b"victim", Some(b"v1"), ver(old, 2)).unwrap();

        let log1 = MemLivenessLog::new();
        log1.store(recent).unwrap();
        let log2 = MemLivenessLog::new();
        log2.store(old).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring.clone(),
            peers.clone(),
            interval,
            grace,
            log1,
        ));
        let _node = crate::testutil::running(Node::serve_with_liveness(
            2, l2, s2, ring, peers, interval, grace, log2,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a2, b"k0").as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 2 did not wipe + resync in time"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            read_from(a2, b"victim"),
            None,
            "straggler resurrected the delete"
        );
        assert_eq!(
            read_from(a1, b"victim"),
            None,
            "node 1 pulled back the deleted value"
        );
        assert_eq!(read_from(a1, b"k5").as_deref(), Some(&b"v"[..]));
        thread::sleep(Duration::from_millis(200));
        assert_eq!(
            read_from(a2, b"k9").as_deref(),
            Some(&b"v"[..]),
            "straggler lost data to a re-wipe"
        );
        assert_eq!(read_from(a2, b"victim"), None);
    }

    #[test]
    fn ae_fills_a_co_owner_with_values_older_than_the_gc_grace() {
        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();
        let grace = 60_000u64;
        let recent = now - 1_000;

        let mut s1 = store(1);
        for i in 0..10u32 {
            s1.apply(format!("k{i}").as_bytes(), Some(b"v"), ver(recent, 1)).unwrap();
        }
        let mut s2 = store(2);
        s2.apply(b"aged", Some(b"a"), ver(now - 600_000, 2)).unwrap();
        s2.apply(b"young", Some(b"y"), ver(recent, 2)).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let mut nodes = Vec::new();
        for (n, l, s) in [(1, l1, s1), (2, l2, s2)] {
            let log = MemLivenessLog::new();
            log.store(recent).unwrap();
            nodes.push(crate::testutil::running(Node::serve_with_liveness(
                n,
                l,
                s,
                ring.clone(),
                peers.clone(),
                interval,
                grace,
                log,
            )));
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        while read_from(a1, b"young").is_none() || read_from(a2, b"k9").is_none() {
            assert!(Instant::now() < deadline, "anti-entropy did not run");
            thread::sleep(Duration::from_millis(20));
        }
        while read_from(a1, b"aged").is_none() {
            assert!(Instant::now() < deadline, "the co-owner never received the value older than the grace");
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(read_from(a1, b"aged").as_deref(), Some(&b"a"[..]));
        let client_write = rpc(
            a1,
            &Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"late",
                value: Some(b"l"),
                version: ver(now - 600_000, 9),
            },
        );
        assert_eq!(
            client_write,
            Response::Apply(crate::replica::Applied::Rejected),
            "a write older than the grace is still refused"
        );
        drop(nodes);
    }

    #[test]
    fn liveness_born_fenced_node_holds_fence_until_membership_push_then_wipes() {
        let now = physical_now_ms();
        let grace = 60_000u64;
        let recent = now - 1_000;
        let old = now - 600_000;

        let mut s1 = store(1);
        s1.apply(b"victim", Some(b"v1"), ver(old, 1)).unwrap();
        let mut s2 = store(2);
        for i in 0..10u32 {
            s2.apply(format!("k{i}").as_bytes(), Some(b"v"), ver(recent, 2))
                .unwrap();
        }

        let log1 = MemLivenessLog::new();
        log1.store(old).unwrap();
        let log2 = MemLivenessLog::new();
        log2.store(recent).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let interval = Duration::from_millis(25);
        let ring1 = Ring::from_members(1, 64, [1]);
        let _n1 = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring1,
            vec![(1, a1)],
            interval,
            grace,
            log1,
        ));
        let ring2 = Ring::from_members(2, 64, [1, 2]);
        let _n2 = crate::testutil::running(Node::serve_with_liveness(
            2,
            l2,
            s2,
            ring2,
            vec![(1, a1), (2, a2)],
            interval,
            grace,
            log2,
        ));

        let hold_until = Instant::now() + Duration::from_millis(300);
        while Instant::now() < hold_until {
            assert!(
                is_fenced_reply(&read_raw(a1, b"k0")),
                "born-fenced straggler unfenced before membership arrived (A.4 race)"
            );
            thread::sleep(Duration::from_millis(30));
        }

        let addrs = vec![(1, vec![a1]), (2, vec![a2])];
        set_members_config_with_addrs(a2, 1, 64, 2, &[1, 2], &addrs);
        set_members_config_with_addrs(a1, 1, 64, 2, &[1, 2], &addrs);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from_at(a1, b"k0", 1).as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 1 did not wipe + resync after the membership push"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            read_from_at(a1, b"victim", 1),
            None,
            "straggler resurrected the delete"
        );
        assert_eq!(
            read_from_at(a2, b"victim", 1),
            None,
            "node 2 pulled back the deleted value"
        );
        assert_eq!(read_from_at(a1, b"k9", 1).as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn liveness_solo_node_with_prior_watermark_unfences_after_a_one_member_push() {
        let now = physical_now_ms();
        let grace = 60_000u64;
        let old = now - 600_000;

        let mut s1 = store(1);
        s1.apply(b"k0", Some(b"v"), ver(old, 1)).unwrap();
        let log1 = MemLivenessLog::new();
        log1.store(old).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let interval = Duration::from_millis(25);
        let ring1 = Ring::from_members(1, 64, [1]);
        let _n1 = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring1,
            vec![(1, a1)],
            interval,
            grace,
            log1,
        ));

        let hold_until = Instant::now() + Duration::from_millis(200);
        while Instant::now() < hold_until {
            assert!(
                is_fenced_reply(&read_raw(a1, b"k0")),
                "solo node with a prior watermark unfenced before its membership push"
            );
            thread::sleep(Duration::from_millis(30));
        }

        set_members_config(a1, 1, 64, 1, &[1]);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from_at(a1, b"k0", 1).as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "solo node did not unfence after its 1-member membership push"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn liveness_pristine_lone_node_unfences_on_first_sweep_without_a_push() {
        let mut s1 = store(1);
        s1.apply(b"k0", Some(b"v"), ver(physical_now_ms(), 1))
            .unwrap();
        let log1 = MemLivenessLog::new();

        let (l1, a1) = Node::bind().unwrap();
        let interval = Duration::from_millis(25);
        let ring1 = Ring::from_members(1, 64, [1]);
        let _n1 = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring1,
            vec![(1, a1)],
            interval,
            60_000,
            log1,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a1, b"k0").as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pristine lone node did not unfence on its first sweep"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn liveness_wipe_leaves_a_durable_marker_with_pre_wipe_stats() {
        use crate::liveness::FileLivenessLog;

        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();
        let grace = 60_000u64;
        let recent = now - 1_000;
        let old = now - 600_000;

        let mut s1 = store(1);
        for i in 0..10u32 {
            s1.apply(format!("k{i}").as_bytes(), Some(b"v"), ver(recent, 1))
                .unwrap();
        }
        let mut s2 = store(2);
        s2.apply(b"victim", Some(b"v1"), ver(old, 2)).unwrap();

        let dir = std::env::temp_dir();
        let wm_path = dir.join(format!("vals-node-wipemark-{}.wm", std::process::id()));
        let marker = wm_path.with_extension("wipe");
        std::fs::remove_file(&wm_path).ok();
        std::fs::remove_file(&marker).ok();
        let log1 = MemLivenessLog::new();
        log1.store(recent).unwrap();
        let log2 = FileLivenessLog::new(&wm_path);
        log2.store(old).unwrap();

        let wipes_before = crate::observe::snapshot().wipes;

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring.clone(),
            peers.clone(),
            interval,
            grace,
            log1,
        ));
        let _node = crate::testutil::running(Node::serve_with_liveness(
            2, l2, s2, ring, peers, interval, grace, log2,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a2, b"k0").as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 2 did not wipe + resync in time"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let contents = std::fs::read_to_string(&marker).expect("wipe marker must exist");
        let line = contents.lines().next().unwrap();
        assert!(line.contains("keys=1"), "pre-wipe key count: {line}");
        assert!(line.contains("zones=1"), "pre-wipe zone count: {line}");
        assert!(
            line.contains(&format!("gc_grace_ms={grace}")),
            "grace recorded: {line}"
        );
        assert!(crate::observe::snapshot().wipes > wipes_before);

        std::fs::remove_file(&wm_path).ok();
        std::fs::remove_file(&marker).ok();
    }

    #[test]
    fn whole_cluster_restart_with_liveness_preserves_all_data() {
        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();
        let grace = 60_000u64;
        let frozen = now - 600_000;

        let mut s1 = store(1);
        let mut s2 = store(2);
        for i in 0..10u32 {
            let k = format!("k{i}");
            s1.apply(k.as_bytes(), Some(b"v"), ver(frozen, 1)).unwrap();
            s2.apply(k.as_bytes(), Some(b"v"), ver(frozen, 1)).unwrap();
        }
        let log1 = MemLivenessLog::new();
        log1.store(frozen).unwrap();
        let log2 = MemLivenessLog::new();
        log2.store(frozen).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring.clone(),
            peers.clone(),
            interval,
            grace,
            log1,
        ));
        let _node = crate::testutil::running(Node::serve_with_liveness(
            2, l2, s2, ring, peers, interval, grace, log2,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if read_from(a1, b"k0").as_deref() == Some(&b"v"[..])
                && read_from(a2, b"k0").as_deref() == Some(&b"v"[..])
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "nodes did not unfence/recover in time"
            );
            thread::sleep(Duration::from_millis(20));
        }
        thread::sleep(Duration::from_millis(300));
        for i in 0..10u32 {
            let k = format!("k{i}");
            assert_eq!(
                read_from(a1, k.as_bytes()).as_deref(),
                Some(&b"v"[..]),
                "node 1 lost {k}"
            );
            assert_eq!(
                read_from(a2, k.as_bytes()).as_deref(),
                Some(&b"v"[..]),
                "node 2 lost {k}"
            );
        }
    }

    #[test]
    fn a_node_that_cannot_reach_the_cluster_fences_itself() {
        let ring = Ring::from_members(2, 64, [1, 2]);
        let now = physical_now_ms();
        let grace = 100u64;

        let mut s1 = store(1);
        s1.apply(b"k", Some(b"v"), ver(now - 10_000, 1)).unwrap();
        let log1 = MemLivenessLog::new();
        log1.store(now - 10_000).unwrap();

        let (_unresponsive_peer, dead_addr) = Node::bind().unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, dead_addr)];
        let _node = crate::testutil::running(Node::serve_with_liveness(
            1,
            l1,
            s1,
            ring,
            peers,
            Duration::from_millis(25),
            grace,
            log1,
        ));

        let deadline = Instant::now() + Duration::from_millis(600);
        while Instant::now() < deadline {
            let resp = read_raw(a1, b"k");
            assert!(
                is_fenced_reply(&resp),
                "an unverifiable node must refuse reads, got {resp:?}"
            );
            thread::sleep(Duration::from_millis(40));
        }
    }

    #[test]
    fn background_compaction_preserves_data_and_keeps_serving() {
        let ring = Ring::from_members(1, 64, [1]);

        let mut s1 = store(1);
        let keys: Vec<Vec<u8>> = (0..20).map(|i| format!("k{i:02}").into_bytes()).collect();
        for round in 1..=5u64 {
            let val = format!("v{round}");
            for k in &keys {
                s1.apply(
                    k,
                    Some(val.as_bytes()),
                    Version {
                        hlc: round,
                        node: 1,
                    },
                )
                .unwrap();
            }
        }

        let (l1, a1) = Node::bind().unwrap();
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve(1, l1, s1, ring, vec![(1, a1)], interval));

        thread::sleep(Duration::from_millis(1300));
        for k in &keys {
            assert_eq!(
                read_from(a1, k).as_deref(),
                Some(&b"v5"[..]),
                "key {k:?} lost/corrupted across background compaction"
            );
        }
    }

    #[test]
    fn compact_loop_releases_a_materialized_zone_dropped_mid_pass() {
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let zid = 7;
        let provider = Rc::new(MemZoneStore::new(
            1,
            ZoneGeom {
                block_size: 256,
                seg_blocks: 4,
                reserved_blocks: 0,
                region_blocks: 4096,
            },
        ));
        let zone0 = provider.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let mut catalog = crate::zone::Catalog::new();
        let cfg = crate::zone::ZoneConfig {
            name: "active".into(),
            rf: 1,
            ordering: Ordering::Unsorted,
            scope: None,
        };
        catalog.insert(zid, cfg.clone());
        let shared = Rc::new(ServerShared::zoned(
            1,
            zone0,
            provider.clone(),
            catalog,
            Ring::new(1),
        ));
        let materialized = shared.zone_store(zid).unwrap();
        for round in 1..=80u64 {
            for key in 0..32u64 {
                materialized
                    .borrow_mut()
                    .apply(
                        format!("k{key:02}").as_bytes(),
                        Some(&[round as u8; 180]),
                        Version {
                            hlc: round,
                            node: 1,
                        },
                    )
                    .unwrap();
            }
        }
        let used_before = materialized.borrow().engine().used_segments();
        assert!(
            materialized.borrow().engine().reclaimable_fraction() >= 0.20
        );
        drop(materialized);

        let ex = LocalExecutor::new();
        let orchestrator = shared.clone();
        let orchestrator_provider = provider.clone();
        ex.spawn(async move {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                let used = orchestrator
                    .zone_store(zid)
                    .unwrap()
                    .borrow()
                    .engine()
                    .used_segments();
                if used < used_before {
                    break;
                }
                assert!(Instant::now() < deadline, "compaction never began");
                vals_rt::yield_now().await;
            }

            let dropped = crate::net::serve_request(
                &orchestrator,
                crate::wire::OwnedRequest::SetZones {
                    auth: Vec::new(),
                    epoch: 1,
                    zones: Vec::new(),
                    incarnation: None,
                },
            );
            assert!(matches!(dropped, Response::Zones { .. }));
            while orchestrator_provider.existing().contains(&zid) {
                assert!(
                    Instant::now() < deadline,
                    "active compaction retained the dropped zone"
                );
                vals_rt::yield_now().await;
            }

            let mut recreated = crate::zone::Catalog::new();
            recreated.insert(zid, cfg);
            let recreated = crate::net::serve_request(
                &orchestrator,
                crate::wire::OwnedRequest::SetZones {
                    auth: Vec::new(),
                    epoch: 2,
                    zones: crate::net::catalog_to_wire(&recreated),
                    incarnation: None,
                },
            );
            assert!(matches!(recreated, Response::Zones { .. }));
            let fresh = orchestrator.zone_store(zid).unwrap();
            assert_eq!(fresh.borrow().get(b"k00").unwrap(), None);
            orchestrator.begin_shutdown();
        });
        ex.spawn(compact_loop(shared.clone(), Duration::from_millis(20)));
        ex.run({
            let shared = shared.clone();
            move || server_poll(&shared)
        });
    }

    #[test]
    fn maint_loop_stops_flush_commit_cleanup_on_shutdown() {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(512, 65_536),
            Config {
                seg_blocks: 64,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..vals_store::FLUSH_SLICE_ENTRIES * 3 {
            let key = format!("k{i:05}");
            s.apply(
                key.as_bytes(),
                Some(b"v"),
                Version {
                    hlc: i as u64 + 1,
                    node: 1,
                },
            )
            .unwrap();
        }
        let plan = s.flush_plan().unwrap().unwrap();
        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
        handle
            .submit(crate::maint::MaintJob::Flush(DEFAULT_ZONE, plan))
            .unwrap();

        let ex = LocalExecutor::new();
        let shutdown = shared.clone();
        ex.spawn(async move {
            while shutdown
                .zone_store(DEFAULT_ZONE)
                .unwrap()
                .borrow()
                .run_count()
                == 0
            {
                vals_rt::yield_now().await;
            }
            shutdown.begin_shutdown();
        });
        ex.spawn(maint_loop(shared.clone(), handle));
        ex.run(crate::reactor::poll_io_nonblocking);
        join.join();

        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        assert_eq!(store.borrow().run_count(), 1);
        assert!(store.borrow().flush_in_flight());
    }

    #[test]
    fn maint_loop_resubmits_cover_scan_chunks_during_a_flush_commit() {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(512, 65_536),
            Config {
                seg_blocks: 64,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        let version = |i: usize| Version { hlc: i as u64 + 1, node: 1 };
        for i in 0..64 {
            s.apply(format!("old{i:03}").as_bytes(), Some(b"v"), version(i)).unwrap();
        }
        s.flush().unwrap().unwrap();
        s.engine_mut().reset_key_filter_for_test();
        let cover = s.engine_mut().cover_scan_plan().expect("the reopened-style run is uncovered");
        for i in 0..20_000 {
            s.apply(format!("k{i:05}").as_bytes(), Some(b"v"), version(1000 + i)).unwrap();
        }
        let plan = s.flush_plan().unwrap().unwrap();
        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let (handle, mut join) = crate::maint::spawn::<MemBlockDevice>();
        let submit = handle.submitter();
        handle
            .submit(crate::maint::MaintJob::Flush(DEFAULT_ZONE, plan))
            .unwrap();

        let ex = LocalExecutor::new();
        let covered_mid_commit = Rc::new(std::cell::Cell::new(false));
        let observer = shared.clone();
        let seen = covered_mid_commit.clone();
        ex.spawn(async move {
            let store = observer.zone_store(DEFAULT_ZONE).unwrap();
            while store.borrow().run_count() < 2 {
                vals_rt::yield_now().await;
            }
            assert!(store.borrow().flush_in_flight());
            submit
                .submit(crate::maint::MaintJob::CoverScan(DEFAULT_ZONE, Box::new(cover)))
                .unwrap();
            observer.begin_maint_job(DEFAULT_ZONE);
            std::thread::sleep(Duration::from_millis(200));
            while store.borrow().flush_in_flight() {
                if store.borrow_mut().engine_mut().key_filter_stats().uncovered_runs == 0 {
                    seen.set(true);
                }
                vals_rt::yield_now().await;
            }
            observer.begin_shutdown();
        });
        ex.spawn(maint_loop(shared.clone(), handle));
        ex.run(crate::reactor::poll_io_nonblocking);
        join.join();
        assert!(covered_mid_commit.get(), "the cover-scan chunk waited for the whole flush commit");
    }

    #[test]
    fn compaction_quiesces_inflight_reads_instead_of_starving() {
        let ring = Ring::from_members(1, 64, [1]);
        let mut s = store(1);
        let keys: Vec<Vec<u8>> = (0..16).map(|i| format!("k{i:02}").into_bytes()).collect();
        for round in 1..=6u64 {
            let val = format!("v{round}");
            for k in &keys {
                s.apply(
                    k,
                    Some(val.as_bytes()),
                    Version {
                        hlc: round,
                        node: 1,
                    },
                )
                .unwrap();
            }
        }

        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let segs_before = shared
            .zone_store(DEFAULT_ZONE)
            .unwrap()
            .borrow()
            .engine()
            .used_segments();

        let ex = LocalExecutor::new();
        let orch = shared.clone();
        ex.spawn(async move {
            let used_segments = || {
                orch.zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .engine()
                    .used_segments()
            };
            let hold = Timer::every(Duration::from_millis(5)).unwrap();
            let started = Instant::now();
            let mut compacted = false;
            while started.elapsed() < Duration::from_secs(5) {
                let g = orch.read_gate().acquire().await;
                hold.tick().await;
                drop(g);
                if used_segments() < segs_before {
                    compacted = true;
                    break;
                }
            }
            assert!(
                compacted,
                "compaction starved under sustained read load: still {} segments \
                 (expected < {segs_before})",
                used_segments()
            );
            drop(orch.read_gate().acquire().await);
            orch.begin_shutdown();
        });
        ex.spawn(compact_loop(shared.clone(), Duration::from_millis(50)));
        ex.run({
            let shared = shared.clone();
            move || server_poll(&shared)
        });
    }

    fn cleanup_under_continuous_reads(key_count: usize) {
        use crate::coord::LocalCluster;
        let ring = Ring::from_members(1, 64, [2]);
        let device_blocks = if key_count > 100_000 { 524_288 } else { 65_536 };
        let large_store = |node| {
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(512, device_blocks),
                Config {
                    seg_blocks: 64,
                    reserved_blocks: 0,
                    ordering: SOrdering::Unsorted,
                },
            );
            VersionedStore::new(engine, node).unwrap()
        };
        let mut s = large_store(1);
        let keys: Vec<Vec<u8>> = (0..key_count)
            .map(|i| format!("k{i:06}").into_bytes())
            .collect();
        for k in &keys {
            s.apply(k, Some(b"v"), Version { hlc: 1, node: 1 }).unwrap();
        }
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring.clone()));

        let mut lc: LocalCluster<MemBlockDevice> = LocalCluster::new();
        lc.insert(2, large_store(2));

        let ex = LocalExecutor::new();
        let max_admission_ms = Rc::new(Cell::new(0u64));

        let reader_shared = shared.clone();
        let reader_max = max_admission_ms.clone();
        ex.spawn(async move {
            let hold = Timer::every(Duration::from_millis(5)).unwrap();
            while !reader_shared.is_shutdown() {
                let started = Instant::now();
                let g = reader_shared.read_gate().acquire().await;
                reader_max.set(
                    reader_max
                        .get()
                        .max(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
                );
                hold.tick().await;
                drop(g);
            }
        });

        let stream_shared = shared.clone();
        ex.spawn(async move {
            let windows = Timer::every(Duration::from_millis(5)).unwrap();
            let mut guard = stream_shared.read_gate().acquire().await;
            while !stream_shared.is_shutdown() {
                windows.tick().await;
                guard.checkpoint().await;
            }
        });

        let driver_shared = shared.clone();
        let driver_max = max_admission_ms.clone();
        ex.spawn(async move {
            let store = driver_shared.zone_store(DEFAULT_ZONE).unwrap();
            let consecutive = Cell::new(0u32);
            let next_attempt_ms = Cell::new(0u64);
            let tick = Timer::every(Duration::from_millis(1)).unwrap();
            let mut forgotten = false;
            let max_passes = key_count
                .div_ceil(crate::antientropy::CLEANUP_MAX_HANDOFF_RPCS_PER_PASS)
                .saturating_mul(2);
            for _ in 0..max_passes {
                let gate = QuiesceGate {
                    shared: &driver_shared,
                    consecutive_timeouts: &consecutive,
                    next_attempt_ms: &next_attempt_ms,
                };
                cleanup_scoped(
                    &store,
                    &lc,
                    &ring,
                    1,
                    usize::MAX,
                    (0, 1),
                    None,
                    None,
                    gate,
                    || true,
                    || false,
                )
                .await
                .unwrap();
                if store.borrow().keys().unwrap().is_empty() {
                    forgotten = true;
                    break;
                }
                tick.tick().await;
            }
            assert!(
                forgotten,
                "cleanup forget starved under sustained read load: {} keys remain",
                store.borrow().keys().unwrap().len()
            );
            for k in &keys {
                assert!(
                    lc.store(2).unwrap().borrow().get(k).unwrap().is_some(),
                    "owner 2 got the handoff for {k:?}"
                );
            }
            assert!(
                driver_max.get() < 100,
                "read admission stalled for {} ms",
                driver_max.get()
            );
            drop(driver_shared.read_gate().acquire().await);
            driver_shared.begin_shutdown();
        });

        ex.run({
            let shared = shared.clone();
            move || server_poll(&shared)
        });
    }

    #[test]
    fn cleanup_quiesces_inflight_reads_instead_of_starving() {
        cleanup_under_continuous_reads(100_000);
    }

    #[test]
    #[ignore = "million-entry re-home release harness"]
    fn million_entry_cleanup_completes_under_continuous_reads() {
        cleanup_under_continuous_reads(1_000_000);
    }

    #[test]
    fn quiesce_wait_doubles_and_clamps_without_overflow() {
        assert_eq!(quiesce_wait(0), COMPACT_QUIESCE_WAIT);
        assert_eq!(quiesce_wait(1), COMPACT_QUIESCE_WAIT * 2);
        assert_eq!(quiesce_wait(2), COMPACT_QUIESCE_WAIT * 4);
        assert_eq!(quiesce_wait(3), COMPACT_QUIESCE_WAIT * 8);
        assert_eq!(quiesce_wait(5), COMPACT_QUIESCE_WAIT_MAX);
        assert_eq!(quiesce_wait(20), COMPACT_QUIESCE_WAIT_MAX);
        assert_eq!(quiesce_wait(64), COMPACT_QUIESCE_WAIT_MAX);
        assert_eq!(quiesce_wait(u32::MAX), COMPACT_QUIESCE_WAIT_MAX);
        assert_eq!(cleanup_quiesce_wait(0), Duration::from_millis(100));
        assert_eq!(cleanup_quiesce_wait(1), Duration::from_millis(200));
        assert_eq!(cleanup_quiesce_wait(2), CLEANUP_QUIESCE_WAIT_MAX);
        assert_eq!(cleanup_quiesce_wait(u32::MAX), CLEANUP_QUIESCE_WAIT_MAX);
        let mut prev = quiesce_wait(0);
        for n in 1..40 {
            let cur = quiesce_wait(n);
            assert!(cur >= prev, "quiesce_wait not monotonic at {n}");
            prev = cur;
        }
    }

    #[test]
    fn compaction_escalates_quiesce_bound_under_sustained_bulk_reads() {
        let ring = Ring::from_members(1, 64, [1]);
        let mut s = store(1);
        let keys: Vec<Vec<u8>> = (0..16).map(|i| format!("k{i:02}").into_bytes()).collect();
        for round in 1..=6u64 {
            let val = format!("v{round}");
            for k in &keys {
                s.apply(
                    k,
                    Some(val.as_bytes()),
                    Version {
                        hlc: round,
                        node: 1,
                    },
                )
                .unwrap();
            }
        }

        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let segs_before = shared
            .zone_store(DEFAULT_ZONE)
            .unwrap()
            .borrow()
            .engine()
            .used_segments();

        let ex = LocalExecutor::new();
        let orch = shared.clone();
        ex.spawn(async move {
            let used_segments = || {
                orch.zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .engine()
                    .used_segments()
            };
            let poll = Timer::every(Duration::from_millis(5)).unwrap();
            let started = Instant::now();
            let mut compacted = false;
            while !compacted && started.elapsed() < Duration::from_secs(10) {
                let g = orch.read_gate().acquire().await;
                while !orch.read_gate().paused() {
                    if used_segments() < segs_before {
                        compacted = true;
                        break;
                    }
                    poll.tick().await;
                    if orch.is_shutdown() {
                        return;
                    }
                }
                if compacted {
                    break;
                }
                let held_since = Instant::now();
                while held_since.elapsed() < Duration::from_millis(150) {
                    poll.tick().await;
                }
                drop(g);
                if used_segments() < segs_before {
                    compacted = true;
                }
            }
            assert!(
                compacted,
                "compaction starved under sustained bulk-read load: still {} segments \
                 (expected < {segs_before}) — the drain bound did not escalate",
                used_segments()
            );
            drop(orch.read_gate().acquire().await);
            orch.begin_shutdown();
        });
        ex.spawn(compact_loop(shared.clone(), Duration::from_millis(300)));
        ex.run({
            let shared = shared.clone();
            move || server_poll(&shared)
        });
    }

    #[test]
    fn sharded_node_per_shard_anti_entropy_heals() {
        use crate::ring::hash64;
        const NSHARD: usize = 2;

        let ring = Ring::from_members(2, 64, [1, 2]);
        let keys: Vec<Vec<u8>> = (0..60).map(|i| format!("k{i:03}").into_bytes()).collect();
        let shard_of = |k: &[u8]| (hash64(k) % NSHARD as u64) as usize;

        let mut n2_stores: Vec<_> = (0..NSHARD).map(|_| store(2)).collect();
        for k in &keys {
            n2_stores[shard_of(k)]
                .apply(k, Some(b"v"), Version { hlc: 100, node: 9 })
                .unwrap();
        }
        let n1_stores: Vec<_> = (0..NSHARD).map(|_| store(1)).collect();

        let n1 = Node::bind_n(NSHARD).unwrap();
        let n2 = Node::bind_n(NSHARD).unwrap();
        let n1_addrs: Vec<SocketAddr> = n1.iter().map(|(_, a)| *a).collect();
        let n2_addrs: Vec<SocketAddr> = n2.iter().map(|(_, a)| *a).collect();
        let peer_shards = vec![(1u32, n1_addrs.clone()), (2u32, n2_addrs)];

        let interval = Duration::from_millis(25);
        let n1_shards: Vec<_> = n1
            .into_iter()
            .zip(n1_stores)
            .map(|((l, _), s)| (l, s))
            .collect();
        let n2_shards: Vec<_> = n2
            .into_iter()
            .zip(n2_stores)
            .map(|((l, _), s)| (l, s))
            .collect();
        let _node = crate::testutil::running(Node::serve_sharded(
            1,
            n1_shards,
            ring.clone(),
            peer_shards.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve_sharded(
            2,
            n2_shards,
            ring,
            peer_shards,
            interval,
        ));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let healed = keys
                .iter()
                .all(|k| read_from(n1_addrs[shard_of(k)], k).as_deref() == Some(&b"v"[..]));
            if healed {
                break;
            }
            assert!(Instant::now() < deadline, "sharded node did not heal");
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn multidrive_node_serves_partitions_across_drives() {
        use crate::coord::Coordinator;
        use crate::drive::{MemDriveSet, PartitionGeom};
        use crate::net::NetTransport;
        use crate::reactor;

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();

        let ds = MemDriveSet::uniform(256, 4096, 2);
        let geom = PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];

        let (weight, handle) = Node::serve_multidrive(
            1,
            shard_listeners,
            ds,
            512,
            geom,
            ring.clone(),
            peer_shards,
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);
        assert_eq!(weight, 2, "ring weight equals the drive count");

        let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));
        for i in 0..40 {
            let k = format!("k{i:02}");
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }
        for i in 0..40 {
            let k = format!("k{i:02}");
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(&b"v"[..]),
                "key {k} missing from its drive-backed partition"
            );
        }
    }

    #[cfg(test)]
    struct FaultDrives {
        inner: crate::drive::MemDriveSet,
        ops: std::cell::Cell<usize>,
        fail_at: usize,
    }
    #[cfg(test)]
    impl crate::drive::DriveSet for FaultDrives {
        type Dev = MemBlockDevice;
        fn drive_count(&self) -> usize {
            self.inner.drive_count()
        }
        fn drive_blocks(&self, d: usize) -> u64 {
            self.inner.drive_blocks(d)
        }
        fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
            self.inner.read_layout_fingerprint()
        }
        fn write_layout_fingerprint(&self, fp: &[u8]) -> Result<(), std::io::Error> {
            self.inner.write_layout_fingerprint(fp)
        }
        fn read_relocate_journal(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
            self.inner.read_relocate_journal()
        }
        fn write_relocate_journal(&self, j: &[u8]) -> Result<(), std::io::Error> {
            let n = self.ops.get() + 1;
            self.ops.set(n);
            if n == self.fail_at {
                return Err(std::io::Error::other("injected crash"));
            }
            self.inner.write_relocate_journal(j)
        }
        fn clear_relocate_journal(&self) -> Result<(), std::io::Error> {
            self.inner.clear_relocate_journal()
        }
        fn open_region(
            &self,
            drive: usize,
            base: u64,
            len: u64,
            create: bool,
        ) -> Result<MemBlockDevice, std::io::Error> {
            self.inner.open_region(drive, base, len, create)
        }
    }

    #[test]
    fn multidrive_live_drive_add_under_load_rebalances_without_loss() {
        use crate::coord::Coordinator;
        use crate::drive::{
            build_stores, drive_caps, node_weight, verify_layout, DriveSet, Layout, MemDriveSet,
        };
        use crate::net::NetTransport;
        use crate::reactor;

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let region_blocks = 512u64;
        let geom = md_geom();
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];

        let ds = MemDriveSet::with_caps(256, vec![4096, 4096, 4096, 0]);
        let (weight, handle) = Node::serve_multidrive(
            1,
            shard_listeners,
            ds.share(),
            region_blocks,
            geom,
            ring.clone(),
            peer_shards,
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let node = crate::testutil::running(handle);
        assert_eq!(weight, node_weight(4), "serve counts all 4 drive slots");

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let n_pre = 200usize;
        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).unwrap();
        }

        ds.set_cap(3, 4096);
        let new_caps = drive_caps(&ds);
        let reb = node.rebalancer();
        let (ds_bg, caps_bg) = (ds.share(), new_caps.clone());
        let hjoin = thread::spawn(move || reb.rebalance_drives(&ds_bg, &caps_bg, geom, 1));

        let mut live_written: Vec<(String, String)> = Vec::new();
        let mut i = 0usize;
        while !hjoin.is_finished() {
            let (k, v) = (format!("live{i:05}"), format!("w{i}"));
            if vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).is_ok()
            {
                live_written.push((k, v));
            }
            i += 1;
        }
        let (moved, new_weight) = hjoin.join().unwrap().unwrap();
        assert!(moved > 0, "some partitions must move onto the new drive");
        assert_eq!(new_weight, node_weight(4), "all 4 drives are now active");
        assert!(
            !live_written.is_empty(),
            "concurrent writes overlapped the move"
        );

        let new_layout = Layout::decode(&ds.read_layout_fingerprint().unwrap().unwrap()).unwrap();
        assert!(
            new_layout.partitions_on(3) > 0,
            "the newly-added drive should host partitions"
        );

        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(v.as_bytes()),
                "pre-existing key {k} lost or wrong after live rebalance"
            );
        }
        for (k, v) in &live_written {
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(v.as_bytes()),
                "concurrently-written key {k} lost or wrong after live rebalance"
            );
        }

        drop(node);
        verify_layout(&ds, &new_layout, false).unwrap();
        let stores = build_stores(&ds, &new_layout, geom, 1, false).unwrap();
        let mut all: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
        for s in &stores {
            for k in s.keys().unwrap() {
                if let Some(v) = s.get(&k).unwrap() {
                    all.insert(k, v);
                }
            }
        }
        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            assert_eq!(
                all.get(k.as_bytes()).map(|v| v.as_slice()),
                Some(v.as_bytes()),
                "pre-existing key {k} not durable after reopen"
            );
        }
    }

    #[test]
    fn multidrive_live_drive_retire_under_load_drains_without_loss() {
        use crate::coord::Coordinator;
        use crate::drive::{drive_caps, node_weight, DriveSet, Layout, MemDriveSet};
        use crate::net::NetTransport;
        use crate::reactor;

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let region_blocks = 512u64;
        let geom = md_geom();
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];

        let ds = MemDriveSet::with_caps(256, vec![4096, 4096, 4096, 4096]);
        let (weight, handle) = Node::serve_multidrive(
            1,
            shard_listeners,
            ds.share(),
            region_blocks,
            geom,
            ring.clone(),
            peer_shards,
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let node = crate::testutil::running(handle);
        assert_eq!(weight, node_weight(4));

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let n_pre = 200usize;
        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).unwrap();
        }

        ds.set_cap(2, 0);
        let new_caps = drive_caps(&ds);
        let reb = node.rebalancer();
        let (ds_bg, caps_bg) = (ds.share(), new_caps.clone());
        let hjoin = thread::spawn(move || reb.rebalance_drives(&ds_bg, &caps_bg, geom, 1));

        let mut live_written: Vec<(String, String)> = Vec::new();
        let mut i = 0usize;
        while !hjoin.is_finished() {
            let (k, v) = (format!("live{i:05}"), format!("w{i}"));
            if vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).is_ok()
            {
                live_written.push((k, v));
            }
            i += 1;
        }
        let (moved, new_weight) = hjoin.join().unwrap().unwrap();
        assert!(
            moved > 0,
            "retiring a drive must move its partitions off it"
        );
        assert_eq!(
            new_weight,
            node_weight(3),
            "ring weight drops with the drive"
        );
        assert!(
            !live_written.is_empty(),
            "concurrent writes overlapped the move"
        );

        let new_layout = Layout::decode(&ds.read_layout_fingerprint().unwrap().unwrap()).unwrap();
        assert_eq!(
            new_layout.partitions_on(2),
            0,
            "the retired drive must host no partitions"
        );

        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(v.as_bytes()),
                "pre-existing key {k} lost or wrong after live retire"
            );
        }
        for (k, v) in &live_written {
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(v.as_bytes()),
                "concurrently-written key {k} lost or wrong after live retire"
            );
        }
    }

    #[test]
    fn multidrive_live_rebalance_crash_mid_move_recovers_offline() {
        use crate::coord::Coordinator;
        use crate::drive::{
            build_stores, drive_caps, plan, relocate, replan, verify_layout, DriveSet, MemDriveSet,
        };
        use crate::net::NetTransport;
        use crate::reactor;

        for fail_at in [2usize, 3] {
            let p = 4;
            let listeners = Node::bind_n(p).unwrap();
            let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
            let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
            let region_blocks = 512u64;
            let geom = md_geom();
            let ring = Ring::from_members(1, 64, [1]);
            let peer_shards = vec![(1u32, addrs.clone())];

            let ds = MemDriveSet::with_caps(256, vec![4096, 4096, 4096, 0]);
            let (_weight, handle) = Node::serve_multidrive(
                1,
                shard_listeners,
                ds.share(),
                region_blocks,
                geom,
                ring.clone(),
                peer_shards,
                Duration::from_secs(3600),
                true,
            )
            .unwrap();
            let node = crate::testutil::running(handle);

            let coord = Coordinator::new(
                0,
                ring.clone(),
                1,
                NetTransport::connect_sharded([(1u32, addrs.clone())]),
            );
            let n = 80usize;
            for i in 0..n {
                let (k, v) = (format!("k{i:04}"), format!("v{i}"));
                vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io)
                    .unwrap();
            }

            let old = plan(&[4096, 4096, 4096, 0], region_blocks, p, p).unwrap();
            ds.set_cap(3, 4096);
            let new_caps = drive_caps(&ds);
            let new = replan(&old, &new_caps, p).unwrap();

            let faulted = FaultDrives {
                inner: ds.share(),
                ops: std::cell::Cell::new(0),
                fail_at,
            };
            let res = node.rebalance_drives(&faulted, &new_caps, geom, 1);
            let aborted = res.is_err();
            if aborted {
                assert!(
                    ds.read_relocate_journal().unwrap().is_some(),
                    "an aborted live rebalance leaves its journal behind"
                );
            }
            drop(node);
            relocate(&ds, &old, &new, geom, 1)
                .unwrap_or_else(|e| panic!("offline resume after crash at {fail_at} failed: {e}"));
            assert!(ds.read_relocate_journal().unwrap().is_none());
            verify_layout(&ds, &new, false).unwrap();
            let stores = build_stores(&ds, &new, geom, 1, false).unwrap();
            let mut all: std::collections::HashMap<Vec<u8>, Vec<u8>> =
                std::collections::HashMap::new();
            for s in &stores {
                for k in s.keys().unwrap() {
                    if let Some(v) = s.get(&k).unwrap() {
                        all.insert(k, v);
                    }
                }
            }
            for i in 0..n {
                let (k, v) = (format!("k{i:04}"), format!("v{i}"));
                assert_eq!(
                    all.get(k.as_bytes()).map(|v| v.as_slice()),
                    Some(v.as_bytes()),
                    "key {k} lost after crash at {fail_at} (aborted={aborted}) + offline resume"
                );
            }
        }
    }

    #[test]
    fn multidrive_live_rebalance_crash_after_swap_pre_done_keeps_post_swap_writes() {
        use crate::coord::Coordinator;
        use crate::drive::{
            build_stores, drive_caps, plan, relocate, replan, verify_layout, DriveSet, MemDriveSet,
        };
        use crate::net::NetTransport;
        use crate::reactor;

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let region_blocks = 512u64;
        let geom = md_geom();
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];

        let ds = MemDriveSet::with_caps(256, vec![4096, 4096, 4096, 0]);
        let (_weight, handle) = Node::serve_multidrive(
            1,
            shard_listeners,
            ds.share(),
            region_blocks,
            geom,
            ring.clone(),
            peer_shards,
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let node = crate::testutil::running(handle);

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let n_pre = 80usize;
        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).unwrap();
        }

        let old = plan(&[4096, 4096, 4096, 0], region_blocks, p, p).unwrap();
        ds.set_cap(3, 4096);
        let new_caps = drive_caps(&ds);
        let new = replan(&old, &new_caps, p).unwrap();

        let faulted = FaultDrives {
            inner: ds.share(),
            ops: std::cell::Cell::new(0),
            fail_at: 3,
        };
        let res = node.rebalance_drives(&faulted, &new_caps, geom, 1);
        assert!(
            res.is_err(),
            "the injected crash must abort the rebalance mid-run"
        );
        assert!(
            ds.read_relocate_journal().unwrap().is_some(),
            "an aborted live rebalance leaves its journal behind"
        );

        let n_post = 80usize;
        let mut post: Vec<(String, String)> = Vec::new();
        for i in 0..n_post {
            let (k, v) = (format!("post{i:04}"), format!("pv{i}"));
            vals_rt::block_on(coord.write(k.as_bytes(), v.as_bytes()), reactor::poll_io).unwrap();
            post.push((k, v));
        }

        drop(node);

        relocate(&ds, &old, &new, geom, 1)
            .unwrap_or_else(|e| panic!("offline resume after post-swap crash failed: {e}"));
        assert!(ds.read_relocate_journal().unwrap().is_none());
        verify_layout(&ds, &new, false).unwrap();
        let stores = build_stores(&ds, &new, geom, 1, false).unwrap();
        let mut all: std::collections::HashMap<Vec<u8>, Vec<u8>> = std::collections::HashMap::new();
        for s in &stores {
            for k in s.keys().unwrap() {
                if let Some(v) = s.get(&k).unwrap() {
                    all.insert(k, v);
                }
            }
        }
        for i in 0..n_pre {
            let (k, v) = (format!("pre{i:04}"), format!("v{i}"));
            assert_eq!(
                all.get(k.as_bytes()).map(|v| v.as_slice()),
                Some(v.as_bytes()),
                "pre-rebalance key {k} lost after post-swap crash + offline resume"
            );
        }
        for (k, v) in &post {
            assert_eq!(
                all.get(k.as_bytes()).map(|v| v.as_slice()),
                Some(v.as_bytes()),
                "post-swap key {k} lost after crash + offline resume (durability window)"
            );
        }
    }

    #[test]
    fn multidrive_with_liveness_unfences_and_persists_per_shard_watermarks() {
        use crate::coord::Coordinator;
        use crate::drive::{MemDriveSet, PartitionGeom};
        use crate::liveness::FileLivenessLog;
        use crate::net::NetTransport;
        use crate::reactor;

        let now = physical_now_ms();
        let base =
            std::env::temp_dir().join(format!("vals-md-liveness-{}-{:x}", std::process::id(), now));
        std::fs::create_dir_all(&base).unwrap();

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();

        let ds = MemDriveSet::uniform(256, 4096, 2);
        let geom = PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];
        let grace = 60_000u64;
        let wm_path = |s: usize| base.join(format!("liveness-shard{s}.wm"));

        let base_c = base.clone();
        let (weight, handle) = Node::serve_multidrive_with_liveness(
            1,
            shard_listeners,
            ds,
            512,
            geom,
            ring.clone(),
            peer_shards,
            Duration::from_millis(25),
            true,
            grace,
            move |s| FileLivenessLog::new(base_c.join(format!("liveness-shard{s}.wm"))),
        )
        .unwrap();
        assert_eq!(weight, 2, "ring weight equals the drive count");

        let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));
        for i in 0..40 {
            let k = format!("k{i:02}");
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let ok = (0..40).all(|i| {
                let k = format!("k{i:02}");
                vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io)
                    .ok()
                    .flatten()
                    .and_then(|s| s.value)
                    .as_deref()
                    == Some(&b"v"[..])
            });
            if ok {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "multidrive liveness node did not unfence + serve in time"
            );
            thread::sleep(Duration::from_millis(20));
        }

        within_secs(10, move || handle.shutdown());
        for s in 0..p {
            let wm = FileLivenessLog::new(wm_path(s))
                .load()
                .unwrap_or_else(|| panic!("shard {s} watermark not persisted"));
            assert!(wm >= now, "shard {s} watermark {wm} < write progress {now}");
        }
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn multidrive_liveness_shard_gt0_unfences_against_single_shard_peers() {
        use crate::coord::Coordinator;
        use crate::drive::{MemDriveSet, PartitionGeom};
        use crate::liveness::FileLivenessLog;
        use crate::net::NetTransport;
        use crate::reactor;

        let now = physical_now_ms();
        let recent = now - 1_000;
        let grace = 60_000u64;

        let base = std::env::temp_dir().join(format!(
            "vals-md-liveness-0079-{}-{:x}",
            std::process::id(),
            now
        ));
        std::fs::create_dir_all(&base).unwrap();
        let wm_path = |s: usize| base.join(format!("liveness-shard{s}.wm"));

        let p = 2;
        let listeners1 = Node::bind_n(p).unwrap();
        let addrs1: Vec<SocketAddr> = listeners1.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners1.into_iter().map(|(l, _)| l).collect();
        for s in 0..p {
            FileLivenessLog::new(wm_path(s)).store(recent).unwrap();
        }

        let (l2, a2) = Node::bind().unwrap();
        let s2 = store(2);
        let log2 = MemLivenessLog::new();
        log2.store(recent).unwrap();

        let ring = Ring::from_members(2, 64, [1, 2]);
        let interval = Duration::from_millis(25);
        let peer_shards = vec![(1u32, addrs1.clone()), (2u32, vec![a2])];

        let ds = MemDriveSet::uniform(256, 4096, 2);
        let geom = PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let base_c = base.clone();
        let (_weight, h1) = Node::serve_multidrive_with_liveness(
            1,
            shard_listeners,
            ds,
            512,
            geom,
            ring.clone(),
            peer_shards,
            interval,
            true,
            grace,
            move |s| FileLivenessLog::new(base_c.join(format!("liveness-shard{s}.wm"))),
        )
        .unwrap();
        let _n1 = crate::testutil::running(h1);
        let _n2 = crate::testutil::running(Node::serve_with_liveness(
            2,
            l2,
            s2,
            ring.clone(),
            vec![(1, addrs1[0]), (2, a2)],
            interval,
            grace,
            log2,
        ));

        let shard_of = |k: &str| (crate::ring::hash64(k.as_bytes()) % 2) as usize;
        let mut k0 = None;
        let mut k1 = None;
        let mut i = 0;
        while k0.is_none() || k1.is_none() {
            let k = format!("key-{i}");
            match shard_of(&k) {
                0 if k0.is_none() => k0 = Some(k),
                1 if k1.is_none() => k1 = Some(k),
                _ => {}
            }
            i += 1;
        }
        let (k0, k1) = (k0.unwrap(), k1.unwrap());

        let coord = Coordinator::new(
            0,
            ring,
            2,
            NetTransport::connect_sharded([(1u32, addrs1.clone()), (2u32, vec![a2])]),
        );
        for k in [&k0, &k1] {
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let s0 = read_from(addrs1[0], k0.as_bytes()).as_deref() == Some(&b"v"[..]);
            let s1 = read_from(addrs1[1], k1.as_bytes()).as_deref() == Some(&b"v"[..]);
            if s0 && s1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shard>0 did not unfence against a single-shard peer \
                 (shard0 served={s0}, shard1 served={s1}); task 0079 regression"
            );
            thread::sleep(Duration::from_millis(20));
        }

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn eager_sweep_rebuilds_local_accounting_without_reconciliation_peers() {
        let mut s = store(1);
        s.append_unknown_for_test(
            b"dirty",
            Some(b"value"),
            Version { hlc: 1, node: 1 },
        )
        .unwrap();
        assert_eq!(s.exact_live_keys(), None);

        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let (maint, mut join) = crate::maint::spawn::<MemBlockDevice>();
        let rebuilt = Rc::new(Cell::new(false));
        let ex = LocalExecutor::new();
        ex.spawn(sweep_loop(
            shared.clone(),
            Vec::new(),
            0,
            Vec::new(),
            Vec::new(),
            maint.submitter(),
        ));
        let observer = shared.clone();
        let rebuilt_out = rebuilt.clone();
        ex.spawn(async move {
            for _ in 0..10_000 {
                if observer
                    .zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .exact_live_keys()
                    == Some(1)
                {
                    rebuilt_out.set(true);
                    break;
                }
                vals_rt::yield_now().await;
            }
            observer.begin_shutdown();
        });
        shared.kick_sweep();
        ex.run(crate::reactor::poll_io_nonblocking);
        drop(maint);
        join.join();

        assert!(
            rebuilt.get(),
            "an eager accounting request must not depend on entering peer reconciliation"
        );
    }

    struct PollGatedSlot {
        done: AtomicBool,
        thread: std::thread::ThreadId,
        waker: Mutex<Option<std::task::Waker>>,
    }

    #[derive(Default)]
    struct PollGatedState {
        gated: AtomicBool,
        pending: Mutex<Vec<Arc<PollGatedSlot>>>,
        submitted: AtomicUsize,
        completed: AtomicUsize,
    }

    struct PollGatedDevice {
        inner: MemBlockDevice,
        state: Arc<PollGatedState>,
    }

    struct PollGatedRead<'a> {
        state: &'a PollGatedState,
        slot: Option<Arc<PollGatedSlot>>,
    }

    impl std::future::Future for PollGatedRead<'_> {
        type Output = ();

        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if let Some(slot) = &self.slot {
                if slot.done.load(AtomicOrdering::Acquire) {
                    return std::task::Poll::Ready(());
                }
                *slot.waker.lock().unwrap() = Some(cx.waker().clone());
                return std::task::Poll::Pending;
            }
            let slot = Arc::new(PollGatedSlot {
                done: AtomicBool::new(false),
                thread: std::thread::current().id(),
                waker: Mutex::new(Some(cx.waker().clone())),
            });
            self.state.pending.lock().unwrap().push(slot.clone());
            self.state.submitted.fetch_add(1, AtomicOrdering::Relaxed);
            vals_spdk::note_async_io_submitted();
            self.slot = Some(slot);
            std::task::Poll::Pending
        }
    }

    impl Drop for PollGatedRead<'_> {
        fn drop(&mut self) {
            let Some(slot) = self.slot.take() else {
                return;
            };
            let mut pending = self.state.pending.lock().unwrap();
            let before = pending.len();
            pending.retain(|queued| !Arc::ptr_eq(queued, &slot));
            if pending.len() != before {
                vals_spdk::note_async_io_completed();
            }
        }
    }

    impl BlockDevice for PollGatedDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }

        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }

        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }

        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            self.inner.write_at(lba, buf)
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            self.inner.flush()
        }

        fn poll_device(&self) -> i32 {
            let here = std::thread::current().id();
            let ready: Vec<Arc<PollGatedSlot>> = {
                let mut pending = self.state.pending.lock().unwrap();
                let (ready, rest) = std::mem::take(&mut *pending)
                    .into_iter()
                    .partition(|slot| slot.thread == here);
                *pending = rest;
                ready
            };
            for slot in &ready {
                slot.done.store(true, AtomicOrdering::Release);
                self.state.completed.fetch_add(1, AtomicOrdering::Relaxed);
                vals_spdk::note_async_io_completed();
                if let Some(waker) = slot.waker.lock().unwrap().take() {
                    waker.wake();
                }
            }
            ready.len() as i32
        }

        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(PollGatedDevice {
                inner: self.inner.share(),
                state: self.state.clone(),
            })
        }
    }

    impl AsyncRead for PollGatedDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            if self.state.gated.load(AtomicOrdering::Acquire) {
                PollGatedRead {
                    state: &self.state,
                    slot: None,
                }
                .await;
            }
            self.inner.read_at(lba, buf)
        }
    }

    #[test]
    fn cleanup_batch_from_poll_gated_device_commits_on_maintenance_thread() {
        let state = Arc::new(PollGatedState::default());
        let engine = Engine::create_prewiped(
            PollGatedDevice {
                inner: MemBlockDevice::new(4096, 1024),
                state: state.clone(),
            },
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: SOrdering::Sorted },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        let ring = Ring::from_members(1, 64, [1, 2]);
        let key_for = |owner| (0..1000)
            .map(|i| format!("k{i:04}").into_bytes())
            .find(|key| ring.replicas(key) == vec![owner]).unwrap();
        let drop_key = key_for(2);
        let keep_key = key_for(1);
        let version = Version { hlc: 100, node: 1 };
        for key in [&drop_key, &keep_key] {
            store.apply(key, Some(b"v"), version).unwrap();
        }
        store.flush().unwrap();
        let fingerprint = (1, 2, 3, 4);
        let dev = store.device_handle();
        let local = RefCell::new(store);
        let mut cluster = crate::coord::LocalCluster::new();
        cluster.insert(2, self::store(2));
        state.gated.store(true, AtomicOrdering::Release);
        let progress = vals_rt::block_on(
            cleanup_scoped(&local, &cluster, &ring, 1, 1, (0, 1), None, None, || true, || true, || false),
            || { dev.poll_device(); },
        ).unwrap();
        assert!(progress.wrapped && progress.deferred);
        let plan = progress.forget_plan.expect("wrapped cleanup batch from poll-completed reads");
        let mut store = local.into_inner();
        let (maint, mut join) = crate::maint::spawn::<PollGatedDevice>();
        assert!(maint.submit(crate::maint::MaintJob::CleanupCompact(DEFAULT_ZONE, fingerprint, plan)).is_ok());
        let deadline = Instant::now() + Duration::from_secs(3);
        let done = loop {
            if let Some(done) = maint.drain().pop() {
                break Some(done);
            }
            if Instant::now() >= deadline {
                break None;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        drop(maint);
        join.join();
        let Some(crate::maint::MaintDone::CleanupCompacted(zone, fp, _, result)) = done else {
            panic!("maintenance cleanup must finish using its own device poll");
        };
        assert_eq!(zone, DEFAULT_ZONE);
        assert_eq!(fp, fingerprint);
        let written = result.unwrap();
        store.compact_commit(written).unwrap();
        assert!(store.get(&drop_key).unwrap().is_none());
        assert_eq!(store.get(&keep_key).unwrap().as_deref(), Some(&b"v"[..]));
        assert!(state.submitted.load(AtomicOrdering::Relaxed) > 0);
        assert_eq!(state.submitted.load(AtomicOrdering::Relaxed), state.completed.load(AtomicOrdering::Relaxed));
        assert_eq!(vals_spdk::async_io_pending(), 0);
    }

    #[test]
    fn idle_shard_sweep_cleanup_progresses_when_reads_complete_only_on_device_poll() {
        let state = Arc::new(PollGatedState::default());
        let engine = Engine::create_prewiped(
            PollGatedDevice {
                inner: MemBlockDevice::new(256, 8192),
                state: state.clone(),
            },
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..64u64 {
            s.apply(
                format!("k{i:03}").as_bytes(),
                Some(b"v"),
                Version { hlc: i + 1, node: 1 },
            )
            .unwrap();
        }
        s.flush().unwrap();
        assert!(s.engine().run_count() > 0, "cleanup must page run-backed keys");
        state.gated.store(true, AtomicOrdering::Release);

        let ring = Ring::from_members(1, 64, [2]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let (maint, mut join) = crate::maint::spawn::<PollGatedDevice>();
        let ex = LocalExecutor::new();
        ex.spawn(sweep_loop(
            shared.clone(),
            Vec::new(),
            0,
            Vec::new(),
            Vec::new(),
            maint.submitter(),
        ));
        let observer = shared.clone();
        let passes = ex.run_until(
            async move {
                let tick = Timer::every(Duration::from_millis(10)).unwrap();
                let deadline = Instant::now() + Duration::from_secs(3);
                while Instant::now() < deadline && observer.sweep_pass_count() < 4 {
                    observer.signal_periodic();
                    tick.tick().await;
                }
                observer.sweep_pass_count()
            },
            {
                let shared = shared.clone();
                move || server_poll(&shared)
            },
        );
        shared.begin_shutdown();
        drop(ex);
        drop(maint);
        join.join();

        assert!(
            state.submitted.load(AtomicOrdering::Relaxed) > 0,
            "the cleanup scan must issue poll-completed device reads"
        );
        assert!(
            passes >= 4,
            "an idle shard's sweep froze on an unpolled cleanup read after {passes} pass(es) \
             ({} submitted, {} completed)",
            state.submitted.load(AtomicOrdering::Relaxed),
            state.completed.load(AtomicOrdering::Relaxed)
        );
        assert_eq!(vals_spdk::async_io_pending(), 0);
    }

    #[test]
    fn cleanup_lap_runs_from_its_origin_to_the_end_then_from_the_start_back_to_it() {
        let mut lap = CleanupLap::starting_at(Some(b"m".to_vec()));
        assert_eq!((lap.resume(), lap.end()), (Some(&b"m"[..]), None));
        assert!(!lap.advance(Some(b"q".to_vec()), false));
        assert!(!lap.advance(None, false), "a page with no cursor and no wrap keeps the cursor");
        assert_eq!(lap.resume(), Some(&b"q"[..]));
        assert!(!lap.advance(None, true), "the end of the keyspace starts the second lap");
        assert_eq!((lap.resume(), lap.end()), (None, Some(&b"m"[..])));
        assert!(!lap.advance(Some(b"c".to_vec()), false));
        assert_eq!((lap.resume(), lap.end()), (Some(&b"c"[..]), Some(&b"m"[..])));
        assert!(lap.advance(None, true), "reaching the origin completes the traversal");

        let mut whole = CleanupLap::starting_at(None);
        assert_eq!((whole.resume(), whole.end()), (None, None));
        assert!(!whole.advance(Some(b"c".to_vec()), false));
        assert!(whole.advance(None, true), "no origin: one lap from the start");
    }

    #[test]
    fn log_every_reports_once_per_interval_with_the_suppressed_count() {
        let t0 = Instant::now();
        let every = Duration::from_secs(60);
        let mut log = LogEvery::default();
        assert_eq!(log.note(t0, every), Some(0));
        assert_eq!(log.note(t0 + Duration::from_secs(1), every), None);
        assert_eq!(log.note(t0 + Duration::from_secs(59), every), None);
        assert_eq!(log.note(t0 + Duration::from_secs(60), every), Some(2));
        assert_eq!(log.note(t0 + Duration::from_secs(61), every), None);
    }

    struct CleanupArm {
        passes: u64,
        elapsed: Duration,
        lap_keys: usize,
    }

    fn cleanup_arm(burst_ms: u64, origin: Option<Vec<u8>>, owned: usize, stray: &[u8]) -> CleanupArm {
        cleanup_arm_on(MemBlockDevice::new(4096, 16384), burst_ms, origin, owned, stray)
    }

    fn cleanup_arm_on<D>(dev: D, burst_ms: u64, origin: Option<Vec<u8>>, owned: usize, stray: &[u8]) -> CleanupArm
    where
        D: BlockDevice + AsyncRead + Send + 'static,
    {
        use crate::drive::MemDriveSet;

        let ring = Ring::from_members(1, 64, [1, 2]);
        let owner_of = |key: &[u8]| ring.replicas(key)[0];
        assert_eq!(owner_of(stray), 2, "the stray belongs on node 2");
        let listeners = Node::bind_n(1).unwrap();
        let addr2 = listeners[0].1;
        let (_w, handle) = Node::serve_multidrive_zoned(
            2,
            listeners.into_iter().map(|(l, _)| l).collect(),
            SharedDs(std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2))),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(2, vec![addr2])],
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let _node2 = crate::testutil::running(handle);
        set_zones(addr2, &crate::zone::Catalog::with_default(1), 1);

        let engine = Engine::create_prewiped(
            dev,
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: SOrdering::Unsorted },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        let mut held = 0;
        let mut i = 0u64;
        while held < owned {
            let key = format!("k{i:07}");
            i += 1;
            if owner_of(key.as_bytes()) == 1 {
                s.apply(key.as_bytes(), Some(b"v"), Version { hlc: 1 << 20, node: 1 }).unwrap();
                held += 1;
            }
        }
        s.flush().unwrap();
        s.apply(stray, Some(b"v"), Version { hlc: 1 << 20, node: 1 }).unwrap();
        let lap_keys = owned + 1;
        let limit = Duration::from_secs(30)
            .max(Duration::from_secs(2 * (lap_keys / crate::antientropy::CLEANUP_MAX_KEYS_PER_PASS + 2) as u64));

        let shared = Rc::new(
            ServerShared::single_zone_on(1, s, ring).with_settings(crate::node_settings::NodeSettings {
                cleanup_idle_burst_ms: burst_ms,
                ..crate::node_settings::NodeSettings::default()
            }),
        );
        shared.set_sweep_every(Duration::from_millis(1000));
        set_cleanup_origin_override(Some(origin));
        let (maint, mut join) = crate::maint::spawn::<D>();
        let ex = LocalExecutor::new();
        ex.spawn(sweep_loop(shared.clone(), vec![(2, addr2)], 0, Vec::new(), Vec::new(), maint.submitter()));
        ex.spawn(maint_loop(shared.clone(), maint));
        let observer = shared.clone();
        let stray_key = stray.to_vec();
        let (passes, elapsed) = ex.run_until(
            async move {
                let tick = Timer::every(Duration::from_millis(5)).unwrap();
                let started = Instant::now();
                let mut signalled = started;
                observer.signal_periodic();
                loop {
                    let gone = observer
                        .zone_store(DEFAULT_ZONE)
                        .unwrap()
                        .borrow()
                        .get(&stray_key)
                        .unwrap()
                        .is_none();
                    if gone || started.elapsed() > limit {
                        return (observer.sweep_pass_count(), started.elapsed());
                    }
                    if signalled.elapsed() >= observer.sweep_every() {
                        signalled = Instant::now();
                        observer.signal_periodic();
                    }
                    tick.tick().await;
                }
            },
            {
                let shared = shared.clone();
                move || server_poll(&shared)
            },
        );
        set_cleanup_origin_override(None);
        shared.begin_shutdown();
        drop(ex);
        join.join();
        assert!(elapsed < limit, "the stray was never cleaned up");
        CleanupArm { passes, elapsed, lap_keys }
    }

    #[test]
    fn cleanup_judges_ownership_with_the_ring_its_placement_stamp_names() {
        ring_adopted_mid_cleanup_keeps_the_newly_owned_key(false);
    }

    #[test]
    fn cleanup_handoff_confirmed_under_a_superseded_ring_does_not_forget() {
        ring_adopted_mid_cleanup_keeps_the_newly_owned_key(true);
    }

    fn ring_adopted_mid_cleanup_keeps_the_newly_owned_key(during_handoff: bool) {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use std::sync::mpsc;

        let old_ring = Ring::from_members(1, 64, [1, 2]);
        let stray = (0..)
            .map(|i| format!("adopted-{i}").into_bytes())
            .find(|k| old_ring.replicas(k) == vec![2])
            .unwrap();
        let mut new_ring = old_ring.clone();
        new_ring.set_members_weighted(old_ring.members_weighted().filter(|&(n, _)| n == 1).collect::<Vec<_>>());
        assert_eq!(new_ring.replicas(&stray), vec![1], "the adopted ring makes node 1 the owner");

        let gated_seen = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let handoffs = Arc::new(AtomicUsize::new(0));
        let lst = TcpListener::bind("127.0.0.1:0").unwrap();
        let peer = lst.local_addr().unwrap();
        {
            let gated_seen = gated_seen.clone();
            let handoffs = handoffs.clone();
            thread::spawn(move || {
                for conn in lst.incoming() {
                    let Ok(mut s) = conn else { return };
                    let gated_seen = gated_seen.clone();
                    let release_rx = release_rx.clone();
                    let handoffs = handoffs.clone();
                    thread::spawn(move || {
                        if crate::proto::server_handshake_blocking(&mut s).is_err() {
                            return;
                        }
                        loop {
                            let mut lenb = [0u8; 4];
                            if s.read_exact(&mut lenb).is_err() {
                                return;
                            }
                            let mut block = vec![0u8; u32::from_le_bytes(lenb) as usize];
                            if s.read_exact(&mut block).is_err() {
                                return;
                            }
                            let reqid = u64::from_le_bytes(block[0..8].try_into().unwrap());
                            let resp = match wire::decode_request(&block[8..]) {
                                Ok(wire::OwnedRequest::GetWatermark) => {
                                    if !during_handoff && !gated_seen.swap(true, AtomicOrdering::SeqCst) {
                                        let _ = release_rx.lock().unwrap().recv();
                                    }
                                    Response::Watermark(physical_now_ms())
                                }
                                Ok(wire::OwnedRequest::Apply { .. }) => {
                                    handoffs.fetch_add(1, AtomicOrdering::SeqCst);
                                    if during_handoff && !gated_seen.swap(true, AtomicOrdering::SeqCst) {
                                        let _ = release_rx.lock().unwrap().recv();
                                    }
                                    Response::Apply(crate::replica::Applied::Won)
                                }
                                _ => Response::Err("mock: unsupported".into()),
                            };
                            let payload = wire::encode_response_payload(&resp);
                            let mut frame = ((8 + payload.len()) as u32).to_le_bytes().to_vec();
                            frame.extend_from_slice(&reqid.to_le_bytes());
                            frame.extend_from_slice(&payload);
                            if s.write_all(&frame).is_err() {
                                return;
                            }
                        }
                    });
                }
            });
        }

        let mut store = self::store(1);
        store.apply(&stray, Some(b"v"), ver(physical_now_ms(), 1)).unwrap();
        let shared = Rc::new(
            ServerShared::single_zone_on(1, store, old_ring)
                .with_liveness(3_600_000, Box::new(crate::liveness::MemLivenessLog::new())),
        );
        shared.set_sweep_every(Duration::from_secs(3600));
        let (maint, mut join) = crate::maint::spawn::<MemBlockDevice>();
        let ex = LocalExecutor::new();
        ex.spawn(sweep_loop(shared.clone(), vec![(2, peer)], 0, Vec::new(), Vec::new(), maint.submitter()));
        ex.spawn(maint_loop(shared.clone(), maint));
        let observer = shared.clone();
        let held = ex.run_until(
            async move {
                let tick = Timer::every(Duration::from_millis(2)).unwrap();
                let started = Instant::now();
                while !gated_seen.load(AtomicOrdering::SeqCst) {
                    assert!(started.elapsed() < Duration::from_secs(10), "the sweep never reached the gated request");
                    tick.tick().await;
                }
                observer.ring().borrow_mut().set_members_weighted(new_ring.members_weighted().collect::<Vec<_>>());
                release_tx.send(()).unwrap();
                let passes = observer.sweep_pass_count();
                let settle = Instant::now();
                while settle.elapsed() < Duration::from_millis(800) || observer.sweep_pass_count() < passes + 2 {
                    assert!(settle.elapsed() < Duration::from_secs(10), "the sweep stalled after the ring change");
                    if observer.sweep_pass_count() < passes + 2 {
                        observer.signal_periodic();
                    }
                    tick.tick().await;
                }
                observer.zone_store(DEFAULT_ZONE).unwrap().borrow().get(&stray).unwrap()
            },
            {
                let shared = shared.clone();
                move || server_poll(&shared)
            },
        );
        shared.begin_shutdown();
        drop(ex);
        join.join();
        assert_eq!(
            held.as_deref(),
            Some(&b"v"[..]),
            "a key the adopted ring gives this node was forgotten on an old-ring handoff ({} handoffs)",
            handoffs.load(AtomicOrdering::SeqCst)
        );
        if !during_handoff {
            assert_eq!(handoffs.load(AtomicOrdering::SeqCst), 0, "cleanup judged the key with the old ring");
        }
    }

    #[test]
    fn idle_cleanup_bursts_through_a_deep_stray_in_fewer_sweep_passes() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let stray = (0..)
            .map(|i| format!("z-stray-{i}").into_bytes())
            .find(|k| ring.replicas(k)[0] == 2)
            .unwrap();
        let owned = 4 * crate::antientropy::CLEANUP_MAX_KEYS_PER_PASS;
        let burst = cleanup_arm(1000, None, owned, &stray);
        let paced = cleanup_arm(0, None, owned, &stray);
        let rate = |arm: &CleanupArm| arm.lap_keys as f64 / arm.elapsed.as_secs_f64();
        eprintln!(
            "cleanup lap of {} keys, stray last: idle burst {} passes {:?} ({:.0} keys/s); one page per pass {} passes {:?} ({:.0} keys/s)",
            burst.lap_keys, burst.passes, burst.elapsed, rate(&burst), paced.passes, paced.elapsed, rate(&paced),
        );
        assert!(paced.passes >= 5, "knockout: one 8,192-key page per pass needs a pass per page ({} passes)", paced.passes);
        assert!(burst.passes < paced.passes, "an idle shard runs more pages per pass ({} vs {})", burst.passes, paced.passes);
    }

    #[test]
    #[ignore = "measurement: cargo test --release -p vals-cluster --lib -- --ignored cleanup_lap_speed_file_backed --nocapture"]
    fn cleanup_lap_speed_file_backed() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let stray = (0..)
            .map(|i| format!("z-stray-{i}").into_bytes())
            .find(|k| ring.replicas(k)[0] == 2)
            .unwrap();
        let depth: usize = std::env::var("VALS_CLEANUP_LAP_KEYS").map_or(1_000_000, |v| v.parse().unwrap());
        for burst_ms in [1000, 0] {
            let dir = std::env::temp_dir().join(format!("vals-cleanup-lap-{}-{burst_ms}", std::process::id()));
            let dev = vals_spdk::FileBlockDevice::create(&dir, 4096, 262_144).unwrap();
            let arm = cleanup_arm_on(dev, burst_ms, None, depth, &stray);
            let _ = std::fs::remove_file(&dir);
            eprintln!(
                "file-backed cleanup lap, stray at depth {}: cleanup_idle_burst_ms={burst_ms}: {} passes, {:?}, {:.0} keys/s",
                arm.lap_keys, arm.passes, arm.elapsed, arm.lap_keys as f64 / arm.elapsed.as_secs_f64(),
            );
        }
    }

    #[test]
    fn cleanup_traversal_from_a_sampled_origin_laps_back_to_strays_before_it() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let stray = (0..)
            .map(|i| format!("a-stray-{i}").into_bytes())
            .find(|k| ring.replicas(k)[0] == 2)
            .unwrap();
        let arm = cleanup_arm(1000, Some(b"k0000100".to_vec()), 1000, &stray);
        assert!(arm.passes >= 1);
    }

    #[test]
    fn logical_accounting_loop_rebuilds_run_backed_store_without_sweep_progress() {
        let mut s = store(1);
        s.apply(
            b"run-backed",
            Some(b"value"),
            Version { hlc: 1, node: 1 },
        )
        .unwrap();
        s.flush().unwrap();
        assert!(s.run_count() > 0);
        s.append_unknown_for_test(
            b"dirty",
            Some(b"value"),
            Version { hlc: 2, node: 1 },
        )
        .unwrap();
        s.invalidate_leaf_seed();
        assert_eq!(s.exact_live_keys(), None);

        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let rebuilt = Rc::new(Cell::new(false));
        let ex = LocalExecutor::new();
        ex.spawn(logical_accounting_loop(shared.clone()));
        let observer = shared.clone();
        let rebuilt_out = rebuilt.clone();
        ex.spawn(async move {
            for _ in 0..10_000 {
                if observer
                    .zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .exact_live_keys()
                    == Some(2)
                {
                    rebuilt_out.set(true);
                    break;
                }
                vals_rt::yield_now().await;
            }
            observer.begin_shutdown();
        });
        shared.kick_logical_accounting();
        ex.run(crate::reactor::poll_io_nonblocking);

        assert!(
            rebuilt.get(),
            "the dedicated accounting task must fold run sidecars without sweep progress"
        );
    }

    #[test]
    fn logical_accounting_loop_resumes_ram_paused_recount_and_reseeds_abandoned_span() {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 1 << 14),
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..600u64 {
            let key = format!("k{i:04}");
            s.apply(key.as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                .unwrap();
            if i % 150 == 149 {
                s.flush().unwrap();
            }
        }
        let base = RangeMap::from_splits([b"k0100".to_vec()]);
        s.set_logical_range_map(10, base.clone());
        while !s.seed_step().unwrap() {}
        assert_eq!(s.logical_range_counts(10), Some(vec![100, 500]));
        let mut split = base.clone();
        assert!(split.split(b"k0400"));
        s.set_logical_range_map(11, split.clone());
        assert!(s.logical_recount_pending());

        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let ex = LocalExecutor::new();
        ex.spawn(logical_accounting_loop(shared.clone()));
        let observer = shared.clone();
        let outcome = Rc::new(Cell::new((false, false, false)));
        let outcome_out = outcome.clone();
        ex.spawn(async move {
            let store = observer.zone_store(DEFAULT_ZONE).unwrap();
            let pressure = crate::ram::override_for_test(2, 1);
            observer.kick_logical_accounting();
            for _ in 0..200 {
                vals_rt::yield_now().await;
            }
            let paused = store.borrow().logical_recount_pending();
            drop(pressure);
            let deadline = Instant::now() + Duration::from_secs(10);
            while store.borrow().logical_range_counts(11).is_none() && Instant::now() < deadline {
                vals_rt::yield_now().await;
            }
            let resumed = store.borrow().logical_range_counts(11) == Some(vec![100, 300, 200]);
            let mut again = split.clone();
            assert!(again.split(b"k0500"));
            store.borrow().set_logical_range_map(12, again);
            assert!(store.borrow().logical_recount_pending());
            store.borrow().understate_split_recount_totals_for_test();
            observer.kick_logical_accounting();
            while store.borrow().logical_range_counts(12).is_none() && Instant::now() < deadline {
                vals_rt::yield_now().await;
            }
            let reseeded = store.borrow().logical_range_counts(12) == Some(vec![100, 300, 100, 100])
                && store.borrow().is_leaf_seeded();
            outcome_out.set((paused, resumed, reseeded));
            observer.begin_shutdown();
        });
        ex.run(crate::reactor::poll_io_nonblocking);

        let (paused, resumed, reseeded) = outcome.get();
        assert!(paused, "RAM pressure pauses the recount");
        assert!(resumed, "the paused recount resumes on its own timer without another kick");
        assert!(reseeded, "an abandoned span reseeds within the same accounting kick");
    }

    #[test]
    fn logical_accounting_loop_folds_range_tombstones_applied_after_the_seed() {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 1 << 14),
            Config {
                seg_blocks: 8,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap().with_max_skew_ms(0);
        for i in 0..600u64 {
            s.apply(format!("k{i:04}").as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                .unwrap();
            if i % 150 == 149 {
                s.flush().unwrap();
            }
        }
        for i in 500..520u64 {
            s.apply(format!("k{i:04}").as_bytes(), Some(b"m"), Version { hlc: 700 + i, node: 1 })
                .unwrap();
        }
        while !s.seed_step().unwrap() {}
        let rt = Version { hlc: 5_000, node: 1 };
        s.apply_range_delete(Some(b"k0100"), Some(b"k0550"), rt, 0).unwrap();
        assert!(s.rt_fold_pending());
        assert_eq!(s.engine().range_tombstone_max_tree_blocks(), 0);

        let ring = Ring::from_members(1, 64, [1]);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, ring));
        let ex = LocalExecutor::new();
        ex.spawn(logical_accounting_loop(shared.clone()));
        let observer = shared.clone();
        let folded = Rc::new(Cell::new(false));
        let folded_out = folded.clone();
        ex.spawn(async move {
            let store = observer.zone_store(DEFAULT_ZONE).unwrap();
            observer.kick_logical_accounting();
            let deadline = Instant::now() + Duration::from_secs(10);
            while store.borrow().rt_fold_pending() && Instant::now() < deadline {
                vals_rt::yield_now().await;
            }
            folded_out.set(!store.borrow().rt_fold_pending());
            observer.begin_shutdown();
        });
        ex.run(crate::reactor::poll_io_nonblocking);

        assert!(folded.get(), "the accounting task folds a range tombstone queued after the seed");
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let s = store.borrow();
        assert!(s.engine().range_tombstone_max_tree_blocks() > 0, "the first fold builds the max trees");
        let covered: Vec<_> = s
            .engine()
            .scan_mutable_entries(Some(b"k0100"), Some(b"k0550"))
            .unwrap()
            .into_iter()
            .filter(|(_, e)| e.version <= crate::replica::pack_version(rt))
            .collect();
        assert!(covered.is_empty(), "the fold removes covered memtable entries: {covered:?}");
        assert_eq!(s.exact_live_keys(), Some(150));
        assert_eq!(s.engine().range_tombstone_accounted_seq(), s.engine().range_tombstone_max_seq());
    }

    #[test]
    fn logical_accounting_loop_prioritizes_scans_without_starving_seed() {
        fn arm(priority: bool) -> (u64, u64, bool) {
            set_seed_scan_priority_override(Some(priority));
            let engine = Engine::create_prewiped(
                MemBlockDevice::new(256, 1 << 17),
                Config {
                    seg_blocks: 16,
                    reserved_blocks: 0,
                    ordering: SOrdering::Sorted,
                },
            );
            let mut s = VersionedStore::new(engine, 1).unwrap();
            for i in 0..8_000u64 {
                let key = format!("k{i:05}");
                s.apply(key.as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                    .unwrap();
                if i % 400 == 399 {
                    s.flush().unwrap();
                }
            }
            assert!(s.run_count() > 0);
            s.invalidate_leaf_seed();
            let shared = Rc::new(ServerShared::single_zone_on(1, s, Ring::from_members(1, 64, [1])));
            let result = Rc::new(Cell::new(None));
            let ex = LocalExecutor::new();
            ex.spawn(logical_accounting_loop(shared.clone()));
            let observer = shared.clone();
            let result_out = result.clone();
            ex.spawn(async move {
                let store = observer.zone_store(DEFAULT_ZONE).unwrap();
                let dev = store.borrow().device_handle();
                let permit = observer.try_scan().unwrap();
                reset_seed_scan_test_counts();
                let before = crate::observe::snapshot();
                observer.kick_logical_accounting();
                for page in 0..60 {
                    let lo = format!("k{:05}", page * 128);
                    let response = crate::net::serve_scan(
                        &store,
                        dev.as_ref(),
                        observer.read_gate(),
                        Some(lo.as_bytes()),
                        None,
                        128,
                        true,
                        Default::default(),
                    )
                    .await;
                    match response {
                        Response::Scan { entries, more, .. } => {
                            assert_eq!(entries.len(), 128);
                            assert!(more);
                        }
                        other => panic!("scan failed: {other:?}"),
                    }
                    vals_rt::yield_now().await;
                }
                let (steps_during_scan, yields_during_scan) = seed_scan_test_counts();
                let after = crate::observe::snapshot();
                assert!(after.ae_seed_scan_yields - before.ae_seed_scan_yields >= yields_during_scan);
                let seeded_during_scan = store.borrow().is_leaf_seeded();
                let deadline = Instant::now() + Duration::from_secs(10);
                while !store.borrow().is_leaf_seeded() && Instant::now() < deadline {
                    vals_rt::yield_now().await;
                }
                let seeded_with_permit = store.borrow().is_leaf_seeded();
                if seeded_with_permit {
                    assert_eq!(store.borrow().exact_live_keys(), Some(8_000));
                }
                drop(permit);
                result_out.set(Some((
                    steps_during_scan,
                    yields_during_scan,
                    seeded_with_permit && !seeded_during_scan,
                )));
                observer.begin_shutdown();
            });
            ex.run(crate::reactor::poll_io_nonblocking);
            set_seed_scan_priority_override(None);
            result.get().unwrap()
        }

        let (priority_steps, yields, priority_seeded) = arm(true);
        let (baseline_steps, baseline_yields, _) = arm(false);
        eprintln!(
            "scan priority: steps={priority_steps}, yields={yields}; disabled: steps={baseline_steps}, yields={baseline_yields}"
        );
        assert!(yields > 0);
        assert_eq!(baseline_yields, 0);
        assert!(
            priority_seeded,
            "the seed must finish with a continuously held scan permit"
        );
        assert!(
            baseline_steps >= priority_steps * 3 && baseline_steps >= priority_steps + 20,
            "scan priority must keep seed steps out of scan drain passes"
        );
    }

    #[derive(Clone, Copy, PartialEq)]
    enum SeedLoad {
        Idle,
        Gate,
        Applies,
    }

    struct DutyArm {
        seed_share: f64,
        foreground_share: f64,
        paused: Duration,
    }

    fn seed_duty_arm(isolation: bool, load: SeedLoad, restarts: u32, ignore_events: bool) -> DutyArm {
        let _policy = crate::replica::seed_policy::policy_for_test(isolation, 25, 64 << 20);
        crate::replica::seed_policy::ignore_foreground_events_for_test(ignore_events);
        set_seed_step_test_spin(Duration::from_millis(2));
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 1 << 16),
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..6_000u64 {
            let key = format!("k{i:05}");
            s.apply(key.as_bytes(), Some(b"v"), Version { hlc: i + 1, node: 1 })
                .unwrap();
            if i % 600 == 599 {
                s.flush().unwrap();
            }
        }
        s.invalidate_leaf_seed();
        s.set_seed_restarts_for_test(restarts);
        let shared = Rc::new(ServerShared::single_zone_on(1, s, Ring::from_members(1, 64, [1])));
        let result = Rc::new(Cell::new(None));
        let ex = LocalExecutor::new();
        ex.spawn(logical_accounting_loop(shared.clone()));
        let observer = shared.clone();
        let result_out = result.clone();
        ex.spawn(async move {
            vals_rt::yield_now().await;
            let store = observer.zone_store(DEFAULT_ZONE).unwrap();
            assert!(store.borrow().seed_driver_attached(), "the accounting task attaches as seed driver");
            let _gate = if load == SeedLoad::Gate {
                Some(observer.read_gate().acquire().await)
            } else {
                None
            };
            if load == SeedLoad::Applies {
                observer.note_foreground_for_test();
            }
            let started = Instant::now();
            observer.kick_logical_accounting();
            let mut work = Duration::ZERO;
            while !store.borrow().is_leaf_seeded() && started.elapsed() < Duration::from_secs(120) {
                if load != SeedLoad::Idle {
                    let unit = Instant::now();
                    while unit.elapsed() < Duration::from_micros(100) {
                        std::hint::spin_loop();
                    }
                    work += unit.elapsed();
                    if load == SeedLoad::Applies {
                        observer.note_foreground_for_test();
                    }
                }
                vals_rt::yield_now().await;
            }
            assert!(store.borrow().is_leaf_seeded(), "seed finished");
            assert_eq!(store.borrow().exact_live_keys(), Some(6_000));
            result_out.set(Some((started.elapsed(), work)));
            observer.begin_shutdown();
        });
        ex.run(crate::reactor::poll_io_nonblocking);
        crate::replica::seed_policy::ignore_foreground_events_for_test(false);
        assert!(
            shared.zone_store(DEFAULT_ZONE).unwrap().borrow().seed_driver_attached(),
            "a driver exiting at shutdown does not re-enable on-demand seeding during the drain"
        );
        let (wall, work) = result.get().unwrap();
        let (busy, paused) = seed_duty_test_totals();
        set_seed_step_test_spin(Duration::ZERO);
        let wall = wall.as_secs_f64();
        DutyArm { seed_share: busy.as_secs_f64() / wall, foreground_share: work.as_secs_f64() / wall, paused }
    }

    #[test]
    fn seed_driver_holds_duty_under_batch_apply_load_without_gate_inflight() {
        let on = seed_duty_arm(true, SeedLoad::Applies, 0, false);
        eprintln!("applies: seed {:.3}, foreground {:.3}, paused {:?}", on.seed_share, on.foreground_share, on.paused);
        assert!(on.seed_share <= 0.32, "seed share {} under sustained applies", on.seed_share);
        assert!(on.foreground_share >= 0.5, "foreground share {}", on.foreground_share);
        assert!(on.paused > Duration::ZERO);

        let knockout = seed_duty_arm(true, SeedLoad::Applies, 0, true);
        eprintln!("knockout (gate-inflight signal only): seed {:.3}, foreground {:.3}", knockout.seed_share, knockout.foreground_share);
        assert_eq!(knockout.paused, Duration::ZERO, "the round-1 signal never saw the applies");
        assert!(knockout.seed_share >= 0.5, "knockout seed share {}", knockout.seed_share);
    }

    #[test]
    fn seed_driver_caps_run_churn_escalation_under_load() {
        let escalated = seed_duty_arm(true, SeedLoad::Applies, 37, false);
        eprintln!("37 restarts: seed {:.3}, foreground {:.3}, paused {:?}", escalated.seed_share, escalated.foreground_share, escalated.paused);
        assert!(escalated.seed_share <= 0.57, "escalated seed share {} exceeds the 50% cap", escalated.seed_share);
        assert!(escalated.paused > Duration::ZERO, "an escalated seed still pauses under load");
        assert!(escalated.foreground_share >= 0.35, "foreground share {}", escalated.foreground_share);
    }

    #[test]
    fn seed_driver_holds_duty_for_gates_and_runs_full_speed_when_idle() {
        let gate = seed_duty_arm(true, SeedLoad::Gate, 0, false);
        eprintln!("gate: seed {:.3}, foreground {:.3}, paused {:?}", gate.seed_share, gate.foreground_share, gate.paused);
        assert!(gate.seed_share <= 0.32, "seed share {} with an in-flight read", gate.seed_share);
        assert!(gate.paused > Duration::ZERO);

        let idle = seed_duty_arm(true, SeedLoad::Idle, 0, false);
        eprintln!("idle: seed {:.3}, paused {:?}", idle.seed_share, idle.paused);
        assert_eq!(idle.paused, Duration::ZERO, "an idle shard seeds at full speed");
        assert!(idle.seed_share >= 0.5, "idle seed share {}", idle.seed_share);

        let off = seed_duty_arm(false, SeedLoad::Applies, 0, false);
        eprintln!("isolation off: seed {:.3}, foreground {:.3}", off.seed_share, off.foreground_share);
        assert_eq!(off.paused, Duration::ZERO);
        assert!(off.foreground_share < 0.25, "isolation off starves foreground work: {}", off.foreground_share);
    }

    #[test]
    fn congruent_shard_addrs_covers_and_minimizes() {
        let mk = |n: usize| -> Vec<SocketAddr> {
            (0..n)
                .map(|i| format!("127.0.0.1:{}", 10000 + i).parse().unwrap())
                .collect()
        };
        let idx = |v: &[SocketAddr]| -> Vec<u16> { v.iter().map(|a| a.port() - 10000).collect() };

        let p = mk(1);
        assert_eq!(idx(&congruent_shard_addrs(&p, 0, 2)), vec![0]);
        assert_eq!(idx(&congruent_shard_addrs(&p, 1, 2)), vec![0]);
        let p = mk(2);
        assert_eq!(idx(&congruent_shard_addrs(&p, 0, 4)), vec![0]);
        assert_eq!(idx(&congruent_shard_addrs(&p, 1, 4)), vec![1]);
        assert_eq!(idx(&congruent_shard_addrs(&p, 2, 4)), vec![0]);
        assert_eq!(idx(&congruent_shard_addrs(&p, 3, 4)), vec![1]);
        let p = mk(2);
        for s in 0..3 {
            assert_eq!(idx(&congruent_shard_addrs(&p, s, 3)), vec![0, 1]);
        }
        let p = mk(2);
        assert_eq!(idx(&congruent_shard_addrs(&p, 0, 2)), vec![0]);
        assert_eq!(idx(&congruent_shard_addrs(&p, 1, 2)), vec![1]);
        let p = mk(2);
        assert_eq!(idx(&congruent_shard_addrs(&p, 0, 1)), vec![0, 1]);
        let p = mk(1);
        assert_eq!(idx(&congruent_shard_addrs(&p, 0, 1)), vec![0]);
        assert!(congruent_shard_addrs(&[], 1, 2).is_empty());

        for k in 1..=6usize {
            for kp in 1..=6usize {
                let p = mk(kp);
                for h in 0u64..500 {
                    let s = (h % k as u64) as usize;
                    let i = (h % kp as u64) as u16;
                    let sub = idx(&congruent_shard_addrs(&p, s, k));
                    assert!(
                        sub.contains(&i),
                        "k={k} kp={kp} h={h}: peer shard {i} missing from shard {s}'s subset {sub:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn multidrive_shard_gt0_refills_from_single_shard_peer_without_missharding() {
        use crate::coord::Coordinator;
        use crate::drive::{MemDriveSet, PartitionGeom};
        use crate::liveness::FileLivenessLog;
        use crate::net::NetTransport;
        use crate::reactor;

        let now = physical_now_ms();
        let recent = now - 1_000;
        let grace = 60_000u64;
        let stale = now.saturating_sub(1_000 + 2 * grace);

        let base = std::env::temp_dir().join(format!(
            "vals-md-refill-0080-{}-{:x}",
            std::process::id(),
            now
        ));
        std::fs::create_dir_all(&base).unwrap();
        let wm_path = |s: usize| base.join(format!("liveness-shard{s}.wm"));

        let p = 2;
        let listeners1 = Node::bind_n(p).unwrap();
        let addrs1: Vec<SocketAddr> = listeners1.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners1.into_iter().map(|(l, _)| l).collect();
        for s in 0..p {
            FileLivenessLog::new(wm_path(s)).store(stale).unwrap();
        }

        let (l2, a2) = Node::bind().unwrap();
        let s2 = store(2);
        let log2 = MemLivenessLog::new();
        log2.store(recent).unwrap();

        let ring = Ring::from_members(2, 64, [1, 2]);
        let interval = Duration::from_millis(25);
        let peer_shards = vec![(1u32, addrs1.clone()), (2u32, vec![a2])];

        let ds = MemDriveSet::uniform(256, 4096, 2);
        let geom = PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        };
        let base_c = base.clone();
        let (_weight, h1) = Node::serve_multidrive_with_liveness(
            1,
            shard_listeners,
            ds,
            512,
            geom,
            ring.clone(),
            peer_shards,
            interval,
            true,
            grace,
            move |s| FileLivenessLog::new(base_c.join(format!("liveness-shard{s}.wm"))),
        )
        .unwrap();
        let _n1 = crate::testutil::running(h1);
        let _n2 = crate::testutil::running(Node::serve_with_liveness(
            2,
            l2,
            s2,
            ring.clone(),
            vec![(1, addrs1[0]), (2, a2)],
            interval,
            grace,
            log2,
        ));

        let shard_of = |k: &str| (crate::ring::hash64(k.as_bytes()) % 2) as usize;
        let mut k0 = None;
        let mut k1 = None;
        let mut i = 0;
        while k0.is_none() || k1.is_none() {
            let k = format!("key-{i}");
            match shard_of(&k) {
                0 if k0.is_none() => k0 = Some(k),
                1 if k1.is_none() => k1 = Some(k),
                _ => {}
            }
            i += 1;
        }
        let (k0, k1) = (k0.unwrap(), k1.unwrap());

        let coord = Coordinator::new(
            0,
            ring,
            2,
            NetTransport::connect_sharded([(1u32, addrs1.clone()), (2u32, vec![a2])]),
        );
        for k in [&k0, &k1] {
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let s0 = read_from(addrs1[0], k0.as_bytes()).as_deref() == Some(&b"v"[..]);
            let s1 = read_from(addrs1[1], k1.as_bytes()).as_deref() == Some(&b"v"[..]);
            if s0 && s1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "shard>0 did not refill + unfence against a single-shard peer \
                 (shard0 served={s0}, shard1 served={s1}); task 0080"
            );
            thread::sleep(Duration::from_millis(20));
        }

        assert_eq!(
            read_from(addrs1[1], k0.as_bytes()),
            None,
            "shard 1 must NOT hold the shard-0 key (no sibling-shard mis-sharding)"
        );
        assert_eq!(
            read_from(addrs1[0], k1.as_bytes()),
            None,
            "shard 0 must NOT hold the shard-1 key"
        );

        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn unsharded_handoff_routes_to_correct_shard_of_more_sharded_peer() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let key = {
            let mut i = 0;
            loop {
                let k = format!("key-{i}");
                let kb = k.clone().into_bytes();
                if ring.replicas(&kb) == vec![2] && crate::ring::hash64(&kb) % 2 == 1 {
                    break kb;
                }
                i += 1;
                assert!(i < 100_000, "no suitable key found");
            }
        };

        let listeners2 = Node::bind_n(2).unwrap();
        let a2: Vec<SocketAddr> = listeners2.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners2.into_iter().map(|(l, _)| l).collect();
        let (a2_0, a2_1) = (a2[0], a2[1]);
        let (l1, a1) = Node::bind().unwrap();

        let full_addrs: Vec<(NodeId, Vec<SocketAddr>)> =
            vec![(1u32, vec![a1]), (2u32, vec![a2_0, a2_1])];
        let interval = Duration::from_millis(25);

        let shard_stores = shard_listeners
            .into_iter()
            .map(|l| (l, store(2)))
            .collect::<Vec<_>>();
        let _n2 = crate::testutil::running(Node::serve_sharded(
            2,
            shard_stores,
            ring.clone(),
            full_addrs.clone(),
            interval,
        ));

        for a in [a2_0, a2_1] {
            set_members_config_with_addrs(a, 1, 64, 1, &[1, 2], &full_addrs);
        }

        let mut s1 = store(1);
        s1.apply(&key, Some(b"v"), Version { hlc: 100, node: 9 })
            .unwrap();
        let _n1 = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            ring.clone(),
            vec![(1, a1), (2, a2_0)],
            interval,
        ));

        set_members_config_with_addrs(a1, 1, 64, 1, &[1, 2], &full_addrs);

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if read_from_at(a2_1, &key, 1).as_deref() == Some(&b"v"[..]) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "handoff did not land the key on node 2's shard 1 \
                 (mis-sharded onto shard 0? task 0081)"
            );
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(
            read_from_at(a2_0, &key, 1),
            None,
            "key must NOT land on node 2's shard 0 (coarse handoff mis-sharding)"
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if read_from_at(a1, &key, 1).is_none() {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 1 did not shed the over-replicated key after handoff"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    struct SharedDs(std::rc::Rc<crate::drive::MemDriveSet>);
    impl crate::drive::DriveSet for SharedDs {
        type Dev = MemBlockDevice;
        fn drive_count(&self) -> usize {
            self.0.drive_count()
        }
        fn drive_blocks(&self, d: usize) -> u64 {
            self.0.drive_blocks(d)
        }
        fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, io::Error> {
            self.0.read_layout_fingerprint()
        }
        fn write_layout_fingerprint(&self, fp: &[u8]) -> Result<(), io::Error> {
            self.0.write_layout_fingerprint(fp)
        }
        fn open_region(
            &self,
            drive: usize,
            base: u64,
            len: u64,
            create: bool,
        ) -> Result<MemBlockDevice, io::Error> {
            self.0.open_region(drive, base, len, create)
        }
    }

    struct PollGatedDs {
        inner: std::rc::Rc<crate::drive::MemDriveSet>,
        states: std::rc::Rc<RefCell<Vec<Arc<PollGatedState>>>>,
    }

    impl crate::drive::DriveSet for PollGatedDs {
        type Dev = PollGatedDevice;
        fn drive_count(&self) -> usize {
            self.inner.drive_count()
        }
        fn drive_blocks(&self, d: usize) -> u64 {
            self.inner.drive_blocks(d)
        }
        fn read_layout_fingerprint(&self) -> Result<Option<Vec<u8>>, io::Error> {
            self.inner.read_layout_fingerprint()
        }
        fn write_layout_fingerprint(&self, fp: &[u8]) -> Result<(), io::Error> {
            self.inner.write_layout_fingerprint(fp)
        }
        fn open_region(
            &self,
            drive: usize,
            base: u64,
            len: u64,
            create: bool,
        ) -> Result<PollGatedDevice, io::Error> {
            let state = Arc::new(PollGatedState::default());
            state.gated.store(true, AtomicOrdering::Release);
            self.states.borrow_mut().push(state.clone());
            Ok(PollGatedDevice {
                inner: self.inner.open_region(drive, base, len, create)?,
                state,
            })
        }
    }

    fn md_geom() -> crate::drive::PartitionGeom {
        crate::drive::PartitionGeom {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: Ordering::Unsorted,
        }
    }

    #[test]
    fn multidrive_zoned_serves_default_zone_across_shards() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::reactor;

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();

        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        Node::provision_multidrive_zoned(
            1,
            &SharedDs(ds.clone()),
            p,
            512,
            8,
            128,
            Vec::new(),
            md_geom(),
        )
        .unwrap();
        let ring = Ring::from_members(1, 64, [1]);
        let peer_shards = vec![(1u32, addrs.clone())];

        let (weight, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            peer_shards,
            Duration::from_secs(3600),
            false,
        )
        .unwrap();
        let dumper = handle.dumper();
        let _node = crate::testutil::running(handle);
        assert_eq!(weight, 2, "ring weight equals the drive count");

        let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));
        for i in 0..40 {
            let k = format!("k{i:02}");
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }
        for i in 0..40 {
            let k = format!("k{i:02}");
            let got = vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io).unwrap();
            assert_eq!(got.and_then(|s| s.value).as_deref(), Some(&b"v"[..]));
        }
        let stats = dumper.all_zone_stats().unwrap();
        assert_eq!(stats.len(), 4);
        assert!(stats.iter().all(|(_, stats)| stats.zone_id == DEFAULT_ZONE));
        assert_eq!(
            stats.iter().map(|(_, stats)| stats.live_keys).sum::<u64>(),
            40
        );
        assert!(stats.iter().all(|(_, stats)| {
            stats.used_segments + stats.free_segments == 32
        }));
    }

    fn raw_shard_scan(addr: SocketAddr, zone: crate::zone::ZoneId) -> Vec<Vec<u8>> {
        match rpc(
            addr,
            &Request::Scan {
                epoch: crate::wire::INSPECT_EPOCH,
                zone_id: zone,
                lo: None,
                hi: None,
                max: 0,
            },
        ) {
            Response::Scan { entries, .. } => entries.into_iter().map(|(k, _)| k).collect(),
            other => panic!("expected scan reply, got {other:?}"),
        }
    }

    fn push_range_map(
        addr: SocketAddr,
        epoch: u64,
        zone_id: crate::zone::ZoneId,
        map: &RangeMap,
    ) {
        let maps = [wire::WireRangeMap {
            zone_id,
            splits: map.splits(),
            tokens: map.tokens(),
            pending: map.pending(),
            placement_debt: map.placement_debt(),
            opaque_tokens: map.supports_opaque_tokens(),
        }];
        let _ = rpc(
            addr,
            &Request::SetRangeMaps {
                auth: &[],
                epoch,
                maps: &maps,
                incarnation: None,
            },
        );
    }

    #[test]
    fn multidrive_zoned_keeps_populated_split_readable_immediately() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::RangeMap;
        use crate::reactor;
        use crate::ring::hash64;
        use crate::zone::{Catalog, ZoneConfig};
        use crate::WriteConcern;

        let p = 2;
        let zid = 2u32;

        let mut catalog = Catalog::with_default(1);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "ranges".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );

        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);

        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_weight, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let node = crate::testutil::running(handle);

        const N: usize = 48;
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:04}").into_bytes()).collect();
        let empty_shard = (hash64(b"") % p as u64) as usize;

        for &a in &addrs {
            set_zones(a, &catalog, 1);
        }
        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();

        coord.set_range_map(zid, RangeMap::single());
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }
        assert_eq!(
            raw_shard_scan(addrs[empty_shard], zid).len(),
            N,
            "single range -> all keys on one shard"
        );
        for k in &keys {
            let got = vals_rt::block_on(coord.read_in(zid, k), reactor::poll_io).unwrap();
            assert_eq!(got.and_then(|s| s.value).as_deref(), Some(&b"v"[..]));
        }
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "pre-split scan complete");
        for (i, (k, _)) in all.iter().enumerate() {
            assert_eq!(k, &keys[i], "pre-split scan ordered");
        }

        let split_key = keys
            .iter()
            .skip(1)
            .find(|k| (hash64(k) % p as u64) as usize != empty_shard)
            .expect("some key's token hashes to a different shard")
            .clone();
        let split_shard = (hash64(&split_key) % p as u64) as usize;
        assert_ne!(empty_shard, split_shard, "split must move to a new shard");
        let mut split_map = RangeMap::single();
        split_map.split(&split_key);
        assert!(!split_map.is_canonical());
        for &a in &addrs {
            push_range_map(a, 1, zid, &split_map);
        }

        coord.set_range_map(zid, split_map.clone());
        for k in &keys {
            let got = vals_rt::block_on(coord.read_in(zid, k), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(&b"v"[..]),
                "immediate post-split read of {k:?} complete"
            );
        }
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "immediate post-split scan complete");
        for (i, (k, _)) in all.iter().enumerate() {
            assert_eq!(k, &keys[i], "immediate post-split scan ordered");
        }

        assert_eq!(
            raw_shard_scan(addrs[empty_shard], zid).len(),
            N,
            "metadata-only split keeps every key on the inherited token's shard"
        );
        assert!(
            raw_shard_scan(addrs[split_shard], zid).is_empty(),
            "metadata-only split does not start a handoff"
        );
        for (s, &a) in addrs.iter().enumerate() {
            for k in raw_shard_scan(a, zid) {
                let want = (hash64(split_map.range_token(&k)) % p as u64) as usize;
                assert_eq!(want, s, "key {k:?} on shard {s} matches its range token");
            }
        }

        let canonical_map = RangeMap::from_splits(vec![split_key.clone()]);
        let moved: Vec<Vec<u8>> = keys.iter().filter(|k| **k >= split_key).cloned().collect();
        let stayed: Vec<Vec<u8>> = keys.iter().filter(|k| **k < split_key).cloned().collect();
        assert!(!moved.is_empty() && !stayed.is_empty(), "test needs a mix");
        for &a in &addrs {
            push_range_map(a, 2, zid, &canonical_map);
        }
        coord.set_range_map(zid, canonical_map);
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let on_empty: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[empty_shard], zid)
                    .into_iter()
                    .collect();
            let on_split: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[split_shard], zid)
                    .into_iter()
                    .collect();
            let spread = stayed.iter().all(|k| on_empty.contains(k))
                && stayed.iter().all(|k| !on_split.contains(k))
                && moved.iter().all(|k| on_split.contains(k))
                && moved.iter().all(|k| !on_empty.contains(k));
            if spread {
                break;
            }
            assert!(Instant::now() < deadline, "canonical presplit did not settle");
            std::thread::sleep(Duration::from_millis(30));
        }

        let merged_map = RangeMap::single();
        for &a in &addrs {
            push_range_map(a, 3, zid, &merged_map);
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let on_empty: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[empty_shard], zid)
                    .into_iter()
                    .collect();
            let on_split: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[split_shard], zid)
                    .into_iter()
                    .collect();
            let union: std::collections::HashSet<&Vec<u8>> =
                on_empty.iter().chain(on_split.iter()).collect();
            assert!(
                keys.iter().all(|k| union.contains(k)),
                "no key may be lost mid-merge"
            );
            let done = keys.iter().all(|k| on_empty.contains(k)) && on_split.is_empty();
            if done {
                break;
            }
            assert!(Instant::now() < deadline, "merge migration did not settle");
            std::thread::sleep(Duration::from_millis(30));
        }
        assert_eq!(
            raw_shard_scan(addrs[empty_shard], zid).len(),
            N,
            "merge folds every key back onto the merged token's shard"
        );

        coord.set_range_map(zid, merged_map.clone());
        for k in &keys {
            let got = vals_rt::block_on(coord.read_in(zid, k), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(&b"v"[..]),
                "post-merge read of {k:?} complete"
            );
        }
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "post-merge scan complete");
        for (i, (k, _)) in all.iter().enumerate() {
            assert_eq!(k, &keys[i], "post-merge scan ordered");
        }

        drop(node);
        let listeners2 = Node::bind_n(p).unwrap();
        let addrs2: Vec<SocketAddr> = listeners2.iter().map(|(_, a)| *a).collect();
        let shard_listeners2: Vec<TcpListener> = listeners2.into_iter().map(|(l, _)| l).collect();
        let (_w2, handle2) = Node::serve_multidrive_zoned(
            1,
            shard_listeners2,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring,
            vec![(1u32, addrs2.clone())],
            Duration::from_secs(3600),
            false,
        )
        .unwrap();
        let _node2 = crate::testutil::running(handle2);
        for &a in &addrs2 {
            set_zones_only(a, &catalog, 1);
            push_range_map(a, 3, zid, &merged_map);
        }
        for k in &keys {
            let shard = (hash64(merged_map.range_token(k)) % p as u64) as usize;
            assert_eq!(
                read_zone(addrs2[shard], zid, k).as_deref(),
                Some(&b"v"[..]),
                "sorted key {k:?} not recovered on boot reopen after merge"
            );
        }
    }

    fn split_moving_off_the_single_range_shard(keys: &[Vec<u8>], p: usize) -> Vec<u8> {
        let single_shard = crate::ring::hash64(b"") % p as u64;
        keys.iter()
            .skip(1)
            .find(|k| crate::ring::hash64(k) % p as u64 != single_shard)
            .expect("some key's token hashes off the single-range shard")
            .clone()
    }

    fn assert_keys_on_token_shards(
        addrs: &[SocketAddr],
        zid: crate::zone::ZoneId,
        map: &RangeMap,
        keys: &[Vec<u8>],
    ) {
        let mut seen = std::collections::BTreeMap::new();
        for (shard, &addr) in addrs.iter().enumerate() {
            for key in raw_shard_scan(addr, zid) {
                let want = (crate::ring::hash64(map.range_token(&key)) % addrs.len() as u64) as usize;
                assert_eq!(want, shard, "stray copy of {key:?} on shard {shard} (owner shard {want})");
                assert!(seen.insert(key.clone(), shard).is_none(), "{key:?} held twice");
            }
        }
        for key in keys {
            assert!(seen.contains_key(key), "{key:?} lost from its owner shard");
        }
    }

    #[test]
    fn sorted_boot_sweep_waits_for_range_maps_and_plants_no_strays() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::reactor;
        use crate::zone::{Catalog, ZoneConfig};
        use crate::WriteConcern;

        let p = 2;
        let zid = 2u32;
        let mut catalog = Catalog::with_default(1);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "boot-race".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let boot = |sweep: Duration, create: bool| {
            let listeners = Node::bind_n(p).unwrap();
            let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
            let shard_listeners: Vec<TcpListener> =
                listeners.into_iter().map(|(l, _)| l).collect();
            let (_w, handle) = Node::serve_multidrive_zoned(
                1,
                shard_listeners,
                SharedDs(ds.clone()),
                512,
                8,
                128,
                md_geom(),
                ring.clone(),
                vec![(1u32, addrs.clone())],
                sweep,
                create,
            )
            .unwrap();
            (addrs, crate::testutil::running(handle))
        };

        let keys: Vec<Vec<u8>> = (0..48).map(|i| format!("k{i:04}").into_bytes()).collect();
        let map = RangeMap::from_splits(vec![split_moving_off_the_single_range_shard(&keys, p)]);
        let (addrs, node) = boot(Duration::from_secs(3600), true);
        for &a in &addrs {
            set_zones(a, &catalog, 1);
            push_range_map(a, 1, zid, &map);
        }
        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, map.clone());
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }
        assert_keys_on_token_shards(&addrs, zid, &map, &keys);
        drop(node);

        let (addrs, node) = boot(Duration::from_millis(20), false);
        let dumper = node.dumper();
        let counts = || (0..p).map(|shard| dumper.sweep_counts(shard)).collect::<Vec<_>>();
        for &a in &addrs {
            set_zones_only(a, &catalog, 1);
        }
        let gap_start = counts();
        thread::sleep(Duration::from_millis(600));
        let gap_end = counts();
        for (shard, (before, after)) in gap_start.iter().zip(&gap_end).enumerate() {
            assert!(after.0 >= before.0 + 2, "shard {shard}: the catalog-to-range-map gap must span sweep passes");
            assert!(after.1 > before.1, "shard {shard}: sweep passes in the gap must hold the sorted zone");
        }
        assert_keys_on_token_shards(&addrs, zid, &map, &keys);
        for &a in &addrs {
            push_range_map(a, 1, zid, &map);
        }
        thread::sleep(Duration::from_millis(300));
        assert_keys_on_token_shards(&addrs, zid, &map, &keys);
    }

    #[test]
    fn sorted_write_fence_holds_until_the_first_range_map_push() {
        use crate::drive::MemDriveSet;
        use crate::zone::{Catalog, ZoneConfig};

        let zid = 2u32;
        let mut catalog = Catalog::with_default(1);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "fenced".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let listeners = Node::bind_n(1).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            listeners.into_iter().map(|(l, _)| l).collect(),
            SharedDs(std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2))),
            512,
            8,
            128,
            md_geom(),
            Ring::from_members(1, 64, [1]),
            vec![(1u32, addrs.clone())],
            Duration::from_secs(3600),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);
        let apply = || {
            rpc(
                addrs[0],
                &Request::Apply {
                    epoch: 0,
                    zone_id: zid,
                    key: b"k",
                    value: Some(b"v"),
                    version: Version { hlc: 1 << 20, node: 9 },
                },
            )
        };
        set_zones_only(addrs[0], &catalog, 1);
        assert!(
            matches!(apply(), Response::StaleEpoch(_)),
            "a sorted write before the first range-map push must be refused retriably"
        );
        let _ = rpc(
            addrs[0],
            &Request::SetRangeMaps {
                auth: &[],
                epoch: 0,
                maps: &[],
                incarnation: None,
            },
        );
        let accepted = apply();
        assert!(
            !matches!(accepted, Response::StaleEpoch(_) | Response::Err(_)),
            "an adopted empty push means single-range placement: {accepted:?}"
        );
    }

    #[test]
    fn restarted_node_without_pd_contact_holds_its_reopened_sorted_zones() {
        use crate::drive::MemDriveSet;
        use crate::zone::{Catalog, ZoneConfig};

        let zid = 2u32;
        let mut catalog = Catalog::with_default(1);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "reopened".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let boot = |create: bool| {
            let listeners = Node::bind_n(1).unwrap();
            let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
            let (_w, handle) = Node::serve_multidrive_zoned(
                1,
                listeners.into_iter().map(|(l, _)| l).collect(),
                SharedDs(ds.clone()),
                512,
                8,
                128,
                md_geom(),
                Ring::from_members(1, 64, [1]),
                vec![(1u32, addrs.clone())],
                Duration::from_secs(3600),
                create,
            )
            .unwrap();
            (addrs[0], crate::testutil::running(handle))
        };
        let apply = |addr: SocketAddr, key: &[u8], hlc: u64| {
            rpc(
                addr,
                &Request::Apply {
                    epoch: 0,
                    zone_id: zid,
                    key,
                    value: Some(b"v"),
                    version: Version { hlc, node: 9 },
                },
            )
        };
        let read = |addr: SocketAddr, key: &[u8]| {
            rpc(addr, &Request::Read { epoch: 0, zone_id: zid, key })
        };
        let range_delete = |addr: SocketAddr| {
            let slices = [(Some(&b"x"[..]), Some(&b"y"[..]))];
            rpc(
                addr,
                &Request::RangeDelete {
                    epoch: 0,
                    zone_id: zid,
                    sync: false,
                    slices: &slices,
                    version: Version { hlc: 3 << 20, node: 9 },
                },
            )
        };

        let (addr, node) = boot(true);
        set_zones(addr, &catalog, 1);
        let first = apply(addr, b"a", 1 << 20);
        assert!(!matches!(first, Response::StaleEpoch(_) | Response::Err(_)), "{first:?}");
        drop(node);

        let (addr, _node) = boot(false);
        let before = crate::observe::counters().range_fence_rejections.get();
        assert!(
            matches!(apply(addr, b"b", 2 << 20), Response::StaleEpoch(_)),
            "a reopened sorted zone must refuse writes before any catalog push"
        );
        assert!(
            matches!(range_delete(addr), Response::StaleEpoch(_)),
            "a reopened sorted zone must refuse range deletes before any catalog push"
        );
        assert!(
            matches!(read(addr, b"a"), Response::StaleEpoch(_)),
            "a reopened sorted zone must refuse fenced reads before any catalog push"
        );
        assert!(crate::observe::counters().range_fence_rejections.get() >= before + 2);
        let _ = rpc(
            addr,
            &Request::SetRangeMaps {
                auth: &[],
                epoch: 0,
                maps: &[],
                incarnation: None,
            },
        );
        assert!(
            matches!(apply(addr, b"b", 2 << 20), Response::StaleEpoch(_)),
            "a range-map push alone does not release a zone the node has no catalog entry for"
        );
        set_zones_only(addr, &catalog, 1);
        let accepted = apply(addr, b"b", 2 << 20);
        assert!(!matches!(accepted, Response::StaleEpoch(_) | Response::Err(_)), "{accepted:?}");
        assert_eq!(read_zone(addr, zid, b"a").as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn planted_wrong_shard_stray_is_handed_off_forgotten_and_ae_converges() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::reactor;
        use crate::zone::{Catalog, ZoneConfig};
        use crate::WriteConcern;

        let p = 2;
        let zid = 2u32;
        let mut catalog = Catalog::with_default(2);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "stray".into(),
                rf: 2,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let ring = Ring::from_members(2, 64, [1, 2]);
        let binds: Vec<Vec<(TcpListener, SocketAddr)>> =
            (0..2).map(|_| Node::bind_n(p).unwrap()).collect();
        let addrs: Vec<Vec<SocketAddr>> = binds
            .iter()
            .map(|b| b.iter().map(|(_, a)| *a).collect())
            .collect();
        let peer_shards: Vec<(NodeId, Vec<SocketAddr>)> =
            vec![(1, addrs[0].clone()), (2, addrs[1].clone())];
        let states = std::rc::Rc::new(RefCell::new(Vec::new()));
        let nodes: Vec<_> = binds
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                let (_w, handle) = Node::serve_multidrive_zoned_with_overrides_and_settings(
                    (i + 1) as NodeId,
                    b.into_iter().map(|(l, _)| l).collect(),
                    PollGatedDs {
                        inner: std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2)),
                        states: states.clone(),
                    },
                    512,
                    8,
                    128,
                    Vec::new(),
                    md_geom(),
                    ring.clone(),
                    peer_shards.clone(),
                    Duration::from_millis(50),
                    true,
                    crate::node_settings::NodeSettings {
                        flush_tail_epochs: 0,
                        ..crate::node_settings::NodeSettings::default()
                    },
                )
                .unwrap();
                crate::testutil::running(handle)
            })
            .collect();
        let all: Vec<SocketAddr> = addrs.iter().flatten().copied().collect();

        let keys: Vec<Vec<u8>> = (0..48).map(|i| format!("k{i:04}").into_bytes()).collect();
        let split = split_moving_off_the_single_range_shard(&keys, p);
        let map = RangeMap::from_splits(vec![split.clone()]);
        for &a in &all {
            set_zones(a, &catalog, 1);
            push_range_map(a, 1, zid, &map);
        }
        let coord = Coordinator::new(
            0,
            ring.clone(),
            2,
            NetTransport::connect_sharded(peer_shards.clone()),
        );
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, map.clone());
        let mut stray = None;
        for k in &keys {
            let version = vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
            if *k == split {
                stray = Some(version);
            }
        }
        let version = stray.unwrap();
        thread::sleep(Duration::from_millis(600));
        let wrong = (crate::ring::hash64(b"") % p as u64) as usize;
        let owner = (crate::ring::hash64(&split) % p as u64) as usize;
        assert_ne!(wrong, owner);

        push_range_map(addrs[0][wrong], 2, zid, &RangeMap::single());
        let planted = rpc(
            addrs[0][wrong],
            &Request::Apply {
                epoch: 0,
                zone_id: zid,
                key: &split,
                value: Some(b"v"),
                version,
            },
        );
        assert!(
            !matches!(planted, Response::StaleEpoch(_) | Response::Err(_)),
            "stray plant: {planted:?}"
        );
        assert!(raw_shard_scan(addrs[0][wrong], zid).contains(&split));
        for &a in &all {
            push_range_map(a, 3, zid, &map);
        }
        let dumper = nodes[0].dumper();
        let quiet_start = dumper.sweep_counts(wrong).0;
        thread::sleep(Duration::from_millis(1000));
        let quiet_end = dumper.sweep_counts(wrong).0;
        assert!(
            quiet_end >= quiet_start + 5,
            "with no request to poll its devices, the stray's shard must keep sweeping while its \
             cleanup reads complete only on device polls ({quiet_start} -> {quiet_end} passes)"
        );

        let tree = |addr: SocketAddr, counterpart: NodeId| {
            rpc(
                addr,
                &Request::SyncTree {
                    zone_id: zid,
                    level: crate::antientropy::DEFAULT_BUCKETS,
                    scope_g: 1,
                    scope: &[0],
                    requester: Some(counterpart),
                },
            )
        };
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let removed = !raw_shard_scan(addrs[0][wrong], zid).contains(&split);
            let converged = (0..p).all(|shard| {
                match (tree(addrs[0][shard], 2), tree(addrs[1][shard], 1)) {
                    (Response::Tree(a), Response::Tree(b)) => a == b,
                    _ => false,
                }
            });
            if removed && converged {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "stray removed={removed} trees converged={converged}"
            );
            thread::sleep(Duration::from_millis(50));
        }
        for node in &addrs {
            assert_keys_on_token_shards(node, zid, &map, &keys);
            assert_eq!(read_zone(node[owner], zid, &split).as_deref(), Some(&b"v"[..]));
        }
        let submitted: usize = states.borrow().iter().map(|s| s.submitted.load(AtomicOrdering::Relaxed)).sum();
        assert!(submitted > 0, "the shards' run reads must complete only on device polls");
    }

    fn push_range_maps(addr: SocketAddr, epoch: u64, maps: &[(crate::zone::ZoneId, &RangeMap)]) {
        let maps: Vec<wire::WireRangeMap> = maps
            .iter()
            .map(|&(zone_id, map)| wire::WireRangeMap {
                zone_id,
                splits: map.splits(),
                tokens: map.tokens(),
                pending: map.pending(),
                placement_debt: map.placement_debt(),
                opaque_tokens: map.supports_opaque_tokens(),
            })
            .collect();
        let _ = rpc(
            addr,
            &Request::SetRangeMaps {
                auth: &[],
                epoch,
                maps: &maps,
                incarnation: None,
            },
        );
    }

    #[test]
    fn former_owner_forgets_a_rehomed_range_while_other_ranges_keep_splitting() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::{opaque_placement_token, OPAQUE_TOKEN_SEED_LEN};
        use crate::reactor;
        use crate::zone::{Catalog, ZoneConfig};
        use crate::WriteConcern;

        let moved_zone = 2u32;
        let busy_zone = 3u32;
        let mut catalog = Catalog::with_default(1);
        for (zid, name) in [(moved_zone, "moved"), (busy_zone, "busy")] {
            catalog.insert(
                zid,
                ZoneConfig { name: name.into(), rf: 1, ordering: Ordering::Sorted, scope: None },
            );
        }
        let ring = Ring::from_members(1, 64, [1, 2]);
        let token_on = |node: NodeId| {
            (0..=u8::MAX)
                .map(|seed| opaque_placement_token([seed; OPAQUE_TOKEN_SEED_LEN]))
                .find(|token| ring.replicas_in_zone(moved_zone, token, 1, None) == vec![node])
                .expect("a token placed on the node")
        };
        let single_on = |node: NodeId| {
            RangeMap::from_opaque_parts(Vec::new(), vec![token_on(node)], vec![None], vec![false]).unwrap()
        };
        let before = single_on(1);
        let after = single_on(2);

        let binds: Vec<Vec<(TcpListener, SocketAddr)>> = (0..2).map(|_| Node::bind_n(1).unwrap()).collect();
        let addrs: Vec<SocketAddr> = binds.iter().map(|b| b[0].1).collect();
        let peer_shards: Vec<(NodeId, Vec<SocketAddr>)> = vec![(1, vec![addrs[0]]), (2, vec![addrs[1]])];
        let _nodes: Vec<_> = binds
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                let (_w, handle) = Node::serve_multidrive_zoned_with_overrides_and_settings(
                    (i + 1) as NodeId,
                    b.into_iter().map(|(l, _)| l).collect(),
                    SharedDs(std::rc::Rc::new(MemDriveSet::uniform(256, 1 << 16, 2))),
                    16384,
                    8,
                    4096,
                    Vec::new(),
                    md_geom(),
                    ring.clone(),
                    peer_shards.clone(),
                    Duration::from_millis(50),
                    true,
                    crate::node_settings::NodeSettings {
                        flush_tail_epochs: 0,
                        cleanup_idle_burst_ms: 0,
                        ..crate::node_settings::NodeSettings::default()
                    },
                )
                .unwrap();
                crate::testutil::running(handle)
            })
            .collect();

        let mut busy = RangeMap::single();
        for &a in &addrs {
            set_zones(a, &catalog, 1);
            push_range_maps(a, 1, &[(moved_zone, &before), (busy_zone, &busy)]);
        }
        let coord = Coordinator::new(0, ring.clone(), 1, NetTransport::connect_sharded(peer_shards.clone()));
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(moved_zone, before.clone());
        let keys: Vec<Vec<u8>> = (0..1500).map(|i| format!("k{i:05}").into_bytes()).collect();
        let write_deadline = Instant::now() + Duration::from_secs(10);
        for k in &keys {
            while let Err(e) =
                vals_rt::block_on(coord.write_in(moved_zone, k, b"v", WriteConcern::All), reactor::poll_io)
            {
                assert!(Instant::now() < write_deadline, "write of {k:?} kept failing: {e:?}");
                thread::sleep(Duration::from_millis(10));
            }
        }
        assert_eq!(raw_shard_scan(addrs[0], moved_zone).len(), keys.len(), "node 1 owns the range before the move");
        assert!(raw_shard_scan(addrs[1], moved_zone).is_empty());

        for &a in &addrs {
            push_range_maps(a, 2, &[(moved_zone, &after), (busy_zone, &busy)]);
        }
        let forget_keys = crate::observe::counters().cleanup_forget_keys.get();
        let stop = Arc::new(AtomicBool::new(false));
        let churn = {
            let stop = stop.clone();
            let addrs = addrs.clone();
            let mut after = after.clone();
            thread::spawn(move || {
                let mut epoch = 2u64;
                while !stop.load(AtomicOrdering::Relaxed) {
                    epoch += 1;
                    assert!(busy.split(format!("b{epoch:08}").as_bytes()));
                    assert!(after.split(format!("k{:05}/{epoch}", epoch * 7 % 1500).as_bytes()));
                    for &a in &addrs {
                        push_range_maps(a, epoch, &[(moved_zone, &after), (busy_zone, &busy)]);
                    }
                    thread::sleep(Duration::from_millis(3));
                }
                epoch
            })
        };

        let deadline = Instant::now() + Duration::from_secs(30);
        let outcome = loop {
            let on_former: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[0], moved_zone).into_iter().collect();
            let on_owner: std::collections::HashSet<Vec<u8>> =
                raw_shard_scan(addrs[1], moved_zone).into_iter().collect();
            assert!(
                keys.iter().all(|k| on_former.contains(k) || on_owner.contains(k)),
                "no key may be lost while the former owner hands the range off"
            );
            if on_former.is_empty() && keys.iter().all(|k| on_owner.contains(k)) {
                break Ok(());
            }
            if Instant::now() >= deadline {
                break Err((on_former.len(), on_owner.len()));
            }
            thread::sleep(Duration::from_millis(50));
        };
        stop.store(true, AtomicOrdering::Relaxed);
        let epochs = churn.join().unwrap();
        if let Err((former, owner)) = outcome {
            panic!(
                "the former owner still holds {former} of {} re-homed keys (new owner has {owner}) \
                 after 30 s of range-map pushes up to epoch {epochs}",
                keys.len()
            );
        }
        assert!(epochs > 20, "the range-map epoch must keep moving during the cleanup ({epochs})");
        assert!(crate::observe::counters().cleanup_forget_keys.get() > forget_keys);
        for k in &keys {
            assert_eq!(read_zone(addrs[1], moved_zone, k).as_deref(), Some(&b"v"[..]));
        }
    }

    #[test]
    fn multidrive_zoned_reports_range_stats() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::RangeMap;
        use crate::reactor;
        use crate::zone::{Catalog, ZoneConfig};
        use crate::WriteConcern;

        let p = 1;
        let zid = 2u32;
        let mut catalog = Catalog::with_default(1);
        catalog.insert(
            zid,
            ZoneConfig {
                name: "ranges".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );

        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);

        for &a in &addrs {
            set_zones(a, &catalog, 1);
        }
        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        let stats_map = RangeMap::from_splits([b"a".to_vec(), b"z".to_vec()]);
        for &addr in &addrs {
            push_range_map(addr, 2, zid, &stats_map);
        }
        coord.set_range_map(zid, stats_map);

        const N: usize = 64;
        for i in 0..N {
            let k = format!("k{i:04}");
            vals_rt::block_on(
                coord.write_in(zid, k.as_bytes(), b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }

        match rpc(addrs[0], &Request::RangeStats) {
            Response::RangeStats { ranges, .. } => {
                let populated = ranges
                    .iter()
                    .find(|r| r.zone_id == zid && r.entries > 0)
                    .expect("populated range reported");
                assert_eq!(
                    ranges.iter().filter(|r| r.zone_id == zid).count(),
                    1,
                    "legacy stats must retain zero-range elision"
                );
                assert!(
                    (N as u64 / 2..=N as u64 * 2).contains(&populated.entries),
                    "estimate {} near {N}",
                    populated.entries
                );
                let mid = populated.midpoint.as_ref().expect("a split midpoint");
                assert!(!mid.is_empty(), "midpoint is a real key");
            }
            other => panic!("expected RangeStats, got {other:?}"),
        }
        match rpc(addrs[0], &Request::RangeStatsV2) {
            Response::RangeStatsV2 { epoch, ranges } => {
                assert_eq!(epoch, 2);
                let sorted: Vec<_> = ranges.iter().filter(|r| r.zone_id == zid).collect();
                assert_eq!(sorted.len(), 3, "V2 must report zero-entry sorted ranges");
                assert!(sorted.iter().any(|r| r.entries == 0));
                let marker = ranges.iter().find(|r| r.zone_id == DEFAULT_ZONE)
                    .expect("V2 default-zone shard marker");
                assert_eq!((marker.shard_index, marker.shard_count), (0, 1));
            }
            other => panic!("expected RangeStatsV2, got {other:?}"),
        }
    }

    #[test]
    fn pd_auto_splits_a_hot_range() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::RangeMap;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::ring::hash64;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let p = 2;
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);

        let pd_dev = MemBlockDevice::new(256, 512);
        let pd_engine = Engine::create_prewiped(
            pd_dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.set_rehome_enabled(false);
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        pd.create_zone(ZoneConfig {
            name: "hot".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("hot") {
                if matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, RangeMap::single());

        const N: usize = 100;
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:04}").into_bytes()).collect();
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }

        let load_stats: Vec<_> = addrs
            .iter()
            .map(|&addr| rpc(addr, &Request::RangeStatsV2))
            .collect();
        assert!(
            load_stats.iter().any(|stats| {
                matches!(stats, Response::RangeStatsV2 { ranges, .. }
                    if ranges.iter().any(|s| s.ops_per_sec > 0))
            }),
            "writes were not reflected in range load stats: {load_stats:?}"
        );
        pd.set_auto_split(0, 100);
        pd.set_auto_load_split(1);

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if pd.range_map(zid).num_ranges() >= 2 {
                break;
            }
            assert!(Instant::now() < deadline, "auto-split did not fire");
            for _ in 0..10 {
                let _ = vals_rt::block_on(coord.read_in(zid, &keys[0]), reactor::poll_io);
            }
            thread::sleep(Duration::from_millis(50));
        }
        pd.set_auto_load_split(0);
        let split_map = pd.range_map(zid);
        assert!(!split_map.splits().is_empty(), "map carries the auto-split");
        assert!(
            split_map.tokens().iter().all(Vec::is_empty),
            "populated auto-split must inherit placement before any later re-home"
        );
        assert!(split_map.pending().iter().all(Option::is_none));
        assert!(split_map.placement_debt().iter().skip(1).all(|debt| *debt));

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let per_shard: Vec<std::collections::HashSet<Vec<u8>>> = addrs
                .iter()
                .map(|&a| raw_shard_scan(a, zid).into_iter().collect())
                .collect();
            let union: std::collections::HashSet<&Vec<u8>> =
                per_shard.iter().flat_map(|s| s.iter()).collect();
            assert!(
                keys.iter().all(|k| union.contains(k)),
                "no key may be lost mid auto-split"
            );
            let placed = keys.iter().all(|k| {
                let s = (hash64(split_map.range_token(k)) % p as u64) as usize;
                per_shard[s].contains(k)
            });
            if placed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "auto-split migration did not settle"
            );
            thread::sleep(Duration::from_millis(30));
        }

        coord.set_range_map(zid, split_map);
        for k in &keys {
            let got = vals_rt::block_on(coord.read_in(zid, k), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(&b"v"[..]),
                "post-auto-split read of {k:?} complete"
            );
        }
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "post-auto-split scan complete");
        for (i, (k, _)) in all.iter().enumerate() {
            assert_eq!(k, &keys[i], "post-auto-split scan ordered");
        }
    }

    #[test]
    fn pd_auto_merges_shrunk_ranges() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::ring::hash64;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let p = 2;
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);

        let pd_dev = MemBlockDevice::new(256, 512);
        let pd_engine = Engine::create_prewiped(
            pd_dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        pd.create_zone(ZoneConfig {
            name: "shrunk".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("shrunk") {
                if matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };

        pd.split_zone(zid, b"k0033".to_vec()).unwrap();
        pd.split_zone(zid, b"k0066".to_vec()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if pd.range_map(zid).num_ranges() >= 3 {
                break;
            }
            assert!(Instant::now() < deadline, "presplit did not commit");
            thread::sleep(Duration::from_millis(20));
        }

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, pd.range_map(zid));

        const N: usize = 30;
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:04}").into_bytes()).collect();
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }

        pd.set_auto_split(0, 100);
        pd.set_auto_merge(1000);

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if pd.range_map(zid).num_ranges() == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "auto-merge did not reclaim ranges");
            thread::sleep(Duration::from_millis(50));
        }
        pd.set_auto_merge(0);
        let merged_map = pd.range_map(zid);
        assert_eq!(merged_map.num_ranges(), 1, "fully merged, not over-merged");

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let per_shard: Vec<std::collections::HashSet<Vec<u8>>> = addrs
                .iter()
                .map(|&a| raw_shard_scan(a, zid).into_iter().collect())
                .collect();
            let union: std::collections::HashSet<&Vec<u8>> =
                per_shard.iter().flat_map(|s| s.iter()).collect();
            assert!(
                keys.iter().all(|k| union.contains(k)),
                "no key may be lost mid auto-merge"
            );
            let placed = keys.iter().all(|k| {
                let s = (hash64(merged_map.range_token(k)) % p as u64) as usize;
                per_shard[s].contains(k)
            });
            if placed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "auto-merge migration did not settle"
            );
            thread::sleep(Duration::from_millis(30));
        }

        coord.set_range_map(zid, merged_map);
        for k in &keys {
            let got = vals_rt::block_on(coord.read_in(zid, k), reactor::poll_io).unwrap();
            assert_eq!(
                got.and_then(|s| s.value).as_deref(),
                Some(&b"v"[..]),
                "post-auto-merge read of {k:?} complete"
            );
        }
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "post-auto-merge scan complete");
        for (i, (k, _)) in all.iter().enumerate() {
            assert_eq!(k, &keys[i], "post-auto-merge scan ordered");
        }
    }

    #[test]
    fn pd_merges_covered_empty_ranges_behind_a_range_delete_and_keeps_uncovered_presplits() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::ring::hash64;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let p = 2;
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);
        let pd_engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 512),
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.set_rehome_enabled(false);
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        let key = |i: usize| format!("k{i:04}").into_bytes();
        let shard_of = |token: &[u8]| (hash64(token) % p as u64) as usize;
        let moved = (100..200)
            .map(key)
            .find(|at| shard_of(at) != shard_of(b""))
            .expect("a presplit boundary whose canonical placement differs from the first range's");
        let moved_at = String::from_utf8(moved.clone()).unwrap()[1..].parse::<usize>().unwrap();
        pd.create_zone_presplit(
            ZoneConfig {
                name: "queue".into(),
                rf: 1,
                ordering: Ordering::Sorted,
                scope: None,
            },
            vec![moved.clone(), key(200), key(300)],
        )
        .unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("queue") {
                if pd.range_map(id).num_ranges() == 4
                    && matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "presplit zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(pd.range_map(zid).token_of(1), moved.as_slice());
        for at in (10..=90).step_by(10) {
            pd.split_zone(zid, key(at)).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(20);
        while pd.range_map(zid).num_ranges() != 13 {
            assert!(Instant::now() < deadline, "inherited splits did not commit");
            thread::sleep(Duration::from_millis(20));
        }

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, pd.range_map(zid));
        for i in 0..250 {
            vals_rt::block_on(coord.write_in(zid, &key(i), b"v", WriteConcern::All), reactor::poll_io)
                .unwrap();
        }
        for (lo, hi) in [(None, Some(key(85))), (Some(key(90)), Some(key(200)))] {
            vals_rt::block_on(
                coord.delete_range_in(zid, lo.as_deref(), hi.as_deref(), WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }
        let live: Vec<usize> = (85..90).chain(200..250).collect();

        pd.set_auto_split(0, 100);
        let want = vec![key(80), key(90), key(200), key(300)];
        let deadline = Instant::now() + Duration::from_secs(30);
        while pd.range_map(zid).splits() != want.as_slice() {
            assert!(
                Instant::now() < deadline,
                "covered merges did not converge: splits {:?}",
                pd.range_map(zid).splits().iter().map(|s| String::from_utf8_lossy(s).into_owned()).collect::<Vec<_>>()
            );
            thread::sleep(Duration::from_millis(50));
        }
        let merged = pd.range_map(zid);
        assert_eq!(merged.token_of(0), b"", "the covered prefix keeps the first range's placement");
        assert_eq!(merged.token_of(2), b"", "the covered presplit range now follows its left neighbour");
        assert_eq!(merged.token_of(4), key(300).as_slice(), "the empty uncovered presplit range stays");

        let merged_lowers: Vec<Vec<u8>> = std::iter::once(Vec::new()).chain(want.iter().cloned()).collect();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let covered = match rpc(addrs[shard_of(b"")], &Request::RangeStatsV3) {
                Response::RangeStatsV3 { accounting_ready: true, ranges, covered_empty, .. }
                    if ranges.iter().filter(|r| r.zone_id == zid).map(|r| r.lower_bound.clone())
                        .collect::<Vec<_>>() == merged_lowers =>
                {
                    covered_empty.iter().map(|&i| ranges[i as usize].lower_bound.clone()).collect()
                }
                _ => Vec::new(),
            };
            if covered.contains(&key(90)) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "cleanup did not hand the moved presplit's range tombstone to its new owner ({moved_at})"
            );
            thread::sleep(Duration::from_millis(50));
        }

        coord.set_range_map(zid, merged);
        for i in 0..300 {
            let got = vals_rt::block_on(coord.read_in(zid, &key(i)), reactor::poll_io).unwrap();
            let want = live.contains(&i).then_some(&b"v"[..]);
            assert_eq!(got.and_then(|s| s.value).as_deref(), want, "read of key {i}");
        }
        let scanned =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        let want: Vec<(Vec<u8>, Vec<u8>)> = live.iter().map(|&i| (key(i), b"v".to_vec())).collect();
        assert_eq!(scanned, want, "scan after covered merges");
    }

    #[test]
    fn range_delete_survives_an_inherited_split_inside_it_and_an_auto_merge_across_its_edges() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(1).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);
        let pd_engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 512),
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.set_rehome_enabled(false);
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        pd.create_zone(ZoneConfig {
            name: "rt-lifecycle".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("rt-lifecycle") {
                if matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };
        let wait_ranges = |n: usize, what: &str| {
            let deadline = Instant::now() + Duration::from_secs(20);
            while pd.range_map(zid).num_ranges() != n {
                assert!(Instant::now() < deadline, "{what} did not commit");
                thread::sleep(Duration::from_millis(20));
            }
        };
        pd.split_zone(zid, b"k0030".to_vec()).unwrap();
        pd.split_zone(zid, b"k0060".to_vec()).unwrap();
        wait_ranges(3, "presplit");

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, pd.range_map(zid));
        let key = |i: usize| format!("k{i:04}").into_bytes();
        for i in 0..90 {
            vals_rt::block_on(coord.write_in(zid, &key(i), b"v", WriteConcern::All), reactor::poll_io)
                .unwrap();
        }
        vals_rt::block_on(
            coord.delete_range_in(zid, Some(&key(20)), Some(&key(45)), WriteConcern::All),
            reactor::poll_io,
        )
        .unwrap();
        let live: Vec<usize> = (0..20).chain(45..90).collect();

        let check = |what: &str, want_counts: &[(&[u8], u64)]| {
            let map = pd.range_map(zid);
            coord.set_range_map(zid, map.clone());
            for i in 0..90 {
                let got = vals_rt::block_on(coord.read_in(zid, &key(i)), reactor::poll_io).unwrap();
                let want = live.contains(&i).then_some(&b"v"[..]);
                assert_eq!(got.and_then(|s| s.value).as_deref(), want, "{what}: read of key {i}");
            }
            let scanned =
                vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
            let want: Vec<(Vec<u8>, Vec<u8>)> = live.iter().map(|&i| (key(i), b"v".to_vec())).collect();
            assert_eq!(scanned, want, "{what}: scan");
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let counts = match rpc(addrs[0], &Request::RangeStatsV3) {
                    Response::RangeStatsV3 { accounting_ready: true, ranges, .. } => Some(ranges),
                    _ => None,
                };
                let matches = counts.as_ref().is_some_and(|ranges| {
                    want_counts.iter().all(|(lower, entries)| {
                        let got = ranges
                            .iter()
                            .find(|r| r.zone_id == zid && r.lower_bound == *lower)
                            .map_or(0, |r| r.entries);
                        got == *entries
                    })
                });
                if matches {
                    break;
                }
                assert!(Instant::now() < deadline, "{what}: range counts {counts:?}, want {want_counts:?}");
                thread::sleep(Duration::from_millis(50));
            }
        };
        check("before splitting", &[(b"", 20), (b"k0030", 15), (b"k0060", 30)]);

        pd.split_zone(zid, key(40)).unwrap();
        wait_ranges(4, "split inside the range tombstone");
        let split = pd.range_map(zid);
        assert_eq!(split.token_of(1), split.token_of(2), "the populated split inherits its parent's placement");
        check("inherited split", &[(b"", 20), (b"k0030", 0), (b"k0040", 15), (b"k0060", 30)]);

        pd.set_auto_split(0, 100);
        pd.set_auto_merge(1000);
        wait_ranges(1, "auto-merge");
        pd.set_auto_merge(0);
        check("auto-merge", &[(b"", 65)]);
    }

    #[test]
    fn pd_merge_does_not_oscillate_against_split() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::RangeMap;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let p = 2;
        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);

        let pd_dev = MemBlockDevice::new(256, 512);
        let pd_engine = Engine::create_prewiped(
            pd_dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        pd.create_zone(ZoneConfig {
            name: "steady".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("steady") {
                if matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, RangeMap::single());

        const N: usize = 100;
        let keys: Vec<Vec<u8>> = (0..N).map(|i| format!("k{i:04}").into_bytes()).collect();
        for k in &keys {
            vals_rt::block_on(
                coord.write_in(zid, k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }

        pd.set_auto_split(70, 100);
        pd.set_auto_merge(1000);

        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if pd.range_map(zid).num_ranges() >= 2 {
                break;
            }
            assert!(Instant::now() < deadline, "auto-split did not fire");
            thread::sleep(Duration::from_millis(50));
        }
        assert_eq!(pd.range_map(zid).num_ranges(), 2, "split to exactly 2 ranges");

        for _ in 0..8 {
            thread::sleep(Duration::from_millis(120));
            assert_eq!(
                pd.range_map(zid).num_ranges(),
                2,
                "must not oscillate split<->merge"
            );
        }

        pd.set_auto_split(0, 100);
        pd.set_auto_merge(0);
        coord.set_range_map(zid, pd.range_map(zid));
        let all =
            vals_rt::block_on(coord.scan_in(zid, None, None, 1, None), reactor::poll_io).unwrap();
        assert_eq!(all.len(), N, "steady-state scan complete");
    }

    #[test]
    fn pd_warns_on_an_oversized_single_range_sorted_zone() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::net::NetTransport;
        use crate::partition::RangeMap;
        use crate::pd::{PdDaemon, RingConfig};
        use crate::reactor;
        use crate::zone::ZoneConfig;
        use crate::WriteConcern;

        let ds = std::rc::Rc::new(MemDriveSet::uniform(256, 4096, 2));
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(1).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();
        let (_w, handle) = Node::serve_multidrive_zoned(
            1,
            shard_listeners,
            SharedDs(ds.clone()),
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(100),
            true,
        )
        .unwrap();
        let _node = crate::testutil::running(handle);

        let pd_dev = MemBlockDevice::new(256, 512);
        let pd_engine = Engine::create_prewiped(
            pd_dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let pd = crate::testutil::running_pd(
            PdDaemon::spawn_single(
                1,
                pd_engine,
                RingConfig { vnodes: 64, rf: 1 },
                vec![(1u32, addrs.clone())],
            )
            .unwrap(),
        );
        pd.set_auto_split(0, 50);
        pd.set_single_range_warn(50);
        pd.add_node_addressed(1, 1, addrs.clone()).unwrap();
        pd.create_zone(ZoneConfig {
            name: "lonely".into(),
            rf: 1,
            ordering: Ordering::Sorted,
            scope: None,
        }).unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let zid = loop {
            if let Some(id) = pd.catalog().by_name("lonely") {
                if matches!(rpc(addrs[0], &Request::Zones), Response::Zones { zones, .. } if zones.iter().any(|z| z.id == id))
                {
                    break id;
                }
            }
            assert!(Instant::now() < deadline, "zone not committed/pushed");
            thread::sleep(Duration::from_millis(20));
        };

        let coord = Coordinator::new(
            0,
            ring.clone(),
            1,
            NetTransport::connect_sharded([(1u32, addrs.clone())]),
        );
        let _ = vals_rt::block_on(coord.refresh_ring(1), reactor::poll_io);
        vals_rt::block_on(coord.refresh_catalog(1), reactor::poll_io).unwrap();
        coord.set_range_map(zid, RangeMap::single());

        const N: usize = 100;
        let before = crate::observe::snapshot().pd_single_range_warnings;
        for i in 0..N {
            let k = format!("k{i:04}").into_bytes();
            vals_rt::block_on(
                coord.write_in(zid, &k, b"v", WriteConcern::All),
                reactor::poll_io,
            )
            .unwrap();
        }

        let mut saw_oversized = false;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if crate::observe::gauges()
                .pd_oversized_single_range_zones
                .get()
                >= 1
            {
                saw_oversized = true;
            }
            let warned = crate::observe::snapshot().pd_single_range_warnings > before;
            if saw_oversized && warned {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "single-range guardrail did not fire (saw_oversized={saw_oversized})"
            );
            thread::sleep(Duration::from_millis(30));
        }
        assert_eq!(pd.range_map(zid).num_ranges(), 1, "warn must not reshard");
    }

    #[test]
    fn multidrive_zoned_with_liveness_unfences_and_persists_watermarks() {
        use crate::coord::Coordinator;
        use crate::drive::MemDriveSet;
        use crate::liveness::FileLivenessLog;
        use crate::net::NetTransport;
        use crate::reactor;

        let now = physical_now_ms();
        let base = std::env::temp_dir().join(format!(
            "vals-mdz-liveness-{}-{:x}",
            std::process::id(),
            now
        ));
        std::fs::create_dir_all(&base).unwrap();

        let p = 4;
        let listeners = Node::bind_n(p).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shard_listeners: Vec<TcpListener> = listeners.into_iter().map(|(l, _)| l).collect();

        let ds = MemDriveSet::uniform(256, 4096, 2);
        let ring = Ring::from_members(1, 64, [1]);
        let grace = 60_000u64;
        let wm_path = |s: usize| base.join(format!("liveness-shard{s}.wm"));

        let base_c = base.clone();
        let (weight, handle) = Node::serve_multidrive_zoned_with_liveness(
            1,
            shard_listeners,
            ds,
            512,
            8,
            128,
            md_geom(),
            ring.clone(),
            vec![(1u32, addrs.clone())],
            Duration::from_millis(25),
            true,
            grace,
            move |s, _provider| FileLivenessLog::new(base_c.join(format!("liveness-shard{s}.wm"))),
        )
        .unwrap();
        assert_eq!(weight, 2);

        let coord = Coordinator::new(0, ring, 1, NetTransport::connect_sharded([(1u32, addrs)]));
        for i in 0..40 {
            let k = format!("k{i:02}");
            vals_rt::block_on(coord.write(k.as_bytes(), b"v"), reactor::poll_io).unwrap();
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let ok = (0..40).all(|i| {
                let k = format!("k{i:02}");
                vals_rt::block_on(coord.read(k.as_bytes()), reactor::poll_io)
                    .ok()
                    .flatten()
                    .and_then(|s| s.value)
                    .as_deref()
                    == Some(&b"v"[..])
            });
            if ok {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "multidrive-zoned liveness node did not unfence + serve in time"
            );
            thread::sleep(Duration::from_millis(20));
        }

        within_secs(10, move || handle.shutdown());
        for s in 0..p {
            let wm = FileLivenessLog::new(wm_path(s))
                .load()
                .unwrap_or_else(|| panic!("shard {s} watermark not persisted"));
            assert!(wm >= now, "shard {s} watermark {wm} < write progress {now}");
        }
        std::fs::remove_dir_all(&base).ok();
    }

    #[test]
    fn joining_node_bootstraps_via_membership_change() {
        let keys: Vec<Vec<u8>> = (0..60).map(|i| format!("k{i:03}").into_bytes()).collect();

        let mut s1 = store(1);
        let mut s2 = store(2);
        for k in &keys {
            let ver = Version { hlc: 100, node: 9 };
            s1.apply(k, Some(b"v"), ver).unwrap();
            s2.apply(k, Some(b"v"), ver).unwrap();
        }
        let s3 = store(3);

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let (l3, a3) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2), (3, a3)];
        let initial = Ring::from_members(2, 64, [1, 2]);
        let interval = Duration::from_secs(3600);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(
            2,
            l2,
            s2,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(3, l3, s3, initial, peers, interval));

        for addr in [a1, a2, a3] {
            assert_eq!(set_members(addr, &[1, 2, 3]), vec![1, 2, 3]);
        }

        let joined = Ring::from_members(2, 64, [1, 2, 3]);
        let owned: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| joined.replicas(k).contains(&3))
            .collect();
        assert!(
            !owned.is_empty() && owned.len() < keys.len(),
            "node 3 should own a proper subset"
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let have = owned
                .iter()
                .all(|k| read_from(a3, k).as_deref() == Some(&b"v"[..]));
            if have {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 3 did not bootstrap in time"
            );
            thread::sleep(Duration::from_millis(20));
        }

        for k in &keys {
            if !joined.replicas(k).contains(&3) {
                assert_eq!(read_from(a3, k).as_deref(), None, "node 3 over-replicated");
            }
        }
    }

    #[test]
    fn membership_change_sheds_over_replicated_keys_from_prior_replica() {
        let keys: Vec<Vec<u8>> = (0..80).map(|i| format!("k{i:03}").into_bytes()).collect();

        let mut s1 = store(1);
        let mut s2 = store(2);
        for k in &keys {
            let ver = Version { hlc: 100, node: 9 };
            s1.apply(k, Some(b"v"), ver).unwrap();
            s2.apply(k, Some(b"v"), ver).unwrap();
        }
        let s3 = store(3);

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let (l3, a3) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2), (3, a3)];
        let initial = Ring::from_members(2, 64, [1, 2]);
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(
            2,
            l2,
            s2,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(3, l3, s3, initial, peers, interval));

        for addr in [a1, a2, a3] {
            assert_eq!(set_members(addr, &[1, 2, 3]), vec![1, 2, 3]);
        }

        let joined = Ring::from_members(2, 64, [1, 2, 3]);
        let shed: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| !joined.replicas(k).contains(&1))
            .collect();
        assert!(!shed.is_empty(), "the join must move some keys off node 1");
        let addr_of = |n: NodeId| match n {
            1 => a1,
            2 => a2,
            _ => a3,
        };

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let settled = keys
                .iter()
                .all(|k| read_from(a1, k).is_some() == joined.replicas(k).contains(&1));
            if settled {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 1 did not shed its over-replicated keys"
            );
            thread::sleep(Duration::from_millis(20));
        }

        for k in &keys {
            for owner in joined.replicas(k) {
                assert_eq!(
                    read_from(addr_of(owner), k).as_deref(),
                    Some(&b"v"[..]),
                    "owner {owner} must still hold {k:?} after cleanup"
                );
            }
        }
    }

    fn set_members_config(addr: SocketAddr, epoch: u64, vnodes: u32, rf: u32, members: &[NodeId]) {
        let weighted: Vec<(NodeId, u32)> = members.iter().map(|&n| (n, 1)).collect();
        let req = Request::SetMembers {
            addrs: &[],
            auth: &[],
            epoch,
            vnodes,
            rf,
            members: &weighted,
            incarnation: None,
        };
        match rpc(addr, &req) {
            Response::Members { .. } => {}
            other => panic!("expected Members, got {other:?}"),
        }
    }

    fn read_from_at(addr: SocketAddr, key: &[u8], epoch: u64) -> Option<Vec<u8>> {
        match rpc(
            addr,
            &Request::Read {
                epoch,
                zone_id: DEFAULT_ZONE,
                key,
            },
        ) {
            Response::Read(Some(stored)) => stored.value,
            _ => None,
        }
    }

    #[test]
    fn cleanup_handoff_is_accepted_under_an_adopted_config_epoch() {
        let keys: Vec<Vec<u8>> = (0..80).map(|i| format!("k{i:03}").into_bytes()).collect();

        let mut s1 = store(1);
        let mut s2 = store(2);
        for k in &keys {
            let ver = Version { hlc: 100, node: 9 };
            s1.apply(k, Some(b"v"), ver).unwrap();
            s2.apply(k, Some(b"v"), ver).unwrap();
        }
        let s3 = store(3);

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let (l3, a3) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2), (3, a3)];
        let initial = Ring::from_members(2, 64, [1, 2]);
        let interval = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve(
            1,
            l1,
            s1,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(
            2,
            l2,
            s2,
            initial.clone(),
            peers.clone(),
            interval,
        ));
        let _node = crate::testutil::running(Node::serve(3, l3, s3, initial, peers, interval));

        for addr in [a1, a2, a3] {
            set_members_config(addr, 7, 64, 2, &[1, 2, 3]);
        }

        let joined = Ring::from_members(2, 64, [1, 2, 3]);
        let shed: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| !joined.replicas(k).contains(&1))
            .collect();
        assert!(!shed.is_empty(), "the join must move some keys off node 1");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let settled = keys
                .iter()
                .all(|k| read_from_at(a1, k, 7).is_some() == joined.replicas(k).contains(&1));
            if settled {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 1 did not shed under an adopted config epoch \
                 (handoff applies likely refused as stale)"
            );
            thread::sleep(Duration::from_millis(20));
        }

        let addr_of = |n: NodeId| match n {
            1 => a1,
            2 => a2,
            _ => a3,
        };
        for k in &keys {
            for owner in joined.replicas(k) {
                assert_eq!(
                    read_from_at(addr_of(owner), k, 7).as_deref(),
                    Some(&b"v"[..]),
                    "owner {owner} must still hold {k:?} after cleanup"
                );
            }
        }
    }

    #[test]
    fn joining_node_eagerly_materializes_and_heals_a_non_default_zone() {
        use crate::zone::{Catalog, ZoneConfig};
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let zid = 1u32;
        let rf = 2usize;
        let geom = ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 512,
        };
        let mut catalog = Catalog::new();
        catalog.insert(
            zid,
            ZoneConfig {
                name: "z1".into(),
                rf,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        let keys: Vec<Vec<u8>> = (0..60).map(|i| format!("k{i:03}").into_bytes()).collect();

        let p1 = MemZoneStore::new(1, geom);
        let p2 = MemZoneStore::new(2, geom);
        let p3 = MemZoneStore::new(3, geom);
        for p in [&p1, &p2] {
            let mut z = p.open(zid, Ordering::Unsorted).unwrap();
            for k in &keys {
                z.apply(k, Some(b"v"), Version { hlc: 100, node: 9 })
                    .unwrap();
            }
            z.sync().unwrap();
        }
        let z0_1 = p1.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let z0_2 = p2.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let z0_3 = p3.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let (l3, a3) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2), (3, a3)];
        let initial = Ring::from_members(rf, 64, [1, 2]);
        let long = Duration::from_secs(3600);
        let _node = crate::testutil::running(Node::serve_zoned(
            1,
            l1,
            z0_1,
            p1,
            catalog.clone(),
            initial.clone(),
            peers.clone(),
            long,
            None,
        ));
        let _node = crate::testutil::running(Node::serve_zoned(
            2,
            l2,
            z0_2,
            p2,
            catalog.clone(),
            initial.clone(),
            peers.clone(),
            long,
            None,
        ));
        let _node = crate::testutil::running(Node::serve_zoned(
            3, l3, z0_3, p3, catalog, initial, peers, long, None,
        ));

        for addr in [a1, a2, a3] {
            assert_eq!(set_members(addr, &[1, 2, 3]), vec![1, 2, 3]);
        }

        let joined = Ring::from_members(rf, 64, [1, 2, 3]);
        let owned: Vec<&Vec<u8>> = keys
            .iter()
            .filter(|k| joined.replicas_in_zone(zid, k, rf, None).contains(&3))
            .collect();
        assert!(
            !owned.is_empty() && owned.len() < keys.len(),
            "node 3 should own a proper subset of zone 1"
        );

        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let have = owned
                .iter()
                .all(|k| read_zone(a3, zid, k).as_deref() == Some(&b"v"[..]));
            if have {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node 3 did not eagerly materialize + heal zone 1"
            );
            thread::sleep(Duration::from_millis(20));
        }
        for k in &keys {
            if !joined.replicas_in_zone(zid, k, rf, None).contains(&3) {
                assert_eq!(
                    read_zone(a3, zid, k).as_deref(),
                    None,
                    "node 3 over-replicated zone 1"
                );
            }
        }
    }

    #[test]
    fn zoned_node_with_liveness_is_born_fenced_and_unfences_after_a_healthy_sweep() {
        use crate::zone::{Catalog, ZoneConfig};
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let zid = 1u32;
        let rf = 2usize;
        let geom = ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 512,
        };
        let mut catalog = Catalog::new();
        catalog.insert(
            zid,
            ZoneConfig {
                name: "z1".into(),
                rf,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        let keys: Vec<Vec<u8>> = (0..20).map(|i| format!("k{i:02}").into_bytes()).collect();

        let p1 = MemZoneStore::new(1, geom);
        let p2 = MemZoneStore::new(2, geom);
        {
            let mut z = p2.open(zid, Ordering::Unsorted).unwrap();
            for k in &keys {
                z.apply(k, Some(b"v"), Version { hlc: 100, node: 9 })
                    .unwrap();
            }
            z.sync().unwrap();
        }
        let z0_1 = p1.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let z0_2 = p2.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let ring = Ring::from_members(rf, 64, [1, 2]);
        let fast = Duration::from_millis(25);
        let _node = crate::testutil::running(Node::serve_zoned_with_liveness(
            1,
            l1,
            z0_1,
            p1,
            catalog.clone(),
            ring.clone(),
            peers.clone(),
            fast,
            60_000,
            MemLivenessLog::new(),
            None,
        ));

        assert!(
            is_fenced_reply(&read_raw(a1, b"k00")),
            "zoned liveness node must start fenced"
        );

        let _node = crate::testutil::running(Node::serve_zoned(
            2, l2, z0_2, p2, catalog, ring, peers, fast, None,
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let healed = keys
                .iter()
                .all(|k| read_zone(a1, zid, k).as_deref() == Some(&b"v"[..]));
            if healed {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "zoned liveness node did not unfence + heal in time"
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    #[test]
    fn a_liveness_wiped_node_refills_range_tombstones_before_unfencing() {
        use crate::replica::{Applied, Stored};
        use crate::zone::{Catalog, ZoneConfig};
        use crate::zonestore::{MemZoneStore, ZoneGeom, ZoneStore};

        let zid = 7u32;
        let geom = ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 1024,
        };
        let mut catalog = Catalog::new();
        catalog.insert(
            zid,
            ZoneConfig {
                name: "rt".into(),
                rf: 2,
                ordering: Ordering::Sorted,
                scope: None,
            },
        );
        let now = physical_now_ms();
        let (old, recent) = (now - 600_000, now - 1_000);
        let key = |i: u32| format!("k{i:02}").into_bytes();
        let rt = ver(recent, 9);
        let p1 = MemZoneStore::new(1, geom);
        let p2 = MemZoneStore::new(2, geom);
        {
            let mut z = p1.open(zid, Ordering::Sorted).unwrap();
            for i in 0..20 {
                z.apply(&key(i), Some(b"stale"), ver(old, 1)).unwrap();
            }
            z.apply(&key(30), Some(b"stale"), ver(old, 1)).unwrap();
            z.sync().unwrap();
            let mut z = p2.open(zid, Ordering::Sorted).unwrap();
            for i in 0..20 {
                z.apply(&key(i), Some(b"v"), ver(recent - 500, 2)).unwrap();
            }
            assert_eq!(
                z.apply_range_delete(Some(&key(5)), Some(&key(15)), rt, now).unwrap(),
                Applied::Won
            );
            z.sync().unwrap();
        }
        let z0_1 = p1.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let z0_2 = p2.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let log1 = MemLivenessLog::new();
        log1.store(old).unwrap();

        let (l1, a1) = Node::bind().unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let peers = vec![(1, a1), (2, a2)];
        let ring = Ring::from_members(2, 64, [1, 2]);
        let fast = Duration::from_millis(25);
        let _node2 = crate::testutil::running(Node::serve_zoned(
            2, l2, z0_2, p2, catalog.clone(), ring.clone(), peers.clone(), fast, None,
        ));
        let _node1 = crate::testutil::running(Node::serve_zoned_with_liveness(
            1, l1, z0_1, p1, catalog, ring, peers, fast, 60_000, log1, None,
        ));
        let fragments = |addr: SocketAddr| match rpc(
            addr,
            &Request::SyncRangeTombstones { zone_id: zid, requester: None, digest: 0, after: None },
        ) {
            Response::RangeTombstones { fragments, .. } => fragments,
            other => panic!("expected range tombstones, got {other:?}"),
        };
        let want = fragments(a2);
        assert_eq!(want.len(), 1);

        let deadline = Instant::now() + Duration::from_secs(10);
        while is_fenced_reply(&read_raw(a1, b"x")) {
            assert!(Instant::now() < deadline, "the wiped node never unfenced");
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(fragments(a1), want, "F was refilled before the node unfenced");
        let read = |i: u32| match rpc(a1, &Request::Read { epoch: 0, zone_id: zid, key: &key(i) }) {
            Response::Read(stored) => stored,
            other => panic!("read failed: {other:?}"),
        };
        for i in [5, 10, 14] {
            assert_eq!(read(i), Some(Stored { version: rt, value: None }), "k{i:02}");
        }
        for i in [0, 4, 15, 19] {
            assert_eq!(read(i).and_then(|s| s.value).as_deref(), Some(&b"v"[..]), "k{i:02}");
        }
        assert_eq!(read(30), None, "the stale node was wiped");
    }

    fn within_secs<T: Send + 'static>(secs: u64, f: impl FnOnce() -> T + Send + 'static) -> T {
        let (tx, rx) = std::sync::mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(f());
        });
        rx.recv_timeout(Duration::from_secs(secs))
            .expect("operation hung past the watchdog")
    }

    fn apply_at(addr: SocketAddr, key: &[u8], value: &[u8], at_ms: u64) {
        let resp = rpc(
            addr,
            &Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key,
                value: Some(value),
                version: ver(at_ms, 1),
            },
        );
        assert!(
            matches!(resp, Response::Apply(_)),
            "apply must land, got {resp:?}"
        );
    }

    #[test]
    fn shutdown_is_bounded_against_a_black_holing_peer() {
        use std::net::TcpListener;

        let lst = TcpListener::bind("127.0.0.1:0").unwrap();
        let hole = lst.local_addr().unwrap();
        let bg = std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((s, _)) = lst.accept() {
                held.push(s);
            }
        });

        let ring = Ring::from_members(2, 64, [1, 2]);
        let (l1, a1) = Node::bind().unwrap();
        let handle = Node::serve(
            1,
            l1,
            store(1),
            ring,
            vec![(1, a1), (2, hole)],
            Duration::from_millis(25),
        );
        thread::sleep(Duration::from_millis(300));
        within_secs(NODE_PEER_RPC_DEADLINE.as_secs() + 5, move || {
            handle.shutdown()
        });
        drop(bg);
    }

    #[test]
    fn shutdown_is_bounded_mid_cleanup_handoff_backlog() {
        use std::net::TcpListener;

        let lst = TcpListener::bind("127.0.0.1:0").unwrap();
        let hole = lst.local_addr().unwrap();
        let bg = std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((s, _)) = lst.accept() {
                held.push(s);
            }
        });

        let dev = MemBlockDevice::new(4096, 16384);
        let mut s1 = VersionedStore::new(
            Engine::create_prewiped(
                dev,
                Config {
                    seg_blocks: 64,
                    reserved_blocks: 0,
                    ordering: SOrdering::Unsorted,
                },
            ),
            1,
        )
        .unwrap();
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut unowned = 0usize;
        for i in 0..400u32 {
            let key = format!("k{i:04}");
            s1.put(key.as_bytes(), b"v").unwrap();
            if !ring.replicas(key.as_bytes()).contains(&1) {
                unowned += 1;
            }
        }
        assert!(
            unowned > 20,
            "the preload must leave a healthy set of unowned keys for the cleanup \
             handoff loop to chew on (got {unowned})"
        );

        let (l1, a1) = Node::bind().unwrap();
        let handle = Node::serve(
            1,
            l1,
            s1,
            ring,
            vec![(1, a1), (2, hole)],
            Duration::from_millis(25),
        );
        thread::sleep(NODE_PEER_RPC_DEADLINE + Duration::from_millis(1500));

        let shutdowns_before = crate::observe::snapshot().graceful_shutdowns;
        within_secs(NODE_PEER_RPC_DEADLINE.as_secs() + 15, move || {
            handle.shutdown()
        });
        assert!(
            crate::observe::snapshot().graceful_shutdowns > shutdowns_before,
            "graceful shutdown must complete and increment graceful_shutdowns"
        );
        drop(bg);
    }

    #[test]
    fn graceful_shutdown_drains_persists_and_joins() {
        use crate::liveness::FileLivenessLog;

        let now = physical_now_ms();
        let dir = std::env::temp_dir();
        let wm_path = dir.join(format!(
            "vals-shutdown-wm-{}-{:x}.wm",
            std::process::id(),
            now
        ));
        std::fs::remove_file(&wm_path).ok();

        let ring = Ring::from_members(1, 64, [1]);
        let (l1, a1) = Node::bind().unwrap();
        let handle = Node::serve_with_liveness(
            1,
            l1,
            store(1),
            ring,
            vec![(1, a1)],
            Duration::from_millis(25),
            60_000,
            FileLivenessLog::new(&wm_path),
        );

        for i in 0..10u32 {
            apply_at(a1, format!("k{i}").as_bytes(), b"v", now);
        }
        let _idle = TcpStream::connect(a1).unwrap();

        let shutdowns_before = crate::observe::snapshot().graceful_shutdowns;
        within_secs(10, move || handle.shutdown());

        assert!(
            TcpStream::connect(a1).is_err(),
            "a shut-down node must not accept connections"
        );
        let wm = FileLivenessLog::new(&wm_path)
            .load()
            .expect("watermark persisted at shutdown");
        assert!(wm >= now, "persisted watermark {wm} < write progress {now}");
        assert!(crate::observe::snapshot().graceful_shutdowns > shutdowns_before);
        std::fs::remove_file(&wm_path).ok();
    }

    #[test]
    fn graceful_shutdown_cancels_stalled_retained_run_read_and_preserves_data() {
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        };
        let state = Arc::new(StalledReadState {
            stall: AtomicBool::new(false),
            started: AtomicBool::new(false),
        });
        let backing = MemBlockDevice::new(256, 4096);
        let retained = backing.share();
        let dev = StalledReadDevice {
            inner: backing,
            state: state.clone(),
        };
        let mut original = VersionedStore::new(Engine::create_prewiped(dev, cfg), 1).unwrap();
        original.put(b"retained", b"value").unwrap();
        original.flush().unwrap();
        original.sync().unwrap();
        drop(original);

        let dev = StalledReadDevice {
            inner: retained.share(),
            state: state.clone(),
        };
        let reopened = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
        let ring = Ring::from_members(1, 64, [1]);
        let (listener, addr) = Node::bind().unwrap();
        let handle = Node::serve(
            1,
            listener,
            reopened,
            ring,
            vec![(1, addr)],
            Duration::from_secs(3600),
        );
        assert_eq!(read_from(addr, b"retained").as_deref(), Some(&b"value"[..]));

        state.stall.store(true, AtomicOrdering::Release);
        let mut blocked = TcpStream::connect(addr).unwrap();
        crate::proto::client_handshake_blocking(&mut blocked).unwrap();
        let payload = wire::encode_request_payload(&Request::Read {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"retained",
        });
        let mut frame = Vec::with_capacity(12 + payload.len());
        frame.extend_from_slice(&((8 + payload.len()) as u32).to_le_bytes());
        frame.extend_from_slice(&1u64.to_le_bytes());
        frame.extend_from_slice(&payload);
        blocked.write_all(&frame).unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while !state.started.load(AtomicOrdering::Acquire) {
            assert!(Instant::now() < deadline, "retained run read did not start");
            thread::sleep(Duration::from_millis(1));
        }
        let shutdowns_before = crate::observe::snapshot().graceful_shutdowns;
        within_secs(6, move || handle.shutdown());
        assert!(crate::observe::snapshot().graceful_shutdowns > shutdowns_before);
        drop(blocked);

        state.stall.store(false, AtomicOrdering::Release);
        let dev = StalledReadDevice {
            inner: retained,
            state,
        };
        let check = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
        assert_eq!(check.get(b"retained").unwrap().as_deref(), Some(&b"value"[..]));
    }

    #[test]
    fn graceful_shutdown_syncs_a_file_backed_engine_durably() {
        use vals_spdk::FileBlockDevice;

        let now = physical_now_ms();
        let path = std::env::temp_dir().join(format!(
            "vals-shutdown-eng-{}-{:x}.dev",
            std::process::id(),
            now
        ));
        let cfg = Config {
            seg_blocks: 4,
            reserved_blocks: 0,
            ordering: SOrdering::Unsorted,
        };
        let dev = FileBlockDevice::create(&path, 256, 4096).unwrap();
        let s1 = VersionedStore::new(Engine::create_prewiped(dev, cfg), 1).unwrap();

        let ring = Ring::from_members(1, 64, [1]);
        let (l1, a1) = Node::bind().unwrap();
        let handle = Node::serve(1, l1, s1, ring, vec![(1, a1)], Duration::from_secs(3600));
        for i in 0..20u32 {
            apply_at(a1, format!("k{i:02}").as_bytes(), b"v", now);
        }
        within_secs(10, move || handle.shutdown());

        let dev = FileBlockDevice::open(&path, 256).unwrap();
        let s = VersionedStore::new(Engine::open(dev, cfg).unwrap(), 1).unwrap();
        for i in 0..20u32 {
            let k = format!("k{i:02}");
            assert_eq!(
                s.get(k.as_bytes()).unwrap().as_deref(),
                Some(&b"v"[..]),
                "{k} lost across a graceful shutdown"
            );
        }
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn sharded_node_shutdown_joins_every_shard() {
        const NSHARD: usize = 2;
        let ring = Ring::from_members(1, 64, [1]);
        let listeners = Node::bind_n(NSHARD).unwrap();
        let addrs: Vec<SocketAddr> = listeners.iter().map(|(_, a)| *a).collect();
        let shards: Vec<_> = listeners
            .into_iter()
            .map(|(l, _)| l)
            .zip((0..NSHARD).map(|_| store(1)))
            .collect();
        let handle = Node::serve_sharded(
            1,
            shards,
            ring,
            vec![(1u32, addrs.clone())],
            Duration::from_millis(25),
        );
        let _idles: Vec<TcpStream> = addrs
            .iter()
            .map(|a| TcpStream::connect(a).unwrap())
            .collect();
        within_secs(10, move || handle.shutdown());
        for a in addrs {
            assert!(
                TcpStream::connect(a).is_err(),
                "shard {a} still accepting after shutdown"
            );
        }
    }

    #[test]
    fn restarted_zoned_node_reopens_persisted_zones_at_boot() {
        use crate::zone::{Catalog, ZoneConfig};
        use crate::zonestore::{FileZoneStore, ZoneGeom, ZoneStore};

        let now = physical_now_ms();
        let dir =
            std::env::temp_dir().join(format!("vals-boot-zones-{}-{:x}", std::process::id(), now));
        std::fs::remove_dir_all(&dir).ok();
        let geom = ZoneGeom {
            block_size: 256,
            seg_blocks: 4,
            reserved_blocks: 0,
            region_blocks: 512,
        };
        let zid = 1u32;
        let mut catalog = Catalog::new();
        catalog.insert(
            zid,
            ZoneConfig {
                name: "z1".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: None,
            },
        );
        let mut catalog_moved = Catalog::new();
        catalog_moved.insert(
            zid,
            ZoneConfig {
                name: "z1".into(),
                rf: 1,
                ordering: Ordering::Unsorted,
                scope: Some([2].into_iter().collect()),
            },
        );
        let ring = Ring::from_members(1, 64, [1]);

        let p = FileZoneStore::new(1, &dir, geom).unwrap();
        let z0 = p.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let (l1, a1) = Node::bind().unwrap();
        let handle = Node::serve_zoned(
            1,
            l1,
            z0,
            p,
            catalog.clone(),
            ring.clone(),
            vec![(1, a1)],
            Duration::from_secs(3600),
            None,
        );
        for i in 0..10u32 {
            let key = format!("k{i}");
            let resp = rpc(
                a1,
                &Request::Apply {
                    epoch: 0,
                    zone_id: zid,
                    key: key.as_bytes(),
                    value: Some(b"v"),
                    version: ver(now, 1),
                },
            );
            assert!(matches!(resp, Response::Apply(_)), "apply failed: {resp:?}");
        }
        within_secs(10, move || handle.shutdown());

        let p = FileZoneStore::new(1, &dir, geom).unwrap();
        let z0 = p.open(DEFAULT_ZONE, Ordering::Unsorted).unwrap();
        let (l2, a2) = Node::bind().unwrap();
        let handle = Node::serve_zoned(
            1,
            l2,
            z0,
            p,
            catalog_moved,
            ring,
            vec![(1, a2)],
            Duration::from_secs(3600),
            None,
        );
        let wm = match rpc(a2, &Request::GetWatermark) {
            Response::Watermark(ms) => ms,
            other => panic!("expected Watermark, got {other:?}"),
        };
        assert!(
            wm >= now,
            "boot watermark {wm} must include the re-opened zone's progress ({now})"
        );
        assert_eq!(read_zone(a2, zid, b"k0").as_deref(), Some(&b"v"[..]));
        within_secs(10, move || handle.shutdown());
        std::fs::remove_dir_all(&dir).ok();
    }
