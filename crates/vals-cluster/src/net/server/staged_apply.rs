    use super::*;
    use crate::hlc::Version;
    use crate::replica::Applied;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use vals_spdk::{AsyncRead, BlockDevice, BlockError, MemBlockDevice};
    use vals_store::{Config, Engine, Ordering as SOrdering};

    const BUCKETS: u32 = crate::antientropy::DEFAULT_BUCKETS;

    fn v(hlc: u64, node: u32) -> Version {
        Version { hlc, node: node.into() }
    }

    struct AsyncReadState {
        hold: AtomicBool,
        panic_sync: AtomicBool,
        started: AtomicUsize,
        reads: AtomicUsize,
        waker: Mutex<Option<Waker>>,
    }

    impl AsyncReadState {
        fn release(&self) {
            self.hold.store(false, Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    struct AsyncOnlyReadDevice {
        inner: MemBlockDevice,
        state: Arc<AsyncReadState>,
    }

    impl BlockDevice for AsyncOnlyReadDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }

        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }

        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            assert!(
                !self.state.panic_sync.load(Ordering::Acquire),
                "cold apply used synchronous BlockDevice::read_at"
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
            Ok(AsyncOnlyReadDevice {
                inner: self.inner.share(),
                state: self.state.clone(),
            })
        }
    }

    impl AsyncRead for AsyncOnlyReadDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.state.reads.fetch_add(1, Ordering::Relaxed);
            std::future::poll_fn(|cx| {
                if self.state.hold.load(Ordering::Acquire) {
                    self.state.started.fetch_add(1, Ordering::Relaxed);
                    *self.state.waker.lock().unwrap() = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            self.inner.read_at(lba, buf)
        }
    }

    fn build_async_only_store(
        hold: bool,
    ) -> (VersionedStore<AsyncOnlyReadDevice>, Arc<AsyncReadState>) {
        let state = Arc::new(AsyncReadState {
            hold: AtomicBool::new(hold),
            panic_sync: AtomicBool::new(false),
            started: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            waker: Mutex::new(None),
        });
        let dev = AsyncOnlyReadDevice {
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
        store.apply(b"cold", Some(b"old"), v(1, 1)).unwrap();
        store.apply(b"batch-cold", Some(b"old"), v(1, 1)).unwrap();
        store.flush().unwrap();
        assert!(store.run_count() >= 1);
        state.panic_sync.store(true, Ordering::Release);
        (store, state)
    }

    #[test]
    fn existence_and_server_digest_probes_have_distinct_callers() {
        let (store, _) = build_async_only_store(false);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let before = vals_store::attribution::lookup_snapshot();
        let probe = store.borrow().probe_version(b"cold");
        let version = vals_rt::block_on(VersionedStore::version_from_probe(probe, &*dev), || {}).unwrap();
        assert_eq!(version, Some(v(1, 1)));
        let existence = vals_store::attribution::lookup_snapshot();
        assert_eq!(existence[2][0] - before[2][0], 1);
        assert_eq!(existence[2][1] - before[2][1], 1);
        assert_eq!(existence[2][2] - before[2][2], 1);
        assert_eq!(existence[1], before[1]);
        let response = vals_rt::block_on(serve_digest_inner(&store, &*dev, b"cold"), || {});
        assert_eq!(response, Response::Digest(Some(v(1, 1))));
        let digest = vals_store::attribution::lookup_snapshot();
        assert_eq!(digest[1][0] - existence[1][0], 1);
        assert_eq!(digest[1][1] - existence[1][1], 1);
        assert_eq!(digest[1][2] - existence[1][2], 1);
        assert_eq!(digest[2], existence[2]);
    }

    #[test]
    fn unrelated_mutable_write_does_not_invalidate_point_probe_or_digest() {
        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let fired = std::cell::Cell::new(false);
        let plan = vals_rt::block_on(build_get_plan(&store, &*dev, b"cold"), || {
            if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                store
                    .borrow_mut()
                    .engine_mut()
                    .put(b"unrelated", b"raw", 100)
                    .unwrap();
                state.release();
            }
        })
        .unwrap();
        assert!(matches!(plan, GetPlan::Found(..)));

        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let fired = std::cell::Cell::new(false);
        let response = vals_rt::block_on(serve_digest(&store, &*dev, &gate, b"cold"), || {
            if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                store
                    .borrow_mut()
                    .engine_mut()
                    .put(b"unrelated", b"raw", 100)
                    .unwrap();
                state.release();
            }
        });
        assert_eq!(response, Response::Digest(Some(v(1, 1))));
    }

    #[test]
    fn winner_directory_point_plan_reads_one_value_and_digest_reads_none() {
        let (mut store, state) = build_async_only_store(false);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let before = state.reads.load(Ordering::Relaxed);
        let plan = vals_rt::block_on(build_get_plan(&store, &*dev, b"cold"), || {}).unwrap();
        let GetPlan::Found(plan, _, _, Some(token), _) = plan else {
            panic!("winner directory must return a direct checked value plan");
        };
        assert_eq!(state.reads.load(Ordering::Relaxed), before);
        let got = std::cell::RefCell::new(None);
        vals_rt::block_on(
            plan.read_value_checked(&*dev, b"cold", token.entry.version, |version, value| {
                *got.borrow_mut() = Some((version, value.to_vec()));
            }),
            || {},
        )
        .unwrap();
        assert_eq!(state.reads.load(Ordering::Relaxed), before + 1);
        assert_eq!(got.into_inner().unwrap().0, token.entry.version);

        let gate = ReadGate::new();
        let response = vals_rt::block_on(serve_digest(&store, &*dev, &gate, b"cold"), || {});
        assert_eq!(response, Response::Digest(Some(v(1, 1))));
        assert_eq!(state.reads.load(Ordering::Relaxed), before + 1);
    }

    #[test]
    fn mixed_batch_probe_sampling_is_shard_scoped_and_one_in_1024() {
        let (store, _) = build_async_only_store(false);
        let shared = ServerShared::single_zone(store);
        assert!(shared.sample_winner_probes());
        for _ in 1..1024 {
            assert!(!shared.sample_winner_probes());
        }
        assert!(shared.sample_winner_probes());
    }

    #[test]
    fn mixed_batch_read_uses_one_value_read_and_zero_digest_reads() {
        let (mut store, state) = build_async_only_store(false);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let reads = vec![
            (b"cold".to_vec(), MixedReadRole::Value),
            (b"batch-cold".to_vec(), MixedReadRole::Digest),
            (b"missing".to_vec(), MixedReadRole::Digest),
        ];
        let before = state.reads.load(Ordering::Relaxed);
        let response = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, Default::default()),
            || {},
        );
        assert_eq!(state.reads.load(Ordering::Relaxed), before + 1);
        assert_eq!(
            response,
            Response::MixedBatchRead {
                start: 0,
                results: vec![
                    MixedReadResult::Reply(MixedReadReply::Value(Some(crate::replica::Stored {
                        version: v(1, 1),
                        value: Some(b"old".to_vec()),
                    }))),
                    MixedReadResult::Reply(MixedReadReply::Digest(Some(v(1, 1)))),
                    MixedReadResult::Reply(MixedReadReply::Digest(None)),
                ],
                more: false,
            }
        );
    }

    #[test]
    fn mixed_batch_storage_generation_change_returns_busy_after_read() {
        let (mut store, state) = build_async_only_store(true);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let reads = vec![(b"cold".to_vec(), MixedReadRole::Value)];
        let fired = std::cell::Cell::new(false);
        let response = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, Default::default()),
            || {
                if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                    state.panic_sync.store(false, Ordering::Release);
                    let mut store = store.borrow_mut();
                    store.clear().unwrap();
                    store.apply(b"cold", Some(b"replacement"), v(3, 1)).unwrap();
                    state.panic_sync.store(true, Ordering::Release);
                    state.release();
                }
            },
        );
        assert_eq!(
            response,
            Response::MixedBatchRead {
                start: 0,
                results: vec![MixedReadResult::Busy],
                more: false,
            }
        );
    }

    #[test]
    fn mixed_batch_flushed_value_probes_overlap_without_a_directory() {
        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let reads = vec![
            (b"cold".to_vec(), MixedReadRole::Value),
            (b"batch-cold".to_vec(), MixedReadRole::Value),
        ];
        let response = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, Default::default()),
            || {
                if state.started.load(Ordering::Acquire) >= 2 {
                    state.release();
                }
            },
        );
        assert!(state.started.load(Ordering::Acquire) >= 2);
        let Response::MixedBatchRead { results, more, .. } = response else {
            panic!("expected mixed batch response");
        };
        assert!(!more);
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|result| matches!(
            result,
            MixedReadResult::Reply(MixedReadReply::Value(Some(_)))
        )));
    }

    #[test]
    fn mixed_batch_directory_value_read_rejects_corrupt_record_bytes() {
        let (mut store, state) = build_async_only_store(false);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let stage = vals_rt::block_on(stage_mixed_value(&store, &*dev, b"cold"), || {});
        let MixedValueStage::Found(ref plan, ..) = stage else {
            panic!("expected staged directory value");
        };
        let (lba, blocks) = plan.read_range();
        dev.write_at(lba, &vec![0u8; blocks * dev.block_size()])
            .unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let result = vals_rt::block_on(
            execute_mixed_value(&store, &*dev, b"cold", stage, Default::default()),
            || {},
        );
        assert!(matches!(result, MixedReadResult::Err(_)));
    }

    #[test]
    fn mixed_batch_large_flushed_values_page_and_resume_under_budget() {
        let dev = MemBlockDevice::new(4096, 16384);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4096,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        let value = vec![7u8; 3 * 1024 * 1024];
        for (i, key) in [b"large-a".as_slice(), b"large-b", b"large-c"]
            .into_iter()
            .enumerate()
        {
            store.apply(key, Some(&value), v(i as u64 + 1, 1)).unwrap();
        }
        store.flush().unwrap();
        store.engine_mut().rebuild_winner_directory().unwrap();
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let reads = vec![
            (b"large-a".to_vec(), MixedReadRole::Value),
            (b"large-b".to_vec(), MixedReadRole::Value),
            (b"large-c".to_vec(), MixedReadRole::Value),
        ];
        let first = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, Default::default()),
            || {},
        );
        let Response::MixedBatchRead {
            ref results, more, ..
        } = first else {
            panic!("expected first mixed page");
        };
        assert!(more);
        assert_eq!(results.len(), 2);
        assert!(wire::encode_response_payload(&first).len() <= SCAN_REPLY_BUDGET);

        let second = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 2, &reads, false, Default::default()),
            || {},
        );
        let Response::MixedBatchRead {
            start,
            ref results,
            more,
        } = second else {
            panic!("expected resumed mixed page");
        };
        assert_eq!(start, 2);
        assert_eq!(results.len(), 1);
        assert!(!more);
        assert!(wire::encode_response_payload(&second).len() <= SCAN_REPLY_BUDGET);
        assert!(wire::encode_response_payload(&second).len() + 8 <= MAX_FRAME);
    }

    #[test]
    fn mixed_batch_paging_stages_only_bounded_lookahead() {
        let dev = MemBlockDevice::new(4096, 32768);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 8192,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        let value = vec![3u8; 3 * 1024 * 1024];
        let reads: Vec<_> = (0..12)
            .map(|i| (format!("linear-{i:03}").into_bytes(), MixedReadRole::Value))
            .collect();
        for (i, (key, _)) in reads.iter().enumerate() {
            store.apply(key, Some(&value), v(i as u64 + 1, 1)).unwrap();
        }
        store.flush().unwrap();
        store.engine_mut().rebuild_winner_directory().unwrap();
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        MIXED_VALUE_STAGES.with(|count| count.set(0));
        let first = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, Default::default()),
            || {},
        );
        let Response::MixedBatchRead { results, more, .. } = first else {
            panic!("expected first page");
        };
        assert!(more);
        assert_eq!(results.len(), 2);
        let staged = MIXED_VALUE_STAGES.with(Cell::get) as usize;
        assert!(staged <= MIXED_STAGE_LOOKAHEAD);
        assert!(staged < reads.len());
    }

    #[test]
    fn mixed_batch_single_value_above_page_budget_succeeds_within_max_frame() {
        let dev = MemBlockDevice::new(4096, 16384);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4096,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store
            .apply(b"huge", Some(&vec![9u8; 9 * 1024 * 1024]), v(1, 1))
            .unwrap();
        store.flush().unwrap();
        store.engine_mut().rebuild_winner_directory().unwrap();
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let response = vals_rt::block_on(
            serve_mixed_batch_read(
                &store,
                &*dev,
                &gate,
                0,
                &[(b"huge".to_vec(), MixedReadRole::Value)],
                false,
                Default::default(),
            ),
            || {},
        );
        let Response::MixedBatchRead { results, more, .. } = &response else {
            panic!("expected successful mixed response");
        };
        assert!(!more);
        let [MixedReadResult::Reply(MixedReadReply::Value(Some(stored)))] =
            results.as_slice()
        else {
            panic!("expected the 9 MiB value, got {results:?}");
        };
        assert_eq!(stored.value.as_ref().unwrap().len(), 9 * 1024 * 1024);
        let encoded = wire::encode_response_payload(&response).len();
        assert!(encoded > SCAN_REPLY_BUDGET);
        assert!(encoded + 8 <= MAX_FRAME);
    }

    #[test]
    fn winner_directory_same_key_change_retries_a_staged_direct_value_read() {
        let (mut store, state) = build_async_only_store(true);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let out = ServerOut::default();
        let reads_before = state.reads.load(Ordering::Relaxed);
        let value_reads_before = crate::observe::counters().point_value_device_reads.get();
        let fired = std::cell::Cell::new(false);
        vals_rt::block_on(
            serve_read_into(&store, &*dev, &gate, b"cold", &out, 77, Default::default()),
            || {
                if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                    store
                        .borrow_mut()
                        .apply(b"cold", Some(b"new"), v(2, 1))
                        .unwrap();
                    state.release();
                }
            },
        );

        assert_eq!(state.reads.load(Ordering::Relaxed), reads_before + 2);
        assert!(
            crate::observe::counters().point_value_device_reads.get()
                >= value_reads_before + 2
        );
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 77);
        assert_eq!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Read(Some(crate::replica::Stored {
                version: v(2, 1),
                value: Some(b"new".to_vec()),
            }))
        );
    }

    #[test]
    fn winner_directory_storage_generation_change_retries_a_staged_direct_value_read() {
        let (mut store, state) = build_async_only_store(true);
        state.panic_sync.store(false, Ordering::Release);
        store.engine_mut().rebuild_winner_directory().unwrap();
        state.panic_sync.store(true, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let out = ServerOut::default();
        let reads_before = state.reads.load(Ordering::Relaxed);
        let fired = std::cell::Cell::new(false);
        vals_rt::block_on(
            serve_read_into(&store, &*dev, &gate, b"cold", &out, 78, Default::default()),
            || {
                if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                    state.panic_sync.store(false, Ordering::Release);
                    let mut store = store.borrow_mut();
                    store.clear().unwrap();
                    store.apply(b"cold", Some(b"replacement"), v(3, 1)).unwrap();
                    state.panic_sync.store(true, Ordering::Release);
                    state.release();
                }
            },
        );

        assert_eq!(state.reads.load(Ordering::Relaxed), reads_before + 2);
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 78);
        assert_eq!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Read(Some(crate::replica::Stored {
                version: v(3, 1),
                value: Some(b"replacement".to_vec()),
            }))
        );
    }

    #[test]
    fn stalled_bucket_digest_requests_are_bounded_charged_and_released() {
        if crate::testutil::isolated_env_test(&[]) { return; }
        let _ledger = crate::ram_broker::override_for_test(None, 0);
        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let ids: Vec<u32> = (0..BUCKETS).collect();
        let held = store.borrow()
            .plan_bucket_digest_page(BUCKETS, &ids, None, SCAN_REPLY_BUDGET).unwrap()
            .resident_bound_bytes(SCAN_REPLY_BUDGET);
        crate::transport::configure(crate::transport::TransportLimits {
            max_queued_bytes: 4 * (held as usize - 1),
            ..Default::default()
        }).unwrap();
        assert!(held > (crate::transport::limits().max_queued_bytes / 4) as u64);
        let transport_bytes = crate::transport::limits().max_queued_bytes;
        let credit = crate::transport::acquire_queued_bytes(transport_bytes).unwrap();
        let rejections = crate::observe::counters().ae_digest_serve_rejections.get();
        let mut cx = Context::from_waker(Waker::noop());
        let mut pending = Box::pin(serve_bucket_digests(&store, &*dev, &gate, BUCKETS, &ids, None, true));
        assert!(pending.as_mut().poll(&mut cx).is_pending());
        assert_eq!(crate::ram_broker::charged_for_test(), held);
        assert_eq!(crate::transport::usage().2, transport_bytes);
        assert_eq!(crate::observe::gauges().ae_digest_serve_bytes.get(), held);
        let reads = state.reads.load(Ordering::Relaxed);
        for _ in 0..128 {
            let mut excess = Box::pin(serve_bucket_digests(&store, &*dev, &gate, BUCKETS, &ids, None, true));
            assert!(matches!(excess.as_mut().poll(&mut cx), Poll::Ready(Response::Busy)));
        }
        assert_eq!(state.reads.load(Ordering::Relaxed), reads);
        assert_eq!(crate::ram_broker::charged_for_test(), held);
        assert_eq!(crate::observe::counters().ae_digest_serve_rejections.get(), rejections + 128);
        assert_eq!(crate::observe::counters().transport_queued_byte_rejections.get(), 0);
        assert_eq!(crate::transport::usage().2, transport_bytes);
        drop(pending);
        assert_eq!(crate::ram_broker::charged_for_test(), 0);
        assert_eq!(crate::observe::gauges().ae_digest_serve_bytes.get(), 0);
        assert_eq!(crate::transport::usage().2, transport_bytes);
        assert_eq!(gate.inflight(), 0);
        state.release();
        let response = vals_rt::block_on(
            serve_bucket_digests(&store, &*dev, &gate, BUCKETS, &ids, None, true), || {});
        match response {
            Response::BucketDigests { digests, more, .. } => {
                assert!(!more);
                assert_eq!(digests, vec![(b"batch-cold".to_vec(), v(1, 1)), (b"cold".to_vec(), v(1, 1))]);
            }
            other => panic!("expected bucket digests, got {other:?}"),
        }
        assert_eq!(crate::ram_broker::charged_for_test(), 0);
        assert_eq!(crate::observe::gauges().ae_digest_serve_bytes.get(), 0);
        assert_eq!(crate::transport::usage().2, transport_bytes);
        drop(credit);
        assert_eq!(crate::transport::usage().2, 0);
        assert_eq!(gate.inflight(), 0);
    }

    #[test]
    fn mutable_change_restarts_bucket_digest_and_sync_values_pages() {
        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let ids: Vec<u32> = (0..BUCKETS).collect();
        let fired = std::cell::Cell::new(false);
        let response = vals_rt::block_on(
            serve_bucket_digests(&store, &*dev, &gate, BUCKETS, &ids, None, true),
            || {
                if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                    state.panic_sync.store(false, Ordering::Release);
                    store
                        .borrow_mut()
                        .apply(b"zzzz", Some(b"new"), v(20, 1))
                        .unwrap();
                    state.release();
                }
            },
        );
        match response {
            Response::BucketDigests { digests, .. } => {
                assert!(digests.iter().any(|(key, version)| {
                    key == b"zzzz" && *version == v(20, 1)
                }));
            }
            other => panic!("expected bucket digests, got {other:?}"),
        }

        let (store, state) = build_async_only_store(true);
        state.panic_sync.store(false, Ordering::Release);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let fired = std::cell::Cell::new(false);
        let response = vals_rt::block_on(
            serve_sync_values(&store, &*dev, &gate, &[b"cold".to_vec()], Default::default()),
            || {
                if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                    state.panic_sync.store(false, Ordering::Release);
                    store
                        .borrow_mut()
                        .apply(b"cold", Some(b"new"), v(20, 1))
                        .unwrap();
                    state.release();
                }
            },
        );
        match response {
            Response::Values { entries, .. } => {
                assert_eq!(entries.len(), 1);
                assert_eq!(entries[0].0, b"cold");
                assert_eq!(entries[0].1.version, v(20, 1));
                assert_eq!(entries[0].1.value.as_deref(), Some(&b"new"[..]));
            }
            other => panic!("expected values, got {other:?}"),
        }
    }

    #[test]
    fn same_key_or_storage_change_invalidates_a_point_probe() {
        let (store, state) = build_async_only_store(true);
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let fired = std::cell::Cell::new(false);
        let plan = vals_rt::block_on(build_get_plan(&store, &*dev, b"cold"), || {
            if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                store
                    .borrow_mut()
                    .engine_mut()
                    .put(b"cold", b"replacement", u128::MAX)
                    .unwrap();
                state.release();
            }
        })
        .unwrap();
        assert!(matches!(plan, GetPlan::Changed));

        let (mut store, state) = build_async_only_store(true);
        store.engine_mut().put(b"flush-key", b"raw", 100).unwrap();
        let store = RefCell::new(store);
        let dev = store.borrow().device_handle();
        let fired = std::cell::Cell::new(false);
        let plan = vals_rt::block_on(build_get_plan(&store, &*dev, b"cold"), || {
            if state.started.load(Ordering::Acquire) != 0 && !fired.replace(true) {
                state.panic_sync.store(false, Ordering::Release);
                store.borrow_mut().flush().unwrap();
                state.release();
            }
        })
        .unwrap();
        assert!(matches!(plan, GetPlan::Changed));
    }

    struct FailingFlushDevice {
        inner: MemBlockDevice,
        fail: Arc<AtomicBool>,
    }

    impl BlockDevice for FailingFlushDevice {
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
            if self.fail.load(Ordering::Relaxed) {
                Err(BlockError::Io(std::io::Error::other(
                    "injected group-commit failure",
                )))
            } else {
                self.inner.flush()
            }
        }

        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(FailingFlushDevice {
                inner: self.inner.share(),
                fail: self.fail.clone(),
            })
        }
    }

    impl AsyncRead for FailingFlushDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.read_at(lba, buf)
        }
    }

    #[derive(Default)]
    struct FlushOracle {
        seq: AtomicU64,
        flushes: Mutex<Vec<(u64, u64, bool)>>,
    }

    impl FlushOracle {
        fn tick(&self) -> u64 {
            self.seq.fetch_add(1, Ordering::SeqCst) + 1
        }

        fn covered(&self, last_write: u64, acked: u64) -> bool {
            self.flushes
                .lock()
                .unwrap()
                .iter()
                .any(|&(submitted, completed, ok)| ok && submitted > last_write && completed < acked)
        }
    }

    type PendingFlushes = Arc<Mutex<Vec<(Arc<vals_spdk::WriteState>, u32, u64)>>>;

    struct AsyncFlushDevice {
        inner: MemBlockDevice,
        pending: PendingFlushes,
        submitted: Arc<AtomicUsize>,
        fail: Arc<AtomicBool>,
        polls_to_complete: u32,
        domain: Option<usize>,
        oracle: Arc<FlushOracle>,
        last_write: Arc<AtomicU64>,
        hold: Arc<AtomicBool>,
        holding: Arc<AtomicBool>,
        blocking: Arc<AtomicUsize>,
        allow_blocking: bool,
        no_async: bool,
    }

    impl AsyncFlushDevice {
        fn new(polls_to_complete: u32) -> Self {
            AsyncFlushDevice {
                inner: MemBlockDevice::new(256, 8192),
                pending: Arc::new(Mutex::new(Vec::new())),
                submitted: Arc::new(AtomicUsize::new(0)),
                fail: Arc::new(AtomicBool::new(false)),
                polls_to_complete,
                domain: None,
                oracle: Arc::default(),
                last_write: Arc::new(AtomicU64::new(0)),
                hold: Arc::new(AtomicBool::new(false)),
                holding: Arc::new(AtomicBool::new(false)),
                blocking: Arc::new(AtomicUsize::new(0)),
                allow_blocking: false,
                no_async: false,
            }
        }

        fn in_domain(polls_to_complete: u32, key: usize) -> Self {
            let mut dev = AsyncFlushDevice::new(polls_to_complete);
            dev.domain = Some(key);
            dev.allow_blocking = true;
            dev
        }
    }

    impl BlockDevice for AsyncFlushDevice {
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
            if self.hold.load(Ordering::SeqCst) {
                self.holding.store(true, Ordering::SeqCst);
                while self.hold.load(Ordering::SeqCst) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            }
            self.inner.write_at(lba, buf)?;
            self.last_write.store(self.oracle.tick(), Ordering::SeqCst);
            Ok(())
        }

        fn flush(&self) -> vals_spdk::Result<()> {
            assert!(
                self.allow_blocking,
                "async-flush device must not take the blocking flush path"
            );
            self.blocking.fetch_add(1, Ordering::Relaxed);
            let submitted = self.oracle.tick();
            self.inner.flush()?;
            let completed = self.oracle.tick();
            self.oracle
                .flushes
                .lock()
                .unwrap()
                .push((submitted, completed, true));
            Ok(())
        }

        fn submit_flush(&self) -> Option<vals_spdk::WriteHandle> {
            if self.no_async {
                return None;
            }
            self.submitted.fetch_add(1, Ordering::Relaxed);
            let state = vals_spdk::WriteState::timed_pending();
            self.pending
                .lock()
                .unwrap()
                .push((state.clone(), self.polls_to_complete, self.oracle.tick()));
            Some(vals_spdk::WriteHandle::pending(state))
        }

        fn flush_domain(&self) -> Option<usize> {
            self.domain
        }

        fn poll_writes(&self) -> i32 {
            let fail = self.fail.load(Ordering::Relaxed);
            let mut pending = self.pending.lock().unwrap();
            let mut done = 0;
            pending.retain_mut(|(state, left, submitted)| {
                if *left == 0 {
                    let completed = self.oracle.tick();
                    self.oracle
                        .flushes
                        .lock()
                        .unwrap()
                        .push((*submitted, completed, !fail));
                    state.complete(!fail);
                    done += 1;
                    false
                } else {
                    *left -= 1;
                    true
                }
            });
            done
        }

        fn poll_device(&self) -> i32 {
            self.poll_writes()
        }

        fn sibling(&self) -> vals_spdk::Result<Self> {
            Ok(AsyncFlushDevice {
                inner: self.inner.share(),
                pending: self.pending.clone(),
                submitted: self.submitted.clone(),
                fail: self.fail.clone(),
                polls_to_complete: self.polls_to_complete,
                domain: self.domain,
                oracle: self.oracle.clone(),
                last_write: self.last_write.clone(),
                hold: self.hold.clone(),
                holding: self.holding.clone(),
                blocking: self.blocking.clone(),
                allow_blocking: self.allow_blocking,
                no_async: self.no_async,
            })
        }
    }

    impl AsyncRead for AsyncFlushDevice {
        async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.read_at(lba, buf)
        }
    }

    fn domain_shared(
        dev: AsyncFlushDevice,
    ) -> ServerShared<AsyncFlushDevice> {
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        ServerShared::single_zone(VersionedStore::new(engine, 1).unwrap())
    }

    fn async_flush_shared(
        polls: u32,
    ) -> (
        ServerShared<AsyncFlushDevice>,
        Arc<AtomicUsize>,
        Arc<AtomicBool>,
        PendingFlushes,
    ) {
        let dev = AsyncFlushDevice::new(polls);
        let (submitted, fail, pending) = (dev.submitted.clone(), dev.fail.clone(), dev.pending.clone());
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let store = VersionedStore::new(engine, 1).unwrap();
        (ServerShared::single_zone(store), submitted, fail, pending)
    }

    struct FailingGcLog;

    struct FlakyGcLog {
        attempts: Rc<std::cell::Cell<u32>>,
    }

    impl crate::liveness::LivenessLog for FlakyGcLog {
        fn load(&self) -> Option<u64> {
            None
        }

        fn store(&self, _watermark_ms: u64) -> std::io::Result<()> {
            let attempt = self.attempts.get() + 1;
            self.attempts.set(attempt);
            if attempt <= 2 {
                Err(std::io::Error::other("injected watermark failure"))
            } else {
                Ok(())
            }
        }
    }

    impl crate::liveness::LivenessLog for FailingGcLog {
        fn load(&self) -> Option<u64> {
            None
        }

        fn store(&self, _watermark_ms: u64) -> std::io::Result<()> {
            Err(std::io::Error::other("injected watermark failure"))
        }
    }

    #[test]
    fn gc_watermark_persistence_failure_does_not_advance_collection() {
        let mut store = VersionedStore::new(
            Engine::create_prewiped(
                MemBlockDevice::new(256, 8192),
                Config {
                    seg_blocks: 16,
                    reserved_blocks: 0,
                    ordering: SOrdering::Sorted,
                },
            ),
            1,
        )
        .unwrap();
        let old = Version {
            hlc: 1_000 << crate::hlc::COUNTER_BITS,
            node: 1,
        };
        let now = Version {
            hlc: 10_000 << crate::hlc::COUNTER_BITS,
            node: 1,
        };
        store.apply_at(b"old", None, old, 10_000).unwrap();
        store
            .apply_at(b"anchor", Some(b"v"), now, 10_000)
            .unwrap();
        let shared = ServerShared::single_zone_on(
            1,
            store,
            Ring::from_members(1, 64, [1]),
        )
        .with_liveness(1_000, Box::new(FailingGcLog));

        assert!(!shared.persist_gc_watermark(10_000));
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let stats = store.borrow_mut().compact(10_000).unwrap();
        assert_eq!(stats.gc_dropped, 0);
        assert!(store.borrow().stored(b"old").unwrap().is_some());
    }

    #[test]
    fn failed_contact_watermark_is_not_later_treated_as_durable() {
        let mut store = VersionedStore::new(
            Engine::create_prewiped(
                MemBlockDevice::new(256, 8192),
                Config {
                    seg_blocks: 16,
                    reserved_blocks: 0,
                    ordering: SOrdering::Sorted,
                },
            ),
            1,
        )
        .unwrap();
        let old = Version {
            hlc: 1_000 << crate::hlc::COUNTER_BITS,
            node: 1,
        };
        let now = Version {
            hlc: 10_000 << crate::hlc::COUNTER_BITS,
            node: 1,
        };
        store.apply_at(b"old", None, old, 10_000).unwrap();
        store
            .apply_at(b"anchor", Some(b"v"), now, 10_000)
            .unwrap();
        let attempts = Rc::new(std::cell::Cell::new(0));
        let shared = ServerShared::single_zone_on(
            1,
            store,
            Ring::from_members(1, 64, [1]),
        )
        .with_liveness(
            1_000,
            Box::new(FlakyGcLog {
                attempts: attempts.clone(),
            }),
        );

        shared.record_contact_fenced(10_000);
        assert_eq!(attempts.get(), 1);
        assert!(!shared.persist_gc_watermark(10_000));
        assert_eq!(attempts.get(), 2);
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        assert_eq!(store.borrow_mut().compact(10_000).unwrap().gc_dropped, 0);
        assert!(store.borrow().stored(b"old").unwrap().is_some());

        assert!(shared.persist_gc_watermark(10_000));
        assert_eq!(attempts.get(), 3);
        assert_eq!(store.borrow_mut().compact(10_000).unwrap().gc_dropped, 1);
        assert!(store.borrow().stored(b"old").unwrap().is_none());
    }

    fn build_store() -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 8192);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for k in [b"a" as &[u8], b"b", b"c", b"d"] {
            s.apply(k, Some(b"v1"), v(1, 1)).unwrap();
        }
        s.flush().unwrap();
        assert!(s.run_count() >= 1, "store must have a run");
        s.apply(b"c", Some(b"v9"), v(9, 1)).unwrap();
        s.apply(b"b", None, v(8, 1)).unwrap();
        s
    }

    #[test]
    fn single_apply_metric_semantics_count_only_won_values_and_tombstones() {
        assert_eq!(single_apply_record_bytes(Applied::Won, 12), Some(12));
        assert_eq!(single_apply_record_bytes(Applied::Won, 0), Some(0));
        assert_eq!(single_apply_record_bytes(Applied::Lost(v(1, 1)), 12), None);
        assert_eq!(single_apply_record_bytes(Applied::Rejected, 12), None);
    }

    #[test]
    fn served_single_apply_increments_record_and_byte_metrics_in_both_modes() {
        let before = crate::observe::snapshot();
        let value = b"metric-value".to_vec();
        for mode in [ApplyMode::Legacy, ApplyMode::Blind] {
            let shared = ServerShared::single_zone(build_store());
            assert!(matches!(
                serve_request_mode(
                    &shared,
                    OwnedRequest::Apply {
                        epoch: 0,
                        zone_id: DEFAULT_ZONE,
                        key: format!("metric-{mode:?}").into_bytes(),
                        value: Some(value.clone()),
                        version: v(100, 1),
                    },
                    mode,
                ),
                Response::Apply(Applied::Won)
            ));
        }
        let after = crate::observe::snapshot();
        assert!(after.records_written - before.records_written >= 2);
        assert!(
            after.record_bytes_written - before.record_bytes_written >= (value.len() * 2) as u64
        );
    }

    #[test]
    fn accepted_data_ops_are_counted_by_current_sorted_range() {
        let shared = ServerShared::single_zone_on(
            1,
            build_store(),
            Ring::from_members(1, 64, [1]),
        );
        shared.catalog.borrow_mut().insert(
            DEFAULT_ZONE,
            ZoneConfig {
                name: "sorted".into(),
                rf: 1,
                ordering: crate::zone::Ordering::Sorted,
                scope: None,
            },
        );
        shared.range_maps.borrow_mut().insert(
            DEFAULT_ZONE,
            RangeMap::from_splits([b"m".to_vec()]),
        );

        assert!(matches!(
            serve_request_mode(
                &shared,
                OwnedRequest::Read {
                    epoch: 0,
                    zone_id: DEFAULT_ZONE,
                    key: b"a".to_vec(),
                },
                ApplyMode::Legacy,
            ),
            Response::Read(_)
        ));
        assert!(matches!(
            serve_request_mode(
                &shared,
                OwnedRequest::Scan {
                    epoch: 0,
                    zone_id: DEFAULT_ZONE,
                    lo: None,
                    hi: Some(b"m".to_vec()),
                    max: 0,
                },
                ApplyMode::Legacy,
            ),
            Response::Scan { .. }
        ));
        assert!(matches!(
            serve_request_mode(
                &shared,
                OwnedRequest::Apply {
                    epoch: 0,
                    zone_id: DEFAULT_ZONE,
                    key: b"z".to_vec(),
                    value: Some(b"v".to_vec()),
                    version: v(100, 1),
                },
                ApplyMode::Legacy,
            ),
            Response::Apply(_)
        ));
        assert!(matches!(
            serve_request_mode(
                &shared,
                OwnedRequest::BatchApply {
                    epoch: 0,
                    zone_id: DEFAULT_ZONE,
                    sync: false,
                    writes: vec![
                        (b"b".to_vec(), Some(b"v".to_vec()), v(101, 1)),
                        (b"z".to_vec(), Some(b"v".to_vec()), v(102, 1)),
                    ],
                },
                ApplyMode::Legacy,
            ),
            Response::Batch(_)
        ));

        assert_eq!(
            shared.range_load.borrow().counts.get(&DEFAULT_ZONE),
            Some(&vec![3, 2]),
        );
        let sampled = shared.sample_range_load();
        assert!(sampled[&DEFAULT_ZONE].iter().sum::<u64>() > 0);
        assert!(shared.range_load.borrow().counts.is_empty());
    }

    fn request_block(reqid: u64, req: &crate::wire::Request<'_>) -> Vec<u8> {
        let payload = wire::encode_request_payload(req);
        let mut block = reqid.to_le_bytes().to_vec();
        block.extend_from_slice(&payload);
        block
    }

    #[test]
    fn serve_request_mode_routes_cold_stale_apply_without_environment_state() {
        for (mode, expected) in [
            (ApplyMode::Legacy, Applied::Lost(v(1, 1))),
            (ApplyMode::Blind, Applied::Won),
        ] {
            let shared = ServerShared::single_zone(build_store());
            let request = OwnedRequest::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"a".to_vec(),
                value: Some(b"stale".to_vec()),
                version: v(0, 1),
            };
            assert_eq!(
                serve_request_mode(&shared, request, mode),
                Response::Apply(expected)
            );
            assert_eq!(
                shared
                    .zone_store(DEFAULT_ZONE)
                    .unwrap()
                    .borrow()
                    .get(b"a")
                    .unwrap()
                    .as_deref(),
                Some(b"v1" as &[u8])
            );
        }
    }

    #[test]
    fn conn_driver_cold_apply_uses_async_read_and_does_not_block_hot_apply() {
        use std::net::{TcpListener, TcpStream};
        use std::os::fd::AsRawFd;

        let (store, state) = build_async_only_store(true);
        let shared = Rc::new(ServerShared::single_zone(store));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let fd = server.as_raw_fd();
        let executor = vals_rt::LocalExecutor::new();
        let out = Rc::new(ServerOut::default());
        let driver = ConnDriver {
            stream: Rc::new(crate::net::Sock::Tcp(server)),
            fd,
            reader: FrameRead::default(),
            out: out.clone(),
            shared,
            spawner: executor.spawner(),
            negotiated: Some(crate::proto::test_negotiated()),
            apply_mode_override: None,
        };

        let cold = request_block(
            1,
            &crate::wire::Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"cold",
                value: Some(b"new"),
                version: v(2, 1),
            },
        );
        assert!(driver.dispatch_frame(&cold));
        executor.run_until(vals_rt::yield_now(), || {});
        assert!(state.started.load(Ordering::Relaxed) > 0);
        assert!(out.buf.borrow().is_empty(), "cold reply must still be pending");

        let hot = request_block(
            2,
            &crate::wire::Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"unrelated",
                value: Some(b"hot"),
                version: v(3, 1),
            },
        );
        assert!(driver.dispatch_frame(&hot));
        {
            let buf = out.buf.borrow();
            assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 2);
            assert!(matches!(
                wire::decode_response(&buf[12..]).unwrap(),
                Response::Apply(Applied::Won)
            ));
        }

        state.release();
        executor.run(|| {});
        assert_eq!(state.reads.load(Ordering::Relaxed), 1);
        assert_eq!(out.reads_inflight.get(), 0);
        drop(client);
    }

    #[test]
    fn conn_driver_blind_cold_apply_returns_won_without_a_run_read() {
        use std::net::{TcpListener, TcpStream};
        use std::os::fd::AsRawFd;

        let (store, state) = build_async_only_store(true);
        let shared = Rc::new(ServerShared::single_zone(store));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let fd = server.as_raw_fd();
        let executor = vals_rt::LocalExecutor::new();
        let out = Rc::new(ServerOut::default());
        let mut driver = ConnDriver {
            stream: Rc::new(crate::net::Sock::Tcp(server)),
            fd,
            reader: FrameRead::default(),
            out: out.clone(),
            shared,
            spawner: executor.spawner(),
            negotiated: Some(crate::proto::test_negotiated()),
            apply_mode_override: Some(ApplyMode::Blind),
        };
        let cold = request_block(
            1,
            &crate::wire::Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"cold",
                value: Some(b"stale"),
                version: v(0, 1),
            },
        );
        assert!(driver.dispatch_frame(&cold));
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 1);
        assert_eq!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Apply(Applied::Won)
        );
        assert_eq!(state.started.load(Ordering::Relaxed), 0);
        assert_eq!(state.reads.load(Ordering::Relaxed), 0);
        assert_eq!(out.reads_inflight.get(), 0);
        drop(buf);
        out.buf.borrow_mut().clear();

        driver.degrade_apply_mode_for_test();
        let second = request_block(
            2,
            &crate::wire::Request::Apply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                key: b"cold",
                value: Some(b"stale-again"),
                version: v(0, 2),
            },
        );
        assert!(driver.dispatch_frame(&second));
        executor.run_until(vals_rt::yield_now(), || {});
        assert!(state.started.load(Ordering::Relaxed) > 0);
        state.release();
        executor.run(|| {});
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 2);
        assert!(matches!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Apply(Applied::Lost(_))
        ));
        drop(buf);
        out.buf.borrow_mut().clear();

        let batch = request_block(
            3,
            &crate::wire::Request::BatchApply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                sync: false,
                writes: &[(b"cold", Some(b"batch-stale"), v(0, 3))],
            },
        );
        assert!(driver.dispatch_frame(&batch));
        executor.run(|| {});
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 3);
        assert!(matches!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Batch(ref results) if matches!(results.as_slice(), [Applied::Lost(_)])
        ));
        assert!(state.reads.load(Ordering::Relaxed) >= 2);
        drop(buf);
        drop(client);
    }

    #[test]
    fn queued_batch_rechecks_degradation_after_dispatch_before_apply() {
        if crate::testutil::isolated_env_test(&[]) { return; }

        use std::net::{TcpListener, TcpStream};
        use std::os::fd::AsRawFd;
        crate::replica::configure_blind_append(true).unwrap();
        vals_store::configure_winner_directory(true).unwrap();
        let (store, state) = build_async_only_store(false);
        let shared = Rc::new(ServerShared::single_zone(store));
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        server.set_nonblocking(true).unwrap();
        let executor = vals_rt::LocalExecutor::new();
        let out = Rc::new(ServerOut::default());
        let driver = ConnDriver {
            fd: server.as_raw_fd(),
            stream: Rc::new(crate::net::Sock::Tcp(server)),
            reader: FrameRead::default(),
            out: out.clone(),
            shared,
            spawner: executor.spawner(),
            negotiated: Some(crate::proto::test_negotiated()),
            apply_mode_override: None,
        };
        let batch = request_block(
            7,
            &crate::wire::Request::BatchApply {
                epoch: 0,
                zone_id: DEFAULT_ZONE,
                sync: false,
                writes: &[(b"batch-cold", Some(b"stale"), v(0, 9))],
            },
        );
        assert!(driver.dispatch_frame(&batch));
        assert_eq!(state.started.load(Ordering::Relaxed), 0, "batch is queued");
        vals_store::degrade_winner_directory();
        executor.run(|| {});
        let buf = out.buf.borrow();
        assert_eq!(u64::from_le_bytes(buf[4..12].try_into().unwrap()), 7);
        assert!(matches!(
            wire::decode_response(&buf[12..]).unwrap(),
            Response::Batch(ref results) if matches!(results.as_slice(), [Applied::Lost(_)])
        ));
        drop(client);
    }

    #[test]
    fn routed_single_apply_legacy_and_blind_cover_cold_puts_and_tombstones() {
        let cases: &[(Option<&[u8]>, Version, Applied)] = &[
            (Some(b"stale-put"), v(0, 1), Applied::Lost(v(1, 1))),
            (Some(b"new-put"), v(20, 1), Applied::Won),
            (None, v(0, 1), Applied::Lost(v(1, 1))),
            (None, v(30, 1), Applied::Won),
        ];
        for mode in [ApplyMode::Legacy, ApplyMode::Blind] {
            for &(value, version, legacy_result) in cases {
                let (store, state) = build_async_only_store(false);
                let store = RefCell::new(store);
                let dev = store.borrow().device_handle();
                let response = vals_rt::block_on(
                    serve_apply(
                        &store,
                        dev.as_ref(),
                        &ReadGate::new(),
                        b"cold",
                        value,
                        version,
                        crate::hlc::physical_now_ms(),
                        mode,
                    ),
                    reactor::poll_io,
                );
                let expected = if mode == ApplyMode::Blind {
                    Applied::Won
                } else {
                    legacy_result
                };
                assert_eq!(response, Response::Apply(expected));
                let reads = state.reads.load(Ordering::Relaxed);
                if mode == ApplyMode::Blind {
                    assert_eq!(reads, 0);
                } else {
                    assert!(reads > 0);
                }
                state.panic_sync.store(false, Ordering::Release);
                let stored = store.borrow().get_versioned(b"cold").unwrap().unwrap();
                let winner = if version.wins_over(&v(1, 1)) {
                    version
                } else {
                    v(1, 1)
                };
                assert_eq!(stored.version, winner);
                if value.is_none() && winner == version {
                    assert_eq!(stored.value, None);
                }
            }
        }
    }

    #[test]
    fn staged_apply_retry_exhaustion_never_falls_back_to_sync_read() {
        let (store, state) = build_async_only_store(false);
        let store = Rc::new(RefCell::new(store));
        let fires = Rc::new(std::cell::Cell::new(0u64));
        {
            let store = store.clone();
            let fires = fires.clone();
            let state = state.clone();
            set_staged_apply_commit_hook(Some(Box::new(move || {
                let attempt = fires.get();
                fires.set(attempt + 1);
                let key = format!("race-{attempt:03}");
                let mut store = store.borrow_mut();
                state.panic_sync.store(false, Ordering::Release);
                store.apply(key.as_bytes(), Some(b"x"), v(10 + attempt, 1)).unwrap();
                store.flush().unwrap();
                state.panic_sync.store(true, Ordering::Release);
            })));
        }
        let dev = store.borrow().device_handle();
        let response = vals_rt::block_on(
            serve_apply(
                store.as_ref(),
                dev.as_ref(),
                &ReadGate::new(),
                b"cold",
                Some(b"new"),
                v(100, 1),
                crate::hlc::physical_now_ms(),
                ApplyMode::Legacy,
            ),
            reactor::poll_io,
        );
        set_staged_apply_commit_hook(None);

        assert!(matches!(response, Response::Busy));
        assert_eq!(fires.get(), 8);
        assert!(state.reads.load(Ordering::Relaxed) >= 8);
    }

    #[test]
    fn staged_batch_retry_exhaustion_never_falls_back_to_sync_read() {
        let (store, state) = build_async_only_store(false);
        let store = Rc::new(RefCell::new(store));
        let fires = Rc::new(std::cell::Cell::new(0u64));
        {
            let store = store.clone();
            let fires = fires.clone();
            let state = state.clone();
            set_staged_batch_commit_hook(Some(Box::new(move || {
                let attempt = fires.get();
                fires.set(attempt + 1);
                let key = format!("batch-race-{attempt:03}");
                let mut store = store.borrow_mut();
                state.panic_sync.store(false, Ordering::Release);
                store.apply(key.as_bytes(), Some(b"x"), v(10 + attempt, 1)).unwrap();
                store.flush().unwrap();
                state.panic_sync.store(true, Ordering::Release);
            })));
        }
        let dev = store.borrow().device_handle();
        let writes = vec![(b"batch-cold".to_vec(), Some(b"new".to_vec()), v(100, 1))];
        let response = vals_rt::block_on(
            serve_batch_apply(
                None,
                DEFAULT_ZONE,
                store.as_ref(),
                dev.as_ref(),
                &ReadGate::new(),
                &writes,
                false,
                ApplyMode::Legacy,
            ),
            reactor::poll_io,
        );
        set_staged_batch_commit_hook(None);

        assert!(matches!(response, Response::Busy));
        assert_eq!(fires.get(), 8);
        assert!(state.reads.load(Ordering::Relaxed) >= 8);
    }

    #[test]
    fn mirror_busy_maps_to_busy_and_hard_failure_maps_to_error() {
        assert_eq!(classify_mirror_acks(&[BatchAck::Busy]), MirrorOutcome::Busy);
        assert_eq!(
            classify_mirror_acks(&[
                BatchAck::Applied(Applied::Won),
                BatchAck::Applied(Applied::Lost(v(9, 2))),
            ]),
            MirrorOutcome::Mirrored
        );
        assert_eq!(
            classify_mirror_acks(&[BatchAck::Applied(Applied::Rejected)]),
            MirrorOutcome::Failed
        );
        assert_eq!(
            classify_mirror_acks(&[BatchAck::Applied(Applied::Won), BatchAck::Busy]),
            MirrorOutcome::Busy
        );
        assert_eq!(
            classify_mirror_acks(&[BatchAck::Stale(7)]),
            MirrorOutcome::Busy
        );
        for hard in [
            BatchAck::Timeout,
            BatchAck::Remote,
            BatchAck::Unreachable,
            BatchAck::Applied(Applied::Rejected),
        ] {
            assert_eq!(classify_mirror_acks(&[hard]), MirrorOutcome::Failed);
        }
        assert_eq!(
            classify_mirror_acks(&[BatchAck::Busy, BatchAck::Unreachable]),
            MirrorOutcome::Failed
        );
        let mut busy_stat = RehomeMirrorStat::default();
        let mut busy_outcome = MirrorOutcome::Mirrored;
        record_mirror_outcome(
            &mut busy_stat,
            &mut busy_outcome,
            MirrorOutcome::Busy,
        );
        assert_eq!(busy_stat.ok, 0);
        assert_eq!(busy_stat.failures, 0);
        assert!(matches!(
            mirrored_response(
                busy_outcome,
                Response::Apply(Applied::Won),
                "mirror failed",
            ),
            Response::Busy
        ));
        assert!(matches!(
            mirrored_response(
                busy_outcome,
                Response::Batch(vec![Applied::Won]),
                "mirror failed",
            ),
            Response::Busy
        ));

        let mut failed_stat = RehomeMirrorStat::default();
        let mut failed_outcome = MirrorOutcome::Mirrored;
        record_mirror_outcome(
            &mut failed_stat,
            &mut failed_outcome,
            MirrorOutcome::Failed,
        );
        assert_eq!(failed_stat.ok, 0);
        assert_eq!(failed_stat.failures, 1);
        assert!(matches!(
            mirrored_response(
                failed_outcome,
                Response::Apply(Applied::Won),
                "mirror failed",
            ),
            Response::Err(message) if message == "mirror failed"
        ));
        assert!(matches!(
            mirrored_response(
                failed_outcome,
                Response::Batch(vec![Applied::Won]),
                "batch mirror failed",
            ),
            Response::Err(message) if message == "batch mirror failed"
        ));
    }

    #[test]
    fn mirror_timeout_accounting_requires_a_later_positive_ack() {
        fn run(script: Vec<Vec<BatchAck>>) -> (MirrorOutcome, RehomeMirrorStat) {
            let attempts = RefCell::new(script.into_iter());
            let target_outcome = vals_rt::block_on(
                apply_mirror_with_timeout_retries(1, || {
                    let acks = attempts.borrow_mut().next().expect("unexpected mirror retry");
                    async move { acks }
                }),
                crate::reactor::poll_io,
            );
            assert!(attempts.borrow_mut().next().is_none(), "unused mirror result");
            let mut stat = RehomeMirrorStat::default();
            let mut outcome = MirrorOutcome::Mirrored;
            record_mirror_outcome(&mut stat, &mut outcome, target_outcome);
            (outcome, stat)
        }

        let (outcome, stat) = run(vec![
            vec![BatchAck::Timeout],
            vec![BatchAck::Applied(Applied::Lost(v(8, 1)))],
        ]);
        assert_eq!(outcome, MirrorOutcome::Mirrored);
        assert_eq!(stat.ok, 1);
        assert_eq!(stat.failures, 0);

        for retry in [
            vec![BatchAck::Busy],
            vec![BatchAck::Stale(7)],
            vec![BatchAck::Remote],
            vec![BatchAck::Unreachable],
            vec![BatchAck::Applied(Applied::Rejected)],
        ] {
            let (outcome, stat) = run(vec![vec![BatchAck::Timeout], retry]);
            assert_eq!(outcome, MirrorOutcome::Failed);
            assert_eq!(stat.ok, 0);
            assert_eq!(stat.failures, 1);
        }

        let (outcome, stat) = run(vec![
            vec![BatchAck::Timeout],
            vec![BatchAck::Timeout],
            vec![BatchAck::Timeout],
        ]);
        assert_eq!(outcome, MirrorOutcome::Failed);
        assert_eq!(stat.ok, 0);
        assert_eq!(stat.failures, 1);

        for retryable in [vec![BatchAck::Busy], vec![BatchAck::Stale(9)]] {
            let (outcome, stat) = run(vec![retryable]);
            assert_eq!(outcome, MirrorOutcome::Busy);
            assert_eq!(stat.ok, 0);
            assert_eq!(stat.failures, 0);
        }
    }

    #[test]
    fn large_cross_shard_mirror_plan_is_bounded_by_destination_groups() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut map = RangeMap::single();
        map.enable_opaque_tokens();
        let shard_count = 4;
        let current_shard =
            (crate::ring::hash64(map.token_of(0)) % shard_count as u64) as usize;
        let mut nonce = 0u8;
        let pending = loop {
            let candidate = vals_client::partition::opaque_placement_token([nonce; 16]);
            let shard = (crate::ring::hash64(&candidate) % shard_count as u64) as usize;
            if shard != current_shard {
                break candidate;
            }
            nonce = nonce.wrapping_add(1);
        };
        assert!(map.set_pending(0, pending));
        let shared = ServerShared::single_zone_on(1, build_store(), ring.clone())
            .with_shard(current_shard, shard_count);
        let cfg = ZoneConfig {
            name: "sorted".into(),
            rf: 2,
            ordering: crate::zone::Ordering::Sorted,
            scope: None,
        };
        let count = MAX_MIRROR_BATCH_ENTRIES * 4 + 17;
        let writes: Vec<wire::OwnedWrite> = (0..count)
            .map(|i| {
                (
                    format!("key-{i:05}").into_bytes(),
                    Some(format!("value-{i}").into_bytes()),
                    v(i as u64 + 1, 9),
                )
            })
            .collect();

        let batches = plan_mirror_batches(
            &shared,
            7,
            &map,
            &cfg,
            &ring,
            writes.iter(),
        );

        assert_eq!(batches.len(), 10);
        assert_eq!(
            batches.iter().map(|batch| batch.entries.len()).sum::<usize>(),
            count * 2,
        );
        assert!(batches.iter().all(|batch| {
            matches!(batch.target, 1 | 2)
                && batch.entries.len() <= MAX_MIRROR_BATCH_ENTRIES
        }));
        for target in [1, 2] {
            let mirrored: Vec<_> = batches
                .iter()
                .filter(|batch| batch.target == target)
                .flat_map(|batch| batch.entries.iter())
                .collect();
            assert_eq!(mirrored.len(), count);
            for (i, entry) in mirrored.into_iter().enumerate() {
                assert_eq!(entry.key, writes[i].0.as_slice());
                assert_eq!(entry.value, writes[i].1.as_deref());
                assert_eq!(entry.version, writes[i].2);
            }
        }
        let sizes: Vec<usize> = batches.iter().map(|batch| batch.entries.len()).collect();
        let calls = Rc::new(std::cell::Cell::new(0));
        let active = Rc::new(std::cell::Cell::new(0));
        let peak = Rc::new(std::cell::Cell::new(0));
        let called = calls.clone();
        let active_calls = active.clone();
        let peak_calls = peak.clone();
        let expected = sizes.clone();
        let outcomes = vals_rt::block_on(
            apply_mirror_batches(&sizes, move |index| {
                called.set(called.get() + 1);
                active_calls.set(active_calls.get() + 1);
                peak_calls.set(peak_calls.get().max(active_calls.get()));
                let len = expected[index];
                let active = active_calls.clone();
                async move {
                    vals_rt::yield_now().await;
                    active.set(active.get() - 1);
                    vec![BatchAck::Applied(Applied::Won); len]
                }
            }),
            crate::reactor::poll_io,
        );
        assert_eq!(calls.get(), 10);
        assert_eq!(peak.get(), 4);
        assert!(outcomes.iter().all(|outcome| *outcome == MirrorOutcome::Mirrored));
    }

    #[test]
    fn mirror_batches_require_complete_acks_and_attribute_partial_and_timeout_failures() {
        let writes = [
            (b"a".to_vec(), Some(b"one".to_vec()), v(1, 1)),
            (b"b".to_vec(), Some(b"two".to_vec()), v(2, 1)),
            (b"m".to_vec(), Some(b"three".to_vec()), v(3, 1)),
        ];
        let batches = vec![
            MirrorBatch {
                lower: b"a".to_vec(),
                target: 2,
                entries: writes[..2].iter().map(|(key, value, version)| BatchEntry {
                    key,
                    value: value.as_deref(),
                    version: *version,
                }).collect(),
            },
            MirrorBatch {
                lower: b"m".to_vec(),
                target: 3,
                entries: writes[2..].iter().map(|(key, value, version)| BatchEntry {
                    key,
                    value: value.as_deref(),
                    version: *version,
                }).collect(),
            },
        ];
        let sizes: Vec<usize> = batches.iter().map(|batch| batch.entries.len()).collect();
        let calls = Rc::new(RefCell::new(Vec::new()));
        let called = calls.clone();
        let expected = sizes.clone();
        let success = vals_rt::block_on(
            apply_mirror_batches(&sizes, move |index| {
                called.borrow_mut().push(index);
                let len = expected[index];
                async move { vec![BatchAck::Applied(Applied::Won); len] }
            }),
            crate::reactor::poll_io,
        );
        assert_eq!(&*calls.borrow(), &[0, 1]);
        assert_eq!(success, vec![MirrorOutcome::Mirrored, MirrorOutcome::Mirrored]);

        let sizes: Vec<usize> = batches.iter().map(|batch| batch.entries.len()).collect();
        let calls = Rc::new(RefCell::new(Vec::new()));
        let called = calls.clone();
        let failed = vals_rt::block_on(
            apply_mirror_batches(&sizes, move |index| {
                called.borrow_mut().push(index);
                let acks = if index == 0 {
                    vec![BatchAck::Applied(Applied::Won)]
                } else if called.borrow().iter().filter(|&&i| i == index).count() == 1 {
                    vec![BatchAck::Timeout]
                } else {
                    vec![BatchAck::Remote]
                };
                async move { acks }
            }),
            crate::reactor::poll_io,
        );
        assert_eq!(&*calls.borrow(), &[0, 1, 1]);
        assert_eq!(failed, vec![MirrorOutcome::Failed, MirrorOutcome::Failed]);
        assert_eq!(batches[1].entries[0].version, writes[2].2);

        let shared = ServerShared::single_zone_on(
            1,
            build_store(),
            Ring::from_members(1, 64, [1]),
        );
        assert_eq!(
            record_mirror_batch_outcomes(&shared, 7, 12, &batches, failed),
            MirrorOutcome::Failed
        );
        let stats = shared.rehome_mirrors.borrow();
        assert_eq!(stats[&(7, b"a".to_vec())].failures, 1);
        assert_eq!(stats[&(7, b"m".to_vec())].failures, 1);
        assert_eq!(stats[&(7, b"a".to_vec())].epoch, 12);
        assert_eq!(stats[&(7, b"m".to_vec())].epoch, 12);
    }

    #[test]
    fn full_batch_apply_gate_sheds_instead_of_queueing() {
        let shared = ServerShared::single_zone(build_store()).with_settings(
            crate::node_settings::NodeSettings { batch_apply_gate: 2, ..Default::default() },
        );
        let gate = &shared.batch_apply_gate;
        let first = gate.try_acquire().unwrap();
        let second = gate.try_acquire().unwrap();
        assert!(gate.try_acquire().is_none());
        drop(first);
        let replacement = gate.try_acquire().unwrap();
        assert_eq!(gate.inflight(), 2);
        drop((second, replacement));
        assert_eq!(gate.inflight(), 0);
    }

    fn fence_shared(
        node: NodeId,
        ring: Ring,
        ordering: crate::zone::Ordering,
        map: Option<RangeMap>,
    ) -> ServerShared<MemBlockDevice> {
        let shared = ServerShared::single_zone_on(node, build_store(), ring);
        let store = shared.zones.borrow_mut().remove(&DEFAULT_ZONE).unwrap();
        shared.zones.borrow_mut().insert(7, store);
        let dev = shared.devices.borrow_mut().remove(&DEFAULT_ZONE).unwrap();
        shared.devices.borrow_mut().insert(7, dev);
        let mut catalog = Catalog::new();
        catalog.insert(7, ZoneConfig {
            name: "fence".into(),
            rf: 1,
            ordering,
            scope: None,
        });
        *shared.catalog.borrow_mut() = catalog;
        if let Some(map) = map {
            shared.range_maps.borrow_mut().insert(7, map);
            shared.range_map_epoch.set(11);
        }
        shared
    }

    fn apply_request(key: &[u8], version: Version) -> OwnedRequest {
        OwnedRequest::Apply {
            epoch: 1,
            zone_id: 7,
            key: key.to_vec(),
            value: Some(b"fenced-value".to_vec()),
            version,
        }
    }

    struct AsyncMirrorHookGuard;
    impl Drop for AsyncMirrorHookGuard {
        fn drop(&mut self) {
            set_async_write_mirror_hook(None);
        }
    }

    #[test]
    fn post_apply_mirror_decision_updates_only_stable_range_marks() {
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut map = RangeMap::single();
        assert!(map.split(b"m"));
        let shared = fence_shared(1, ring.clone(), crate::zone::Ordering::Sorted, Some(map));
        let initial = shared.range_write_mark_register(7, b"m".to_vec(), 0);
        assert!(!post_apply_needs_mirror(&shared, 7, b"z"));
        assert!(shared.range_write_mark_register(7, b"m".to_vec(), 0) > initial);

        let mut pending = RangeMap::single();
        assert!(pending.split(b"m"));
        assert!(pending.set_pending(1, b"m".to_vec()));
        let shared = fence_shared(1, ring.clone(), crate::zone::Ordering::Sorted, Some(pending));
        let initial = shared.range_write_mark_register(7, b"m".to_vec(), 0);
        assert!(post_apply_needs_mirror(&shared, 7, b"z"));
        assert_eq!(shared.range_write_mark_register(7, b"m".to_vec(), 0), initial);

        let shared = fence_shared(1, ring, crate::zone::Ordering::Sorted, None);
        let initial = shared.range_write_mark_register(7, Vec::new(), 0);
        assert!(!post_apply_needs_mirror(&shared, 7, b"z"));
        assert!(shared.range_write_mark_register(7, Vec::new(), 0) > initial);
    }

    #[test]
    fn batch_mirror_composition_filters_rejected_results() {
        let writes = vec![
            (b"future".to_vec(), Some(b"rejected".to_vec()), v(30, 1)),
            (b"winner".to_vec(), Some(b"accepted".to_vec()), v(20, 1)),
            (b"gc-floor".to_vec(), None, v(10, 1)),
        ];
        let mirrored = applied_batch_writes(
            &writes,
            &[Applied::Rejected, Applied::Won, Applied::Rejected],
        );
        assert_eq!(mirrored.len(), 1);
        assert_eq!(mirrored[0].0, b"winner");
    }

    #[test]
    fn batch_non_result_does_not_start_mirror_fanout() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut map = RangeMap::single();
        assert!(map.split(&boundary));
        assert!(map.set_pending(1, boundary.clone()));
        let shared = Rc::new(fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(map),
        ));
        let writes = vec![(boundary, Some(b"value".to_vec()), v(10, current))];

        let response = vals_rt::block_on(
            mirror_batch_response(shared.clone(), 7, &writes, Response::Busy, false, None),
            crate::reactor::poll_io,
        );
        assert!(matches!(response, Response::Busy));
        assert!(shared.rehome_mirrors.borrow().is_empty());
        assert!(shared.rehome_transport.borrow().is_none());

        let response = vals_rt::block_on(
            mirror_batch_response(
                shared.clone(),
                7,
                &writes,
                Response::Err("local failure".into()),
                false,
                None,
            ),
            crate::reactor::poll_io,
        );
        assert!(matches!(response, Response::Err(ref e) if e == "local failure"));
        assert!(shared.rehome_mirrors.borrow().is_empty());
        assert!(shared.rehome_transport.borrow().is_none());
    }

    #[test]
    fn pending_activation_during_local_probe_is_mirrored_after_mutation() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut stable = RangeMap::single();
        assert!(stable.split(&boundary));
        let shared = Rc::new(fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(stable.clone()),
        ));
        shared.range_map_epoch.set(10);
        let initial_mark = shared.range_write_mark_register(7, boundary.clone(), 0);
        let store = shared.zone_store(7).unwrap();
        store
            .borrow_mut()
            .apply(&boundary, Some(b"old"), v(10, current))
            .unwrap();
        store.borrow_mut().flush().unwrap();
        let writes = vec![(
            boundary.clone(),
            Some(b"new".to_vec()),
            v(20, current),
        )];
        let fired = Rc::new(std::cell::Cell::new(false));
        let hook_shared = shared.clone();
        let hook_boundary = boundary.clone();
        let hook_fired = fired.clone();
        set_staged_batch_commit_hook(Some(Box::new(move || {
            if hook_fired.replace(true) {
                return;
            }
            let mut pending = stable.clone();
            assert!(pending.set_pending(1, hook_boundary.clone()));
            hook_shared.range_maps.borrow_mut().insert(7, pending);
            hook_shared.range_map_epoch.set(11);
        })));
        let dev = store.borrow().device_handle();
        let response = vals_rt::block_on(
            serve_mirrored_batch_apply(
                shared.clone(),
                7,
                &store,
                dev.as_ref(),
                &shared.read_gate,
                &writes,
                false,
                ApplyMode::Legacy,
                None,
            ),
            crate::reactor::poll_io,
        );
        set_staged_batch_commit_hook(None);

        assert!(fired.get());
        assert!(matches!(response, Response::Err(_)));
        assert_eq!(
            shared.range_write_mark_register(7, boundary.clone(), 0),
            initial_mark,
        );
        assert_eq!(
            store.borrow().get(&boundary).unwrap().as_deref(),
            Some(b"new" as &[u8]),
        );
        assert_eq!(shared.rehome_mirrors.borrow()[&(7, boundary)].failures, 1);
    }

    #[test]
    fn pending_activation_during_group_commit_is_mirrored_after_durability() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut stable = RangeMap::single();
        assert!(stable.split(&boundary));
        let shared = Rc::new(fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(stable.clone()),
        ));
        shared.range_map_epoch.set(10);
        let initial_mark = shared.range_write_mark_register(7, boundary.clone(), 0);
        let store = shared.zone_store(7).unwrap();
        let dev = store.borrow().device_handle();
        let writes = vec![(
            boundary.clone(),
            Some(b"durable".to_vec()),
            v(20, current),
        )];

        enum RaceResult {
            Activated,
            Applied(Response),
        }
        let hook_shared = shared.clone();
        let hook_boundary = boundary.clone();
        let futures: Vec<Pin<Box<dyn Future<Output = RaceResult> + '_>>> = vec![
            Box::pin(async move {
                vals_rt::yield_now().await;
                let mut pending = stable;
                assert!(pending.set_pending(1, hook_boundary));
                hook_shared.range_maps.borrow_mut().insert(7, pending);
                hook_shared.range_map_epoch.set(11);
                RaceResult::Activated
            }),
            Box::pin(async {
                RaceResult::Applied(
                    serve_mirrored_batch_apply(
                        shared.clone(),
                        7,
                        &store,
                        dev.as_ref(),
                        &shared.read_gate,
                        &writes,
                        true,
                        ApplyMode::Blind,
                        None,
                    )
                    .await,
                )
            }),
        ];
        let results = vals_rt::block_on(
            vals_rt::join_all(futures),
            crate::reactor::poll_io,
        );
        let response = results
            .into_iter()
            .find_map(|result| match result {
                RaceResult::Activated => None,
                RaceResult::Applied(response) => Some(response),
            })
            .unwrap();
        assert!(matches!(response, Response::Err(_)));
        assert_eq!(
            shared.range_write_mark_register(7, boundary.clone(), 0),
            initial_mark,
        );
        assert_eq!(
            store.borrow().get(&boundary).unwrap().as_deref(),
            Some(b"durable" as &[u8]),
        );
        assert_eq!(shared.rehome_mirrors.borrow()[&(7, boundary)].failures, 1);
    }

    #[test]
    fn final_mirror_fence_rejects_epoch_change_while_source_is_pending() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut pending = RangeMap::single();
        assert!(pending.split(&boundary));
        assert!(pending.set_pending(1, boundary.clone()));
        let shared = fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(pending),
        );
        shared.range_map_epoch.set(11);

        let response = fence_mirrored_response(
            &shared,
            7,
            std::iter::once(boundary.as_slice()),
            10,
            None,
            Response::Batch(vec![Applied::Won]),
        );
        assert!(matches!(response, Response::StaleEpoch(11)));
    }

    #[test]
    fn single_write_fence_matches_multi_key_fence() {
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut map = RangeMap::single();
        assert!(map.split(b"m"));
        assert!(map.set_pending(1, b"m".to_vec()));
        for node in [1, 2, 3] {
            let shared = fence_shared(
                node,
                ring.clone(),
                crate::zone::Ordering::Sorted,
                Some(map.clone()),
            );
            for key in [b"a" as &[u8], b"z"] {
                assert_eq!(
                    rehome_write_fence_one(&shared, 7, key),
                    rehome_write_fence(&shared, 7, std::iter::once(key)),
                );
            }
        }
        let shared = fence_shared(1, ring, crate::zone::Ordering::Unsorted, None);
        assert_eq!(rehome_write_fence_one(&shared, 7, b"z"), None);
    }

    #[test]
    fn blind_apply_rechecks_fence_after_cutover_lands_before_ack() {
        let _hook_guard = AsyncMirrorHookGuard;
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut prepared = RangeMap::single();
        assert!(prepared.split(&boundary));
        assert!(prepared.set_pending(1, boundary.clone()));
        let shared = Rc::new(fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(prepared.clone()),
        ));
        shared.range_map_epoch.set(10);
        assert!(rehome_write_fence(
            shared.as_ref(),
            7,
            std::iter::once(boundary.as_slice()),
        )
        .is_none());

        let version = v(40, current);
        let staged = shared
            .zone_store(7)
            .unwrap()
            .borrow_mut()
            .apply_unconditional_stage(
                &boundary,
                Some(b"local-orphan"),
                version,
                0,
                ApplyMode::Blind,
            )
            .unwrap();
        let StagedApply::Done(applied) = staged else {
            panic!("new key must complete without a run read");
        };
        let mut committed = prepared;
        assert!(committed.commit_pending(1));
        let hook_shared = shared.clone();
        set_async_write_mirror_hook(Some(Box::new(move || {
            hook_shared.range_maps.borrow_mut().insert(7, committed.clone());
            hook_shared.range_map_epoch.set(11);
        })));

        let response = vals_rt::block_on(
            mirror_apply_response(
                shared.clone(),
                7,
                (boundary.clone(), Some(b"local-orphan".to_vec()), version),
                applied,
            ),
            crate::reactor::poll_io,
        );
        assert!(matches!(response, Response::StaleEpoch(11)));
        assert_eq!(
            shared
                .zone_store(7)
                .unwrap()
                .borrow()
                .get(&boundary)
                .unwrap()
                .as_deref(),
            Some(b"local-orphan" as &[u8])
        );
    }

    #[test]
    fn blind_synced_batch_rechecks_fence_after_cutover_lands_before_ack() {
        let _hook_guard = AsyncMirrorHookGuard;
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut prepared = RangeMap::single();
        assert!(prepared.split(&boundary));
        assert!(prepared.set_pending(1, boundary.clone()));
        let shared = Rc::new(fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(prepared.clone()),
        ));
        shared.range_map_epoch.set(10);
        let writes = vec![(
            boundary.clone(),
            Some(b"batch-orphan".to_vec()),
            v(41, current),
        )];
        assert!(rehome_write_fence(
            shared.as_ref(),
            7,
            writes.iter().map(|(key, _, _)| key.as_slice()),
        )
        .is_none());
        let store = shared.zone_store(7).unwrap();
        let response = drive_batch_mode(&store, &writes, true, ApplyMode::Blind);
        let Response::Batch(results) = response else {
            panic!("local batch must apply");
        };
        let mut committed = prepared;
        assert!(committed.commit_pending(1));
        let hook_shared = shared.clone();
        set_async_write_mirror_hook(Some(Box::new(move || {
            hook_shared.range_maps.borrow_mut().insert(7, committed.clone());
            hook_shared.range_map_epoch.set(11);
        })));

        let response = vals_rt::block_on(
            mirror_batch_response(shared.clone(), 7, &writes, Response::Batch(results), true, None),
            crate::reactor::poll_io,
        );
        assert!(matches!(response, Response::StaleEpoch(11)));
        assert_eq!(
            store.borrow().get(&boundary).unwrap().as_deref(),
            Some(b"batch-orphan" as &[u8])
        );
    }

    #[test]
    fn range_fence_allows_owners_and_pending_targets_and_rejects_former_owners() {
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let pending = ring.replicas_in_zone(7, &boundary, 1, None)[0];
        let mut prepared = RangeMap::single();
        assert!(prepared.split(&boundary));
        assert!(prepared.set_pending(1, boundary.clone()));

        let current_shared = fence_shared(
            current,
            ring.clone(),
            crate::zone::Ordering::Sorted,
            Some(prepared.clone()),
        );
        assert!(matches!(
            serve_request(&current_shared, apply_request(&boundary, v(20, 1))),
            Response::Apply(_)
        ));

        let pending_shared = fence_shared(
            pending,
            ring.clone(),
            crate::zone::Ordering::Sorted,
            Some(prepared.clone()),
        );
        assert!(matches!(
            serve_request(&pending_shared, apply_request(&boundary, v(21, 1))),
            Response::Apply(_)
        ));

        let mut committed = prepared;
        assert!(committed.commit_pending(1));
        let former_shared = fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(committed),
        );
        let before = crate::observe::counters().range_fence_rejections.get();
        assert!(matches!(
            serve_request(&former_shared, apply_request(&boundary, v(22, 1))),
            Response::StaleEpoch(11)
        ));
        assert!(crate::observe::counters().range_fence_rejections.get() > before);
    }

    #[test]
    fn opaque_range_fence_allows_pending_then_rejects_former_owner() {
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let boundary = b"m".to_vec();
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut nonce = 0u8;
        let opaque = loop {
            let token = vals_client::partition::opaque_placement_token([nonce; 16]);
            if ring.replicas_in_zone(7, &token, 1, None)[0] != current {
                break token;
            }
            nonce = nonce.wrapping_add(1);
        };
        let pending = ring.replicas_in_zone(7, &opaque, 1, None)[0];
        let mut prepared = RangeMap::from_splits([boundary.clone()]);
        prepared.enable_opaque_tokens();
        assert!(prepared.set_pending(1, opaque));
        let pending_shared = fence_shared(
            pending,
            ring.clone(),
            crate::zone::Ordering::Sorted,
            Some(prepared.clone()),
        );
        assert!(matches!(
            serve_request(&pending_shared, apply_request(&boundary, v(30, 1))),
            Response::Apply(_)
        ));

        assert!(prepared.commit_pending(1));
        let former_shared = fence_shared(
            current,
            ring,
            crate::zone::Ordering::Sorted,
            Some(prepared),
        );
        assert!(matches!(
            serve_request(&former_shared, apply_request(&boundary, v(31, 1))),
            Response::StaleEpoch(11)
        ));
    }

    #[test]
    fn range_fence_skips_unsorted_and_missing_maps_and_rejects_whole_batch() {
        let ring = Ring::from_members(1, 64, [1, 2, 3]);
        let mut boundary = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &boundary, 1, None)
        {
            boundary.push(b'x');
        }
        let former = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut committed = RangeMap::single();
        assert!(committed.split(&boundary));
        assert!(committed.set_pending(1, boundary.clone()));
        assert!(committed.commit_pending(1));

        let unsorted = fence_shared(
            former,
            ring.clone(),
            crate::zone::Ordering::Unsorted,
            Some(committed.clone()),
        );
        assert!(matches!(
            serve_request(&unsorted, apply_request(&boundary, v(30, 1))),
            Response::Apply(_)
        ));

        let missing = fence_shared(
            former,
            ring.clone(),
            crate::zone::Ordering::Sorted,
            None,
        );
        assert!(matches!(
            serve_request(&missing, apply_request(&boundary, v(31, 1))),
            Response::Apply(_)
        ));

        let sorted = fence_shared(
            former,
            ring,
            crate::zone::Ordering::Sorted,
            Some(committed),
        );
        let batch = OwnedRequest::BatchApply {
            epoch: 1,
            zone_id: 7,
            sync: false,
            writes: vec![
                (b"aa-new".to_vec(), Some(b"owner".to_vec()), v(32, 1)),
                (boundary, Some(b"former".to_vec()), v(33, 1)),
            ],
        };
        assert!(matches!(serve_request(&sorted, batch), Response::StaleEpoch(11)));
        assert_eq!(
            sorted.zone_store(7).unwrap().borrow().get(b"aa-new").unwrap(),
            None
        );
    }

    #[test]
    fn mirror_failure_is_not_an_ack_and_is_reported() {
        let ring = Ring::from_members(1, 64, [1, 2]);
        let mut key = b"m".to_vec();
        while ring.replicas_in_zone(7, b"", 1, None)
            == ring.replicas_in_zone(7, &key, 1, None)
        {
            key.push(b'x');
        }
        let current = ring.replicas_in_zone(7, b"", 1, None)[0];
        let mut shared = ServerShared::single_zone_on(current, build_store(), ring.clone());
        let mut catalog = Catalog::new();
        catalog.insert(7, ZoneConfig {
            name: "sorted".into(),
            rf: 1,
            ordering: crate::zone::Ordering::Sorted,
            scope: None,
        });
        shared.catalog = Tracked::new(catalog);
        let mut map = RangeMap::single();
        assert!(map.split(&key));
        assert!(map.set_pending(1, key.clone()));
        shared.range_maps.borrow_mut().insert(7, map);
        shared.range_map_epoch.set(9);
        let shared = Rc::new(shared);
        let writes = vec![(key.clone(), Some(b"v".to_vec()), v(10, current))];
        let apply_response = vals_rt::block_on(
            mirror_apply_response(shared.clone(), 7, writes[0].clone(), Applied::Won),
            crate::reactor::poll_io,
        );
        assert!(matches!(apply_response, Response::Err(_)));
        assert_eq!(shared.rehome_mirrors.borrow()[&(7, key.clone())].failures, 1);

        let batch_response = vals_rt::block_on(
            mirror_batch_response(
                shared.clone(),
                7,
                &writes,
                Response::Batch(vec![Applied::Won]),
                false,
                None,
            ),
            crate::reactor::poll_io,
        );
        assert!(matches!(batch_response, Response::Err(_)));
        assert_eq!(shared.rehome_mirrors.borrow()[&(7, key)].failures, 2);
    }

    fn drive_batch_mode(
        store: &RefCell<VersionedStore<MemBlockDevice>>,
        writes: &[crate::wire::OwnedWrite],
        sync: bool,
        mode: ApplyMode,
    ) -> Response {
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        vals_rt::block_on(
            serve_batch_apply(
                None,
                DEFAULT_ZONE,
                store,
                dev.as_ref(),
                &gate,
                writes,
                sync,
                mode,
            ),
            reactor::poll_io,
        )
    }

    fn drive_batch(
        store: &RefCell<VersionedStore<MemBlockDevice>>,
        writes: &[crate::wire::OwnedWrite],
        sync: bool,
    ) -> Response {
        drive_batch_mode(store, writes, sync, ApplyMode::Legacy)
    }

    fn batch(
        store: &RefCell<VersionedStore<MemBlockDevice>>,
        writes: &[crate::wire::OwnedWrite],
    ) -> Vec<Applied> {
        match drive_batch(store, writes, false) {
            Response::Batch(r) => r,
            other => panic!("expected Batch, got {other:?}"),
        }
    }

    fn corpus() -> Vec<crate::wire::OwnedWrite> {
        let future = Version {
            hlc: (crate::hlc::physical_now_ms() + 1_000_000_000) << 16,
            node: 1,
        };
        let w =
            |k: &[u8], val: Option<&[u8]>, ver: Version| (k.to_vec(), val.map(|v| v.to_vec()), ver);
        vec![
            w(b"a", Some(b"a1"), v(9, 1)),
            w(b"a", Some(b"a2"), v(15, 1)),
            w(b"a", Some(b"alo"), v(3, 1)),
            w(b"d", None, v(7, 1)),
            w(b"c", Some(b"cwin"), v(12, 1)),
            w(b"c", Some(b"clo"), v(4, 1)),
            w(b"b", Some(b"bwin"), v(20, 1)),
            w(b"z", Some(b"znew"), v(3, 1)),
            w(b"z", Some(b"z2"), v(5, 1)),
            w(b"z", Some(b"z0"), v(2, 1)),
            w(b"y", None, v(3, 1)),
            w(b"a", Some(b"nope"), future),
        ]
    }

    #[test]
    fn cold_overwrites_count_probe_runs_and_blocks() {
        let store = RefCell::new(build_store());
        let probe_counts = Rc::new(std::cell::Cell::new((0, 0)));
        {
            let probe_counts = probe_counts.clone();
            set_staged_batch_probe_hook(Some(Box::new(move |runs, blocks| {
                let (total_runs, total_blocks) = probe_counts.get();
                probe_counts.set((total_runs + runs, total_blocks + blocks));
            })));
        }
        let writes = vec![
            (b"a".to_vec(), Some(b"a-over".to_vec()), v(30, 1)),
            (b"d".to_vec(), Some(b"d-over".to_vec()), v(31, 1)),
        ];
        let results = batch(&store, &writes);
        set_staged_batch_probe_hook(None);
        assert_eq!(results, vec![Applied::Won, Applied::Won]);
        let (runs, blocks) = probe_counts.get();
        assert!(
            runs >= 2,
            "both flushed keys must probe at least one run each (got {runs})"
        );
        assert!(
            blocks >= 2,
            "both cold resolves must read at least one block each (got {blocks})"
        );
        assert!(
            blocks <= runs,
            "early exit: blocks read ({blocks}) never exceed runs probed ({runs})"
        );
    }

    #[test]
    fn covered_batch_stragglers_lose_and_sync_values_returns_the_cover() {
        let store = RefCell::new(build_store());
        let wall = crate::hlc::physical_now_ms();
        assert_eq!(
            store.borrow_mut().apply_range_delete(Some(b"a"), Some(b"c"), v(10, 1), wall).unwrap(),
            Applied::Won
        );
        let seq = store.borrow().engine().next_log_seq();
        let w = |k: &[u8], val: Option<&[u8]>, ver: Version| (k.to_vec(), val.map(<[u8]>::to_vec), ver);
        let stragglers = vec![
            w(b"a", Some(b"late"), v(5, 1)),
            w(b"b", Some(b"late"), v(10, 1)),
            w(b"ab", None, v(9, 1)),
        ];
        assert_eq!(batch(&store, &stragglers), vec![Applied::Lost(v(10, 1)); 3]);
        assert_eq!(store.borrow().engine().next_log_seq(), seq, "covered batch entries append nothing");
        let newer = vec![w(b"a", Some(b"new"), v(11, 1)), w(b"c", Some(b"old"), v(2, 1))];
        assert_eq!(batch(&store, &newer), vec![Applied::Won, Applied::Lost(v(9, 1))]);

        let dev = store.borrow().device_handle();
        let keys = [b"a".to_vec(), b"ab".to_vec(), b"b".to_vec(), b"c".to_vec(), b"zz".to_vec()];
        let response = vals_rt::block_on(
            serve_sync_values(&store, &*dev, &ReadGate::new(), &keys, Default::default()),
            reactor::poll_io,
        );
        let Response::Values { entries, more: false } = response else {
            panic!("expected one values page, got {response:?}");
        };
        let stored = |ver: Version, val: Option<&[u8]>| crate::replica::Stored { version: ver, value: val.map(<[u8]>::to_vec) };
        assert_eq!(
            entries,
            vec![
                (b"a".to_vec(), stored(v(11, 1), Some(b"new"))),
                (b"ab".to_vec(), stored(v(10, 1), None)),
                (b"b".to_vec(), stored(v(10, 1), None)),
                (b"c".to_vec(), stored(v(9, 1), Some(b"v9"))),
            ]
        );
    }

    #[test]
    fn blind_batch_accepts_repeated_candidates_without_run_probes() {
        let (store, state) = build_async_only_store(false);
        let store = RefCell::new(store);
        let writes = vec![
            (b"batch-cold".to_vec(), Some(b"stale".to_vec()), v(0, 1)),
            (b"batch-cold".to_vec(), Some(b"new".to_vec()), v(20, 1)),
            (b"batch-cold".to_vec(), Some(b"mid".to_vec()), v(10, 1)),
            (b"batch-cold".to_vec(), None, v(30, 1)),
            (b"batch-cold".to_vec(), None, v(30, 1)),
            (b"batch-cold".to_vec(), Some(b"hidden".to_vec()), v(25, 1)),
        ];
        let dev = store.borrow().device_handle();
        let response = vals_rt::block_on(
            serve_batch_apply(
                None,
                DEFAULT_ZONE,
                &store,
                dev.as_ref(),
                &ReadGate::new(),
                &writes,
                true,
                ApplyMode::Blind,
            ),
            reactor::poll_io,
        );
        assert_eq!(
            response,
            Response::Batch(vec![Applied::Won; writes.len()])
        );
        assert_eq!(state.reads.load(Ordering::Relaxed), 0);
        state.panic_sync.store(false, Ordering::Release);
        assert_eq!(
            store
                .borrow()
                .get_versioned(b"batch-cold")
                .unwrap()
                .unwrap()
                .version,
            v(30, 1)
        );
        assert_eq!(store.borrow().get(b"batch-cold").unwrap(), None);
    }

    #[test]
    fn staged_batch_matches_sync_reference() {
        let writes = corpus();

        let mut reference = build_store();
        let ref_results: Vec<Applied> = writes
            .iter()
            .map(|(k, val, ver)| reference.apply(k, val.as_deref(), *ver).unwrap())
            .collect();

        let staged = RefCell::new(build_store());
        let staged_results = batch(&staged, &writes);

        assert_eq!(staged_results, ref_results, "per-entry Applied vector");

        for key in [b"a" as &[u8], b"b", b"c", b"d", b"y", b"z", b"missing"] {
            let s = staged.borrow();
            let r = reference.get(key).unwrap();
            assert_eq!(s.get(key).unwrap(), r, "final value mismatch for {key:?}");
            assert_eq!(
                s.get_versioned(key).unwrap().map(|v| v.version),
                reference.get_versioned(key).unwrap().map(|v| v.version),
                "final version mismatch for {key:?}",
            );
        }
        assert_eq!(
            staged.borrow().version_leaf_digests(BUCKETS).unwrap(),
            reference.version_leaf_digests(BUCKETS).unwrap(),
            "leaf-accumulator drift",
        );
    }

    #[test]
    fn staged_batch_retries_and_converges_on_generation_change() {
        let writes = corpus();

        let mut reference = build_store();
        let ref_results: Vec<Applied> = writes
            .iter()
            .map(|(k, val, ver)| reference.apply(k, val.as_deref(), *ver).unwrap())
            .collect();

        let store = Rc::new(RefCell::new(build_store()));

        let fires = Rc::new(std::cell::Cell::new(0u32));
        {
            let store = store.clone();
            let fires = fires.clone();
            set_staged_batch_commit_hook(Some(Box::new(move || {
                if fires.get() == 0 {
                    fires.set(1);
                    let mut s = store.borrow_mut();
                    s.apply(b"unrelated", Some(b"x"), v(1, 1)).unwrap();
                    s.flush().unwrap();
                }
            })));
        }

        let got = batch(&store, &writes);
        set_staged_batch_commit_hook(None);

        assert_eq!(
            fires.get(),
            1,
            "the mid-batch run-set change must have fired"
        );
        assert_eq!(got, ref_results, "batch must converge to the sync result");
        for key in [b"a" as &[u8], b"b", b"c", b"d", b"y", b"z"] {
            assert_eq!(
                store.borrow().get(key).unwrap(),
                reference.get(key).unwrap(),
                "final value mismatch for {key:?} after the race",
            );
        }
    }

    #[test]
    fn cancelled_group_commit_leader_hands_off_to_followers() {
        let shared = ServerShared::single_zone(build_store());
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let mut leader = Box::pin(group_commit(&shared, DEFAULT_ZONE, store.as_ref()));
        let mut follower = Box::pin(group_commit(&shared, DEFAULT_ZONE, store.as_ref()));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(leader.as_mut().poll(&mut cx), Poll::Pending));
        assert!(shared.sync_groups.borrow()[&DEFAULT_ZONE].running);
        assert!(matches!(follower.as_mut().poll(&mut cx), Poll::Pending));
        drop(leader);
        assert!(!shared.sync_groups.borrow()[&DEFAULT_ZONE].running);
        assert_eq!(shared.sync_groups.borrow()[&DEFAULT_ZONE].commits, 0);
        let result = (0..8).find_map(|_| match follower.as_mut().poll(&mut cx) {
            Poll::Ready(r) => Some(r),
            Poll::Pending => None,
        });
        assert!(matches!(result, Some(SyncResult::Ok)), "the follower re-leads its own round");
        assert_eq!(shared.sync_groups.borrow()[&DEFAULT_ZONE].commits, 1);
    }

    #[test]
    fn group_commit_failure_reaches_every_joined_batch() {
        let fail = Arc::new(AtomicBool::new(false));
        let dev = FailingFlushDevice {
            inner: MemBlockDevice::new(256, 8192),
            fail: fail.clone(),
        };
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let store = VersionedStore::new(engine, 1).unwrap();
        let shared = ServerShared::single_zone(store);
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let batches: Vec<Vec<crate::wire::OwnedWrite>> = (0..4u64)
            .map(|i| {
                vec![(
                    format!("failed-group-{i}").into_bytes(),
                    Some(b"v".to_vec()),
                    v(3_000 + i, 1),
                )]
            })
            .collect();
        fail.store(true, Ordering::Relaxed);
        let futures: Vec<_> = batches
            .iter()
            .map(|writes| {
                serve_batch_apply(
                    Some(&shared),
                    DEFAULT_ZONE,
                    store.as_ref(),
                    dev.as_ref(),
                    &gate,
                    writes,
                    true,
                    ApplyMode::Legacy,
                )
            })
            .collect();
        let responses = vals_rt::block_on(vals_rt::join_all(futures), reactor::poll_io);
        assert!(responses.iter().all(|r| {
            matches!(r, Response::Err(e) if e.contains("injected group-commit failure"))
        }));

        let blind = vec![(
            b"failed-blind-group".to_vec(),
            Some(b"v".to_vec()),
            v(4_000, 1),
        )];
        let before = crate::observe::counters().blind_sync_errors.get();
        let response = vals_rt::block_on(
            serve_batch_apply(
                Some(&shared),
                DEFAULT_ZONE,
                store.as_ref(),
                dev.as_ref(),
                &gate,
                &blind,
                true,
                ApplyMode::Blind,
            ),
            reactor::poll_io,
        );
        assert!(matches!(
            response,
            Response::Err(e) if e.contains("injected group-commit failure")
        ));
        assert!(crate::observe::counters().blind_sync_errors.get() > before);
    }

    #[test]
    fn async_group_commit_keeps_the_shard_responsive_and_splits_late_joiners() {
        let (shared, submitted, _fail, pending) = async_flush_shared(50);
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let first = vec![(b"early".to_vec(), Some(b"v".to_vec()), v(5_000, 1))];
        let late = vec![(b"late".to_vec(), Some(b"v".to_vec()), v(5_001, 1))];
        let mut cx = Context::from_waker(Waker::noop());
        let mut a = Box::pin(serve_batch_apply(
            Some(&shared),
            DEFAULT_ZONE,
            store.as_ref(),
            dev.as_ref(),
            &gate,
            &first,
            true,
            ApplyMode::Legacy,
        ));
        assert!(a.as_mut().poll(&mut cx).is_pending());
        assert!(a.as_mut().poll(&mut cx).is_pending());
        assert_eq!(submitted.load(Ordering::Relaxed), 1, "leader submitted its flush");
        assert_eq!(shared.device_waits.get(), 1, "server_poll must spin while a flush is pending");
        let mut b = Box::pin(serve_batch_apply(
            Some(&shared),
            DEFAULT_ZONE,
            store.as_ref(),
            dev.as_ref(),
            &gate,
            &late,
            true,
            ApplyMode::Legacy,
        ));
        assert!(b.as_mut().poll(&mut cx).is_pending(), "a late joiner applies while the flush is in flight");
        assert!(store.borrow().get(b"late").unwrap().is_some());
        let mut a_done = None;
        for _ in 0..200 {
            if let Poll::Ready(r) = a.as_mut().poll(&mut cx) {
                a_done = Some(r);
                break;
            }
            assert!(b.as_mut().poll(&mut cx).is_pending(), "late joiner released by an earlier flush");
        }
        assert!(matches!(a_done, Some(Response::Batch(_))));
        let mut b_done = None;
        for _ in 0..200 {
            dev.poll_writes();
            if let Poll::Ready(r) = b.as_mut().poll(&mut cx) {
                b_done = Some(r);
                break;
            }
        }
        assert!(matches!(b_done, Some(Response::Batch(_))));
        assert_eq!(submitted.load(Ordering::Relaxed), 2, "the late joiner needed its own flush");
        assert_eq!(shared.sync_groups.borrow()[&DEFAULT_ZONE].commits, 2);
        assert_eq!(shared.device_waits.get(), 0);
        assert!(pending.lock().unwrap().is_empty());
        assert!(!shared.sync_groups.borrow()[&DEFAULT_ZONE].running);
    }

    #[test]
    fn async_group_commit_shares_one_flush_and_reports_failure_to_all() {
        let (shared, submitted, fail, _pending) = async_flush_shared(3);
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let batches: Vec<Vec<crate::wire::OwnedWrite>> = (0..6u64)
            .map(|i| vec![(format!("async-{i}").into_bytes(), Some(b"v".to_vec()), v(6_000 + i, 1))])
            .collect();
        let run = |shared: &ServerShared<AsyncFlushDevice>| {
            let futures: Vec<_> = batches
                .iter()
                .map(|writes| {
                    serve_batch_apply(
                        Some(shared),
                        DEFAULT_ZONE,
                        store.as_ref(),
                        dev.as_ref(),
                        &gate,
                        writes,
                        true,
                        ApplyMode::Legacy,
                    )
                })
                .collect();
            vals_rt::block_on(vals_rt::join_all(futures), reactor::poll_io)
        };
        let ok = run(&shared);
        assert!(ok.iter().all(|r| matches!(r, Response::Batch(_))));
        assert_eq!(submitted.load(Ordering::Relaxed), 1, "joined batches share one flush");
        fail.store(true, Ordering::Relaxed);
        let failed = run(&shared);
        assert!(failed
            .iter()
            .all(|r| matches!(r, Response::Err(e) if e.contains("device flush failed"))));
        assert_eq!(shared.device_waits.get(), 0);
    }

    #[test]
    fn shards_in_one_flush_domain_share_device_flushes() {
        let histograms = crate::observe::histograms();
        let device_before = histograms.domain_flush_device_us.snapshot().1;
        let publish_before = histograms.domain_flush_publish_us.snapshot().1;
        let busy_before = crate::observe::counters().domain_flush_busy_us_total.get();
        let depth_before = crate::observe::domain_flush_vlog_writes_inflight().snapshot().0;
        let bytes_before = vals_store::vlog::vlog_write_bytes();
        let synced_before = crate::observe::counters().synced_writes.get();
        let key = 0x0297_d0d0;
        let shards: Vec<_> = (0..6)
            .map(|i| {
                let mut dev = AsyncFlushDevice::new(20);
                dev.domain = Some(key);
                let submitted = dev.submitted.clone();
                (domain_shared(dev).with_shard(i, 6), submitted)
            })
            .collect();
        let shard_before: Vec<_> = shards.iter().map(|(shared, _)| {
            (shared.shard_write_counters.writes.get(), shared.shard_write_counters.ops.get())
        }).collect();
        let stores: Vec<_> = shards
            .iter()
            .map(|(shared, _)| shared.zone_store(DEFAULT_ZONE).unwrap())
            .collect();
        let devs: Vec<_> = stores.iter().map(|s| s.borrow().device_handle()).collect();
        let writes: Vec<Vec<crate::wire::OwnedWrite>> = (0..6u64)
            .map(|i| vec![(format!("d{i}").into_bytes(), Some(b"v".to_vec()), v(7_000 + i, 1))])
            .collect();
        let gate = ReadGate::new();
        let mut futs: Vec<_> = (0..6)
            .map(|i| {
                Box::pin(serve_batch_apply(
                    Some(&shards[i].0),
                    DEFAULT_ZONE,
                    stores[i].as_ref(),
                    devs[i].as_ref(),
                    &gate,
                    &writes[i],
                    true,
                    ApplyMode::Legacy,
                ))
            })
            .collect();
        let mut cx = Context::from_waker(Waker::noop());
        let mut done: Vec<Option<Response>> = (0..6).map(|_| None).collect();
        for _ in 0..500 {
            for (i, f) in futs.iter_mut().enumerate() {
                if done[i].is_none() {
                    if let Poll::Ready(r) = f.as_mut().poll(&mut cx) {
                        done[i] = Some(r);
                    }
                }
            }
            for (shared, _) in &shards {
                shared.publish_domain_flushes();
            }
        }
        assert!(done.iter().all(|r| matches!(r, Some(Response::Batch(_)))));
        let flushes: usize = shards.iter().map(|(_, n)| n.load(Ordering::Relaxed)).sum();
        assert_eq!(
            flushes, 2,
            "the first shard's flush was claimed before the others prepared; \
             every other shard shares the second"
        );
        for (shared, _) in &shards {
            assert!(shared.domain_flushes.borrow().is_empty());
            assert_eq!(shared.device_waits.get(), 0);
        }
        assert!(histograms.domain_flush_device_us.snapshot().1 >= device_before + 2);
        assert!(histograms.domain_flush_publish_us.snapshot().1 >= publish_before + 2);
        assert!(crate::observe::counters().domain_flush_busy_us_total.get() > busy_before);
        assert!(crate::observe::domain_flush_vlog_writes_inflight().snapshot().0 >= depth_before + 2);
        assert!(vals_store::vlog::vlog_write_bytes() > bytes_before);
        assert!(crate::observe::counters().synced_writes.get() >= synced_before + 6);
        for ((shared, _), (writes, ops)) in shards.iter().zip(shard_before) {
            assert!(shared.shard_write_counters.writes.get() >= writes + 1);
            assert!(shared.shard_write_counters.ops.get() >= ops + 1);
        }
    }

    struct TestShard {
        shared: ServerShared<AsyncFlushDevice>,
        store: Rc<RefCell<VersionedStore<AsyncFlushDevice>>>,
        dev: Arc<AsyncFlushDevice>,
        gate: ReadGate,
    }

    impl TestShard {
        fn new(dev: AsyncFlushDevice) -> Self {
            let shared = domain_shared(dev);
            let store = shared.zone_store(DEFAULT_ZONE).unwrap();
            let dev = store.borrow().device_handle();
            TestShard {
                shared,
                store,
                dev,
                gate: ReadGate::new(),
            }
        }

        fn commit<'a>(
            &'a self,
            writes: &'a [crate::wire::OwnedWrite],
        ) -> Pin<Box<impl Future<Output = Response> + 'a>> {
            Box::pin(serve_batch_apply(
                Some(&self.shared),
                DEFAULT_ZONE,
                self.store.as_ref(),
                self.dev.as_ref(),
                &self.gate,
                writes,
                true,
                ApplyMode::Legacy,
            ))
        }
    }

    fn one_write(key: &str, hlc: u64) -> Vec<crate::wire::OwnedWrite> {
        vec![(key.as_bytes().to_vec(), Some(b"v".to_vec()), v(hlc, 1))]
    }

    fn drive<F: Future>(mut fut: Pin<&mut F>, limit: Duration) -> Option<F::Output> {
        let mut cx = Context::from_waker(Waker::noop());
        let started = std::time::Instant::now();
        loop {
            if let Poll::Ready(r) = fut.as_mut().poll(&mut cx) {
                return Some(r);
            }
            if started.elapsed() > limit {
                return None;
            }
            std::thread::yield_now();
        }
    }

    fn poll_until<F: Future>(mut fut: Pin<&mut F>, mut reached: impl FnMut() -> bool) {
        let mut cx = Context::from_waker(Waker::noop());
        for _ in 0..1_000 {
            if reached() {
                return;
            }
            assert!(fut.as_mut().poll(&mut cx).is_pending(), "finished before the checkpoint");
        }
        panic!("checkpoint never reached");
    }

    const NO_STALL: Option<Duration> = Some(Duration::from_secs(3_600));

    #[test]
    fn cancelled_leader_mid_flush_hands_off_without_failing_waiters() {
        struct CountWakes(AtomicUsize);
        impl std::task::Wake for CountWakes {
            fn wake(self: Arc<Self>) {
                self.wake_by_ref();
            }
            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let shard = TestShard::new(AsyncFlushDevice::new(4));
        let (wa, wb, wc, wd) = (
            one_write("cancel-a", 8_000),
            one_write("cancel-b", 8_001),
            one_write("cancel-c", 8_002),
            one_write("cancel-d", 8_003),
        );
        let mut a = shard.commit(&wa);
        let mut b = shard.commit(&wb);
        let mut c = shard.commit(&wc);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(a.as_mut().poll(&mut cx).is_pending());
        let wakes = Arc::new(CountWakes(AtomicUsize::new(0)));
        let waker = Waker::from(wakes.clone());
        let mut wait_cx = Context::from_waker(&waker);
        assert!(b.as_mut().poll(&mut wait_cx).is_pending());
        assert!(c.as_mut().poll(&mut wait_cx).is_pending());
        poll_until(a.as_mut(), || shard.dev.submitted.load(Ordering::Relaxed) == 1);
        assert_eq!(shard.shared.sync_groups.borrow()[&DEFAULT_ZONE].waiters.len(), 0);
        drop(a);
        let cancelled_at = shard.dev.oracle.tick();
        assert!(wakes.0.load(Ordering::Relaxed) >= 2, "both waiters must be woken at hand-off");
        assert!(!shard.shared.sync_groups.borrow()[&DEFAULT_ZONE].running);
        assert_eq!(shard.shared.domain_flushes.borrow().len(), 1, "the orphaned flush stays tracked");
        assert_eq!(shard.shared.device_waits.get(), 0);
        let mut d = shard.commit(&wd);
        assert!(d.as_mut().poll(&mut cx).is_pending(), "new arrival joins the hand-off");
        let mut futures = [b.as_mut(), c.as_mut(), d.as_mut()];
        let mut acks = [None; 3];
        let started = std::time::Instant::now();
        while acks.iter().any(Option::is_none) && started.elapsed() < Duration::from_secs(5) {
            for (f, ack) in futures.iter_mut().zip(acks.iter_mut()) {
                if ack.is_none() {
                    if let Poll::Ready(got) = f.as_mut().poll(&mut cx) {
                        assert!(matches!(got, Response::Batch(ref results) if results.len() == 1),
                            "each waiter gets one batch result: {got:?}");
                        *ack = Some(shard.dev.oracle.tick());
                    }
                }
            }
        }
        assert!(acks.iter().all(Option::is_some), "no waiter may be stranded: {acks:?}");
        let last_write = shard.dev.last_write.load(Ordering::SeqCst).max(cancelled_at);
        for ack in acks.into_iter().flatten() {
            assert!(shard.dev.oracle.covered(last_write, ack));
        }
        assert_eq!(shard.dev.submitted.load(Ordering::Relaxed), 2, "the successor flushes again");
        assert_eq!(shard.shared.sync_groups.borrow()[&DEFAULT_ZONE].commits, 1);
        assert!(!shard.shared.sync_groups.borrow()[&DEFAULT_ZONE].running);
        assert_eq!(shard.shared.device_waits.get(), 0);
        shard.shared.publish_domain_flushes();
        assert!(shard.shared.domain_flushes.borrow().is_empty());
        assert!(shard.dev.pending.lock().unwrap().is_empty());
    }

    #[test]
    fn domain_flush_failure_reaches_other_shard_waiter() {
        let key = 0x0305_0001;
        let domain = FlushDomain::get(key);
        let shard_a = TestShard::new(AsyncFlushDevice::in_domain(8, key));
        let shard_b = TestShard::new(AsyncFlushDevice::in_domain(8, key));
        let (wa, wb, wc) = (
            one_write("fail-a", 8_100),
            one_write("fail-b", 8_101),
            one_write("fail-c", 8_102),
        );
        let mut a = shard_a.commit(&wa);
        poll_until(a.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 1);
        let mut b = shard_b.commit(&wb);
        poll_until(b.as_mut(), || shard_b.shared.device_waits.get() == 1);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(b.as_mut().poll(&mut cx).is_pending(), "B waits for a flush numbered after A's first");
        assert_eq!(domain.claimed.load(Ordering::SeqCst), 1);
        assert_eq!(shard_b.dev.submitted.load(Ordering::Relaxed), 0);
        assert!(matches!(drive(a.as_mut(), Duration::from_secs(5)), Some(Response::Batch(_))));
        let mut c = shard_a.commit(&wc);
        poll_until(c.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 2);
        assert_eq!(shard_a.dev.submitted.load(Ordering::Relaxed), 2);
        shard_a.dev.fail.store(true, Ordering::Relaxed);
        for _ in 0..10 {
            shard_a.dev.poll_writes();
        }
        shard_a.shared.publish_domain_flushes();
        assert_eq!(domain.failed.load(Ordering::SeqCst), 2);
        let got = drive(b.as_mut(), Duration::from_secs(5));
        assert!(matches!(got, Some(Response::Err(ref e)) if e.contains("device flush failed")),
            "other shard must observe the failed flush: {got:?}");
        let got = drive(c.as_mut(), Duration::from_secs(5));
        assert!(matches!(got, Some(Response::Err(ref e)) if e.contains("device flush failed")));
        assert_eq!(domain.completed.load(Ordering::SeqCst), 1);
        assert!(!domain.inflight.load(Ordering::SeqCst));
        assert_eq!(shard_b.dev.blocking.load(Ordering::Relaxed), 0);
        assert_eq!(shard_b.dev.submitted.load(Ordering::Relaxed), 0);
        assert!(shard_a.shared.domain_flushes.borrow().is_empty());
        assert_eq!(shard_a.shared.device_waits.get(), 0);
        assert_eq!(shard_b.shared.device_waits.get(), 0);
    }

    #[test]
    fn domain_flush_acks_only_writes_completed_before_a_covering_flush_was_submitted() {
        let key = 0x0303_0001;
        let oracle = Arc::new(FlushOracle::default());
        let mut dev_a = AsyncFlushDevice::in_domain(3, key);
        dev_a.oracle = oracle.clone();
        dev_a.allow_blocking = false;
        let (hold, holding) = (dev_a.hold.clone(), dev_a.holding.clone());
        let mut dev_b = AsyncFlushDevice::in_domain(3, key);
        dev_b.oracle = oracle.clone();
        dev_b.allow_blocking = false;
        let domain = FlushDomain::get(key);
        let a_thread = {
            let hold = hold.clone();
            let oracle = oracle.clone();
            std::thread::spawn(move || {
                set_domain_flush_stall(NO_STALL);
                let shard = TestShard::new(dev_a);
                let writes = one_write("oracle-a", 9_000);
                hold.store(true, Ordering::SeqCst);
                let mut a = shard.commit(&writes);
                let got = drive(a.as_mut(), Duration::from_secs(20));
                let acked = oracle.tick();
                let ok = matches!(got, Some(Response::Batch(_)));
                (ok, shard.dev.last_write.load(Ordering::SeqCst), acked, shard.dev.submitted.load(Ordering::Relaxed))
            })
        };
        let started = std::time::Instant::now();
        while !holding.load(Ordering::SeqCst) {
            assert!(started.elapsed() < Duration::from_secs(10), "shard A never wrote");
            std::thread::sleep(Duration::from_millis(1));
        }
        set_domain_flush_stall(NO_STALL);
        let shard_b = TestShard::new(dev_b);
        let writes = one_write("oracle-b", 9_001);
        let mut b = shard_b.commit(&writes);
        poll_until(b.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 1);
        assert_eq!(shard_b.dev.submitted.load(Ordering::Relaxed), 1, "B claimed and submitted flush 1");
        hold.store(false, Ordering::SeqCst);
        let got = drive(b.as_mut(), Duration::from_secs(20));
        let b_acked = oracle.tick();
        assert!(matches!(got, Some(Response::Batch(_))));
        let b_last = shard_b.dev.last_write.load(Ordering::SeqCst);
        let (a_ok, a_last, a_acked, a_submitted) = a_thread.join().unwrap();
        set_domain_flush_stall(None);
        assert!(a_ok);
        let flushes = oracle.flushes.lock().unwrap().clone();
        assert_eq!(flushes.len(), 2, "{flushes:?}");
        assert!(flushes.iter().all(|&(_, _, ok)| ok));
        let first = flushes.iter().map(|&(s, _, _)| s).min().unwrap();
        assert!(a_last > first, "A's held write completed after flush 1 was submitted");
        assert_eq!(a_submitted, 1, "A needed a flush of its own, not flush 1");
        assert!(oracle.covered(b_last, b_acked));
        assert!(oracle.covered(a_last, a_acked));
        assert_eq!(domain.completed.load(Ordering::SeqCst), 2);
        assert!(!domain.inflight.load(Ordering::SeqCst));
    }

    #[test]
    fn stalled_domain_owner_falls_back_then_recovers_and_its_successor_releads() {
        let key = 0x0303_0002;
        let domain = FlushDomain::get(key);
        let shard_a = TestShard::new(AsyncFlushDevice::in_domain(2, key));
        let shard_b = TestShard::new(AsyncFlushDevice::in_domain(2, key));
        let (w1, w2) = (one_write("stall-b1", 10_000), one_write("stall-b2", 10_001));
        let mut b1 = shard_b.commit(&w1);
        let mut b2 = shard_b.commit(&w2);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(b1.as_mut().poll(&mut cx).is_pending());
        assert!(b2.as_mut().poll(&mut cx).is_pending());
        poll_until(b1.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 1);
        drop(b1);
        assert!(domain.inflight.load(Ordering::SeqCst));
        assert_eq!(shard_b.shared.domain_flushes.borrow().len(), 1);

        set_domain_flush_stall(Some(Duration::from_millis(30)));
        let stalls = crate::observe::counters().domain_flush_stalls.get();
        let wa = one_write("stall-a1", 10_002);
        let mut a1 = shard_a.commit(&wa);
        let got = drive(a1.as_mut(), Duration::from_secs(10));
        assert!(matches!(got, Some(Response::Batch(_))));
        assert!(crate::observe::counters().domain_flush_stalls.get() > stalls);
        assert_eq!(shard_a.dev.blocking.load(Ordering::Relaxed), 1, "A fell back to its own flush");
        assert_eq!(shard_a.dev.submitted.load(Ordering::Relaxed), 0);
        assert!(domain.inflight.load(Ordering::SeqCst), "B's flush is still unpublished");

        set_domain_flush_stall(NO_STALL);
        let got = drive(b2.as_mut(), Duration::from_secs(5));
        assert!(matches!(got, Some(Response::Batch(_))), "successor must re-lead: {got:?}");
        assert_eq!(shard_b.dev.submitted.load(Ordering::Relaxed), 2);
        assert_eq!(shard_b.dev.blocking.load(Ordering::Relaxed), 0);
        assert!(shard_b.shared.domain_flushes.borrow().is_empty());
        assert!(!domain.inflight.load(Ordering::SeqCst));
        assert_eq!(domain.completed.load(Ordering::SeqCst), 2);

        let wa = one_write("stall-a2", 10_003);
        let mut a2 = shard_a.commit(&wa);
        let got = drive(a2.as_mut(), Duration::from_secs(5));
        set_domain_flush_stall(None);
        assert!(matches!(got, Some(Response::Batch(_))));
        assert_eq!(shard_a.dev.blocking.load(Ordering::Relaxed), 1, "no second fallback");
        assert_eq!(shard_a.dev.submitted.load(Ordering::Relaxed), 1);
        assert_eq!(domain.completed.load(Ordering::SeqCst), 3);
        assert!(!domain.inflight.load(Ordering::SeqCst));
    }

    fn abandoned_domain_claim_is_reclaimed(key: usize, unsubmittable: bool) {
        set_domain_flush_stall(NO_STALL);
        let domain = FlushDomain::get(key);
        let shard_c = TestShard::new(AsyncFlushDevice::in_domain(2, key));
        let shard_a = TestShard::new(AsyncFlushDevice::in_domain(2, key));
        let mut dev_b = AsyncFlushDevice::in_domain(2, key);
        dev_b.no_async = unsubmittable;
        let shard_b = TestShard::new(dev_b);
        let (wc, wa, wb) = (
            one_write("abandon-c", 11_000),
            one_write("abandon-a", 11_001),
            one_write("abandon-b", 11_002),
        );
        let mut c = shard_c.commit(&wc);
        poll_until(c.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 1);
        let mut a = shard_a.commit(&wa);
        poll_until(a.as_mut(), || shard_a.shared.device_waits.get() == 1);
        assert_eq!(domain.claimed.load(Ordering::SeqCst), 1, "A waits on C's flush");
        assert!(matches!(drive(c.as_mut(), Duration::from_secs(5)), Some(Response::Batch(_))));
        assert!(!domain.inflight.load(Ordering::SeqCst));
        let mut b = shard_b.commit(&wb);
        if unsubmittable {
            let got = drive(b.as_mut(), Duration::from_secs(5));
            assert!(matches!(got, Some(Response::Batch(_))));
            assert_eq!(shard_b.dev.blocking.load(Ordering::Relaxed), 1);
            drop(b);
        } else {
            poll_until(b.as_mut(), || domain.claimed.load(Ordering::SeqCst) == 2);
            assert!(domain.inflight.load(Ordering::SeqCst));
            drop(b);
        }
        assert_eq!(domain.claimed.load(Ordering::SeqCst), 2, "B claimed the flush A needs");
        drop(shard_b);
        assert!(!domain.inflight.load(Ordering::SeqCst));
        assert_eq!(domain.completed.load(Ordering::SeqCst), 1, "abandoned claim is never published");
        let got = drive(a.as_mut(), Duration::from_secs(5));
        set_domain_flush_stall(None);
        assert!(matches!(got, Some(Response::Batch(_))), "A must re-claim, not stall: {got:?}");
        assert_eq!(shard_a.dev.blocking.load(Ordering::Relaxed), 0);
        assert_eq!(shard_a.dev.submitted.load(Ordering::Relaxed), 1);
        assert_eq!(domain.claimed.load(Ordering::SeqCst), 3);
        assert_eq!(domain.completed.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn dropped_domain_flush_owner_releases_inflight() {
        abandoned_domain_claim_is_reclaimed(0x0303_0003, false);
    }

    #[test]
    fn unsubmittable_domain_claim_does_not_stall_other_shards() {
        abandoned_domain_claim_is_reclaimed(0x0303_0004, true);
    }

    #[test]
    fn finalize_shutdown_drains_cancelled_device_flushes() {
        for domain in [Some(0x0303_0005), None] {
            let mut dev = AsyncFlushDevice::new(50);
            dev.domain = domain;
            dev.allow_blocking = true;
            let shard = TestShard::new(dev);
            let writes = one_write("shutdown", 12_000);
            let mut a = shard.commit(&writes);
            poll_until(a.as_mut(), || shard.dev.submitted.load(Ordering::Relaxed) == 1);
            drop(a);
            assert_eq!(shard.dev.pending.lock().unwrap().len(), 1);
            assert_eq!(shard.shared.domain_flushes.borrow().len(), 1);
            shard.shared.finalize_shutdown();
            assert!(shard.dev.pending.lock().unwrap().is_empty(), "no flush in flight at close");
            assert!(shard.shared.domain_flushes.borrow().is_empty());
            if let Some(key) = domain {
                let domain = FlushDomain::get(key);
                assert!(!domain.inflight.load(Ordering::SeqCst));
                assert_eq!(domain.completed.load(Ordering::SeqCst), 1);
            }
            drop(shard);
        }
    }

    #[test]
    fn finalize_shutdown_times_out_with_unfinished_domain_flush_tracked() {
        let key = 0x0305_0002;
        let domain = FlushDomain::get(key);
        let shard = TestShard::new(AsyncFlushDevice::in_domain(u32::MAX, key));
        let writes = one_write("shutdown-timeout", 12_100);
        let mut a = shard.commit(&writes);
        poll_until(a.as_mut(), || shard.dev.submitted.load(Ordering::Relaxed) == 1);
        drop(a);
        assert_eq!(shard.shared.domain_flushes.borrow().len(), 1);
        super::shared::set_shutdown_flush_drain(Some(Duration::from_millis(50)));
        let started = std::time::Instant::now();
        shard.shared.finalize_shutdown();
        let elapsed = started.elapsed();
        super::shared::set_shutdown_flush_drain(None);
        assert!(elapsed < Duration::from_secs(2), "shutdown drain did not time out: {elapsed:?}");
        assert_eq!(shard.dev.pending.lock().unwrap().len(), 1);
        assert_eq!(shard.shared.domain_flushes.borrow().len(), 1);
        assert!(domain.inflight.load(Ordering::SeqCst));
        assert_eq!(domain.completed.load(Ordering::SeqCst), 0);
        assert_eq!(domain.failed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn concurrent_durable_batches_share_one_group_commit() {
        let shared = ServerShared::single_zone(build_store());
        let store = shared.zone_store(DEFAULT_ZONE).unwrap();
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let batches: Vec<Vec<crate::wire::OwnedWrite>> = (0..8u64)
            .map(|i| {
                vec![(
                    format!("batch-group-{i}").into_bytes(),
                    Some(b"v".to_vec()),
                    v(2_000 + i, 1),
                )]
            })
            .collect();
        let before = shared
            .sync_groups
            .borrow()
            .get(&DEFAULT_ZONE)
            .map_or(0, |group| group.commits);
        let futures: Vec<_> = batches
            .iter()
            .map(|writes| {
                serve_batch_apply(
                    Some(&shared),
                    DEFAULT_ZONE,
                    store.as_ref(),
                    dev.as_ref(),
                    &gate,
                    writes,
                    true,
                    ApplyMode::Legacy,
                )
            })
            .collect();
        let responses = vals_rt::block_on(vals_rt::join_all(futures), reactor::poll_io);
        assert!(responses.iter().all(|r| matches!(r, Response::Batch(_))));
        let after = shared.sync_groups.borrow()[&DEFAULT_ZONE].commits;
        assert_eq!(
            after - before,
            1,
            "all batches installed before the barrier must share one sync"
        );
    }

    #[test]
    fn ram_budget_rejects_client_writes_but_keeps_reads_available() {
        let shared = ServerShared::single_zone(build_store());
        let before = crate::observe::counters().ram_busy_rejections.get();
        let override_guard = crate::ram::override_for_test(2, 1);

        let apply = OwnedRequest::Apply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"ram-apply".to_vec(),
            value: Some(b"v".to_vec()),
            version: v(100, 1),
        };
        assert!(matches!(serve_request(&shared, apply), Response::Busy));

        let batch = OwnedRequest::BatchApply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            sync: false,
            writes: vec![(b"ram-batch".to_vec(), Some(b"v".to_vec()), v(101, 1))],
        };
        assert!(matches!(serve_request(&shared, batch), Response::Busy));

        let read = OwnedRequest::Read {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"a".to_vec(),
        };
        assert!(matches!(serve_request(&shared, read), Response::Read(Some(_))));
        assert_eq!(
            crate::observe::counters().ram_busy_rejections.get(),
            before + 2
        );

        drop(override_guard);
        let apply = OwnedRequest::Apply {
            epoch: 0,
            zone_id: DEFAULT_ZONE,
            key: b"ram-restored".to_vec(),
            value: Some(b"v".to_vec()),
            version: v(102, 1),
        };
        assert!(matches!(serve_request(&shared, apply), Response::Apply(_)));
    }

    #[test]
    fn flush_build_accounting_tracks_each_zone_once() {
        let shared = ServerShared::single_zone(build_store());
        shared.begin_flush_build(DEFAULT_ZONE, 100);
        assert_eq!(shared.flush_builds.borrow().get(&DEFAULT_ZONE), Some(&100));
        shared.begin_flush_build(DEFAULT_ZONE, 40);
        assert_eq!(shared.flush_builds.borrow().get(&DEFAULT_ZONE), Some(&40));
        shared.finish_flush_build(DEFAULT_ZONE);
        assert!(shared.flush_builds.borrow().is_empty());
    }

    #[test]
    fn staged_batch_answers_busy_at_the_reserve_floor() {
        let dev = MemBlockDevice::new(256, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Unsorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store.engine_mut().set_maint_reserve_enabled(true);
        let store = RefCell::new(store);

        let mut hit_busy = false;
        for i in 0..200_000u32 {
            let writes = vec![(
                format!("k{:04}", i % 64).into_bytes(),
                Some(vec![7u8; 200]),
                v(i as u64 + 1, 1),
            )];
            match drive_batch(&store, &writes, false) {
                Response::Batch(_) => {}
                Response::Busy => {
                    hit_busy = true;
                    break;
                }
                other => panic!("reserve-floor staged batch must be Busy, got {other:?}"),
            }
        }
        assert!(
            hit_busy,
            "an armed, churned store must eventually refuse the staged batch with Busy"
        );
    }

    #[test]
    fn capped_unplannable_multi_run_is_terminal_not_busy() {
        if crate::testutil::isolated_env_test(&[("VALS_VLOG_GC", "0")]) { return; }
        let dev = MemBlockDevice::new(256, 256);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut version = 1u64;
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store.engine_mut().set_maint_reserve_enabled(true);
        let store = RefCell::new(store);

        for round in 0..16u32 {
            let writes_this_run = if round == 0 { 40 } else { 120 };
            for i in 0..writes_this_run {
                let key = if round == 0 || i < 20 {
                    b"hot-key".to_vec()
                } else {
                    format!("round-{round:02}-key-{i:04}-{}", "k".repeat(200)).into_bytes()
                };
                let writes = vec![(
                    key,
                    Some(vec![round as u8; 32]),
                    v(version, 1),
                )];
                version += 1;
                match drive_batch(&store, &writes, false) {
                    Response::Batch(_) => {}
                    Response::Busy => break,
                    Response::Err(message) if message.starts_with("zone region exhausted:") => {
                        break;
                    }
                    other => panic!("unexpected capped-region response: {other:?}"),
                }
            }
            if !store.borrow().engine().region_exhausted() {
                store.borrow_mut().flush().unwrap();
            }
            if store.borrow().engine().region_exhausted() {
                break;
            }
        }

        assert!(store.borrow().run_count() > 1);
        assert!(
            store.borrow().engine().region_exhausted(),
            "capped state: runs={} free={} at_floor={}",
            store.borrow().run_count(),
            store.borrow().engine().free_segments(),
            store.borrow().engine().at_maintenance_floor()
        );
        let terminal = vec![(b"terminal".to_vec(), Some(vec![9u8; 800]), v(version, 1))];
        assert!(matches!(
            drive_batch(&store, &terminal, false),
            Response::Err(message) if message.starts_with("zone region exhausted:")
        ));
    }

    #[test]
    fn full_zone_backpressures_and_stays_isolated() {
        let dev = MemBlockDevice::new(256, 1024);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        store.engine_mut().set_maint_reserve_enabled(true);
        let store = RefCell::new(store);

        let mut hit_busy = false;
        for i in 0..200_000u32 {
            let writes = vec![(
                format!("k{i:08}").into_bytes(),
                Some(vec![7u8; 3900]),
                v(i as u64 + 1, 1),
            )];
            match drive_batch(&store, &writes, false) {
                Response::Batch(_) => {}
                Response::Busy => {
                    hit_busy = true;
                    break;
                }
                other => panic!("pending-flush capacity must remain Busy, got {other:?}"),
            }
        }
        assert!(hit_busy);
        store
            .borrow_mut()
            .flush()
            .expect("a full zone must still be able to flush its memtable (no wedge)");
        assert!(store.borrow().engine().region_exhausted());
        let terminal = vec![(b"terminal".to_vec(), Some(vec![8u8; 3900]), v(999_999, 1))];
        assert!(matches!(
            drive_batch(&store, &terminal, false),
            Response::Err(message) if message.starts_with("zone region exhausted:")
        ));

        let dev2 = MemBlockDevice::new(256, 8192);
        let engine2 = Engine::create_prewiped(
            dev2,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store2 = VersionedStore::new(engine2, 2).unwrap();
        store2.engine_mut().set_maint_reserve_enabled(true);
        store2
            .apply(b"live", Some(b"ok"), v(1, 2))
            .expect("second zone still accepts writes while the first is full");
        assert_eq!(
            store2.get(b"live").unwrap().as_deref(),
            Some(b"ok" as &[u8]),
            "second zone still serves reads while the first is full"
        );

        let dev3 = MemBlockDevice::new(256, 256);
        let engine3 = Engine::create_prewiped(
            dev3,
            Config {
                seg_blocks: 4,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let store3 = RefCell::new(VersionedStore::new(engine3, 3).unwrap());
        let mut got_busy = false;
        for i in 0..200_000u32 {
            let writes = vec![(
                format!("f{i:08}").into_bytes(),
                Some(vec![7u8; 200]),
                v(i as u64 + 1, 3),
            )];
            match drive_batch(&store3, &writes, false) {
                Response::Batch(_) => {}
                Response::Busy => {
                    got_busy = true;
                    break;
                }
                other => panic!(
                    "genuine DeviceFull must map to retryable Busy at the serve path, got {other:?}"
                ),
            }
        }
        assert!(
            got_busy,
            "a disarmed region fills to genuine DeviceFull, which must surface as Busy"
        );
    }

