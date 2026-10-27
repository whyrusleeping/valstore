    use super::*;
    use crate::hlc::Version;
    use crate::integrity::{reset_corrupt_read_log, ReadSite, CORRUPT_READ_LINES};
    use vals_spdk::MemBlockDevice;
    use vals_store::vlog::ValuePtr;
    use vals_store::{Config, Engine, Ordering as SOrdering};

    const SITE: ReadSite = ReadSite { zone: 3, shard: 5 };

    fn v(hlc: u64) -> Version {
        Version { hlc, node: 1 }
    }

    fn memtable_store(keys: &[&[u8]]) -> RefCell<VersionedStore<MemBlockDevice>> {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 8192),
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        for key in keys {
            store.apply(key, Some(b"value-0339"), v(1)).unwrap();
        }
        RefCell::new(store)
    }

    fn ptr_of(store: &RefCell<VersionedStore<MemBlockDevice>>, key: &[u8]) -> ValuePtr {
        store.borrow().mutable_entry(key).unwrap().ptr
    }

    fn set_ptr(store: &RefCell<VersionedStore<MemBlockDevice>>, key: &[u8], ptr: ValuePtr) {
        store.borrow_mut().engine_mut().set_mutable_ptr_for_test(key, ptr);
    }

    fn bit11_len(ptr: ValuePtr) -> ValuePtr {
        ValuePtr { len: ptr.len ^ 0x800, ..ptr }
    }

    fn corrupt_reads(kind: &str) -> u64 {
        crate::observe::corrupt_read_metrics()
            .into_iter()
            .find(|(k, _)| *k == kind)
            .unwrap()
            .1
    }

    fn get(store: &RefCell<VersionedStore<MemBlockDevice>>, key: &[u8]) -> Response {
        let dev = store.borrow().device_handle();
        let out = ServerOut::default();
        vals_rt::block_on(
            serve_read_into(store, &*dev, &ReadGate::new(), key, &out, 9, SITE),
            || {},
        );
        let buf = out.buf.borrow();
        wire::decode_response(&buf[12..]).unwrap()
    }

    fn scan(store: &RefCell<VersionedStore<MemBlockDevice>>) -> Response {
        let dev = store.borrow().device_handle();
        vals_rt::block_on(
            serve_scan(store, &*dev, &ReadGate::new(), None, None, 0, true, SITE),
            || {},
        )
    }

    #[track_caller]
    fn expect_failure(resp: Response, reason: &str, kind: &str, before: u64, line: &str) {
        match resp {
            Response::Err(msg) => assert!(msg.contains(reason), "{msg}"),
            other => panic!("expected an error reply, got {other:?}"),
        }
        assert!(corrupt_reads(kind) > before, "{kind} counter did not move");
        let lines = CORRUPT_READ_LINES.with(|l| l.borrow().clone());
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains(line), "{} does not contain {line}", lines[0]);
        assert!(lines[0].contains(reason), "{}", lines[0]);
        reset_corrupt_read_log();
    }

    #[test]
    fn get_fails_bad_len_wrong_record_and_stale_version_with_counter_and_log() {
        reset_corrupt_read_log();
        let store = memtable_store(&[b"key-a", b"key-b"]);
        let (a, b) = (ptr_of(&store, b"key-a"), ptr_of(&store, b"key-b"));
        assert_eq!(a.len, b.len);

        set_ptr(&store, b"key-a", bit11_len(a));
        let before = corrupt_reads("length");
        let line = format!(
            "zone 3 shard 5 get key 6b65792d61 (5 bytes) segment {} offset {} len {}: ",
            a.segment.0,
            a.offset,
            a.len ^ 0x800
        );
        expect_failure(get(&store, b"key-a"), "length mismatch", "length", before, &line);

        set_ptr(&store, b"key-a", b);
        let before = corrupt_reads("key");
        let line = format!(
            "zone 3 shard 5 get key 6b65792d61 (5 bytes) segment {} offset {} len {}: ",
            b.segment.0, b.offset, b.len
        );
        expect_failure(get(&store, b"key-a"), "record key mismatch", "key", before, &line);

        store.borrow_mut().apply(b"key-a", Some(b"value-0340"), v(2)).unwrap();
        set_ptr(&store, b"key-a", a);
        let before = corrupt_reads("version");
        expect_failure(get(&store, b"key-a"), "record version mismatch", "version", before, "get key 6b65792d61");

        assert_eq!(
            get(&store, b"key-b"),
            Response::Read(Some(crate::replica::Stored {
                version: v(1),
                value: Some(b"value-0339".to_vec()),
            }))
        );
        assert!(CORRUPT_READ_LINES.with(|l| l.borrow().is_empty()));
    }

    #[test]
    fn scan_fails_bad_len_and_wrong_record_with_counter_and_log() {
        reset_corrupt_read_log();
        let store = memtable_store(&[b"key-a", b"key-b", b"key-c"]);
        let Response::Scan { entries, .. } = scan(&store) else {
            panic!("healthy scan must succeed");
        };
        assert_eq!(entries.len(), 3);
        let (b, c) = (ptr_of(&store, b"key-b"), ptr_of(&store, b"key-c"));

        set_ptr(&store, b"key-b", bit11_len(b));
        let before = corrupt_reads("length");
        let line = format!(
            "zone 3 shard 5 scan key 6b65792d62 (5 bytes) segment {} offset {} len {}: ",
            b.segment.0,
            b.offset,
            b.len ^ 0x800
        );
        expect_failure(scan(&store), "length mismatch", "length", before, &line);

        set_ptr(&store, b"key-b", c);
        let before = corrupt_reads("key");
        let line = format!(
            "zone 3 shard 5 scan key 6b65792d62 (5 bytes) segment {} offset {} len {}: ",
            c.segment.0, c.offset, c.len
        );
        expect_failure(scan(&store), "record key mismatch", "key", before, &line);
    }

    #[test]
    fn mixed_batch_and_sync_values_fail_a_wrong_record_with_counter_and_log() {
        reset_corrupt_read_log();
        let store = memtable_store(&[b"key-a", b"key-b"]);
        set_ptr(&store, b"key-a", ptr_of(&store, b"key-b"));
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();

        let before = corrupt_reads("key");
        let reads = vec![
            (b"key-a".to_vec(), MixedReadRole::Value),
            (b"key-b".to_vec(), MixedReadRole::Value),
        ];
        let resp = vals_rt::block_on(
            serve_mixed_batch_read(&store, &*dev, &gate, 0, &reads, false, SITE),
            || {},
        );
        let Response::MixedBatchRead { results, .. } = resp else {
            panic!("expected a mixed batch reply, got {resp:?}");
        };
        assert!(
            matches!(&results[0], MixedReadResult::Err(msg) if msg.contains("record key mismatch")),
            "{results:?}"
        );
        assert!(matches!(&results[1], MixedReadResult::Reply(MixedReadReply::Value(Some(_)))));
        assert!(corrupt_reads("key") > before);
        let lines = CORRUPT_READ_LINES.with(|l| l.borrow().clone());
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("zone 3 shard 5 mixed key 6b65792d61"), "{}", lines[0]);
        reset_corrupt_read_log();

        let before = corrupt_reads("key");
        let resp = vals_rt::block_on(
            serve_sync_values(&store, &*dev, &gate, &[b"key-a".to_vec()], SITE),
            || {},
        );
        expect_failure(resp, "record key mismatch", "key", before, "zone 3 shard 5 sync-values key 6b65792d61");
    }

    #[test]
    fn corrupt_read_log_is_rate_limited_per_thread_but_every_read_counts() {
        reset_corrupt_read_log();
        let store = memtable_store(&[b"key-a"]);
        set_ptr(&store, b"key-a", bit11_len(ptr_of(&store, b"key-a")));
        let before = corrupt_reads("length");
        for _ in 0..3 {
            assert!(matches!(get(&store, b"key-a"), Response::Err(_)));
        }
        assert!(corrupt_reads("length") >= before + 3);
        assert_eq!(CORRUPT_READ_LINES.with(|l| l.borrow().len()), 1);
        reset_corrupt_read_log();
    }

    #[test]
    fn merge_rewrite_corruption_counts_logs_once_and_excludes_the_segment() {
        reset_corrupt_read_log();
        let store = memtable_store(&[b"key-a", b"key-b"]);
        let b = ptr_of(&store, b"key-b");
        let err = vals_store::EngineError::CorruptValueRecord {
            key: Box::from(&b"key-a"[..]),
            ptr: b,
            reason: "record key mismatch",
        };
        let before = corrupt_reads("key");
        for _ in 0..2 {
            crate::integrity::corrupt_merge_record(SITE, store.borrow_mut().engine_mut(), &err);
        }
        assert!(corrupt_reads("key") >= before + 2);
        let lines = CORRUPT_READ_LINES.with(|l| l.borrow().clone());
        assert_eq!(lines.len(), 1, "{lines:?}");
        let line = format!(
            "zone 3 shard 5 merge rewrite key 6b65792d61 (5 bytes) segment {} offset {} len {}: record key mismatch; failing the merge unpublished",
            b.segment.0, b.offset, b.len
        );
        assert!(lines[0].contains(&line), "{} does not contain {line}", lines[0]);
        assert_eq!(store.borrow().engine().value_gc_excluded(), 1);

        let before = corrupt_reads("key");
        crate::integrity::corrupt_merge_record(SITE, store.borrow_mut().engine_mut(), &vals_store::EngineError::Cancelled);
        assert_eq!(corrupt_reads("key"), before);
        assert_eq!(store.borrow().engine().value_gc_excluded(), 1);
        reset_corrupt_read_log();
    }
