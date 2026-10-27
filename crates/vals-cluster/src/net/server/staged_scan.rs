    use super::*;
    use std::rc::Rc;
    use vals_spdk::MemBlockDevice;
    use vals_store::{Config, Engine, Ordering as SOrdering};

    fn build_flushed_store() -> VersionedStore<MemBlockDevice> {
        let dev = MemBlockDevice::new(256, 8192);
        let engine = Engine::create_prewiped(
            dev,
            Config {
                seg_blocks: 16,
                reserved_blocks: 0,
                ordering: SOrdering::Sorted,
            },
        );
        let mut store = VersionedStore::new(engine, 1).unwrap();
        for i in 0..120u32 {
            let k = format!("k{i:04}");
            store
                .put(k.as_bytes(), format!("v{i:04}").as_bytes())
                .unwrap();
        }
        store.flush().unwrap();
        assert!(store.run_count() >= 1, "store must have a run");
        for i in 60..140u32 {
            let k = format!("k{i:04}");
            store
                .put(k.as_bytes(), format!("w{i:04}").as_bytes())
                .unwrap();
        }
        store
    }

    fn flushed_store() -> RefCell<VersionedStore<MemBlockDevice>> {
        RefCell::new(build_flushed_store())
    }

    fn drive(
        store: &RefCell<VersionedStore<MemBlockDevice>>,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
    ) -> Vec<(Vec<u8>, crate::replica::Stored)> {
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let resp = vals_rt::block_on(
            serve_scan(store, dev.as_ref(), &gate, lo, hi, max, true, Default::default()),
            reactor::poll_io,
        );
        match resp {
            Response::Scan { entries, .. } => entries,
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    struct HookGuard;
    impl Drop for HookGuard {
        fn drop(&mut self) {
            set_staged_scan_reborrow_hook(None);
        }
    }

    fn assert_entries_eq(
        a: &[(Vec<u8>, crate::replica::Stored)],
        b: &[(Vec<u8>, crate::replica::Stored)],
    ) {
        assert_eq!(a.len(), b.len(), "entry count");
        for (x, y) in a.iter().zip(b) {
            assert_eq!(x.0, y.0, "key");
            assert_eq!(x.1.version, y.1.version, "version for {:?}", x.0);
            assert_eq!(x.1.value, y.1.value, "value for {:?}", x.0);
        }
    }

    #[test]
    fn staged_serve_scan_matches_sync_reference_over_a_flushed_store() {
        let store = flushed_store();
        let (reference, _) = store
            .borrow()
            .scan_page(None, None, usize::MAX, SCAN_REPLY_BUDGET)
            .unwrap();
        let staged = drive(&store, None, None, 0);
        assert_eq!(staged.len(), reference.len(), "entry count");
        for (a, b) in staged.iter().zip(&reference) {
            assert_eq!(a.0, b.0, "key");
            assert_eq!(a.1.version, b.1.version, "version for {:?}", a.0);
            assert_eq!(a.1.value, b.1.value, "value for {:?}", a.0);
        }
        assert!(staged.windows(2).all(|w| w[0].0 < w[1].0), "ascending");
        let k0100 = staged
            .iter()
            .find(|(k, _)| k == b"k0100")
            .expect("k0100 present");
        assert_eq!(
            k0100.1.value.as_deref(),
            Some(&b"w0100"[..]),
            "memtable overwrite must win over the run copy"
        );
    }

    #[test]
    fn multi_scan_keys_only_matches_full_versions_and_point_tombstones() {
        let store = flushed_store();
        store.borrow_mut().delete(b"k0100").unwrap();
        store.borrow_mut().flush().unwrap();
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        let scans = vec![(Some(b"k0050".to_vec()), Some(b"k0130".to_vec()), 0)];
        let call = |keys_only| vals_rt::block_on(scan::serve_multi_scan::<_, false>(&store, dev.as_ref(), &gate, 0, &scans, keys_only, true, Default::default()), reactor::poll_io);
        let Response::MultiScan { pages: full, .. } = call(false) else { panic!("full scan"); };
        let Response::MultiScan { pages: keys, .. } = call(true) else { panic!("key scan"); };
        assert_eq!(keys[0].entries.iter().map(|(k, s)| (k, s.version, s.value.is_some())).collect::<Vec<_>>(),
            full[0].entries.iter().map(|(k, s)| (k, s.version, s.value.is_some())).collect::<Vec<_>>());
        assert!(keys[0].entries.iter().any(|(k, s)| k == b"k0100" && s.value.is_none()));
        assert!(keys[0].entries.iter().all(|(_, s)| s.value.as_ref().is_none_or(Vec::is_empty)));
    }

    #[test]
    fn bounded_staged_scan_matches_sync_reference() {
        let store = flushed_store();
        let staged = drive(&store, Some(b"k0050"), Some(b"k0130"), 0);
        let (fallback, _) = store.borrow()
            .scan_page(Some(b"k0050"), Some(b"k0130"), usize::MAX, SCAN_REPLY_BUDGET)
            .unwrap();

        assert!(!staged.is_empty());
        assert_eq!(staged.len(), fallback.len(), "staged vs fallback count");
        for (a, b) in staged.iter().zip(&fallback) {
            assert_eq!(a.0, b.0);
            assert_eq!(a.1.version, b.1.version);
            assert_eq!(a.1.value, b.1.value);
        }
    }

    #[test]
    fn unrelated_puts_between_staged_borrows_do_not_restart_the_scan() {
        let _hook_guard = HookGuard;

        let store = Rc::new(flushed_store());
        {
            let mut s = store.borrow_mut();
            for i in 140..5000u32 {
                let key = format!("k{i:04}");
                s.put(key.as_bytes(), b"new").unwrap();
            }
        }
        let (lo, hi): (&[u8], &[u8]) = (b"k0050", b"y");
        let reference = {
            let (r, _) = store
                .borrow()
                .scan_page(Some(lo), Some(hi), usize::MAX, SCAN_REPLY_BUDGET)
                .unwrap();
            r
        };

        let fires = Rc::new(std::cell::Cell::new(0u32));
        {
            let store = store.clone();
            let fires = fires.clone();
            set_staged_scan_reborrow_hook(Some(Box::new(move || {
                let n = fires.get();
                fires.set(n + 1);
                store
                    .borrow_mut()
                    .put(format!("zzzz{n:04}").as_bytes(), b"out-of-range")
                    .unwrap();
            })));
        }

        let got = drive(&store, Some(lo), Some(hi), 0);
        set_staged_scan_reborrow_hook(None);

        assert_eq!(fires.get(), 2, "unrelated puts must not restart the merge");
        assert!(!got.is_empty());
        assert_entries_eq(&got, &reference);
    }

    #[test]
    fn put_to_returned_key_between_staged_borrows_is_reflected() {
        let _hook_guard = HookGuard;

        let store = Rc::new(flushed_store());
        store
            .borrow_mut()
            .engine_mut()
            .rebuild_winner_directory()
            .unwrap();
        let acked = Rc::new(RefCell::new(None));
        {
            let store = store.clone();
            let acked = acked.clone();
            set_staged_scan_reborrow_hook(Some(Box::new(move || {
                if acked.borrow().is_none() {
                    let version = store
                        .borrow_mut()
                        .put(b"k0100", b"new0100")
                        .unwrap();
                    *acked.borrow_mut() = Some(version);
                }
            })));
        }

        let got = drive(&store, Some(b"k0050"), Some(b"k0130"), 0);
        set_staged_scan_reborrow_hook(None);

        let acked = acked.borrow().expect("mid-scan put fired");
        let entry = got
            .iter()
            .find(|(key, _)| key == b"k0100")
            .expect("updated key returned");
        assert!(entry.1.version >= acked);
        assert_eq!(entry.1.value.as_deref(), Some(&b"new0100"[..]));
    }

    #[test]
    fn restart_cap_falls_back_to_sync_and_returns_correct_page() {
        let _hook_guard = HookGuard;

        let store = Rc::new(flushed_store());
        let (lo, hi): (&[u8], &[u8]) = (b"k0050", b"k0130");
        let reference = {
            let (r, _) = store
                .borrow()
                .scan_page(Some(lo), Some(hi), usize::MAX, SCAN_REPLY_BUDGET)
                .unwrap();
            r
        };

        let fires = Rc::new(std::cell::Cell::new(0u32));
        {
            let store = store.clone();
            let fires = fires.clone();
            set_staged_scan_reborrow_hook(Some(Box::new(move || {
                let n = fires.get();
                fires.set(n + 1);
                let mut s = store.borrow_mut();
                s.put(format!("zzzz{n:04}").as_bytes(), b"out-of-range")
                    .unwrap();
                s.flush().unwrap();
            })));
        }

        let got = drive(&store, Some(lo), Some(hi), 0);
        set_staged_scan_reborrow_hook(None);

        assert!(
            fires.get() >= 3,
            "restart cap must be exhausted, fired {} times",
            fires.get()
        );
        assert!(!got.is_empty());
        assert_entries_eq(&got, &reference);
    }

    const RT_WALL: u64 = 10_000;

    fn rt_v(hlc: u64) -> crate::hlc::Version {
        crate::hlc::Version { hlc, node: 1 }
    }

    fn rt_put(s: &mut VersionedStore<MemBlockDevice>, key: &[u8], hlc: u64) {
        assert_eq!(
            s.apply_at(key, Some(key), rt_v(hlc), RT_WALL).unwrap(),
            crate::replica::Applied::Won,
            "{key:?}"
        );
    }

    fn rt_store(flush_covered: bool) -> RefCell<VersionedStore<MemBlockDevice>> {
        let engine = Engine::create_prewiped(
            MemBlockDevice::new(256, 8192),
            Config { seg_blocks: 16, reserved_blocks: 0, ordering: SOrdering::Sorted },
        );
        let mut s = VersionedStore::new(engine, 1).unwrap();
        for i in 0..10 {
            rt_put(&mut s, format!("a{i:04}").as_bytes(), 10);
        }
        for i in 0..100 {
            rt_put(&mut s, format!("m{i:04}").as_bytes(), 10);
        }
        if flush_covered {
            s.flush().unwrap();
        }
        for i in 0..10 {
            rt_put(&mut s, format!("z{i:04}").as_bytes(), 10);
        }
        assert_eq!(
            s.apply_range_delete(Some(b"m"), Some(b"n"), rt_v(20), RT_WALL).unwrap(),
            crate::replica::Applied::Won
        );
        rt_put(&mut s, b"m0050", 30);
        RefCell::new(s)
    }

    fn scan_rt(
        store: &RefCell<VersionedStore<MemBlockDevice>>,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max: u32,
        bit3: bool,
    ) -> Response {
        let dev = store.borrow().device_handle();
        let gate = ReadGate::new();
        vals_rt::block_on(
            serve_scan(store, dev.as_ref(), &gate, lo, hi, max, bit3, Default::default()),
            reactor::poll_io,
        )
    }

    fn keys_of(entries: &[(Vec<u8>, crate::replica::Stored)]) -> Vec<Vec<u8>> {
        entries.iter().map(|(k, _)| k.clone()).collect()
    }

    fn frag(lo: Option<&[u8]>, hi: Option<&[u8]>, hlc: u64) -> crate::wire::RtFragment {
        crate::wire::RtFragment { lo: lo.map(<[u8]>::to_vec), hi: hi.map(<[u8]>::to_vec), version: rt_v(hlc) }
    }

    #[test]
    fn scan_trailer_goes_to_bit3_peers_and_old_clients_fail_only_on_windows_meeting_range_tombstones() {
        for flush_covered in [false, true] {
            let store = rt_store(flush_covered);
            let live: Vec<Vec<u8>> = (0..10)
                .map(|i| format!("a{i:04}").into_bytes())
                .chain(std::iter::once(b"m0050".to_vec()))
                .chain((0..10).map(|i| format!("z{i:04}").into_bytes()))
                .collect();
            let before = crate::observe::snapshot();
            match scan_rt(&store, None, None, 0, true) {
                Response::Scan { entries, more, fragments, resume } => {
                    assert_eq!(keys_of(&entries), live, "covered keys are filtered, the newer write wins");
                    assert!(!more && resume.is_none());
                    assert_eq!(fragments, vec![frag(Some(b"m"), Some(b"n"), 20)]);
                }
                other => panic!("expected Scan, got {other:?}"),
            }
            let after = crate::observe::snapshot();
            assert!(after.scan_rt_filtered_entries_total >= before.scan_rt_filtered_entries_total + 99);
            match scan_rt(&store, None, None, 0, false) {
                Response::Err(msg) => assert_eq!(msg, scan::RANGE_TOMBSTONES_REQUIRED),
                other => panic!("an old client must not see a window over range tombstones: {other:?}"),
            }
            for (lo, hi, max, want) in [
                (None, Some(&b"b"[..]), 0, &live[..10]),
                (Some(&b"z"[..]), None, 0, &live[11..]),
                (None, None, 4, &live[..4]),
                (Some(&b"a0005"[..]), Some(&b"m"[..]), 0, &live[5..10]),
            ] {
                match scan_rt(&store, lo, hi, max, false) {
                    Response::Scan { entries, fragments, resume, .. } => {
                        assert_eq!(keys_of(&entries), want, "{lo:?}..{hi:?} max {max}");
                        assert!(fragments.is_empty() && resume.is_none());
                    }
                    other => panic!("windows without fragments are served unchanged: {other:?}"),
                }
                match scan_rt(&store, lo, hi, max, true) {
                    Response::Scan { entries, fragments, resume, .. } => {
                        assert_eq!(keys_of(&entries), want);
                        assert!(fragments.is_empty() && resume.is_none(), "no trailer without fragments");
                    }
                    other => panic!("{other:?}"),
                }
            }
            match scan_rt(&store, Some(b"m0010"), None, 1, true) {
                Response::Scan { entries, more, fragments, resume } => {
                    assert_eq!(keys_of(&entries), vec![b"m0050".to_vec()]);
                    assert!(more && resume.is_none());
                    assert_eq!(fragments, vec![frag(Some(b"m0010"), Some(b"m0050\0"), 20)], "clipped to the page window");
                }
                other => panic!("{other:?}"),
            }
            match scan_rt(&store, Some(b"m0051"), Some(b"m0060"), 0, true) {
                Response::Scan { entries, more, fragments, resume } => {
                    assert!(entries.is_empty() && !more && resume.is_none());
                    assert_eq!(fragments, vec![frag(Some(b"m0051"), Some(b"m0060"), 20)], "a page reports fragments over spans it skipped");
                }
                other => panic!("{other:?}"),
            }
        }
    }

    struct BudgetGuard;
    impl Drop for BudgetGuard {
        fn drop(&mut self) {
            scan::set_scan_covered_budget(None);
        }
    }

    #[test]
    fn covered_work_budget_cuts_bit3_pages_with_resume_keys_that_make_progress() {
        let _guard = BudgetGuard;
        scan::set_scan_covered_budget(Some(vals_store::ScanCoveredBudget { filtered_entries: 20, block_reads: u64::MAX }));
        for flush_covered in [false, true] {
            let store = rt_store(flush_covered);
            let mut cursor: Option<Vec<u8>> = None;
            let (mut got, mut pages, mut resumed, mut empty) = (Vec::new(), 0, 0, 0);
            let mut covered = Vec::new();
            loop {
                pages += 1;
                assert!(pages < 100);
                let Response::Scan { entries, more, fragments, resume } = scan_rt(&store, cursor.as_deref(), None, 3, true) else {
                    panic!("expected Scan");
                };
                if let Some(r) = &resume {
                    resumed += 1;
                    assert!(cursor.as_deref().is_none_or(|c| r.as_slice() > c), "resume keys move forward");
                    empty += entries.is_empty() as usize;
                }
                covered.extend(fragments);
                let next = resume.or_else(|| entries.last().map(|(k, _)| [k.as_slice(), &[0]].concat()));
                got.extend(keys_of(&entries));
                if !more {
                    break;
                }
                cursor = next;
            }
            let want: Vec<Vec<u8>> = (0..10)
                .map(|i| format!("a{i:04}").into_bytes())
                .chain(std::iter::once(b"m0050".to_vec()))
                .chain((0..10).map(|i| format!("z{i:04}").into_bytes()))
                .collect();
            assert_eq!(got, want, "flush_covered {flush_covered}");
            assert!(resumed >= 99 / 20, "the covered span takes several resume pages: {resumed}");
            assert!(empty > 0, "a bit-3 page may carry only a resume key");
            assert!(covered.iter().all(|f| f.version == rt_v(20)) && !covered.is_empty());
            match scan_rt(&store, Some(b"m"), None, 3, false) {
                Response::Err(msg) => assert_eq!(msg, scan::RANGE_TOMBSTONES_REQUIRED),
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn fragments_count_toward_the_reply_budget_and_shrink_the_window() {
        let entry = |k: &[u8]| (k.to_vec(), crate::replica::Stored { version: rt_v(1), value: None });
        let f = |lo: &[u8], hi: Option<&[u8]>| vals_store::range_tombstone::Fragment {
            lo: lo.to_vec(),
            hi: hi.map(<[u8]>::to_vec),
            version: 7,
        };
        let (mut entries, mut fragments) = (vec![entry(b"b0"), entry(b"d0")], vec![f(b"a", Some(b"b")), f(b"c", Some(b"d")), f(b"e", None)]);
        let (mut resume, mut more) = (None, false);
        scan::fit_scan_reply(b"", &mut entries, &mut fragments, &mut resume, &mut more, 60);
        assert_eq!(keys_of(&entries), vec![b"b0".to_vec()]);
        assert_eq!(fragments, vec![f(b"a", Some(b"b"))]);
        assert_eq!((resume.as_deref(), more), (Some(&b"c"[..]), true));

        let (mut entries, mut fragments) = (Vec::new(), vec![f(b"a", None)]);
        let (mut resume, mut more) = (Some(b"q".to_vec()), true);
        scan::fit_scan_reply(b"a", &mut entries, &mut fragments, &mut resume, &mut more, 1);
        assert_eq!(fragments, vec![f(b"a", None)], "an item at lo always fits");
        assert_eq!(resume.as_deref(), Some(&b"q"[..]));

        let (mut entries, mut fragments) = (vec![entry(b"c0")], vec![f(b"a", Some(b"b")), f(b"b", Some(b"x"))]);
        let (mut resume, mut more) = (None, false);
        scan::fit_scan_reply(b"a", &mut entries, &mut fragments, &mut resume, &mut more, 30);
        assert!(entries.is_empty());
        assert_eq!(fragments, vec![f(b"a", Some(b"b"))]);
        assert_eq!((resume.as_deref(), more), (Some(&b"b"[..]), true), "the window ends where fragments stop fitting");
    }
