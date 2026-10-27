use super::*;
use std::rc::Rc;
use vals_spdk::MemBlockDevice;
use vals_store::{Config, Engine, Ordering};
use vals_store::range_tombstone::FragmentSet;

fn site() -> crate::integrity::ReadSite {
    static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(489000);
    thread_local! { static ZONE: u32 = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
    crate::integrity::ReadSite { zone: ZONE.with(|z| *z), shard: 0 }
}

fn version(hlc: u64) -> crate::hlc::Version { crate::hlc::Version { hlc, node: 1 } }

fn store(runs: bool) -> RefCell<VersionedStore<MemBlockDevice>> {
    let e = Engine::create_prewiped(MemBlockDevice::new(256, 32768),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted });
    let mut s = VersionedStore::new(e, 1).unwrap();
    for i in 0..140 {
        let k = format!("k{i:04}");
        s.apply_at(k.as_bytes(), Some(k.as_bytes()), version(10), 10000).unwrap();
    }
    if runs { s.flush().unwrap(); }
    for i in 60..160 {
        let k = format!("k{i:04}");
        s.apply_at(k.as_bytes(), (i % 7 != 0).then_some(b"new".as_slice()), version(30), 10000).unwrap();
    }
    s.apply_range_delete(Some(b"k0020"), Some(b"k0090"), version(20), 10000).unwrap();
    RefCell::new(s)
}

fn call<D: BlockDevice + AsyncRead>(s: &RefCell<VersionedStore<D>>, lo: Option<&[u8]>, hi: Option<&[u8]>, max: u32, keys: bool) -> Response {
    let dev = s.borrow().device_handle();
    vals_rt::block_on(serve_scan_rev(s, &*dev, &ReadGate::new(), lo, hi, max, true, keys, site()), || {}).0
}

struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        set_staged_scan_reborrow_hook(None);
        set_scan_covered_budget(None);
        set_zone_scan_reply_budget(site().zone, None);
        READ_HOOK.with(|h| h.borrow_mut().take());
    }
}

fn assert_coverage(s: &RefCell<VersionedStore<MemBlockDevice>>, lower: &[u8], hi: Option<&[u8]>, got: &[crate::wire::RtFragment]) {
    let expected: Vec<_> = s.borrow().engine().range_tombstones().clipped(lower, hi).into_iter().map(|f| crate::wire::RtFragment {
        lo: (!f.lo.is_empty()).then_some(f.lo), hi: f.hi, version: crate::replica::unpack_version(f.version),
    }).collect();
    assert_eq!(got, expected, "coverage {lower:?}..{hi:?}");
}

#[test]
fn pages_match_reversed_ascending_with_limits_reply_budgets_and_empty_covered_pages() {
    let _reset = Reset;
    set_scan_covered_budget(Some(vals_store::ScanCoveredBudget { filtered_entries: 7, block_reads: u64::MAX }));
    let mut empty = 0;
    for runs in [false, true] {
        let s = store(runs);
        for (lo, end) in [(None, None), (Some(b"k0015".as_slice()), Some(b"k0130".as_slice())),
            (Some(b"k".as_slice()), Some(b"k".as_slice())), (Some(b"z".as_slice()), Some(b"a".as_slice()))] {
            let (mut expected, _) = s.borrow().scan_page(lo, end, usize::MAX, usize::MAX).unwrap();
            expected.reverse();
            for cap in [0, 1, 3, 40] {
                for budget in [1, 64, 180, SCAN_REPLY_BUDGET] {
                    set_zone_scan_reply_budget(site().zone, Some(budget));
                    for keys in [false, true] {
                        let mut got = Vec::new();
                        let mut hi = end.map(<[u8]>::to_vec);
                        for page in 0..1000 {
                            let Response::Scan { entries, more, fragments, resume } = call(&s, lo, hi.as_deref(), cap, keys) else { panic!("scan failed"); };
                            let lower = resume.as_deref().or_else(|| if more { entries.last().map(|(k, _)| k.as_slice()) } else { lo }).unwrap_or_default();
                            assert_coverage(&s, lower, hi.as_deref(), &fragments);
                            assert!(entries.iter().all(|(k, _)| k.as_slice() >= lower && hi.as_deref().is_none_or(|h| k.as_slice() < h)));
                            assert!(cap == 0 || entries.len() <= cap as usize);
                            empty += (entries.is_empty() && more) as usize;
                            let next = resume.or_else(|| entries.last().map(|(k, _)| k.clone()));
                            got.extend(entries);
                            if !more { break; }
                            let next = next.expect("truncated page must have a cursor");
                            assert!(hi.as_ref().is_none_or(|h| next < *h), "exclusive reverse progress");
                            hi = Some(next);
                            assert!(page < 999, "scan must terminate");
                        }
                        let mut want = expected.clone();
                        if keys { for (_, stored) in &mut want { stored.value = stored.value.as_ref().map(|_| Vec::new()); } }
                        assert_eq!(got, want, "runs={runs} cap={cap} budget={budget} keys={keys}");
                    }
                }
            }
        }
    }
    assert!(empty > 0);
}

#[test]
fn reverse_fit_preserves_exclusive_fragment_ends_and_prefix_keys() {
    let entry = |k: &[u8]| (k.to_vec(), Stored { version: version(50), value: None });
    let all: Vec<_> = [b"".as_slice(), b"\0", b"a", b"a\0", b"b", b"c", b"c\0", b"d", b"z"].into_iter().rev().map(entry).collect();
    let mut set = FragmentSet::default();
    for (lo, hi, v) in [(b"".as_slice(), Some(b"a".as_slice()), 1), (b"a", Some(b"c".as_slice()), 2), (b"d", None, 3)] {
        set.overlay(Some(lo), hi, v).unwrap();
    }
    for budget in 0..250 {
        for end in [None, Some(b"c\0".as_slice()), Some(b"\0".as_slice())] {
            let mut hi = end.map(<[u8]>::to_vec);
            let mut got = Vec::new();
            for page in 0..100 {
                let mut entries: Vec<_> = all.iter().filter(|(k, _)| hi.as_ref().is_none_or(|h| k < h)).cloned().collect();
                let mut fragments = set.clipped(b"", hi.as_deref());
                let (mut resume, mut more) = (None, false);
                fit_reply(hi.as_deref(), &mut entries, &mut fragments, &mut resume, &mut more, budget);
                let lower = resume.as_deref().unwrap_or_default();
                assert_eq!(fragments, set.clipped(lower, hi.as_deref()), "budget={budget} hi={hi:?}");
                assert!(entries.iter().all(|(k, _)| k.as_slice() >= lower));
                got.extend(entries);
                if !more { break; }
                let next = resume.unwrap();
                assert!(hi.as_ref().is_none_or(|h| next < *h));
                hi = Some(next);
                assert!(page < 99);
            }
            assert_eq!(got, all.iter().filter(|(k, _)| end.is_none_or(|h| k.as_slice() < h)).cloned().collect::<Vec<_>>());
        }
    }
}

#[test]
fn reply_cut_at_an_omitted_keys_successor_discards_empty_fragments() {
    let mut set = FragmentSet::default();
    set.overlay(None, Some(b"a\0"), 1).unwrap();
    let mut entries: Vec<_> = [b"z", b"a"].into_iter().map(|k| (k.to_vec(), Stored { version: version(50), value: None })).collect();
    let mut fragments = set.clipped(b"", None);
    let (mut resume, mut more) = (None, false);
    fit_reply(None, &mut entries, &mut fragments, &mut resume, &mut more, 54);
    assert_eq!(entries.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(), vec![b"z".as_slice()]);
    assert_eq!(resume.as_deref(), Some(b"a\0".as_slice()));
    assert!(more);
    assert_eq!(fragments, set.clipped(resume.as_deref().unwrap(), None));
    assert!(fragments.is_empty());
}

#[test]
fn concurrent_overwrites_and_deletes_patch_descending_entries_with_and_without_directory() {
    let _reset = Reset;
    for directory in [false, true] {
        for keys in [false, true] {
            let s = Rc::new(store(true));
            if directory { s.borrow_mut().engine_mut().rebuild_winner_directory().unwrap(); }
            let fires = Rc::new(Cell::new(0));
            let cloned = s.clone();
            let count = fires.clone();
            set_staged_scan_reborrow_hook(Some(Box::new(move || {
                count.set(count.get() + 1);
                let mut s = cloned.borrow_mut();
                s.apply_at(b"k0120", Some(b"patched"), version(100), 10000).unwrap();
                s.apply_at(b"k0100", None, version(100), 10000).unwrap();
            })));
            let Response::Scan { entries, fragments, more, .. } = call(&s, None, None, 0, keys) else { panic!("scan"); };
            assert_eq!(fires.get(), 1, "mutable changes need no storage restart");
            assert!(!more);
            for (key, value) in [(b"k0120".as_slice(), Some(if keys { b"".as_slice() } else { b"patched".as_slice() })), (b"k0100", None)] {
                let stored = &entries.iter().find(|(k, _)| k == key).unwrap().1;
                assert_eq!(stored.version, version(100));
                assert_eq!(stored.value.as_deref(), value);
            }
            assert_coverage(&s, b"", None, &fragments);
            set_staged_scan_reborrow_hook(None);
        }
    }
}

#[test]
fn storage_changes_before_finish_exhaust_restart_cap_then_use_sync_reverse_fallback() {
    let _reset = Reset;
    for keys in [false, true] {
        let s = Rc::new(store(true));
        let reference = call(&s, Some(b"k0100"), Some(b"k0130"), 7, keys);
        let fires = Rc::new(Cell::new(0));
        let cloned = s.clone();
        let count = fires.clone();
        set_staged_scan_reborrow_hook(Some(Box::new(move || {
            let n = count.get();
            count.set(n + 1);
            let mut s = cloned.borrow_mut();
            s.apply_at(format!("z{n}").as_bytes(), Some(b"outside"), version(100 + n), 10000).unwrap();
            s.flush().unwrap();
        })));
        assert_eq!(call(&s, Some(b"k0100"), Some(b"k0130"), 7, keys), reference);
        assert_eq!(fires.get(), 3);
        set_staged_scan_reborrow_hook(None);
    }
}

thread_local! {
    static READ_HOOK: RefCell<Option<Box<dyn FnOnce() -> bool>>> = const { RefCell::new(None) };
}

struct YieldDevice(MemBlockDevice);
impl BlockDevice for YieldDevice {
    fn block_size(&self) -> usize { self.0.block_size() }
    fn num_blocks(&self) -> u64 { self.0.num_blocks() }
    fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> { self.0.read_at(lba, buf) }
    fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> { self.0.write_at(lba, buf) }
    fn flush(&self) -> vals_spdk::Result<()> { self.0.flush() }
    fn sibling(&self) -> vals_spdk::Result<Self> { Ok(Self(self.0.share())) }
}
impl AsyncRead for YieldDevice {
    async fn read_async(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
        vals_rt::yield_now().await;
        let hook = READ_HOOK.with(|h| h.borrow_mut().take());
        if hook.is_some_and(|hook| hook()) {
            return Err(vals_spdk::BlockError::Io(std::io::Error::other("injected stale read failure")));
        }
        self.0.read_at(lba, buf)
    }
}

fn yielding_store(runs: bool) -> Rc<RefCell<VersionedStore<YieldDevice>>> {
    let engine = Engine::create_prewiped(YieldDevice(MemBlockDevice::new(256, 32768)),
        Config { seg_blocks: 16, reserved_blocks: 0, ordering: Ordering::Sorted });
    let mut s = VersionedStore::new(engine, 1).unwrap();
    for k in [b"a", b"b", b"c", b"d"] { s.apply_at(k, Some(k), version(10), 10000).unwrap(); }
    if runs { s.flush().unwrap(); }
    Rc::new(RefCell::new(s))
}

#[test]
fn storage_changes_during_index_and_value_reads_discard_stale_errors_and_restart() {
    let _reset = Reset;
    for runs in [false, true] {
        let s = yielding_store(runs);
        let cloned = s.clone();
        READ_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(move || {
            let mut s = cloned.borrow_mut();
            s.apply_at(b"c", Some(b"changed"), version(100), 10000).unwrap();
            s.flush().unwrap();
            true
        })));
        let Response::Scan { entries, more, .. } = call(&s, None, None, 0, false) else { panic!("stale error must be discarded"); };
        let (mut expected, _) = s.borrow().scan_page(None, None, usize::MAX, usize::MAX).unwrap();
        expected.reverse();
        assert_eq!(entries, expected);
        assert!(!more);
        assert!(READ_HOOK.with(|h| h.borrow().is_none()));
    }
    let s = yielding_store(true);
    READ_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(|| true)));
    assert!(matches!(call(&s, None, None, 0, false), Response::Err(_)), "a current-generation read error must propagate");
}

#[test]
fn writes_during_value_reads_patch_winners_and_range_deletes_restart_coverage() {
    let _reset = Reset;
    for range_delete in [false, true] {
        let s = yielding_store(false);
        let cloned = s.clone();
        READ_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(move || {
            let mut s = cloned.borrow_mut();
            if range_delete {
                s.apply_range_delete(Some(b"b"), Some(b"d"), version(100), 10000).unwrap();
            } else {
                s.apply_at(b"c", Some(b"changed"), version(100), 10000).unwrap();
                s.apply_at(b"b", None, version(100), 10000).unwrap();
            }
            false
        })));
        let Response::Scan { entries, fragments, more, .. } = call(&s, None, None, 0, false) else { panic!("scan"); };
        let (mut expected, _) = s.borrow().scan_page(None, None, usize::MAX, usize::MAX).unwrap();
        expected.reverse();
        assert_eq!(entries, expected);
        assert_eq!(fragments.len(), range_delete as usize);
        if range_delete {
            assert_eq!(fragments[0], crate::wire::RtFragment { lo: Some(b"b".to_vec()), hi: Some(b"d".to_vec()), version: version(100) });
        }
        assert!(!more);
    }
}

#[test]
fn keys_only_memtable_pages_do_not_read_values() {
    let _reset = Reset;
    let s = yielding_store(false);
    READ_HOOK.with(|h| *h.borrow_mut() = Some(Box::new(|| panic!("keys-only must not read values"))));
    let Response::Scan { entries, .. } = call(&s, None, None, 0, true) else { panic!("scan"); };
    assert_eq!(entries.len(), 4);
    assert!(entries.iter().all(|(_, s)| s.value == Some(Vec::new())));
    assert!(READ_HOOK.with(|h| h.borrow().is_some()));
}

#[test]
fn mem_and_block_refills_are_generation_guarded() {
    let _reset = Reset;
    for mem in [false, true] {
        let mut e = Engine::create_prewiped(MemBlockDevice::new(256, 131072),
            Config { seg_blocks: 64, reserved_blocks: 0, ordering: Ordering::Sorted });
        for i in 0..200 { e.put(format!("k{i:05}").as_bytes(), b"run", crate::replica::pack_version(version(10))).unwrap(); }
        e.flush(vals_store::RunBuildOpts { block_target_bytes: 32, bloom_bits_per_key: 10, num_buckets: 64 }).unwrap();
        let mut s = VersionedStore::new(e, 1).unwrap();
        if mem {
            for i in 0..5000 { s.apply_at(format!("m{i:05}").as_bytes(), Some(b"mem"), version(20), 10000).unwrap(); }
        }
        let s = Rc::new(RefCell::new(s));
        let (mut expected, _) = s.borrow().scan_page(None, Some(b"z"), usize::MAX, usize::MAX).unwrap();
        expected.reverse();
        let cloned = s.clone();
        let fires = Rc::new(Cell::new(0));
        let count = fires.clone();
        set_staged_scan_reborrow_hook(Some(Box::new(move || {
            let n = count.get();
            count.set(n + 1);
            if n == 0 {
                let mut s = cloned.borrow_mut();
                s.apply_at(b"z", Some(b"outside"), version(100), 10000).unwrap();
                s.flush().unwrap();
            }
        })));
        let Response::Scan { entries, more, .. } = call(&s, None, Some(b"z"), 0, false) else { panic!("scan"); };
        assert_eq!(entries, expected);
        assert!(!more);
        assert!(fires.get() >= 4, "first refill restarts, second attempt refills and finishes");
        set_staged_scan_reborrow_hook(None);
    }
}

#[test]
fn reverse_rejects_unnegotiated_resume_and_fragments() {
    let _reset = Reset;
    let s = store(false);
    let dev = s.borrow().device_handle();
    let call_old = |lo, hi| vals_rt::block_on(serve_scan_rev(&s, &*dev, &ReadGate::new(), lo, hi, 0, false, false, site()), || {}).0;
    assert!(matches!(call_old(None, None), Response::Err(_)));
    assert!(matches!(call_old(Some(b"k0100"), None), Response::Scan { .. }));
    set_zone_scan_reply_budget(site().zone, Some(1));
    assert!(matches!(call_old(Some(b"k0100"), None), Response::Err(_)));
}
