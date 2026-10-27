use super::*;
use std::collections::VecDeque;

fn run<F: Future>(f: F) -> F::Output { vals_rt::block_on(f, || {}) }
fn entry(key: &[u8]) -> (Vec<u8>, Stored) {
    (key.to_vec(), Stored { version: Version { hlc: 10, node: 1 }, value: Some(key.to_vec()) })
}
fn page(more: bool, resume: Option<&[u8]>, keys: &[&[u8]]) -> ScanPage {
    ScanPage { entries: keys.iter().map(|k| entry(k)).collect(), more,
        resume: resume.map(<[u8]>::to_vec), fragments: Vec::new() }
}

struct Scripted {
    replies: RefCell<VecDeque<Vec<Result<ScanPage, TransportError>>>>,
    calls: RefCell<Vec<Vec<crate::wire::OwnedScan>>>,
    retain_error: bool,
    retained: Cell<usize>,
}

impl Transport for Scripted {
    async fn apply(&self, _: NodeId, _: ZoneId, _: &[u8], _: Option<&[u8]>, _: Version) -> Result<Applied, TransportError> { unreachable!() }
    async fn read(&self, _: NodeId, _: ZoneId, _: &[u8]) -> Result<Option<Stored>, TransportError> { unreachable!() }
    async fn members(&self, _: NodeId) -> Result<RingView, TransportError> { unreachable!() }
    async fn zones(&self, _: NodeId) -> Result<Catalog, TransportError> { unreachable!() }
    async fn scan(&self, _: NodeId, _: ZoneId, _: Option<&[u8]>, _: Option<&[u8]>, _: u32) -> Result<ScanPage, TransportError> {
        panic!("reverse must never fall back to ascending")
    }
    async fn scan_many_rev(&self, _: NodeId, _: ZoneId, scans: &[crate::wire::WireScan<'_>], _: bool) -> Vec<Result<ScanPage, TransportError>> {
        self.calls.borrow_mut().push(scans.iter().map(|&(lo, hi, max)|
            (lo.map(<[u8]>::to_vec), hi.map(<[u8]>::to_vec), max)).collect());
        self.replies.borrow_mut().pop_front().expect("no unexpected continuation")
    }
    fn retain_response(&self, _: NodeId, _: usize) -> Result<Option<Credit>, TransportError> {
        self.retained.set(self.retained.get() + 1);
        if self.retain_error { Err(TransportError::Overloaded) } else { Ok(None) }
    }
}

fn coordinator(replies: Vec<Vec<Result<ScanPage, TransportError>>>) -> Coordinator<Scripted> {
    let mut ring = Ring::new(1);
    ring.add_node(1);
    let c = Coordinator::new(9, ring, 1, Scripted {
        replies: RefCell::new(replies.into()), calls: RefCell::new(Vec::new()), retain_error: false, retained: Cell::new(0),
    }).with_manual_refresh();
    c.catalog.borrow_mut().insert(2, crate::zone::ZoneConfig {
        name: "reverse".into(), rf: 1, ordering: Ordering::Sorted, scope: None,
    });
    c
}

#[test]
fn reverse_rejects_nonadvancing_coverage_including_implicit_last_keys() {
    for p in [page(true, None, &[]), page(true, Some(b"z"), &[]), page(true, Some(b"zz"), &[]),
        page(true, Some(b"a"), &[]), page(false, Some(b"m"), &[]), page(true, None, &[b"z"])] {
        let c = coordinator(vec![vec![Ok(p)]]);
        let err = run(c.scan_in_rev(2, Some(b"b"), Some(b"z"), 1, None)).unwrap_err();
        assert!(matches!(err, CoordError::ScanQuorumFailed { got: 0, needed: 1, .. }), "{err:?}");
        assert_eq!(c.transport.calls.borrow().len(), 1);
        assert_eq!(c.transport.retained.get(), 0);
    }
    assert!(validate_page(&page(true, Some(b""), &[]), None, Some(b"\0")).is_ok());
    assert!(validate_page(&page(true, None, &[b""]), None, None).is_ok());
}

#[test]
fn reverse_empty_covered_pages_resume_exclusively_and_keep_per_query_limits() {
    let c = coordinator(vec![
        vec![Ok(page(true, Some(b"m"), &[])), Ok(page(false, None, &[b"z"]))],
        vec![Ok(page(true, None, &[b"a\0", b"a"]))],
        vec![Ok(page(false, None, &[b""]))],
    ]);
    let got = run(c.scan_many_in_rev(2, &[(None, Some(b"n"), Some(3)), (Some(b"z"), None, Some(1))], 1, false)).unwrap();
    assert_eq!(got[0], vec![entry(b"a\0"), entry(b"a"), entry(b"")]);
    assert_eq!(got[1], vec![entry(b"z")]);
    assert_eq!(*c.transport.calls.borrow(), vec![
        vec![(None, Some(b"n".to_vec()), 3), (Some(b"z".to_vec()), None, 1)],
        vec![(None, Some(b"m".to_vec()), 3)],
        vec![(None, Some(b"a".to_vec()), 1)],
    ]);
    assert_eq!(c.transport.retained.get(), 4);
}

#[test]
fn reverse_failures_preserve_quorum_details_stale_epochs_and_admission_errors() {
    for error in [TransportError::Unavailable, TransportError::Remote("reverse unsupported".into()),
        TransportError::Overloaded, TransportError::StaleEpoch(99)] {
        let c = coordinator(vec![vec![Err(error.clone())]]);
        let got = run(c.scan_in_rev(2, None, Some(b"z"), 1, None)).unwrap_err();
        match error {
            TransportError::Overloaded => assert!(matches!(got, CoordError::Overloaded)),
            TransportError::StaleEpoch(99) => assert!(matches!(got, CoordError::StaleRing { epoch: 99 })),
            _ => match got {
                CoordError::ScanQuorumFailed { zone_id, cursor, needed, got, replicas } => {
                    assert_eq!((zone_id, cursor, needed, got), (2, Some(b"z".to_vec()), 1, 0));
                    assert_eq!(replicas.len(), 1);
                    assert_eq!(replicas[0].node, 1);
                    assert_eq!(replicas[0].error, error);
                }
                other => panic!("{other:?}"),
            }
        }
    }
    let mut c = coordinator(vec![vec![Ok(page(false, None, &[]))]]);
    c.transport.retain_error = true;
    assert!(matches!(run(c.scan_in_rev(2, None, None, 1, None)), Err(CoordError::Overloaded)));
    let c = coordinator(vec![vec![]]);
    assert!(matches!(run(c.scan_in_rev(2, None, None, 1, None)), Err(CoordError::ScanQuorumFailed { got: 0, .. })));
    let c = coordinator(vec![]);
    assert!(matches!(run(c.transport.scan_rev(1, 2, None, None, 1, false)), Err(TransportError::Remote(s)) if s == "reverse scan unsupported"));
}

#[test]
fn reverse_batches_are_bounded_and_zero_limits_or_empty_windows_send_nothing() {
    let max = crate::wire::MAX_MULTI_SCAN_ENTRIES;
    let c = coordinator(vec![vec![Ok(page(false, None, &[b"a"])); max], vec![Ok(page(false, None, &[b"a"]))]]);
    let scans = vec![(None, None, Some(1)); max + 1];
    let got = run(c.scan_many_in_rev(2, &scans, 0, false)).unwrap();
    assert_eq!(got, vec![vec![entry(b"a")]; max + 1]);
    assert_eq!(c.transport.calls.borrow().iter().map(Vec::len).collect::<Vec<_>>(), vec![max, 1]);
    let c = coordinator(vec![]);
    let got = run(c.scan_many_in_rev(2, &[(None, None, Some(0)), (Some(b"z"), Some(b"a"), None),
        (Some(b"a"), Some(b"a"), None)], 1, false)).unwrap();
    assert_eq!(got, vec![Vec::new(); 3]);
    assert!(run(c.scan_many_in_rev(2, &[], 1, false)).unwrap().is_empty());
    assert!(c.transport.calls.borrow().is_empty());
    assert!(matches!(run(c.scan_in_rev(DEFAULT_ZONE, None, None, 1, None)), Err(CoordError::NotSorted(_))));
}
