use super::*;
use std::time::Duration;

#[test]
fn single_scan_lane_admits_normal_work_after_bounded_chunks() {
    for linger in [Duration::ZERO, COVER_SCAN_LINGER] {
        let (tx, rx) = mpsc::channel();
        let mut dispatch = Dispatch::new(rx, 3, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)))
            .with_classes(|class: &usize| *class, 1, linger, Some(COVER_SCAN_DUTY_CHUNKS));
        for owner in 0..3 {
            for _ in 0..COVER_SCAN_DUTY_CHUNKS * 3 {
                tx.send((owner, CLASS_SCAN)).unwrap();
            }
            tx.send((owner, CLASS_NORMAL)).unwrap();
        }
        for owner in 0..3 {
            for _ in 0..COVER_SCAN_DUTY_CHUNKS {
                assert!(matches!(dispatch.pick(0), Pick::Job(_, CLASS_SCAN, _)));
                dispatch.scan_done();
            }
            tx.send((owner, CLASS_URGENT)).unwrap();
            assert!(matches!(dispatch.pick(0), Pick::Job(_, CLASS_URGENT, _)));
            match dispatch.pick(0) {
                Pick::Job(picked, CLASS_NORMAL, _) => assert_eq!(picked, owner),
                _ => panic!("normal work starved after the scan duty share"),
            }
        }
        for chunk in 1..=COVER_SCAN_DUTY_CHUNKS * 2 {
            assert!(matches!(dispatch.pick(0), Pick::Job(_, CLASS_SCAN, _)));
            assert_eq!(dispatch.scan_chunks, chunk.min(COVER_SCAN_DUTY_CHUNKS));
            dispatch.scan_done();
        }
    }
}

#[test]
fn other_lanes_do_not_change_scan_duty_counter() {
    let (tx, rx) = mpsc::channel();
    let mut dispatch = Dispatch::new(rx, 1, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)))
        .with_classes(|class: &usize| *class, 1, Duration::ZERO, Some(COVER_SCAN_DUTY_CHUNKS));
    tx.send((0, CLASS_SCAN)).unwrap();
    assert!(matches!(dispatch.pick(0), Pick::Job(_, CLASS_SCAN, _)));
    assert_eq!(dispatch.scan_chunks, 1);
    for lane in [1, 2] {
        tx.send((0, CLASS_SCAN)).unwrap();
        assert!(matches!(dispatch.pick(lane), Pick::Job(_, CLASS_SCAN, _)));
        assert_eq!(dispatch.scan_chunks, 1);
        tx.send((0, CLASS_NORMAL)).unwrap();
        assert!(matches!(dispatch.pick(lane), Pick::Job(_, CLASS_NORMAL, _)));
        assert_eq!(dispatch.scan_chunks, 1);
    }
}

#[test]
fn unlimited_scan_lane_keeps_scan_priority() {
    for linger in [Duration::ZERO, COVER_SCAN_LINGER] {
        let (tx, rx) = mpsc::channel();
        let mut dispatch = Dispatch::new(rx, 1, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)))
            .with_classes(|class: &usize| *class, 1, linger, None);
        tx.send((0, CLASS_NORMAL)).unwrap();
        for _ in 0..COVER_SCAN_DUTY_CHUNKS * 2 {
            tx.send((0, CLASS_SCAN)).unwrap();
            assert!(matches!(dispatch.pick(0), Pick::Job(_, CLASS_SCAN, _)));
            assert_eq!(dispatch.scan_chunks, 0);
            dispatch.scan_done();
        }
        assert!(matches!(dispatch.pick(1), Pick::Job(_, CLASS_NORMAL, _)));
    }
}

#[test]
fn active_cover_chunks_skip_the_single_lane_duty_share_but_not_urgent_work() {
    let (tx, rx) = mpsc::channel();
    let mut dispatch = Dispatch::new(rx, 2, Arc::new(AtomicBool::new(false)), Arc::new(AtomicUsize::new(0)))
        .with_classes(|class: &usize| *class, 1, Duration::ZERO, Some(COVER_SCAN_DUTY_CHUNKS));
    for _ in 0..COVER_SCAN_DUTY_CHUNKS {
        tx.send((0, CLASS_SCAN)).unwrap();
    }
    for _ in 0..COVER_SCAN_DUTY_CHUNKS * 3 {
        tx.send((1, CLASS_COVER)).unwrap();
    }
    tx.send((0, CLASS_NORMAL)).unwrap();
    for _ in 0..COVER_SCAN_DUTY_CHUNKS * 2 {
        assert!(matches!(dispatch.pick(0), Pick::Job(1, CLASS_COVER, _)));
        dispatch.scan_done();
    }
    assert_eq!(dispatch.scan_chunks, 0, "cover chunks do not spend the duty share");
    tx.send((0, CLASS_URGENT)).unwrap();
    assert!(matches!(dispatch.pick(0), Pick::Job(0, CLASS_URGENT, _)));
    for _ in 0..COVER_SCAN_DUTY_CHUNKS {
        assert!(matches!(dispatch.pick(0), Pick::Job(1, CLASS_COVER, _)));
    }
    for _ in 0..COVER_SCAN_DUTY_CHUNKS {
        assert!(matches!(dispatch.pick(0), Pick::Job(0, CLASS_SCAN, _)));
    }
    tx.send((1, CLASS_COVER)).unwrap();
    assert!(matches!(dispatch.pick(0), Pick::Job(1, CLASS_COVER, _)), "an exhausted duty share still admits cover chunks first");
    assert!(matches!(dispatch.pick(0), Pick::Job(0, CLASS_NORMAL, _)));
    tx.send((1, CLASS_COVER)).unwrap();
    tx.send((0, CLASS_NORMAL)).unwrap();
    assert!(matches!(dispatch.pick(1), Pick::Job(0, CLASS_NORMAL, _)), "merge lanes keep merging first");
    assert!(matches!(dispatch.pick(1), Pick::Job(1, CLASS_COVER, _)));
}
