use super::*;

#[test]
fn reverse_scan_quorum_pages_limits_and_range_tombstones_over_tcp() {
    let zid = 491;
    let cfg = ZoneConfig { name: "reverse".into(), rf: 3, ordering: Ordering::Sorted, scope: None };
    let addrs: Vec<_> = (1..=3).map(|n| (n, spawn_zoned_server(n, zid, cfg.clone()))).collect();
    let c = Coordinator::new(0, Ring::from_members(3, 64, [1, 2, 3]), 2, NetTransport::connect(&addrs));
    run(c.refresh_catalog(1)).unwrap();
    for i in 0..80 {
        let k = format!("k{i:03}");
        run(c.write_in(zid, k.as_bytes(), k.as_bytes(), WriteConcern::All)).unwrap();
    }
    for k in [b"".as_slice(), b"k", b"k\0"] {
        run(c.write_in(zid, k, k, WriteConcern::All)).unwrap();
    }
    c.transport().set_down(3, true);
    run(c.delete_in(zid, b"k020", WriteConcern::Quorum(2))).unwrap();
    run(c.delete_range_in(zid, Some(b"k030"), Some(b"k050"), WriteConcern::Quorum(2))).unwrap();
    run(c.write_in(zid, b"k040", b"new", WriteConcern::Quorum(2))).unwrap();
    c.transport().set_down(3, false);
    c.set_range_map(zid, RangeMap::from_splits([b"k025".to_vec(), b"k060".to_vec()]));
    let queries = [
        (None, None, None),
        (Some(&b"k010"[..]), Some(&b"k070"[..]), Some(27)),
        (Some(&b"k030"[..]), Some(&b"k049"[..]), None),
        (None, Some(&b"k000"[..]), None),
        (Some(&b"absent"[..]), Some(&b"b"[..]), None),
        (None, None, Some(0)),
    ];
    let expected: Vec<_> = queries.iter().map(|&(lo, hi, limit)| {
        let mut all = run(c.scan_in(zid, lo, hi, 3, None)).unwrap();
        all.reverse();
        if let Some(n) = limit { all.truncate(n); }
        all
    }).collect();
    assert_eq!(expected[0].len(), 63);
    super::super::server::set_zone_scan_reply_budget(zid, Some(80));
    for down in [None, Some(1), Some(2), Some(3)] {
        if let Some(n) = down { c.transport().set_down(n, true); }
        for keys_only in [false, true] {
            let got = run(c.scan_many_in_rev(zid, &queries, 2, keys_only)).unwrap();
            for (got, want) in got.iter().zip(&expected) {
                assert_eq!(got.iter().map(|(k, s)| (k.clone(), s.value.clone().unwrap())).collect::<Vec<_>>(),
                    want.iter().map(|(k, v)| (k.clone(), if keys_only { Vec::new() } else { v.clone() })).collect::<Vec<_>>(), "down={down:?} keys={keys_only}");
            }
        }
        assert_eq!(run(c.scan_in_rev(zid, None, None, 2, Some(7))).unwrap(), expected[0][..7]);
        if let Some(n) = down { c.transport().set_down(n, false); }
    }
    let page = run(c.transport().scan_rev(1, zid, Some(b"k060"), None, 1, false)).unwrap();
    assert_eq!(page.entries[0].0, b"k079");
    assert!(page.more);
    super::super::server::set_zone_scan_reply_budget(zid, None);
    let repeated = vec![(Some(&b"k000"[..]), Some(&b"k005"[..]), Some(2)); 270];
    let before = c.transport().rpc_attempts();
    let got = run(c.scan_many_in_rev(zid, &repeated, 3, false)).unwrap();
    assert_eq!(got.len(), 270);
    assert!(got.iter().all(|p| p.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>() == vec![b"k004", b"k003"]));
    assert!(c.transport().rpc_attempts() - before < 30);
}

#[test]
fn reverse_scan_routes_each_slice_by_its_lower_bound_across_shards() {
    let zid = 1491;
    let shards = spawn_sorted_shards(1, zid, 4);
    let c = Coordinator::new(0, Ring::from_members(1, 64, [1]), 1, NetTransport::connect_sharded([(1, shards)]));
    run(c.refresh_catalog(1)).unwrap();
    let map = RangeMap::from_splits([b"g".to_vec(), b"n".to_vec(), b"t".to_vec()]);
    c.set_range_map(zid, map.clone());
    let keys: Vec<_> = [b"a", b"h", b"o", b"z"].into_iter()
        .flat_map(|p| (0..5).map(move |i| [p.as_slice(), format!("{i}").as_bytes()].concat())).collect();
    let target_shards: std::collections::HashSet<_> = keys.iter()
        .map(|k| crate::partition::data_shard(Some(&map), k, 4)).collect();
    assert!(target_shards.len() >= 2);
    for k in &keys { run(c.write_in(zid, k, k, WriteConcern::All)).unwrap(); }
    super::super::server::set_zone_scan_reply_budget(zid, Some(60));
    let got = run(c.scan_in_rev(zid, None, None, 1, None)).unwrap();
    assert_eq!(got, keys.iter().rev().map(|k| (k.clone(), k.clone())).collect::<Vec<_>>());
    let queries = [(None, Some(b"g".as_slice()), Some(2)), (Some(b"g".as_slice()), Some(b"n".as_slice()), Some(2)),
        (Some(b"n".as_slice()), Some(b"t".as_slice()), Some(2)), (Some(b"t".as_slice()), None, Some(2))];
    let got = run(c.scan_many_in_rev(zid, &queries, 1, true)).unwrap();
    for (i, page) in got.iter().enumerate() {
        assert_eq!(page.iter().map(|(k, _)| k).collect::<Vec<_>>(), keys[i*5..i*5+5].iter().rev().take(2).collect::<Vec<_>>());
    }
    super::super::server::set_zone_scan_reply_budget(zid, None);
}

#[test]
fn reverse_scan_server_requires_negotiation_and_preserves_ascending_wire_mode() {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    fn frame(s: &mut TcpStream) -> Vec<u8> {
        let mut len = [0; 4];
        s.read_exact(&mut len).unwrap();
        let mut out = vec![0; u32::from_le_bytes(len) as usize];
        s.read_exact(&mut out).unwrap();
        out
    }
    let zid = 2491;
    let addr = spawn_zoned_server(1, zid, ZoneConfig { name: "reverse-feature".into(), rf: 1, ordering: Ordering::Sorted, scope: None });
    let c = Coordinator::new(0, Ring::from_members(1, 64, [1]), 1, NetTransport::connect(&[(1, addr)]));
    run(c.refresh_catalog(1)).unwrap();
    for k in [b"a", b"b"] { run(c.write_in(zid, k, k, WriteConcern::All)).unwrap(); }
    for missing in [crate::proto::FEATURE_REVERSE_SCAN, crate::proto::FEATURE_MULTI_SCAN, 0] {
        let mut s = TcpStream::connect(addr).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        let mut hello = crate::proto::encode_hello();
        let n = hello.len();
        hello[n-8..].copy_from_slice(&(crate::proto::FEATURES & !missing).to_le_bytes());
        s.write_all(&hello).unwrap();
        frame(&mut s);
        for reverse in [true, false] {
            let payload = wire::encode_request_payload(&Request::MultiScan { epoch: 0, zone_id: zid, keys_only: false, reverse, start: 0, scans: &[(None, None, 0)] });
            let mut request = ((payload.len()+8) as u32).to_le_bytes().to_vec();
            request.extend_from_slice(&1u64.to_le_bytes());
            request.extend_from_slice(&payload);
            s.write_all(&request).unwrap();
            let response = wire::decode_response(&frame(&mut s)[8..]).unwrap();
            if missing == crate::proto::FEATURE_MULTI_SCAN || reverse && missing != 0 {
                assert!(matches!(response, Response::Err(ref e) if e.contains("not negotiated")), "{response:?}");
            } else {
                let Response::MultiScan { pages, .. } = response else { panic!("{response:?}"); };
                assert_eq!(pages[0].entries.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(),
                    if reverse { vec![b"b", b"a"] } else { vec![b"a", b"b"] });
            }
        }
    }
}
