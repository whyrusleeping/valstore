use super::*;

fn reply(stream: &mut impl Write, frame: &[u8], response: Response) {
    let id = u64::from_le_bytes(frame[..8].try_into().unwrap());
    write_frame(stream, &frame_msg(id, &wire::encode_response_payload(&response)).unwrap());
}

#[test]
fn reverse_scan_requires_both_features_without_ascending_fallback() {
    for missing in [crate::proto::FEATURE_REVERSE_SCAN, crate::proto::FEATURE_MULTI_SCAN] {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream);
            let mut hello = crate::proto::encode_hello();
            let n = hello.len();
            hello[n-8..].copy_from_slice(&(crate::proto::FEATURES & !missing).to_le_bytes());
            stream.write_all(&hello).unwrap();
            for _ in 0..2 {
                let frame = read_frame(&mut stream);
                assert_eq!(wire::decode_request(&frame[8..]).unwrap(), wire::OwnedRequest::GetWatermark);
                reply(&mut stream, &frame, Response::Watermark(0));
            }
        });
        let t = NetTransport::connect(&[(1, addr)]);
        let err = vals_rt::block_on(t.scan_rev(1, 2, None, None, 1, false), reactor::poll_io).unwrap_err();
        assert!(matches!(err, TransportError::Remote(ref s) if s.contains("REVERSE_SCAN")), "{err:?}");
        let errors = vals_rt::block_on(t.scan_many_rev(1, 2, &[(None, None, 0); 2], true), reactor::poll_io);
        assert!(errors.iter().all(|r| matches!(r, Err(TransportError::Remote(s)) if s.contains("REVERSE_SCAN"))));
        vals_rt::block_on(t.rpc(1, 0, &Request::GetWatermark, 1), reactor::poll_io).unwrap();
        server.join().unwrap();
    }
}

#[test]
fn reverse_scan_cold_connection_uses_shared_multi_scan_encoding_and_outer_cursor() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        read_frame(&mut stream);
        stream.write_all(&crate::proto::encode_hello()).unwrap();
        let frame = read_frame(&mut stream);
        assert_eq!(wire::decode_request(&frame[8..]).unwrap(), wire::OwnedRequest::GetWatermark);
        reply(&mut stream, &frame, Response::Watermark(0));
        for start in 0..2 {
            let frame = read_frame(&mut stream);
            let wire::OwnedRequest::MultiScan { reverse, keys_only, start: got, scans, .. } = wire::decode_request(&frame[8..]).unwrap() else { panic!("expected MultiScan"); };
            assert!(reverse && keys_only);
            assert_eq!(got, start);
            assert_eq!(scans, vec![(None, Some(b"z".to_vec()), 1), (Some(b"a".to_vec()), None, 0)]);
            reply(&mut stream, &frame, Response::MultiScan { start, more: start == 0, keys_only, pages: vec![ScanPage {
                entries: vec![(vec![b'y' - start as u8], Stored { version: crate::hlc::Version { hlc: 9, node: 1 }, value: Some(Vec::new()) })],
                ..Default::default()
            }] });
        }
    });
    let t = NetTransport::connect(&[(1, addr)]);
    let out = vals_rt::block_on(t.scan_many_rev(1, 2, &[(None, Some(b"z"), 1), (Some(b"a"), None, 0)], true), reactor::poll_io);
    assert_eq!(out.len(), 2);
    for (i, page) in out.into_iter().enumerate() {
        let page = page.unwrap();
        assert_eq!(page.entries[0].0, vec![b'y' - i as u8]);
        assert_eq!(page.entries[0].1.value, Some(Vec::new()));
    }
    assert_eq!(t.transport_credit_stats().unwrap().in_flight_ops, 0);
    t.conn(1, 0).unwrap().st.borrow_mut().reset_failed(&mut Vec::new());
    assert_eq!(t.transport_credit_stats().unwrap().in_flight_bytes, 0);
    server.join().unwrap();
}

#[test]
fn reverse_scan_reprobes_capability_after_connection_reset() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        for supported in [true, false] {
            let (mut stream, _) = listener.accept().unwrap();
            read_frame(&mut stream);
            let mut hello = crate::proto::encode_hello();
            if !supported {
                let n = hello.len();
                hello[n-8..].copy_from_slice(&(crate::proto::FEATURES & !crate::proto::FEATURE_REVERSE_SCAN).to_le_bytes());
            }
            stream.write_all(&hello).unwrap();
            let frame = read_frame(&mut stream);
            assert_eq!(wire::decode_request(&frame[8..]).unwrap(), wire::OwnedRequest::GetWatermark);
            reply(&mut stream, &frame, Response::Watermark(0));
            let frame = read_frame(&mut stream);
            if supported {
                assert!(matches!(wire::decode_request(&frame[8..]).unwrap(), wire::OwnedRequest::MultiScan { reverse: true, .. }));
                reply(&mut stream, &frame, Response::MultiScan { start: 0, more: false, keys_only: false, pages: vec![ScanPage::default()] });
            } else {
                assert_eq!(wire::decode_request(&frame[8..]).unwrap(), wire::OwnedRequest::GetWatermark);
                reply(&mut stream, &frame, Response::Watermark(0));
            }
        }
    });
    let t = NetTransport::connect(&[(1, addr)]);
    vals_rt::block_on(t.scan_rev(1, 2, None, None, 1, false), reactor::poll_io).unwrap();
    t.conn(1, 0).unwrap().st.borrow_mut().reset_failed(&mut Vec::new());
    let err = vals_rt::block_on(t.scan_rev(1, 2, None, None, 1, false), reactor::poll_io).unwrap_err();
    assert!(matches!(err, TransportError::Remote(ref s) if s.contains("REVERSE_SCAN")), "{err:?}");
    vals_rt::block_on(t.rpc(1, 0, &Request::GetWatermark, 1), reactor::poll_io).unwrap();
    server.join().unwrap();
}
