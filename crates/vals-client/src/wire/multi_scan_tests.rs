use super::*;

#[test]
fn multi_scan_codec_roundtrips_modes_and_rejects_malformed_frames() {
    let scans = [(Some(&b"a"[..]), Some(&b"z"[..]), 10), (None, None, 0)];
    for (keys_only, reverse) in [(false, false), (true, false), (false, true), (true, true)] {
        let request = Request::MultiScan { epoch: 7, zone_id: 4, keys_only, start: 1, scans: &scans, reverse };
        let bytes = encode_request_payload(&request);
        assert_eq!(decode_request(&bytes).unwrap(), OwnedRequest::MultiScan {
            epoch: 7, zone_id: 4, keys_only, reverse, start: 1,
            scans: vec![(Some(b"a".to_vec()), Some(b"z".to_vec()), 10), (None, None, 0)],
        });
        for end in 0..bytes.len() { assert!(decode_request(&bytes[..end]).is_err()); }
        let mut bad = bytes.clone(); bad.push(0);
        assert_eq!(decode_request(&bad), Err(WireError::TrailingBytes));
        assert_eq!(bytes[13], keys_only as u8 | ((reverse as u8) << 1));
        let mut bad = bytes.clone(); bad[13] = 4;
        assert_eq!(decode_request(&bad), Err(WireError::BadTag(4)));
        let mut bad = bytes.clone(); bad[14..18].copy_from_slice(&3u32.to_le_bytes());
        assert!(decode_request(&bad).is_err());
        let mut bad = bytes.clone(); bad[18..22].copy_from_slice(&257u32.to_le_bytes());
        assert_eq!(decode_request(&bad), Err(WireError::TooManyEntries));
        let version = Version { hlc: 8, node: 2 };
        let response = Response::MultiScan { start: 1, more: true, keys_only, pages: vec![crate::coord::ScanPage {
            entries: vec![(b"b".to_vec(), Stored { version, value: Some(if keys_only { vec![] } else { b"value".to_vec() }) }),
                (b"c".to_vec(), Stored { version, value: None })],
            more: true, resume: Some(b"d".to_vec()),
            fragments: vec![RtFragment { lo: None, hi: Some(b"b".to_vec()), version }],
        }] };
        let bytes = encode_response_payload(&response);
        assert_eq!(decode_response(&bytes).unwrap(), response);
        for end in 0..bytes.len() { assert!(decode_response(&bytes[..end]).is_err()); }
        let mut bad = bytes; bad.push(0);
        assert_eq!(decode_response(&bad), Err(WireError::TrailingBytes));
    }
}
