use super::*;

const OUTSIDE: Option<&str> = Some("run footer section outside the run");

fn reason<T>(r: Result<T, SnapshotError>) -> Option<&'static str> {
    match r {
        Err(SnapshotError::Corrupt(why)) => Some(why),
        _ => None,
    }
}

fn restamp(run: &mut [u8], at: usize, value: u64) {
    let n = run.len();
    let f = &mut run[n - FOOTER_LEN..];
    f[at..at + 8].copy_from_slice(&value.to_le_bytes());
    let crc = crc::crc32(&f[..FOOTER_LEN - 4]);
    f[FOOTER_LEN - 4..].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn run_footer_sections_outside_the_run_are_refused_before_reading() {
    const INDEX_OFF: usize = 24;
    const INDEX_LEN: usize = 32;
    const BLOOM_OFF: usize = 40;
    const BLOOM_LEN: usize = 48;
    const SIDECAR_OFF: usize = 56;
    const SIDECAR_LEN: usize = 64;
    for version in [RUN_SIDECAR_VERSION, RUN_VERSION] {
        let good = build_run_versioned(&firehose(200), RunBuildOpts::default(), version);
        let ft = decode_footer(&good[good.len() - FOOTER_LEN..]).unwrap();
        let body = (good.len() - FOOTER_LEN) as u64;
        let mut cases = vec![
            (INDEX_LEN, u64::MAX),
            (INDEX_LEN, body - ft.index_off + 1),
            (INDEX_OFF, u64::MAX),
            (INDEX_OFF, body),
            (BLOOM_LEN, u64::MAX),
            (BLOOM_LEN, body - ft.bloom_off + 1),
            (BLOOM_OFF, u64::MAX - ft.bloom_len + 1),
            (SIDECAR_LEN, u64::MAX),
            (SIDECAR_OFF, body - ft.sidecar_len + 1),
        ];
        if version == RUN_SIDECAR_VERSION {
            cases.push((SIDECAR_LEN, body - ft.sidecar_off + 1));
        }
        for (at, value) in cases {
            let what = format!("v{version} footer field at {at} = {value}");
            let mut bytes = good.clone();
            restamp(&mut bytes, at, value);
            let (dev, mut alloc) = dev_alloc(2048, 4);
            let handle = write_run(&*dev, &mut alloc, &bytes).unwrap();
            let seg_lbas: Vec<u64> = handle.segs.iter().map(|s| alloc.segment_range(*s).0).collect();
            let seg_bytes = 4 * BS;
            assert_eq!(reason(DeviceRun::open(dev.clone(), &alloc, handle.clone())), OUTSIDE, "{what}");
            assert_eq!(reason(DeviceRun::open_scan(dev.clone(), &alloc, handle.clone())), OUTSIDE, "{what}");
            assert_eq!(reason(read_run_parts(&*dev, &handle, &seg_lbas, seg_bytes)), OUTSIDE, "{what}");
            assert_eq!(reason(read_run_range_tombstones(&*dev, &alloc, &handle, |_| {})), OUTSIDE, "{what}");
            assert_eq!(Run::parse(bytes).err(), OUTSIDE, "{what}");
        }
        let (dev, mut alloc) = dev_alloc(2048, 4);
        let handle = write_run(&*dev, &mut alloc, &good).unwrap();
        assert!(DeviceRun::open(dev, &alloc, handle).is_ok());
        assert!(Run::parse(good).is_ok());
    }
}
