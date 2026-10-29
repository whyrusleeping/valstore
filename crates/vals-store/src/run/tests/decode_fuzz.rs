use super::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

fn gen_entries(rng: &mut Rng) -> Vec<SnapshotEntry> {
    let n = rng.range(0, 40) as usize;
    let mut keys = std::collections::BTreeSet::new();
    for _ in 0..n {
        let len = rng.range(0, 12) as usize;
        let key: Vec<u8> = (0..len).map(|_| b"abc"[rng.range(0, 2) as usize]).collect();
        keys.insert(key);
    }
    keys.into_iter()
        .map(|key| SnapshotEntry {
            key,
            ptr: ValuePtr { segment: SegmentId(rng.next()), offset: rng.next() as u32, len: rng.next() as u32 },
            seq: rng.next(),
            version: (rng.next() as u128) << 64 | rng.next() as u128,
            tombstone: rng.next() & 1 == 1,
        })
        .collect()
}

fn mutate(rng: &mut Rng, mut body: Vec<u8>) -> Vec<u8> {
    let small = |rng: &mut Rng| [0u32, 1, 2, 3, 7, 12, 40, u32::MAX, u32::MAX >> 1][rng.range(0, 8) as usize];
    match rng.range(0, 5) {
        0 if !body.is_empty() => {
            for _ in 0..rng.range(1, 4) {
                let i = rng.range(0, body.len() as u64 - 1) as usize;
                body[i] ^= rng.next() as u8 | 1;
            }
        }
        1 if !body.is_empty() => {
            let n = rng.range(0, body.len() as u64 - 1) as usize;
            body.truncate(n);
        }
        2 => {
            for _ in 0..rng.range(1, 8) {
                body.push(rng.next() as u8);
            }
        }
        3 if body.len() >= 4 => {
            let i = rng.range(0, body.len() as u64 - 4) as usize;
            body[i..i + 4].copy_from_slice(&small(rng).to_le_bytes());
        }
        4 if body.len() >= 4 => {
            let count = small(rng).min(rng.range(0, 48) as u32);
            body[..4].copy_from_slice(&count.to_le_bytes());
        }
        _ => {
            if let Some(b) = body.last_mut() {
                *b ^= 0x80;
            }
        }
    }
    body
}

fn reseal(mut body: Vec<u8>) -> Vec<u8> {
    let crc = crc::crc32(&body);
    body.extend_from_slice(&crc.to_le_bytes());
    body
}

fn drain(cursor: Result<BlockCursor, &'static str>) -> Result<Vec<SnapshotEntry>, &'static str> {
    let mut cursor = cursor?;
    let mut out = Vec::new();
    while let Some(e) = cursor.next_entry()? {
        out.push(e);
    }
    Ok(out)
}

fn walk(block: &[u8]) -> Result<Vec<(Vec<u8>, u128)>, SnapshotError> {
    let mut fence = FenceBuilder::default();
    fence.push(b"", 0, block.len() as u32).unwrap();
    let mut walker = BlockWalker::new(Arc::new(fence.finish()), 0);
    walker.install(block.to_vec(), 0, 1);
    let mut out = Vec::new();
    loop {
        match walker.advance(&mut |_| true)? {
            Walk::Entry(version) => out.push((walker.key.clone(), version)),
            Walk::Done => return Ok(out),
            Walk::Window | Walk::Blocked => panic!("a one-block window walk neither refills nor blocks"),
        }
    }
}

fn check_block(block: &[u8], probes: &[Vec<u8>]) {
    let decoded = decode_block(block);
    assert_eq!(drain(BlockCursor::new(block.to_vec(), None, false)), decoded, "cursor drain diverged from decode_block");
    if block.len() >= 8 && block_crc_ok(block) {
        assert_eq!(decode_block_body(&block[..block.len() - 4]), decoded, "body decode diverged from decode_block");
    }
    let walked = walk(block);
    let first = block_first_key(block);
    let lookups: Vec<_> = probes.iter().map(|p| block_lookup(block, p)).collect();
    let seeks: Vec<_> = probes
        .iter()
        .flat_map(|p| [false, true].map(|excl| drain(BlockCursor::new(block.to_vec(), Some(p), excl))))
        .collect();
    let Ok(entries) = decoded else {
        assert!(walked.is_err(), "walker accepted a block decode_block refuses");
        return;
    };
    if !entries.windows(2).all(|w| w[0].key < w[1].key) {
        assert!(
            matches!(walked, Err(SnapshotError::Corrupt("run keys not strictly increasing"))),
            "walker accepted unordered keys: {walked:?}"
        );
        return;
    }
    let versions: Vec<_> = entries.iter().map(|e| (e.key.clone(), e.version)).collect();
    assert_eq!(walked.unwrap(), versions, "walker diverged from decode_block");
    assert_eq!(first, Ok(entries.first().map(|e| e.key.clone())), "first key diverged");
    for (p, got) in probes.iter().zip(lookups) {
        assert_eq!(got, Ok(entries.iter().find(|e| &e.key == p).cloned()), "lookup of {p:?} diverged");
    }
    let mut seeks = seeks.into_iter();
    for p in probes {
        for excl in [false, true] {
            let want: Vec<_> = entries.iter().filter(|e| if excl { e.key > *p } else { e.key >= *p }).cloned().collect();
            assert_eq!(seeks.next().unwrap(), Ok(want), "seek to {p:?} (exclusive {excl}) diverged");
        }
    }
}

#[test]
fn block_decoders_agree_and_never_panic_on_resealed_mutations() {
    let mut rng = Rng(0x0536_B10C_F022_0001);
    for round in 0..4000u32 {
        let entries = gen_entries(&mut rng);
        let block = encode_block(&entries);
        let mut probes: Vec<Vec<u8>> = entries.iter().map(|e| e.key.clone()).collect();
        for _ in 0..4 {
            let len = rng.range(0, 12) as usize;
            probes.push((0..len).map(|_| b"abcd"[rng.range(0, 3) as usize]).collect());
        }
        if round % 64 == 0 {
            assert_eq!(decode_block(&block).as_ref(), Ok(&entries));
            check_block(&block, &probes);
        }
        let body = mutate(&mut rng, block[..block.len() - 4].to_vec());
        let bytes = if rng.range(0, 7) == 0 { body } else { reseal(body) };
        if catch_unwind(AssertUnwindSafe(|| check_block(&bytes, &probes))).is_err() {
            panic!("round {round}: block decoders panicked or diverged on {} bytes: {bytes:02x?}", bytes.len());
        }
    }
}

#[test]
fn striped_readers_return_the_logical_bytes_across_segments() {
    let mut rng = Rng(0x0536_57A1_9ED0_0001);
    for _ in 0..150 {
        let bs = [256usize, 512, 4096][rng.range(0, 2) as usize];
        let seg_blocks = rng.range(1, 5);
        let segs = rng.range(1, 5) as usize;
        let seg_bytes = seg_blocks as usize * bs;
        let dev = MemBlockDevice::new(bs, 2 * segs as u64 * seg_blocks);
        let mut slots: Vec<u64> = (0..2 * segs as u64).collect();
        for i in (1..slots.len()).rev() {
            slots.swap(i, rng.range(0, i as u64) as usize);
        }
        let lbas: Vec<u64> = slots[..segs].iter().map(|s| s * seg_blocks).collect();
        let total = segs * seg_bytes;
        let logical: Vec<u8> = (0..total).map(|_| rng.next() as u8).collect();
        for (i, lba) in lbas.iter().enumerate() {
            dev.write_at(*lba, &logical[i * seg_bytes..(i + 1) * seg_bytes]).unwrap();
        }
        let loc = RangeLocator { seg_lbas: lbas.iter().copied().collect(), seg_bytes, block_size: bs, off: 0, len: total };
        for _ in 0..20 {
            let off = rng.range(0, total as u64) as usize;
            let len = rng.range(0, (total - off) as u64) as usize;
            let want = &logical[off..off + len];
            let qd = rng.range(1, 8) as usize;
            assert_eq!(read_run_range_lbas(&dev, &lbas, seg_bytes, off as u64, len).unwrap(), want);
            assert_eq!(read_run_range_batched(&dev, &lbas, seg_bytes, off as u64, len, qd).unwrap(), want);
            assert_eq!(block_on(loc.read_range(&dev, off, len)).unwrap(), want);
        }
        let past = |r: Result<Vec<u8>, SnapshotError>| matches!(r, Err(SnapshotError::Corrupt("run offset past its segments")));
        assert!(past(read_run_range_lbas(&dev, &lbas, seg_bytes, total as u64 - 1, 2)));
        assert!(past(read_run_range_batched(&dev, &lbas, seg_bytes, total as u64 - 1, 2, 4)));
    }
}
