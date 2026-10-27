use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};

use vals_spdk::{BlockDevice, MemBlockDevice};
use vals_store::alloc::{Allocator, SegmentId};
use vals_store::vlog::{
    decode_record, decode_record_value, decode_seg_header, replay_segment, Record, RecordKind,
    ValueLog, HEADER_LEN, SEG_HEADER_LEN,
};

const BS: usize = 256;
const SEG_BLOCKS: u64 = 4;
const DEVICE_BLOCKS: u64 = 512;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }

    fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    fn bool(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }

    fn buf(&mut self, max: usize) -> Vec<u8> {
        let n = self.below(max as u64 + 1) as usize;
        (0..n).map(|_| self.byte()).collect()
    }

    fn u128(&mut self) -> u128 {
        (self.next_u64() as u128) << 64 | self.next_u64() as u128
    }

    fn kind(&mut self) -> RecordKind {
        if self.bool() {
            RecordKind::Put
        } else {
            RecordKind::Delete
        }
    }
}

struct Appended {
    seq: u64,
    kind: RecordKind,
    tombstone: bool,
    version: u128,
    key: Vec<u8>,
    value: Vec<u8>,
}

fn gen_record(r: &mut Rng) -> (RecordKind, bool, u128, Vec<u8>, Vec<u8>) {
    (r.kind(), r.bool(), r.u128(), r.buf(24), r.buf(600))
}

fn build_batch(
    r: &mut Rng,
) -> (
    MemBlockDevice,
    Allocator,
    Vec<Appended>,
    BTreeSet<SegmentId>,
) {
    let dev = MemBlockDevice::new(BS, DEVICE_BLOCKS);
    let mut alloc = Allocator::new(DEVICE_BLOCKS, SEG_BLOCKS, 0);
    let mut log = ValueLog::new(BS, SEG_BLOCKS, 0, 0);

    let n = 1 + r.below(30) as usize;
    let mut appended = Vec::with_capacity(n);
    let mut segs = BTreeSet::new();
    for _ in 0..n {
        let (kind, tombstone, version, key, value) = gen_record(r);
        let (seq, ptr) = log
            .append_tombstone(&dev, &mut alloc, kind, tombstone, version, &key, &value)
            .expect("record fits a segment");
        segs.insert(ptr.segment);
        appended.push(Appended {
            seq,
            kind,
            tombstone,
            version,
            key,
            value,
        });
    }
    log.sync(&dev).unwrap();
    (dev, alloc, appended, segs)
}

fn read_segment(dev: &MemBlockDevice, alloc: &Allocator, seg: SegmentId) -> Vec<u8> {
    let (start, blocks) = alloc.segment_range(seg);
    let mut buf = vec![0u8; blocks as usize * BS];
    dev.read_at(start, &mut buf).unwrap();
    buf
}

fn mutate(r: &mut Rng, base: &[u8]) -> Vec<u8> {
    let mut b = base.to_vec();
    match r.below(5) {
        0 => {
            if !b.is_empty() {
                for _ in 0..1 + r.below(4) {
                    let i = r.below(b.len() as u64) as usize;
                    b[i] ^= r.byte();
                }
            }
        }
        1 => {
            if !b.is_empty() {
                let n = r.below(b.len() as u64) as usize;
                b.truncate(n);
            }
        }
        2 => {
            for _ in 0..r.below(8) {
                b.push(r.byte());
            }
        }
        3 => {
            if b.len() >= 4 {
                let i = r.below((b.len() - 3) as u64) as usize;
                b[i..i + 4].copy_from_slice(&u32::MAX.to_le_bytes());
            }
        }
        _ => {
            for byte in b.iter_mut().skip(SEG_HEADER_LEN) {
                *byte = r.byte();
            }
        }
    }
    b
}

fn assert_no_panic(label: &str, bytes: &[u8]) {
    let outcomes = [
        catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_record(bytes);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_record_value(bytes);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = decode_seg_header(bytes);
        })),
        catch_unwind(AssertUnwindSafe(|| {
            let _ = replay_segment(bytes);
        })),
    ];
    if outcomes.iter().any(|o| o.is_err()) {
        panic!(
            "decoder panicked ({label}) on {} bytes: {:02x?}",
            bytes.len(),
            bytes
        );
    }
}

#[test]
fn replay_roundtrips_over_random_batches() {
    for seed in 0..400u64 {
        let mut r = Rng::new(seed.wrapping_mul(0xD1B5_4A32_D192_ED03).wrapping_add(1));
        let (dev, alloc, appended, segs) = build_batch(&mut r);

        let mut recovered: Vec<Record> = Vec::new();
        for seg in &segs {
            let buf = read_segment(&dev, &alloc, *seg);
            let (hdr, recs) = replay_segment(&buf).expect("valid segment replays");
            for (off, rec) in recs {
                assert_eq!(
                    rec.epoch, hdr.epoch,
                    "replay must only return records matching the segment epoch (seed {seed})"
                );
                let off = off as usize;
                let len = HEADER_LEN + rec.key.len() + rec.value.len();
                let re = decode_record(&buf[off..off + len])
                    .expect("recovered record re-decodes from its offset");
                assert_eq!(re, rec, "re-decode mismatch at seed {seed}");
                recovered.push(rec);
            }
        }

        assert_eq!(
            recovered.len(),
            appended.len(),
            "every appended record must survive replay (seed {seed})"
        );
        for exp in &appended {
            let got = recovered
                .iter()
                .find(|r| r.seq == exp.seq)
                .unwrap_or_else(|| panic!("record seq {} lost at seed {seed}", exp.seq));
            assert_eq!(got.key, exp.key, "key mismatch seq {} seed {seed}", exp.seq);
            assert_eq!(
                got.value, exp.value,
                "value mismatch seq {} seed {seed}",
                exp.seq
            );
            assert_eq!(
                got.kind, exp.kind,
                "kind mismatch seq {} seed {seed}",
                exp.seq
            );
            assert_eq!(
                got.tombstone, exp.tombstone,
                "tombstone flag mismatch seq {} seed {seed}",
                exp.seq
            );
            assert_eq!(
                got.version, exp.version,
                "version mismatch seq {} seed {seed}",
                exp.seq
            );
        }
    }
}

#[test]
fn decoders_never_panic_on_arbitrary_bytes() {
    assert_no_panic("empty", &[]);
    for b in 0u16..=255 {
        assert_no_panic("single-byte", &[b as u8]);
        assert_no_panic("two-byte", &[b as u8, 0xff]);
    }
    let mut r = Rng::new(0xFACE_FEED_1234_5678);
    for _ in 0..5000 {
        let bytes = r.buf(2 * BS);
        assert_no_panic("random", &bytes);
    }
}

#[test]
fn replay_never_panics_on_mutated_segments_and_only_truncates() {
    let mut r = Rng::new(0x0BAD_C0DE_DEAD_BEEF);
    for seed in 0..1200u64 {
        let (dev, alloc, _appended, _segs) = build_batch(&mut r);
        let base = read_segment(&dev, &alloc, SegmentId(0));
        let baseline = replay_segment(&base).expect("valid segment replays");

        let m = mutate(&mut r, &base);

        assert_no_panic("mutated-segment", &m);

        let replayed = catch_unwind(AssertUnwindSafe(|| replay_segment(&m)));
        if let Ok(Ok((_, recs))) = replayed {
            assert!(
                recs.len() <= baseline.1.len(),
                "mutation extended replay at seed {seed}: {} > {}",
                recs.len(),
                baseline.1.len()
            );
            for (i, (off, rec)) in recs.iter().enumerate() {
                let (base_off, base_rec) = &baseline.1[i];
                assert_eq!(
                    (off, &rec.key, &rec.value, rec.seq),
                    (base_off, &base_rec.key, &base_rec.value, base_rec.seq),
                    "mutated replay diverged from the valid prefix at index {i}, seed {seed}"
                );
            }
        }
    }
}
