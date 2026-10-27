use std::collections::HashMap;

use vals_spdk::{BlockDevice, BlockError, ReadOp};

use crate::alloc::{Allocator, SegmentId};
use crate::crc;

const MAN_CHAIN_MAGIC: u32 = 0x434e_4d56;
const CHAIN_VERSION: u16 = 1;
const CHAIN_HDR_LEN: usize = 46;
const NO_NEXT: u64 = u64::MAX;

#[derive(Debug)]
pub enum SnapshotError {
    Io(BlockError),
    DeviceFull,
    Corrupt(&'static str),
    Unsupported(&'static str),
    EncodeIntegrity(Box<crate::run::EncodeIntegrityError>),
}

impl std::fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotError::Io(e) => write!(f, "snapshot io error: {e}"),
            SnapshotError::DeviceFull => write!(f, "no free segments for snapshot"),
            SnapshotError::Corrupt(why) => write!(f, "corrupt snapshot: {why}"),
            SnapshotError::Unsupported(what) => write!(
                f,
                "device holds an intact {what} this release does not know \
                 (written by a newer release?); refusing to open"
            ),
            SnapshotError::EncodeIntegrity(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SnapshotError {}

impl From<BlockError> for SnapshotError {
    fn from(e: BlockError) -> Self {
        SnapshotError::Io(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub generation: u64,
    pub segs: Vec<SegmentId>,
}

#[derive(Debug, Clone, Copy)]
struct ChainHeader {
    generation: u64,
    seg_index: u32,
    seg_count: u32,
    total_len: u64,
    next_seg: u64,
    chunk_len: u32,
}

fn encode_chain_header(h: &ChainHeader, magic: u32) -> [u8; CHAIN_HDR_LEN] {
    let mut b = [0u8; CHAIN_HDR_LEN];
    b[0..4].copy_from_slice(&magic.to_le_bytes());
    b[4..6].copy_from_slice(&CHAIN_VERSION.to_le_bytes());
    b[6..14].copy_from_slice(&h.generation.to_le_bytes());
    b[14..18].copy_from_slice(&h.seg_index.to_le_bytes());
    b[18..22].copy_from_slice(&h.seg_count.to_le_bytes());
    b[22..30].copy_from_slice(&h.total_len.to_le_bytes());
    b[30..38].copy_from_slice(&h.next_seg.to_le_bytes());
    b[38..42].copy_from_slice(&h.chunk_len.to_le_bytes());
    let header_crc = crc::crc32(&b[0..42]);
    b[42..46].copy_from_slice(&header_crc.to_le_bytes());
    b
}

enum ChainHeaderDecode {
    Header(ChainHeader),
    UnknownVersion { generation: u64, seg_index: u32 },
    Absent,
}

fn decode_chain_header(block: &[u8], magic: u32) -> ChainHeaderDecode {
    if block.len() < CHAIN_HDR_LEN {
        return ChainHeaderDecode::Absent;
    }
    if u32::from_le_bytes(block[0..4].try_into().unwrap()) != magic {
        return ChainHeaderDecode::Absent;
    }
    let stored_crc = u32::from_le_bytes(block[42..46].try_into().unwrap());
    if crc::crc32(&block[0..42]) != stored_crc {
        return ChainHeaderDecode::Absent;
    }
    if u16::from_le_bytes(block[4..6].try_into().unwrap()) != CHAIN_VERSION {
        return ChainHeaderDecode::UnknownVersion {
            generation: u64::from_le_bytes(block[6..14].try_into().unwrap()),
            seg_index: u32::from_le_bytes(block[14..18].try_into().unwrap()),
        };
    }
    ChainHeaderDecode::Header(ChainHeader {
        generation: u64::from_le_bytes(block[6..14].try_into().unwrap()),
        seg_index: u32::from_le_bytes(block[14..18].try_into().unwrap()),
        seg_count: u32::from_le_bytes(block[18..22].try_into().unwrap()),
        total_len: u64::from_le_bytes(block[22..30].try_into().unwrap()),
        next_seg: u64::from_le_bytes(block[30..38].try_into().unwrap()),
        chunk_len: u32::from_le_bytes(block[38..42].try_into().unwrap()),
    })
}

impl ChainHeaderDecode {
    #[cfg(test)]
    fn header(self) -> Option<ChainHeader> {
        match self {
            ChainHeaderDecode::Header(h) => Some(h),
            _ => None,
        }
    }
}

fn segment_capacity(dev: &impl BlockDevice, alloc: &Allocator) -> usize {
    alloc.blocks_per_segment() as usize * dev.block_size() - CHAIN_HDR_LEN
}

pub(crate) fn write_chain(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    bytes: &[u8],
    magic: u32,
    generation: u64,
    prev: Option<&LiveSnapshot>,
) -> Result<LiveSnapshot, SnapshotError> {
    let cap = segment_capacity(dev, alloc);
    assert!(cap > 0, "segment too small to hold a chain header");
    let n = bytes.len().div_ceil(cap).max(1);

    let mut segs = Vec::with_capacity(n);
    for _ in 0..n {
        match alloc.allocate() {
            Some(s) => segs.push(s),
            None => {
                for s in &segs {
                    let _ = alloc.free(*s);
                }
                return Err(SnapshotError::DeviceFull);
            }
        }
    }

    let seg_bytes = alloc.blocks_per_segment() as usize * dev.block_size();
    let total_len = bytes.len() as u64;
    let seg_count = n as u32;

    let seg_lbas: Vec<u64> = segs.iter().map(|s| alloc.segment_range(*s).0).collect();
    let next_segs: Vec<u64> = (0..n)
        .map(|i| if i + 1 < n { segs[i + 1].0 } else { NO_NEXT })
        .collect();

    let write_seg = |i: usize| -> Result<(), SnapshotError> {
        let start = i * cap;
        let end = (start + cap).min(bytes.len());
        let chunk = &bytes[start..end];
        let hdr = ChainHeader {
            generation,
            seg_index: i as u32,
            seg_count,
            total_len,
            next_seg: next_segs[i],
            chunk_len: chunk.len() as u32,
        };
        let mut buf = vec![0u8; seg_bytes];
        buf[0..CHAIN_HDR_LEN].copy_from_slice(&encode_chain_header(&hdr, magic));
        buf[CHAIN_HDR_LEN..CHAIN_HDR_LEN + chunk.len()].copy_from_slice(chunk);
        crate::attribution::write_bytes(5, buf.len());
        dev.write_at(seg_lbas[i], &buf)?;
        Ok(())
    };

    let write_phase = || -> Result<(), SnapshotError> {
        for i in (1..n).rev() {
            write_seg(i)?;
        }
        if n > 1 {
            dev.flush()?;
        }
        write_seg(0)?;
        dev.flush()?;
        Ok(())
    };
    if let Err(e) = write_phase() {
        let zero = vec![0u8; dev.block_size()];
        for s in &segs {
            let (lba, _) = alloc.segment_range(*s);
            crate::attribution::write_bytes(5, zero.len());
            let _ = dev.write_at(lba, &zero);
            let _ = alloc.free(*s);
        }
        return Err(e);
    }

    if let Some(prev) = prev {
        let zero = vec![0u8; dev.block_size()];
        for s in &prev.segs {
            let (lba, _) = alloc.segment_range(*s);
            crate::attribution::write_bytes(5, zero.len());
            let _ = dev.write_at(lba, &zero);
            let _ = alloc.free(*s);
        }
    }

    Ok(LiveSnapshot { generation, segs })
}

pub fn write_manifest(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    manifest: &crate::manifest::Manifest,
    generation: u64,
    prev: Option<&LiveSnapshot>,
) -> Result<LiveSnapshot, SnapshotError> {
    let bytes = crate::manifest::encode(manifest);
    let barrier = alloc.dealloc_barrier_begin();
    let live = write_chain(dev, alloc, &bytes, MAN_CHAIN_MAGIC, generation, prev)?;
    alloc.dealloc_barrier_commit(barrier);
    Ok(live)
}

pub fn load_manifest(
    dev: &impl BlockDevice,
    alloc: &Allocator,
) -> Result<Option<(crate::manifest::Manifest, LiveSnapshot)>, SnapshotError> {
    load_latest_chain(dev, alloc, MAN_CHAIN_MAGIC, |b| match crate::manifest::decode(b) {
        Ok(m) => Ok(Some(m)),
        Err(crate::manifest::UNSUPPORTED_VERSION) => Err(SnapshotError::Unsupported("manifest version")),
        Err(_) => Ok(None),
    })
}

pub(crate) fn load_latest_chain<T>(
    dev: &impl BlockDevice,
    alloc: &Allocator,
    magic: u32,
    parse: impl Fn(&[u8]) -> Result<Option<T>, SnapshotError>,
) -> Result<Option<(T, LiveSnapshot)>, SnapshotError> {
    let block_size = dev.block_size();
    let num_segs = alloc.num_segments();

    let mut headers: HashMap<SegmentId, ChainHeader> = HashMap::new();
    let mut heads_by_gen: HashMap<u64, SegmentId> = HashMap::new();
    let mut unknown_head_gen: Option<u64> = None;
    const BATCH: usize = 4096;
    let mut bufs: Vec<Vec<u8>> = (0..BATCH.min(num_segs.max(1) as usize))
        .map(|_| vec![0u8; block_size])
        .collect();
    let mut s: u64 = 0;
    while s < num_segs {
        let n = ((num_segs - s) as usize).min(BATCH);
        {
            let mut ops: Vec<ReadOp> = bufs[..n]
                .iter_mut()
                .enumerate()
                .map(|(i, b)| ReadOp {
                    lba: alloc.segment_range(SegmentId(s + i as u64)).0,
                    buf: b.as_mut_slice(),
                })
                .collect();
            dev.read_batch(&mut ops)?;
        }
        for (i, b) in bufs[..n].iter().enumerate() {
            match decode_chain_header(b, magic) {
                ChainHeaderDecode::Header(h) => {
                    let seg = SegmentId(s + i as u64);
                    if h.seg_index == 0 {
                        heads_by_gen.entry(h.generation).or_insert(seg);
                    }
                    headers.insert(seg, h);
                }
                ChainHeaderDecode::UnknownVersion { generation, seg_index: 0 } => {
                    unknown_head_gen = Some(unknown_head_gen.map_or(generation, |g| g.max(generation)));
                }
                ChainHeaderDecode::UnknownVersion { .. } | ChainHeaderDecode::Absent => {}
            }
        }
        s += n as u64;
    }

    let mut gens: Vec<u64> = heads_by_gen.keys().copied().collect();
    gens.sort_unstable_by(|a, b| b.cmp(a));
    for gen in gens {
        if unknown_head_gen.is_some_and(|u| u >= gen) {
            return Err(SnapshotError::Unsupported("chain header version"));
        }
        let head = heads_by_gen[&gen];
        if let Some((payload, live)) = load_chain(dev, alloc, &headers, head, gen)? {
            if let Some(v) = parse(&payload)? {
                return Ok(Some((v, live)));
            }
        }
    }
    if unknown_head_gen.is_some() {
        return Err(SnapshotError::Unsupported("chain header version"));
    }
    Ok(None)
}

fn load_chain(
    dev: &impl BlockDevice,
    alloc: &Allocator,
    headers: &HashMap<SegmentId, ChainHeader>,
    head: SegmentId,
    gen: u64,
) -> Result<Option<(Vec<u8>, LiveSnapshot)>, SnapshotError> {
    let head_hdr = headers[&head];
    let seg_count = head_hdr.seg_count as usize;
    let block_size = dev.block_size();
    let seg_bytes = alloc.blocks_per_segment() as usize * block_size;

    let mut payload = Vec::with_capacity(head_hdr.total_len as usize);
    let mut segs = Vec::with_capacity(seg_count);
    let mut cur = head;
    let mut buf = vec![0u8; seg_bytes];
    for expected_index in 0..seg_count {
        let Some(h) = headers.get(&cur) else {
            return Ok(None);
        };
        if h.generation != gen || h.seg_index as usize != expected_index {
            return Ok(None);
        }
        let (lba, _) = alloc.segment_range(cur);
        dev.read_at(lba, &mut buf)?;
        let chunk_len = h.chunk_len as usize;
        if CHAIN_HDR_LEN + chunk_len > buf.len() {
            return Ok(None);
        }
        payload.extend_from_slice(&buf[CHAIN_HDR_LEN..CHAIN_HDR_LEN + chunk_len]);
        segs.push(cur);
        if h.next_seg == NO_NEXT {
            if expected_index + 1 != seg_count {
                return Ok(None);
            }
            break;
        }
        cur = SegmentId(h.next_seg);
    }

    if payload.len() as u64 != head_hdr.total_len || segs.len() != seg_count {
        return Ok(None);
    }
    Ok(Some((
        payload,
        LiveSnapshot {
            generation: gen,
            segs,
        },
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use vals_spdk::MemBlockDevice;

    const BS: usize = 4096;

    fn dev_alloc(num_blocks: u64, seg_blocks: u64) -> (MemBlockDevice, Allocator) {
        let dev = MemBlockDevice::new(BS, num_blocks);
        let alloc = Allocator::new(num_blocks, seg_blocks, 0);
        (dev, alloc)
    }

    use crate::manifest::{Manifest, RunMeta};
    use crate::run::RunHandle;

    fn manifest_with(runs: usize) -> Manifest {
        Manifest {
            generation: 0,
            covered_seq: 4242,
            covered_epoch: 17,
            runs: (0..runs)
                .map(|i| RunMeta {
                    handle: RunHandle {
                        segs: vec![SegmentId(i as u64 * 3 + 1)],
                        len: 1000 + i as u64,
                    },
                    min_key: format!("k{i:04}").into_bytes(),
                    max_key: format!("k{i:04}zzz").into_bytes(),
                    entry_count: 100 + i as u64,
                    level: (i % 3) as u32,
                    value_segs: None,
                    value_bytes: None,
                    tombstone_min_version: None,
                    value_seg_bytes: None,
                })
                .collect(),
        }
    }

    #[test]
    fn manifest_round_trips_through_the_chain() {
        let (dev, mut alloc) = dev_alloc(256, 16);
        let m = manifest_with(5);
        let live = write_manifest(&dev, &mut alloc, &m, 1, None).unwrap();
        assert_eq!(live.generation, 1);
        assert_eq!(live.segs.len(), 1, "small manifest fits one segment");
        let (loaded, loaded_live) = load_manifest(&dev, &alloc).unwrap().expect("a manifest");
        assert_eq!(loaded, m);
        assert_eq!(loaded_live, live);
    }

    #[test]
    fn round_trip_multi_segment_chain() {
        let (dev, mut alloc) = dev_alloc(512, 2);
        let m = manifest_with(200);
        let live = write_manifest(&dev, &mut alloc, &m, 1, None).unwrap();
        assert!(
            live.segs.len() > 1,
            "expected a multi-segment chain, got {}",
            live.segs.len()
        );

        let (loaded, _) = load_manifest(&dev, &alloc).unwrap().expect("a manifest");
        assert_eq!(loaded, m);
    }

    #[test]
    fn empty_device_has_no_manifest() {
        let (dev, alloc) = dev_alloc(256, 16);
        assert!(load_manifest(&dev, &alloc).unwrap().is_none());
    }

    #[test]
    fn newer_manifest_generation_supersedes_and_frees_old() {
        let (dev, mut alloc) = dev_alloc(256, 16);
        let m1 = manifest_with(3);
        let live1 = write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let used_after_1 = alloc.used_segments();

        let m2 = manifest_with(7);
        let live2 = write_manifest(&dev, &mut alloc, &m2, 2, Some(&live1)).unwrap();
        assert!(live1.segs.iter().all(|s| alloc.is_free(*s)));
        assert_eq!(
            alloc.used_segments(),
            used_after_1,
            "old freed, new allocated"
        );

        let (loaded, loaded_live) = load_manifest(&dev, &alloc).unwrap().expect("a manifest");
        assert_eq!(loaded, m2);
        assert_eq!(loaded_live.generation, 2);
        assert_eq!(loaded_live, live2);
    }

    #[test]
    fn incomplete_higher_generation_is_ignored() {
        let (dev, mut alloc) = dev_alloc(256, 16);
        let m1 = manifest_with(3);
        write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();

        let frag = alloc.allocate().unwrap();
        let seg_bytes = alloc.blocks_per_segment() as usize * BS;
        let mut buf = vec![0u8; seg_bytes];
        let hdr = ChainHeader {
            generation: 2,
            seg_index: 1,
            seg_count: 2,
            total_len: 9999,
            next_seg: NO_NEXT,
            chunk_len: 0,
        };
        buf[0..CHAIN_HDR_LEN].copy_from_slice(&encode_chain_header(&hdr, MAN_CHAIN_MAGIC));
        let (lba, _) = alloc.segment_range(frag);
        dev.write_at(lba, &buf).unwrap();

        let (loaded, loaded_live) = load_manifest(&dev, &alloc)
            .unwrap()
            .expect("falls back to gen 1");
        assert_eq!(loaded, m1);
        assert_eq!(loaded_live.generation, 1);
    }

    #[test]
    fn corrupt_head_chunk_falls_back() {
        let (dev, mut alloc) = dev_alloc(512, 16);
        let m1 = manifest_with(3);
        write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let m2 = manifest_with(4);
        let live2 = write_manifest(&dev, &mut alloc, &m2, 2, None).unwrap();

        let (lba, _) = alloc.segment_range(live2.segs[0]);
        let mut buf = vec![0u8; alloc.blocks_per_segment() as usize * BS];
        dev.read_at(lba, &mut buf).unwrap();
        buf[CHAIN_HDR_LEN + 4] ^= 0xff;
        dev.write_at(lba, &buf).unwrap();

        let (loaded, loaded_live) = load_manifest(&dev, &alloc).unwrap().expect("falls back");
        assert_eq!(loaded, m1);
        assert_eq!(loaded_live.generation, 1);
    }

    fn write_unknown_version_head(dev: &MemBlockDevice, alloc: &mut Allocator, generation: u64) {
        let seg = alloc.allocate().unwrap();
        let hdr = ChainHeader {
            generation,
            seg_index: 0,
            seg_count: 1,
            total_len: 0,
            next_seg: NO_NEXT,
            chunk_len: 0,
        };
        let mut b = encode_chain_header(&hdr, MAN_CHAIN_MAGIC);
        b[4..6].copy_from_slice(&(CHAIN_VERSION + 1).to_le_bytes());
        let crc = crc::crc32(&b[0..42]);
        b[42..46].copy_from_slice(&crc.to_le_bytes());
        let mut buf = vec![0u8; alloc.blocks_per_segment() as usize * BS];
        buf[..CHAIN_HDR_LEN].copy_from_slice(&b);
        dev.write_at(alloc.segment_range(seg).0, &buf).unwrap();
    }

    #[test]
    fn unknown_chain_version_fails_loud_only_when_not_superseded() {
        let (dev, mut alloc) = dev_alloc(256, 16);
        let m1 = manifest_with(3);
        write_manifest(&dev, &mut alloc, &m1, 5, None).unwrap();
        write_unknown_version_head(&dev, &mut alloc, 4);
        let (loaded, live) = load_manifest(&dev, &alloc).unwrap().expect("gen 5 supersedes the stale head");
        assert_eq!((loaded, live.generation), (m1, 5));

        write_unknown_version_head(&dev, &mut alloc, 6);
        assert!(matches!(load_manifest(&dev, &alloc), Err(SnapshotError::Unsupported("chain header version"))));

        let (dev, mut alloc) = dev_alloc(256, 16);
        write_unknown_version_head(&dev, &mut alloc, 1);
        assert!(matches!(load_manifest(&dev, &alloc), Err(SnapshotError::Unsupported("chain header version"))));
    }

    #[test]
    fn device_full_rolls_back_allocation() {
        let (dev, mut alloc) = dev_alloc(8, 2);
        let before = alloc.free_segments();
        let big = manifest_with(2000);
        let err = write_manifest(&dev, &mut alloc, &big, 1, None).unwrap_err();
        assert!(matches!(err, SnapshotError::DeviceFull));
        assert_eq!(alloc.free_segments(), before, "reservation rolled back");
    }

    #[test]
    fn distinct_magics_do_not_cross_decode() {
        const TEST_MAGIC: u32 = 0x5453_4554;
        assert_ne!(TEST_MAGIC, MAN_CHAIN_MAGIC);

        let (dev, mut alloc) = dev_alloc(256, 16);
        let m = manifest_with(4);
        write_manifest(&dev, &mut alloc, &m, 1, None).unwrap();
        write_chain(&dev, &mut alloc, b"foreign payload", TEST_MAGIC, 1, None).unwrap();

        let (loaded, _) = load_manifest(&dev, &alloc)
            .unwrap()
            .expect("manifest present");
        assert_eq!(loaded, m);
        let (payload, _) = load_latest_chain(&dev, &alloc, TEST_MAGIC, |b| Ok(Some(b.to_vec())))
            .unwrap()
            .expect("foreign chain present");
        assert_eq!(payload, b"foreign payload");
    }

    #[test]
    fn superseded_prev_segments_have_their_headers_wiped() {
        let (dev, mut alloc) = dev_alloc(256, 16);
        let m1 = manifest_with(3);
        let live1 = write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let mut buf = vec![0u8; BS];
        let (lba0, _) = alloc.segment_range(live1.segs[0]);
        dev.read_at(lba0, &mut buf).unwrap();
        assert!(decode_chain_header(&buf, MAN_CHAIN_MAGIC).header().is_some());

        let m2 = manifest_with(7);
        write_manifest(&dev, &mut alloc, &m2, 2, Some(&live1)).unwrap();

        for s in &live1.segs {
            let (lba, _) = alloc.segment_range(*s);
            dev.read_at(lba, &mut buf).unwrap();
            assert!(
                decode_chain_header(&buf, MAN_CHAIN_MAGIC).header().is_none(),
                "freed prev segment {s:?} still has a decodable chain header",
            );
        }
    }

    struct FailingDevice {
        inner: MemBlockDevice,
        writes: std::sync::atomic::AtomicUsize,
        fail_on_write: std::sync::atomic::AtomicUsize,
        fail_flush: std::sync::atomic::AtomicBool,
    }
    impl FailingDevice {
        fn new(inner: MemBlockDevice) -> Self {
            use std::sync::atomic::{AtomicBool, AtomicUsize};
            Self {
                inner,
                writes: AtomicUsize::new(0),
                fail_on_write: AtomicUsize::new(0),
                fail_flush: AtomicBool::new(false),
            }
        }
        fn arm_write(&self, nth: usize) {
            use std::sync::atomic::Ordering::Relaxed;
            self.writes.store(0, Relaxed);
            self.fail_on_write.store(nth, Relaxed);
        }
        fn arm_flush(&self) {
            self.fail_flush
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
        fn disarm(&self) {
            use std::sync::atomic::Ordering::Relaxed;
            self.fail_on_write.store(0, Relaxed);
            self.fail_flush.store(false, Relaxed);
        }
    }
    impl BlockDevice for FailingDevice {
        fn block_size(&self) -> usize {
            self.inner.block_size()
        }
        fn num_blocks(&self) -> u64 {
            self.inner.num_blocks()
        }
        fn read_at(&self, lba: u64, buf: &mut [u8]) -> vals_spdk::Result<()> {
            self.inner.read_at(lba, buf)
        }
        fn write_at(&self, lba: u64, buf: &[u8]) -> vals_spdk::Result<()> {
            use std::sync::atomic::Ordering::Relaxed;
            let n = self.writes.fetch_add(1, Relaxed) + 1;
            let fail = self.fail_on_write.load(Relaxed);
            if fail != 0 && n == fail {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected write failure",
                )));
            }
            self.inner.write_at(lba, buf)
        }
        fn flush(&self) -> vals_spdk::Result<()> {
            if self.fail_flush.load(std::sync::atomic::Ordering::Relaxed) {
                return Err(BlockError::Io(std::io::Error::other(
                    "injected flush failure",
                )));
            }
            self.inner.flush()
        }
    }

    #[test]
    fn io_write_error_on_non_head_segment_reclaims_segments() {
        let dev = FailingDevice::new(MemBlockDevice::new(BS, 512));
        let mut alloc = Allocator::new(512, 2, 0);

        let m1 = manifest_with(3);
        let prev = write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let before = alloc.free_segments();

        let m2 = manifest_with(200);
        assert!(
            m2 != m1,
            "sanity: gen-2 manifest differs from the surviving prev"
        );
        dev.arm_write(1);
        let err = write_manifest(&dev, &mut alloc, &m2, 2, Some(&prev)).unwrap_err();
        assert!(matches!(err, SnapshotError::Io(_)), "got {err:?}");
        assert_eq!(
            alloc.free_segments(),
            before,
            "reserved manifest segments returned to the allocator on I/O error"
        );

        dev.disarm();
        let (loaded, loaded_live) = load_manifest(&dev, &alloc)
            .unwrap()
            .expect("prev chain still present");
        assert_eq!(loaded, m1);
        assert_eq!(loaded_live.generation, 1);
        assert_eq!(loaded_live, prev);
    }

    #[test]
    fn io_error_on_head_write_reclaims_segments() {
        let dev = FailingDevice::new(MemBlockDevice::new(BS, 512));
        let mut alloc = Allocator::new(512, 16, 0);

        let m1 = manifest_with(3);
        let prev = write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let before = alloc.free_segments();

        let m2 = manifest_with(5);
        dev.arm_write(1);
        let err = write_manifest(&dev, &mut alloc, &m2, 2, Some(&prev)).unwrap_err();
        assert!(matches!(err, SnapshotError::Io(_)), "got {err:?}");
        assert_eq!(
            alloc.free_segments(),
            before,
            "reserved head segment returned to the allocator on I/O error"
        );

        dev.disarm();
        let (loaded, loaded_live) = load_manifest(&dev, &alloc)
            .unwrap()
            .expect("prev chain still present");
        assert_eq!(loaded, m1);
        assert_eq!(loaded_live, prev);
    }

    #[test]
    fn io_error_on_final_flush_reclaims_and_scrubs_segments() {
        let dev = FailingDevice::new(MemBlockDevice::new(BS, 512));
        let mut alloc = Allocator::new(512, 16, 0);

        let m1 = manifest_with(3);
        let prev = write_manifest(&dev, &mut alloc, &m1, 1, None).unwrap();
        let before = alloc.free_segments();

        let would_reserve = alloc.allocate().expect("a free segment");
        alloc.free(would_reserve).unwrap();

        let m2 = manifest_with(5);
        dev.arm_flush();
        let err = write_manifest(&dev, &mut alloc, &m2, 2, Some(&prev)).unwrap_err();
        assert!(matches!(err, SnapshotError::Io(_)), "got {err:?}");
        assert_eq!(
            alloc.free_segments(),
            before,
            "reserved segment returned to the allocator on final-flush failure"
        );

        let mut buf = vec![0u8; BS];
        let (lba, _) = alloc.segment_range(would_reserve);
        dev.read_at(lba, &mut buf).unwrap();
        assert!(
            decode_chain_header(&buf, MAN_CHAIN_MAGIC).header().is_none(),
            "torn head header must be scrubbed off the freed segment",
        );

        dev.disarm();
        let (loaded, loaded_live) = load_manifest(&dev, &alloc)
            .unwrap()
            .expect("prev chain still present");
        assert_eq!(loaded, m1);
        assert_eq!(loaded_live, prev);
    }
}
