use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunHandle {
    pub segs: Vec<SegmentId>,
    pub len: u64,
}

pub fn allocate_run_segs(
    alloc: &mut Allocator,
    block_size: usize,
    run_len: usize,
) -> Result<Vec<SegmentId>, SnapshotError> {
    let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
    let n = run_len.div_ceil(seg_bytes).max(1);
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
    Ok(segs)
}

pub(super) fn run_seg_write_buf(run_bytes: &[u8], i: usize, seg_bytes: usize, block_size: usize) -> Vec<u8> {
    let start = i * seg_bytes;
    let end = ((i + 1) * seg_bytes).min(run_bytes.len());
    let chunk = &run_bytes[start..end.max(start)];
    let nblocks = chunk.len().div_ceil(block_size).max(1);
    let mut buf = vec![0u8; nblocks * block_size];
    buf[..chunk.len()].copy_from_slice(chunk);
    buf
}

#[cfg(test)]
pub async fn write_run_segs<D: BlockDevice>(
    dev: &D,
    segs: &[SegmentId],
    seg_lbas: &[u64],
    seg_bytes: usize,
    block_size: usize,
    run_bytes: &[u8],
) -> Result<RunHandle, SnapshotError> {
    debug_assert_eq!(segs.len(), seg_lbas.len());
    for (i, &lba) in seg_lbas.iter().enumerate() {
        let buf = run_seg_write_buf(run_bytes, i, seg_bytes, block_size);
        crate::attribution::write_bytes(3, buf.len());
        dev.write_at(lba, &buf)?;
        if (i + 1) % FLUSH_WRITE_BATCH_SEGS == 0 {
            vals_rt::yield_now().await;
        }
    }
    dev.flush()?;
    Ok(RunHandle {
        segs: segs.to_vec(),
        len: run_bytes.len() as u64,
    })
}

#[cfg(test)]
const FLUSH_WRITE_BATCH_SEGS: usize = 4;

pub struct SegmentStreamSink<'a, D: BlockDevice> {
    pub(super) dev: &'a D,
    segs: &'a [SegmentId],
    pub(super) seg_lbas: &'a [u64],
    pub(super) seg_bytes: usize,
    block_size: usize,
    buf: Vec<u8>,
    seg_idx: usize,
    position: u64,
    paced: bool,
    write_source: usize,
    flush_every_bytes: u64,
    since_flush_bytes: u64,
    pub(super) err: Option<SnapshotError>,
}

impl<'a, D: BlockDevice> SegmentStreamSink<'a, D> {
    pub fn new(
        dev: &'a D,
        segs: &'a [SegmentId],
        seg_lbas: &'a [u64],
        seg_bytes: usize,
        block_size: usize,
    ) -> Self {
        debug_assert_eq!(segs.len(), seg_lbas.len());
        SegmentStreamSink {
            dev,
            segs,
            seg_lbas,
            seg_bytes,
            block_size,
            buf: Vec::with_capacity(seg_bytes),
            seg_idx: 0,
            position: 0,
            paced: false,
            write_source: 4,
            flush_every_bytes: 0,
            since_flush_bytes: 0,
            err: None,
        }
    }

    pub fn write_source(mut self, source: usize) -> Self {
        self.write_source = source;
        self
    }

    pub fn paced(mut self, paced: bool) -> Self {
        self.paced = paced;
        self
    }

    pub fn flush_every(mut self, bytes: u64) -> Self {
        self.flush_every_bytes = bytes;
        self
    }

    fn flush_full_segments(&mut self) {
        if self.err.is_some() {
            return;
        }
        while self.buf.len() >= self.seg_bytes {
            let Some(&lba) = self.seg_lbas.get(self.seg_idx) else {
                self.err = Some(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
                return;
            };
            let write_t = std::time::Instant::now();
            crate::attribution::write_bytes(self.write_source, self.seg_bytes);
            if let Err(e) = self.dev.write_at(lba, &self.buf[..self.seg_bytes]) {
                self.err = Some(SnapshotError::from(e));
                return;
            }
            self.since_flush_bytes += self.seg_bytes as u64;
            if self.flush_every_bytes > 0 && self.since_flush_bytes >= self.flush_every_bytes {
                if let Err(e) = self.dev.flush() {
                    self.err = Some(SnapshotError::from(e));
                    return;
                }
                self.since_flush_bytes = 0;
            }
            if self.paced {
                crate::pace::maint_pace_charge(write_t.elapsed());
            }
            self.buf.drain(..self.seg_bytes);
            self.seg_idx += 1;
        }
    }

    pub(super) fn read_logical(&self, off: u64, len: usize) -> Result<Vec<u8>, SnapshotError> {
        if self.err.is_some() {
            return Err(SnapshotError::Corrupt("streamed run write already failed"));
        }
        let off = off as usize;
        let end = off
            .checked_add(len)
            .ok_or(SnapshotError::Corrupt("stream read range overflow"))?;
        let flushed = self.seg_idx * self.seg_bytes;
        if end > flushed + self.buf.len() {
            return Err(SnapshotError::Corrupt("stream read past written bytes"));
        }
        let mut out = Vec::with_capacity(len);
        if off < flushed {
            let dev_len = len.min(flushed - off);
            let bytes = read_run_range_batched(
                self.dev,
                self.seg_lbas,
                self.seg_bytes,
                off as u64,
                dev_len,
                READBACK_QD,
            )?;
            out.extend_from_slice(&bytes);
        }
        if end > flushed {
            let start = off.max(flushed) - flushed;
            out.extend_from_slice(&self.buf[start..end - flushed]);
        }
        Ok(out)
    }

    pub async fn finish(mut self) -> Result<RunHandle, SnapshotError> {
        if let Some(e) = self.err.take() {
            return Err(e);
        }
        let mut used = self.seg_idx;
        if !self.buf.is_empty() {
            let Some(&lba) = self.seg_lbas.get(self.seg_idx) else {
                return Err(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
            };
            let nblocks = self.buf.len().div_ceil(self.block_size).max(1);
            self.buf.resize(nblocks * self.block_size, 0);
            crate::attribution::write_bytes(self.write_source, self.buf.len());
            self.dev.write_at(lba, &self.buf)?;
            used += 1;
        } else if used == 0 {
            let Some(&lba) = self.seg_lbas.first() else {
                return Err(SnapshotError::Corrupt(
                    "streamed run exceeded its reserved segments",
                ));
            };
            let buf = vec![0u8; self.block_size];
            crate::attribution::write_bytes(self.write_source, buf.len());
            self.dev.write_at(lba, &buf)?;
            used = 1;
        }
        self.dev.flush()?;
        vals_rt::yield_now().await;
        Ok(RunHandle {
            segs: self.segs[..used].to_vec(),
            len: self.position,
        })
    }
}

impl<'a, D: BlockDevice> RunSink for SegmentStreamSink<'a, D> {
    fn write(&mut self, bytes: &[u8]) {
        self.position += bytes.len() as u64;
        if self.err.is_some() {
            return;
        }
        self.buf.extend_from_slice(bytes);
        if self.buf.len() >= self.seg_bytes {
            self.flush_full_segments();
        }
    }
    fn position(&self) -> u64 {
        self.position
    }
}

pub fn write_run(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    run_bytes: &[u8],
) -> Result<RunHandle, SnapshotError> {
    write_run_source(dev, alloc, run_bytes, 6)
}

pub(crate) fn write_run_source(
    dev: &impl BlockDevice,
    alloc: &mut Allocator,
    run_bytes: &[u8],
    source: usize,
) -> Result<RunHandle, SnapshotError> {
    let block_size = dev.block_size();
    let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
    let segs = allocate_run_segs(alloc, block_size, run_bytes.len())?;
    for (i, seg) in segs.iter().enumerate() {
        let buf = run_seg_write_buf(run_bytes, i, seg_bytes, block_size);
        let (lba, _) = alloc.segment_range(*seg);
        crate::attribution::write_bytes(source, buf.len());
        dev.write_at(lba, &buf)?;
    }
    dev.flush()?;
    Ok(RunHandle {
        segs,
        len: run_bytes.len() as u64,
    })
}

#[derive(Clone, Copy, Debug)]
struct Span {
    lba: u64,
    read_len: usize,
    skip: usize,
    take: usize,
}

struct StripedSpans<F> {
    seg_bytes: usize,
    block_size: usize,
    pos: usize,
    remaining: usize,
    seg_lba: F,
}

fn striped_spans<F: Fn(usize) -> Option<u64>>(
    seg_bytes: usize,
    block_size: usize,
    off: usize,
    len: usize,
    seg_lba: F,
) -> StripedSpans<F> {
    StripedSpans { seg_bytes, block_size, pos: off, remaining: len, seg_lba }
}

impl<F: Fn(usize) -> Option<u64>> Iterator for StripedSpans<F> {
    type Item = Result<Span, SnapshotError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let seg_idx = self.pos / self.seg_bytes;
        let within = self.pos % self.seg_bytes;
        let Some(seg_lba) = (self.seg_lba)(seg_idx) else {
            self.remaining = 0;
            return Some(Err(SnapshotError::Corrupt("run offset past its segments")));
        };
        let block_size = self.block_size;
        let take = self.remaining.min(self.seg_bytes - within);
        let blk_start = within - (within % block_size);
        let blk_end = (within + take).div_ceil(block_size) * block_size;
        self.pos += take;
        self.remaining -= take;
        Some(Ok(Span {
            lba: seg_lba + (blk_start / block_size) as u64,
            read_len: blk_end - blk_start,
            skip: within - blk_start,
            take,
        }))
    }
}

fn read_run_range_core<D: BlockDevice>(
    dev: &D,
    seg_bytes: usize,
    off: u64,
    len: usize,
    resolve_seg_lba: impl Fn(usize) -> Option<u64>,
) -> Result<Vec<u8>, SnapshotError> {
    let mut out = Vec::with_capacity(len);
    for span in striped_spans(seg_bytes, dev.block_size(), off as usize, len, resolve_seg_lba) {
        let span = span?;
        let mut buf = vec![0u8; span.read_len];
        dev.read_at(span.lba, &mut buf)?;
        out.extend_from_slice(&buf[span.skip..span.skip + span.take]);
    }
    Ok(out)
}

pub(super) fn read_run_range(
    dev: &impl BlockDevice,
    alloc: &Allocator,
    segs: &[SegmentId],
    seg_bytes: usize,
    off: u64,
    len: usize,
) -> Result<Vec<u8>, SnapshotError> {
    read_run_range_core(dev, seg_bytes, off, len, |seg_idx| {
        segs.get(seg_idx).map(|s| alloc.segment_range(*s).0)
    })
}

pub(super) fn read_run_range_lbas<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    off: u64,
    len: usize,
) -> Result<Vec<u8>, SnapshotError> {
    read_run_range_core(dev, seg_bytes, off, len, |seg_idx| {
        seg_lbas.get(seg_idx).copied()
    })
}

const RUN_READ_OP_MAX_BYTES: usize = 128 * 1024;

pub(super) const SIDECAR_READBACK_WINDOW: usize = 1024 * 1024;

pub(super) const READBACK_QD: usize = 8;

pub(super) fn read_run_range_batched<D: BlockDevice>(
    dev: &D,
    seg_lbas: &[u64],
    seg_bytes: usize,
    off: u64,
    len: usize,
    qd: usize,
) -> Result<Vec<u8>, SnapshotError> {
    use vals_spdk::ReadOp;
    let block_size = dev.block_size();
    let mut spans: Vec<(Span, Vec<u8>)> =
        striped_spans(seg_bytes, block_size, off as usize, len, |i| seg_lbas.get(i).copied())
            .map(|span| span.map(|s| (s, crate::vlog::io_buf(s.read_len))))
            .collect::<Result<_, _>>()?;
    let total: usize = spans.iter().map(|(_, buf)| buf.len()).sum();
    let chunk = (total / qd.max(1))
        .clamp(block_size, RUN_READ_OP_MAX_BYTES)
        .div_ceil(block_size)
        * block_size;
    let mut ops: Vec<ReadOp<'_>> = Vec::new();
    for (s, buf) in &mut spans {
        let lba0 = s.lba;
        let mut done = 0usize;
        let mut rest: &mut [u8] = buf;
        while !rest.is_empty() {
            let n = rest.len().min(chunk);
            let (head, tail) = rest.split_at_mut(n);
            ops.push(ReadOp {
                lba: lba0 + (done / block_size) as u64,
                buf: head,
            });
            done += n;
            rest = tail;
        }
    }
    dev.read_batch(&mut ops)?;
    drop(ops);
    let mut out = Vec::with_capacity(len);
    for (s, buf) in &spans {
        out.extend_from_slice(&buf[s.skip..s.skip + s.take]);
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeLocator {
    pub(super) seg_lbas: smallvec::SmallVec<[u64; 4]>,
    pub(super) seg_bytes: usize,
    pub(super) block_size: usize,
    pub(super) off: u64,
    pub(super) len: usize,
}

impl RangeLocator {
    pub fn read_bytes(&self) -> usize {
        let start = self.off as usize;
        (start + self.len).div_ceil(self.block_size) * self.block_size
            - start / self.block_size * self.block_size
    }

    pub async fn read<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
    ) -> Result<Vec<u8>, SnapshotError> {
        self.read_range(dev, 0, self.len).await
    }

    pub(crate) async fn read_range<D: AsyncRead + BlockDevice>(
        &self,
        dev: &D,
        off: usize,
        len: usize,
    ) -> Result<Vec<u8>, SnapshotError> {
        let end = off
            .checked_add(len)
            .filter(|end| *end <= self.len)
            .ok_or(SnapshotError::Corrupt("run range past locator"))?;
        let mut out = Vec::with_capacity(len);
        let spans = striped_spans(self.seg_bytes, self.block_size, self.off as usize + off, end - off, |i| {
            self.seg_lbas.get(i).copied()
        });
        for span in spans {
            let span = span?;
            let mut buf = vec![0u8; span.read_len];
            // SAFETY: `buf` is owned by this future, so leaking the read leaks it too.
            unsafe { dev.read_async_in_place(span.lba, &mut buf) }
                .await
                .map_err(SnapshotError::Io)?;
            out.extend_from_slice(&buf[span.skip..span.skip + span.take]);
        }
        Ok(out)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
