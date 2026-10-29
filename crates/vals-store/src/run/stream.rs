use super::*;

pub(super) enum Walk {
    Entry(u128),
    Window,
    Done,
    Blocked,
}

pub(super) struct BlockWalker {
    blocks: Arc<FenceIndex>,
    pub(super) next_block: usize,
    pub(super) win: Vec<u8>,
    win_off: u64,
    win_end_block: usize,
    cur_block: usize,
    pos: usize,
    body_end: usize,
    left: u32,
    in_block: bool,
    first_in_block: bool,
    pub(super) key: Vec<u8>,
    has_key: bool,
}

impl BlockWalker {
    pub(super) fn new(blocks: Arc<FenceIndex>, start_block: usize) -> Self {
        let start = start_block.min(blocks.len());
        BlockWalker {
            blocks,
            next_block: start,
            win: Vec::new(),
            win_off: 0,
            win_end_block: start,
            cur_block: start,
            pos: 0,
            body_end: 0,
            left: 0,
            in_block: false,
            first_in_block: false,
            key: Vec::new(),
            has_key: false,
        }
    }

    pub(super) fn window(&self) -> Option<(u64, usize, usize)> {
        let first = self.next_block;
        let start = *self.blocks.get(first)?.1;
        let mut end = start + self.blocks.block_ref(first).1 as u64;
        let mut last = first + 1;
        while let Some((_, off, len)) = self.blocks.get(last) {
            if *off != end || (end - start) as usize + *len as usize > SIDECAR_CURSOR_WINDOW {
                break;
            }
            end = off + *len as u64;
            last += 1;
        }
        Some((start, (end - start) as usize, last))
    }

    pub(super) fn install(&mut self, bytes: Vec<u8>, off: u64, end_block: usize) {
        self.win = bytes;
        self.win_off = off;
        self.cur_block = self.next_block;
        self.win_end_block = end_block;
        self.next_block = end_block;
        self.in_block = false;
    }

    pub(super) fn advance(&mut self, reserve: &mut impl FnMut(u64) -> bool) -> Result<Walk, SnapshotError> {
        loop {
            if !self.in_block {
                if self.cur_block >= self.win_end_block {
                    return Ok(if self.next_block >= self.blocks.len() {
                        Walk::Done
                    } else {
                        Walk::Window
                    });
                }
                let (_, off, len) = self.blocks.get(self.cur_block).unwrap();
                let s = off
                    .checked_sub(self.win_off)
                    .map(|s| s as usize)
                    .filter(|s| s + *len as usize <= self.win.len())
                    .ok_or(SnapshotError::Corrupt("run block outside its read window"))?;
                let block = &self.win[s..s + *len as usize];
                self.left = block_frame(block).map_err(|e| SnapshotError::Corrupt(e.run_block_why()))?;
                self.pos = s + 4;
                self.body_end = s + block.len() - 4;
                self.in_block = true;
                self.first_in_block = true;
            }
            if self.left == 0 {
                if self.pos != self.body_end {
                    return Err(SnapshotError::Corrupt("run block has trailing bytes"));
                }
                self.in_block = false;
                self.cur_block += 1;
                continue;
            }
            let mut c = Cursor { b: &self.win[..self.body_end], off: self.pos };
            let prev_len = if self.first_in_block { 0 } else { self.key.len() };
            let k = c.entry_key(prev_len).map_err(SnapshotError::Corrupt)?;
            let version = c.entry_version().map_err(SnapshotError::Corrupt)?;
            if !reserve(((k.shared + k.suffix.len()) as u64).saturating_mul(2)) {
                return Ok(Walk::Blocked);
            }
            if self.has_key && k.suffix <= &self.key[k.shared..] {
                return Err(SnapshotError::Corrupt("run keys not strictly increasing"));
            }
            self.key.truncate(k.shared);
            self.key.extend_from_slice(k.suffix);
            self.has_key = true;
            self.first_in_block = false;
            self.pos = c.off;
            self.left -= 1;
            return Ok(Walk::Entry(version));
        }
    }
}

pub struct BlockVersionStream<D: BlockDevice> {
    pub(super) dev: Arc<D>,
    pub(super) seg_lbas: Vec<u64>,
    pub(super) seg_bytes: usize,
    pub(super) num_buckets: u32,
    pub(super) walker: BlockWalker,
}

impl<D: BlockDevice> BlockVersionStream<D> {
    pub fn resident_bound_bytes(&self) -> u64 {
        (SIDECAR_CURSOR_WINDOW.max(self.walker.win.capacity()) + self.walker.key.capacity()) as u64
    }

    pub fn next_step_reserving(&mut self, mut reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        if self.num_buckets == 0 {
            return Err(SnapshotError::Corrupt("run has zero buckets"));
        }
        match self.walker.advance(&mut reserve)? {
            Walk::Entry(version) => Ok(Some(SidecarCursorStep::Entry((self.walker.key.clone(), version)))),
            Walk::Blocked => Ok(None),
            Walk::Done => Ok(Some(SidecarCursorStep::Done)),
            Walk::Window => {
                let (off, len, end) = self.walker.window().expect("a pending window has a block");
                if !reserve((len as u64).saturating_mul(2)) {
                    return Ok(None);
                }
                self.walker.win = Vec::new();
                let bytes = read_run_range_lbas(&*self.dev, &self.seg_lbas, self.seg_bytes, off, len)?;
                self.walker.install(bytes, off, end);
                Ok(Some(SidecarCursorStep::Checkpoint))
            }
        }
    }
}

pub struct RunStreamSource<D: BlockDevice> {
    pub(super) dev: Arc<D>,
    pub(super) version: u16,
    pub(super) seg_lbas: Vec<u64>,
    pub(super) seg_bytes: usize,
    pub(super) num_buckets: u32,
    pub(super) index: Option<Arc<FenceIndex>>,
    pub(super) sidecar: Option<RangeLocator>,
}

impl<D: BlockDevice> RunStreamSource<D> {
    pub fn version_stream(&self) -> Result<Option<RunVersionStream<D>>, SnapshotError> {
        self.version_stream_on(self.dev.clone())
    }

    pub(crate) fn streamed_bytes(&self) -> u64 {
        match (&self.sidecar, &self.index) {
            (Some(loc), _) => loc.len() as u64,
            (None, Some(index)) => index.last().map_or(0, |(_, off, len)| off + *len as u64),
            (None, None) => 0,
        }
    }

    pub(crate) fn version_stream_on(&self, dev: Arc<D>) -> Result<Option<RunVersionStream<D>>, SnapshotError> {
        if self.version < 3 {
            return Ok(None);
        }
        match (&self.sidecar, &self.index) {
            (Some(loc), _) => Ok(Some(RunVersionStream::Sidecar(SidecarStream::new(
                dev,
                loc.clone(),
            )?))),
            (None, Some(index)) => Ok(Some(RunVersionStream::Blocks(BlockVersionStream {
                dev,
                seg_lbas: self.seg_lbas.clone(),
                seg_bytes: self.seg_bytes,
                num_buckets: self.num_buckets,
                walker: BlockWalker::new(index.clone(), 0),
            }))),
            (None, None) => Err(SnapshotError::Corrupt("run stream source has no block index")),
        }
    }

    pub fn sidecar_versions(&self) -> Result<SidecarVersions, SnapshotError> {
        let Some(loc) = &self.sidecar else {
            return Err(SnapshotError::Corrupt("run has no sidecar"));
        };
        let side = read_run_range_lbas(&*self.dev, &loc.seg_lbas, loc.seg_bytes, loc.off, loc.len)?;
        SidecarVersions::from_sidecar(self.version, side)
    }
}

pub enum RunVersionStream<D: BlockDevice> {
    Sidecar(SidecarStream<D>),
    Blocks(BlockVersionStream<D>),
}

impl<D: BlockDevice> RunVersionStream<D> {
    pub fn resident_bound_bytes(&self) -> u64 {
        match self {
            RunVersionStream::Sidecar(s) => s.resident_bound_bytes(),
            RunVersionStream::Blocks(s) => s.resident_bound_bytes(),
        }
    }

    pub fn next_step(&mut self) -> Result<SidecarCursorStep, SnapshotError> {
        Ok(self.next_step_reserving(|_| true)?.unwrap())
    }

    pub fn next_step_reserving(&mut self, reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        match self {
            RunVersionStream::Sidecar(s) => s.next_step_reserving(reserve),
            RunVersionStream::Blocks(s) => s.next_step_reserving(reserve),
        }
    }
}

pub struct BlockVersionCursor {
    pub(super) loc: RangeLocator,
    pub(super) base: u64,
    pub(super) num_buckets: u32,
    pub(super) max_block: usize,
    pub(super) walker: BlockWalker,
}

impl BlockVersionCursor {
    pub fn scanned_through(&self) -> Option<&[u8]> {
        self.walker.has_key.then_some(self.walker.key.as_slice())
    }

    pub(crate) fn current_block(&self) -> usize {
        self.walker.cur_block
    }

    pub fn resident_bound_bytes(&self) -> usize {
        SIDECAR_CURSOR_WINDOW.max(self.max_block).saturating_mul(2).saturating_add(self.max_block)
    }

    pub async fn next_matching_step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<SidecarCursorStep, SnapshotError> {
        if self.num_buckets == 0 {
            return Err(SnapshotError::Corrupt("run has zero buckets"));
        }
        loop {
            match self.walker.advance(&mut |_| true)? {
                Walk::Entry(version) => {
                    let key = self.walker.key.as_slice();
                    if after.is_none_or(|a| key > a) && want.contains(&bucket_of(key)) {
                        return Ok(SidecarCursorStep::Entry((key.to_vec(), version)));
                    }
                }
                Walk::Done => return Ok(SidecarCursorStep::Done),
                Walk::Blocked => unreachable!("an unbounded reserve never blocks"),
                Walk::Window => {
                    let (off, len, end) = self.walker.window().expect("a pending window has a block");
                    self.walker.win = Vec::new();
                    let bytes = self.loc.read_range(dev, (off - self.base) as usize, len).await?;
                    self.walker.install(bytes, off, end);
                    vals_rt::yield_now().await;
                    return Ok(SidecarCursorStep::Checkpoint);
                }
            }
        }
    }
}

pub enum VersionSource {
    Sidecar { loc: RangeLocator, version: u16 },
    Blocks(BlockVersionCursor),
}

impl VersionSource {
    pub fn resident_bound_bytes(&self) -> usize {
        match self {
            VersionSource::Sidecar { loc, version } => {
                if *version >= 3 {
                    SIDECAR_CURSOR_WINDOW.saturating_mul(2)
                } else {
                    loc.len()
                }
            }
            VersionSource::Blocks(cursor) => cursor.resident_bound_bytes(),
        }
    }

    pub fn stored_len(&self) -> usize {
        match self {
            VersionSource::Sidecar { loc, .. } => loc.len(),
            VersionSource::Blocks(cursor) => cursor.loc.len(),
        }
    }
}

pub(super) enum KeySourceKind {
    Sidecar {
        off: u64,
        body_len: u64,
        pos: u64,
        carry: Vec<u8>,
        crc: crc::Crc32,
        header_read: bool,
        max_key: usize,
    },
    Blocks {
        blocks: Vec<(u64, u32)>,
        next: usize,
    },
}

pub struct RunKeySource {
    pub(super) seg_lbas: Vec<u64>,
    pub(super) seg_bytes: usize,
    pub(super) kind: KeySourceKind,
    pub(super) bytes_read: u64,
}

impl RunKeySource {
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    #[cfg(test)]
    pub(crate) fn first_sidecar_record_location(&self, block_size: usize) -> Option<(u64, usize)> {
        let KeySourceKind::Sidecar { off, .. } = &self.kind else {
            return None;
        };
        let logical = *off + 4;
        let seg = (logical / self.seg_bytes as u64) as usize;
        let within = (logical % self.seg_bytes as u64) as usize;
        Some((self.seg_lbas[seg] + (within / block_size) as u64, within % block_size))
    }

    pub fn step<D: BlockDevice>(
        &mut self,
        dev: &D,
        max_bytes: usize,
        mut f: impl FnMut(&[u8]),
    ) -> Result<bool, SnapshotError> {
        let max_bytes = max_bytes.max(1);
        match &mut self.kind {
            KeySourceKind::Sidecar { off, body_len, pos, carry, crc, header_read, max_key } => {
                if *pos < *body_len {
                    let take = (max_bytes as u64).min(*body_len - *pos) as usize;
                    let chunk = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, *off + *pos, take)?;
                    crc.update(&chunk);
                    *pos += chunk.len() as u64;
                    self.bytes_read += chunk.len() as u64;
                    carry.extend_from_slice(&chunk);
                }
                let mut consumed = 0usize;
                if !*header_read && carry.len() >= 4 {
                    if u32::from_le_bytes(carry[..4].try_into().unwrap()) == 0 {
                        return Err(SnapshotError::Corrupt("sidecar has zero buckets"));
                    }
                    consumed = 4;
                    *header_read = true;
                }
                if *header_read {
                    while carry.len() - consumed >= 4 {
                        let kl = u32::from_le_bytes(carry[consumed..consumed + 4].try_into().unwrap()) as usize;
                        if kl > *max_key {
                            return Err(SnapshotError::Corrupt("sidecar record key longer than any run block"));
                        }
                        let len = 4 + kl + 16;
                        if (len as u64) > (carry.len() - consumed) as u64 + (*body_len - *pos) {
                            return Err(SnapshotError::Corrupt("sidecar record truncated"));
                        }
                        if carry.len() - consumed < len {
                            break;
                        }
                        f(&carry[consumed + 4..consumed + 4 + kl]);
                        consumed += len;
                    }
                }
                carry.drain(..consumed);
                if *pos < *body_len {
                    return Ok(false);
                }
                if !carry.is_empty() || !*header_read {
                    return Err(SnapshotError::Corrupt("sidecar record truncated"));
                }
                let trailer = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, *off + *body_len, 4)?;
                self.bytes_read += 4;
                if crc.clone().finalize() != u32::from_le_bytes(trailer.as_slice().try_into().unwrap()) {
                    return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
                }
                Ok(true)
            }
            KeySourceKind::Blocks { blocks, next } => {
                if *next >= blocks.len() {
                    return Ok(true);
                }
                let first = *next;
                let start = blocks[first].0;
                let mut end = start;
                let mut last = first;
                while last < blocks.len() && blocks[last].0 == end && (last == first || ((end - start) as usize) < max_bytes) {
                    end = blocks[last].0 + blocks[last].1 as u64;
                    last += 1;
                }
                let win = read_run_range_lbas(dev, &self.seg_lbas, self.seg_bytes, start, (end - start) as usize)?;
                self.bytes_read += win.len() as u64;
                for &(off, len) in &blocks[first..last] {
                    let s = (off - start) as usize;
                    let entries = decode_block(&win[s..s + len as usize]).map_err(SnapshotError::Corrupt)?;
                    for e in &entries {
                        f(&e.key);
                    }
                }
                *next = last;
                Ok(*next >= blocks.len())
            }
        }
    }
}
