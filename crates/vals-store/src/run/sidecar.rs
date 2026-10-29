use super::*;

pub(super) fn encode_sidecar_record(buf: &mut Vec<u8>, key: &[u8], version: u128) {
    buf.clear();
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&version.to_le_bytes());
}

pub(super) fn sidecar_body(side: &[u8]) -> Result<&[u8], &'static str> {
    if side.len() < 4 {
        return Err("sidecar truncated");
    }
    let body = &side[..side.len() - 4];
    let stored = u32::from_le_bytes(side[side.len() - 4..].try_into().unwrap());
    if crc::crc32(body) != stored {
        return Err("sidecar crc mismatch");
    }
    Ok(body)
}

pub struct SidecarVersions {
    body: Vec<u8>,
    version: u16,
    off: usize,
    groups_left: u32,
    records_left: u32,
}

impl SidecarVersions {
    pub(super) fn from_body(version: u16, body: Vec<u8>) -> Result<Self, SnapshotError> {
        let mut c = Cursor { b: &body, off: 0 };
        let groups_left = c.u32().map_err(SnapshotError::Corrupt)?;
        let off = c.off;
        Ok(SidecarVersions {
            body,
            version,
            off,
            groups_left,
            records_left: 0,
        })
    }

    pub(super) fn from_sidecar(version: u16, mut side: Vec<u8>) -> Result<Self, SnapshotError> {
        sidecar_body(&side).map_err(SnapshotError::Corrupt)?;
        side.truncate(side.len() - 4);
        Self::from_body(version, side)
    }

    pub fn body_len(&self) -> usize {
        self.body.len()
    }

    pub fn fold_page(
        &mut self,
        max_keys: usize,
        mut f: impl FnMut(&[u8], u128),
    ) -> Result<bool, SnapshotError> {
        let mut c = Cursor {
            b: &self.body,
            off: self.off,
        };
        let mut folded = 0usize;
        if self.version >= 3 {
            while c.off < self.body.len() && folded < max_keys {
                let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
                let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
                let version = c.u128().map_err(SnapshotError::Corrupt)?;
                f(key, version);
                folded += 1;
            }
            self.off = c.off;
            return Ok(self.off == self.body.len());
        }

        while folded < max_keys {
            if self.records_left == 0 {
                if self.groups_left == 0 {
                    self.off = c.off;
                    return Ok(true);
                }
                self.records_left = c.u32().map_err(SnapshotError::Corrupt)?;
                self.groups_left -= 1;
                continue;
            }
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
            let version = c.u128().map_err(SnapshotError::Corrupt)?;
            f(key, version);
            self.records_left -= 1;
            folded += 1;
        }
        self.off = c.off;
        Ok(self.groups_left == 0 && self.records_left == 0)
    }
}

pub(super) fn sidecar_v3_bucket_digests(
    body: &[u8],
    bucket: u32,
    num_buckets: u32,
    bucket_of: impl Fn(&[u8]) -> u32,
) -> Result<Vec<Digest>, &'static str> {
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32()?;
    debug_assert_eq!(n, num_buckets, "sidecar head disagrees with footer");
    if bucket >= n {
        return Err("bucket out of range");
    }
    let mut out = Vec::new();
    while c.off < body.len() {
        let kl = c.u32()? as usize;
        let k = c.take(kl)?;
        let v = c.take(16)?;
        if bucket_of(k) % n.max(1) == bucket {
            out.push((k.to_vec(), v.to_vec()));
        }
    }
    Ok(out)
}

pub const SIDECAR_CURSOR_WINDOW: usize = 256 * 1024;

pub struct SidecarStream<D: BlockDevice> {
    dev: Arc<D>,
    loc: RangeLocator,
    body_len: usize,
    read_off: usize,
    buf: Vec<u8>,
    consumed: usize,
    crc: crc::Crc32,
    header_read: bool,
    previous: Option<Vec<u8>>,
}

impl<D: BlockDevice> SidecarStream<D> {
    pub(super) fn new(dev: Arc<D>, loc: RangeLocator) -> Result<Self, SnapshotError> {
        let body_len = loc.len().checked_sub(4)
            .ok_or(SnapshotError::Corrupt("sidecar truncated"))?;
        Ok(Self {
            dev, loc, body_len, read_off: 0, buf: Vec::new(), consumed: 0,
            crc: crc::Crc32::new(), header_read: false, previous: None,
        })
    }

    pub fn resident_bound_bytes(&self) -> u64 {
        (SIDECAR_CURSOR_WINDOW.max(self.buf.capacity()) + self.previous.as_ref().map_or(0, Vec::len)) as u64
    }

    fn fill(&mut self, need: usize, reserve: &mut impl FnMut(u64) -> bool)
        -> Result<Option<bool>, SnapshotError> {
        let remaining = self.buf.len() - self.consumed;
        if remaining >= need { return Ok(Some(true)); }
        if self.read_off == self.body_len {
            return Err(SnapshotError::Corrupt("sidecar record truncated"));
        }
        let take = SIDECAR_CURSOR_WINDOW.max(need).saturating_sub(remaining)
            .min(self.body_len - self.read_off);
        if !reserve((take as u64).saturating_mul(2)) { return Ok(None); }
        if self.consumed != 0 {
            self.buf.drain(..self.consumed);
            self.consumed = 0;
        }
        self.buf.reserve_exact(take);
        let chunk = read_run_range_lbas(&*self.dev, &self.loc.seg_lbas,
            self.loc.seg_bytes, self.loc.off + self.read_off as u64, take)?;
        self.crc.update(&chunk);
        self.read_off += chunk.len();
        self.buf.extend_from_slice(&chunk);
        if self.read_off == self.body_len {
            let trailer = read_run_range_lbas(&*self.dev, &self.loc.seg_lbas,
                self.loc.seg_bytes, self.loc.off + self.body_len as u64, 4)?;
            if self.crc.clone().finalize() != u32::from_le_bytes(trailer.as_slice().try_into().unwrap()) {
                return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
            }
        }
        Ok(Some(false))
    }

    pub fn next_step(&mut self) -> Result<SidecarCursorStep, SnapshotError> {
        Ok(self.next_step_reserving(|_| true)?.unwrap())
    }

    pub fn next_step_reserving(&mut self, mut reserve: impl FnMut(u64) -> bool)
        -> Result<Option<SidecarCursorStep>, SnapshotError> {
        if !self.header_read {
            let Some(ready) = self.fill(4, &mut reserve)? else { return Ok(None); };
            if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
            let buckets = u32::from_le_bytes(self.buf[self.consumed..self.consumed + 4].try_into().unwrap());
            if buckets == 0 { return Err(SnapshotError::Corrupt("sidecar has zero buckets")); }
            self.consumed += 4;
            self.header_read = true;
        }
        if self.buf.len() == self.consumed && self.read_off == self.body_len {
            return Ok(Some(SidecarCursorStep::Done));
        }
        let Some(ready) = self.fill(4, &mut reserve)? else { return Ok(None); };
        if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
        let kl = u32::from_le_bytes(self.buf[self.consumed..self.consumed + 4].try_into().unwrap()) as usize;
        let len = 4usize.checked_add(kl).and_then(|n| n.checked_add(16))
            .ok_or(SnapshotError::Corrupt("sidecar record length overflow"))?;
        if len > self.body_len - (self.read_off - (self.buf.len() - self.consumed)) {
            return Err(SnapshotError::Corrupt("sidecar record truncated"));
        }
        let Some(ready) = self.fill(len, &mut reserve)? else { return Ok(None); };
        if !ready { return Ok(Some(SidecarCursorStep::Checkpoint)); }
        if !reserve((kl as u64).saturating_mul(2)) { return Ok(None); }
        let start = self.consumed + 4;
        let end = start + kl;
        let key = self.buf[start..end].to_vec();
        if self.previous.as_ref().is_some_and(|p| *p >= key) {
            return Err(SnapshotError::Corrupt("sidecar keys not strictly increasing"));
        }
        let version = u128::from_le_bytes(self.buf[end..end + 16].try_into().unwrap());
        self.consumed += len;
        self.previous = Some(key.clone());
        Ok(Some(SidecarCursorStep::Entry((key, version))))
    }
}

pub enum SidecarCursorStep {
    Entry((Vec<u8>, u128)),
    Done,
    Checkpoint,
}

pub struct SidecarCursor {
    loc: RangeLocator,
    body_len: usize,
    read_off: usize,
    buf: Vec<u8>,
    consumed: usize,
    crc: crc::Crc32,
    header_read: bool,
    crc_verified: bool,
    last_key: Option<(usize, usize)>,
    scanned: Option<Vec<u8>>,
}

impl SidecarCursor {
    pub fn new(loc: RangeLocator) -> Result<Self, SnapshotError> {
        let body_len = loc
            .len()
            .checked_sub(4)
            .ok_or(SnapshotError::Corrupt("sidecar truncated"))?;
        Ok(SidecarCursor {
            loc,
            body_len,
            read_off: 0,
            buf: Vec::with_capacity(SIDECAR_CURSOR_WINDOW),
            consumed: 0,
            crc: crc::Crc32::new(),
            header_read: false,
            crc_verified: false,
            last_key: None,
            scanned: None,
        })
    }

    pub fn scanned_through(&self) -> Option<&[u8]> {
        self.scanned.as_deref()
    }

    async fn fill<D: AsyncRead + BlockDevice>(
        &mut self,
        need: usize,
        dev: &D,
    ) -> Result<(bool, bool), SnapshotError> {
        let mut read = false;
        while self.buf.len().saturating_sub(self.consumed) < need {
            if self.read_off == self.body_len {
                return Ok((false, read));
            }
            if let Some((start, len)) = self.last_key.take() {
                let scanned = self.scanned.get_or_insert_with(Vec::new);
                scanned.clear();
                scanned.extend_from_slice(&self.buf[start..start + len]);
            }
            if self.consumed != 0 {
                self.buf.drain(..self.consumed);
                self.consumed = 0;
            }
            let available = self.buf.len();
            let target = SIDECAR_CURSOR_WINDOW.max(need);
            let take = target
                .saturating_sub(available)
                .min(self.body_len - self.read_off);
            let chunk = self.loc.read_range(dev, self.read_off, take).await?;
            self.crc.update(&chunk);
            self.read_off += chunk.len();
            self.buf.extend_from_slice(&chunk);
            read = true;
            vals_rt::yield_now().await;
            if self.read_off == self.body_len && !self.crc_verified {
                let trailer = self.loc.read_range(dev, self.body_len, 4).await?;
                let stored = u32::from_le_bytes(trailer.as_slice().try_into().unwrap());
                if self.crc.clone().finalize() != stored {
                    return Err(SnapshotError::Corrupt("sidecar crc mismatch"));
                }
                self.crc_verified = true;
            }
        }
        Ok((true, read))
    }

    pub async fn next_matching_step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<SidecarCursorStep, SnapshotError> {
        if !self.header_read {
            let (ready, read) = self.fill(4, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let n = u32::from_le_bytes(
                self.buf[self.consumed..self.consumed + 4]
                    .try_into()
                    .unwrap(),
            );
            if n == 0 {
                return Err(SnapshotError::Corrupt("sidecar has zero buckets"));
            }
            self.consumed += 4;
            self.header_read = true;
        }
        loop {
            let available = self.buf.len() - self.consumed;
            if available == 0 && self.read_off == self.body_len {
                return Ok(SidecarCursorStep::Done);
            }
            let (ready, read) = self.fill(4, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let kl = u32::from_le_bytes(
                self.buf[self.consumed..self.consumed + 4]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let record_len = 4usize
                .checked_add(kl)
                .and_then(|n| n.checked_add(16))
                .ok_or(SnapshotError::Corrupt("sidecar record length overflow"))?;
            let record_off = self.read_off - (self.buf.len() - self.consumed);
            if record_len > self.body_len - record_off {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            let (ready, read) = self.fill(record_len, dev).await?;
            if !ready {
                return Err(SnapshotError::Corrupt("sidecar record truncated"));
            }
            if read {
                return Ok(SidecarCursorStep::Checkpoint);
            }
            let key_start = self.consumed + 4;
            let key_end = key_start + kl;
            let key = &self.buf[key_start..key_end];
            let take = after.is_none_or(|a| key > a) && want.contains(&bucket_of(key));
            let version = u128::from_le_bytes(
                self.buf[key_end..key_end + 16].try_into().unwrap(),
            );
            self.consumed += record_len;
            self.last_key = Some((key_start, kl));
            if take {
                return Ok(SidecarCursorStep::Entry((key.to_vec(), version)));
            }
        }
    }
}

pub fn parse_want_bucket_digests(
    side: &[u8],
    version: u16,
    want: &std::collections::HashSet<u32>,
    bucket_of: impl Fn(&[u8]) -> u32,
) -> Result<Vec<Digest>, SnapshotError> {
    if want.is_empty() {
        return Ok(Vec::new());
    }
    let body = sidecar_body(side).map_err(SnapshotError::Corrupt)?;
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32().map_err(SnapshotError::Corrupt)?;
    let mut out = Vec::new();
    if version >= 3 {
        while c.off < body.len() {
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            let k = c.take(kl).map_err(SnapshotError::Corrupt)?;
            if want.contains(&(bucket_of(k) % n.max(1))) {
                let key = k.to_vec();
                let v = c.take(16).map_err(SnapshotError::Corrupt)?.to_vec();
                out.push((key, v));
            } else {
                c.take(16).map_err(SnapshotError::Corrupt)?;
            }
        }
        return Ok(out);
    }
    for b in 0..n {
        let count = c.u32().map_err(SnapshotError::Corrupt)? as usize;
        let take = want.contains(&b);
        for _ in 0..count {
            let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
            if take {
                let k = c.take(kl).map_err(SnapshotError::Corrupt)?.to_vec();
                let v = c.take(16).map_err(SnapshotError::Corrupt)?.to_vec();
                out.push((k, v));
            } else {
                c.take(kl).map_err(SnapshotError::Corrupt)?;
                c.take(16).map_err(SnapshotError::Corrupt)?;
            }
        }
    }
    out.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

pub fn parse_want_bucket_digests_page(
    side: &[u8],
    version: u16,
    want: &std::collections::HashSet<u32>,
    bucket_of: impl Fn(&[u8]) -> u32,
    after: Option<&[u8]>,
    max_bytes: usize,
    per_entry_overhead: usize,
) -> Result<(Vec<Digest>, Option<Vec<u8>>), SnapshotError> {
    if version < 3 {
        let mut all = parse_want_bucket_digests(side, version, want, &bucket_of)?;
        all.retain(|(key, _)| {
            want.contains(&bucket_of(key)) && after.is_none_or(|a| key.as_slice() > a)
        });
        let mut bytes = 0usize;
        let mut seen = false;
        let mut stop = None;
        let keep = all
            .iter()
            .position(|(key, _)| {
                let size = key.len().saturating_add(per_entry_overhead);
                if seen && bytes.saturating_add(size) > max_bytes {
                    stop = Some(key.clone());
                    true
                } else {
                    seen = true;
                    bytes = bytes.saturating_add(size);
                    false
                }
            })
            .unwrap_or(all.len());
        all.truncate(keep);
        return Ok((all, stop));
    }
    let body = sidecar_body(side).map_err(SnapshotError::Corrupt)?;
    let mut c = Cursor { b: body, off: 0 };
    let n = c.u32().map_err(SnapshotError::Corrupt)?;
    let mut out = Vec::new();
    let mut bytes = 0usize;
    while c.off < body.len() {
        let kl = c.u32().map_err(SnapshotError::Corrupt)? as usize;
        let key = c.take(kl).map_err(SnapshotError::Corrupt)?;
        let version = c.take(16).map_err(SnapshotError::Corrupt)?;
        if !want.contains(&(bucket_of(key) % n.max(1)))
            || after.is_some_and(|a| key <= a)
        {
            continue;
        }
        let size = key.len().saturating_add(per_entry_overhead);
        if !out.is_empty() && bytes.saturating_add(size) > max_bytes {
            return Ok((out, Some(key.to_vec())));
        }
        bytes = bytes.saturating_add(size);
        out.push((key.to_vec(), version.to_vec()));
    }
    Ok((out, None))
}
