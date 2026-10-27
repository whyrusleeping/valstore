use super::*;

impl<D: BlockDevice> Engine<D> {
    pub fn version_digest_page(
        &self,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
    ) -> Result<VersionDigestPage, EngineError> {
        if want.is_empty() {
            return Ok((Vec::new(), false));
        }

        let in_range = |k: &[u8]| after.is_none_or(|a| k > a);

        let mut sources: Vec<Vec<(Vec<u8>, u128)>> = Vec::with_capacity(self.runs.len() + 1);

        let mut mem: Vec<(Vec<u8>, u128)> = self
            .index
            .versions()
            .into_iter()
            .filter(|(k, _)| want.contains(&bucket_of(k)) && in_range(k))
            .map(|(k, v)| (k.to_vec(), v))
            .collect();
        mem.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        sources.push(mem);

        let granularity = self.runs_bucket_granularity();
        let mut source_stop: Option<Vec<u8>> = None;
        for or in &self.runs {
            if or.run.num_buckets() != granularity {
                return Err(EngineError::Snapshot(SnapshotError::Corrupt(
                    "run bucket-digest granularity disagrees across runs",
                )));
            }
            let (digests, stop) = or.run.want_bucket_digests_page(
                &self.alloc,
                want,
                &bucket_of,
                after,
                max_bytes,
                per_entry_overhead,
            )?;
            if let Some(stop) = stop {
                if source_stop.as_ref().is_none_or(|current| stop < *current) {
                    source_stop = Some(stop);
                }
            }
            let src: Vec<(Vec<u8>, u128)> = digests
                .into_iter()
                .filter(|(k, _)| want.contains(&bucket_of(k)) && in_range(k))
                .map(|(k, vbytes)| {
                    let v = u128::from_le_bytes(vbytes.as_slice().try_into().unwrap_or([0; 16]));
                    (k, v)
                })
                .collect();
            sources.push(src);
        }

        let (mut out, mut more) = merge_digest_sources(
            sources,
            usize::MAX,
            max_bytes,
            per_entry_overhead,
        );
        if let Some(stop) = source_stop {
            let keep = out.partition_point(|(key, _)| *key < stop);
            out.truncate(keep);
            more = true;
        }
        Ok((out, more))
    }

}

impl<D: BlockDevice + AsyncRead> Engine<D> {
    pub fn plan_version_digest_page(
        &self,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
    ) -> Result<DigestPagePlan, EngineError> {
        self.plan_version_digest_page_covered(want, bucket_of, after, max_bytes, per_entry_overhead, None)
    }

    pub fn plan_version_digest_page_covered(
        &self,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
        cover: Option<std::sync::Arc<FragmentSet>>,
    ) -> Result<DigestPagePlan, EngineError> {
        let cover = cover.filter(|cover| !cover.is_empty());
        if want.is_empty() {
            return Ok(DigestPagePlan {
                mem: Vec::new(),
                mem_stop: None,
                sources: Vec::new(),
                after: after.map(<[u8]>::to_vec),
                cover,
            });
        }
        let mut mem = Vec::new();
        let mut mem_bytes = 0usize;
        let mut mem_stop = None;
        let mut scan_after = after.map(<[u8]>::to_vec);
        loop {
            let (entries, more) = self.index.maintenance_page(scan_after.as_deref(), 1024);
            if entries.is_empty() {
                break;
            }
            let last = entries.last().unwrap().0.clone();
            for (key, entry) in entries {
                if !want.contains(&bucket_of(&key)) || digest_covered(cover.as_deref(), &key, entry.version) {
                    continue;
                }
                let size = key.len().saturating_add(per_entry_overhead);
                if !mem.is_empty() && mem_bytes.saturating_add(size) > max_bytes {
                    mem_stop = Some(key);
                    break;
                }
                mem_bytes = mem_bytes.saturating_add(size);
                mem.push((key, entry.version));
            }
            if mem_stop.is_some() || !more {
                break;
            }
            scan_after = Some(last);
        }

        let granularity = self.runs_bucket_granularity();
        let mut sources: Vec<run::VersionSource> = Vec::with_capacity(self.runs.len());
        for or in &self.runs {
            if or.run.num_buckets() != granularity {
                return Err(EngineError::Snapshot(SnapshotError::Corrupt(
                    "run bucket-digest granularity disagrees across runs",
                )));
            }
            sources.push(or.run.version_source(&self.alloc, after));
        }
        Ok(DigestPagePlan {
            mem,
            mem_stop,
            sources,
            after: after.map(<[u8]>::to_vec),
            cover,
        })
    }

    pub async fn version_digest_page_from_plan(
        plan: DigestPagePlan,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
        dev: &D,
    ) -> Result<VersionDigestPage, EngineError> {
        let mut checkpoint = NoopDigestCheckpoint;
        let (out, more, _) = Self::version_digest_page_from_plan_cooperative(
            plan,
            want,
            bucket_of,
            after,
            max_bytes,
            per_entry_overhead,
            dev,
            &mut checkpoint,
        )
        .await?
        .expect("no-op digest checkpoint cannot invalidate a page");
        Ok((out, more))
    }

    pub async fn version_digest_page_from_plan_cooperative<C: DigestCheckpoint>(
        plan: DigestPagePlan,
        want: &std::collections::HashSet<u32>,
        bucket_of: impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
        max_bytes: usize,
        per_entry_overhead: usize,
        dev: &D,
        checkpoint: &mut C,
    ) -> Result<Option<ResumableDigestPage>, EngineError> {
        if want.is_empty() {
            return Ok(Some((Vec::new(), false, None)));
        }
        let DigestPagePlan {
            mem,
            mem_stop,
            sources,
            after: planned_after,
            cover,
        } = plan;
        debug_assert_eq!(
            planned_after.as_deref(),
            after,
            "digest page executed with a different `after` than it was planned for"
        );
        let mut cursors = Vec::with_capacity(sources.len() + 1);
        cursors.push(DigestSourceCursor::Materialized { entries: mem, pos: 0 });
        for source in sources {
            let (loc, version) = match source {
                run::VersionSource::Blocks(cursor) => {
                    cursors.push(DigestSourceCursor::Blocks(cursor));
                    continue;
                }
                run::VersionSource::Sidecar { loc, version } => (loc, version),
            };
            if version >= 3 {
                cursors.push(DigestSourceCursor::Streaming(run::SidecarCursor::new(loc)?));
            } else {
                let mut side = Vec::with_capacity(loc.len());
                while side.len() < loc.len() {
                    let take = run::SIDECAR_CURSOR_WINDOW.min(loc.len() - side.len());
                    let chunk = loc.read_range(dev, side.len(), take).await?;
                    side.extend_from_slice(&chunk);
                    if !checkpoint.checkpoint().await {
                        return Ok(None);
                    }
                }
                let entries = run::parse_want_bucket_digests(
                    &side,
                    version,
                    want,
                    &bucket_of,
                )?
                .into_iter()
                .filter(|(key, _)| {
                    want.contains(&bucket_of(key)) && after.is_none_or(|a| key.as_slice() > a)
                })
                .map(|(key, version)| {
                    (key, u128::from_le_bytes(version.as_slice().try_into().unwrap_or([0; 16])))
                })
                .collect();
                cursors.push(DigestSourceCursor::Materialized { entries, pos: 0 });
            }
        }
        let mut heads: Vec<DigestHead> = cursors.iter().map(|_| DigestHead::Scanning(None)).collect();
        let mut unsettled = heads.len();
        let mut consumed = Vec::new();
        let mut out: Vec<(Vec<u8>, u128)> = Vec::new();
        let mut bytes = 0usize;
        let mut consumed_last: Option<Vec<u8>> = None;
        loop {
            if unsettled > 0 {
                let last = consumed_last.as_deref();
                let min_key = ready_min(&heads);
                let lagging = heads
                    .iter()
                    .enumerate()
                    .filter_map(|(i, head)| {
                        let pos = head_position(head, last)?;
                        lags(pos, min_key).then_some((i, pos))
                    })
                    .min_by(|a, b| a.1.cmp(&b.1))
                    .map(|(i, _)| i);
                if let Some(i) = lagging {
                    let outcome = step_digest_head(
                        &mut cursors,
                        &mut heads,
                        i,
                        &mut unsettled,
                        last,
                        dev,
                        want,
                        &bucket_of,
                        after,
                        checkpoint,
                    )
                    .await?;
                    match outcome {
                        DigestStep::Continue => continue,
                        DigestStep::Invalidated => return Ok(None),
                        DigestStep::Expired(resume) => {
                            return Ok(Some(expired_page(out, resume, mem_stop.as_deref())));
                        }
                    }
                }
            }
            let Some(min_key) = ready_min(&heads).map(<[u8]>::to_vec) else {
                return Ok(Some((out, mem_stop.is_some(), None)));
            };
            if mem_stop.as_ref().is_some_and(|stop| min_key >= *stop) {
                return Ok(Some((out, true, None)));
            }
            let version = heads
                .iter()
                .filter_map(|head| match head {
                    DigestHead::Ready(key, v) if *key == min_key => Some(*v),
                    _ => None,
                })
                .max()
                .unwrap_or(0);
            let covered = digest_covered(cover.as_deref(), &min_key, version);
            if !covered {
                let size = min_key.len().saturating_add(per_entry_overhead);
                if !out.is_empty() && bytes.saturating_add(size) > max_bytes {
                    return Ok(Some((out, true, None)));
                }
                bytes = bytes.saturating_add(size);
            }
            consumed.clear();
            for (i, head) in heads.iter_mut().enumerate() {
                if matches!(head, DigestHead::Ready(key, _) if *key == min_key) {
                    *head = DigestHead::Consumed;
                    consumed.push(i);
                }
            }
            unsettled += consumed.len();
            if !covered {
                out.push((min_key.clone(), version));
            }
            consumed_last = Some(min_key);
            for &i in &consumed {
                if !matches!(heads[i], DigestHead::Consumed) {
                    continue;
                }
                let last = consumed_last.as_deref();
                let outcome = step_digest_head(
                    &mut cursors,
                    &mut heads,
                    i,
                    &mut unsettled,
                    last,
                    dev,
                    want,
                    &bucket_of,
                    after,
                    checkpoint,
                )
                .await?;
                match outcome {
                    DigestStep::Continue => {}
                    DigestStep::Invalidated => return Ok(None),
                    DigestStep::Expired(resume) => {
                        return Ok(Some(expired_page(out, resume, mem_stop.as_deref())));
                    }
                }
            }
        }
    }
}

enum DigestHead {
    Scanning(Option<Vec<u8>>),
    Consumed,
    Ready(Vec<u8>, u128),
    Done,
}

enum DigestStep {
    Continue,
    Invalidated,
    Expired(Vec<u8>),
}

fn ready_min(heads: &[DigestHead]) -> Option<&[u8]> {
    heads
        .iter()
        .filter_map(|head| match head {
            DigestHead::Ready(key, _) => Some(key.as_slice()),
            _ => None,
        })
        .min()
}

fn head_position<'a>(head: &'a DigestHead, last: Option<&'a [u8]>) -> Option<Option<&'a [u8]>> {
    match head {
        DigestHead::Scanning(pos) => Some(pos.as_deref()),
        DigestHead::Consumed => Some(last),
        DigestHead::Ready(..) | DigestHead::Done => None,
    }
}

fn lags(pos: Option<&[u8]>, min_key: Option<&[u8]>) -> bool {
    match (pos, min_key) {
        (Some(pos), Some(min_key)) => pos < min_key,
        _ => true,
    }
}

fn digest_frontier(heads: &[DigestHead], last: Option<&[u8]>, after: Option<&[u8]>) -> Option<Vec<u8>> {
    let min_key = ready_min(heads);
    let mut frontier: Option<&[u8]> = None;
    for head in heads {
        let Some(pos) = head_position(head, last) else {
            continue;
        };
        if !lags(pos, min_key) {
            continue;
        }
        let pos = pos?;
        if frontier.is_none_or(|current| pos < current) {
            frontier = Some(pos);
        }
    }
    frontier
        .filter(|frontier| after.is_none_or(|after| *frontier > after))
        .map(<[u8]>::to_vec)
}

fn expired_page(
    out: Vec<(Vec<u8>, u128)>,
    resume: Vec<u8>,
    mem_stop: Option<&[u8]>,
) -> ResumableDigestPage {
    if mem_stop.is_some_and(|stop| resume.as_slice() >= stop) {
        return (out, true, None);
    }
    (out, true, Some(resume))
}

#[allow(clippy::too_many_arguments)]
async fn step_digest_head<D: AsyncRead + BlockDevice, C: DigestCheckpoint>(
    cursors: &mut [DigestSourceCursor],
    heads: &mut [DigestHead],
    i: usize,
    unsettled: &mut usize,
    last: Option<&[u8]>,
    dev: &D,
    want: &std::collections::HashSet<u32>,
    bucket_of: &impl Fn(&[u8]) -> u32,
    after: Option<&[u8]>,
    checkpoint: &mut C,
) -> Result<DigestStep, EngineError> {
    match cursors[i].step(dev, want, bucket_of, after).await? {
        run::SidecarCursorStep::Entry((key, version)) => {
            heads[i] = DigestHead::Ready(key, version);
            *unsettled -= 1;
        }
        run::SidecarCursorStep::Done => {
            heads[i] = DigestHead::Done;
            *unsettled -= 1;
        }
        run::SidecarCursorStep::Checkpoint => {
            if let Some(scanned) = cursors[i].scanned_through() {
                let current = head_position(&heads[i], last).flatten();
                if current.is_none_or(|current| scanned > current) {
                    heads[i] = DigestHead::Scanning(Some(scanned.to_vec()));
                }
            }
            if !checkpoint.checkpoint().await {
                return Ok(DigestStep::Invalidated);
            }
            if checkpoint.expired() {
                if let Some(resume) = digest_frontier(heads, last, after) {
                    return Ok(DigestStep::Expired(resume));
                }
            }
        }
    }
    Ok(DigestStep::Continue)
}

#[allow(async_fn_in_trait)]
pub trait DigestCheckpoint {
    async fn checkpoint(&mut self) -> bool;

    fn expired(&self) -> bool {
        false
    }
}

struct NoopDigestCheckpoint;

impl DigestCheckpoint for NoopDigestCheckpoint {
    async fn checkpoint(&mut self) -> bool {
        true
    }
}

pub struct DigestPagePlan {
    mem: Vec<(Vec<u8>, u128)>,
    mem_stop: Option<Vec<u8>>,
    sources: Vec<run::VersionSource>,
    after: Option<Vec<u8>>,
    cover: Option<std::sync::Arc<FragmentSet>>,
}

fn digest_covered(cover: Option<&FragmentSet>, key: &[u8], version: u128) -> bool {
    cover.is_some_and(|cover| range_delete::is_covered(cover.cover(key), version))
}

impl DigestPagePlan {
    pub fn resident_bound_bytes(&self, max_bytes: usize) -> u64 {
        let mem = self
            .mem
            .iter()
            .map(|(key, _)| key.len().saturating_add(16))
            .sum::<usize>();
        let sources = self
            .sources
            .iter()
            .map(run::VersionSource::resident_bound_bytes)
            .sum::<usize>();
        let after = self.after.as_ref().map_or(0, Vec::len);
        mem.saturating_add(sources)
            .saturating_add(after)
            .saturating_add(max_bytes)
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

enum DigestSourceCursor {
    Materialized {
        entries: Vec<(Vec<u8>, u128)>,
        pos: usize,
    },
    Streaming(run::SidecarCursor),
    Blocks(run::BlockVersionCursor),
}

impl DigestSourceCursor {
    async fn step<D: AsyncRead + BlockDevice>(
        &mut self,
        dev: &D,
        want: &std::collections::HashSet<u32>,
        bucket_of: &impl Fn(&[u8]) -> u32,
        after: Option<&[u8]>,
    ) -> Result<run::SidecarCursorStep, EngineError> {
        match self {
            DigestSourceCursor::Materialized { entries, pos } => match entries.get(*pos) {
                Some(entry) => {
                    *pos += 1;
                    Ok(run::SidecarCursorStep::Entry(entry.clone()))
                }
                None => Ok(run::SidecarCursorStep::Done),
            },
            DigestSourceCursor::Streaming(cursor) => {
                Ok(cursor.next_matching_step(dev, want, bucket_of, after).await?)
            }
            DigestSourceCursor::Blocks(cursor) => {
                Ok(cursor.next_matching_step(dev, want, bucket_of, after).await?)
            }
        }
    }

    fn scanned_through(&self) -> Option<&[u8]> {
        match self {
            DigestSourceCursor::Materialized { .. } => None,
            DigestSourceCursor::Streaming(cursor) => cursor.scanned_through(),
            DigestSourceCursor::Blocks(cursor) => cursor.scanned_through(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
pub enum StagedVersion {
    Memtable(u128),
    Runs {
        key: Vec<u8>,
        probes: ProbeList,
        mutable: Option<u128>,
        generation: u64,
        cover: Option<u128>,
    },
    Absent,
}

impl StagedVersion {
    pub fn probe_bytes(&self) -> u64 {
        match self {
            StagedVersion::Runs { probes, .. } => probes.iter().map(|l| l.len() as u64).sum(),
            _ => 0,
        }
    }
}

fn merge_digest_sources(
    sources: Vec<Vec<(Vec<u8>, u128)>>,
    max_entries: usize,
    max_bytes: usize,
    per_entry_overhead: usize,
) -> (Vec<(Vec<u8>, u128)>, bool) {
    let mut cursors: Vec<usize> = vec![0; sources.len()];
    let mut out: Vec<(Vec<u8>, u128)> = Vec::new();
    let mut bytes = 0usize;
    let mut more = false;
    loop {
        let mut min_key: Option<&[u8]> = None;
        for (s, &c) in cursors.iter().enumerate() {
            if let Some((k, _)) = sources[s].get(c) {
                if min_key.is_none_or(|m| k.as_slice() < m) {
                    min_key = Some(k.as_slice());
                }
            }
        }
        let Some(min_key) = min_key else { break };
        let min_key = min_key.to_vec();

        let mut win: Option<u128> = None;
        if let Some((k, v)) = sources[0].get(cursors[0]) {
            if k.as_slice() == min_key.as_slice() {
                win = Some(*v);
                cursors[0] += 1;
            }
        }
        for s in (1..sources.len()).rev() {
            if let Some((k, v)) = sources[s].get(cursors[s]) {
                if k.as_slice() == min_key.as_slice() {
                    if win.is_none_or(|w| *v > w) {
                        win = Some(*v);
                    }
                    cursors[s] += 1;
                }
            }
        }
        let version = win.expect("min_key came from some source");

        let sz = min_key.len() + per_entry_overhead;
        if out.len() >= max_entries
            || (!out.is_empty() && bytes.saturating_add(sz) > max_bytes)
        {
            more = true;
            break;
        }
        bytes = bytes.saturating_add(sz);
        out.push((min_key, version));
    }
    (out, more)
}

