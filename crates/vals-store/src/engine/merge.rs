use super::*;

#[derive(Clone, Debug)]
pub(super) enum VlogRewrite {
    None,
    All,
    Victims(std::collections::HashSet<SegmentId>),
}

#[derive(Clone, Copy, Debug)]
enum PlanKind {
    Recurring,
    LoneGc,
    Pressure,
    Forget(usize),
}

impl PlanKind {
    fn rt_growth(self) -> usize {
        match self {
            PlanKind::Forget(growth) => growth,
            _ => 0,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeKind {
    Recurring,
    Forced,
    Pressure,
    ShadowFold,
    VlogGc,
    LoneGc,
    Forget,
}

impl MergeKind {
    pub const ALL: [MergeKind; 7] = [
        MergeKind::Recurring,
        MergeKind::Forced,
        MergeKind::Pressure,
        MergeKind::ShadowFold,
        MergeKind::VlogGc,
        MergeKind::LoneGc,
        MergeKind::Forget,
    ];

    pub fn label(self) -> &'static str {
        match self {
            MergeKind::Recurring => "recurring",
            MergeKind::Forced => "forced",
            MergeKind::Pressure => "pressure",
            MergeKind::ShadowFold => "shadow",
            MergeKind::VlogGc => "vlog_gc",
            MergeKind::LoneGc => "lonegc",
            MergeKind::Forget => "forget",
        }
    }
}

struct StartSelection {
    start: usize,
    end: usize,
    forced: bool,
}

struct HeadroomSelection {
    start: usize,
    end: usize,
    forced: bool,
    seg_bytes: usize,
    commit_headroom_segs: u64,
    fold: Option<ShadowFoldWindow>,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ShadowFoldWindow {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) predicted_bytes: u64,
}

pub(super) struct ShadowSources<D: BlockDevice> {
    sources: Vec<(run::RunStreamSource<D>, bool)>,
    metas: Vec<RunMeta>,
    complete: bool,
    streamed_bytes: u64,
}

pub(super) struct ShadowProbe<D: BlockDevice> {
    streams: Vec<(run::RunVersionStream<D>, bool)>,
    heap: std::collections::BinaryHeap<std::cmp::Reverse<(Vec<u8>, usize, u128)>>,
    primed: usize,
    present: bool,
    shadowed: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ShadowVerdict {
    Keep,
    Shadowed,
    Collect,
}

impl<D: BlockDevice> ShadowProbe<D> {
    pub(super) fn new(sources: Vec<(run::RunStreamSource<D>, bool)>, dev: &Arc<D>) -> Result<Self, EngineError> {
        let mut streams = Vec::with_capacity(sources.len());
        for (source, newer) in sources {
            let stream = source.version_stream_on(dev.clone())?.ok_or(EngineError::Snapshot(
                SnapshotError::Corrupt("shadow fold source has no version stream"),
            ))?;
            streams.push((stream, newer));
        }
        Ok(ShadowProbe {
            streams,
            heap: std::collections::BinaryHeap::new(),
            primed: 0,
            present: false,
            shadowed: false,
        })
    }

    fn advance(&mut self, i: usize) -> Result<(), EngineError> {
        loop {
            match self.streams[i].0.next_step()? {
                run::SidecarCursorStep::Entry((key, version)) => {
                    self.heap.push(std::cmp::Reverse((key, i, version)));
                    return Ok(());
                }
                run::SidecarCursorStep::Done => return Ok(()),
                run::SidecarCursorStep::Checkpoint => {}
            }
        }
    }

    fn catch_up(&mut self, key: &[u8], budget: &mut usize) -> Result<bool, EngineError> {
        while self.primed < self.streams.len() {
            if *budget == 0 {
                return Ok(false);
            }
            self.advance(self.primed)?;
            self.primed += 1;
            *budget -= 1;
        }
        while self.heap.peek().is_some_and(|h| h.0 .0.as_slice() < key) {
            if *budget == 0 {
                return Ok(false);
            }
            let std::cmp::Reverse((_, i, _)) = self.heap.pop().unwrap();
            self.advance(i)?;
            *budget -= 1;
        }
        Ok(true)
    }

    pub(super) fn step(
        &mut self,
        e: &SnapshotEntry,
        complete: bool,
        gc: Option<u128>,
        budget: &mut usize,
    ) -> Result<Option<ShadowVerdict>, EngineError> {
        if !self.catch_up(&e.key, budget)? {
            return Ok(None);
        }
        while self.heap.peek().is_some_and(|h| h.0 .0 == e.key) {
            if *budget == 0 {
                return Ok(None);
            }
            let std::cmp::Reverse((_, i, version)) = self.heap.pop().unwrap();
            self.present = true;
            self.shadowed |= if self.streams[i].1 { version >= e.version } else { version > e.version };
            self.advance(i)?;
            *budget -= 1;
        }
        let verdict = if self.shadowed {
            ShadowVerdict::Shadowed
        } else if complete && !self.present && e.tombstone && gc.is_some_and(|c| e.version < c) {
            ShadowVerdict::Collect
        } else {
            ShadowVerdict::Keep
        };
        self.present = false;
        self.shadowed = false;
        Ok(Some(verdict))
    }
}

enum PressureRewriteSelection {
    Selected(Option<(VlogRewrite, u64)>),
    NoPlan,
}

struct CompactPlanAssembly {
    merged_inputs: Vec<RunMeta>,
    gc_collect_before: Option<u128>,
    rt_gc_before: Option<u128>,
    usable: usize,
    sum_input_value_bytes: Option<u64>,
    pressure_rewrite: Option<(VlogRewrite, u64)>,
}

struct CompactPlanOutput {
    merged_inputs: Vec<RunMeta>,
    run_version: u16,
    segs: Vec<SegmentId>,
    seg_lbas: Vec<u64>,
    new_level: u32,
    gc_collect_before: Option<u128>,
    vlog_rewrite: VlogRewrite,
    vlog_segs: Vec<SegmentId>,
    vlog_seg_lbas: Vec<u64>,
    vlog_epochs: Vec<u64>,
    vlog_fence: bool,
    paced: bool,
    kind: MergeKind,
    inner: bool,
    rt: RtCompaction,
}

pub struct CompactPlan<D: BlockDevice> {
    pub(super) merged_inputs: Vec<RunMeta>,
    pub(super) segs: Vec<SegmentId>,
    seg_lbas: Vec<u64>,
    pub(super) seg_bytes: usize,
    block_size: usize,
    pub(super) reserved_blocks: u64,
    pub(super) blocks_per_segment: u64,
    pub(super) opts: RunBuildOpts,
    run_version: u16,
    new_level: u32,
    gc_collect_before: Option<u128>,
    forget: std::collections::HashMap<Vec<u8>, u128>,
    forget_floor: u64,
    released: Option<FragmentSet>,
    pub(super) vlog_rewrite: VlogRewrite,
    vlog_skip: std::collections::HashSet<SegmentId>,
    pub(super) vlog_segs: Vec<SegmentId>,
    vlog_seg_lbas: Vec<u64>,
    vlog_epochs: Vec<u64>,
    vlog_fence: bool,
    vlog_seg_capacity: usize,
    pub(super) paced: bool,
    dev: Arc<D>,
    pub(super) key_sink: Option<crate::keyfilter::KeyFilterSink>,
    pressure: bool,
    kind: MergeKind,
    inner: bool,
    pub(super) shadow: Option<ShadowSources<D>>,
    rts: Vec<RtRecord>,
    rt_filter: Option<FragmentSet>,
    rt_floor: u64,
    rt_accounted_seq: u64,
    rt_collected: Vec<RtRecord>,
}

impl<D: BlockDevice> CompactPlan<D> {
    fn new(
        dev: Arc<D>,
        alloc: &Allocator,
        block_size: usize,
        opts: RunBuildOpts,
        output: CompactPlanOutput,
    ) -> Self {
        let seg_bytes = alloc.blocks_per_segment() as usize * block_size;
        CompactPlan {
            merged_inputs: output.merged_inputs,
            segs: output.segs,
            seg_lbas: output.seg_lbas,
            seg_bytes,
            block_size,
            reserved_blocks: alloc.reserved_blocks(),
            blocks_per_segment: alloc.blocks_per_segment(),
            opts,
            run_version: output.run_version,
            new_level: output.new_level,
            gc_collect_before: output.gc_collect_before,
            forget: std::collections::HashMap::new(),
            forget_floor: 0,
            released: None,
            vlog_rewrite: output.vlog_rewrite,
            vlog_skip: std::collections::HashSet::new(),
            vlog_segs: output.vlog_segs,
            vlog_seg_lbas: output.vlog_seg_lbas,
            vlog_epochs: output.vlog_epochs,
            vlog_fence: output.vlog_fence,
            vlog_seg_capacity: seg_bytes,
            paced: output.paced,
            dev,
            key_sink: None,
            pressure: false,
            kind: output.kind,
            inner: output.inner,
            shadow: None,
            rts: output.rt.keep,
            rt_filter: output.rt.filter,
            rt_floor: output.rt.floor,
            rt_accounted_seq: output.rt.accounted_seq,
            rt_collected: output.rt.collected,
        }
    }

    pub fn is_pressure(&self) -> bool {
        self.pressure
    }

    pub fn kind(&self) -> MergeKind {
        self.kind
    }

    pub fn segments(&self) -> Vec<SegmentId> {
        self.segs
            .iter()
            .chain(self.vlog_segs.iter())
            .copied()
            .collect()
    }

    pub fn serving_dev(&self) -> &D {
        &self.dev
    }

    pub fn co_compacts(&self) -> bool {
        !matches!(self.vlog_rewrite, VlogRewrite::None)
    }

    pub fn input_bytes(&self) -> u64 {
        self.merged_inputs
            .iter()
            .map(|meta| meta.handle.len)
            .sum()
    }

    pub fn read_bytes(&self) -> u64 {
        self.input_bytes() + self.shadow.as_ref().map_or(0, |s| s.streamed_bytes)
    }

    pub fn working_set_bytes(&self) -> u64 {
        let sources = self.shadow.as_ref().map_or(0, |s| {
            s.sources.len() as u64 * 2 * run::SIDECAR_CURSOR_WINDOW as u64
        });
        self.merged_inputs.iter().map(|meta| meta.entry_count).sum::<u64>()
            .saturating_mul(40).saturating_add(2 * 1024 * 1024).saturating_add(sources)
    }
}

pub struct WrittenCompact<D: BlockDevice> {
    pub(super) handle: crate::run::RunHandle,
    pub(super) min_key: Vec<u8>,
    pub(super) max_key: Vec<u8>,
    pub(super) entry_count: u64,
    new_level: u32,
    merged_inputs: Vec<RunMeta>,
    unused_segs: Vec<SegmentId>,
    pub(super) dropped_tombstones: Vec<(Vec<u8>, u128)>,
    forget: std::collections::HashMap<Vec<u8>, u128>,
    tail_forgets: Vec<(Vec<u8>, u128)>,
    released: Option<FragmentSet>,
    value_segs: Vec<SegmentId>,
    value_bytes: u64,
    value_seg_bytes: Vec<u64>,
    value_bytes_rewritten: u64,
    tombstone_min_version: Option<u128>,
    pub(super) co_compacted: bool,
    vlog_out_used: Vec<SegmentId>,
    vlog_unused_segs: Vec<SegmentId>,
    pub(super) parts: crate::run::RunParts,
    _metadata: crate::attribution::MergeMetadata,
    dev: Arc<D>,
    key_sink: Option<crate::keyfilter::KeyFilterSink>,
    kind: MergeKind,
    inner: bool,
    shadow_sources: Vec<RunMeta>,
    rts: Vec<RtRecord>,
    rt_dominated: usize,
    rt_collected: Vec<RtRecord>,
    covered_dropped: u64,
}

#[cfg(test)]
impl<D: BlockDevice> WrittenCompact<D> {
    pub(crate) fn key_sink_for_test(&self) -> Option<&crate::keyfilter::KeyFilterSink> {
        self.key_sink.as_ref()
    }

    pub(crate) fn merged_inputs_for_test(&self) -> &[RunMeta] {
        &self.merged_inputs
    }
}

impl<D: BlockDevice> WrittenCompact<D> {
    pub fn kind(&self) -> MergeKind {
        self.kind
    }

    pub fn output_segments(&self) -> Vec<SegmentId> {
        self.handle
            .segs
            .iter()
            .chain(self.unused_segs.iter())
            .chain(self.vlog_out_used.iter())
            .chain(self.vlog_unused_segs.iter())
            .copied()
            .collect()
    }
}

pub async fn compact_and_write<D: BlockDevice>(
    plan: CompactPlan<D>,
    write_dev: Arc<D>,
) -> Result<WrittenCompact<D>, (Vec<SegmentId>, EngineError)> {
    compact_and_write_inner(plan, write_dev, None).await
}

pub async fn compact_and_write_cancellable<D: BlockDevice>(
    plan: CompactPlan<D>,
    write_dev: Arc<D>,
    cancel: &std::sync::atomic::AtomicBool,
) -> Result<WrittenCompact<D>, (Vec<SegmentId>, EngineError)> {
    compact_and_write_inner(plan, write_dev, Some(cancel)).await
}

async fn compact_and_write_inner<D: BlockDevice>(
    mut plan: CompactPlan<D>,
    write_dev: Arc<D>,
    cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<WrittenCompact<D>, (Vec<SegmentId>, EngineError)> {
    crate::attribution::MERGE_ATTEMPTS.add(1);
    let _timer = crate::attribution::Timer::new(&crate::attribution::MERGE_WALL, Some(&crate::attribution::MERGE_CPU));
    let detached = Allocator::new(
        write_dev.num_blocks(),
        plan.blocks_per_segment,
        plan.reserved_blocks,
    );

    let all_reserved: Vec<SegmentId> = plan
        .segs
        .iter()
        .chain(plan.vlog_segs.iter())
        .copied()
        .collect();

    let mut metadata = crate::attribution::MergeMetadata::default();
    let mut input_metadata = 0;
    let mut opened: Vec<DeviceRun<D>> = Vec::with_capacity(plan.merged_inputs.len());
    for meta in &plan.merged_inputs {
        match DeviceRun::open_scan(write_dev.clone(), &detached, meta.handle.clone()) {
            Ok(r) => {
                let stats = r.resident_stats();
                input_metadata += stats.filter_bytes + stats.fence_bytes;
                metadata.set(input_metadata);
                opened.push(r);
            },
            Err(e) => return Err((all_reserved, EngineError::from(e))),
        }
    }
    #[cfg(all(test, target_os = "linux", target_env = "gnu"))]
    super::tests::rss_bench::merge_snapshot(
        "merge-open",
        opened.iter().map(|r| r.resident_stats().filter_bytes).sum(),
        opened.iter().map(|r| r.resident_stats().fence_bytes).sum(),
        0,
    );
    let n_upper: u64 = plan.merged_inputs.iter().map(|m| m.entry_count).sum();
    let mut key_sink = plan.key_sink.take();

    let gc = plan.gc_collect_before;
    let forget = plan.forget.clone();
    let forget_floor = plan.forget_floor;
    let mut tail_forgets: Vec<(Vec<u8>, u128)> = Vec::new();
    let (mut probe, shadow_complete, shadow_sources) = match plan.shadow.take() {
        Some(s) => match ShadowProbe::new(s.sources, &write_dev) {
            Ok(probe) => (Some(probe), s.complete, s.metas),
            Err(e) => return Err((all_reserved, e)),
        },
        None => (None, false, Vec::new()),
    };
    let stream_gc = if probe.is_some() { None } else { gc };
    let rt_filter = plan.rt_filter.take();
    let mut drop_cover = rt_filter.as_ref().map(|f| f.cursor_from(&[]));
    let mut keep_cover = rt_filter.as_ref().map(|f| f.cursor_from(&[]));
    let released = plan.released.take();
    let mut released_cover = released.as_ref().map(|f| f.cursor_from(&[]));
    let mut covered_dropped: u64 = 0;
    let mut collected: Vec<(Vec<u8>, u128)> = Vec::new();
    let mut shadow_dropped: u64 = 0;
    let readahead = merge_readahead() && !(plan.paced && crate::pace::maint_pacing_active());
    let window_bytes = merge_read_window_bytes();
    let qd = merge_read_qd();
    let sources: Vec<CompactSource<'_, D>> = opened
        .iter()
        .map(|r| {
            if readahead {
                CompactSource::Prefetch(r.prefetch_scan(&detached, window_bytes, qd))
            } else {
                CompactSource::Scan(ScanSource {
                    scan: r.block_scan(None, None),
                    alloc: &detached,
                })
            }
        })
        .collect();
    let mut stream = MergeStream::new(sources, |e: &SnapshotEntry| {
        if drop_cover.as_mut().is_some_and(|c| range_delete::is_covered(c.cover(&e.key), e.version)) {
            return false;
        }
        let forgotten = forget.get(e.key.as_slice()).is_some_and(|v| e.version <= *v);
        if forgotten && replays_after_flush(e.seq, forget_floor) {
            tail_forgets.push((e.key.clone(), e.version));
        }
        (e.tombstone && stream_gc.is_some_and(|c| e.version < c)) || forgotten
    });

    let victims: Option<&std::collections::HashSet<SegmentId>> = match &plan.vlog_rewrite {
        VlogRewrite::Victims(v) => Some(v),
        _ => None,
    };
    let mut vlog_writer = match &plan.vlog_rewrite {
        VlogRewrite::None => None,
        VlogRewrite::All | VlogRewrite::Victims(_) => Some(crate::vlog::VlogWriter::new(
            &*write_dev,
            &plan.vlog_segs,
            &plan.vlog_seg_lbas,
            &plan.vlog_epochs,
            plan.vlog_seg_capacity,
            plan.block_size,
        )
        .fence_first_header(plan.vlog_fence)),
    };

    let sink = run::SegmentStreamSink::new(
        &*write_dev,
        &plan.segs,
        &plan.seg_lbas,
        plan.seg_bytes,
        plan.block_size,
    )
    .flush_every(if plan.paced {
        crate::pace::maint_flush_bytes()
    } else {
        0
    });
    let mut rts = std::mem::take(&mut plan.rts);
    let rt_dominated = crate::range_tombstone::drop_dominated(&mut rts, plan.rt_accounted_seq);
    let mut builder = run::RunStreamBuilder::new(sink, plan.opts, n_upper as usize)
        .version(plan.run_version)
        .range_tombstones(plan.rt_floor, rts.clone());
    metadata.set(input_metadata + builder.profile_bytes());

    let mut entry_count: u64 = 0;
    let mut tomb_min: Option<u128> = None;
    let mut liveness = ValueLivenessAcc::default();
    let mut value_bytes_rewritten: u64 = 0;

    vals_rt::yield_now().await;
    let mut chunk: Vec<SnapshotEntry> = Vec::with_capacity(CO_COMPACT_CHUNK);
    let mut pending: Option<SnapshotEntry> = None;
    let mut exhausted = false;
    while !exhausted {
        if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
            return Err((all_reserved, EngineError::Cancelled));
        }
        let iter_t = std::time::Instant::now();
        chunk.clear();
        let mut budget = CO_COMPACT_CHUNK;
        while chunk.len() < CO_COMPACT_CHUNK && budget > 0 {
            let e = match pending.take() {
                Some(e) => e,
                None => match stream.next() {
                    Ok(Some(e)) => {
                        if keep_cover.as_mut().is_some_and(|c| range_delete::is_covered(c.cover(&e.key), e.version))
                            || released_cover.as_mut().is_some_and(|c| range_delete::is_covered(c.cover(&e.key), e.version))
                        {
                            covered_dropped += 1;
                            budget = budget.saturating_sub(1);
                            continue;
                        }
                        e
                    }
                    Ok(None) => {
                        exhausted = true;
                        break;
                    }
                    Err(e) => return Err((all_reserved, e)),
                },
            };
            let Some(p) = probe.as_mut() else {
                chunk.push(e);
                continue;
            };
            budget -= 1;
            match p.step(&e, shadow_complete, gc, &mut budget) {
                Ok(Some(ShadowVerdict::Keep)) => chunk.push(e),
                Ok(Some(ShadowVerdict::Shadowed)) => shadow_dropped += 1,
                Ok(Some(ShadowVerdict::Collect)) => collected.push((e.key, e.version)),
                Ok(None) => {
                    pending = Some(e);
                    break;
                }
                Err(err) => return Err((all_reserved, err)),
            }
        }
        crate::attribution::MERGE_INPUT.add(iter_t.elapsed().as_nanos() as u64);
        if let Some(writer) = vlog_writer.as_mut() {
            if let Err(e) = rewrite_chunk_values(
                &detached,
                &*write_dev,
                plan.block_size,
                writer,
                &mut chunk,
                victims,
                &plan.vlog_skip,
                &mut value_bytes_rewritten,
            ) {
                return Err((all_reserved, e));
            }
        }
        for e in chunk.drain(..) {
            entry_count += 1;
            if e.tombstone {
                tomb_min = Some(tomb_min.map_or(e.version, |m| m.min(e.version)));
            }
            liveness.add(e.ptr);
            if let Some(sink) = key_sink.as_mut() {
                sink.insert(&e.key);
            }
            builder.push_owned(e);
        }
        if let Some(e) = builder.take_failure() {
            return Err((all_reserved, EngineError::EncodeIntegrity(e)));
        }
        metadata.set(input_metadata + builder.profile_bytes());
        if plan.paced {
            crate::pace::maint_pace_charge(iter_t.elapsed());
        }
        vals_rt::yield_now().await;
    }
    drop(probe);
    crate::attribution::SHADOW_FOLD_DROPPED.add(shadow_dropped);
    crate::attribution::SHADOW_FOLD_COLLECTED.add(collected.len() as u64);
    let mut dropped_tombstones = stream.into_dropped();
    dropped_tombstones.extend(collected);
    #[cfg(all(test, target_os = "linux", target_env = "gnu"))]
    super::tests::rss_bench::merge_snapshot(
        "builder-ready",
        opened.iter().map(|r| r.resident_stats().filter_bytes).sum(),
        opened.iter().map(|r| r.resident_stats().fence_bytes).sum(),
        builder.profile_bytes(),
    );
    drop(opened);
    metadata.set(builder.profile_bytes());

    if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
        return Err((all_reserved, EngineError::Cancelled));
    }
    let finish_t = std::time::Instant::now();
    let (vlog_out_used, vlog_unused_segs) = match vlog_writer {
        None => (Vec::new(), Vec::new()),
        Some(writer) => match writer.finish().await {
            Ok(used) => {
                let unused = plan.vlog_segs[used.len()..].to_vec();
                (used, unused)
            }
            Err(e) => return Err((all_reserved, EngineError::from(e))),
        },
    };
    let (value_segs, value_seg_bytes, value_bytes) = liveness.into_parts();
    let co_compacted = !matches!(plan.vlog_rewrite, VlogRewrite::None);

    if cancel.is_some_and(|c| c.load(std::sync::atomic::Ordering::Relaxed)) {
        return Err((all_reserved, EngineError::Cancelled));
    }
    let (handle, parts, built) = match builder.finish_with_bounds().await {
        Ok(h) => h,
        Err(e) => return Err((all_reserved, EngineError::from(e))),
    };
    metadata.set(parts.estimated_resident_bytes() as u64);
    #[cfg(all(test, target_os = "linux", target_env = "gnu"))]
    super::tests::rss_bench::merge_snapshot("builder-finish", 0, 0, parts.estimated_resident_bytes() as u64);
    let (min_key, max_key) = (built.min_key, built.max_key);
    if plan.paced {
        crate::pace::maint_pace_charge(finish_t.elapsed());
    }
    let n_used = handle.segs.len();
    let unused_segs = plan.segs[n_used..].to_vec();
    Ok(WrittenCompact {
        handle,
        min_key,
        max_key,
        entry_count,
        new_level: plan.new_level,
        merged_inputs: plan.merged_inputs,
        unused_segs,
        dropped_tombstones,
        forget,
        tail_forgets,
        released,
        value_segs,
        value_bytes,
        value_seg_bytes,
        value_bytes_rewritten,
        tombstone_min_version: tomb_min,
        co_compacted,
        vlog_out_used,
        vlog_unused_segs,
        parts,
        _metadata: metadata,
        dev: plan.dev,
        key_sink,
        kind: plan.kind,
        inner: plan.inner,
        shadow_sources,
        rts,
        rt_dominated,
        rt_collected: std::mem::take(&mut plan.rt_collected),
        covered_dropped,
    })
}

const CO_COMPACT_CHUNK: usize = 8192;

const MERGE_TIER_K: usize = 6;

const MERGE_TIER_MIN_K: usize = 4;

pub(super) const MERGE_FORCE_RUNS: usize = MERGE_TIER_K;

const MERGE_TIER_SIZE_RATIO: f64 = 1.5;

const MERGE_INPUT_CAP_BYTES: u64 = 2 * 1024 * 1024 * 1024;

const VICTIM_DEAD_FRACTION: f64 = 0.5;

pub(super) fn merge_tier_k() -> usize {
    std::env::var("VALS_MERGE_TIER_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&k| k >= 2)
        .unwrap_or(MERGE_TIER_K)
}

fn merge_tier_min_k() -> usize {
    let k = merge_tier_k();
    std::env::var("VALS_MERGE_TIER_MIN_K")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(MERGE_TIER_MIN_K)
        .clamp(2, k)
}

fn merge_force_runs() -> usize {
    std::env::var("VALS_MERGE_FORCE_RUNS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&r| r >= 2)
        .unwrap_or(MERGE_FORCE_RUNS)
}

pub(super) fn merge_tier_size_ratio() -> f64 {
    std::env::var("VALS_MERGE_TIER_SIZE_RATIO")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&r: &f64| r > 0.0)
        .unwrap_or(MERGE_TIER_SIZE_RATIO)
}

pub(super) fn merge_input_cap_bytes() -> u64 {
    std::env::var("VALS_MERGE_INPUT_CAP_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&b| b > 0)
        .unwrap_or(MERGE_INPUT_CAP_BYTES)
}

fn merge_freeze_bytes() -> u64 {
    std::env::var("VALS_MERGE_FREEZE_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&b| b > 0)
        .unwrap_or_else(merge_input_cap_bytes)
}

pub(super) fn victim_dead_fraction() -> f64 {
    std::env::var("VALS_VICTIM_DEAD_FRACTION")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&f: &f64| (0.0..=1.0).contains(&f))
        .unwrap_or(VICTIM_DEAD_FRACTION)
}

static ADMIT_BAND_PCT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();

pub fn configure_admit_band_pct(value: u64) -> Result<(), String> {
    if !(1..=100).contains(&value) { return Err("admit_band_pct must be in 1..=100".into()); }
    let active = ADMIT_BAND_PCT.get_or_init(|| value);
    if *active == value { Ok(()) } else { Err(format!("admit_band_pct conflicts with process-stable configuration {active}")) }
}

#[cfg(test)]
thread_local! {
    pub(super) static TEST_ADMIT_BAND_PCT: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn admit_band_pct() -> u64 {
    #[cfg(test)]
    if let Some(value) = TEST_ADMIT_BAND_PCT.with(|v| v.get()) { return value; }
    *ADMIT_BAND_PCT.get_or_init(|| 6)
}

const SHADOW_SAMPLE_BLOCKS: usize = 2;

const SHADOW_FOLD_MIN_GAIN_SEGS: u64 = 8;

pub(super) const SHADOW_EST_FREE_GATE: f64 = 0.60;

const SHADOW_FOLD_MAX_BACKOFF_SHIFT: u32 = 6;

pub(super) fn shadow_est_enabled() -> bool {
    std::env::var("VALS_SHADOW_EST").map_or(true, |v| v != "0")
}

pub(super) fn shadow_fold_enabled() -> bool {
    std::env::var("VALS_SHADOW_FOLD").map_or(true, |v| v != "0")
}

pub(super) fn shadow_sample_blocks() -> usize {
    std::env::var("VALS_SHADOW_SAMPLE_BLOCKS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(SHADOW_SAMPLE_BLOCKS)
}

pub(super) fn shadow_fold_min_gain_segs() -> u64 {
    std::env::var("VALS_SHADOW_FOLD_MIN_GAIN_SEGS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(SHADOW_FOLD_MIN_GAIN_SEGS)
}

const MERGE_READ_WINDOW_BYTES: usize = 2 * 1024 * 1024;

const MERGE_READ_QD: usize = 8;

fn merge_readahead() -> bool {
    !matches!(std::env::var("VALS_MERGE_READAHEAD").as_deref(), Ok("0"))
}

fn merge_read_window_bytes() -> usize {
    std::env::var("VALS_MERGE_READ_WINDOW_BYTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&b| b > 0)
        .unwrap_or(MERGE_READ_WINDOW_BYTES)
}

fn merge_read_qd() -> usize {
    std::env::var("VALS_MERGE_READ_QD")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&q| q >= 1)
        .unwrap_or(MERGE_READ_QD)
}

pub(super) fn select_capped_suffix(sizes: &[u64], size_cap: u64) -> usize {
    let Some(mut start) = sizes.len().checked_sub(1) else {
        return 0;
    };
    let mut cumulative = sizes[start];
    while start > 0 {
        let next = cumulative.saturating_add(sizes[start - 1]);
        if next > size_cap {
            break;
        }
        start -= 1;
        cumulative = next;
    }
    start
}

pub(super) fn select_merge_suffix(sizes: &[u64], k: usize, size_ratio: f64, size_cap: u64) -> usize {
    let n = sizes.len();
    if n == 0 {
        return 0;
    }
    let mut start = n - 1;
    let mut cum = sizes[n - 1];
    while start > 0 {
        let cand = start - 1;
        let cand_size = sizes[cand];
        if (n - cand) > k {
            break;
        }
        if cum.saturating_add(cand_size) > size_cap {
            break;
        }
        if (cand_size as f64) > size_ratio * (cum as f64) {
            break;
        }
        start = cand;
        cum = cum.saturating_add(cand_size);
    }
    start
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct MergePolicy {
    pub(super) k: usize,
    pub(super) min_k: usize,
    pub(super) size_ratio: f64,
    pub(super) size_cap: u64,
    pub(super) force_runs: usize,
    pub(super) freeze_bytes: u64,
}

impl MergePolicy {
    pub(super) fn from_env() -> Self {
        MergePolicy {
            k: merge_tier_k(),
            min_k: merge_tier_min_k(),
            size_ratio: merge_tier_size_ratio(),
            size_cap: merge_input_cap_bytes(),
            force_runs: merge_force_runs(),
            freeze_bytes: merge_freeze_bytes(),
        }
    }

    pub(super) fn frozen(&self, size: u64) -> bool {
        size >= self.freeze_bytes
            || size as f64 * (1.0 + 1.0 / self.size_ratio) > self.size_cap as f64
    }

    pub(super) fn active_start(&self, sizes: &[u64]) -> usize {
        sizes.iter().rposition(|&s| self.frozen(s)).map_or(0, |i| i + 1)
    }

    pub(super) fn tiered_suffix_start(&self, sizes: &[u64]) -> usize {
        let lo = self.active_start(sizes);
        lo + select_merge_suffix(&sizes[lo..], self.k, self.size_ratio, self.size_cap)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct MergeWindow {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) forced: bool,
}

pub(super) fn select_forced_window(sizes: &[u64], policy: &MergePolicy) -> Option<(usize, usize)> {
    let mut best: Option<(usize, usize, u64)> = None;
    for end in (2..=sizes.len()).rev() {
        let mut total = 0u64;
        let mut largest = 0u64;
        for start in (end.saturating_sub(policy.k)..end).rev() {
            let size = sizes[start];
            if policy.frozen(size) {
                break;
            }
            total = total.saturating_add(size);
            if total > policy.size_cap {
                break;
            }
            largest = largest.max(size);
            let fanin = end - start;
            if fanin < 2 || largest as f64 > policy.size_ratio * (total - largest) as f64 {
                continue;
            }
            let cheaper = best.is_none_or(|(s, e, t)| {
                let (cost, best_cost) = (
                    total as u128 * (e - s - 1) as u128,
                    t as u128 * (fanin - 1) as u128,
                );
                cost < best_cost || (cost == best_cost && e == end)
            });
            if cheaper {
                best = Some((start, end, total));
            }
        }
    }
    best.map(|(start, end, _)| (start, end))
}

pub(super) fn select_recurring_merge(sizes: &[u64], policy: &MergePolicy) -> Option<MergeWindow> {
    let n = sizes.len();
    if n < 2 {
        return None;
    }
    let start = policy.tiered_suffix_start(sizes);
    if n - start >= policy.min_k.max(2) {
        return Some(MergeWindow { start, end: n, forced: false });
    }
    let lo = policy.active_start(sizes);
    if n - lo >= policy.force_runs {
        let unbalanced = MergePolicy { size_ratio: f64::INFINITY, ..*policy };
        let (start, end) = select_forced_window(&sizes[lo..], policy)
            .or_else(|| select_forced_window(&sizes[lo..], &unbalanced))?;
        return Some(MergeWindow { start: lo + start, end: lo + end, forced: true });
    }
    None
}

static INNER_WINDOW_MERGES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn inner_window_merges_total() -> u64 {
    INNER_WINDOW_MERGES.load(std::sync::atomic::Ordering::Relaxed)
}

const COMMIT_FORGET_WARN_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

pub(super) static COMMIT_FORGET_FAILURES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static COMMIT_FORGET_WARNED: std::sync::Mutex<Option<std::time::Instant>> = std::sync::Mutex::new(None);

pub(super) fn replays_after_flush(seq: u64, flushed_through_seq: u64) -> bool {
    flushed_through_seq == 0 || seq > flushed_through_seq
}

fn note_commit_forget_failures(failed: u64, err: &LogError) {
    let total = COMMIT_FORGET_FAILURES.fetch_add(failed, std::sync::atomic::Ordering::Relaxed) + failed;
    let now = std::time::Instant::now();
    let mut warned = COMMIT_FORGET_WARNED.lock().unwrap_or_else(|p| p.into_inner());
    if warned.is_none_or(|t| now.duration_since(t) >= COMMIT_FORGET_WARN_EVERY) {
        *warned = Some(now);
        log::warn!(
            "engine: merge commit could not append {failed} forget delete record(s) ({err}); those keys stay live at their current version ({total} since start; logged at most once a minute)"
        );
    }
}

pub(super) const MAINT_RESERVE_DEAD_MIN_FRACTION: f64 = 0.10;

pub(super) const MAINT_RESERVE_PLAN_MARGIN_SEGS: u64 = 16;

pub(super) const STALL_TERMINAL_MIN_PLAN_FAILURES: u32 = 3;

#[allow(clippy::too_many_arguments)]
fn rewrite_chunk_values<D: BlockDevice>(
    alloc: &Allocator,
    dev: &D,
    block_size: usize,
    writer: &mut crate::vlog::VlogWriter<'_, D>,
    chunk: &mut [SnapshotEntry],
    victims: Option<&std::collections::HashSet<SegmentId>>,
    skip: &std::collections::HashSet<SegmentId>,
    bytes_rewritten: &mut u64,
) -> Result<(), EngineError> {
    let rewrite: Vec<usize> = chunk
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            victims.is_none_or(|v| v.contains(&e.ptr.segment)) && !skip.contains(&e.ptr.segment)
        })
        .map(|(i, _)| i)
        .collect();
    if rewrite.is_empty() {
        return Ok(());
    }
    let mut items: Vec<(u64, ValuePtr)> = Vec::with_capacity(rewrite.len());
    for &i in &rewrite {
        let e = &chunk[i];
        match crate::vlog::value_ptr_start(alloc, block_size, &e.ptr) {
            Ok(start) => items.push((start, e.ptr)),
            Err(LogError::Corrupt(reason)) => {
                return Err(EngineError::CorruptValueRecord {
                    key: e.key.clone().into_boxed_slice(),
                    ptr: e.ptr,
                    reason,
                })
            }
            Err(e) => return Err(EngineError::from(e)),
        }
    }
    let seg_bytes = alloc.blocks_per_segment() * block_size as u64;
    let plan_spans = crate::vlog::plan_read_spans_detached(block_size, seg_bytes, &items)?;
    let expected = |slot: usize| {
        let e = &chunk[rewrite[slot]];
        (e.key.as_slice(), e.version)
    };
    let records = match plan_spans.execute_sync_checked(dev, &expected) {
        Ok(records) => records,
        Err(crate::vlog::SpanReadError {
            failed: Some((slot, ptr)),
            error: LogError::Corrupt(reason),
        }) => {
            return Err(EngineError::CorruptValueRecord {
                key: chunk[rewrite[slot]].key.clone().into_boxed_slice(),
                ptr,
                reason,
            })
        }
        Err(e) => return Err(EngineError::from(e.error)),
    };
    for (&i, rec) in rewrite.iter().zip(records) {
        let e = &mut chunk[i];
        let new_ptr = writer
            .append(
                RecordKind::Put,
                e.tombstone,
                e.seq,
                e.version,
                &e.key,
                &rec.value,
            )
            .map_err(EngineError::from)?;
        *bytes_rewritten += new_ptr.len as u64;
        e.ptr = new_ptr;
    }
    Ok(())
}

type MergeOutput = (Vec<SnapshotEntry>, Vec<(Vec<u8>, u128)>);

type MergeAllOutput = (CompactRunsStats, Vec<(Vec<u8>, u128)>);

#[cfg(test)]
pub(super) fn merge_runs<D: BlockDevice, F: FnMut(&SnapshotEntry) -> bool>(
    opened: &[DeviceRun<D>],
    alloc: &Allocator,
    drop_winner: F,
) -> Result<MergeOutput, EngineError> {
    let cursors: Vec<run::RunScan<'_, D>> =
        opened.iter().map(|r| r.block_scan(None, None)).collect();
    merge_cursors(cursors, alloc, drop_winner)
}

pub(super) trait MergeSource {
    fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, EngineError>;
}

struct ScanSource<'a, D: BlockDevice> {
    scan: run::RunScan<'a, D>,
    alloc: &'a Allocator,
}

impl<D: BlockDevice> MergeSource for ScanSource<'_, D> {
    fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, EngineError> {
        self.scan.next(self.alloc).map_err(EngineError::from)
    }
}

enum CompactSource<'a, D: BlockDevice> {
    Scan(ScanSource<'a, D>),
    Prefetch(run::RunPrefetchScan<'a, D>),
}

impl<D: BlockDevice> MergeSource for CompactSource<'_, D> {
    fn next_entry(&mut self) -> Result<Option<SnapshotEntry>, EngineError> {
        match self {
            CompactSource::Scan(s) => s.next_entry(),
            CompactSource::Prefetch(p) => p.next_entry().map_err(EngineError::from),
        }
    }
}

fn merge_cursors<D: BlockDevice, F: FnMut(&SnapshotEntry) -> bool>(
    cursors: Vec<run::RunScan<'_, D>>,
    alloc: &Allocator,
    drop_winner: F,
) -> Result<MergeOutput, EngineError> {
    let sources: Vec<ScanSource<'_, D>> = cursors
        .into_iter()
        .map(|scan| ScanSource { scan, alloc })
        .collect();
    merge_sources(sources, drop_winner)
}

pub(super) struct MergeStream<S: MergeSource, F: FnMut(&SnapshotEntry) -> bool> {
    sources: Vec<S>,
    heads: Vec<Option<SnapshotEntry>>,
    primed: bool,
    last_key: Option<Vec<u8>>,
    dropped: Vec<(Vec<u8>, u128)>,
    drop_winner: F,
}

impl<S: MergeSource, F: FnMut(&SnapshotEntry) -> bool> MergeStream<S, F> {
    pub(super) fn new(sources: Vec<S>, drop_winner: F) -> Self {
        MergeStream {
            heads: Vec::with_capacity(sources.len()),
            sources,
            primed: false,
            last_key: None,
            dropped: Vec::new(),
            drop_winner,
        }
    }

    pub(super) fn next(&mut self) -> Result<Option<SnapshotEntry>, EngineError> {
        if !self.primed {
            for s in &mut self.sources {
                self.heads.push(s.next_entry()?);
            }
            self.primed = true;
        }
        loop {
            let mut min_key: Option<&[u8]> = None;
            for h in self.heads.iter().flatten() {
                if min_key.is_none_or(|m| h.key.as_slice() < m) {
                    min_key = Some(h.key.as_slice());
                }
            }
            let Some(min_key) = min_key else {
                return Ok(None);
            };

            if let Some(last) = &self.last_key {
                if min_key <= last.as_slice() {
                    return Err(EngineError::Snapshot(SnapshotError::Corrupt(
                        "merge: run cursor not strictly ascending",
                    )));
                }
            }
            let min_key = min_key.to_vec();

            let mut best: Option<SnapshotEntry> = None;
            for i in (0..self.heads.len()).rev() {
                if self.heads[i].as_ref().is_some_and(|h| h.key == min_key) {
                    let h = self.heads[i].take().unwrap();
                    if best.as_ref().is_none_or(|b| h.version > b.version) {
                        best = Some(h);
                    }
                    self.heads[i] = self.sources[i].next_entry()?;
                }
            }
            let best = best.expect("min_key came from some run head");
            self.last_key = Some(min_key);
            if (self.drop_winner)(&best) {
                self.dropped.push((best.key.clone(), best.version));
                continue;
            }
            return Ok(Some(best));
        }
    }

    fn into_dropped(self) -> Vec<(Vec<u8>, u128)> {
        self.dropped
    }
}

pub(super) fn merge_sources<S: MergeSource, F: FnMut(&SnapshotEntry) -> bool>(
    sources: Vec<S>,
    drop_winner: F,
) -> Result<MergeOutput, EngineError> {
    let mut stream = MergeStream::new(sources, drop_winner);
    let mut merged: Vec<SnapshotEntry> = Vec::new();
    while let Some(e) = stream.next()? {
        merged.push(e);
    }
    Ok((merged, stream.into_dropped()))
}

fn co_compact_opt_in() -> bool {
    matches!(std::env::var("VALS_VLOG_CO_COMPACT").as_deref(), Ok("1"))
}

pub(super) fn vlog_gc_enabled() -> bool {
    std::env::var("VALS_VLOG_GC").map_or(true, |v| v != "0")
}

pub(super) fn resume_next_epoch(max_epoch_all: Option<u64>, covered_epoch: Option<u64>) -> u64 {
    match (max_epoch_all, covered_epoch) {
        (Some(a), Some(c)) => a.max(c) + 1,
        (Some(a), None) => a + 1,
        (None, Some(c)) => c + 1,
        (None, None) => 0,
    }
}

impl<D: BlockDevice> Engine<D> {
    pub fn compact_in_flight(&self) -> bool {
        self.compact_in_flight
    }

    pub fn run_count(&self) -> usize {
        self.runs.len()
    }

    pub fn frozen_run_count(&self) -> usize {
        self.runs.iter().filter(|or| self.merge_policy.frozen(or.meta.handle.len)).count()
    }

    pub fn stranded_run_count(&self) -> usize {
        let end = self.runs.iter().rposition(|or| self.merge_policy.frozen(or.meta.handle.len));
        end.map_or(0, |end| self.runs[..end].iter()
            .filter(|or| !self.merge_policy.frozen(or.meta.handle.len)).count())
    }

    pub fn max_run_bytes(&self) -> u64 {
        self.runs.iter().map(|or| or.meta.handle.len).max().unwrap_or(0)
    }

    #[cfg(any(test, feature = "test-fixtures"))]
    pub fn storage_audit_for_test(&self) -> Result<u64, String> {
        let mut owner: std::collections::HashMap<SegmentId, String> = std::collections::HashMap::new();
        let mut claim = |seg: SegmentId, who: String| -> Result<(), String> {
            if self.alloc.is_free(seg) {
                return Err(format!("{who} references free segment {seg:?}"));
            }
            match owner.insert(seg, who.clone()) {
                Some(prev) => Err(format!("segment {seg:?} owned by both {prev} and {who}")),
                None => Ok(()),
            }
        };
        for (i, or) in self.runs.iter().enumerate() {
            for seg in &or.meta.handle.segs {
                claim(*seg, format!("run[{i}] index"))?;
            }
        }
        let mut pinned = std::collections::HashSet::new();
        self.extend_pinned_segments(&mut pinned);
        for seg in pinned {
            claim(seg, "pinned retired run".to_string())?;
        }
        if let Some(live) = &self.live_manifest {
            for seg in &live.segs {
                claim(*seg, "manifest".to_string())?;
            }
        }
        let mut values: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        for or in &self.runs {
            values.extend(or.meta.value_segs.iter().flatten().copied());
        }
        values.extend(self.flush_value_segs.iter().copied());
        values.extend(self.log.tail_segs_through(u64::MAX));
        values.extend(self.log.open_segment());
        self.for_each_mutable_value_segment(&mut |seg| {
            values.insert(seg);
        });
        for seg in values {
            if self.alloc.is_free(seg) {
                return Err(format!("value segment {seg:?} is free while referenced"));
            }
            if let Some(prev) = owner.get(&seg) {
                return Err(format!("value segment {seg:?} is also {prev}"));
            }
        }
        let mut entries = 0u64;
        for (i, or) in self.runs.iter().enumerate() {
            let reopened = DeviceRun::open(self.dev.clone(), &self.alloc, or.meta.handle.clone())
                .map_err(|e| format!("run[{i}] reopen: {e:?}"))?;
            let mut scan = reopened.block_scan(None, None);
            let mut n = 0u64;
            while scan.next(&self.alloc).map_err(|e| format!("run[{i}] scan after {n}: {e:?}"))?.is_some() {
                n += 1;
            }
            if n != or.meta.entry_count {
                return Err(format!("run[{i}] scanned {n} entries, manifest says {}", or.meta.entry_count));
            }
            entries += n;
        }
        Ok(entries)
    }

    pub fn recurring_merge_selectable(&self) -> bool {
        if self.poisoned || self.compact_in_flight {
            return false;
        }
        let sizes: Vec<u64> = self.runs.iter().map(|or| or.meta.handle.len).collect();
        select_recurring_merge(&sizes, &self.merge_policy).is_some()
    }

    pub fn run_handles(&self) -> Vec<crate::run::RunHandle> {
        self.runs.iter().map(|or| or.meta.handle.clone()).collect()
    }

    pub fn runs_tombstone_min_version(&self) -> Option<u128> {
        self.runs.iter().filter_map(|or| or.meta.tombstone_min_version).min()
    }

    pub fn mutable_tombstone_min_version(&self) -> Option<u128> {
        self.mutable_tombstones.filter(|_| !self.index.is_empty()).map(|(min, _)| min)
    }

    pub fn run_version_stream(
        &self,
        handle: &crate::run::RunHandle,
    ) -> Result<Option<crate::run::RunVersionStream<D>>, EngineError> {
        let Some(or) = self.runs.iter().find(|or| &or.meta.handle == handle) else {
            return Ok(None);
        };
        Ok(or.run.version_stream(&self.alloc)?)
    }

    pub fn run_sidecar_versions(
        &self,
        handle: &crate::run::RunHandle,
    ) -> Result<Option<crate::run::SidecarVersions>, EngineError> {
        let Some(or) = self.runs.iter().find(|or| &or.meta.handle == handle) else {
            return Ok(None);
        };
        Ok(Some(or.run.sidecar_versions(&self.alloc)?))
    }

    pub fn runs_removed_epoch(&self) -> u64 {
        self.runs_removed_epoch
    }

    pub fn flush_in_flight(&self) -> bool {
        self.flush_in_flight
    }

    pub fn compact_runs(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
    ) -> Result<CompactRunsStats, EngineError> {
        if self.poisoned || self.runs.len() < 2 {
            return Ok(CompactRunsStats::default());
        }

        let mutable: std::collections::HashMap<Box<[u8]>, u128> =
            self.index.versions().into_iter().collect();
        let (mut stats, dropped) = self.merge_all_runs(opts, gc_collect_before, |e| {
            e.tombstone
                && gc_collect_before.is_some_and(|c| e.version < c)
                && mutable
                    .get(e.key.as_slice())
                    .is_none_or(|version| *version > e.version)
        })?;

        let mut current_dropped: Vec<(Vec<u8>, u128)> = Vec::new();
        for (k, v) in dropped {
            match self.index.get(&k) {
                Some(e) => debug_assert!(
                    e.version > v,
                    "GC-dropped tombstone not superseded by its memtable copy"
                ),
                None => current_dropped.push((k, v)),
            }
        }
        stats.current_dropped_tombstones = current_dropped;
        Ok(stats)
    }

    pub(super) fn merge_all_runs(
        &mut self,
        opts: RunBuildOpts,
        rt_gc_before: Option<u128>,
        mut drop_winner: impl FnMut(&SnapshotEntry) -> bool,
    ) -> Result<MergeAllOutput, EngineError> {
        let inputs: Vec<RunMeta> = self.runs.iter().map(|or| or.meta.clone()).collect();
        let rt = self.rt_merge_plan(&inputs, rt_gc_before);
        let cursors: Vec<run::RunScan<'_, D>> = self
            .runs
            .iter()
            .map(|or| or.run.block_scan(None, None))
            .collect();
        let mut drop_cover = rt.filter.as_ref().map(|f| f.cursor_from(&[]));
        let (mut merged, dropped) = merge_cursors(cursors, &self.alloc, |e: &SnapshotEntry| {
            !drop_cover.as_mut().is_some_and(|c| range_delete::is_covered(c.cover(&e.key), e.version)) && drop_winner(e)
        })?;
        let mut covered_dropped = 0u64;
        if let Some(filter) = &rt.filter {
            let mut cover = filter.cursor_from(&[]);
            merged.retain(|e| {
                let keep = !range_delete::is_covered(cover.cover(&e.key), e.version);
                covered_dropped += u64::from(!keep);
                keep
            });
        }

        let entry_count = merged.len() as u64;

        let new_level = self.runs.iter().map(|or| or.meta.level).max().unwrap_or(0) + 1;

        let mut covered = self.key_filter_inputs_covered(&inputs);
        let mut rebuild_covered = self.key_filter_rebuild_inputs_covered(&inputs);
        if let Some(mut sink) = self.key_filter_merge_sink(&inputs) {
            for e in &merged {
                sink.insert(&e.key);
            }
            covered |= self.key_filter.owns(&sink);
            rebuild_covered |= self.key_filter.owns_rebuild(&sink);
        }
        let run_version = run::merge_write_version(self.runs.iter().map(|or| or.run.format_version()));
        let RtCompaction { floor: rt_floor, keep: mut rts, collected: rt_collected, accounted_seq, .. } = rt;
        let rt_dominated = crate::range_tombstone::drop_dominated(&mut rts, accounted_seq);
        let (run_bytes, built) = run::build_run_checked_rts(
            &merged,
            opts,
            run_version,
            run::EncodePath::Merge,
            run::RunSource::Inline,
            run::RunRts { seq_floor: rt_floor, records: &rts },
        )
        .map_err(EngineError::EncodeIntegrity)?;
        let (min_key, max_key) = (built.min_key, built.max_key);
        let handle = run::write_run_source(&*self.dev, &mut self.alloc, &run_bytes, 4)?;
        let run = match DeviceRun::open(self.dev.clone(), &self.alloc, handle.clone()) {
            Ok(r) => r,
            Err(e) => {
                for s in &handle.segs {
                    let _ = self.alloc.free(*s);
                }
                return Err(e.into());
            }
        };
        let (value_segs, value_seg_bytes, value_bytes) = value_liveness(&merged);

        let seg_bytes = self.alloc.blocks_per_segment() * self.dev.block_size() as u64;
        let input_bytes: u64 = self
            .runs
            .iter()
            .map(|or| {
                (or.meta.handle.segs.len() as u64) * seg_bytes
                    + or.meta.value_bytes.unwrap_or(0)
            })
            .sum();
        let output_bytes: u64 = (handle.segs.len() as u64) * seg_bytes;

        let meta = RunMeta {
            handle: handle.clone(),
            min_key,
            max_key,
            entry_count,
            level: new_level,
            value_segs: Some(value_segs),
            value_bytes: Some(value_bytes),
            tombstone_min_version: tombstone_min_version(&merged),
            value_seg_bytes: Some(value_seg_bytes),
        };

        let covered_seq = self.last_flush_seq;
        let covered_epoch = self.last_flush_epoch;
        let generation = self.manifest_gen + 1;
        let manifest = Manifest {
            generation,
            covered_seq,
            covered_epoch,
            runs: vec![meta.clone()],
        };
        let live = snapshot_io::write_manifest(
            &*self.dev,
            &mut self.alloc,
            &manifest,
            generation,
            self.live_manifest.as_ref(),
        )
        .map_err(EngineError::from)?;

        let removed: Vec<(crate::run::RunHandle, u64)> = self
            .runs
            .iter()
            .map(|or| (or.meta.handle.clone(), pins::retained_index_bytes(&or.run)))
            .collect();
        let runs_merged = self.runs.len();

        let rt_count = self.run_rt_count();
        let mut opened = OpenRun::new(meta, run, covered).with_rts(rts).with_max_tree(!self.rts.fragments.is_empty());
        opened.rebuild_covered = rebuild_covered;
        self.runs = vec![opened];
        self.note_run_rts_replaced(false, rt_count);
        self.runs_removed_epoch += 1;
        self.manifest_gen = generation;
        self.live_manifest = Some(live);
        self.bump_storage_generation();
        self.key_filter_maintain();
        range_delete::note_covered_dropped(covered_dropped);
        range_delete::note_dominated_dropped(rt_dominated);
        self.finish_rt_collection(&rt_collected);

        let segments_freed = self.retire_removed_runs(removed);

        self.refresh_data_reserve();
        if (self.winner_directory.is_some() || winner_directory_requested())
            && (!dropped.is_empty() || covered_dropped > 0 || !rt_collected.is_empty())
        {
            self.refresh_winner_directory_after_commit();
        }

        Ok((
            CompactRunsStats {
                runs_merged,
                entries_written: entry_count as usize,
                segments_freed,
                input_bytes,
                output_bytes,
                current_dropped_tombstones: Vec::new(),
                covered_dropped,
                collected_range_tombstones: rt_collected,
                released_range_tombstones: 0,
                dominated_range_tombstones: rt_dominated,
            },
            dropped,
        ))
    }

    fn note_pressure_infeasible(&mut self, kind: PlanKind) {
        if matches!(kind, PlanKind::Pressure) && self.at_maintenance_floor() {
            self.pressure_infeasible_streak =
                self.pressure_infeasible_streak.saturating_add(1);
        }
    }

    pub fn compact_plan(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        self.compact_plan_inner(opts, gc_collect_before, PlanKind::Recurring)
    }

    pub fn compact_plan_lone_gc(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        if gc_collect_before.is_none() {
            return Ok(None);
        }
        let next = self.gc_fold_start() + 1;
        let plan = self.compact_plan_inner(opts, gc_collect_before, PlanKind::LoneGc);
        self.gc_fold_cursor = next;
        plan
    }

    fn gc_fold_start(&self) -> usize {
        if self.gc_fold_cursor < self.runs.len() { self.gc_fold_cursor } else { 0 }
    }

    fn gc_fold_selection(&self, sizes: &[u64]) -> StartSelection {
        let start = self.gc_fold_start();
        let mut end = start + 1;
        let mut bytes = sizes[start];
        while end < sizes.len() && bytes.saturating_add(sizes[end]) <= self.merge_policy.size_cap {
            bytes += sizes[end];
            end += 1;
        }
        StartSelection { start, end, forced: false }
    }

    pub fn compact_plan_pressure(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        let plan = self.compact_plan_inner(opts, gc_collect_before, PlanKind::Pressure)?;
        if plan.is_none() && !self.at_maintenance_floor() {
            return self.compact_plan_inner(opts, gc_collect_before, PlanKind::Recurring);
        }
        Ok(plan)
    }

    pub fn can_plan_forget(&self) -> bool {
        !self.poisoned && !self.runs.is_empty() && !self.flush_in_flight && !self.compact_in_flight
    }

    pub fn forget_plan(
        &mut self,
        opts: RunBuildOpts,
        keys: &std::collections::HashMap<Vec<u8>, u128>,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        if keys.is_empty() || !self.can_plan_forget() {
            return Ok(None);
        }
        self.forget_plan_releasing(opts, keys, &FragmentSet::default())
    }

    pub fn forget_plan_releasing(
        &mut self,
        opts: RunBuildOpts,
        keys: &std::collections::HashMap<Vec<u8>, u128>,
        released: &FragmentSet,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        let released = self.releasable_range_tombstones(released);
        if (keys.is_empty() && released.is_empty()) || !self.can_plan_forget() {
            return Ok(None);
        }
        let growth = if released.is_empty() {
            0
        } else {
            let records: Vec<RtRecord> = self.runs.iter().flat_map(|or| or.rts.iter().cloned()).collect();
            let clipped: Vec<RtRecord> = records.iter().flat_map(|r| r.without(&released)).collect();
            crate::range_tombstone::rt_block_len(&clipped).saturating_sub(crate::range_tombstone::rt_block_len(&records))
        };
        let Some(mut plan) = self.compact_plan_inner(opts, None, PlanKind::Forget(growth))? else {
            return Ok(None);
        };
        plan.forget = keys.clone();
        plan.forget_floor = self.last_flush_seq;
        if !released.is_empty() {
            plan.rts = plan.rts.iter().flat_map(|r| r.without(&released)).collect();
            self.remove_released_mutable(&released);
            plan.released = Some(released);
        }
        Ok(Some(plan))
    }

    fn compact_plan_eligible(&self, kind: PlanKind) -> bool {
        let min_input_runs = match kind {
            PlanKind::Recurring => 2,
            PlanKind::LoneGc | PlanKind::Pressure | PlanKind::Forget(_) => 1,
        };
        !self.poisoned && self.runs.len() >= min_input_runs && !self.compact_in_flight
    }

    fn pressure_plan_preamble(
        &mut self,
        opts: RunBuildOpts,
        kind: PlanKind,
    ) -> Option<CompactPlan<D>> {
        if !matches!(kind, PlanKind::Pressure) {
            return None;
        }
        if self.reclaim_orphan_segments() > 0 {
            self.pressure_infeasible_streak = 0;
        }
        if vlog_gc_enabled() {
            if let Some(plan) = self.plan_aged_vlog_gc(opts) {
                self.compact_in_flight = true;
                self.pressure_infeasible_streak = 0;
                return Some(plan);
            }
        }
        None
    }

    fn select_compaction_start(&self, kind: PlanKind) -> Option<StartSelection> {
        let sizes: Vec<u64> = self.runs.iter().map(|or| or.meta.handle.len).collect();
        let selection = match kind {
            PlanKind::Forget(_) => StartSelection {
                start: 0,
                end: sizes.len(),
                forced: false,
            },
            PlanKind::Recurring => {
                let window = select_recurring_merge(&sizes, &self.merge_policy)?;
                StartSelection {
                    start: window.start,
                    end: window.end,
                    forced: window.forced,
                }
            }
            PlanKind::LoneGc | PlanKind::Pressure => StartSelection {
                start: select_capped_suffix(&sizes, self.merge_policy.size_cap),
                end: sizes.len(),
                forced: false,
            },
        };
        if matches!(kind, PlanKind::LoneGc) && selection.start != 0 {
            if self.runs.iter().any(|or| or.run.format_version() < 3) {
                return None;
            }
            return Some(self.gc_fold_selection(&sizes));
        }
        Some(selection)
    }

    fn fit_compaction_headroom(
        &mut self,
        kind: PlanKind,
        selection: StartSelection,
        gc: Option<u128>,
    ) -> Result<HeadroomSelection, EngineError> {
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let commit_headroom_segs = self
            .manifest_headroom_segs()
            .saturating_add(self.flush_headroom_segs(seg_bytes));
        let pressure_at_floor =
            matches!(kind, PlanKind::Pressure) && self.at_maintenance_floor();
        let one_run_fits = pressure_at_floor
            && self
                .runs
                .last()
                .is_some_and(|newest| {
                    self.one_run_victim_needed_segs(
                        newest,
                        seg_bytes,
                        commit_headroom_segs,
                        victim_dead_fraction(),
                    )
                    .is_some_and(|needed| needed <= self.alloc.free_segments())
                });
        let pressure_floor_min_two =
            pressure_at_floor && self.runs.len() >= 2 && !one_run_fits;
        let mut start = selection.start;
        let mut end = selection.end;
        if pressure_floor_min_two && self.runs.len() - start == 1 {
            start = self.runs.len() - 2;
        }
        let fold = match kind {
            PlanKind::Pressure => self.shadow_fold_window(seg_bytes, commit_headroom_segs, gc),
            PlanKind::LoneGc if start != 0 || end != self.runs.len() => {
                Some(ShadowFoldWindow { start, end, predicted_bytes: 0 })
            }
            _ => None,
        };
        if let Some(window) = fold {
            start = window.start;
            end = window.end;
        }
        loop {
            let bound = OpenRun::rewrite_output_bound(self.runs[start..end].iter()) + kind.rt_growth();
            let output_segs = bound.div_ceil(seg_bytes).max(1) as u64;
            let needed_segments = output_segs.saturating_add(commit_headroom_segs);
            if needed_segments <= self.alloc.free_segments() {
                break;
            }
            let can_shrink = match kind {
                PlanKind::Recurring => end - start > 2,
                PlanKind::Pressure => {
                    let min_runs = if pressure_floor_min_two { 2 } else { 1 };
                    self.runs.len() - start > min_runs
                }
                PlanKind::LoneGc | PlanKind::Forget(_) => false,
            };
            if !can_shrink {
                self.note_pressure_infeasible(kind);
                return Err(EngineError::InsufficientHeadroom {
                    needed_segments,
                    free_segments: self.alloc.free_segments(),
                });
            }
            start += 1;
        }
        Ok(HeadroomSelection {
            start,
            end,
            forced: selection.forced,
            seg_bytes,
            commit_headroom_segs,
            fold,
        })
    }

    fn select_pressure_rewrite(
        &mut self,
        kind: PlanKind,
        merged_inputs: &[RunMeta],
        usable: usize,
        selection: &HeadroomSelection,
        sum_input_value_bytes: Option<u64>,
    ) -> Result<PressureRewriteSelection, EngineError> {
        if !matches!(kind, PlanKind::Pressure) {
            return Ok(PressureRewriteSelection::Selected(None));
        }
        if selection.fold.is_some() {
            let budget = self
                .alloc
                .free_segments()
                .saturating_sub(selection.commit_headroom_segs)
                .saturating_sub(
                    self.rewrite_output_bound(merged_inputs)
                        .div_ceil(selection.seg_bytes)
                        .max(1) as u64,
                );
            let victims = self
                .select_victims(merged_inputs, usable as u64)
                .and_then(|(victims, _)| Self::victim_lives_ascending(merged_inputs, &victims))
                .and_then(|lives| Self::affordable_victim_prefix(&lives, usable, budget));
            return Ok(PressureRewriteSelection::Selected(Some(match victims {
                Some((victims, bytes)) if !victims.is_empty() => (VlogRewrite::Victims(victims), bytes),
                _ => (VlogRewrite::None, 0),
            })));
        }
        let selected = self
            .select_victims(merged_inputs, usable as u64)
            .map(|(victims, victim_bytes)| {
                if merged_inputs.len() == 1 && !victims.is_empty() {
                    let output_bound = self.rewrite_output_bound(&merged_inputs[..1])
                        .div_ceil(selection.seg_bytes)
                        .max(1) as u64;
                    let budget = self
                        .alloc
                        .free_segments()
                        .saturating_sub(selection.commit_headroom_segs)
                        .saturating_sub(output_bound);
                    Self::victim_lives_ascending(merged_inputs, &victims)
                        .and_then(|lives| {
                            Self::affordable_victim_prefix(&lives, usable, budget)
                        })
                        .unwrap_or_default()
                } else {
                    (victims, victim_bytes)
                }
            });
        let pre_floor_multi = merged_inputs.len() > 1 && !self.at_maintenance_floor();
        match selected {
            Some((victims, victim_bytes)) if !victims.is_empty() => {
                if pre_floor_multi {
                    let output_segs = self.rewrite_output_bound(merged_inputs)
                        .div_ceil(selection.seg_bytes)
                        .max(1) as u64;
                    let net_segs = (victims.len() as u64)
                        .saturating_sub((victim_bytes as usize).div_ceil(usable) as u64);
                    if net_segs < output_segs.div_ceil(2) {
                        return Ok(PressureRewriteSelection::NoPlan);
                    }
                }
                Ok(PressureRewriteSelection::Selected(Some((
                    VlogRewrite::Victims(victims),
                    victim_bytes,
                ))))
            }
            _ if pre_floor_multi => Ok(PressureRewriteSelection::NoPlan),
            _ if merged_inputs.len() == 1 => {
                if self.at_maintenance_floor() {
                    let needed_segments = if self.runs.len() > 1 {
                        let two_run_bytes: usize =
                            OpenRun::rewrite_output_bound(self.runs[self.runs.len() - 2..].iter());
                        two_run_bytes.div_ceil(selection.seg_bytes).max(1) as u64
                            + selection.commit_headroom_segs
                            + 4
                    } else {
                        self.rewrite_output_bound(&merged_inputs[..1])
                            .div_ceil(selection.seg_bytes)
                            .max(1) as u64
                            + selection.commit_headroom_segs
                            + 4
                    };
                    self.note_pressure_infeasible(kind);
                    return Err(EngineError::InsufficientHeadroom {
                        needed_segments,
                        free_segments: self.alloc.free_segments(),
                    });
                }
                Ok(PressureRewriteSelection::NoPlan)
            }
            _ => {
                let rewrite = match sum_input_value_bytes {
                    Some(value_bytes) => (VlogRewrite::All, value_bytes),
                    None => (VlogRewrite::None, 0),
                };
                Ok(PressureRewriteSelection::Selected(Some(rewrite)))
            }
        }
    }

    fn assemble_compact_plan(
        &mut self,
        opts: RunBuildOpts,
        kind: PlanKind,
        selection: HeadroomSelection,
        assembly: CompactPlanAssembly,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        let CompactPlanAssembly {
            merged_inputs,
            gc_collect_before,
            rt_gc_before,
            usable,
            sum_input_value_bytes,
            pressure_rewrite,
        } = assembly;
        let start = selection.start;
        let whole = start == 0 && selection.end == self.runs.len();
        let shadow = selection.fold.map(|_| self.shadow_sources(&selection, gc_collect_before));
        let run_version = run::merge_write_version(merged_inputs.iter().filter_map(|m| {
            self.runs.iter().find(|or| or.meta.handle == m.handle).map(|or| or.run.format_version())
        }));
        let bound = self.rewrite_output_bound(&merged_inputs) + kind.rt_growth();
        let segs = run::allocate_run_segs(&mut self.alloc, self.block_size, bound)
            .map_err(EngineError::from)?;
        let seg_lbas: Vec<u64> = segs
            .iter()
            .map(|s| self.alloc.segment_range(*s).0)
            .collect();

        let new_level = self.runs[start..selection.end]
            .iter()
            .map(|or| or.meta.level)
            .max()
            .unwrap_or(0)
            + 1;
        let cluster_all = whole && co_compact_opt_in();
        let (mut mode, mut vlog_out_bytes) = if let Some(rewrite) = pressure_rewrite {
            rewrite
        } else if cluster_all {
            match sum_input_value_bytes {
                Some(vb) => (VlogRewrite::All, vb),
                None => (VlogRewrite::None, 0),
            }
        } else {
            (VlogRewrite::None, 0)
        };

        let mut unavailable_vlog_segs = 0u64;
        let (vlog_segs, vlog_seg_lbas, vlog_epochs) = if vlog_out_bytes > 0 {
            let available = self
                .alloc
                .free_segments()
                .saturating_sub(selection.commit_headroom_segs);
            let mut vlog_n = 2 * (vlog_out_bytes as usize).div_ceil(usable).max(1) + 2;
            if vlog_n as u64 > available {
                if let VlogRewrite::Victims(victims) = &mode {
                    if let Some((victim, live_bytes)) =
                        Self::smallest_victim(&merged_inputs, victims)
                    {
                        mode = VlogRewrite::Victims(std::iter::once(victim).collect());
                        vlog_out_bytes = live_bytes;
                        vlog_n = 2 * (vlog_out_bytes as usize).div_ceil(usable).max(1) + 2;
                    }
                }
            }
            let has_headroom = vlog_n as u64 <= available;
            match has_headroom.then(|| {
                run::allocate_run_segs(
                    &mut self.alloc,
                    self.block_size,
                    vlog_n * selection.seg_bytes,
                )
            }) {
                Some(Ok(vsegs)) => {
                    let vlbas: Vec<u64> = vsegs
                        .iter()
                        .map(|s| self.alloc.segment_range(*s).0)
                        .collect();
                    let vepochs = self.log.reserve_epochs(vsegs.len());
                    (vsegs, vlbas, vepochs)
                }
                _ => {
                    unavailable_vlog_segs = vlog_n as u64;
                    mode = VlogRewrite::None;
                    (Vec::new(), Vec::new(), Vec::new())
                }
            }
        } else {
            (Vec::new(), Vec::new(), Vec::new())
        };

        if matches!(kind, PlanKind::Pressure)
            && selection.fold.is_none()
            && merged_inputs.len() == 1
            && matches!(mode, VlogRewrite::None)
        {
            for seg in &segs {
                self.alloc
                    .free(*seg)
                    .expect("compact_plan_inner: freeing a reserved output segment");
            }
            self.note_pressure_infeasible(kind);
            return Err(EngineError::InsufficientHeadroom {
                needed_segments: selection
                    .commit_headroom_segs
                    .saturating_add(segs.len() as u64)
                    .saturating_add(unavailable_vlog_segs),
                free_segments: self.alloc.free_segments(),
            });
        }

        let inner = matches!(kind, PlanKind::Recurring) && selection.end < self.runs.len();
        let shadow_fold = selection.fold.is_some();
        if let Some(window) = selection.fold.filter(|_| matches!(kind, PlanKind::Pressure)) {
            self.shadow.pending = Some(ShadowFoldPending { predicted_bytes: window.predicted_bytes });
            self.shadow.folds += 1;
        }
        let gc_collect_before = if shadow.as_ref().is_some_and(|s| !s.complete) { None } else { gc_collect_before };
        let at_floor = self.at_maintenance_floor();
        self.compact_in_flight = true;
        self.pressure_infeasible_streak = 0;

        if let Some(sources) = &shadow {
            crate::attribution::SHADOW_FOLD_SOURCE_BYTES.add(sources.streamed_bytes);
        }
        let mut plan = CompactPlan::new(
            self.dev.clone(),
            &self.alloc,
            self.block_size,
            opts,
            CompactPlanOutput {
                run_version,
                rt: self.rt_merge_plan(&merged_inputs, rt_gc_before),
                merged_inputs,
                segs,
                seg_lbas,
                new_level,
                gc_collect_before,
                vlog_rewrite: mode,
                vlog_segs,
                vlog_seg_lbas,
                vlog_epochs,
                vlog_fence: self.log.epoch_fence_pending(),
                paced: !matches!(kind, PlanKind::Pressure) || (shadow_fold && !at_floor),
                inner,
                kind: match kind {
                    PlanKind::Recurring if selection.forced => MergeKind::Forced,
                    PlanKind::Recurring => MergeKind::Recurring,
                    PlanKind::Pressure if shadow_fold => MergeKind::ShadowFold,
                    PlanKind::Pressure => MergeKind::Pressure,
                    PlanKind::LoneGc => MergeKind::LoneGc,
                    PlanKind::Forget(_) => MergeKind::Forget,
                },
            },
        );
        plan.shadow = shadow;
        Ok(Some(plan))
    }

    fn shadow_sources(
        &self,
        selection: &HeadroomSelection,
        gc: Option<u128>,
    ) -> ShadowSources<D> {
        let (start, end) = (selection.start, selection.end);
        let collectible = gc.is_some_and(|cutoff| {
            self.runs[start..end].iter().any(|or| or.meta.tombstone_min_version.is_some_and(|m| m < cutoff))
        });
        let first = if collectible { 0 } else { start };
        let mut sources = ShadowSources {
            sources: Vec::new(),
            metas: Vec::new(),
            complete: first == 0,
            streamed_bytes: 0,
        };
        for (i, or) in self.runs.iter().enumerate().skip(first) {
            if (start..end).contains(&i) {
                continue;
            }
            let source = or.run.stream_source(&self.alloc);
            sources.streamed_bytes += source.streamed_bytes();
            sources.sources.push((source, i >= end));
            sources.metas.push(or.meta.clone());
        }
        sources
    }

    #[cfg(test)]
    pub(super) fn compact_plan_window_for_test(
        &mut self,
        opts: RunBuildOpts,
        start: usize,
        end: usize,
        gc_collect_before: Option<u128>,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        if !self.compact_plan_eligible(PlanKind::Pressure) {
            return Ok(None);
        }
        let selection = self.fit_compaction_headroom(
            PlanKind::Recurring,
            StartSelection { start, end, forced: false },
            gc_collect_before,
        )?;
        assert_eq!((selection.start, selection.end), (start, end), "test window must fit");
        let merged_inputs: Vec<RunMeta> = self.runs[start..end].iter().map(|or| or.meta.clone()).collect();
        let whole = start == 0 && end == self.runs.len();
        let usable = selection.seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let plan = self.assemble_compact_plan(
            opts,
            PlanKind::Recurring,
            selection,
            CompactPlanAssembly {
                merged_inputs,
                gc_collect_before: gc_collect_before.filter(|_| whole),
                rt_gc_before: gc_collect_before,
                usable,
                sum_input_value_bytes: None,
                pressure_rewrite: None,
            },
        )?;
        Ok(plan.map(|mut plan| {
            plan.key_sink = self.key_filter_merge_sink(&plan.merged_inputs);
            plan
        }))
    }

    fn compact_plan_inner(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
        kind: PlanKind,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        let plan = self.compact_plan_select(opts, gc_collect_before, kind)?;
        Ok(plan.map(|mut plan| {
            plan.pressure = matches!(kind, PlanKind::Pressure);
            plan.vlog_skip = self.gc_excluded_segs.clone();
            plan.key_sink = self.key_filter_merge_sink(&plan.merged_inputs);
            plan
        }))
    }

    fn compact_plan_select(
        &mut self,
        opts: RunBuildOpts,
        gc_collect_before: Option<u128>,
        kind: PlanKind,
    ) -> Result<Option<CompactPlan<D>>, EngineError> {
        if !self.compact_plan_eligible(kind) {
            return Ok(None);
        }
        if let Some(plan) = self.pressure_plan_preamble(opts, kind) {
            return Ok(Some(plan));
        }
        let Some(start) = self.select_compaction_start(kind) else {
            return Ok(None);
        };
        let selection = self.fit_compaction_headroom(kind, start, gc_collect_before)?;
        let merged_inputs: Vec<RunMeta> = self.runs[selection.start..selection.end]
            .iter()
            .map(|or| or.meta.clone())
            .collect();

        let rt_gc_before = gc_collect_before;
        let gc_collect_before = if (selection.start == 0 && selection.end == self.runs.len())
            || selection.fold.is_some()
        {
            gc_collect_before
        } else {
            None
        };
        let usable = selection.seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let sum_input_value_bytes: Option<u64> = merged_inputs
            .iter()
            .map(|m| m.value_bytes)
            .try_fold(0u64, |acc, vb| vb.map(|v| acc + v));
        let pressure_rewrite = match self.select_pressure_rewrite(
            kind,
            &merged_inputs,
            usable,
            &selection,
            sum_input_value_bytes,
        )? {
            PressureRewriteSelection::Selected(rewrite) => rewrite,
            PressureRewriteSelection::NoPlan => return Ok(None),
        };

        self.assemble_compact_plan(
            opts,
            kind,
            selection,
            CompactPlanAssembly {
                merged_inputs,
                gc_collect_before,
                rt_gc_before,
                usable,
                sum_input_value_bytes,
                pressure_rewrite,
            },
        )
    }

    fn aged_vlog_gc_candidate(&self, usable: u64, at_floor: bool) -> Option<usize> {
        let mut best: Option<(usize, u64)> = None;
        for (i, or) in self.runs.iter().enumerate() {
            let m = &or.meta;
            let (Some(segs), Some(live)) = (&m.value_segs, m.value_bytes) else {
                continue;
            };
            let mut dead = (segs.len() as u64)
                .saturating_mul(usable)
                .saturating_sub(live);
            if let (false, Some(bytes)) = (self.gc_excluded_segs.is_empty(), &m.value_seg_bytes) {
                for (seg, seg_live) in segs.iter().zip(bytes) {
                    if self.gc_excluded_segs.contains(seg) {
                        dead = dead.saturating_sub(usable.saturating_sub(*seg_live));
                    }
                }
            }
            let qualifies = if at_floor {
                dead > 0
            } else {
                dead >= m.handle.len
            };
            if qualifies && best.is_none_or(|(_, d)| dead > d) {
                best = Some((i, dead));
            }
        }
        best.map(|(i, _)| i)
    }

    fn aged_vlog_gc_shape(
        &self,
    ) -> Option<(usize, std::collections::HashSet<SegmentId>, u64)> {
        if !vlog_gc_enabled() {
            return None;
        }
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        if seg_bytes == 0 {
            return None;
        }
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let at_floor = self.at_maintenance_floor();
        let idx = self.aged_vlog_gc_candidate(usable as u64, at_floor)?;
        let meta = &self.runs[idx].meta;
        let mut shared: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        for (i, or) in self.runs.iter().enumerate() {
            if i == idx {
                continue;
            }
            let vs = or.meta.value_segs.as_ref()?;
            shared.extend(vs.iter().copied());
        }
        let inputs = std::slice::from_ref(meta);
        let (mut victims, _) = self.select_victims(inputs, usable as u64)?;
        victims.retain(|s| !shared.contains(s));
        if victims.is_empty() {
            return None;
        }
        let lives = Self::victim_lives_ascending(inputs, &victims)?;
        let commit_headroom = self
            .manifest_headroom_segs()
            .saturating_add(self.flush_headroom_segs(seg_bytes));
        let output_segs = OpenRun::rewrite_output_bound(self.runs[idx..=idx].iter()).div_ceil(seg_bytes).max(1) as u64;
        let budget = self
            .alloc
            .free_segments()
            .saturating_sub(commit_headroom)
            .saturating_sub(output_segs);
        let (victims, victim_live) = Self::affordable_victim_prefix(&lives, usable, budget)?;
        let net_gain = (victims.len() as u64)
            .saturating_sub((victim_live as usize).div_ceil(usable) as u64);
        let min_gain = if at_floor { 1 } else { output_segs };
        if net_gain < min_gain {
            return None;
        }
        Some((idx, victims, victim_live))
    }

    fn rewrite_output_bound(&self, inputs: &[RunMeta]) -> usize {
        let open = || inputs.iter().filter_map(|m| self.runs.iter().find(|or| or.meta.handle == m.handle));
        let unopened: usize = inputs
            .iter()
            .filter(|m| !self.runs.iter().any(|or| or.meta.handle == m.handle))
            .map(|m| m.handle.len as usize)
            .sum();
        OpenRun::rewrite_output_bound(open()) + unopened
    }

    pub(super) fn aged_vlog_gc_feasible(&self) -> bool {
        self.aged_vlog_gc_shape().is_some()
    }

    fn plan_aged_vlog_gc(&mut self, opts: RunBuildOpts) -> Option<CompactPlan<D>> {
        let (idx, victims, victim_live) = self.aged_vlog_gc_shape()?;
        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.block_size;
        let usable = seg_bytes.saturating_sub(SEG_HEADER_LEN).max(1);
        let meta = self.runs[idx].meta.clone();
        let run_version = run::merge_write_version([self.runs[idx].run.format_version()]);
        let bound = self.rewrite_output_bound(std::slice::from_ref(&meta));

        let segs = run::allocate_run_segs(&mut self.alloc, self.block_size, bound).ok()?;
        let vlog_segs = if victim_live == 0 {
            Vec::new()
        } else {
            let vlog_n = 2 * (victim_live as usize).div_ceil(usable).max(1) + 2;
            match run::allocate_run_segs(&mut self.alloc, self.block_size, vlog_n * seg_bytes) {
                Ok(v) => v,
                Err(_) => {
                    for seg in &segs {
                        let _ = self.alloc.free(*seg);
                    }
                    return None;
                }
            }
        };
        let seg_lbas: Vec<u64> = segs
            .iter()
            .map(|s| self.alloc.segment_range(*s).0)
            .collect();
        let vlog_seg_lbas: Vec<u64> = vlog_segs
            .iter()
            .map(|s| self.alloc.segment_range(*s).0)
            .collect();
        let vlog_epochs = self.log.reserve_epochs(vlog_segs.len());
        let new_level = meta.level + 1;

        Some(CompactPlan::new(
            self.dev.clone(),
            &self.alloc,
            self.block_size,
            opts,
            CompactPlanOutput {
                run_version,
                rt: self.rt_merge_plan(std::slice::from_ref(&meta), None),
                merged_inputs: vec![meta],
                segs,
                seg_lbas,
                new_level,
                gc_collect_before: None,
                vlog_rewrite: VlogRewrite::Victims(victims),
                vlog_segs,
                vlog_seg_lbas,
                vlog_epochs,
                vlog_fence: self.log.epoch_fence_pending(),
                paced: false,
                kind: MergeKind::VlogGc,
                inner: false,
            },
        ))
    }

    pub(super) fn select_victims(
        &self,
        inputs: &[RunMeta],
        seg_payload: u64,
    ) -> Option<(std::collections::HashSet<SegmentId>, u64)> {
        let (mut victims, live) =
            Self::select_victims_excluding(inputs, seg_payload, self.log.open_segment(), victim_dead_fraction())?;
        if !victims.iter().any(|s| self.gc_excluded_segs.contains(s)) {
            return Some((victims, live));
        }
        victims.retain(|s| !self.gc_excluded_segs.contains(s));
        let live = Self::victim_lives_ascending(inputs, &victims)?.iter().map(|&(_, l)| l).sum();
        Some((victims, live))
    }

    pub fn exclude_from_value_gc(&mut self, seg: SegmentId) -> bool {
        if !self.gc_excluded_segs.insert(seg) {
            return false;
        }
        log::error!(
            "engine: value segment {} holds a corrupt record a merge refused to rewrite; excluding it from \
             value-log GC until restart so other merges and space reclaim continue ({} segment(s) excluded)",
            seg.0,
            self.gc_excluded_segs.len()
        );
        true
    }

    pub fn value_gc_excluded(&self) -> usize {
        self.gc_excluded_segs.len()
    }

    pub(super) fn select_victims_excluding(
        inputs: &[RunMeta],
        seg_payload: u64,
        open: Option<SegmentId>,
        threshold: f64,
    ) -> Option<(std::collections::HashSet<SegmentId>, u64)> {
        let mut live: std::collections::HashMap<SegmentId, u64> = std::collections::HashMap::new();
        for m in inputs {
            let segs = m.value_segs.as_ref()?;
            let bytes = m.value_seg_bytes.as_ref()?;
            if segs.len() != bytes.len() {
                return None;
            }
            for (s, b) in segs.iter().zip(bytes) {
                *live.entry(*s).or_insert(0) += *b;
            }
        }
        let mut victims: std::collections::HashSet<SegmentId> = std::collections::HashSet::new();
        let mut victim_live: u64 = 0;
        for (seg, live_bytes) in &live {
            if open == Some(*seg) {
                continue;
            }
            let dead_frac = 1.0 - (*live_bytes as f64 / seg_payload.max(1) as f64);
            if dead_frac >= threshold {
                victims.insert(*seg);
                victim_live = victim_live.saturating_add(*live_bytes);
            }
        }
        Some((victims, victim_live))
    }

    pub(super) fn smallest_victim(
        inputs: &[RunMeta],
        victims: &std::collections::HashSet<SegmentId>,
    ) -> Option<(SegmentId, u64)> {
        victims
            .iter()
            .copied()
            .filter_map(|victim| {
                let mut live = 0u64;
                for meta in inputs {
                    let segs = meta.value_segs.as_ref()?;
                    let bytes = meta.value_seg_bytes.as_ref()?;
                    for (seg, value_bytes) in segs.iter().zip(bytes) {
                        if *seg == victim {
                            live = live.saturating_add(*value_bytes);
                        }
                    }
                }
                Some((victim, live))
            })
            .min_by_key(|(_, live)| *live)
    }

    pub(super) fn victim_lives_ascending(
        inputs: &[RunMeta],
        victims: &std::collections::HashSet<SegmentId>,
    ) -> Option<Vec<(SegmentId, u64)>> {
        let mut lives: std::collections::HashMap<SegmentId, u64> = victims
            .iter()
            .copied()
            .map(|victim| (victim, 0))
            .collect();
        if lives.is_empty() {
            return Some(Vec::new());
        }
        for meta in inputs {
            let segs = meta.value_segs.as_ref()?;
            let bytes = meta.value_seg_bytes.as_ref()?;
            for (seg, value_bytes) in segs.iter().zip(bytes) {
                if let Some(live) = lives.get_mut(seg) {
                    *live = live.saturating_add(*value_bytes);
                }
            }
        }
        let mut lives: Vec<_> = lives.into_iter().collect();
        lives.sort_by_key(|&(seg, live)| (live, seg));
        Some(lives)
    }

    pub(super) fn gainful_victim_prefix_len(lives: &[(SegmentId, u64)], usable: usize) -> Option<usize> {
        Self::gainful_victim_prefix(lives.iter().map(|&(_, live)| live), usable).map(|(k, _)| k)
    }

    pub(super) fn gainful_victim_prefix(lives: impl IntoIterator<Item = u64>, usable: usize) -> Option<(usize, u64)> {
        let mut sum = 0u64;
        for (i, live) in lives.into_iter().enumerate() {
            sum = sum.saturating_add(live);
            let rewritten_segs = (sum as usize).div_ceil(usable) as u64;
            if (i + 1) as u64 > rewritten_segs {
                return Some((i + 1, sum));
            }
        }
        None
    }

    fn affordable_victim_prefix(
        lives: &[(SegmentId, u64)],
        usable: usize,
        budget_segs: u64,
    ) -> Option<(std::collections::HashSet<SegmentId>, u64)> {
        let mut sum = 0u64;
        let mut best: Option<(usize, u64)> = None;
        for (i, (_, live)) in lives.iter().enumerate() {
            sum = sum.saturating_add(*live);
            let rewritten_segs = (sum as usize).div_ceil(usable) as u64;
            let reservation = if sum == 0 {
                0
            } else {
                2 * rewritten_segs.max(1) + 2
            };
            if (i + 1) as u64 > rewritten_segs && reservation <= budget_segs {
                best = Some((i + 1, sum));
            }
        }
        best.map(|(k, live)| {
            (
                lives[..k].iter().map(|&(seg, _)| seg).collect(),
                live,
            )
        })
    }

    pub fn compact_commit(
        &mut self,
        mut written: WrittenCompact<D>,
    ) -> Result<CompactRunsStats, EngineError> {
        let refresh_winner_directory = (self.winner_directory.is_some() || winner_directory_requested())
            && (written.co_compacted
                || !written.dropped_tombstones.is_empty()
                || !written.forget.is_empty()
                || written.covered_dropped > 0
                || !written.rt_collected.is_empty());
        let reserved = written.output_segments();
        let protected = self.protected_segments();
        let mut input_handles = std::collections::HashSet::new();
        let inputs_current = !written.merged_inputs.is_empty()
            && written.merged_inputs.iter().all(|meta| {
                input_handles.insert((meta.handle.segs.clone(), meta.handle.len))
                    && meta.handle.segs.iter().all(|seg| !self.alloc.is_free(*seg))
                    && meta
                        .value_segs
                        .as_ref()
                        .is_none_or(|segs| segs.iter().all(|seg| !self.alloc.is_free(*seg)))
                    && self
                        .runs
                        .iter()
                        .filter(|or| or.meta.handle == meta.handle)
                        .count()
                        == 1
                    && self.runs.iter().any(|or| or.meta == *meta)
            });
        let mut output_segs = std::collections::HashSet::new();
        let outputs_owned = reserved.iter().all(|seg| {
            output_segs.insert(*seg) && !self.alloc.is_free(*seg) && !protected.contains(seg)
        });
        let sources_current = written
            .shadow_sources
            .iter()
            .all(|meta| self.runs.iter().any(|or| or.meta == *meta));
        let is_input = |or: &OpenRun<D>| written.merged_inputs.iter().any(|m| m.handle == or.meta.handle);
        let collection_safe = written.rt_collected.iter().all(|r| self.rt_outside_runs_filtered(r, &is_input));
        if !inputs_current || !outputs_owned || !sources_current || !collection_safe {
            self.free_compact_segs(&reserved);
            return Err(EngineError::CompactionConflict);
        }
        let covered = written.key_sink.as_ref().is_some_and(|s| self.key_filter.owns(s))
            || self.key_filter_inputs_covered(&written.merged_inputs);
        let rebuild_covered = written.key_sink.as_ref().is_some_and(|s| self.key_filter.owns_rebuild(s))
            || self.key_filter_rebuild_inputs_covered(&written.merged_inputs);

        let survivor_metas: Vec<RunMeta> = self
            .runs
            .iter()
            .filter(|or| !is_input(or))
            .map(|or| or.meta.clone())
            .collect();
        let runs_merged = self.runs.len() - survivor_metas.len();

        let meta = RunMeta {
            handle: written.handle.clone(),
            min_key: written.min_key,
            max_key: written.max_key,
            entry_count: written.entry_count,
            level: written.new_level,
            value_segs: Some(written.value_segs.clone()),
            value_bytes: Some(written.value_bytes),
            tombstone_min_version: written.tombstone_min_version,
            value_seg_bytes: Some(written.value_seg_bytes.clone()),
        };
        let entry_count = written.entry_count;

        let mut current_dropped: Vec<(Vec<u8>, u128)> = Vec::new();
        for (k, v) in &written.dropped_tombstones {
            let mut has_candidate = false;
            let mut unsafe_drop = false;
            if let Some(e) = self.index.get(k) {
                has_candidate = true;
                unsafe_drop |= e.version <= *v;
            }
            for or in self.runs.iter().filter(|or| {
                !is_input(or) && !written.shadow_sources.iter().any(|m| m.handle == or.meta.handle)
            }) {
                if k.as_slice() < or.meta.min_key.as_slice()
                    || k.as_slice() > or.meta.max_key.as_slice()
                {
                    continue;
                }
                match or.run.lookup(&self.alloc, k) {
                    Ok(Some(e)) => {
                        has_candidate = true;
                        unsafe_drop |= e.version <= *v;
                    }
                    Ok(None) => {}
                    Err(e) => {
                        self.free_compact_segs(&reserved);
                        return Err(EngineError::from(e));
                    }
                }
            }
            if unsafe_drop {
                self.free_compact_segs(&reserved);
                return Err(EngineError::CompactionConflict);
            }
            if !has_candidate {
                current_dropped.push((k.clone(), *v));
            }
        }

        let seg_bytes = self.alloc.blocks_per_segment() as usize * self.dev.block_size();
        let run = DeviceRun::from_parts(
            written.dev,
            seg_bytes,
            written.handle.clone(),
            written.parts,
        );

        let mut run_metas: Vec<RunMeta> = Vec::with_capacity(1 + survivor_metas.len());
        let mut inserted = false;
        for or in &self.runs {
            if is_input(or) {
                if !inserted {
                    run_metas.push(meta.clone());
                    inserted = true;
                }
            } else {
                run_metas.push(or.meta.clone());
            }
        }
        if !inserted {
            run_metas.push(meta.clone());
        }
        let generation = self.manifest_gen + 1;
        let manifest = Manifest {
            generation,
            covered_seq: self.last_flush_seq,
            covered_epoch: self.last_flush_epoch,
            runs: run_metas,
        };
        let live = match snapshot_io::write_manifest(
            &*self.dev,
            &mut self.alloc,
            &manifest,
            generation,
            self.live_manifest.as_ref(),
        ) {
            Ok(l) => l,
            Err(e) => {
                drop(run);
                self.free_compact_segs(&reserved);
                return Err(EngineError::from(e));
            }
        };

        let mut forget_failures = 0u64;
        let mut forget_error = None;
        for (key, version) in &written.forget {
            if self.index.get(key).is_some_and(|e| e.version == *version) {
                match self.append_forget_delete(key, *version) {
                    Ok(()) => {
                        self.index.remove(key);
                        if !current_dropped.iter().any(|(k, _)| k == key) {
                            current_dropped.push((key.clone(), *version));
                        }
                    }
                    Err(e) => {
                        forget_failures += 1;
                        forget_error.get_or_insert(e);
                    }
                }
            }
        }
        if let Some(e) = forget_error {
            note_commit_forget_failures(forget_failures, &e);
        }
        self.append_tail_forget_deletes(&written.tail_forgets);
        let mut new_runs: Vec<OpenRun<D>> = Vec::with_capacity(1 + survivor_metas.len());
        let mut removed: Vec<(crate::run::RunHandle, u64)> = Vec::new();
        let mut merged_slot: Option<(RunMeta, DeviceRun<D>)> = Some((meta, run));
        let mut output_rts = Some(std::mem::take(&mut written.rts));
        let rt_active = !self.rts.fragments.is_empty();
        let rt_count = self.run_rt_count();
        let mut inserted = false;
        for or in std::mem::take(&mut self.runs) {
            if is_input(&or) {
                removed.push((or.meta.handle.clone(), pins::retained_index_bytes(&or.run)));
                if !inserted {
                    let (m, r) = merged_slot.take().unwrap();
                    let mut opened = OpenRun::new(m, r, covered).with_rts(output_rts.take().unwrap_or_default()).with_max_tree(rt_active);
                    opened.rebuild_covered = rebuild_covered;
                    new_runs.push(opened);
                    inserted = true;
                }
            } else {
                new_runs.push(or);
            }
        }
        if let Some((m, r)) = merged_slot.take() {
            let mut opened = OpenRun::new(m, r, covered).with_rts(output_rts.take().unwrap_or_default()).with_max_tree(rt_active);
            opened.rebuild_covered = rebuild_covered;
            new_runs.push(opened);
        }
        self.runs = new_runs;
        self.note_run_rts_replaced(written.released.is_some(), rt_count);
        if written.inner {
            INNER_WINDOW_MERGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        self.runs_removed_epoch += 1;
        self.manifest_gen = generation;
        self.live_manifest = Some(live);
        self.bump_storage_generation();
        self.key_filter_maintain();
        range_delete::note_covered_dropped(written.covered_dropped);
        range_delete::note_dominated_dropped(written.rt_dominated);
        let released = written.released.take().map_or(0, |released| {
            self.clip_released_range_tombstones(&released);
            released.len()
        });
        self.finish_rt_collection(&written.rt_collected);

        let mut segments_freed = self.retire_removed_runs(removed);
        for seg in &written.unused_segs {
            let _ = self.alloc.free(*seg);
        }

        if written.co_compacted {
            for seg in &written.vlog_unused_segs {
                let _ = self.alloc.free(*seg);
            }
            let inputs_known = written.merged_inputs.iter().all(|m| m.value_segs.is_some());
            let survivors_known = survivor_metas.iter().all(|m| m.value_segs.is_some());
            if inputs_known && survivors_known {
                let mut keep: std::collections::HashSet<SegmentId> =
                    std::collections::HashSet::new();
                for m in &survivor_metas {
                    if let Some(vs) = &m.value_segs {
                        keep.extend(vs.iter().copied());
                    }
                    keep.extend(m.handle.segs.iter().copied());
                }
                keep.extend(written.value_segs.iter().copied());
                keep.extend(written.handle.segs.iter().copied());
                keep.extend(self.flush_value_segs.iter().copied());
                if let Some(open) = self.log.open_segment() {
                    keep.insert(open);
                }
                self.for_each_mutable_value_segment(&mut |seg| {
                        keep.insert(seg);
                    });
                let mut dead: std::collections::HashSet<SegmentId> =
                    std::collections::HashSet::new();
                for m in &written.merged_inputs {
                    if let Some(vs) = &m.value_segs {
                        for s in vs {
                            if !keep.contains(s) {
                                dead.insert(*s);
                            }
                        }
                    }
                }
                for seg in dead {
                    if self.alloc.free(seg).is_ok() {
                        segments_freed += 1;
                    }
                }
            }
        }
        self.compact_in_flight = false;
        segments_freed += self.reclaim_orphan_segments();
        self.pressure_infeasible_streak = 0;

        let input_bytes: u64 = written
            .merged_inputs
            .iter()
            .map(|m| (m.handle.segs.len() as u64) * seg_bytes as u64)
            .sum::<u64>()
            + written.value_bytes_rewritten;
        let output_bytes: u64 = (written.handle.segs.len() as u64) * seg_bytes as u64
            + if written.co_compacted {
                let usable = (seg_bytes as u64)
                    .saturating_sub(SEG_HEADER_LEN as u64)
                    .max(1);
                written.value_bytes_rewritten.div_ceil(usable) * seg_bytes as u64
            } else {
                0
            };

        self.refresh_data_reserve();

        if refresh_winner_directory {
            self.refresh_winner_directory_after_commit();
        }

        if let Some(p) = self.shadow.pending.take() {
            let input_bytes: u64 = written
                .merged_inputs
                .iter()
                .map(|m| m.value_bytes.unwrap_or(0) + m.handle.len)
                .sum();
            let realized = input_bytes.saturating_sub(written.value_bytes + written.handle.len);
            if realized.saturating_mul(2) < p.predicted_bytes {
                self.shadow.disarms += 1;
                self.shadow.backoff_shift =
                    (self.shadow.backoff_shift + 1).min(SHADOW_FOLD_MAX_BACKOFF_SHIFT);
            } else {
                self.shadow.backoff_shift = 0;
            }
            let spacing =
                (written.handle.segs.len() as u64).max(1) << self.shadow.backoff_shift;
            self.shadow.cooldown_until_epoch =
                self.log.next_epoch().saturating_add(spacing);
        }

        Ok(CompactRunsStats {
            runs_merged,
            entries_written: entry_count as usize,
            segments_freed,
            input_bytes,
            output_bytes,
            current_dropped_tombstones: current_dropped,
            covered_dropped: written.covered_dropped,
            collected_range_tombstones: std::mem::take(&mut written.rt_collected),
            released_range_tombstones: released,
            dominated_range_tombstones: written.rt_dominated,
        })
    }

}

