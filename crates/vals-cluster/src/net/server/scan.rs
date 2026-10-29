use super::*;

pub(super) mod reverse;

pub(super) fn sync_tree_geometry_error(level: u32, scope_g: u32, scope: &[u32]) -> Option<String> {
    let leaves = crate::antientropy::DEFAULT_BUCKETS;
    if level == 0 || !leaves.is_multiple_of(level) {
        return Some(format!("bad tree geometry: level {level} must divide {leaves}"));
    }
    if scope_g == 0 || !level.is_multiple_of(scope_g) {
        return Some(format!("bad tree geometry: scope group {scope_g} must divide level {level}"));
    }
    scope
        .iter()
        .find(|&&id| id >= scope_g)
        .map(|id| format!("bad tree geometry: scope id {id} is not below scope group {scope_g}"))
}

pub(super) fn bucket_digest_geometry_error(num_buckets: u32) -> Option<String> {
    (num_buckets == 0).then(|| "bad bucket geometry: num_buckets must be non-zero".to_string())
}

pub(super) async fn serve_sync_tree<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    level: u32,
    scope_g: u32,
    scope: &std::collections::HashSet<u32>,
    requester: Option<NodeId>,
) -> Response {
    match crate::replica::ensure_tree_ready_cooperative_cell(store, scope_g, scope).await {
        Ok(true) => {}
        Ok(false) => return Response::Busy,
        Err(e) => return Response::Err(format!("{e}")),
    }
    let token = store.borrow().tree_scope_token(scope_g, scope);
    match store
        .borrow()
        .scoped_leaf_digests(level, scope_g, scope, requester)
    {
        Ok(nodes) if store.borrow().tree_scope_token_valid(&token) => Response::Tree(nodes),
        Ok(_) => Response::Busy,
        Err(crate::replica::LeafDigestError::Dirty) => Response::Busy,
        Err(e) => Response::Err(format!("{e}")),
    }
}

struct DigestServeCheckpoint<'a, 'g, D: BlockDevice> {
    guard: &'g mut InflightGuard<'a>,
    store: &'g RefCell<VersionedStore<D>>,
    generation: (u64, u64),
    deadline: Option<std::time::Instant>,
    polled_at: &'g std::cell::Cell<std::time::Instant>,
}

impl<D: BlockDevice + AsyncRead> vals_store::DigestCheckpoint for DigestServeCheckpoint<'_, '_, D> {
    async fn checkpoint(&mut self) -> bool {
        self.guard.checkpoint().await;
        self.store.borrow().aggregate_generation() == self.generation
    }

    fn expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| {
            let now = std::time::Instant::now();
            now >= deadline || now.duration_since(self.polled_at.get()) >= digest_serve_hold_slice()
        })
    }
}

pub(super) const AE_DIGEST_SERVE_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

const AE_DIGEST_SERVE_HOLD_SLICE: std::time::Duration = std::time::Duration::from_millis(5);

#[cfg(test)]
thread_local! {
    pub(super) static DIGEST_SERVE_TEST_HOLD_SLICE: std::cell::Cell<Option<std::time::Duration>> =
        const { std::cell::Cell::new(None) };
}

fn digest_serve_hold_slice() -> std::time::Duration {
    #[cfg(test)]
    if let Some(slice) = DIGEST_SERVE_TEST_HOLD_SLICE.with(std::cell::Cell::get) {
        return slice;
    }
    AE_DIGEST_SERVE_HOLD_SLICE
}

static AE_DIGEST_SERVE_BYTES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

struct AeDigestServeGuard {
    bytes: u64,
}

impl AeDigestServeGuard {
    fn acquire(bytes: u64) -> Option<Self> {
        let queued_cap = crate::transport::limits().max_queued_bytes;
        let cap = if queued_cap == usize::MAX { u64::MAX } else { (queued_cap / 4) as u64 };
        AE_DIGEST_SERVE_BYTES.fetch_update(
            std::sync::atomic::Ordering::Relaxed,
            std::sync::atomic::Ordering::Relaxed,
            |used| used.checked_add(bytes).filter(|&next| used == 0 || next <= cap),
        ).ok()?;
        crate::ram_broker::charge(0, bytes);
        crate::observe::gauges().ae_digest_serve_bytes.adjust(0, bytes);
        Some(Self { bytes })
    }
}

impl Drop for AeDigestServeGuard {
    fn drop(&mut self) {
        crate::ram_broker::charge(self.bytes, 0);
        crate::observe::gauges()
            .ae_digest_serve_bytes
            .adjust(self.bytes, 0);
        AE_DIGEST_SERVE_BYTES.fetch_sub(self.bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

pub(super) async fn serve_bucket_digests<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    num_buckets: u32,
    ids: &[u32],
    after: Option<&[u8]>,
    resumable: bool,
) -> Response {
    let started = std::time::Instant::now();
    let deadline = resumable.then(|| started + AE_DIGEST_SERVE_TIME_BUDGET);
    let response =
        serve_bucket_digests_inner(store, dev, gate, num_buckets, ids, after, deadline).await;
    crate::observe::histograms()
        .ae_digest_serve_duration_ms
        .observe_us(started.elapsed().as_millis() as u64);
    response
}

pub(super) async fn serve_bucket_digests_inner<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    num_buckets: u32,
    ids: &[u32],
    after: Option<&[u8]>,
    deadline: Option<std::time::Instant>,
) -> Response {
    let mut guard = gate.acquire().await;
    for _ in 0..3 {
        if store.borrow().rt_fold_not_ready_for_caller() {
            crate::observe::counters().rt_fold_not_ready_busy.inc();
            return Response::Busy;
        }
        let generation = store.borrow().aggregate_generation();
        let accounted = store.borrow().accounted_epoch();
        let planned = if deadline.is_some() {
            store.borrow().plan_resumable_bucket_digest_page(num_buckets, ids, after, SCAN_REPLY_BUDGET)
        } else {
            store.borrow().plan_bucket_digest_page(num_buckets, ids, after, SCAN_REPLY_BUDGET)
        };
        let plan = match planned {
            Ok(p) => p,
            Err(e) => return Response::Err(format!("{e}")),
        };
        let held = plan.resident_bound_bytes(SCAN_REPLY_BUDGET);
        let Some(serve_guard) = AeDigestServeGuard::acquire(held) else {
            crate::observe::counters().ae_digest_serve_rejections.inc();
            return Response::Busy;
        };
        let polled_at = std::cell::Cell::new(std::time::Instant::now());
        let mut checkpoint = DigestServeCheckpoint {
            guard: &mut guard,
            store,
            generation,
            deadline,
            polled_at: &polled_at,
        };
        let page = VersionedStore::bucket_digest_page_from_plan_cooperative(
            plan,
            num_buckets,
            ids,
            after,
            SCAN_REPLY_BUDGET,
            dev,
            &mut checkpoint,
        );
        let mut page = std::pin::pin!(page);
        let result = std::future::poll_fn(|cx| {
            polled_at.set(std::time::Instant::now());
            page.as_mut().poll(cx)
        })
        .await;
        drop(serve_guard);
        match result {
            Ok(Some(((digests, more), resume)))
                if store.borrow().aggregate_generation() == generation
                    && store.borrow().accounted_epoch() == accounted
                    && !store.borrow().rt_fold_not_ready_for_caller() =>
            {
                if resume.is_some() {
                    crate::observe::counters().ae_digest_serve_time_truncations.inc();
                }
                return Response::BucketDigests {
                    digests,
                    more,
                    resume,
                };
            }
            Ok(Some(_)) | Ok(None) => continue,
            Err(_) if store.borrow().aggregate_generation() != generation => continue,
            Err(e) => return Response::Err(format!("{e}")),
        }
    }
    Response::Busy
}

const PAGE_READ_CONCURRENCY: usize = 256;

const SPAN_READ_CONCURRENCY: usize = 64;

async fn execute_generation_checked<D, F, T>(
    store: &RefCell<VersionedStore<D>>,
    generation: (u64, u64),
    futures: Vec<F>,
) -> Option<Vec<T>>
where
    D: BlockDevice,
    F: Future<Output = T>,
{
    let results = vals_rt::join_all(futures).await;
    (store.borrow().aggregate_generation() == generation).then_some(results)
}

async fn execute_storage_checked<D, F, T>(
    store: &RefCell<VersionedStore<D>>,
    generation: u64,
    futures: Vec<F>,
) -> Option<Vec<T>>
where
    D: BlockDevice,
    F: Future<Output = T>,
{
    let results = vals_rt::join_all(futures).await;
    (store.borrow().storage_generation() == generation).then_some(results)
}

#[allow(clippy::too_many_arguments)]
async fn execute_span_page<D: BlockDevice + AsyncRead>(
    guard: &mut InflightGuard<'_>,
    store: &RefCell<VersionedStore<D>>,
    generation: u64,
    keys: Vec<(Vec<u8>, crate::hlc::Version)>,
    plan: vals_store::SpanReadPlan,
    dev: &D,
    site: crate::integrity::ReadSite,
) -> Result<Option<Vec<(Vec<u8>, crate::replica::Stored)>>, vals_store::vlog::LogError> {
    debug_assert_eq!(plan.num_records(), keys.len());
    vals_store::attribution::SCAN_VALUES.add(plan.num_records() as u64);
    let mut blobs: Vec<Option<Vec<u8>>> = Vec::with_capacity(keys.len());
    blobs.resize_with(keys.len(), || None);
    let expected = |slot: usize| {
        let (key, version) = &keys[slot];
        (key.as_slice(), crate::replica::pack_version(*version))
    };
    let mut next = 0usize;
    while next < plan.num_spans() {
        let window = next..(next + SPAN_READ_CONCURRENCY).min(plan.num_spans());
        next = window.end;
        vals_store::attribution::SCAN_SPANS.add(window.len() as u64);
        let futures: Vec<_> = window
            .map(|i| plan.execute_span_checked(i, dev, &expected))
            .collect();
        let Some(results) = execute_storage_checked(store, generation, futures).await else {
            return Ok(None);
        };
        for r in results {
            let recs = r.map_err(|e| {
                if let Some((slot, ptr)) = e.failed {
                    crate::integrity::corrupt_read(site, "scan", &keys[slot].0, ptr, &e.error);
                }
                e.error
            })?;
            for (slot, rec) in recs {
                blobs[slot] = Some(rec.value);
            }
        }
        guard.checkpoint().await;
        if store.borrow().storage_generation() != generation {
            return Ok(None);
        }
    }
    let mut out = Vec::with_capacity(keys.len());
    for ((key, version), blob) in keys.into_iter().zip(blobs) {
        let blob = blob.expect("every slot decoded by exactly one span");
        let value = crate::replica::adopt_blob(blob);
        out.push((key, crate::replica::Stored { version, value }));
    }
    Ok(Some(out))
}

async fn finalize_scan_entries<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    generation: u64,
    mutable_generation: u64,
    dev: &D,
    mut entries: Vec<(Vec<u8>, crate::replica::Stored)>,
    site: crate::integrity::ReadSite,
) -> Result<Option<Vec<(Vec<u8>, crate::replica::Stored)>>, String> {
    if entries.is_empty() {
        return Ok(Some(entries));
    }
    for _ in 0..8 {
        let stale = {
            let s = store.borrow();
            if s.storage_generation() != generation {
                return Ok(None);
            }
            if s.mutable_generation() == mutable_generation {
                return Ok(Some(entries));
            }
            let keys: Vec<&[u8]> = entries.iter().map(|(key, _)| key.as_slice()).collect();
            match s.engine().winner_read_tokens(&keys) {
                Some(tokens) => tokens
                    .into_iter()
                    .enumerate()
                    .filter_map(|(i, token)| {
                        token
                            .is_some_and(|token| {
                                crate::replica::unpack_version(token.entry.version)
                                    > entries[i].1.version
                            })
                            .then_some(i)
                    })
                    .collect::<Vec<_>>(),
                None => {
                    let mut after = entries.last().unwrap().0.clone();
                    after.push(0);
                    match s.engine().scan_mutable_entries(
                        Some(entries[0].0.as_slice()),
                        Some(after.as_slice()),
                    ) {
                        Some(mutable) => {
                            let mut pos = 0usize;
                            let mut stale = Vec::new();
                            for (i, (key, stored)) in entries.iter().enumerate() {
                                while mutable
                                    .get(pos)
                                    .is_some_and(|(mutable_key, _)| mutable_key < key)
                                {
                                    pos += 1;
                                }
                                if mutable.get(pos).is_some_and(|(mutable_key, entry)| {
                                    mutable_key == key
                                        && crate::replica::unpack_version(entry.version)
                                            > stored.version
                                }) {
                                    stale.push(i);
                                }
                            }
                            stale
                        }
                        None => entries
                            .iter()
                            .enumerate()
                            .filter_map(|(i, (key, stored))| {
                                s.mutable_entry(key)
                                    .is_some_and(|entry| {
                                        crate::replica::unpack_version(entry.version)
                                            > stored.version
                                    })
                                    .then_some(i)
                            })
                            .collect(),
                    }
                }
            }
        };
        if stale.is_empty() {
            return Ok(Some(entries));
        }
        let futures: Vec<_> = stale
            .iter()
            .map(|&i| {
                let key = entries[i].0.as_slice();
                async move {
                    let stage = stage_mixed_value(store, dev, key).await;
                    (i, execute_mixed_value(store, dev, key, stage, site).await)
                }
            })
            .collect();
        let mut retry = false;
        for (i, result) in vals_rt::join_all(futures).await {
            match result {
                MixedReadResult::Reply(MixedReadReply::Value(Some(stored))) => {
                    crate::observe::counters().scan_finalize_patches.inc();
                    entries[i].1 = stored;
                }
                MixedReadResult::Busy | MixedReadResult::Reply(MixedReadReply::Value(None)) => {
                    retry = true;
                }
                MixedReadResult::Err(msg) => return Err(msg),
                MixedReadResult::Reply(MixedReadReply::Digest(_)) => {
                    return Err("invalid scan finalize result".into());
                }
            }
        }
        if retry {
            continue;
        }
    }
    Ok(None)
}

async fn execute_entry_plans<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    generation: (u64, u64),
    plans: Vec<(Vec<u8>, crate::replica::ValuePlan)>,
    dev: &D,
    site: crate::integrity::ReadSite,
) -> Result<Option<Vec<(Vec<u8>, crate::replica::Stored)>>, vals_store::vlog::LogError> {
    use crate::replica::ValuePlan;
    let mut out = Vec::with_capacity(plans.len());
    let mut rest = plans.into_iter();
    loop {
        let window: Vec<(Vec<u8>, ValuePlan)> =
            rest.by_ref().take(PAGE_READ_CONCURRENCY).collect();
        if window.is_empty() {
            break;
        }
        let futures: Vec<_> = window
            .iter()
            .filter_map(|(key, plan)| match plan {
                ValuePlan::Read(plan) => Some(crate::replica::execute_read_plan_checked(plan, dev, key)),
                ValuePlan::Covered(_) => None,
            })
            .collect();
        let Some(results) = execute_generation_checked(store, generation, futures).await else {
            return Ok(None);
        };
        let mut results = results.into_iter();
        for (key, plan) in window {
            let stored = match plan {
                ValuePlan::Covered(version) => crate::replica::Stored { version, value: None },
                ValuePlan::Read(plan) => results.next().expect("one result per read plan").inspect_err(|e| {
                    crate::integrity::corrupt_read(site, "sync-values", &key, plan.ptr(), e);
                })?,
            };
            out.push((key, stored));
        }
    }
    Ok(Some(out))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_scan<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    max: u32,
    range_tombstones: bool,
    site: crate::integrity::ReadSite,
) -> Response {
    serve_scan_counted(store, dev, gate, lo, hi, max, range_tombstones, site).await.0
}

pub(crate) async fn serve_scan_keys<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    site: crate::integrity::ReadSite,
) -> Response {
    let (unbounded, range_tombstones, keys_only) = (0, true, true);
    serve_scan_mode(store, dev, gate, lo, hi, unbounded, range_tombstones, keys_only, site).await.0
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_scan_counted<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    max: u32,
    range_tombstones: bool,
    site: crate::integrity::ReadSite,
) -> (Response, vals_store::RtScanStats) {
    serve_scan_mode(store, dev, gate, lo, hi, max, range_tombstones, false, site).await
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn serve_scan_mode<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    max: u32,
    range_tombstones: bool,
    keys_only: bool,
    site: crate::integrity::ReadSite,
) -> (Response, vals_store::RtScanStats) {
    let mut guard = gate.acquire().await;
    let cap = if max == 0 { usize::MAX } else { max as usize };
    let started = std::time::Instant::now();
    let mut page = None;
    for _ in 0..3 {
        let (staged, mutable_generation, cover) =
            match serve_scan_staged(store, dev, &mut guard, lo, hi, cap, keys_only).await {
                Ok(p) => p,
                Err(e) => return (Response::Err(format!("{e}")), Default::default()),
            };
        let generation = store.borrow().storage_generation();
        let (keys, plan, more) = match staged {
            ScanData::Keys(entries, more) => {
                page = Some((entries, more, cover));
                break;
            }
            ScanData::Values(page) => page,
        };
        match execute_span_page(&mut guard, store, generation, keys, plan, dev, site).await {
            Ok(Some(entries)) if store.borrow().storage_generation() == generation => {
                match finalize_scan_entries(
                    store,
                    generation,
                    mutable_generation,
                    dev,
                    entries,
                    site,
                )
                .await
                {
                    Ok(Some(entries)) => {
                        page = Some((entries, more, cover));
                        break;
                    }
                    Ok(None) => {
                        crate::observe::counters().scan_span_retries.inc();
                        continue;
                    }
                    Err(e) => return (Response::Err(e), Default::default()),
                }
            }
            Ok(Some(_)) | Ok(None) => {
                crate::observe::counters().scan_span_retries.inc();
                continue;
            }
            Err(_) if store.borrow().storage_generation() != generation => {
                crate::observe::counters().scan_span_retries.inc();
                continue;
            }
            Err(e) => return (Response::Err(format!("{e}")), Default::default()),
        }
    }
    let Some((mut entries, mut more, (mut resume, stats))) = page else {
        return (Response::Busy, Default::default());
    };
    publish_scan_rt_stats(stats);
    let mut fragments = {
        let s = store.borrow();
        let set = s.engine().range_tombstones();
        if set.is_empty() {
            Vec::new()
        } else {
            let end = match (&resume, more) {
                (Some(r), _) => Some(r.clone()),
                (None, true) => Some(entries.last().map_or_else(|| lo.unwrap_or_default().to_vec(), |(k, _)| {
                    let mut k = k.clone();
                    k.push(0);
                    k
                })),
                (None, false) => hi.map(<[u8]>::to_vec),
            };
            set.clipped(lo.unwrap_or_default(), end.as_deref())
        }
    };
    if !range_tombstones && (!fragments.is_empty() || resume.is_some()) {
        return (Response::Err(RANGE_TOMBSTONES_REQUIRED.into()), stats);
    }
    if !fragments.is_empty() || keys_only {
        fit_scan_reply(lo.unwrap_or_default(), &mut entries, &mut fragments, &mut resume, &mut more, scan_reply_budget(site.zone));
    }
    if resume.is_some() {
        crate::observe::counters().scan_rt_resume_pages_total.inc();
    }
    crate::observe::histograms()
        .scan_latency_us
        .observe(started.elapsed());
    crate::observe::counters()
        .scan_entries_returned
        .add(entries.len() as u64);
    let fragments = fragments
        .into_iter()
        .map(|f| crate::wire::RtFragment {
            lo: (!f.lo.is_empty()).then_some(f.lo),
            hi: f.hi,
            version: crate::replica::unpack_version(f.version),
        })
        .collect();
    (Response::Scan { entries, more, fragments, resume }, stats)
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn serve_multi_scan<D: BlockDevice + AsyncRead, const REVERSE: bool>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    start: u32,
    scans: &[crate::wire::OwnedScan],
    keys_only: bool,
    range_tombstones: bool,
    site: crate::integrity::ReadSite,
) -> Response {
    if start as usize > scans.len() {
        return Response::Err("MultiScan cursor out of range".into());
    }
    let mut pages = Vec::new();
    let mut bytes = 11usize;
    let budget = scan_reply_budget(site.zone);
    'page: for window in scans[start as usize..].chunks(4) {
        let futures = window.iter().map(|(lo, hi, max)| async move {
            if REVERSE {
                reverse::serve_scan_rev(store, dev, gate, lo.as_deref(), hi.as_deref(), *max, range_tombstones, keys_only, site).await
            } else {
                serve_scan_mode(store, dev, gate, lo.as_deref(), hi.as_deref(), *max, range_tombstones, keys_only, site).await
            }
        }).collect();
        for (response, _) in vals_rt::join_all(futures).await {
            let Response::Scan { entries, more, fragments, resume } = response else { return response; };
            let size = 10 + resume.as_ref().map_or(0, |r| 4 + r.len())
                + entries.iter().map(|(k, s)| 4 + k.len() + 17
                    + if keys_only { 0 } else { s.value.as_ref().map_or(0, |v| 4 + v.len()) }).sum::<usize>()
                + fragments.iter().map(|f| 18 + f.lo.as_ref().map_or(0, |v| 4 + v.len())
                    + f.hi.as_ref().map_or(0, |v| 4 + v.len())).sum::<usize>();
            if !pages.is_empty() && bytes.saturating_add(size) > budget { break 'page; }
            bytes = bytes.saturating_add(size);
            pages.push(crate::coord::ScanPage { entries, more, fragments, resume });
        }
    }
    Response::MultiScan { start, more: start as usize + pages.len() < scans.len(), keys_only, pages }
}

pub(super) const RANGE_TOMBSTONES_REQUIRED: &str = "range tombstones present: client lacks RANGE_TOMBSTONES";

const SCAN_FRAGMENT_WIRE_OVERHEAD: usize = 2 * (1 + 4) + 16;

const SCAN_ENTRY_WIRE_OVERHEAD: usize = 4 + 16 + 1 + 4;

fn publish_scan_rt_stats(stats: vals_store::RtScanStats) {
    if stats == vals_store::RtScanStats::default() {
        return;
    }
    let counters = crate::observe::counters();
    counters.scan_rt_filtered_entries_total.add(stats.filtered_entries);
    counters.scan_rt_skipped_blocks_total.add(stats.skipped_blocks);
    counters.scan_rt_block_reads_total.add(stats.block_reads);
    counters.scan_rt_index_probes_total.add(stats.index_probes);
}

pub(super) fn fit_scan_reply(
    lo: &[u8],
    entries: &mut Vec<(Vec<u8>, crate::replica::Stored)>,
    fragments: &mut Vec<vals_store::range_tombstone::Fragment>,
    resume: &mut Option<Vec<u8>>,
    more: &mut bool,
    budget: usize,
) {
    let (mut e, mut f, mut used) = (0usize, 0usize, 0usize);
    let cut = loop {
        let next_entry = entries.get(e).map(|(k, _)| k.as_slice());
        let next_fragment = fragments.get(f).map(|x| x.lo.as_slice());
        let (start, size, is_entry) = match (next_entry, next_fragment) {
            (None, None) => return,
            (Some(k), Some(fl)) if fl <= k => (fl, fragments[f].lo.len() + fragments[f].hi.as_ref().map_or(0, Vec::len) + SCAN_FRAGMENT_WIRE_OVERHEAD, false),
            (None, Some(fl)) => (fl, fragments[f].lo.len() + fragments[f].hi.as_ref().map_or(0, Vec::len) + SCAN_FRAGMENT_WIRE_OVERHEAD, false),
            (Some(k), _) => (k, k.len() + entries[e].1.value.as_ref().map_or(0, Vec::len) + SCAN_ENTRY_WIRE_OVERHEAD, true),
        };
        if used.saturating_add(size) > budget && start > lo {
            break start.to_vec();
        }
        used = used.saturating_add(size);
        if is_entry {
            e += 1;
        } else {
            f += 1;
        }
    };
    entries.truncate(e);
    fragments.truncate(f);
    for x in fragments.iter_mut() {
        if x.hi.as_deref().is_none_or(|h| h > cut.as_slice()) {
            x.hi = Some(cut.clone());
        }
    }
    *resume = Some(cut);
    *more = true;
}

#[cfg(test)]
thread_local! {
    static STAGED_SCAN_REBORROW_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_staged_scan_reborrow_hook(hook: Option<Box<dyn FnMut()>>) {
    STAGED_SCAN_REBORROW_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
fn staged_scan_reborrow_hook() {
    STAGED_SCAN_REBORROW_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f();
        }
    });
}

#[cfg(test)]
thread_local! {
    static ASYNC_WRITE_MIRROR_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_async_write_mirror_hook(hook: Option<Box<dyn FnMut()>>) {
    ASYNC_WRITE_MIRROR_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
pub(super) fn async_write_mirror_hook() {
    ASYNC_WRITE_MIRROR_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f();
        }
    });
}

#[cfg(test)]
thread_local! {
    static STAGED_APPLY_COMMIT_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_staged_apply_commit_hook(hook: Option<Box<dyn FnMut()>>) {
    STAGED_APPLY_COMMIT_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
pub(super) fn staged_apply_commit_hook() {
    STAGED_APPLY_COMMIT_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f();
        }
    });
}

#[cfg(test)]
thread_local! {
    static STAGED_BATCH_COMMIT_HOOK: RefCell<Option<Box<dyn FnMut()>>> =
        const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn set_staged_batch_commit_hook(hook: Option<Box<dyn FnMut()>>) {
    STAGED_BATCH_COMMIT_HOOK.with(|h| *h.borrow_mut() = hook);
}

#[cfg(test)]
pub(super) fn staged_batch_commit_hook() {
    STAGED_BATCH_COMMIT_HOOK.with(|h| {
        if let Some(f) = h.borrow_mut().as_mut() {
            f();
        }
    });
}

type ScanCoverOutcome = (Option<Vec<u8>>, vals_store::RtScanStats);

#[cfg(test)]
thread_local! {
    static SCAN_COVERED_BUDGET_OVERRIDE: Cell<Option<vals_store::ScanCoveredBudget>> = const { Cell::new(None) };
}

#[cfg(test)]
pub(super) fn set_scan_covered_budget(budget: Option<vals_store::ScanCoveredBudget>) {
    SCAN_COVERED_BUDGET_OVERRIDE.with(|b| b.set(budget));
}

#[cfg(test)]
static ZONE_SCAN_REPLY_BUDGETS: Mutex<Vec<(ZoneId, usize)>> = Mutex::new(Vec::new());

#[cfg(test)]
pub(crate) fn set_zone_scan_reply_budget(zone: ZoneId, budget: Option<usize>) {
    let mut budgets = ZONE_SCAN_REPLY_BUDGETS.lock().unwrap();
    budgets.retain(|(z, _)| *z != zone);
    budgets.extend(budget.map(|b| (zone, b)));
}

fn scan_reply_budget(_zone: ZoneId) -> usize {
    #[cfg(test)]
    if let Some(&(_, budget)) = ZONE_SCAN_REPLY_BUDGETS.lock().unwrap().iter().find(|(z, _)| *z == _zone) {
        return budget;
    }
    SCAN_REPLY_BUDGET
}

fn scan_covered_budget() -> vals_store::ScanCoveredBudget {
    #[cfg(test)]
    if let Some(budget) = SCAN_COVERED_BUDGET_OVERRIDE.with(Cell::get) {
        return budget;
    }
    vals_store::SCAN_COVERED_BUDGET
}

enum ScanData {
    Values(crate::replica::SpanPage),
    Keys(Vec<(Vec<u8>, Stored)>, bool),
}

fn scan_keys<D: BlockDevice>(
    s: &VersionedStore<D>,
    winners: Vec<(Vec<u8>, vals_store::index::IndexEntry)>,
) -> Vec<(Vec<u8>, Stored)> {
    winners.into_iter().map(|(key, mut entry)| {
        if let Some(current) = s.mutable_entry(&key) {
            if current.version > entry.version { entry = current; }
        }
        (key, Stored {
            version: crate::replica::unpack_version(entry.version),
            value: (!entry.tombstone).then(Vec::new),
        })
    }).collect()
}

fn finish_covered<D: BlockDevice + AsyncRead>(
    s: &VersionedStore<D>,
    done: vals_store::StagedScanDone,
    keys_only: bool,
) -> Result<(ScanData, ScanCoverOutcome), vals_store::EngineError> {
    let cover = (done.resume().map(<[u8]>::to_vec), done.rt_stats());
    let data = if keys_only {
        let (winners, more) = done.into_winners();
        ScanData::Keys(scan_keys(s, winners), more)
    } else {
        ScanData::Values(s.finish_staged_scan(done)?)
    };
    Ok((data, cover))
}

async fn serve_scan_staged<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    guard: &mut InflightGuard<'_>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    cap: usize,
    keys_only: bool,
) -> Result<(ScanData, u64, ScanCoverOutcome), vals_store::EngineError> {
    const MAX_RESTARTS: usize = 3;
    'restart: for _ in 0..MAX_RESTARTS {
        let (staged, mutable_generation) = {
            let s = store.borrow();
            (
                s.stage_scan_page_covered(lo, hi, cap, SCAN_REPLY_BUDGET, scan_covered_budget())?,
                s.mutable_generation(),
            )
        };
        let mut merge = match staged {
            vals_store::StagedScanPage::Done(done) => {
                let (page, cover) = finish_covered(&store.borrow(), done, keys_only)?;
                return Ok((page, mutable_generation, cover));
            }
            vals_store::StagedScanPage::Runs(merge) => merge,
        };
        merge.meter_server_scan();
        let generation = merge.generation();
        loop {
            let stepped = merge.step(dev).await;
            if store.borrow().storage_generation() != generation {
                crate::observe::counters().scan_staged_restarts.inc();
                continue 'restart;
            }
            let step = stepped?;
            guard.checkpoint().await;
            if store.borrow().storage_generation() != generation {
                crate::observe::counters().scan_staged_restarts.inc();
                continue 'restart;
            }
            match step {
                vals_store::StagedScanStep::Done => break,
                vals_store::StagedScanStep::NeedMem { from } => {
                    #[cfg(test)]
                    staged_scan_reborrow_hook();
                    let s = store.borrow();
                    if s.storage_generation() != generation {
                        crate::observe::counters().scan_staged_restarts.inc();
                        continue 'restart;
                    }
                    s.scan_supply_mem(&mut merge, &from)?;
                }
                vals_store::StagedScanStep::NeedBlocks { cursor } => {
                    #[cfg(test)]
                    staged_scan_reborrow_hook();
                    let s = store.borrow();
                    if s.storage_generation() != generation {
                        crate::observe::counters().scan_staged_restarts.inc();
                        continue 'restart;
                    }
                    s.scan_supply_blocks(&mut merge, cursor)?;
                }
            }
        }
        #[cfg(test)]
        staged_scan_reborrow_hook();
        let s = store.borrow();
        if s.storage_generation() != generation {
            crate::observe::counters().scan_staged_restarts.inc();
            continue 'restart;
        }
        let (page, cover) = finish_covered(&s, merge.finish(), keys_only)?;
        return Ok((page, mutable_generation, cover));
    }
    crate::observe::counters().scan_sync_fallbacks.inc();
    let s = store.borrow();
    Ok((
        if keys_only {
            let (winners, more) = s.engine().scan_winners_page(lo, hi, cap, SCAN_REPLY_BUDGET)?;
            ScanData::Keys(scan_keys(&s, winners), more)
        } else {
            ScanData::Values(s.plan_scan(lo, hi, cap, SCAN_REPLY_BUDGET)?)
        },
        s.mutable_generation(),
        (None, vals_store::RtScanStats::default()),
    ))
}

pub(super) const MIXED_STAGE_LOOKAHEAD: usize = 8;

#[cfg(test)]
thread_local! {
    pub(super) static MIXED_VALUE_STAGES: Cell<u64> = const { Cell::new(0) };
}

pub(super) enum MixedValueStage {
    Found(
        vals_store::ReadPlan,
        u64,
        Option<vals_store::index::IndexEntry>,
        Option<vals_store::engine::WinnerReadToken>,
        u128,
    ),
    Result(MixedReadResult),
}

fn publish_winner_probe_stats(stats: vals_store::engine::WinnerProbeStats) {
    let counters = crate::observe::counters();
    counters.winner_probe_lookups.add(stats.lookups);
    counters.winner_probe_home_slot_hits.add(stats.home_slot_hits);
    counters.winner_probe_empty_misses.add(stats.empty_misses);
    counters
        .winner_probe_fingerprint_mismatches
        .add(stats.fingerprint_mismatches);
    counters
        .winner_probe_deleted_buckets
        .add(stats.deleted_buckets);
    counters
        .winner_probe_collision_fallbacks
        .add(stats.collision_fallbacks);
    counters
        .winner_probe_full_key_collisions
        .add(stats.full_key_collisions);
    counters.winner_probe_length_total.add(stats.total_probe_len);
    counters.winner_probe_length_le_1.add(stats.probe_len_le_1);
    counters.winner_probe_length_2_4.add(stats.probe_len_2_4);
    counters.winner_probe_length_5_8.add(stats.probe_len_5_8);
    counters.winner_probe_length_gt_8.add(stats.probe_len_gt_8);
}

fn stage_mixed_winner<D: BlockDevice>(
    store: &RefCell<VersionedStore<D>>,
    token: Option<vals_store::engine::WinnerReadToken>,
) -> MixedValueStage {
    let Some(token) = token else {
        return MixedValueStage::Result(MixedReadResult::Reply(MixedReadReply::Value(None)));
    };
    let store = store.borrow();
    match store.engine().plan_winner_read(token) {
        Ok(plan) => MixedValueStage::Found(
            plan,
            store.storage_generation(),
            None,
            Some(token),
            token.entry.version,
        ),
        Err(e) => MixedValueStage::Result(MixedReadResult::Err(
            format!("{e}").chars().take(256).collect(),
        )),
    }
}

pub(super) async fn stage_mixed_value<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    key: &[u8],
) -> MixedValueStage {
    #[cfg(test)]
    MIXED_VALUE_STAGES.with(|count| count.set(count.get() + 1));
    for _ in 0..8 {
        match build_get_plan(store, dev, key).await {
            Ok(GetPlan::Found(plan, generation, mutable, directory_entry, version)) => {
                return MixedValueStage::Found(plan, generation, mutable, directory_entry, version);
            }
            Ok(GetPlan::Absent) => {
                return MixedValueStage::Result(MixedReadResult::Reply(
                    MixedReadReply::Value(None),
                ));
            }
            Ok(GetPlan::Covered(version)) => {
                return MixedValueStage::Result(MixedReadResult::Reply(MixedReadReply::Value(Some(
                    Stored { version: crate::replica::unpack_version(version), value: None },
                ))));
            }
            Ok(GetPlan::Changed) => continue,
            Err(e) => {
                return MixedValueStage::Result(MixedReadResult::Err(
                    format!("{e}").chars().take(256).collect(),
                ));
            }
        }
    }
    MixedValueStage::Result(MixedReadResult::Busy)
}

fn mixed_value_wire_bound(stage: &MixedValueStage) -> usize {
    match stage {
        MixedValueStage::Found(plan, ..) => plan.read_len().saturating_add(32).max(1029),
        MixedValueStage::Result(MixedReadResult::Err(_)) => 1029,
        MixedValueStage::Result(_) => 1,
    }
}

pub(super) async fn execute_mixed_value<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    key: &[u8],
    stage: MixedValueStage,
    site: crate::integrity::ReadSite,
) -> MixedReadResult {
    let MixedValueStage::Found(plan, generation, mutable, directory_entry, version) = stage else {
        let MixedValueStage::Result(result) = stage else {
            unreachable!()
        };
        return result;
    };
    let value = std::cell::RefCell::new(None);
    let save = |record_version, blob: &[u8]| {
        let (version, bytes) = crate::replica::decode_read(record_version, blob);
        *value.borrow_mut() = Some(Stored {
            version,
            value: bytes.map(<[u8]>::to_vec),
        });
    };
    let counters = crate::observe::counters();
    counters.point_value_device_reads.inc();
    counters
        .point_value_device_blocks
        .add(plan.read_len().div_ceil(dev.block_size()) as u64);
    let read = plan.read_value_checked(dev, key, version, save).await;
    let store = store.borrow();
    let changed = store.storage_generation() != generation
        || directory_entry.map_or_else(
            || store.mutable_entry(key) != mutable,
            |token| !store.engine().winner_read_token_matches(key, token),
        );
    if changed {
        return MixedReadResult::Busy;
    }
    if let Err(e) = read {
        crate::integrity::corrupt_read(site, "mixed", key, plan.ptr(), &e);
        return MixedReadResult::Err(format!("{e}").chars().take(256).collect());
    }
    let stored = value.into_inner().expect("value read callback ran");
    if stored.value.is_some() {
        counters.records_read.inc();
    }
    MixedReadResult::Reply(MixedReadReply::Value(Some(stored)))
}

pub(super) async fn serve_mixed_batch_read<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    start: u32,
    reads: &[(Vec<u8>, MixedReadRole)],
    sample_winner_probes: bool,
    site: crate::integrity::ReadSite,
) -> Response {
    let start = start as usize;
    if start > reads.len() {
        return Response::Err("mixed batch read cursor out of range".into());
    }
    if start == reads.len() {
        return Response::MixedBatchRead {
            start: start as u32,
            results: Vec::new(),
            more: false,
        };
    }

    let _guard = gate.acquire().await;
    let winner_tokens = {
        let keys: Vec<&[u8]> = reads.iter().map(|(key, _)| key.as_slice()).collect();
        if sample_winner_probes {
            let profiled = store.borrow().engine().winner_read_tokens_profiled(&keys);
            if let Some((_, stats)) = &profiled {
                publish_winner_probe_stats(*stats);
            }
            profiled.map(|(tokens, _)| tokens)
        } else {
            store.borrow().engine().winner_read_tokens(&keys)
        }
    };
    const MIXED_REPLY_BASE: usize = 1 + 4 + 1 + 4;
    const MIXED_ERROR_BOUND: usize = 1 + 4 + 256 * 4;
    let mut value_stages = Vec::new();
    let mut projected = MIXED_REPLY_BASE;
    let mut end = start;
    'page: while end < reads.len() {
        let window_end = (end + MIXED_STAGE_LOOKAHEAD).min(reads.len());
        let positions: Vec<usize> = (end..window_end)
            .filter(|&i| reads[i].1 == MixedReadRole::Value)
            .collect();
        let stage_futs: Vec<_> = positions
            .iter()
            .map(|&i| {
                let winner_tokens = &winner_tokens;
                async move {
                    let stage = match winner_tokens {
                        Some(tokens) => stage_mixed_winner(store, tokens[i]),
                        None => stage_mixed_value(store, dev, reads[i].0.as_slice()).await,
                    };
                    (i, stage)
                }
            })
            .collect();
        value_stages.resize_with(window_end - start, || None);
        for (i, stage) in vals_rt::join_all(stage_futs).await {
            value_stages[i - start] = Some(stage);
        }
        while end < window_end {
            let bytes = match reads[end].1 {
                MixedReadRole::Value => mixed_value_wire_bound(
                    value_stages[end - start]
                        .as_ref()
                        .expect("one staged result per value role"),
                ),
                MixedReadRole::Digest => MIXED_ERROR_BOUND,
            };
            if projected.saturating_add(bytes) > SCAN_REPLY_BUDGET {
                if end == start {
                    end += 1;
                }
                break 'page;
            }
            projected = projected.saturating_add(bytes);
            end += 1;
        }
    }

    let pending: Vec<_> = (start..end)
        .map(|i| (i, reads[i].1, value_stages[i - start].take()))
        .collect();
    let futs: Vec<_> = pending
        .into_iter()
        .map(|(i, role, stage)| {
            let key = reads[i].0.as_slice();
            let winner_tokens = &winner_tokens;
            async move {
                match role {
                    MixedReadRole::Value => {
                        let stage = stage.expect("one staged result per value role");
                        let frame_bound = mixed_value_wire_bound(&stage)
                            .saturating_add(MIXED_REPLY_BASE)
                            .saturating_add(8);
                        if frame_bound > MAX_FRAME {
                            MixedReadResult::Err(
                                "value exceeds mixed-read frame maximum".into(),
                            )
                        } else {
                            execute_mixed_value(store, dev, key, stage, site).await
                        }
                    }
                    MixedReadRole::Digest => {
                        if let Some(tokens) = winner_tokens {
                            MixedReadResult::Reply(MixedReadReply::Digest(
                                tokens[i]
                                    .map(|token| crate::replica::unpack_version(token.entry.version)),
                            ))
                        } else {
                            match serve_digest_inner(store, dev, key).await {
                                Response::Digest(version) => {
                                    MixedReadResult::Reply(MixedReadReply::Digest(version))
                                }
                                Response::Busy => MixedReadResult::Busy,
                                Response::Err(msg) => {
                                    MixedReadResult::Err(msg.chars().take(256).collect())
                                }
                                _ => MixedReadResult::Err("invalid digest response".into()),
                            }
                        }
                    }
                }
            }
        })
        .collect();
    let results = vals_rt::join_all(futs).await;
    Response::MixedBatchRead {
        start: start as u32,
        results,
        more: end < reads.len(),
    }
}

pub(super) async fn serve_sync_values<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    keys: &[Vec<u8>],
    site: crate::integrity::ReadSite,
) -> Response {
    let _guard = gate.acquire().await;
    for _ in 0..3 {
        let generation = store.borrow().aggregate_generation();
        let (plans, more) = match store.borrow().plan_values(keys, SCAN_REPLY_BUDGET) {
            Ok(p) => p,
            Err(e) => return Response::Err(format!("{e}")),
        };
        let entries = match execute_entry_plans(store, generation, plans, dev, site).await {
            Ok(Some(entries)) => entries,
            Ok(None) => continue,
            Err(_) if store.borrow().aggregate_generation() != generation => continue,
            Err(e) => return Response::Err(format!("{e}")),
        };
        if store.borrow().aggregate_generation() == generation {
            return Response::Values { entries, more };
        }
    }
    Response::Busy
}

