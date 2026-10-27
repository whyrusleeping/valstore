use super::*;

#[cfg(test)]
mod coordinator_tests;

#[allow(clippy::too_many_arguments)]
pub(crate) async fn serve_scan_rev<D: BlockDevice + AsyncRead>(
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
            match serve_staged(store, dev, &mut guard, lo, hi, cap, keys_only).await {
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
            Ok(Some(mut entries)) if store.borrow().storage_generation() == generation => {
                entries.reverse();
                match finalize_scan_entries(store, generation, mutable_generation, dev, entries, site).await {
                    Ok(Some(mut entries)) => {
                        entries.reverse();
                        page = Some((entries, more, cover));
                        break;
                    }
                    Ok(None) => crate::observe::counters().scan_span_retries.inc(),
                    Err(e) => return (Response::Err(e), Default::default()),
                }
            }
            Ok(_) => crate::observe::counters().scan_span_retries.inc(),
            Err(_) if store.borrow().storage_generation() != generation => {
                crate::observe::counters().scan_span_retries.inc();
            }
            Err(e) => return (Response::Err(format!("{e}")), Default::default()),
        }
    }
    let Some((mut entries, mut more, (mut resume, stats))) = page else {
        return (Response::Busy, Default::default());
    };
    publish_scan_rt_stats(stats);
    let lower = resume.as_deref().or_else(|| {
        if more { entries.last().map(|(k, _)| k.as_slice()) } else { lo }
    }).unwrap_or_default();
    let mut fragments = store.borrow().engine().range_tombstones().clipped(lower, hi);
    fit_reply(hi, &mut entries, &mut fragments, &mut resume, &mut more, scan_reply_budget(site.zone));
    if !range_tombstones && (!fragments.is_empty() || resume.is_some()) {
        return (Response::Err(RANGE_TOMBSTONES_REQUIRED.into()), stats);
    }
    if resume.is_some() {
        crate::observe::counters().scan_rt_resume_pages_total.inc();
    }
    crate::observe::histograms().scan_latency_us.observe(started.elapsed());
    crate::observe::counters().scan_entries_returned.add(entries.len() as u64);
    let fragments = fragments.into_iter().map(|f| crate::wire::RtFragment {
        lo: (!f.lo.is_empty()).then_some(f.lo), hi: f.hi,
        version: crate::replica::unpack_version(f.version),
    }).collect();
    (Response::Scan { entries, more, fragments, resume }, stats)
}

fn fit_reply(
    hi: Option<&[u8]>,
    entries: &mut Vec<(Vec<u8>, Stored)>,
    fragments: &mut Vec<vals_store::range_tombstone::Fragment>,
    resume: &mut Option<Vec<u8>>,
    more: &mut bool,
    budget: usize,
) {
    let (mut e, mut f, mut used) = (0usize, fragments.len(), 0usize);
    let cut = loop {
        let entry = entries.get(e);
        let fragment = f.checked_sub(1).map(|i| &fragments[i]);
        let take_fragment = match (entry, fragment) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some((k, _)), Some(x)) => x.hi.as_deref().is_none_or(|h| h > k.as_slice()),
        };
        let (cut, size) = if take_fragment {
            let x = fragment.unwrap();
            (x.hi.clone(), x.lo.len() + x.hi.as_ref().map_or(0, Vec::len) + SCAN_FRAGMENT_WIRE_OVERHEAD)
        } else if let Some((key, stored)) = entry {
            let mut after = key.clone();
            after.push(0);
            (Some(after), key.len() + stored.value.as_ref().map_or(0, Vec::len) + SCAN_ENTRY_WIRE_OVERHEAD)
        } else {
            return;
        };
        if used.saturating_add(size) > budget && (e > 0 || f < fragments.len()) {
            if let Some(cut) = cut.filter(|cut| hi.is_none_or(|h| cut.as_slice() < h)) {
                break cut;
            }
        }
        used = used.saturating_add(size);
        if take_fragment { f -= 1; } else { e += 1; }
    };
    entries.truncate(e);
    fragments.drain(..f);
    fragments.retain(|x| x.hi.as_ref().is_none_or(|hi| hi > &cut));
    for x in fragments.iter_mut() {
        if x.lo < cut { x.lo = cut.clone(); }
    }
    *resume = Some(cut);
    *more = true;
}

async fn serve_staged<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    guard: &mut InflightGuard<'_>,
    lo: Option<&[u8]>,
    hi: Option<&[u8]>,
    cap: usize,
    keys_only: bool,
) -> Result<(ScanData, u64, ScanCoverOutcome), vals_store::EngineError> {
    'restart: for _ in 0..3 {
        let (staged, mutable_generation) = {
            let s = store.borrow();
            (s.stage_scan_page_rev(lo, hi, cap, SCAN_REPLY_BUDGET, scan_covered_budget())?, s.mutable_generation())
        };
        let mut merge = match staged {
            vals_store::StagedScanRevPage::Done(done) => {
                let (page, cover) = finish_covered(&store.borrow(), done, keys_only)?;
                return Ok((page, mutable_generation, cover));
            }
            vals_store::StagedScanRevPage::Runs(merge) => merge,
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
            #[cfg(test)]
            staged_scan_reborrow_hook();
            let s = store.borrow();
            if s.storage_generation() != generation {
                crate::observe::counters().scan_staged_restarts.inc();
                continue 'restart;
            }
            match step {
                vals_store::StagedScanStep::Done => {
                    let (page, cover) = finish_covered(&s, merge.finish(), keys_only)?;
                    return Ok((page, mutable_generation, cover));
                }
                vals_store::StagedScanStep::NeedMem { from } => s.engine().scan_supply_mem_rev(&mut merge, &from)?,
                vals_store::StagedScanStep::NeedBlocks { cursor } => s.engine().scan_supply_blocks_rev(&mut merge, cursor)?,
            }
        }
    }
    crate::observe::counters().scan_sync_fallbacks.inc();
    let s = store.borrow();
    Ok((
        if keys_only {
            let (winners, more) = s.engine().scan_winners_page_rev(lo, hi, cap, SCAN_REPLY_BUDGET)?;
            ScanData::Keys(scan_keys(&s, winners), more)
        } else {
            ScanData::Values(s.plan_scan_rev(lo, hi, cap, SCAN_REPLY_BUDGET)?)
        },
        s.mutable_generation(),
        (None, vals_store::RtScanStats::default()),
    ))
}

#[cfg(test)]
mod tests;
