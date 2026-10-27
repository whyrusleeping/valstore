use super::*;

pub(super) async fn serve_read_into<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    key: &[u8],
    out: &ServerOut,
    reqid: u64,
    site: crate::integrity::ReadSite,
) {
    const MAX_ATTEMPTS: usize = 8;
    let _guard = gate.acquire().await;
    for _ in 0..MAX_ATTEMPTS {
        let (plan, generation, mutable, directory_entry, version) =
            match build_get_plan(store, dev, key).await {
                Ok(GetPlan::Found(plan, generation, mutable, directory_entry, version)) => {
                    (plan, generation, mutable, directory_entry, version)
                }
                Ok(GetPlan::Absent) => {
                    out.push_read_reply(reqid, None);
                    return;
                }
                Ok(GetPlan::Covered(version)) => {
                    out.push_read_reply(reqid, Some((crate::replica::unpack_version(version), None)));
                    return;
                }
                Ok(GetPlan::Changed) => continue,
                Err(e) => {
                    crate::observe::counters().server_errors.inc();
                    out.push_resp(reqid, &Response::Err(format!("{e}")));
                    return;
                }
            };
        let value = std::cell::RefCell::new(None);
        let save = |record_version, blob: &[u8]| {
            let (version, bytes) = crate::replica::decode_read(record_version, blob);
            *value.borrow_mut() = Some((version, bytes.map(<[u8]>::to_vec)));
        };
        let counters = crate::observe::counters();
        counters.point_value_device_reads.inc();
        counters
            .point_value_device_blocks
            .add(plan.read_len().div_ceil(dev.block_size()) as u64);
        let res = plan.read_value_checked(dev, key, version, save).await;
        if let Err(e) = res {
            let s = store.borrow();
            let changed = s.storage_generation() != generation
                || directory_entry.map_or_else(
                    || s.mutable_entry(key) != mutable,
                    |token| !s.engine().winner_read_token_matches(key, token),
                );
            if changed {
                continue;
            }
            crate::integrity::corrupt_read(site, "get", key, plan.ptr(), &e);
            crate::observe::counters().server_errors.inc();
            out.push_resp(reqid, &Response::Err(format!("{e}")));
            return;
        }
        let s = store.borrow();
        let changed = s.storage_generation() != generation
            || directory_entry.map_or_else(
                || s.mutable_entry(key) != mutable,
                |token| !s.engine().winner_read_token_matches(key, token),
            );
        if changed {
            continue;
        }
        drop(s);
        let value = value.into_inner().expect("value read callback ran");
        if value.1.is_some() {
            crate::observe::counters().records_read.inc();
        }
        out.push_read_reply(reqid, Some((value.0, value.1.as_deref())));
        return;
    }
    out.push_resp(reqid, &Response::Busy);
}

#[allow(clippy::large_enum_variant)]
pub(super) enum GetPlan {
    Found(
        vals_store::ReadPlan,
        u64,
        Option<vals_store::index::IndexEntry>,
        Option<vals_store::engine::WinnerReadToken>,
        u128,
    ),
    Absent,
    Covered(u128),
    Changed,
}

pub(super) async fn build_get_plan<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    key: &[u8],
) -> Result<GetPlan, vals_store::EngineError> {
    let probe = store.borrow().probe_get(key);
    let (probe_key, probes, mutable, generation, cover) = match probe {
        vals_store::StagedGet::Memtable(plan) => {
            let s = store.borrow();
            let mutable = s.mutable_entry(key);
            let Some(version) = mutable.map(|entry| entry.version) else {
                return Ok(GetPlan::Changed);
            };
            return Ok(GetPlan::Found(
                *plan,
                s.storage_generation(),
                mutable,
                None,
                version,
            ));
        }
        vals_store::StagedGet::WinnerDirectory { plan, token } => {
            let s = store.borrow();
            return Ok(GetPlan::Found(
                *plan,
                s.storage_generation(),
                None,
                Some(token),
                token.entry.version,
            ));
        }
        vals_store::StagedGet::Absent => return Ok(GetPlan::Absent),
        vals_store::StagedGet::Covered(version) => return Ok(GetPlan::Covered(version)),
        vals_store::StagedGet::Err(e) => return Err(e),
        vals_store::StagedGet::Runs {
            key,
            probes,
            mutable,
            generation,
            cover,
        } => (key, probes, mutable, generation, cover),
    };
    let c = crate::observe::counters();
    c.read_probe_runs.add(probes.len() as u64);
    let probe_bytes: u64 = probes.iter().map(|l| l.len() as u64).sum();
    let probed = vals_store::resolve_run_probe_counted(&probe_key, &probes, dev).await;
    let s = store.borrow();
    if s.storage_generation() != generation || s.mutable_entry(&probe_key) != mutable {
        return Ok(GetPlan::Changed);
    }
    let (run, blocks) = probed?;
    c.read_probe_blocks.add(blocks);
    c.read_probe_bytes.add(probe_bytes);
    let (ptr, version) = match (mutable, run) {
        (Some(mem), Some(cold)) if cold.version > mem.version => (cold.ptr, cold.version),
        (Some(mem), _) => (mem.ptr, mem.version),
        (None, Some(cold)) => (cold.ptr, cold.version),
        (None, None) => return Ok(cover.map_or(GetPlan::Absent, GetPlan::Covered)),
    };
    if let Some(cover) = cover.filter(|&c| version <= c) {
        return Ok(GetPlan::Covered(cover));
    }
    Ok(GetPlan::Found(
        s.plan_get_ptr(&ptr)?,
        generation,
        mutable,
        None,
        version,
    ))
}

pub(super) async fn serve_digest<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    gate: &ReadGate,
    key: &[u8],
) -> Response {
    let _guard = gate.acquire().await;
    serve_digest_inner(store, dev, key).await
}

pub(super) async fn serve_digest_inner<D: BlockDevice + AsyncRead>(
    store: &RefCell<VersionedStore<D>>,
    dev: &D,
    key: &[u8],
) -> Response {
    vals_store::attribution::DIGEST_LOOKUPS.add(1);
    if let Some(version) = store.borrow().winner_directory_version(key) {
        return Response::Digest(version);
    }
    const MAX_ATTEMPTS: usize = 8;
    let c = crate::observe::counters();
    for _ in 0..MAX_ATTEMPTS {
        let probe = store.borrow().probe_version_for(key, vals_store::attribution::LookupCaller::Digest);
        let (generation, mutable) = match &probe {
            vals_store::StagedVersion::Runs {
                probes,
                mutable,
                generation,
                ..
            } => {
                c.read_probe_runs.add(probes.len() as u64);
                (*generation, *mutable)
            }
            _ => {
                let s = store.borrow();
                (
                    s.storage_generation(),
                    s.mutable_entry(key).map(|entry| entry.version),
                )
            }
        };
        let probe_bytes = probe.probe_bytes();
        let resolved = VersionedStore::version_from_probe_counted(probe, dev).await;
        let s = store.borrow();
        if s.storage_generation() != generation
            || s.mutable_entry(key).map(|entry| entry.version) != mutable
        {
            continue;
        }
        match resolved {
            Ok((version, blocks)) => {
                c.read_probe_blocks.add(blocks);
                c.read_probe_bytes.add(probe_bytes);
                return Response::Digest(version);
            }
            Err(e) => return Response::Err(format!("{e}")),
        }
    }
    Response::Busy
}

