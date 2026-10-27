use super::*;

impl<D: BlockDevice> VersionedStore<D> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        Ok(self.read(key)?.and_then(|s| s.value))
    }

    pub fn get_versioned(&self, key: &[u8]) -> Result<Option<Stored>, EngineError> {
        self.read(key)
    }

    pub fn scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Stored)>, EngineError> {
        Ok(self.scan_page(lo, hi, usize::MAX, usize::MAX)?.0)
    }

    pub fn scan_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<EntryPage, EngineError> {
        let (raw, more) = self
            .engine
            .scan_bounded_versioned(lo, hi, max_entries, max_bytes)?;
        let page = raw
            .into_iter()
            .map(|(key, blob, raw_version)| {
                let (kind, val) = decode(&blob);
                let version = unpack_version(raw_version);
                (
                    key,
                    Stored {
                        version,
                        value: (kind == KIND_PUT).then(|| val.to_vec()),
                    },
                )
            })
            .collect();
        Ok((page, more))
    }

    pub fn entries(&self) -> Result<Vec<(Vec<u8>, Stored)>, EngineError> {
        let all = self.engine.recovered_versions()?;
        let mut out = Vec::with_capacity(all.len());
        for (key, _) in all {
            if let Some(stored) = self.read(&key)? {
                out.push((key.to_vec(), stored));
            }
        }
        Ok(out)
    }

    pub fn keys(&self) -> Result<Vec<Vec<u8>>, EngineError> {
        Ok(self
            .engine
            .recovered_versions()?
            .into_iter()
            .map(|(k, _)| k.to_vec())
            .collect())
    }

    pub fn stored(&self, key: &[u8]) -> Result<Option<Stored>, EngineError> {
        self.read(key)
    }

    pub fn version_leaf_digests(
        &self,
        num_buckets: u32,
    ) -> Result<Vec<u64>, LeafDigestError> {
        self.ensure_seeded()?;
        self.versions.leaf_digests(num_buckets)
    }

    pub fn scoped_leaf_digests(
        &self,
        level: u32,
        scope_g: u32,
        scope: &std::collections::HashSet<u32>,
        counterpart: Option<NodeId>,
    ) -> Result<Vec<(u32, u64)>, LeafDigestError> {
        self.ensure_seeded()?;
        if self.regroup_pending() {
            return Err(LeafDigestError::Dirty);
        }
        let own = self.node;
        let scoped = counterpart.is_some() && self.has_placement();
        let keep_group = |group: &[NodeId]| -> bool {
            match counterpart {
                Some(peer) if scoped => group.contains(&own) && group.contains(&peer),
                _ => true,
            }
        };
        self.versions
            .scoped_leaf_digests(level, scope_g, scope, keep_group)
    }

    pub fn bucket_entries(
        &self,
        num_buckets: u32,
        ids: &[u32],
    ) -> Result<Vec<(Vec<u8>, Stored)>, EngineError> {
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
        let (digests, _more) = self.engine.version_digest_page(
            &want,
            |k| crate::antientropy::bucket_of(k, num_buckets),
            None,
            usize::MAX,
            0,
        )?;
        let mut out = Vec::with_capacity(digests.len());
        for (key, _version) in digests {
            if let Some(stored) = self.read(&key)? {
                out.push((key, stored));
            }
        }
        Ok(out)
    }

    pub fn bucket_digest_page(
        &self,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<DigestPage, EngineError> {
        debug_assert!(
            self.engine.run_count() == 0 || num_buckets == INDEX_BUCKETS,
            "bucket_digest_page over runs is finest-level only: num_buckets ({num_buckets}) \
             must equal INDEX_BUCKETS ({INDEX_BUCKETS})"
        );
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
        let mut cursor = after.map(<[u8]>::to_vec);
        let mut out: Vec<(Vec<u8>, Version)> = Vec::new();
        loop {
            let (raw, more) = self.engine.version_digest_page(
                &want,
                |k| crate::antientropy::bucket_of(k, num_buckets),
                cursor.as_deref(),
                max_bytes,
                DIGEST_WIRE_OVERHEAD,
            )?;
            let last = raw.last().map(|(key, _)| key.clone());
            let mut page: Vec<(Vec<u8>, Version)> = raw
                .into_iter()
                .map(|(k, v)| (k, unpack_version(v)))
                .collect();
            let tail_covered = self.filter_digest_page(&mut page);
            if out.is_empty() {
                out = page;
                if !more || !tail_covered {
                    return Ok((out, more));
                }
            } else if let Some(first) = page.into_iter().next() {
                out.push(first);
                return Ok((out, true));
            } else if !more {
                return Ok((out, false));
            }
            cursor = last;
        }
    }

    pub fn plan_get(&self, key: &[u8]) -> Result<Option<vals_store::ReadPlan>, EngineError>
    where
        D: AsyncRead,
    {
        self.engine.plan_get(key)
    }

    pub fn probe_get(&self, key: &[u8]) -> vals_store::StagedGet
    where
        D: AsyncRead,
    {
        self.engine.probe_get(key)
    }

    pub fn plan_get_ptr(
        &self,
        ptr: &vals_store::vlog::ValuePtr,
    ) -> Result<vals_store::ReadPlan, EngineError>
    where
        D: AsyncRead,
    {
        self.engine.plan_get_ptr(ptr)
    }

    pub fn probe_version(&self, key: &[u8]) -> vals_store::StagedVersion
    where
        D: AsyncRead,
    {
        self.probe_version_for(key, vals_store::attribution::LookupCaller::Exists)
    }

    pub(crate) fn probe_version_for(
        &self,
        key: &[u8],
        caller: vals_store::attribution::LookupCaller,
    ) -> vals_store::StagedVersion
    where
        D: AsyncRead,
    {
        self.engine.probe_version_for(key, caller)
    }

    pub fn winner_directory_version(&self, key: &[u8]) -> Option<Option<Version>> {
        self.engine
            .winner_directory_version(key)
            .map(|version| version.map(unpack_version))
    }

    pub async fn version_from_probe(
        probe: vals_store::StagedVersion,
        dev: &D,
    ) -> Result<Option<Version>, EngineError>
    where
        D: AsyncRead,
    {
        Ok(Engine::<D>::version_from_probe(probe, dev)
            .await?
            .map(unpack_version))
    }

    pub async fn version_from_probe_counted(
        probe: vals_store::StagedVersion,
        dev: &D,
    ) -> Result<(Option<Version>, u64), EngineError>
    where
        D: AsyncRead,
    {
        let (v, blocks) = Engine::<D>::version_from_probe_counted(probe, dev).await?;
        Ok((v.map(unpack_version), blocks))
    }

    pub fn finish_cleanup_page(
        &self,
        done: vals_store::StagedScanDone,
    ) -> Result<(Vec<(Vec<u8>, Version)>, bool), EngineError>
    where
        D: AsyncRead,
    {
        let (raw, more) = self.engine.finish_staged_versions(done)?;
        Ok((raw.into_iter().map(|(k, v)| (k, unpack_version(v))).collect(), more))
    }

    pub fn plan_bucket_digest_page(
        &self,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
        max_bytes: usize,
    ) -> Result<vals_store::DigestPagePlan, EngineError>
    where
        D: AsyncRead,
    {
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
        self.engine.plan_version_digest_page_covered(
            &want,
            move |k| crate::antientropy::bucket_of(k, num_buckets),
            after,
            max_bytes,
            DIGEST_WIRE_OVERHEAD,
            self.digest_cover(),
        )
    }

    pub async fn bucket_digest_page_from_plan(
        plan: vals_store::DigestPagePlan,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
        max_bytes: usize,
        dev: &D,
    ) -> Result<DigestPage, EngineError>
    where
        D: AsyncRead,
    {
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
        let (raw, more) = Engine::<D>::version_digest_page_from_plan(
            plan,
            &want,
            move |k| crate::antientropy::bucket_of(k, num_buckets),
            after,
            max_bytes,
            DIGEST_WIRE_OVERHEAD,
            dev,
        )
        .await?;
        let page = raw
            .into_iter()
            .map(|(k, v)| (k, unpack_version(v)))
            .collect();
        Ok((page, more))
    }

    pub async fn bucket_digest_page_from_plan_cooperative<C: vals_store::DigestCheckpoint>(
        plan: vals_store::DigestPagePlan,
        num_buckets: u32,
        ids: &[u32],
        after: Option<&[u8]>,
        max_bytes: usize,
        dev: &D,
        checkpoint: &mut C,
    ) -> Result<Option<(DigestPage, Option<Vec<u8>>)>, EngineError>
    where
        D: AsyncRead,
    {
        let want: std::collections::HashSet<u32> = ids.iter().copied().collect();
        let Some((raw, more, resume)) = Engine::<D>::version_digest_page_from_plan_cooperative(
            plan,
            &want,
            move |k| crate::antientropy::bucket_of(k, num_buckets),
            after,
            max_bytes,
            DIGEST_WIRE_OVERHEAD,
            dev,
            checkpoint,
        )
        .await?
        else {
            return Ok(None);
        };
        let page = raw
            .into_iter()
            .map(|(k, v)| (k, unpack_version(v)))
            .collect();
        Ok(Some(((page, more), resume)))
    }

    pub fn plan_scan(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<SpanPage, EngineError>
    where
        D: AsyncRead,
    {
        let (keys, plan, more) = self.engine.scan_plan(lo, hi, max_entries, max_bytes)?;
        let keys = keys
            .into_iter()
            .map(|(k, v)| (k, unpack_version(v)))
            .collect();
        Ok((keys, plan, more))
    }

    pub fn stage_scan_page(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<vals_store::StagedScanPage, EngineError>
    where
        D: AsyncRead,
    {
        self.engine.stage_scan_page(lo, hi, max_entries, max_bytes)
    }

    pub fn stage_scan_page_covered(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        budget: vals_store::ScanCoveredBudget,
    ) -> Result<vals_store::StagedScanPage, EngineError>
    where
        D: AsyncRead,
    {
        self.engine.stage_scan_page_covered(lo, hi, max_entries, max_bytes, budget)
    }

    pub fn stage_scan_page_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
        budget: vals_store::ScanCoveredBudget,
    ) -> Result<vals_store::StagedScanRevPage, EngineError>
    where
        D: AsyncRead,
    {
        self.engine.stage_scan_page_rev(lo, hi, max_entries, max_bytes, budget)
    }

    pub fn plan_scan_rev(
        &self,
        lo: Option<&[u8]>,
        hi: Option<&[u8]>,
        max_entries: usize,
        max_bytes: usize,
    ) -> Result<SpanPage, EngineError>
    where
        D: AsyncRead,
    {
        let (keys, plan, more) = self.engine.scan_plan_rev(lo, hi, max_entries, max_bytes)?;
        let keys = keys.into_iter().map(|(k, v)| (k, unpack_version(v))).collect();
        Ok((keys, plan, more))
    }

    pub fn storage_generation(&self) -> u64 {
        self.engine.storage_generation()
    }

    pub fn mutable_generation(&self) -> u64 {
        self.engine.mutable_generation()
    }

    pub fn aggregate_generation(&self) -> (u64, u64) {
        (
            self.engine.storage_generation(),
            self.engine.mutable_generation(),
        )
    }

    pub(crate) fn tree_scope_token(
        &self,
        scope_g: u32,
        scope: &std::collections::HashSet<u32>,
    ) -> TreeScopeToken {
        let buckets = self
            .versions
            .buckets
            .borrow()
            .iter()
            .enumerate()
            .filter_map(|(slot, bucket)| {
                scope
                    .contains(&(slot as u32 % scope_g))
                    .then_some((slot as u32, bucket.generation))
            })
            .collect();
        TreeScopeToken {
            buckets,
            placement: self.placement_generation.get(),
        }
    }

    pub(crate) fn tree_scope_token_valid(&self, token: &TreeScopeToken) -> bool {
        if self.placement_generation.get() != token.placement || self.regroup_pending() || self.rt_fold_pending() {
            return false;
        }
        let buckets = self.versions.buckets.borrow();
        token
            .buckets
            .iter()
            .all(|&(slot, generation)| buckets[slot as usize].generation == generation)
    }

    pub(super) fn dirty_snapshot(
        &self,
        scope_g: u32,
        scope: &std::collections::HashSet<u32>,
    ) -> Option<DirtySnapshot> {
        self.versions.refresh_dirty_gauges();
        let metadata = self.versions.buckets.borrow();
        let buckets = self
            .versions
            .dirty
            .borrow()
            .keys()
            .filter_map(|&slot| {
                scope
                    .contains(&(slot as u32 % scope_g))
                    .then_some((slot as u32, metadata[slot].generation))
            })
            .collect::<Vec<_>>();
        (!buckets.is_empty()).then(|| DirtySnapshot {
            buckets,
            aggregate: self.aggregate_generation(),
            placement: self.placement_generation.get(),
            accounted: self.accounted_epoch(),
        })
    }

    pub(super) fn dirty_snapshot_valid(&self, snapshot: &DirtySnapshot) -> bool {
        if self.aggregate_generation() != snapshot.aggregate
            || self.placement_generation.get() != snapshot.placement
            || self.regroup_pending()
            || self.rt_fold_pending()
            || self.accounted_epoch() != snapshot.accounted
        {
            return false;
        }
        let buckets = self.versions.buckets.borrow();
        let dirty = self.versions.dirty.borrow();
        snapshot.buckets.iter().all(|&(slot, generation)| {
            buckets[slot as usize].generation == generation && dirty.contains_key(&(slot as usize))
        })
    }

    pub(super) fn publish_dirty_rebuild(
        &self,
        snapshot: &DirtySnapshot,
        digests: std::collections::HashMap<Box<[NodeId]>, std::collections::HashMap<u32, u64>>,
        counts: &std::collections::HashMap<u32, u64>,
    ) -> bool {
        if !self.dirty_snapshot_valid(snapshot) {
            return false;
        }
        let mut groups = self.versions.leaves.borrow_mut();
        for &(slot, _) in &snapshot.buckets {
            for leaves in groups.values_mut() {
                leaves[slot as usize] = 0;
            }
        }
        for (group, bucket_digests) in digests {
            let leaves = groups.entry(group).or_insert_with(VersionIndex::empty_group);
            for (slot, digest) in bucket_digests {
                leaves[slot as usize] = digest;
            }
        }
        let mut buckets = self.versions.buckets.borrow_mut();
        let mut dirty = self.versions.dirty.borrow_mut();
        let mut live = self.versions.live.get();
        for &(slot, generation) in &snapshot.buckets {
            let bucket = &mut buckets[slot as usize];
            if bucket.generation != generation || !dirty.contains_key(&(slot as usize)) {
                return false;
            }
            let count = counts.get(&slot).copied().unwrap_or(0);
            live = live.saturating_sub(bucket.logical_keys).saturating_add(count);
            bucket.logical_keys = count;
            dirty.remove(&(slot as usize));
        }
        self.versions.live.set(live);
        drop(dirty);
        drop(buckets);
        drop(groups);
        self.versions.refresh_dirty_gauges();
        true
    }

    pub fn mutable_entry(&self, key: &[u8]) -> Option<vals_store::index::IndexEntry> {
        self.engine.mutable_entry(key)
    }

    #[cfg(test)]
    pub(crate) fn bump_scan_generation_for_test(&mut self) {
        self.engine.truncate().expect("test generation bump");
        self.engine
            .put(b"generation-churn", b"v", 1)
            .expect("test rewrite");
        self.engine
            .flush(vals_store::RunBuildOpts::default())
            .expect("test flush");
    }

    pub fn scan_supply_mem(
        &self,
        merge: &mut vals_store::StagedScanMerge,
        from: &[u8],
    ) -> Result<(), EngineError>
    where
        D: AsyncRead,
    {
        self.engine.scan_supply_mem(merge, from)
    }

    pub fn scan_supply_blocks(
        &self,
        merge: &mut vals_store::StagedScanMerge,
        cursor: usize,
    ) -> Result<(), EngineError>
    where
        D: AsyncRead,
    {
        self.engine.scan_supply_blocks(merge, cursor)
    }

    pub fn finish_staged_scan(
        &self,
        done: vals_store::StagedScanDone,
    ) -> Result<SpanPage, EngineError>
    where
        D: AsyncRead,
    {
        let (keys, plan, more) = self.engine.plan_staged_scan(done)?;
        let keys = keys
            .into_iter()
            .map(|(k, v)| (k, unpack_version(v)))
            .collect();
        Ok((keys, plan, more))
    }

    pub fn plan_values(&self, keys: &[Vec<u8>], max_bytes: usize) -> Result<PlanPage, EngineError>
    where
        D: AsyncRead,
    {
        let mut out = Vec::with_capacity(keys.len());
        let mut bytes = 0usize;
        let mut more = false;
        for (i, key) in keys.iter().enumerate() {
            let sz = key.len() + self.engine.record_len(key).unwrap_or(0) as usize;
            if i > 0 && bytes.saturating_add(sz) > max_bytes {
                more = true;
                break;
            }
            bytes = bytes.saturating_add(sz);
            if let Some(plan) = self.engine.plan_get(key)? {
                out.push((key.clone(), ValuePlan::Read(plan)));
            } else if let Some(cover) = self.engine.range_tombstone_cover(key) {
                out.push((key.clone(), ValuePlan::Covered(unpack_version(cover))));
            }
        }
        Ok((out, more))
    }

    pub fn values_page(
        &self,
        keys: &[Vec<u8>],
        max_bytes: usize,
    ) -> Result<EntryPage, EngineError> {
        let mut out = Vec::with_capacity(keys.len());
        let mut bytes = 0usize;
        let mut more = false;
        for (i, key) in keys.iter().enumerate() {
            let sz = key.len() + self.engine.record_len(key).unwrap_or(0) as usize;
            if i > 0 && bytes.saturating_add(sz) > max_bytes {
                more = true;
                break;
            }
            bytes = bytes.saturating_add(sz);
            if let Some(stored) = self.read(key)? {
                out.push((key.clone(), stored));
            }
        }
        Ok((out, more))
    }

    pub fn device_handle(&self) -> std::sync::Arc<D> {
        self.engine.device_handle()
    }

    pub fn version(&self, key: &[u8]) -> Result<Option<Version>, EngineError> {
        self.current_version(key)
    }

    #[cfg(test)]
    pub(crate) fn digest_version(&self, key: &[u8]) -> Result<Option<Version>, EngineError> {
        Ok(self.engine.version_for(key, vals_store::attribution::LookupCaller::Digest)?.map(unpack_version))
    }

    pub(super) fn read(&self, key: &[u8]) -> Result<Option<Stored>, EngineError> {
        let Some(version) = self.current_version(key)? else {
            return Ok(None);
        };
        if self.engine.range_tombstone_cover(key) == Some(pack_version(version)) {
            return Ok(Some(Stored { version, value: None }));
        }
        Ok(self.engine.get(key)?.map(|raw| {
            let (kind, val) = decode(&raw);
            Stored {
                version,
                value: (kind == KIND_PUT).then(|| val.to_vec()),
            }
        }))
    }

    pub(super) fn current_version(&self, key: &[u8]) -> Result<Option<Version>, EngineError> {
        Ok(self.engine.version(key)?.map(unpack_version))
    }

}
